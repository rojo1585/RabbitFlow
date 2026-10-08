using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RedRabbit.Abstractions;
using RedRabbit.Configuration;
using RedRabbit.Diagnostics;
using RedRabbit.Exceptions;
using RedRabbit.Infrastructure.Connection;
using RedRabbit.Infrastructure.Topology;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Channels;
using global::RedRabbit.Infrastructure.Versioning;

namespace RedRabbit.Infrastructure.Consuming;
/// <summary>
/// Consumes messages from a single RabbitMQ queue and dispatches them in batches
/// to registered <see cref="IBatchRabbitHandler{TEvent}"/> implementations.
///
/// <para>
/// Architecture: A <see cref="Channel{T}"/> bridges the RabbitMQ callback thread and the
/// dispatch loop. Messages arrive via <c>OnMessageReceived</c> → channel write.
/// A background loop reads from the channel, buffers messages, and dispatches when:
/// <list type="bullet">
///   <item>The batch reaches <see cref="RabbitConsumerOptions.BatchSize"/>.</item>
///   <item>The timeout <see cref="RabbitConsumerOptions.BatchTimeoutMs"/> expires since the first message.</item>
/// </list>
/// </para>
///
/// <para>
/// Acknowledgment: All-or-nothing. The entire batch is ACKed after the handler succeeds.
/// If the handler throws, ALL messages are NACKed (retry/DLQ applies to all).
/// </para>
/// </summary>
internal sealed class NamedBatchRabbitConsumer : IAsyncDisposable
{
    /// <summary>
    /// Caches compiled batch handler invokers per handler type.
    /// Avoids reflection on every batch dispatch.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, Func<object, IReadOnlyList<object>, IReadOnlyList<MessageContext>, CancellationToken, Task>> BatchHandlerInvokers = new();

    private readonly string _consumerKey;
    private readonly RabbitConsumerOptions _options;
    private readonly ManagedConnection _connection;
    private readonly HandlerTypeRegistry _registry;
    private readonly IMessageSerializer _serializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<NamedBatchRabbitConsumer> _logger;
    private readonly RabbitMqMetrics _metrics;
    private readonly RetryPolicy? _retryPolicy;
    private readonly RetryQueuePublisher? _retryPublisher;
    private readonly EventUpgraderRegistry? _upgraderRegistry;
    private volatile IChannel? _currentChannel;
    private volatile bool _disposed;

    /// <summary>
    /// Set when the host's stopping token fires. From then on, cancellations and closed
    /// buffers are shutdown artefacts: affected messages are NACKed with requeue and never
    /// counted as a retry or dead-lettered.
    /// </summary>
    private volatile bool _isShuttingDown;

    /// <summary>
    /// Token passed to batch handlers. Cancelled only when the shutdown drain timeout expires
    /// (or on dispose) — NOT when shutdown starts — so the in-flight batch can finish and be
    /// ACKed during the drain window.
    /// </summary>
    private readonly CancellationTokenSource _handlerCts = new();

    public NamedBatchRabbitConsumer(string consumerKey,
                                    RabbitConsumerOptions options,
                                    ManagedConnection connection,
                                    HandlerTypeRegistry registry,
                                    IMessageSerializer serializer,
                                    IServiceScopeFactory scopeFactory,
                                    ILogger<NamedBatchRabbitConsumer> logger,
                                    RabbitMqMetrics metrics,
                                    EventUpgraderRegistry? upgraderRegistry = null)
    {
        if (!options.EnableBatchConsumer)
            throw new ArgumentException($"Consumer '{consumerKey}' is not configured for batch mode. Set EnableBatchConsumer=true in the consumer options.", nameof(options));

        _consumerKey = consumerKey;
        _options = options;
        _connection = connection;
        _registry = registry;
        _serializer = serializer;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _metrics = metrics;
        _upgraderRegistry = upgraderRegistry;

        if (options.EnableRetry)
        {
            _retryPolicy = new RetryPolicy(options);
            _retryPublisher = new RetryQueuePublisher(consumerKey, options.QueueName, connection, maxConcurrency: 1, logger);
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[BatchConsumer:{Key}] Starting batch consumer loop → queue '{Queue}' (batchSize={BatchSize}, timeout={TimeoutMs}ms)", _consumerKey, _options.QueueName, _options.BatchSize, _options.BatchTimeoutMs);

        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            IChannel? channel = null;
            try
            {
                channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                _currentChannel = channel;

                await InitializeChannelAsync(channel, cancellationToken).
                    ConfigureAwait(false);

                var messageChannel = Channel.CreateBounded<BufferedMessage>(new BoundedChannelOptions(_options.PrefetchCount * 2)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });

                var consumer = new AsyncEventingBasicConsumer(channel);

                async Task OnReceived(object sender, BasicDeliverEventArgs ea)
                {
                    await OnMessageReceived(ea, messageChannel.Writer)
                        .ConfigureAwait(false);
                }
                consumer.ReceivedAsync += OnReceived;

                var shutdownTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

                Task OnShutdown(object sender, ShutdownEventArgs e)
                {
                    consumer.ReceivedAsync -= OnReceived;
                    shutdownTcs.TrySetResult(true);
                    return Task.CompletedTask;
                }

                channel.ChannelShutdownAsync += OnShutdown;

                var consumerTag = await channel.BasicConsumeAsync(
                    queue: _options.QueueName,
                    autoAck: false,
                    consumerTag: _options.ConsumerTag ?? string.Empty,
                    consumer: consumer,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("[BatchConsumer:{Key}] Consuming from '{Queue}' (tag={Tag}, prefetch={Prefetch}, singleActive={SingleActive})", _consumerKey, _options.QueueName, consumerTag, _options.PrefetchCount, _options.SingleActiveConsumer);

                var dispatchTask = RunDispatchLoopAsync(channel, messageChannel.Reader);

                try
                {
                    var stoppingTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    using var reg = cancellationToken.Register(() => stoppingTcs.TrySetResult());

                    await Task.WhenAny(shutdownTcs.Task, stoppingTcs.Task).ConfigureAwait(false);

                    if (stoppingTcs.Task.IsCompleted)
                    {
                        _isShuttingDown = true;
                        await GracefulDrainAsync(channel, consumerTag, messageChannel.Writer, dispatchTask)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        messageChannel.Writer.TryComplete();
                        await dispatchTask.ConfigureAwait(false);
                    }
                }
                finally
                {
                    channel.ChannelShutdownAsync -= OnShutdown;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("[BatchConsumer:{Key}] Consumer loop stopped", _consumerKey);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[BatchConsumer:{Key}] Channel/consume error, reconnecting in 2s...", _consumerKey);

                try
                {
                    await Task.Delay(2000, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            finally
            {
                _currentChannel = null;
                if (channel is not null)
                    await SafeCloseChannelAsync(channel)
                        .ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Graceful-shutdown protocol, run when the host's stopping token fires:
    /// <list type="number">
    ///   <item>The buffer is completed, so the dispatch loop flushes what it already holds as a
    ///   final <c>drain</c> batch and exits. Deliveries that arrive afterwards (or were waiting
    ///   for buffer space) are NACKed with requeue by <see cref="OnMessageReceived"/>. Completing
    ///   the buffer first also releases a delivery callback blocked on a full buffer, which could
    ///   otherwise hold up the consumer cancellation below.</item>
    ///   <item><c>BasicCancelAsync</c> (bounded by <see cref="CancelConsumerTimeout"/>) — the broker
    ///   stops delivering new messages.</item>
    ///   <item>The loop gets <see cref="RabbitConsumerOptions.ShutdownDrainTimeout"/> to finish
    ///   (the in-flight batch is ACKed normally).</item>
    ///   <item>If the timeout expires, <see cref="_handlerCts"/> is cancelled: the in-flight batch
    ///   is abandoned and NACKed with requeue, and anything still buffered is NACKed with requeue.</item>
    /// </list>
    /// The channel is closed by <see cref="RunAsync"/> only after this method returns, so the
    /// final ACK/NACKs reach the broker.
    /// </summary>
    private async Task GracefulDrainAsync(IChannel channel, string consumerTag, ChannelWriter<BufferedMessage> writer, Task dispatchTask)
    {
        writer.TryComplete();

        try
        {
            using var cancelTimeout = new CancellationTokenSource(CancelConsumerTimeout);
            await channel.BasicCancelAsync(consumerTag, cancellationToken: cancelTimeout.Token)
                .ConfigureAwait(false);
            _logger.LogInformation("[BatchConsumer:{Key}] Consumer cancelled; draining buffered messages (timeout={Timeout}s)...", _consumerKey, _options.ShutdownDrainTimeout.TotalSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[BatchConsumer:{Key}] BasicCancelAsync failed during graceful drain", _consumerKey);
        }

        if (await Task.WhenAny(dispatchTask, Task.Delay(_options.ShutdownDrainTimeout)).ConfigureAwait(false) == dispatchTask)
        {
            _logger.LogInformation("[BatchConsumer:{Key}] All buffered messages processed during graceful drain.", _consumerKey);
            return;
        }

        _logger.LogWarning("[BatchConsumer:{Key}] Graceful drain timeout expired; cancelling the in-flight batch (its messages and any still buffered will be NACKed with requeue, no retry count).", _consumerKey);

        try { _handlerCts.Cancel(); }
        catch (ObjectDisposedException) { }

        // The loop now abandons the in-flight batch and requeues the rest — only NACKs left to send.
        if (await Task.WhenAny(dispatchTask, Task.Delay(AbandonGracePeriod)).ConfigureAwait(false) != dispatchTask)
            _logger.LogWarning("[BatchConsumer:{Key}] Dispatch loop did not finish within {Grace}s after cancellation; closing the channel (the broker will requeue unacked messages).", _consumerKey, AbandonGracePeriod.TotalSeconds);
    }

    /// <summary>
    /// Maximum time to wait, after the drain timeout, for the dispatch loop to send its
    /// requeue NACKs before the channel is closed.
    /// </summary>
    private static readonly TimeSpan AbandonGracePeriod = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum time to wait for the broker to confirm the consumer cancellation during shutdown.
    /// </summary>
    private static readonly TimeSpan CancelConsumerTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Background loop that reads messages from the buffer and dispatches them in batches.
    /// A batch is flushed when it reaches <see cref="RabbitConsumerOptions.BatchSize"/>, when
    /// <see cref="RabbitConsumerOptions.BatchTimeoutMs"/> elapses since its first message, or
    /// immediately once the buffer is completed (shutdown / channel closed). The loop exits when
    /// the buffer is completed and empty.
    /// </summary>
    private async Task RunDispatchLoopAsync(IChannel channel, ChannelReader<BufferedMessage> reader)
    {
        var buffer = new List<BufferedMessage>(_options.BatchSize);
        var batchTimeout = TimeSpan.FromMilliseconds(_options.BatchTimeoutMs);

        try
        {
            // Wait (without timeout) for the first message of the next batch.
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                var deadline = DateTime.UtcNow + batchTimeout;
                var flushReason = "timeout";

                while (true)
                {
                    while (buffer.Count < _options.BatchSize && reader.TryRead(out var msg))
                        buffer.Add(msg);

                    if (buffer.Count >= _options.BatchSize)
                    {
                        flushReason = "size";
                        break;
                    }

                    var remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                        break;

                    using var timeoutCts = new CancellationTokenSource(remaining);
                    try
                    {
                        if (!await reader.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false))
                        {
                            flushReason = "drain";
                            break;
                        }
                    }
                    catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
                    {
                        break;
                    }
                }

                if (buffer.Count > 0)
                    await DispatchBatchAsync(channel, buffer, flushReason).ConfigureAwait(false);

                buffer.Clear();

                if (_handlerCts.IsCancellationRequested)
                {
                    await RequeueRemainingAsync(channel, reader).ConfigureAwait(false);
                    return;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[BatchConsumer:{Key}] Batch dispatch loop failed", _consumerKey);
        }
    }

    /// <summary>
    /// NACKs (with requeue) every message still buffered after the drain timeout expired.
    /// </summary>
    private async Task RequeueRemainingAsync(IChannel channel, ChannelReader<BufferedMessage> reader)
    {
        var tags = new List<ulong>();
        while (reader.TryRead(out var msg))
            tags.Add(msg.DeliveryTag);

        if (tags.Count == 0) return;

        _logger.LogWarning("[BatchConsumer:{Key}] Requeuing {Count} buffered message(s) not processed before the drain timeout", _consumerKey, tags.Count);
        await NackMultipleAsync(channel, tags, requeue: true).ConfigureAwait(false);
    }

    /// <summary>
    /// Dispatches a flushed buffer. A consumer may have batch handlers for several event types,
    /// so the buffer is split into one batch per event type (preserving arrival order) and each
    /// is dispatched to its own handler.
    /// </summary>
    private async Task DispatchBatchAsync(IChannel channel, List<BufferedMessage> buffer, string flushReason)
    {
        foreach (var group in buffer.GroupBy(m => m.EventTypeName))
        {
            await DispatchEventTypeBatchAsync(channel, group.ToList(), group.Key, flushReason).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Dispatches a batch of messages of a single event type to its batch handler.
    /// ACKs all on success; on failure all are retried or dead-lettered together. If shutdown
    /// interrupts the batch, all are NACKed with requeue without counting a retry.
    /// </summary>
    private async Task DispatchEventTypeBatchAsync(IChannel channel, List<BufferedMessage> batch, string eventTypeName, string flushReason)
    {
        if (batch.Count == 0) return;

        _metrics.BatchesDispatched.Add(1,
            new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
            new(RabbitMqMetrics.TagEventType, eventTypeName),
            new(RabbitMqMetrics.TagQueue, _options.QueueName),
            new(RabbitMqMetrics.TagFlushReason, flushReason));

        _metrics.BatchSize.Record(batch.Count,
            new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
            new(RabbitMqMetrics.TagEventType, eventTypeName),
            new(RabbitMqMetrics.TagQueue, _options.QueueName));

        var deliveryTags = batch.Select(m => m.DeliveryTag).ToList();

        var resolved = _registry.Resolve(_consumerKey, eventTypeName);
        if (resolved is null)
        {
            _logger.LogError("[BatchConsumer:{Key}] No batch handler for '{EventType}' — NACKing {Count} messages", _consumerKey, eventTypeName, batch.Count);
            await NackMultipleAsync(channel, deliveryTags, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var (handlerType, _, isBatch) = resolved.Value;
        if (!isBatch)
        {
            _logger.LogError("[BatchConsumer:{Key}] Handler for '{EventType}' is not a batch handler — NACKing {Count} messages", _consumerKey, eventTypeName, batch.Count);
            await NackMultipleAsync(channel, deliveryTags, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var events = batch.Select(m => m.Event).ToList();
        var contexts = batch.Select(m => m.Context).ToList();

        var deliveryCount = batch.Max(m => m.Context.RetryCount) + 1;

        using var processActivity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} process", ActivityKind.Consumer);

        if (processActivity is not null)
        {
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingSystem, RabbitMqActivitySource.SystemRabbitMq);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingDestinationKind, RabbitMqActivitySource.DestinationKindQueue);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingDestinationName, _options.QueueName);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingOperation, RabbitMqActivitySource.OperationProcess);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingEventName, eventTypeName);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingConsumerKey, _consumerKey);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingDeliveryAttempt, deliveryCount);
            processActivity.SetTag("messaging.batch.message_count", batch.Count);
        }

        var handlerSw = ValueStopwatch.StartNew();

        var scope = _scopeFactory.CreateAsyncScope();
        var scopeOwnedByAbandonedHandler = false;
        try
        {
            var handler = scope.ServiceProvider.GetService(handlerType);

            if (handler is null)
            {
                _logger.LogError("[BatchConsumer:{Key}] Batch handler '{HandlerType}' not in DI — NACKing {Count} messages", _consumerKey, handlerType.Name, batch.Count);
                await NackMultipleAsync(channel, deliveryTags, requeue: false)
                    .ConfigureAwait(false);
                return;
            }

            var invoker = BatchHandlerInvokers.GetOrAdd(handlerType, CompileBatchInvoker);
            var handlerTask = invoker(handler, events, contexts, _handlerCts.Token);

            var outcome = await HandlerExecution.WaitAsync(
                handlerTask,
                scope,
                error => OnAbandonedBatchFinished(handlerType, batch.Count, error),
                _logger,
                _handlerCts.Token).ConfigureAwait(false);

            scopeOwnedByAbandonedHandler = outcome == HandlerOutcome.Abandoned;

            if (outcome == HandlerOutcome.Abandoned)
            {
                _logger.LogWarning("[BatchConsumer:{Key}] Batch handler '{HandlerType}' did not complete within {Timeout}s during shutdown — NACKing {Count} messages with requeue. The handler may still be running in the background.",
                    _consumerKey, handlerType.Name, _options.ShutdownDrainTimeout.TotalSeconds, batch.Count);

                await NackMultipleAsync(channel, deliveryTags, requeue: true).ConfigureAwait(false);

                _metrics.HandlersAbandoned.Add(1,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName));
                return;
            }

            await AckMultipleAsync(channel, deliveryTags)
                .ConfigureAwait(false);

            _metrics.ProcessingDurationMs.Record(handlerSw.GetElapsedMilliseconds(),
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            _metrics.Consumed.Add(batch.Count,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            _logger.LogDebug("[BatchConsumer:{Key}] Batch of {Count} {EventType} messages processed successfully", _consumerKey, batch.Count, eventTypeName);
        }
        catch (OperationCanceledException) when (_isShuttingDown)
        {
            _logger.LogInformation("[BatchConsumer:{Key}] Batch handler cancelled during graceful shutdown — NACKing {Count} messages with requeue", _consumerKey, batch.Count);
            await NackMultipleAsync(channel, deliveryTags, requeue: true).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.ConsumeErrors.Add(batch.Count,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagErrorType, ex.GetType().FullName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            processActivity?.SetTag(RabbitMqActivitySource.TagErrorType, ex.GetType().FullName);

            _logger.LogError(ex, "[BatchConsumer:{Key}] Batch handler failed for {Count} {EventType} messages (attempt {Attempt}/{Max})", _consumerKey, batch.Count, eventTypeName, deliveryCount, _retryPolicy?.MaxRetries ?? 1);

            if (ShouldRetry(deliveryCount))
            {
                _metrics.Retried.Add(batch.Count,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName),
                    new(RabbitMqMetrics.TagAttempt, deliveryCount));

                var retryDelay = _retryPolicy!.GetRetryDelay(deliveryCount) ?? TimeSpan.Zero;
                foreach (var msg in batch)
                {
                    var published = await PublishToRetryQueueAsync(msg.Body, msg.Properties, retryDelay)
                        .ConfigureAwait(false);

                    if (published)
                        await AckAsync(channel, msg.DeliveryTag).ConfigureAwait(false);
                    else
                        await NackAsync(channel, msg.DeliveryTag, requeue: true).ConfigureAwait(false);
                }
            }
            else
            {
                _metrics.DeadLettered.Add(batch.Count,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName),
                    new(RabbitMqMetrics.TagDlqName, _options.ResolvedDlqName));

                if (_options.EnableDeadLetter)
                    _logger.LogWarning("[BatchConsumer:{Key}] Batch retries exhausted ({Attempts}) — dead-lettering {Count} messages to DLQ",
                        _consumerKey, deliveryCount, batch.Count);
                else
                    _logger.LogWarning("[BatchConsumer:{Key}] Batch retries exhausted ({Attempts}) — discarding {Count} messages (EnableDeadLetter=false)",
                        _consumerKey, deliveryCount, batch.Count);

                await NackMultipleAsync(channel, deliveryTags, requeue: false).ConfigureAwait(false);

                foreach (var msg in batch)
                {
                    await InvokeDeadLetterHandlerAsync(msg, ex, deliveryCount).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            if (!scopeOwnedByAbandonedHandler)
                await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Called when a batch abandoned at shutdown (its messages already NACKed with requeue)
    /// finally finishes and its DI scope has been released.
    /// </summary>
    private void OnAbandonedBatchFinished(Type handlerType, int count, Exception? error)
    {
        if (error is null || error is OperationCanceledException)
            _logger.LogInformation("[BatchConsumer:{Key}] Abandoned batch handler '{HandlerType}' finished after shutdown; its {Count} messages were already requeued and will be redelivered", _consumerKey, handlerType.Name, count);
        else
            _logger.LogWarning(error, "[BatchConsumer:{Key}] Abandoned batch handler '{HandlerType}' failed after shutdown; its {Count} messages were already requeued and will be redelivered", _consumerKey, handlerType.Name, count);
    }

    /// <summary>
    /// Handles a single message delivery from RabbitMQ.
    /// Validates, deserializes, and writes to the channel for the dispatch loop.
    /// </summary>
    /// <remarks>
    /// Any exception thrown during pre-handler processing (e.g. deserialization) is
    /// caught by the top-level try/catch and treated as a poison message — routed
    /// through the retry/DLQ flow so it eventually lands in the DLQ instead of
    /// looping forever via broker requeue.
    /// </remarks>
    private async Task OnMessageReceived(BasicDeliverEventArgs ea, ChannelWriter<BufferedMessage> writer)
    {
        var body = ea.Body;
        var properties = ea.BasicProperties;
        var deliveryTag = ea.DeliveryTag;

        var retryCount = ExtractRetryCount(properties);
        var deliveryCount = retryCount + 1;

        try
        {
            await ProcessMessageAsync(ea, writer, body, properties, deliveryTag, retryCount)
                .ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            await NackAsync(deliveryTag, requeue: true).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_isShuttingDown)
        {
            await NackAsync(deliveryTag, requeue: true).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException and not ThreadAbortException)
        {
            await HandlePoisonMessageAsync(ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Processes a single message: header validation, handler resolution, deserialization,
    /// and write to the dispatch channel.
    /// </summary>
    private async Task ProcessMessageAsync(BasicDeliverEventArgs ea, ChannelWriter<BufferedMessage> writer, ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, ulong deliveryTag, int retryCount)
    {
        var eventTypeName = GetHeaderString(properties, MessageHeaders.EventType);
        if (eventTypeName is null)
        {
            _logger.LogWarning("[BatchConsumer:{Key}] Missing '{Header}' header — Nack (deliveryTag={Tag})", _consumerKey, MessageHeaders.EventType, deliveryTag);
            await NackAsync(deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var resolved = _registry.Resolve(_consumerKey, eventTypeName);
        if (resolved is null)
        {
            _logger.LogWarning("[BatchConsumer:{Key}] No handler for '{EventType}' — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var (_, eventType, isBatch) = resolved.Value;

        if (!isBatch)
        {
            _logger.LogWarning("[BatchConsumer:{Key}] Handler for '{EventType}' is not IBatchRabbitHandler<> — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var @event = DeserializeEvent(body, properties, eventTypeName, eventType, deliveryTag);
        if (@event is null)
        {
            _logger.LogError("[BatchConsumer:{Key}] Failed to deserialize '{EventType}' (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var context = BuildMessageContext(ea, retryCount);

        var parentContext = ExtractParentContext(properties);
        using var receiveActivity = StartConsumeActivity(eventTypeName, ea, parentContext, RabbitMqActivitySource.OperationReceive);

        var bodyCopy = body.ToArray();

        await writer.WriteAsync(new BufferedMessage(@event, context, deliveryTag, bodyCopy, properties, eventTypeName))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Deserializes the message body into the handler's event type. When an
    /// <see cref="EventUpgraderRegistry"/> is available and the message carries an older
    /// <c>x-event-version</c>, the body is deserialized into that version's type and upgraded
    /// to the latest version (same behaviour as <see cref="NamedRabbitConsumer"/>).
    /// </summary>
    private object? DeserializeEvent(ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, string eventTypeName, Type eventType, ulong deliveryTag)
    {
        if (_upgraderRegistry is not null)
        {
            var eventVersion = GetEventVersion(properties);
            var highestVersion = _upgraderRegistry.GetHighestVersion(eventTypeName);
            var deserializationType = _upgraderRegistry.GetTypeForVersion(eventTypeName, eventVersion);

            if (deserializationType is not null && eventVersion < highestVersion)
            {
                var oldEvent = _serializer.Deserialize(body, deserializationType);
                if (oldEvent is null)
                    return null;

                using var upgradeScope = _scopeFactory.CreateScope();
                var upgraded = _upgraderRegistry.Upgrade(eventTypeName, oldEvent, eventVersion, upgradeScope.ServiceProvider);

                _logger.LogDebug("[BatchConsumer:{Key}] Upgraded '{EventType}' v{Version} → v{HighestVersion} (deliveryTag={Tag})", _consumerKey, eventTypeName, eventVersion, highestVersion, deliveryTag);
                return upgraded;
            }
        }

        return _serializer.Deserialize(body, eventType);
    }

    private static int GetEventVersion(IReadOnlyBasicProperties properties)
    {
        var str = GetHeaderString(properties, MessageHeaders.EventVersion);
        if (str is null || !int.TryParse(str, out var version))
            return 1;
        return version;
    }

    /// <summary>
    /// Handles a poison message — one that threw an exception during pre-handler processing
    /// (deserialization, etc.) before it was buffered for batch dispatch. Routes the
    /// message through the same retry/DLQ flow as a batch handler failure so it eventually
    /// lands in the DLQ instead of looping forever via broker requeue.
    /// </summary>
    /// <remarks>
    /// This mirrors the catch block in <see cref="DispatchEventTypeBatchAsync"/>:
    /// <list type="bullet">
    ///   <item>If <see cref="ShouldRetry"/> is true: publish to the retry queue and ACK the
    ///   original delivery. The retry queue's TTL re-delivers it to the main queue after
    ///   the delay.</item>
    ///   <item>If retries are exhausted: NACK with requeue=false so the broker dead-letters
    ///   the message to the DLQ via x-dead-letter-exchange, then invoke the (optional)
    ///   IDeadLetterHandler for application-level notification.</item>
    /// </list>
    /// If the retry-publish fails (broker/channel unavailable), fall back to NACK with
    /// requeue=true to preserve at-least-once semantics without losing the message.
    /// </remarks>
    private async Task HandlePoisonMessageAsync(BasicDeliverEventArgs ea, ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, Exception ex, int deliveryCount)
    {
        var eventTypeName = GetHeaderString(properties, MessageHeaders.EventType) ?? "<unknown>";

        if (ex is MessageDeserializationException)
        {
            _metrics.ConsumeErrors.Add(1,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagErrorType, nameof(MessageDeserializationException)),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            _logger.LogError(ex, "[BatchConsumer:{Key}] Message deserialization failed for '{EventType}' — routing to DLQ/discard without retry (EnableDeadLetter={EnableDeadLetter}, deliveryTag={Tag})",
                _consumerKey, eventTypeName, _options.EnableDeadLetter, ea.DeliveryTag);

            await NackAsync(ea.DeliveryTag, requeue: false).ConfigureAwait(false);

            await InvokeDeadLetterHandlerAsync(ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
            return;
        }

        _metrics.ConsumeErrors.Add(1,
            new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
            new(RabbitMqMetrics.TagEventType, eventTypeName),
            new(RabbitMqMetrics.TagErrorType, ex.GetType().FullName),
            new(RabbitMqMetrics.TagQueue, _options.QueueName));

        _logger.LogError(ex, "[BatchConsumer:{Key}] Poison message (pre-handler failure) attempt {Attempt}/{Max} (deliveryTag={Tag})", _consumerKey, deliveryCount, _retryPolicy?.MaxRetries ?? 1, ea.DeliveryTag);

        if (ShouldRetry(deliveryCount))
        {
            _metrics.Retried.Add(1,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName),
                new(RabbitMqMetrics.TagAttempt, deliveryCount));

            var retryDelay = _retryPolicy!.GetRetryDelay(deliveryCount) ?? TimeSpan.Zero;
            var publishSucceeded = await PublishToRetryQueueAsync(body, properties, retryDelay)
                .ConfigureAwait(false);

            if (publishSucceeded)
                await AckAsync(ea.DeliveryTag).ConfigureAwait(false);
            else
                await NackAsync(ea.DeliveryTag, requeue: true).ConfigureAwait(false);

        }
        else
        {
            _metrics.DeadLettered.Add(1,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName),
                new(RabbitMqMetrics.TagDlqName, _options.ResolvedDlqName));

            if (_options.EnableDeadLetter)
                _logger.LogWarning("[BatchConsumer:{Key}] Poison message retries exhausted ({Attempts}) — dead-lettering to DLQ (deliveryTag={Tag})",
                    _consumerKey, deliveryCount, ea.DeliveryTag);
            else
                _logger.LogWarning("[BatchConsumer:{Key}] Poison message retries exhausted ({Attempts}) — discarding message (EnableDeadLetter=false, deliveryTag={Tag})",
                    _consumerKey, deliveryCount, ea.DeliveryTag);

            await NackAsync(ea.DeliveryTag, requeue: false).ConfigureAwait(false);

            await InvokeDeadLetterHandlerAsync(ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Compiles a fast delegate that invokes <see cref="IBatchRabbitHandler{TEvent}.HandleBatchAsync"/>
    /// without reflection per call.
    /// 
    /// <para>
    /// The delegate takes <c>IReadOnlyList&lt;object&gt;</c> because events are stored as <c>object</c>
    /// in the buffer. Inside the lambda, <c>Enumerable.Cast&lt;T&gt;().ToList()</c> converts
    /// to the strongly-typed list the handler expects.
    /// </para>
    /// </summary>
    private static Func<object, IReadOnlyList<object>, IReadOnlyList<MessageContext>, CancellationToken, Task> CompileBatchInvoker(Type handlerType)
    {
        var handlerInterface = handlerType.GetInterfaces()
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IBatchRabbitHandler<>));

        var eventType = handlerInterface.GetGenericArguments()[0];
        var handleMethod = typeof(IBatchRabbitHandler<>)
            .MakeGenericType(eventType)
            .GetMethod(nameof(IBatchRabbitHandler<IIntegrationEvent>.HandleBatchAsync))!;

        var hParam = Expression.Parameter(typeof(object), "h");
        var eParam = Expression.Parameter(typeof(IReadOnlyList<object>), "events");
        var cParam = Expression.Parameter(typeof(IReadOnlyList<MessageContext>), "contexts");
        var tParam = Expression.Parameter(typeof(CancellationToken), "t");

        var castMethod = typeof(Enumerable).GetMethods()
            .First(m => m.Name == "Cast" && m.IsGenericMethod && m.GetParameters().Length == 1)
            .MakeGenericMethod(eventType);

        var toListMethod = typeof(Enumerable).GetMethods()
            .First(m => m.Name == "ToList" && m.IsGenericMethod && m.GetParameters().Length == 1)
            .MakeGenericMethod(eventType);

        var castCall = Expression.Call(null, castMethod, eParam);
        var toListCall = Expression.Call(null, toListMethod, castCall);

        var call = Expression.Call(Expression.Convert(hParam, handlerInterface), handleMethod, toListCall, cParam, tParam);

        return Expression.Lambda<Func<object, IReadOnlyList<object>, IReadOnlyList<MessageContext>, CancellationToken, Task>>(
            call, hParam, eParam, cParam, tParam).Compile();
    }
    private bool ShouldRetry(int deliveryCount)
    {
        return _retryPolicy is not null && _retryPolicy.ShouldRetry(deliveryCount);
    }

    /// <summary>
    /// Converts <see cref="IReadOnlyBasicProperties"/> (received from the broker on consume)
    /// to a <see cref="BasicProperties"/> struct (required for re-publishing via
    /// <c>BasicPublishAsync</c>). See NamedRabbitConsumer.ToBasicProperties for details.
    /// </summary>
    /// <param name="stripXDeath">When true, removes the x-death header so the broker adds
    /// fresh entries on the next dead-letter cycle.</param>
    /// <param name="source">The source properties to copy.</param>
    private static BasicProperties ToBasicProperties(IReadOnlyBasicProperties source, bool stripXDeath = false)
    {
        var props = new BasicProperties
        {
            ContentType = source.ContentType,
            ContentEncoding = source.ContentEncoding,
            DeliveryMode = source.DeliveryMode,
            Priority = source.Priority,
            CorrelationId = source.CorrelationId,
            ReplyTo = source.ReplyTo,
            Expiration = source.Expiration,
            MessageId = source.MessageId,
            Timestamp = source.Timestamp,
            Type = source.Type,
            UserId = source.UserId,
            AppId = source.AppId,
            ReplyToAddress = source.ReplyToAddress,
        };

        if (source.Headers is not null)
        {
            props.Headers = new Dictionary<string, object?>(source.Headers);

            if (stripXDeath)
                props.Headers.Remove("x-death");
        }

        return props;
    }

    /// <summary>
    /// Publishes a message to the retry queue that corresponds to the given delay.
    /// Uses the default AMQP exchange (empty string) with the retry queue name as
    /// routing key. The retry queue has a TTL and dead-letters (via the default exchange)
    /// straight back to the main queue, so the message is re-delivered after the TTL expires.
    /// The publish is confirmed by the broker (see <see cref="RetryQueuePublisher"/>), so the
    /// caller may safely ACK the original on <c>true</c>.
    /// </summary>
    /// <returns><c>true</c> if the broker confirmed the publish; <c>false</c> otherwise (the caller must NACK with requeue).</returns>
    private Task<bool> PublishToRetryQueueAsync(ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, TimeSpan delay)
    {
        var props = ToBasicProperties(properties);
        props.Headers ??= new Dictionary<string, object?>();
        props.Headers[MessageHeaders.DeliveryCount] = (ExtractRetryCount(properties) + 1).ToString();

        return _retryPublisher!.PublishAsync(body, props, delay);
    }

    /// <summary>
    /// Invokes the (optional) <see cref="IDeadLetterHandler"/> registered for this consumer.
    /// This is purely an application-level notification — the message has already been
    /// dead-lettered to the DLQ by the broker via the main queue's x-dead-letter-exchange.
    /// This method does NOT re-publish the message.
    /// </summary>
    private async Task InvokeDeadLetterHandlerAsync(BufferedMessage msg, Exception handlerException, int deliveryCount)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var dlHandlers = scope.ServiceProvider.GetServices<IDeadLetterHandler>();
            var dlHandler = dlHandlers.FirstOrDefault(h => h.ConsumerKey == _consumerKey);

            if (dlHandler is not null)
            {
                await dlHandler.HandleAsync(new DeadLetterMessage
                {
                    OriginalRoutingKey = msg.Context.RoutingKey,
                    OriginalExchange = msg.Context.Exchange,
                    OriginalBody = msg.Body,
                    ExceptionType = handlerException.GetType().FullName,
                    ExceptionMessage = handlerException.Message,
                    RetryCount = deliveryCount,
                    DeadLetteredAt = DateTime.UtcNow,
                    ConsumerKey = _consumerKey,
                    CorrelationId = msg.Context.CorrelationId,
                    MessageId = msg.Context.MessageId,
                }).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[BatchConsumer:{Key}] IDeadLetterHandler threw (deliveryTag={Tag})", _consumerKey, msg.DeliveryTag);
        }
    }

    /// <summary>
    /// Overload of <see cref="InvokeDeadLetterHandlerAsync(BufferedMessage, Exception, int)"/>
    /// that takes individual components instead of a <see cref="BufferedMessage"/>.
    /// Used by the poison-message path where the message could not be deserialized into
    /// a BufferedMessage (so we don't have one to pass).
    /// </summary>
    private async Task InvokeDeadLetterHandlerAsync(BasicDeliverEventArgs ea, ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, Exception handlerException, int deliveryCount)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var dlHandlers = scope.ServiceProvider.GetServices<IDeadLetterHandler>();
            var dlHandler = dlHandlers.FirstOrDefault(h => h.ConsumerKey == _consumerKey);

            if (dlHandler is not null)
            {
                await dlHandler.HandleAsync(new DeadLetterMessage
                {
                    OriginalRoutingKey = ea.RoutingKey,
                    OriginalExchange = ea.Exchange,
                    OriginalBody = body,
                    ExceptionType = handlerException.GetType().FullName,
                    ExceptionMessage = handlerException.Message,
                    RetryCount = deliveryCount,
                    DeadLetteredAt = DateTime.UtcNow,
                    ConsumerKey = _consumerKey,
                    CorrelationId = GetHeaderString(properties, MessageHeaders.CorrelationId),
                    MessageId = GetHeaderString(properties, MessageHeaders.MessageId),
                }).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[BatchConsumer:{Key}] IDeadLetterHandler threw (deliveryTag={Tag})", _consumerKey, ea.DeliveryTag);
        }
    }

    /// <summary>
    /// ACKs each delivery tag individually. <c>multiple: true</c> is not used on purpose:
    /// it would also settle lower, unrelated tags still buffered or belonging to a batch of
    /// another event type.
    /// </summary>
    private async Task AckMultipleAsync(IChannel channel, List<ulong> deliveryTags)
    {
        foreach (var tag in deliveryTags)
            await AckAsync(channel, tag).ConfigureAwait(false);
    }

    /// <summary>
    /// NACKs each delivery tag individually (see <see cref="AckMultipleAsync"/>).
    /// </summary>
    private async Task NackMultipleAsync(IChannel channel, List<ulong> deliveryTags, bool requeue)
    {
        foreach (var tag in deliveryTags)
            await NackAsync(channel, tag, requeue).ConfigureAwait(false);
    }

    /// <summary>
    /// Acks a single delivery tag on the current channel. Used by the per-message
    /// (pre-buffer) path.
    /// </summary>
    private Task AckAsync(ulong deliveryTag)
    {
        var channel = _currentChannel;
        return channel is null ? Task.CompletedTask : AckAsync(channel, deliveryTag);
    }

    private async Task AckAsync(IChannel channel, ulong deliveryTag)
    {
        try
        {
            await channel.BasicAckAsync(deliveryTag: deliveryTag, multiple: false)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[BatchConsumer:{Key}] Ack failed (deliveryTag={Tag}, channel likely closed)", _consumerKey, deliveryTag);
        }
    }

    /// <summary>
    /// Nacks a single delivery tag on the current channel. Used by the per-message
    /// (pre-buffer) path.
    /// </summary>
    private Task NackAsync(ulong deliveryTag, bool requeue)
    {
        var channel = _currentChannel;
        return channel is null ? Task.CompletedTask : NackAsync(channel, deliveryTag, requeue);
    }

    private async Task NackAsync(IChannel channel, ulong deliveryTag, bool requeue)
    {
        try
        {
            await channel.BasicNackAsync(deliveryTag: deliveryTag, multiple: false, requeue: requeue)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[BatchConsumer:{Key}] Nack failed (deliveryTag={Tag}, channel likely closed)", _consumerKey, deliveryTag);
        }
    }

    private async Task InitializeChannelAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.BasicQosAsync(prefetchSize: 0,
            prefetchCount: _options.PrefetchCount,
            global: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await TopologyDeclarator.DeclareConsumerTopologyAsync(channel, _options, _logger, cancellationToken)
            .ConfigureAwait(false);
    }
    private int ExtractRetryCount(IReadOnlyBasicProperties properties)
    {
        var countStr = GetHeaderString(properties, MessageHeaders.DeliveryCount);
        if (countStr is not null && int.TryParse(countStr, out var count) && count >= 0)
            return count;
        return 0;
    }

    private static MessageContext BuildMessageContext(BasicDeliverEventArgs ea, int retryCount)
    {
        return new MessageContext
        {
            CorrelationId = GetHeaderString(ea.BasicProperties, MessageHeaders.CorrelationId),
            MessageId = GetHeaderString(ea.BasicProperties, MessageHeaders.MessageId),
            PublishedAt = ParseHeaderDateTime(ea.BasicProperties, MessageHeaders.PublishedAt),
            PublisherName = GetHeaderString(ea.BasicProperties, MessageHeaders.PublisherName),
            RoutingKey = ea.RoutingKey,
            Exchange = ea.Exchange,
            DeliveryTag = ea.DeliveryTag,
            Redelivered = ea.Redelivered,
            ConsumerTag = ea.ConsumerTag,
            RetryCount = retryCount,
            Headers = ExtractCustomHeaders(ea.BasicProperties.Headers),
        };
    }

    private static ActivityContext ExtractParentContext(IReadOnlyBasicProperties properties)
    {
        var traceParent = GetHeaderString(properties, RabbitMqActivitySource.TraceParentHeader);

        if (traceParent is not null &&
            ActivityContext.TryParse(traceParent, null, out var context))
        {
            return context;
        }

        return Activity.Current?.Context ?? default;
    }

    private Activity? StartConsumeActivity(
        string eventTypeName,
        BasicDeliverEventArgs ea,
        ActivityContext parentContext,
        string operation)
    {
        var links = parentContext != default ? new[] { new ActivityLink(parentContext) } : null;

        var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} {operation}",
            ActivityKind.Consumer,
            parentContext: default,
            links: links);

        if (activity is null) return null;

        activity.SetTag(RabbitMqActivitySource.TagMessagingSystem, RabbitMqActivitySource.SystemRabbitMq);
        activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationKind, RabbitMqActivitySource.DestinationKindQueue);
        activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationName, _options.QueueName);
        activity.SetTag(RabbitMqActivitySource.TagMessagingOperation, operation);
        activity.SetTag(RabbitMqActivitySource.TagMessagingRabbitmqRoutingKey, ea.RoutingKey);
        activity.SetTag(RabbitMqActivitySource.TagMessagingEventName, eventTypeName);
        activity.SetTag(RabbitMqActivitySource.TagMessagingConsumerKey, _consumerKey);

        var messageId = GetHeaderString(ea.BasicProperties, MessageHeaders.MessageId);
        if (messageId is not null)
            activity.SetTag(RabbitMqActivitySource.TagMessagingMessageId, messageId);

        var correlationId = GetHeaderString(ea.BasicProperties, MessageHeaders.CorrelationId);
        if (correlationId is not null)
            activity.SetTag(RabbitMqActivitySource.TagMessagingConversationId, correlationId);

        return activity;
    }

    private static string? GetHeaderString(IReadOnlyBasicProperties properties, string key)
    {
        if (properties.Headers is null || !properties.Headers.TryGetValue(key, out var value))
            return null;

        return value switch
        {
            string s => s,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> rom => Encoding.UTF8.GetString(rom.Span),
            _ => value?.ToString(),
        };
    }

    private static DateTime? ParseHeaderDateTime(IReadOnlyBasicProperties properties, string key)
    {
        var str = GetHeaderString(properties, key);
        return str is not null && DateTime.TryParse(str, out var dt) ? dt : null;
    }

    private static Dictionary<string, string> ExtractCustomHeaders(IDictionary<string, object?>? headers)
    {
        var result = new Dictionary<string, string>();
        if (headers is null) return result;

        foreach (var (key, value) in headers)
        {
            if (key.StartsWith("x-")) continue;

            var strValue = value switch
            {
                string s => s,
                byte[] bytes => Encoding.UTF8.GetString(bytes),
                ReadOnlyMemory<byte> rom => Encoding.UTF8.GetString(rom.Span),
                _ => value?.ToString(),
            };

            if (strValue is not null)
                result[key] = strValue;
        }

        return result;
    }

    private readonly struct ValueStopwatch
    {
        private readonly long _startTimestamp;

        private ValueStopwatch(long startTimestamp) => _startTimestamp = startTimestamp;

        public static ValueStopwatch StartNew() => new(Stopwatch.GetTimestamp());

        public double GetElapsedMilliseconds() =>
           Stopwatch.GetElapsedTime(_startTimestamp).TotalMilliseconds;
    }

    private static async Task SafeCloseChannelAsync(IChannel channel)
    {
        try { await channel.CloseAsync().ConfigureAwait(false); }
        catch (AlreadyClosedException) { }
        catch (ObjectDisposedException) { }
        catch { }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _isShuttingDown = true;

        try { _handlerCts.Cancel(); }
        catch (ObjectDisposedException) { }

        if (_retryPublisher is not null)
            await _retryPublisher.DisposeAsync().ConfigureAwait(false);

        _handlerCts.Dispose();
    }
}
