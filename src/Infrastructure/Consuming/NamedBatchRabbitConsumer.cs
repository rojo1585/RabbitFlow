using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Topology;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Channels;

namespace RabbitFlow.Infrastructure.Consuming;


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
    private static readonly ConcurrentDictionary<Type, Func<object, IReadOnlyList<object>, IReadOnlyList<MessageContext>, Task>> BatchHandlerInvokers = new();

    private readonly string _consumerKey;
    private readonly RabbitConsumerOptions _options;
    private readonly ManagedConnection _connection;
    private readonly HandlerTypeRegistry _registry;
    private readonly IMessageSerializer _serializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<NamedBatchRabbitConsumer> _logger;
    private readonly RabbitMqMetrics _metrics;
    private readonly RetryPolicy? _retryPolicy;

    private volatile IChannel? _currentChannel;
    private volatile bool _disposed;

    public NamedBatchRabbitConsumer(
        string consumerKey,
        RabbitConsumerOptions options,
        ManagedConnection connection,
        HandlerTypeRegistry registry,
        IMessageSerializer serializer,
        IServiceScopeFactory scopeFactory,
        ILogger<NamedBatchRabbitConsumer> logger,
        RabbitMqMetrics metrics)
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

        if (options.EnableRetry)
            _retryPolicy = new RetryPolicy(options);
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[BatchConsumer:{Key}] Starting batch consumer loop → queue '{Queue}' (batchSize={BatchSize}, timeout={TimeoutMs}ms)", _consumerKey, _options.QueueName, _options.BatchSize, _options.BatchTimeoutMs);

        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            IChannel? channel = null;
            try
            {
                channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                _currentChannel = channel;

                await InitializeChannelAsync(channel, cancellationToken).ConfigureAwait(false);

                var messageChannel = Channel.CreateBounded<BufferedMessage>(new BoundedChannelOptions(_options.PrefetchCount * 2)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });

                var consumer = new AsyncEventingBasicConsumer(channel);

                async Task OnReceived(object sender, BasicDeliverEventArgs ea)
                {
                    await OnMessageReceived(ea, messageChannel.Writer).ConfigureAwait(false);
                }
                consumer.ReceivedAsync += OnReceived;

                var consumerTag = await channel.BasicConsumeAsync(
                    queue: _options.QueueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("[BatchConsumer:{Key}] Consuming from '{Queue}' (tag={Tag}, prefetch={Prefetch})", _consumerKey, _options.QueueName, consumerTag, _options.PrefetchCount);

                var shutdownTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

                Task OnShutdown(object sender, ShutdownEventArgs e)
                {
                    consumer.ReceivedAsync -= OnReceived;
                    shutdownTcs.TrySetResult(true);
                    return Task.CompletedTask;
                }

                channel.ChannelShutdownAsync += OnShutdown;

                try
                {
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                    var dispatchTask = RunDispatchLoopAsync(channel, messageChannel.Reader, linkedCts.Token);

                    using var reg = cancellationToken.Register(() =>
                    {
                        messageChannel.Writer.TryComplete();
                        shutdownTcs.TrySetCanceled();
                    });

                    await shutdownTcs.Task.ConfigureAwait(false);

                    messageChannel.Writer.TryComplete();
                    await dispatchTask.ConfigureAwait(false);
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
                    await SafeCloseChannelAsync(channel).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Background loop that reads messages from the channel, buffers them,
    /// and dispatches batches when size or timeout is reached.
    /// </summary>
    private async Task RunDispatchLoopAsync(IChannel channel, ChannelReader<BufferedMessage> reader, CancellationToken cancellationToken)
    {
        var buffer = new List<BufferedMessage>(_options.BatchSize);
        var timeoutMs = _options.BatchTimeoutMs;

        while (!cancellationToken.IsCancellationRequested)
        {
            buffer.Clear();
            DateTime? batchStart = null;
            bool dispatched = false;

            while (buffer.Count < _options.BatchSize && !cancellationToken.IsCancellationRequested)
            {
                int remainingTimeoutMs = timeoutMs;
                if (batchStart is not null)
                {
                    var elapsed = (int)(DateTime.UtcNow - batchStart.Value).TotalMilliseconds;
                    remainingTimeoutMs = Math.Max(0, timeoutMs - elapsed);
                }

                try
                {
                    using var timeoutCts = new CancellationTokenSource(remainingTimeoutMs);
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                        timeoutCts.Token, cancellationToken);

                    if (await reader.WaitToReadAsync(linkedCts.Token).ConfigureAwait(false))
                    {
                        while (buffer.Count < _options.BatchSize && reader.TryRead(out var msg))
                        {
                            buffer.Add(msg);
                            batchStart ??= DateTime.UtcNow;
                        }
                    }
                    else
                    {
                        if (buffer.Count > 0)
                            await DispatchBatchAsync(channel, buffer, cancellationToken, "drain").ConfigureAwait(false);
                        return;
                    }
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    if (buffer.Count > 0)
                    {
                        await DispatchBatchAsync(channel, buffer, cancellationToken, "timeout").ConfigureAwait(false);
                        dispatched = true;
                    }
                    break;
                }
            }

            if (!dispatched && buffer.Count > 0)
                await DispatchBatchAsync(channel, buffer, cancellationToken, "size").ConfigureAwait(false);
        }

        buffer.Clear();
        while (reader.TryRead(out var msg))
            buffer.Add(msg);

        if (buffer.Count > 0)
        {
            _logger.LogInformation("[BatchConsumer:{Key}] Draining {Count} remaining messages on shutdown", _consumerKey, buffer.Count);
            await DispatchBatchAsync(channel, buffer, CancellationToken.None, "drain").ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Dispatches a batch of buffered messages to the appropriate batch handler.
    /// ACKs all on success, NACKs all on failure.
    /// </summary>
    private async Task DispatchBatchAsync(IChannel channel, List<BufferedMessage> batch, CancellationToken cancellationToken, string flushReason = "size")
    {
        if (batch.Count == 0) return;

        var firstEventTypeName = batch[0].EventTypeName;

        _metrics.BatchesDispatched.Add(1,
            new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
            new(RabbitMqMetrics.TagEventType, firstEventTypeName),
            new(RabbitMqMetrics.TagQueue, _options.QueueName),
            new(RabbitMqMetrics.TagFlushReason, flushReason));

        _metrics.BatchSize.Record(batch.Count,
            new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
            new(RabbitMqMetrics.TagEventType, firstEventTypeName),
            new(RabbitMqMetrics.TagQueue, _options.QueueName));


        var resolved = _registry.Resolve(_consumerKey, firstEventTypeName);
        if (resolved is null)
        {
            _logger.LogError("[BatchConsumer:{Key}] No batch handler for '{EventType}' — NACKing {Count} messages", _consumerKey, firstEventTypeName, batch.Count);
            await NackMultipleAsync(channel, batch.Select(m => m.DeliveryTag).ToList(), requeue: false).ConfigureAwait(false);
            return;
        }

        var (handlerType, eventType, isBatch) = resolved.Value;
        if (!isBatch)
        {
            _logger.LogError("[BatchConsumer:{Key}] Handler for '{EventType}' is not a batch handler — NACKing {Count} messages", _consumerKey, firstEventTypeName, batch.Count);
            await NackMultipleAsync(channel, batch.Select(m => m.DeliveryTag).ToList(), requeue: false).ConfigureAwait(false);
            return;
        }

        var deliveryTags = batch.Select(m => m.DeliveryTag).ToList();
        var events = batch.Select(m => m.Event).ToList();
        var contexts = batch.Select(m => m.Context).ToList();

        var deliveryCount = batch[0].Context.RetryCount + 1;

        using var processActivity = RabbitMqActivitySource.Source.StartActivity($"{firstEventTypeName} process", ActivityKind.Consumer);

        if (processActivity is not null)
        {
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingSystem, RabbitMqActivitySource.SystemRabbitMq);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingDestinationKind, RabbitMqActivitySource.DestinationKindQueue);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingDestinationName, _options.QueueName);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingOperation, RabbitMqActivitySource.OperationProcess);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingEventName, firstEventTypeName);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingConsumerKey, _consumerKey);
            processActivity.SetTag(RabbitMqActivitySource.TagMessagingDeliveryAttempt, deliveryCount);
            processActivity.SetTag("messaging.batch.message_count", batch.Count);
        }

        var handlerSw = ValueStopwatch.StartNew();

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var handler = scope.ServiceProvider.GetService(handlerType);

            if (handler is null)
            {
                _logger.LogError("[BatchConsumer:{Key}] Batch handler '{HandlerType}' not in DI — NACKing {Count} messages", _consumerKey, handlerType.Name, batch.Count);
                await NackMultipleAsync(channel, deliveryTags, requeue: false).ConfigureAwait(false);
                return;
            }

            var invoker = BatchHandlerInvokers.GetOrAdd(handlerType, CompileBatchInvoker);
            await invoker(handler, events, contexts).ConfigureAwait(false);

            await AckMultipleAsync(channel, deliveryTags).ConfigureAwait(false);

            _metrics.ProcessingDurationMs.Record(handlerSw.GetElapsedMilliseconds(),
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, firstEventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            _metrics.Consumed.Add(batch.Count,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, firstEventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            _logger.LogDebug("[BatchConsumer:{Key}] Batch of {Count} {EventType} messages processed successfully", _consumerKey, batch.Count, firstEventTypeName);
        }
        catch (Exception ex)
        {
            _metrics.ConsumeErrors.Add(batch.Count,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, firstEventTypeName),
                new(RabbitMqMetrics.TagErrorType, ex.GetType().FullName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            processActivity?.SetTag(RabbitMqActivitySource.TagErrorType, ex.GetType().FullName);

            _logger.LogError(ex, "[BatchConsumer:{Key}] Batch handler failed for {Count} {EventType} messages (attempt {Attempt}/{Max})", _consumerKey, batch.Count, firstEventTypeName, deliveryCount, _retryPolicy?.MaxRetries ?? 1);

            if (ShouldRetry(deliveryCount))
            {
                _metrics.Retried.Add(batch.Count,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, firstEventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName),
                    new(RabbitMqMetrics.TagAttempt, deliveryCount));

                var retryDelay = _retryPolicy!.GetRetryDelay(deliveryCount) ?? TimeSpan.Zero;
                var allPublished = true;
                foreach (var msg in batch)
                {
                    var published = await PublishToRetryQueueAsync(msg.Body, msg.Properties, retryDelay).ConfigureAwait(false);
                    if (!published) allPublished = false;
                }

                if (allPublished)
                    await AckMultipleAsync(channel, deliveryTags).ConfigureAwait(false);
                else
                    await NackMultipleAsync(channel, deliveryTags, requeue: true).ConfigureAwait(false);

            }
            else
            {
                _metrics.DeadLettered.Add(batch.Count,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, firstEventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName),
                    new(RabbitMqMetrics.TagDlqName, _options.ResolvedDlqName));

                // NACK with requeue=false triggers broker-side dead-lettering via the
                // main queue's x-dead-letter-exchange (a "direct" DLX bound to the DLQ
                // with the "dead" routing key). Each message lands in the DLQ exactly once.
                //
                // Previously, the client ALSO re-published each message to the DLQ here,
                // producing duplicate copies (the "double DLQ" bug). The client-side
                // publish has been removed; the broker is the sole writer to the DLQ.
                await NackMultipleAsync(channel, deliveryTags, requeue: false).ConfigureAwait(false);

                // Invoke the (optional) IDeadLetterHandler for each message for
                // application-level notification (e.g., alerting, audit). This does NOT
                // re-publish the messages; the broker has already dead-lettered them.
                foreach (var msg in batch)
                {
                    await InvokeDeadLetterHandlerAsync(msg, ex, deliveryCount).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// Handles a single message delivery from RabbitMQ.
    /// Validates, deserializes, and writes to the channel for the dispatch loop.
    /// </summary>
    private async Task OnMessageReceived(BasicDeliverEventArgs ea, ChannelWriter<BufferedMessage> writer)
    {
        var body = ea.Body;
        var properties = ea.BasicProperties;
        var deliveryTag = ea.DeliveryTag;

        var eventTypeName = GetHeaderString(properties, MessageHeaders.EventType);
        if (eventTypeName is null)
        {
            _logger.LogWarning("[BatchConsumer:{Key}] Missing '{Header}' header — Nack (deliveryTag={Tag})", _consumerKey, MessageHeaders.EventType, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        var resolved = _registry.Resolve(_consumerKey, eventTypeName);
        if (resolved is null)
        {
            _logger.LogWarning("[BatchConsumer:{Key}] No handler for '{EventType}' — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        var (_, eventType, isBatch) = resolved.Value;

        if (!isBatch)
        {
            _logger.LogWarning("[BatchConsumer:{Key}] Handler for '{EventType}' is not IBatchRabbitHandler<> — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        var @event = _serializer.Deserialize(body, eventType);
        if (@event is null)
        {
            _logger.LogError("[BatchConsumer:{Key}] Failed to deserialize '{EventType}' (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        var retryCount = ExtractRetryCount(properties);
        var context = BuildMessageContext(ea, retryCount);

        var parentContext = ExtractParentContext(properties);
        using var receiveActivity = StartConsumeActivity(eventTypeName, ea, parentContext, RabbitMqActivitySource.OperationReceive);

        await writer.WriteAsync(new BufferedMessage(@event, context, deliveryTag, body, properties, eventTypeName)).ConfigureAwait(false);
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
    private static Func<object, IReadOnlyList<object>, IReadOnlyList<MessageContext>, Task> CompileBatchInvoker(Type handlerType)
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

        var castMethod = typeof(Enumerable).GetMethod("Cast", [typeof(IEnumerable)])!
            .MakeGenericMethod(eventType);
        var toListMethod = typeof(Enumerable).GetMethod("ToList", [typeof(IEnumerable<>).MakeGenericType(eventType)])!;

        var castEvents = Expression.Call(Expression.Call(null, castMethod, eParam), toListMethod);

        var call = Expression.Call(Expression.Convert(hParam, handlerInterface), handleMethod, castEvents, cParam);

        return Expression.Lambda<Func<object, IReadOnlyList<object>, IReadOnlyList<MessageContext>, Task>>(call, hParam, eParam, cParam).Compile();
    }
    private bool ShouldRetry(int deliveryCount)
    {
        return _retryPolicy is not null && _retryPolicy.ShouldRetry(deliveryCount);
    }

    /// <summary>
    /// Publishes a message to the retry queue that corresponds to the given delay.
    /// Uses the default AMQP exchange (empty string) with the retry queue name as
    /// routing key. The retry queue has a TTL and x-dead-letter-exchange pointing
    /// back to the main exchange, so the message is re-delivered to the main queue
    /// after the TTL expires.
    /// </summary>
    /// <returns><c>true</c> if the publish succeeded; <c>false</c> if the broker or channel was unavailable.</returns>
    private async Task<bool> PublishToRetryQueueAsync(ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, TimeSpan delay)
    {
        var retryQueueName = $"{_options.QueueName}.retry.{(int)delay.TotalSeconds}s";

        IChannel? retryChannel = null;
        try
        {
            retryChannel = await _connection.CreateChannelAsync().ConfigureAwait(false);
            await retryChannel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: retryQueueName,
                mandatory: false,
                basicProperties: (BasicProperties)properties,
                body: body).ConfigureAwait(false);

            _logger.LogInformation("[BatchConsumer:{Key}] Published to retry queue '{RetryQueue}' (delay={DelayMs}ms)", _consumerKey, retryQueueName, (int)delay.TotalMilliseconds);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[BatchConsumer:{Key}] Failed to publish to retry queue '{RetryQueue}'", _consumerKey, retryQueueName);
            return false;
        }
        finally
        {
            if (retryChannel is not null)
                await SafeCloseChannelAsync(retryChannel).ConfigureAwait(false);
        }
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

    private async Task AckMultipleAsync(IChannel channel, List<ulong> deliveryTags)
    {
        if (deliveryTags.Count == 0) return;

        try
        {
            await channel.BasicAckAsync(deliveryTag: deliveryTags[^1], multiple: true).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[BatchConsumer:{Key}] Multi-ack failed (channel likely closed)", _consumerKey);
        }
    }

    private async Task NackMultipleAsync(IChannel channel, List<ulong> deliveryTags, bool requeue)
    {
        if (deliveryTags.Count == 0) return;

        try
        {
            await channel.BasicNackAsync(deliveryTag: deliveryTags[^1], multiple: true, requeue: requeue).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[BatchConsumer:{Key}] Multi-nack failed (channel likely closed)", _consumerKey);
        }
    }

    private async Task NackAsync(ulong deliveryTag, bool requeue)
    {
        var channel = _currentChannel;
        if (channel is null) return;

        try
        {
            await channel.BasicNackAsync(deliveryTag: deliveryTag, multiple: false, requeue: requeue).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[BatchConsumer:{Key}] Nack failed (deliveryTag={Tag}, channel likely closed)", _consumerKey, deliveryTag);
        }
    }
    private async Task InitializeChannelAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false, cancellationToken: cancellationToken).ConfigureAwait(false);

        await TopologyDeclarator.DeclareConsumerTopologyAsync(channel, _options, _logger, cancellationToken).ConfigureAwait(false);
    }
    private int ExtractRetryCount(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null || !properties.Headers.TryGetValue("x-death", out var value))
            return 0;

        try
        {
            var deathEntries = value switch
            {
                IList list => list,
                _ => null
            };

            if (deathEntries is null) return 0;

            var totalDeaths = 0;
            foreach (var entry in deathEntries)
            {
                if (entry is not IDictionary dict) continue;

                if (dict["queue"] is string deadQueue &&
                    (deadQueue == _options.QueueName || deadQueue.StartsWith($"{_options.QueueName}.retry.")))
                {
                    if (dict["count"] is long count)
                        totalDeaths += (int)count;
                }
            }

            return totalDeaths;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[BatchConsumer:{Key}] Failed to parse x-death header, assuming first delivery", _consumerKey);
            return 0;
        }
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

    private Activity? StartConsumeActivity(string eventTypeName, BasicDeliverEventArgs ea, ActivityContext parentContext, string operation)
    {
        var links = parentContext != default ? new[] { new ActivityLink(parentContext) } : null;

        var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} {operation}", ActivityKind.Consumer, parentContext: default, links: links);

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

    public ValueTask DisposeAsync()
    {
        if (_disposed) return default;
        _disposed = true;
        return default;
    }
}
