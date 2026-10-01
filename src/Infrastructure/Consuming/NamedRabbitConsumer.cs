using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Exceptions;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Topology;
using RabbitFlow.Infrastructure.Versioning;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;

namespace RabbitFlow.Infrastructure.Consuming;

/// <summary>
/// Consumes messages from a single RabbitMQ queue and dispatches them to
/// registered <see cref="IRabbitHandler{T}"/> implementations.
///
/// <para>
/// Channel strategy: A single long-lived channel per consumer. If the channel or
/// connection drops, the consumer reconnects automatically in a loop.
/// </para>
///
/// <para>
/// Concurrency: Controlled by <see cref="RabbitConsumerOptions.PrefetchCount"/> (AMQP level)
/// and optionally <see cref="RabbitConsumerOptions.MaxConcurrentHandlers"/> (handler level
/// via <see cref="SemaphoreSlim"/>).
/// </para>
/// </summary>
internal sealed class NamedRabbitConsumer : IAsyncDisposable
{
    private static readonly ConcurrentDictionary<Type, Func<object, object, MessageContext, CancellationToken, Task>> HandlerInvokers = new();

    private readonly string _consumerKey;
    private readonly RabbitConsumerOptions _options;
    private readonly ManagedConnection _connection;
    private readonly HandlerTypeRegistry _registry;
    private readonly EventUpgraderRegistry? _upgraderRegistry;
    private readonly IMessageSerializer _serializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<NamedRabbitConsumer> _logger;
    private readonly RabbitMqMetrics _metrics;
    private readonly SemaphoreSlim? _concurrencyLimiter;
    /// <summary>
    /// The initial count of <see cref="_concurrencyLimiter"/> (i.e., <c>MaxConcurrentHandlers</c>).
    /// Stored separately because <see cref="SemaphoreSlim"/> does not expose <c>InitialCount</c>
    /// (unlike <see cref="System.Threading.Semaphore"/>). Used by <see cref="GetInFlightCount"/>
    /// to compute <c>_concurrencyLimiterInitialCount - _concurrencyLimiter.CurrentCount</c>
    /// (permits held = in-flight handlers).
    /// </summary>
    private readonly int _concurrencyLimiterInitialCount;
    private readonly RetryPolicy? _retryPolicy;
    private volatile IChannel? _currentChannel;
    private readonly RetryQueuePublisher? _retryPublisher;
    private volatile bool _disposed;

    /// <summary>
    /// Set to true when the consumer begins graceful shutdown (stoppingToken fired).
    /// Used by InvokeHandlerWithRetryAsync to distinguish OperationCanceledException
    /// caused by shutdown (→ NACK with requeue, no retry count) from OCE caused by
    /// handler logic (→ retry/DLQ path). This is checked INSTEAD of _shutdownCts.IsCancellationRequested
    /// because the channel close (which happens during shutdown) can produce OCE before
    /// _shutdownCts.Cancel() is called by GracefulDrainAsync.
    /// </summary>
    private volatile bool _isShuttingDown;

    /// <summary>
    /// Cancellation token source whose <see cref="CancellationTokenSource.Token"/> is passed to
    /// handlers via the <c>CancellationToken</c> parameter in <see cref="OnMessageReceived"/>.
    /// It is cancelled in TWO places:
    /// <list type="bullet">
    ///   <item>In <see cref="GracefulDrainAsync"/>, AFTER the drain timeout expires — so
    ///   cooperative in-flight handlers can abort and have their messages NACKed with requeue
    ///   (no retry count) by <see cref="InvokeHandlerWithRetryAsync"/>'s catch block.</item>
    ///   <item>In <see cref="DisposeAsync"/>, as a final safety net (consumer is being
    ///   disposed, no further handler invocations are possible).</item>
    /// </list>
    /// During the drain window (before the timeout), the token is NOT cancelled, so
    /// in-flight handlers can complete naturally and ACK their messages (no data loss).
    /// </summary>
    private readonly CancellationTokenSource _shutdownCts = new();

    /// <summary>
    /// Tracks the number of in-flight message handlers (entered <see cref="OnMessageReceived"/>
    /// but not yet returned). Used by <see cref="GracefulDrainAsync"/> to wait for in-flight
    /// handlers to complete during graceful shutdown when <see cref="_concurrencyLimiter"/> is
    /// not configured (i.e., <see cref="RabbitConsumerOptions.MaxConcurrentHandlers"/> == 0).
    /// When the limiter IS configured, <see cref="_concurrencyLimiterInitialCount"/> minus
    /// <see cref="SemaphoreSlim.CurrentCount"/> already tracks in-flight work, so this field
    /// is redundant but still maintained for uniformity (cost: a single <c>Interlocked</c> op
    /// per message — negligible).
    /// </summary>
    private int _inFlightCount;

    public NamedRabbitConsumer(string consumerKey,
                               RabbitConsumerOptions options,
                               ManagedConnection connection,
                               HandlerTypeRegistry registry,
                               IMessageSerializer serializer,
                               IServiceScopeFactory scopeFactory,
                               ILogger<NamedRabbitConsumer> logger,
                               RabbitMqMetrics metrics,
                               EventUpgraderRegistry? upgraderRegistry = null)
    {
        _consumerKey = consumerKey;
        _options = options;
        _connection = connection;
        _registry = registry;
        _upgraderRegistry = upgraderRegistry;
        _serializer = serializer;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _metrics = metrics;

        if (options.MaxConcurrentHandlers > 0)
        {
            _concurrencyLimiter = new SemaphoreSlim(options.MaxConcurrentHandlers);
            _concurrencyLimiterInitialCount = options.MaxConcurrentHandlers;
        }

        if (options.EnableRetry)
        {
            _retryPolicy = new RetryPolicy(options);
            var handlerConcurrency = options.MaxConcurrentHandlers > 0 ? options.MaxConcurrentHandlers : options.PrefetchCount;
            _retryPublisher = new RetryQueuePublisher(consumerKey, options.QueueName, connection, handlerConcurrency, logger);
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[Consumer:{Key}] Starting consumer loop → queue '{Queue}'", _consumerKey, _options.QueueName);

        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            IChannel? channel = null;
            try
            {
                CreateChannelOptions? channelOptions = null;
                if (_options.MaxConcurrentHandlers > 0)
                {
                    var dispatchConcurrency = (ushort)Math.Min(65535, _options.MaxConcurrentHandlers);
                  channelOptions = new CreateChannelOptions(
                        publisherConfirmationsEnabled: false,
                        publisherConfirmationTrackingEnabled: false,
                        consumerDispatchConcurrency: dispatchConcurrency);
                }
                else if (_options.PrefetchCount > 1)
                {
                    channelOptions = new CreateChannelOptions(
                        publisherConfirmationsEnabled: false,
                        publisherConfirmationTrackingEnabled: false,
                        consumerDispatchConcurrency: _options.PrefetchCount);
                }

                channel = await _connection.CreateChannelAsync(channelOptions, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                _currentChannel = channel;

                await InitializeChannelAsync(channel, cancellationToken)
                    .ConfigureAwait(false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                async Task OnReceived(object sender, BasicDeliverEventArgs ea)
                {
                    await OnMessageReceived(channel, ea).ConfigureAwait(false);
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

                _logger.LogInformation("[Consumer:{Key}] Consuming from '{Queue}' (tag={Tag}, prefetch={Prefetch}, singleActive={SingleActive})", _consumerKey, _options.QueueName, consumerTag, _options.PrefetchCount, _options.SingleActiveConsumer);

                try
                {
  var stoppingTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    using var reg = cancellationToken.Register(() => stoppingTcs.TrySetResult(true));

                    await Task.WhenAny(shutdownTcs.Task, stoppingTcs.Task).ConfigureAwait(false);

                    if (stoppingTcs.Task.IsCompleted)
                    {

                        _isShuttingDown = true; 
                        await GracefulDrainAsync(channel, consumerTag).ConfigureAwait(false);
                    }
                }
                finally
                {
                    channel.ChannelShutdownAsync -= OnShutdown;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("[Consumer:{Key}] Consumer loop stopped", _consumerKey);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Consumer:{Key}] Channel/consume error, reconnecting in 2s...", _consumerKey);

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
    /// Performs the graceful-shutdown drain protocol:
    /// <list type="number">
    ///   <item>Cancels the consumer (broker stops delivering NEW messages) via
    ///   <c>BasicCancelAsync</c>.</item>
    ///   <item>Polls the in-flight handler count (either via <see cref="_concurrencyLimiter"/>
    ///   when configured, or via <see cref="_inFlightCount"/> for the unlimited case) until
    ///   it reaches zero or <see cref="RabbitConsumerOptions.ShutdownDrainTimeout"/> elapses.</item>
    ///   <item>If the drain timeout expires with handlers still in flight, cancels
    ///   <see cref="_shutdownCts"/> so the handlers' <c>CancellationToken</c> (passed via
    ///   <see cref="ProcessMessageAsync"/>) fires — cooperative handlers will throw
    ///   <see cref="OperationCanceledException"/>, which <see cref="InvokeHandlerWithRetryAsync"/>
    ///   routes to a NACK-with-requeue WITHOUT counting as a retry. Non-cooperative handlers
    ///   continue running; their messages are re-queued by the broker when the channel is
    ///   closed by the outer <see cref="RunAsync"/> finally block.</item>
    /// </list>
    /// This method is only invoked when the host's stoppingToken fires (graceful app shutdown).
    /// When the channel closes naturally (broker restart / fault), the channel is already gone
    /// so there is nothing to drain — <see cref="RunAsync"/> skips this method in that case.
    /// </summary>
    /// <param name="channel">The channel the consumer is currently consuming from.</param>
    /// <param name="consumerTag">The consumer tag returned by <c>BasicConsumeAsync</c>.</param>
    private async Task GracefulDrainAsync(IChannel channel, string consumerTag)
    {
        try
        {
            await channel.BasicCancelAsync(consumerTag, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false);
            _logger.LogInformation("[Consumer:{Key}] Consumer cancelled; draining in-flight handlers (timeout={Timeout}s)...", _consumerKey, _options.ShutdownDrainTimeout.TotalSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Consumer:{Key}] BasicCancelAsync failed during graceful drain", _consumerKey);
        }

        var drainDeadline = DateTime.UtcNow + _options.ShutdownDrainTimeout;
        while (DateTime.UtcNow < drainDeadline)
        {
            if (GetInFlightCount() == 0)
            {
                _logger.LogInformation("[Consumer:{Key}] All in-flight handlers completed during graceful drain.", _consumerKey);
                return;
            }

            await Task.Delay(100, CancellationToken.None).ConfigureAwait(false);
        }

        var remaining = GetInFlightCount();
        _logger.LogWarning("[Consumer:{Key}] Graceful drain timeout expired with {Count} in-flight handler(s); cancelling handler token (will NACK with requeue, no retry count).",
            _consumerKey, remaining);

        try
        {
            _shutdownCts.Cancel();
        }
        catch (ObjectDisposedException) { }

    if (remaining > 0)
        {
            try
            {
                await Task.Delay(500, CancellationToken.None).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }

    /// <summary>
    /// Returns the number of currently in-flight message handlers. When
    /// <see cref="_concurrencyLimiter"/> is configured (MaxConcurrentHandlers > 0), uses
    /// <see cref="_concurrencyLimiterInitialCount"/> minus <see cref="SemaphoreSlim.CurrentCount"/>
    /// (permits held by handlers). Otherwise uses the <see cref="_inFlightCount"/> counter
    /// maintained by <see cref="OnMessageReceived"/>.
    /// </summary>
    private int GetInFlightCount()
    {
        if (_concurrencyLimiter is not null)
            return _concurrencyLimiterInitialCount - _concurrencyLimiter.CurrentCount;

        return Volatile.Read(ref _inFlightCount);
    }

    private async Task OnMessageReceived(IChannel channel, BasicDeliverEventArgs ea)
    {
    Interlocked.Increment(ref _inFlightCount);
        try
        {
            var body = ea.Body;
            var properties = ea.BasicProperties;
            var deliveryTag = ea.DeliveryTag;

            var retryCount = ExtractRetryCount(properties);
            var deliveryCount = retryCount + 1;

            try
            {
                await ProcessMessageAsync(channel, ea, body, properties, deliveryTag, retryCount, deliveryCount, _shutdownCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException and not ThreadAbortException)
            {
                await HandlePoisonMessageAsync(channel, ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _inFlightCount);
        }
    }

    /// <summary>
    /// Processes a single message: header validation, handler resolution, deserialization,
    /// (optional) event upgrade, and handler dispatch with retry/DLQ.
    /// </summary>
    /// <remarks>
    /// Any exception thrown by this method is caught by <see cref="OnMessageReceived"/>'s
    /// top-level try/catch and treated as a poison message (routed to retry/DLQ). This
    /// prevents an infinite requeue loop when the message body is corrupt, the schema is
    /// incompatible, or an event upgrader throws.
    /// </remarks>
    private async Task ProcessMessageAsync(IChannel channel,
                                           BasicDeliverEventArgs ea,
                                           ReadOnlyMemory<byte> body,
                                           IReadOnlyBasicProperties properties,
                                           ulong deliveryTag,
                                           int retryCount,
                                           int deliveryCount,
                                           CancellationToken cancellationToken)
    {
        var eventTypeName = GetHeaderString(properties, MessageHeaders.EventType);
        if (eventTypeName is null)
        {
            _logger.LogWarning("[Consumer:{Key}] Missing '{Header}' header — Nack (deliveryTag={Tag})", _consumerKey, MessageHeaders.EventType, deliveryTag);
            await NackAsync(channel, deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var resolved = _registry.Resolve(_consumerKey, eventTypeName);
        if (resolved is null)
        {
            _logger.LogWarning("[Consumer:{Key}] No handler for '{EventType}' — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(channel, deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var (handlerType, eventType, isBatch) = resolved.Value;

        if (isBatch)
        {
            _logger.LogWarning("[Consumer:{Key}] Handler for '{EventType}' is IBatchRabbitHandler<>, use batch consumer mode — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(channel, deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var eventVersion = GetEventVersion(properties);
        object? @event;

        if (_upgraderRegistry is not null)
        {
            var highestVersion = _upgraderRegistry.GetHighestVersion(eventTypeName);
            var deserializationType = _upgraderRegistry.GetTypeForVersion(eventTypeName, eventVersion);

            if (deserializationType is not null && eventVersion < highestVersion)
            {
                var oldEvent = _serializer.Deserialize(body, deserializationType);
                if (oldEvent is null)
                {
                    _logger.LogError("[Consumer:{Key}] Failed to deserialize '{EventType}' v{Version} (deliveryTag={Tag})", _consumerKey, eventTypeName, eventVersion, deliveryTag);
                    await NackAsync(channel, deliveryTag, requeue: false)
                        .ConfigureAwait(false);
                    return;
                }

                using var upgradeScope = _scopeFactory.CreateScope();
                @event = _upgraderRegistry.Upgrade(eventTypeName, oldEvent, eventVersion, upgradeScope.ServiceProvider);

                _logger.LogDebug("[Consumer:{Key}] Upgraded '{EventType}' v{Version} → v{HighestVersion}", _consumerKey, eventTypeName, eventVersion, highestVersion);
            }
            else
                @event = _serializer.Deserialize(body, eventType);

        }
        else
            @event = _serializer.Deserialize(body, eventType);

        if (@event is null)
        {
            _logger.LogError("[Consumer:{Key}] Failed to deserialize '{EventType}' (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(channel, deliveryTag, requeue: false)
                .ConfigureAwait(false);
            return;
        }

        var context = BuildMessageContext(ea, retryCount);

        var parentContext = ExtractParentContext(properties);

        using var receiveActivity = StartConsumeActivity(eventTypeName, ea, parentContext, RabbitMqActivitySource.OperationReceive);

        if (_concurrencyLimiter is not null)
        {
            await _concurrencyLimiter.WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            try
            {
                await InvokeHandlerWithRetryAsync(channel, handlerType, @event, context, deliveryTag, deliveryCount, ea, body, properties, eventTypeName, parentContext, cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    _concurrencyLimiter.Release();
                }
                catch (ObjectDisposedException) { }
            }
        }
        else
        {
            await InvokeHandlerWithRetryAsync(channel, handlerType, @event, context, deliveryTag, deliveryCount, ea, body, properties, eventTypeName, parentContext, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Handles a poison message — one that threw an exception during pre-handler processing
    /// (deserialization, event upgrade, etc.) before the handler was invoked. Routes the
    /// message through the same retry/DLQ flow as a handler failure so it eventually lands
    /// in the DLQ instead of looping forever via broker requeue.
    /// </summary>
    /// <remarks>
    /// This mirrors the catch block in <see cref="InvokeHandlerWithRetryAsync"/>:
    /// <list type="bullet">
    ///   <item>If <see cref="ShouldRetry"/> is true: publish to the retry queue and ACK the
    ///   original delivery (so the broker stops redelivering it). The retry queue's TTL
    ///   re-delivers it to the main queue after the delay.</item>
    ///   <item>If retries are exhausted: NACK with requeue=false so the broker dead-letters
    ///   the message to the DLQ via x-dead-letter-exchange, then invoke the (optional)
    ///   IDeadLetterHandler for application-level notification.</item>
    /// </list>
    /// If the retry-publish fails (broker/channel unavailable), fall back to NACK with
    /// requeue=true to preserve at-least-once semantics without losing the message.
    /// </remarks>
    private async Task HandlePoisonMessageAsync(IChannel channel, BasicDeliverEventArgs ea, ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, Exception ex, int deliveryCount)
    {
        var eventTypeName = GetHeaderString(properties, MessageHeaders.EventType) ?? "<unknown>";

        if (ex is MessageDeserializationException)
        {
            _metrics.ConsumeErrors.Add(1,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagErrorType, nameof(MessageDeserializationException)),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            _logger.LogError(ex, "[Consumer:{Key}] Message deserialization failed for '{EventType}' — routing to DLQ/discard without retry (EnableDeadLetter={EnableDeadLetter}, deliveryTag={Tag})", _consumerKey, eventTypeName, _options.EnableDeadLetter, ea.DeliveryTag);

            await NackAsync(channel, ea.DeliveryTag, requeue: false).ConfigureAwait(false);

            await InvokeDeadLetterHandlerAsync(ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
            return;
        }

        _metrics.ConsumeErrors.Add(1,
            new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
            new(RabbitMqMetrics.TagEventType, eventTypeName),
            new(RabbitMqMetrics.TagErrorType, ex.GetType().FullName),
            new(RabbitMqMetrics.TagQueue, _options.QueueName));

        _logger.LogError(ex, "[Consumer:{Key}] Poison message (pre-handler failure) attempt {Attempt}/{Max} (deliveryTag={Tag})", _consumerKey, deliveryCount, _retryPolicy?.MaxRetries ?? 1, ea.DeliveryTag);

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
                await AckAsync(channel, ea.DeliveryTag).ConfigureAwait(false);
            else
                await NackAsync(channel, ea.DeliveryTag, requeue: true).ConfigureAwait(false);

        }
        else
        {
            _metrics.DeadLettered.Add(1,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName),
                new(RabbitMqMetrics.TagDlqName, _options.ResolvedDlqName));

            if (_options.EnableDeadLetter)
                _logger.LogWarning("[Consumer:{Key}] Poison message retries exhausted ({Attempts}) — dead-lettering to DLQ (deliveryTag={Tag})",
                    _consumerKey, deliveryCount, ea.DeliveryTag);
            else
                _logger.LogWarning("[Consumer:{Key}] Poison message retries exhausted ({Attempts}) — discarding message (EnableDeadLetter=false, deliveryTag={Tag})",
                    _consumerKey, deliveryCount, ea.DeliveryTag);

            await NackAsync(channel, ea.DeliveryTag, requeue: false).ConfigureAwait(false);

            await InvokeDeadLetterHandlerAsync(ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
        }
    }

    private async Task InvokeHandlerWithRetryAsync(IChannel channel,
                                                   Type handlerType,
                                                   object @event,
                                                   MessageContext context,
                                                   ulong deliveryTag,
                                                   int deliveryCount,
                                                   BasicDeliverEventArgs ea,
                                                   ReadOnlyMemory<byte> body,
                                                   IReadOnlyBasicProperties properties,
                                                   string eventTypeName,
                                                   ActivityContext parentContext,
                                                   CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetService(handlerType);

        if (handler is null)
        {
            _logger.LogError("[Consumer:{Key}] Handler '{HandlerType}' not in DI — Nack (deliveryTag={Tag})", _consumerKey, handlerType.Name, deliveryTag);
            await NackAsync(channel, deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        using var processActivity = StartConsumeActivity(eventTypeName, ea, parentContext, RabbitMqActivitySource.OperationProcess);

        processActivity?.SetTag(RabbitMqActivitySource.TagMessagingDeliveryAttempt, deliveryCount);

        var handlerSw = ValueStopwatch.StartNew();
        try
        {
            var invoker = HandlerInvokers.GetOrAdd(handlerType, CompileInvoker);
            var handlerTask = invoker(handler, @event, context, cancellationToken);

            var shutdownTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = _shutdownCts.Token.Register(() => shutdownTcs.TrySetResult(true));

            var winner = await Task.WhenAny(handlerTask, shutdownTcs.Task).ConfigureAwait(false);

            if (winner == handlerTask)
            {

                await handlerTask.ConfigureAwait(false);

                await AckAsync(channel, deliveryTag)
                    .ConfigureAwait(false);

                _metrics.ProcessingDurationMs.Record(handlerSw.GetElapsedMilliseconds(),
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName));

                _metrics.Consumed.Add(1,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName));
            }
            else
            {
                _logger.LogWarning("[Consumer:{Key}] Handler '{HandlerType}' did not complete within {Timeout}s during shutdown — NACK with requeue (deliveryTag={Tag}). The handler may still be running in the background.",
                    _consumerKey, handlerType.Name, _options.ShutdownDrainTimeout.TotalSeconds, deliveryTag);

                await NackAsync(channel, deliveryTag, requeue: true).ConfigureAwait(false);

                _metrics.ConsumeErrors.Add(1,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagErrorType, "ShutdownHandlerTimeout"),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName));
            }
        }
        catch (Exception ex)
        {
            if (_isShuttingDown && ex is OperationCanceledException)
            {
                _logger.LogInformation("[Consumer:{Key}] Handler cancelled during graceful shutdown drain — NACK with requeue (deliveryTag={Tag})", _consumerKey, deliveryTag);
                await NackAsync(channel, deliveryTag, requeue: true).ConfigureAwait(false);
                return;
            }

            _metrics.ConsumeErrors.Add(1,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagErrorType, ex.GetType().FullName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            processActivity?.SetTag(RabbitMqActivitySource.TagErrorType, ex.GetType().FullName);

            _logger.LogError(ex, "[Consumer:{Key}] Handler '{HandlerType}' failed attempt {Attempt}/{Max} (deliveryTag={Tag})", _consumerKey, handlerType.Name, deliveryCount, _retryPolicy?.MaxRetries ?? 1, deliveryTag);

            if (ShouldRetry(deliveryCount))
            {
                _metrics.Retried.Add(1,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName),
                    new(RabbitMqMetrics.TagAttempt, deliveryCount));

                _logger.LogInformation("[Consumer:{Key}] Retrying (attempt {Attempt}, deliveryTag={Tag})", _consumerKey, deliveryCount, deliveryTag);

                var retryDelay = _retryPolicy!.GetRetryDelay(deliveryCount) ?? TimeSpan.Zero;
                var publishSucceeded = await PublishToRetryQueueAsync(body, properties, retryDelay)
                    .ConfigureAwait(false);

                if (publishSucceeded)
                    await AckAsync(channel, deliveryTag).ConfigureAwait(false);
                else
                    await NackAsync(channel, deliveryTag, requeue: true).ConfigureAwait(false);
            }
            else
            {
                _metrics.DeadLettered.Add(1,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName),
                    new(RabbitMqMetrics.TagDlqName, _options.ResolvedDlqName));

                if (_options.EnableDeadLetter)
                    _logger.LogWarning("[Consumer:{Key}] Retries exhausted ({Attempts}) — dead-lettering to DLQ (deliveryTag={Tag})", _consumerKey, deliveryCount, deliveryTag);
                else
                    _logger.LogWarning("[Consumer:{Key}] Retries exhausted ({Attempts}) — discarding message (EnableDeadLetter=false, deliveryTag={Tag})", _consumerKey, deliveryCount, deliveryTag);

                await NackAsync(channel, deliveryTag, requeue: false).ConfigureAwait(false);

                await InvokeDeadLetterHandlerAsync(ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
            }
        }
    }

    private bool ShouldRetry(int deliveryCount)
    {
        return _retryPolicy is not null && _retryPolicy.ShouldRetry(deliveryCount);
    }

    /// <summary>
    /// Converts <see cref="IReadOnlyBasicProperties"/> (received from the broker on consume)
    /// to a <see cref="BasicProperties"/> struct (required for re-publishing via
    /// <c>BasicPublishAsync</c>). In RabbitMQ.Client 7.x, consumed messages arrive with
    /// <c>ReadOnlyBasicProperties</c> (a class), while <c>BasicPublishAsync</c> requires
    /// <c>BasicProperties</c> (a struct). A direct cast throws <c>InvalidCastException</c>,
    /// so we manually copy the fields.
    /// </summary>
    /// <param name="stripXDeath">
    /// When true, removes the <c>x-death</c> header from the copied headers. This is used
    /// when publishing to a retry queue: the broker will add fresh x-death entries when the
    /// retry queue's TTL expires and the message is dead-lettered back to the main exchange.
    /// Keeping the old x-death would cause the broker to merge/update existing entries instead
    /// of adding new ones, making the retry count unreliable.
    /// <param name="source">The source properties to copy.</param>
    /// </param>
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
    private async Task InvokeDeadLetterHandlerAsync(BasicDeliverEventArgs ea, ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, Exception handlerException, int deliveryCount)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var dlHandlers = scope.ServiceProvider.GetServices<IDeadLetterHandler>();
            var dlHandler = dlHandlers.FirstOrDefault(h => h.ConsumerKey == _consumerKey);

            if (dlHandler is not null)
            {
                var deadLetterMsg = new DeadLetterMessage
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
                };

                await dlHandler.HandleAsync(deadLetterMsg).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Consumer:{Key}] IDeadLetterHandler threw (deliveryTag={Tag})", _consumerKey, ea.DeliveryTag);
        }
    }

    private int ExtractRetryCount(IReadOnlyBasicProperties properties)
    {
        var countStr = GetHeaderString(properties, MessageHeaders.DeliveryCount);
        if (countStr is not null && int.TryParse(countStr, out var count) && count >= 0)
            return count;
        return 0;
    }

    private static Func<object, object, MessageContext, CancellationToken, Task> CompileInvoker(Type handlerType)
    {
        var handlerInterface = handlerType.GetInterfaces()
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRabbitHandler<>));

        var eventType = handlerInterface.GetGenericArguments()[0];
        var handleMethod = typeof(IRabbitHandler<>).MakeGenericType(eventType)
            .GetMethod(nameof(IRabbitHandler<IIntegrationEvent>.HandleAsync))!;

        var hParam = Expression.Parameter(typeof(object), "h");
        var eParam = Expression.Parameter(typeof(object), "e");
        var cParam = Expression.Parameter(typeof(MessageContext), "c");
        var tParam = Expression.Parameter(typeof(CancellationToken), "t");

        var call = Expression.Call(Expression.Convert(hParam, handlerInterface), handleMethod, Expression.Convert(eParam, eventType), cParam, tParam);

        return Expression.Lambda<Func<object, object, MessageContext, CancellationToken, Task>>(call, hParam, eParam, cParam, tParam).Compile();
    }

    private async Task AckAsync(IChannel channel, ulong deliveryTag)
    {
        try
        {
            await channel.BasicAckAsync(deliveryTag: deliveryTag, multiple: false).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[Consumer:{Key}] Ack failed (deliveryTag={Tag}, channel likely closed)", _consumerKey, deliveryTag);
        }
    }

    private async Task NackAsync(IChannel channel, ulong deliveryTag, bool requeue)
    {
        try
        {
            await channel.BasicNackAsync(deliveryTag: deliveryTag, multiple: false, requeue: requeue).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[Consumer:{Key}] Nack failed (deliveryTag={Tag}, channel likely closed)", _consumerKey, deliveryTag);
        }
    }

    private async Task InitializeChannelAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: _options.PrefetchCount,
            global: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await TopologyDeclarator.DeclareConsumerTopologyAsync(channel, _options, _logger, cancellationToken).ConfigureAwait(false);
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

    private static string? GetHeaderString(IReadOnlyBasicProperties properties, string key)
    {
        if (properties.Headers is null || !properties.Headers.TryGetValue(key, out var value))
            return null;

        return value switch
        {
            string s => s,
            byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> rom => System.Text.Encoding.UTF8.GetString(rom.Span),
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

    /// <summary>
    /// High-performance stopwatch that avoids <see cref="System.Diagnostics.Stopwatch"/>
    /// allocation. Returns elapsed milliseconds as double.
    /// </summary>
    private readonly struct ValueStopwatch
    {
        private readonly long _startTimestamp;

        private ValueStopwatch(long startTimestamp) => _startTimestamp = startTimestamp;

        public static ValueStopwatch StartNew() => new(Stopwatch.GetTimestamp());

        public double GetElapsedMilliseconds() =>
            Stopwatch.GetElapsedTime(_startTimestamp).TotalMilliseconds;
    }

    /// <summary>
    /// Extracts the event version from the x-event-version header.
    /// Returns 1 if the header is missing or unparseable.
    /// </summary>
    private static int GetEventVersion(IReadOnlyBasicProperties properties)
    {
        var str = GetHeaderString(properties, MessageHeaders.EventVersion);
        if (str is null || !int.TryParse(str, out var version))
            return 1;
        return version;
    }

    /// <summary>
    /// Extracts the W3C traceparent from AMQP headers and returns
    /// the <see cref="ActivityContext"/> to use as parent for consumer activities.
    /// </summary>
    private static ActivityContext ExtractParentContext(IReadOnlyBasicProperties properties)
    {
        var traceParent = GetHeaderString(properties, RabbitMqActivitySource.TraceParentHeader);

        if (traceParent is not null && ActivityContext.TryParse(traceParent, null, out var context))
            return context;

        return Activity.Current?.Context ?? default;
    }

    /// <summary>
    /// Starts a consumer <see cref="Activity"/> linked to the publisher's trace context.
    /// Sets standard OTel messaging tags.
    /// </summary>
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

    private static async Task SafeCloseChannelAsync(IChannel channel)
    {
        try
        {
            await channel.CloseAsync().ConfigureAwait(false);
        }
        catch (AlreadyClosedException) { }
        catch (ObjectDisposedException) { }
        catch { }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _isShuttingDown = true;

        try { _shutdownCts.Cancel(); }
        catch (ObjectDisposedException) { }

        var drainDeadline = DateTime.UtcNow + _options.ShutdownDrainTimeout;
        while (DateTime.UtcNow < drainDeadline)
        {
            if (GetInFlightCount() == 0)
                break;

            await Task.Delay(100, CancellationToken.None).ConfigureAwait(false);
        }

        var remaining = GetInFlightCount();
        if (remaining > 0)
            _logger.LogWarning("[Consumer:{Key}] DisposeAsync: {Count} handler(s) still in flight after drain timeout; disposing semaphore (they may throw ObjectDisposedException on Release).", _consumerKey, remaining);
        
        if (_retryPublisher is not null)
            await _retryPublisher.DisposeAsync().ConfigureAwait(false);

        _concurrencyLimiter?.Dispose();
        _shutdownCts.Dispose();
    }
}
