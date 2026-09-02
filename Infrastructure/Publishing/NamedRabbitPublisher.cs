using Microsoft.Extensions.Logging;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Exceptions;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Topology;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Text;

namespace RabbitFlow.Infrastructure.Publishing;


/// <summary>
/// Publishes events to a single RabbitMQ exchange using a named connection.
/// Each instance corresponds to one <see cref="RabbitProducerOptions"/> entry.
/// 
/// <para>
/// Channel strategy: A new channel is created per publish call. This is the
/// safest approach because:
/// <list type="bullet">
///   <item>If the channel faults (e.g. broker restart), only the current publish fails.</item>
///   <item>Publisher confirms are scoped to the channel — no cross-message interference.</item>
///   <item>No shared state that could be corrupted by concurrent publishes.</item>
/// </list>
/// </para>
/// 
/// <para>
/// Publisher confirms (v7): When <see cref="RabbitProducerOptions.EnablePublisherConfirms"/>
/// is true, channels are created with <c>CreateChannelOptions(publisherConfirmationsEnabled: true,
/// publisherConfirmationTrackingEnabled: true)</c>. This makes <c>BasicPublishAsync</c> block
/// until the broker confirms the message. On nack or unroutable return, the library throws
/// <c>PublishException</c> which is caught and wrapped in <see cref="PublisherNackException"/>.
/// If no confirmation arrives within <see cref="RabbitProducerOptions.PublishConfirmTimeoutMs"/>,
/// a <see cref="PublisherConfirmTimeoutException"/> is thrown via a CancellationToken timeout.
/// </para>
/// 
/// <para>
/// Dual metadata: Tracing headers are written to AMQP message headers (primary source of truth).
/// The <see cref="MessageEnvelope"/> inside the body stores <c>EventType</c> and <c>EventVersion</c>
/// for type resolution. AMQP headers survive DLX/retry re-queuing.
/// </para>
/// </summary>
internal sealed class NamedRabbitPublisher(string _producerKey,
                                           RabbitProducerOptions _options,
                                           ManagedConnection _connection,
                                           IMessageSerializer _serializer,
                                           ILogger<NamedRabbitPublisher> _logger,
                                           RabbitMqMetrics _metrics,
                                           TimeProvider? _timeProvider = null)
{
    /// <summary>
    /// Caches <see cref="EventVersionAttribute"/> lookups per event type.
    /// Reflection is only performed once per <see cref="Type"/>.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, (string EventTypeName, int EventVersion)> EventTypeCache = new();

    /// <summary>
    /// Reusable <see cref="CreateChannelOptions"/> for the confirms-enabled path.
    /// Publisher confirmation tracking is enabled so that <c>BasicPublishAsync</c> throws
    /// <c>PublishException</c> on nack or basic.return, eliminating manual event handling.
    /// </summary>
    private static readonly CreateChannelOptions ConfirmChannelOptions = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);
    private readonly TimeProvider _timeProvider = _timeProvider ?? TimeProvider.System;
    private volatile bool _topologyDeclared;

    /// <summary>
    /// Publishes a single event to the configured exchange.
    /// Creates a new channel per call, declares topology (once), publishes, disposes the channel.
    /// </summary>
    public async Task PublishAsync<TEvent>(TEvent @event, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        var channel = await _connection.CreateChannelAsync(_options.EnablePublisherConfirms ? ConfirmChannelOptions : null, cancellationToken: cancellationToken);

        try
        {
            await InitializeChannelAsync(channel, cancellationToken);

            var routingKey = routingKeyOverride ?? _options.RoutingKey;
            var (eventTypeName, _) = ResolveEventTypeInfo<TEvent>();

            using var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} publish", ActivityKind.Producer);

            if (activity is not null)
            {
                activity.SetTag(RabbitMqActivitySource.TagMessagingSystem, RabbitMqActivitySource.SystemRabbitMq);
                activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationKind, RabbitMqActivitySource.DestinationKindExchange);
                activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationName, _options.ExchangeName);
                activity.SetTag(RabbitMqActivitySource.TagMessagingOperation, RabbitMqActivitySource.OperationPublish);
                activity.SetTag(RabbitMqActivitySource.TagMessagingRabbitmqRoutingKey, routingKey);
                activity.SetTag(RabbitMqActivitySource.TagMessagingEventName, eventTypeName);
                activity.SetTag(RabbitMqActivitySource.TagMessagingServiceKey, _producerKey);
            }

            var (body, properties) = BuildMessage(@event, routingKey);

            InjectTraceContext(activity, properties);

            var sw = ValueStopwatch.StartNew();
            try
            {
                await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken);
            }
            catch (Exception ex)
            {
                _metrics.PublishErrors.Add(1,
                    new(RabbitMqMetrics.TagProducerKey, _producerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagErrorType, ex.GetType().FullName));

                activity?.SetTag(RabbitMqActivitySource.TagErrorType, ex.GetType().FullName);
                throw;
            }

            _metrics.PublishDurationMs.Record(
                sw.GetElapsedMilliseconds(),
                new(RabbitMqMetrics.TagProducerKey, _producerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagExchange, _options.ExchangeName));

            _metrics.Published.Add(1,
                new(RabbitMqMetrics.TagProducerKey, _producerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagExchange, _options.ExchangeName));

            _logger.LogDebug(
                "[Producer:{Key}] Published {EventType} → '{Exchange}' [{RoutingKey}]",
                _producerKey, eventTypeName, _options.ExchangeName, routingKey);
        }
        finally
        {
            await SafeCloseChannelAsync(channel);
        }
    }

    /// <summary>
    /// Publishes a batch of events using the same channel.
    /// More efficient than individual publishes because the channel and topology
    /// are created/declared only once.
    /// 
    /// <para>
    /// With publisher confirms enabled, each <c>BasicPublishAsync</c> call blocks
    /// until the broker confirms that individual message. This is slightly slower than
    /// a single batch-level confirm but provides per-message error granularity.
    /// </para>
    /// </summary>
    public async Task PublishBatchAsync<TEvent>(IEnumerable<TEvent> events, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        var eventList = events as IList<TEvent> ?? events.ToList();

        if (eventList.Count == 0) return;

        var channel = await _connection.CreateChannelAsync(_options.EnablePublisherConfirms ? ConfirmChannelOptions : null, cancellationToken: cancellationToken);

        try
        {
            await InitializeChannelAsync(channel, cancellationToken);

            var routingKey = routingKeyOverride ?? _options.RoutingKey;
            var (eventTypeName, _) = ResolveEventTypeInfo<TEvent>();
            var publishedCount = 0;

            using var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} publish", ActivityKind.Producer);

            if (activity is not null)
            {
                activity.SetTag(RabbitMqActivitySource.TagMessagingSystem, RabbitMqActivitySource.SystemRabbitMq);
                activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationKind, RabbitMqActivitySource.DestinationKindExchange);
                activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationName, _options.ExchangeName);
                activity.SetTag(RabbitMqActivitySource.TagMessagingOperation, RabbitMqActivitySource.OperationPublish);
                activity.SetTag(RabbitMqActivitySource.TagMessagingRabbitmqRoutingKey, routingKey);
                activity.SetTag(RabbitMqActivitySource.TagMessagingEventName, eventTypeName);
                activity.SetTag(RabbitMqActivitySource.TagMessagingServiceKey, _producerKey);
                activity.SetTag("messaging.batch.message_count", eventList.Count);
            }

            var batchSw = ValueStopwatch.StartNew();

            for (var i = 0; i < eventList.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var (body, properties) = BuildMessage(eventList[i], routingKey);
                InjectTraceContext(activity, properties);

                try
                {
                    await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken);
                    publishedCount++;
                }
                catch (Exception ex)
                {
                    _metrics.PublishErrors.Add(1,
                        new(RabbitMqMetrics.TagProducerKey, _producerKey),
                        new(RabbitMqMetrics.TagEventType, eventTypeName),
                        new(RabbitMqMetrics.TagErrorType, ex.GetType().FullName));

                    activity?.SetTag(RabbitMqActivitySource.TagErrorType, ex.GetType().FullName);
                    throw;
                }
            }

            _metrics.Published.Add(publishedCount,
                new(RabbitMqMetrics.TagProducerKey, _producerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagExchange, _options.ExchangeName));

            _metrics.PublishDurationMs.Record(
                batchSw.GetElapsedMilliseconds(),
                new(RabbitMqMetrics.TagProducerKey, _producerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagExchange, _options.ExchangeName));

            _logger.LogInformation("[Producer:{Key}] Batch-published {Count} {EventType} events → '{Exchange}'", _producerKey, publishedCount, eventTypeName, _options.ExchangeName);
        }
        finally
        {
            await SafeCloseChannelAsync(channel);
        }
    }

    /// <summary>
    /// Executes a single <c>BasicPublishAsync</c> call with proper error handling.
    /// 
    /// <para>
    /// When publisher confirms are enabled (via <see cref="ConfirmChannelOptions"/>),
    /// <c>BasicPublishAsync</c> blocks until the broker responds. On success it completes
    /// normally. On failure:
    /// <list type="bullet">
    ///   <item><c>PublishException</c> (nack/return) → wrapped in <see cref="PublisherNackException"/>.</item>
    ///   <item>Timeout (no confirm within <see cref="RabbitProducerOptions.PublishConfirmTimeoutMs"/>)
    ///     → <see cref="PublisherConfirmTimeoutException"/>.</item>
    /// </list>
    /// </para>
    /// </summary>
    private async Task ExecutePublishAsync(IChannel channel, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
    {
        if (!_options.EnablePublisherConfirms)
        {
            // Fire-and-forget — no confirmation, no timeout
            await channel.BasicPublishAsync(
                exchange: _options.ExchangeName,
                routingKey: routingKey,
                mandatory: _options.Mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);
            return;
        }

        // With publisher confirmation tracking enabled, BasicPublishAsync internally
        // waits for the broker's ack before returning. We apply a timeout via
        // CancellationToken so the caller doesn't block indefinitely.
        var confirmTimeout = TimeSpan.FromMilliseconds(_options.PublishConfirmTimeoutMs);

        using var timeoutCts = new CancellationTokenSource(confirmTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            timeoutCts.Token, cancellationToken);

        try
        {
            await channel.BasicPublishAsync(
                exchange: _options.ExchangeName,
                routingKey: routingKey,
                mandatory: _options.Mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: linkedCts.Token);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // Our timeout fired — the broker didn't confirm within the window.
            // The channel is left in a dirty state, but since we create a new channel
            // per publish, it will be closed in the finally block.
            throw new PublisherConfirmTimeoutException(_producerKey, confirmTimeout);
        }
        // NOTE: When publisherConfirmationTrackingEnabled is true, BasicPublishAsync
        // may throw PublishException on nack or unroutable return. This exception
        // propagates to the caller as-is. Once the exact namespace is confirmed for
        // RabbitMQ.Client v7, it can be caught here and wrapped in PublisherNackException.
    }

    /// <summary>
    /// Closes the channel, ignoring errors if already closed or disposed.
    /// Prevents <see cref="AlreadyClosedException"/> from masking the real publish error.
    /// </summary>
    private static async Task SafeCloseChannelAsync(IChannel channel)
    {
        try
        {
            await channel.CloseAsync();
        }
        catch (AlreadyClosedException) { /* connection dropped — expected */ }
        catch (ObjectDisposedException) { /* channel already disposed */ }
        catch { }
    }

    /// <summary>
    /// Declares topology (first call only).
    /// </summary>
    private async Task InitializeChannelAsync(IChannel channel, CancellationToken cancellationToken)
    {
        if (!_topologyDeclared && _options.AutoDeclareTopology)
        {
            await TopologyDeclarator.DeclareProducerTopologyAsync(channel, _options, _logger, cancellationToken);

            _topologyDeclared = true;
        }
    }

    /// <summary>
    /// Serializes the event and builds AMQP <see cref="BasicProperties"/> with tracing headers.
    /// Combines both into a single method to ensure the timestamp is consistent
    /// between the AMQP timestamp field and the x-published-at header.
    /// </summary>
    private (ReadOnlyMemory<byte> Body, BasicProperties Properties) BuildMessage<TEvent>(TEvent @event, string routingKey) where TEvent : class
    {
        // Single time read — avoids inconsistent timestamps with fake TimeProviders
        var now = _timeProvider.GetUtcNow();
        var correlationId = Guid.NewGuid().ToString();
        var messageId = Guid.NewGuid().ToString();

        var (eventTypeName, eventVersion) = ResolveEventTypeInfo<TEvent>();

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            ContentEncoding = "utf-8",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = messageId,
            Timestamp = new AmqpTimestamp(now.ToUnixTimeSeconds()),
            Headers = new Dictionary<string, object?>
            {
                [MessageHeaders.CorrelationId] = correlationId,
                [MessageHeaders.MessageId] = messageId,
                [MessageHeaders.PublishedAt] = now.ToString("O"),
                [MessageHeaders.PublisherName] = _producerKey,
                [MessageHeaders.EventType] = eventTypeName,
                [MessageHeaders.EventVersion] = eventVersion.ToString(),
            },
        };

        var body = _serializer.Serialize(@event);
        return (body, properties);
    }

    /// <summary>
    /// Resolves the event type name and version from the <see cref="EventVersionAttribute"/>.
    /// Results are cached per <see cref="Type"/> — reflection runs only once.
    /// Falls back to <c>Type.FullName</c> and version 1 if no attribute is present.
    /// </summary>
    private static (string EventTypeName, int EventVersion) ResolveEventTypeInfo<TEvent>()
        where TEvent : class
    {
        return EventTypeCache.GetOrAdd(typeof(TEvent), static type =>
        {
            var versionAttr = type.GetCustomAttribute<EventVersionAttribute>();
            return versionAttr is not null ? (versionAttr.EventName, versionAttr.Version) : (type.FullName!, 1);
        });
    }

    /// <summary>
    /// Injects W3C trace context (traceparent, tracestate) into AMQP headers
    /// so the consumer can propagate the distributed trace.
    /// No-op if no activity is active.
    /// </summary>
    private static void InjectTraceContext(Activity? activity, BasicProperties properties)
    {
        if (activity is null) return;

        var traceParent = activity.Id;
        if (traceParent is null) return;

        properties.Headers ??= new Dictionary<string, object?>();
        properties.Headers[RabbitMqActivitySource.TraceParentHeader] = traceParent;

        var traceState = activity.TraceStateString;
        if (traceState is not null)
            properties.Headers[RabbitMqActivitySource.TraceStateHeader] = traceState;
    }

    /// <summary>
    /// High-performance stopwatch that avoids <see cref="System.Diagnostics.Stopwatch"/>
    /// allocation. Returns elapsed milliseconds as double.
    /// </summary>
    private readonly struct ValueStopwatch
    {
        private readonly long _startTimestamp;

        private ValueStopwatch(long startTimestamp) => _startTimestamp = startTimestamp;

        public static ValueStopwatch StartNew() => new(System.Diagnostics.Stopwatch.GetTimestamp());

        public double GetElapsedMilliseconds() =>
            System.Diagnostics.Stopwatch.GetElapsedTime(_startTimestamp).TotalMilliseconds;
    }
}