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
/// Channel strategy (configurable via <see cref="RabbitProducerOptions.ChannelPoolSize"/>):
/// <list type="bullet">
///   <item>
///     <b>Channel pool</b> (default, <see cref="RabbitProducerOptions.ChannelPoolSize"/> &gt; 0):
///     Channels are rented from a pool and returned after use. This eliminates
///     the AMQP round-trip overhead of channel creation per publish — the #1
///     throughput bottleneck in channel-per-publish strategies.
///   </item>
///   <item>
///     <b>Channel-per-publish</b> (<see cref="RabbitProducerOptions.ChannelPoolSize"/> = 0):
///     A new channel is created per publish call and closed immediately.
///     This is the safest approach because:
///     <list type="bullet">
///       <item>If the channel faults (e.g. broker restart), only the current publish fails.</item>
///       <item>Publisher confirms are scoped to the channel — no cross-message interference.</item>
///       <item>No shared state that could be corrupted by concurrent publishes.</item>
///     </list>
///     Use this mode for backward compatibility or when throughput is not critical.
///   </item>
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
/// Channel pool + publisher confirms: When both are enabled, faulted channels (e.g. after
/// confirm timeout) are <b>discarded</b> from the pool — they're never reused because
/// unconfirmed messages leave the channel's confirm sequence in an inconsistent state.
/// A fresh channel is created on the next rental.
/// </para>
///
/// <para>
/// Dual metadata: Tracing headers are written to AMQP message headers (primary source of truth).
/// The <see cref="MessageEnvelope"/> inside the body stores <c>EventType</c> and <c>EventVersion</c>
/// for type resolution. AMQP headers survive DLX/retry re-queuing.
/// </para>
/// </summary>
internal sealed class NamedRabbitPublisher : IAsyncDisposable
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
    private static readonly CreateChannelOptions ConfirmChannelOptions = new(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

    private readonly string _producerKey;
    private readonly RabbitProducerOptions _options;
    private readonly ManagedConnection _connection;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<NamedRabbitPublisher> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly RabbitMqMetrics _metrics;

    /// <summary>
    /// Tracks whether topology has been declared at least once. Used only for logging
    /// (to avoid repeating the "Declared exchange" log on every publish). The actual
    /// topology declaration runs on every channel rental because ExchangeDeclareAsync
    /// is idempotent (create-if-not-exists) and cheap (one AMQP round-trip, cached by
    /// the broker). This guarantees topology exists on every new connection, even after
    /// broker restarts that lose metadata — without needing to subscribe to connection
    /// shutdown events or reset a sticky flag.
    /// </summary>
    private volatile bool _topologyDeclared;

    /// <summary>
    /// Channel pool — null when <see cref="RabbitProducerOptions.ChannelPoolSize"/> is 0
    /// (channel-per-publish mode).
    /// </summary>
    private readonly ChannelPool? _channelPool;

    /// <summary>
    /// Whether channel pooling is enabled for this publisher.
    /// </summary>
    private readonly bool _poolingEnabled;

    public NamedRabbitPublisher(string producerKey,
                                RabbitProducerOptions options,
                                ManagedConnection connection,
                                IMessageSerializer serializer,
                                ILogger<NamedRabbitPublisher> logger,
                                RabbitMqMetrics metrics,
                                TimeProvider? timeProvider = null)
    {
        _producerKey = producerKey;
        _options = options;
        _connection = connection;
        _serializer = serializer;
        _logger = logger;
        _metrics = metrics;
        _timeProvider = timeProvider ?? TimeProvider.System;

        _poolingEnabled = options.ChannelPoolSize > 0;

        if (_poolingEnabled)
        {
            var channelOptions = options.EnablePublisherConfirms ? ConfirmChannelOptions : null;
            _channelPool = new ChannelPool(connection, channelOptions, options.ChannelPoolSize, logger, onChannelCreated: ()
                => _metrics.ChannelPoolCreated.Add(1, new KeyValuePair<string, object?>(RabbitMqMetrics.TagProducerKey, producerKey)));

            _logger.LogInformation("[Producer:{Key}] Channel pooling ENABLED (size={PoolSize}, confirms={Confirms})", producerKey, options.ChannelPoolSize, options.EnablePublisherConfirms);
        }
        else
        {
            _channelPool = null;

            _logger.LogInformation("[Producer:{Key}] Channel pooling DISABLED (channel-per-publish, confirms={Confirms})", producerKey, options.EnablePublisherConfirms);
        }
    }

    /// <summary>
    /// Publishes a single event to the configured exchange.
    ///
    /// <para>
    /// With pooling: Rents a channel from the pool, publishes, returns the channel.
    /// Without pooling: Creates a new channel, publishes, closes the channel.
    /// </para>
    /// </summary>
    public async Task PublishAsync<TEvent>(TEvent @event, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        await PublishAsync(@event, correlationId: null, routingKeyOverride, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes a single event with an explicit correlation ID.
    /// </summary>
    public async Task PublishAsync<TEvent>(TEvent @event, string? correlationId, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        if (_poolingEnabled)
            await PublishWithPoolAsync(@event, correlationId, routingKeyOverride, cancellationToken)
                .ConfigureAwait(false);
        else
            await PublishWithoutPoolAsync(@event, correlationId, routingKeyOverride, cancellationToken)
                .ConfigureAwait(false);
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
        if (_poolingEnabled)
            await PublishBatchWithPoolAsync(events, routingKeyOverride, cancellationToken).ConfigureAwait(false);
        else
            await PublishBatchWithoutPoolAsync(events, routingKeyOverride, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes using a channel from the pool.
    /// Faulted channels (after confirm timeout) are discarded.
    /// </summary>
    private async Task PublishWithPoolAsync<TEvent>(TEvent @event, string? correlationId, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        RecordPoolRented();
        var channel = await _channelPool!.RentAsync(cancellationToken)
            .ConfigureAwait(false);
        bool channelFaulted = false;

        try
        {
            await InitializeChannelAsync(channel, cancellationToken)
                .ConfigureAwait(false);

            var routingKey = routingKeyOverride ?? _options.RoutingKey;
            var (eventTypeName, _) = ResolveEventTypeInfo<TEvent>();

            using var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} publish", ActivityKind.Producer);

            SetPublishActivityTags(activity, routingKey, eventTypeName);

            var (body, properties) = BuildMessage(@event, routingKey, correlationId);
            InjectTraceContext(activity, properties);

            var sw = ValueStopwatch.StartNew();
            try
            {
                await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (PublisherConfirmTimeoutException)
            {
                channelFaulted = true;
                _metrics.PublishErrors.Add(1,
                    new(RabbitMqMetrics.TagProducerKey, _producerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagErrorType, nameof(PublisherConfirmTimeoutException)));

                activity?.SetTag(RabbitMqActivitySource.TagErrorType, nameof(PublisherConfirmTimeoutException));
                throw;
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

            RecordPublishSuccess(sw, eventTypeName, routingKey);
        }
        finally
        {
            if (channelFaulted)
            {
                _channelPool.Discard(channel);
                RecordPoolDiscarded();
            }
            else
            {
                _channelPool.Return(channel);
                RecordPoolReturned();
            }
        }
    }

    /// <summary>
    /// Batch publishes using a channel from the pool.
    /// </summary>
    private async Task PublishBatchWithPoolAsync<TEvent>(IEnumerable<TEvent> events, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        var eventList = events as IList<TEvent> ?? [.. events];
        if (eventList.Count == 0) return;

        RecordPoolRented();
        var channel = await _channelPool!.RentAsync(cancellationToken)
            .ConfigureAwait(false);
        bool channelFaulted = false;

        try
        {
            await InitializeChannelAsync(channel, cancellationToken)
                .ConfigureAwait(false);

            var routingKey = routingKeyOverride ?? _options.RoutingKey;
            var (eventTypeName, _) = ResolveEventTypeInfo<TEvent>();
            var publishedCount = 0;

            using var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} publish", ActivityKind.Producer);

            SetPublishActivityTags(activity, routingKey, eventTypeName);

            activity?.SetTag("messaging.batch.message_count", eventList.Count);

            var batchSw = ValueStopwatch.StartNew();

            for (var i = 0; i < eventList.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var (body, properties) = BuildMessage(eventList[i], routingKey);
                InjectTraceContext(activity, properties);

                try
                {
                    await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken).ConfigureAwait(false);
                    publishedCount++;
                }
                catch (PublisherConfirmTimeoutException)
                {
                    channelFaulted = true;
                    _metrics.PublishErrors.Add(1,
                        new(RabbitMqMetrics.TagProducerKey, _producerKey),
                        new(RabbitMqMetrics.TagEventType, eventTypeName),
                        new(RabbitMqMetrics.TagErrorType, nameof(PublisherConfirmTimeoutException)));

                    activity?.SetTag(RabbitMqActivitySource.TagErrorType, nameof(PublisherConfirmTimeoutException));
                    throw;
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
            if (channelFaulted)
            {
                _channelPool.Discard(channel);
                RecordPoolDiscarded();
            }
            else
            {
                _channelPool.Return(channel);
                RecordPoolReturned();
            }
        }
    }

    /// <summary>
    /// Publishes using a new channel per call (backward compatible).
    /// </summary>
    private async Task PublishWithoutPoolAsync<TEvent>(TEvent @event, string? correlationId, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        var channel = await _connection.CreateChannelAsync(_options.EnablePublisherConfirms ? ConfirmChannelOptions : null, cancellationToken: cancellationToken).ConfigureAwait(false);

        try
        {
            await InitializeChannelAsync(channel, cancellationToken)
                .ConfigureAwait(false);

            var routingKey = routingKeyOverride ?? _options.RoutingKey;
            var (eventTypeName, _) = ResolveEventTypeInfo<TEvent>();

            using var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} publish", ActivityKind.Producer);

            SetPublishActivityTags(activity, routingKey, eventTypeName);

            var (body, properties) = BuildMessage(@event, routingKey, correlationId);
            InjectTraceContext(activity, properties);

            var sw = ValueStopwatch.StartNew();
            try
            {
                await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken).ConfigureAwait(false);
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

            RecordPublishSuccess(sw, eventTypeName, routingKey);
        }
        finally
        {
            await SafeCloseChannelAsync(channel).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Batch publishes using a new channel (backward compatible).
    /// </summary>
    private async Task PublishBatchWithoutPoolAsync<TEvent>(IEnumerable<TEvent> events, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        var eventList = events as IList<TEvent> ?? events.ToList();
        if (eventList.Count == 0) return;

        var channel = await _connection.CreateChannelAsync(_options.EnablePublisherConfirms ? ConfirmChannelOptions : null, cancellationToken: cancellationToken).ConfigureAwait(false);

        try
        {
            await InitializeChannelAsync(channel, cancellationToken)
                .ConfigureAwait(false);

            var routingKey = routingKeyOverride ?? _options.RoutingKey;
            var (eventTypeName, _) = ResolveEventTypeInfo<TEvent>();
            var publishedCount = 0;

            using var activity = RabbitMqActivitySource.Source.StartActivity($"{eventTypeName} publish", ActivityKind.Producer);

            SetPublishActivityTags(activity, routingKey, eventTypeName);

            activity?.SetTag("messaging.batch.message_count", eventList.Count);

            var batchSw = ValueStopwatch.StartNew();

            for (var i = 0; i < eventList.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var (body, properties) = BuildMessage(eventList[i], routingKey);
                InjectTraceContext(activity, properties);

                try
                {
                    await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken).ConfigureAwait(false);
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
            await SafeCloseChannelAsync(channel)
                .ConfigureAwait(false);
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
    private async Task ExecutePublishAsync(IChannel channel,
                                           string routingKey,
                                           BasicProperties properties,
                                           ReadOnlyMemory<byte> body,
                                           CancellationToken cancellationToken)
    {
        if (!_options.EnablePublisherConfirms)
        {
            await channel.BasicPublishAsync(
                exchange: _options.ExchangeName,
                routingKey: routingKey,
                mandatory: _options.Mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        var confirmTimeout = TimeSpan.FromMilliseconds(_options.PublishConfirmTimeoutMs);

        using var timeoutCts = new CancellationTokenSource(confirmTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);

        try
        {
            await channel.BasicPublishAsync(
                exchange: _options.ExchangeName,
                routingKey: routingKey,
                mandatory: _options.Mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: linkedCts.Token).ConfigureAwait(false);
        }
        catch (PublishException ex)
        {
            throw new PublisherNackException(_producerKey, ex.PublishSequenceNumber, ex.IsReturn, ex);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new PublisherConfirmTimeoutException(_producerKey, confirmTimeout);
        }
    }

    /// <summary>
    /// Sets common activity tags for a publish operation.
    /// </summary>
    private void SetPublishActivityTags(Activity? activity, string routingKey, string eventTypeName)
    {
        if (activity is null) return;

        activity.SetTag(RabbitMqActivitySource.TagMessagingSystem, RabbitMqActivitySource.SystemRabbitMq);
        activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationKind, RabbitMqActivitySource.DestinationKindExchange);
        activity.SetTag(RabbitMqActivitySource.TagMessagingDestinationName, _options.ExchangeName);
        activity.SetTag(RabbitMqActivitySource.TagMessagingOperation, RabbitMqActivitySource.OperationPublish);
        activity.SetTag(RabbitMqActivitySource.TagMessagingRabbitmqRoutingKey, routingKey);
        activity.SetTag(RabbitMqActivitySource.TagMessagingEventName, eventTypeName);
        activity.SetTag(RabbitMqActivitySource.TagMessagingServiceKey, _producerKey);
    }

    /// <summary>
    /// Records successful publish metrics and logs.
    /// </summary>
    private void RecordPublishSuccess(ValueStopwatch sw, string eventTypeName, string routingKey)
    {
        _metrics.PublishDurationMs.Record(
            sw.GetElapsedMilliseconds(),
            new(RabbitMqMetrics.TagProducerKey, _producerKey),
            new(RabbitMqMetrics.TagEventType, eventTypeName),
            new(RabbitMqMetrics.TagExchange, _options.ExchangeName));

        _metrics.Published.Add(1,
            new(RabbitMqMetrics.TagProducerKey, _producerKey),
            new(RabbitMqMetrics.TagEventType, eventTypeName),
            new(RabbitMqMetrics.TagExchange, _options.ExchangeName));

        _logger.LogDebug("[Producer:{Key}] Published {EventType} → '{Exchange}' [{RoutingKey}]", _producerKey, eventTypeName, _options.ExchangeName, routingKey);
    }


    /// <summary>
    /// Records a channel rental from the pool.
    /// </summary>
    private void RecordPoolRented()
    {
        _metrics.ChannelPoolRented.Add(1, new KeyValuePair<string, object?>(RabbitMqMetrics.TagProducerKey, _producerKey));
    }

    /// <summary>
    /// Records a channel return to the pool.
    /// </summary>
    private void RecordPoolReturned()
    {
        _metrics.ChannelPoolReturned.Add(1, new KeyValuePair<string, object?>(RabbitMqMetrics.TagProducerKey, _producerKey));
    }

    /// <summary>
    /// Records a channel discard from the pool (faulted/dirty channel).
    /// </summary>
    private void RecordPoolDiscarded()
    {
        _metrics.ChannelPoolDiscarded.Add(1, new KeyValuePair<string, object?>(RabbitMqMetrics.TagProducerKey, _producerKey));
    }

    /// <summary>
    /// Closes the channel, ignoring errors if already closed or disposed.
    /// Prevents <see cref="AlreadyClosedException"/> from masking the real publish error.
    /// </summary>
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

    /// <summary>
    /// Declares topology on the channel. Runs on EVERY channel rental (not just the first)
    /// because <c>ExchangeDeclareAsync</c> is idempotent (create-if-not-exists) and cheap.
    /// This guarantees topology exists on every new connection, even after broker restarts
    /// that lose metadata — without needing to subscribe to connection shutdown events or
    /// reset a sticky flag.
    /// </summary>
    private async Task InitializeChannelAsync(IChannel channel, CancellationToken cancellationToken)
    {
        if (_options.AutoDeclareTopology)
        {
            await TopologyDeclarator.DeclareProducerTopologyAsync(channel, _options, _logger, cancellationToken).ConfigureAwait(false);

            if (!_topologyDeclared)
                _topologyDeclared = true;
        }
    }

    /// <summary>
    /// Serializes the event and builds AMQP <see cref="BasicProperties"/> with tracing headers.
    /// Combines both into a single method to ensure the timestamp is consistent
    /// between the AMQP timestamp field and the x-published-at header.
    /// </summary>
    private (ReadOnlyMemory<byte> Body, BasicProperties Properties) BuildMessage<TEvent>(TEvent @event, string routingKey, string? correlationId = null) where TEvent : class
    {

        var now = _timeProvider.GetUtcNow();
        var effectiveCorrelationId = correlationId ?? Guid.NewGuid().ToString();
        var messageId = Guid.NewGuid().ToString();

        var (eventTypeName, eventVersion) = ResolveEventTypeInfo<TEvent>();

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            ContentEncoding = "utf-8",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = messageId,
            CorrelationId = effectiveCorrelationId,
            Timestamp = new AmqpTimestamp(now.ToUnixTimeMilliseconds()),
            Headers = new Dictionary<string, object?>
            {
                [MessageHeaders.CorrelationId] = effectiveCorrelationId,
                [MessageHeaders.MessageId] = messageId,
                [MessageHeaders.PublishedAt] = now.ToString("O"),
                [MessageHeaders.PublisherName] = _producerKey,
                [MessageHeaders.EventType] = eventTypeName,
                [MessageHeaders.EventVersion] = eventVersion.ToString(),
            },
        };
        var body = _serializer.Serialize(@event, eventTypeName, eventVersion);
        return (body, properties);
    }

    /// <summary>
    /// Resolves the event type name and version from the <see cref="EventVersionAttribute"/>.
    /// Results are cached per <see cref="Type"/> — reflection runs only once.
    /// Falls back to <c>Type.FullName</c> and version 1 if no attribute is present.
    /// </summary>
    private static (string EventTypeName, int EventVersion) ResolveEventTypeInfo<TEvent>() where TEvent : class
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
    /// High-performance stopwatch that avoids <see cref="Stopwatch"/>
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
    /// Disposes the channel pool (if pooling is enabled).
    /// Rented channels are NOT closed — their callers must return or discard them.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_channelPool is not null)
            await _channelPool.DisposeAsync().ConfigureAwait(false);
    }
}
