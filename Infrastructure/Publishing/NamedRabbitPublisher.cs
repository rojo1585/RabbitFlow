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
internal sealed class NamedRabbitPublisher(string producerKey,
                                           RabbitProducerOptions options,
                                           ManagedConnection connection,
                                           IMessageSerializer serializer,
                                           ILogger<NamedRabbitPublisher> logger,
                                           TimeProvider? timeProvider = null)
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
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private volatile bool _topologyDeclared;

    /// <summary>
    /// Publishes a single event to the configured exchange.
    /// Creates a new channel per call, declares topology (once), publishes, disposes the channel.
    /// </summary>
    public async Task PublishAsync<TEvent>(TEvent @event, string? routingKeyOverride, CancellationToken cancellationToken) where TEvent : class
    {
        var channel = await connection.CreateChannelAsync(options.EnablePublisherConfirms ? ConfirmChannelOptions : null, cancellationToken: cancellationToken);

        try
        {
            await InitializeChannelAsync(channel, cancellationToken);

            var routingKey = routingKeyOverride ?? options.RoutingKey;
            var (body, properties) = BuildMessage(@event, routingKey);

            using var activity = StartPublishActivity<TEvent>(routingKey);

            await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken);

            logger.LogDebug("[Producer:{Key}] Published {EventType} → '{Exchange}' [{RoutingKey}]", producerKey, typeof(TEvent).FullName, options.ExchangeName, routingKey);
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

        var channel = await connection.CreateChannelAsync(options.EnablePublisherConfirms ? ConfirmChannelOptions : null, cancellationToken: cancellationToken);

        try
        {
            await InitializeChannelAsync(channel, cancellationToken);

            var routingKey = routingKeyOverride ?? options.RoutingKey;
            var publishedCount = 0;

            for (var i = 0; i < eventList.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var (body, properties) = BuildMessage(eventList[i], routingKey);

                await ExecutePublishAsync(channel, routingKey, properties, body, cancellationToken);
                publishedCount++;
            }

            logger.LogInformation("[Producer:{Key}] Batch-published {Count} {EventType} events => '{Exchange}'", producerKey, publishedCount, typeof(TEvent).FullName, options.ExchangeName);
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
        if (!options.EnablePublisherConfirms)
        {
            await channel.BasicPublishAsync(
                exchange: options.ExchangeName,
                routingKey: routingKey,
                mandatory: options.Mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);
            return;
        }

        // With publisher confirmation tracking enabled, BasicPublishAsync internally
        // waits for the broker's ack before returning. We apply a timeout via
        // CancellationToken so the caller doesn't block indefinitely.
        var confirmTimeout = TimeSpan.FromMilliseconds(options.PublishConfirmTimeoutMs);

        using var timeoutCts = new CancellationTokenSource(confirmTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);

        try
        {
            await channel.BasicPublishAsync(
                exchange: options.ExchangeName,
                routingKey: routingKey,
                mandatory: options.Mandatory,
                basicProperties: properties,
                body: body,
                cancellationToken: linkedCts.Token);
        }
        catch (PublishException ex)
        {
            // Broker nacked the message or returned it as unroutable.
            // Wrap in a domain exception so consumers don't need a dependency on RabbitMQ.Client.
            throw new PublisherNackException(producerKey, ex.PublishSequenceNumber, ex.IsReturn, ex);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // Our timeout fired — the broker didn't confirm within the window.
            // The channel is left in a dirty state, but since we create a new channel
            // per publish, it will be closed in the finally block.
            throw new PublisherConfirmTimeoutException(producerKey, confirmTimeout);
        }
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
        if (!_topologyDeclared && options.AutoDeclareTopology)
        {
            await TopologyDeclarator.DeclareProducerTopologyAsync(channel, options, logger, cancellationToken);

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

        // Resolve event type info (cached — no reflection after first call per type)
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
                [MessageHeaders.PublisherName] = producerKey,
                [MessageHeaders.EventType] = eventTypeName,
                [MessageHeaders.EventVersion] = eventVersion.ToString(),
            },
        };

        var body = serializer.Serialize(@event);
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
    /// Starts a System.Diagnostics Activity for distributed tracing.
    /// 
    /// <para>
    /// NOTE: Uses legacy <c>new Activity()</c> API. Phase 11 (Tracing) will replace
    /// with <see cref="ActivitySource"/> for proper OpenTelemetry integration.
    /// </para>
    /// 
    /// <para>
    /// Skips creation when no tracing infrastructure is detected to avoid
    /// unnecessary allocations.
    /// </para>
    /// </summary>
    private static Activity? StartPublishActivity<TEvent>(string routingKey) where TEvent : class
    {
        if (Activity.Current is null && Activity.DefaultIdFormat == ActivityIdFormat.Unknown)
            return null;

        var activity = new Activity($"{typeof(TEvent).FullName} publish");
        activity.SetTag("messaging.system", "rabbitmq");
        activity.SetTag("messaging.destination.kind", "exchange");
        activity.SetTag("messaging.event.name", typeof(TEvent).FullName);
        activity.SetTag("messaging.rabbitmq.routing_key", routingKey);
        activity.Start();
        return activity;
    }
}