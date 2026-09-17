using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Win32;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Topology;
using RabbitFlow.Infrastructure.Versioning;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using System.Threading.RateLimiting;

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
    private static readonly ConcurrentDictionary<Type, Func<object, object, MessageContext, Task>> HandlerInvokers = new();

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
    private readonly RetryPolicy? _retryPolicy;
    private volatile IChannel? _currentChannel;
    private volatile bool _disposed;

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
            _concurrencyLimiter = new SemaphoreSlim(options.MaxConcurrentHandlers);

        if (options.EnableRetry)
            _retryPolicy = new RetryPolicy(options);
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[Consumer:{Key}] Starting consumer loop → queue '{Queue}'", _consumerKey, _options.QueueName);

        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            IChannel? channel = null;
            try
            {
                channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                _currentChannel = channel;

                await InitializeChannelAsync(channel, cancellationToken).ConfigureAwait(false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += OnMessageReceived;

                var consumerTag = await channel.BasicConsumeAsync(
                    queue: _options.QueueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("[Consumer:{Key}] Consuming from '{Queue}' (tag={Tag}, prefetch={Prefetch})", _consumerKey, _options.QueueName, consumerTag, _options.PrefetchCount);

                var shutdownTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

                Task OnShutdown(object sender, ShutdownEventArgs e)
                {
                    consumer.ReceivedAsync -= OnMessageReceived;
                    shutdownTcs.TrySetResult(true);
                    return Task.CompletedTask;
                }

                channel.ChannelShutdownAsync += OnShutdown;

                try
                {
                    using var reg = cancellationToken.Register(() => shutdownTcs.TrySetCanceled());
                    await shutdownTcs.Task.ConfigureAwait(false);
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

    private async Task OnMessageReceived(object sender, BasicDeliverEventArgs ea)
    {
        var body = ea.Body;
        var properties = ea.BasicProperties;
        var deliveryTag = ea.DeliveryTag;

        var eventTypeName = GetHeaderString(properties, MessageHeaders.EventType);
        if (eventTypeName is null)
        {
            _logger.LogWarning("[Consumer:{Key}] Missing '{Header}' header — Nack (deliveryTag={Tag})", _consumerKey, MessageHeaders.EventType, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        var resolved = _registry.Resolve(_consumerKey, eventTypeName);
        if (resolved is null)
        {
            _logger.LogWarning("[Consumer:{Key}] No handler for '{EventType}' — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        var (handlerType, eventType, isBatch) = resolved.Value;

        if (isBatch)
        {
            _logger.LogWarning("[Consumer:{Key}] Handler for '{EventType}' is IBatchRabbitHandler<>, use batch consumer mode — Nack (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
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
                    await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
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
        {
            @event = _serializer.Deserialize(body, eventType);
        }

        if (@event is null)
        {
            _logger.LogError("[Consumer:{Key}] Failed to deserialize '{EventType}' (deliveryTag={Tag})", _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        var retryCount = ExtractRetryCount(properties);
        var deliveryCount = retryCount + 1;
        var context = BuildMessageContext(ea, retryCount);

        var parentContext = ExtractParentContext(properties);

        using var receiveActivity = StartConsumeActivity(eventTypeName, ea, parentContext, RabbitMqActivitySource.OperationReceive);

        if (_concurrencyLimiter is not null)
        {
            await _concurrencyLimiter.WaitAsync().ConfigureAwait(false);
            try
            {
                await InvokeHandlerWithRetryAsync(handlerType, @event, context, deliveryTag, deliveryCount, ea, body, properties, eventTypeName, parentContext).ConfigureAwait(false);
            }
            finally
            {
                _concurrencyLimiter.Release();
            }
        }
        else
        {
            await InvokeHandlerWithRetryAsync(handlerType, @event, context, deliveryTag, deliveryCount, ea, body, properties, eventTypeName, parentContext).ConfigureAwait(false);
        }
    }

    private async Task InvokeHandlerWithRetryAsync(Type handlerType,
                                                   object @event,
                                                   MessageContext context,
                                                   ulong deliveryTag,
                                                   int deliveryCount,
                                                   BasicDeliverEventArgs ea,
                                                   ReadOnlyMemory<byte> body,
                                                   IReadOnlyBasicProperties properties,
                                                   string eventTypeName,
                                                   ActivityContext parentContext)
    {
        using var scope = _scopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetService(handlerType);

        if (handler is null)
        {
            _logger.LogError("[Consumer:{Key}] Handler '{HandlerType}' not in DI — Nack (deliveryTag={Tag})", _consumerKey, handlerType.Name, deliveryTag);
            await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            return;
        }

        using var processActivity = StartConsumeActivity(eventTypeName, ea, parentContext, RabbitMqActivitySource.OperationProcess);

        processActivity?.SetTag(RabbitMqActivitySource.TagMessagingDeliveryAttempt, deliveryCount);

        var handlerSw = ValueStopwatch.StartNew();
        try
        {
            var invoker = HandlerInvokers.GetOrAdd(handlerType, CompileInvoker);
            await invoker(handler, @event, context).ConfigureAwait(false);
            await AckAsync(deliveryTag).ConfigureAwait(false);

            _metrics.ProcessingDurationMs.Record(handlerSw.GetElapsedMilliseconds(),
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));

            _metrics.Consumed.Add(1,
                new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                new(RabbitMqMetrics.TagEventType, eventTypeName),
                new(RabbitMqMetrics.TagQueue, _options.QueueName));
        }
        catch (Exception ex)
        {
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
                await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
            }
            else
            {
                _metrics.DeadLettered.Add(1,
                    new(RabbitMqMetrics.TagConsumerKey, _consumerKey),
                    new(RabbitMqMetrics.TagEventType, eventTypeName),
                    new(RabbitMqMetrics.TagQueue, _options.QueueName),
                    new(RabbitMqMetrics.TagDlqName, _options.ResolvedDlqName));

                _logger.LogWarning("[Consumer:{Key}] Retries exhausted ({Attempts}) — dead-lettering (deliveryTag={Tag})", _consumerKey, deliveryCount, deliveryTag);
                await NackAsync(deliveryTag, requeue: false).ConfigureAwait(false);
                await DeadLetterMessageAsync(ea, body, properties, ex, deliveryCount).ConfigureAwait(false);
            }
        }
    }

    private bool ShouldRetry(int deliveryCount)
    {
        return _retryPolicy is not null && _retryPolicy.ShouldRetry(deliveryCount);
    }

    private async Task DeadLetterMessageAsync(BasicDeliverEventArgs ea, ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties, Exception handlerException, int deliveryCount)
    {
        if (_options.EnableDeadLetter)
        {
            try
            {
                var dlqChannel = await _connection.CreateChannelAsync().ConfigureAwait(false);
                try
                {
                    await dlqChannel.BasicPublishAsync(
                        exchange: string.Empty,
                        routingKey: _options.ResolvedDlqName,
                        mandatory: false,
                        basicProperties: (BasicProperties)properties,
                        body: body).ConfigureAwait(false);

                    _logger.LogInformation("[Consumer:{Key}] Dead-lettered to '{Dlq}' (deliveryTag={Tag})", _consumerKey, _options.ResolvedDlqName, ea.DeliveryTag);
                }
                finally
                {
                    await SafeCloseChannelAsync(dlqChannel).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Consumer:{Key}] Failed to dead-letter to '{Dlq}' (deliveryTag={Tag})", _consumerKey, _options.ResolvedDlqName, ea.DeliveryTag);
            }
        }

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
        if (properties.Headers is null || !properties.Headers.TryGetValue("x-death", out var value))
            return 0;

        try
        {
            var deathEntries = value switch
            {
                System.Collections.IList list => list,
                _ => null
            };

            if (deathEntries is null) return 0;

            var totalDeaths = 0;
            foreach (var entry in deathEntries)
            {
                if (entry is not System.Collections.IDictionary dict) continue;

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
            _logger.LogDebug(ex, "[Consumer:{Key}] Failed to parse x-death header, assuming first delivery", _consumerKey);
            return 0;
        }
    }

    private static Func<object, object, MessageContext, Task> CompileInvoker(Type handlerType)
    {
        var handlerInterface = handlerType.GetInterfaces()
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRabbitHandler<>));

        var eventType = handlerInterface.GetGenericArguments()[0];
        var handleMethod = typeof(IRabbitHandler<>).MakeGenericType(eventType)
            .GetMethod(nameof(IRabbitHandler<IIntegrationEvent>.HandleAsync))!;

        var hParam = Expression.Parameter(typeof(object), "h");
        var eParam = Expression.Parameter(typeof(object), "e");
        var cParam = Expression.Parameter(typeof(MessageContext), "c");

        var call = Expression.Call(Expression.Convert(hParam, handlerInterface),
            handleMethod,
            Expression.Convert(eParam, eventType),
            cParam);

        return Expression.Lambda<Func<object, object, MessageContext, Task>>(call, hParam, eParam, cParam).Compile();
    }

    private async Task AckAsync(ulong deliveryTag)
    {
        var channel = _currentChannel;
        if (channel is null) return;

        try
        {
            await channel.BasicAckAsync(deliveryTag: deliveryTag, multiple: false).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[Consumer:{Key}] Ack failed (deliveryTag={Tag}, channel likely closed)", _consumerKey, deliveryTag);
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
    /// Falls back to <see cref="Activity.Current?.Context"/> if no header is present.
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

    public ValueTask DisposeAsync()
    {
        if (_disposed) return default;
        _disposed = true;
        _concurrencyLimiter?.Dispose();
        return default;
    }
}