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
using System.Collections.Concurrent;
using System.Collections.Generic;
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
/// Message processing flow:
/// <list type="number">
///   <item>Receive message from RabbitMQ (manual ack mode).</item>
///   <item>Extract <c>x-event-type</c> header → resolve handler type from registry.</item>
///   <item>Extract retry count from <c>x-death</c> header (RabbitMQ-managed).</item>
///   <item>Deserialize body to the resolved event type.</item>
///   <item>Create a fresh DI scope, resolve the handler, call <see cref="IRabbitHandler{T}.HandleAsync"/>.</item>
///   <item>On success: Ack. On failure: check retry policy → Nack (retry) or dead-letter.</item>
/// </list>
/// </para>
///
/// <para>
/// Retry/dead-letter strategy:
/// <list type="bullet">
///   <item>If retries remain and retry is enabled: Nack with requeue=false.
///     RabbitMQ routes the message to the DLX → retry queue (TTL) → back to main queue.</item>
///   <item>If retries exhausted or retry is disabled: Nack with requeue=false.
///     The consumer publishes the original message to the DLQ and invokes
///     <see cref="IDeadLetterHandler"/> if registered.</item>
/// </list>
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
    /// <summary>
    /// Caches compiled expression-tree delegates for invoking
    /// <c>IRabbitHandler&lt;T&gt;.HandleAsync</c> without reflection per call.
    /// Key: handler concrete type.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, Func<object, object, MessageContext, Task>>
        HandlerInvokers = new();

    private readonly string _consumerKey;
    private readonly RabbitConsumerOptions _options;
    private readonly ManagedConnection _connection;
    private readonly HandlerTypeRegistry _registry;
    private readonly IMessageSerializer _serializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<NamedRabbitConsumer> _logger;
    private readonly SemaphoreSlim? _concurrencyLimiter;
    private readonly RetryPolicy? _retryPolicy;

    /// <summary>
    /// The current AMQP channel. Written at the start of each consume loop iteration,
    /// read by the message handler for ack/nack. Volatile for visibility across threads.
    /// </summary>
    private volatile IChannel? _currentChannel;
    private volatile bool _disposed;

    public NamedRabbitConsumer(
        string consumerKey,
        RabbitConsumerOptions options,
        ManagedConnection connection,
        HandlerTypeRegistry registry,
        IMessageSerializer serializer,
        IServiceScopeFactory scopeFactory,
        ILogger<NamedRabbitConsumer> logger)
    {
        _consumerKey = consumerKey;
        _options = options;
        _connection = connection;
        _registry = registry;
        _serializer = serializer;
        _scopeFactory = scopeFactory;
        _logger = logger;

        if (options.MaxConcurrentHandlers > 0)
        {
            _concurrencyLimiter = new SemaphoreSlim(options.MaxConcurrentHandlers);
        }

        if (options.EnableRetry)
        {
            _retryPolicy = new RetryPolicy(options);
        }
    }

    /// <summary>
    /// Main consumer loop. Creates a channel, starts consuming, and reconnects
    /// if the channel or connection drops. Runs until <paramref name="cancellationToken"/>
    /// is cancelled.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[Consumer:{Key}] Starting consumer loop → queue '{Queue}'", _consumerKey, _options.QueueName);

        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            IChannel? channel = null;
            try
            {
                channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
                _currentChannel = channel;

                await InitializeChannelAsync(channel, cancellationToken);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += OnMessageReceived;

                var consumerTag = await channel.BasicConsumeAsync(
                    queue: _options.QueueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken);

                _logger.LogInformation("[Consumer:{Key}] Consuming from '{Queue}' (tag={Tag}, prefetch={Prefetch})", _consumerKey, _options.QueueName, consumerTag, _options.PrefetchCount);

                // Wait for the channel to close (connection loss, broker shutdown, etc.)
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
                    await shutdownTcs.Task;
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
                _logger.LogWarning(ex,
                    "[Consumer:{Key}] Channel/consume error, reconnecting in 2s...",
                    _consumerKey);

                try
                {
                    await Task.Delay(2000, cancellationToken);
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
                {
                    await SafeCloseChannelAsync(channel);
                }
            }
        }
    }
    /// <summary>
    /// Handles an incoming message from RabbitMQ.
    /// Dispatches to the registered handler via a DI scope.
    /// Implements retry/dead-letter logic based on <see cref="RetryPolicy"/>.
    /// </summary>
    private async Task OnMessageReceived(object sender, BasicDeliverEventArgs ea)
    {
        var body = ea.Body;
        var properties = ea.BasicProperties;
        var deliveryTag = ea.DeliveryTag;

        // 1. Extract event type from headers
        var eventTypeName = GetHeaderString(properties, MessageHeaders.EventType);
        if (eventTypeName is null)
        {
            _logger.LogWarning(
                "[Consumer:{Key}] Missing '{Header}' header — Nack (deliveryTag={Tag})",
                _consumerKey, MessageHeaders.EventType, deliveryTag);
            await NackAsync(deliveryTag, requeue: false);
            return;
        }

        // 2. Resolve handler type from registry
        var resolved = _registry.Resolve(_consumerKey, eventTypeName);
        if (resolved is null)
        {
            _logger.LogWarning(
                "[Consumer:{Key}] No handler registered for event type '{EventType}' — Nack (deliveryTag={Tag})",
                _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false);
            return;
        }

        var (handlerType, eventType) = resolved.Value;

        // 3. Deserialize the event body
        var @event = _serializer.Deserialize(body, eventType);
        if (@event is null)
        {
            _logger.LogError(
                "[Consumer:{Key}] Failed to deserialize '{EventType}' (deliveryTag={Tag}) — Nack",
                _consumerKey, eventTypeName, deliveryTag);
            await NackAsync(deliveryTag, requeue: false);
            return;
        }

        // 4. Extract retry count from x-death header (RabbitMQ-managed)
        var retryCount = ExtractRetryCount(properties);
        var deliveryCount = retryCount + 1;

        // 5. Build MessageContext from AMQP headers
        var context = BuildMessageContext(ea, retryCount);

        // 6. Invoke handler within concurrency limiter (if configured)
        if (_concurrencyLimiter is not null)
        {
            await _concurrencyLimiter.WaitAsync();
            try
            {
                await InvokeHandlerWithRetryAsync(
                    handlerType, @event, context, deliveryTag, deliveryCount,
                    ea, body, properties);
            }
            finally
            {
                _concurrencyLimiter.Release();
            }
        }
        else
        {
            await InvokeHandlerWithRetryAsync(
                handlerType, @event, context, deliveryTag, deliveryCount,
                ea, body, properties);
        }
    }

    /// <summary>
    /// Invokes the handler and applies retry/dead-letter logic on failure.
    /// </summary>
    private async Task InvokeHandlerWithRetryAsync(
        Type handlerType,
        object @event,
        MessageContext context,
        ulong deliveryTag,
        int deliveryCount,
        BasicDeliverEventArgs ea,
        ReadOnlyMemory<byte> body,
        IReadOnlyBasicProperties properties)
    {
        using var scope = _scopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetService(handlerType);

        if (handler is null)
        {
            _logger.LogError(
                "[Consumer:{Key}] Handler '{HandlerType}' not registered in DI — Nack (deliveryTag={Tag})",
                _consumerKey, handlerType.Name, deliveryTag);
            await NackAsync(deliveryTag, requeue: false);
            return;
        }

        try
        {
            var invoker = HandlerInvokers.GetOrAdd(handlerType, CompileInvoker);
            await invoker(handler, @event, context);

            // Handler succeeded — acknowledge the message
            await AckAsync(deliveryTag);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "[Consumer:{Key}] Handler '{HandlerType}' failed on attempt {Attempt}/{MaxRetries} (deliveryTag={Tag})",
                _consumerKey, handlerType.Name, deliveryCount,
                _retryPolicy?.MaxRetries ?? 1, deliveryTag);

            if (ShouldRetry(deliveryCount))
            {
                // Nack with requeue=false → message goes to DLX → retry queue → back to main queue
                _logger.LogInformation(
                    "[Consumer:{Key}] Retrying message (attempt {Attempt}, deliveryTag={Tag})",
                    _consumerKey, deliveryCount, deliveryTag);

                await NackAsync(deliveryTag, requeue: false);
            }
            else
            {
                // Retries exhausted — dead-letter the message
                _logger.LogWarning(
                    "[Consumer:{Key}] Retries exhausted ({Attempts} attempts, deliveryTag={Tag}) — dead-lettering",
                    _consumerKey, deliveryCount, deliveryTag);

                await NackAsync(deliveryTag, requeue: false);
                await DeadLetterMessageAsync(ea, body, properties, ex, deliveryCount);
            }
        }
    }

    /// <summary>
    /// Determines whether a message should be retried based on the retry policy.
    /// </summary>
    private bool ShouldRetry(int deliveryCount)
    {
        if (_retryPolicy is null) return false;
        return _retryPolicy.ShouldRetry(deliveryCount);
    }

    /// <summary>
    /// Sends the dead-lettered message to the DLQ and invokes the <see cref="IDeadLetterHandler"/>.
    /// </summary>
    private async Task DeadLetterMessageAsync(
        BasicDeliverEventArgs ea,
        ReadOnlyMemory<byte> body,
        IReadOnlyBasicProperties properties,
        Exception handlerException,
        int deliveryCount)
    {
        // 1. Publish the original message to the DLQ
        if (_options.EnableDeadLetter)
        {
            try
            {
                var dlqChannel = await _connection.CreateChannelAsync();
                try
                {
                    await dlqChannel.BasicPublishAsync(
                        exchange: string.Empty,
                        routingKey: _options.ResolvedDlqName,
                        mandatory: false,
                        basicProperties: (BasicProperties)properties,
                        body: body);

                    _logger.LogInformation(
                        "[Consumer:{Key}] Published dead-lettered message to '{Dlq}' (deliveryTag={Tag})",
                        _consumerKey, _options.ResolvedDlqName, ea.DeliveryTag);
                }
                finally
                {
                    await SafeCloseChannelAsync(dlqChannel);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "[Consumer:{Key}] Failed to publish dead-lettered message to '{Dlq}' (deliveryTag={Tag})",
                    _consumerKey, _options.ResolvedDlqName, ea.DeliveryTag);
            }
        }

        // 2. Invoke IDeadLetterHandler if registered
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

                await dlHandler.HandleAsync(deadLetterMsg);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "[Consumer:{Key}] IDeadLetterHandler threw exception (deliveryTag={Tag})",
                _consumerKey, ea.DeliveryTag);
        }
    }

    /// <summary>
    /// Extracts the retry count from the <c>x-death</c> AMQP header.
    /// RabbitMQ automatically populates this header when a message is dead-lettered.
    /// </summary>
    /// <param name="properties">The message properties containing headers.</param>
    /// <returns>
    /// The number of times the message has been dead-lettered (0 = first delivery).
    /// </returns>
    /// <remarks>
    /// The x-death header is a list of death entries. Each entry contains:
    /// <c>count</c> (times through this queue), <c>exchange</c>, <c>queue</c>, <c>reason</c>, etc.
    /// We sum the count of all entries whose queue matches our main queue or any of our retry queues.
    /// </remarks>
    private int ExtractRetryCount(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null || !properties.Headers.TryGetValue("x-death", out var value))
            return 0;

        try
        {
            var deathEntries = value switch
            {
                // RabbitMQ .NET client may return x-death as List<object?> or as a structured type
                System.Collections.IList list => list,
                _ => null
            };

            if (deathEntries is null) return 0;

            var totalDeaths = 0;

            foreach (var entry in deathEntries)
            {
                if (entry is not System.Collections.IDictionary dict) continue;

                // Only count deaths from our own queues (main + retry)
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
            _logger.LogDebug(ex,
                "[Consumer:{Key}] Failed to parse x-death header, assuming first delivery",
                _consumerKey);
            return 0;
        }
    }

    /// <summary>
    /// Compiles an expression-tree delegate that calls
    /// <c>IRabbitHandler&lt;T&gt;.HandleAsync(T, MessageContext)</c>
    /// without reflection overhead.
    /// </summary>
    private static Func<object, object, MessageContext, Task> CompileInvoker(Type handlerType)
    {
        var handlerInterface = handlerType.GetInterfaces()
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRabbitHandler<>));

        var eventType = handlerInterface.GetGenericArguments()[0];
        var handleMethod = typeof(IRabbitHandler<>)
            .MakeGenericType(eventType)
            .GetMethod(nameof(IRabbitHandler<IIntegrationEvent>.HandleAsync))!;

        // Build: (object h, object e, MessageContext c) => ((IRabbitHandler<T>)h).HandleAsync((T)e, c)
        var hParam = Expression.Parameter(typeof(object), "h");
        var eParam = Expression.Parameter(typeof(object), "e");
        var cParam = Expression.Parameter(typeof(MessageContext), "c");

        var call = Expression.Call(
            Expression.Convert(hParam, handlerInterface),
            handleMethod,
            Expression.Convert(eParam, eventType),
            cParam);

        return Expression.Lambda<Func<object, object, MessageContext, Task>>(
            call, hParam, eParam, cParam).Compile();
    }

    /// <summary>
    /// Acknowledges a message. Errors are logged but not propagated —
    /// the channel may have closed between delivery and ack.
    /// </summary>
    private async Task AckAsync(ulong deliveryTag)
    {
        var channel = _currentChannel;
        if (channel is null) return;

        try
        {
            await channel.BasicAckAsync(deliveryTag: deliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex,
                "[Consumer:{Key}] Ack failed for deliveryTag={Tag} (channel likely closed)",
                _consumerKey, deliveryTag);
        }
    }

    /// <summary>
    /// Negative-acknowledges a message. With <paramref name="requeue"/>=false,
    /// the message is dead-lettered (routed to DLX/retry queue by topology).
    /// </summary>
    private async Task NackAsync(ulong deliveryTag, bool requeue)
    {
        var channel = _currentChannel;
        if (channel is null) return;

        try
        {
            await channel.BasicNackAsync(deliveryTag: deliveryTag, multiple: false, requeue: requeue);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex,
                "[Consumer:{Key}] Nack failed for deliveryTag={Tag} (channel likely closed)",
                _consumerKey, deliveryTag);
        }
    }

    /// <summary>
    /// Sets prefetch count and declares consumer topology (exchange, queue, bindings, DLX).
    /// </summary>
    private async Task InitializeChannelAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: _options.PrefetchCount,
            global: false,
            cancellationToken: cancellationToken);

        await TopologyDeclarator.DeclareConsumerTopologyAsync(
            channel, _options, _logger, cancellationToken);
    }

    /// <summary>
    /// Builds a <see cref="MessageContext"/> from AMQP delivery properties and headers.
    /// </summary>
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

    /// <summary>
    /// Reads a string header value. Returns <c>null</c> if missing or not a string.
    /// Handles both <c>string</c> and <c>byte[]</c> (RabbitMQ encodes strings as UTF-8 bytes).
    /// </summary>
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

    /// <summary>
    /// Reads a DateTime header value. Returns <c>null</c> if missing or unparseable.
    /// </summary>
    private static DateTime? ParseHeaderDateTime(IReadOnlyBasicProperties properties, string key)
    {
        var str = GetHeaderString(properties, key);
        return str is not null && DateTime.TryParse(str, out var dt) ? dt : null;
    }

    /// <summary>
    /// Extracts all custom headers as a dictionary.
    /// Filters out internal headers (x- prefixed).
    /// </summary>
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
                byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
                ReadOnlyMemory<byte> rom => System.Text.Encoding.UTF8.GetString(rom.Span),
                _ => value?.ToString(),
            };

            if (strValue is not null)
                result[key] = strValue;
        }

        return result;
    }

    /// <summary>
    /// Closes the channel, ignoring errors if already closed.
    /// </summary>
    private static async Task SafeCloseChannelAsync(IChannel channel)
    {
        try
        {
            await channel.CloseAsync();
        }
        catch (AlreadyClosedException) { }
        catch (ObjectDisposedException) { }
        catch { }
    }

    /// <summary>
    /// Disposes the concurrency limiter semaphore.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _concurrencyLimiter?.Dispose();
    }
}
