using Microsoft.Extensions.Logging;
using RabbitFlow.Configuration;
using RabbitMQ.Client;

namespace RabbitFlow.Infrastructure.Topology;


/// <summary>
/// Declares AMQP topology (exchanges, queues, bindings, DLX, retry queues)
/// on a channel. Called at startup by producers and consumers.
/// 
/// <para>
/// All declarations use passive=false (create-if-not-exists).
/// If the resource already exists with different parameters,
/// a precondition failed exception is thrown.
/// </para>
/// </summary>
internal static class TopologyDeclarator
{
    /// <summary>
    /// Declares producer topology: exchange only.
    /// Queues are not needed for producers.
    /// </summary>
    public static async Task DeclareProducerTopologyAsync(
        IChannel channel,
        RabbitProducerOptions options,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        if (!options.AutoDeclareTopology) return;

        await channel.ExchangeDeclareAsync(
            exchange: options.ExchangeName,
            type: options.ExchangeType,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        logger.LogInformation(
            "[Producer:{Key}] Declared exchange '{Exchange}' ({Type})",
            options.ServiceKey, options.ExchangeName, options.ExchangeType);
    }

    /// <summary>
    /// Declares consumer topology: exchange, queue, binding,
    /// and optionally DLX with retry queues.
    /// </summary>
    public static async Task DeclareConsumerTopologyAsync(
        IChannel channel,
        RabbitConsumerOptions options,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        if (!options.AutoDeclareTopology) return;

        // 1. Main exchange
        await channel.ExchangeDeclareAsync(
            exchange: options.ExchangeName,
            type: options.ExchangeType,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        logger.LogInformation(
            "[Consumer:{Key}] Declared exchange '{Exchange}' ({Type})",
            options.ServiceKey, options.ExchangeName, options.ExchangeType);

        // 2. Retry topology (before main queue, because main queue references DLX)
        if (options.EnableRetry)
        {
            await DeclareRetryTopologyAsync(channel, options, logger, cancellationToken);
        }

        // 3. Dead letter queue (standalone, NOT bound to DLX)
        if (options.EnableDeadLetter)
        {
            await DeclareDeadLetterQueueAsync(channel, options, logger, cancellationToken);
        }

        // 4. Main queue (with x-dead-letter-exchange pointing to DLX for retry)
        var queueArgs = new Dictionary<string, object?>();

        if (options.EnableRetry)
        {
            // NACKed messages go to the retry DLX for delay-based re-delivery.
            queueArgs["x-dead-letter-exchange"] = options.ResolvedDlxName;
        }

        await channel.QueueDeclareAsync(
            queue: options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs,
            cancellationToken: cancellationToken);

        // 5. Bind queue to exchange
        await channel.QueueBindAsync(
            queue: options.QueueName,
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey,
            cancellationToken: cancellationToken);

        logger.LogInformation(
            "[Consumer:{Key}] Declared queue '{Queue}' bound to '{Exchange}' with key '{RoutingKey}'",
            options.ServiceKey, options.QueueName, options.ExchangeName, options.RoutingKey);
    }

    /// <summary>
    /// Declares the DLX exchange (direct) and a SINGLE retry queue with a TTL.
    /// 
    /// <para>
    /// Design: Only ONE retry queue is bound to the DLX. All failed messages
    /// flow through this single queue regardless of attempt count. The consumer
    /// tracks retry count via the <c>x-death</c> header and decides when to
    /// stop retrying and dead-letter instead.
    /// </para>
    /// 
    /// <para>
    /// Why not multiple retry queues with different TTLs?
    /// With a DLX, you cannot route to different queues based on retry count —
    /// RabbitMQ always sends dead-lettered messages to the same DLX.
    /// Binding multiple queues to the same DLX with the same routing key would
    /// cause duplicate deliveries. Per-attempt delay requires either the
    /// <c>rabbitmq_delayed_message_exchange</c> plugin or consumer-controlled routing.
    /// </para>
    /// 
    /// <para>
    /// The retry delay is taken from the first non-zero entry in <see cref="RabbitConsumerOptions.ResolvedRetryDelays"/>.
    /// If all delays are zero, a 5-second default is used.
    /// </para>
    /// 
    /// <para>
    /// Topology:
    /// <code>
    /// Main Queue (x-dead-letter-exchange: DLX)
    ///     ↓ NACK (requeue=false)
    /// DLX Exchange: "{QueueName}.dlx" (direct)
    ///     ↓ routing key = main routing key
    /// Retry Queue: "{QueueName}.retry" (TTL=configured delay)
    ///     └── x-dead-letter-exchange: main exchange
    ///     └── x-dead-letter-routing-key: main routing key
    ///     ↓ TTL expires → Main Exchange → Main Queue (next attempt)
    /// 
    /// DLQ: "{QueueName}.dlq" (standalone — consumer publishes directly)
    /// </code>
    /// </para>
    /// </summary>
    private static async Task DeclareRetryTopologyAsync(
        IChannel channel,
        RabbitConsumerOptions options,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var dlxName = options.ResolvedDlxName;
        var delays = options.ResolvedRetryDelays;

        // Pick the first non-zero delay, default to 5 seconds
        var retryDelay = delays.FirstOrDefault(d => d > TimeSpan.Zero);
        if (retryDelay == TimeSpan.Zero)
            retryDelay = TimeSpan.FromSeconds(5);

        var retryQueueName = $"{options.QueueName}.retry";

        // 1. DLX Exchange (direct — routes by key)
        await channel.ExchangeDeclareAsync(
            exchange: dlxName,
            type: "direct",
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        // 2. Single retry queue with TTL
        var retryArgs = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = options.ExchangeName,
            ["x-dead-letter-routing-key"] = options.RoutingKey,
            ["x-message-ttl"] = (int)retryDelay.TotalMilliseconds,
        };

        await channel.QueueDeclareAsync(
            queue: retryQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryArgs,
            cancellationToken: cancellationToken);

        // 3. Bind retry queue to DLX with the main routing key
        await channel.QueueBindAsync(
            queue: retryQueueName,
            exchange: dlxName,
            routingKey: options.RoutingKey,
            cancellationToken: cancellationToken);

        logger.LogInformation(
            "[Consumer:{Key}] Declared DLX '{Dlx}' (direct) → retry queue '{RetryQueue}' (TTL={TTL}ms)",
            options.ServiceKey, dlxName, retryQueueName, (int)retryDelay.TotalMilliseconds);
    }

    /// <summary>
    /// Declares the dead-letter queue (DLQ) as a standalone queue.
    /// 
    /// <para>
    /// The DLQ is NOT bound to the DLX exchange. This prevents exhausted messages
    /// from entering the retry queue. When the consumer determines that
    /// retries are exhausted, it publishes the original message directly to this queue
    /// and invokes the <see cref="Abstractions.IDeadLetterHandler"/>.
    /// </para>
    /// </summary>
    private static async Task DeclareDeadLetterQueueAsync(
        IChannel channel,
        RabbitConsumerOptions options,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var dlqName = options.ResolvedDlqName;

        await channel.QueueDeclareAsync(
            queue: dlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        logger.LogInformation(
            "[Consumer:{Key}] Declared DLQ '{Dlq}' (standalone, not bound to DLX)",
            options.ServiceKey, dlqName);
    }
}