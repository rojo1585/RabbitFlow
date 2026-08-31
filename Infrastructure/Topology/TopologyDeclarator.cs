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
    public static async Task DeclareProducerTopologyAsync(IChannel channel, RabbitProducerOptions options, ILogger logger, CancellationToken cancellationToken = default)
    {
        if (!options.AutoDeclareTopology) return;

        await channel.ExchangeDeclareAsync(
            exchange: options.ExchangeName,
            type: options.ExchangeType,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        logger.LogInformation("[Producer:{Key}] Declared exchange '{Exchange}' ({Type})", options.ServiceKey, options.ExchangeName, options.ExchangeType);
    }

    /// <summary>
    /// Declares consumer topology: exchange, queue, binding,
    /// and optionally DLX with retry queues.
    /// </summary>
    public static async Task DeclareConsumerTopologyAsync(IChannel channel, RabbitConsumerOptions options, ILogger logger, CancellationToken cancellationToken = default)
    {
        if (!options.AutoDeclareTopology) return;

        // 1. Main exchange
        await channel.ExchangeDeclareAsync(
            exchange: options.ExchangeName,
            type: options.ExchangeType,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        logger.LogInformation("[Consumer:{Key}] Declared exchange '{Exchange}' ({Type})", options.ServiceKey, options.ExchangeName, options.ExchangeType);

        // 2. Retry topology (before main queue because main queue may reference DLX)
        if (options.EnableRetry)
            await DeclareRetryTopologyAsync(channel, options, logger, cancellationToken);

        // 3. Dead letter topology (DLQ — not bound to DLX, consumer publishes directly)
        if (options.EnableDeadLetter)
            await DeclareDeadLetterQueueAsync(channel, options, logger, cancellationToken);

        // 4. Main queue (with x-dead-letter-exchange pointing to DLX for retry)
        var queueArgs = new Dictionary<string, object?>();

        if (options.EnableRetry)
            // NACKed messages go to the retry DLX for delay-based re-delivery.
            // The consumer decides whether to retry or dead-letter based on x-death count.
            queueArgs["x-dead-letter-exchange"] = options.ResolvedDlxName;

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

        logger.LogInformation("[Consumer:{Key}] Declared queue '{Queue}' bound to '{Exchange}' with key '{RoutingKey}'", options.ServiceKey, options.QueueName, options.ExchangeName, options.RoutingKey);
    }

    /// <summary>
    /// Declares the DLX exchange (direct) and retry queues with per-queue TTLs.
    /// 
    /// <para>
    /// The DLX use <c>direct</c> type (NOT fanout) so that the consumer can control
    /// routing. When the consumer NACKs, the message goes to the DLX with the main
    /// queue's routing key. The retry queues are bound to the DLX with that same key,
    /// so the message enters the retry queue chain.
    /// </para>
    /// 
    /// <para>
    /// IMPORTANT: The DLQ is NOT bound to this DLX. When retries are exhausted,
    /// the consumer publishes the dead-lettered message directly to the DLQ exchange
    /// or uses a separate publishing path. This prevents exhausted messages from
    /// entering retry queues.
    /// </para>
    /// 
    /// <para>
    /// Topology when EnableRetry=true, MaxRetries=3, Delays=[5s, 30s]:
    /// <code>
    /// Main Queue (x-dead-letter-exchange: DLX)
    ///     ↓ NACK (requeue=false)
    /// DLX Exchange: "{QueueName}.dlx" (direct)
    ///     ↓ routing key = main routing key
    /// Retry Queue 1: "{QueueName}.retry.5s" (TTL=5s)
    ///     └── x-dead-letter-exchange: main exchange
    ///     └── x-dead-letter-routing-key: main routing key
    ///     ↓ TTL expires → Main Exchange → Main Queue (attempt 2)
    /// Retry Queue 2: "{QueueName}.retry.30s" (TTL=30s)
    ///     └── x-dead-letter-exchange: main exchange
    ///     └── x-dead-letter-routing-key: main routing key
    ///     ↓ TTL expires → Main Exchange → Main Queue (attempt 3)
    /// 
    /// DLQ: "{QueueName}.dlq" (NOT bound to DLX — consumer publishes directly)
    /// </code>
    /// </para>
    /// </summary>
    private static async Task DeclareRetryTopologyAsync(IChannel channel, RabbitConsumerOptions options, ILogger logger, CancellationToken cancellationToken)
    {
        var dlxName = options.ResolvedDlxName;
        var delays = options.ResolvedRetryDelays;

        // 1. DLX Exchange (direct — routes by key)
        await channel.ExchangeDeclareAsync(
            exchange: dlxName,
            type: "direct",
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        // 2. Retry queues (one per non-zero delay)
        var retryCount = 0;
        for (int i = 0; i < delays.Length; i++)
        {
            var delay = delays[i];

            // Skip zero-delay entries: the consumer handles immediate retry
            // by NACKing with requeue=true for the first failure.
            if (delay <= TimeSpan.Zero)
                continue;

            retryCount++;
            var retryQueueName = $"{options.QueueName}.retry.{(long)delay.TotalSeconds}s";

            var retryArgs = new Dictionary<string, object?>
            {
                // When TTL expires, message goes back to the main exchange
                ["x-dead-letter-exchange"] = options.ExchangeName,
                ["x-dead-letter-routing-key"] = options.RoutingKey,
                ["x-message-ttl"] = (int)delay.TotalMilliseconds,
            };

            await channel.QueueDeclareAsync(
                queue: retryQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: retryArgs,
                cancellationToken: cancellationToken);

            // Bind retry queue to DLX using the MAIN routing key.
            // When a message is NACKed from the main queue, RabbitMQ routes it
            // to the DLX with the message's original routing key (or the queue's
            // x-dead-letter-routing-key if set). We use the main routing key
            // so that all retry queues receive the dead-lettered message.
            await channel.QueueBindAsync(
                queue: retryQueueName,
                exchange: dlxName,
                routingKey: options.RoutingKey,
                cancellationToken: cancellationToken);

            logger.LogInformation("[Consumer:{Key}] Declared retry queue '{RetryQueue}' (TTL={TTL}ms)", options.ServiceKey, retryQueueName, (int)delay.TotalMilliseconds);
        }

        logger.LogInformation("[Consumer:{Key}] Declared DLX '{Dlx}' (direct) with {Count} retry queue(s)", options.ServiceKey, dlxName, retryCount);
    }

    /// <summary>
    /// Declares the dead-letter queue (DLQ) as a standalone queue.
    /// 
    /// <para>
    /// The DLQ is NOT bound to the DLX exchange. This prevents exhausted messages
    /// from accidentally entering retry queues. When the consumer determines that
    /// retries are exhausted, it publishes the original message directly to this queue
    /// or invokes the <see cref="Abstractions.IDeadLetterHandler"/>.
    /// </para>
    /// 
    /// <para>
    /// When retry is disabled but dead-letter is enabled, the main queue's
    /// x-dead-letter-exchange is NOT set (see <see cref="DeclareConsumerTopologyAsync"/>).
    /// In that case, the consumer publishes dead-lettered messages directly to the DLQ.
    /// </para>
    /// </summary>
    private static async Task DeclareDeadLetterQueueAsync(IChannel channel, RabbitConsumerOptions options, ILogger logger, CancellationToken cancellationToken)
    {
        var dlqName = options.ResolvedDlqName;

        // DLQ: simple durable queue, not bound to any exchange.
        // The consumer publishes dead-lettered messages here directly.
        await channel.QueueDeclareAsync(
            queue: dlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        logger.LogInformation("[Consumer:{Key}] Declared DLQ '{Dlq}' (standalone, not bound to DLX)", options.ServiceKey, dlqName);
    }
}