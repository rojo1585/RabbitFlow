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
    /// Routing key used to bind the dead-letter queue (DLQ) to the dead-letter
    /// exchange (DLX). The main queue sets <c>x-dead-letter-routing-key</c> to
    /// this value so that dead-lettered messages are routed exclusively to the
    /// DLQ and never to any retry queue.
    /// </summary>
    public const string DeadLetterRoutingKey = "dead";

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
            cancellationToken: cancellationToken).ConfigureAwait(false);

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
            cancellationToken: cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[Consumer:{Key}] Declared exchange '{Exchange}' ({Type})", options.ServiceKey, options.ExchangeName, options.ExchangeType);

        // 2. Dead letter topology (before main queue, because main queue references DLX)
        if (options.EnableDeadLetter || options.EnableRetry)
            await DeclareDeadLetterTopologyAsync(channel, options, logger, cancellationToken).ConfigureAwait(false);

        // 3. Main queue (with x-dead-letter-exchange pointing to DLX)
        var queueArgs = new Dictionary<string, object?>();

        if (options.EnableDeadLetter || options.EnableRetry)
        {
            // DLX is a "direct" exchange; messages dead-lettered from the main queue
            // are routed with the "dead" routing key, which only the DLQ is bound to.
            // This ensures that NACKed messages reach the DLQ exactly once. Retry
            // routing is performed by the client (publish to retry queue), NOT by the
            // DLX, which previously caused exponential message amplification under a
            // fanout DLX bound to both the DLQ and retry queues.
            queueArgs["x-dead-letter-exchange"] = options.ResolvedDlxName;
            queueArgs["x-dead-letter-routing-key"] = DeadLetterRoutingKey;
        }

        await channel.QueueDeclareAsync(
            queue: options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // 4. Bind queue to exchange
        await channel.QueueBindAsync(
            queue: options.QueueName,
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[Consumer:{Key}] Declared queue '{Queue}' bound to '{Exchange}' with key '{RoutingKey}'", options.ServiceKey, options.QueueName, options.ExchangeName, options.RoutingKey);
    }

    /// <summary>
    /// Declares the dead-letter exchange, dead-letter queue, and retry queues.
    ///
    /// <para>
    /// Topology when EnableRetry=true, MaxRetries=3, Delays=[0s, 5s, 30s]:
    ///
    /// <code>
    /// DLX Exchange: "{QueueName}.dlx" (direct)
    ///     ↑ NACKed (requeue=false) messages from the main queue arrive here,
    ///       routed with the "dead" routing key.
    ///     │
    ///     └── DLQ: "{QueueName}.dlq" (permanent storage)
    ///         └── Bound to DLX with routing key "dead"
    ///
    /// Retry Queues (NOT bound to DLX):
    ///   "{QueueName}.retry.0s"  (TTL=1ms)
    ///   "{QueueName}.retry.5s"  (TTL=5000ms)
    ///   "{QueueName}.retry.30s" (TTL=30000ms)
    ///     └── x-dead-letter-exchange: main exchange
    ///     └── x-dead-letter-routing-key: main routing key
    /// </code>
    ///
    /// Flow:
    ///   Retry:    Main Queue → handler fails → client publishes to retry queue → ACK original
    ///             Retry queue (TTL expires) → broker dead-letters → Main Exchange → Main Queue
    ///   Final:    Main Queue → handler fails (retries exhausted) → NACK(requeue=false)
    ///             → broker dead-letters via x-dead-letter-exchange → DLX (direct, rk="dead") → DLQ
    ///             → IDeadLetterHandler invoked for notification
    /// </para>
    /// </summary>
    private static async Task DeclareDeadLetterTopologyAsync(IChannel channel, RabbitConsumerOptions options, ILogger logger, CancellationToken cancellationToken)
    {
        var dlxName = options.ResolvedDlxName;
        var dlqName = options.ResolvedDlqName;

        await channel.ExchangeDeclareAsync(
            exchange: dlxName,
            type: "direct",
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueDeclareAsync(
            queue: dlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            queue: dlqName,
            exchange: dlxName,
            routingKey: DeadLetterRoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[Consumer:{Key}] Declared DLX '{Dlx}' (direct) → DLQ '{Dlq}' (routing key '{RoutingKey}')", options.ServiceKey, dlxName, dlqName, DeadLetterRoutingKey);

        if (options.EnableRetry)
        {
            var delays = options.ResolvedRetryDelays;

            for (int i = 0; i < delays.Length; i++)
            {
                var delay = delays[i];
                var retryQueueName = $"{options.QueueName}.retry.{(int)delay.TotalSeconds}s";

                var retryArgs = new Dictionary<string, object?>
                {
                    ["x-dead-letter-exchange"] = options.ExchangeName,
                    ["x-dead-letter-routing-key"] = options.RoutingKey,
                };

                var ttlMs = delay > TimeSpan.Zero ? (int)delay.TotalMilliseconds : 1;
                retryArgs["x-message-ttl"] = ttlMs;

                await channel.QueueDeclareAsync(
                    queue: retryQueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: retryArgs,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                logger.LogInformation("[Consumer:{Key}] Declared retry queue '{RetryQueue}' (TTL={TTL}ms, not bound to DLX)", options.ServiceKey, retryQueueName, ttlMs);
            }
        }
    }
}
