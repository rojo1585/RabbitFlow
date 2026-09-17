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
            queueArgs["x-dead-letter-exchange"] = options.ResolvedDlxName;

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
    /// DLX Exchange: "{QueueName}.dlx" (fanout)
    ///     ↑ NACKed messages arrive here
    ///     │
    ///     ├── Retry Queue 1: "{QueueName}.retry.5s" (TTL=5s)
    ///     │     └── x-dead-letter-exchange: main exchange
    ///     │     └── x-dead-letter-routing-key: main routing key
    ///     │
    ///     ├── Retry Queue 2: "{QueueName}.retry.30s" (TTL=30s)
    ///     │     └── x-dead-letter-exchange: main exchange
    ///     │     └── x-dead-letter-routing-key: main routing key
    ///     │
    ///     └── DLQ: "{QueueName}.dlq" (permanent storage)
    ///         └── Bound to DLX with routing key "{QueueName}"
    /// </code>
    /// 
    /// Flow:
    ///   Main Queue → NACK → DLX → retry queue (TTL expires) → Main Exchange → Main Queue
    ///   After MaxRetries: Main Queue → NACK → DLX → DLQ → IDeadLetterHandler
    /// </para>
    /// </summary>
    private static async Task DeclareDeadLetterTopologyAsync(IChannel channel, RabbitConsumerOptions options, ILogger logger, CancellationToken cancellationToken)
    {
        var dlxName = options.ResolvedDlxName;
        var dlqName = options.ResolvedDlqName;

        // 1. DLX Exchange (fanout — receives all dead-lettered messages)
        await channel.ExchangeDeclareAsync(
            exchange: dlxName,
            type: "fanout",
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // 2. DLQ (final resting place for exhausted messages)
        await channel.QueueDeclareAsync(
            queue: dlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        // Bind DLQ to DLX with queue name as routing key
        // The dead letter consumer will consume from this queue
        await channel.QueueBindAsync(
            queue: dlqName,
            exchange: dlxName,
            routingKey: dlqName,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[Consumer:{Key}] Declared DLX '{Dlx}' → DLQ '{Dlq}'", options.ServiceKey, dlxName, dlqName);

        // 3. Retry queues (one per delay)
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

                if (delay > TimeSpan.Zero)
                    retryArgs["x-message-ttl"] = (int)delay.TotalMilliseconds;


                await channel.QueueDeclareAsync(
                    queue: retryQueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: retryArgs,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                // Bind retry queue to DLX
                await channel.QueueBindAsync(
                    queue: retryQueueName,
                    exchange: dlxName,
                    routingKey: retryQueueName,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                logger.LogInformation("[Consumer:{Key}] Declared retry queue '{RetryQueue}' (TTL={TTL}ms)", options.ServiceKey, retryQueueName, (int)delay.TotalMilliseconds);
            }
        }
    }
}