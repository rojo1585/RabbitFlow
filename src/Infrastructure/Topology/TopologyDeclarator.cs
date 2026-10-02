using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RedRabbit.Configuration;

namespace RedRabbit.Infrastructure.Topology;

/// <summary>
/// Declares AMQP topology (exchanges, queues, bindings, DLX, retry queues)
/// on a channel. Called at startup by producers and consumers.
///
/// <para>
/// Declarative mode (default): resources are created-if-not-exists (passive=false).
/// Passive mode (DeadLetterOptions.AutoDeclare=false): resources are checked for existence
/// (passive=true) and a clear exception is thrown if they don't exist, rather than a cryptic
/// broker PRECONDITION_FAILED at runtime.
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
            arguments: options.ExchangeArguments,
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

        await channel.ExchangeDeclareAsync(
            exchange: options.ExchangeName,
            type: options.ExchangeType,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[Consumer:{Key}] Declared exchange '{Exchange}' ({Type})", options.ServiceKey, options.ExchangeName, options.ExchangeType);

        if (options.EnableDeadLetter)
            await DeclareDeadLetterTopologyAsync(channel, options, logger, cancellationToken)
                .ConfigureAwait(false);
        else if (options.EnableRetry)
            await DeclareRetryQueuesAsync(channel, options, logger, cancellationToken)
                .ConfigureAwait(false);

        var queueArgs = BuildMainQueueArguments(options);

        await channel.QueueDeclareAsync(
            queue: options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            queue: options.QueueName,
            exchange: options.ExchangeName,
            routingKey: options.RoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        logger.LogInformation("[Consumer:{Key}] Declared queue '{Queue}' bound to '{Exchange}' with key '{RoutingKey}'", options.ServiceKey, options.QueueName, options.ExchangeName, options.RoutingKey);
    }

    /// <summary>
    /// Builds the arguments dictionary for the main queue, merging user-provided
    /// <see cref="RabbitConsumerOptions.QueueArguments"/> with framework-set args
    /// (x-dead-letter-exchange, x-message-ttl, x-single-active-consumer).
    /// Framework args take precedence to avoid misconfiguration.
    /// </summary>
    private static Dictionary<string, object?> BuildMainQueueArguments(RabbitConsumerOptions options)
    {
        var queueArgs = options.QueueArguments is not null ? new Dictionary<string, object?>(options.QueueArguments) : [];

        if (options.EnableDeadLetter)
        {
            queueArgs[RabbitMqArgs.DeadLetterExchange] = options.ResolvedDlxName;
            queueArgs[RabbitMqArgs.DeadLetterRoutingKey] = options.ResolvedDlqRoutingKey;
        }

        if (options.MessageTtl is not null)
            queueArgs[RabbitMqArgs.MessageTtl] = (int)options.MessageTtl.Value.TotalMilliseconds;

        if (options.SingleActiveConsumer)
            queueArgs[RabbitMqArgs.SingleActiveConsumer] = true;

        return queueArgs;
    }

    /// <summary>
    /// Declares the dead-letter exchange, dead-letter queue, and retry queues.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When <see cref="DeadLetterOptions.AutoDeclare"/> is true (default), the DLX and DLQ
    /// are declared (create-if-not-exists). When false, a passive check (passive=true) is
    /// performed — if the DLX or DLQ don't exist, a clear exception is thrown.
    /// </para>
    /// <para>
    /// Retry queues are ALWAYS auto-declared by the framework (they are an implementation
    /// detail of the client-side retry mechanism). The user cannot reference external retry
    /// queues because their names and TTLs are derived from RetryDelays.
    /// </para>
    /// </remarks>
    private static async Task DeclareDeadLetterTopologyAsync(IChannel channel, RabbitConsumerOptions options, ILogger logger, CancellationToken cancellationToken)
    {
        var dlxName = options.ResolvedDlxName;
        var dlqName = options.ResolvedDlqName;
        var dlxType = options.ResolvedDlxType;
        var dlqRoutingKey = options.ResolvedDlqRoutingKey;
        var autoDeclare = options.ResolvedDlqAutoDeclare;
        var dlxArgs = options.ResolvedDlxExchangeArguments;
        var dlqArgs = options.ResolvedDlqQueueArguments;

        if (dlxType == "fanout" && dlqRoutingKey != "dead")
        {
            logger.LogWarning(@"[Consumer:{Key}] DeadLetter.ExchangeType is 'fanout' but RoutingKey is '{RoutingKey}'.
                                Fanout exchanges ignore the routing key — the DLQ will receive all messages regardless. 
                                Set RoutingKey to 'dead' (default) to avoid confusion.", options.ServiceKey, dlqRoutingKey);
        }

        if (autoDeclare)
        {
            // Declarative mode: create-if-not-exists (passive=false).
            await channel.ExchangeDeclareAsync(
                exchange: dlxName,
                type: dlxType,
                durable: true,
                autoDelete: false,
                arguments: dlxArgs,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await channel.QueueDeclareAsync(
                queue: dlqName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: dlqArgs,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            logger.LogInformation("[Consumer:{Key}] Declared DLX '{Dlx}' ({Type}) → DLQ '{Dlq}' (routing key '{RoutingKey}')", options.ServiceKey, dlxName, dlxType, dlqName, dlqRoutingKey);
        }
        else
        {
            await DeclarePassiveExchangeAsync(channel, dlxName, options.ServiceKey, "dead-letter exchange", cancellationToken).ConfigureAwait(false);
            await DeclarePassiveQueueAsync(channel, dlqName, options.ServiceKey, "dead-letter queue", cancellationToken).ConfigureAwait(false);

            logger.LogInformation("[Consumer:{Key}] Verified external DLX '{Dlx}' and DLQ '{Dlq}' exist (AutoDeclare=false)", options.ServiceKey, dlxName, dlqName);
        }

        await channel.QueueBindAsync(
            queue: dlqName,
            exchange: dlxName,
            routingKey: dlqRoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        if (options.EnableRetry)
            await DeclareRetryQueuesAsync(channel, options, logger, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Declares the retry queues (one per delay in RetryDelays).
    /// These are NOT bound to any DLX — the client publishes directly to them via the
    /// default exchange. When the TTL expires, the broker dead-letters the message back
    /// to the main exchange (x-dead-letter-exchange), re-delivering it to the main queue.
    /// </summary>
    /// <remarks>
    /// Called by <see cref="DeclareDeadLetterTopologyAsync"/> when EnableRetry=true and
    /// EnableDeadLetter=true, OR directly by <see cref="DeclareConsumerTopologyAsync"/>
    /// when EnableRetry=true but EnableDeadLetter=false (retry without DLQ).
    /// </remarks>
    private static async Task DeclareRetryQueuesAsync(IChannel channel, RabbitConsumerOptions options, ILogger logger, CancellationToken cancellationToken)
    {
        var delays = options.ResolvedRetryDelays;

        for (int i = 0; i < delays.Length; i++)
        {
            var delay = delays[i];
            var retryQueueName = $"{options.QueueName}.retry.{(int)delay.TotalSeconds}s";
          
            var retryArgs = new Dictionary<string, object?>
            {
                [RabbitMqArgs.DeadLetterExchange] = string.Empty,
                [RabbitMqArgs.DeadLetterRoutingKey] = options.QueueName,
            };

            var ttlMs = delay > TimeSpan.Zero ? (int)delay.TotalMilliseconds : 1;
            retryArgs[RabbitMqArgs.MessageTtl] = ttlMs;

            await channel.QueueDeclareAsync(
                queue: retryQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: retryArgs,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            logger.LogInformation("[Consumer:{Key}] Declared retry queue '{RetryQueue}' (TTL={TTL}ms, dead-letters directly to main queue '{MainQueue}')", options.ServiceKey, retryQueueName, ttlMs, options.QueueName);
        }
    }

    /// <summary>
    /// Passively checks that an exchange exists (passive=true). Throws a clear
    /// <see cref="OperationInterruptedException"/> if it doesn't, which surfaces as a
    /// startup failure rather than a runtime PRECONDITION_FAILED.
    /// </summary>
    private static async Task DeclarePassiveExchangeAsync(IChannel channel, string exchangeName, string consumerKey, string roleDescription, CancellationToken cancellationToken)
    {
        try
        {
            await channel.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: "direct", // ignored when passive=true
                durable: true, // ignored when passive=true
                autoDelete: false, // ignored when passive=true
                passive: true,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (OperationInterruptedException ex) when (ex.ShutdownReason?.ReplyCode == 404)
        {
            throw new InvalidOperationException(
                $"Consumer '{consumerKey}': {roleDescription} '{exchangeName}' does not exist and DeadLetter.AutoDeclare=false. " +
                $"Either create it externally (via RabbitMQ Management UI or ops scripts) or set DeadLetter.AutoDeclare=true.",
                ex);
        }
    }

    /// <summary>
    /// Passively checks that a queue exists (passive=true). Throws a clear exception if it doesn't.
    /// </summary>
    private static async Task DeclarePassiveQueueAsync(IChannel channel, string queueName, string consumerKey, string roleDescription, CancellationToken cancellationToken)
    {
        try
        {
            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: true, // ignored when passive=true
                exclusive: false, // ignored when passive=true
                autoDelete: false, // ignored when passive=true
                passive: true,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (OperationInterruptedException ex) when (ex.ShutdownReason?.ReplyCode == 404)
        {
            throw new InvalidOperationException(@$"Consumer '{consumerKey}': {roleDescription} '{queueName}' does not exist and DeadLetter.AutoDeclare=false.
                                                   Either create it externally (via RabbitMQ Management UI or ops scripts) or set DeadLetter.AutoDeclare=true.",
                                                   ex);
        }
    }
}

