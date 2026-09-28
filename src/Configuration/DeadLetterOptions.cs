
namespace RabbitFlow.Configuration;

/// <summary>
/// Flexible configuration for the dead-letter exchange (DLX), dead-letter queue (DLQ),
/// and their topology declaration behavior.
/// </summary>
/// <remarks>
/// <para>
/// When <see cref="RabbitConsumerOptions.DeadLetter"/> is <c>null</c> and
/// <see cref="RabbitConsumerOptions.EnableDeadLetter"/> is <c>true</c> (default), the
/// framework uses backward-compatible defaults: a <c>direct</c> DLX named
/// <c>"{QueueName}.dlx"</c>, a DLQ named <c>"{QueueName}.dlq"</c> bound with routing key
/// <c>"dead"</c>, both auto-declared at startup.
/// </para>
/// <para>
/// Set <see cref="RabbitConsumerOptions.DeadLetter"/> to an instance of this class to
/// customize any of those values, or to reference an externally-managed DLX/DLQ (via
/// <see cref="AutoDeclare"/>=<c>false</c>).
/// </para>
/// </remarks>
public sealed class DeadLetterOptions
{
    /// <summary>
    /// Name of the dead-letter exchange. If null, defaults to "{QueueName}.dlx".
    /// </summary>
    public string? ExchangeName { get; init; }

    /// <summary>
    /// Type of the dead-letter exchange. Defaults to "direct".
    /// </summary>
    /// <remarks>
    /// <para>
    /// Common values: <c>"direct"</c> (default), <c>"fanout"</c>, <c>"topic"</c>.
    /// </para>
    /// <para>
    /// When <see cref="ExchangeType"/> is <c>"fanout"</c>, the <see cref="RoutingKey"/>
    /// is ignored by the broker (fanout delivers to all bound queues regardless of key).
    /// The framework still passes it to <c>QueueBindAsync</c> for API consistency, but
    /// it has no effect. A startup warning is logged if RoutingKey is set to a non-default
    /// value with a fanout exchange to surface the likely confusion.
    /// </para>
    /// </remarks>
    public string ExchangeType { get; init; } = "direct";

    /// <summary>
    /// Name of the dead-letter queue. If null, defaults to "{QueueName}.dlq".
    /// </summary>
    public string? QueueName { get; init; }

    /// <summary>
    /// Routing key used to bind the DLQ to the DLX. Defaults to "dead".
    /// </summary>
    /// <remarks>
    /// Only relevant when <see cref="ExchangeType"/> is <c>"direct"</c> or <c>"topic"</c>.
    /// Ignored by the broker when <see cref="ExchangeType"/> is <c>"fanout"</c>.
    /// </remarks>
    public string RoutingKey { get; init; } = "dead";

    /// <summary>
    /// Additional queue arguments for the DLQ (e.g. <c>x-max-length</c>, <c>x-queue-type</c>,
    /// <c>x-message-ttl</c>). Passed to <c>QueueDeclareAsync</c> as the <c>arguments</c>
    /// parameter.
    /// </summary>
    /// <remarks>
    /// Use the constants in <see cref="RabbitMqArgs"/> (e.g.
    /// <see cref="RabbitMqArgs.QueueType"/>, <see cref="RabbitMqArgs.MaxLength"/>) to avoid
    /// typos in argument names.
    /// </remarks>
    public Dictionary<string, object?>? QueueArguments { get; init; }

    /// <summary>
    /// Additional exchange arguments for the DLX (e.g. <c>alternate-exchange</c>).
    /// </summary>
    public Dictionary<string, object?>? ExchangeArguments { get; init; }

    /// <summary>
    /// If <c>true</c> (default), the framework declares the DLX and DLQ at startup
    /// (create-if-not-exists, <c>passive=false</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Set to <c>false</c> when the DLX/DLQ already exist and are managed externally
    /// (e.g. by ops, shared across services, or declared by another team). The framework
    /// will NOT declare them, but will still:
    /// </para>
    /// <list type="bullet">
    ///   <item>Set <c>x-dead-letter-exchange</c> on the main queue to point to <see cref="ExchangeName"/>.</item>
    ///   <item>Set <c>x-dead-letter-routing-key</c> on the main queue to <see cref="RoutingKey"/>.</item>
    /// </list>
    /// <para>
    /// When <c>false</c>, the framework performs a passive check (<c>passive=true</c>) to
    /// verify the DLX and DLQ actually exist. If they don't, startup fails fast with a
    /// clear <see cref="Exceptions.RabbitMqConfigurationException"/> rather than a cryptic
    /// broker <c>PRECONDITION_FAILED</c> at runtime.
    /// </para>
    /// </remarks>
    public bool AutoDeclare { get; init; } = true;
}
