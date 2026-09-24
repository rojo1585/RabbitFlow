namespace RabbitFlow.Configuration;

/// <summary>
/// Defines a message producer bound to a specific exchange and connection.
/// The <see cref="RabbitFlow.Abstractions.IEventPublisher.PublishAsync{TEvent}(TEvent, string?,CancellationToken)"/> 
/// </summary>
public sealed class RabbitProducerOptions
{
    /// <summary>
    /// Unique identifier for this producer.
    /// Used in <see cref="RabbitFlow.Abstractions.IEventPublisher.PublishAsync{TEvent}(TEvent, string?, CancellationToken)"/>
    /// to target a specific producer when multiple exist.
    /// Must be unique across all producers.
    /// </summary>
    public required string ServiceKey { get; init; }

    /// <summary>
    /// References <see cref="RabbitConnectionOptions.Name"/> to determine
    /// which connection this producer uses.
    /// </summary>
    public required string ConnectionName { get; init; }

    /// <summary>
    /// Name of the exchange to publish to. Declared automatically at startup.
    /// </summary>
    public required string ExchangeName { get; init; }

    /// <summary>   
    /// Exchange type: direct, topic, fanout, or headers.
    /// Defaults to "direct".
    /// </summary>
    public string ExchangeType { get; init; } = "direct";

    /// <summary>
    /// Default routing key used when none is provided at publish time.
    /// </summary>
    public required string RoutingKey { get; init; }

    /// <summary>
    /// Whether to use the AMQP mandatory flag.
    /// When true, the broker returns unroutable messages.
    /// Defaults to true to prevent silent message loss.
    /// </summary>
    public bool Mandatory { get; init; } = true;

    /// <summary>
    /// Whether to enable publisher confirms for this producer.
    /// When true, channels are created with <c>CreateChannelOptions(publisherConfirmationsEnabled: true,
    /// publisherConfirmationTrackingEnabled: true)</c> so that <c>BasicPublishAsync</c> blocks until
    /// the broker confirms the message. On failure, a <see cref="Exceptions.PublisherNackException"/>
    /// is thrown. On timeout, a <see cref="Exceptions.PublisherConfirmTimeoutException"/> is thrown.
    /// Defaults to true.
    /// </summary>
    public bool EnablePublisherConfirms { get; init; } = true;

    /// <summary>
    /// Maximum time in milliseconds to wait for publisher confirms.
    /// After this timeout, a <see cref="TimeoutException"/> is thrown.
    /// Defaults to 5000ms (5 seconds).
    /// Only applies when <see cref="EnablePublisherConfirms"/> is true.
    /// </summary>
    public int PublishConfirmTimeoutMs { get; init; } = 5_000;

    /// <summary>
    /// Whether to automatically declare the exchange topology at startup.
    /// Defaults to true.
    /// </summary>
    public bool AutoDeclareTopology { get; init; } = true;

    /// <summary>
    /// Size of the channel pool for this producer.
    ///
    /// <para>
    /// When greater than 0, channels are reused across publishes instead of
    /// being created and destroyed per publish. This eliminates the AMQP
    /// round-trip overhead of channel creation — the #1 throughput bottleneck
    /// in channel-per-publish strategies.
    /// </para>
    ///
    /// <para>
    /// The pool uses a rent/return pattern: each publish rents a channel from
    /// the pool, uses it, and returns it. A <see cref="System.Threading.SemaphoreSlim"/>
    /// gates concurrency — when all channels are rented, callers await until
    /// one is returned (natural backpressure).
    /// </para>
    ///
    /// <para>
    /// Recommended values:
    /// <list type="bullet">
    ///   <item>4–8 for publisher-confirms channels (good for 40k–200k msg/s).</item>
    ///   <item>2–4 for fire-and-forget channels (higher per-channel throughput).</item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// Set to 0 to disable pooling and revert to the channel-per-publish strategy
    /// (backward compatible). Defaults to 4.
    /// </para>
    /// </summary>
    public int ChannelPoolSize { get; init; } = 4;
}