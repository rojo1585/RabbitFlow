namespace RabbitFlow.Configuration;



/// <summary>
/// Defines a message consumer bound to a specific queue, exchange, and connection.
/// The <see cref="ServiceKey"/> is used to map <see cref="Abstractions.IRabbitHandler{T}"/>
/// or <see cref="Abstractions.IBatchRabbitHandler{T}"/> implementations to the correct consumer.
/// </summary>
public sealed record RabbitConsumerOptions
{
    /// <summary>
    /// Unique identifier for this consumer.
    /// Handlers set their <see cref="Abstractions.IRabbitHandler{T}.ConsumerKey"/>
    /// to this value to be invoked for messages from this queue.
    /// Must be unique across all consumers.
    /// </summary>
    public required string ServiceKey { get; init; }

    /// <summary>
    /// References <see cref="RabbitConnectionOptions.Name"/> to determine
    /// which connection this consumer uses.
    /// </summary>
    public required string ConnectionName { get; init; }

    /// <summary>
    /// Name of the exchange to bind the queue to. Declared automatically at startup.
    /// </summary>
    public required string ExchangeName { get; init; }

    /// <summary>
    /// Exchange type: direct, topic, fanout, or headers.
    /// Defaults to "direct".
    /// </summary>
    public string ExchangeType { get; init; } = "direct";

    /// <summary>
    /// Name of the queue to consume from. Declared automatically at startup.
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// Routing key used to bind the queue to the exchange.
    /// For topic exchanges, this can contain wildcards (* and #).
    /// </summary>
    public required string RoutingKey { get; init; }

    /// <summary>
    /// Maximum number of unacknowledged messages delivered to this consumer.
    /// Controls the prefetch window to prevent the consumer from being overwhelmed.
    /// Defaults to 10.
    /// </summary>
    /// <remarks>
    /// For batch consumers, this should be >= <see cref="BatchSize"/> to ensure
    /// enough messages are pre-fetched to fill batches efficiently.
    /// </remarks>
    public ushort PrefetchCount { get; init; } = 10;

    /// <summary>
    /// Maximum number of handler invocations that can run concurrently.
    /// When set to 0 (default), there is no limit beyond the prefetch count.
    /// When set to a value greater than 0, a <see cref="System.Threading.SemaphoreSlim"/>
    /// throttles handler execution independently of AMQP prefetch.
    /// Useful when handlers call external APIs and you want to limit concurrency.
    /// Defaults to 0 (unlimited).
    /// </summary>
    /// <remarks>
    /// Not applicable when <see cref="EnableBatchConsumer"/> is true — batch consumers
    /// process one batch at a time.
    /// </remarks>
    public int MaxConcurrentHandlers { get; init; } = 0;

    // ─── Dead Letter Settings ──────────────────────────────────────────

    /// <summary>
    /// Whether to configure a dead-letter exchange (DLX) and dead-letter queue (DLQ)
    /// for this consumer. When a message is NACKed without requeue and has exhausted
    /// its retry attempts, it is routed to the DLQ.
    /// Defaults to true.
    /// </summary>
    public bool EnableDeadLetter { get; init; } = true;

    /// <summary>
    /// Name of the dead-letter exchange. If null, defaults to "{QueueName}.dlx".
    /// </summary>
    public string? DeadLetterExchangeName { get; init; }

    /// <summary>
    /// Name of the dead-letter queue. If null, defaults to "{QueueName}.dlq".
    /// </summary>
    public string? DeadLetterQueueName { get; init; }

    // ─── Retry Settings ────────────────────────────────────────────────

    /// <summary>
    /// Whether to enable automatic retry for failed messages.
    /// When enabled, failed messages are routed to a retry queue with a TTL,
    /// then re-delivered to the original queue after the TTL expires.
    /// The retry count is tracked via the x-death header.
    /// Defaults to true.
    /// </summary>
    public bool EnableRetry { get; init; } = true;

    /// <summary>
    /// Maximum number of delivery attempts before the message is sent to the DLQ.
    /// The first delivery counts as attempt 1.
    /// Defaults to 3.
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    /// Delay durations between retry attempts.
    /// Each entry creates a separate retry queue with the corresponding TTL.
    /// If null, defaults to [0s, 5s, 30s].
    /// The array length should be at least <see cref="MaxRetries"/> - 1.
    /// Index 0 is the delay before retry attempt 2, index 1 before attempt 3, etc.
    /// </summary>
    public TimeSpan[]? RetryDelays { get; init; }

    // ─── Batch Consumer Settings ──────────────────────────────────────

    /// <summary>
    /// Whether this consumer operates in batch mode.
    /// When true, the consumer requires an <see cref="Abstractions.IBatchRabbitHandler{TEvent}"/>
    /// to be registered instead of (or in addition to) <see cref="Abstractions.IRabbitHandler{TEvent}"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// In batch mode, messages are buffered until either:
    /// <list type="bullet">
    ///   <item><see cref="BatchSize"/> messages have arrived, or</item>
    ///   <item><see cref="BatchTimeoutMs"/> milliseconds have elapsed since the first message in the batch.</item>
    /// </list>
    /// The batch is then dispatched to the <see cref="Abstractions.IBatchRabbitHandler{TEvent}.HandleBatchAsync"/> method.
    /// </para>
    /// <para>
    /// All-or-nothing ACK: if the batch handler throws, ALL messages in the batch are NACKed.
    /// Partial failure handling is the handler's responsibility.
    /// </para>
    /// <para>
    /// When false (default), each message is dispatched individually to <see cref="Abstractions.IRabbitHandler{TEvent}"/>.
    /// </para>
    /// </remarks>
    public bool EnableBatchConsumer { get; init; }

    /// <summary>
    /// Maximum number of messages to buffer before flushing the batch.
    /// The batch is dispatched immediately when this count is reached.
    /// Only used when <see cref="EnableBatchConsumer"/> is true.
    /// Defaults to 10.
    /// </summary>
    public int BatchSize { get; init; } = 10;

    /// <summary>
    /// Maximum time in milliseconds to wait for the batch to fill before flushing.
    /// When the timer fires, all buffered messages are dispatched as a batch,
    /// even if <see cref="BatchSize"/> has not been reached.
    /// Only used when <see cref="EnableBatchConsumer"/> is true.
    /// Defaults to 5000 (5 seconds).
    /// </summary>
    public int BatchTimeoutMs { get; init; } = 5000;

    // ─── Topology ─────────────────────────────────────────────────────

    /// <summary>
    /// Whether to automatically declare the exchange, queue, and bindings at startup.
    /// Defaults to true.
    /// </summary>
    public bool AutoDeclareTopology { get; init; } = true;

    // ─── Computed Properties ──────────────────────────────────────────

    /// <summary>
    /// Resolved dead-letter exchange name (falls back to convention if not explicitly set).
    /// </summary>
    internal string ResolvedDlxName => DeadLetterExchangeName ?? $"{QueueName}.dlx";

    /// <summary>
    /// Resolved dead-letter queue name (falls back to convention if not explicitly set).
    /// </summary>
    internal string ResolvedDlqName => DeadLetterQueueName ?? $"{QueueName}.dlq";

    /// <summary>
    /// Resolved retry delays (falls back to default if not explicitly set).
    /// </summary>
    internal TimeSpan[] ResolvedRetryDelays => RetryDelays ?? [TimeSpan.Zero, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30)];
}
