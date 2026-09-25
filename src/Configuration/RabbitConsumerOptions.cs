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
    /// <remarks>
    /// When <c>true</c> and <see cref="DeadLetter"/> is <c>null</c>, the framework auto-declares
    /// a DLX/DLQ with default names ({QueueName}.dlx, {QueueName}.dlq), bound with routing key
    /// "dead" — backward-compatible behavior. When <c>true</c> and <see cref="DeadLetter"/> is
    /// set, the framework uses the provided <see cref="DeadLetterOptions"/>. Setting this to
    /// <c>false</c> while also setting <see cref="DeadLetter"/> is a configuration error
    /// (validated at startup).
    /// </remarks>
    public bool EnableDeadLetter { get; init; } = true;

    /// <summary>
    /// Name of the dead-letter exchange. If null, defaults to "{QueueName}.dlx".
    /// </summary>
    public string? DeadLetterExchangeName { get; init; }

    /// <summary>
    /// Name of the dead-letter queue. If null, defaults to "{QueueName}.dlq".
    /// </summary>
    /// <remarks>
    /// Shortcut for <see cref="DeadLetterOptions.QueueName"/>. If <see cref="DeadLetter"/>
    /// is set, this property is ignored (use <see cref="DeadLetterOptions.QueueName"/> instead).
    /// </remarks>
    public string? DeadLetterQueueName { get; init; }

    /// <summary>
    /// Flexible dead-letter configuration. When set, overrides
    /// <see cref="DeadLetterExchangeName"/> and <see cref="DeadLetterQueueName"/> with
    /// full control over exchange type, routing key, queue arguments, and auto-declare behavior.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When <c>null</c> (default), the framework uses backward-compatible defaults
    /// (see <see cref="EnableDeadLetter"/>).
    /// </para>
    /// <para>
    /// Set to an instance to:
    /// </para>
    /// <list type="bullet">
    ///   <item>Customize DLX/DLQ names, exchange type, or routing key.</item>
    ///   <item>Pass extra queue arguments (e.g. x-max-length, x-queue-type=quorum).</item>
    ///   <item>Reference an externally-managed DLX/DLQ (<see cref="DeadLetterOptions.AutoDeclare"/> = false).</item>
    /// </list>
    /// </remarks>
    public DeadLetterOptions? DeadLetter { get; init; }

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

    /// <summary>
    /// Additional arguments for the main queue (passed to QueueDeclareAsync).
    /// Use the constants in <see cref="RabbitMqArgs"/> to avoid typos.
    /// </summary>
    public Dictionary<string, object?>? QueueArguments { get; init; }

    /// <summary>
    /// Time-to-live for messages in the main queue. Messages older than this are discarded
    /// (or dead-lettered if <see cref="EnableDeadLetter"/> is true).
    /// Sets x-message-ttl on the queue declaration.
    /// </summary>
    public TimeSpan? MessageTtl { get; init; }

    /// <summary>
    /// When true, only one consumer in a consumer group consumes at a time. Other consumers
    /// registered with the same queue will be standby; they take over only if the active
    /// consumer dies (failover without duplicate consumption). Sets x-single-active-consumer.
    /// </summary>
    public bool SingleActiveConsumer { get; init; }

    /// <summary>
    /// Consumer tag used to identify this consumer in the RabbitMQ Management UI and logs.
    /// If null, the broker generates one (e.g. amq.ctag-xxxx).
    /// </summary>
    public string? ConsumerTag { get; init; }

    // ─── Computed Properties ──────────────────────────────────────────

    /// <summary>
    /// Resolved dead-letter exchange name (falls back to convention if not explicitly set).
    /// </summary>
    internal string ResolvedDlxName => DeadLetter?.ExchangeName ?? DeadLetterExchangeName ?? $"{QueueName}.dlx";

    /// <summary>
    /// Resolved dead-letter queue name (falls back to convention if not explicitly set).
    /// </summary>
    internal string ResolvedDlqName => DeadLetter?.QueueName ?? DeadLetterQueueName ?? $"{QueueName}.dlq";

    /// <summary>
    /// Resolved dead-letter exchange type (defaults to "direct").
    /// </summary>
    internal string ResolvedDlxType => DeadLetter?.ExchangeType ?? "direct";

    /// <summary>
    /// Resolved routing key for binding the DLQ to the DLX (defaults to "dead").
    /// </summary>
    internal string ResolvedDlqRoutingKey => DeadLetter?.RoutingKey ?? "dead";

    /// <summary>
    /// Whether to auto-declare the DLX/DLQ (defaults to true).
    /// </summary>
    internal bool ResolvedDlqAutoDeclare => DeadLetter?.AutoDeclare ?? true;

    /// <summary>
    /// Resolved DLQ queue arguments (may be null).
    /// </summary>
    internal Dictionary<string, object?>? ResolvedDlqQueueArguments => DeadLetter?.QueueArguments;

    /// <summary>
    /// Resolved DLX exchange arguments (may be null).
    /// </summary>
    internal Dictionary<string, object?>? ResolvedDlxExchangeArguments => DeadLetter?.ExchangeArguments;

    /// <summary>
    /// Resolved retry delays (falls back to default if not explicitly set).
    /// </summary>
    internal TimeSpan[] ResolvedRetryDelays => RetryDelays ?? [TimeSpan.Zero, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30)];
}
