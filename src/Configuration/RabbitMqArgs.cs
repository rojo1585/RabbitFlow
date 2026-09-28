using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Configuration;

/// <summary>
/// Well-known RabbitMQ argument keys and values for use in
/// <see cref="RabbitConsumerOptions.QueueArguments"/>,
/// <see cref="DeadLetterOptions.QueueArguments"/>,
/// <see cref="DeadLetterOptions.ExchangeArguments"/>, and
/// <see cref="RabbitProducerOptions.ExchangeArguments"/>.
/// </summary>
/// <remarks>
/// <para>
/// These constants reduce the risk of typos in argument names (which fail at runtime with
/// cryptic broker errors, not at compile time). Use them in place of string literals:
/// </para>
/// <code>
/// QueueArguments = new()
/// {
///     [RabbitMqArgs.QueueType] = RabbitMqArgs.QueueTypes.Quorum,
///     [RabbitMqArgs.MaxLength] = 10000,
///     [RabbitMqArgs.MessageTtl] = 604800000
/// }
/// </code>
/// </remarks>
public static class RabbitMqArgs
{
    // ─── Queue arguments ──────────────────────────────────────────────

    /// <summary>
    /// <c>x-queue-type</c> — selects the queue implementation type.
    /// Use values from <see cref="QueueTypes"/>.
    /// </summary>
    public const string QueueType = "x-queue-type";

    /// <summary>
    /// <c>x-max-length</c> — maximum number of ready messages in the queue.
    /// When exceeded, messages are dropped from the head (or rejected, per <see cref="Overflow"/>).
    /// </summary>
    public const string MaxLength = "x-max-length";

    /// <summary>
    /// <c>x-max-length-bytes</c> — maximum total body size of ready messages in the queue.
    /// </summary>
    public const string MaxLengthBytes = "x-max-length-bytes";

    /// <summary>
    /// <c>x-max-priority</c> — maximum priority level the queue supports (enables priority queues).
    /// Messages published with a higher priority are delivered first.
    /// </summary>
    public const string MaxPriority = "x-max-priority";

    /// <summary>
    /// <c>x-message-ttl</c> — time-to-live (in milliseconds) for messages in the queue.
    /// Messages older than this are discarded (or dead-lettered if a DLX is configured).
    /// </summary>
    public const string MessageTtl = "x-message-ttl";

    /// <summary>
    /// <c>x-expires</c> — how long (in milliseconds) a queue can remain unused before being
    /// automatically deleted by the broker.
    /// </summary>
    public const string Expires = "x-expires";

    /// <summary>
    /// <c>x-single-active-consumer</c> — when true, only one consumer per group consumes at a time
    /// (failover active/passive without duplicate consumption). Set to <c>true</c> / <c>false</c>.
    /// </summary>
    public const string SingleActiveConsumer = "x-single-active-consumer";

    /// <summary>
    /// <c>x-dead-letter-exchange</c> — exchange to dead-letter messages to when rejected or expired.
    /// </summary>
    public const string DeadLetterExchange = "x-dead-letter-exchange";

    /// <summary>
    /// <c>x-dead-letter-routing-key</c> — routing key used when dead-lettering.
    /// Overrides the message's original routing key.
    /// </summary>
    public const string DeadLetterRoutingKey = "x-dead-letter-routing-key";

    /// <summary>
    /// <c>x-overflow</c> — behavior when <see cref="MaxLength"/> is reached.
    /// Use values from <see cref="OverflowBehaviors"/>.
    /// </summary>
    public const string Overflow = "x-overflow";

    /// <summary>
    /// <c>x-queue-master-locator</c> — strategy for selecting the master node of a queue
    /// in a cluster (e.g. <c>"min-masters"</c>, <c>"client-local"</c>, <c>"random"</c>).
    /// </summary>
    public const string QueueMasterLocator = "x-queue-master-locator";

    /// <summary>
    /// <c>x-quorum-initial-group-size</c> — initial quorum queue replication factor.
    /// Only relevant when <see cref="QueueType"/> is <see cref="QueueTypes.Quorum"/>.
    /// </summary>
    public const string QuorumInitialGroupSize = "x-quorum-initial-group-size";

    /// <summary>
    /// <c>x-delivery-limit</c> — for quorum/streams: max redelivery count before the message
    /// is dropped (not dead-lettered).
    /// </summary>
    public const string DeliveryLimit = "x-delivery-limit";

    // ─── Exchange arguments ───────────────────────────────────────────

    /// <summary>
    /// <c>alternate-exchange</c> — name of an exchange to route unroutable messages to
    /// (when no queue binding matches the routing key).
    /// </summary>
    public const string AlternateExchange = "alternate-exchange";

    /// <summary>
    /// Well-known <c>x-queue-type</c> values. Use with <see cref="QueueType"/>.
    /// </summary>
    public static class QueueTypes
    {
        /// <summary>
        /// <c>"classic"</c> — durable queue backed by the classic queue implementation.
        /// Default when <c>x-queue-type</c> is not set.
        /// </summary>
        public const string Classic = "classic";

        /// <summary>
        /// <c>"quorum"</c> — Raft-based replicated queue for high availability and data safety.
        /// Recommended for critical workloads; requires a cluster of 3+ nodes for full benefit.
        /// </summary>
        public const string Quorum = "quorum";

        /// <summary>
        /// <c>"stream"</c> — append-only log with non-destructive consumption (RabbitMQ 3.9+).
        /// Suitable for event sourcing and large fan-out scenarios.
        /// </summary>
        public const string Stream = "stream";
    }

    /// <summary>
    /// Well-known <c>x-overflow</c> values. Use with <see cref="Overflow"/>.
    /// </summary>
    public static class OverflowBehaviors
    {
        /// <summary>
        /// <c>"drop-head"</c> (default) — drop the oldest messages from the head of the queue
        /// when <see cref="MaxLength"/> is reached.
        /// </summary>
        public const string DropHead = "drop-head";

        /// <summary>
        /// <c>"reject-publish"</c> — reject new publishes when <see cref="MaxLength"/> is reached.
        /// The publisher receives a <c>nack</c> (with publisher confirms enabled).
        /// </summary>
        public const string RejectPublish = "reject-publish";

        /// <summary>
        /// <c>"reject-publish-dlx"</c> — like <see cref="RejectPublish"/>, but dead-letters the
        /// rejected message if a DLX is configured (RabbitMQ 3.8+).
        /// </summary>
        public const string RejectPublishDlx = "reject-publish-dlx";
    }
}
