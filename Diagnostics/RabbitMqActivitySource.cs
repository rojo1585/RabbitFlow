using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace RabbitFlow.Diagnostics
{
    /// <summary>
    /// Central <see cref="ActivitySource"/> for all Apymsa.RabbitMQ distributed tracing.
    /// 
    /// <para>
    /// Activities emitted by this source are automatically collected by any
    /// OpenTelemetry SDK configured with <c>AddSource("Apymsa.RabbitMQ")</c>.
    /// </para>
    /// 
    /// <para>
    /// Tags follow the <see href="https://opentelemetry.io/docs/specs/semconv/messaging/">
    /// OpenTelemetry Semantic Conventions for Messaging</see>:
    /// <list type="bullet">
    ///   <item><c>messaging.system</c> = <c>"rabbitmq"</c></item>
    ///   <item><c>messaging.destination.kind</c> = <c>"exchange"</c> | <c>"queue"</c></item>
    ///   <item><c>messaging.destination.name</c> = exchange or queue name</item>
    ///   <item><c>messaging.operation</c> = <c>"publish"</c> | <c>"receive"</c> | <c>"process"</c></item>
    /// </list>
    /// </para>
    /// 
    /// <para>
    /// <b>Usage in the host:</b>
    /// <code>
    /// builder.Services.AddOpenTelemetry()
    ///     .WithTracing(tracing => tracing
    ///         .AddSource(RabbitMqActivitySource.SourceName));
    /// </code>
    /// </para>
    /// </summary>
    public static class RabbitMqActivitySource
    {
        /// <summary>
        /// The name used to register the <see cref="ActivitySource"/>.
        /// Consumers must pass this exact string to <c>AddSource()</c>.
        /// </summary>
        public const string SourceName = "Apymsa.RabbitMQ";

        /// <summary>
        /// The singleton <see cref="ActivitySource"/> instance.
        /// All publisher and consumer activities are created through this.
        /// </summary>
        public static ActivitySource Source { get; } = new(SourceName, "1.0.0");

        // ────────────────────────────────────────────────
        // Activity names
        // ────────────────────────────────────────────────

        /// <summary>
        /// Activity name for publish operations.
        /// Format: <c>{event-type} publish</c>
        /// </summary>
        public const string PublishActivityName = "{event-type} publish";

        /// <summary>
        /// Activity name for consume (receive) operations.
        /// Format: <c>{event-type} receive</c>
        /// </summary>
        public const string ReceiveActivityName = "{event-type} receive";

        /// <summary>
        /// Activity name for handler processing.
        /// Format: <c>{event-type} process</c>
        /// </summary>
        public const string ProcessActivityName = "{event-type} process";

        // ────────────────────────────────────────────────
        // OTel semantic convention tag names
        // ────────────────────────────────────────────────

        /// <summary>Always <c>"rabbitmq"</c>.</summary>
        public const string TagMessagingSystem = "messaging.system";

        /// <summary><c>"exchange"</c> or <c>"queue"</c>.</summary>
        public const string TagMessagingDestinationKind = "messaging.destination.kind";

        /// <summary>Name of the exchange or queue.</summary>
        public const string TagMessagingDestinationName = "messaging.destination.name";

        /// <summary><c>"publish"</c>, <c>"receive"</c>, or <c>"process"</c>.</summary>
        public const string TagMessagingOperation = "messaging.operation";

        /// <summary>The RabbitMQ routing key.</summary>
        public const string TagMessagingRabbitmqRoutingKey = "messaging.rabbitmq.routing_key";

        /// <summary>The full .NET type name of the event.</summary>
        public const string TagMessagingEventName = "messaging.event.name";

        /// <summary>The producer or consumer service key.</summary>
        public const string TagMessagingServiceKey = "messaging.service.key";

        /// <summary>The consumer key that received the message.</summary>
        public const string TagMessagingConsumerKey = "messaging.consumer.key";

        /// <summary>Message correlation ID.</summary>
        public const string TagMessagingMessageId = "messaging.message.id";

        /// <summary>Message correlation ID (for linking across services).</summary>
        public const string TagMessagingConversationId = "messaging.conversation_id";

        /// <summary>Retry/delivery attempt number (1-based).</summary>
        public const string TagMessagingDeliveryAttempt = "messaging.delivery_attempt";

        /// <summary>Error type when the operation fails.</summary>
        public const string TagErrorType = "error.type";

        // ────────────────────────────────────────────────
        // Tag values
        // ────────────────────────────────────────────────

        /// <summary>Tag value for RabbitMQ.</summary>
        public const string SystemRabbitMq = "rabbitmq";

        /// <summary>Tag value for exchange destination kind.</summary>
        public const string DestinationKindExchange = "exchange";

        /// <summary>Tag value for queue destination kind.</summary>
        public const string DestinationKindQueue = "queue";

        /// <summary>Tag value for publish operation.</summary>
        public const string OperationPublish = "publish";

        /// <summary>Tag value for receive operation.</summary>
        public const string OperationReceive = "receive";

        /// <summary>Tag value for process operation.</summary>
        public const string OperationProcess = "process";

        // ────────────────────────────────────────────────
        // AMQP trace propagation headers
        // ────────────────────────────────────────────────

        /// <summary>
        /// W3C traceparent header injected into AMQP headers for distributed trace propagation.
        /// Format: <c>00-{trace-id}-{parent-id}-{trace-flags}</c>
        /// </summary>
        public const string TraceParentHeader = "traceparent";

        /// <summary>
        /// W3C tracestate header for vendor-specific trace state.
        /// </summary>
        public const string TraceStateHeader = "tracestate";
    }
}
