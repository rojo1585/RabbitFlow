using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace RabbitFlow.Diagnostics;


/// <summary>
/// Central <see cref="ActivitySource"/> for all distributed tracing.
/// 
/// <para>
/// The <see cref="SourceName"/> and <see cref="Source"/> are initialized once during
/// <c>AddRabbitMQ()</c> via <see cref="Initialize"/>. Before initialization, defaults
/// to <see cref="Configuration.RabbitMqSettings.DefaultInstrumentationName"/>.
/// </para>
/// 
/// <para>
/// Tags follow the <see href="https://opentelemetry.io/docs/specs/semconv/messaging/">
/// OpenTelemetry Semantic Conventions for Messaging</see>.
/// </para>
/// 
/// <para>
/// <b>Usage in the host:</b>
/// <code>
/// // Configure RabbitMQ first so Initialize() runs
/// services.AddRabbitMQ(configuration);
/// 
/// // Then use the resolved name for OTel
/// services.AddOpenTelemetry()
///     .WithTracing(tracing => tracing
///         .AddSource(RabbitMqActivitySource.SourceName));
/// </code>
/// </para>
/// </summary>
public static class RabbitMqActivitySource
{
    private static readonly object Lock = new();

    /// <summary>
    /// The name used to register the <see cref="ActivitySource"/>.
    /// Consumers must pass this exact string to <c>AddSource()</c>.
    /// Set during <see cref="Initialize"/>.
    /// </summary>
    public static string SourceName { get; private set; } = Configuration.RabbitMqSettings.DefaultInstrumentationName;

    /// <summary>
    /// The singleton <see cref="ActivitySource"/> instance.
    /// All publisher and consumer activities are created through this.
    /// Replaced during <see cref="Initialize"/>.
    /// </summary>
    public static ActivitySource Source { get; private set; } = new(Configuration.RabbitMqSettings.DefaultInstrumentationName, "1.0.0");

    /// <summary>
    /// Whether <see cref="Initialize"/> has been called.
    /// </summary>
    public static bool IsInitialized { get; private set; }

    /// <summary>
    /// Initializes the <see cref="SourceName"/> and <see cref="Source"/> with a custom name.
    /// Called once internally by <c>AddRabbitMQ()</c>.
    /// Subsequent calls are ignored.
    /// </summary>
    /// <param name="name">
    /// The instrumentation name. Must match what is passed to <c>AddSource()</c>
    /// and <c>AddMeter()</c> in the OpenTelemetry configuration.
    /// </param>
    internal static void Initialize(string name)
    {
        if (IsInitialized) return;

        lock (Lock)
        {
            if (IsInitialized) return;

            SourceName = name;
            Source = new ActivitySource(name, "1.0.0");
            IsInitialized = true;
        }
    }

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