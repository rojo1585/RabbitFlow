using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{

    /// <summary>
    /// Wrapper structure serialized into the message body.
    /// Separates metadata (event type, version, tracing) from the payload,
    /// allowing the consumer to deserialize without prior knowledge of the type.
    /// </summary>
    public sealed class MessageEnvelope
    {
        /// <summary>
        /// The full type name of the event (e.g. "MyApp.Events.OrderCreatedEvent").
        /// Used by the consumer to resolve the correct <see cref="Type"/> from the registry.
        /// </summary>
        public required string EventType { get; init; }

        /// <summary>
        /// The event schema version. Defaults to 1.
        /// Used by the versioned event type registry to select the correct type.
        /// </summary>
        public int EventVersion { get; init; } = 1;

        /// <summary>
        /// Correlation ID for distributed tracing across services.
        /// </summary>
        public string? CorrelationId { get; init; }

        /// <summary>
        /// Unique message identifier.
        /// </summary>
        public string? MessageId { get; init; }

        /// <summary>
        /// UTC timestamp of when the message was published.
        /// </summary>
        public DateTime? PublishedAt { get; init; }

        /// <summary>
        /// The <see cref="Configuration.RabbitProducerOptions.ServiceKey"/> of the producer.
        /// </summary>
        public string? PublisherName { get; init; }

        /// <summary>
        /// The serialized event payload.
        /// Can be a JSON object, or a pre-serialized string depending on the serializer.
        /// </summary>
        public required object? Payload { get; init; }
    }
}
