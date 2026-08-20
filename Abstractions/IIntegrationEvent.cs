using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{
    /// <summary>
    /// Marker interface for integration events published through RabbitMQ.
    /// Implement this interface on your event classes/records.
    /// <para>
    /// This interface has no members — it exists purely as a type constraint
    /// to distinguish integration events from other message types.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// public record OrderCreatedEvent(Guid OrderId, string CustomerEmail) : IIntegrationEvent;
    /// </code>
    /// </example>
    public interface IIntegrationEvent;
}
