using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions
{
    /// <summary>
    /// Thrown internally when no handler is found for a consumed message.
    /// This is logged as a warning — it does not propagate to the caller.
    /// </summary>
    public sealed class HandlerNotFoundException(string consumerKey, string routingKey, Type? eventType = null) : RabbitMqException($"No handler registered for consumer '{consumerKey}' " +
                   $"with routing key '{routingKey}'" +
                   (eventType != null ? $" and event type '{eventType.FullName}'" : "") + ".")
    {
        /// <summary>
        /// The consumer key that was used to attempt to resolve a handler.
        /// </summary>
        public string ConsumerKey { get; } = consumerKey;
        /// <summary>
        /// The routing key that was used to attempt to resolve a handler.
        /// </summary>
        public string RoutingKey { get; } = routingKey;
        /// <summary>
        /// The event type that was used to attempt to resolve a handler, if any.
        /// </summary>
        public Type? EventType { get; } = eventType;
    }
}
