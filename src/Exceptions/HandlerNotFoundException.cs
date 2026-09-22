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

        public string ConsumerKey { get; } = consumerKey;
        public string RoutingKey { get; } = routingKey;
        public Type? EventType { get; } = eventType;
    }
}
