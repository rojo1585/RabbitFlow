using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{

    /// <summary>
    /// Contract for handling a specific event type from a specific consumer.
    /// Implementations are resolved from the DI container within a dedicated scope per message.
    /// The scope is disposed after the handler completes, ensuring proper cleanup of
    /// scoped services like DbContexts.
    /// </summary>
    /// <typeparam name="TEvent">
    /// The event type this handler processes.
    /// The type is automatically registered in the event type registry
    /// when the handler is discovered via DI.
    /// </typeparam>
    public interface IRabbitHandler<TEvent> where TEvent : class
    {
        /// <summary>
        /// Identifies which consumer configuration this handler belongs to.
        /// Must match a <see cref="Configuration.RabbitConsumerOptions.ServiceKey"/>
        /// defined in the RabbitMQ configuration.
        /// </summary>
        string ConsumerKey { get; }

        /// <summary>
        /// Processes the delivered event.
        /// Called within a fresh DI scope that is disposed after this method returns.
        /// </summary>
        /// <param name="event">The deserialized event instance.</param>
        /// <param name="context">
        /// Message metadata including correlation ID, retry count, and custom headers.
        /// </param>
        /// <param name="cancellationToken">
        /// </param>
        Task HandleAsync(TEvent @event, MessageContext context, CancellationToken cancellationToken);
    }
}
