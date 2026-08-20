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
    /// <example>
    /// <code>
    /// public class OrderCreatedHandler : IRabbitHandler&lt;OrderCreatedEvent&gt;
    /// {
    ///     public string ConsumerKey => "orders-consumer";
    ///     private readonly IMediator _mediator;
    ///     
    ///     public OrderCreatedHandler(IMediator mediator) => _mediator = mediator;
    ///     
    ///     public async Task HandleAsync(OrderCreatedEvent @event, MessageContext context)
    ///     {
    ///         // Access tracing info via context.CorrelationId, context.RetryCount, etc.
    ///         await _mediator.Send(new ProcessOrderCommand(@event.OrderId));
    ///     }
    /// }
    /// </code>
    /// </example>
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
        Task HandleAsync(TEvent @event, MessageContext context);
    }
}
