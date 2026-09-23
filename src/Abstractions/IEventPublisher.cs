using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{
    /// <summary>
    /// Publishes integration events to RabbitMQ.
    /// When only one producer is registered, the key parameter can be omitted.
    /// When multiple producers are registered, the key selects which one to use.
    /// </summary>
    public interface IEventPublisher
    {
        /// <summary>
        /// Publishes an event using the default producer (valid only when a single producer is registered).
        /// Throws <see cref="InvalidOperationException"/> when multiple producers are registered.
        /// </summary>
        /// <typeparam name="TEvent">The event type to publish. Must be a reference type.</typeparam>
        /// <param name="event">The event instance to serialize and publish.</param>
        /// <param name="routingKey">
        /// Optional routing key override. If null, uses the producer's default routing key.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when multiple producers are registered and no key is provided.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// Thrown when publisher confirms are enabled and the confirm is not received within the timeout.
        /// </exception>
        Task PublishAsync<TEvent>(TEvent @event, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class;

        /// <summary>
        /// Publishes an event using the default producer with an explicit correlation ID.
        /// Use this overload to propagate a correlation ID from an upstream context (e.g. an
        /// incoming HTTP request or a consumed message) so distributed tracing spans can be
        /// correlated across multi-hop service calls.
        /// </summary>
        /// <typeparam name="TEvent">The event type to publish. Must be a reference type.</typeparam>
        /// <param name="event">The event instance to serialize and publish.</param>
        /// <param name="correlationId">
        /// The correlation ID to attach to the message. Written to the AMQP
        /// <c>correlation-id</c> property and the <c>x-correlation-id</c> header.
        /// If null, a new GUID is generated (backward-compatible behavior).
        /// </param>
        /// <param name="routingKey">
        /// Optional routing key override. If null, uses the producer's default routing key.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when multiple producers are registered and no key is provided.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// Thrown when publisher confirms are enabled and the confirm is not received within the timeout.
        /// </exception>
        Task PublishAsync<TEvent>(TEvent @event, string? correlationId, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class;

        /// <summary>
        /// Publishes an event using a specific producer identified by <paramref name="producerKey"/>.
        /// </summary>
        /// <typeparam name="TEvent">The event type to publish. Must be a reference type.</typeparam>
        /// <param name="producerKey">
        /// The <see cref="Configuration.RabbitProducerOptions.ServiceKey"/> of the target producer.
        /// </param>
        /// <param name="event">The event instance to serialize and publish.</param>
        /// <param name="routingKey">
        /// Optional routing key override. If null, uses the producer's default routing key.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <exception cref="KeyNotFoundException">
        /// Thrown when no producer with the specified key is registered.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// Thrown when publisher confirms are enabled and the confirm is not received within the timeout.
        /// </exception>
        Task PublishAsync<TEvent>(string producerKey, TEvent @event, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class;

        /// <summary>
        /// Publishes an event using a specific producer with an explicit correlation ID.
        /// Use this overload to propagate a correlation ID from an upstream context.
        /// </summary>
        /// <typeparam name="TEvent">The event type to publish. Must be a reference type.</typeparam>
        /// <param name="producerKey">
        /// The <see cref="Configuration.RabbitProducerOptions.ServiceKey"/> of the target producer.
        /// </param>
        /// <param name="event">The event instance to serialize and publish.</param>
        /// <param name="correlationId">
        /// The correlation ID to attach to the message. If null, a new GUID is generated.
        /// </param>
        /// <param name="routingKey">
        /// Optional routing key override. If null, uses the producer's default routing key.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <exception cref="KeyNotFoundException">
        /// Thrown when no producer with the specified key is registered.
        /// </exception>
        /// <exception cref="TimeoutException">
        /// Thrown when publisher confirms are enabled and the confirm is not received within the timeout.
        /// </exception>
        Task PublishAsync<TEvent>(string producerKey, TEvent @event, string? correlationId, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class;
    }
}
