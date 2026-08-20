using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{
    /// <summary>
    /// Publishes batches of events to RabbitMQ for high-throughput scenarios.
    /// Reuses the same channel for the entire batch, reducing overhead.
    /// </summary>
    public interface IBatchEventPublisher
    {
        /// <summary>
        /// Publishes a batch of events using the default producer.
        /// Throws <see cref="InvalidOperationException"/> when multiple producers are registered.
        /// </summary>
        /// <typeparam name="TEvent">The event type. Must be a reference type.</typeparam>
        /// <param name="events">The collection of events to publish.</param>
        /// <param name="routingKey">
        /// Optional routing key override. If null, uses the producer's default.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task PublishBatchAsync<TEvent>(IEnumerable<TEvent> events, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class;

        /// <summary>
        /// Publishes a batch of events using a specific producer.
        /// </summary>
        /// <typeparam name="TEvent">The event type. Must be a reference type.</typeparam>
        /// <param name="producerKey">
        /// The <see cref="Configuration.RabbitProducerOptions.ServiceKey"/> of the target producer.
        /// </param>
        /// <param name="events">The collection of events to publish.</param>
        /// <param name="routingKey">
        /// Optional routing key override. If null, uses the producer's default.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task PublishBatchAsync<TEvent>(string producerKey, IEnumerable<TEvent> events, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class;
    }
}
