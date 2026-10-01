using RabbitFlow.Exceptions;

namespace RabbitFlow.Abstractions;

/// <summary>
/// Publishes batches of events to RabbitMQ for high-throughput scenarios.
/// Reuses the same channel for the entire batch, reducing overhead.
/// </summary>
public interface IBatchEventPublisher
{
    /// <summary>
    /// Publishes a batch of events to RabbitMQ.
    /// </summary>
    /// <typeparam name="TEvent">The event type. Must be a reference type.</typeparam>
    /// <param name="events">The collection of events to publish.</param>
    /// <param name="options">
    /// Optional publish settings:
    /// <list type="bullet">
    ///   <item><see cref="PublishOptions.ProducerKey"/> — target a specific producer (required when multiple are registered).</item>
    ///   <item><see cref="PublishOptions.RoutingKey"/> — override the producer's default routing key.</item>
    /// </list>
    /// If null, uses the default producer with the default routing key.
    /// <see cref="PublishOptions.CorrelationId"/> is ignored for batch publishes (each message
    /// gets its own auto-generated correlation ID at the channel level).
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="AmbiguousProducerException">
    /// Thrown when <see cref="PublishOptions.ProducerKey"/> is null and zero or multiple producers are registered.
    /// </exception>
    /// <exception cref="ProducerNotFoundException">
    /// Thrown when <see cref="PublishOptions.ProducerKey"/> references an unknown producer.
    /// </exception>
    Task PublishBatchAsync<TEvent>(IEnumerable<TEvent> events, PublishOptions? options = null, CancellationToken cancellationToken = default) where TEvent : class;
}
