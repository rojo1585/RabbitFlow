using RedRabbit.Exceptions;

namespace RedRabbit.Abstractions;

/// <summary>
/// Publishes integration events to RabbitMQ.
/// When only one producer is registered, the producer key can be omitted.
/// When multiple producers are registered, <see cref="PublishOptions.ProducerKey"/>
/// selects which producer to use.
/// </summary>
public interface IEventPublisher
{
    /// <summary>
    /// Publishes an event to RabbitMQ.
    /// </summary>
    /// <typeparam name="TEvent">The event type to publish. Must be a reference type.</typeparam>
    /// <param name="event">The event instance to serialize and publish.</param>
    /// <param name="options">
    /// Optional publish settings:
    /// <list type="bullet">
    ///   <item><see cref="PublishOptions.ProducerKey"/> — target a specific producer (required when multiple are registered).</item>
    ///   <item><see cref="PublishOptions.CorrelationId"/> — attach a correlation ID for distributed tracing (auto-generated if null).</item>
    ///   <item><see cref="PublishOptions.RoutingKey"/> — override the producer's default routing key.</item>
    /// </list>
    /// If null, uses the default producer with auto-generated correlation ID and default routing key.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="AmbiguousProducerException">
    /// Thrown when <see cref="PublishOptions.ProducerKey"/> is null and zero or multiple producers are registered.
    /// </exception>
    /// <exception cref="ProducerNotFoundException">
    /// Thrown when <see cref="PublishOptions.ProducerKey"/> references an unknown producer.
    /// </exception>
    /// <exception cref="PublisherNackException">
    /// Thrown when publisher confirms are enabled and the broker nacks or returns the message.
    /// </exception>
    /// <exception cref="PublisherConfirmTimeoutException">
    /// Thrown when publisher confirms are enabled and the broker does not confirm within the configured timeout.
    /// </exception>
    Task PublishAsync<TEvent>(TEvent @event, PublishOptions? options = null, CancellationToken cancellationToken = default) where TEvent : class;
}