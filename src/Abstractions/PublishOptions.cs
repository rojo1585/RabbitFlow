namespace RedRabbit.Abstractions;

/// <summary>
/// Optional settings for a publish operation. Pass to <see cref="IEventPublisher.PublishAsync{TEvent}"/>
/// to override the producer's defaults (routing key, correlation ID) or target a specific producer.
/// </summary>
/// <param name="ProducerKey">
/// The <see cref="Configuration.RabbitProducerOptions.ServiceKey"/> of the target producer.
/// If null, uses the default producer (valid only when a single producer is registered;
/// throws <see cref="Exceptions.AmbiguousProducerException"/> if zero or multiple producers are registered).
/// </param>
/// <param name="CorrelationId">
/// The correlation ID to attach to the message. If null, a new GUID is generated.
/// Propagates across service boundaries for distributed tracing.
/// </param>
/// <param name="RoutingKey">
/// Optional routing key override. If null, uses the producer's default routing key.
/// </param>
public sealed record PublishOptions(string? ProducerKey = null, string? CorrelationId = null, string? RoutingKey = null);