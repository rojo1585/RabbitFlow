using Microsoft.Extensions.Logging;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Exceptions;
using RabbitFlow.Infrastructure.Connection;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Infrastructure.Publishing;


/// <summary>
/// Facade that implements <see cref="IEventPublisher"/> and <see cref="IBatchEventPublisher"/>.
/// Routes publish calls to the correct <see cref="NamedRabbitPublisher"/> based on the producer key.
/// 
/// <para>
/// Resolution rules:
/// <list type="bullet">
///   <item>
///     <c>PublishAsync(event)</c> (no key): Uses the single registered producer.
///     Throws <see cref="AmbiguousProducerException"/> if zero or multiple producers are registered.
///   </item>
///   <item>
///     <c>PublishAsync(key, event)</c> (with key): Uses the producer with the matching key.
///     Throws <see cref="ProducerNotFoundException"/> if the key doesn't exist.
///   </item>
/// </list>
/// </para>
/// 
/// <para>Registered as a Singleton in DI.</para>
/// </summary>
internal sealed class CompositeEventPublisher : IEventPublisher, IBatchEventPublisher
{
    private readonly Dictionary<string, NamedRabbitPublisher> _producers;
    private readonly ILogger<CompositeEventPublisher> _logger;
    private readonly NamedRabbitPublisher? _defaultProducer;

    public CompositeEventPublisher(RabbitConnectionRegistry connectionRegistry, IMessageSerializer serializer, IEnumerable<RabbitProducerOptions> producerConfigs, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<CompositeEventPublisher>();
        _producers = [];

        foreach (var config in producerConfigs)
        {
            // Validate no duplicate ServiceKey — fail fast at startup, not at publish time
            if (!_producers.TryAdd(config.ServiceKey, CreatePublisher(config, connectionRegistry, serializer, loggerFactory)))
                throw Exceptions.RabbitMqConfigurationException.DuplicateProducerKey(config.ServiceKey);

            _logger.LogDebug("Registered producer '{Key}' → exchange '{Exchange}' on connection '{Connection}'", config.ServiceKey, config.ExchangeName, config.ConnectionName);
        }

        // Cache the single producer for fast default resolution (no enumerator allocation)
        if (_producers.Count == 1)
            _defaultProducer = _producers.Values.First();

        _logger.LogInformation("Composite publisher initialized with {Count} producer(s)", _producers.Count);
    }

    /// <summary>
    /// Resolves the default producer (the only one registered).
    /// Uses the cached field when available to avoid enumerator allocation.
    /// </summary>
    /// <exception cref="AmbiguousProducerException">
    /// Thrown when zero or multiple producers are registered.
    /// </exception>
    private NamedRabbitPublisher ResolveDefaultProducer()
    {
        if (_defaultProducer is not null)
            return _defaultProducer;

        if (_producers.Count == 0)
            throw new AmbiguousProducerException();

        throw new AmbiguousProducerException(_producers.Count, [.. _producers.Keys]);
    }

    /// <summary>
    /// Resolves a producer by key.
    /// </summary>
    /// <exception cref="ProducerNotFoundException">
    /// Thrown when no producer with the given key exists.
    /// </exception>
    private NamedRabbitPublisher ResolveProducer(string producerKey)
    {
        if (_producers.TryGetValue(producerKey, out var producer))
            return producer;

        throw new ProducerNotFoundException(producerKey);
    }

    /// <summary>
    /// Factory method to create a <see cref="NamedRabbitPublisher"/> from config.
    /// Separated for readability in the constructor loop.
    /// </summary>
    private static NamedRabbitPublisher CreatePublisher(RabbitProducerOptions config, RabbitConnectionRegistry connectionRegistry, IMessageSerializer serializer, ILoggerFactory loggerFactory)
    {
        var connection = connectionRegistry.GetConnection(config.ConnectionName);
        return new NamedRabbitPublisher(producerKey: config.ServiceKey, options: config, connection: connection, serializer: serializer, logger: loggerFactory.CreateLogger<NamedRabbitPublisher>());
    }

    // ─── IEventPublisher ──────────────────────────────────────────────

    /// <inheritdoc/>
    public Task PublishAsync<TEvent>(TEvent @event, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class
    {
        var producer = ResolveDefaultProducer();
        return producer.PublishAsync(@event, routingKey, cancellationToken);
    }

    /// <inheritdoc/>
    public Task PublishAsync<TEvent>(string producerKey, TEvent @event, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class
    {
        var producer = ResolveProducer(producerKey);
        return producer.PublishAsync(@event, routingKey, cancellationToken);
    }

    // ─── IBatchEventPublisher ────────────────────────────────────────

    /// <inheritdoc/>
    public Task PublishBatchAsync<TEvent>(IEnumerable<TEvent> events, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class
    {
        var producer = ResolveDefaultProducer();
        return producer.PublishBatchAsync(events, routingKey, cancellationToken);
    }

    /// <inheritdoc/>
    public Task PublishBatchAsync<TEvent>(string producerKey, IEnumerable<TEvent> events, string? routingKey = null, CancellationToken cancellationToken = default) where TEvent : class
    {
        var producer = ResolveProducer(producerKey);
        return producer.PublishBatchAsync(events, routingKey, cancellationToken);
    }
}