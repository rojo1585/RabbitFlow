using Microsoft.Extensions.Logging;
using RedRabbit.Abstractions;
using RedRabbit.Configuration;
using RedRabbit.Diagnostics;
using RedRabbit.Exceptions;
using RedRabbit.Infrastructure.Connection;

namespace RedRabbit.Infrastructure.Publishing;
/// <summary>
/// Facade that implements <see cref="IEventPublisher"/> and <see cref="IBatchEventPublisher"/>.
/// Routes publish calls to the correct <see cref="NamedRabbitPublisher"/> based on
/// <see cref="PublishOptions.ProducerKey"/>.
///
/// <para>
/// Resolution rules:
/// <list type="bullet">
///   <item>
///     <c>PublishAsync(event, options: null)</c> (or with <c>options.ProducerKey == null</c>):
///     Uses the single registered producer. Throws <see cref="AmbiguousProducerException"/>
///     if zero or multiple producers are registered.
///   </item>
///   <item>
///     <c>PublishAsync(event, options with ProducerKey)</c>: Uses the producer with the matching
///     key. Throws <see cref="ProducerNotFoundException"/> if the key doesn't exist.
///   </item>
/// </list>
/// </para>
/// 
/// <para>Registered as a Singleton in DI.</para>
/// </summary>
internal sealed class CompositeEventPublisher : IEventPublisher, IBatchEventPublisher, IAsyncDisposable
{
    private readonly Dictionary<string, NamedRabbitPublisher> _producers;
    private readonly ILogger<CompositeEventPublisher> _logger;
    private readonly NamedRabbitPublisher? _defaultProducer;
    private bool _disposed;

    public CompositeEventPublisher(RabbitConnectionRegistry connectionRegistry,
                                   IMessageSerializer serializer,
                                   IEnumerable<RabbitProducerOptions> producerConfigs,
                                   ILoggerFactory loggerFactory,
                                   RabbitMqMetrics metrics)
    {
        _logger = loggerFactory.CreateLogger<CompositeEventPublisher>();
        _producers = [];

        foreach (var config in producerConfigs)
        {
            _producers[config.ServiceKey] = CreatePublisher(config, connectionRegistry, serializer, loggerFactory, metrics);

            _logger.LogDebug("Registered producer '{Key}' → exchange '{Exchange}' on connection '{Connection}' (pool={PoolSize})", config.ServiceKey, config.ExchangeName, config.ConnectionName, config.ChannelPoolSize > 0 ? config.ChannelPoolSize.ToString() : "off");
        }

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
    private static NamedRabbitPublisher CreatePublisher(RabbitProducerOptions config,
                                                        RabbitConnectionRegistry connectionRegistry,
                                                        IMessageSerializer serializer,
                                                        ILoggerFactory loggerFactory,
                                                        RabbitMqMetrics metrics)
    {
        var connection = connectionRegistry.GetConnection(config.ConnectionName);
        return new NamedRabbitPublisher(
            producerKey: config.ServiceKey,
            options: config,
            connection: connection,
            serializer: serializer,
            logger: loggerFactory.CreateLogger<NamedRabbitPublisher>(),
            metrics: metrics);
    }

    /// <summary>
    /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(CompositeEventPublisher), "CompositeEventPublisher has been disposed. Publishing after disposal is not allowed.");
    }

    /// <inheritdoc/>
    public Task PublishAsync<TEvent>(TEvent @event, PublishOptions? options = null, CancellationToken cancellationToken = default) where TEvent : class
    {
        ThrowIfDisposed();
        var producerKey = options?.ProducerKey;
        var producer = producerKey is not null
            ? ResolveProducer(producerKey)
            : ResolveDefaultProducer();
        return producer.PublishAsync(@event, options?.CorrelationId, options?.RoutingKey, cancellationToken);
    }

    /// <inheritdoc/>
    public Task PublishBatchAsync<TEvent>(IEnumerable<TEvent> events, PublishOptions? options = null, CancellationToken cancellationToken = default) where TEvent : class
    {
        ThrowIfDisposed();
        var producerKey = options?.ProducerKey;
        var producer = producerKey is not null
            ? ResolveProducer(producerKey)
            : ResolveDefaultProducer();
        return producer.PublishBatchAsync(events, options?.RoutingKey, cancellationToken);
    }

    /// <summary>
    /// Disposes all publishers and their channel pools.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _logger.LogInformation("Disposing {Count} publisher(s)...", _producers.Count);

        var tasks = _producers.Values.Select(p => p.DisposeAsync().AsTask()).ToArray();
        await Task.WhenAll(tasks).ConfigureAwait(false);

        _producers.Clear();
    }
}