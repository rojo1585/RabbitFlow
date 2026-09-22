using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Consuming;
using RabbitFlow.Infrastructure.Publishing;
using RabbitFlow.Infrastructure.Serialization;
using RabbitFlow.Infrastructure.Versioning;
using System.Text.Json;

namespace RabbitFlow.Extensions;



/// <summary>
/// Fluent builder for configuring RabbitMQ services.
/// Obtained via <c>services.AddRabbitMQ(...)</c>.
/// </summary>
public sealed class RabbitMqBuilder
{
    private readonly IServiceCollection _services;
    private readonly RabbitMqSettings _settings;
    private Type? _customSerializerType;
    private IMessageSerializer? _customSerializerInstance;
    private Action<JsonSerializerOptions>? _jsonOptionsConfigure;
    private string? _instrumentationName;

    /// <summary>
    /// Marker singleton used to detect multiple <c>AddRabbitMQ</c> calls on the same
    /// <see cref="IServiceCollection"/>. Private nested so it cannot be accidentally
    /// registered or removed by external code.
    /// </summary>
    private sealed class ConfiguredMarker { }

    internal RabbitMqBuilder(IServiceCollection services, RabbitMqSettings settings)
    {
        _services = services;
        _settings = settings;
    }

    /// <summary>
    /// Overrides the OpenTelemetry instrumentation name.
    /// This name is used for both the <see cref="System.Diagnostics.ActivitySource"/> (tracing)
    /// and the <see cref="System.Diagnostics.Metrics.Meter"/> (metrics).
    /// </summary>
    /// <param name="name">
    /// A unique name like <c>"Some.RabbitMQ"</c>.
    /// Must match what is passed to <c>AddSource()</c> and <c>AddMeter()</c>
    /// in the OpenTelemetry configuration.
    /// </param>
    /// <returns>This builder for chaining.</returns>
    /// <remarks>
    /// If not called, the value from <see cref="RabbitMqSettings.InstrumentationName"/>
    /// is used, falling back to <see cref="RabbitMqSettings.DefaultInstrumentationName"/>.
    /// </remarks>
    /// <example>
    /// <code>
    /// services.AddRabbitMQ(configuration)
    ///     .WithInstrumentationName("Some.RabbitMQ");
    /// 
    /// services.AddOpenTelemetry()
    ///     .WithTracing(t => t.AddSource("Some.RabbitMQ"))
    ///     .WithMetrics(m => m.AddMeter("Some.RabbitMQ"));
    /// </code>
    /// </example>
    public RabbitMqBuilder WithInstrumentationName(string name)
    {
        _instrumentationName = name ?? throw new ArgumentNullException(nameof(name));
        return this;
    }

    /// <summary>
    /// Registers a custom <see cref="IMessageSerializer"/> by type.
    /// </summary>
    public RabbitMqBuilder WithSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        AssertSerializerNotConfigured();
        _customSerializerType = typeof(TSerializer);
        return this;
    }

    /// <summary>
    /// Registers a custom <see cref="IMessageSerializer"/> instance.
    /// </summary>
    public RabbitMqBuilder WithSerializer(IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        AssertSerializerNotConfigured();
        _customSerializerInstance = serializer;
        return this;
    }

    /// <summary>
    /// Configures the default <see cref="SystemTextJsonSerializer"/> with custom options.
    /// </summary>
    public RabbitMqBuilder WithJsonSerializer(Action<JsonSerializerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        AssertSerializerNotConfigured();
        _jsonOptionsConfigure = configure;
        return this;
    }

    /// <summary>
    /// Registers all RabbitMQ services into DI.
    /// Called internally by <c>AddRabbitMQ</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <c>AddRabbitMQ</c> has already been called on the same <see cref="IServiceCollection"/>.
    /// Multiple calls are not supported because they would silently overwrite the first call's
    /// connections/producers/consumers, replace the <c>HandlerTypeRegistry</c> (losing registered
    /// handlers), and leak the first <c>RabbitMqMetrics</c> instance. Multi-broker support is
    /// achieved within a single <c>AddRabbitMQ</c> call by adding multiple entries to the
    /// <c>Connections</c> dictionary.
    /// </exception>
    internal void Build()
    {
        if (_services.Any(d => d.ServiceType == typeof(ConfiguredMarker)))
        {
            throw new InvalidOperationException(
                "AddRabbitMQ has already been called on this service collection. " +
                "Multiple AddRabbitMQ calls are not supported because they would silently " +
                "overwrite the first call's configuration, replace the HandlerTypeRegistry " +
                "(losing registered handlers), and leak the first RabbitMqMetrics instance. " +
                "To connect to multiple brokers, vhosts, or clusters, add multiple entries " +
                "to the Connections dictionary in a single AddRabbitMQ call instead.");
        }

        _services.AddSingleton<ConfiguredMarker>();

        // Add logging services if not already registered
        _services.TryAddSingleton<ILoggerFactory, NullLoggerFactory>();
        _services.TryAdd(ServiceDescriptor.Singleton(typeof(ILogger<>), typeof(NullLogger<>)));

        // Resolve the instrumentation name: builder override > settings > default
        var instrName = _instrumentationName
            ?? _settings.InstrumentationName
            ?? RabbitMqSettings.DefaultInstrumentationName;

        // 1. Configuration (include InstrumentationName so IOptions works)
        _services.AddOptions<RabbitMqSettings>()
            .Configure(settings =>
            {
                settings.Connections = _settings.Connections;
                settings.Producers = _settings.Producers;
                settings.Consumers = _settings.Consumers;
                settings.InstrumentationName = instrName;
            })
            .ValidateOnStart();

        _services.AddSingleton<IValidateOptions<RabbitMqSettings>, ValidateRabbitMqSettings>();

        // 2. Diagnostics — initialize ActivitySource (static) + register Metrics (DI)
        RabbitMqActivitySource.Initialize(instrName);
        _services.AddSingleton(new RabbitMqMetrics(instrName));

        // 3. Connection Registry

        _services.AddSingleton<IRabbitConnectionRegistry, RabbitConnectionRegistry>();

        // 4. Handler Registry
        _services.AddSingleton<HandlerTypeRegistry>();

        // 4b. Event Upgrader Registry (empty if no upgraders registered)
        _services.AddSingleton<EventUpgraderRegistry>();

        // 5. Serializer
        RegisterSerializer();

        // 6. Publishers
        _services.AddSingleton<CompositeEventPublisher>(sp =>
        {
            var settings = sp.GetRequiredService<IOptions<RabbitMqSettings>>().Value;
            var registry = sp.GetRequiredService<IRabbitConnectionRegistry>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var metrics = sp.GetRequiredService<RabbitMqMetrics>();
            return new CompositeEventPublisher(registry, serializer, settings.Producers, loggerFactory, metrics);
        });
        _services.AddSingleton<IEventPublisher>(sp => sp.GetRequiredService<CompositeEventPublisher>());
        _services.AddSingleton<IBatchEventPublisher>(sp => sp.GetRequiredService<CompositeEventPublisher>());

        // 7. Hosted Services (order matters)
        _services.AddHostedService<ConnectionInitializerHostedService>();
        _services.AddHostedService<RabbitConsumerHostedService>();

    }

    private void RegisterSerializer()
    {
        if (_customSerializerInstance is not null)
        {
            _services.AddSingleton<IMessageSerializer>(_customSerializerInstance);
            return;
        }

        if (_customSerializerType is not null)
        {
            _services.AddSingleton(typeof(IMessageSerializer), _customSerializerType);
            return;
        }

        if (_jsonOptionsConfigure is not null)
        {
            _services.AddSingleton<IMessageSerializer>(sp =>
            {
                var options = new JsonSerializerOptions();
                _jsonOptionsConfigure(options);
                return new SystemTextJsonSerializer(options);
            });
            return;
        }

        _services.AddSingleton<IMessageSerializer, SystemTextJsonSerializer>();
    }

    private void AssertSerializerNotConfigured()
    {
        if (_customSerializerType is not null || _customSerializerInstance is not null || _jsonOptionsConfigure is not null)
            throw new InvalidOperationException("A serializer is already configured. Call WithSerializer only once.");
    }
}