using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Consuming;
using RabbitFlow.Infrastructure.Publishing;
using RabbitFlow.Infrastructure.Serializartion;
using RabbitFlow.Infrastructure.Versioning;
using System;
using System.Collections.Generic;
using System.Text;
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
    internal void Build()
    {
        // Resolve the instrumentation name: builder override > settings > default
        var instrName = _instrumentationName ?? _settings.InstrumentationName ?? RabbitMqSettings.DefaultInstrumentationName;

        // 1. Configuration
        _services.Configure<RabbitMqSettings>(settings =>
        {
            settings.Connections = _settings.Connections;
            settings.Producers = _settings.Producers;
            settings.Consumers = _settings.Consumers;
            settings.InstrumentationName = instrName;
        });

        // 2. Diagnostics — initialize ActivitySource + register Metrics 
        RabbitMqActivitySource.Initialize(instrName);
        _services.AddSingleton(new RabbitMqMetrics(instrName));

        // 3. Connection Registry
        _services.AddSingleton<RabbitConnectionRegistry>();
        _services.AddSingleton<IRabbitConnectionRegistry>(sp =>
            sp.GetRequiredService<RabbitConnectionRegistry>());

        // 4. Handler Registry
        _services.AddSingleton<HandlerTypeRegistry>();

        // 4b. Event Upgrader Registry
        _services.AddSingleton<EventUpgraderRegistry>();

        // 5. Serializer
        RegisterSerializer();

        // 6. Publishers
        _services.AddSingleton<CompositeEventPublisher>(sp =>
        {
            var settings = sp.GetRequiredService<IOptions<RabbitMqSettings>>().Value;
            var registry = sp.GetRequiredService<RabbitConnectionRegistry>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var metrics = sp.GetRequiredService<RabbitMqMetrics>();
            return new CompositeEventPublisher(registry, serializer, settings.Producers, loggerFactory, metrics);
        });
        _services.AddSingleton<IEventPublisher>(sp =>
            sp.GetRequiredService<CompositeEventPublisher>());
        _services.AddSingleton<IBatchEventPublisher>(sp =>
            sp.GetRequiredService<CompositeEventPublisher>());

        // 7. Hosted Services
        _services.AddHostedService<ConnectionInitializerHostedService>();
        _services.AddHostedService<RabbitConsumerHostedService>();

        // 8. Validation
        ValidateConfiguration();
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

    private void ValidateConfiguration()
    {
        foreach (var producer in _settings.Producers)
        {
            if (!_settings.Connections.ContainsKey(producer.ConnectionName))
                throw Exceptions.RabbitMqConfigurationException.MissingConnection($"Producer '{producer.ServiceKey}'", producer.ConnectionName);
        }

        foreach (var consumer in _settings.Consumers)
        {
            if (!_settings.Connections.ContainsKey(consumer.ConnectionName))
                throw Exceptions.RabbitMqConfigurationException.MissingConnection($"Consumer '{consumer.ServiceKey}'", consumer.ConnectionName);
        }
    }

    private void AssertSerializerNotConfigured()
    {
        if (_customSerializerType is not null || _customSerializerInstance is not null || _jsonOptionsConfigure is not null)
            throw new InvalidOperationException("A serializer is already configured. Call WithSerializer only once.");
    }
}