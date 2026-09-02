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
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace RabbitFlow.Extensions;


/// <summary>
/// Fluent builder for configuring RabbitMQ services.
/// 
/// <para>
/// Obtained via <c>services.AddRabbitMQ(...)</c>. All configuration
/// methods return <c>this</c> for chaining.
/// </para>
/// 
/// <para>
/// The builder does NOT register services immediately. Registration happens
/// when the owning <c>AddRabbitMQ</c> extension method calls
/// <see cref="Build"/> internally.
/// </para>
/// </summary>
/// <example>
/// <code>
/// services.AddRabbitMQ(configuration, builder => builder
///     .WithJsonSerializer(options =>
///     {
///         options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
///     })
///     .WithSerializer&lt;MyProtobufSerializer&gt;());
/// </code>
/// </example>
public sealed class RabbitMqBuilder
{
    private readonly IServiceCollection _services;
    private readonly RabbitMqSettings _settings;
    private Type? _customSerializerType;
    private IMessageSerializer? _customSerializerInstance;
    private Action<JsonSerializerOptions>? _jsonOptionsConfigure;

    internal RabbitMqBuilder(IServiceCollection services, RabbitMqSettings settings)
    {
        _services = services;
        _settings = settings;
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
    /// Registers all Some.RabbitMQ services into DI.
    /// Called internally by <c>AddRabbitMQ</c>.
    /// </summary>
    internal void Build()
    {
        // 1. Configuration
        _services.Configure<RabbitMqSettings>(settings =>
        {
            settings.Connections = _settings.Connections;
            settings.Producers = _settings.Producers;
            settings.Consumers = _settings.Consumers;
        });

        // 2. Connection Registry
        _services.AddSingleton<RabbitConnectionRegistry>();
        _services.AddSingleton<IRabbitConnectionRegistry>(sp =>
            sp.GetRequiredService<RabbitConnectionRegistry>());

        // 3. Handler Registry
        _services.AddSingleton<HandlerTypeRegistry>();

        // 4. Serializer
        RegisterSerializer();

        // 5. Diagnostics (Metrics + Tracing)
        _services.AddSingleton<RabbitMqMetrics>();

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

        // 7. Hosted Services (order matters)
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
            throw new InvalidOperationException(
                "A serializer is already configured. Call WithSerializer only once.");
    }
}