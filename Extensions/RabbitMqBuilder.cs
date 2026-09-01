using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
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
/// Fluent builder for configuring Apymsa.RabbitMQ services.
/// 
/// <para>
/// Obtained via <c>services.AddApymsaRabbitMQ(...)</c>. All configuration
/// methods return <c>this</c> for chaining.
/// </para>
/// 
/// <para>
/// The builder does NOT register services immediately. Registration happens
/// when the owning <c>AddApymsaRabbitMQ</c> extension method calls
/// <see cref="Build"/> internally.
/// </para>
/// </summary>
/// <example>
/// <code>
/// services.AddApymsaRabbitMQ(configuration, builder => builder
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
    /// Registers a custom <see cref="IMessageSerializer"/> implementation.
    /// The implementation must have a public parameterless constructor.
    /// </summary>
    /// <typeparam name="TSerializer">
    /// The serializer type. Must implement <see cref="IMessageSerializer"/> and
    /// have a public parameterless constructor.
    /// </typeparam>
    /// <returns>This builder for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if a serializer was already configured via another method.
    /// </exception>
    public RabbitMqBuilder WithSerializer<TSerializer>() where TSerializer : class, IMessageSerializer
    {
        if (_customSerializerType is not null || _customSerializerInstance is not null || _jsonOptionsConfigure is not null)
            throw new InvalidOperationException("A serializer is already configured. Call WithSerializer only once.");

        _customSerializerType = typeof(TSerializer);
        return this;
    }

    /// <summary>
    /// Registers a custom <see cref="IMessageSerializer"/> instance.
    /// Use this when the serializer requires constructor parameters or external setup.
    /// </summary>
    /// <param name="serializer">The serializer instance to use.</param>
    /// <returns>This builder for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if a serializer was already configured via another method.
    /// </exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="serializer"/> is null.</exception>
    public RabbitMqBuilder WithSerializer(IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        if (_customSerializerType is not null || _customSerializerInstance is not null || _jsonOptionsConfigure is not null)
            throw new InvalidOperationException("A serializer is already configured. Call WithSerializer only once.");

        _customSerializerInstance = serializer;
        return this;
    }

    /// <summary>
    /// Configures the default <see cref="SystemTextJsonSerializer"/> with custom
    /// <see cref="JsonSerializerOptions"/>. The serializer is registered as a Singleton.
    /// </summary>
    /// <param name="configure">
    /// A delegate to customize the <see cref="JsonSerializerOptions"/>.
    /// The options are pre-initialized with <c>CamelCase</c> naming and
    /// <c>JsonIgnoreCondition.WhenWritingNull</c>.
    /// </param>
    /// <returns>This builder for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if a serializer was already configured via another method.
    /// </exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure"/> is null.</exception>
    public RabbitMqBuilder WithJsonSerializer(Action<JsonSerializerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        if (_customSerializerType is not null || _customSerializerInstance is not null || _jsonOptionsConfigure is not null)
            throw new InvalidOperationException("A serializer is already configured. Call WithSerializer only once.");

        _jsonOptionsConfigure = configure;
        return this;
    }

    /// <summary>
    /// Registers all Apymsa.RabbitMQ services into the <see cref="IServiceCollection"/>.
    /// Called internally by <c>AddApymsaRabbitMQ</c> after the user's configuration delegate.
    /// 
    /// <para>
    /// Registration order (matters for hosted service startup sequence):
    /// <list type="number">
    ///   <item><c>RabbitMqSettings</c> — IOptions configuration binding.</item>
    ///   <item><c>RabbitConnectionRegistry</c> — Singleton (also as <c>IRabbitConnectionRegistry</c>).</item>
    ///   <item><c>HandlerTypeRegistry</c> — Singleton, collects all <c>HandlerRegistration</c> from DI.</item>
    ///   <item><c>IMessageSerializer</c> — Default or custom serializer as Singleton.</item>
    ///   <item><c>CompositeEventPublisher</c> — Singleton (as <c>IEventPublisher</c> + <c>IBatchEventPublisher</c>).</item>
    ///   <item><c>ConnectionInitializerHostedService</c> — Starts connections (registered FIRST).</item>
    ///   <item><c>RabbitConsumerHostedService</c> — Starts consumers (registered SECOND).</item>
    /// </list>
    /// </para>
    /// </summary>
    internal void Build()
    {
        _services.Configure<RabbitMqSettings>(settings =>
        {
            settings.Connections = _settings.Connections;
            settings.Producers = _settings.Producers;
            settings.Consumers = _settings.Consumers;
        });

        _services.AddSingleton<RabbitConnectionRegistry>();
        _services.AddSingleton<IRabbitConnectionRegistry>(sp => sp.GetRequiredService<RabbitConnectionRegistry>());

        _services.AddSingleton<HandlerTypeRegistry>();

        RegisterSerializer();

        _services.AddSingleton<CompositeEventPublisher>(sp =>
        {
            var settings = sp.GetRequiredService<IOptions<RabbitMqSettings>>().Value;
            var registry = sp.GetRequiredService<RabbitConnectionRegistry>();
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            return new CompositeEventPublisher(registry, serializer, settings.Producers, loggerFactory);
        });
        _services.AddSingleton<IEventPublisher>(sp => sp.GetRequiredService<CompositeEventPublisher>());
        _services.AddSingleton<IBatchEventPublisher>(sp => sp.GetRequiredService<CompositeEventPublisher>());

        _services.AddHostedService<ConnectionInitializerHostedService>();
        _services.AddHostedService<RabbitConsumerHostedService>();

        ValidateConfiguration();
    }

    /// <summary>
    /// Registers the message serializer based on builder configuration.
    /// Priority: custom instance > custom type > JSON with options > default JSON.
    /// </summary>
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

    /// <summary>
    /// Validates that all connection references in producers and consumers
    /// point to existing connections defined in the settings.
    /// </summary>
    /// <exception cref="Exceptions.RabbitMqConfigurationException">
    /// Thrown when a producer or consumer references a non-existent connection.
    /// </exception>
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
}
