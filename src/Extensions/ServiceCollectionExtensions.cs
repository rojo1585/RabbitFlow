using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Extensions;

/// <summary>
/// Extension methods for registering RabbitMQ services.
/// </summary>
public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Registers all RabbitMQ services from <see cref="IConfiguration"/>.
        /// </summary>
        /// <param name="configuration">The application configuration.</param>
        /// <param name="sectionName">
        /// Configuration section name. Defaults to <see cref="RabbitMqSettings.SectionName"/> (<c>"RabbitMQ"</c>).
        /// </param>
        /// <param name="configure">
        /// Optional builder callback for serializer, instrumentation name, etc.
        /// </param>
        /// <returns>The service collection for chaining.</returns>
        /// <example>
        /// <code>
        /// // Minimal — reads from "RabbitMQ" section in appsettings.json
        /// services.AddRabbitMQ(builder.Configuration);
        /// 
        /// // With OTel instrumentation name override
        /// services.AddRabbitMQ(builder.Configuration)
        ///     .WithInstrumentationName("Some.RabbitMQ");
        /// 
        /// // With OTel configuration
        /// services.AddOpenTelemetry()
        ///     .WithTracing(t => t.AddSource(RabbitMqActivitySource.SourceName))
        ///     .WithMetrics(m => m.AddMeter(RabbitMqMetrics.MeterName));
        /// </code>
        /// </example>
        public IServiceCollection AddRabbitMQ(IConfiguration configuration, string? sectionName = null, Action<RabbitMqBuilder>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configuration);

            var section = configuration.GetSection(sectionName ?? RabbitMqSettings.SectionName);
            var settings = new RabbitMqSettings();
            section.Bind(settings);

            var builder = new RabbitMqBuilder(services, settings);
            configure?.Invoke(builder);
            builder.Build();

            return services;
        }

        /// <summary>
        /// Registers all RabbitMQ services with code-based configuration.
        /// </summary>
        /// <param name="configureSettings">Action to configure <see cref="RabbitMqSettings"/> programmatically.</param>
        /// <param name="configureBuilder">
        /// Optional builder callback for serializer, instrumentation name, etc.
        /// </param>
        /// <returns>The service collection for chaining.</returns>
        /// <example>
        /// <code>
        /// services.AddRabbitMQ(settings =>
        /// {
        ///     settings.Connections["main"] = new RabbitConnectionOptions
        ///     {
        ///         HostName = "localhost",
        ///         UserName = "guest",
        ///         Password = "guest"
        ///     };
        ///     settings.Producers.Add(new RabbitProducerOptions
        ///     {
        ///         ServiceKey = "orders",
        ///         ConnectionName = "main",
        ///         ExchangeName = "orders"
        ///     });
        /// }, builder => builder
        ///     .WithInstrumentationName("some.RabbitMQ"));
        /// </code>
        /// </example>
        public IServiceCollection AddRabbitMQ(Action<RabbitMqSettings> configureSettings, Action<RabbitMqBuilder>? configureBuilder = null)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(configureSettings);

            var settings = new RabbitMqSettings();
            configureSettings(settings);

            var builder = new RabbitMqBuilder(services, settings);
            configureBuilder?.Invoke(builder);
            builder.Build();

            return services;
        }
    }
}