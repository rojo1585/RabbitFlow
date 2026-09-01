using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Extensions;


/// <summary>
/// Extension methods for registering Apymsa.RabbitMQ services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers all Apymsa.RabbitMQ services from <see cref="IConfiguration"/>.
    /// </summary>
    public static IServiceCollection AddApymsaRabbitMQ(this IServiceCollection services, IConfiguration configuration, string? sectionName = null, Action<RabbitMqBuilder>? configure = null)
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
    /// Registers all Apymsa.RabbitMQ services with code-based configuration.
    /// </summary>
    public static IServiceCollection AddApymsaRabbitMQ(this IServiceCollection services, Action<RabbitMqSettings> configureSettings, Action<RabbitMqBuilder>? configureBuilder = null)
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
