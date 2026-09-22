using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitFlow.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Infrastructure.Connection;


/// <summary>
/// Background service that starts all managed connections at application startup.
/// 
/// <para>
/// This is registered as a hosted service so that:
/// <list type="bullet">
///   <item>Connections start in the background (don't block app startup)</item>
///   <item>If RabbitMQ is down at startup, the app still starts and the connections
///   will establish when RabbitMQ becomes available.</item>
///   <item>On graceful shutdown, <see cref="StopAsync"/> triggers connection cleanup.</item>
/// </list>
/// </para>
/// 
/// <para>
/// Execution order: This hosted service should start BEFORE consumer hosted services.
/// The <see cref="ServiceCollectionExtensions"/>
/// <c>StartAsync</c> dependencies.
/// </para>
/// </summary>
internal sealed class ConnectionInitializerHostedService(IRabbitConnectionRegistry registry, ILogger<ConnectionInitializerHostedService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Initializing RabbitMQ connections...");

        try
        {
            registry.StartAll();
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Fatal error starting RabbitMQ connections — application will not start");
            throw;
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("RabbitMQ connection initializer stopping");

        return Task.CompletedTask;
    }
}
