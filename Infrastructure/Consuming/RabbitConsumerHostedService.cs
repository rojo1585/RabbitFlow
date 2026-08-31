using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Exceptions;
using RabbitFlow.Infrastructure.Connection;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Infrastructure.Consuming;


/// <summary>
/// <see cref="BackgroundService"/> that starts and manages all configured consumers.
///
/// <para>
/// Lifecycle:
/// <list type="number">
///   <item><see cref="ExecuteAsync"/> freezes the <see cref="HandlerTypeRegistry"/> and
///   starts one <see cref="NamedRabbitConsumer"/> per <see cref="RabbitConsumerOptions"/>.
///   Each consumer runs in its own background task.</item>
///   <item>All consumers run concurrently until the application shuts down.</item>
///   <item><see cref="StopAsync"/> signals cancellation and waits for all consumers to finish.</item>
/// </list>
/// </para>
///
/// <para>
/// Registered as a Singleton hosted service via
/// <c>services.AddHostedService&lt;RabbitConsumerHostedService&gt;()</c>.
/// </para>
/// </summary>
public sealed class RabbitConsumerHostedService(
    RabbitConnectionRegistry connectionRegistry,
    HandlerTypeRegistry handlerRegistry,
    IMessageSerializer serializer,
    IServiceScopeFactory scopeFactory,
    ILoggerFactory loggerFactory,
    IOptions<RabbitMqSettings> settings) : BackgroundService
{
    private readonly RabbitConnectionRegistry _connectionRegistry = connectionRegistry;
    private readonly HandlerTypeRegistry _handlerRegistry = handlerRegistry;
    private readonly IMessageSerializer _serializer = serializer;
    private readonly IServiceScopeFactory _scopeFactory = scopeFactory;
    private readonly ILoggerFactory _loggerFactory = loggerFactory;
    private readonly ILogger<RabbitConsumerHostedService> _logger = loggerFactory.CreateLogger<RabbitConsumerHostedService>();
    private readonly List<RabbitConsumerOptions> _consumerConfigs = settings.Value.Consumers;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_consumerConfigs.Count == 0)
        {
            _logger.LogInformation("No consumers configured — consumer hosted service idle.");
            return;
        }

        // Validate no duplicate ServiceKeys — fail fast at startup
        var duplicateKeys = _consumerConfigs.GroupBy(c => c.ServiceKey)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (duplicateKeys.Count > 0)
        {
            throw new RabbitMqConfigurationException($"Duplicate consumer ServiceKey(s): [{string.Join(", ", duplicateKeys)}]. " +
                "Each consumer must have a unique ServiceKey.");
        }

        // Freeze the registry — no more registrations after this point
        _handlerRegistry.Freeze();

        _logger.LogInformation("Starting {Count} consumer(s)...",_consumerConfigs.Count);

        // Start all consumers as parallel background tasks
        var consumerTasks = new List<Task>(_consumerConfigs.Count);

        foreach (var config in _consumerConfigs)
        {
            var consumer = new NamedRabbitConsumer(
                consumerKey: config.ServiceKey,
                options: config,
                connection: _connectionRegistry.GetConnection(config.ConnectionName),
                registry: _handlerRegistry,
                serializer: _serializer,
                scopeFactory: _scopeFactory,
                logger: _loggerFactory.CreateLogger<NamedRabbitConsumer>());

            // Each consumer runs its own reconnect loop
            consumerTasks.Add(consumer.RunAsync(stoppingToken));
        }

        // Wait for all consumers to complete (they stop when stoppingToken is cancelled)
        await Task.WhenAll(consumerTasks);

        _logger.LogInformation("All consumers stopped.");
    }
}
