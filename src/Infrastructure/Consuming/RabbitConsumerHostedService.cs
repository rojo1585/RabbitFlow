using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Infrastructure.Connection;
using RabbitFlow.Infrastructure.Versioning;

namespace RabbitFlow.Infrastructure.Consuming;



/// <summary>
/// <see cref="BackgroundService"/> that starts and manages all configured consumers.
/// Supports both individual (<see cref="NamedRabbitConsumer"/>) and batch
/// (<see cref="NamedBatchRabbitConsumer"/>) consumers based on configuration.
/// </summary>
public sealed class RabbitConsumerHostedService(IRabbitConnectionRegistry _connectionRegistry,
                                                HandlerTypeRegistry _handlerRegistry,
                                                IMessageSerializer _serializer,
                                                IServiceScopeFactory _scopeFactory,
                                                ILoggerFactory _loggerFactory,
                                                RabbitMqMetrics _metrics,
                                                EventUpgraderRegistry _upgraderRegistry,
                                                IOptions<RabbitMqSettings> _settings) : BackgroundService
{
    private readonly ILogger<RabbitConsumerHostedService> _logger = _loggerFactory.CreateLogger<RabbitConsumerHostedService>();
    private readonly List<RabbitConsumerOptions> _consumerConfigs = _settings.Value.Consumers;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_consumerConfigs.Count == 0)
        {
            _logger.LogInformation("No consumers configured — consumer hosted service idle.");
            return;
        }

        _handlerRegistry.Freeze();

        _logger.LogInformation("Starting {Count} consumer(s)...", _consumerConfigs.Count);

        var consumerTasks = new List<Task>(_consumerConfigs.Count);

        foreach (var config in _consumerConfigs)
        {
            var connection = _connectionRegistry.GetConnection(config.ConnectionName);

            if (config.EnableBatchConsumer)
            {
                _logger.LogInformation("[Consumer:{Key}] Using BATCH mode (size={BatchSize}, timeout={TimeoutMs}ms) on queue '{Queue}'",config.ServiceKey, config.BatchSize, config.BatchTimeoutMs, config.QueueName);

                var batchConsumer = new NamedBatchRabbitConsumer(
                    consumerKey: config.ServiceKey,
                    options: config,
                    connection: connection,
                    registry: _handlerRegistry,
                    serializer: _serializer,
                    scopeFactory: _scopeFactory,
                    logger: _loggerFactory.CreateLogger<NamedBatchRabbitConsumer>(),
                    metrics: _metrics);

                consumerTasks.Add(batchConsumer.RunAsync(stoppingToken));
            }
            else
            {
                _logger.LogInformation("[Consumer:{Key}] Using INDIVIDUAL mode on queue '{Queue}'",config.ServiceKey, config.QueueName);

                var consumer = new NamedRabbitConsumer(
                    consumerKey: config.ServiceKey,
                    options: config,
                    connection: connection,
                    registry: _handlerRegistry,
                    serializer: _serializer,
                    scopeFactory: _scopeFactory,
                    logger: _loggerFactory.CreateLogger<NamedRabbitConsumer>(),
                    metrics: _metrics,
                    upgraderRegistry: _upgraderRegistry);

                consumerTasks.Add(consumer.RunAsync(stoppingToken));
            }
        }

        await Task.WhenAll(consumerTasks).ConfigureAwait(false);

        _logger.LogInformation("All consumers stopped.");
    }
}