using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedRabbit.Abstractions;
using RedRabbit.Configuration;
using RedRabbit.Diagnostics;
using RedRabbit.Infrastructure.Connection;
using RedRabbit.Infrastructure.Versioning;

namespace RedRabbit.Infrastructure.Consuming;

/// <summary>
/// <see cref="BackgroundService"/> that starts and manages all configured consumers.
/// Supports both individual (<see cref="NamedRabbitConsumer"/>) and batch
/// (<see cref="NamedBatchRabbitConsumer"/>) consumers based on configuration.
/// </summary>
internal sealed class RabbitConsumerHostedService(RabbitConnectionRegistry _connectionRegistry,
                                                HandlerTypeRegistry _handlerRegistry,
                                                IMessageSerializer _serializer,
                                                IServiceScopeFactory _scopeFactory,
                                                ILoggerFactory _loggerFactory,
                                                RabbitMqMetrics _metrics,
                                                EventUpgraderRegistry _upgraderRegistry,
                                                IOptions<RabbitMqSettings> _settings,
                                                IServiceProvider _serviceProvider) : BackgroundService
{
    private readonly ILogger<RabbitConsumerHostedService> _logger = _loggerFactory.CreateLogger<RabbitConsumerHostedService>();
    private readonly List<RabbitConsumerOptions> _consumerConfigs = _settings.Value.Consumers;
    /// <summary>
    /// Executes the hosted service, starting all configured consumers and awaiting their completion.
    /// </summary>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_consumerConfigs.Count == 0)
        {
            _logger.LogInformation("No consumers configured — consumer hosted service idle.");
            return;
        }

        _handlerRegistry.Freeze();

        _logger.LogInformation("Starting {Count} consumer(s)...", _consumerConfigs.Count);

        var consumers = new List<IAsyncDisposable>(_consumerConfigs.Count);
        var consumerTasks = new List<Task>(_consumerConfigs.Count);

        var hostShutdownTimeout = ShutdownBudget.GetHostShutdownTimeout(_serviceProvider);

        foreach (var configured in _consumerConfigs)
        {
            var config = ApplyShutdownBudget(configured, hostShutdownTimeout);
            var connection = _connectionRegistry.GetConnection(config.ConnectionName);

            if (config.EnableBatchConsumer)
            {
                _logger.LogInformation("[Consumer:{Key}] Using BATCH mode (size={BatchSize}, timeout={TimeoutMs}ms) on queue '{Queue}'", config.ServiceKey, config.BatchSize, config.BatchTimeoutMs, config.QueueName);

                var batchConsumer = new NamedBatchRabbitConsumer(
                    consumerKey: config.ServiceKey,
                    options: config,
                    connection: connection,
                    registry: _handlerRegistry,
                    serializer: _serializer,
                    scopeFactory: _scopeFactory,
                    logger: _loggerFactory.CreateLogger<NamedBatchRabbitConsumer>(),
                    metrics: _metrics,
                    upgraderRegistry: _upgraderRegistry);

                consumers.Add(batchConsumer);
                consumerTasks.Add(batchConsumer.RunAsync(stoppingToken));
            }
            else
            {
                _logger.LogInformation("[Consumer:{Key}] Using INDIVIDUAL mode on queue '{Queue}'", config.ServiceKey, config.QueueName);

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

                consumers.Add(consumer);
                consumerTasks.Add(consumer.RunAsync(stoppingToken));
            }
        }

        try
        {
            await Task.WhenAll(consumerTasks).ConfigureAwait(false);

            _logger.LogInformation("All consumers stopped.");
        }
        finally
        {
            await Task.WhenAll(consumers.Select(DisposeConsumerAsync)).ConfigureAwait(false);
        }
    }

    private async Task DisposeConsumerAsync(IAsyncDisposable consumer)
    {
        try
        {
            await consumer.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing consumer during shutdown.");
        }
    }

    /// <summary>
    /// Returns <paramref name="config"/> with its <see cref="RabbitConsumerOptions.ShutdownDrainTimeout"/>
    /// capped to what fits in the host's shutdown timeout (see <see cref="ShutdownBudget"/>).
    /// </summary>
    private RabbitConsumerOptions ApplyShutdownBudget(RabbitConsumerOptions config, TimeSpan hostShutdownTimeout)
    {
        var effective = ShutdownBudget.GetEffectiveDrainTimeout(config.ShutdownDrainTimeout, hostShutdownTimeout);
        if (effective == config.ShutdownDrainTimeout)
            return config;

        _logger.LogWarning("[Consumer:{Key}] ShutdownDrainTimeout ({Configured}s) does not fit in the host shutdown timeout ({HostTimeout}s) minus {Reserve}s reserved for closing; using {Effective}s. Increase HostOptions.ShutdownTimeout to allow longer drains.",
            config.ServiceKey, config.ShutdownDrainTimeout.TotalSeconds, hostShutdownTimeout.TotalSeconds, ShutdownBudget.PostDrainReserve.TotalSeconds, effective.TotalSeconds);

        return config with { ShutdownDrainTimeout = effective };
    }
}