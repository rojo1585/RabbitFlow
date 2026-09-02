using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitFlow.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Infrastructure.HealthChecks;

/// <summary>
/// Reports the health of a single named RabbitMQ connection.
/// 
/// <para>
/// Health status:
/// <list type="bullet">
///   <item><see cref="HealthStatus.Healthy"/> — The connection is open and operational.</item>
///   <item><see cref="HealthStatus.Unhealthy"/> — The connection is closed, reconnecting, or was never established.</item>
/// </list>
/// </para>
/// 
/// <para>
/// Register one health check per connection for granular monitoring:
/// <code>
/// builder.Services.AddHealthChecks()
///     .AddRabbitMq("Some")
///     .AddRabbitMq("SomeTwo");
/// </code>
/// </para>
/// 
/// <para>
/// The check is lightweight — it reads a volatile boolean from
/// <see cref="IRabbitConnectionRegistry.IsConnected"/>. It does NOT
/// create a channel or send a heartbeat. The underlying
/// <see cref="Infrastructure.Connection.ManagedConnection"/>
/// uses AMQP heartbeats to detect broken connections and updates the
/// state automatically.
/// </para>
/// </summary>
/// <remarks>
/// Creates a health check for a specific connection.
/// </remarks>
/// <param name="connectionName">
/// The connection name as defined in <c>RabbitMqSettings.Connections</c>.
/// </param>
/// <param name="registry">The connection registry to query.</param>
public sealed class RabbitMqHealthCheck(string connectionName, IRabbitConnectionRegistry registry) : IHealthCheck
{
    private readonly string _connectionName = connectionName ?? throw new ArgumentNullException(nameof(connectionName));
    private readonly IRabbitConnectionRegistry _registry = registry ?? throw new ArgumentNullException(nameof(registry));

    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_registry.IsConnected(_connectionName))
            return Task.FromResult(HealthCheckResult.Healthy($"Connection '{_connectionName}' is open."));
        

        return Task.FromResult(HealthCheckResult.Unhealthy($"Connection '{_connectionName}' is closed or reconnecting."));
    }
}
