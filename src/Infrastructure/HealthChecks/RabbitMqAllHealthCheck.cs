using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Infrastructure.HealthChecks;

// <summary>
/// Combined health check that reports on ALL configured RabbitMQ connections.
/// 
/// <para>
/// Returns:
/// <list type="bullet">
///   <item><see cref="HealthStatus.Healthy"/> — All connections are open.</item>
///   <item><see cref="HealthStatus.Unhealthy"/> — One or more connections are closed.</item>
/// </list>
/// </para>
/// 
/// <para>
/// The data dictionary includes individual connection statuses for
/// dashboard visibility:
/// <code>{
///   "some": "Healthy",
///   "someTwo": "Unhealthy"
/// }</code>
/// </para>
/// 
/// <para>
/// Registered via <c>AddRabbitMqAll()</c> which tags it with <c>"rabbitmq"</c>.
/// </para>

/// <summary>
/// Creates a combined health check for all configured connections.
/// </summary>
/// <param name="registry">The connection registry.</param>
/// <param name="settings">The RabbitMQ settings to read connection names from.</param>
public sealed class RabbitMqAllHealthCheck(IRabbitConnectionRegistry registry, IOptions<RabbitMqSettings> settings) : IHealthCheck
{
    private readonly IRabbitConnectionRegistry _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly string[] _connectionNames = settings?.Value.Connections.Keys.ToArray() ?? throw new ArgumentNullException(nameof(settings));

    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var data = new Dictionary<string, object>();
        var anyUnhealthy = false;

        foreach (var name in _connectionNames)
        {
            var connected = _registry.IsConnected(name);
            var status = connected ? "Healthy" : "Unhealthy";
            data[name] = status;

            if (!connected)
                anyUnhealthy = true;
        }

        if (!anyUnhealthy)
            return Task.FromResult(HealthCheckResult.Healthy("All RabbitMQ connections are open.", data));

        var unhealthyCount = data.Count(kvp => kvp.Value.Equals("Unhealthy"));

        return Task.FromResult(HealthCheckResult.Unhealthy($"{unhealthyCount} of {_connectionNames.Length} RabbitMQ connection(s) are closed.", data: data));
    }
}
