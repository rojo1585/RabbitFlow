using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Infrastructure.HealthChecks;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Extensions;


/// <summary>
/// Extension methods for registering RabbitMQ health checks.
/// 
/// <para>
/// One health check per connection provides granular visibility:
/// <code>
/// builder.Services.AddHealthChecks()
///     .AddRabbitMq("Some")
///     .AddRabbitMq("SomeTwo");
/// </code>
/// </para>
/// 
/// <para>
/// To check ALL connections with a single registration:
/// <code>
/// builder.Services.AddHealthChecks()
///     .AddRabbitMqAll();
/// </code>
/// </para>
/// 
/// <para>
/// <b>Usage in Program.cs:</b>
/// <code>
/// builder.Services.AddRabbitMQ(builder.Configuration);
/// builder.Services.AddHealthChecks()
///     .AddRabbitMqAll();
/// 
/// // Or individual connections:
/// builder.Services.AddHealthChecks()
///     .AddRabbitMq("Some")
///     .AddRabbitMq("SomeTwo");
/// </code>
/// </para>
/// </summary>
public static class HealthCheckExtensions
{
    extension(IHealthChecksBuilder builder)
    {
        /// <summary>
        /// Adds a health check for a specific named RabbitMQ connection.
        /// </summary>
        /// <param name="builder">The health checks builder.</param>
        /// <param name="connectionName">
        /// The connection name as defined in <c>RabbitMqSettings.Connections</c>.
        /// </param>
        /// <param name="name">
        /// The health check registration name. Defaults to <c>rabbitmq-{connectionName}</c>.
        /// </param>
        /// <param name="failureStatus">
        /// The status to report when the connection is unhealthy.
        /// Defaults to <see cref="HealthStatus.Unhealthy"/>.
        /// </param>
        /// <param name="tags">
        /// Optional tags for filtering in health check UI endpoints.
        /// The tag <c>"rabbitmq"</c> is always included.
        /// </param>
        /// <returns>The builder for chaining.</returns>
        public IHealthChecksBuilder AddRabbitMq(string connectionName, string? name = null, HealthStatus? failureStatus = null, string[]? tags = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(connectionName);

            var checkName = name ?? $"rabbitmq-{connectionName}";
            var allTags = MergeTags(tags, "rabbitmq");

            return builder.Add(new HealthCheckRegistration(
                name: checkName,
                factory: sp => new RabbitMqHealthCheck(connectionName, sp.GetRequiredService<IRabbitConnectionRegistry>()),
                failureStatus: failureStatus,
                tags: allTags));
        }

        /// <summary>
        /// Registers a single health check that reports on ALL configured RabbitMQ connections.
        /// 
        /// <para>
        /// Returns <see cref="HealthStatus.Healthy"/> if all connections are open,
        /// <see cref="HealthStatus.Unhealthy"/> if any are closed.
        /// The response data includes per-connection status for dashboard visibility.
        /// </para>
        /// </summary>
        /// <param name="builder">The health checks builder.</param>
        /// <param name="name">
        /// The health check name. Defaults to <c>"rabbitmq"</c>.
        /// </param>
        /// <param name="failureStatus">
        /// The status to report when any connection is unhealthy.
        /// Defaults to <see cref="HealthStatus.Unhealthy"/>.
        /// </param>
        /// <param name="tags">
        /// Additional tags. The tag <c>"rabbitmq"</c> is always included.
        /// </param>
        /// <returns>The builder for chaining.</returns>
        /// <remarks>
        /// <c>IRabbitConnectionRegistry</c> and <c>IOptions&lt;RabbitMqSettings&gt;</c> are registered.
        /// </remarks>
        public IHealthChecksBuilder AddRabbitMqAll(string? name = null, HealthStatus? failureStatus = null, string[]? tags = null)
        {
            ArgumentNullException.ThrowIfNull(builder);

            var allTags = MergeTags(tags, "rabbitmq");

            return builder.Add(new HealthCheckRegistration(
                name: name ?? "rabbitmq",
                factory: sp => new RabbitMqAllHealthCheck(sp.GetRequiredService<IRabbitConnectionRegistry>(), sp.GetRequiredService<IOptions<RabbitMqSettings>>()),
                failureStatus: failureStatus,
                tags: allTags));
        }
    }

    /// <summary>
    /// Merges user-provided tags with a required tag.
    /// </summary>
    private static string[] MergeTags(string[]? userTags, string requiredTag)
    {
        if (userTags is null || userTags.Length == 0)
            return [requiredTag];

        var merged = new string[userTags.Length + 1];
        userTags.AsSpan().CopyTo(merged);
        merged[^1] = requiredTag;
        return merged;
    }
}