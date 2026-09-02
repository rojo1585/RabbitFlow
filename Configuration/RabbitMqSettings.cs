using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Configuration;

/// <summary>
/// Root configuration model bound from appsettings.json.
/// Contains all connections, producers, and consumers definitions.
/// 
/// <example>
/// In appsettings.json:
/// <code>
/// {
///   "RabbitMQ": {
///     "InstrumentationName": "Some.RabbitMQ",
///     "Connections": {
///       "Some": { "HostName": "...", "UserName": "...", ... },
///       "SomeTwo": { "HostName": "...", "UserName": "...", ... }
///     },
///     "Producers": [
///       { "ServiceKey": "some-service", "ConnectionName": "some", ... }
///     ],
///     "Consumers": [
///       { "ServiceKey": "some-consumer", "ConnectionName": "some", ... }
///     ]
///   }
/// }
/// </code>
/// </example>
/// </summary>
public sealed class RabbitMqSettings
{
    /// <summary>
    /// Default configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "RabbitMQ";

    /// <summary>
    /// Default instrumentation name used for OpenTelemetry <see cref="System.Diagnostics.ActivitySource"/>
    /// and <see cref="System.Diagnostics.Metrics.Meter"/> when <see cref="InstrumentationName"/> is not set.
    /// </summary>
    public const string DefaultInstrumentationName = "RabbitMQ";

    /// <summary>
    /// Optional custom name for OpenTelemetry instrumentation.
    /// This name is used for both the <c>ActivitySource</c> (tracing) and the <c>Meter</c> (metrics).
    /// 
    /// <para>
    /// When set, pass this same value to <c>AddSource()</c> and <c>AddMeter()</c>:
    /// <code>
    /// services.AddRabbitMQ(configuration);
    /// services.AddOpenTelemetry()
    ///     .WithTracing(t => t.AddSource(settings.InstrumentationName))
    ///     .WithMetrics(m => m.AddMeter(settings.InstrumentationName));
    /// </code>
    /// </para>
    /// 
    /// <para>
    /// Defaults to <see cref="DefaultInstrumentationName"/> (<c>"RabbitMQ"</c>) if not specified.
    /// </para>
    /// </summary>
    public string? InstrumentationName { get; set; }

    /// <summary>
    /// Named connections to RabbitMQ brokers.
    /// Each key is the connection name used by producers and consumers.
    /// </summary>
    public Dictionary<string, RabbitConnectionOptions> Connections { get; set; } = new();

    /// <summary>
    /// Producer definitions. Each producer publishes to a specific exchange
    /// on a specific connection.
    /// </summary>
    public List<RabbitProducerOptions> Producers { get; set; } = new();

    /// <summary>
    /// Consumer definitions. Each consumer listens on a specific queue
    /// on a specific connection.
    /// </summary>
    public List<RabbitConsumerOptions> Consumers { get; set; } = new();
}
