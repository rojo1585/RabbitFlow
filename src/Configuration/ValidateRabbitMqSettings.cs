using Microsoft.Extensions.Options;
using RabbitFlow.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Configuration;


/// <summary>
/// Validates <see cref="RabbitMqSettings"/> using the idiomatic
/// <c>IValidateOptions{T}</c> + <c>ValidateOnStart()</c> pattern.
///
/// <para>
/// Registered automatically by <c>AddRabbitMQ</c>. Validation runs at host startup
/// (non-blocking during service registration), ensuring fail-fast before any
/// hosted service starts consuming or publishing messages.
/// </para>
///
/// <para>
/// Validation rules:
/// <list type="bullet">
///   <item>Connections must exist when producers or consumers are configured.</item>
///   <item>Connection names must be unique.</item>
///   <item>Producer <c>ServiceKey</c>s must be unique.</item>
///   <item>Consumer <c>ServiceKey</c>s must be unique.</item>
///   <item>Every <c>ConnectionName</c> reference in producers/consumers must exist.</item>
///   <item>All string identifiers (ServiceKey, ConnectionName, ExchangeName, QueueName,
///   RoutingKey, HostName, UserName, Password) must be non-empty and non-whitespace.</item>
///   <item>Numeric bounds: <c>Port</c> in [1, 65535], <c>PrefetchCount</c> &gt;= 1,
///   <c>BatchSize</c> &gt;= 1, <c>BatchTimeoutMs</c> &gt; 0, <c>MaxRetries</c> &gt;= 1,
///   <c>MaxConcurrentHandlers</c> &gt;= 0, <c>ChannelPoolSize</c> &gt;= 0,
///   <c>PublishConfirmTimeoutMs</c> &gt; 0, <c>RequestedHeartbeatSeconds</c> &gt; 0,
///   <c>ConnectionTimeoutSeconds</c> &gt; 0, <c>InitialConnectRetryCount</c> &gt;= 0,
///   <c>MaxBackoffSeconds</c> &gt; 0.</item>
///   <item>When retry is enabled, <c>RetryDelays</c> (if provided) must have at least
///   <c>MaxRetries</c> - 1 entries so every retry attempt has a delay.</item>
/// </list>
/// </para>
/// </summary>
internal sealed class ValidateRabbitMqSettings : IValidateOptions<RabbitMqSettings>
{
    /// <summary>
    /// The default options name used by the .NET options system.
    /// </summary>
    private const string DefaultName = "";

    public ValidateOptionsResult Validate(string? name, RabbitMqSettings settings)
    {
        if (name is not null && name != DefaultName)
            return ValidateOptionsResult.Skip;

        var errors = new List<string>();

        ValidateConnections(settings, errors);
        ValidateProducers(settings, errors);
        ValidateConsumers(settings, errors);

        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }

    private static void ValidateConnections(RabbitMqSettings settings, List<string> errors)
    {
        if (settings.Connections.Count == 0 && (settings.Producers.Count > 0 || settings.Consumers.Count > 0))
            errors.Add("Producers or consumers are configured but no connections are defined.");

        var duplicateNames = settings.Connections
            .GroupBy(kvp => kvp.Key)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (duplicateNames.Count > 0)
            errors.Add($"Duplicate connection name(s): [{string.Join(", ", duplicateNames)}]. Each connection must have a unique name.");

        foreach (var (key, conn) in settings.Connections)
        {
            var prefix = $"Connection '{key}'";

            ValidateNonEmptyString(errors, conn.Name, $"{prefix}: Name");
            ValidateNonEmptyString(errors, conn.HostName, $"{prefix}: HostName");
            ValidateNonEmptyString(errors, conn.UserName, $"{prefix}: UserName");
            ValidateNonEmptyString(errors, conn.Password, $"{prefix}: Password");

            if (conn.Port < 1 || conn.Port > 65535)
                errors.Add($"{prefix}: Port must be between 1 and 65535 (got {conn.Port}).");

            if (conn.RequestedHeartbeatSeconds < 1)
                errors.Add($"{prefix}: RequestedHeartbeatSeconds must be >= 1 (got {conn.RequestedHeartbeatSeconds}). A value of 0 disables heartbeats, which prevents detection of dead TCP connections.");

            if (conn.ConnectionTimeoutSeconds < 1)
                errors.Add($"{prefix}: ConnectionTimeoutSeconds must be >= 1 (got {conn.ConnectionTimeoutSeconds}).");

            if (conn.InitialConnectRetryCount < 0)
                errors.Add($"{prefix}: InitialConnectRetryCount must be >= 0 (got {conn.InitialConnectRetryCount}).");

            if (conn.MaxBackoffSeconds < 1)
                errors.Add($"{prefix}: MaxBackoffSeconds must be >= 1 (got {conn.MaxBackoffSeconds}).");
        }
    }

    private static void ValidateProducers(RabbitMqSettings settings, List<string> errors)
    {
        var duplicateKeys = settings.Producers
            .GroupBy(p => p.ServiceKey)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (duplicateKeys.Count > 0)
            errors.Add($"Duplicate producer ServiceKey(s): [{string.Join(", ", duplicateKeys)}]. Each producer must have a unique ServiceKey.");


        foreach (var producer in settings.Producers)
        {
            var prefix = $"Producer '{producer.ServiceKey}'";

            ValidateNonEmptyString(errors, producer.ServiceKey, $"{prefix}: ServiceKey");
            ValidateNonEmptyString(errors, producer.ConnectionName, $"{prefix}: ConnectionName");
            ValidateNonEmptyString(errors, producer.ExchangeName, $"{prefix}: ExchangeName");
            ValidateNonEmptyString(errors, producer.ExchangeType, $"{prefix}: ExchangeType");
            ValidateNonEmptyString(errors, producer.RoutingKey, $"{prefix}: RoutingKey");

            if (producer.ChannelPoolSize < 0)
                errors.Add($"{prefix}: ChannelPoolSize must be >= 0 (got {producer.ChannelPoolSize}). Use 0 to disable pooling.");

            if (producer.PublishConfirmTimeoutMs <= 0)
                errors.Add($"{prefix}: PublishConfirmTimeoutMs must be > 0 (got {producer.PublishConfirmTimeoutMs}).");

            if (!settings.Connections.ContainsKey(producer.ConnectionName))
                errors.Add(RabbitMqConfigurationException.MissingConnection(prefix, producer.ConnectionName).Message);
        }
    }

    private static void ValidateConsumers(RabbitMqSettings settings, List<string> errors)
    {
        var duplicateKeys = settings.Consumers
            .GroupBy(c => c.ServiceKey)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (duplicateKeys.Count > 0)
            errors.Add($"Duplicate consumer ServiceKey(s): [{string.Join(", ", duplicateKeys)}]. Each consumer must have a unique ServiceKey.");

        foreach (var consumer in settings.Consumers)
        {
            var prefix = $"Consumer '{consumer.ServiceKey}'";

            ValidateNonEmptyString(errors, consumer.ServiceKey, $"{prefix}: ServiceKey");
            ValidateNonEmptyString(errors, consumer.ConnectionName, $"{prefix}: ConnectionName");
            ValidateNonEmptyString(errors, consumer.ExchangeName, $"{prefix}: ExchangeName");
            ValidateNonEmptyString(errors, consumer.ExchangeType, $"{prefix}: ExchangeType");
            ValidateNonEmptyString(errors, consumer.QueueName, $"{prefix}: QueueName");
            ValidateNonEmptyString(errors, consumer.RoutingKey, $"{prefix}: RoutingKey");

            if (consumer.PrefetchCount < 1)
                errors.Add($"{prefix}: PrefetchCount must be >= 1 (got {consumer.PrefetchCount}). A value of 0 deadlocks the batch consumer (BoundedChannel capacity = PrefetchCount * 2).");

            if (consumer.MaxConcurrentHandlers < 0)
                errors.Add($"{prefix}: MaxConcurrentHandlers must be >= 0 (got {consumer.MaxConcurrentHandlers}). Use 0 for unlimited.");

            if (consumer.MaxRetries < 1)
                errors.Add($"{prefix}: MaxRetries must be >= 1 (got {consumer.MaxRetries}). A value of 0 would dead-letter every message on the first failure.");

            if (consumer.EnableBatchConsumer)
            {
                if (consumer.BatchSize < 1)
                    errors.Add($"{prefix}: BatchSize must be >= 1 when EnableBatchConsumer is true (got {consumer.BatchSize}).");

                if (consumer.BatchTimeoutMs <= 0)
                    errors.Add($"{prefix}: BatchTimeoutMs must be > 0 when EnableBatchConsumer is true (got {consumer.BatchTimeoutMs}). A value of 0 causes a busy-loop or prevents the batch from ever flushing by size.");
            }

            if (consumer.EnableRetry && consumer.RetryDelays is { } delays)
            {
                var requiredCount = consumer.MaxRetries - 1;
                if (delays.Length < requiredCount)
                    errors.Add($"{prefix}: RetryDelays must have at least {requiredCount} entries (one per retry attempt after the first) when MaxRetries={consumer.MaxRetries}, but got {delays.Length}.");

                foreach (var d in delays)
                {
                    if (d < TimeSpan.Zero)
                        errors.Add($"{prefix}: RetryDelays must not contain negative values (got {d}).");
                }
            }

            if (!settings.Connections.ContainsKey(consumer.ConnectionName))
                errors.Add(RabbitMqConfigurationException.MissingConnection(prefix, consumer.ConnectionName).Message);
        }
    }

    /// <summary>
    /// Validates that a string is non-null, non-empty, and non-whitespace.
    /// Adds an error to <paramref name="errors"/> if the check fails.
    /// </summary>
    private static void ValidateNonEmptyString(List<string> errors, string? value, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(value))
            errors.Add($"{fieldName} must be a non-empty, non-whitespace string.");
    }
}