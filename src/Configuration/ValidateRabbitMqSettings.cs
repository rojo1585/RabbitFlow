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
            if (!settings.Connections.ContainsKey(producer.ConnectionName))
                errors.Add(RabbitMqConfigurationException.MissingConnection($"Producer '{producer.ServiceKey}'", producer.ConnectionName).Message);
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
            if (!settings.Connections.ContainsKey(consumer.ConnectionName))
                errors.Add(RabbitMqConfigurationException.MissingConnection($"Consumer '{consumer.ServiceKey}'", consumer.ConnectionName).Message);
        }
    }
}
