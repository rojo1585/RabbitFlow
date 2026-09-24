using Microsoft.Extensions.DependencyInjection;
using RabbitFlow.Abstractions;
using RabbitFlow.Exceptions;

namespace RabbitFlow.Infrastructure.Versioning;

/// <summary>
/// Describes a registered upgrader: from version, to version, upgrader type, and the compiled upgrade function.
/// </summary>
public sealed record UpgraderEntry(string EventName, int FromVersion, int ToVersion, Type FromType, Type ToType, Type UpgraderType, Func<object, object, object> UpgradeFunc);

/// <summary>
/// Registry that holds all <see cref="IEventUpgrader{TFrom, TTo}"/> implementations.
/// 
/// <para>
/// At startup, all upgraders registered via <c>AddEventUpgrader()</c> are collected.
/// For each event name, the upgraders are sorted by <c>FromVersion</c> and validated
/// to form a complete chain from the lowest version to the highest.
/// </para>
/// 
/// <para>
/// When a message arrives with a version lower than the highest registered,
/// <see cref="Upgrade"/> chains the necessary upgraders to transform
/// the event to the latest version.
/// </para>
/// 
/// <para>
/// Thread safety: Immutable after construction.
/// </para>
/// </summary>
public sealed class EventUpgraderRegistry
{
    /// <summary>
    /// Maps eventName → sorted chain of upgrader entries (by FromVersion).
    /// </summary>
    private readonly Dictionary<string, UpgraderEntry[]> _chains;

    /// <summary>
    /// Maps (eventName, version) → the concrete event type registered for that version.
    /// Used by the consumer to deserialize into the correct type before upgrading.
    /// </summary>
    private readonly Dictionary<(string EventName, int Version), Type> _versionTypes;

    /// <summary>
    /// Maps eventName → highest registered version number.
    /// </summary>
    private readonly Dictionary<string, int> _highestVersions;

    /// <summary>
    /// Maps eventName → highest version event type.
    /// Used by the consumer to resolve the handler.
    /// </summary>
    private readonly Dictionary<string, Type> _latestTypes;
    /// <summary>
    /// Upgrader registry constructor. Validates that all registered upgraders form a continuous chain
    /// </summary>
    /// <param name="entries"></param>
    /// <exception cref="InvalidOperationException"></exception>
    public EventUpgraderRegistry(IEnumerable<UpgraderEntry> entries)
    {
        var chains = new Dictionary<string, List<UpgraderEntry>>();
        var versionTypes = new Dictionary<(string, int), Type>();
        var highestVersions = new Dictionary<string, int>();
        var latestTypes = new Dictionary<string, Type>();

        foreach (var entry in entries)
        {
            if (entry.ToVersion <= entry.FromVersion)
            {
                throw new InvalidOperationException(
                    @$"Event '{entry.EventName}' upgrader v{entry.FromVersion}→v{entry.ToVersion} is invalid: 
                    ToVersion must be strictly greater than FromVersion. 
                    Downgrades, self-loops, and cycles are not allowed because they cause silent data corruption 
                    (Upgrade would return a lower version than the handler expects).");
            }

            // Track all version types
            versionTypes[(entry.EventName, entry.FromVersion)] = entry.FromType;
            versionTypes[(entry.EventName, entry.ToVersion)] = entry.ToType;

            // Track highest version
            if (!highestVersions.TryGetValue(entry.EventName, out var highest) || entry.ToVersion > highest)
            {
                highestVersions[entry.EventName] = entry.ToVersion;
                latestTypes[entry.EventName] = entry.ToType;
            }
            if (!highestVersions.TryGetValue(entry.EventName, out highest) || entry.FromVersion > highest)
            {
                highestVersions[entry.EventName] = entry.FromVersion;
                latestTypes[entry.EventName] = entry.FromType;
            }

            // Add to chain
            if (!chains.TryGetValue(entry.EventName, out var chain))
            {
                chain = [];
                chains[entry.EventName] = chain;
            }
            chain.Add(entry);
        }

        // Sort each chain by FromVersion and validate continuity
        var validatedChains = new Dictionary<string, UpgraderEntry[]>();
        foreach (var (eventName, chain) in chains)
        {
            var sorted = chain.OrderBy(e => e.FromVersion).ToArray();
            for (int i = 0; i < sorted.Length - 1; i++)
            {
                if (sorted[i].ToVersion != sorted[i + 1].FromVersion)
                {
                    throw new InvalidOperationException($"Event '{eventName}' upgrade chain is not continuous: " +
                        $"upgrader v{sorted[i].FromVersion}→v{sorted[i].ToVersion} " +
                        $"does not connect to v{sorted[i + 1].FromVersion}→v{sorted[i + 1].ToVersion}.");
                }
            }

            validatedChains[eventName] = sorted;
        }

        _chains = validatedChains;
        _versionTypes = versionTypes;
        _highestVersions = highestVersions;
        _latestTypes = latestTypes;
    }

    /// <summary>
    /// Gets the highest registered version for an event.
    /// </summary>
    public int GetHighestVersion(string eventName)
        => _highestVersions.GetValueOrDefault(eventName, 1);

    /// <summary>
    /// Gets the event type for the highest registered version.
    /// </summary>
    public Type? GetLatestType(string eventName)
        => _latestTypes.GetValueOrDefault(eventName);

    /// <summary>
    /// Gets the event type for a specific version of an event.
    /// Returns null if the version is not registered.
    /// </summary>
    public Type? GetTypeForVersion(string eventName, int version)
        => _versionTypes.GetValueOrDefault((eventName, version));

    /// <summary>
    /// Upgrades an event from the given version to the latest registered version.
    /// Returns the original event (unmodified) if it is already at the latest version.
    /// </summary>
    /// <param name="eventName">The logical event name.</param>
    /// <param name="event">The deserialized event (may be an older version).</param>
    /// <param name="fromVersion">The version of the event.</param>
    /// <param name="serviceProvider">
    /// Used to resolve <see cref="IEventUpgrader{TFrom, TTo}"/> instances from DI.
    /// Typically the scoped <see cref="IServiceProvider"/> of the current message processing scope.
    /// </param>
    /// <returns>The event upgraded to the latest version, or the original if already latest.</returns>
    /// <exception cref="EventVersionNewerThanRegisteredException">
    /// Thrown when <paramref name="fromVersion"/> is greater than the highest registered version.
    /// This indicates a producer-publishes-newer-schema-than-consumer-knows scenario.
    /// The caller (typically the consumer's poison-message handler) should route the message
    /// to the retry queue / DLQ rather than silently feeding a newer schema to a handler
    /// expecting the latest known version.
    /// </exception>
    public object Upgrade(string eventName, object @event, int fromVersion, IServiceProvider serviceProvider)
    {
        var highest = GetHighestVersion(eventName);

        if (fromVersion > highest)
            throw new EventVersionNewerThanRegisteredException(eventName, fromVersion, highest);

        if (fromVersion == highest || !_chains.TryGetValue(eventName, out var chain))
            return @event;

        object current = @event;
        foreach (var entry in chain)
        {
            if (entry.FromVersion < fromVersion)
                continue;

            if (entry.FromVersion != fromVersion)
                break;

            var upgrader = serviceProvider.GetRequiredService(entry.UpgraderType);
            current = entry.UpgradeFunc(upgrader, current);
            fromVersion = entry.ToVersion;
        }

        return current;
    }
}