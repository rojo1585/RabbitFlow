using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Exceptions;

/// <summary>
/// Thrown when a consumed message carries an event version newer than the highest
/// version registered in the <c>EventUpgraderRegistry</c>.
/// </summary>
/// <remarks>
/// <para>
/// This indicates a producer-publishes-newer-schema-than-consumer-knows scenario:
/// the consumer has upgraders up to version N, but the message declares version N+K.
/// Returning the event unchanged (as the previous behavior did) would silently feed
/// a newer schema to a handler expecting the latest known version, causing
/// <c>JsonException</c> or silent field loss.
/// </para>
/// <para>
/// Throwing allows the consumer's poison-message handler to route the message to
/// the retry queue / DLQ instead of silently corrupting data. The message can be
/// processed once the consumer is updated with upgraders that cover the new version.
/// </para>
/// </remarks>
public sealed class EventVersionNewerThanRegisteredException(string eventName, int receivedVersion, int highestRegisteredVersion) :
    RabbitMqException($"Event '{eventName}' was received with version {receivedVersion}, but the highest registered version is {highestRegisteredVersion}. " +
        $"The consumer does not know how to handle this newer version. Update the consumer with upgraders covering version {receivedVersion}.")
{
    /// <summary>
    /// The logical event name (from <c>[EventVersion]</c> attribute).
    /// </summary>
    public string EventName { get; } = eventName;

    /// <summary>
    /// The version declared in the incoming message's <c>x-event-version</c> header.
    /// </summary>
    public int ReceivedVersion { get; } = receivedVersion;

    /// <summary>
    /// The highest version registered in the <c>EventUpgraderRegistry</c> for this event.
    /// </summary>
    public int HighestRegisteredVersion { get; } = highestRegisteredVersion;
}