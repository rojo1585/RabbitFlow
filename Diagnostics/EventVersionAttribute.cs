using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Diagnostics
{

    /// <summary>
    /// Tags an event class with its logical name and schema version.
    /// Used by the versioned event type registry to support multiple versions of the same event.
    /// </summary>
    /// <remarks>
    /// When versioning is enabled:
    /// <list type="bullet">
    ///   <item>The publisher writes the event name and version to the x-event-type and x-event-version headers.</item>
    ///   <item>The consumer uses these headers to select the correct handler type from the registry.</item>
    ///   <item>If no version header is present, the highest registered version is used.</item>
    /// </list>
    /// </remarks>
    /// <example>
    /// <code>
    /// [EventVersion("Update", 1)]
    /// public record UpdateEventV1(
    /// 
    /// [EventVersion("Update", 2)]
    /// public record UpdateEventV2(int some);
    /// </code>
    /// </example>
    /// <remarks>
    /// Tags an event class with a logical name and version.
    /// </remarks>
    /// <param name="eventName">Logical event name for routing and registry lookup.</param>
    /// <param name="version">Schema version. Defaults to 1.</param>
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class EventVersionAttribute(string eventName, int version = 1) : Attribute
    {
        /// <summary>
        /// The logical event name used as the routing key and registry key.
        /// Does not have to match the class name.
        /// </summary>
        public string EventName { get; } = eventName ?? throw new ArgumentNullException(nameof(eventName));

        /// <summary>
        /// The schema version of this event class.
        /// Higher versions should be backward-compatible or have a registered <c>IEventUpgrader</c>.
        /// </summary>
        public int Version { get; } = version;
    }
}
