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
    /// [EventVersion("UberDelivered", 1)]
    /// public record UberDeliveredEvent(int BranchId, int ShipmentId);
    /// 
    /// [EventVersion("UberDelivered", 2)]
    /// public record UberDeliveredEventV2(int BranchId, int ShipmentId, string TrackingUrl);
    /// </code>
    /// </example>
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public sealed class EventVersionAttribute : Attribute
    {
        /// <summary>
        /// The logical event name used as the routing key and registry key.
        /// Does not have to match the class name.
        /// </summary>
        public string EventName { get; }

        /// <summary>
        /// The schema version of this event class.
        /// Higher versions should be backward-compatible or have a registered <c>IEventUpgrader</c>.
        /// </summary>
        public int Version { get; }

        /// <summary>
        /// Tags an event class with a logical name and version.
        /// </summary>
        /// <param name="eventName">Logical event name for routing and registry lookup.</param>
        /// <param name="version">Schema version. Defaults to 1.</param>
        public EventVersionAttribute(string eventName, int version = 1)
        {
            EventName = eventName ?? throw new ArgumentNullException(nameof(eventName));
            Version = version;
        }
    }
}
