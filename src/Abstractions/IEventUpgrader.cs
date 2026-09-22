using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{
    /// <summary>
    /// Transforms an event from an older schema version to a newer one.
    /// 
    /// <para>
    /// Upgraders are registered in DI and discovered at startup. When a consumer
    /// receives a message with an older <c>x-event-version</c> header, the library
    /// automatically chains upgraders to transform the event to the highest
    /// registered version before passing it to the handler.
    /// <typeparam name="TFrom">The older event type (source).</typeparam>
    /// <typeparam name="TTo">The newer event type (target).</typeparam>
    /// </para>
    /// </summary>
    public interface IEventUpgrader<TFrom, TTo> where TFrom : class where TTo : class
    {
        /// <summary>
        /// The logical event name this upgrader handles.
        /// Must match the <see cref="Diagnostics.EventVersionAttribute.EventName"/>
        /// on both <typeparamref name="TFrom"/> and <typeparamref name="TTo"/>.
        /// This is used to group upgraders into chains per event name.
        /// </summary>
        string EventName { get; }

        /// <summary>
        /// The source version this upgrader handles.
        /// Typically matches <see cref="Diagnostics.EventVersionAttribute.Version"/>
        /// on <typeparamref name="TFrom"/>.
        /// </summary>
        int FromVersion { get; }

        /// <summary>
        /// The target version this upgrader produces.
        /// Typically matches <see cref="Diagnostics.EventVersionAttribute.Version"/>
        /// on <typeparamref name="TTo"/>.
        /// </summary>
        int ToVersion { get; }

        /// <summary>
        /// Transforms the source event into the target event.
        /// </summary>
        /// <param name="source">The deserialized older event.</param>
        /// <returns>The transformed newer event.</returns>
        TTo Upgrade(TFrom source);
    }

}
