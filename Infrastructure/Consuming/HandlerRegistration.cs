using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Infrastructure.Consuming;


/// <summary>
/// Describes a handler registration: which handler type processes which event type
/// for a given consumer key. The <see cref="EventTypeName"/> is resolved from
/// <see cref="Diagnostics.EventVersionAttribute"/> (if present) or <see cref="Type.FullName"/>
/// to match the <c>x-event-type</c> AMQP header written by the publisher.
/// </summary>
/// <param name="ConsumerKey">The consumer configuration this handler belongs to.</param>
/// <param name="HandlerType">The concrete handler class (e.g. <c>OrderCreatedHandler</c>).</param>
/// <param name="EventType">The event type the handler processes (e.g. <c>OrderCreatedEvent</c>).</param>
/// <param name="EventTypeName">
/// The event type name used for AMQP header matching. Matches the value written
/// by <c>NamedRabbitPublisher</c> in the <c>x-event-type</c> header.
/// </param>
public sealed record HandlerRegistration(string ConsumerKey, Type HandlerType, Type EventType, string EventTypeName);

/// <summary>
/// Maps (consumerKey, eventTypeName) to (handlerType, eventType) at runtime.
/// Populated at startup from <see cref="HandlerRegistration"/> instances registered via DI.
/// 
/// <para>
/// Registration pattern:
/// <c>AddRabbitHandler&lt;THandler&gt;(consumerKey)</c> registers a <see cref="HandlerRegistration"/>
/// singleton in DI. When <see cref="HandlerTypeRegistry"/> is constructed, it collects all
/// registrations and builds a lookup dictionary.
/// </para>
/// 
/// <para>
/// Thread safety: Immutable after construction.
/// </para>
/// </summary>
public sealed class HandlerTypeRegistry
{
    private readonly Dictionary<(string ConsumerKey, string EventTypeName), (Type HandlerType, Type EventType)> _lookup;

    /// <summary>
    /// Creates the registry and populates it from all <see cref="HandlerRegistration"/>
    /// instances registered in the DI container.
    /// </summary>
    /// <param name="registrations">
    /// All handler registrations collected from DI.
    /// Using <see cref="IEnumerable{T}"/> instead of <see cref="IServiceProvider"/>
    /// follows the explicit dependencies principle (avoids service locator anti-pattern).
    /// </param>
    public HandlerTypeRegistry(IEnumerable<HandlerRegistration> registrations)
    {
        var lookup = new Dictionary<(string, string), (Type, Type)>();

        foreach (var reg in registrations)
        {
            var key = (reg.ConsumerKey, reg.EventTypeName);

            if (lookup.ContainsKey(key))
                throw new InvalidOperationException($"Duplicate handler registration for consumer '{reg.ConsumerKey}' " +$"and event type name '{reg.EventTypeName}'.");


            lookup[key] = (reg.HandlerType, reg.EventType);
        }

        _lookup = lookup;
    }

    /// <summary>
    /// Resolves the handler type and event type for a given consumer key and event type name.
    /// Returns <c>null</c> if no handler is registered for this combination.
    /// </summary>
    public (Type HandlerType, Type EventType)? Resolve(string consumerKey, string eventTypeName)
    {
        return _lookup.GetValueOrDefault((consumerKey, eventTypeName));
    }

    /// <summary>
    /// Called by <see cref="RabbitConsumerHostedService"/> at startup.
    /// Verifies the registry is ready for use.
    /// Since the registry is populated in the constructor, this is a no-op,
    /// but it provides a clear lifecycle hook.
    /// </summary>
    public void Freeze()
    {
    }
}
