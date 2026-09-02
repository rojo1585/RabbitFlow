using Microsoft.Extensions.DependencyInjection;
using RabbitFlow.Abstractions;
using RabbitFlow.Diagnostics;
using RabbitFlow.Infrastructure.Consuming;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace RabbitFlow.Extensions
{
    /// <summary>
    /// Extension methods for registering RabbitMQ event handlers.
    /// </summary>
    public static class RabbitHandlerExtensions
    {
        /// <summary>
        /// Registers an <see cref="IRabbitHandler{TEvent}"/> implementation in DI and
        /// records the mapping in the <see cref="HandlerTypeRegistry"/> so the consumer
        /// can dispatch messages to it.
        ///
        /// <para>
        /// The handler is registered as <b>Transient</b> — a fresh instance is created
        /// for each message via a dedicated DI scope (enabling scoped services like DbContexts).
        /// </para>
        ///
        /// <para>
        /// The event type name used for routing is resolved from <see cref="EventVersionAttribute"/>
        /// if present on the event class (matching the publisher's behavior), otherwise falls
        /// back to <see cref="Type.FullName"/>. This ensures the consumer registry key matches
        /// the <c>x-event-type</c> AMQP header written by the publisher.
        /// </para>
        /// </summary>
        /// <typeparam name="THandler">
        /// The handler class. Must implement <see cref="IRabbitHandler{TEvent}"/> for exactly one event type.
        /// </typeparam>
        /// <param name="consumerKey">
        /// Must match the <see cref="Apymsa.RabbitMQ.Configuration.RabbitConsumerOptions.ServiceKey"/>
        /// of the consumer that should dispatch to this handler.
        /// </param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when <typeparamref name="THandler"/> does not implement
        /// <see cref="IRabbitHandler{TEvent}"/> or implements it for multiple event types.
        /// </exception>
        /// <example>
        /// <code>
        /// services.AddRabbitHandler&lt;SomeCreatedHandler&gt;("some-consumer");
        /// </code>
        /// </example>
        public static IServiceCollection AddRabbitHandler<THandler>(
            this IServiceCollection services,
            string consumerKey)
            where THandler : class
        {
            var handlerType = typeof(THandler);

            // Validate: must implement IRabbitHandler<TEvent> for exactly one event type
            var handlerInterfaces = handlerType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRabbitHandler<>))
                .ToList();

            if (handlerInterfaces.Count == 0)
            {
                throw new InvalidOperationException(
                    $"{handlerType.Name} does not implement IRabbitHandler<TEvent>.");
            }

            if (handlerInterfaces.Count > 1)
            {
                throw new InvalidOperationException(
                    $"{handlerType.Name} implements IRabbitHandler<TEvent> for multiple event types. " +
                    $"Use separate handler classes for each event type.");
            }

            var eventType = handlerInterfaces[0].GetGenericArguments()[0];

            // Resolve the event type name using the SAME logic as the publisher:
            // EventVersionAttribute.EventName if present, otherwise Type.FullName.
            // This is critical for correct handler lookup at consume time.
            var eventTypeName = ResolveEventTypeName(eventType);

            // Register the handler as transient (resolved per message via scope)
            services.AddTransient(handlerType);

            // Store the mapping as a singleton HandlerRegistration.
            // The HandlerTypeRegistry will collect all registrations when constructed.
            services.AddSingleton(new HandlerRegistration(consumerKey, handlerType, eventType, eventTypeName));

            return services;
        }

        /// <summary>
        /// Resolves the event type name for handler registry matching.
        /// Uses <see cref="EventVersionAttribute.EventName"/> if present, otherwise <see cref="Type.FullName"/>.
        /// This mirrors the logic in <c>NamedRabbitPublisher.ResolveEventTypeInfo</c>.
        /// </summary>
        private static string ResolveEventTypeName(Type eventType)
        {
            var versionAttr = eventType.GetCustomAttribute<EventVersionAttribute>();
            return versionAttr is not null
                ? versionAttr.EventName
                : eventType.FullName ?? throw new InvalidOperationException(
                    $"Event type '{eventType.Name}' has no FullName (nested/generic types are not supported).");
        }
    }

}
