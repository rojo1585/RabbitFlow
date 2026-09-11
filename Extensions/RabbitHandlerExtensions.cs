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
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="consumerKey">
        /// Must match a <see cref="Configuration.RabbitConsumerOptions.ServiceKey"/>.
        /// </param>
        /// <typeparam name="THandler">
        /// The handler type implementing <see cref="IRabbitHandler{TEvent}"/>.
        /// </typeparam>
        public static IServiceCollection AddRabbitHandler<THandler>(this IServiceCollection services, string consumerKey) where THandler : class
        {
            var handlerType = typeof(THandler);

            var handlerInterfaces = handlerType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRabbitHandler<>))
                .ToList();

            if (handlerInterfaces.Count == 0)
                throw new InvalidOperationException(
                    $"{handlerType.Name} does not implement IRabbitHandler<TEvent>.");

            if (handlerInterfaces.Count > 1)
                throw new InvalidOperationException(
                    $"{handlerType.Name} implements IRabbitHandler<TEvent> for multiple event types.");

            var eventType = handlerInterfaces[0].GetGenericArguments()[0];
            var eventTypeName = ResolveEventTypeName(eventType);

            services.AddTransient(handlerType);
            services.AddSingleton(new HandlerRegistration(consumerKey, handlerType, eventType, eventTypeName, IsBatch: false));

            return services;
        }

        /// <summary>
        /// Registers an <see cref="IBatchRabbitHandler{TEvent}"/> implementation in DI and
        /// records the mapping in the <see cref="HandlerTypeRegistry"/> so the batch consumer
        /// can dispatch messages to it.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="consumerKey">
        /// Must match a <see cref="Configuration.RabbitConsumerOptions.ServiceKey"/> with
        /// <see cref="Configuration.RabbitConsumerOptions.EnableBatchConsumer"/> set to true.
        /// </param>
        /// <typeparam name="THandler">
        /// The handler type implementing <see cref="IBatchRabbitHandler{TEvent}"/>.
        /// </typeparam>
        /// <example>
        /// <code>
        /// // In appsettings.json:
        /// // { "Consumers": [{ "ServiceKey": "orders-batch", "EnableBatchConsumer": true, "BatchSize": 50, ... }] }
        /// 
        /// // In Program.cs:
        /// services.AddBatchRabbitHandler&lt;OrderCreatedBatchHandler&gt;("orders-batch");
        /// </code>
        /// </example>
        public static IServiceCollection AddBatchRabbitHandler<THandler>(this IServiceCollection services, string consumerKey) where THandler : class
        {
            var handlerType = typeof(THandler);

            var handlerInterfaces = handlerType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IBatchRabbitHandler<>))
                .ToList();

            if (handlerInterfaces.Count == 0)
                throw new InvalidOperationException($"{handlerType.Name} does not implement IBatchRabbitHandler<TEvent>.");

            if (handlerInterfaces.Count > 1)
                throw new InvalidOperationException($"{handlerType.Name} implements IBatchRabbitHandler<TEvent> for multiple event types.");

            var eventType = handlerInterfaces[0].GetGenericArguments()[0];
            var eventTypeName = ResolveEventTypeName(eventType);

            services.AddTransient(handlerType);
            services.AddSingleton(new HandlerRegistration(consumerKey, handlerType, eventType, eventTypeName, IsBatch: true));

            return services;
        }

        private static string ResolveEventTypeName(Type eventType)
        {
            var versionAttr = eventType.GetCustomAttribute<EventVersionAttribute>();
            return versionAttr is not null ? versionAttr.EventName : eventType.FullName ?? throw new InvalidOperationException($"Event type '{eventType.Name}' has no FullName (nested/generic types are not supported).");
        }
    }


}
