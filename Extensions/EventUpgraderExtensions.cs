using Microsoft.Extensions.DependencyInjection;
using RabbitFlow.Abstractions;
using RabbitFlow.Diagnostics;
using RabbitFlow.Infrastructure.Versioning;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace RabbitFlow.Extensions
{

    /// <summary>
    /// Extension methods for registering event upgraders.
    /// </summary>
    public static class EventUpgraderExtensions
    {
        /// <summary>
        /// Registers an <see cref="IEventUpgrader{TFrom, TTo}"/> implementation and
        /// compiles a type-erased upgrade delegate for the <see cref="EventUpgraderRegistry"/>.
        /// </summary>
        /// <typeparam name="TUpgrader">
        /// The upgrader type implementing <see cref="IEventUpgrader{TFrom, TTo}"/>.
        /// </typeparam>
        /// <example>
        /// <code>
        /// services.AddEventUpgrader&lt;OrderCreatedV1ToV2Upgrader&gt;();
        /// services.AddEventUpgrader&lt;OrderCreatedV2ToV3Upgrader&gt;();
        /// 
        /// // Now v1 messages will be auto-upgraded to v3 before handler dispatch.
        /// </code>
        /// </example>
        public static IServiceCollection AddEventUpgrader<TUpgrader>(this IServiceCollection services) where TUpgrader : class
        {
            var upgraderType = typeof(TUpgrader);

            var upgraderInterfaces = upgraderType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEventUpgrader<,>))
                .ToList();

            if (upgraderInterfaces.Count == 0)
                throw new InvalidOperationException($"{upgraderType.Name} does not implement IEventUpgrader<TFrom, TTo>.");

            if (upgraderInterfaces.Count > 1)
                throw new InvalidOperationException($"{upgraderType.Name} implements IEventUpgrader<TFrom, TTo> for multiple type pairs.");

            var iface = upgraderInterfaces[0];
            var fromType = iface.GetGenericArguments()[0];
            var toType = iface.GetGenericArguments()[1];

            // Validate both types have [EventVersion] with the same EventName
            var fromAttr = fromType.GetCustomAttribute<EventVersionAttribute>();
            var toAttr = toType.GetCustomAttribute<EventVersionAttribute>();

            if (fromAttr is null)
                throw new InvalidOperationException($"{fromType.Name} is missing [EventVersion] attribute.");
            if (toAttr is null)
                throw new InvalidOperationException($"{toType.Name} is missing [EventVersion] attribute.");
            if (fromAttr.EventName != toAttr.EventName)
                throw new InvalidOperationException($"Event name mismatch: {fromType.Name} has '{fromAttr.EventName}' but {toType.Name} has '{toAttr.EventName}'.");

            services.AddTransient(upgraderType);

            var upgradeFunc = CompileUpgradeDelegate(upgraderType, iface, fromType, toType);

            var entry = new UpgraderEntry(
                EventName: fromAttr.EventName,
                FromVersion: fromAttr.Version,
                ToVersion: toAttr.Version,
                FromType: fromType,
                ToType: toType,
                UpgraderType: upgraderType,
                UpgradeFunc: upgradeFunc);

            services.AddSingleton(entry);

            return services;
        }

        /// <summary>
        /// Compiles a fast delegate that calls <c>IEventUpgrader&lt;TFrom, TTo&gt;.Upgrade()</c>
        /// with type erasure (object → object).
        /// </summary>
        private static Func<object, object, object> CompileUpgradeDelegate(Type upgraderType, Type iface, Type fromType, Type toType)
        {
            var upgradeMethod = iface.GetMethod(nameof(IEventUpgrader<object, object>.Upgrade))!;

            var uParam = Expression.Parameter(typeof(object), "upgrader");
            var eParam = Expression.Parameter(typeof(object), "event");

            var call = Expression.Call(Expression.Convert(uParam, iface), upgradeMethod, Expression.Convert(eParam, fromType));

            var convert = Expression.Convert(call, typeof(object));

            return Expression.Lambda<Func<object, object, object>>(convert, uParam, eParam).Compile();
        }
    }

}
