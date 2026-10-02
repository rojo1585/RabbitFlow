using System;
using System.Collections.Generic;
using System.Text;

namespace RedRabbit.Exceptions
{
    /// <summary>
    /// Thrown when a producer or consumer references a connection name
    /// that does not exist in the configuration.
    /// </summary>
    public sealed class ConnectionNotFoundException(string connectionName) : RabbitMqException($"No RabbitMQ connection registered with name '{connectionName}'. " +

<<<<<<< TODO: Unmerged change from project 'RedRabbit (net9.0)', Before:
                   $"Ensure the name matches a key in the {nameof(Configuration.RabbitMqSettings.Connections)} dictionary.")
    {
=======
                   $"Ensure the name matches a key in the {nameof(RabbitMqSettings.Connections)} dictionary.")
    {
>>>>>>> After
                   $"Ensure the name matches a key in the {nameof(RedRabbit.Configuration.RabbitMqSettings.Connections)} dictionary.")
    {
        /// <summary>
        /// The connection name that was not found.
        /// </summary>
        public string ConnectionName { get; } = connectionName;
    }

}
