using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions
{
    /// <summary>
    /// Thrown when a producer or consumer references a connection name
    /// that does not exist in the configuration.
    /// </summary>
    public sealed class ConnectionNotFoundException(string connectionName) : RabbitMqException($"No RabbitMQ connection registered with name '{connectionName}'. " +
                   $"Ensure the name matches a key in the {nameof(Configuration.RabbitMqSettings.Connections)} dictionary.")
    {
        /// <summary>
        /// The connection name that was not found.
        /// </summary>
        public string ConnectionName { get; } = connectionName;
    }

}
