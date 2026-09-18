using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{
    /// <summary>
    /// Provides visibility into the state of all managed RabbitMQ connections.
    /// Useful for health checks, dashboards, and debugging.
    /// Inject this service to check connection status at runtime.
    /// </summary>
    public interface IRabbitConnectionRegistry
    {
        /// <summary>
        /// Returns true if the named connection is currently open and operational.
        /// </summary>
        /// <param name="connectionName">
        /// The <see cref="Configuration.RabbitConnectionOptions.Name"/> to check.
        /// </param>
        /// <returns>True if connected, false if disconnected, connecting, or unknown.</returns>
        bool IsConnected(string connectionName);

        /// <summary>
        /// Returns the connection status for all registered connections.
        /// Useful for health check implementations and dashboards.
        /// </summary>
        /// <returns>
        /// A dictionary where the key is the connection name and the value
        /// indicates whether it is currently connected.
        /// </returns>
        IReadOnlyDictionary<string, bool> GetAllConnectionStates();
    }
}
