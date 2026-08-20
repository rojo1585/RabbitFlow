using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Configuration
{

    /// <summary>
    /// Root configuration model bound from appsettings.json.
    /// Contains all connections, producers, and consumers definitions.
    /// 
    /// <example>
    /// In appsettings.json:
    /// <code>
    /// {
    ///   "RabbitMQ": {
    ///     "Connections": {
    ///       "some": { "HostName": "...", "UserName": "...", ... },
    ///       "some_one": { "HostName": "...", "UserName": "...", ... }
    ///     },
    ///     "Producers": [
    ///       { "ServiceKey": "uber-delivered", "ConnectionName": "some", ... }
    ///     ],
    ///     "Consumers": [
    ///       { "ServiceKey": "uber-consumer", "ConnectionName": "some", ... }
    ///     ]
    ///   }
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public sealed class RabbitMqSettings
    {
        /// <summary>
        /// Default configuration section name in appsettings.json.
        /// </summary>
        public const string SectionName = "RabbitMQ";

        /// <summary>
        /// Named connections to RabbitMQ brokers.
        /// Each key is the connection name used by producers and consumers.
        /// </summary>
        public Dictionary<string, RabbitConnectionOptions> Connections { get; set; } = new();

        /// <summary>
        /// Producer definitions. Each producer publishes to a specific exchange
        /// on a specific connection.
        /// </summary>
        public List<RabbitProducerOptions> Producers { get; set; } = new();

        /// <summary>
        /// Consumer definitions. Each consumer listens on a specific queue
        /// on a specific connection.
        /// </summary>
        public List<RabbitConsumerOptions> Consumers { get; set; } = new();
    }
}
