using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions
{
    /// <summary>
    /// Thrown when attempting to publish to a producer key that is not registered.
    /// </summary>
    /// <remarks>
    /// Initializes a new instance of the <see cref="ProducerNotFoundException"/> class with a specified producer key.
    /// </remarks>
    /// <param name="producerKey"></param>
    public sealed class ProducerNotFoundException(string producerKey) : RabbitMqException($"No producer registered with key '{producerKey}'. " +
                   $"Check that the key matches a {nameof(Configuration.RabbitProducerOptions.ServiceKey)} " +
                   "in the RabbitMQ configuration.")
    {
        /// <summary>
        /// The producer key that was requested but not found.
        /// </summary>
        public string ProducerKey { get; } = producerKey;
    }

}
