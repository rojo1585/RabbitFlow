using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions
{

    /// <summary>
    /// Thrown when publisher confirms are enabled but the broker
    /// does not confirm the message within the configured timeout.
    /// </summary>
    public sealed class PublisherConfirmTimeoutException(string producerKey, TimeSpan timeout) : RabbitMqException($"Publisher '{producerKey}' did not receive a confirm within {timeout.TotalMilliseconds}ms.")
    {
        /// <summary>
        /// The producer that timed out.
        /// </summary>
        public string ProducerKey { get; } = producerKey;

        /// <summary>
        /// The configured timeout duration.
        /// </summary>
        public TimeSpan Timeout { get; } = timeout;
    }

}
