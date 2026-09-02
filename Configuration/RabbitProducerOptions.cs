using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Configuration
{

    /// <summary>
    /// Defines a message producer bound to a specific exchange and connection.
    /// The <see cref="ServiceKey"/> is used to route publish calls to the correct producer.
    /// </summary>
    public sealed class RabbitProducerOptions
    {
        /// <summary>
        /// Unique identifier for this producer.
        /// Used in <see cref="Abstractions.IEventPublisher.PublishAsync"/>
        /// to target a specific producer when multiple exist.
        /// Must be unique across all producers.
        /// </summary>
        public required string ServiceKey { get; init; }

        /// <summary>
        /// References <see cref="RabbitConnectionOptions.Name"/> to determine
        /// which connection this producer uses.
        /// </summary>
        public required string ConnectionName { get; init; }

        /// <summary>
        /// Name of the exchange to publish to. Declared automatically at startup.
        /// </summary>
        public required string ExchangeName { get; init; }

        /// <summary>
        /// Exchange type: direct, topic, fanout, or headers.
        /// Defaults to "direct".
        /// </summary>
        public string ExchangeType { get; init; } = "direct";

        /// <summary>
        /// Default routing key used when none is provided at publish time.
        /// </summary>
        public required string RoutingKey { get; init; }

        /// <summary>
        /// Whether to use the AMQP mandatory flag.
        /// When true, the broker returns unroutable messages.
        /// Defaults to true to prevent silent message loss.
        /// </summary>
        public bool Mandatory { get; init; } = true;

        /// <summary>
        /// Whether to enable publisher confirms for this producer.
        /// When true, channels are created with <c>CreateChannelOptions(publisherConfirmationsEnabled: true,
        /// publisherConfirmationTrackingEnabled: true)</c> so that <c>BasicPublishAsync</c> blocks until
        /// the broker confirms the message. On failure, a <see cref="Exceptions.PublisherNackException"/>
        /// is thrown. On timeout, a <see cref="Exceptions.PublisherConfirmTimeoutException"/> is thrown.
        /// Defaults to true.
        /// </summary>
        public bool EnablePublisherConfirms { get; init; } = true;

        /// <summary>
        /// Maximum time in milliseconds to wait for publisher confirms.
        /// After this timeout, a <see cref="TimeoutException"/> is thrown.
        /// Defaults to 5000ms (5 seconds).
        /// Only applies when <see cref="EnablePublisherConfirms"/> is true.
        /// </summary>
        public int PublishConfirmTimeoutMs { get; init; } = 5_000;

        /// <summary>
        /// Whether to automatically declare the exchange topology at startup.
        /// Defaults to true.
        /// </summary>
        public bool AutoDeclareTopology { get; init; } = true;
    }
}
