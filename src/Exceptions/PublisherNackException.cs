using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions
{

    /// <summary>
    /// Thrown when the broker nacks a published message or returns it as unroutable.
    ///
    /// <para>
    /// In RabbitMQ.Client v7, this wraps <c>RabbitMQ.Client.Exceptions.PublishException</c>
    /// which is thrown by <c>BasicPublishAsync</c> when publisher confirmation tracking is enabled
    /// and the broker responds with a nack or basic.return.
    /// </para>
    ///
    /// <para>
    /// Callers should catch <see cref="PublisherNackException"/> instead of the underlying
    /// <c>PublishException</c> to avoid a hard dependency on the RabbitMQ.Client driver.
    /// The <see cref="IsReturn"/> property distinguishes a basic.return (unroutable message)
    /// from a basic.nack (broker rejected the message), and <see cref="PublishSequenceNumber"/>
    /// correlates the failure with the broker's publish sequence.
    /// </para>
    /// </summary>
    public sealed class PublisherNackException(string producerKey, ulong publishSequenceNumber, bool isReturn, Exception? innerException = null) :
        RabbitMqException(isReturn ? $"Publisher '{producerKey}': message #{publishSequenceNumber} was returned as unroutable (no matching queue binding on the exchange)." : $"Publisher '{producerKey}': message #{publishSequenceNumber} was nacked by the broker.", innerException)
    {
        /// <summary>
        /// The producer that sent the nacked message.
        /// </summary>
        public string ProducerKey { get; } = producerKey;

        /// <summary>
        /// The broker-assigned publish sequence number of the failed message.
        /// </summary>
        public ulong PublishSequenceNumber { get; } = publishSequenceNumber;

        /// <summary>
        /// <c>true</c> if the message was returned as unroutable (basic.return);
        /// <c>false</c> if the broker sent a negative acknowledgement (basic.nack).
        /// </summary>
        public bool IsReturn { get; } = isReturn;
    }
}
