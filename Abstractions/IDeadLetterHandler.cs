using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Abstractions
{
    /// <summary>
    /// Processes messages that have exhausted all retry attempts and been
    /// routed to the dead-letter queue (DLQ).
    /// Use this to log, store in a database, send alerts, or trigger manual workflows.
    /// </summary>
    /// <remarks>
    /// Implementations are resolved from DI within a dedicated scope per dead-lettered message.
    /// If no handler is registered for a consumer key, the dead-lettered message is
    /// acknowledged and a warning is logged.
    /// </remarks>
    public interface IDeadLetterHandler
    {
        /// <summary>
        /// Identifies which consumer's DLQ this handler processes.
        /// Must match a <see cref="Configuration.RabbitConsumerOptions.ServiceKey"/>.
        /// </summary>
        string ConsumerKey { get; }

        /// <summary>
        /// Processes a dead-lettered message.
        /// </summary>
        /// <param name="message">The dead-lettered message with original body and failure info.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task HandleAsync(DeadLetterMessage message, CancellationToken cancellationToken = default);
    }
}
