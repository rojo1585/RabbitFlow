using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions
{
    /// <summary>
    /// Thrown when <see cref="Abstractions.IEventPublisher.PublishAsync{TEvent}(TEvent, string?, CancellationToken)"/>
    /// is called without a producer key but multiple producers are registered.
    /// </summary>
    public sealed class AmbiguousProducerException : RabbitMqException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AmbiguousProducerException"/> class with a message indicating
        /// </summary>
        /// <param name="producerCount"></param>
        /// <param name="registeredKeys"></param>
        public AmbiguousProducerException(int producerCount, string[] registeredKeys)
            : base($"Cannot resolve a default producer because {producerCount} producers are registered: " +
                   $"[{string.Join(", ", registeredKeys)}]. " +
                   "Specify a producer key explicitly, or register only one producer.")
        {
        }
        /// <summary>
        /// Initializes a new instance of the <see cref="AmbiguousProducerException"/> class with a message indicating
        /// </summary>
        public AmbiguousProducerException()
            : base("Cannot resolve a default producer because no producers are registered.")
        {
        }
    }
}
