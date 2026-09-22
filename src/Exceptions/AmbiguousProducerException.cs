using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions
{
    /// <summary>
    /// Thrown when <see cref="Abstractions.IEventPublisher.PublishAsync"/>
    /// is called without a producer key but multiple producers are registered.
    /// </summary>
    public sealed class AmbiguousProducerException : RabbitMqException
    {
        public AmbiguousProducerException(int producerCount, string[] registeredKeys)
            : base($"Cannot resolve a default producer because {producerCount} producers are registered: " +
                   $"[{string.Join(", ", registeredKeys)}]. " +
                   "Specify a producer key explicitly, or register only one producer.")
        {
        }

        public AmbiguousProducerException()
            : base("Cannot resolve a default producer because no producers are registered.")
        {
        }
    }
}
