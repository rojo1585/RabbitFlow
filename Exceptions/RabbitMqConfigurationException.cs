using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Exceptions;


/// <summary>
/// Thrown when the RabbitMQ configuration is invalid.
/// This is thrown at service registration time, not at runtime.
/// </summary>
public sealed class RabbitMqConfigurationException : RabbitMqException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqConfigurationException"/> class with a specified error message.
    /// </summary>
    /// <param name="message"></param>
    public RabbitMqConfigurationException(string message) : base(message) { }
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqConfigurationException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="inner"></param>
    public RabbitMqConfigurationException(string message, Exception inner) : base(message, inner) { }

    /// <summary>
    /// Creates a configuration exception for a duplicate service key.
    /// </summary>
    public static RabbitMqConfigurationException DuplicateProducerKey(string key)
        => new($"Duplicate producer ServiceKey '{key}'. Each producer must have a unique ServiceKey.");

    /// <summary>
    /// Creates a configuration exception for a duplicate service key.
    /// </summary>
    public static RabbitMqConfigurationException DuplicateConsumerKey(string key)
        => new($"Duplicate consumer ServiceKey '{key}'. Each consumer must have a unique ServiceKey.");

    /// <summary>
    /// Creates a configuration exception for a duplicate connection name.
    /// </summary>
    public static RabbitMqConfigurationException DuplicateConnectionName(string name)
        => new($"Duplicate connection name '{name}'. Each connection must have a unique name.");

    /// <summary>
    /// Creates a configuration exception for a missing connection reference.
    /// </summary>
    public static RabbitMqConfigurationException MissingConnection(string producerOrConsumer, string connectionName)
        => new($"{producerOrConsumer} references connection '{connectionName}' but no connection with that name exists in the configuration.");
}
