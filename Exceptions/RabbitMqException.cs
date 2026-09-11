namespace RabbitFlow.Exceptions;

/// <summary>
/// Base exception for all RabbitMQ library errors.
/// </summary>
public class RabbitMqException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqException"/> class.
    /// </summary>
    public RabbitMqException() { }
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqException"/> class with a specified error message.
    /// </summary>
    /// <param name="message"></param>
    public RabbitMqException(string message) : base(message) { }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="innerException"></param>
    public RabbitMqException(string message, Exception innerException) : base(message, innerException) { }
}
