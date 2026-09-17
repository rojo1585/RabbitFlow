using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Configuration;


/// <summary>
/// Defines connection parameters for a single RabbitMQ connection.
/// Each named connection can point to a different host, port, or virtual host.
/// </summary>
public sealed class RabbitConnectionOptions
{
    /// <summary>
    /// Unique identifier for this connection. Used by producers and consumers
    /// to reference which connection they should use.
    /// Must be unique across all connections in the configuration.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// RabbitMQ server hostname or IP address.
    /// </summary>
    public required string HostName { get; init; }

    /// <summary>
    /// AMQP protocol port. Defaults to 5672.
    /// </summary>
    public int Port { get; init; } = 5672;

    /// <summary>
    /// Username for AMQP authentication.
    /// </summary>
    public required string UserName { get; init; }

    /// <summary>
    /// Password for AMQP authentication.
    /// </summary>
    public required string Password { get; init; }

    /// <summary>
    /// RabbitMQ virtual host. Defaults to "/".
    /// </summary>
    public string VirtualHost { get; init; } = "/";

    /// <summary>
    /// Heartbeat interval in seconds. RabbitMQ will close connections
    /// that don't respond within this interval.
    /// Defaults to 60 seconds.
    /// </summary>
    public int RequestedHeartbeatSeconds { get; init; } = 60;

    /// <summary>
    /// Connection timeout in seconds. Includes the AMQP handshake.
    /// Defaults to 30 seconds.
    /// </summary>
    public int ConnectionTimeoutSeconds { get; init; } = 30;

    /// <summary>
    /// Maximum number of retry attempts for initial connection.
    /// After exhausting retries, the connection will keep trying indefinitely
    /// with the maximum backoff delay.
    /// Defaults to 5.
    /// </summary>
    public int InitialConnectRetryCount { get; init; } = 5;

    /// <summary>
    /// Maximum backoff delay in seconds for reconnection attempts.
    /// Defaults to 60 seconds.
    /// </summary>
    public int MaxBackoffSeconds { get; init; } = 60;

    /// <summary>
    /// TLS/SSL configuration for this connection.
    /// When null (default), the connection uses plaintext AMQP (port 5672).
    /// When set with <see cref="TlsOptions.Enabled"/> = true, the connection
    /// uses AMQPS (typically port 5671).
    /// </summary>
    /// <remarks>
    /// Required for cloud brokers (AWS Amazon MQ, Azure, CloudAMQP) and
    /// any environment that mandates encrypted connections.
    /// Supports both server-authenticated TLS and mutual TLS (mTLS).
    /// </remarks>
    /// <example>
    /// <code>
    /// // Cloud broker with server TLS
    /// new RabbitConnectionOptions
    /// {
    ///     Name = "cloud",
    ///     HostName = "b-123.mq.amazonaws.com",
    ///     Port = 5671,
    ///     UserName = "user",
    ///     Password = "pass",
    ///     Tls = new TlsOptions { Enabled = true }
    /// };
    /// 
    /// // mTLS with client certificate
    /// new RabbitConnectionOptions
    /// {
    ///     Name = "onprem",
    ///     HostName = "rabbit.internal",
    ///     Port = 5671,
    ///     UserName = "user",
    ///     Password = "pass",
    ///     Tls = new TlsOptions
    ///     {
    ///         Enabled = true,
    ///         ServerName = "rabbit.internal",
    ///         CertPath = "/certs/client.p12",
    ///         CertPassphrase = "secret"
    ///     }
    /// };
    /// </code>
    /// </example>
    public TlsOptions? Tls { get; init; }
}
