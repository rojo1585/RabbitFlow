using System;
using System.Collections.Generic;
using System.Security.Authentication;
using System.Text;

namespace RabbitFlow.Configuration;

/// <summary>
/// TLS/SSL configuration for a RabbitMQ connection.
/// 
/// <para>
/// Required for connecting to cloud-hosted brokers (AWS Amazon MQ, Azure Service Bus,
/// CloudAMQP, etc.) or any environment that mandates encrypted connections.
/// </para>
/// 
/// <para>
/// Supports both server-authenticated TLS (broker presents a certificate)
/// and mutual TLS / mTLS (client also presents a certificate).
/// </para>
/// 
/// <para>
/// <b>appsettings.json example:</b>
/// <code>
/// "Connections": {
///   "cloud": {
///     "HostName": "b-123.mq.us-east-1.amazonaws.com",
///     "Port": 5671,
///     "Tls": {
///       "Enabled": true,
///       "ServerName": "b-123.mq.us-east-1.amazonaws.com",
///       "Protocol": "Tls12"
///     }
///   }
/// }
/// </code>
/// </para>
/// 
/// <para>
/// <b>mTLS example (client certificate):</b>
/// <code>
/// "Tls": {
///   "Enabled": true,
///   "ServerName": "my-broker.internal",
///   "CertPath": "/certs/client.p12",
///   "CertPassphrase": "secret",
///   "Protocol": "Tls13"
/// }
/// </code>
/// </para>
/// </summary>
public sealed class TlsOptions
{
    /// <summary>
    /// Whether to enable TLS/SSL for this connection.
    /// When true, the connection uses the AMQPS protocol (typically port 5671).
    /// Defaults to false for backward compatibility.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// The server name used for SNI (Server Name Indication) and the default
    /// hostname to match against the server's X.509 certificate CN/SAN.
    /// If null, falls back to <see cref="RabbitConnectionOptions.HostName"/>.
    /// </summary>
    /// <remarks>
    /// Important for cloud brokers where the hostname differs from the certificate
    /// CN (e.g. AWS Amazon MQ uses a load balancer with a different cert name).
    /// </remarks>
    public string? ServerName { get; init; }

    /// <summary>
    /// Path to the client certificate file for mutual TLS (mTLS).
    /// Supports PFX/P12 format.
    /// If null, only server authentication is performed (standard TLS).
    /// </summary>
    public string? CertPath { get; init; }

    /// <summary>
    /// Passphrase for the client certificate file.
    /// Only used when <see cref="CertPath"/> is set.
    /// </summary>
    public string? CertPassphrase { get; init; }

    /// <summary>
    /// The TLS protocol version(s) to use.
    /// Defaults to <see cref="SslProtocols.None"/> which lets the OS choose
    /// the highest supported protocol (typically TLS 1.2 or 1.3 on modern systems).
    /// </summary>
    /// <remarks>
    /// For restrictive corporate environments, set this to <see cref="SslProtocols.Tls12"/>
    /// or <see cref="SslProtocols.Tls13"/> explicitly.
    /// </remarks>
    public SslProtocols Protocol { get; init; } = SslProtocols.None;

    /// <summary>
    /// Whether to accept server certificates from unknown issuers (self-signed certs).
    /// Defaults to false for security.
    /// </summary>
    /// <remarks>
    /// <b>WARNING:</b> Only enable this in development/staging environments.
    /// Never enable in production — it defeats server certificate validation
    /// and exposes the connection to man-in-the-middle attacks.
    /// </remarks>
    public bool AllowUnknownCAs { get; init; }

    /// <summary>
    /// Whether to disable certificate revocation check (CRL / OCSP).
    /// Defaults to false (revocation checks are performed).
    /// </summary>
    /// <remarks>
    /// Some environments (air-gapped networks, brokers with self-signed certs)
    /// may require% require this to be true if CRL endpoints are unreachable.
    /// </remarks>
    public bool DisableCertificateRevocationCheck { get; init; }
}
