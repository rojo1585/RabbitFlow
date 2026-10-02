using System.Security.Authentication;

namespace RedRabbit.Configuration;

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
    /// Path to a CA certificate (PEM or DER) or PEM bundle used as the ONLY trusted root(s)
    /// when validating the broker's certificate. Use this for brokers whose certificates are
    /// issued by a private/corporate CA that is not in the OS trust store.
    /// </summary>
    /// <remarks>
    /// Hostname verification, expiry, signature and revocation (unless
    /// <see cref="DisableCertificateRevocationCheck"/>) are still enforced. This is the
    /// production-safe alternative to <see cref="AllowUnknownCAs"/>.
    /// </remarks>
    public string? CaCertificatePath { get; init; }

    /// <summary>
    /// Whether to accept server certificates whose chain ends in an untrusted root
    /// (e.g. self-signed certificates). Defaults to false for security.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Only an untrusted root is tolerated: hostname mismatches and expired, not-yet-valid,
    /// revoked or badly signed certificates are still rejected. Ignored when
    /// <see cref="CaCertificatePath"/> is set.
    /// </para>
    /// <para>
    /// <b>WARNING:</b> Only enable this in development/staging environments. It accepts any
    /// self-signed certificate for the expected hostname and therefore exposes the connection
    /// to man-in-the-middle attacks. In production use <see cref="CaCertificatePath"/>.
    /// Self-signed certificates usually have no revocation endpoint, so this typically also
    /// requires <see cref="DisableCertificateRevocationCheck"/> = true.
    /// </para>
    /// </remarks>
    public bool AllowUnknownCAs { get; init; }

    /// <summary>
    /// Whether to disable certificate revocation checks (CRL / OCSP).
    /// Defaults to false (revocation checks are performed).
    /// </summary>
    /// <remarks>
    /// Some environments (air-gapped networks, private CAs without CRL/OCSP endpoints,
    /// self-signed certificates) require this to be true, otherwise the TLS handshake fails
    /// because the revocation status cannot be determined.
    /// </remarks>
    public bool DisableCertificateRevocationCheck { get; init; }
}
