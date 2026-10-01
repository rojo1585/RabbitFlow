using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Infrastructure.Connection;

/// <summary>
/// Server certificate validation for TLS connections, driven by <see cref="TlsOptions"/>.
/// <para>
/// Validation rules (in order):
/// </para>
/// <list type="number">
///   <item>No policy errors → accepted.</item>
///   <item>Any error other than a chain error (hostname mismatch, no certificate) → rejected.
///   Neither <see cref="TlsOptions.CaCertificatePath"/> nor <see cref="TlsOptions.AllowUnknownCAs"/>
///   relaxes hostname verification.</item>
///   <item><see cref="TlsOptions.CaCertificatePath"/> set → the chain is rebuilt trusting ONLY
///   the configured CA(s) as roots (custom root trust). Expiry, signature and revocation (unless
///   <see cref="TlsOptions.DisableCertificateRevocationCheck"/>) are still enforced.</item>
///   <item><see cref="TlsOptions.AllowUnknownCAs"/> → accepted only if the sole chain problem is
///   an untrusted root. Expired, not-yet-valid, revoked or badly signed certificates are still
///   rejected.</item>
///   <item>Otherwise → rejected.</item>
/// </list>
/// </summary>
internal static class TlsCertificateValidator
{
    /// <summary>
    /// Creates the validation callback for <paramref name="tls"/>, or <c>null</c> when the
    /// driver's default validation (system trust store) is sufficient.
    /// </summary>
    public static RemoteCertificateValidationCallback? CreateCallback(TlsOptions tls)
    {
        if (tls.CaCertificatePath is null && !tls.AllowUnknownCAs)
            return null;

        var trustedCas = tls.CaCertificatePath is not null ? LoadCertificates(tls.CaCertificatePath) : null;

        return (_, certificate, chain, sslPolicyErrors) => Validate(tls, trustedCas, certificate, chain, sslPolicyErrors);
    }

    internal static bool Validate(TlsOptions tls, X509Certificate2Collection? trustedCas, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors sslPolicyErrors)
    {
        if (sslPolicyErrors == SslPolicyErrors.None)
            return true;

        if ((sslPolicyErrors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0 || certificate is null)
            return false;

        if (trustedCas is { Count: > 0 })
            return BuildChainWithCustomRoots(tls, trustedCas, certificate, chain);

        if (tls.AllowUnknownCAs && chain is not null)
            return chain.ChainStatus.All(s => s.Status is X509ChainStatusFlags.NoError or X509ChainStatusFlags.UntrustedRoot);

        return false;
    }

    private static bool BuildChainWithCustomRoots(TlsOptions tls, X509Certificate2Collection trustedCas, X509Certificate certificate, X509Chain? presentedChain)
    {
        using var customChain = new X509Chain();
        customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        customChain.ChainPolicy.CustomTrustStore.AddRange(trustedCas);
        customChain.ChainPolicy.RevocationMode = tls.DisableCertificateRevocationCheck ? X509RevocationMode.NoCheck : X509RevocationMode.Online;

        // Intermediates sent by the server.
        if (presentedChain is not null)
        {
            foreach (var element in presentedChain.ChainElements)
                customChain.ChainPolicy.ExtraStore.Add(element.Certificate);
        }

        var serverCertificate = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
        return customChain.Build(serverCertificate);
    }

    /// <summary>
    /// Loads one or more CA certificates from a PEM bundle or a single DER/PEM certificate file.
    /// </summary>
    internal static X509Certificate2Collection LoadCertificates(string path)
    {
        var collection = new X509Certificate2Collection();

        if (File.ReadAllText(path).Contains("-----BEGIN CERTIFICATE-----", StringComparison.Ordinal))
        {
            collection.ImportFromPemFile(path);
        }
        else
        {
#if NET9_0_OR_GREATER
            collection.Add(X509CertificateLoader.LoadCertificateFromFile(path));
#else
            collection.Add(new X509Certificate2(path));
#endif
        }

        if (collection.Count == 0)
            throw new InvalidOperationException($"No certificates found in CA certificate file '{path}'.");

        return collection;
    }
}
