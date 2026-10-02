using FluentAssertions;
using RedRabbit.Configuration;
using RedRabbit.Infrastructure.Connection;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace RedRabbit.Tests.Unit.Connection;

public class TlsCertificateValidatorTests
{
    private const string BrokerHost = "broker.internal";

    [Fact]
    public void CreateCallback_WithoutCustomCaOrAllowUnknownCAs_ReturnsNull()
    {
        TlsCertificateValidator.CreateCallback(new TlsOptions { Enabled = true }).Should().BeNull();
    }

    [Fact]
    public void Validate_NoPolicyErrors_Accepts()
    {
        using var ca = CreateCa("CN=Corp Root CA");
        using var leaf = CreateLeaf(ca, BrokerHost);

        TlsCertificateValidator.Validate(new TlsOptions(), null, leaf, null, SslPolicyErrors.None).Should().BeTrue();
    }

    [Fact]
    public void Validate_NameMismatch_IsRejected_EvenWithAllowUnknownCAsAndCustomCa()
    {
        using var ca = CreateCa("CN=Corp Root CA");
        using var leaf = CreateLeaf(ca, BrokerHost);
        var tls = new TlsOptions { AllowUnknownCAs = true, DisableCertificateRevocationCheck = true };

        TlsCertificateValidator.Validate(tls, [ca], leaf, null, SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeFalse();
    }

    [Fact]
    public void Validate_ChainErrors_WithoutRelaxation_IsRejected()
    {
        using var selfSigned = CreateCa("CN=" + BrokerHost);
        using var chain = BuildSystemChain(selfSigned);

        TlsCertificateValidator.Validate(new TlsOptions(), null, selfSigned, chain, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeFalse();
    }

    [Fact]
    public void Validate_AllowUnknownCAs_AcceptsSelfSignedCertificate()
    {
        using var selfSigned = CreateCa("CN=" + BrokerHost);
        using var chain = BuildSystemChain(selfSigned);

        TlsCertificateValidator.Validate(new TlsOptions { AllowUnknownCAs = true }, null, selfSigned, chain, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue();
    }

    [Fact]
    public void Validate_AllowUnknownCAs_RejectsExpiredSelfSignedCertificate()
    {
        using var expired = CreateCa("CN=" + BrokerHost, notBefore: DateTimeOffset.UtcNow.AddDays(-10), notAfter: DateTimeOffset.UtcNow.AddDays(-1));
        using var chain = BuildSystemChain(expired);

        TlsCertificateValidator.Validate(new TlsOptions { AllowUnknownCAs = true }, null, expired, chain, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeFalse();
    }

    [Fact]
    public void Validate_CustomCa_AcceptsCertificateIssuedByThatCa()
    {
        using var ca = CreateCa("CN=Corp Root CA");
        using var leaf = CreateLeaf(ca, BrokerHost);
        using var chain = BuildSystemChain(leaf);

        TlsCertificateValidator.Validate(new TlsOptions { DisableCertificateRevocationCheck = true }, [ca], leaf, chain, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue();
    }

    [Fact]
    public void Validate_CustomCa_RejectsCertificateIssuedByAnotherCa()
    {
        using var trustedCa = CreateCa("CN=Corp Root CA");
        using var otherCa = CreateCa("CN=Attacker CA");
        using var leaf = CreateLeaf(otherCa, BrokerHost);
        using var chain = BuildSystemChain(leaf);

        TlsCertificateValidator.Validate(new TlsOptions { DisableCertificateRevocationCheck = true, AllowUnknownCAs = true }, [trustedCa], leaf, chain, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeFalse();
    }

    [Fact]
    public void LoadCertificates_ReadsAllCertificatesFromPemBundle()
    {
        using var ca1 = CreateCa("CN=Root One");
        using var ca2 = CreateCa("CN=Root Two");
        var path = Path.Combine(Path.GetTempPath(), $"rabbitflow-ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, ca1.ExportCertificatePem() + "\n" + ca2.ExportCertificatePem());

        try
        {
            TlsCertificateValidator.LoadCertificates(path).Should().HaveCount(2);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void LoadCertificates_ReadsDerCertificate()
    {
        using var ca = CreateCa("CN=Root DER");
        var path = Path.Combine(Path.GetTempPath(), $"rabbitflow-ca-{Guid.NewGuid():N}.cer");
        File.WriteAllBytes(path, ca.Export(X509ContentType.Cert));

        try
        {
            TlsCertificateValidator.LoadCertificates(path).Should().ContainSingle();
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static X509Certificate2 CreateCa(string subject, DateTimeOffset? notBefore = null, DateTimeOffset? notAfter = null)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign | X509KeyUsageFlags.DigitalSignature, true));
        return request.CreateSelfSigned(notBefore ?? DateTimeOffset.UtcNow.AddDays(-1), notAfter ?? DateTimeOffset.UtcNow.AddYears(1));
    }

    private static X509Certificate2 CreateLeaf(X509Certificate2 issuer, string dnsName)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={dnsName}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(dnsName);
        request.CertificateExtensions.Add(san.Build());
        return request.Create(issuer, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddDays(30), Guid.NewGuid().ToByteArray());
    }

    /// <summary>
    /// Builds the chain the way SslStream would against the system trust store, so the
    /// resulting ChainStatus mirrors what the validation callback receives at runtime.
    /// </summary>
    private static X509Chain BuildSystemChain(X509Certificate2 certificate)
    {
        var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.Build(certificate);
        return chain;
    }
}
