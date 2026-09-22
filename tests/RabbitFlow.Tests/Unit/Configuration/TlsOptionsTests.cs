using FluentAssertions;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Authentication;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Configuration;
public class TlsOptionsTests
{
    [Fact]
    public void Enabled_DefaultsToFalse()
    {
        var tls = new TlsOptions();
        tls.Enabled.Should().BeFalse();
    }

    [Fact]
    public void ServerName_DefaultsToNull()
    {
        var tls = new TlsOptions();
        tls.ServerName.Should().BeNull();
    }

    [Fact]
    public void CertPath_DefaultsToNull()
    {
        var tls = new TlsOptions();
        tls.CertPath.Should().BeNull();
    }

    [Fact]
    public void CertPassphrase_DefaultsToNull()
    {
        var tls = new TlsOptions();
        tls.CertPassphrase.Should().BeNull();
    }

    [Fact]
    public void Protocol_DefaultsToNone()
    {
        var tls = new TlsOptions();
        tls.Protocol.Should().Be(SslProtocols.None);
    }

    [Fact]
    public void AllowUnknownCAs_DefaultsToFalse()
    {
        var tls = new TlsOptions();
        tls.AllowUnknownCAs.Should().BeFalse();
    }

    [Fact]
    public void DisableCertificateRevocationCheck_DefaultsToFalse()
    {
        var tls = new TlsOptions();
        tls.DisableCertificateRevocationCheck.Should().BeFalse();
    }

    [Fact]
    public void CanCreate_ServerTls()
    {
        var tls = new TlsOptions
        {
            Enabled = true,
            ServerName = "b-123.mq.amazonaws.com",
            Protocol = SslProtocols.Tls12
        };

        tls.Enabled.Should().BeTrue();
        tls.ServerName.Should().Be("b-123.mq.amazonaws.com");
        tls.Protocol.Should().Be(SslProtocols.Tls12);
        tls.CertPath.Should().BeNull(); // no mTLS
    }

    [Fact]
    public void CanCreate_MutualTls()
    {
        var tls = new TlsOptions
        {
            Enabled = true,
            ServerName = "rabbit.internal",
            CertPath = "/certs/client.p12",
            CertPassphrase = "secret",
            Protocol = SslProtocols.Tls13
        };

        tls.CertPath.Should().Be("/certs/client.p12");
        tls.CertPassphrase.Should().Be("secret");
        tls.Protocol.Should().Be(SslProtocols.Tls13);
    }
}

public class RabbitConnectionOptionsTlsTests
{
    [Fact]
    public void Tls_DefaultsToNull()
    {
        var opts = new RabbitConnectionOptions
        {
            Name = "test",
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        opts.Tls.Should().BeNull();
    }

    [Fact]
    public void Tls_CanBeSet()
    {
        var opts = new RabbitConnectionOptions
        {
            Name = "cloud",
            HostName = "b-123.mq.amazonaws.com",
            Port = 5671,
            UserName = "user",
            Password = "pass",
            Tls = new TlsOptions { Enabled = true }
        };
        opts.Tls.Should().NotBeNull();
        opts.Tls!.Enabled.Should().BeTrue();
    }
}
