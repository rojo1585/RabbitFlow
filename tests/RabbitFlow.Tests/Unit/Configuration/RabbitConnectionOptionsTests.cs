using FluentAssertions;
using RabbitFlow.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Unit.Configuration;


public class RabbitConnectionOptionsTests
{
    [Fact]
    public void DefaultPort_Is5672()
    {
        var opts = new RabbitConnectionOptions
        {
            Name = "test",
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        opts.Port.Should().Be(5672);
    }

    [Fact]
    public void DefaultVirtualHost_IsRoot()
    {
        var opts = new RabbitConnectionOptions
        {
            Name = "test",
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        opts.VirtualHost.Should().Be("/");
    }

    [Fact]
    public void DefaultHeartbeat_Is60()
    {
        var opts = new RabbitConnectionOptions
        {
            Name = "test",
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        opts.RequestedHeartbeatSeconds.Should().Be(60);
    }

    [Fact]
    public void DefaultConnectionTimeout_Is30()
    {
        var opts = new RabbitConnectionOptions
        {
            Name = "test",
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        opts.ConnectionTimeoutSeconds.Should().Be(30);
    }
}
