using FluentAssertions;
using RedRabbit.Configuration;

namespace RedRabbit.Tests.Unit.Configuration;



public class RabbitConnectionOptionsTests
{
    [Fact]
    public void DefaultPort_Is5672()
    {
        var opts = new RabbitConnectionOptions
        {
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
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        opts.ConnectionTimeoutSeconds.Should().Be(30);
    }
}
