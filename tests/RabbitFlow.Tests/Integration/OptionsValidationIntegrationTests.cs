using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitFlow.Configuration;
using RabbitFlow.Extensions;

namespace RabbitFlow.Tests.Integration;


[Collection(RabbitMqCollection.Name)]
public class OptionsValidationIntegrationTests(RabbitMqFixture fixture)
{
    [Fact]
    public async Task ValidConfiguration_HostStartsSuccessfully()
    {
        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new ()
                {
                    Name = "main",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password
                };

                settings.Producers.Add(new ()
                {
                    ServiceKey = "test",
                    ConnectionName = "main",
                    ExchangeName = "test",
                    RoutingKey = "test"
                });
            });
        });

        using var host = hostBuilder.Build();
       var ct = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var act = () => host.StartAsync(ct.Token);

        await act.Should().NotThrowAsync();

        await host.StopAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task MissingConnectionReference_HostFailsToStart()
    {
        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new ()
                {
                    Name = "main",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password
                };

                settings.Producers.Add(new ()
                {
                    ServiceKey = "broken",
                    ConnectionName = "nonexistent",
                    ExchangeName = "broken",
                    RoutingKey = "broken"
                });
            });
        });

        using var host = hostBuilder.Build();
        var ct = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var act = () => host.StartAsync(ct.Token);

        await act.Should().ThrowAsync<OptionsValidationException>();
    }

    [Fact]
    public async Task DuplicateProducerKey_HostFailsToStart()
    {
        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new ()
                {
                    Name = "main",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password
                };

                settings.Producers.Add(new ()
                {
                    ServiceKey = "dup",
                    ConnectionName = "main",
                    ExchangeName = "ex1",
                    RoutingKey = "rk"
                });

                settings.Producers.Add(new ()
                {
                    ServiceKey = "dup",
                    ConnectionName = "main",
                    ExchangeName = "ex2",
                    RoutingKey = "rk"
                });
            });
        });

        using var host = hostBuilder.Build();
        var ct = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var act = () => host.StartAsync(ct.Token);

        await act.Should().ThrowAsync<OptionsValidationException>();
    }

    [Fact]
    public async Task IOptions_ResolvesWithCorrectSettings()
    {
        const string instrName = "Test.Instrumentation";

        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new ()
                {
                    Name = "main",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password
                };
            }, builder => builder.WithInstrumentationName(instrName));
        });

        using var host = hostBuilder.Build();
        var ct = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await host.StartAsync(ct.Token);

        var options = host.Services.GetRequiredService<IOptions<RabbitMqSettings>>();
        options.Value.InstrumentationName.Should().Be(instrName);
        options.Value.Connections.Should().ContainKey("main");

        await host.StopAsync(TimeSpan.FromSeconds(10));
    }
}
