using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;
namespace RabbitFlow.Tests.Integration;


[Collection(RabbitMqCollection.Name)]
public class RetryAndDeadLetterTests(RabbitMqFixture fixture)
{
    [EventVersion("flaky-event", 1)]
    public record FlakyEvent(Guid Id, bool ShouldSucceed) : IIntegrationEvent;

    private class FlakyHandler : IRabbitHandler<FlakyEvent>
    {
        public string ConsumerKey => "flaky-consumer";
        public static int AttemptCount;
        public static readonly List<FlakyEvent> SucceededEvents = [];
        public static bool NextAttemptSucceeds;

        public Task HandleAsync(FlakyEvent @event, MessageContext context)
        {
            Interlocked.Increment(ref AttemptCount);

            if (NextAttemptSucceeds || @event.ShouldSucceed)
            {
                SucceededEvents.Add(@event);
                return Task.CompletedTask;
            }

            throw new InvalidOperationException("Simulated transient failure");
        }
    }

    [Fact]
    public async Task HandlerFails_MessageIsRetried()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        using var host = await BuildHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);

        await publisher.PublishAsync(@event);

        await Task.Delay(3000);

        FlakyHandler.AttemptCount.Should().BeGreaterOrEqualTo(1);
    }

    [Fact]
    public async Task HandlerFailsThenSucceeds_MessageIsProcessed()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        using var host = await BuildHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: true);

        await publisher.PublishAsync(@event);

        var received = await WaitForAsync(() => FlakyHandler.SucceededEvents.FirstOrDefault(e => e.Id == @event.Id), timeout: TimeSpan.FromSeconds(10));

        received.Should().NotBeNull();
    }

    [Fact]
    public async Task HandlerAlwaysFails_MessageIsDeadLettered()
    {
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        using var host = await BuildHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);

        await publisher.PublishAsync(@event);

        await Task.Delay(15000);

        FlakyHandler.AttemptCount.Should().BeGreaterOrEqualTo(1);
        FlakyHandler.SucceededEvents.Should().NotContain(e => e.Id == @event.Id);
    }

    private async Task<IHost> BuildHostAsync()
    {
        var hostBuilder = Host.CreateDefaultBuilder();

        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new RabbitConnectionOptions
                {
                    Name = "main",
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    InitialConnectRetryCount = 3,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new RabbitProducerOptions
                {
                    ServiceKey = "flaky-producer",
                    ConnectionName = "main",
                    ExchangeName = "flaky",
                    ExchangeType = "direct",
                    RoutingKey = "flaky-event",
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "flaky-consumer",
                    ConnectionName = "main",
                    ExchangeName = "flaky",
                    ExchangeType = "direct",
                    QueueName = "flaky-queue",
                    RoutingKey = "flaky-event",
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3,
                    RetryDelays = [TimeSpan.Zero, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)]
                });
            });

            services.AddRabbitHandler<FlakyHandler>("flaky-consumer");
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);
        return host;
    }

    private static async Task<T> WaitForAsync<T>(Func<T?> condition, TimeSpan timeout, TimeSpan? pollInterval = null)
    {
        var poll = pollInterval ?? TimeSpan.FromMilliseconds(200);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            var result = condition();
            if (result is not null)
                return result;
            await Task.Delay(poll);
        }

        throw new TimeoutException($"WaitForAsync timed out after {timeout.TotalSeconds}s");
    }
}