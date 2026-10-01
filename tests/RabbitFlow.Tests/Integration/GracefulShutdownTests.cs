using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;
using System.Reflection;

namespace RabbitFlow.Tests.Integration;



[Collection(RabbitMqCollection.Name)]
public class GracefulShutdownTests(RabbitMqFixture fixture)
{
    [EventVersion("shutdown-event", 1)]
    public record ShutdownEvent(Guid Id) : IIntegrationEvent;
    private class SlowHandler : IRabbitHandler<ShutdownEvent>
    {
        public static int ProcessedCount;
        public static readonly List<Guid> ProcessedIds = [];

        public async Task HandleAsync(ShutdownEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(500, cancellationToken);
            Interlocked.Increment(ref ProcessedCount);
            ProcessedIds.Add(@event.Id);
        }
    }

    private class UncancellableSlowHandler : IRabbitHandler<ShutdownEvent>
    {
        public static int ProcessedCount;
        public static readonly List<Guid> ProcessedIds = [];
        public static int StartedCount;

        public async Task HandleAsync(ShutdownEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref StartedCount);
            await Task.Delay(TimeSpan.FromSeconds(2));
            Interlocked.Increment(ref ProcessedCount);
            ProcessedIds.Add(@event.Id);
        }
    }
    private class BlockingHandler : IRabbitHandler<ShutdownEvent>
    {
        public static int StartedCount;
        public static int CompletedCount;

        public async Task HandleAsync(ShutdownEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref StartedCount);
            await Task.Delay(TimeSpan.FromSeconds(10));
            Interlocked.Increment(ref CompletedCount);
        }
    }

    [Fact]
    public async Task StopAsync_CompletesWithoutHanging()
    {
        SlowHandler.ProcessedCount = 0;
        SlowHandler.ProcessedIds.Clear();

        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new RabbitConnectionOptions
                {
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new RabbitProducerOptions
                {
                    ServiceKey = "shutdown-producer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown",
                    ExchangeType = "direct",
                    RoutingKey = "shutdown-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "shutdown-consumer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown",
                    ExchangeType = "direct",
                    QueueName = "shutdown-queue",
                    RoutingKey = "shutdown-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });
            });

            services.AddRabbitHandler<SlowHandler>("shutdown-consumer");
        });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        await publisher.PublishAsync(new ShutdownEvent(Guid.NewGuid()));

        await Task.Delay(2000);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync(TimeSpan.FromSeconds(30));
        sw.Stop();

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task StopAsync_InFlightHandlers_CompleteBeforeShutdown()
    {
        SlowHandler.ProcessedCount = 0;
        SlowHandler.ProcessedIds.Clear();

        var hostBuilder = Host.CreateDefaultBuilder();
        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new RabbitConnectionOptions
                {
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new RabbitProducerOptions
                {
                    ServiceKey = "shutdown-producer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown2",
                    ExchangeType = "direct",
                    RoutingKey = "shutdown-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "shutdown-consumer",
                    ConnectionName = "main",
                    ExchangeName = "shutdown2",
                    ExchangeType = "direct",
                    QueueName = "shutdown2-queue",
                    RoutingKey = "shutdown-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3
                });
            });

            services.AddRabbitHandler<SlowHandler>("shutdown-consumer");
        });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var eventId = Guid.NewGuid();
        await publisher.PublishAsync(new ShutdownEvent(eventId));

        await Task.Delay(300);

        await host.StopAsync(TimeSpan.FromSeconds(30));

        SlowHandler.ProcessedIds.Should().Contain(eventId);
    }

    [Fact]
    public async Task ShutdownDrain_AllowsInProgressHandlers_ToComplete()
    {
        UncancellableSlowHandler.ProcessedCount = 0;
        UncancellableSlowHandler.ProcessedIds.Clear();
        UncancellableSlowHandler.StartedCount = 0;

        const string exchangeName = "shutdown-drain-test";
        const string queueName = "shutdown-drain-queue";

        using var host = await BuildHostAsync(
            shutdownDrainTimeout: TimeSpan.FromSeconds(10),
            exchangeName: exchangeName,
            queueName: queueName,
            handlerType: typeof(UncancellableSlowHandler));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var eventIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in eventIds)
            await publisher.PublishAsync(new ShutdownEvent(id));

        await WaitForAsync(() => UncancellableSlowHandler.StartedCount >= 3, TimeSpan.FromSeconds(5));

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync();
        sw.Stop();

        UncancellableSlowHandler.ProcessedCount.Should().Be(3,
            "all in-flight handlers should complete during the drain timeout, not be cancelled");
        UncancellableSlowHandler.ProcessedIds.Should().BeEquivalentTo(eventIds,
            "all 3 published event ids should have been processed by the handler");

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30),
            "shutdown should not hang — 30s is a generous anti-hang ceiling for CI, not a performance assertion");
    }

    [Fact]
    public async Task ShutdownDrain_NacksUnfinishedMessages_WithRequeue_AfterTimeout()
    {
        BlockingHandler.StartedCount = 0;
        BlockingHandler.CompletedCount = 0;

        const string exchangeName = "shutdown-drain-timeout-test";
        const string queueName = "shutdown-drain-timeout-queue";

        using var host = await BuildHostAsync(
            shutdownDrainTimeout: TimeSpan.FromSeconds(1),
            exchangeName: exchangeName,
            queueName: queueName,
            handlerType: typeof(BlockingHandler));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await publisher.PublishAsync(new ShutdownEvent(Guid.NewGuid()));

        await WaitForAsync(() => BlockingHandler.StartedCount >= 1, TimeSpan.FromSeconds(5));

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var stopTask = host.StopAsync();
        var winner = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(30)));
        sw.Stop();

        (winner == stopTask).Should().BeTrue(
            "shutdown should complete, not hang — 30s ceiling guards against regressions where the handler blocks shutdown");

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30),
            "shutdown should not hang — 30s is a generous anti-hang ceiling, not a performance assertion");

        BlockingHandler.StartedCount.Should().Be(1,
            "the handler should have been invoked exactly once — no spurious reprocessing during shutdown");

        BlockingHandler.CompletedCount.Should().Be(0,
            "the blocking handler should NOT have completed — it should be abandoned after the drain timeout " +
            "and the message NACKed with requeue so the broker can redeliver it on the next consumer start");
    }

    private async Task<IHost> BuildHostAsync(TimeSpan shutdownDrainTimeout, string exchangeName, string queueName, Type handlerType)
    {
        var hostBuilder = Host.CreateDefaultBuilder();

        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new RabbitConnectionOptions
                {
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new RabbitProducerOptions
                {
                    ServiceKey = "shutdown-producer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    RoutingKey = "shutdown-event",
                    EnablePublisherConfirms = true,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "shutdown-consumer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    QueueName = queueName,
                    RoutingKey = "shutdown-event",
                    PrefetchCount = 10,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3,
                    ShutdownDrainTimeout = shutdownDrainTimeout
                });
            });

            var addRabbitHandlerMethod = typeof(RabbitHandlerExtensions)
                .GetMethod(nameof(RabbitHandlerExtensions.AddRabbitHandler), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(handlerType);

            addRabbitHandlerMethod.Invoke(null, [services, "shutdown-consumer"]);
        });

        var host = hostBuilder.Build();
        await host.StartAsync();

        await Task.Delay(2000);
        return host;
    }

    private static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout, TimeSpan? pollInterval = null)
    {
        var poll = pollInterval ?? TimeSpan.FromMilliseconds(200);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(poll);
        }

        throw new TimeoutException($"WaitForAsync timed out after {timeout.TotalSeconds}s");
    }
}

