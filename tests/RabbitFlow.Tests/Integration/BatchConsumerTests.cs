using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitFlow.Abstractions;
using RabbitFlow.Configuration;
using RabbitFlow.Diagnostics;
using RabbitFlow.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Tests.Integration;



[Collection(RabbitMqCollection.Name)]
public class BatchConsumerTests(RabbitMqFixture fixture)
{
    [EventVersion("telemetry-event", 1)]
    public record TelemetryEvent(Guid DeviceId, double Value, DateTime Timestamp) : IIntegrationEvent;

    private class TelemetryBatchHandler : IBatchRabbitHandler<TelemetryEvent>
    {
        public string ConsumerKey => "telemetry-batch-consumer";
        public static readonly List<IReadOnlyList<TelemetryEvent>> ReceivedBatches = [];
        public static readonly List<TelemetryEvent> AllReceived = [];

        public Task HandleBatchAsync(IReadOnlyList<TelemetryEvent> events, IReadOnlyList<MessageContext> contexts)
        {
            ReceivedBatches.Add(events);
            AllReceived.AddRange(events);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task BatchConsumer_MultipleMessages_DispatchedAsBatch()
    {
        TelemetryBatchHandler.ReceivedBatches.Clear();
        TelemetryBatchHandler.AllReceived.Clear();

        using var host = await BuildHostAsync();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var events = Enumerable.Range(0, 5)
            .Select(i => new TelemetryEvent(Guid.NewGuid(), i * 10.5, DateTime.UtcNow))
            .ToList();

        foreach (var e in events)
            await publisher.PublishAsync(e);

        await WaitForAsync(() => TelemetryBatchHandler.AllReceived.Count >= 5, timeout: TimeSpan.FromSeconds(15));

        foreach (var e in events)
        {
            TelemetryBatchHandler.AllReceived.Should().Contain(r => r.DeviceId == e.DeviceId);
        }

        TelemetryBatchHandler.ReceivedBatches.Should().NotBeEmpty();
    }

    [Fact]
    public async Task BatchConsumer_PartialBatch_FlushedOnTimeout()
    {
        TelemetryBatchHandler.ReceivedBatches.Clear();
        TelemetryBatchHandler.AllReceived.Clear();

        using var host = await BuildHostAsync(batchSize: 100, batchTimeoutMs: 3000);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var event1 = new TelemetryEvent(Guid.NewGuid(), 1.0, DateTime.UtcNow);
        var event2 = new TelemetryEvent(Guid.NewGuid(), 2.0, DateTime.UtcNow);

        await publisher.PublishAsync(event1);
        await publisher.PublishAsync(event2);

        await WaitForAsync(() => TelemetryBatchHandler.AllReceived.Count >= 2, timeout: TimeSpan.FromSeconds(10));

        TelemetryBatchHandler.AllReceived.Should().Contain(r => r.DeviceId == event1.DeviceId);
        TelemetryBatchHandler.AllReceived.Should().Contain(r => r.DeviceId == event2.DeviceId);
    }

    private async Task<IHost> BuildHostAsync(int batchSize = 10, int batchTimeoutMs = 5000)
    {
        var hostBuilder = Host.CreateDefaultBuilder();

        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new()
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

                settings.Producers.Add(new()
                {
                    ServiceKey = "telemetry-producer",
                    ConnectionName = "main",
                    ExchangeName = "telemetry",
                    ExchangeType = "direct",
                    RoutingKey = "telemetry-event",
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new()
                {
                    ServiceKey = "telemetry-batch-consumer",
                    ConnectionName = "main",
                    ExchangeName = "telemetry",
                    ExchangeType = "direct",
                    QueueName = "telemetry-batch-queue",
                    RoutingKey = "telemetry-event",
                    PrefetchCount = 20,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3,
                    EnableBatchConsumer = true,
                    BatchSize = batchSize,
                    BatchTimeoutMs = batchTimeoutMs
                });
            });

            services.AddBatchRabbitHandler<TelemetryBatchHandler>("telemetry-batch-consumer");
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
