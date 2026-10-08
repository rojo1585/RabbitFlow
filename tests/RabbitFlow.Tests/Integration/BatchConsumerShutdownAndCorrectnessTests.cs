using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RedRabbit.Abstractions;
using RedRabbit.Diagnostics;
using RedRabbit.Extensions;

namespace RedRabbit.Tests.Integration;

[Collection(RabbitMqCollection.Name)]
public class BatchConsumerShutdownAndCorrectnessTests(RabbitMqFixture fixture)
{
    private const string ConsumerKey = "batch-correctness-consumer";

    // ───────────────────────── events ─────────────────────────

    [EventVersion("batch-shutdown-event", 1)]
    public record BatchShutdownEvent(Guid Id) : IIntegrationEvent;

    [EventVersion("batch-mix-a", 1)]
    public record MixA(Guid Id) : IIntegrationEvent;

    [EventVersion("batch-mix-b", 1)]
    public record MixB(Guid Id) : IIntegrationEvent;

    [EventVersion("batch-retry-event", 1)]
    public record BatchRetryEvent(Guid Id, string Payload) : IIntegrationEvent;

    [EventVersion("batch-versioned", 1)]
    public record VersionedV1(Guid Id, string Name) : IIntegrationEvent;

    [EventVersion("batch-versioned", 2)]
    public record VersionedV2(Guid Id, string FirstName, string LastName) : IIntegrationEvent;

    // ───────────────────────── handlers ─────────────────────────

    /// <summary>Takes 2s per batch (ignores cancellation) — finishes inside a generous drain window.</summary>
    private class SlowBatchHandler : IBatchRabbitHandler<BatchShutdownEvent>
    {
        public static int StartedCount;
        public static readonly List<Guid> Processed = [];

        public async Task HandleBatchAsync(IReadOnlyList<BatchShutdownEvent> events, IReadOnlyList<MessageContext> contexts, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref StartedCount);
            await Task.Delay(TimeSpan.FromSeconds(2));
            lock (Processed) Processed.AddRange(events.Select(e => e.Id));
        }
    }

    /// <summary>Takes 10s per batch (ignores cancellation) — outlives a short drain window.</summary>
    private class BlockingBatchHandler : IBatchRabbitHandler<BatchShutdownEvent>
    {
        public static int StartedCount;
        public static int CompletedCount;

        public async Task HandleBatchAsync(IReadOnlyList<BatchShutdownEvent> events, IReadOnlyList<MessageContext> contexts, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref StartedCount);
            await Task.Delay(TimeSpan.FromSeconds(10));
            Interlocked.Increment(ref CompletedCount);
        }
    }

    /// <summary>Records everything it receives (used by a second host to observe redeliveries).</summary>
    private class RecordingBatchHandler : IBatchRabbitHandler<BatchShutdownEvent>
    {
        public static readonly List<(Guid Id, int RetryCount)> Received = [];

        public Task HandleBatchAsync(IReadOnlyList<BatchShutdownEvent> events, IReadOnlyList<MessageContext> contexts, CancellationToken cancellationToken)
        {
            lock (Received)
            {
                for (var i = 0; i < events.Count; i++)
                    Received.Add((events[i].Id, contexts[i].RetryCount));
            }
            return Task.CompletedTask;
        }
    }

    private class MixAHandler : IBatchRabbitHandler<MixA>
    {
        public static readonly List<Guid> Received = [];

        public Task HandleBatchAsync(IReadOnlyList<MixA> events, IReadOnlyList<MessageContext> contexts, CancellationToken cancellationToken)
        {
            lock (Received) Received.AddRange(events.Select(e => e.Id));
            return Task.CompletedTask;
        }
    }

    private class MixBHandler : IBatchRabbitHandler<MixB>
    {
        public static readonly List<Guid> Received = [];

        public Task HandleBatchAsync(IReadOnlyList<MixB> events, IReadOnlyList<MessageContext> contexts, CancellationToken cancellationToken)
        {
            lock (Received) Received.AddRange(events.Select(e => e.Id));
            return Task.CompletedTask;
        }
    }

    /// <summary>Fails its first invocation, then records what it receives.</summary>
    private class FailOnceBatchHandler : IBatchRabbitHandler<BatchRetryEvent>
    {
        public static int Invocations;
        public static readonly List<BatchRetryEvent> Received = [];

        public Task HandleBatchAsync(IReadOnlyList<BatchRetryEvent> events, IReadOnlyList<MessageContext> contexts, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref Invocations) == 1)
                throw new InvalidOperationException("Simulated batch failure");

            lock (Received) Received.AddRange(events);
            return Task.CompletedTask;
        }
    }

    private class VersionedBatchHandler : IBatchRabbitHandler<VersionedV2>
    {
        public static readonly List<VersionedV2> Received = [];

        public Task HandleBatchAsync(IReadOnlyList<VersionedV2> events, IReadOnlyList<MessageContext> contexts, CancellationToken cancellationToken)
        {
            lock (Received) Received.AddRange(events);
            return Task.CompletedTask;
        }
    }

    private class VersionedV1ToV2Upgrader : IEventUpgrader<VersionedV1, VersionedV2>
    {
        public string EventName => "batch-versioned";
        public int FromVersion => 1;
        public int ToVersion => 2;

        public VersionedV2 Upgrade(VersionedV1 source)
        {
            var parts = source.Name.Split(' ', 2);
            return new VersionedV2(source.Id, parts[0], parts.Length > 1 ? parts[1] : string.Empty);
        }
    }

    // ───────────────────────── tests ─────────────────────────

    [Fact]
    public async Task Shutdown_InFlightBatchFinishingWithinDrain_IsAckedAndNotRedelivered()
    {
        SlowBatchHandler.StartedCount = 0;
        lock (SlowBatchHandler.Processed) SlowBatchHandler.Processed.Clear();
        lock (RecordingBatchHandler.Received) RecordingBatchHandler.Received.Clear();

        const string name = "batch-shutdown-drain-ok";
        var ids = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToList();

        using (var host = await BuildHostAsync(name, s => s.AddBatchRabbitHandler<SlowBatchHandler>(ConsumerKey), shutdownDrainTimeout: TimeSpan.FromSeconds(15)))
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            foreach (var id in ids)
                await publisher.PublishAsync(new BatchShutdownEvent(id));

            await WaitForAsync(() => SlowBatchHandler.StartedCount >= 1, TimeSpan.FromSeconds(10));

            await host.StopAsync();
        }

        lock (SlowBatchHandler.Processed)
            SlowBatchHandler.Processed.Should().BeEquivalentTo(ids, "the in-flight batch must complete during the drain window");

        // A new consumer on the same queue must not see them again: they were ACKed before the channel closed.
        using var observer = await BuildHostAsync(name, s => s.AddBatchRabbitHandler<RecordingBatchHandler>(ConsumerKey));
        await Task.Delay(TimeSpan.FromSeconds(4));
        await observer.StopAsync();

        lock (RecordingBatchHandler.Received)
            RecordingBatchHandler.Received.Should().BeEmpty("messages processed during the drain must not be redelivered");
    }

    [Fact]
    public async Task Shutdown_BatchExceedingDrain_IsRequeuedWithoutCountingARetry()
    {
        BlockingBatchHandler.StartedCount = 0;
        BlockingBatchHandler.CompletedCount = 0;
        lock (RecordingBatchHandler.Received) RecordingBatchHandler.Received.Clear();

        const string name = "batch-shutdown-drain-timeout";
        var ids = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid()).ToList();

        using (var host = await BuildHostAsync(name, s => s.AddBatchRabbitHandler<BlockingBatchHandler>(ConsumerKey), shutdownDrainTimeout: TimeSpan.FromSeconds(1)))
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            foreach (var id in ids)
                await publisher.PublishAsync(new BatchShutdownEvent(id));

            await WaitForAsync(() => BlockingBatchHandler.StartedCount >= 1, TimeSpan.FromSeconds(10));

            var stopTask = host.StopAsync();
            var winner = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(30)));
            (winner == stopTask).Should().BeTrue("shutdown must not wait for the blocking batch handler");
        }

        BlockingBatchHandler.CompletedCount.Should().Be(0, "the batch handler was abandoned when the drain timeout expired");

        // The abandoned messages must come back as plain redeliveries: not retried, not dead-lettered.
        using var observer = await BuildHostAsync(name, s => s.AddBatchRabbitHandler<RecordingBatchHandler>(ConsumerKey));
        await WaitForAsync(() => { lock (RecordingBatchHandler.Received) return RecordingBatchHandler.Received.Count >= ids.Count; }, TimeSpan.FromSeconds(15));
        await observer.StopAsync();

        lock (RecordingBatchHandler.Received)
        {
            RecordingBatchHandler.Received.Select(r => r.Id).Should().BeEquivalentTo(ids);
            RecordingBatchHandler.Received.Should().OnlyContain(r => r.RetryCount == 0,
                "a shutdown requeue must not consume a retry attempt");
        }
    }

    [Fact]
    public async Task MixedEventTypesInOneFlush_EachTypeReachesItsOwnHandler()
    {
        lock (MixAHandler.Received) MixAHandler.Received.Clear();
        lock (MixBHandler.Received) MixBHandler.Received.Clear();

        var aIds = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid()).ToList();
        var bIds = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid()).ToList();

        using var host = await BuildHostAsync("batch-mixed-types", s =>
        {
            s.AddBatchRabbitHandler<MixAHandler>(ConsumerKey);
            s.AddBatchRabbitHandler<MixBHandler>(ConsumerKey);
        }, batchSize: 10, batchTimeoutMs: 1500);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        for (var i = 0; i < 3; i++)
        {
            await publisher.PublishAsync(new MixA(aIds[i]));
            await publisher.PublishAsync(new MixB(bIds[i]));
        }

        await WaitForAsync(() =>
        {
            lock (MixAHandler.Received) lock (MixBHandler.Received)
                return MixAHandler.Received.Count >= 3 && MixBHandler.Received.Count >= 3;
        }, TimeSpan.FromSeconds(15));

        lock (MixAHandler.Received) MixAHandler.Received.Should().BeEquivalentTo(aIds);
        lock (MixBHandler.Received) MixBHandler.Received.Should().BeEquivalentTo(bIds);
    }

    [Fact]
    public async Task RetriedBatch_IsRepublishedWithItsOriginalBodies()
    {
        FailOnceBatchHandler.Invocations = 0;
        lock (FailOnceBatchHandler.Received) FailOnceBatchHandler.Received.Clear();

        // Enough messages, with distinct payloads, that reused delivery buffers would show up as
        // corrupted or mismatched bodies after the retry.
        var events = Enumerable.Range(0, 20)
            .Select(i => new BatchRetryEvent(Guid.NewGuid(), $"payload-{i}-{new string((char)('a' + i % 26), 32 + i)}"))
            .ToList();

        using var host = await BuildHostAsync("batch-retry-bodies", s => s.AddBatchRabbitHandler<FailOnceBatchHandler>(ConsumerKey), batchSize: 20, batchTimeoutMs: 1000);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        foreach (var e in events)
            await publisher.PublishAsync(e);

        await WaitForAsync(() => { lock (FailOnceBatchHandler.Received) return FailOnceBatchHandler.Received.Count >= events.Count; }, TimeSpan.FromSeconds(30));

        lock (FailOnceBatchHandler.Received)
            FailOnceBatchHandler.Received.Should().BeEquivalentTo(events, "retried messages must carry exactly their original bodies");
    }

    [Fact]
    public async Task OlderEventVersion_IsUpgradedBeforeBatchDispatch()
    {
        lock (VersionedBatchHandler.Received) VersionedBatchHandler.Received.Clear();

        var id = Guid.NewGuid();

        using var host = await BuildHostAsync("batch-versioning", s =>
        {
            s.AddBatchRabbitHandler<VersionedBatchHandler>(ConsumerKey);
            s.AddEventUpgrader<VersionedV1ToV2Upgrader>();
        }, batchTimeoutMs: 1000);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        await publisher.PublishAsync(new VersionedV1(id, "Ada Lovelace"));

        await WaitForAsync(() => { lock (VersionedBatchHandler.Received) return VersionedBatchHandler.Received.Count >= 1; }, TimeSpan.FromSeconds(15));

        lock (VersionedBatchHandler.Received)
            VersionedBatchHandler.Received.Should().ContainSingle()
                .Which.Should().Be(new VersionedV2(id, "Ada", "Lovelace"));
    }

    // ───────────────────────── helpers ─────────────────────────

    private async Task<IHost> BuildHostAsync(string name,
                                             Action<IServiceCollection> registerHandlers,
                                             TimeSpan? shutdownDrainTimeout = null,
                                             int batchSize = 10,
                                             int batchTimeoutMs = 500)
    {
        var hostBuilder = Host.CreateDefaultBuilder();

        hostBuilder.ConfigureServices(services =>
        {
            services.AddRabbitMQ(settings =>
            {
                settings.Connections["main"] = new()
                {
                    HostName = fixture.HostName,
                    Port = fixture.Port,
                    UserName = fixture.UserName,
                    Password = fixture.Password,
                    RequestedHeartbeatSeconds = 10,
                    ConnectionTimeoutSeconds = 10,
                    MaxBackoffSeconds = 5
                };

                settings.Producers.Add(new()
                {
                    ServiceKey = $"{name}-producer",
                    ConnectionName = "main",
                    ExchangeName = name,
                    ExchangeType = "direct",
                    RoutingKey = name,
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new()
                {
                    ServiceKey = ConsumerKey,
                    ConnectionName = "main",
                    ExchangeName = name,
                    ExchangeType = "direct",
                    QueueName = $"{name}-queue",
                    RoutingKey = name,
                    PrefetchCount = 20,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = 3,
                    RetryDelays = [TimeSpan.Zero, TimeSpan.FromSeconds(1)],
                    EnableBatchConsumer = true,
                    BatchSize = batchSize,
                    BatchTimeoutMs = batchTimeoutMs,
                    ShutdownDrainTimeout = shutdownDrainTimeout ?? TimeSpan.FromSeconds(10)
                });
            });

            registerHandlers(services);
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
