using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RedRabbit.Abstractions;
using RedRabbit.Configuration;
using RedRabbit.Diagnostics;
using RedRabbit.Extensions;
namespace RedRabbit.Tests.Integration;

[Collection(RabbitMqCollection.Name)]
public class RetryAndDeadLetterTests(RabbitMqFixture fixture)
{
    [EventVersion("flaky-event", 1)]
    public record FlakyEvent(Guid Id, bool ShouldSucceed) : IIntegrationEvent;

    [EventVersion("retry-isolation-event", 1)]
    public record RetryIsolationEvent(Guid Id) : IIntegrationEvent;

    [EventVersion("concurrency-event", 1)]
    public record ConcurrencyEvent(Guid Id) : IIntegrationEvent;

    private class FlakyHandler : IRabbitHandler<FlakyEvent>
    {
        public static int AttemptCount;
        public static readonly List<FlakyEvent> SucceededEvents = [];
        public static bool NextAttemptSucceeds;

        public Task HandleAsync(FlakyEvent @event, MessageContext context, CancellationToken cancellationToken)
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

    /// <summary>
    /// Handler bound to consumer "retry-isolation-a" on the shared fanout exchange.
    /// ALWAYS throws — used to drive retries on consumer A so we can verify whether
    /// those retries leak to consumer B (regression guard for retry isolation).
    /// </summary>
    private class FailingHandlerA : IRabbitHandler<RetryIsolationEvent>
    {
        public static int AttemptCount;

        public Task HandleAsync(RetryIsolationEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref AttemptCount);
            throw new InvalidOperationException("Always fails");
        }
    }

    /// <summary>
    /// Handler bound to consumer "retry-isolation-b" on the shared fanout exchange.
    /// ALWAYS succeeds and records every message it receives. If retry isolation is
    /// broken (retry queue dead-lettering to the business exchange instead of the
    /// default exchange), this handler will receive A's retries and <see cref="ReceivedCount"/>
    /// will exceed 1.
    /// </summary>
    private class SucceedingHandlerB : IRabbitHandler<RetryIsolationEvent>
    {
        public static int ReceivedCount;
        public static readonly List<Guid> ReceivedIds = [];

        public Task HandleAsync(RetryIsolationEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref ReceivedCount);
            ReceivedIds.Add(@event.Id);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Handler that tracks how many handler invocations are running concurrently.
    /// Used to verify that <see cref="RabbitConsumerOptions.MaxConcurrentHandlers"/>
    /// actually enables concurrent handler execution: the RabbitMQ.Client dispatcher
    /// must deliver more than one message at a time (which requires
    /// <c>consumerDispatchConcurrency</c> to be set on the channel, not the default of 1).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="CurrentConcurrent"/> counts in-flight invocations (incremented on entry,
    /// decremented on exit). <see cref="MaxConcurrent"/> records the maximum value of
    /// <see cref="CurrentConcurrent"/> observed across all invocations (lock-free CAS loop).
    /// <see cref="ProcessedCount"/> counts handlers that completed successfully.
    /// </para>
    /// <para>
    /// Each invocation delays 500ms to force overlap with subsequent deliveries. If the
    /// dispatcher is serial (consumerDispatchConcurrency = 1), MaxConcurrent stays at 1
    /// because only one message is ever in flight. Once the dispatcher is concurrent,
    /// MaxConcurrent rises to at least 2.
    /// </para>
    /// </remarks>
    private class ConcurrencyTrackingHandler : IRabbitHandler<ConcurrencyEvent>
    {
        public static int CurrentConcurrent;
        public static int MaxConcurrent;
        public static int ProcessedCount;

        public async Task HandleAsync(ConcurrencyEvent @event, MessageContext context, CancellationToken cancellationToken)
        {
            var current = Interlocked.Increment(ref CurrentConcurrent);

            // Track the maximum concurrency observed via a lock-free CAS loop.
            int observedMax;
            do
            {
                observedMax = Volatile.Read(ref MaxConcurrent);
                if (current <= observedMax) break;
            } while (Interlocked.CompareExchange(ref MaxConcurrent, current, observedMax) != observedMax);

            try
            {
                // Delay to force overlap between handler invocations.
                await Task.Delay(500, cancellationToken);
                Interlocked.Increment(ref ProcessedCount);
            }
            finally
            {
                Interlocked.Decrement(ref CurrentConcurrent);
            }
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

    [Fact]
    public async Task HandlerAlwaysFails_MessageReachesDlq_AfterExactly_MaxRetries_3()
    {
        // Arrange
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        const int maxRetries = 3;
        var retryDelays = new[] { TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1) };
        const string exchangeName = "retry-test-3";
        const string queueName = "retry-test-3-queue";

        using var host = await BuildHostAsync(maxRetries, retryDelays, exchangeName, queueName);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);

        // Act
        await publisher.PublishAsync(@event);

        // Wait for the handler to be invoked at least MaxRetries times
        await WaitForAsync(() => FlakyHandler.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Give a small additional window for the DLQ routing to complete and to detect zombie retries
        await Task.Delay(2000);

        // Assert: handler invoked exactly MaxRetries times (not more — not a zombie)
        FlakyHandler.AttemptCount.Should().Be(maxRetries,
            "the message should be dead-lettered after exactly MaxRetries attempts, not retried indefinitely");

        // Assert: the message reached the DLQ
        var dlqMessage = await GetMessageFromDlqAsync(queueName, TimeSpan.FromSeconds(10));
        dlqMessage.Should().NotBeNull("the message should have been dead-lettered to the DLQ after retries were exhausted");
    }

    [Fact]
    public async Task HandlerAlwaysFails_MessageReachesDlq_AfterExactly_MaxRetries_5()
    {
        // Arrange
        FlakyHandler.AttemptCount = 0;
        FlakyHandler.SucceededEvents.Clear();
        FlakyHandler.NextAttemptSucceeds = false;

        const int maxRetries = 5;
        var retryDelays = new[]
        {
            TimeSpan.Zero,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1)
        };
        const string exchangeName = "retry-test-5";
        const string queueName = "retry-test-5-queue";

        using var host = await BuildHostAsync(maxRetries, retryDelays, exchangeName, queueName);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var @event = new FlakyEvent(Guid.NewGuid(), ShouldSucceed: false);

        // Act
        await publisher.PublishAsync(@event);

        // Wait for the handler to be invoked at least MaxRetries times
        await WaitForAsync(() => FlakyHandler.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Give a small additional window for the DLQ routing to complete and to detect zombie retries
        await Task.Delay(2000);

        // Assert: handler invoked exactly MaxRetries times (not more — not a zombie)
        FlakyHandler.AttemptCount.Should().Be(maxRetries,
            "the message should be dead-lettered after exactly MaxRetries attempts, not retried indefinitely");

        // Assert: the message reached the DLQ
        var dlqMessage = await GetMessageFromDlqAsync(queueName, TimeSpan.FromSeconds(10));
        dlqMessage.Should().NotBeNull("the message should have been dead-lettered to the DLQ after retries were exhausted");
    }

    /// <summary>
    /// Regression test for retry isolation: when consumer A fails and retries on a fanout exchange,
    /// consumer B (bound to the same exchange, different queue) must receive the ORIGINAL message
    /// exactly ONCE — it must NOT receive A's retries.
    ///
    /// <para>
    /// Before the retry-isolation fix, the retry queue's dead-letter destination was configured to use
    /// the business exchange with the original routing key. On fanout (and topic) exchanges, this
    /// caused A's retries to be broadcast to ALL queues bound to the exchange — including B's queue.
    /// With the fix, the retry queue dead-letters to the default exchange ("") with the main queue
    /// name as routing key, so retries are delivered ONLY to A's main queue.
    /// </para>
    /// <para>
    /// If this test fails with <c>SucceedingHandlerB.ReceivedCount == 3</c> (or any value greater
    /// than 1), the retry isolation fix has been reverted or broken.
    /// </para>
    /// </summary>
    [Fact]
    public async Task Retry_DoesNotBroadcast_ToOtherConsumers_OnSameExchange()
    {
        // Arrange — reset static state on both handlers
        FailingHandlerA.AttemptCount = 0;
        SucceedingHandlerB.ReceivedCount = 0;
        SucceedingHandlerB.ReceivedIds.Clear();

        const string exchangeName = "retry-isolation-test";
        const string queueA = "retry-isolation-a-queue";
        const string queueB = "retry-isolation-b-queue";
        // fanout ignores the routing key for delivery, but RabbitFlow validation requires
        // a non-empty routing key — use a placeholder (same value for both queues is fine,
        // fanout does not filter on it).
        const string routingKey = "isolation";
        const int maxRetries = 3;
        var retryDelays = new[] { TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1) };

        // Build host with TWO consumers on the SAME fanout exchange, each on its own queue.
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
                    ServiceKey = "isolation-producer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "fanout", // KEY: fanout broadcasts to all bound queues
                    RoutingKey = routingKey,
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                // Consumer A — always fails, with retry
                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "retry-isolation-a",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "fanout",
                    QueueName = queueA,
                    RoutingKey = routingKey,
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = maxRetries,
                    RetryDelays = retryDelays
                });

                // Consumer B — always succeeds, with retry (should never need it)
                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "retry-isolation-b",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "fanout",
                    QueueName = queueB,
                    RoutingKey = routingKey,
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = maxRetries,
                    RetryDelays = retryDelays
                });
            });

            services.AddRabbitHandler<FailingHandlerA>("retry-isolation-a");
            services.AddRabbitHandler<SucceedingHandlerB>("retry-isolation-b");
        });

        using var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);

        // Act — publish a single message to the fanout exchange.
        // The broker delivers one copy to queueA (A will fail and retry) and one copy to
        // queueB (B will succeed immediately).
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var eventId = Guid.NewGuid();
        await publisher.PublishAsync(new RetryIsolationEvent(eventId));

        // Wait for handler A to be invoked MaxRetries times (then DLQ).
        await WaitForAsync(() => FailingHandlerA.AttemptCount >= maxRetries, TimeSpan.FromSeconds(30));

        // Grace window for any spurious deliveries to B to arrive (they should not — if they
        // do, the retry queue is dead-lettering to the business exchange, which is the bug).
        await Task.Delay(3000);

        // Assert — A saw all MaxRetries attempts (retries work for A).
        FailingHandlerA.AttemptCount.Should().Be(maxRetries,
            "handler A should be invoked exactly MaxRetries times before the message is dead-lettered");

        // Assert — B received the message exactly ONCE: the original delivery, not A's retries.
        // If this is 3 (or any value > 1), retries are broadcasting to other consumers on the
        // same exchange (the bug the retry-isolation fix prevents).
        SucceedingHandlerB.ReceivedCount.Should().Be(1,
            "handler B should receive the message ONCE (the original delivery), not the retries. " +
            "If it received more, retries are broadcasting to other consumers on the same exchange (bug).");

        SucceedingHandlerB.ReceivedIds.Should().ContainSingle(id => id == eventId,
            "handler B should have received exactly the published event id, exactly once.");

        await host.StopAsync();
    }

    /// <summary>
    /// Regression test for the MaxConcurrentHandlers fix: with
    /// <see cref="RabbitConsumerOptions.MaxConcurrentHandlers"/> set to 5 and
    /// <see cref="RabbitConsumerOptions.PrefetchCount"/> set to 5, the dispatcher
    /// must deliver more than one message at a time so that handler invocations
    /// overlap.
    /// <para>
    /// Before the fix, RabbitMQ.Client 7's default <c>consumerDispatchConcurrency = 1</c>
    /// meant the dispatcher only delivered one message at a time, so
    /// <c>MaxConcurrentHandlers</c> had no observable effect. This test FAILS in
    /// that scenario (<see cref="ConcurrencyTrackingHandler.MaxConcurrent"/> == 1)
    /// and PASSES once the channel is created with <c>consumerDispatchConcurrency</c>
    /// set (<see cref="ConcurrencyTrackingHandler.MaxConcurrent"/> &gt;= 2).
    /// </para>
    /// </summary>
    [Fact]
    public async Task MaxConcurrentHandlers_AllowsConcurrentHandlerExecution()
    {
        // Arrange — reset static state on the concurrency handler.
        ConcurrencyTrackingHandler.CurrentConcurrent = 0;
        ConcurrencyTrackingHandler.MaxConcurrent = 0;
        ConcurrencyTrackingHandler.ProcessedCount = 0;

        const string exchangeName = "concurrency-test";
        const string queueName = "concurrency-test-queue";

        using var host = await BuildHostForConcurrencyAsync(exchangeName, queueName);

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act — publish 5 messages.
        for (var i = 0; i < 5; i++)
            await publisher.PublishAsync(new ConcurrencyEvent(Guid.NewGuid()));

        // Wait for all 5 to be processed.
        await WaitForAsync(() => ConcurrencyTrackingHandler.ProcessedCount >= 5, TimeSpan.FromSeconds(15));

        // Give a small grace window for the MaxConcurrent counter to settle
        // (the max is recorded on entry, before the delay, so by this point all
        // 5 invocations have entered and the maximum is final).
        await Task.Delay(500);

        // Assert — all 5 were processed AND at least 2 ran concurrently.
        ConcurrencyTrackingHandler.ProcessedCount.Should().Be(5, "all 5 messages should be processed");
        ConcurrencyTrackingHandler.MaxConcurrent.Should().BeGreaterOrEqualTo(2,
            "with MaxConcurrentHandlers=5 and PrefetchCount=5, at least 2 handlers should run concurrently. " +
            "If MaxConcurrent is 1, the dispatcher is serial (bug: consumerDispatchConcurrency not set).");
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

    /// <summary>
    /// Builds a host with parameterized retry settings and unique exchange/queue names
    /// to avoid cross-test message interference.
    /// </summary>
    private async Task<IHost> BuildHostAsync(int maxRetries, TimeSpan[] retryDelays, string exchangeName, string queueName)
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
                    ServiceKey = "flaky-producer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
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
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    QueueName = queueName,
                    RoutingKey = "flaky-event",
                    PrefetchCount = 1,
                    EnableDeadLetter = true,
                    EnableRetry = true,
                    MaxRetries = maxRetries,
                    RetryDelays = retryDelays
                });
            });

            services.AddRabbitHandler<FlakyHandler>("flaky-consumer");
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);
        return host;
    }

    /// <summary>
    /// Builds a host whose consumer enables concurrent handler execution:
    /// <see cref="RabbitConsumerOptions.MaxConcurrentHandlers"/> = 5 and
    /// <see cref="RabbitConsumerOptions.PrefetchCount"/> = 5. The dispatcher must
    /// deliver up to 5 messages at a time so the handler invocations overlap.
    /// Retry and DLQ are disabled to keep the test focused (the handler always
    /// succeeds, so there is no DLQ/retry path to exercise).
    /// </summary>
    private async Task<IHost> BuildHostForConcurrencyAsync(string exchangeName, string queueName)
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
                    ServiceKey = "concurrency-producer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    RoutingKey = "concurrency-event",
                    EnablePublisherConfirms = true,
                    PublishConfirmTimeoutMs = 5_000,
                    ChannelPoolSize = 2
                });

                settings.Consumers.Add(new RabbitConsumerOptions
                {
                    ServiceKey = "concurrency-consumer",
                    ConnectionName = "main",
                    ExchangeName = exchangeName,
                    ExchangeType = "direct",
                    QueueName = queueName,
                    RoutingKey = "concurrency-event",
                    PrefetchCount = 5,
                    MaxConcurrentHandlers = 5, // KEY: enable concurrent handlers
                    EnableDeadLetter = false,   // handler always succeeds — no DLQ needed
                    EnableRetry = false,
                    MaxRetries = 1
                });
            });

            services.AddRabbitHandler<ConcurrencyTrackingHandler>("concurrency-consumer");
        });

        var host = hostBuilder.Build();
        await host.StartAsync();
        await Task.Delay(2000);
        return host;
    }

    /// <summary>
    /// Polls the DLQ ({queueName}.dlq) for a single message within the given timeout.
    /// Returns null if no message arrives before the deadline.
    /// </summary>
    private async Task<BasicGetResult?> GetMessageFromDlqAsync(string queueName, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        await using var connection = await fixture.CreateRawConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        while (DateTime.UtcNow < deadline)
        {
            var result = await channel.BasicGetAsync(queueName + ".dlq", autoAck: true);
            if (result is not null)
                return result;
            await Task.Delay(200);
        }
        return null;
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