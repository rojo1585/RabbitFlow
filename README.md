<p align="center">
  <img src="https://raw.githubusercontent.com/dotnet/brand/main/logo/dotnet-logo.svg" alt="RabbitFlow Logo" width="100" height="100" />
</p>
<h1 align="center">RabbitFlow</h1>
<p align="center">
  <b>Enterprise-grade, high-throughput RabbitMQ client framework for .NET 8, 9, and 10</b>
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-architecture">Architecture</a> •
  <a href="#-installation">Installation</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-advanced-capabilities">Advanced Capabilities</a> •
  <a href="#-multi-broker-support">Multi-Broker</a> •
  <a href="#-resilience--dlq">Resilience &amp; DLQ</a> •
  <a href="#-configuration-reference">Configuration</a> •
  <a href="#-observability--metrics">Metrics</a>
</p>

<p align="center">
  <a href="https://dotnet.microsoft.com/"><img src="https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-512BD4?style=for-the-badge&logo=dotnet&logoColor=white" alt=".NET Support" /></a>
  <a href="https://www.nuget.org/packages/RabbitFlow"><img src="https://img.shields.io/nuget/v/RabbitFlow?style=for-the-badge&logo=nuget&color=004880" alt="NuGet Version" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License: MIT" /></a>
  <a href="https://opentelemetry.io/"><img src="https://img.shields.io/badge/OpenTelemetry-Enabled-008080?style=for-the-badge&logo=opentelemetry&logoColor=white" alt="OpenTelemetry Ready" /></a>
</p>

---

## ⚡ Features

| Feature | Description |
|---|---|
| 🌐 **Multi-Broker Support** | Connect to multiple RabbitMQ brokers, clusters, or vhosts simultaneously within a single app. Each producer and consumer binds to a named connection. |
| ⚡ **Channel Pooling** | High-throughput producer channel reuse using `SemaphoreSlim` backpressure — eliminates channel-per-publish overhead. |
| 📦 **Batch Consumer** | Buffer high-volume messages in-memory and execute bulk processing with size/timeout triggers. |
| 🔄 **Event Versioning** | Transparently upgrade legacy event schemas to latest contracts via `IEventUpgrader<TFrom, TTo>` chains. Cycles and downgrades are rejected at startup. |
| 🛡️ **Resilience & DLQ** | Client-side retry with exponential backoff, automatic Dead-Letter Queue (DLQ) topology setup, and poison-message protection. One NACK = one copy (no amplification). |
| 📊 **Native OpenTelemetry** | Out-of-the-box distributed tracing (`ActivitySource`) and custom metrics (`Meter`). |
| 🩺 **Health Checks** | Native ASP.NET Core health check integration for individual connections and aggregate cluster health. |
| ✅ **Fail-Fast Validation** | `IValidateOptions<T>` + `ValidateOnStart()` validates all configuration bounds at startup — no silent hangs in runtime. |

---

## 🏗️ Architecture

```text
Publisher App                      RabbitMQ Broker                     Consumer App
┌─────────────────┐             ┌─────────────────────┐             ┌─────────────────┐
│ Publisher       │             │ RabbitMQ Exchange   │             │ Single Consumer │
│ Service         │             └──────────┬──────────┘             │  OrderCreated   │
└────────┬────────┘                        │                        └────────▲────────┘
         │ Rent Channel                    │ Route                           │ Consume
         ▼                                 ▼                                 │
┌─────────────────┐             ┌─────────────────────┐             ┌────────┴────────┐
│ Channel Pool    │─Publish─────► Queue:              ├────────────►│ Buffer /        │
└─────────────────┘ Message     │ orders.created      │             │ Dispatch        │
                                └──────────┬──────────┘             └─────────────────┘
                                           │
                                           │ On failure: publish to retry queue
                                           ▼
                                ┌─────────────────────┐             ┌─────────────────┐
                                │ Retry Queue TTL     ├────────────►│ Batch Consumer  │
                                └──────────┬──────────┘             │  Notification   │
                                           │ TTL expires →          └─────────────────┘
                                           │ re-deliver
                                           ▼
                                ┌─────────────────────┐
                                │ Dead-Letter Queue   │ (Retries exhausted / NACK)
                                └─────────────────────┘
```

### Resilience Flow (Client-Side Retry, No Message Amplification):
1. **Handler fails** → Consumer publishes to the appropriate retry queue (via default exchange) and ACKs the original delivery.
2. **Retry queue TTL expires** → Broker dead-letters back to the main exchange → Message re-delivered to the main queue.
3. **Retries exhausted** → Consumer NACKs with `requeue=false` → Broker dead-letters via `x-dead-letter-exchange` (a direct DLX bound to the DLQ with routing key `"dead"`).
4. **Poison message** (deserialize/upgrade throws) → Same retry/DLQ flow via top-level `try/catch` in `OnMessageReceived`.

---

## 📦 Installation

Install via .NET CLI:
```bash
dotnet add package RabbitFlow
```

Or Package Manager Console:
```powershell
Install-Package RabbitFlow
```

---

## 🚀 Quick Start

### 1️⃣ Configuration (`appsettings.json`)
```json
{
  "RabbitMQ": {
    "Connections": {
      "main": {
        "Name": "main",
        "HostName": "localhost",
        "Port": 5672,
        "UserName": "guest",
        "Password": "guest",
        "VirtualHost": "/"
      }
    },
    "Producers": [
      {
        "ServiceKey": "orders",
        "ConnectionName": "main",
        "ExchangeName": "orders",
        "ExchangeType": "direct",
        "RoutingKey": "order-created",
        "ChannelPoolSize": 4
      }
    ],
    "Consumers": [
      {
        "ServiceKey": "orders-consumer",
        "ConnectionName": "main",
        "ExchangeName": "orders",
        "QueueName": "orders.created",
        "RoutingKey": "order-created",
        "PrefetchCount": 10,
        "MaxRetries": 3
      }
    ]
  }
}
```
> **Note:** The `Name` field in each connection must match its dictionary key (`"main"` in the example above). This is validated at startup.

### 2️⃣ Dependency Injection (`Program.cs`)
```csharp
using RabbitFlow.Abstractions;
using RabbitFlow.Diagnostics;

var builder = WebApplication.CreateBuilder(args);

// Register RabbitMQ Core Services (call ONCE — see Multi-Broker section for multiple connections)
builder.Services.AddRabbitMQ(builder.Configuration);

// Register Consumer Handlers
builder.Services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer");

// Configure OpenTelemetry (Optional)
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(RabbitMqActivitySource.SourceName))
    .WithMetrics(m => m.AddMeter(RabbitMqMetrics.MeterName));

var app = builder.Build();
app.Run();
```

### 3️⃣ Publishing Messages
```csharp
using RabbitFlow.Abstractions;

public record OrderCreatedEvent : IIntegrationEvent
{
    public Guid OrderId { get; init; }
    public string CustomerEmail { get; init; } = string.Empty;
}

public class OrderService(IEventPublisher publisher)
{
    public async Task CreateOrderAsync(Guid orderId, string email)
    {
        // With a single producer registered, use the parameterless overload:
        await publisher.PublishAsync(new OrderCreatedEvent
        {
            OrderId = orderId,
            CustomerEmail = email
        });
    }
}
```
> Multiple producers? Use `publisher.PublishAsync("producer-key", @event)` to target a specific one. Without a key, the call throws `AmbiguousProducerException` when 2+ producers are registered.

### 4️⃣ Consuming Messages
```csharp
using RabbitFlow.Abstractions;

public class OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) : IRabbitHandler<OrderCreatedEvent>
{
    // Must match a Consumer's ServiceKey in configuration
    public string ConsumerKey => "orders-consumer";

    public Task HandleAsync(OrderCreatedEvent @event, MessageContext context)
    {
        logger.LogInformation(
            "Processing Order {OrderId} | CorrelationId: {CorrelationId}",
            @event.OrderId, 
            context.CorrelationId);

        return Task.CompletedTask;
    }
}
```

---

## 🌐 Multi-Broker Support

RabbitFlow connects to multiple RabbitMQ brokers, clusters, or vhosts in a single application. Each connection is a named entry in the `Connections` dictionary, and each producer/consumer references its connection by name.

> ⚠️ **Important:** `AddRabbitMQ` must be called **exactly once** per `IServiceCollection`. A second call throws `InvalidOperationException` to prevent silent configuration loss. Multi-broker is achieved by adding multiple entries to `Connections`, `Producers`, and `Consumers` in a single `AddRabbitMQ` call.

### Example: Two brokers (orders on broker A, notifications on broker B)
```csharp
builder.Services.AddRabbitMQ(settings =>
{
    // ── Broker A Connection ──
    settings.Connections["orders-broker"] = new RabbitConnectionOptions
    {
        Name = "orders-broker",
        HostName = "broker-a.example.com",
        UserName = "guest",
        Password = "guest"
    };

    // ── Broker B Connection ──
    settings.Connections["notifications-broker"] = new RabbitConnectionOptions
    {
        Name = "notifications-broker",
        HostName = "broker-b.example.com",
        UserName = "guest",
        Password = "guest"
    };

    // ── Producer on broker A ──
    settings.Producers.Add(new RabbitProducerOptions
    {
        ServiceKey = "orders-producer",
        ConnectionName = "orders-broker",      // ← references the connection above
        ExchangeName = "orders",
        RoutingKey = "order-created",
        ChannelPoolSize = 4
    });

    // ── Producer on broker B ──
    settings.Producers.Add(new RabbitProducerOptions
    {
        ServiceKey = "notifications-producer",
        ConnectionName = "notifications-broker",
        ExchangeName = "notifications",
        RoutingKey = "notification-pending",
        ChannelPoolSize = 4
    });

    // ── Consumer on broker A ──
    settings.Consumers.Add(new RabbitConsumerOptions
    {
        ServiceKey = "orders-consumer",
        ConnectionName = "orders-broker",
        ExchangeName = "orders",
        QueueName = "orders.created",
        RoutingKey = "order-created",
        PrefetchCount = 10,
        MaxRetries = 3
    });

    // ── Consumer on broker B ──
    settings.Consumers.Add(new RabbitConsumerOptions
    {
        ServiceKey = "notifications-consumer",
        ConnectionName = "notifications-broker",
        ExchangeName = "notifications",
        QueueName = "notifications.pending",
        RoutingKey = "notification-pending",
        PrefetchCount = 50,
        MaxRetries = 3
    });
});

// ── Register handlers for each consumer ──
builder.Services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer");
builder.Services.AddRabbitHandler<NotificationHandler>("notifications-consumer");
```

### Publishing to a specific broker
When multiple producers are registered, use the keyed overload:
```csharp
public class OrderService(IEventPublisher publisher)
{
    public Task CreateOrderAsync(Guid orderId, string email)
        => publisher.PublishAsync("orders-producer", new OrderCreatedEvent { OrderId = orderId, CustomerEmail = email });
}
```

---

## 💎 Advanced Capabilities

### 📥 Batch Consumer
Process high-throughput workloads (e.g., bulk database inserts) by accumulating messages in-memory.

> 💡 **Tip:** Use batch processing when throughput exceeds 5,000 msg/sec to drastically reduce I/O bottlenecks.

`appsettings.json`:
```json
{
  "Consumers": [{
    "ServiceKey": "notifications-batch",
    "ConnectionName": "main",
    "ExchangeName": "notifications",
    "QueueName": "notifications.pending",
    "RoutingKey": "#",
    "EnableBatchConsumer": true,
    "BatchSize": 50,
    "BatchTimeoutMs": 3000,
    "PrefetchCount": 100
  }]
}
```

Implementation:
```csharp
builder.Services.AddBatchRabbitHandler<NotificationBatchHandler>("notifications-batch");

public class NotificationBatchHandler : IBatchRabbitHandler<NotificationEvent>
{
    public string ConsumerKey => "notifications-batch";

    public async Task HandleBatchAsync(
        IReadOnlyList<NotificationEvent> events,
        IReadOnlyList<MessageContext> contexts)
    {
        // Bulk Operation (e.g., EF Core AddRangeAsync / Dapper ExecuteAsync)
        await SaveNotificationsToDbAsync(events);
    }
}
```
> **Note:** Batch handler failures NACK all messages in the batch together. Each message is then individually re-published to the retry queue (or NACKed to the DLQ if retries are exhausted).

### 🏊 Channel Pooling
By default, each producer holds a managed pool of AMQP channels. Publishes rent a channel, execute the operation, and return it back to the pool.

> ⚡ **Important:** Creating and destroying channels per publish is the #1 cause of throughput limits in AMQP applications. Channel pooling eliminates this latency entirely.

| Pool Size | Publisher Confirms | Estimated Throughput |
|---|---|---|
| **4** | Enabled | 40,000 – 80,000 msg/s |
| **8** | Enabled | 80,000 – 150,000 msg/s |
| **4** | Disabled | 100,000 – 200,000 msg/s |
| **0** | — | Channel-per-publish (Backward compatibility) |

### 🔀 Event Versioning
Upgrade historical message contracts without disrupting running production consumers.

```csharp
[EventVersion("OrderCreated", 1)]
public record OrderCreatedV1(Guid OrderId, string Email);

[EventVersion("OrderCreated", 2)]
public record OrderCreatedV2(Guid OrderId, string Email, string Phone);

// Upgrader definition
public class OrderV1ToV2Upgrader : IEventUpgrader<OrderCreatedV1, OrderCreatedV2>
{
    public string EventName => "OrderCreated";
    public int FromVersion => 1;
    public int ToVersion => 2;

    public OrderCreatedV2 Upgrade(OrderCreatedV1 source)
        => new(source.OrderId, source.Email, Phone: "N/A");
}

// Registration
builder.Services.AddEventUpgrader<OrderV1ToV2Upgrader>();
```

**Safety guarantees (validated at startup):**
- ✅ **No cycles:** `ToVersion` must be strictly greater than `FromVersion`. Self-loops (V1→V1), downgrades (V3→V2), and cycles (V1→V2→V1) are rejected with `InvalidOperationException`.
- ✅ **No gaps:** Each upgrader's `ToVersion` must match the next upgrader's `FromVersion`.
- ✅ **Newer-than-highest rejected:** If a message arrives with a version higher than the highest registered (e.g., message is V5 but consumer only knows up to V3), `EventVersionNewerThanRegisteredException` is thrown and the message is routed to retry/DLQ via the poison-message handler — no silent field loss.

---

## 🛡️ Resilience & DLQ

RabbitFlow provides client-side retry with automatic Dead-Letter Queue topology. This ensures exactly-once routing to the DLQ (no message amplification).

### How It Works
```text
Main Queue → handler fails
   ├─ ShouldRetry=true  → publish to retry queue + ACK original
   │                        │
   │                        └─► retry queue (TTL) → expires → broker dead-letters → main exchange → main queue
   │                        │
   │                        └─► (x-death header updated; deliveryCount increments)
   │
   └─ ShouldRetry=false → NACK(requeue=false)
                            │
                            └─► broker dead-letters via x-dead-letter-exchange
                            │
                            └─► DLX (direct, routing key "dead") → DLQ (exactly one copy)
                            │
                            └─► IDeadLetterHandler invoked (notification only, no re-publish)
```

### Topology Declared Per Consumer
| Resource | Type | Purpose |
|---|---|---|
| `{QueueName}.dlx` | direct exchange | Receives dead-lettered messages from the main queue |
| `{QueueName}.dlq` | durable queue | Final resting place; bound to DLX with routing key `"dead"` |
| `{QueueName}.retry.0s` | durable queue, TTL=1ms | Immediate retry (delay=0) |
| `{QueueName}.retry.5s` | durable queue, TTL=5000ms | Retry after 5s |
| `{QueueName}.retry.30s` | durable queue, TTL=30000ms | Retry after 30s |

> **Note:** Retry queues are NOT bound to the DLX. The client publishes directly to them via the default exchange. This prevents the exponential amplification bug that occurs with a fanout DLX bound to both DLQ and retry queues.

### Poison Message Protection
If deserialization, event upgrade, or handler resolution throws (corrupt JSON, incompatible schema, upgrader exception), the top-level `try/catch` in `OnMessageReceived` catches it and routes the message through the same retry/DLQ flow. A single corrupt message cannot hang the consumer indefinitely.

### Configuration
```json
{
  "Consumers": [{
    "ServiceKey": "orders-consumer",
    "EnableDeadLetter": true,
    "EnableRetry": true,
    "MaxRetries": 3,
    "RetryDelays": ["00:00:00", "00:00:05", "00:00:30"]
  }]
}
```
- **`MaxRetries`**: Maximum number of delivery attempts (default: `3`). The first delivery counts as attempt 1.
- **`RetryDelays`**: One entry per retry attempt after the first. Must have at least `MaxRetries - 1` entries. Default: `["00:00:00", "00:00:05", "00:00:30"]`.

### `IDeadLetterHandler` (Optional Notification)
Register an `IDeadLetterHandler` to receive a callback when a message is dead-lettered (e.g., for alerting or audit). This is a notification only — the broker has already written the message to the DLQ.

```csharp
public class AlertingDeadLetterHandler(ILogger<AlertingDeadLetterHandler> logger) : IDeadLetterHandler
{
    public string ConsumerKey => "orders-consumer";

    public Task HandleAsync(DeadLetterMessage message)
    {
        logger.LogError(
            "Message dead-lettered: {Exchange}/{RoutingKey} | Exception: {ExceptionType}: {ExceptionMessage}",
            message.OriginalExchange, message.OriginalRoutingKey,
            message.ExceptionType, message.ExceptionMessage);
        return Task.CompletedTask;
    }
}

// Registration (no extension method needed — register as IDeadLetterHandler)
builder.Services.AddSingleton<IDeadLetterHandler, AlertingDeadLetterHandler>();
```

### ⚠️ Exceptions
RabbitFlow throws domain-specific exceptions (all inherit from `RabbitMqException`) so callers don't take a hard dependency on the `RabbitMQ.Client` driver:

| Exception | When Thrown |
|---|---|
| `PublisherNackException` | Broker nacked or returned a published message (publisher confirms). Exposes `IsReturn`, `PublishSequenceNumber`. |
| `PublisherConfirmTimeoutException` | Broker didn't confirm within `PublishConfirmTimeoutMs`. |
| `ProducerNotFoundException` | `PublishAsync("key", ...)` referenced an unknown producer key. |
| `AmbiguousProducerException` | `PublishAsync(event)` (no key) called with 0 or 2+ producers registered. |
| `HandlerNotFoundException` | No handler registered for a consumed message's consumer key + event type. |
| `ConnectionNotFoundException` | A producer/consumer referenced a connection name that doesn't exist. |
| `EventVersionNewerThanRegisteredException` | Message arrived with a version higher than the highest registered upgrader. Exposes `ReceivedVersion`, `HighestRegisteredVersion`. |
| `RabbitMqConfigurationException` | Configuration validation failed at startup. |

---

## 🔧 Configuration Reference

### `RabbitConnectionOptions`
| Property | Type | Default | Validation |
|---|---|---|---|
| `Name` | `string` (required) | — | Non-empty, must match dictionary key |
| `HostName` | `string` (required) | — | Non-empty |
| `Port` | `int` | `5672` | `1–65535` |
| `UserName` | `string` (required) | — | Non-empty |
| `Password` | `string` (required) | — | Non-empty |
| `VirtualHost` | `string` | `"/"` | — |
| `RequestedHeartbeatSeconds` | `int` | `60` | `>= 1` |
| `ConnectionTimeoutSeconds` | `int` | `30` | `>= 1` |
| `InitialConnectRetryCount` | `int` | `5` | `>= 0` |
| `MaxBackoffSeconds` | `int` | `60` | `>= 1` |
| `Tls` | `TlsOptions?` | `null` | — |

### `RabbitProducerOptions`
| Property | Type | Default | Validation |
|---|---|---|---|
| `ServiceKey` | `string` (required) | — | Non-empty, unique |
| `ConnectionName` | `string` (required) | — | Must exist in `Connections` |
| `ExchangeName` | `string` (required) | — | Non-empty |
| `ExchangeType` | `string` | `"direct"` | Non-empty |
| `RoutingKey` | `string` (required) | — | Non-empty |
| `Mandatory` | `bool` | `true` | — |
| `EnablePublisherConfirms` | `bool` | `true` | — |
| `PublishConfirmTimeoutMs` | `int` | `5000` | `> 0` |
| `AutoDeclareTopology` | `bool` | `true` | — |
| `ChannelPoolSize` | `int` | `4` | `>= 0` (`0` = channel-per-publish) |

### `RabbitConsumerOptions`
| Property | Type | Default | Validation |
|---|---|---|---|
| `ServiceKey` | `string` (required) | — | Non-empty, unique |
| `ConnectionName` | `string` (required) | — | Must exist in `Connections` |
| `ExchangeName` | `string` (required) | — | Non-empty |
| `ExchangeType` | `string` | `"direct"` | Non-empty |
| `QueueName` | `string` (required) | — | Non-empty |
| `RoutingKey` | `string` (required) | — | Non-empty |
| `PrefetchCount` | `ushort` | `10` | `>= 1` (`0` deadlocks batch consumer) |
| `MaxConcurrentHandlers` | `int` | `0` | `>= 0` (`0` = unlimited) |
| `EnableDeadLetter` | `bool` | `true` | — |
| `EnableRetry` | `bool` | `true` | — |
| `MaxRetries` | `int` | `3` | `>= 1` |
| `RetryDelays` | `TimeSpan[]?` | `[0s,5s,30s]` | `>= MaxRetries-1` entries, no negatives |
| `EnableBatchConsumer` | `bool` | `false` | — |
| `BatchSize` | `int` | `10` | `>= 1` (when `EnableBatchConsumer=true`) |
| `BatchTimeoutMs` | `int` | `5000` | `> 0` (when `EnableBatchConsumer=true`) |
| `AutoDeclareTopology` | `bool` | `true` | — |

> Validation runs at startup via `IValidateOptions<RabbitMqSettings>` + `ValidateOnStart()`. Invalid configuration throws `OptionsValidationException` before any hosted service starts.

---

## 📊 Observability & Metrics

### OpenTelemetry
RabbitFlow exposes an `ActivitySource` (tracing) and a `Meter` (metrics). Register them in your OTel pipeline:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(RabbitMqActivitySource.SourceName))
    .WithMetrics(m => m.AddMeter(RabbitMqMetrics.MeterName));
```

### Metrics Exposed
| Metric | Type | Tags |
|---|---|---|
| `rabbitflow.published` | Counter | `producer_key`, `event_type`, `exchange` |
| `rabbitflow.publish_errors` | Counter | `producer_key`, `event_type`, `error_type` |
| `rabbitflow.publish_duration_ms` | Histogram | `producer_key`, `event_type`, `exchange` |
| `rabbitflow.consumed` | Counter | `consumer_key`, `event_type`, `queue` |
| `rabbitflow.consume_errors` | Counter | `consumer_key`, `event_type`, `error_type`, `queue` |
| `rabbitflow.retried` | Counter | `consumer_key`, `event_type`, `queue`, `attempt` |
| `rabbitflow.dead_lettered` | Counter | `consumer_key`, `event_type`, `queue`, `dlq_name` |
| `rabbitflow.processing_duration_ms` | Histogram | `consumer_key`, `event_type`, `queue` |
| `rabbitflow.batches_dispatched` | Counter | `consumer_key`, `event_type`, `queue` |
| `rabbitflow.batch_size` | Histogram | `consumer_key`, `event_type`, `queue` |

### Health Checks
```csharp
builder.Services.AddHealthChecks()
    .AddRabbitMqHealthCheck(connectionName: "main");   // Single connection
    // or
    .AddRabbitMqAllHealthCheck();                       // Aggregate all connections
```

---

## 🔌 Connection Resilience

RabbitFlow owns the reconnection lifecycle. The driver's built-in auto-recovery is disabled (`AutomaticRecoveryEnabled = false`) to avoid races with the manual reconnect loop:

1. **Connection drops** → Driver fires `ConnectionShutdownAsync`.
2. `OnConnectionShutdown` signals a `TaskCompletionSource` and nulls the connection.
3. The background loop wakes up, applies exponential backoff (1s → 2s → 4s → ... → `MaxBackoffSeconds` cap), and creates a new `IConnection`.
4. Publishers and consumers re-declare topology idempotently on the new connection.

This ensures exactly one reconnection path and a single source of truth for the live `IConnection` — no duplicate TCP connections during broker restarts.

---

## 📄 License

[MIT](LICENSE) — see LICENSE for details.