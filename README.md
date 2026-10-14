<h1 align="center">RedRabbit</h1>
<p align="center">
  <b>Typed RabbitMQ publishing and consuming for .NET 8, 9 and 10 — built on RabbitMQ.Client 7, wired through dependency injection and hosted services.</b>
</p>

<p align="center">
  <a href="https://dotnet.microsoft.com/"><img src="https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-512BD4?style=for-the-badge&logo=dotnet&logoColor=white" alt=".NET 8 | 9 | 10" /></a>
  <a href="https://www.nuget.org/packages/RedRabbit"><img src="https://img.shields.io/nuget/v/RedRabbit?style=for-the-badge&logo=nuget&color=004880" alt="NuGet" /></a>
  <a href="https://github.com/rojo1585/RedRabbit/blob/main/LICENSE.txt"><img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License: MIT" /></a>
</p>

> **Status: `1.x`.** The public API follows [Semantic Versioning](https://semver.org): breaking changes only in a new major version. The library has not yet been proven in production at scale, so start with a non-critical service; read the [project status](https://github.com/rojo1585/RedRabbit/blob/main/STATUS.md) for current limitations before adopting it.

---

## What it is

RedRabbit is a thin, opinionated layer over `RabbitMQ.Client` 7 for applications built on the .NET generic host (ASP.NET Core, worker services). You describe connections, producers and consumers in configuration, implement handler interfaces, and RedRabbit takes care of:

- **Publishing** typed events with publisher confirms, a per-producer channel pool and unroutable-message detection.
- **Consuming** in hosted services, one DI scope per message (or per batch), with configurable concurrency.
- **Retries and dead-lettering** through delay queues and a dead-letter queue it declares for you.
- **Event versioning**: older event versions are upgraded to the version your handler expects.
- **Graceful shutdown**: in-flight work gets a drain window that fits inside the host's shutdown timeout.
- **Reconnection** with exponential backoff, **TLS** (including private CAs), **health checks** and **OpenTelemetry** traces and metrics.

## What it is not

- **Not a service bus.** No sagas, request/response, scheduling, outbox or inbox.
- **Not exactly-once.** Delivery is *at-least-once*: handlers must be idempotent (see [Delivery guarantees](#delivery-guarantees)).
- **Not transport-agnostic.** It is RabbitMQ-only, on AMQP 0-9-1.
- **Not interoperable by default.** Messages are wrapped in a RedRabbit JSON envelope and rely on RedRabbit headers. Producers and consumers on the other side are expected to use RedRabbit too (or follow the [wire format](#wire-format)).

---

## Installation

```bash
dotnet add package RedRabbit
```

Requirements: .NET 8, 9 or 10 and the generic host (`Microsoft.Extensions.Hosting`). Integration tests run against RabbitMQ 3.13.

---

## Quick start

### 1. Configuration (`appsettings.json`)

```json
{
  "RabbitMQ": {
    "Connections": {
      "main": {
        "HostName": "localhost",
        "UserName": "guest",
        "Password": "guest"
      }
    },
    "Producers": [
      {
        "ServiceKey": "orders",
        "ConnectionName": "main",
        "ExchangeName": "orders",
        "RoutingKey": "order-created"
      }
    ],
    "Consumers": [
      {
        "ServiceKey": "orders-consumer",
        "ConnectionName": "main",
        "ExchangeName": "orders",
        "QueueName": "orders.created",
        "RoutingKey": "order-created"
      }
    ]
  }
}
```

The connection name is the dictionary key (`"main"`). Configuration is validated at startup; invalid settings stop the host with an `OptionsValidationException` listing every problem.

### 2. Registration (`Program.cs`)

```csharp
using RedRabbit.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRabbitMQ(builder.Configuration);                         // reads the "RabbitMQ" section
builder.Services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer");   // binds a handler to a consumer

var app = builder.Build();
app.Run();
```

`AddRabbitMQ` can be called only once per service collection; several brokers are configured as several entries under `Connections`.

### 3. The event

```csharp
using RedRabbit.Diagnostics;

[EventVersion("order-created", 1)]
public sealed record OrderCreated(Guid OrderId, string CustomerEmail);
```

`[EventVersion]` gives the event a stable name that travels in the `x-event-type` header. Without it, the type's full name (`Namespace.OrderCreated`) is used, which only works if publisher and consumer share the same type. **Use the attribute for events exchanged between services.**

### 4. Publishing

```csharp
using RedRabbit.Abstractions;

public sealed class OrderService(IEventPublisher publisher)
{
    public Task CreateOrderAsync(Guid orderId, string email, CancellationToken ct)
        => publisher.PublishAsync(new OrderCreated(orderId, email), cancellationToken: ct);
}
```

### 5. Consuming

```csharp
using RedRabbit.Abstractions;

public sealed class OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) : IRabbitHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated @event, MessageContext context, CancellationToken cancellationToken)
    {
        logger.LogInformation("Order {OrderId} (correlation {CorrelationId}, retry {RetryCount})",
            @event.OrderId, context.CorrelationId, context.RetryCount);
        return Task.CompletedTask;
    }
}
```

Returning normally ACKs the message. Throwing sends it through [retries and dead-lettering](#retries-and-dead-lettering).

---

## Publishing

`IEventPublisher` and `IBatchEventPublisher` are singletons.

```csharp
await publisher.PublishAsync(evt);                                                  // the only producer
await publisher.PublishAsync(evt, new PublishOptions(ProducerKey: "orders"));      // a specific producer
await publisher.PublishAsync(evt, new PublishOptions(RoutingKey: "order.eu"));     // override the routing key
await publisher.PublishAsync(evt, new PublishOptions(CorrelationId: requestId));   // keep a correlation id
```

- **Choosing a producer.** Without `ProducerKey` there must be exactly one producer; otherwise `AmbiguousProducerException` is thrown. An unknown key throws `ProducerNotFoundException`.
- **Publisher confirms** (`EnablePublisherConfirms`, on by default). `PublishAsync` completes only after the broker confirms the message. A broker NACK throws `PublisherNackException`; no confirm within `PublishConfirmTimeoutMs` throws `PublisherConfirmTimeoutException`.
- **Unroutable messages** (`Mandatory`, on by default). If no queue is bound for the routing key, `PublisherNackException` is thrown with `IsReturn = true`. This detection requires publisher confirms; with confirms disabled, returned messages are silently dropped.
- **Channel pool** (`ChannelPoolSize`, default 4). Publishes rent a channel from a per-producer pool; when every channel is in use, callers wait. `0` opens and closes a channel per publish.
- **Persistence** (`Persistent`, on by default) marks messages as persistent (delivery mode 2).
- **Topology.** With `AutoDeclareTopology` (default), the producer declares its exchange as durable before publishing.
- **Broker unavailable.** A publish waits up to 30 seconds for a connection, then throws `TimeoutException`.

### Batch publishing

```csharp
await batchPublisher.PublishBatchAsync(events, new PublishOptions(ProducerKey: "orders"));
```

Messages are published one after another on a single channel, each with its own confirm. **The batch is not atomic**: if message N fails, messages 1..N-1 are already published and the exception surfaces to the caller. `PublishOptions.CorrelationId` is not applied to batches; every message gets its own correlation id.

---

## Consuming

Each consumer in configuration runs in a hosted service. It declares its topology (unless `AutoDeclareTopology` is `false`), consumes with manual acknowledgements and dispatches each message to the handler registered for its event type.

- **Handlers** are resolved from a **new DI scope per message**, so they can depend on scoped services such as a `DbContext`. Register them with `AddRabbitHandler<THandler>(consumerKey)`. One handler type per event type; a consumer can have handlers for several event types.
- **Concurrency.** Up to `MaxConcurrentHandlers` messages are processed in parallel. With the default `0`, the limit is `PrefetchCount` (default 10). Set `PrefetchCount = 1` (or `MaxConcurrentHandlers = 1`) to process messages one at a time.
- **Ordering is not guaranteed** with parallel processing or retries.
- **`MessageContext`** exposes the correlation id, message id, publish time, publisher name, routing key, exchange, `Redelivered`, the retry count and the message headers (headers starting with `x-` are excluded).
- **`CancellationToken`** passed to the handler is cancelled only if the shutdown drain timeout expires (see [Graceful shutdown](#graceful-shutdown)).

### Batch consumer

```json
{
  "ServiceKey": "notifications-batch",
  "ConnectionName": "main",
  "ExchangeName": "notifications",
  "QueueName": "notifications.pending",
  "RoutingKey": "notification",
  "EnableBatchConsumer": true,
  "BatchSize": 50,
  "BatchTimeoutMs": 2000,
  "PrefetchCount": 100
}
```

```csharp
builder.Services.AddBatchRabbitHandler<NotificationBatchHandler>("notifications-batch");

public sealed class NotificationBatchHandler : IBatchRabbitHandler<NotificationRequested>
{
    public Task HandleBatchAsync(IReadOnlyList<NotificationRequested> events,
                                 IReadOnlyList<MessageContext> contexts,
                                 CancellationToken cancellationToken)
        => SaveAllAsync(events, cancellationToken);   // e.g. one bulk insert
}
```

- A batch is flushed when it reaches `BatchSize`, or `BatchTimeoutMs` after its first message arrives.
- If a flush contains several event types, it is split into one batch per type, each sent to its own handler.
- **All or nothing per batch:** success ACKs every message; an exception retries or dead-letters every message of the batch together.
- Keep `PrefetchCount` ≥ `BatchSize`; otherwise batches never fill up and always flush on the timeout.

---

## Retries and dead-lettering

With the defaults (`EnableRetry = true`, `EnableDeadLetter = true`, `MaxRetries = 3`), a message whose handler throws is retried with a delay, and after the last attempt it is moved to a dead-letter queue.

```text
attempt 1 fails ─► republished to {queue}.retry.0s   (confirmed) ─► ACK original
                   └─ TTL expires ─► back to {queue}
attempt 2 fails ─► republished to {queue}.retry.5s  ─► ...
attempt 3 fails ─► NACK (no requeue) ─► broker dead-letters to {queue}.dlx ─► {queue}.dlq
                                       └─► IDeadLetterHandler (optional notification)
```

- `MaxRetries` is the total number of attempts, including the first. `RetryDelays` holds one delay per retry (default `0s, 5s, 30s`); it needs at least `MaxRetries - 1` entries.
- The attempt count travels in the `redrabbit-delivery-count` header and is exposed as `MessageContext.RetryCount` (`0` on the first attempt).
- A retry is ACKed only after the broker confirms the copy in the retry queue. If that publish fails, the original is NACKed with requeue, so the message is not lost (it is redelivered immediately, without counting an attempt).

### What happens to each kind of failure

| Situation | Outcome |
|---|---|
| Handler throws | Retried; dead-lettered after `MaxRetries` attempts |
| Body cannot be deserialized | Dead-lettered immediately (no retries) |
| Event version newer than any registered upgrader | Retried, then dead-lettered |
| Missing `x-event-type` header, or no handler registered for the event type | Rejected without requeue → dead-lettered; logged as a warning |
| Host shutting down before the handler finishes | Requeued without counting an attempt |
| `EnableRetry = false` | Dead-lettered on the first failure |
| `EnableDeadLetter = false` | Messages that would be dead-lettered are **discarded** |

### Topology declared per consumer

| Resource | Default name | Notes |
|---|---|---|
| Main queue | `QueueName` | Durable, bound to `ExchangeName` with `RoutingKey`; dead-letters to the DLX |
| Dead-letter exchange | `{QueueName}.dlx` | `direct` |
| Dead-letter queue | `{QueueName}.dlq` | Durable, bound to the DLX with routing key `dead` |
| Retry queues | `{QueueName}.retry.{N}s` | One per distinct delay; TTL = delay; dead-letter back to the main queue |

The DLX/DLQ can be renamed or pointed at existing resources with the `DeadLetter` options (`AutoDeclare = false` only verifies that they exist).

### Dead-letter notifications

```csharp
public sealed class AlertOnDeadLetter(ILogger<AlertOnDeadLetter> logger) : IDeadLetterHandler
{
    public string ConsumerKey => "orders-consumer";

    public Task HandleAsync(DeadLetterMessage message, CancellationToken cancellationToken = default)
    {
        logger.LogError("Dead-lettered {MessageId}: {ExceptionType} {ExceptionMessage}",
            message.MessageId, message.ExceptionType, message.ExceptionMessage);
        return Task.CompletedTask;
    }
}

builder.Services.AddScoped<IDeadLetterHandler, AlertOnDeadLetter>();
```

The handler is a notification only: by the time it runs, the message is already in the DLQ (or discarded). Exceptions it throws are logged and ignored. It is not invoked for messages rejected because of a missing header or a missing handler.

---

## Event versioning

When an event's contract changes, consumers can keep accepting the old version:

```csharp
[EventVersion("order-created", 1)]
public sealed record OrderCreatedV1(Guid OrderId, string Email);

[EventVersion("order-created", 2)]
public sealed record OrderCreatedV2(Guid OrderId, string Email, string Channel);

public sealed class OrderCreatedV1ToV2 : IEventUpgrader<OrderCreatedV1, OrderCreatedV2>
{
    public string EventName => "order-created";
    public int FromVersion => 1;
    public int ToVersion => 2;
    public OrderCreatedV2 Upgrade(OrderCreatedV1 source) => new(source.OrderId, source.Email, Channel: "unknown");
}

builder.Services.AddEventUpgrader<OrderCreatedV1ToV2>();
builder.Services.AddRabbitHandler<OrderCreatedHandler>("orders-consumer");   // handles OrderCreatedV2
```

- The publisher sends the version from `[EventVersion]` in the `x-event-version` header.
- The consumer deserializes the message as the version it was sent with and runs the upgrader chain up to the latest registered version. This works for both single and batch consumers.
- Chains are validated at startup: they must start at version 1, have no gaps, and every upgrader must go strictly forward.
- A message newer than the latest registered version throws `EventVersionNewerThanRegisteredException` (it is retried and then dead-lettered).
- Upgraders are resolved from DI, so they can have dependencies.

---

## Wire format

Each message carries these headers:

| Header | Content |
|---|---|
| `x-event-type` | Event name (`[EventVersion]` name or type full name) — **required by consumers** |
| `x-event-version` | Event version (default `1`) |
| `x-correlation-id`, `x-message-id` | Correlation id (generated if not provided) and a unique message id |
| `x-published-at`, `x-publisher-name` | Publish time (ISO 8601) and producer key |
| `traceparent`, `tracestate` | W3C trace context, when tracing is enabled |
| `redrabbit-delivery-count` | Retry attempt counter (added on retries) |

The default serializer (System.Text.Json, camelCase, nulls omitted) writes an envelope:

```json
{ "eventType": "order-created", "eventVersion": 1, "payload": { "orderId": "…", "customerEmail": "…" } }
```

To change it, call one of these inside `AddRabbitMQ(configuration, configure: b => …)`:

- `WithJsonSerializer(options => …)` to tweak the JSON options;
- `WithSerializer<TSerializer>()` or `WithSerializer(instance)` to use your own `IMessageSerializer`. Its `ContentType` is sent as the message content type.

---

## Connections

- **Multiple brokers or vhosts.** Add one entry per broker under `Connections` and point each producer/consumer at it with `ConnectionName`. Each entry is one AMQP connection.
- **Startup.** Connections are opened in the background, so the application starts even if RabbitMQ is down. Consumers start consuming once connected; publishes wait up to 30 seconds.
- **Reconnection.** When a connection drops, it is reopened with exponential backoff (1 s doubling up to `MaxBackoffSeconds`). Consumers re-declare their topology and resume. The driver's own automatic recovery is disabled on purpose.
- **One host per connection.** There is no list of cluster nodes; use a load balancer or DNS name in front of a cluster.
- **Code-based configuration** is also available: `AddRabbitMQ(settings => { settings.Connections["main"] = new() { … }; })`.

### TLS

```json
"Connections": {
  "secure": {
    "HostName": "rabbit.internal",
    "Port": 5671,
    "UserName": "app",
    "Password": "…",
    "Tls": {
      "Enabled": true,
      "CaCertificatePath": "/etc/ssl/corp-root-ca.pem"
    }
  }
}
```

- The server certificate is validated against the OS trust store, including hostname and revocation (CRL/OCSP).
- `CaCertificatePath` trusts a private CA (PEM bundle or DER) as the only root.
- `CertPath` / `CertPassphrase` enable mutual TLS.
- `DisableCertificateRevocationCheck` is needed when the CA has no reachable CRL/OCSP endpoint.
- `AllowUnknownCAs` accepts untrusted roots; it is for development only.

---

## Graceful shutdown

When the host stops, each consumer stops receiving new messages and gives in-flight handlers up to `ShutdownDrainTimeout` (default 20 s) to finish and be ACKed. Handlers still running after that:

- receive cancellation through their `CancellationToken`;
- have their messages requeued without counting an attempt;
- keep their DI scope alive until they actually finish.

The drain is capped automatically so that it fits inside the host's shutdown timeout (`HostOptions.ShutdownTimeout`, 30 s by default) minus 10 s reserved for closing. A warning is logged when that happens. To drain for longer, raise the host timeout:

```csharp
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(60));
```

---

## Observability

### OpenTelemetry

RedRabbit uses one `ActivitySource` and one `Meter`, both named after `InstrumentationName` (default `"RabbitMQ"`; change it with `WithInstrumentationName` or the `InstrumentationName` setting).

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource("RabbitMQ"))
    .WithMetrics(m => m.AddMeter("RabbitMQ"));
```

**Tracing.** Spans are named `{event} publish`, `{event} receive` and `{event} process`, with `messaging.*` attributes. Trace context is propagated in the `traceparent` header; consumer spans link to the producer span.

**Metrics**

| Metric | Type | Tags |
|---|---|---|
| `redrabbit.published` | Counter | `producer_key`, `event_type`, `exchange` |
| `redrabbit.publish_errors` | Counter | `producer_key`, `event_type`, `error_type` |
| `redrabbit.publish_duration_ms` | Histogram | `producer_key`, `event_type`, `exchange` |
| `redrabbit.consumed` | Counter | `consumer_key`, `event_type`, `queue` |
| `redrabbit.consume_errors` | Counter | `consumer_key`, `event_type`, `queue`, `error_type` |
| `redrabbit.processing_duration_ms` | Histogram | `consumer_key`, `event_type`, `queue` |
| `redrabbit.retried` | Counter | `consumer_key`, `event_type`, `queue`, `attempt` |
| `redrabbit.dead_lettered` | Counter | `consumer_key`, `event_type`, `queue`, `dlq_name` |
| `redrabbit.handlers_abandoned` | Counter | `consumer_key`, `event_type`, `queue` |
| `redrabbit.batches_dispatched` | Counter | `consumer_key`, `event_type`, `queue`, `flush_reason` |
| `redrabbit.batch_size` | Histogram | `consumer_key`, `event_type`, `queue` |
| `redrabbit.channel_pool.rented` / `.returned` / `.discarded` / `.created` | Counter | `producer_key` |

`redrabbit.dead_lettered` counts messages that exhausted their retries (including those discarded when `EnableDeadLetter = false`).

### Health checks

```csharp
builder.Services.AddHealthChecks()
    .AddRabbitMq("main")    // one connection: Healthy while it is open
    .AddRabbitMqAll();      // every connection: Unhealthy if any is closed
```

Both are tagged `rabbitmq`. `IRabbitConnectionRegistry` exposes the same connection state to your own code.

---

## Configuration reference

### Connection (`Connections:{name}`)

| Property | Default | Notes |
|---|---|---|
| `HostName`, `UserName`, `Password` | — | Required |
| `Port` | `5672` | 1–65535 (TLS usually `5671`) |
| `VirtualHost` | `/` | |
| `RequestedHeartbeatSeconds` | `60` | ≥ 1 |
| `ConnectionTimeoutSeconds` | `30` | TCP connect and AMQP handshake timeout; ≥ 1 |
| `MaxBackoffSeconds` | `60` | Reconnection backoff cap; ≥ 1 |
| `Tls` | `null` | `Enabled`, `ServerName`, `CaCertificatePath`, `CertPath`, `CertPassphrase`, `Protocol`, `DisableCertificateRevocationCheck`, `AllowUnknownCAs` |

### Producer (`Producers[]`)

| Property | Default | Notes |
|---|---|---|
| `ServiceKey`, `ConnectionName`, `ExchangeName`, `RoutingKey` | — | Required; `ServiceKey` must be unique |
| `ExchangeType` | `direct` | |
| `EnablePublisherConfirms` | `true` | |
| `PublishConfirmTimeoutMs` | `5000` | > 0 |
| `Mandatory` | `true` | Unroutable messages throw (requires confirms) |
| `Persistent` | `true` | |
| `ChannelPoolSize` | `4` | `0` = channel per publish |
| `AutoDeclareTopology` | `true` | Declares the exchange |
| `ExchangeArguments` | `null` | |

### Consumer (`Consumers[]`)

| Property | Default | Notes |
|---|---|---|
| `ServiceKey`, `ConnectionName`, `ExchangeName`, `QueueName`, `RoutingKey` | — | Required; `ServiceKey` must be unique |
| `ExchangeType` | `direct` | |
| `PrefetchCount` | `10` | ≥ 1 |
| `MaxConcurrentHandlers` | `0` | `0` = up to `PrefetchCount` |
| `ShutdownDrainTimeout` | `00:00:20` | Capped to fit the host shutdown timeout |
| `EnableRetry` | `true` | |
| `MaxRetries` | `3` | Total attempts; ≥ 1 |
| `RetryDelays` | `[0s, 5s, 30s]` | ≥ `MaxRetries - 1` entries; use whole seconds |
| `EnableDeadLetter` | `true` | `false` discards instead of dead-lettering |
| `DeadLetter` | `null` | `ExchangeName`, `QueueName`, `ExchangeType`, `RoutingKey`, `AutoDeclare`, arguments |
| `EnableBatchConsumer` | `false` | |
| `BatchSize` | `10` | Batch mode only |
| `BatchTimeoutMs` | `5000` | Batch mode only |
| `AutoDeclareTopology` | `true` | Declares exchange, queues and bindings |
| `QueueArguments` | `null` | Extra arguments for the main queue |
| `MessageTtl` | `null` | Main queue message TTL |
| `SingleActiveConsumer` | `false` | |
| `ConsumerTag` | `null` | |

Top-level settings: `InstrumentationName` (default `"RabbitMQ"`).

> RabbitMQ refuses to redeclare an existing queue with different arguments. After changing `RetryDelays`, `MessageTtl`, `QueueArguments` or dead-letter settings on a queue that already exists, delete the affected queues (or migrate them) first.

---

## Exceptions

All RedRabbit exceptions derive from `RabbitMqException`.

| Exception | Thrown when |
|---|---|
| `AmbiguousProducerException` | Publishing without `ProducerKey` while zero or several producers are configured |
| `ProducerNotFoundException` | `ProducerKey` does not match any producer |
| `PublisherNackException` | The broker NACKed the message, or returned it as unroutable (`IsReturn`) |
| `PublisherConfirmTimeoutException` | No confirm within `PublishConfirmTimeoutMs` |
| `EventVersionNewerThanRegisteredException` | (consumer side) a message is newer than the latest registered upgrader |
| `MessageDeserializationException` | (consumer side) the body cannot be deserialized; the message is dead-lettered |

Configuration errors surface at startup as `OptionsValidationException`. A publish that cannot get a connection within 30 seconds throws `TimeoutException`.

---

## Delivery guarantees

- **At-least-once.** A message can be processed more than once: after a connection loss before the ACK, after a shutdown that requeued an unfinished handler, or if an ACK is lost. Make handlers idempotent.
- **No loss in the retry path.** The original message is ACKed only after its retry copy is confirmed by the broker.
- **Publishing is not transactional** with your database. If you need that, use an outbox in your application.

---

## Documentation

- [Project status and limitations](https://github.com/rojo1585/RedRabbit/blob/main/STATUS.md)
- [Concurrency design](https://github.com/rojo1585/RedRabbit/blob/main/CONCURRENCY.md)

## License

[MIT](https://github.com/rojo1585/RedRabbit/blob/main/LICENSE.txt)
