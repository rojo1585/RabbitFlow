# Project Status

This page describes the current state of RedRabbit: how mature it is, what has been verified, and the limitations you should know about before adopting it. For what the package does and how to use it, see the [README](README.md).

---

## Maturity

| | |
|---|---|
| **Version** | `0.x` pre-release (first published version: `0.1.0-preview.1`) |
| **API stability** | Not frozen. Public types and option names may still change before `1.0`. |
| **Recommended use** | Pilot in a non-critical service (notifications, audit, webhooks) before business-critical workloads. |
| **Maintainers** | One. The project is MIT-licensed, so it can be forked and maintained by adopters if needed. |

---

## What is verified

- **221 automated tests**, run on every pull request and on `main` by GitHub Actions:
  - unit tests for configuration validation, retry policy, versioning, serialization, TLS validation, shutdown budget and handler execution;
  - integration tests against a real RabbitMQ in Docker (Testcontainers) covering publish/consume round trips, publisher confirms, multiple connections, retries and dead-lettering (exact attempt counts), batch consumers, event upgrades and graceful shutdown.
- **Build:** the package targets `net8.0`, `net9.0` and `net10.0` and builds with zero warnings (warnings are treated as errors).
- **Test runtime:** tests run on .NET 8 only. The `net9.0` and `net10.0` builds are compiled but not exercised by the test suite.
- **Broker version:** integration tests use **RabbitMQ 3.13**. RabbitMQ 4.x has not been tested.

Not verified: throughput and latency under load (no benchmarks are published), long-running soak tests, and behaviour against clustered brokers.

---

## Limitations

These are known and current. Some are design decisions, others are candidates for improvement before `1.0`.

### Delivery and scope

- **At-least-once delivery.** Messages can be processed more than once (connection loss before an ACK, a handler abandoned at shutdown, a lost ACK). Handlers must be idempotent; there is no built-in inbox or deduplication.
- **No transactional publishing.** Publishing is not coordinated with database transactions; there is no outbox.
- **No service-bus features.** No sagas, request/response, scheduled messages or message routing beyond RabbitMQ exchanges.
- **Ordering is not guaranteed.** Consumers process up to `PrefetchCount` messages in parallel by default, and retries change the order. Sequential processing requires `PrefetchCount = 1` or `MaxConcurrentHandlers = 1`.

### Interoperability

- **Proprietary wire format.** Messages use a RedRabbit JSON envelope and require the `x-event-type` header. Systems that do not use RedRabbit must follow that format to publish to RedRabbit consumers, and must unwrap the envelope to read RedRabbit messages. A custom `IMessageSerializer` can change the body format, but the headers are still required.
- **No custom headers on publish.** The publishing API does not let you add your own headers.

### Connections

- **One host per connection.** There is no list of cluster nodes or client-side failover; put a load balancer or a DNS name in front of a cluster.
- **Publishing while disconnected waits up to 30 seconds** (not configurable) for a connection before throwing `TimeoutException`.
- **Reconnection detection is polled.** While a connection is being re-established, channel creation checks for it every 200 ms.

### Publishing

- **Topology is declared on every publish** when `AutoDeclareTopology` is on (one extra round trip to the broker per publish). Turn it off for producers whose exchange already exists if this matters.
- **Batch publishing is not atomic.** A failure part-way leaves the earlier messages published. `PublishOptions.CorrelationId` is not applied to batches.
- **Unroutable messages are only detected with publisher confirms enabled.** With confirms disabled, returned messages are dropped silently.

### Consuming

- **Rejected messages are only logged.** Messages without an `x-event-type` header, or with no registered handler, are rejected to the dead-letter queue with a warning log, but they are not counted in metrics and do not trigger `IDeadLetterHandler`.
- **Queue arguments cannot change in place.** RabbitMQ refuses to redeclare a queue with different arguments. Changing `RetryDelays`, `MessageTtl`, `QueueArguments` or dead-letter settings for an existing queue requires deleting or migrating the queue.
- **Retry delays are whole seconds.** Retry queues are named `{queue}.retry.{N}s`; two delays that round to the same second collide.
- **Quorum and stream queues are not supported as first-class options.** A queue type can be passed through `QueueArguments` (main queue) or `DeadLetter.QueueArguments` (DLQ), but this is untested. Retry queues are always declared without a queue type, so they get the virtual host's default (normally classic).

### Configuration and diagnostics

- **No configuration reload.** Settings are read once at startup; changes require a restart.
- **One tracing name per process.** The `ActivitySource` is static: the first `InstrumentationName` registered in a process is used for tracing. Metrics are per container and unaffected.
- **Logging volume.** Some per-message paths (retries, batch dispatch) log at `Information` level, and logging does not use source-generated `LoggerMessage` delegates.

### Maintenance

- **Duplicated consumer code.** The single-message and batch consumers share a lot of logic that is implemented twice, so fixes must be applied to both.
- **Single maintainer.** Pin exact package versions in production, and consider a fork if you need guarantees beyond the upstream release cadence.

---

## Planned before 1.0

- Freeze and track the public API.
- Run the test suite on .NET 10 and against RabbitMQ 4.x.
- Declare producer topology once per connection instead of on every publish.
- Signal reconnection instead of polling it.
- Source-generated logging, with per-message logs moved to `Debug`.
- Extract the logic shared by both consumer types.

Feature requests and bug reports are welcome as [GitHub issues](https://github.com/rojo1585/RedRabbit/issues).

---

## Adopting it in a pilot

1. Start with a non-critical service and make its handlers idempotent.
2. Monitor `redrabbit.consume_errors`, `redrabbit.retried`, `redrabbit.dead_lettered` and `redrabbit.handlers_abandoned`, and alert on messages arriving in the `.dlq` queues.
3. Check the logs for rejected messages (missing `x-event-type` header or no handler), since they are not counted in metrics.
4. Deploy a few times under load and confirm that shutdowns finish within the host timeout without unexpected dead-lettered messages.
5. Report problems as GitHub issues.
