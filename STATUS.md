# Project Status

> This document provides a transparent assessment of the current state of RabbitFlow.
> It complements the [README](README.md) (which describes what the package does) with
> known limitations, technical debt, maintenance model, and adoption recommendations.

---

## Current Version

**Pre-1.0 (active development)**

The package is functionally complete for its intended use cases. The existing test
suite (unit + integration with Testcontainers/RabbitMQ) passes on GitHub Actions
using .NET 8 and .NET 10 SDKs.

A **pilot deployment** in a non-critical service is recommended before expanding to
business-critical workloads.

---

## Maintenance Model

### Authorship & License
RabbitFlow is an open-source project released under the [MIT License](LICENSE). The
MIT license grants full rights to use, modify, fork, and distribute — ensuring the
package remains usable by any adopter regardless of the author's continued
involvement.

### Bus factor
The current bus factor is 1 (single maintainer). This is the main sustainability risk
and is acknowledged honestly here. Mitigations:

| Risk | Mitigation | Status |
|------|------------|--------|
| Maintainer becomes unavailable | MIT license → any adopter can fork and continue | ✅ In place |
| Knowledge concentration | Codebase is documented (README, STATUS, CONCURRENCY, XML docs); any senior .NET dev can apply patches | ✅ In place |
| Long-term sustainability | Encourage external contributions via GitHub; build a small community around the package | 🟡 Post-1.0 |

If your organization depends on RabbitFlow for business-critical workloads, consider:
- Pinning a specific version in your `nuget.config` (so upstream changes don't surprise you).
- Maintaining an internal fork if you need guarantees beyond the upstream cadence.
- Sponsoring or contributing to the project to help reduce the bus factor.

---

## Known Limitations (By Design)

These are deliberate scope decisions for the current version — not bugs:

1. **Single instrumentation name for tracing per process** — `RabbitMqActivitySource`
   is a static singleton. The first `AddRabbitMQ()` call's `InstrumentationName` wins
   for tracing; subsequent calls with different names are silently ignored for
   tracing. Metrics (`RabbitMqMetrics`) are per-DI-container and not affected.
   To use different tracing names in the same process, isolate per app domain.

2. **No outbox pattern** — RabbitFlow does not coordinate publish operations with
   database transactions. If you need exactly-once publish with EF Core, implement
   an outbox table and a dispatcher outside RabbitFlow.

3. **No inbox / idempotency** — Handlers are responsible for idempotency. RabbitFlow
   guarantees at-least-once delivery (the broker may redeliver on reconnect or
   retry); duplicate consumption is possible and must be handled by the handler.

4. **No SAGA orchestration** — RabbitFlow is a message publisher/consumer, not a
   workflow engine. For SAGA support, consider MassTransit or NServiceBus.

5. **No circuit breaker / hedging** — Publisher-side retries when the broker is down
   rely on the `ManagedConnection` reconnect loop with exponential backoff. There
   is no Polly integration or circuit breaker.

6. **Driver's `AutomaticRecoveryEnabled` is disabled** — RabbitFlow owns the
   reconnection lifecycle to avoid races with the manual reconnect loop. This is
   intentional and documented in `CONCURRENCY.md`.

---

## Known Technical Debt

These items do not affect correctness but are candidates for future improvement:

| Item | Impact | Candidate Solution |
|------|--------|--------------------|
| ~400 lines duplicated between `NamedRabbitConsumer` and `NamedBatchRabbitConsumer` | Maintainability — changes must be applied in both classes | Extract a shared `RabbitConsumerBase` or helper class |
| No load-test benchmarks published | Capacity planning requires adopters to benchmark their own workload | Add a load-test script to the repo; publish reference numbers for common configurations |
| `ManagedConnection.CreateChannelAsync` uses a 200ms busy-poll while waiting for reconnect | Reconnect latency up to ~200ms average | Replace with `TaskCompletionSource` signaled by the connection loop |
| `NamedRabbitPublisher` declares topology on every channel rental instead of only after reconnect | One extra AMQP round-trip per publish | Track topology-declared state per connection and reset on shutdown |
| Bus factor = 1 | Sustainability | Encourage external contributions; MIT license enables forking |

---

## Roadmap

Features explicitly **deferred** to future versions to keep the 1.0 scope focused
and the API surface minimal:

- **1.1** — Refactors (`RabbitConsumerBase` extraction, `CreateChannelAsync` TCS
  signaling, topology declaration optimization).
- **1.2** — Top 2-3 features requested by early adopters (candidates: Outbox,
  Inbox/idempotency, Polly integration, `IDeadLetterHandler<TEvent>` typed,
  `IOptionsMonitor` hot-reload, Quorum/Stream queue first-class support).
- **2.0+** — Larger features that may require API changes (Avro/Protobuf schema
  registry, Roslyn analyzers, source generators for handler registration).

Feature requests are welcome — please open a GitHub issue with your use case.

---

## Adoption Recommendation

| Workload type | Recommendation |
|---------------|----------------|
| **Non-critical service** (notifications, webhooks, audit logs) | ✅ Adopt now — pilot for 4-6 weeks |
| **Business-critical service** (orders, payments, inventory) | 🟡 After successful pilot in non-critical service |
| **Mission-critical / life-safety** | 🔴 Wait for 1.0 stable + production soak reports from early adopters |

### Pilot checklist
- [ ] Deploy to a non-critical service
- [ ] Monitor `rabbitflow.consume_errors`, `rabbitflow.dead_lettered`, `rabbitflow.retried` for 1 week
- [ ] Verify no `OutOfMemoryException` or connection leak trends
- [ ] Confirm graceful shutdown behavior (in-flight handlers complete within timeout)
- [ ] Report any issues via GitHub Issues

---

## License

[MIT](LICENSE) — see [LICENSE](LICENSE) for details.
