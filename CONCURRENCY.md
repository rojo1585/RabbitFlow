# Concurrency Design

This document describes how RedRabbit's concurrent components work today and the invariants they rely on. Read it before changing connection management, channel pools, consumers, shutdown or diagnostics code.

---

## Overview

| Component | Shared by | Concurrency mechanism |
|---|---|---|
| `ManagedConnection` | All producers and consumers on one named connection | One reconnection loop; `lock` around connection swaps |
| `ChannelPool` | Concurrent publishes of one producer; retries of one consumer | `SemaphoreSlim` + `ConcurrentQueue` + `Interlocked` |
| `RetryQueuePublisher` | Concurrent failing handlers of one consumer | Pooled confirm channels; ACK only after the broker confirms |
| `NamedRabbitConsumer` | Parallel deliveries on one channel | Driver dispatch concurrency + optional `SemaphoreSlim` limiter |
| `NamedBatchRabbitConsumer` | Delivery callbacks and one dispatch loop | Bounded `Channel<T>` (many writers, one reader) |
| `HandlerExecution` | Each handler invocation | Ownership transfer of the DI scope at shutdown |
| `RabbitConsumerHostedService` | All consumers | Consumers run and dispose in parallel inside a shutdown budget |
| `HandlerTypeRegistry`, `EventUpgraderRegistry` | All consumers | Immutable after construction |
| `RabbitMqActivitySource` | Whole process | Static, initialized once under a `lock` |

---

## Connections — `ManagedConnection`

- `Start()` runs **one background loop** per connection (guarded by `Interlocked.CompareExchange`); a second call throws.
- The loop connects, then awaits a `TaskCompletionSource` that is completed when the connection shuts down. It then waits with exponential backoff (1 s doubling up to `MaxBackoffSeconds`) and reconnects.
- The driver's `AutomaticRecoveryEnabled` and `TopologyRecoveryEnabled` are **off**, so this loop is the only recovery path. Consumers re-declare their own topology when they recreate their channel.
- `_connection` and its shutdown `TaskCompletionSource` are swapped together under `lock (_lock)`. The shutdown handler clears `_connection` only if the event comes from the current connection (`ReferenceEquals`), so a late event from an old connection is ignored.
- `CreateChannelAsync` reads the current connection without the lock (it reads a single reference). While the connection is down, it polls every 200 ms until one is available, the timeout (default 30 s) expires, or the caller cancels.
- `DisposeAsync` is single-shot (`Interlocked.Exchange`). It cancels the loop, waits for it for at most 2 s, and closes the connection. A cancellation registration completes the pending `TaskCompletionSource`, so the loop never blocks disposal.

`RabbitConnectionRegistry` creates one `ManagedConnection` per configured name. `StartAll` can run only once, and its dictionary is never modified after construction.

---

## Channel pools — `ChannelPool`

Each **producer** has its own pool (`ChannelPoolSize`), and each **consumer with retries enabled** has one for its retry publishes (sized to its handler concurrency, between 1 and 8).

- `SemaphoreSlim(maxSize, maxSize)` bounds the number of rented channels. When all are rented, callers wait; that wait is the backpressure.
- Idle channels live in a `ConcurrentQueue`. `RentAsync` discards closed idle channels and opens a new one when none is usable.
- Every successful `RentAsync` is matched by exactly one `Return` (healthy channel) or `Discard` (closed, or in an unknown state after a confirm timeout). Both release the semaphore. A failed `RentAsync` releases its permit itself.
- `_currentCount` and `_disposed` are only accessed through `Interlocked` / `Volatile`. There is no `lock` on the rent/return path.
- `DisposeAsync` is single-shot. It waits for rented channels to come back for a bounded time (10 s by default, 2 s for retry pools), then closes the idle channels. Channels still rented when it gives up are not closed by the pool, because their renter is still using them.

---

## Retry publishing — `RetryQueuePublisher`

- A failed message is republished to `{queue}.retry.{N}s` on a pooled channel with **publisher confirms** and **`mandatory: true`**, waiting at most 30 s for the confirm.
- The consumer ACKs the original **only if the confirm arrives**. A NACK, a return (missing retry queue), a timeout or a channel error makes it NACK the original with requeue instead. The message is therefore never lost between the two queues; at worst it is processed again.
- After a confirm, a NACK or a return, the channel goes back to the pool. After a timeout or any other exception it is discarded, because its confirm state is unknown.

---

## Single-message consumer — `NamedRabbitConsumer`

### Dispatch

- Each consumer owns one channel at a time, created in its run loop. If the channel or connection fails, the loop waits 2 s and creates a new channel.
- Parallelism comes from the driver's **`consumerDispatchConcurrency`**: `MaxConcurrentHandlers` if set, otherwise `PrefetchCount`. When `MaxConcurrentHandlers > 0`, a `SemaphoreSlim` of the same size also guards the handler section.
- **ACK/NACK always use the channel that delivered the message.** That channel is captured in the delivery callback and passed down to every ACK/NACK call. A delivery tag is only valid on its own channel, so a channel field that may have been replaced must never be used.
- An `Interlocked` in-flight counter tracks callbacks that have not returned yet. The shutdown drain uses it.

### Shutdown sequence

When the host's stopping token fires:

1. `_isShuttingDown` is set; from then on, an `OperationCanceledException` from a handler means "requeue", not "failure".
2. `BasicCancelAsync` (bounded to 5 s) stops new deliveries.
3. In-flight handlers get `ShutdownDrainTimeout` to finish, checked every 100 ms. Handlers that finish are ACKed normally.
4. If the timeout expires, the handlers' `CancellationToken` is cancelled. Each pending handler wait is then abandoned and its message NACKed with requeue, without counting a retry. A 500 ms grace period lets those NACKs go out.
5. Only then does the run loop close the channel.

`DisposeAsync` cancels the handler token again, waits at most 3 s for in-flight callbacks to send their NACKs, then disposes the retry pool, the limiter and the token source.

---

## Batch consumer — `NamedBatchRabbitConsumer`

### Pipeline

- Delivery callbacks validate and deserialize each message, then write it to a **bounded `Channel<T>`** (capacity `PrefetchCount × 2`, `Wait` when full). Many callbacks write; **one dispatch loop reads**.
- The message body is **copied** before it is buffered. `RabbitMQ.Client` reuses the delivery memory after the callback returns, while the buffered copy is needed later for retry publishing and `IDeadLetterHandler`.
- The dispatch loop flushes a batch on `BatchSize`, on `BatchTimeoutMs` after the first message, or immediately once the buffer is completed. Each flush is split by event type; each group is dispatched to its handler in its own DI scope.
- ACK/NACK are sent **one delivery tag at a time** (`multiple: false`) on the delivering channel. `multiple: true` would also settle lower tags that are still buffered or belong to another group.

### Two cancellation signals

| Signal | When it fires | Effect |
|---|---|---|
| Host stopping token | Shutdown starts | Stop buffering and cancel the consumer |
| `_handlerCts` (handler token) | Only when the drain timeout expires, or on dispose | Abandon the in-flight batch |

The handler token is deliberately separate, so a batch already being processed is not interrupted the moment shutdown begins.

### Shutdown sequence

1. `_isShuttingDown` is set.
2. **The buffer is completed first.** This releases any delivery callback blocked on a full buffer. Deliveries that arrive afterwards are NACKed with requeue.
3. `BasicCancelAsync` (bounded to 5 s) stops new deliveries. Completing the buffer before cancelling avoids waiting on a callback that is itself waiting for buffer space.
4. The dispatch loop flushes what it holds as a final batch and exits. It gets `ShutdownDrainTimeout` to do so.
5. If the timeout expires, `_handlerCts` is cancelled: the in-flight batch is abandoned and NACKed with requeue, and anything still buffered is NACKed with requeue. A 5 s grace period lets those NACKs go out.
6. Only then is the channel closed.

If the channel closes on its own (broker restart, connection loss), the buffer is completed and the loop is awaited. The broker requeues every unacknowledged delivery.

---

## Handler scopes at shutdown — `HandlerExecution`

Both consumers run each handler invocation through `HandlerExecution.WaitAsync`, which waits for the handler or the shutdown signal:

- **Handler finishes first:** the caller keeps ownership of the DI scope and disposes it. A handler exception is rethrown to the caller.
- **Shutdown wins:** the caller NACKs with requeue, and **ownership of the scope moves to a continuation**. That continuation disposes the scope only when the handler actually finishes, and observes its exception. A handler that ignores cancellation therefore never sees its `DbContext` or other scoped services disposed underneath it.

---

## Shutdown budget — `RabbitConsumerHostedService`

- All consumers run concurrently and drain in parallel. Each consumer's `ShutdownDrainTimeout` is capped to `HostOptions.ShutdownTimeout − 10 s` (`ShutdownBudget`). The 10 s reserve covers the bounded steps after the drain (consumer cancel ≤ 5 s, abandon grace, channel close).
- Consumers are disposed **in parallel**, and every wait during disposal is bounded (≤ 3 s in-flight NACKs, ≤ 2 s retry pool), so the total does not grow with the number of consumers.
- `HostOptions` is read by reflection when available, so the library does not depend on the full `Microsoft.Extensions.Hosting` package. The fallback is 30 s.

---

## Shared registries and diagnostics

- **`HandlerTypeRegistry` and `EventUpgraderRegistry`** are built once from DI registrations and never modified. All lookups are reads, so they are safe for any number of concurrent consumers.
- **`RabbitMqActivitySource`** is static. `Initialize` uses a double-checked `lock`, and the first instrumentation name wins for the whole process. Readers only use `Source` after the host is built, which is after initialization. **`RabbitMqMetrics`** is a per-container singleton.
- **`CompositeEventPublisher`** is a singleton holding one `NamedRabbitPublisher` per producer. Its dictionary is read-only after construction, and publishing is safe from any thread. Its disposal flag is a plain field. It is disposed with the DI container, after hosted services stop, so publishing concurrently with container disposal is not supported.

---

## Invariants

Changes to concurrent code must preserve these:

1. **One recovery path per connection.** Keep `AutomaticRecoveryEnabled = false`, and swap `_connection` and its shutdown signal only under `lock (_lock)`.
2. **Channel pools:** every `RentAsync` is matched by exactly one `Return` or `Discard`. Use no `lock` on the rent/return path, and never close a channel another caller has rented.
3. **ACK/NACK use the channel that delivered the message**, passed as a parameter, never a field that may have been replaced. Batch consumers settle tags individually (`multiple: false`).
4. **Never ACK a failed message before its retry copy is confirmed** by the broker.
5. **Never touch `ea.Body` outside the delivery callback** without copying it first.
6. **Batch shutdown order:** complete the buffer, then cancel the consumer, then drain, then abandon. The handler token must stay separate from the host stopping token.
7. **A DI scope is disposed by exactly one owner,** and never while its handler is still running (`HandlerExecution`).
8. **Every shutdown wait is bounded,** and the drain is capped by `ShutdownBudget` so the host's shutdown timeout is never exceeded.
9. **Registries are immutable after construction.**
10. **`RabbitMqActivitySource` is initialized once per process;** later names are ignored for tracing.

---

## Tests that cover this design

| Area | Tests |
|---|---|
| Scope ownership at shutdown | `HandlerExecutionTests` (unit); `GracefulShutdownTests.ShutdownDrain_AbandonedHandler_KeepsScopedServicesAliveUntilItFinishes` |
| Drain and requeue (single consumer) | `GracefulShutdownTests` |
| Drain, requeue, body copy, per-type split (batch) | `BatchConsumerShutdownAndCorrectnessTests` |
| Shutdown budget | `ShutdownBudgetTests`; `GracefulShutdownTests.ShutdownDrain_LongerThanHostShutdownTimeout_IsCappedSoStopFinishesInTime` |
| Retry confirms and exact attempt counts | `RetryAndDeadLetterTests` |
| Parallel handlers | `RetryAndDeadLetterTests.MaxConcurrentHandlers_AllowsConcurrentHandlerExecution` |

Not covered by automated tests: broker restarts or connection loss under load, and channel-pool exhaustion under heavy concurrency.
