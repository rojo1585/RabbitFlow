# Concurrency Analysis — RabbitFlow

> This document records the concurrency design and invariants of RabbitFlow.
> Future contributors MUST read this before modifying any concurrent code
> (connection management, channel pooling, consumer loops, event upgrading,
> static diagnostics).

## Summary

| ID  | Component                      | Concern                                    | Status                          |
|-----|--------------------------------|--------------------------------------------|---------------------------------|
| C1  | NamedRabbitConsumer            | ACK/NACK on volatile `_currentChannel`     | ✅ Resolved                      |
| C2  | ChannelPool                    | Rent/return/disc concurrency               | ✅ OK                            |
| C3  | ManagedConnection              | Reconnect loop race                        | ✅ OK                            |
| C4  | EventUpgraderRegistry          | Thread safety via immutability             | ✅ OK                            |
| C5  | HandlerTypeRegistry            | Thread safety via immutability             | ✅ OK                            |
| C6  | RabbitMqActivitySource         | Static mutable state                       | ✅ OK in practice (documented limitation) |
| C7  | RabbitConnectionRegistry       | StartAll idempotency                       | ✅ OK                            |
| C8  | ManagedConnection.DisposeAsync | Possible deadlock                          | ✅ OK (no deadlock)              |
| C9  | ChannelPool.DisposeAsync       | Possible deadlock                          | ✅ OK (10s latency worst case)   |

## Detailed Analysis

### C1: NamedRabbitConsumer — ACK/NACK on volatile `_currentChannel` — Resolved

**Concern**: Multiple in-flight messages (with `PrefetchCount > 1`) all used a shared
`volatile IChannel? _currentChannel` field for ACK/NACK. When the channel dropped, the
reconnect loop replaced `_currentChannel` with a new channel. In-flight handlers from
the old channel then ACKed/NACKed on the new channel (with a `deliveryTag` belonging to
the old channel), producing `AlreadyClosedException` silently swallowed.

**Resolution**: The concrete `IChannel` is captured in the `OnReceived` closure and
threaded through `ProcessMessageAsync` → `InvokeHandlerWithRetryAsync` → `AckAsync`/
`NackAsync`. ACK/NACK always operate on the channel that received the message.
`AckAsync(IChannel channel, ulong deliveryTag)` and
`NackAsync(IChannel channel, ulong deliveryTag, bool requeue)` take the channel as
a parameter and never read `_currentChannel`. The `volatile IChannel? _currentChannel`
field is kept for diagnostics/future use but is not in the ACK/NACK path.

**Invariant**: `AckAsync` and `NackAsync` MUST accept `IChannel channel` as a parameter
and use it directly. Never read `_currentChannel` for ACK/NACK operations.

### C2: ChannelPool — Rent/Return/Discard — OK

**Concern**: The `ChannelPool` is shared across all publishers for one named connection.
Multiple concurrent rentals, returns, and discards must not corrupt the idle queue, the
semaphore count, the `_currentCount` counter, or the `_disposed` flag. A lost
`SemaphoreSlim.Release()` would deadlock the pool; a duplicate increment would leak
channels; a torn read of `_disposed` would race with `DisposeAsync`.

**Verification**: Inspected `ChannelPool.cs`:
- `SemaphoreSlim _semaphore` (sized `MaxSize, MaxSize`) gates concurrent rentals. Each
  `RentAsync` does `await _semaphore.WaitAsync(cts)`; each `Return`/`Discard` does
  `_semaphore.Release()`. The semaphore provides backpressure when all channels are in
  use and guarantees the count can never exceed `MaxSize`.
- `ConcurrentQueue<IChannel> _available` holds idle channels. `TryDequeue`/`Enqueue`
  are lock-free and concurrent-safe; the `while (_available.TryDequeue(...))` loop in
  `RentAsync` discards closed channels lazily on next rental.
- `Interlocked.Increment/Decrement(ref _currentCount)` tracks total channels (idle +
  rented) for diagnostics; never read with `++`/`--`.
- `Interlocked.Exchange(ref _disposed, 1)` in `DisposeAsync` guarantees single-execution
  semantics even if called concurrently (matches the pattern in `ManagedConnection`).
  `IsDisposed` uses `Volatile.Read(ref _disposed)`.
- `Return`/`Discard` swallow `ObjectDisposedException` from `_semaphore.Release()` so
  returning a channel after pool disposal doesn't crash the caller.
- `RentAsync` releases the semaphore on `catch` so a failed `CreateChannelAsync` doesn't
  leak a permit.

**Result**: No race. The pool is safe for concurrent use. No plain `lock` in the hot
path; only lock-free primitives.

### C3: ManagedConnection — Reconnect Loop — OK

**Concern**: The reconnect loop must (a) be the only recovery path, (b) atomically swap
`_connection` and `_connectionClosedTcs` together, and (c) avoid racing with itself if
two shutdowns fire back-to-back. A duplicate recovery loop would open two TCP
connections and confuse `IsConnected`.

**Verification**: Inspected `ManagedConnection.cs`:
- `Start()` uses `Interlocked.CompareExchange(ref _started, 1, 0)` — only one loop can
  be started; a second call throws `InvalidOperationException`.
- `ConnectionLoopAsync` is the single recovery loop. After `TryConnectAsync`, it
  awaits `_connectionClosedTcs.Task` (a `TaskCompletionSource<bool>` with
  `RunContinuationsAsynchronously`) to be completed by `OnConnectionShutdown`.
- `TryConnectAsync` and `OnConnectionShutdown` both swap `_connection` and
  `_connectionClosedTcs` under `lock (_lock)`. `OnConnectionShutdown` only nulls
  `_connection` if `ReferenceEquals(sender, _connection)` — protecting against a
  stale shutdown handler from a previous (already-replaced) connection.
- `ConnectionFactory.AutomaticRecoveryEnabled = false` and `TopologyRecoveryEnabled = false`
  — the RabbitMQ client does NOT run its own recovery; `ManagedConnection` is the sole
  recovery path. (This is the key invariant: enabling `AutomaticRecoveryEnabled` would
  create a duplicate recovery race.)
- `BackoffDelayAsync` runs on the loop token, so dispose cancels it cleanly.

**Result**: No race. Single recovery loop, atomic swap, no duplicate paths.

### C4: EventUpgraderRegistry — Immutability — OK

**Concern**: The registry is a singleton resolved from DI and read concurrently from
every consumer dispatch. If the internal dictionaries were mutated after construction,
a concurrent reader could see a partially-built chain or a torn `UpgraderEntry[]`.

**Verification**: Inspected `EventUpgraderRegistry.cs`:
- All four internal dictionaries (`_chains`, `_versionTypes`, `_highestVersions`,
  `_latestTypes`) are `readonly` and populated only in the constructor. No public or
  internal method writes to them after `ctor` returns.
- All public methods (`GetHighestVersion`, `GetLatestType`, `GetTypeForVersion`,
  `Upgrade`) are pure reads. `Upgrade` iterates the immutable `UpgraderEntry[]` chain
  and resolves the upgrader instance from the scoped `IServiceProvider` (per-message
  scope), so no shared mutable state is touched during dispatch.
- The constructor validates that the chain starts at version 1 (`sorted[0].FromVersion == 1`)
  and is continuous (each upgrader's `ToVersion` matches the next one's `FromVersion`).
  Without the v1 check, a chain registered as V2→V3 would silently deserialize V1
  messages as the latest type (default field values → data corruption). The
  constructor throws `InvalidOperationException` if any event's chain does not start
  at v1 or has a gap.

**Result**: Thread-safe by immutability. The chain validation in the constructor
protects against "silent data corruption" bugs that race conditions can produce.

### C5: HandlerTypeRegistry — Immutability — OK

**Concern**: The `HandlerTypeRegistry` is a singleton resolved from DI and read on
every message dispatch. A mutable dictionary would be unsafe under concurrent reads
+ writes.

**Verification**: Inspected `HandlerTypeRegistry.cs`. The internal dictionary mapping
`(EventName, ...)` to handler types is populated in the constructor and never mutated
after. All public lookup methods are pure reads. Same pattern as
`EventUpgraderRegistry` — thread-safety by post-construction immutability.

**Result**: No race. Read-only after `ctor`.

### C6: RabbitMqActivitySource — Static Mutable State — OK in practice (documented limitation)

**Concern**: `RabbitMqActivitySource` is a `static` class with a mutable `Source`
property (replaced in `Initialize`). C# does not guarantee atomicity of static
property reads without `volatile` or a lock, so a reader calling `RabbitMqActivitySource.Source`
*immediately* after `Initialize` could in principle see the old `ActivitySource`.

**Verification**: Inspected `RabbitMqActivitySource.cs`:
- `Initialize(string name)` uses a `lock (Lock)` double-checked pattern:
  ```csharp
  if (IsInitialized) return;
  lock (Lock) { if (IsInitialized) return;
      SourceName = name; Source = new ActivitySource(name, "1.0.0"); IsInitialized = true; }
  ```
- The `Source` property itself is not `volatile`, but the writes happen under a `lock`,
  and the readers (publishers/consumers) only resolve `Source` on the first
  publish/consume — which always happens after `Build()` has returned, i.e., after
  `Initialize` finished and the lock was released. A released `lock` issues a memory
  barrier, so subsequent reads from any thread observe the new `Source`.
- **Note on the static-singleton limitation**: `Initialize` is a no-op on second and
  subsequent calls (the first name wins). This is deliberate — RabbitFlow uses a static
  `ActivitySource`, so only one instrumentation name per process is supported for
  tracing. This affects test and multi-tenant scenarios that register two RabbitFlow
  instances with different instrumentation names in the same process: the second
  `InstrumentationName` is silently ignored for tracing (metrics are per-DI-container
  and not affected). The `RabbitMqActivitySource` XML doc-comment documents this
  limitation. The documentation-only approach (no throw) is the correct trade-off:
  it preserves legitimate multi-tenant test scenarios where multiple
  `IServiceCollection` instances use different instrumentation names in the same
  process.

**Result**: OK in practice — the `ActivitySource` is always resolved after `Build()`,
so the lack of `volatile` on the property is not observable. The static-singleton
limitation is documented (not enforced with a throw) to avoid breaking multi-tenant
test scenarios.

### C7: RabbitConnectionRegistry — StartAll Idempotency — OK

**Concern**: `StartAll` is called by `ConnectionInitializerHostedService` at startup.
If it were called twice (e.g., a misconfigured host or a manual restart), each
`ManagedConnection.Start()` would throw `InvalidOperationException` ("already
started") — but the second `StartAll` call would have already advanced the registry's
`_started` flag, leaving the registry in an inconsistent state where subsequent calls
silently no-op while the inner connections are broken.

**Verification**: Inspected `RabbitConnectionRegistry.cs`:
- `StartAll` opens with
  `if (Interlocked.CompareExchange(ref _started, 1, 0) != 0) throw new InvalidOperationException(...)`.
  This makes `StartAll` strictly idempotent *at the registry level*: the second call
  throws before touching any `ManagedConnection`, so no partial-start state is left
  behind.
- `DisposeAsync` uses `Interlocked.Exchange(ref _disposed, 1)` for the same
  single-execution guarantee.

**Result**: No race. `StartAll` is one-shot at the registry level.

### C8: ManagedConnection.DisposeAsync — No Deadlock — OK

**Concern**: `DisposeAsync` calls `_loopCts.Cancel()` and then
`Task.WhenAny(_loopTask, Task.Delay(2000))`. The `_loopTask` could be blocked on
`await closedTcs.Task` (waiting for a natural shutdown) or on
`await Task.Delay(backoff)`. If the loop never wakes from `closedTcs.Task`, `DisposeAsync`
would hang — unless the cancel propagates into `closedTcs`.

**Verification**: Inspected `ConnectionLoopAsync` and `TryConnectAsync`:
```csharp
var closedTcs = _connectionClosedTcs;
if (closedTcs is not null)
{
    await using var reg = cancellationToken.Register(() => closedTcs.TrySetCanceled())
                                                .ConfigureAwait(false);
    await closedTcs.Task.ConfigureAwait(false);
}
```
The `cancellationToken.Register(() => closedTcs.TrySetCanceled())` callback is the key:
when `DisposeAsync` calls `_loopCts.Cancel()`, the registered callback fires and
`closedTcs.TrySetCanceled()` completes the TCS, so the `await closedTcs.Task` throws
`OperationCanceledException`, which is caught by
`catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { break; }`
— the loop exits cleanly.
- For the `await Task.Delay(backoff, cancellationToken)` case, the cancel propagates
  directly via the token.
- `Task.WhenAny(_loopTask, Task.Delay(2000))` bounds the wait to 2 seconds regardless,
  so even a hypothetical stuck loop wouldn't hang `DisposeAsync` indefinitely.

**Result**: No deadlock. The CTS registration breaks the `closedTcs.Task` wait, the
2-second `Task.WhenAny` is a hard backstop.

### C9: ChannelPool.DisposeAsync — No Deadlock (10s latency) — OK

**Concern**: `ChannelPool.DisposeAsync` does `for (var i = 0; i < MaxSize; i++)
await _semaphore.WaitAsync(cts.Token)`. If a renter never returns its channel (e.g.,
a publisher stuck on a network call), `DisposeAsync` would block forever acquiring all
`MaxSize` permits.

**Verification**: Inspected `ChannelPool.DisposeAsync`:
```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
for (var i = 0; i < MaxSize; i++)
{
    try { await _semaphore.WaitAsync(cts.Token).ConfigureAwait(false); }
    catch (OperationCanceledException)
    {
        _logger.LogWarning("... timed out ({Acquired}/{Total} ...) Forcing closure.", i, MaxSize);
        break;
    }
}
```
The `CancellationTokenSource(TimeSpan.FromSeconds(10))` is the hard backstop. If any
renter hasn't returned within 10 seconds total, the loop breaks and proceeds to close
the idle channels. The worst-case latency is 10 seconds — not a deadlock.
- Rented channels are NOT closed by the pool (the renter still owns them); they're
  left to be GC'd / closed by their own `finally` blocks. This is intentional — the
  pool can't safely close a channel another thread is actively using.

**Result**: No deadlock. Worst case is a 10-second wait followed by a forced shutdown
log and a (potentially) leaked channel that the renter is still using. Acceptable
given the alternative (closing an in-use channel would corrupt the renter).

## Concurrency Invariants

Future contributors MUST preserve these invariants when modifying concurrent code:

1. **`ManagedConnection`**: Only ONE reconnection loop per connection. `AutomaticRecoveryEnabled`
   must remain `false` to avoid duplicate recovery paths. The `lock (_lock)` protects
   `_connection` and `_connectionClosedTcs` swaps.

2. **`ChannelPool`**: `SemaphoreSlim` gates concurrent rentals. `ConcurrentQueue` holds
   idle channels. `Interlocked` protects `_currentCount` and `_disposed`. Never use a
   plain `lock` in the rent/return hot path.

3. **`NamedRabbitConsumer` / `NamedBatchRabbitConsumer`**: ACK/NACK MUST use the concrete
   `IChannel` from the `OnReceived` closure, NOT the `_currentChannel` field. The
   `IChannel` parameter MUST be threaded through all method calls.

4. **`EventUpgraderRegistry` / `HandlerTypeRegistry`**: Immutable after construction.
   Never mutate the internal dictionaries post-ctor. All public methods are read-only.

5. **`RabbitMqActivitySource`**: Static singleton. `Initialize` is a no-op after
   the first call — the first name wins. Subsequent calls with the same name are no-ops;
   calls with a different name are silently ignored for tracing (metrics are per-DI-container
   and not affected). This limitation is documented in the XML doc-comment.

6. **`RabbitConnectionRegistry.StartAll`**: Uses `Interlocked.CompareExchange` for
   idempotency. Can only be called once.

7. **`CompositeEventPublisher`**: `ThrowIfDisposed()` must be called at the start of
   every public publish method.

## Testing Concurrency

Recommendations for stress tests to verify concurrency invariants:

1. **Connection loss under load**: Start a consumer with `PrefetchCount=50`, consume
   for 10 seconds, then kill the RabbitMQ broker. Verify:
   - In-flight messages are requeued by the broker (at-least-once).
   - The consumer reconnects and resumes consuming.
   - No `ObjectDisposedException` or `AlreadyClosedException` propagates to the handler.

2. **Channel pool exhaustion**: Configure a producer with `ChannelPoolSize=2`, then
   publish 100 messages concurrently. Verify backpressure (some publishes await the
   semaphore) and no channel leaks (all channels returned to the pool).

3. **Concurrent dispose**: Start a consumer, then call `DisposeAsync` while messages
   are in-flight. Verify:
   - The `_shutdownCts` cancellation fires only in `DisposeAsync` (NOT on `stoppingToken`
     — in-flight handlers must be allowed to complete during graceful shutdown).
   - The `_shutdownCts` is properly disposed (no `CancellationTokenSource` leak).
   - The `_concurrencyLimiter` (if configured via `MaxConcurrentHandlers > 0`) is disposed.
   - In-flight handlers complete naturally; their ACKs target the correct channel.

4. **Event upgrader chain**: Register a multi-step upgrade chain (V1→V2→V3→V4),
   then consume 1000 V1 messages concurrently. Verify all are upgraded to V4 without
   race conditions on the registry (it's immutable, so this should pass).

5. **Static ActivitySource multi-name**: In a test process, call `AddRabbitMQ` with
   `InstrumentationName="A"`, then call `AddRabbitMQ` on a different
   `IServiceCollection` with `InstrumentationName="B"`. Verify the second call does
   NOT throw — the first name wins for tracing, and `RabbitMqMetrics.MeterName` for
   each DI container reflects its own `InstrumentationName`.
