using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Collections.Concurrent;

namespace RabbitFlow.Infrastructure.Connection;

/// <summary>
/// A pool of reusable AMQP channels that eliminates the per-publish channel
/// creation overhead — the #1 throughput bottleneck in channel-per-publish strategies.
///
/// <para>
/// Architecture:
/// <list type="bullet">
///   <item>
///     <b>Rent/Return pattern</b>: Callers rent a channel, use it, then return it.
///     Channels are reused across publishes instead of created and destroyed each time.
///   </item>
///   <item>
///     <b>Semaphore-gated concurrency</b>: A <see cref="SemaphoreSlim"/> limits
///     concurrent rentals to <see cref="MaxSize"/>. When all channels are in use,
///     callers await the semaphore — providing natural backpressure.
///   </item>
///   <item>
///     <b>Lazy creation</b>: Channels are created on first rental, not at startup.
///     This avoids requiring the connection to be ready at construction time.
///   </item>
///   <item>
///     <b>Health-aware return</b>: <see cref="Return"/> checks <c>IsOpen</c>.
///     Closed channels are discarded. <see cref="Discard"/> forces disposal
///     for channels in a dirty state (e.g. after publisher confirm timeout).
///   </item>
///   <item>
///     <b>Connection resilience</b>: When the underlying <see cref="ManagedConnection"/>
///     drops, all pooled channels become invalid. On next rental, they're discarded
///     and fresh channels are created on the reconnected connection.
///   </item>
/// </list>
/// </para>
///
/// <para>
/// Thread safety: All public methods are safe to call concurrently.
/// </para>
///
/// <para>
/// Typical configuration:
/// <list type="bullet">
///   <item>Pool size 4–8 for publisher-confirms channels (good for 40k–200k msg/s).</item>
///   <item>Pool size 2–4 for fire-and-forget channels (higher per-channel throughput).</item>
///   <item>Pool size 0 disables pooling — reverts to channel-per-publish (backward compat).</item>
/// </list>
/// </para>
/// </summary>
internal sealed class ChannelPool : IAsyncDisposable
{
    private readonly ManagedConnection _connection;
    private readonly CreateChannelOptions? _channelOptions;
    private readonly ILogger _logger;

    /// <summary>
    /// Optional callback invoked when a new channel is created by the pool.
    /// Used for metrics (<c>rabbitmq.channel_pool.created</c>).
    /// </summary>
    private readonly Action? _onChannelCreated;

    /// <summary>
    /// Available (idle) channels ready to be rented.
    /// </summary>
    private readonly ConcurrentQueue<IChannel> _available = new();

    /// <summary>
    /// Limits concurrent rentals to <see cref="MaxSize"/>.
    /// Each rental acquires one permit; each return releases it.
    /// </summary>
    private readonly SemaphoreSlim _semaphore;

    /// <summary>
    /// Tracks the total number of channels created (available + rented).
    /// Used for diagnostics and logging only — not for control flow.
    /// </summary>
    private int _currentCount;
    
    //   _disposed: 0 = live, 1 = disposed
    private int _disposed;

    /// <summary>
    /// Returns true if this pool has been disposed.
    /// Thread-safe: reads the _disposed int with Volatile.Read.
    /// </summary>
    private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    /// <summary>
    /// The maximum number of channels this pool will manage concurrently.
    /// </summary>
    public int MaxSize { get; }

    /// <summary>
    /// Creates a new channel pool.
    /// </summary>
    /// <param name="connection">The managed connection to create channels on.</param>
    /// <param name="channelOptions">
    /// Channel creation options (e.g. publisher confirms configuration).
    /// Passed through to <see cref="ManagedConnection.CreateChannelAsync"/>.
    /// </param>
    /// <param name="maxSize">
    /// Maximum number of concurrent channel rentals.
    /// When all channels are rented, callers await the semaphore.
    /// </param>
    /// <param name="logger">Logger for pool events.</param>
    /// <param name="onChannelCreated">
    /// Optional callback invoked when a new channel is created.
    /// Use this to increment metrics counters.
    /// </param>
    public ChannelPool(ManagedConnection connection, CreateChannelOptions? channelOptions, int maxSize, ILogger logger, Action? onChannelCreated = null)
    {
        if (maxSize < 1)
            throw new ArgumentOutOfRangeException(nameof(maxSize), "Pool size must be >= 1.");

        _connection = connection;
        _channelOptions = channelOptions;
        MaxSize = maxSize;
        _logger = logger;
        _onChannelCreated = onChannelCreated;
        _semaphore = new SemaphoreSlim(maxSize, maxSize);
    }

    /// <summary>
    /// Rents a healthy channel from the pool.
    /// <para>
    /// If an idle channel is available and open, it's returned immediately.
    /// If idle channels are closed (e.g. after connection loss), they're discarded
    /// and a fresh channel is created.
    /// If no idle channels are available, a new channel is created.
    /// If the pool is at capacity (<see cref="MaxSize"/> rentals active),
    /// the caller awaits until a channel is returned.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A healthy, open <see cref="IChannel"/> ready for use.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the pool has been disposed.</exception>
    public async Task<IChannel> RentAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, nameof(ChannelPool));

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            while (_available.TryDequeue(out var channel))
            {
                if (channel.IsOpen)
                {
                    _logger.LogDebug("[ChannelPool] Reused channel #{Number} from pool (available={Available}, total={Total})", channel.ChannelNumber, _available.Count, Volatile.Read(ref _currentCount));
                    return channel;
                }

                Interlocked.Decrement(ref _currentCount);
                await SafeCloseAsync(channel).ConfigureAwait(false);
            }

            var newChannel = await _connection.CreateChannelAsync(_channelOptions, cancellationToken: cancellationToken).ConfigureAwait(false);

            Interlocked.Increment(ref _currentCount);
            _onChannelCreated?.Invoke();

            _logger.LogDebug("[ChannelPool] Created new channel #{Number} (available=0, total={Total})", newChannel.ChannelNumber, Volatile.Read(ref _currentCount));

            return newChannel;
        }
        catch
        {
            if (!IsDisposed)
            {
                try { _semaphore.Release(); }
                catch (ObjectDisposedException) { }
            }
            throw;
        }
    }

    /// <summary>
    /// Returns a channel to the pool for reuse.
    ///
    /// <para>
    /// The channel is returned to the idle queue only if it's still open.
    /// Closed channels are discarded (decrementing the count).
    /// The semaphore is always released so the next waiting rental can proceed.
    /// </para>
    /// </summary>
    /// <param name="channel">The channel to return.</param>
    public void Return(IChannel channel)
    {
        if (IsDisposed || !channel.IsOpen)
        {
            Interlocked.Decrement(ref _currentCount);
            _ = SafeCloseAsync(channel);
        }
        else
        {
            _available.Enqueue(channel);

            _logger.LogDebug("[ChannelPool] Returned channel #{Number} to pool (available={Available}, total={Total})", channel.ChannelNumber, _available.Count, Volatile.Read(ref _currentCount));
        }

        try { _semaphore.Release(); }
        catch (ObjectDisposedException) { }
    }

    /// <summary>
    /// Discards a channel that's in a dirty or faulted state.
    ///
    /// <para>
    /// Use this instead of <see cref="Return"/> when the channel's internal
    /// state is compromised and must not be reused — for example, after a
    /// <see cref="Exceptions.PublisherConfirmTimeoutException"/> where unconfirmed
    /// messages leave the channel's confirm sequence in an inconsistent state.
    /// </para>
    /// </summary>
    /// <param name="channel">The channel to discard.</param>
    public void Discard(IChannel channel)
    {
        Interlocked.Decrement(ref _currentCount);
        _ = SafeCloseAsync(channel);

        _logger.LogDebug("[ChannelPool] Discarded channel #{Number} (available={Available}, total={Total})", channel.ChannelNumber, _available.Count, Volatile.Read(ref _currentCount));

        if (!IsDisposed)
        {
            try { _semaphore.Release(); }
            catch (ObjectDisposedException) { }
        }
    }

    /// <summary>
    /// Gets the current number of idle channels in the pool.
    /// For diagnostics/metrics only.
    /// </summary>
    public int AvailableCount => _available.Count;

    /// <summary>
    /// Gets the total number of channels (idle + rented).
    /// For diagnostics/metrics only.
    /// </summary>
    public int TotalCount => Volatile.Read(ref _currentCount);

    /// <summary>
    /// Safely closes a channel, ignoring errors if already closed or disposed.
    /// </summary>
    private async Task SafeCloseAsync(IChannel channel)
    {
        try
        {
            if (channel.IsOpen)
                await channel.CloseAsync().ConfigureAwait(false);

            channel.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[ChannelPool] Error closing channel.");
        }
    }

    /// <summary>
    /// Disposes the pool. Closes all idle channels.
    /// Rented channels are NOT closed — their callers are responsible for returning or discarding them.
    /// </summary>
    /// <remarks>
    /// Thread-safe: uses <see cref="Interlocked.Exchange"/> to ensure that only one thread
    /// executes the dispose body, matching the pattern in <see cref="ManagedConnection.DisposeAsync"/>.
    /// Without this guard, two concurrent calls would both pass the _disposed check, both set
    /// _disposed = true, and both execute the body — double-disposing the SemaphoreSlim and
    /// throwing ObjectDisposedException.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _logger.LogInformation("[ChannelPool] Disposing. Waiting for in-flight channels to return...");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        for (var i = 0; i < MaxSize; i++)
        {
            try
            {
                await _semaphore.WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("[ChannelPool] Graceful shutdown timed out ({Acquired}/{Total} channels returned). Forcing closure.", i, MaxSize);
                break;
            }
        }
        _logger.LogInformation("[ChannelPool] Disposing (closing {Count} idle channels)...", _available.Count);

        while (_available.TryDequeue(out var channel))
        {
            await SafeCloseAsync(channel).ConfigureAwait(false);
        }

        _semaphore.Dispose();

        _logger.LogInformation("[ChannelPool] Disposed.");
    }
}




