using Microsoft.Extensions.Logging;
using RabbitFlow.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitFlow.Infrastructure.Connection;

/// <summary>
/// Manages a single named RabbitMQ connection with automatic reconnection
/// and exponential backoff.
/// 
/// <para>
/// Lifecycle:
/// <list type="number">
///   <item><see cref="Start"/> is called by the registry at startup.</item>
///   <item>A background loop connects with backoff: 1s → 2s → 4s → 8s → 16s → 32s → 60s (cap).</item>
///   <item>On connection loss, automatically reconnects with the same backoff.</item>
///   <item><see cref="DisposeAsync"/> cancels the loop and closes the connection cleanly.</item>
/// </list>
/// </para>
/// 
/// <para>
/// Thread safety: <see cref="IsConnected"/> and <see cref="CreateChannelAsync"/>
/// are safe to call from multiple threads.
/// </para>
/// </summary>
/// <remarks>
/// Creates a new managed connection. Does NOT connect yet — call <see cref="Start"/>.
/// </remarks>
/// <param name="_name">
/// The connection name (typically the dictionary key from configuration).
/// This overrides <see cref="RabbitConnectionOptions.Name"/>.
/// </param>
public sealed class ManagedConnection(string _name, RabbitConnectionOptions _options, ILogger<ManagedConnection> _logger) : IAsyncDisposable
{
    private readonly Lock _lock = new();

    private IConnection? _connection;
    private TaskCompletionSource<bool>? _connectionClosedTcs;
    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;
    private int _backoffSeconds = 1;
    private volatile bool _disposed;

    /// <summary>
    /// The connection name from <see cref="RabbitConnectionOptions.Name"/>.
    /// </summary>
    public string Name { get; } = _name;

    /// <summary>
    /// Whether the connection is currently open and operational.
    /// Thread-safe: reads the volatile _connection field.
    /// </summary>
    public bool IsConnected
    {
        get
        {
            var conn = _connection;
            return conn is not null && conn.IsOpen;
        }
    }

    /// <summary>
    /// Starts the background connection loop. Returns immediately.
    /// The connection is established asynchronously.
    /// </summary>
    /// <exception cref="ObjectDisposedException">Thrown if this instance has been disposed.</exception>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ManagedConnection));

        _loopCts = new CancellationTokenSource();
        _loopTask = Task.Run(() => ConnectionLoopAsync(_loopCts.Token));
    }

    /// <summary>
    /// Creates a new AMQP channel on the current connection.
    /// The caller owns the channel and is responsible for disposing it.
    /// 
    /// <para>
    /// If the connection is not open, this waits up to <paramref name="timeout"/>
    /// for the connection to become available.
    /// </para>
    /// 
    /// <para>
    /// The <paramref name="channelOptions"/> parameter is passed through to
    /// <c>IConnection.CreateChannelAsync</c>. Use it to enable publisher confirms
    /// via <c>CreateChannelOptions(publisherConfirmationsEnabled: true, ...).</c>
    /// </para>
    /// </summary>
    /// <param name="channelOptions">
    /// Optional channel creation options (e.g. publisher confirms configuration).
    /// When <c>null</c>, the broker defaults are used (no confirms).
    /// </param>
    /// <returns>A new <see cref="IChannel"/> on the current connection.</returns>
    /// <exception cref="TimeoutException">
    /// Thrown if no connection is available within the timeout.
    /// </exception>
    public async Task<IChannel> CreateChannelAsync(CreateChannelOptions? channelOptions = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ManagedConnection));

        var effectiveTimeout = timeout ?? TimeSpan.FromSeconds(30);
        using var timeoutCts = new CancellationTokenSource(effectiveTimeout);
        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, _loopCts?.Token ?? CancellationToken.None, cancellationToken);

        try
        {
            while (true)
            {
                combinedCts.Token.ThrowIfCancellationRequested();

                var conn = _connection;
                if (conn is not null && conn.IsOpen)
                {
                    try
                    {
                        var channel = await conn.CreateChannelAsync(channelOptions, combinedCts.Token);
                        _logger.LogDebug($"[{Name}] Channel #{channel.ChannelNumber} created");
                        return channel;
                    }
                    catch (AlreadyClosedException)
                    {
                        continue;
                    }
                }

                // Connection not ready — wait briefly and retry
                await Task.Delay(200, combinedCts.Token);
            }
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            throw new TimeoutException($"[{Name}] No connection available within {effectiveTimeout.TotalSeconds}s.");
        }
    }

    /// <summary>
    /// The main connection loop. Runs on a background thread.
    /// Connects, waits for the connection to close naturally, then reconnects.
    /// </summary>
    private async Task ConnectionLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation($"[{Name}] Connection loop started → {_options.HostName}:{_options.Port}/{_options.VirtualHost}");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await TryConnectAsync(cancellationToken);

                var closedTcs = _connectionClosedTcs;
                if (closedTcs is not null)
                {
                    await using var reg = cancellationToken.Register(
                        () => closedTcs.TrySetCanceled());
                    await closedTcs.Task;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[{Name}] Connection error");
                SafeCloseConnection();
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                await BackoffDelayAsync(cancellationToken);
            }
        }

        _logger.LogInformation($"[{Name}] Connection loop stopped");
    }

    /// <summary>
    /// Attempts to establish a connection with the configured parameters.
    /// Sets up the shutdown handler and a TCS that signals when the connection closes.
    /// </summary>
    private async Task TryConnectAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            RequestedHeartbeat = TimeSpan.FromSeconds(_options.RequestedHeartbeatSeconds),
            ContinuationTimeout = TimeSpan.FromSeconds(_options.ConnectionTimeoutSeconds),
        };

        _logger.LogInformation($"[{Name}] Connecting to {_options.HostName}:{_options.Port}...", _options.HostName, _options.Port);

        var connection = await factory.CreateConnectionAsync(cancellationToken);

        var closedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        bool wasDisposed;
        lock (_lock)
        {
            wasDisposed = _disposed;
            if (!wasDisposed)
            {
                _connection = connection;
                _connectionClosedTcs = closedTcs;
                _backoffSeconds = 1;
            }
        }

        if (wasDisposed)
        {
            await connection.CloseAsync(cancellationToken);
            return;
        }

        connection.ConnectionShutdownAsync += OnConnectionShutdown;

        _logger.LogInformation($"[{Name}] Connected to {_options.HostName}:{_options.Port}/{_options.VirtualHost} (local port {connection.LocalPort})");
    }

    /// <summary>
    /// Handles connection shutdown events. Completes the TCS so that
    /// <see cref="ConnectionLoopAsync"/> wakes up and reconnects.
    /// </summary>
    private Task OnConnectionShutdown(object sender, ShutdownEventArgs args)
    {
        // Unsubscribe from this connection's events
        if (sender is IConnection conn)
            conn.ConnectionShutdownAsync -= OnConnectionShutdown;

        lock (_lock)
            if (ReferenceEquals(sender, _connection))
                _connection = null;

        // Signal the loop to wake up
        _connectionClosedTcs?.TrySetResult(true);

        if (args.Initiator == ShutdownInitiator.Application)
            _logger.LogInformation($"[{Name}] Connection closed by application");

        else
            _logger.LogWarning($"[{Name}] Connection lost: {args.ReplyText} (initiator: {args.Initiator}), will reconnect...", args.ReplyText, args.Initiator);


        return Task.CompletedTask;
    }

    /// <summary>
    /// Applies exponential backoff: 1s → 2s → 4s → 8s → 16s → 32s → 60s (cap).
    /// </summary>
    private async Task BackoffDelayAsync(CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromSeconds(_backoffSeconds);

        _logger.LogInformation($"[{Name}] Reconnecting in {delay.TotalSeconds}s...");
        await Task.Delay(delay, cancellationToken);

        // Double the backoff, capped at MaxBackoffSeconds
        _backoffSeconds = Math.Min(_backoffSeconds * 2, _options.MaxBackoffSeconds);
    }

    /// <summary>
    /// Safely closes and nulls the existing connection without throwing.
    /// </summary>
    private void SafeCloseConnection()
    {
        IConnection? connToClose;

        lock (_lock)
        {
            connToClose = _connection;
            _connection = null;
        }

        if (connToClose is null) return;

        try
        {
            connToClose.ConnectionShutdownAsync -= OnConnectionShutdown;
        }
        catch { /* ignore */ }

        try
        {
            _ = connToClose.CloseAsync();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, $"[{Name}] Error closing connection (ignoring)");
        }
    }

    /// <summary>
    /// Disposes the managed connection. Cancels the reconnection loop
    /// and closes the AMQP connection cleanly.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _logger.LogInformation($"[{Name}] Disposing...");

        // Cancel the connection loop
        if (_loopCts is not null)
        {
            _loopCts.Cancel();

            if (_loopTask is not null)
            {
                try
                {
                    // Give the loop a moment to finish gracefully
                    await Task.WhenAny(_loopTask, Task.Delay(2000));
                }
                catch (OperationCanceledException) { /* expected */ }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, $"[{Name}] Error waiting for loop to stop");
                }
            }

            _loopCts.Dispose();
        }

        // Close the AMQP connection
        IConnection? connToClose;
        lock (_lock)
        {
            connToClose = _connection;
            _connection = null;

            if (connToClose is not null)
            {
                try
                {
                    connToClose.ConnectionShutdownAsync -= OnConnectionShutdown;
                }
                catch { /* ignore */ }
            }
        }

        if (connToClose is not null)
        {
            try
            {
                await connToClose.CloseAsync();
                _logger.LogInformation($"[{Name}] Connection closed (disposed)");
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, $"[{Name}] Error closing connection during dispose");
            }
        }
    }
}