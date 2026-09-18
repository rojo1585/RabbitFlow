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
    private readonly object _lock = new();
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
    public async Task<IChannel> CreateChannelAsync(CreateChannelOptions? channelOptions = null,  TimeSpan? timeout = null, CancellationToken cancellationToken = default)
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
                        var channel = await conn.CreateChannelAsync(channelOptions, combinedCts.Token).ConfigureAwait(false);
                        _logger.LogDebug("[{Name}] Channel #{ChannelNumber} created", Name, channel.ChannelNumber);
                        return channel;
                    }
                    catch (AlreadyClosedException)
                    {
                        continue;
                    }
                }

                await Task.Delay(200, combinedCts.Token).ConfigureAwait(false);
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
        _logger.LogInformation("[{Name}] Connection loop started → {Host}:{Port}/{VHost}", Name, _options.HostName, _options.Port, _options.VirtualHost);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await TryConnectAsync(cancellationToken).ConfigureAwait(false);

                var closedTcs = _connectionClosedTcs;
                if (closedTcs is not null)
                {
                    await using var reg = cancellationToken.Register(() => closedTcs.TrySetCanceled()).ConfigureAwait(false);
                    await closedTcs.Task.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Name}] Connection error", Name);
                SafeCloseConnection();
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                await BackoffDelayAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        _logger.LogInformation("[{Name}] Connection loop stopped", Name);
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

        if (_options.Tls is { Enabled: true })
        {
            factory.Ssl = BuildSslOptions(_options.Tls);
            _logger.LogInformation("[{Name}] TLS enabled → ServerName={ServerName}, mTLS={Mtls}, Protocol={Protocol}", Name, _options.Tls.ServerName ?? _options.HostName, _options.Tls.CertPath is not null, _options.Tls.Protocol);
        }

        _logger.LogInformation("[{Name}] Connecting to {Host}:{Port}{Tls}...", Name, _options.HostName, _options.Port, _options.Tls is { Enabled: true } ? " (TLS)" : "");

        var connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);

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
            await connection.CloseAsync().ConfigureAwait(false);
            return;
        }

        connection.ConnectionShutdownAsync += OnConnectionShutdown;

        _logger.LogInformation("[{Name}] Connected to {Host}:{Port}/{VHost} (local port {LocalPort})", Name, _options.HostName, _options.Port, _options.VirtualHost, connection.LocalPort);
    }

    /// <summary>
    /// Handles connection shutdown events. Completes the TCS so that
    /// <see cref="ConnectionLoopAsync"/> wakes up and reconnects.
    /// </summary>
    private Task OnConnectionShutdown(object sender, ShutdownEventArgs args)
    {
        if (sender is IConnection conn)
            conn.ConnectionShutdownAsync -= OnConnectionShutdown;

        lock (_lock)
        {
            if (ReferenceEquals(sender, _connection))
                _connection = null;

        }
        _connectionClosedTcs?.TrySetResult(true);

        if (args.Initiator == ShutdownInitiator.Application)
            _logger.LogInformation("[{Name}] Connection closed by application", Name);
        else
            _logger.LogWarning("[{Name}] Connection lost: {Reason} (initiator: {Initiator}), will reconnect...", Name, args.ReplyText, args.Initiator);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Applies exponential backoff: 1s → 2s → 4s → 8s → 16s → 32s → 60s (cap).
    /// </summary>
    private async Task BackoffDelayAsync(CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromSeconds(_backoffSeconds);

        _logger.LogInformation("[{Name}] Reconnecting in {Delay}s...", Name, delay.TotalSeconds);

        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

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
        catch { }

        try
        {
            _ = connToClose.CloseAsync();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[{Name}] Error closing connection (ignoring)", Name);
        }
    }

    /// <summary>
    /// Builds <see cref="SslOption"/> from <see cref="TlsOptions"/>.
    /// Called only when TLS is enabled.
    /// </summary>
    private static SslOption BuildSslOptions(TlsOptions tls)
    {
        var ssl = new SslOption
        {
            Enabled = true,
            ServerName = tls.ServerName ?? string.Empty,
            CertPath = tls.CertPath ?? string.Empty,
            CertPassphrase = tls.CertPassphrase,
            Version = tls.Protocol,
        };

        if (tls.AllowUnknownCAs || tls.DisableCertificateRevocationCheck)
        {
            ssl.CertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) =>
            {
                if (tls.AllowUnknownCAs && sslPolicyErrors == System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors)
                    return true;

                return sslPolicyErrors == System.Net.Security.SslPolicyErrors.None;
            };
        }

        return ssl;
    }

    /// <summary>
    /// Disposes the managed connection. Cancels the reconnection loop
    /// and closes the AMQP connection cleanly.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _logger.LogInformation("[{Name}] Disposing...", Name);

        if (_loopCts is not null)
        {
            _loopCts.Cancel();

            if (_loopTask is not null)
            {
                try
                {
                    await Task.WhenAny(_loopTask, Task.Delay(2000)).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "[{Name}] Error waiting for loop to stop", Name);
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
                catch { }
            }
        }

        if (connToClose is not null)
        {
            try
            {
                await connToClose.CloseAsync().ConfigureAwait(false);
                _logger.LogInformation("[{Name}] Connection closed (disposed)", Name);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "[{Name}] Error closing connection during dispose", Name);
            }
        }
    }
}