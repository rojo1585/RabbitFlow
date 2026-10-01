using Microsoft.Extensions.Logging;
using RabbitFlow.Infrastructure.Connection;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitFlow.Infrastructure.Consuming;
/// <summary>
/// Publishes failed messages to the consumer's delay retry queues
/// (<c>{QueueName}.retry.{N}s</c>) with broker-side guarantees.
/// <para>
/// The caller ACKs the original delivery only when <see cref="PublishAsync"/> returns
/// <c>true</c>, so this publish must be reliable — otherwise a message silently dropped by
/// the broker is lost forever. To guarantee that:
/// </para>
/// <list type="bullet">
///   <item>Channels are opened with publisher confirms + confirmation tracking, so
///   <c>BasicPublishAsync</c> completes only after the broker ACKs the message and throws
///   <see cref="PublishException"/> on a NACK.</item>
///   <item><c>mandatory: true</c> — if the retry queue does not exist (e.g.
///   <c>AutoDeclareTopology=false</c> and ops did not create it) the broker returns the
///   message and the publish fails, instead of being silently discarded.</item>
///   <item>The confirm wait is bounded by <see cref="ConfirmTimeout"/>.</item>
/// </list>
/// Channels are pooled (via <see cref="ChannelPool"/>) instead of opened/closed per retry.
/// Shared by <see cref="NamedRabbitConsumer"/> and <see cref="NamedBatchRabbitConsumer"/>.
/// </summary>
internal sealed class RetryQueuePublisher : IAsyncDisposable
{
    /// <summary>
    /// Maximum time to wait for a channel plus the broker confirm of a retry publish.
    /// </summary>
    internal static readonly TimeSpan ConfirmTimeout = TimeSpan.FromSeconds(30);

    private const int MaxPoolSize = 8;

    private static readonly CreateChannelOptions ConfirmChannelOptions = new(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);

    private readonly string _consumerKey;
    private readonly string _queueName;
    private readonly ILogger _logger;
    private readonly ChannelPool _channelPool;

    /// <param name="consumerKey">The owning consumer's key (used for logging).</param>
    /// <param name="queueName">The consumer's main queue name; retry queues derive from it.</param>
    /// <param name="connection">The connection used to open confirm channels.</param>
    /// <param name="maxConcurrency">
    /// The consumer's handler concurrency. The pool is sized to it (clamped to 1..8) so
    /// concurrent failing handlers do not serialize on a single retry channel.
    /// </param>
    /// <param name="logger">The owning consumer's logger.</param>
    public RetryQueuePublisher(string consumerKey, string queueName, ManagedConnection connection, int maxConcurrency, ILogger logger)
    {
        _consumerKey = consumerKey;
        _queueName = queueName;
        _logger = logger;
        _channelPool = new ChannelPool(connection, ConfirmChannelOptions, Math.Clamp(maxConcurrency, 1, MaxPoolSize), logger);
    }

    /// <summary>
    /// Returns the retry queue name for the given delay: <c>{QueueName}.retry.{N}s</c>.
    /// Must match the names declared by <c>TopologyDeclarator.DeclareRetryQueuesAsync</c>.
    /// </summary>
    public string GetRetryQueueName(TimeSpan delay) => $"{_queueName}.retry.{(int)delay.TotalSeconds}s";

    /// <summary>
    /// Publishes <paramref name="body"/> to the retry queue for <paramref name="delay"/> and
    /// waits for the broker confirm.
    /// </summary>
    /// <returns>
    /// <c>true</c> only if the broker confirmed the message; <c>false</c> if it was nacked,
    /// returned as unroutable, timed out, or the channel/connection was unavailable. On
    /// <c>false</c> the caller must NOT ack the original delivery.
    /// </returns>
    public async Task<bool> PublishAsync(ReadOnlyMemory<byte> body, BasicProperties properties, TimeSpan delay)
    {
        var retryQueueName = GetRetryQueueName(delay);

        using var timeoutCts = new CancellationTokenSource(ConfirmTimeout);

        IChannel channel;
        try
        {
            channel = await _channelPool.RentAsync(timeoutCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Consumer:{Key}] Failed to obtain a channel to publish to retry queue '{RetryQueue}'", _consumerKey, retryQueueName);
            return false;
        }

        var reusable = false;
        try
        {
            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: retryQueueName,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: timeoutCts.Token).ConfigureAwait(false);

            reusable = true;
            _logger.LogInformation("[Consumer:{Key}] Published to retry queue '{RetryQueue}' (delay={DelayMs}ms, confirmed)", _consumerKey, retryQueueName, (int)delay.TotalMilliseconds);
            return true;
        }
        catch (PublishException ex) when (ex.IsReturn)
        {
            // basic.return leaves the channel healthy.
            reusable = true;
            _logger.LogError(ex, "[Consumer:{Key}] Retry queue '{RetryQueue}' does not exist (message returned as unroutable). Ensure it is declared (AutoDeclareTopology=true) or create it externally.", _consumerKey, retryQueueName);
            return false;
        }
        catch (PublishException ex)
        {
            reusable = true;
            _logger.LogError(ex, "[Consumer:{Key}] Broker NACKed the publish to retry queue '{RetryQueue}'", _consumerKey, retryQueueName);
            return false;
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            _logger.LogError("[Consumer:{Key}] Timed out after {Timeout}s waiting for the broker confirm of the publish to retry queue '{RetryQueue}'", _consumerKey, ConfirmTimeout.TotalSeconds, retryQueueName);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Consumer:{Key}] Failed to publish to retry queue '{RetryQueue}'", _consumerKey, retryQueueName);
            return false;
        }
        finally
        {
            if (reusable)
                _channelPool.Return(channel);
            else
                _channelPool.Discard(channel);
        }
    }

    public ValueTask DisposeAsync() => _channelPool.DisposeAsync();
}