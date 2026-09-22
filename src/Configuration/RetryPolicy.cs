namespace RabbitFlow.Configuration;



/// <summary>
/// Defines retry behavior for a consumer.
/// Built from <see cref="RabbitConsumerOptions"/> retry settings.
/// </summary>
/// <remarks>
/// Creates a retry policy from consumer options. The policy is immutable after
/// construction; <see cref="Delays"/> is exposed as <see cref="IReadOnlyList{T}"/>
/// to prevent callers from mutating the underlying array.
/// </remarks>
public sealed class RetryPolicy(RabbitConsumerOptions options)
{
    /// <summary>
    /// Maximum number of delivery attempts before the message is dead-lettered.
    /// The first delivery counts as attempt 1. Defaults to 3, meaning a message
    /// is delivered at most 3 times before being routed to the DLQ.
    /// </summary>
    public int MaxRetries { get; } = options.MaxRetries;

    /// <summary>
    /// Delay durations between retry attempts, exposed as an immutable list.
    /// Index 0 = delay before retry attempt 2.
    /// Index 1 = delay before retry attempt 3. Etc.
    /// </summary>
    /// <remarks>
    /// The array length should be at least <see cref="MaxRetries"/> - 1 so that
    /// every retry attempt has a corresponding delay.
    /// </remarks>
    public IReadOnlyList<TimeSpan> Delays { get; } = options.ResolvedRetryDelays;

    /// <summary>
    /// Determines whether a message with the given delivery count should be retried
    /// or dead-lettered.
    /// </summary>
    /// <param name="deliveryCount">
    /// Number of times the message has been delivered (1 = first attempt).
    /// </param>
    /// <returns>
    /// <c>true</c> if the message should be retried (i.e., it has not yet reached
    /// <see cref="MaxRetries"/> delivery attempts); <c>false</c> if it should be
    /// dead-lettered.
    /// </returns>
    /// <remarks>
    /// With <see cref="MaxRetries"/> = 3 (default), the message is retried while
    /// <c>deliveryCount</c> is 1 or 2, and dead-lettered on the third failure:
    /// <list type="bullet">
    ///   <item><c>ShouldRetry(1)</c> → <c>true</c> (retry to attempt 2)</item>
    ///   <item><c>ShouldRetry(2)</c> → <c>true</c> (retry to attempt 3)</item>
    ///   <item><c>ShouldRetry(3)</c> → <c>false</c> (dead-letter)</item>
    /// </list>
    /// This honors the documented contract that <see cref="MaxRetries"/> is the
    /// maximum number of delivery attempts. The previous implementation used
    /// <c>&lt;=</c>, which silently delivered one extra attempt.
    /// </remarks>
    public bool ShouldRetry(int deliveryCount)
    {
        return deliveryCount < MaxRetries;
    }

    /// <summary>
    /// Gets the TTL for the retry queue corresponding to the given delivery count.
    /// Used by the consumer to select the appropriate retry queue when a message
    /// is eligible for retry (i.e., <see cref="ShouldRetry"/> returned <c>true</c>).
    /// </summary>
    /// <param name="deliveryCount">
    /// The current delivery count (1-based). For the first failure (deliveryCount=1),
    /// this returns the delay before retry attempt 2.
    /// </param>
    /// <returns>
    /// The time-to-live for the retry queue the message should be published to, or
    /// <c>null</c> if <paramref name="deliveryCount"/> is out of range (the caller
    /// should dead-letter the message in that case).
    /// </returns>
    public TimeSpan? GetRetryDelay(int deliveryCount)
    {
        var delayIndex = deliveryCount - 1;
        if (delayIndex < 0 || delayIndex >= Delays.Count)
            return null;

        return Delays[delayIndex];
    }
}