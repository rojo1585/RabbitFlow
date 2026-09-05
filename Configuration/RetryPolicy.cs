using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitFlow.Configuration
{

    /// <summary>
    /// Defines retry behavior for a consumer.
    /// Built from <see cref="RabbitConsumerOptions"/> retry settings.
    /// </summary>
    /// <remarks>
    /// Creates a retry policy from consumer options.
    /// </remarks>

    /// <summary>
    /// Defines retry behavior for a consumer.
    /// Built from <see cref="RabbitConsumerOptions"/> retry settings.
    /// </summary>
    /// <remarks>
    /// Creates a retry policy from consumer options.
    /// </remarks>
    public sealed class RetryPolicy(RabbitConsumerOptions options)
    {

        /// <summary>
        /// Maximum number of delivery attempts before dead-lettering.
        /// </summary>
        public int MaxRetries { get; } = options.MaxRetries;

        /// <summary>
        /// Delay durations between retry attempts.
        /// Index 0 = delay before retry attempt 2.
        /// Index 1 = delay before retry attempt 3. Etc.
        /// </summary>
        public TimeSpan[] Delays { get; } = options.ResolvedRetryDelays;

        /// <summary>
        /// Determines whether a message with the given delivery count should be retried.
        /// </summary>
        /// <param name="deliveryCount">
        /// Number of times the message has been delivered (1 = first attempt).
        /// </param>
        /// <returns>
        /// <c>true</c> if the message should be retried; <c>false</c> if it should be dead-lettered.
        /// </returns>
        public bool ShouldRetry(int deliveryCount)
        {
            return deliveryCount <= MaxRetries;
        }

        /// <summary>
        /// Gets the TTL for the retry queue corresponding to the given delivery count.
        /// </summary>
        /// <param name="deliveryCount">
        /// The current delivery count (1-based). For the first failure (deliveryCount=1),
        /// this returns the delay before retry attempt 2.
        /// </param>
        /// <returns>
        /// The time-to-live for the retry queue, or null if no more retries are available.
        /// </returns>
        public TimeSpan? GetRetryDelay(int deliveryCount)
        {
            // deliveryCount=1 (first failure) → Delays[0] (delay before attempt 2)
            // deliveryCount=2 (second failure) → Delays[1] (delay before attempt 3)
            var delayIndex = deliveryCount - 1;
            if (delayIndex < 0 || delayIndex >= Delays.Length)
                return null;

            return Delays[delayIndex];
        }
    }
}
