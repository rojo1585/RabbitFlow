using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedRabbit.Infrastructure.Consuming;
/// <summary>
/// Result of <see cref="HandlerExecution.WaitAsync"/>
/// </summary>
internal enum HandlerOutcome
{
    ///<summary>
    ///The handler finished before shutdown. The caller still owns the handlers resources and must release them.
    ///</summary>
    Completed,
    /// <summary>
    /// Shutdown fired first and the handler was left running in the background. ownership of the handlers resources was transferred
    /// they are released when the handler actually finishes. Note: the caller not release them.
    /// </summary>
    Abandoned,
}
/// <summary>
/// WAits for a message handler while honouring  the consumers shutdown signal, without releasing the handlers underneath it
/// </summary>
/// <para>
/// When shutdown drain timeout expires, consumers stop waiting for slow handlerss and NACK their messages with requeue.
/// A non cooperative handler keeps running in the background though, and any service it uses such as a <c>DbContext</c> 
/// must stay alive until it really finishes.
/// Releasing it earlier makes the handler fail with an <c>ObjectDisposedException</c>.
/// </para>"
internal static class HandlerExecution
{
    /// <summary>
    /// Waits for <paramref name="handlerTask"/> unless <paramref name="shutdownToken"/> fires first.
    /// </summary>
    /// <param name="handlerTask"> The running handler task</param>
    /// <param name="handlerResources">Resource that must outlive the handler. release by this method
    /// only when the outcome is <see cref="HandlerOutcome.Abandoned"/></param>
    /// <param name="shutdownToken">Signals that the consumer is shutting down</param>
    /// <param name="onAbandonedHandlerFinished">Invoked when an abandoned handler finishes</param>
    /// <param name="logger">Logger for recording events</param>
    /// <returns>The outcome of the handler execution</returns>
    public static async Task<HandlerOutcome> WaitAsync(Task handlerTask,
                                                       IAsyncDisposable handlerResources,
                                                       Action<Exception?> onAbandonedHandlerFinished,
                                                       ILogger logger,
                                                       CancellationToken shutdownToken)
    {
        if (!handlerTask.IsCompleted && shutdownToken.CanBeCanceled)
        {
            var shutdownTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = shutdownToken.Register(static state => ((TaskCompletionSource)state!).TrySetResult(), shutdownTcs);

            var winner = await Task.WhenAny(handlerTask, shutdownTcs.Task)
                .ConfigureAwait(false);

            if (winner != handlerTask)
            {
                _ = ReleaseWhenFinishedAscyn(handlerTask, handlerResources, onAbandonedHandlerFinished, logger);
                return HandlerOutcome.Abandoned;
            }
        }
        await handlerTask.ConfigureAwait(false);
        return HandlerOutcome.Completed;
    }

    private static async Task ReleaseWhenFinishedAscyn(Task handlerTask,
                                                       IAsyncDisposable handlerResources,
                                                       Action<Exception?> onAbandonedHandlerFinished,
                                                       ILogger logger)
    {
        Exception? handlerError = null;
        try { await handlerTask.ConfigureAwait(false); }
        catch (Exception ex) { handlerError = ex; }

        try { await handlerResources.DisposeAsync().ConfigureAwait(false); }
        catch (Exception ex) { logger.LogWarning(ex, "Failed to release the resource of an abandoned handler"); }

        try { onAbandonedHandlerFinished(handlerError); }
        catch (Exception ex) { logger.LogWarning(ex, "Abandoned handler completion callback threw"); }

    }
}
