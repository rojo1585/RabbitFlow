
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using RedRabbit.Infrastructure.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedRabbit.Tests.Unit.Consuming;

public class HandlerExecutionTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task HandlerCompletesBeforeShutdown_ReturnsCompleted_AndLeavesResourcesToCaller()
    {
        var resources = new TrackingDisposable();
        using var shutdown = new CancellationTokenSource();
        var callbackInvoked = false;

        var outcome = await HandlerExecution.WaitAsync(Task.CompletedTask, resources, _ => callbackInvoked = true, NullLogger.Instance, shutdown.Token);

        outcome.Should().Be(HandlerOutcome.Completed);
        resources.IsDisposed.Should().BeFalse("the caller still owns the resources when the handler completes");
        callbackInvoked.Should().BeFalse();
    }

    [Fact]
    public async Task HandlerFailsBeforeShutdown_RethrowsHandlerException_AndLeavesResourcesToCaller()
    {
        var resources = new TrackingDisposable();
        using var shutdown = new CancellationTokenSource();
        var failure = new InvalidOperationException("boom");

        var act = () => HandlerExecution.WaitAsync(Task.FromException(failure), resources, _ => { }, NullLogger.Instance, shutdown.Token);

        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
        resources.IsDisposed.Should().BeFalse();
    }

    [Fact]
    public async Task AlreadyCompletedHandler_WinsOverAlreadyRequestedShutdown()
    {
        var resources = new TrackingDisposable();
        using var shutdown = new CancellationTokenSource();
        shutdown.Cancel();

        var outcome = await HandlerExecution.WaitAsync(Task.CompletedTask, resources, _ => { }, NullLogger.Instance, shutdown.Token);

        outcome.Should().Be(HandlerOutcome.Completed);
        resources.IsDisposed.Should().BeFalse();
    }

    [Fact]
    public async Task ShutdownBeforeCompletion_ReturnsAbandoned_AndKeepsResourcesAliveUntilHandlerFinishes()
    {
        var resources = new TrackingDisposable();
        using var shutdown = new CancellationTokenSource();
        var handler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        var waitTask = HandlerExecution.WaitAsync(handler.Task, resources, error => finished.TrySetResult(error), NullLogger.Instance, shutdown.Token);
        shutdown.Cancel();
        var outcome = await waitTask.WaitAsync(Timeout);

        outcome.Should().Be(HandlerOutcome.Abandoned);
        resources.IsDisposed.Should().BeFalse("the handler is still running and may be using its scoped services");

        handler.SetResult();
        var error = await finished.Task.WaitAsync(Timeout);

        error.Should().BeNull();
        resources.IsDisposed.Should().BeTrue("the resources must be released once the abandoned handler finishes");
    }

    [Fact]
    public async Task AbandonedHandlerFails_ReportsException_AndStillReleasesResources()
    {
        var resources = new TrackingDisposable();
        using var shutdown = new CancellationTokenSource();
        var handler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("late failure");

        var waitTask = HandlerExecution.WaitAsync(handler.Task, resources, error => finished.TrySetResult(error), NullLogger.Instance, shutdown.Token);
        shutdown.Cancel();
        (await waitTask.WaitAsync(Timeout)).Should().Be(HandlerOutcome.Abandoned);

        handler.SetException(failure);
        var error = await finished.Task.WaitAsync(Timeout);

        error.Should().BeSameAs(failure);
        resources.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task AbandonedHandler_ResourcesAreReleasedBeforeCompletionCallback()
    {
        var resources = new TrackingDisposable();
        using var shutdown = new CancellationTokenSource();
        var handler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposedWhenCallbackRan = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var waitTask = HandlerExecution.WaitAsync(handler.Task, resources, _ => disposedWhenCallbackRan.TrySetResult(resources.IsDisposed), NullLogger.Instance, shutdown.Token);
        shutdown.Cancel();
        await waitTask.WaitAsync(Timeout);
        handler.SetResult();

        (await disposedWhenCallbackRan.Task.WaitAsync(Timeout)).Should().BeTrue();
    }

    [Fact]
    public async Task AbandonedHandler_DisposeFailure_IsSwallowed_AndCallbackStillRuns()
    {
        var resources = new TrackingDisposable(throwOnDispose: true);
        using var shutdown = new CancellationTokenSource();
        var handler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        var waitTask = HandlerExecution.WaitAsync(handler.Task, resources, error => finished.TrySetResult(error), NullLogger.Instance, shutdown.Token);
        shutdown.Cancel();
        await waitTask.WaitAsync(Timeout);
        handler.SetResult();

        (await finished.Task.WaitAsync(Timeout)).Should().BeNull();
        resources.DisposeAttempts.Should().Be(1);
    }

    [Fact]
    public async Task UncancellableShutdownToken_WaitsForHandler()
    {
        var resources = new TrackingDisposable();
        var handler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var waitTask = HandlerExecution.WaitAsync(handler.Task, resources, _ => { }, NullLogger.Instance, CancellationToken.None);
        waitTask.IsCompleted.Should().BeFalse();

        handler.SetResult();

        (await waitTask.WaitAsync(Timeout)).Should().Be(HandlerOutcome.Completed);
        resources.IsDisposed.Should().BeFalse();
    }

    private sealed class TrackingDisposable(bool throwOnDispose = false) : IAsyncDisposable
    {
        private int _disposeAttempts;

        public bool IsDisposed => Volatile.Read(ref _disposeAttempts) > 0 && !throwOnDispose;

        public int DisposeAttempts => Volatile.Read(ref _disposeAttempts);

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeAttempts);
            if (throwOnDispose)
                throw new InvalidOperationException("dispose failed");
            return ValueTask.CompletedTask;
        }
    }
}
