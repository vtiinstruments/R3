using System.Collections.Concurrent;

namespace R3.Tests;

public class ReactivePropertyConcurrencyTest
{
    [Fact]
    public async Task ConcurrentWritersSerializeNotifications()
    {
        using var property = new ReactiveProperty<int>(0, null);
        var first = new ConcurrentQueue<int>();
        var second = new ConcurrentQueue<int>();
        var active = 0;
        var overlaps = 0;
        using var subscription1 = property.Skip(1).Subscribe(value =>
        {
            if (Interlocked.Increment(ref active) != 1)
            {
                Interlocked.Increment(ref overlaps);
            }
            first.Enqueue(value);
            Thread.SpinWait(100);
            Interlocked.Decrement(ref active);
        });
        using var subscription2 = property.Skip(1).Subscribe(second.Enqueue);

        await Task.WhenAll(Enumerable.Range(0, 4).Select(worker => Task.Run(() =>
        {
            for (var i = 1; i <= 1000; i++)
            {
                if (worker % 2 == 0)
                {
                    property.Value = worker * 1000 + i;
                }
                else
                {
                    property.OnNext(worker * 1000 + i);
                }
            }
        }))).WaitAsync(TimeSpan.FromSeconds(20));

        overlaps.ShouldBe(0);
        first.Count.ShouldBe(4000);
        first.Order().ShouldBe(Enumerable.Range(1, 4000));
        second.ToArray().ShouldBe(first.ToArray());
        property.CurrentValue.ShouldBe(first.Last());
    }

    [Fact]
    public async Task ReplayAndRegistrationCannotLoseAConcurrentWrite()
    {
        using var property = new ReactiveProperty<int>(0);
        using var replayEntered = new ManualResetEventSlim();
        using var releaseReplay = new ManualResetEventSlim();
        using var writerStarted = new ManualResetEventSlim();
        var values = new ConcurrentQueue<int>();
        var subscribing = Task.Run(() => property.Subscribe(value =>
        {
            values.Enqueue(value);
            if (value == 0)
            {
                replayEntered.Set();
                if (!releaseReplay.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("Replay was not released.");
                }
            }
        }));
        replayEntered.Wait(TimeSpan.FromSeconds(10)).ShouldBeTrue();
        var writer = Task.Factory.StartNew(() =>
        {
            writerStarted.Set();
            property.Value = 1;
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        try
        {
            writerStarted.Wait(TimeSpan.FromSeconds(10)).ShouldBeTrue();
            (await Task.WhenAny(writer, Task.Delay(200))).ShouldNotBe(writer);
        }
        finally
        {
            releaseReplay.Set();
        }

        using var subscription = await subscribing.WaitAsync(TimeSpan.FromSeconds(10));
        await writer.WaitAsync(TimeSpan.FromSeconds(10));
        values.ToArray().ShouldBe([0, 1]);
    }

    [Fact]
    public async Task CurrentValueDoesNotTearMultiwordStructs()
    {
        using var property = new ReactiveProperty<(long Value, long Complement)>((0, ~0L));
        var torn = 0;
        using var start = new ManualResetEventSlim();
        var writer = Task.Run(() =>
        {
            start.Wait();
            for (long i = 1; i <= 100_000; i++)
            {
                property.Value = (i, ~i);
            }
        });
        var reader = Task.Run(() =>
        {
            start.Wait();
            for (var i = 0; i < 100_000; i++)
            {
                var value = property.CurrentValue;
                if (value.Complement != ~value.Value)
                {
                    Interlocked.Increment(ref torn);
                }
            }
        });
        start.Set();
        await Task.WhenAll(writer, reader).WaitAsync(TimeSpan.FromSeconds(20));

        torn.ShouldBe(0);
        property.CurrentValue.ShouldBe((100_000L, ~100_000L));
    }

    [Fact]
    public void CurrentValueDoesNotWaitForNotificationLock()
    {
        using var property = new ReactiveProperty<int>(0);
        var readFinished = false;
        var observed = 0;
        using var subscription = property.Skip(1).Subscribe(_ =>
        {
            var read = Task.Factory.StartNew(() => property.CurrentValue, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
            readFinished = read.Wait(TimeSpan.FromSeconds(2));
            if (readFinished)
            {
                observed = read.Result;
            }
        });

        property.Value = 1;

        readFinished.ShouldBeTrue();
        observed.ShouldBe(1);
    }

    [Theory]
    [InlineData("complete")]
    [InlineData("dispose")]
    [InlineData("error")]
    public async Task TerminalNotificationsWaitForAnActiveWrite(string action)
    {
        using var property = new ReactiveProperty<int>(0);
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var terminalStarted = new ManualResetEventSlim();
        var events = new ConcurrentQueue<string>();
        using var subscription = property.Skip(1).Subscribe(
            _ =>
            {
                events.Enqueue("next");
                entered.Set();
                if (!release.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("Write was not released.");
                }
                events.Enqueue("next-finished");
            },
            _ => events.Enqueue("error"),
            _ => events.Enqueue("completed"));
        var writer = Task.Run(() => property.Value = 1);
        entered.Wait(TimeSpan.FromSeconds(10)).ShouldBeTrue();
        var terminal = Task.Factory.StartNew(() =>
        {
            terminalStarted.Set();
            switch (action)
            {
                case "complete": property.OnCompleted(); break;
                case "dispose": property.Dispose(); break;
                case "error": property.OnErrorResume(new InvalidOperationException("test")); break;
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        try
        {
            terminalStarted.Wait(TimeSpan.FromSeconds(10)).ShouldBeTrue();
            (await Task.WhenAny(terminal, Task.Delay(200))).ShouldNotBe(terminal);
        }
        finally
        {
            release.Set();
        }
        await Task.WhenAll(writer, terminal).WaitAsync(TimeSpan.FromSeconds(10));

        events.ToArray().ShouldBe(["next", "next-finished", action == "error" ? "error" : "completed"]);
        property.IsDisposed.ShouldBe(action == "dispose");
        property.IsCompleted.ShouldBe(action == "complete");
        property.HasObservers.ShouldBe(action == "error");
    }

    [Fact]
    public void SourceCleanupRunsOutsideTheNotificationMonitor()
    {
        using var property = new CleanupProbeProperty();

        property.Dispose();

        property.CleanupAcquiredMonitor.ShouldBeTrue();
    }

    sealed class CleanupProbeProperty : ReactiveProperty<int>
    {
        public bool CleanupAcquiredMonitor { get; private set; }

        protected override void DisposeCore()
        {
            var probe = Task.Factory.StartNew(() =>
            {
                lock (this)
                {
                    return IsDisposed;
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            CleanupAcquiredMonitor = probe.Wait(TimeSpan.FromSeconds(2)) && probe.Result;
        }
    }
}
