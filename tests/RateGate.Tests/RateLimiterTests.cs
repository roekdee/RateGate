using System.Diagnostics;
using Xunit;

namespace RateGate.Tests;

public sealed class RateLimiterTests
{
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    [Fact]
    public void Constructor_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(0, OneSecond));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(-1, OneSecond));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(1, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RateLimiter(1, OneSecond, capacity: 0));
    }

    [Fact]
    public void TryAcquire_RejectsNonPositivePermits()
    {
        using var limiter = new RateLimiter(10, OneSecond);
        Assert.Throws<ArgumentOutOfRangeException>(() => limiter.TryAcquire(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => limiter.TryAcquire(-5));
    }

    [Fact]
    public void TryAcquire_ConsumesTokensUntilBucketEmpty()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 5, OneSecond, capacity: 5, clock);

        // Bucket starts full at capacity (5).
        for (int i = 0; i < 5; i++)
        {
            Assert.True(limiter.TryAcquire(), $"acquire #{i} should succeed");
        }

        // Sixth acquire fails: no tokens left and no time has passed.
        Assert.False(limiter.TryAcquire());
        Assert.Equal(0, limiter.AvailableTokens, precision: 6);
    }

    [Fact]
    public void TryAcquire_MultiplePermitsAtOnce()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        Assert.True(limiter.TryAcquire(7));
        Assert.False(limiter.TryAcquire(4)); // only 3 left
        Assert.True(limiter.TryAcquire(3));
        Assert.False(limiter.TryAcquire(1));
    }

    [Fact]
    public void Refill_AddsTokensProportionalToElapsedTime()
    {
        var clock = new VirtualTimeProvider();
        // 10 tokens per second, capacity 10, start full.
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        // Drain the bucket.
        Assert.True(limiter.TryAcquire(10));
        Assert.False(limiter.TryAcquire(1));

        // Advance 500ms => 5 tokens refilled.
        clock.Advance(TimeSpan.FromMilliseconds(500));
        Assert.Equal(5, limiter.AvailableTokens, precision: 6);
        Assert.True(limiter.TryAcquire(5));
        Assert.False(limiter.TryAcquire(1));
    }

    [Fact]
    public void Refill_NeverExceedsCapacity()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        Assert.True(limiter.TryAcquire(10));

        // Advance far beyond what is needed to fully refill.
        clock.Advance(TimeSpan.FromSeconds(100));
        Assert.Equal(10, limiter.AvailableTokens, precision: 6);

        // Burst cannot exceed capacity.
        Assert.True(limiter.TryAcquire(10));
        Assert.False(limiter.TryAcquire(1));
    }

    [Fact]
    public async Task WaitAsync_CompletesImmediatelyWhenTokensAvailable()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 5, OneSecond, capacity: 5, clock);

        Task wait = limiter.WaitAsync(3);
        Assert.True(wait.IsCompletedSuccessfully);
        await wait;
    }

    [Fact]
    public void WaitAsync_RejectsRequestLargerThanCapacity()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 5, OneSecond, capacity: 5, clock);

        // WaitAsync validates synchronously before returning a Task.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = limiter.WaitAsync(6);
        });
    }

    [Fact]
    public async Task WaitAsync_BlocksThenCompletesAfterRefill()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        // Empty the bucket.
        Assert.True(limiter.TryAcquire(10));

        // This wait cannot complete yet.
        Task wait = limiter.WaitAsync(5);
        Assert.False(wait.IsCompleted);

        // Advance enough virtual time for 5 tokens (500ms at 10/s) and pump.
        clock.Advance(TimeSpan.FromMilliseconds(500));
        limiter.Pump();

        await wait.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(wait.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitAsync_ServesWaitersInFifoOrder()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 1, OneSecond, capacity: 1, clock);

        // Drain the single token.
        Assert.True(limiter.TryAcquire(1));

        // Queue three waiters in a known order.
        Task w1 = limiter.WaitAsync(1);
        Task w2 = limiter.WaitAsync(1);
        Task w3 = limiter.WaitAsync(1);

        // Only one token's worth of time elapses: the head (w1) must complete and the
        // others must remain pending. This proves FIFO without racing continuations.
        clock.Advance(OneSecond);
        limiter.Pump();
        await w1.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(w1.IsCompletedSuccessfully);
        Assert.False(w2.IsCompleted);
        Assert.False(w3.IsCompleted);

        clock.Advance(OneSecond);
        limiter.Pump();
        await w2.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(w2.IsCompletedSuccessfully);
        Assert.False(w3.IsCompleted);

        clock.Advance(OneSecond);
        limiter.Pump();
        await w3.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(w3.IsCompletedSuccessfully);
    }

    [Fact]
    public void WaitAsync_HeadWaiterIsNotStarvedBySmallerLaterRequest()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        Assert.True(limiter.TryAcquire(10)); // empty

        Task big = limiter.WaitAsync(5);   // head, needs 5
        Task small = limiter.WaitAsync(1); // behind, needs 1

        // Refill 3 tokens: not enough for the head (5), and the small request
        // must NOT jump ahead even though 3 >= 1.
        clock.Advance(TimeSpan.FromMilliseconds(300));
        limiter.Pump();

        Assert.False(big.IsCompleted);
        Assert.False(small.IsCompleted);
    }

    [Fact]
    public async Task WaitAsync_CancellationFaultsTheWait()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 1, OneSecond, capacity: 1, clock);

        Assert.True(limiter.TryAcquire(1)); // empty

        using var cts = new CancellationTokenSource();
        Task wait = limiter.WaitAsync(1, cts.Token);
        Assert.False(wait.IsCompleted);

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
    }

    [Fact]
    public async Task WaitAsync_CancellingHeadAllowsNextWaiterToProceed()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 1, OneSecond, capacity: 1, clock);

        Assert.True(limiter.TryAcquire(1)); // empty

        using var cts = new CancellationTokenSource();
        Task head = limiter.WaitAsync(1, cts.Token);
        Task next = limiter.WaitAsync(1);

        // Cancel the head; the next waiter becomes head.
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => head);

        // One token's worth of time then becomes available for the new head.
        clock.Advance(OneSecond);
        limiter.Pump();

        await next.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(next.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitAsync_AlreadyCancelledTokenReturnsCancelledTask()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 1, OneSecond, capacity: 1, clock);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Task wait = limiter.WaitAsync(1, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
    }

    [Fact]
    public async Task ConcurrentTryAcquire_GrantsExactlyCapacityTokens()
    {
        var clock = new VirtualTimeProvider();
        const int capacity = 1000;
        using var limiter = new RateLimiter(permits: capacity, OneSecond, capacity: capacity, clock);

        int granted = 0;
        const int threads = 16;
        const int attemptsPerThread = 200; // 16 * 200 = 3200 > 1000 capacity

        var tasks = new Task[threads];
        using var start = new ManualResetEventSlim(false);
        for (int t = 0; t < threads; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                start.Wait();
                for (int i = 0; i < attemptsPerThread; i++)
                {
                    if (limiter.TryAcquire(1))
                    {
                        Interlocked.Increment(ref granted);
                    }
                }
            });
        }

        start.Set();
        await Task.WhenAll(tasks);

        // No time advanced => no refill => exactly capacity tokens may be granted, never more.
        Assert.Equal(capacity, granted);
    }

    [Fact]
    public async Task ConcurrentWaitAsync_AllEventuallyComplete()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 100, OneSecond, capacity: 100, clock);

        Assert.True(limiter.TryAcquire(100)); // empty

        const int waiters = 50;
        var tasks = new Task[waiters];
        for (int i = 0; i < waiters; i++)
        {
            tasks[i] = limiter.WaitAsync(1);
        }

        // 50 tokens need 0.5s at 100/s.
        clock.Advance(TimeSpan.FromSeconds(1));
        limiter.Pump();

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.All(tasks, t => Assert.True(t.IsCompletedSuccessfully));
    }

    [Fact]
    public async Task Dispose_FaultsPendingWaiters()
    {
        var clock = new VirtualTimeProvider();
        var limiter = new RateLimiter(permits: 1, OneSecond, capacity: 1, clock);

        Assert.True(limiter.TryAcquire(1)); // empty
        Task wait = limiter.WaitAsync(1);

        limiter.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => wait);
    }

    [Fact]
    public void TryAcquire_AfterDispose_Throws()
    {
        var limiter = new RateLimiter(1, OneSecond);
        limiter.Dispose();
        Assert.Throws<ObjectDisposedException>(() => limiter.TryAcquire());
    }

    [Fact]
    public void MonotonicTimeProvider_IsNonDecreasing()
    {
        var provider = new MonotonicTimeProvider();
        TimeSpan first = provider.GetElapsed();
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < 5)
        {
            // tiny busy wait to let the clock advance
        }

        TimeSpan second = provider.GetElapsed();
        Assert.True(second >= first);
    }

    [Fact]
    public void AvailablePermits_ReflectsConsumption()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        // Bucket starts full.
        Assert.Equal(10, limiter.AvailablePermits);

        Assert.True(limiter.TryAcquire(3));
        Assert.Equal(7, limiter.AvailablePermits);

        Assert.True(limiter.TryAcquire(7));
        Assert.Equal(0, limiter.AvailablePermits);

        // Exhausted: no whole permits left, and TryAcquire agrees.
        Assert.False(limiter.TryAcquire(1));
        Assert.Equal(0, limiter.AvailablePermits);
    }

    [Fact]
    public void AvailablePermits_IncreasesAfterRefill()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        Assert.True(limiter.TryAcquire(10));
        Assert.Equal(0, limiter.AvailablePermits);

        // 400ms at 10/s => 4 tokens refilled.
        clock.Advance(TimeSpan.FromMilliseconds(400));
        Assert.Equal(4, limiter.AvailablePermits);

        // Refill is capped at capacity.
        clock.Advance(TimeSpan.FromSeconds(100));
        Assert.Equal(10, limiter.AvailablePermits);
    }

    [Fact]
    public void AvailablePermits_FloorsPartialToken()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 10, OneSecond, capacity: 10, clock);

        Assert.True(limiter.TryAcquire(10));

        // 250ms at 10/s => 2.5 tokens. AvailablePermits floors to 2; the fractional
        // half-token is not yet a usable permit.
        clock.Advance(TimeSpan.FromMilliseconds(250));
        Assert.Equal(2, limiter.AvailablePermits);
        Assert.Equal(2.5, limiter.AvailableTokens, precision: 6);

        // Only the whole permits can actually be acquired.
        Assert.True(limiter.TryAcquire(2));
        Assert.False(limiter.TryAcquire(1));
    }

    [Fact]
    public void TryAcquire_NeverBlocks_WhenExhausted()
    {
        var clock = new VirtualTimeProvider();
        using var limiter = new RateLimiter(permits: 1, OneSecond, capacity: 1, clock);

        Assert.True(limiter.TryAcquire(1)); // empty the bucket

        // With no tokens and no time advanced, TryAcquire must return immediately
        // rather than wait or queue. Guard with a generous wall-clock budget so a
        // hang would surface as a failure instead of blocking the suite.
        var sw = Stopwatch.StartNew();
        bool acquired = limiter.TryAcquire(1);
        sw.Stop();

        Assert.False(acquired);
        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds(1),
            $"TryAcquire blocked for {sw.Elapsed}; it must return without waiting.");
    }

    [Fact]
    public async Task WaitAsync_RealClock_EnforcesThroughput()
    {
        // Sanity check against the real monotonic clock (not a timing assertion on
        // exact duration, only that a blocked waiter eventually completes).
        using var limiter = new RateLimiter(permits: 100, TimeSpan.FromMilliseconds(100));

        Assert.True(limiter.TryAcquire(100)); // empty the bucket

        Task wait = limiter.WaitAsync(10);
        await wait.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(wait.IsCompletedSuccessfully);
    }
}
