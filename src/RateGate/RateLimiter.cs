namespace RateGate;

/// <summary>
/// A thread-safe token-bucket rate limiter.
/// <para>
/// The bucket holds up to <see cref="Capacity"/> tokens and refills continuously at a
/// rate of <c>permits / period</c> tokens per second. Callers consume tokens
/// synchronously with <see cref="TryAcquire"/> or wait for them asynchronously with
/// <see cref="WaitAsync(int, CancellationToken)"/>. Pending waiters are served in
/// strict FIFO order.
/// </para>
/// </summary>
public sealed class RateLimiter : IDisposable
{
    private readonly object _gate = new();
    private readonly ITimeProvider _time;
    private readonly double _capacity;
    private readonly double _tokensPerSecond;

    // FIFO queue of pending waiters. Always served front-to-back so that a large
    // request at the head cannot be starved by smaller requests behind it.
    private readonly LinkedList<Waiter> _waiters = new();

    private double _tokens;
    private TimeSpan _lastRefill;
    private bool _disposed;

    /// <summary>
    /// Creates a token bucket.
    /// </summary>
    /// <param name="permits">Number of tokens granted per <paramref name="period"/>. Must be positive.</param>
    /// <param name="period">The refill window for <paramref name="permits"/> tokens. Must be positive.</param>
    /// <param name="capacity">
    /// Maximum tokens the bucket can hold (burst size). Must be positive. When omitted,
    /// defaults to <paramref name="permits"/>.
    /// </param>
    /// <param name="timeProvider">Monotonic clock. Defaults to <see cref="MonotonicTimeProvider"/>.</param>
    public RateLimiter(
        int permits,
        TimeSpan period,
        int? capacity = null,
        ITimeProvider? timeProvider = null)
    {
        if (permits <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(permits), "permits must be positive.");
        }

        if (period <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(period), "period must be positive.");
        }

        int effectiveCapacity = capacity ?? permits;
        if (effectiveCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be positive.");
        }

        _time = timeProvider ?? new MonotonicTimeProvider();
        _capacity = effectiveCapacity;
        _tokensPerSecond = permits / period.TotalSeconds;

        // Buckets start full so an idle limiter allows an immediate burst up to capacity.
        _tokens = _capacity;
        _lastRefill = _time.GetElapsed();
    }

    /// <summary>Maximum number of tokens the bucket can hold.</summary>
    public int Capacity => (int)_capacity;

    /// <summary>
    /// Number of tokens currently available, after accounting for elapsed-time refill.
    /// Primarily intended for diagnostics and tests.
    /// </summary>
    public double AvailableTokens
    {
        get
        {
            lock (_gate)
            {
                Refill();
                return _tokens;
            }
        }
    }

    /// <summary>
    /// Attempts to consume <paramref name="permits"/> tokens immediately without waiting.
    /// </summary>
    /// <param name="permits">Number of tokens to consume. Must be positive.</param>
    /// <returns><c>true</c> if the tokens were consumed; otherwise <c>false</c>.</returns>
    public bool TryAcquire(int permits = 1)
    {
        ValidatePermits(permits);

        lock (_gate)
        {
            ThrowIfDisposed();
            Refill();

            // Honour FIFO: if anyone is already queued ahead of us, do not jump the line.
            if (_waiters.Count == 0 && _tokens >= permits)
            {
                _tokens -= permits;
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Asynchronously waits until <paramref name="permits"/> tokens are available, then
    /// consumes them. Waiters are served in FIFO order.
    /// </summary>
    /// <param name="permits">Number of tokens to consume. Must be positive and at most <see cref="Capacity"/>.</param>
    /// <param name="cancellationToken">Cancels the wait. The reservation is released on cancellation.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// If <paramref name="permits"/> is not positive or exceeds <see cref="Capacity"/> (which could never be satisfied).
    /// </exception>
    /// <exception cref="OperationCanceledException">If <paramref name="cancellationToken"/> is cancelled.</exception>
    public Task WaitAsync(int permits = 1, CancellationToken cancellationToken = default)
    {
        ValidatePermits(permits);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        lock (_gate)
        {
            ThrowIfDisposed();

            if (permits > _capacity)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(permits),
                    $"permits ({permits}) exceeds bucket capacity ({Capacity}); the request can never be satisfied.");
            }

            Refill();

            // Fast path: nobody ahead of us and enough tokens.
            if (_waiters.Count == 0 && _tokens >= permits)
            {
                _tokens -= permits;
                return Task.CompletedTask;
            }

            // Slow path: enqueue and let the scheduler grant tokens as they refill.
            var waiter = new Waiter(permits);
            LinkedListNode<Waiter> node = _waiters.AddLast(waiter);

            if (cancellationToken.CanBeCanceled)
            {
                waiter.Registration = cancellationToken.Register(
                    static state => CancelWaiter(state!),
                    new CancelState(this, node));
            }

            ScheduleNextWake();
            return waiter.Task;
        }
    }

    /// <summary>
    /// Recomputes available tokens based on time elapsed since the last refill.
    /// Must be called while holding <see cref="_gate"/>.
    /// </summary>
    private void Refill()
    {
        TimeSpan now = _time.GetElapsed();
        double seconds = (now - _lastRefill).TotalSeconds;
        if (seconds <= 0)
        {
            return;
        }

        _tokens = Math.Min(_capacity, _tokens + (seconds * _tokensPerSecond));
        _lastRefill = now;
    }

    /// <summary>
    /// Grants tokens to queued waiters in FIFO order while enough tokens are available.
    /// Must be called while holding <see cref="_gate"/>. Returns waiters to complete
    /// outside the lock to avoid running continuations under the gate.
    /// </summary>
    private List<Waiter>? DrainReadyWaiters()
    {
        List<Waiter>? completed = null;

        while (_waiters.First is { } node)
        {
            Waiter waiter = node.Value;
            if (_tokens < waiter.Permits)
            {
                break;
            }

            _tokens -= waiter.Permits;
            _waiters.RemoveFirst();
            waiter.Registration.Dispose();
            (completed ??= new List<Waiter>()).Add(waiter);
        }

        return completed;
    }

    /// <summary>
    /// Refills, drains ready waiters, and schedules a timer to wake the head waiter when
    /// the bucket will next hold enough tokens. Must be called while holding the gate.
    /// </summary>
    private void ScheduleNextWake()
    {
        Refill();
        List<Waiter>? ready = DrainReadyWaiters();

        // Complete granted waiters outside the lock.
        if (ready is not null)
        {
            CompleteOutsideLock(ready);
        }

        if (_waiters.First is not { } head)
        {
            return;
        }

        double deficit = head.Value.Permits - _tokens;
        if (deficit <= 0)
        {
            // Tokens already sufficient (e.g. another thread added them); re-drain promptly.
            QueueImmediateWake();
            return;
        }

        double secondsUntilReady = deficit / _tokensPerSecond;
        TimeSpan delay = TimeSpan.FromSeconds(secondsUntilReady);

        // Cap the granularity so virtual-clock-driven tests (which advance time manually
        // and then poke the limiter) and real timers both make progress.
        head.Value.ArmTimer(this, delay);
    }

    /// <summary>Wakes the limiter on a background thread to re-evaluate the queue.</summary>
    private void QueueImmediateWake()
    {
        ThreadPool.UnsafeQueueUserWorkItem(static state => ((RateLimiter)state!).Pump(), this);
    }

    /// <summary>
    /// Re-evaluates the waiter queue. Invoked by waiter timers (real time) or by tests
    /// after advancing the virtual clock.
    /// </summary>
    internal void Pump()
    {
        List<Waiter>? ready;
        bool reschedule;

        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            Refill();
            ready = DrainReadyWaiters();
            reschedule = _waiters.First is not null;
        }

        if (ready is not null)
        {
            CompleteOutsideLock(ready);
        }

        if (reschedule)
        {
            lock (_gate)
            {
                if (!_disposed && _waiters.First is { } head)
                {
                    double deficit = head.Value.Permits - _tokens;
                    if (deficit > 0)
                    {
                        head.Value.ArmTimer(this, TimeSpan.FromSeconds(deficit / _tokensPerSecond));
                    }
                    else
                    {
                        QueueImmediateWake();
                    }
                }
            }
        }
    }

    private static void CompleteOutsideLock(List<Waiter> ready)
    {
        foreach (Waiter waiter in ready)
        {
            waiter.TrySetResult();
        }
    }

    private static void CancelWaiter(object state)
    {
        var (limiter, node) = (CancelState)state;
        Waiter waiter = node.Value;

        bool removed = false;
        lock (limiter._gate)
        {
            // The node may already be detached if the waiter was granted concurrently.
            if (node.List is not null)
            {
                limiter._waiters.Remove(node);
                removed = true;
            }
        }

        if (removed)
        {
            waiter.TrySetCanceled();
            // The head may have changed; re-evaluate scheduling.
            limiter.QueueImmediateWake();
        }
    }

    private void ValidatePermits(int permits)
    {
        if (permits <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(permits), "permits must be positive.");
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RateLimiter));
        }
    }

    /// <summary>
    /// Disposes the limiter and faults all pending waiters with
    /// <see cref="ObjectDisposedException"/>.
    /// </summary>
    public void Dispose()
    {
        List<Waiter> pending = new();
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            for (LinkedListNode<Waiter>? node = _waiters.First; node is not null; node = node.Next)
            {
                pending.Add(node.Value);
            }

            _waiters.Clear();
        }

        foreach (Waiter waiter in pending)
        {
            waiter.Registration.Dispose();
            waiter.DisposeTimer();
            waiter.TrySetException(new ObjectDisposedException(nameof(RateLimiter)));
        }
    }

    private readonly struct CancelState
    {
        private readonly RateLimiter _limiter;
        private readonly LinkedListNode<Waiter> _node;

        public CancelState(RateLimiter limiter, LinkedListNode<Waiter> node)
        {
            _limiter = limiter;
            _node = node;
        }

        public void Deconstruct(out RateLimiter limiter, out LinkedListNode<Waiter> node)
        {
            limiter = _limiter;
            node = _node;
        }
    }

    private sealed class Waiter
    {
        private readonly TaskCompletionSource _tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private Timer? _timer;

        public Waiter(int permits) => Permits = permits;

        public int Permits { get; }

        public Task Task => _tcs.Task;

        public CancellationTokenRegistration Registration { get; set; }

        public void ArmTimer(RateLimiter limiter, TimeSpan delay)
        {
            if (delay < TimeSpan.Zero)
            {
                delay = TimeSpan.Zero;
            }

            if (_timer is null)
            {
                _timer = new Timer(static s => ((RateLimiter)s!).Pump(), limiter, delay, Timeout.InfiniteTimeSpan);
            }
            else
            {
                _timer.Change(delay, Timeout.InfiniteTimeSpan);
            }
        }

        public void DisposeTimer() => _timer?.Dispose();

        public void TrySetResult()
        {
            DisposeTimer();
            _tcs.TrySetResult();
        }

        public void TrySetCanceled()
        {
            DisposeTimer();
            _tcs.TrySetCanceled();
        }

        public void TrySetException(Exception ex)
        {
            DisposeTimer();
            _tcs.TrySetException(ex);
        }
    }
}
