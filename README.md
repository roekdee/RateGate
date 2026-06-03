# RateGate

A thread-safe token-bucket rate limiter for .NET with synchronous `TryAcquire` and asynchronous `WaitAsync`.

![CI](https://github.com/roekdee/RateGate/actions/workflows/ci.yml/badge.svg)

## Features

- **Token-bucket algorithm** — smooth, continuous refill with configurable burst capacity.
- **Synchronous `TryAcquire`** — non-blocking, returns immediately whether or not tokens were available.
- **Asynchronous `WaitAsync`** — awaits until enough tokens refill, with full `CancellationToken` support.
- **FIFO fairness** — queued waiters are served strictly in arrival order; a large request at the head is never starved by smaller requests behind it.
- **Monotonic clock** — refill is driven by `Stopwatch`, immune to wall-clock adjustments.
- **Deterministically testable** — an injectable time provider lets unit tests drive a virtual clock with no real delays.
- **Production-quality** — nullable-enabled, warnings-as-errors, no external dependencies.

## Usage

```csharp
using RateGate;

// 100 permits per second, with a burst capacity of 100 tokens.
using var limiter = new RateLimiter(permits: 100, period: TimeSpan.FromSeconds(1));

// Synchronous, non-blocking check.
if (limiter.TryAcquire())
{
    DoWork();
}
else
{
    // Over the limit right now — shed load, return 429, etc.
}

// Asynchronous wait until a permit is available.
await limiter.WaitAsync(cancellationToken: token);
await CallDownstreamServiceAsync();

// Acquire several permits at once (e.g. a batch of work items).
if (limiter.TryAcquire(permits: 5))
{
    ProcessBatch();
}

await limiter.WaitAsync(permits: 5, cancellationToken: token);
```

## Build & test

```bash
dotnet test
```

Requires the .NET 8 SDK. The library targets `net8.0`.

## Design notes

**Token bucket.** The limiter holds up to `capacity` tokens and refills continuously
at `permits / period` tokens per second. A request for *n* permits succeeds when at
least *n* tokens are available; otherwise it either fails fast (`TryAcquire`) or queues
(`WaitAsync`). Because the bucket can hold up to `capacity` tokens, an idle limiter
permits a burst up to that size before settling into the steady-state rate. When
`capacity` is omitted it defaults to `permits`.

**Concurrency.** All state transitions happen under a single lock. Available tokens are
recomputed lazily from elapsed time on each operation, so there is no background refill
thread. Pending `WaitAsync` callers are tracked in a FIFO queue of
`TaskCompletionSource`-backed waiters; the head waiter reserves its slot first, which
guarantees ordering and prevents starvation. Continuations run asynchronously
(`RunContinuationsAsynchronously`) so they never execute under the lock. A per-waiter
timer wakes the queue exactly when the next grant becomes possible.

**Virtual clock for tests.** Timing is abstracted behind `ITimeProvider`. Production uses
`MonotonicTimeProvider` (a shared `Stopwatch`). Tests inject `VirtualTimeProvider`, whose
clock only advances when `Advance(...)` is called. This makes every refill and ordering
assertion deterministic — the test suite never relies on real `Thread.Sleep`/`Task.Delay`
for timing correctness.

## Tech

- .NET 8 (`net8.0`), C# with nullable reference types enabled
- `System.Diagnostics.Stopwatch` for the monotonic clock
- `TaskCompletionSource` + `System.Threading.Timer` for async waiting
- xUnit for unit, concurrency, and cancellation tests
- GitHub Actions CI (Ubuntu, `dotnet test`)
