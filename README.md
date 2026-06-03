# RateGate

![CI](https://github.com/roekdee/RateGate/actions/workflows/ci.yml/badge.svg)

A token-bucket rate limiter for .NET. You can either check synchronously with `TryAcquire` (returns right away, true or false) or wait asynchronously with `WaitAsync` until enough tokens refill. Both take an optional permit count so you can grab several at once.

The bucket holds up to `capacity` tokens and refills continuously at `permits / period` per second. An idle limiter lets you burst up to `capacity`, then settles into the steady rate. If you don't pass a capacity it defaults to `permits`.

## Usage

```csharp
using RateGate;

// 100 permits/sec, burst capacity 100
using var limiter = new RateLimiter(permits: 100, period: TimeSpan.FromSeconds(1));

if (limiter.TryAcquire())
{
    DoWork();
}
else
{
    // over the limit right now — shed load, return 429, whatever
}

// wait until a permit frees up
await limiter.WaitAsync(cancellationToken: token);
await CallDownstreamServiceAsync();

// grab several at once
if (limiter.TryAcquire(permits: 5)) ProcessBatch();
await limiter.WaitAsync(permits: 5, cancellationToken: token);
```

## Build & test

```bash
dotnet test
```

Needs the .NET 8 SDK. The library targets `net8.0`.

## How it works

All the state lives behind one lock. There's no background refill thread — available tokens are recomputed from elapsed time on each call, which keeps it simple and means an idle limiter costs nothing. Waiters sit in a FIFO queue of `TaskCompletionSource`-backed entries; the head reserves its slot first, so a large request at the front won't get starved by smaller ones behind it. Continuations run asynchronously so they don't fire under the lock, and a per-waiter timer wakes the queue when the next grant is due.

Timing goes through an `ITimeProvider`. Production uses a `Stopwatch`-based monotonic clock (so wall-clock changes don't mess with refill). Tests inject a virtual clock that only moves when you call `Advance(...)`, which is what lets the ordering and refill tests be deterministic instead of leaning on `Task.Delay`.

## Notes

The virtual-clock abstraction is the part I'm happiest about — without it, the concurrency and ordering tests would be flaky and slow, and with it they're exact. That was worth the small bit of indirection.

It's a single in-process limiter; there's no distributed/shared-state mode, so each process has its own bucket. If I needed cross-instance limiting I'd back it with Redis or similar. A `TryAcquire` overload that reports the wait time until the next token would also be a nice addition.
