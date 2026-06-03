using System.Diagnostics;

namespace RateGate;

/// <summary>
/// Default <see cref="ITimeProvider"/> backed by a high-resolution monotonic
/// <see cref="Stopwatch"/>. Started once at construction and shared for the lifetime
/// of the limiter.
/// </summary>
public sealed class MonotonicTimeProvider : ITimeProvider
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    /// <inheritdoc />
    public TimeSpan GetElapsed() => _stopwatch.Elapsed;
}
