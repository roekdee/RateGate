namespace RateGate;

/// <summary>
/// Provides a monotonic time source for the rate limiter. The default implementation
/// is backed by <see cref="System.Diagnostics.Stopwatch"/>. Tests can substitute a
/// virtual clock to make refill timing deterministic without real delays.
/// </summary>
public interface ITimeProvider
{
    /// <summary>
    /// Gets the elapsed time since an arbitrary, fixed origin. The value only ever
    /// increases and is not affected by wall-clock adjustments.
    /// </summary>
    TimeSpan GetElapsed();
}
