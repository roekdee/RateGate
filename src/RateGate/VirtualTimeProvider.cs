namespace RateGate;

/// <summary>
/// A manually controlled <see cref="ITimeProvider"/> for deterministic testing.
/// Time only advances when <see cref="Advance"/> is called, so refill behaviour can
/// be asserted without relying on real sleeps.
/// </summary>
public sealed class VirtualTimeProvider : ITimeProvider
{
    private readonly object _gate = new();
    private TimeSpan _elapsed;

    /// <summary>Creates a virtual clock starting at zero.</summary>
    public VirtualTimeProvider()
        : this(TimeSpan.Zero)
    {
    }

    /// <summary>Creates a virtual clock starting at <paramref name="start"/>.</summary>
    public VirtualTimeProvider(TimeSpan start) => _elapsed = start;

    /// <inheritdoc />
    public TimeSpan GetElapsed()
    {
        lock (_gate)
        {
            return _elapsed;
        }
    }

    /// <summary>Advances the virtual clock by <paramref name="delta"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">If <paramref name="delta"/> is negative.</exception>
    public void Advance(TimeSpan delta)
    {
        if (delta < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delta), "Time cannot move backwards.");
        }

        lock (_gate)
        {
            _elapsed += delta;
        }
    }
}
