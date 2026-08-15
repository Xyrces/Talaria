// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Sagas;

/// <summary>
/// Registry of all sagas mapped via <c>MapSaga</c>.
/// </summary>
/// <since>1.0.0</since>
public sealed class SagaRegistry
{
    private readonly List<SagaRegistration> _registrations = new();
    private readonly object _lock = new();
    private bool _sealed;

    /// <summary>True if the registry has been sealed and no further registrations can be added.</summary>
    public bool IsSealed
    {
        get
        {
            lock (_lock)
            {
                return _sealed;
            }
        }
    }

    /// <summary>The registrations added so far, in insertion order.</summary>
    public IReadOnlyList<SagaRegistration> Registrations
    {
        get
        {
            lock (_lock)
            {
                return _registrations.ToList();
            }
        }
    }

    /// <summary>
    /// Seals the registry so no further saga registrations can be added.
    /// Idempotent: subsequent calls have no effect.
    /// </summary>
    internal void Seal()
    {
        lock (_lock)
        {
            _sealed = true;
        }
    }

    /// <summary>Adds a saga registration.</summary>
    /// <param name="registration">The saga registration to add.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the registry has already been sealed by TalariaListener.
    /// </exception>
    internal void Add(SagaRegistration registration)
    {
        lock (_lock)
        {
            if (_sealed)
            {
                throw new InvalidOperationException(
                    "Saga registrations are captured when the host starts. " +
                    "Call MapSaga before the host runs (e.g. during startup, before app.Run()).");
            }

            _registrations.Add(registration);
        }
    }
}
