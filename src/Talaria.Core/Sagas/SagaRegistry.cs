// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Sagas;

/// <summary>
/// Registry of all sagas mapped via <c>MapSaga</c>.
/// </summary>
/// <since>1.0.0</since>
public class SagaRegistry
{
    private readonly List<SagaRegistration> _registrations = new();
    private bool _sealed;

    /// <summary>The registrations added so far, in insertion order.</summary>
    public IReadOnlyList<SagaRegistration> Registrations => _registrations;

    /// <summary>
    /// Seals the registry so no further saga registrations can be added.
    /// Idempotent: subsequent calls have no effect.
    /// </summary>
    internal void Seal()
    {
        _sealed = true;
    }

    /// <summary>Adds a saga registration.</summary>
    /// <param name="registration">The saga registration to add.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the registry has already been sealed by the hosted service.
    /// </exception>
    public void Add(SagaRegistration registration)
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
