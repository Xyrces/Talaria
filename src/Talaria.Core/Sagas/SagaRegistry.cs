// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Sagas;

/// <summary>
/// Registry of all sagas mapped via <c>MapSaga</c>.
/// </summary>
public class SagaRegistry
{
    private readonly List<SagaRegistration> _registrations = new();

    public IReadOnlyList<SagaRegistration> Registrations => _registrations;

    public void Add(SagaRegistration registration)
    {
        _registrations.Add(registration);
    }
}
