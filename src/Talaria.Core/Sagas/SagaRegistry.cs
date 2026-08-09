namespace Talaria.Core.Sagas;

/// <summary>
/// Registry of all sagas mapped via <c>MapSaga</c>.
/// </summary>
/// <since>1.0.0</since>
public class SagaRegistry
{
    private readonly List<SagaRegistration> _registrations = new();

    /// <summary>The registrations added so far, in insertion order.</summary>
    public IReadOnlyList<SagaRegistration> Registrations => _registrations;

    /// <summary>Adds a saga registration.</summary>
    /// <param name="registration">The saga registration to add.</param>
    public void Add(SagaRegistration registration)
    {
        _registrations.Add(registration);
    }
}
