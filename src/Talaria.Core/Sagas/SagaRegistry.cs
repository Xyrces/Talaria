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
