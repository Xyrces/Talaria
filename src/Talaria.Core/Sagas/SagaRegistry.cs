using Microsoft.Extensions.DependencyInjection;

namespace Talaria.Core.Sagas;

public class SagaRegistry
{
    public List<SagaRegistration> Registrations { get; } = new();

    public void Add(SagaRegistration registration)
    {
        Registrations.Add(registration);
    }
}
