// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Sagas;

namespace Talaria.Core.Registration;

/// <summary>
/// Registry-based extension methods for configuring sagas directly against a
/// <see cref="SagaRegistry"/>. These mirror the <see cref="IServiceProvider"/>
/// overload in <see cref="TalariaEndpointExtensions"/> and are useful for
/// host-agnostic composition roots that build a <see cref="Hosting.TalariaListener"/>
/// manually.
/// </summary>
public static class SagaRegistryExtensions
{
    /// <summary>
    /// Configures a saga workflow.
    /// </summary>
    /// <typeparam name="TState">The CLR saga state type. Must be a reference type with a public parameterless constructor.</typeparam>
    /// <param name="registry">The saga registry to mutate.</param>
    /// <param name="configure">A callback that uses <see cref="SagaConfigurator{TState}"/> to declare the saga's steps and dispatch routes.</param>
    /// <returns>The same <paramref name="registry"/>, for chaining.</returns>
    public static SagaRegistry MapSaga<TState>(
        this SagaRegistry registry,
        Action<SagaConfigurator<TState>> configure) where TState : class, new()
    {
        var configurator = new SagaConfigurator<TState>(registry);
        configure(configurator);
        configurator.Complete();

        return registry;
    }
}
