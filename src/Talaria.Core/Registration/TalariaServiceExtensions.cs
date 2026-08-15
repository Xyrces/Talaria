// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Talaria.Core;
using Talaria.Core.Hosting;

namespace Talaria.Core.Registration;

/// <summary>
/// Extension methods for registering Talaria messaging services.
/// </summary>
/// <since>1.0.0</since>
public static class TalariaServiceExtensions
{
    /// <summary>
    /// Adds Talaria messaging services to the DI container.
    /// </summary>
    /// <param name="services">The service collection to register against.</param>
    /// <returns>A <see cref="TalariaBuilder"/> for fluent configuration.</returns>
    /// <remarks>
    /// Registers the <see cref="TalariaOptions"/>, the topic + saga registries, the
    /// options validator, the shared <see cref="TalariaListener"/>, and the single
    /// <see cref="TalariaHostedService"/> adapter. The host does not start any consumers
    /// until at least one <c>MapTopic</c> or <c>MapSaga</c> call is also made against the
    /// service provider.
    /// </remarks>
    public static TalariaBuilder AddTalaria(this IServiceCollection services)
    {
        services.TryAddSingleton<TopicRegistry>();
        services.TryAddSingleton<Talaria.Core.Sagas.SagaRegistry>();
        services.AddOptions<TalariaOptions>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<TalariaOptions>, TalariaOptionsValidator>());
        services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<TalariaOptions>>().Value);
        services.TryAddSingleton<TalariaListener>();
        services.AddHostedService<TalariaHostedService>();

        return new TalariaBuilder(services);
    }

    /// <summary>
    /// Adds Talaria messaging services with configuration.
    /// </summary>
    /// <param name="services">The service collection to register against.</param>
    /// <param name="configure">A callback that mutates <see cref="TalariaOptions"/> at registration time.</param>
    /// <returns>A <see cref="TalariaBuilder"/> for fluent configuration.</returns>
    public static TalariaBuilder AddTalaria(
        this IServiceCollection services,
        Action<TalariaOptions> configure)
    {
        var builder = services.AddTalaria();
        builder.Configure(configure);
        return builder;
    }

    /// <summary>
    /// Returns the shared <see cref="TalariaListener"/> from a built DI container so the
    /// caller can manage its lifecycle manually (e.g. in a console app that still uses DI
    /// for stores and transport, or in a custom composition root).
    /// </summary>
    /// <param name="services">The built service provider.</param>
    /// <returns>The singleton <see cref="TalariaListener"/>.</returns>
    public static TalariaListener BuildTalariaListener(this IServiceProvider services)
    {
        return services.GetRequiredService<TalariaListener>();
    }
}
