using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
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
    /// options validator, and the topic + saga hosted services. The host does not
    /// start any consumers until at least one <c>MapTopic</c> or <c>MapSaga</c> call
    /// is also made against the service provider.
    /// </remarks>
    public static TalariaBuilder AddTalaria(this IServiceCollection services)
    {
        services.TryAddSingleton<TopicRegistry>();
        services.TryAddSingleton<Talaria.Core.Sagas.SagaRegistry>();
        services.AddOptions<TalariaOptions>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<TalariaOptions>, TalariaOptionsValidator>());
        services.AddHostedService<TalariaHostedService>();
        services.AddHostedService<SagaHostedService>();

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
}
