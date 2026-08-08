using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Talaria.Core.Hosting;

namespace Talaria.Core.Registration;

/// <summary>
/// Extension methods for registering Talaria messaging services.
/// </summary>
public static class TalariaServiceExtensions
{
    /// <summary>
    /// Adds Talaria messaging services to the DI container.
    /// </summary>
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
    public static TalariaBuilder AddTalaria(
        this IServiceCollection services,
        Action<TalariaOptions> configure)
    {
        var builder = services.AddTalaria();
        builder.Configure(configure);
        return builder;
    }
}
