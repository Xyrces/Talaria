// SPDX-License-Identifier: Apache-2.0
using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Transports.RabbitMq;

public static class RabbitMqExtensions
{
    public static TalariaBuilder UseRabbitMq(this TalariaBuilder builder, string connectionString)
        => builder.UseRabbitMq(options => options.ConnectionString = connectionString);

    public static TalariaBuilder UseRabbitMq(this TalariaBuilder builder, Action<RabbitMqOptions> configure)
    {
        var options = new RabbitMqOptions();
        configure(options);
        if (!Uri.TryCreate(options.ConnectionString, UriKind.Absolute, out var uri) || uri.Scheme is not ("amqp" or "amqps"))
            throw new ArgumentException("RabbitMQ requires an amqp:// or amqps:// connection string.");
        if (options.PrefetchCount == 0) throw new ArgumentOutOfRangeException(nameof(options.PrefetchCount));
        builder.UseTransport(_ => new RabbitMqTransport(options));
        builder.Services.AddSingleton<ITopologyProvisioner>(sp => (RabbitMqTransport)sp.GetRequiredService<ITransport>());
        return builder;
    }
}
