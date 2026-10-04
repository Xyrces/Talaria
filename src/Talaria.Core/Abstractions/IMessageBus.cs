// SPDX-License-Identifier: Apache-2.0
namespace Talaria.Core.Abstractions;

/// <summary>Application-facing sending; producers and connections are owned by Talaria.</summary>
public interface IMessageBus
{
    Task SendAsync<T>(T message, CancellationToken ct = default);
    Task PublishAsync<T>(T message, CancellationToken ct = default);
    Task SendAsync<T>(string destination, T message, CancellationToken ct = default);
    Task PublishAsync<T>(string destination, T message, CancellationToken ct = default);
}
