// SPDX-License-Identifier: Apache-2.0
namespace Talaria.Core.Abstractions;

/// <summary>Application-facing sending; producers and connections are owned by Talaria.</summary>
public interface IMessageBus
{
    /// <summary>Sends a command message to its convention destination.</summary>
    Task SendAsync<T>(T message, CancellationToken ct = default);

    /// <summary>Publishes an event message to its convention destination.</summary>
    Task PublishAsync<T>(T message, CancellationToken ct = default);

    /// <summary>Sends a command message to the specified <paramref name="destination"/>.</summary>
    Task SendAsync<T>(string destination, T message, CancellationToken ct = default);

    /// <summary>Publishes an event message to the specified <paramref name="destination"/>.</summary>
    Task PublishAsync<T>(string destination, T message, CancellationToken ct = default);

    /// <summary>Schedules a command message to its convention destination after the specified <paramref name="delay"/>.</summary>
    Task ScheduleSendAsync<T>(T message, TimeSpan delay, CancellationToken ct = default);

    /// <summary>Schedules a command message to its convention destination at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task ScheduleSendAsync<T>(T message, DateTimeOffset dueAt, CancellationToken ct = default);

    /// <summary>Schedules a command message to the specified <paramref name="destination"/> after the specified <paramref name="delay"/>.</summary>
    Task ScheduleSendAsync<T>(string destination, T message, TimeSpan delay, CancellationToken ct = default);

    /// <summary>Schedules a command message to the specified <paramref name="destination"/> at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task ScheduleSendAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default);

    /// <summary>Schedules an event message to its convention destination after the specified <paramref name="delay"/>.</summary>
    Task SchedulePublishAsync<T>(T message, TimeSpan delay, CancellationToken ct = default);

    /// <summary>Schedules an event message to its convention destination at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task SchedulePublishAsync<T>(T message, DateTimeOffset dueAt, CancellationToken ct = default);

    /// <summary>Schedules an event message to the specified <paramref name="destination"/> after the specified <paramref name="delay"/>.</summary>
    Task SchedulePublishAsync<T>(string destination, T message, TimeSpan delay, CancellationToken ct = default);

    /// <summary>Schedules an event message to the specified <paramref name="destination"/> at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task SchedulePublishAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default);
}
