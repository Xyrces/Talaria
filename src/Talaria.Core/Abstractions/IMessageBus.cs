// SPDX-License-Identifier: Apache-2.0
using Talaria.Core.Registration;

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
    Task ScheduleSendAsync<T>(T message, TimeSpan delay, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Schedule delay must be non-negative.");
        }

        return ScheduleSendAsync(message, DateTimeOffset.UtcNow.Add(delay), ct);
    }

    /// <summary>Schedules a command message to its convention destination at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task ScheduleSendAsync<T>(T message, DateTimeOffset dueAt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        return ScheduleSendAsync(MessageNames.Destination(MessageNames.Contract(typeof(T))), message, dueAt, ct);
    }

    /// <summary>Schedules a command message to the specified <paramref name="destination"/> after the specified <paramref name="delay"/>.</summary>
    Task ScheduleSendAsync<T>(string destination, T message, TimeSpan delay, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentNullException.ThrowIfNull(message);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Schedule delay must be non-negative.");
        }

        return ScheduleSendAsync(destination, message, DateTimeOffset.UtcNow.Add(delay), ct);
    }

    /// <summary>Schedules a command message to the specified <paramref name="destination"/> at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task ScheduleSendAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default)
        => throw new NotSupportedException($"Message bus '{GetType().Name}' does not support scheduled sending.");

    /// <summary>Schedules an event message to its convention destination after the specified <paramref name="delay"/>.</summary>
    Task SchedulePublishAsync<T>(T message, TimeSpan delay, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Schedule delay must be non-negative.");
        }

        return SchedulePublishAsync(message, DateTimeOffset.UtcNow.Add(delay), ct);
    }

    /// <summary>Schedules an event message to its convention destination at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task SchedulePublishAsync<T>(T message, DateTimeOffset dueAt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        return SchedulePublishAsync(MessageNames.Destination(MessageNames.Contract(typeof(T))), message, dueAt, ct);
    }

    /// <summary>Schedules an event message to the specified <paramref name="destination"/> after the specified <paramref name="delay"/>.</summary>
    Task SchedulePublishAsync<T>(string destination, T message, TimeSpan delay, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentNullException.ThrowIfNull(message);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Schedule delay must be non-negative.");
        }

        return SchedulePublishAsync(destination, message, DateTimeOffset.UtcNow.Add(delay), ct);
    }

    /// <summary>Schedules an event message to the specified <paramref name="destination"/> at the specified <paramref name="dueAt"/> timestamp.</summary>
    Task SchedulePublishAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default)
        => throw new NotSupportedException($"Message bus '{GetType().Name}' does not support scheduled publishing.");
}
