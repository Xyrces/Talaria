// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Sagas;

/// <summary>
/// Represents the lifecycle phase of a compensatable saga.
/// </summary>
/// <since>1.0.0</since>
public enum SagaPhase
{
    /// <summary>The saga is executing normal forward steps.</summary>
    Forward = 0,

    /// <summary>The saga is actively executing compensating steps in reverse order.</summary>
    Compensating = 1,

    /// <summary>All compensating steps have completed.</summary>
    Compensated = 2
}

/// <summary>
/// Describes a single pending compensation operation in a sequential compensation queue.
/// </summary>
/// <param name="OperationId">Unique operation identifier used to fence duplicate or stale acknowledgments.</param>
/// <param name="StepName">Logical name of the compensation step.</param>
/// <param name="CommandType">CLR assembly-qualified or full type name of the compensating command.</param>
/// <param name="DestinationTopic">Destination topic for the compensating command.</param>
/// <param name="PayloadJson">Serialized JSON payload of the compensating command.</param>
/// <param name="Generation">Compensation generation counter when this step was enqueued.</param>
/// <since>1.0.0</since>
public sealed record PendingCompensationEntry(
    string OperationId,
    string StepName,
    string CommandType,
    string DestinationTopic,
    string PayloadJson,
    int Generation);

/// <summary>
/// Durable compensation journal persisted as part of the saga's state JSON payload.
/// Requires no database schema changes across InMemory, Redis, or SQL Server state stores.
/// </summary>
/// <since>1.0.0</since>
public sealed class SagaCompensationState
{
    /// <summary>Gets or sets the current lifecycle phase of the saga.</summary>
    public SagaPhase Phase { get; set; } = SagaPhase.Forward;

    /// <summary>Gets or sets the monotonically increasing compensation generation counter.</summary>
    public int Generation { get; set; }

    /// <summary>Gets or sets the chronological list of named forward steps recorded via <see cref="ISagaContext{TState}.RecordStep(string)"/>.</summary>
    public List<string> RecordedSteps { get; set; } = new();

    /// <summary>Gets or sets the ordered queue of pending sequential compensation operations.</summary>
    public List<PendingCompensationEntry> PendingSteps { get; set; } = new();

    /// <summary>Gets or sets the list of completed compensation step names and operation identifiers.</summary>
    public List<string> CompletedSteps { get; set; } = new();
}

/// <summary>
/// Implemented by saga state types that participate in durable sequential compensation workflows.
/// </summary>
/// <since>1.0.0</since>
public interface ICompensatableState
{
    /// <summary>Gets or sets the durable compensation journal for this saga instance.</summary>
    SagaCompensationState Compensation { get; set; }
}
