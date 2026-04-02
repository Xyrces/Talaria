namespace Talaria.Specs.Messages;

/// <summary>
/// Test message types used across BDD specs.
/// </summary>
public sealed record OrderPlaced(string OrderId, decimal Total);

public sealed record PaymentCompleted(string OrderId);

public sealed record PaymentFailed(string OrderId, string Reason);

public sealed record ShipOrder(string OrderId);
