namespace Talaria.Core.Attributes;

/// <summary>
/// Identifies the correlation property on a message type for saga state lookups.
/// If not present, the convention-based "CorrelationId" property is used.
/// </summary>
/// <since>1.0.0</since>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SagaCorrelationAttribute : Attribute;
