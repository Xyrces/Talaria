namespace Talaria.Core.Attributes;

/// <summary>
/// Identifies the correlation property on a message type for saga state lookups.
/// If not present, the convention-based "CorrelationId" property is used.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SagaCorrelationAttribute : Attribute;
