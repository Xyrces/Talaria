// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Attributes;

/// <summary>
/// Identifies the correlation property on a message type for saga state lookups.
/// If not present, <see cref="Talaria.Core.Sagas.CorrelationResolver"/> falls back through:
/// <c>CorrelationId</c>, then <c>Id</c>, then any property whose name ends with <c>Id</c>.
/// </summary>
/// <since>1.0.0</since>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SagaCorrelationAttribute : Attribute;
