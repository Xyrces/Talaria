// SPDX-License-Identifier: Apache-2.0
namespace Talaria.Core.Abstractions;

/// <summary>Optional transport capability. Completes once enumeration has established a subscription.</summary>
public interface IConsumerReadiness
{
    Task Ready { get; }
}
