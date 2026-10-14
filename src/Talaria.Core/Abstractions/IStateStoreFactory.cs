// SPDX-License-Identifier: Apache-2.0
namespace Talaria.Core.Abstractions;

/// <summary>Creates typed saga stores for providers sharing one application database.</summary>
public interface IStateStoreFactory
{
    IStateStore<TState> GetStore<TState>() where TState : class, new();
}
