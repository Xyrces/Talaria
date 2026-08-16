// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Abstractions;

/// <summary>
/// A class-based consumer for request messages that returns a typed response.
/// Implementations are resolved from a per-message DI scope and invoked by
/// the consumer engine for each delivered request envelope.
/// </summary>
/// <typeparam name="TRequest">The CLR request type delivered on the topic.</typeparam>
/// <typeparam name="TResponse">The CLR response type returned by the consumer.</typeparam>
/// <since>1.0.0</since>
public interface IRequestConsumer<TRequest, TResponse>
    where TRequest : class
{
    /// <summary>
    /// Consumes a single request and returns its response.
    /// </summary>
    /// <param name="context">The consume context, including the full envelope, headers, cancellation token, and scoped service provider.</param>
    /// <param name="ct">Cancellation token that is canceled when the consumer loop is shutting down.</param>
    /// <returns>A task that completes with the response payload.</returns>
    Task<TResponse> ConsumeAsync(ConsumeContext<TRequest> context, CancellationToken ct = default);
}
