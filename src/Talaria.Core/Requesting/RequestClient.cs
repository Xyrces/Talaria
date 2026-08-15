// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Requesting;

/// <summary>
/// Default implementation of <see cref="IRequestClient{TRequest}"/>.
/// </summary>
/// <typeparam name="TRequest">The CLR request type.</typeparam>
internal sealed class RequestClient<TRequest> : IRequestClient<TRequest>
    where TRequest : class
{
    private readonly RequestClientFactory _factory;
    private readonly string _topic;

    public RequestClient(RequestClientFactory factory, string topic)
    {
        _factory = factory;
        _topic = topic;
    }

    /// <inheritdoc />
    public async Task<TResponse> GetResponseAsync<TResponse>(TRequest request, CancellationToken ct = default)
        where TResponse : class
    {
        return await _factory.RequestAsync<TRequest, TResponse>(_topic, request, ct).ConfigureAwait(false);
    }
}
