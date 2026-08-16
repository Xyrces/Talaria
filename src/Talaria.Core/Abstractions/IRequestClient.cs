// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Abstractions;

/// <summary>
/// A typed client that publishes a request and awaits a correlated response.
/// </summary>
/// <typeparam name="TRequest">The CLR request type.</typeparam>
/// <remarks>
/// <para>
/// Implementations publish the request to the destination topic with
/// <c>talaria.request_id</c> and <c>talaria.reply_to</c> headers, then wait
/// for a response on an inbox topic. The requester removes the pending
/// request on the first response (or fault) it receives; duplicate responses
/// are ignored.
/// </para>
/// <para>
/// Response delivery is at-least-once: the responder may publish the same
/// response more than once if its offset commit fails after publishing.
/// First-wins semantics in the requester suppress duplicates.
/// </para>
/// </remarks>
/// <since>1.0.0</since>
public interface IRequestClient<TRequest> where TRequest : class
{
    /// <summary>
    /// Publishes a request and returns the correlated response.
    /// </summary>
    /// <typeparam name="TResponse">The expected CLR response type.</typeparam>
    /// <param name="request">The request payload.</param>
    /// <param name="ct">Cancellation token; cancels the wait early. <see cref="TalariaOptions.DefaultRequestTimeout"/> always applies and can only be shortened by cancelling this token.</param>
    /// <returns>A task that completes with the response payload.</returns>
    /// <exception cref="RequestTimeoutException">The response was not received before the timeout elapsed.</exception>
    /// <exception cref="RequestFaultException">The responder published a fault response.</exception>
    Task<TResponse> GetResponseAsync<TResponse>(TRequest request, CancellationToken ct = default) where TResponse : class;
}

/// <summary>
/// Thrown when a request does not receive a response within the configured timeout.
/// </summary>
/// <since>1.0.0</since>
public sealed class RequestTimeoutException : Exception
{
    /// <summary>
    /// Creates a new <see cref="RequestTimeoutException"/>.
    /// </summary>
    /// <param name="requestId">The request identifier that timed out.</param>
    public RequestTimeoutException(string requestId)
        : base($"Request '{requestId}' did not receive a response within the configured timeout.")
    {
        RequestId = requestId;
    }

    /// <summary>The request identifier that timed out.</summary>
    public string RequestId { get; }
}

/// <summary>
/// Thrown when a responder publishes a fault for a request.
/// </summary>
/// <since>1.0.0</since>
public sealed class RequestFaultException : Exception
{
    /// <summary>
    /// Creates a new <see cref="RequestFaultException"/>.
    /// </summary>
    /// <param name="requestId">The request identifier that faulted.</param>
    /// <param name="exceptionType">The type of the responder exception, when available.</param>
    /// <param name="message">The fault message.</param>
    public RequestFaultException(string requestId, string? exceptionType, string message)
        : base($"Request '{requestId}' faulted: {message}")
    {
        RequestId = requestId;
        ResponderExceptionType = exceptionType;
        ResponderMessage = message;
    }

    /// <summary>The request identifier that faulted.</summary>
    public string RequestId { get; }

    /// <summary>The type of the responder exception, when available.</summary>
    public string? ResponderExceptionType { get; }

    /// <summary>The fault message from the responder.</summary>
    public string ResponderMessage { get; }
}
