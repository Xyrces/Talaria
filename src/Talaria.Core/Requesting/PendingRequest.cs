// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Requesting;

/// <summary>
/// State for a single in-flight request held by <see cref="RequestClientFactory"/>.
/// </summary>
internal sealed class PendingRequest
{
    public PendingRequest(
        TaskCompletionSource<object> tcs,
        Type responseType,
        CancellationTokenRegistration timeoutRegistration = default)
    {
        Tcs = tcs;
        ResponseType = responseType;
        TimeoutRegistration = timeoutRegistration;
    }

    public TaskCompletionSource<object> Tcs { get; }

    public Type ResponseType { get; }

    public CancellationTokenRegistration TimeoutRegistration { get; set; }

    public void DisposeRegistrations()
    {
        TimeoutRegistration.Dispose();
    }
}
