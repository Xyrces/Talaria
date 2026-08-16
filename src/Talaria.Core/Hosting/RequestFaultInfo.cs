// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Hosting;

/// <summary>
/// Minimal payload used when a request handler fault is reported to the reply topic.
/// The requester relies on the headers, not this body.
/// </summary>
internal sealed record RequestFaultInfo;

/// <summary>
/// Header keys used to carry fault metadata on request/response fault messages.
/// </summary>
internal static class RequestClientFaultHeaders
{
    internal const string ExceptionTypeKey = "talaria.request_fault.exception_type";
    internal const string ExceptionMessageKey = "talaria.request_fault.exception_message";
}
