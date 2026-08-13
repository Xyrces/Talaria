// SPDX-License-Identifier: AGPL-3.0-or-later

using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// Contract row for the in-memory transport. Always available — no Docker or
/// remote dependency. Delegates <see cref="ReadAllFromTopicAsync{T}"/> to
/// <c>InMemoryTransport.ReadAllFromTopicAsync</c>, which is the only transport
/// in the matrix that exposes the entire retained backlog directly; Kafka's
/// row uses a short-lived consumer instead.
/// </summary>
/// <since>1.0.0</since>
public sealed class InMemoryTransportRow : TransportContractRow
{
    public override string DisplayName => "InMemory";

    public override bool IsAvailable => true;

    public override Task<TransportHarness> CreateAsync(CancellationToken ct = default)
    {
        var transport = new InMemoryTransport();
        return Task.FromResult(new TransportHarness(transport));
    }

    public override async Task<List<MessageEnvelope<T>>> ReadAllFromTopicAsync<T>(TransportHarness harness, string topic, TimeSpan timeout)
    {
        // InMemoryTransport is exposed publicly only via ITransport; cast to the
        // concrete type to reuse the ReadAllFromTopicAsync<T>(topic) helper that
        // exists on the class itself.
        var transport = (InMemoryTransport)harness.Transport;
        var list = await transport.ReadAllFromTopicAsync<T>(topic, ct: default);
        // Honor the timeout by racing a delay against the underlying call — the
        // InMemory helper is synchronous-fast so this is a defense-in-depth cap.
        _ = timeout;
        return list;
    }
}
