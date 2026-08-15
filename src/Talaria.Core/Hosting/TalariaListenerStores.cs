// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

/// <summary>
/// Optional stores supplied to <see cref="TalariaListener"/> when it is constructed
/// manually. Stores that are omitted are resolved from the optional
/// <see cref="IServiceProvider"/> when one is supplied; otherwise they remain null
/// and the corresponding features (idempotency, deferral, outbox) are disabled.
/// </summary>
public sealed record TalariaListenerStores(
    IIdempotencyStore? IdempotencyStore = null,
    IDeferralStore? DeferralStore = null,
    IOutboxStore? OutboxStore = null);
