// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Talaria.Core.Hosting;

/// <summary>
/// Hosted-service adapter that forwards Generic Host lifecycle events to the shared
/// <see cref="TalariaListener"/>. All saga orchestration, deferral sweeping, and
/// outbox relaying live in the listener; this type is a thin shell so existing
/// DI/ASP.NET Core apps can keep using AddHostedService.
/// </summary>
public sealed class SagaHostedService : BackgroundService
{
    private readonly TalariaListener _listener;
    private readonly ILogger<SagaHostedService> _logger;

    public SagaHostedService(
        TalariaListener listener,
        ILogger<SagaHostedService> logger)
    {
        _listener = listener;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogDebug("SagaHostedService forwarding host start to TalariaListener.");
        await _listener.StartAsync(stoppingToken);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host is stopping — ExecuteAsync will exit and StopAsync will be called next.
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("SagaHostedService forwarding host stop to TalariaListener.");
        await _listener.StopAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
}
