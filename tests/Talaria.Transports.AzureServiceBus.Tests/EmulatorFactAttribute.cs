// SPDX-License-Identifier: Apache-2.0

using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Gates an Azure Service Bus integration test on the local emulator being
/// available. The test only runs when the operator sets
/// <c>TALARIA_RUN_ASB_EMULATOR=1</c> in the environment; otherwise the test
/// is skipped with a message that explains the opt-in.
/// </summary>
/// <remarks>
/// <para>
/// The Service Bus emulator (<c>microsoft/azure-service-bus-emulator</c>)
/// runs on <c>localhost:5672</c> and accepts the special connection string
/// <c>UseDevelopmentEnvironment=true</c>. We deliberately do NOT probe the
/// port here: the integration tests instantiate a real
/// <see cref="Azure.Messaging.ServiceBus.ServiceBusClient"/> against that
/// endpoint, which is expensive enough that we want the operator to opt in
/// explicitly rather than accidentally running it on every CI worker.
/// </para>
/// <para>
/// Mirrors the convention used by the Kafka transport's
/// <c>DockerFactAttribute</c> — opt-in via env var, skip with a helpful
/// message otherwise.
/// </para>
/// </remarks>
/// <since>1.0.0</since>
public sealed class EmulatorFactAttribute : FactAttribute
{
    /// <summary>
    /// Name of the environment variable that opts a build into running the
    /// emulator-gated tests. The tests are skipped unless this variable is
    /// set to a truthy value (<c>1</c>, <c>true</c>, <c>yes</c>, or
    /// <c>on</c>, case-insensitive).
    /// </summary>
    public const string EnvironmentVariable = "TALARIA_RUN_ASB_EMULATOR";

    public EmulatorFactAttribute()
    {
        if (!IsEmulatorOptIn())
        {
            Skip = $"Set {EnvironmentVariable}=1 to run Azure Service Bus emulator integration tests.";
        }
    }

    /// <summary>
    /// Returns <c>true</c> when the environment variable opt-in is present
    /// with a truthy value.
    /// </summary>
    public static bool IsEmulatorOptIn()
    {
        var raw = Environment.GetEnvironmentVariable(EnvironmentVariable);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        return raw.Trim().ToUpperInvariant() switch
        {
            "1" or "TRUE" or "YES" or "Y" or "ON" => true,
            _ => false,
        };
    }
}
