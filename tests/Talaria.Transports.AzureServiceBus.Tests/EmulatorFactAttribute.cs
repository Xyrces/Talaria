// SPDX-License-Identifier: Apache-2.0

using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Gates an Azure Service Bus integration test on the emulator being
/// available. Required Docker mode starts the SQL-backed Testcontainers
/// fixture; an environment opt-in can target an existing emulator.
/// </summary>
/// <remarks>
/// <para>
/// The Service Bus emulator listens on <c>localhost:5672</c> and uses the
/// documented <c>UseDevelopmentEmulator=true</c> connection-string option.
/// The fixture starts SQL Server and the emulator with checked-in entities.
/// </para>
/// <para>
/// Missing Docker fails in required mode instead of silently skipping tests.
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
            Skip = $"Set {EnvironmentVariable}=1 for an existing emulator or TALARIA_REQUIRE_DOCKER=1 to start the SQL-backed emulator testcontainers.";
        }
    }

    /// <summary>
    /// Returns <c>true</c> when the environment variable opt-in is present
    /// with a truthy value.
    /// </summary>
    public static bool IsEmulatorOptIn()
    {
        if (Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") == "1") return true;
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
