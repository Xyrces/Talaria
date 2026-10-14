// SPDX-License-Identifier: Apache-2.0
using System.Diagnostics;
using Xunit;

namespace Talaria.Persistence.SqlServer.Tests;

public sealed class DockerFactAttribute : FactAttribute
{
    private static readonly Lazy<bool> DockerAvailable = new(ProbeDocker);

    public DockerFactAttribute()
    {
        if (!IsDockerRunning() && Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") != "1")
            Skip = "Docker is unavailable; set TALARIA_REQUIRE_DOCKER=1 in CI to require SQL Server integration tests.";
    }

    public static bool IsDockerRunning() => DockerAvailable.Value;

    private static bool ProbeDocker()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo("docker", "info")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            });
            return process is not null && process.WaitForExit(30000) && process.ExitCode == 0;
        }
        catch { return false; }
    }
}
