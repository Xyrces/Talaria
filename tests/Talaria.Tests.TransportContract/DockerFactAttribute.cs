// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Diagnostics;
using Xunit;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// xUnit attribute that auto-skips a fact when no Docker daemon is available.
/// Mirrors the attribute in <c>Talaria.Transports.Kafka.Tests</c> so the
/// shared matrix can gate transport rows on broker availability without
/// coupling this project to the Kafka-specific test assembly.
/// </summary>
public sealed class DockerFactAttribute : FactAttribute
{
    public DockerFactAttribute()
    {
        if (!IsDockerRunning())
        {
            Skip = "Docker daemon is not running on this host environment.";
        }
    }

    public static bool IsDockerRunning()
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "info",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var proc = Process.Start(psi);
            if (proc == null) return false;
            bool exited = proc.WaitForExit(3000);
            return exited && proc.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
