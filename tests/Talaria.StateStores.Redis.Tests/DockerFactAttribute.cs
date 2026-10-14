using System.Diagnostics;
using Xunit;

namespace Talaria.StateStores.Redis.Tests;

public sealed class DockerFactAttribute : FactAttribute
{
    public DockerFactAttribute()
    {
        if (!IsDockerRunning())
        {
            if (Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") != "1")
                Skip = "Docker daemon is not running on this host environment.";
        }
    }

    // Probe once per test process and cache the result: docker info can take
    // several seconds while Docker Desktop is busy starting containers, and a
    // short per-call timeout makes the same run randomly pass or skip.
    private static readonly Lazy<bool> DockerAvailable = new(ProbeDocker);

    public static bool IsDockerRunning()
    {
        var available = DockerAvailable.Value;
        if (!available && Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") == "1")
            throw new InvalidOperationException("TALARIA_REQUIRE_DOCKER=1 but Docker is unavailable; provider tests cannot be skipped.");
        return available;
    }

    private static bool ProbeDocker()
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
            bool exited = proc.WaitForExit(30000);
            return exited && proc.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
