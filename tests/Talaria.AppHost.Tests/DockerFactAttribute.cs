using System.Diagnostics;
using Xunit;

namespace Talaria.AppHost.Tests;

public sealed class DockerFactAttribute : FactAttribute
{
    public DockerFactAttribute()
    {
        if (IsCiEnvironment())
        {
            Skip = "AppHost Aspire multi-container tests are skipped in CI environment due to runner resource limits.";
        }
        else if (!IsDockerRunning())
        {
            Skip = "Docker daemon is not running on this host environment.";
        }
    }

    private static bool IsCiEnvironment()
    {
        return string.Equals(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), "true", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(Environment.GetEnvironmentVariable("CI"), "true", StringComparison.OrdinalIgnoreCase);
    }

    // Probe once per test process and cache the result: docker info can take
    // several seconds while Docker Desktop is busy starting containers, and a
    // short per-call timeout makes the same run randomly pass or skip.
    private static readonly Lazy<bool> DockerAvailable = new(ProbeDocker);

    public static bool IsDockerRunning() => DockerAvailable.Value;

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
