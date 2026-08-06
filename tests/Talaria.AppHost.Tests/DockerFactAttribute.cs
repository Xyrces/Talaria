using System.Diagnostics;
using Xunit;

namespace Talaria.AppHost.Tests;

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
