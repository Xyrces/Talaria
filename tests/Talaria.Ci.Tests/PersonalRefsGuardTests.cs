// SPDX-License-Identifier: AGPL-3.0-or-later
//
// PersonalRefsGuardTests.cs
//
// xUnit coverage for scripts/check-personal-refs.sh. These tests shell out
// to bash against an ephemeral git repo (created in a per-test temp dir) and
// exercise the script's main code paths: clean repo (no hits), per-pattern
// hits, exclude paths honored, allow-mode override, and the self-test.
//
// Skipped on Windows because the script requires bash. The skip is a runtime
// early-return so we don't need a SkippableFact package dependency.

namespace Talaria.Ci.Tests;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit;

public class PersonalRefsGuardTests
{
    private static readonly string ScriptRelativePath =
        Path.Combine("..", "..", "..", "..", "..", "scripts", "check-personal-refs.sh");

    private static bool SupportedOnThisPlatform =>
        !RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    private static string ResolveScriptPath()
    {
        var path = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, ScriptRelativePath));
        if (!File.Exists(path))
        {
            throw new FileNotFoundException(
                $"check-personal-refs.sh not found at '{path}'. " +
                "Make sure the repo's scripts/ directory ships alongside the test assembly.");
        }
        return path;
    }

    private static async Task<(string StdOut, string StdErr, int ExitCode)> RunBashAsync(
        string workingDir,
        string scriptPath,
        IReadOnlyDictionary<string, string>? env = null,
        params string[] args)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "bash",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = workingDir,
        };
        psi.ArgumentList.Add(scriptPath);
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }
        if (env != null)
        {
            foreach (var kv in env)
            {
                psi.Environment[kv.Key] = kv.Value;
            }
        }

        using var proc = Process.Start(psi)!;
        var stdout = await proc.StandardOutput.ReadToEndAsync();
        var stderr = await proc.StandardError.ReadToEndAsync();
        await proc.WaitForExitAsync();
        return (stdout, stderr, proc.ExitCode);
    }

    private static string CreateEphemeralRepo(
        IReadOnlyDictionary<string, string> files)
    {
        var dir = Path.Combine(Path.GetTempPath(), "talaria-ci-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        RunProcess("git", "init -q", dir);
        RunProcess("git", "config user.email self-test@local", dir);
        RunProcess("git", "config user.name self-test", dir);
        foreach (var kv in files)
        {
            var full = Path.Combine(dir, kv.Key);
            Directory.CreateDirectory(Path.GetDirectoryName(full)!);
            File.WriteAllText(full, kv.Value);
        }
        RunProcess("git", "add -A", dir);
        RunProcess("git", "commit -q -m init", dir);
        return dir;
    }

    private static void RunProcess(string exe, string args, string cwd)
    {
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = cwd,
        };
        foreach (var a in args.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            psi.ArgumentList.Add(a);
        }
        using var p = Process.Start(psi)!;
        p.WaitForExit();
    }

    [Fact]
    public async Task SelfTest_Passes()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var (stdout, stderr, rc) = await RunBashAsync(
            Path.GetTempPath(), scriptPath, null, "--self-test");
        Assert.True(rc == 0, $"self-test exited {rc}. stderr:\n{stderr}\nstdout:\n{stdout}");
        Assert.Contains("self-test OK", stderr);
    }

    [Fact]
    public async Task Clean_Repo_NoHits_ReturnsZero()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            ["src/clean.cs"] = "namespace Talaria;\npublic class C {}\n",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath, null);
            Assert.True(rc == 0, $"expected exit 0, got {rc}. stderr:\n{stderr}");
            Assert.Contains("PERSONAL_REFS_GUARD OK", stderr);
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task DetectsPersonalEmail()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            ["src/hit.cs"] = "// jtn5016@gmail.com used to be here\npublic class C {}\n",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath,
                new Dictionary<string, string> { ["PERSONAL_REFS_GUARD"] = "deny" });
            Assert.Equal(1, rc);
            Assert.Contains("src/hit.cs", stderr);
            Assert.Contains("personal author identity", stderr);
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task DetectsHostLocalPath()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            ["src/hit.cs"] = "// /home/jtn5016/work/repo was here\npublic class C {}\n",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath,
                new Dictionary<string, string> { ["PERSONAL_REFS_GUARD"] = "deny" });
            Assert.Equal(1, rc);
            Assert.Contains("host-local absolute path", stderr);
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task DetectsHardcodedApiKey()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            ["src/hit.cs"] = @"var ApiKey = ""abcd1234""; public class C {}",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath,
                new Dictionary<string, string> { ["PERSONAL_REFS_GUARD"] = "deny" });
            Assert.Equal(1, rc);
            Assert.Contains("hardcoded API key", stderr);
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task DetectsTodoRealValueHere()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            ["src/hit.cs"] = "// TODO: real value here\npublic class C {}\n",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath,
                new Dictionary<string, string> { ["PERSONAL_REFS_GUARD"] = "deny" });
            Assert.Equal(1, rc);
            Assert.Contains("placeholder TODO/FIXME", stderr);
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task ExcludesCrPersonalDir()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            [".cr/personal/should_skip.cs"] = "// jtn5016@gmail.com\n",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath,
                new Dictionary<string, string> { ["PERSONAL_REFS_GUARD"] = "deny" });
            Assert.True(rc == 0, $"expected exit 0 (excluded), got {rc}. stderr:\n{stderr}");
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task ExcludesNodeModules()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            ["node_modules/should_skip.cs"] = "// jtn5016@gmail.com\n",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath,
                new Dictionary<string, string> { ["PERSONAL_REFS_GUARD"] = "deny" });
            Assert.True(rc == 0, $"expected exit 0 (excluded), got {rc}. stderr:\n{stderr}");
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task AllowEnvVar_SuppressesWarnings()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var repo = CreateEphemeralRepo(new Dictionary<string, string>
        {
            ["README.md"] = "# clean repo\n",
            ["src/hit.cs"] = "// jtn5016@gmail.com\n",
        });
        try
        {
            var (_, stderr, rc) = await RunBashAsync(repo, scriptPath,
                new Dictionary<string, string> { ["PERSONAL_REFS_GUARD"] = "allow" });
            Assert.True(rc == 0, $"expected exit 0 in allow mode, got {rc}. stderr:\n{stderr}");
            Assert.Contains("skipping personal-refs guard", stderr);
        }
        finally
        {
            Directory.Delete(repo, recursive: true);
        }
    }

    [Fact]
    public async Task SetupErrorOnNonGitRepo()
    {
        SkipOnWindows();
        var scriptPath = ResolveScriptPath();
        var dir = Path.Combine(Path.GetTempPath(), "talaria-ci-nongit-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var (_, stderr, rc) = await RunBashAsync(dir, scriptPath, null);
            Assert.Equal(2, rc);
            Assert.Contains("not inside a git working tree", stderr);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static void SkipOnWindows()
    {
        Skip.If(NotSupported(), "Bash required for check-personal-refs.sh; skipped on Windows.");

        static bool NotSupported() => !SupportedOnThisPlatform;
    }
}

// Local helper namespace; xUnit doesn't ship SkippableFact, so we hand-roll
// a minimal Skip class for the runtime early-return pattern.
internal static class Skip
{
    public static void If(bool condition, string reason)
    {
        if (condition)
        {
            throw new SkipException(reason);
        }
    }
}

public class SkipException : Exception
{
    public SkipException(string reason) : base(reason) { }
}
