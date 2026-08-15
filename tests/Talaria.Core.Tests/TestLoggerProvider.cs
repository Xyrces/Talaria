// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging;

namespace Talaria.Core.Tests;

/// <summary>
/// In-memory logger that captures formatted log entries for test assertions.
/// </summary>
internal sealed class TestLoggerProvider : ILoggerProvider
{
    private readonly List<LogEntry> _entries = new();

    public IReadOnlyList<LogEntry> Entries => _entries;

    public ILogger CreateLogger(string categoryName)
    {
        return new TestLogger(_entries, categoryName);
    }

    public void Dispose()
    {
    }

    internal sealed record LogEntry(LogLevel Level, string Category, string Message, Exception? Exception);

    private sealed class TestLogger : ILogger
    {
        private readonly List<LogEntry> _entries;
        private readonly string _category;

        public TestLogger(List<LogEntry> entries, string category)
        {
            _entries = entries;
            _category = category;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            _entries.Add(new LogEntry(logLevel, _category, formatter(state, exception), exception));
        }
    }
}
