using System.Collections.Concurrent;
using TelemetryIngestion.Core.Abstractions;
using TelemetryIngestion.Core.Models;

namespace TelemetryIngestion.Core.Tests;

/// <summary>Store whose behaviour per call is scripted by the test.</summary>
internal sealed class ScriptedStore : ITelemetryStore
{
    private readonly Func<TelemetryDocument, int, Exception?> _behaviour;
    private readonly ConcurrentDictionary<string, int> _calls = new();

    public ScriptedStore(Func<TelemetryDocument, int, Exception?> behaviour) => _behaviour = behaviour;

    public ConcurrentDictionary<string, TelemetryDocument> Saved { get; } = new();

    public int CallsFor(string id) => _calls.TryGetValue(id, out int n) ? n : 0;

    public Task UpsertAsync(TelemetryDocument document, CancellationToken cancellationToken)
    {
        int call = _calls.AddOrUpdate(document.Id, 1, (_, n) => n + 1);
        Exception? failure = _behaviour(document, call);
        if (failure is not null) throw failure;
        Saved[document.Id] = document;
        return Task.CompletedTask;
    }
}

internal sealed class RecordingDeadLetters : IDeadLetterSink
{
    public bool Fail { get; init; }
    public ConcurrentBag<DeadLetterRecord> Records { get; } = new();

    public Task SendAsync(DeadLetterRecord record, CancellationToken cancellationToken)
    {
        if (Fail) throw new InvalidOperationException("dead-letter store unavailable");
        Records.Add(record);
        return Task.CompletedTask;
    }
}

internal static class Events
{
    public static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    public static TelemetryEvent Create(string id, string device = "device-1", DateTimeOffset? at = null) =>
        new(id, device, at ?? Now, "temperature", 21.5);
}
