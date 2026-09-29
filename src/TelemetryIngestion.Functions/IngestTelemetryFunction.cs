using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using TelemetryIngestion.Core.Abstractions;
using TelemetryIngestion.Core.Models;
using TelemetryIngestion.Core.Pipeline;

namespace TelemetryIngestion.Functions;

public sealed class IngestTelemetryFunction
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private const int MaxRawPayloadChars = 4_000;

    private readonly IngestionPipeline _pipeline;
    private readonly IDeadLetterSink _deadLetters;
    private readonly ILogger<IngestTelemetryFunction> _logger;

    public IngestTelemetryFunction(IngestionPipeline pipeline, IDeadLetterSink deadLetters, ILogger<IngestTelemetryFunction> logger)
    {
        _pipeline = pipeline;
        _deadLetters = deadLetters;
        _logger = logger;
    }

    /// <summary>
    /// Receives a batch from Event Hubs. The checkpoint only advances when this method returns,
    /// and the pipeline returns only after every event is written or dead-lettered.
    /// </summary>
    [Function("IngestTelemetry")]
    public async Task Run(
        [EventHubTrigger("%EventHubName%", Connection = "EventHubConnection", ConsumerGroup = "%EventHubConsumerGroup%", IsBatched = true)]
        string[] messages,
        CancellationToken cancellationToken)
    {
        var events = new List<TelemetryEvent>(messages.Length);
        int unreadable = 0;

        foreach (string message in messages)
        {
            TelemetryEvent? parsed = null;
            string? error = null;
            try
            {
                parsed = JsonSerializer.Deserialize<TelemetryEvent>(message, Json);
                if (parsed is null) error = "Message body was empty or null.";
            }
            catch (JsonException ex)
            {
                error = ex.Message;
            }

            if (parsed is not null)
            {
                events.Add(parsed);
                continue;
            }

            unreadable++;
            string raw = message.Length > MaxRawPayloadChars ? message[..MaxRawPayloadChars] : message;
            await _deadLetters.SendAsync(
                new DeadLetterRecord(null, null, "Unreadable", error ?? "Unknown", raw, DateTimeOffset.UtcNow),
                cancellationToken);
        }

        IngestionResult result = await _pipeline.ProcessAsync(events, cancellationToken);

        _logger.LogInformation(
            "Telemetry batch: {Received} received, {Written} written, {DeadLettered} dead-lettered, {Duplicates} duplicates, " +
            "{Unreadable} unreadable, {ThrottleRetries} throttle retries, {TransientRetries} transient retries",
            messages.Length, result.Written, result.DeadLettered, result.Duplicates,
            unreadable, result.ThrottleRetries, result.TransientRetries);

        if (result.DeadLettered + unreadable > 0)
        {
            _logger.LogWarning("{Count} telemetry events were dead-lettered in this batch", result.DeadLettered + unreadable);
        }
    }
}
