using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TelemetryIngestion.Core.Abstractions;
using TelemetryIngestion.Core.Partitioning;
using TelemetryIngestion.Core.Pipeline;
using TelemetryIngestion.Core.Resilience;
using TelemetryIngestion.Functions;

IHost host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        IConfiguration config = context.Configuration;

        services.AddSingleton(_ => new CosmosClient(Required(config, "CosmosConnection"), new CosmosClientOptions
        {
            // Bulk mode groups concurrent point writes into fewer, larger requests per partition.
            AllowBulkExecution = true,
            // The pipeline handles 429s itself (shared cool-down + Retry-After), so the SDK
            // must not silently retry them as well and hide the pressure from us.
            MaxRetryAttemptsOnRateLimitedRequests = 0,
            SerializerOptions = new CosmosSerializationOptions
            {
                PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase   // Id -> id, Pk -> pk
            }
        }));

        services.AddSingleton<ITelemetryStore>(sp => new CosmosTelemetryStore(
            sp.GetRequiredService<CosmosClient>().GetContainer(Required(config, "CosmosDatabase"), Required(config, "CosmosContainer"))));

        services.AddSingleton<IDeadLetterSink>(sp => new CosmosDeadLetterSink(
            sp.GetRequiredService<CosmosClient>().GetContainer(Required(config, "CosmosDatabase"), Required(config, "CosmosDeadLetterContainer"))));

        services.AddSingleton<IPartitionKeyStrategy>(_ => new BucketedPartitionKeyStrategy(
            TimeSpan.FromMinutes(IntSetting(config, "PartitionBucketMinutes", 60)),
            IntSetting(config, "PartitionShardCount", 8)));

        services.AddSingleton(_ => new RetryPolicy(maxAttempts: IntSetting(config, "MaxWriteAttempts", 8)));
        services.AddSingleton<ThrottleGate>();   // one gate per worker, shared by all invocations
        services.AddSingleton(_ => new IngestionOptions { MaxConcurrency = IntSetting(config, "MaxWriteConcurrency", 32) });
        services.AddSingleton(sp => new IngestionPipeline(
            sp.GetRequiredService<ITelemetryStore>(),
            sp.GetRequiredService<IDeadLetterSink>(),
            sp.GetRequiredService<IPartitionKeyStrategy>(),
            sp.GetRequiredService<RetryPolicy>(),
            sp.GetRequiredService<ThrottleGate>(),
            sp.GetRequiredService<IngestionOptions>()));
    })
    .Build();

host.Run();

static string Required(IConfiguration config, string key) =>
    config[key] is { Length: > 0 } value ? value : throw new InvalidOperationException($"Missing app setting '{key}'.");

static int IntSetting(IConfiguration config, string key, int fallback) =>
    int.TryParse(config[key], out int value) && value > 0 ? value : fallback;
