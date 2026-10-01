using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaDotNet.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaDotNet.Messaging;

/// <summary>The outcome of one topic-creation attempt, stripped of Kafka types so it can be asserted in tests.</summary>
/// <param name="Topic">Topic name.</param>
/// <param name="Created">True when this call created it; false when it already existed.</param>
/// <param name="Error">Broker error text, or null on success.</param>
public sealed record TopicOutcome(string Topic, bool Created, string? Error);

/// <summary>
/// Turns the broker's per-topic report into a plain result. Kafka reports
/// "already exists" as an *error* code, which is noisy: for a topic bootstrapper
/// it is the expected outcome on every start after the first.
/// </summary>
public static class TopicCreation
{
    /// <summary>Classify the broker's report into human-meaningful outcomes.</summary>
    public static IReadOnlyList<TopicOutcome> Classify(IEnumerable<CreateTopicReport> reports)
    {
        ArgumentNullException.ThrowIfNull(reports);

        var outcomes = new List<TopicOutcome>();
        foreach (var report in reports)
        {
            outcomes.Add(report.Error.Code switch
            {
                ErrorCode.NoError => new TopicOutcome(report.Topic, Created: true, Error: null),
                ErrorCode.TopicAlreadyExists => new TopicOutcome(report.Topic, Created: false, Error: null),
                _ => new TopicOutcome(report.Topic, Created: false, Error: report.Error.Reason),
            });
        }

        return outcomes;
    }
}

/// <summary>
/// Creates the sample's topics, so the first run does not depend on a human
/// remembering <c>kafka-topics.sh</c>. Idempotent: re-creating an existing topic is
/// reported and ignored.
///
/// It runs as a <see cref="BackgroundService"/> rather than blocking host startup,
/// and retries while the broker is unreachable, so a broker that is still coming up
/// does not stop the app from starting.
/// </summary>
public sealed class TopicBootstrapper : BackgroundService
{
    private const int MaxAttempts = 10;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(3);

    private readonly IAdminClient _admin;
    private readonly KafkaOptions _options;
    private readonly ILogger<TopicBootstrapper> _logger;

    public TopicBootstrapper(IAdminClient admin, IOptions<KafkaOptions> options, ILogger<TopicBootstrapper> logger)
    {
        _admin = admin;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var specifications = OrderTopics.All
            .Select(topic => new TopicSpecification
            {
                Name = topic.Name,
                NumPartitions = topic.Partitions,
                ReplicationFactor = topic.ReplicationFactor,
            })
            .ToList();

        _logger.LogInformation("Ensuring {Count} Kafka topic(s) exist on {Brokers}",
            specifications.Count, _options.BootstrapServers);

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            if (await TryCreateAsync(specifications, stoppingToken).ConfigureAwait(false))
            {
                return;
            }

            _logger.LogWarning("Could not reach the broker to create topics (attempt {Attempt}/{Max}); retrying in {Delay}",
                attempt, MaxAttempts, RetryDelay);

            try
            {
                await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }

        _logger.LogError("Gave up creating Kafka topics. Is the broker at {Brokers} running?", _options.BootstrapServers);
    }

    private async Task<bool> TryCreateAsync(List<TopicSpecification> specifications, CancellationToken stoppingToken)
    {
        try
        {
            await _admin.CreateTopicsAsync(specifications, new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) })
                .ConfigureAwait(false);
            _logger.LogInformation("Created Kafka topic(s): {Topics}", string.Join(", ", specifications.Select(s => s.Name)));
            return true;
        }
        catch (CreateTopicsException ex)
        {
            foreach (var outcome in TopicCreation.Classify(ex.Results))
            {
                if (outcome.Error is not null)
                {
                    // A broker said no for a reason retrying will not fix (usually a
                    // bad replication factor on a single-node cluster).
                    _logger.LogError("Topic {Topic} could not be created: {Error}", outcome.Topic, outcome.Error);
                }
                else
                {
                    _logger.LogInformation("Topic {Topic} {State}", outcome.Topic,
                        outcome.Created ? "created" : "already existed");
                }
            }

            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return true;
        }
        catch (KafkaException ex)
        {
            _logger.LogDebug(ex, "Kafka admin call failed; will retry");
            return false;
        }
        catch (Exception ex)
        {
            // Never let topic bookkeeping take the process down.
            _logger.LogError(ex, "Unexpected error while creating Kafka topics");
            return true;
        }
    }
}
