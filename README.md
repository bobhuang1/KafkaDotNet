# KafkaDotNet

A complete, runnable example of using **Apache Kafka** to move messages between
independent components in **C# / .NET 10**.

Two processes talk to each other through a broker and nothing else: an HTTP API
that accepts orders, and a background worker that processes them. Neither knows
the other exists. That is the point of an event-driven design, and this repo is
small enough to read in one sitting and complete enough to show the parts that
usually bite: partitioning, consumer groups, offset commits, retries, delayed
redelivery and dead-lettering.

> This is the first repo in a three-part series. [RedisDotNet](https://github.com/bobhuang1/RedisDotNet)
> builds the same pipeline on Redis, and [KafkaVsRabbitMQ](https://github.com/bobhuang1/KafkaVsRabbitMQ)
> puts Kafka and RabbitMQ side by side.

---

## The scenario

An order pipeline for a small shop:

```
                 POST /orders
                      │
                      ▼
        ┌───────────────────────────┐
        │  KafkaDotNet.OrderApi     │   producer
        │  ASP.NET Core minimal API │
        └─────────────┬─────────────┘
                      │  key = customerId
                      ▼
        ┌───────────────────────────────────────────┐
        │              Kafka broker                 │
        │                                           │
        │  orders.placed        (3 partitions)      │
        │  orders.placed.retry  (3 partitions)      │
        │  orders.placed.dlq    (1 partition)       │
        │  orders.confirmed     (3 partitions)      │
        │  orders.rejected      (1 partition)       │
        └──────┬──────────────────────┬─────────────┘
               │                      │
               │ group:               │ group:
               │ order-processor      │ order-notifications
               ▼                      ▼
   ┌─────────────────────────┐   ┌──────────────────────────┐
   │ KafkaDotNet.OrderProcessor │ │  notification sender     │
   │ • main consumer          │   │  (same topic, its own    │
   │ • retry consumer         │   │   offsets — sees every   │
   │ • simulated inventory +  │   │   order too)             │
   │   payment handling       │   └──────────────────────────┘
   └─────────────────────────┘
```

On success the worker publishes `OrderConfirmed`; on a business "no" it publishes
`OrderRejected`; on a *transient* failure it reschedules onto the retry topic with
a delay; and once attempts run out it parks the original event on the dead-letter
topic. All five topics are created at startup, so there is no setup script.

---

## Why Kafka is a good fit (and where it differs from a queue)

| Property | How this sample uses it |
| --- | --- |
| **Ordering per key** | Records are keyed by `customerId`, so all of one customer's orders land on one partition and are processed in order. |
| **Parallelism** | Three partitions means up to three worker instances can process orders at once with no coordination code. |
| **Independent readers** | The processor and the notification sender use different consumer groups and each gets *every* order. A queue would have split them. |
| **Replay** | A new consumer group starts from the beginning of the topic by default, so a new subscriber sees history without the producer changing anything. |
| **At-least-once** | Offsets are committed by hand **after** the work completes; a crash means redelivery, never a lost order. |
| **Retention instead of deletion** | Messages are not removed when consumed. A bug fixed today can be re-processed by resetting an offset. |

The trade-offs — no per-message acknowledgement, no built-in delayed delivery, no
broker-side routing — are exactly what the retry and dead-letter machinery in
`OrderProcessor` has to build by hand. See
[KafkaVsRabbitMQ](https://github.com/bobhuang1/KafkaVsRabbitMQ) for the direct
comparison.

---

## Projects

| Project | Type | Role |
| --- | --- | --- |
| `src/KafkaDotNet.Contracts` | class library | The events (`OrderPlaced`, `OrderConfirmed`, `OrderRejected`), topic names and header keys. No Kafka dependency. |
| `src/KafkaDotNet.Messaging` | class library | Kafka plumbing: JSON serializer, options, config factory, partition keys, publishers, topic bootstrap, retry policy. |
| `src/KafkaDotNet.OrderApi` | ASP.NET Core | Publishes `OrderPlaced`. Runs on `http://localhost:5080`. |
| `src/KafkaDotNet.OrderProcessor` | worker service | Consumes, processes, retries, dead-letters. |
| `tests/KafkaDotNet.Tests` | xunit | 54 tests covering serialization, headers, ordering keys, config, the retry schedule and all four routing branches. |

---

## Prerequisites

- **.NET 10 SDK**
- **Docker** (for the broker) — or any reachable Kafka cluster

## Run it

**1. Start the broker**

```bash
docker compose up -d
```

This brings up a single-node Kafka in KRaft mode (no ZooKeeper) on
`localhost:9092`, plus a UI at <http://localhost:8080>.

**2. Start the worker** (leave it running)

```bash
dotnet run --project src/KafkaDotNet.OrderProcessor
```

**3. Start the API** (in a second terminal)

```bash
dotnet run --project src/KafkaDotNet.OrderApi
```

**4. Publish some orders**

```bash
# One order
curl -X POST http://localhost:5080/orders \
  -H 'Content-Type: application/json' \
  -d '{ "customerId": "customer-1", "items": [ { "sku": "SKU-001", "quantity": 2, "unitPrice": 19.99 } ] }'

# A spread of demo orders that exercise every branch
curl -X POST 'http://localhost:5080/orders/demo?count=6'
```

**5. Watch the worker's log.** The demo orders are deliberately chosen so that a
single call shows all four outcomes:

| Demo order | What happens | Where to look |
| --- | --- | --- |
| index 0 — `OUT-OF-STOCK` | permanent failure | `orders.rejected` |
| index 1 — total > 5000 | transient failure, then success on the retry | `orders.placed.retry` → `orders.confirmed` |
| the rest | processed immediately | `orders.confirmed` |
| a poison message you produce yourself | never deserializes | logged and skipped |
| a handler that never stops failing | attempts exhausted | `orders.placed.dlq` |

Inspect any topic at <http://localhost:8080> (Kafka UI), or from the broker:

```bash
docker exec kafkadotnet-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.confirmed --from-beginning
```

**6. Stop**

```bash
docker compose down -v
```

---

## How the delivery logic works

### Partitioning decides ordering

```csharp
// src/KafkaDotNet.Messaging/PartitionKeys.cs
public static string ForCustomer(string customerId) => customerId.Trim().ToLowerInvariant();
```

The key is hashed to a partition. Everything with the same key goes to the same
partition, and a partition is an append-only log, so per-customer order is
guaranteed. Change the key to `Guid.NewGuid()` and you get parallelism with no
ordering; key by customer and you get both. This one line is the most
consequential decision in the pipeline, which is why it has a name and tests.

### Committing offsets is what "at-least-once" means

```csharp
// src/KafkaDotNet.OrderProcessor/Consumers/ConsumedRecordHandler.cs
var outcome = await processor.ProcessAsync(key, order, attempt, cancellationToken);
consumer.Commit(result);   // <- only AFTER the work succeeded
```

`EnableAutoCommit = false` plus a manual commit *after* the work is the standard
at-least-once pattern. Commit first and you have at-most-once (orders vanish on a
crash); commit after and a crash means the order is processed twice, so the
handler must be idempotent. That is the trade every queue makes in some form, and
Kafka makes you choose it explicitly.

### Retries, with a delay Kafka does not provide

Kafka has no "deliver this again in 5 seconds" feature, so it is built here in two
pieces:

1. the producer stamps a `not-before` header and writes to `orders.placed.retry`
   (same key, so the message keeps its partition);
2. `RetryConsumer` re-reads the record only once that time has passed — it seeks
   back onto the record and **pauses the partition** rather than sleeping, so the
   partition's position is preserved and ordering is not broken.

The schedule itself is a plain `RetryPolicy` (exponential with a ceiling),
unit-tested in isolation:

```
attempt 1: 2s    attempt 2: 4s    attempt 3: 8s    attempt 4: 16s (cap 30s)
```

### Dead-lettering instead of losing messages

When the attempts run out, the *original* event is published to
`orders.placed.dlq` with the failure reason in a header, and the offset is
committed. Nothing is dropped and nothing loops forever. (A *permanent* failure —
an out-of-stock SKU — skips the retry schedule entirely and becomes an
`OrderRejected`, because retrying it would only waste time.)

### Fan-out with consumer groups

`NotificationConsumer` subscribes to the same `orders.placed` topic in its own
group. Both it and the processor see every order. That is the difference from a
classic queue: a consumer group owns its offsets, so "who has read what" is
tracked per group.

---

## Configuration

`appsettings.json` in each app:

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "ConsumerGroup": "order-processor",
    "RetryConsumerGroup": "order-processor-retry",
    "PollTimeoutMs": 500,
    "Retry": {
      "MaxAttempts": 4,
      "InitialDelay": "00:00:02",
      "BackoffMultiplier": 2.0,
      "MaxDelay": "00:00:30"
    }
  }
}
```

Point `BootstrapServers` at a real cluster and nothing else changes.

---

## Tests

```bash
dotnet test
```

The suite runs with **no broker and no Docker**: the routing logic depends on
`IEventPublisher<T>`, so a recording fake captures exactly what would have been
produced.

```
Passed! - Failed: 0, Passed: 54, Skipped: 0, Total: 54
```

---

## Things worth knowing

- **Topic count is a real limit.** Consumers in a group are capped at the
  partition count — a fourth worker on a three-partition topic sits idle.
- **`AutoOffsetReset` only applies to a group with no committed offsets.** It is
  not "start here every time".
- **An uncommitted offset is not an instant redelivery.** Within a running
  session the consumer's position has already moved on; the replay happens after
  a restart/rebalance. That is why the sample retries *explicitly* through the
  retry topic instead of relying on redelivery.
- **Idempotent producer ≠ idempotent consumer.** `EnableIdempotence` stops the
  producer's own retries from duplicating a write. Duplicate *deliveries* to your
  handler are still possible and are your handler's problem.
- **The retry consumer pauses partitions, it does not sleep.** Sleeping would
  block the poll loop and risk a rebalance (`MaxPollIntervalMs`).

---

## License

This project is free software, released under the **GNU General Public License
v3.0**. You may redistribute and/or modify it under those terms; see
[LICENSE.md](LICENSE.md) for the full text.
