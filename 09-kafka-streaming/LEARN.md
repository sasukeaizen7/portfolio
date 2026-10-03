# Learning notes: 09 · Kafka

## Vocabulary, with this project's examples
- **Topic**: a named, append-only log (`clicks`). **Partition**: one ordered shard of a topic (3 here). Order is guaranteed *within* a partition only.
- **Key**: decides the partition (hash of the key). Keying by `user_id` keeps each user's events in order.
- **Offset**: the position of a message in a partition. A consumer group stores *its* offsets, so several groups can read the same topic independently.
- **Consumer group**: consumers sharing a `group.id` split the partitions. More consumers than partitions means idle consumers, so partitions cap parallelism.
- **Rebalance**: when a consumer joins or leaves, partitions are reassigned. `cooperative-sticky` moves only the partitions that must move.
- **acks=all + idempotent producer**: a write is acknowledged once all in-sync replicas have it, and the broker drops the producer's own retry duplicates.
- **KRaft**: Kafka's built-in consensus, which replaced ZooKeeper (removed in Kafka 4).

## Questions you should be able to answer
**At-most-once, at-least-once, exactly-once: which one is this?** Commit offsets *before* processing and a crash loses messages (at-most-once). Commit *after* and a crash re-processes them (at-least-once, what this consumer does). Exactly-once *results* then come from idempotent writes. Kafka transactions can do end-to-end exactly-once from topic to topic, but not into an external database. That's why "idempotent sink" is the standard answer.

**Why not increment the counters for every received event?** A replayed batch would increment them again. Counting only the rows that `RETURNING` reports as newly inserted ties the aggregate to the deduplicated table, in the same transaction.

**Why a dead-letter queue instead of skipping or crashing?** Crashing on a poison message blocks the partition forever, because the consumer re-reads it after every restart. Skipping silently loses data. A DLQ keeps the pipeline flowing *and* keeps the bad message for inspection and replay.

**Event time vs processing time?** A purchase made at 12:01 on a phone that reconnects at 12:20 belongs to the 12:01 minute. Aggregating by event time is correct, but a window can then change after it "closed". Stream processors (Flink, Spark Structured Streaming) handle this with **watermarks**: how late is too late.

**Kafka vs Kinesis?** Same model (stream → shards/partitions → consumers with checkpoints). Kinesis is managed by AWS with per-shard throughput limits and shorter retention by default. Kafka is open source and self-hosted, or managed with MSK or Confluent.

## Exercises
1. Add a third consumer, then a fourth. Explain why the fourth stays idle.
2. Write a **lag monitor**: per partition, the latest offset minus the group's committed offset (`consumer.get_watermark_offsets` + `committed`).
3. Make the DLQ idempotent too, so a replay doesn't store the same dead letter twice. Hint: hash the payload.
4. Add a **watermark**: ignore events more than 15 minutes late, and count them in a `too_late` table.
5. Rewrite the consumer with Spark Structured Streaming (`readStream.format("kafka")`, `foreachBatch`) and compare.
6. Add a JSON Schema (or Avro + Schema Registry) and reject events that don't match it.

## Interview pitch
"I built a streaming pipeline: a producer sends a deliberately messy clickstream (duplicates, late events, malformed messages) to a 3-partition Kafka topic keyed by user, and a consumer group writes it to Postgres. Offsets are committed after the database transaction, so delivery is at-least-once. The writes are idempotent, and the per-minute aggregates are built only from newly inserted rows, so results are exactly-once. A test replays a whole batch and checks that nothing doubles. Bad messages go to a dead-letter topic instead of blocking the partition."
