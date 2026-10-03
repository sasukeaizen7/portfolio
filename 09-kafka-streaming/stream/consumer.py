"""Consume click events into Postgres. Run several copies with the same group id: Kafka splits the
topic's partitions between them, and reassigns them if one stops (consumer-group rebalancing).

    python -m stream.consumer

Offsets are committed manually, AFTER the database commit -> at-least-once delivery; the sink's
idempotent writes turn that into exactly-once results. Invalid messages go to the dead-letter topic.
"""

from __future__ import annotations

import os
import signal

import psycopg
from confluent_kafka import Consumer, KafkaError, Producer

from .sink import ensure_schema, write_batch

TOPIC = os.environ.get("CLICKS_TOPIC", "clicks")
DLQ_TOPIC = os.environ.get("CLICKS_DLQ_TOPIC", "clicks.dlq")
running = True


def stop(*_):
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": os.environ.get("CONSUMER_GROUP", "clicks-to-postgres"),
        "enable.auto.commit": False,            # we commit after the DB commit, never before
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    })
    dlq = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    consumer.subscribe([TOPIC], on_assign=lambda c, parts: print(f"  assigned {[p.partition for p in parts]}", flush=True))
    conn = psycopg.connect(os.environ["PG_DSN"])
    ensure_schema(conn)
    totals = {"inserted": 0, "duplicates": 0, "invalid": 0}
    try:
        while running:
            msgs = consumer.consume(num_messages=500, timeout=1.0)
            batch = []
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print(f"  kafka error: {m.error()}", flush=True)
                    continue
                batch.append((m.value(), m.partition(), m.offset()))
            if not batch:
                continue
            result = write_batch(conn, batch)                     # DB transaction commits here
            for raw, error in result["invalid"]:
                dlq.produce(DLQ_TOPIC, value=raw, headers={"error": error})
            dlq.flush(10)
            consumer.commit(asynchronous=False)                    # ...and only then the offsets
            for k in totals:
                totals[k] += len(result[k]) if k == "invalid" else result[k]
            print(f"  batch {len(batch)}: +{result['inserted']} new, {result['duplicates']} duplicates, "
                  f"{len(result['invalid'])} to DLQ | totals {totals}", flush=True)
    finally:
        consumer.close()                                         # leave the group cleanly -> fast rebalance
        conn.close()


if __name__ == "__main__":
    main()
