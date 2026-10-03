"""Produce fake click events to Kafka at a steady rate.

    python -m stream.producer --rate 200 --seconds 300

Producer settings that matter: acks=all (a write counts only once every in-sync replica has it) and
enable.idempotence (the broker drops the producer's own retry duplicates). The generator still sends
some duplicates on purpose, standing in for upstream systems that resend.
"""

from __future__ import annotations

import argparse
import os
import time

from confluent_kafka import Producer

from .events import ClickGenerator

TOPIC = os.environ.get("CLICKS_TOPIC", "clicks")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=int, default=100, help="events per second")
    p.add_argument("--seconds", type=int, default=0, help="0 = run forever")
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                         "acks": "all", "enable.idempotence": True, "linger.ms": 20, "compression.type": "zstd"})
    failures = []
    gen = ClickGenerator(seed=args.seed)
    started, sent = time.monotonic(), 0
    while not args.seconds or time.monotonic() - started < args.seconds:
        tick = time.monotonic()
        for key, value in gen.batch(args.rate):
            producer.produce(TOPIC, key=key, value=value,
                             on_delivery=lambda err, msg: failures.append(err) if err else None)
            sent += 1
        producer.poll(0)                                    # serve delivery callbacks
        time.sleep(max(0.0, 1 - (time.monotonic() - tick)))
        if int(time.monotonic() - started) % 10 == 0:
            print(f"  sent {sent:,} messages, {len(failures)} delivery failures", flush=True)
    producer.flush(30)
    print(f"done: {sent:,} messages, {len(failures)} delivery failures")


if __name__ == "__main__":
    main()
