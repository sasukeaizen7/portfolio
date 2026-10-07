"""Simulate the source system: one folder of JSON-lines files per day, written by project 09's click
generator (so the landing data has duplicates, late events and malformed lines). From day 3 the source
starts sending a new field, `device`: a schema change the pipeline must absorb without breaking."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from stream.events import ClickGenerator

from .common import LANDING


def land_day(day: str, events: int = 20_000, seed: int | None = None, add_device: bool = False) -> int:
    gen = ClickGenerator(seed=seed if seed is not None else int(day.replace("-", "")))
    start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    folder = LANDING / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    written = 0
    for hour in range(24):                                     # one file per hour, like a real export
        lines = []
        for key, value in gen.batch(events // 24, start + timedelta(hours=hour, minutes=30)):
            if add_device and key != "bad":                    # malformed lines stay as they are, for the quarantine
                record = json.loads(value)
                record["device"] = "mobile" if int(record["event_id"], 16) % 3 else "desktop"   # ~2/3 mobile
                value = json.dumps(record).encode()
            lines.append(value.decode())
        (folder / f"part-{hour:02d}.json").write_text("\n".join(lines) + "\n", encoding="utf-8")
        written += len(lines)
    return written
