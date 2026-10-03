import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))
FIXTURES = ROOT / "tests" / "fixtures"


@pytest.fixture
def snapshot():
    from velib.gbfs import Snapshot

    return Snapshot(datetime(2026, 10, 3, 16, 0, 41, tzinfo=UTC),
                    json.loads((FIXTURES / "station_information.json").read_text(encoding="utf-8")),
                    json.loads((FIXTURES / "station_status.json").read_text(encoding="utf-8")))
