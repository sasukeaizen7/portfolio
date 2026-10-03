import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def paris_payload() -> dict:
    """A real Open-Meteo response for Paris, 1-3 January 2024."""
    return json.loads((FIXTURES / "paris_2024-01-01_03.json").read_text(encoding="utf-8"))
