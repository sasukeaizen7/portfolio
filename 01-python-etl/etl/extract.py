"""Extract: daily weather from the Open-Meteo archive API (free, no API key).

The HTTP layer is the part of a pipeline most likely to fail, so it gets explicit timeouts,
retries with exponential backoff on transient errors (429 and 5xx, network errors), and fails
loudly on anything else (a 400 means our request is wrong: retrying won't fix it).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date

import requests

log = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARIABLES = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "wind_speed_10m_max"]
RETRY_STATUSES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class City:
    name: str
    latitude: float
    longitude: float


CITIES = {
    "paris": City("Paris", 48.8566, 2.3522),
    "lyon": City("Lyon", 45.7640, 4.8357),
    "marseille": City("Marseille", 43.2965, 5.3698),
    "lille": City("Lille", 50.6292, 3.0573),
    "bordeaux": City("Bordeaux", 44.8378, -0.5792),
}


class ExtractError(RuntimeError):
    pass


def fetch_daily_weather(city: City, start: date, end: date, *, session: requests.Session | None = None,
                        attempts: int = 4, backoff_s: float = 1.0, timeout_s: float = 30.0) -> dict:
    """Return the raw JSON payload for one city and date range."""
    if end < start:
        raise ValueError(f"end ({end}) is before start ({start})")
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "Europe/Paris",
    }
    http = session or requests.Session()
    for attempt in range(1, attempts + 1):
        try:
            response = http.get(ARCHIVE_URL, params=params, timeout=timeout_s)
        except (requests.ConnectionError, requests.Timeout) as e:
            error = f"network error: {e}"
        else:
            if response.status_code == 200:
                return response.json()
            if response.status_code not in RETRY_STATUSES:
                raise ExtractError(f"{city.name}: HTTP {response.status_code}: {response.text[:200]}")
            error = f"HTTP {response.status_code}"
        if attempt == attempts:
            raise ExtractError(f"{city.name}: giving up after {attempts} attempts ({error})")
        wait = backoff_s * 2 ** (attempt - 1)
        log.warning("%s: attempt %d failed (%s), retrying in %.1fs", city.name, attempt, error, wait)
        time.sleep(wait)
    raise AssertionError("unreachable")
