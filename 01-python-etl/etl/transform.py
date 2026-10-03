"""Transform: raw API JSON -> a typed, validated pandas DataFrame (one row per city and day).

Validation happens here, before anything is written: a pipeline that loads bad data and fails
later is much harder to debug than one that refuses the batch with a clear message.
"""

from __future__ import annotations

import pandas as pd

from .extract import DAILY_VARIABLES, City

COLUMNS = {
    "time": "date",
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "precipitation_sum": "precipitation_mm",
    "wind_speed_10m_max": "wind_max_kmh",
}
SCHEMA = {
    "city": "string",
    "date": "datetime64[ns]",
    "temp_max_c": "float64",
    "temp_min_c": "float64",
    "precipitation_mm": "float64",
    "wind_max_kmh": "float64",
}


class ValidationError(ValueError):
    pass


def to_frame(payload: dict, city: City) -> pd.DataFrame:
    daily = payload.get("daily")
    if not daily or "time" not in daily:
        raise ValidationError(f"{city.name}: payload has no 'daily.time' block")
    missing = [v for v in DAILY_VARIABLES if v not in daily]
    if missing:
        raise ValidationError(f"{city.name}: missing variables {missing}")
    lengths = {k: len(daily[k]) for k in ["time", *DAILY_VARIABLES]}
    if len(set(lengths.values())) != 1:
        raise ValidationError(f"{city.name}: arrays have different lengths {lengths}")

    df = pd.DataFrame({new: daily[old] for old, new in COLUMNS.items()})
    df.insert(0, "city", city.name)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df = df.astype(SCHEMA)
    validate(df)
    return df


def validate(df: pd.DataFrame) -> None:
    """Business rules. Missing values are allowed (the API returns null for days it has no data)."""
    problems = []
    if df.duplicated(["city", "date"]).any():
        problems.append("duplicate (city, date) rows")
    both = df.dropna(subset=["temp_min_c", "temp_max_c"])
    if (both["temp_min_c"] > both["temp_max_c"]).any():
        problems.append("temp_min_c above temp_max_c")
    if (df["temp_max_c"].dropna().abs() > 60).any():
        problems.append("temperature outside [-60, 60] °C")
    if (df["precipitation_mm"].dropna() < 0).any() or (df["wind_max_kmh"].dropna() < 0).any():
        problems.append("negative precipitation or wind")
    if problems:
        raise ValidationError("; ".join(problems))
