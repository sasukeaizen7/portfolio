import copy

import pytest

from etl.extract import CITIES
from etl.transform import SCHEMA, ValidationError, to_frame

PARIS = CITIES["paris"]


def test_frame_has_schema_and_values(paris_payload):
    df = to_frame(paris_payload, PARIS)
    assert list(df.columns) == list(SCHEMA)
    assert len(df) == 3 and (df["city"] == "Paris").all()
    assert df.loc[1, "precipitation_mm"] == pytest.approx(13.6)
    assert str(df["date"].dtype) == "datetime64[ns]"


def test_nulls_are_kept_not_rejected(paris_payload):
    p = copy.deepcopy(paris_payload)
    p["daily"]["precipitation_sum"][0] = None
    assert to_frame(p, PARIS)["precipitation_mm"].isna().sum() == 1


@pytest.mark.parametrize("mutate, message", [
    (lambda d: d.pop("wind_speed_10m_max"), "missing variables"),
    (lambda d: d["temperature_2m_max"].pop(), "different lengths"),
    (lambda d: d.__setitem__("temperature_2m_min", [20.0, 20.0, 20.0]), "temp_min_c above temp_max_c"),
    (lambda d: d.__setitem__("precipitation_sum", [-1.0, 0.0, 0.0]), "negative precipitation"),
    (lambda d: d.__setitem__("time", ["2024-01-01"] * 3), "duplicate"),
])
def test_bad_batches_are_refused(paris_payload, mutate, message):
    p = copy.deepcopy(paris_payload)
    mutate(p["daily"])
    with pytest.raises(ValidationError, match=message):
        to_frame(p, PARIS)


def test_missing_daily_block():
    with pytest.raises(ValidationError, match="no 'daily.time'"):
        to_frame({"error": True}, PARIS)
