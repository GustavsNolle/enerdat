"""Delivery windows must follow the calendar, not a fixed 24 hours."""

import pandas as pd
import pytest

from enerdat.partitions import delivery_window, gate_closure

# EU DST transitions in 2026: last Sunday of March / October.
SPRING_FORWARD = "2026-03-29"  # 23-hour day
FALL_BACK = "2026-10-25"       # 25-hour day


@pytest.mark.parametrize(
    "partition_key,expected_hours",
    [
        ("2026-08-28", 24),
        (SPRING_FORWARD, 23),
        (FALL_BACK, 25),
        ("2026-01-15", 24),
    ],
)
def test_delivery_window_length(partition_key, expected_hours):
    start, end = delivery_window(partition_key)
    actual = (end - start).total_seconds() / 3600
    assert actual == expected_hours, (
        f"{partition_key} spans {actual}h, expected {expected_hours}h. "
        "A fixed Timedelta(days=1) would give 24 for every day and silently "
        "drop or duplicate an hour twice a year."
    )


def test_delivery_window_covers_whole_local_day():
    start, end = delivery_window(FALL_BACK)
    assert start.strftime("%H:%M") == "00:00"
    assert end.strftime("%H:%M") == "00:00"
    assert end.date() == pd.Timestamp("2026-10-26").date()


@pytest.mark.parametrize(
    "partition_key,expected_utc",
    [
        # CEST (UTC+2): 12:00 local on D-1 is 10:00 UTC
        ("2026-08-28", "2026-08-27 10:00:00+00:00"),
        # CET (UTC+1): 12:00 local on D-1 is 11:00 UTC
        ("2026-01-15", "2026-01-14 11:00:00+00:00"),
    ],
)
def test_gate_closure_tracks_the_utc_offset(partition_key, expected_utc):
    assert str(gate_closure(partition_key)) == expected_utc


def test_gate_closure_precedes_delivery():
    for key in ["2026-08-28", SPRING_FORWARD, FALL_BACK, "2026-01-15"]:
        start, _ = delivery_window(key)
        assert gate_closure(key) < start.tz_convert("UTC")
