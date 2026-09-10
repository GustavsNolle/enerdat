"""Delivery windows must follow the calendar, not a fixed 24 hours."""

import pandas as pd
import pytest

from enerdat.config import DECISION_TIME_LOCAL, MARKET_TZ
from enerdat.partitions import decision_deadline, delivery_window

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
    "partition_key,utc_offset_hours",
    [
        ("2026-08-28", 2),  # CEST
        ("2026-01-15", 1),  # CET
    ],
)
def test_decision_deadline_tracks_the_utc_offset(partition_key, utc_offset_hours):
    """Derived from the configured local hour, so changing it cannot leave a
    stale literal behind -- and the UTC answer must move with DST."""
    expected = (
        pd.Timestamp(partition_key, tz=MARKET_TZ)
        - pd.DateOffset(days=1)
        + pd.Timedelta(hours=DECISION_TIME_LOCAL.hour)
    ).tz_convert("UTC")
    assert decision_deadline(partition_key) == expected
    assert decision_deadline(partition_key).hour == (
        DECISION_TIME_LOCAL.hour - utc_offset_hours
    )


def test_decision_deadline_precedes_delivery():
    for key in ["2026-08-28", SPRING_FORWARD, FALL_BACK, "2026-01-15"]:
        start, _ = delivery_window(key)
        assert decision_deadline(key) < start.tz_convert("UTC")
