"""Daily partitions, and the market-time window each one covers.

One partition == one delivery day D == one ENTSO-E API call per dataset.
"""

from __future__ import annotations

import dagster as dg
import pandas as pd

from enerdat.config import DECISION_TIME_LOCAL, MARKET_TZ, PARTITION_START

daily_partitions = dg.DailyPartitionsDefinition(
    start_date=PARTITION_START,
    timezone=MARKET_TZ,
    # ENTSO-E publishes the day-ahead forecast for D during D-1, so D is
    # fetchable before it starts. end_offset=1 makes tomorrow a valid partition.
    end_offset=1,
)


def delivery_window(partition_key: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Market-time [start, end) for delivery day `partition_key`.

    Uses calendar arithmetic (DateOffset), never Timedelta(days=1): on DST
    transition days the window is 23 or 25 hours long, and a fixed 24-hour
    offset silently drops or duplicates an hour twice a year.
    """
    start = pd.Timestamp(partition_key, tz=MARKET_TZ)
    end = start + pd.DateOffset(days=1)
    return start, end


def decision_deadline(partition_key: str) -> pd.Timestamp:
    """The instant the information set for delivery day D is frozen.

    DECISION_TIME_LOCAL on D-1, returned in UTC. Nothing published after this
    may inform the schedule for D.
    """
    start, _ = delivery_window(partition_key)
    previous_day = (start - pd.DateOffset(days=1)).date()
    local = pd.Timestamp(
        dt_combine(previous_day, DECISION_TIME_LOCAL), tz=MARKET_TZ
    )
    return local.tz_convert("UTC")


def dt_combine(date, time):
    import datetime as _dt

    return _dt.datetime.combine(date, time)


# Kept so older call sites and tests keep working; the deadline is the same
# object, only its name changed when it stopped being the auction's.
gate_closure = decision_deadline
