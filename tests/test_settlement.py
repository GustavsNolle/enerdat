"""Settlement sign conventions. Every imbalance bug is a sign bug."""

import dagster as dg
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

from enerdat.assets.settlement import imbalance_settlement_mart
from enerdat.resources import LakeResource

DELIVERY_DATE = "2026-08-28"
T0 = pd.Timestamp("2026-08-29T06:00:00Z")


def _setup(tmp_path, actual, scheduled, price_long, price_short, n=4):
    """Build a tiny forecast_error table plus matching prices."""
    lake = LakeResource(root=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "s.duckdb"))
    index = pd.date_range("2026-08-28T00:00:00Z", periods=n, freq="15min")

    lake.write(
        "entsoe_imbalance_price",
        DELIVERY_DATE,
        pd.concat(
            [
                pd.DataFrame({"valid_time_utc": index, "variable": "Long",
                              "value": price_long, "zone": "NL",
                              "delivery_date": DELIVERY_DATE, "retrieved_at_utc": T0}),
                pd.DataFrame({"valid_time_utc": index, "variable": "Short",
                              "value": price_short, "zone": "NL",
                              "delivery_date": DELIVERY_DATE, "retrieved_at_utc": T0}),
            ],
            ignore_index=True,
        ),
        T0,
    )

    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE forecast_error AS SELECT * FROM (VALUES " +
                     ",".join(
                         f"(TIMESTAMPTZ '{t.isoformat()}', DATE '{DELIVERY_DATE}', "
                         f"{actual}::DOUBLE, {scheduled}::DOUBLE)" for t in index
                     ) +
                     ") AS t(valid_time_utc, delivery_date, actual_mw, tso_forecast_mw)")

    resources = {"lake": lake, "duckdb": duckdb}
    assert dg.materialize([imbalance_settlement_mart], resources=resources).success

    with duckdb.get_connection() as conn:
        return conn.execute(
            """SELECT tso_imbalance_mwh, tso_settlement_eur, tso_cost_eur, interval_hours
               FROM imbalance_settlement ORDER BY valid_time_utc"""
        ).fetchall()


def test_long_position_at_a_positive_price_earns(tmp_path):
    # over-delivered 8 MW for 15 min = +2 MWh, paid the long price of 50
    rows = _setup(tmp_path, actual=108, scheduled=100, price_long=50, price_short=200)
    mwh, settlement, cost, hours = rows[0]
    assert hours == pytest.approx(0.25)
    assert mwh == pytest.approx(2.0)
    assert settlement == pytest.approx(100.0)
    assert cost == pytest.approx(-100.0), "being long at a positive price is income"


def test_short_position_pays_the_short_price(tmp_path):
    # under-delivered 8 MW for 15 min = -2 MWh, charged the short price of 200
    rows = _setup(tmp_path, actual=92, scheduled=100, price_long=50, price_short=200)
    mwh, settlement, cost, _ = rows[0]
    assert mwh == pytest.approx(-2.0)
    assert settlement == pytest.approx(-400.0)
    assert cost == pytest.approx(400.0)


def test_short_uses_short_price_not_long(tmp_path):
    """Dual pricing: the direction of the error selects the price."""
    rows = _setup(tmp_path, actual=92, scheduled=100, price_long=1, price_short=500)
    _, _, cost, _ = rows[0]
    assert cost == pytest.approx(1000.0), "short settled at the long price"


def test_being_long_into_a_negative_price_costs_money(tmp_path):
    """The case that breaks any |error| * price model.

    Over-delivering when the system is already long is penalised, not rewarded.
    """
    rows = _setup(tmp_path, actual=108, scheduled=100, price_long=-70, price_short=10)
    mwh, settlement, cost, _ = rows[0]
    assert mwh == pytest.approx(2.0), "still a long position"
    assert settlement == pytest.approx(-140.0)
    assert cost == pytest.approx(140.0), "a long position at a negative price must cost"


def test_perfect_forecast_costs_nothing(tmp_path):
    rows = _setup(tmp_path, actual=100, scheduled=100, price_long=-70, price_short=500)
    for mwh, settlement, cost, _ in rows:
        assert mwh == pytest.approx(0.0)
        assert cost == pytest.approx(0.0)
