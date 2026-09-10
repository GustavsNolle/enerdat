"""Settlement sign conventions. Every imbalance bug is a sign bug."""

import dagster as dg
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

from enerdat.assets.settlement import imbalance_settlement_mart
from enerdat.resources import LakeResource

DELIVERY_DATE = "2026-08-28"
T0 = pd.Timestamp("2026-08-29T06:00:00Z")


def _setup(tmp_path, actual, scheduled, price_long, price_short, n=4,
           candidate=None, day_ahead=0.0):
    """Build a tiny candidate_forecast table plus matching prices."""
    lake = LakeResource(root=str(tmp_path / "raw"), zone="BE")
    duckdb = DuckDBResource(database=str(tmp_path / "s.duckdb"))
    index = pd.date_range("2026-08-28T00:00:00Z", periods=n, freq="15min")

    with duckdb.get_connection() as conn:
        cand = "NULL::DOUBLE" if candidate is None else f"{candidate}::DOUBLE"
        conn.execute("CREATE OR REPLACE TABLE candidate_forecast AS SELECT * FROM (VALUES " +
                     ",".join(
                         f"(TIMESTAMPTZ '{t.isoformat()}', DATE '{DELIVERY_DATE}', "
                         f"{actual}::DOUBLE, {scheduled}::DOUBLE, {cand}, "
                         f"{day_ahead}::DOUBLE, {price_long}::DOUBLE, {price_short}::DOUBLE)"
                         for t in index
                     ) +
                     ") AS t(valid_time_utc, delivery_date, actual_mw, tso_forecast_mw,"
                     " candidate_mw, price_day_ahead, price_long, price_short)")

    resources = {"lake": lake, "duckdb": duckdb}
    assert dg.materialize([imbalance_settlement_mart], resources=resources).success

    with duckdb.get_connection() as conn:
        return conn.execute(
            """SELECT tso_imbalance_mwh, tso_settlement_eur, tso_cost_eur,
                      interval_hours, candidate_cost_eur, savings_eur
               FROM imbalance_settlement ORDER BY valid_time_utc"""
        ).fetchall()


def test_long_position_at_a_positive_price_earns(tmp_path):
    # over-delivered 8 MW for 15 min = +2 MWh, paid the long price of 50
    rows = _setup(tmp_path, actual=108, scheduled=100, price_long=50, price_short=200)
    mwh, settlement, cost, hours, _, _ = rows[0]
    assert hours == pytest.approx(0.25)
    assert mwh == pytest.approx(2.0)
    assert settlement == pytest.approx(100.0)
    assert cost == pytest.approx(-100.0), "being long at a positive price is income"


def test_short_position_pays_the_short_price(tmp_path):
    # under-delivered 8 MW for 15 min = -2 MWh, charged the short price of 200
    rows = _setup(tmp_path, actual=92, scheduled=100, price_long=50, price_short=200)
    mwh, settlement, cost, _, _, _ = rows[0]
    assert mwh == pytest.approx(-2.0)
    assert settlement == pytest.approx(-400.0)
    assert cost == pytest.approx(400.0)


def test_short_uses_short_price_not_long(tmp_path):
    """Dual pricing: the direction of the error selects the price."""
    rows = _setup(tmp_path, actual=92, scheduled=100, price_long=1, price_short=500)
    _, _, cost, _, _, _ = rows[0]
    assert cost == pytest.approx(1000.0), "short settled at the long price"


def test_being_long_into_a_negative_price_costs_money(tmp_path):
    """The case that breaks any |error| * price model.

    Over-delivering when the system is already long is penalised, not rewarded.
    """
    rows = _setup(tmp_path, actual=108, scheduled=100, price_long=-70, price_short=10)
    mwh, settlement, cost, _, _, _ = rows[0]
    assert mwh == pytest.approx(2.0), "still a long position"
    assert settlement == pytest.approx(-140.0)
    assert cost == pytest.approx(140.0), "a long position at a negative price must cost"


def test_perfect_forecast_costs_nothing(tmp_path):
    rows = _setup(tmp_path, actual=100, scheduled=100, price_long=-70, price_short=500)
    for mwh, settlement, cost, _, _, _ in rows:
        assert mwh == pytest.approx(0.0)
        assert cost == pytest.approx(0.0)


def test_savings_is_null_without_a_candidate(tmp_path):
    rows = _setup(tmp_path, actual=92, scheduled=100, price_long=50, price_short=200)
    for *_, candidate_cost, savings in rows:
        assert candidate_cost is None
        assert savings is None, "no candidate schedule means no claim of savings"


def test_a_perfect_candidate_saves_the_whole_baseline_cost(tmp_path):
    rows = _setup(
        tmp_path, actual=92, scheduled=100,
        price_long=50, price_short=200, candidate=92,
    )
    _, _, tso_cost, _, candidate_cost, savings = rows[0]
    assert candidate_cost == pytest.approx(0.0)
    assert savings == pytest.approx(tso_cost)
    assert savings == pytest.approx(400.0)


def test_a_worse_candidate_shows_negative_savings(tmp_path):
    """Losing to the TSO must report as a loss, not an absolute value.

    The candidate is short by 28 MW where the TSO was short by 8, so it is
    deeper into the expensive direction and must settle worse.
    """
    rows = _setup(
        tmp_path, actual=92, scheduled=100,
        price_long=50, price_short=200, candidate=120,
    )
    _, _, tso_cost, _, candidate_cost, savings = rows[0]
    assert tso_cost == pytest.approx(400.0)
    assert candidate_cost == pytest.approx(1400.0)
    assert savings == pytest.approx(-1000.0)


def test_a_bigger_absolute_error_can_settle_cheaper(tmp_path):
    """Cost is not a function of |error|, and this is why MAE is the wrong metric.

    The TSO is short by 8 MW and pays the 200 EUR short price. The candidate is
    wrong by 32 MW -- four times the error -- but in the *long* direction, so it
    is paid the 50 EUR long price and earns money. Optimising MAE would pick the
    TSO schedule; optimising euros picks the other one.
    """
    rows = _setup(
        tmp_path, actual=92, scheduled=100,
        price_long=50, price_short=200, candidate=60,
    )
    _, _, tso_cost, _, candidate_cost, savings = rows[0]
    assert abs(92 - 60) > abs(92 - 100), "candidate should have the larger error"
    assert tso_cost == pytest.approx(400.0)
    assert candidate_cost == pytest.approx(-400.0), "long at a positive price earns"
    assert savings == pytest.approx(800.0)


def test_opportunity_cost_is_measured_against_the_day_ahead_price(tmp_path):
    """The schedule was already sold at day-ahead, so only the spread costs.

    Short by 8 MW for 15 min = -2 MWh, bought back at the 200 short price
    having sold at 100: the loss is the 100 spread on 2 MWh, not the full 400.
    """
    rows = _setup(
        tmp_path, actual=92, scheduled=100,
        price_long=50, price_short=200, day_ahead=100,
    )
    mwh, settlement, cost, _, _, _ = rows[0]
    assert mwh == pytest.approx(-2.0)
    assert settlement == pytest.approx(-400.0), "raw cash exchanged is unchanged"
    assert cost == pytest.approx(200.0), "opportunity cost is the spread only"


def test_imbalance_at_the_day_ahead_price_is_free(tmp_path):
    """Deviating costs nothing when the imbalance price equals day-ahead.

    Raw settlement still shows cash moving, which is exactly why it is the
    wrong thing to minimise.
    """
    rows = _setup(
        tmp_path, actual=92, scheduled=100,
        price_long=100, price_short=100, day_ahead=100,
    )
    _, settlement, cost, _, _, _ = rows[0]
    assert settlement == pytest.approx(-200.0)
    assert cost == pytest.approx(0.0)
