"""The cost-optimal quantile, and why it is not the mean."""

import numpy as np
import pandas as pd
import pytest

from enerdat.assets.model import cost_optimal_tau
from enerdat.config import TAU_BOUNDS


def _history(day_ahead, long_price, short_price, n=500):
    return pd.DataFrame(
        {
            "price_day_ahead": np.full(n, float(day_ahead)),
            "price_long": np.full(n, float(long_price)),
            "price_short": np.full(n, float(short_price)),
        }
    )


def test_symmetric_prices_give_the_median():
    """When both directions cost the same, the best schedule is the middle."""
    tau = cost_optimal_tau(_history(day_ahead=100, long_price=80, short_price=120))
    assert tau == pytest.approx(0.5)


def test_expensive_shorts_bias_the_schedule_low():
    """Short costs 4x what long costs, so under-promise.

    c_long = 100-90 = 10, c_short = 140-100 = 40, tau = 10/50 = 0.2.
    Scheduling the 20th percentile means being long four times out of five --
    deliberately, because the rare short is what hurts.
    """
    tau = cost_optimal_tau(_history(day_ahead=100, long_price=90, short_price=140))
    assert tau == pytest.approx(0.2)
    assert tau < 0.5


def test_expensive_longs_bias_the_schedule_high():
    tau = cost_optimal_tau(_history(day_ahead=100, long_price=40, short_price=110))
    assert tau == pytest.approx(60 / 70)
    assert tau > 0.5


def test_a_profitable_direction_cannot_argue_for_recklessness():
    """A long price ABOVE day-ahead means that hour paid to over-deliver.

    Floored at zero rather than allowed to offset: otherwise a handful of
    profitable hours would drag tau to an extreme and the schedule with it.
    """
    frame = _history(day_ahead=100, long_price=90, short_price=140, n=400)
    frame.loc[:19, "price_long"] = 500.0  # 20 hours where being long paid
    tau = cost_optimal_tau(frame)
    low, high = TAU_BOUNDS
    assert low <= tau <= high
    # Those hours contribute 0 to c_long rather than a large negative
    assert tau < 0.25


def test_tau_is_clamped_when_one_side_never_costs():
    tau = cost_optimal_tau(_history(day_ahead=100, long_price=100, short_price=200))
    low, _ = TAU_BOUNDS
    assert tau == pytest.approx(low)


def test_missing_prices_fall_back_rather_than_guess():
    frame = _history(day_ahead=100, long_price=90, short_price=140)
    assert cost_optimal_tau(frame.drop(columns=["price_day_ahead"])) is None
    assert cost_optimal_tau(frame.assign(price_long=np.nan)) is None


def test_single_pricing_disarms_the_quantile_objective():
    """The degeneracy that produced a fictional billion euros.

    Under one imbalance price both directions share a slope, so settlement cost
    is linear in the schedule and its optimum is not interior -- it runs to
    zero or to infinity. A model given that objective under-nominates its way
    to an imaginary profit. Detect it and fall back rather than "optimise" it.

    ENTSO-E labels the columns Long and Short regardless of regime, so this is
    invisible until the values are compared, and most of Europe is single
    priced.
    """
    frame = _history(day_ahead=100, long_price=250, short_price=250)
    assert cost_optimal_tau(frame) is None


def test_dual_pricing_still_yields_a_quantile():
    frame = _history(day_ahead=100, long_price=90, short_price=140)
    assert cost_optimal_tau(frame) == pytest.approx(0.2)


def test_a_single_differing_interval_is_enough_to_keep_it():
    """Near-identical is not identical; only exact equality disarms it."""
    frame = _history(day_ahead=100, long_price=90, short_price=90)
    frame.loc[0, "price_short"] = 500.0
    assert cost_optimal_tau(frame) is not None
