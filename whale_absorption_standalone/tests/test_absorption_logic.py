from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import _compute_absorption_score, _notional_from_size


def test_notional_contract_multiplier() -> None:
    n = _notional_from_size("mexc_perps", price=70000.0, size=1000.0)
    assert round(n, 2) == 70000.0


def test_notional_quote_rule() -> None:
    n = _notional_from_size("bitmex_perps", price=70000.0, size=120000.0)
    assert n == 120000.0


def test_quote_field_priority() -> None:
    n = _notional_from_size("default", price=70000.0, size=0.5, row={"notional_usd": 250000})
    assert n == 250000.0


def test_score_improves_with_more_flow_and_lower_displacement() -> None:
    low = _compute_absorption_score(
        total_usd=1_000_000,
        min_burst_usd=1_000_000,
        trade_count=4,
        min_hits=4,
        unique_exchange_count=2,
        displacement_bps=4.5,
        max_displacement_bps=3.0,
        dominant_share=0.60,
    )
    high = _compute_absorption_score(
        total_usd=3_000_000,
        min_burst_usd=1_000_000,
        trade_count=9,
        min_hits=4,
        unique_exchange_count=4,
        displacement_bps=1.2,
        max_displacement_bps=3.0,
        dominant_share=0.76,
    )
    assert high > low


def test_score_penalizes_trend_continuation_displacement() -> None:
    a = _compute_absorption_score(
        total_usd=2_500_000,
        min_burst_usd=1_000_000,
        trade_count=8,
        min_hits=4,
        unique_exchange_count=3,
        displacement_bps=1.0,
        max_displacement_bps=3.0,
        dominant_share=0.70,
    )
    b = _compute_absorption_score(
        total_usd=2_500_000,
        min_burst_usd=1_000_000,
        trade_count=8,
        min_hits=4,
        unique_exchange_count=3,
        displacement_bps=6.0,
        max_displacement_bps=3.0,
        dominant_share=0.70,
    )
    assert a > b
