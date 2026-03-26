from __future__ import annotations

from server import TapeHub, _filter_trades, _to_ms


def test_to_ms_normalizes_seconds_to_milliseconds() -> None:
    assert _to_ms(1_700_000_000) == 1_700_000_000_000


def test_to_ms_keeps_millisecond_values() -> None:
    assert _to_ms(1_700_000_000_123) == 1_700_000_000_123


def test_mk_key_is_stable_for_small_float_variance() -> None:
    hub = TapeHub(maxlen=64)
    a = {
        "source": "x",
        "exchange": "y",
        "ts_ms": 1700000000000,
        "price": 70880.123456,
        "size_usd": 512345.67891,
        "side": "buy",
    }
    b = {
        "source": "x",
        "exchange": "y",
        "ts_ms": 1700000000000,
        "price": 70880.123499,
        "size_usd": 512345.67912,
        "side": "buy",
    }
    assert hub._mk_key(a) == hub._mk_key(b)


def test_filter_trades_market_and_source() -> None:
    trades = [
        {"exchange": "binance_perps", "source": "binance_perps"},
        {"exchange": "bybit_spot", "source": "bybit_spot"},
        {"exchange": "coinbase", "source": "coinbase"},
        {"exchange": "okx_perps", "source": "okx_swap"},
    ]
    perp = _filter_trades(trades, market="perp", sources=None)
    assert [t["exchange"] for t in perp] == ["binance_perps", "okx_perps"]
    spot = _filter_trades(trades, market="spot", sources=None)
    assert [t["exchange"] for t in spot] == ["bybit_spot", "coinbase"]
    scoped = _filter_trades(trades, market="all", sources="okx_perps,coinbase")
    assert [t["exchange"] for t in scoped] == ["coinbase", "okx_perps"]
    src_scoped = _filter_trades(trades, market="all", sources="okx_swap")
    assert [t["exchange"] for t in src_scoped] == ["okx_perps"]
