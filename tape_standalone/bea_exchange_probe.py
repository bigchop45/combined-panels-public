#!/usr/bin/env python3
"""
Discover BEA BTC symbols and tape exchanges quickly.

Usage:
  .venv/bin/python bea_exchange_probe.py
  .venv/bin/python bea_exchange_probe.py --samples 3 --limit 300
"""

from __future__ import annotations

import argparse
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv


def _normalize_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for k in ("trades", "prints", "data", "tape", "rows", "items", "events", "lines"):
        v = payload.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    data = payload.get("data")
    if isinstance(data, dict):
        for k in ("trades", "prints", "tape", "rows", "items"):
            v = data.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _normalize_history_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return [x for x in payload["rows"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def _get_notional_usd(row: dict[str, Any]) -> float:
    price = float(row.get("price") or row.get("px") or 0.0)
    for k in (
        "usd",
        "size_usd",
        "notional",
        "notional_usd",
        "value_usd",
        "quote_qty",
        "quote_size",
    ):
        if k in row:
            try:
                v = float(row[k])
                if v > 0:
                    return v
            except Exception:
                pass
    try:
        size = float(row.get("size") or row.get("qty") or row.get("q") or 0.0)
    except Exception:
        size = 0.0
    if size <= 0 or price <= 0:
        return 0.0
    ex = str(row.get("exchange") or row.get("venue") or "").lower()
    if "mexc" in ex and "perp" in ex:
        return size * 0.001 * price
    naive = size * price
    if "perp" in ex and naive > 12_000_000:
        return size * 0.001 * price
    return naive


def _candidate_symbols(base_url: str, headers: dict[str, str]) -> list[str]:
    candidates = ["BTC-USD", "BTCUSDT", "BTC-USD-PERP", "XBTUSD", "BTC-USD-SWAP"]
    url = f"{base_url}/market/symbols"
    try:
        with httpx.Client(timeout=20.0) as client:
            r = client.get(url, headers=headers)
            if r.status_code == 200:
                data = r.json()
                items: list[str] = []
                if isinstance(data, list):
                    items = [str(x) for x in data]
                elif isinstance(data, dict):
                    for key in ("symbols", "items", "data"):
                        if isinstance(data.get(key), list):
                            items = [str(x) for x in data[key]]
                            break
                btc = [s for s in items if "BTC" in s.upper() or "XBT" in s.upper()]
                candidates = list(dict.fromkeys(btc + candidates))
    except Exception:
        pass
    return candidates


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe BEA tape exchanges for BTC symbols.")
    parser.add_argument("--samples", type=int, default=2, help="Polls per symbol (default 2)")
    parser.add_argument("--limit", type=int, default=200, help="Expected tape depth, informational only")
    parser.add_argument("--symbol", action="append", default=[], help="Extra symbol(s) to test")
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    load_dotenv(base / ".env", override=True)
    token = (os.environ.get("BEA_TOKEN") or "").strip()
    base_url = (os.environ.get("BEA_REST_BASE") or "https://www.bleedingedgealpha.net/api/v1").rstrip("/")
    if not token:
        print("Missing BEA_TOKEN in tape_standalone/.env")
        return 1

    headers = {"Authorization": f"Bearer {token}"}
    symbols = _candidate_symbols(base_url, headers)
    if args.symbol:
        symbols = list(dict.fromkeys(args.symbol + symbols))

    print(f"BEA base: {base_url}")
    print(f"Symbols to probe: {', '.join(symbols[:20])}")
    print("-" * 88)

    by_symbol_counts: dict[str, Counter[str]] = {}
    by_symbol_notional: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    with httpx.Client(timeout=25.0) as client:
        for sym in symbols:
            sym_counter: Counter[str] = Counter()
            notional_totals: dict[str, float] = defaultdict(float)
            ok = False
            for _ in range(max(1, args.samples)):
                try:
                    r = client.get(
                        f"{base_url}/market/tape",
                        params={"symbol": sym},
                        headers=headers,
                    )
                except Exception:
                    continue
                if r.status_code != 200:
                    continue
                rows = _normalize_rows(r.json())
                if not rows:
                    # Fallback: discover active exchanges from /market/history prices map.
                    try:
                        rh = client.get(
                            f"{base_url}/market/history",
                            params={"symbol": sym, "limit": min(300, max(50, args.limit))},
                            headers=headers,
                        )
                    except Exception:
                        continue
                    if rh.status_code == 200:
                        hrows = _normalize_history_rows(rh.json())
                        if hrows:
                            ok = True
                            for row in hrows:
                                prices = row.get("prices")
                                if isinstance(prices, dict):
                                    for ex, px in prices.items():
                                        if px is None:
                                            continue
                                        try:
                                            _ = float(px)
                                        except Exception:
                                            continue
                                        exs = str(ex)
                                        sym_counter[exs] += 1
                            continue
                    continue
                ok = True
                for row in rows:
                    ex = str(row.get("exchange") or row.get("venue") or "?")
                    sym_counter[ex] += 1
                    notional_totals[ex] += _get_notional_usd(row)
            if ok:
                by_symbol_counts[sym] = sym_counter
                by_symbol_notional[sym] = notional_totals

    if not by_symbol_counts:
        print("No symbols returned tape rows. Check token/symbol naming.")
        return 2

    for sym, counts in by_symbol_counts.items():
        total = sum(counts.values())
        print(f"\n{sym}  ->  {total} trades across {len(counts)} exchanges")
        top = counts.most_common(10)
        for ex, n in top:
            usd = by_symbol_notional[sym].get(ex, 0.0)
            print(f"  {ex:14}  trades={n:4d}  notional≈${usd:,.0f}")

    print("\nSuggested symbols for tape:", ", ".join(by_symbol_counts.keys()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
