"""
Tape backend:
- /api/bea/tape       raw BEA passthrough
- /api/tape/live      merged multi-source tape
- /api/sources/status source health
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import hashlib
import json
import os
import random
import ssl
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import certifi
import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

BASE = Path(__file__).resolve().parent
PUBLIC = BASE / "public"
DOTENV_OVERRIDE = os.environ.get("TAPE_DOTENV_OVERRIDE", "0").strip() in ("1", "true", "True")
load_dotenv(BASE / ".env", override=DOTENV_OVERRIDE)

BEA_TOKEN = os.environ.get("BEA_TOKEN", "").strip()
BEA_REST_BASE = os.environ.get("BEA_REST_BASE", "https://www.bleedingedgealpha.net/api/v1").rstrip("/")
BEA_POLL_MS = int(os.environ.get("BEA_POLL_MS", "1200"))
BEA_SYMBOLS = [s.strip() for s in os.environ.get("BEA_SYMBOLS", "BTC-USD").split(",") if s.strip()]

ENABLE_BINANCE_PERPS = os.environ.get("ENABLE_BINANCE_PERPS", "1").strip() not in ("0", "false", "False")
ENABLE_BINANCE_SPOT = os.environ.get("ENABLE_BINANCE_SPOT", "1").strip() not in ("0", "false", "False")
ENABLE_BYBIT_LINEAR = os.environ.get("ENABLE_BYBIT_LINEAR", "1").strip() not in ("0", "false", "False")
ENABLE_BYBIT_SPOT = os.environ.get("ENABLE_BYBIT_SPOT", "1").strip() not in ("0", "false", "False")
ENABLE_OKX_SWAP = os.environ.get("ENABLE_OKX_SWAP", "1").strip() not in ("0", "false", "False")
ENABLE_OKX_SPOT = os.environ.get("ENABLE_OKX_SPOT", "1").strip() not in ("0", "false", "False")
ENABLE_COINBASE = os.environ.get("ENABLE_COINBASE", "1").strip() not in ("0", "false", "False")
ENABLE_KRAKEN = os.environ.get("ENABLE_KRAKEN", "1").strip() not in ("0", "false", "False")
ENABLE_BITSTAMP = os.environ.get("ENABLE_BITSTAMP", "1").strip() not in ("0", "false", "False")
ENABLE_BITMEX = os.environ.get("ENABLE_BITMEX", "1").strip() not in ("0", "false", "False")
ENABLE_DERIBIT = os.environ.get("ENABLE_DERIBIT", "1").strip() not in ("0", "false", "False")
ENABLE_HYPERLIQUID = os.environ.get("ENABLE_HYPERLIQUID", "1").strip() not in ("0", "false", "False")
ENABLE_BITFINEX = os.environ.get("ENABLE_BITFINEX", "1").strip() not in ("0", "false", "False")
ENABLE_BTCC = os.environ.get("ENABLE_BTCC", "0").strip() not in ("0", "false", "False")
BTCC_WS_URL = os.environ.get("BTCC_WS_URL", "").strip()
REORDER_BUFFER_MS = int(os.environ.get("TAPE_REORDER_BUFFER_MS", "180"))
MAX_ADAPTIVE_REORDER_MS = int(os.environ.get("TAPE_MAX_ADAPTIVE_REORDER_MS", "900"))
MAX_NOTIONAL_USD = float(os.environ.get("TAPE_MAX_NOTIONAL_USD", "25000000"))
MAX_TRADE_AGE_MS = int(os.environ.get("TAPE_MAX_TRADE_AGE_MS", "180000"))
MAX_FUTURE_SKEW_MS = int(os.environ.get("TAPE_MAX_FUTURE_SKEW_MS", "2500"))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso_to_ms(ts: str) -> int:
    try:
        s = ts.replace("Z", "+00:00")
        return int(dt.datetime.fromisoformat(s).timestamp() * 1000)
    except Exception:
        return _now_ms()


def _to_ms(v: Any) -> int:
    try:
        ts = int(float(v))
    except Exception:
        return _now_ms()
    if ts < 1_000_000_000_000:
        ts *= 1000
    return ts


def _bea_symbol_to_binance(symbol: str) -> str:
    s = symbol.strip().upper()
    if s.endswith("-USD"):
        return f"{s[:-4]}USDT"
    return s.replace("-", "")


def _binance_to_bea_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}-USD"
    return s


def _norm_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for k in ("trades", "prints", "data", "tape", "rows", "items", "events", "lines", "tape_trades"):
        v = payload.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    d = payload.get("data")
    if isinstance(d, dict):
        for k in ("trades", "prints", "tape", "rows", "items"):
            v = d.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


# Per-source/venue notional normalization rules
# kind:
# - base: size is base asset amount (USD notional = price * size)
# - quote: size is already quote/USD notional
# - contracts: size is contracts; convert via multiplier -> base then *price
NOTIONAL_RULES: dict[str, dict[str, Any]] = {
    "default": {"kind": "base"},
    "mexc_perps": {"kind": "contracts", "multiplier": 0.001},
    "okx_perps": {"kind": "contracts", "multiplier": 0.01},
    "bitmex_perps": {"kind": "quote"},
    "deribit_perps": {"kind": "quote"},
}


def _notional_from_size(exchange: str, price: float, size: float, row: dict[str, Any] | None = None) -> float:
    if price <= 0 or size <= 0:
        return 0.0
    r = NOTIONAL_RULES.get(exchange, NOTIONAL_RULES["default"])
    if row:
        for k in ("quote_qty", "quoteQty", "quote_size", "quote_qty_usd", "usd", "size_usd", "notional", "notional_usd", "value_usd"):
            v = row.get(k)
            if v is None:
                continue
            try:
                u = float(v)
            except Exception:
                continue
            if u > 0:
                return u
    kind = str(r.get("kind", "base"))
    if kind == "quote":
        return size
    if kind == "contracts":
        mult = float(r.get("multiplier", 1.0))
        return size * mult * price
    return size * price


def _compute_notional_usd(row: dict[str, Any], price: float) -> float:
    try:
        sz = float(row.get("size") or row.get("qty") or row.get("quantity") or row.get("q") or 0)
    except Exception:
        sz = 0.0
    ex = str(row.get("exchange") or row.get("venue") or "").lower()
    return _notional_from_size(ex, price, sz, row=row)


def _from_bea_row(row: dict[str, Any], symbol_hint: str | None = None) -> dict[str, Any] | None:
    try:
        price = float(row.get("price") or row.get("px") or row.get("p") or 0)
    except Exception:
        return None
    if price <= 0:
        return None
    usd = _compute_notional_usd(row, price)
    if usd <= 0:
        return None
    ex = str(row.get("exchange") or row.get("venue") or "bea_unknown")
    side_raw = str(row.get("side") or row.get("aggressor") or "").lower()
    side = "sell" if ("sell" in side_raw or side_raw in ("s", "ask", "-1")) else "buy"
    t = row.get("ts_ms") or row.get("t") or row.get("ts") or row.get("time")
    ts = _to_ms(t if t is not None else _now_ms())
    sym = str(row.get("symbol") or symbol_hint or "BTC-USD")
    return {
        "source": "bea",
        "symbol": sym,
        "exchange": ex,
        "price": price,
        "side": side,
        "size_usd": usd,
        "ts_ms": ts,
        "trade_id": str(row.get("id") or row.get("trade_id") or row.get("tid") or ""),
    }


@dataclass
class SourceState:
    name: str
    enabled: bool = True
    connected: bool = False
    last_ok_ms: int = 0
    errors: int = 0
    last_error: str = ""
    extra: dict[str, Any] = field(default_factory=dict)
    reconnects: int = 0
    msg_count: int = 0
    anomaly_drops: int = 0
    msg_times_ms: deque[int] = field(default_factory=lambda: deque(maxlen=4096))
    lag_ewma_ms: float = 0.0


class TapeHub:
    def __init__(self, maxlen: int = 12000) -> None:
        self.trades: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self._by_symbol: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=maxlen))
        self._seen: set[str] = set()
        self._seen_q: deque[str] = deque(maxlen=maxlen * 8)
        self._seq = 0
        self._lag_ewma_ms = 0.0
        self._adaptive_reorder_ms = REORDER_BUFFER_MS
        self.states: dict[str, SourceState] = {
            "bea": SourceState("bea", enabled=bool(BEA_TOKEN)),
            "binance_perps": SourceState("binance_perps", enabled=ENABLE_BINANCE_PERPS),
            "binance_spot": SourceState("binance_spot", enabled=ENABLE_BINANCE_SPOT),
            "bybit_linear": SourceState("bybit_linear", enabled=ENABLE_BYBIT_LINEAR),
            "bybit_spot": SourceState("bybit_spot", enabled=ENABLE_BYBIT_SPOT),
            "okx_swap": SourceState("okx_swap", enabled=ENABLE_OKX_SWAP),
            "okx_spot": SourceState("okx_spot", enabled=ENABLE_OKX_SPOT),
            "coinbase": SourceState("coinbase", enabled=ENABLE_COINBASE),
            "kraken": SourceState("kraken", enabled=ENABLE_KRAKEN),
            "bitstamp": SourceState("bitstamp", enabled=ENABLE_BITSTAMP),
            "bitmex": SourceState("bitmex", enabled=ENABLE_BITMEX),
            "deribit": SourceState("deribit", enabled=ENABLE_DERIBIT),
            "hyperliquid_perps": SourceState("hyperliquid_perps", enabled=ENABLE_HYPERLIQUID),
            "bitfinex_spot": SourceState("bitfinex_spot", enabled=ENABLE_BITFINEX),
            "btcc_spot": SourceState("btcc_spot", enabled=ENABLE_BTCC),
        }
        self._lock = asyncio.Lock()

    def _track_lag(self, st: SourceState | None, lag_ms: int) -> None:
        if lag_ms < 0 or lag_ms > 60_000:
            return
        if self._lag_ewma_ms <= 0:
            self._lag_ewma_ms = float(lag_ms)
        else:
            self._lag_ewma_ms = self._lag_ewma_ms * 0.92 + float(lag_ms) * 0.08
        self._adaptive_reorder_ms = min(
            MAX_ADAPTIVE_REORDER_MS,
            max(REORDER_BUFFER_MS, int(self._lag_ewma_ms * 1.35) + 35),
        )
        if st is not None:
            if st.lag_ewma_ms <= 0:
                st.lag_ewma_ms = float(lag_ms)
            else:
                st.lag_ewma_ms = st.lag_ewma_ms * 0.92 + float(lag_ms) * 0.08

    def _is_anomalous(self, t: dict[str, Any], now_ms: int) -> tuple[bool, str]:
        try:
            px = float(t.get("price") or 0)
            usd = float(t.get("size_usd") or 0)
            ts = int(t.get("ts_ms") or 0)
        except Exception:
            return True, "parse"
        if px <= 0 or usd <= 0:
            return True, "non_positive"
        if usd > MAX_NOTIONAL_USD:
            return True, "notional_cap"
        if ts > 0 and now_ms - ts > MAX_TRADE_AGE_MS:
            return True, "too_old"
        if ts > 0 and ts - now_ms > MAX_FUTURE_SKEW_MS:
            return True, "future_skew"
        return False, ""

    def _mk_key(self, t: dict[str, Any]) -> str:
        tid = t.get("trade_id") or ""
        if tid:
            return f"{t['source']}|{t['exchange']}|{tid}"
        ts_ms = int(t.get("ts_ms") or 0)
        try:
            px = f"{float(t.get('price') or 0):.2f}"
        except Exception:
            px = "0"
        try:
            sz = f"{float(t.get('size_usd') or 0):.2f}"
        except Exception:
            sz = "0"
        raw = f"{t.get('source','')}|{t.get('exchange','')}|{ts_ms}|{px}|{sz}|{t.get('side','')}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    async def add_trade(self, t: dict[str, Any]) -> None:
        now_ms = _now_ms()
        src = str(t.get("source") or "")
        st = self.states.get(src)
        bad, reason = self._is_anomalous(t, now_ms)
        if bad:
            if st is not None:
                st.anomaly_drops += 1
                st.extra["last_drop_reason"] = reason
            return
        ts = int(t.get("ts_ms") or 0)
        if ts > 0:
            self._track_lag(st, now_ms - ts)
        key = self._mk_key(t)
        async with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)
            self._seen_q.append(key)
            self._seq += 1
            t["_seq"] = self._seq
            self.trades.appendleft(t)
            sym = str(t.get("symbol", "")).upper()
            if sym:
                self._by_symbol[sym].appendleft(t)
            if st is not None:
                st.msg_count += 1
                st.msg_times_ms.append(now_ms)
            while len(self._seen_q) > self.trades.maxlen * 6:
                self._seen.discard(self._seen_q.popleft())

    async def snapshot(self, symbol: str, limit: int = 250) -> list[dict[str, Any]]:
        sym = symbol.upper()
        now = _now_ms()
        cutoff = now - max(0, self._adaptive_reorder_ms)
        out: list[dict[str, Any]] = []
        async with self._lock:
            candidates = self._by_symbol.get(sym)
            if candidates is None:
                candidates = deque()
            cand_list = list(candidates)
            for t in cand_list:
                ts = int(t.get("ts_ms") or 0)
                if ts > 0 and ts > cutoff:
                    continue
                out.append(t)
            # If every trade is still inside the reorder window (common in bursts), the UI would
            # show nothing; fall back to newest trades so tape/SSE are not empty while data exists.
            if not out and cand_list:
                out = cand_list[:]
            # Stable ordering by timestamp, then ingest sequence.
            out.sort(key=lambda x: (int(x.get("ts_ms") or 0), int(x.get("_seq") or 0)), reverse=True)
            if len(out) > limit:
                out = out[:limit]
        return out

    def adaptive_reorder_ms(self) -> int:
        return int(max(REORDER_BUFFER_MS, self._adaptive_reorder_ms))


hub = TapeHub()
tasks: list[asyncio.Task[Any]] = []
http_client: httpx.AsyncClient | None = None
SSL_CTX = ssl.create_default_context(cafile=certifi.where())


def _mark_ok(state: SourceState, extra: dict[str, Any] | None = None, connected_event: bool = False) -> None:
    state.connected = True
    state.last_ok_ms = _now_ms()
    if connected_event:
        state.reconnects += 1
    state.extra["retry_attempts"] = 0
    state.extra["retry_delay_ms"] = 0
    if extra:
        state.extra.update(extra)


def _mark_err(state: SourceState, e: Exception) -> None:
    state.connected = False
    state.errors += 1
    state.last_error = str(e)[:220]


async def _sleep_retry(state: SourceState, base: float = 1.0, cap: float = 20.0) -> None:
    attempts = int(state.extra.get("retry_attempts") or 0) + 1
    state.extra["retry_attempts"] = attempts
    delay = min(cap, base * (2 ** min(attempts, 6)))
    jitter = random.uniform(0.0, delay * 0.25)
    total = delay + jitter
    state.extra["retry_delay_ms"] = int(total * 1000)
    await asyncio.sleep(total)


async def _bea_loop() -> None:
    state = hub.states["bea"]
    if not state.enabled:
        return
    assert http_client is not None
    url = f"{BEA_REST_BASE}/market/tape"
    headers = {"Authorization": f"Bearer {BEA_TOKEN}"}
    poll_sec = max(0.35, BEA_POLL_MS / 1000.0)
    while True:
        try:
            for sym in BEA_SYMBOLS:
                r = await http_client.get(url, params={"symbol": sym}, headers=headers)
                if r.status_code != 200:
                    raise RuntimeError(f"bea {r.status_code}: {r.text[:120]}")
                rows = _norm_rows(r.json())
                _mark_ok(state, {"symbols": BEA_SYMBOLS, "last_count": len(rows), "poll_ms": BEA_POLL_MS})
                for row in rows:
                    if isinstance(row, dict):
                        tr = _from_bea_row(row, symbol_hint=sym)
                        if tr:
                            await hub.add_trade(tr)
            await asyncio.sleep(poll_sec)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state, base=max(0.5, poll_sec), cap=8.0)


async def _binance_perps_loop(symbol: str) -> None:
    state = hub.states["binance_perps"]
    if not state.enabled:
        return
    ws_url = f"wss://fstream.binance.com/ws/{symbol.lower()}@aggTrade"
    bea_symbol = _binance_to_bea_symbol(symbol)
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                _mark_ok(state, {"symbols": [symbol]}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("e") != "aggTrade":
                        continue
                    price = float(d["p"])
                    qty = float(d["q"])
                    await hub.add_trade(
                        {
                            "source": "binance_perps",
                            "symbol": bea_symbol,
                            "exchange": "binance_perps",
                            "price": price,
                            "side": "sell" if bool(d.get("m")) else "buy",
                            "size_usd": _notional_from_size("binance_perps", price, qty),
                            "ts_ms": _to_ms(d.get("T") or _now_ms()),
                            "trade_id": str(d.get("a") or ""),
                        }
                    )
                    state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _binance_spot_loop(symbol: str) -> None:
    state = hub.states["binance_spot"]
    if not state.enabled:
        return
    ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
    bea_symbol = _binance_to_bea_symbol(symbol)
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                _mark_ok(state, {"symbols": [symbol]}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("e") != "trade":
                        continue
                    price = float(d["p"])
                    qty = float(d["q"])
                    await hub.add_trade(
                        {
                            "source": "binance_spot",
                            "symbol": bea_symbol,
                            "exchange": "binance_spot",
                            "price": price,
                            "side": "sell" if bool(d.get("m")) else "buy",
                            "size_usd": _notional_from_size("binance_spot", price, qty),
                            "ts_ms": _to_ms(d.get("T") or _now_ms()),
                            "trade_id": str(d.get("t") or ""),
                        }
                    )
                    state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _bybit_loop(symbol: str, market: str) -> None:
    state = hub.states["bybit_linear" if market == "linear" else "bybit_spot"]
    if not state.enabled:
        return
    ws_url = f"wss://stream.bybit.com/v5/public/{market}"
    topic = f"publicTrade.{symbol}"
    sub = {"op": "subscribe", "args": [topic]}
    bea_symbol = _binance_to_bea_symbol(symbol)
    exch = "bybit_perps" if market == "linear" else "bybit_spot"
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(state, {"symbols": [symbol], "market": market}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if not isinstance(d, dict) or not str(d.get("topic", "")).startswith("publicTrade."):
                        continue
                    for tr in d.get("data", []):
                        try:
                            price = float(tr.get("p"))
                            qty = float(tr.get("v"))
                        except Exception:
                            continue
                        await hub.add_trade(
                            {
                                "source": state.name,
                                "symbol": bea_symbol,
                                "exchange": exch,
                                "price": price,
                                "side": "buy" if str(tr.get("S", "")).lower().startswith("b") else "sell",
                                "size_usd": _notional_from_size(exch, price, qty),
                                "ts_ms": _to_ms(tr.get("T") or _now_ms()),
                                "trade_id": str(tr.get("i") or ""),
                            }
                        )
                        state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _okx_loop(inst_id: str) -> None:
    is_swap = inst_id.endswith("-SWAP")
    state = hub.states["okx_swap" if is_swap else "okx_spot"]
    if not state.enabled:
        return
    ws_url = "wss://ws.okx.com:8443/ws/v5/public"
    sub = {"op": "subscribe", "args": [{"channel": "trades", "instId": inst_id}]}
    bea_symbol = "BTC-USD"
    exch = "okx_perps" if is_swap else "okx_spot"
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(state, {"instId": inst_id}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if not isinstance(d, dict):
                        continue
                    for tr in d.get("data", []):
                        try:
                            price = float(tr.get("px"))
                            qty = float(tr.get("sz"))
                        except Exception:
                            continue
                        await hub.add_trade(
                            {
                                "source": state.name,
                                "symbol": bea_symbol,
                                "exchange": exch,
                                "price": price,
                                "side": str(tr.get("side") or "buy").lower(),
                                "size_usd": _notional_from_size(exch, price, qty),
                                "ts_ms": _to_ms(tr.get("ts") or _now_ms()),
                                "trade_id": str(tr.get("tradeId") or ""),
                            }
                        )
                        state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _coinbase_loop() -> None:
    state = hub.states["coinbase"]
    if not state.enabled:
        return
    ws_url = "wss://ws-feed.exchange.coinbase.com"
    sub = {"type": "subscribe", "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}]}
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(state, {"product": "BTC-USD"}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("type") != "match":
                        continue
                    price = float(d["price"])
                    qty = float(d["size"])
                    await hub.add_trade(
                        {
                            "source": "coinbase",
                            "symbol": "BTC-USD",
                            "exchange": "coinbase",
                            "price": price,
                            "side": str(d.get("side") or "buy").lower(),
                            "size_usd": _notional_from_size("coinbase", price, qty),
                            "ts_ms": _iso_to_ms(str(d.get("time", ""))),
                            "trade_id": str(d.get("trade_id") or ""),
                        }
                    )
                    state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _kraken_loop() -> None:
    state = hub.states["kraken"]
    if not state.enabled:
        return
    ws_url = "wss://ws.kraken.com/v2"
    sub = {"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(state, {"symbol": "BTC/USD"}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("channel") != "trade":
                        continue
                    for block in d.get("data", []):
                        for tr in block.get("trades", []):
                            try:
                                price = float(tr.get("price"))
                                qty = float(tr.get("qty"))
                            except Exception:
                                continue
                            await hub.add_trade(
                                {
                                    "source": "kraken",
                                    "symbol": "BTC-USD",
                                    "exchange": "kraken",
                                    "price": price,
                                    "side": str(tr.get("side") or "buy").lower(),
                                    "size_usd": _notional_from_size("kraken", price, qty),
                                    "ts_ms": _to_ms(tr.get("timestamp") or _now_ms()),
                                    "trade_id": str(tr.get("trade_id") or ""),
                                }
                            )
                            state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _bitstamp_loop() -> None:
    state = hub.states["bitstamp"]
    if not state.enabled:
        return
    ws_url = "wss://ws.bitstamp.net"
    sub = {"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(state, {"channel": "live_trades_btcusd"}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("event") != "trade":
                        continue
                    tr = d.get("data", {})
                    price = float(tr.get("price", 0))
                    qty = float(tr.get("amount", 0))
                    side = "sell" if int(tr.get("type", 0)) == 1 else "buy"
                    ts = _to_ms(str(tr.get("microtimestamp") or "0")[:13] or _now_ms())
                    await hub.add_trade(
                        {
                            "source": "bitstamp",
                            "symbol": "BTC-USD",
                            "exchange": "bitstamp",
                            "price": price,
                            "side": side,
                            "size_usd": _notional_from_size("bitstamp", price, qty),
                            "ts_ms": ts,
                            "trade_id": str(tr.get("id") or ""),
                        }
                    )
                    state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _bitmex_loop() -> None:
    state = hub.states["bitmex"]
    if not state.enabled:
        return
    ws_url = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                _mark_ok(state, {"symbol": "XBTUSD"}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("table") != "trade":
                        continue
                    for tr in d.get("data", []):
                        price = float(tr.get("price", 0))
                        size = float(tr.get("size", 0))  # XBTUSD size is USD notional contracts
                        await hub.add_trade(
                            {
                                "source": "bitmex",
                                "symbol": "BTC-USD",
                                "exchange": "bitmex_perps",
                                "price": price,
                                "side": str(tr.get("side") or "Buy").lower(),
                                "size_usd": _notional_from_size("bitmex_perps", price, size),
                                "ts_ms": _iso_to_ms(str(tr.get("timestamp") or "")),
                                "trade_id": str(tr.get("trdMatchID") or ""),
                            }
                        )
                        state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _deribit_loop() -> None:
    state = hub.states["deribit"]
    if not state.enabled:
        return
    ws_url = "wss://www.deribit.com/ws/api/v2"
    sub = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "public/subscribe",
        "params": {"channels": ["trades.BTC-PERPETUAL.raw"]},
    }
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(state, {"channel": "trades.BTC-PERPETUAL.raw"}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    p = d.get("params", {})
                    if not isinstance(p, dict):
                        continue
                    for tr in p.get("data", []):
                        price = float(tr.get("price", 0))
                        amt = float(tr.get("amount", 0))  # treated as quote notional for perp
                        await hub.add_trade(
                            {
                                "source": "deribit",
                                "symbol": "BTC-USD",
                                "exchange": "deribit_perps",
                                "price": price,
                                "side": str(tr.get("direction") or "buy").lower(),
                                "size_usd": _notional_from_size("deribit_perps", price, amt),
                                "ts_ms": _to_ms(tr.get("timestamp") or _now_ms()),
                                "trade_id": str(tr.get("trade_id") or ""),
                            }
                        )
                        state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _hyperliquid_loop(coin: str) -> None:
    state = hub.states["hyperliquid_perps"]
    if not state.enabled:
        return
    ws_url = "wss://api.hyperliquid.xyz/ws"
    sub = {"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(state, {"coin": coin}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if not isinstance(d, dict) or str(d.get("channel")) != "trades":
                        continue
                    data = d.get("data")
                    if not isinstance(data, list):
                        continue
                    for tr in data:
                        if not isinstance(tr, dict):
                            continue
                        try:
                            price = float(tr.get("px"))
                            qty = float(tr.get("sz"))
                        except Exception:
                            continue
                        ts = _to_ms(tr.get("time") or _now_ms())
                        tid = str(tr.get("tid") or tr.get("hash") or "")
                        await hub.add_trade(
                            {
                                "source": "hyperliquid_perps",
                                "symbol": "BTC-USD",
                                "exchange": "hyperliquid_perps",
                                "price": price,
                                "side": (
                                    "sell"
                                    if str(tr.get("side") or "").lower() in {"a", "ask", "sell", "s"}
                                    else "buy"
                                ),
                                "size_usd": _notional_from_size("hyperliquid_perps", price, qty),
                                "ts_ms": ts,
                                "trade_id": tid,
                            }
                        )
                        state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _bitfinex_loop(symbol: str = "tBTCUSD") -> None:
    state = hub.states["bitfinex_spot"]
    if not state.enabled:
        return
    ws_url = "wss://api-pub.bitfinex.com/ws/2"
    sub = {"event": "subscribe", "channel": "trades", "symbol": symbol}
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                chan_id: int | None = None
                _mark_ok(state, {"symbol": symbol}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if isinstance(d, dict):
                        if d.get("event") == "subscribed" and d.get("channel") == "trades":
                            try:
                                chan_id = int(d.get("chanId"))
                            except Exception:
                                chan_id = chan_id
                        continue
                    if not isinstance(d, list) or len(d) < 2:
                        continue
                    if chan_id is not None and d[0] != chan_id:
                        continue
                    payload = d[1]
                    rows: list[Any] = []
                    if payload == "te" and len(d) >= 3 and isinstance(d[2], list):
                        rows = [d[2]]
                    elif isinstance(payload, list) and payload and isinstance(payload[0], list):
                        rows = payload
                    for tr in rows:
                        try:
                            trade_id = str(tr[0])
                            ts = _to_ms(tr[1])
                            amt = float(tr[2])
                            price = float(tr[3])
                        except Exception:
                            continue
                        qty = abs(amt)
                        side = "buy" if amt > 0 else "sell"
                        await hub.add_trade(
                            {
                                "source": "bitfinex_spot",
                                "symbol": "BTC-USD",
                                "exchange": "bitfinex_spot",
                                "price": price,
                                "side": side,
                                "size_usd": _notional_from_size("bitfinex_spot", price, qty),
                                "ts_ms": ts,
                                "trade_id": trade_id,
                            }
                        )
                        state.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


async def _btcc_loop(symbol: str = "BTC_USD") -> None:
    state = hub.states["btcc_spot"]
    if not state.enabled:
        return
    if not BTCC_WS_URL:
        _mark_err(state, RuntimeError("BTCC_WS_URL not set"))
        return
    req = {"action": "GetTrades", "symbol": symbol, "count": 100}
    while True:
        try:
            async with websockets.connect(BTCC_WS_URL, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                _mark_ok(state, {"symbol": symbol}, connected_event=True)
                while True:
                    await ws.send(json.dumps(req))
                    msg = json.loads(await ws.recv())
                    rows = []
                    if isinstance(msg, dict):
                        data = msg.get("Data") or msg.get("data") or []
                        if isinstance(data, list):
                            rows = data
                    for tr in rows:
                        if not isinstance(tr, dict):
                            continue
                        try:
                            price = float(tr.get("price") or tr.get("Price") or tr.get("px") or 0)
                            qty = float(tr.get("amount") or tr.get("quantity") or tr.get("qty") or tr.get("size") or 0)
                        except Exception:
                            continue
                        if price <= 0 or qty <= 0:
                            continue
                        side_v = str(tr.get("side") or tr.get("Side") or "")
                        side = "sell" if side_v.upper().startswith("S") else "buy"
                        ts = _to_ms(tr.get("timestamp") or tr.get("time") or tr.get("Timestamp") or _now_ms())
                        tid = str(tr.get("id") or tr.get("trade_id") or tr.get("tid") or "")
                        await hub.add_trade(
                            {
                                "source": "btcc_spot",
                                "symbol": "BTC-USD",
                                "exchange": "btcc_spot",
                                "price": price,
                                "side": side,
                                "size_usd": _notional_from_size("btcc_spot", price, qty),
                                "ts_ms": ts,
                                "trade_id": tid,
                            }
                        )
                        state.last_ok_ms = _now_ms()
                    await asyncio.sleep(0.9)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(state, e)
            await _sleep_retry(state)


app = FastAPI(title="Tape multi-source proxy")
tasks: list[asyncio.Task[Any]] = []
http_client: httpx.AsyncClient | None = None
_feeds_started = False


@app.on_event("startup")
async def _startup() -> None:
    global http_client, _feeds_started
    if _feeds_started:
        return
    _feeds_started = True
    http_client = httpx.AsyncClient(timeout=25.0, trust_env=False)
    tasks.append(asyncio.create_task(_bea_loop(), name="bea-loop"))
    if ENABLE_BINANCE_PERPS:
        tasks.append(asyncio.create_task(_binance_perps_loop("BTCUSDT"), name="binance-perps"))
    if ENABLE_BINANCE_SPOT:
        tasks.append(asyncio.create_task(_binance_spot_loop("BTCUSDT"), name="binance-spot"))
    if ENABLE_BYBIT_LINEAR:
        tasks.append(asyncio.create_task(_bybit_loop("BTCUSDT", "linear"), name="bybit-linear"))
    if ENABLE_BYBIT_SPOT:
        tasks.append(asyncio.create_task(_bybit_loop("BTCUSDT", "spot"), name="bybit-spot"))
    if ENABLE_OKX_SWAP:
        tasks.append(asyncio.create_task(_okx_loop("BTC-USDT-SWAP"), name="okx-swap"))
    if ENABLE_OKX_SPOT:
        tasks.append(asyncio.create_task(_okx_loop("BTC-USDT"), name="okx-spot"))
    if ENABLE_COINBASE:
        tasks.append(asyncio.create_task(_coinbase_loop(), name="coinbase"))
    if ENABLE_KRAKEN:
        tasks.append(asyncio.create_task(_kraken_loop(), name="kraken"))
    if ENABLE_BITSTAMP:
        tasks.append(asyncio.create_task(_bitstamp_loop(), name="bitstamp"))
    if ENABLE_BITMEX:
        tasks.append(asyncio.create_task(_bitmex_loop(), name="bitmex"))
    if ENABLE_DERIBIT:
        tasks.append(asyncio.create_task(_deribit_loop(), name="deribit"))
    if ENABLE_HYPERLIQUID:
        tasks.append(asyncio.create_task(_hyperliquid_loop("BTC"), name="hyperliquid-perps"))
    if ENABLE_BITFINEX:
        tasks.append(asyncio.create_task(_bitfinex_loop("tBTCUSD"), name="bitfinex-spot"))
    if ENABLE_BTCC:
        tasks.append(asyncio.create_task(_btcc_loop("BTC_USD"), name="btcc-spot"))


@app.on_event("shutdown")
async def _shutdown() -> None:
    global http_client, _feeds_started
    if not _feeds_started:
        return
    _feeds_started = False
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(Exception):
            await t
    tasks.clear()
    if http_client is not None:
        await http_client.aclose()
        http_client = None


@app.get("/api/bea/tape")
async def bea_tape(symbol: str = "BTC-USD") -> dict | list:
    if not BEA_TOKEN:
        raise HTTPException(503, detail="Set BEA_TOKEN in tape_standalone/.env")
    assert http_client is not None
    url = f"{BEA_REST_BASE}/market/tape"
    r = await http_client.get(url, params={"symbol": symbol}, headers={"Authorization": f"Bearer {BEA_TOKEN}"})
    if r.status_code != 200:
        raise HTTPException(r.status_code, detail=r.text[:800] if r.text else "BEA error")
    return r.json()


@app.get("/api/tape/debug")
async def tape_debug(symbol: str = "BTC-USD") -> dict[str, Any]:
    """Inbound feed health: if `global_deque_count` is 0, no venue/BEA trades are reaching this process."""
    snap = await hub.snapshot(symbol=symbol, limit=5000)
    now = _now_ms()
    src_out: dict[str, Any] = {}
    for k, st in hub.states.items():
        age = None if st.last_ok_ms <= 0 else max(0, (now - st.last_ok_ms) // 1000)
        src_out[k] = {
            "enabled": st.enabled,
            "connected": st.connected,
            "errors": st.errors,
            "msg_count": st.msg_count,
            "last_ok_age_s": age,
            "last_error": (st.last_error or "")[:160],
        }
    return {
        "symbol": symbol.upper(),
        "global_deque_count": len(hub.trades),
        "snapshot_count_for_symbol": len(snap),
        "adaptive_reorder_ms": hub.adaptive_reorder_ms(),
        "feeds_started": _feeds_started,
        "background_tasks": len(tasks),
        "http_client_ready": http_client is not None,
        "sources": src_out,
    }


@app.get("/api/tape/live")
async def tape_live(
    symbol: str = "BTC-USD",
    limit: int = 250,
    market: str = "all",
    sources: str | None = None,
) -> dict[str, Any]:
    lim = max(1, min(2000, limit))
    trades = await hub.snapshot(symbol=symbol, limit=2000)
    trades = _filter_trades(trades, market=market, sources=sources)
    if len(trades) > lim:
        trades = trades[:lim]
    return {"symbol": symbol, "trades": trades, "sources": list(hub.states.keys()), "adaptive_reorder_ms": hub.adaptive_reorder_ms()}


def _filter_trades(trades: list[dict[str, Any]], market: str, sources: str | None) -> list[dict[str, Any]]:
    m = market.strip().lower()
    if m == "perp":
        trades = [t for t in trades if "_perps" in str(t.get("exchange", ""))]
    elif m == "spot":
        trades = [
            t
            for t in trades
            if "_spot" in str(t.get("exchange", ""))
            or str(t.get("exchange", "")) in {"coinbase", "kraken", "bitstamp"}
        ]

    if sources:
        allowed = {s.strip() for s in sources.split(",") if s.strip()}
        if allowed:
            trades = [
                t
                for t in trades
                if str(t.get("exchange", "")) in allowed or str(t.get("source", "")) in allowed
            ]
    return trades


@app.get("/api/tape/stream")
async def tape_stream(
    symbol: str = "BTC-USD",
    limit: int = 250,
    market: str = "all",
    sources: str | None = None,
    stream_ms: int = 120,
) -> StreamingResponse:
    lim = max(1, min(2000, limit))
    wait_ms = max(50, min(1000, stream_ms))

    async def _gen() -> Any:
        last_sig = ""
        while True:
            trades = await hub.snapshot(symbol=symbol, limit=2000)
            trades = _filter_trades(trades, market=market, sources=sources)
            if len(trades) > lim:
                trades = trades[:lim]
            top = trades[0] if trades else {}
            sig = f"{top.get('ts_ms','')}|{top.get('_seq','')}|{len(trades)}|{hub.adaptive_reorder_ms()}"
            if sig != last_sig:
                last_sig = sig
                payload = {
                    "symbol": symbol,
                    "trades": trades,
                    "adaptive_reorder_ms": hub.adaptive_reorder_ms(),
                    "server_ms": _now_ms(),
                }
                yield f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
            else:
                yield ": keepalive\n\n"
            await asyncio.sleep(wait_ms / 1000.0)

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/sources/status")
async def sources_status() -> dict[str, Any]:
    now = _now_ms()
    out: dict[str, Any] = {"now_ms": now, "sources": {}, "adaptive_reorder_ms": hub.adaptive_reorder_ms()}
    for k, st in hub.states.items():
        while st.msg_times_ms and now - st.msg_times_ms[0] > 10_000:
            st.msg_times_ms.popleft()
        mps10 = round(len(st.msg_times_ms) / 10.0, 2)
        score = 100.0
        if not st.enabled:
            score = 0.0
        else:
            age_s = None if st.last_ok_ms <= 0 else max(0, (now - st.last_ok_ms) / 1000.0)
            if age_s is not None:
                score -= min(45.0, age_s * 2.6)
            score -= min(35.0, st.errors * 1.2)
            score -= min(20.0, st.reconnects * 0.8)
            score -= min(20.0, st.anomaly_drops * 0.6)
            if st.connected and mps10 < 0.25:
                score -= 8.0
            score = max(0.0, min(100.0, score))
        out["sources"][k] = {
            "enabled": st.enabled,
            "connected": st.connected,
            "errors": st.errors,
            "last_error": st.last_error,
            "last_ok_ms": st.last_ok_ms,
            "last_ok_age_s": None if st.last_ok_ms <= 0 else max(0, (now - st.last_ok_ms) // 1000),
            "reconnects": st.reconnects,
            "msg_count": st.msg_count,
            "msg_per_s_10s": mps10,
            "anomaly_drops": st.anomaly_drops,
            "lag_ewma_ms": int(st.lag_ewma_ms),
            "quality_score": round(score, 1),
            "extra": st.extra,
        }
    return out


app.mount("/", StaticFiles(directory=str(PUBLIC), html=True), name="site")
