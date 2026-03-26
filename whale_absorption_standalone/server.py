from __future__ import annotations

import asyncio
import contextlib
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
DOTENV_OVERRIDE = os.environ.get("APP_DOTENV_OVERRIDE", "0").strip() in ("1", "true", "True")
load_dotenv(BASE / ".env", override=DOTENV_OVERRIDE)

APP_SYMBOL = os.environ.get("APP_SYMBOL", "BTC-USD").strip() or "BTC-USD"
BEA_TOKEN = os.environ.get("BEA_TOKEN", "").strip()
BEA_REST_BASE = os.environ.get("BEA_REST_BASE", "https://www.bleedingedgealpha.net/api/v1").rstrip("/")
BEA_POLL_MS = int(os.environ.get("BEA_POLL_MS", "1200"))

ENABLE_BEA = os.environ.get("ENABLE_BEA", "1").strip() not in ("0", "false", "False")
ENABLE_BINANCE_PERPS = os.environ.get("ENABLE_BINANCE_PERPS", "1").strip() not in ("0", "false", "False")
ENABLE_BYBIT_LINEAR = os.environ.get("ENABLE_BYBIT_LINEAR", "1").strip() not in ("0", "false", "False")
ENABLE_OKX_SWAP = os.environ.get("ENABLE_OKX_SWAP", "1").strip() not in ("0", "false", "False")
ENABLE_HYPERLIQUID = os.environ.get("ENABLE_HYPERLIQUID", "1").strip() not in ("0", "false", "False")
ENABLE_BITFINEX = os.environ.get("ENABLE_BITFINEX", "1").strip() not in ("0", "false", "False")

REORDER_BUFFER_MS = int(os.environ.get("APP_REORDER_BUFFER_MS", "180"))
MAX_ADAPTIVE_REORDER_MS = int(os.environ.get("APP_MAX_ADAPTIVE_REORDER_MS", "900"))
MAX_NOTIONAL_USD = float(os.environ.get("APP_MAX_NOTIONAL_USD", "25000000"))
MAX_TRADE_AGE_MS = int(os.environ.get("APP_MAX_TRADE_AGE_MS", "180000"))
MAX_FUTURE_SKEW_MS = int(os.environ.get("APP_MAX_FUTURE_SKEW_MS", "2500"))

ABS_WINDOW_MS = int(os.environ.get("ABS_WINDOW_MS", "2500"))
ABS_PRICE_BPS = float(os.environ.get("ABS_PRICE_BPS", "2.0"))
ABS_MIN_BURST_USD = float(os.environ.get("ABS_MIN_BURST_USD", "1000000"))
ABS_MIN_HITS = int(os.environ.get("ABS_MIN_HITS", "4"))
ABS_MAX_DISPLACEMENT_BPS = float(os.environ.get("ABS_MAX_DISPLACEMENT_BPS", "3.0"))
ABS_MIN_DOM_SHARE = float(os.environ.get("ABS_MIN_DOM_SHARE", "0.55"))
ABS_MIN_EXCHANGES = int(os.environ.get("ABS_MIN_EXCHANGES", "2"))
ABS_EMIT_MIN_SCORE = float(os.environ.get("ABS_EMIT_MIN_SCORE", "55"))
ABS_CONFIRM_WINDOW_MS = int(os.environ.get("ABS_CONFIRM_WINDOW_MS", "7000"))
ABS_CONFIRM_REVERT_BPS = float(os.environ.get("ABS_CONFIRM_REVERT_BPS", "2.0"))
ABS_CONFIRM_BREAKOUT_BPS = float(os.environ.get("ABS_CONFIRM_BREAKOUT_BPS", "1.1"))

TF_PROFILES: dict[str, dict[str, int]] = {
    "5m": {"window_ms": 2500, "lookback_ms": 300_000},
    "12m": {"window_ms": 4200, "lookback_ms": 720_000},
    "24m": {"window_ms": 6200, "lookback_ms": 1_440_000},
    "1h": {"window_ms": 9000, "lookback_ms": 3_600_000},
}


def _resolve_tf(tf: str | None) -> str:
    t = str(tf or "5m").strip().lower()
    return t if t in TF_PROFILES else "5m"

SSL_CTX = ssl.create_default_context(cafile=certifi.where())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_ms(v: Any) -> int:
    try:
        ts = int(float(v))
    except Exception:
        return _now_ms()
    if ts < 1_000_000_000_000:
        ts *= 1000
    return ts


def _iso_to_ms(ts: str) -> int:
    try:
        s = ts.replace("Z", "+00:00")
        return int(__import__("datetime").datetime.fromisoformat(s).timestamp() * 1000)
    except Exception:
        return _now_ms()


def _binance_to_app_symbol(symbol: str) -> str:
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
    if isinstance(payload.get("data"), dict):
        d = payload["data"]
        for k in ("trades", "prints", "rows", "items"):
            v = d.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


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
    r = NOTIONAL_RULES.get(exchange, NOTIONAL_RULES["default"])
    kind = str(r.get("kind", "base"))
    if kind == "quote":
        return size
    if kind == "contracts":
        return size * float(r.get("multiplier", 1.0)) * price
    return size * price


def _compute_absorption_score(
    *,
    total_usd: float,
    min_burst_usd: float,
    trade_count: int,
    min_hits: int,
    unique_exchange_count: int,
    displacement_bps: float,
    max_displacement_bps: float,
    dominant_share: float,
    stale_penalty: bool = False,
) -> float:
    n = min(1.0, max(0.0, __import__("math").log10(total_usd / max(1.0, min_burst_usd) + 1.0)))
    h = min(1.0, trade_count / max(1.0, float(min_hits * 2)))
    e = min(1.0, unique_exchange_count / 4.0)
    d = max(0.0, min(1.0, 1.0 - (displacement_bps / max(0.0001, max_displacement_bps))))
    rf = max(0.0, min(1.0, (dominant_share - 0.5) / 0.5))
    s = 100.0 * (0.30 * n + 0.20 * h + 0.20 * e + 0.20 * d + 0.10 * rf)
    if unique_exchange_count < 2:
        s -= 15.0
    if displacement_bps > max_displacement_bps:
        s -= 25.0
    if stale_penalty:
        s -= 10.0
    return max(0.0, min(100.0, s))


@dataclass
class SourceState:
    name: str
    enabled: bool = True
    connected: bool = False
    last_ok_ms: int = 0
    errors: int = 0
    last_error: str = ""
    reconnects: int = 0
    msg_count: int = 0
    anomaly_drops: int = 0
    msg_times_ms: deque[int] = field(default_factory=lambda: deque(maxlen=4096))
    lag_ewma_ms: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)


class Hub:
    def __init__(self, maxlen: int = 20000) -> None:
        self.trades: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self.by_symbol: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=maxlen))
        self.seen: set[str] = set()
        self.seen_q: deque[str] = deque(maxlen=maxlen * 8)
        self.seq = 0
        self.lag_ewma_ms = 0.0
        self.adaptive_reorder = REORDER_BUFFER_MS
        self.lock = asyncio.Lock()
        self.states: dict[str, SourceState] = {
            "bea": SourceState("bea", enabled=ENABLE_BEA and bool(BEA_TOKEN)),
            "binance_perps": SourceState("binance_perps", enabled=ENABLE_BINANCE_PERPS),
            "bybit_linear": SourceState("bybit_linear", enabled=ENABLE_BYBIT_LINEAR),
            "okx_swap": SourceState("okx_swap", enabled=ENABLE_OKX_SWAP),
            "hyperliquid_perps": SourceState("hyperliquid_perps", enabled=ENABLE_HYPERLIQUID),
            "bitfinex_spot": SourceState("bitfinex_spot", enabled=ENABLE_BITFINEX),
        }

    def _track_lag(self, st: SourceState | None, lag_ms: int) -> None:
        if lag_ms < 0 or lag_ms > 60000:
            return
        self.lag_ewma_ms = float(lag_ms) if self.lag_ewma_ms <= 0 else self.lag_ewma_ms * 0.92 + lag_ms * 0.08
        self.adaptive_reorder = min(MAX_ADAPTIVE_REORDER_MS, max(REORDER_BUFFER_MS, int(self.lag_ewma_ms * 1.35) + 35))
        if st:
            st.lag_ewma_ms = float(lag_ms) if st.lag_ewma_ms <= 0 else st.lag_ewma_ms * 0.92 + lag_ms * 0.08

    def _mk_key(self, t: dict[str, Any]) -> str:
        tid = str(t.get("trade_id") or "")
        if tid:
            return f"{t.get('source')}|{t.get('exchange')}|{tid}"
        raw = (
            f"{t.get('source')}|{t.get('exchange')}|{int(t.get('ts_ms') or 0)}|"
            f"{float(t.get('price') or 0):.2f}|{float(t.get('size_usd') or 0):.2f}|{t.get('side')}"
        )
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

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

    async def add_trade(self, t: dict[str, Any]) -> None:
        now_ms = _now_ms()
        st = self.states.get(str(t.get("source") or ""))
        bad, reason = self._is_anomalous(t, now_ms)
        if bad:
            if st:
                st.anomaly_drops += 1
                st.extra["last_drop_reason"] = reason
            return
        ts = int(t.get("ts_ms") or 0)
        if ts > 0:
            self._track_lag(st, now_ms - ts)
        key = self._mk_key(t)
        async with self.lock:
            if key in self.seen:
                return
            self.seen.add(key)
            self.seen_q.append(key)
            self.seq += 1
            t["_seq"] = self.seq
            self.trades.appendleft(t)
            sym = str(t.get("symbol", "")).upper()
            if sym:
                self.by_symbol[sym].appendleft(t)
            if st:
                st.msg_count += 1
                st.msg_times_ms.append(now_ms)
            while len(self.seen_q) > self.trades.maxlen * 5:
                self.seen.discard(self.seen_q.popleft())

    async def snapshot(self, symbol: str, limit: int = 2000) -> list[dict[str, Any]]:
        now = _now_ms()
        cutoff = now - max(0, self.adaptive_reorder)
        out: list[dict[str, Any]] = []
        async with self.lock:
            cand_list = list(self.by_symbol.get(symbol.upper(), deque()))
            for t in cand_list:
                ts = int(t.get("ts_ms") or 0)
                if ts > 0 and ts > cutoff:
                    continue
                out.append(t)
            if not out and cand_list:
                out = cand_list[:]
        out.sort(key=lambda x: (int(x.get("ts_ms") or 0), int(x.get("_seq") or 0)), reverse=True)
        return out[: max(1, min(5000, limit))]


class AbsorptionEngine:
    def __init__(self, hub: Hub) -> None:
        self.hub = hub
        self.window_ms = ABS_WINDOW_MS
        self.price_bps = ABS_PRICE_BPS
        self.min_burst_usd = ABS_MIN_BURST_USD
        self.min_hits = ABS_MIN_HITS
        self.max_displacement_bps = ABS_MAX_DISPLACEMENT_BPS
        self.min_dom_share = ABS_MIN_DOM_SHARE
        self.min_exchanges = ABS_MIN_EXCHANGES
        self.min_emit_score = ABS_EMIT_MIN_SCORE
        self.confirm_window_ms = ABS_CONFIRM_WINDOW_MS
        self.confirm_revert_bps = ABS_CONFIRM_REVERT_BPS
        self.confirm_breakout_bps = ABS_CONFIRM_BREAKOUT_BPS
        self.events_by_symbol: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=800))
        self.event_seen: set[str] = set()
        self.event_seen_q: deque[str] = deque(maxlen=10000)
        self.lock = asyncio.Lock()

    async def process_symbol(
        self,
        symbol: str,
        source_filter: str | None = None,
        tf: str = "5m",
        min_burst_override: float | None = None,
    ) -> None:
        tf_key = _resolve_tf(tf)
        window_ms = int(TF_PROFILES[tf_key]["window_ms"])
        min_burst_usd = float(min_burst_override) if (min_burst_override is not None and min_burst_override > 0) else self.min_burst_usd
        trades = await self.hub.snapshot(symbol, limit=2200)
        if source_filter:
            allowed = {x.strip() for x in source_filter.split(",") if x.strip()}
            trades = [t for t in trades if str(t.get("source", "")) in allowed]
        now = _now_ms()
        recent = [t for t in trades if now - int(t.get("ts_ms") or now) <= window_ms]
        if len(recent) < self.min_hits:
            return
        groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for t in recent:
            p = float(t.get("price") or 0)
            side = "buy" if str(t.get("side") or "").lower().startswith("b") else "sell"
            band_width = max(0.5, p * (self.price_bps / 10000.0))
            band = int(round(p / band_width))
            groups[(side, band)].append(t)

        for (dominant_side, band), bucket in groups.items():
            if len(bucket) < self.min_hits:
                continue
            bucket.sort(key=lambda x: int(x.get("ts_ms") or 0))
            total = sum(float(x.get("size_usd") or 0) for x in bucket)
            dom = sum(float(x.get("size_usd") or 0) for x in bucket if str(x.get("side", "")).lower().startswith(dominant_side[0]))
            opp = max(0.0, total - dom)
            dom_share = dom / max(1e-9, total)
            if total < min_burst_usd or dom_share < self.min_dom_share:
                continue
            exchanges = sorted({str(x.get("exchange") or "") for x in bucket if x.get("exchange")})
            if len(exchanges) < self.min_exchanges:
                continue
            p0 = float(bucket[0].get("price") or 0)
            p1 = float(bucket[-1].get("price") or 0)
            mid = max(1.0, (p0 + p1) / 2.0)
            displacement_bps = abs(p1 - p0) / mid * 10000.0
            score = _compute_absorption_score(
                total_usd=total,
                min_burst_usd=min_burst_usd,
                trade_count=len(bucket),
                min_hits=self.min_hits,
                unique_exchange_count=len(exchanges),
                displacement_bps=displacement_bps,
                max_displacement_bps=self.max_displacement_bps,
                dominant_share=dom_share,
                stale_penalty=False,
            )
            if score < self.min_emit_score:
                continue
            ts0 = int(bucket[0].get("ts_ms") or now)
            ts1 = int(bucket[-1].get("ts_ms") or now)
            zone = sum(float(x.get("price") or 0) * float(x.get("size_usd") or 0) for x in bucket) / max(1e-9, total)
            post = [
                t
                for t in trades
                if int(t.get("ts_ms") or 0) > ts1 and int(t.get("ts_ms") or 0) <= ts1 + self.confirm_window_ms
            ]
            state = "ABSORBING"
            meaning = "Aggressive flow is meeting strong passive liquidity."
            confirm = self._confirm_failure(
                dominant_side=dominant_side,
                ref_price=zone,
                post_trades=post,
            )
            if score >= 85:
                state = "HEAVY_ABSORBING"
                meaning = "Heavy aggression is being absorbed with weak price response."
            if confirm["confirmed"]:
                state = "CONFIRMED_FAILURE"
                meaning = "Aggressive move is likely dying; reversal pressure is visible."
            event_id = hashlib.sha1(f"{symbol}|{tf_key}|{dominant_side}|{band}|{ts1}|{round(total,2)}".encode("utf-8")).hexdigest()
            if event_id in self.event_seen:
                continue
            confidence = "high" if score >= 85 else "medium" if score >= 70 else "low"
            event = {
                "event_id": event_id,
                "symbol": symbol,
                "timeframe": tf_key,
                "ts_start_ms": ts0,
                "ts_end_ms": ts1,
                "zone_price": zone,
                "band_bps": self.price_bps,
                "dominant_side": dominant_side,
                "total_usd": total,
                "trade_count": len(bucket),
                "unique_exchange_count": len(exchanges),
                "displacement_bps": displacement_bps,
                "absorption_score": score,
                "confidence": confidence,
                "state": state,
                "meaning": meaning,
                "exchanges": exchanges,
                "source_mix": dict((k, sum(float(x.get("size_usd") or 0) for x in bucket if str(x.get("source")) == k)) for k in {str(x.get("source")) for x in bucket}),
                "flow": {"dominant_usd": dom, "opposing_usd": opp, "dominant_share": dom_share},
                "confirmation": confirm,
            }
            band_half = zone * (self.price_bps / 10000.0)
            zone_low = zone - band_half
            zone_high = zone + band_half
            if dominant_side == "buy":
                # Buyers absorbed: downside confirmation, upside failure.
                event["confirmation_level"] = zone_low
                event["failure_level"] = zone_high
            else:
                # Sellers absorbed: upside confirmation, downside failure.
                event["confirmation_level"] = zone_high
                event["failure_level"] = zone_low
            event["zone_low"] = zone_low
            event["zone_high"] = zone_high
            async with self.lock:
                self.event_seen.add(event_id)
                self.event_seen_q.append(event_id)
                self.events_by_symbol[f"{symbol.upper()}|{tf_key}"].appendleft(event)
                while len(self.event_seen_q) > 8000:
                    self.event_seen.discard(self.event_seen_q.popleft())

    async def events(self, symbol: str, limit: int = 100, tf: str = "5m") -> list[dict[str, Any]]:
        tf_key = _resolve_tf(tf)
        async with self.lock:
            return list(self.events_by_symbol[f"{symbol.upper()}|{tf_key}"])[: max(1, min(1000, limit))]

    async def stats(self, symbol: str, tf: str = "5m") -> dict[str, Any]:
        tf_key = _resolve_tf(tf)
        lookback_ms = int(TF_PROFILES[tf_key]["lookback_ms"])
        ev = await self.events(symbol, limit=300, tf=tf_key)
        if not ev:
            return {"tf": tf_key, "events_window": 0, "high_conf_window": 0, "confirmed_window": 0, "avg_score_window": 0.0, "notional_window": 0.0}
        now = _now_ms()
        w = [e for e in ev if now - int(e.get("ts_end_ms") or now) <= lookback_ms]
        if not w:
            return {"tf": tf_key, "events_window": 0, "high_conf_window": 0, "confirmed_window": 0, "avg_score_window": 0.0, "notional_window": 0.0}
        return {
            "tf": tf_key,
            "events_window": len(w),
            "high_conf_window": sum(1 for e in w if str(e.get("confidence")) == "high"),
            "confirmed_window": sum(1 for e in w if str(e.get("state")) == "CONFIRMED_FAILURE"),
            "avg_score_window": round(sum(float(e.get("absorption_score") or 0) for e in w) / len(w), 1),
            "notional_window": round(sum(float(e.get("total_usd") or 0) for e in w), 2),
        }

    def _confirm_failure(self, *, dominant_side: str, ref_price: float, post_trades: list[dict[str, Any]]) -> dict[str, Any]:
        if not post_trades:
            return {"confirmed": False, "reason": "no_post_flow", "opp_share": 0.0}
        px = [float(t.get("price") or 0) for t in post_trades if float(t.get("price") or 0) > 0]
        if not px or ref_price <= 0:
            return {"confirmed": False, "reason": "bad_prices", "opp_share": 0.0}
        hi = max(px)
        lo = min(px)
        post_buy = sum(float(t.get("size_usd") or 0) for t in post_trades if str(t.get("side", "")).lower().startswith("b"))
        post_sell = sum(float(t.get("size_usd") or 0) for t in post_trades if str(t.get("side", "")).lower().startswith("s"))
        total = max(1e-9, post_buy + post_sell)
        opp_share = (post_sell / total) if dominant_side == "buy" else (post_buy / total)
        up_bps = (hi - ref_price) / ref_price * 10000.0
        down_bps = (ref_price - lo) / ref_price * 10000.0
        if dominant_side == "buy":
            breakout_ok = up_bps <= self.confirm_breakout_bps
            revert_ok = down_bps >= self.confirm_revert_bps
        else:
            breakout_ok = down_bps <= self.confirm_breakout_bps
            revert_ok = up_bps >= self.confirm_revert_bps
        confirmed = bool(breakout_ok and revert_ok and opp_share >= 0.52)
        return {
            "confirmed": confirmed,
            "reason": "confirmed_failure" if confirmed else "unconfirmed",
            "opp_share": round(opp_share, 3),
            "breakout_bps": round(up_bps if dominant_side == "buy" else down_bps, 2),
            "revert_bps": round(down_bps if dominant_side == "buy" else up_bps, 2),
        }

    async def matrix(self, symbol: str, tf: str = "5m") -> dict[str, Any]:
        tf_key = _resolve_tf(tf)
        lookback_ms = int(min(120_000, max(30_000, TF_PROFILES[tf_key]["lookback_ms"] // 10)))
        ev = await self.events(symbol, limit=80, tf=tf_key)
        now = _now_ms()
        w = [e for e in ev if now - int(e.get("ts_end_ms") or now) <= lookback_ms]
        if not w:
            return {
                "tf": tf_key,
                "flow_pressure": 0,
                "response_efficiency": 0,
                "venue_agreement": 0,
                "failure_confirmation": 0,
                "state": "IDLE",
                "message": "No recent absorption cluster.",
                "top_zones": [],
            }
        flow = min(100.0, sum(float(e.get("total_usd") or 0) for e in w) / 3_500_000.0 * 100.0)
        resp = max(0.0, min(100.0, 100.0 - (sum(float(e.get("displacement_bps") or 0) for e in w) / max(1, len(w)) / 6.0 * 100.0)))
        venue = max(0.0, min(100.0, (sum(int(e.get("unique_exchange_count") or 0) for e in w) / max(1, len(w)) / 6.0) * 100.0))
        conf = max(0.0, min(100.0, (sum(1 for e in w if str(e.get("state")) == "CONFIRMED_FAILURE") / max(1, len(w))) * 100.0))
        state = "BUILDING"
        if conf >= 45:
            state = "CONFIRMED_FAILURE"
        elif flow >= 55 and resp >= 55:
            state = "ABSORBING"
        msg = {
            "BUILDING": "Absorption is building but not confirmed yet.",
            "ABSORBING": "Heavy aggression is being absorbed. Watch for failure confirmation.",
            "CONFIRMED_FAILURE": "Move-death conditions detected: absorption + failed follow-through.",
        }[state]
        top = sorted(w, key=lambda x: float(x.get("absorption_score") or 0), reverse=True)[:12]
        return {
            "tf": tf_key,
            "flow_pressure": round(flow, 1),
            "response_efficiency": round(resp, 1),
            "venue_agreement": round(venue, 1),
            "failure_confirmation": round(conf, 1),
            "state": state,
            "message": msg,
            "top_zones": [
                {
                    "zone_price": float(x.get("zone_price") or 0),
                    "score": float(x.get("absorption_score") or 0),
                    "state": str(x.get("state") or ""),
                    "dominant_side": str(x.get("dominant_side") or ""),
                }
                for x in top
            ],
        }

    async def ladder(
        self,
        symbol: str,
        tf: str = "5m",
        source_filter: str | None = None,
        half_levels: int = 18,
    ) -> dict[str, Any]:
        """Aggregate aggressive flow by price band for a DOM-style absorption ladder."""
        tf_key = _resolve_tf(tf)
        lookback_ms = min(int(TF_PROFILES[tf_key]["lookback_ms"]), 1_800_000)
        trades = await self.hub.snapshot(symbol, limit=5000)
        if source_filter:
            allowed = {x.strip() for x in source_filter.split(",") if x.strip()}
            trades = [t for t in trades if str(t.get("source", "")) in allowed]
        now = _now_ms()
        recent = [t for t in trades if now - int(t.get("ts_ms") or now) <= lookback_ms]
        if not recent:
            return {
                "tf": tf_key,
                "anchor_price": 0.0,
                "band_width": 0.0,
                "price_bps": self.price_bps,
                "lookback_ms": lookback_ms,
                "max_usd": 1.0,
                "rows": [],
                "markers": {},
            }

        recent.sort(key=lambda x: int(x.get("ts_ms") or 0))
        tail = recent[-min(400, len(recent)) :]
        tw = sum(float(t.get("size_usd") or 0) for t in tail)
        if tw > 0:
            anchor = sum(float(t.get("price") or 0) * float(t.get("size_usd") or 0) for t in tail) / tw
        else:
            anchor = float(tail[-1].get("price") or 0)
        if anchor <= 0:
            return {
                "tf": tf_key,
                "anchor_price": 0.0,
                "band_width": 0.0,
                "price_bps": self.price_bps,
                "lookback_ms": lookback_ms,
                "max_usd": 1.0,
                "rows": [],
                "markers": {},
            }

        band_width = max(0.5, anchor * (self.price_bps / 10000.0))
        buckets: dict[int, dict[str, Any]] = {}
        for t in recent:
            p = float(t.get("price") or 0)
            if p <= 0:
                continue
            k = int(round(p / band_width))
            if k not in buckets:
                buckets[k] = {"buy_usd": 0.0, "sell_usd": 0.0, "hits": 0, "exchanges": set()}
            usd = float(t.get("size_usd") or 0)
            side = str(t.get("side") or "").lower()
            if side.startswith("b"):
                buckets[k]["buy_usd"] += usd
            else:
                buckets[k]["sell_usd"] += usd
            buckets[k]["hits"] += 1
            ex = str(t.get("exchange") or "")
            if ex:
                buckets[k]["exchanges"].add(ex)

        k_center = int(round(anchor / band_width))
        hl = max(6, min(32, int(half_levels)))
        k_lo = k_center - hl
        k_hi = k_center + hl

        rows: list[dict[str, Any]] = []
        max_usd = 1.0
        for k in range(k_lo, k_hi + 1):
            b = buckets.get(k)
            buy_usd = float(b.get("buy_usd", 0.0)) if b else 0.0
            sell_usd = float(b.get("sell_usd", 0.0)) if b else 0.0
            hits = int(b.get("hits", 0)) if b else 0
            venues = len(b.get("exchanges", set())) if b else 0
            tot = buy_usd + sell_usd
            max_usd = max(max_usd, buy_usd, sell_usd, 1.0)
            net = buy_usd - sell_usd
            imb = (buy_usd / tot) if tot > 0 else 0.5
            price_mid = k * band_width
            rows.append(
                {
                    "band": k,
                    "price": round(price_mid, 2 if price_mid >= 1000 else 4),
                    "buy_usd": round(buy_usd, 2),
                    "sell_usd": round(sell_usd, 2),
                    "net_usd": round(net, 2),
                    "hits": hits,
                    "venues": venues,
                    "imbalance": round(imb, 3),
                }
            )

        ev = await self.events(symbol, limit=5, tf=tf_key)
        markers: dict[str, float] = {}
        if ev:
            e = ev[0]
            for key in ("confirmation_level", "failure_level", "zone_low", "zone_high", "zone_price"):
                v = e.get(key)
                if v is not None:
                    try:
                        markers[key] = float(v)
                    except Exception:
                        pass

        rows.sort(key=lambda r: float(r.get("price") or 0), reverse=True)

        return {
            "tf": tf_key,
            "anchor_price": round(anchor, 2),
            "band_width": round(band_width, 6),
            "price_bps": self.price_bps,
            "lookback_ms": lookback_ms,
            "max_usd": round(max_usd, 2),
            "rows": rows,
            "markers": markers,
        }


hub = Hub()
engine = AbsorptionEngine(hub)
app = FastAPI(title="Whale Absorption Detector")
tasks: list[asyncio.Task[Any]] = []
http_client: httpx.AsyncClient | None = None
_feeds_started = False


def _mark_ok(st: SourceState, extra: dict[str, Any] | None = None, connected_event: bool = False) -> None:
    st.connected = True
    st.last_ok_ms = _now_ms()
    if connected_event:
        st.reconnects += 1
    st.extra["retry_attempts"] = 0
    st.extra["retry_delay_ms"] = 0
    if extra:
        st.extra.update(extra)


def _mark_err(st: SourceState, e: Exception) -> None:
    st.connected = False
    st.errors += 1
    st.last_error = str(e)[:180]


async def _sleep_retry(st: SourceState, base: float = 0.8, cap: float = 12.0) -> None:
    attempts = int(st.extra.get("retry_attempts") or 0) + 1
    st.extra["retry_attempts"] = attempts
    delay = min(cap, base * (2 ** min(attempts, 6))) + random.uniform(0.0, 0.25)
    st.extra["retry_delay_ms"] = int(delay * 1000)
    await asyncio.sleep(delay)


def _from_bea_row(row: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    try:
        px = float(row.get("price") or row.get("px") or row.get("p") or 0)
        sz = float(row.get("size") or row.get("qty") or row.get("q") or 0)
    except Exception:
        return None
    if px <= 0 or sz <= 0:
        return None
    ex = str(row.get("exchange") or row.get("venue") or "bea_unknown")
    side_raw = str(row.get("side") or row.get("aggressor") or "").lower()
    side = "sell" if ("sell" in side_raw or side_raw in ("s", "ask", "-1")) else "buy"
    return {
        "source": "bea",
        "symbol": symbol,
        "exchange": ex,
        "price": px,
        "side": side,
        "size_usd": _notional_from_size(ex, px, sz, row),
        "ts_ms": _to_ms(row.get("ts_ms") or row.get("t") or row.get("time") or _now_ms()),
        "trade_id": str(row.get("id") or row.get("trade_id") or row.get("tid") or ""),
    }


async def _bea_loop() -> None:
    st = hub.states["bea"]
    if not st.enabled:
        return
    assert http_client is not None
    url = f"{BEA_REST_BASE}/market/tape"
    headers = {"Authorization": f"Bearer {BEA_TOKEN}"}
    poll_sec = max(0.35, BEA_POLL_MS / 1000.0)
    while True:
        try:
            r = await http_client.get(url, params={"symbol": APP_SYMBOL}, headers=headers)
            if r.status_code != 200:
                raise RuntimeError(f"bea {r.status_code}")
            rows = _norm_rows(r.json())
            _mark_ok(st, {"last_count": len(rows), "poll_ms": BEA_POLL_MS})
            for row in rows:
                t = _from_bea_row(row, APP_SYMBOL)
                if t:
                    await hub.add_trade(t)
            await asyncio.sleep(poll_sec)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(st, e)
            await _sleep_retry(st, base=poll_sec, cap=8.0)


async def _binance_perps_loop(symbol: str = "BTCUSDT") -> None:
    st = hub.states["binance_perps"]
    if not st.enabled:
        return
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@aggTrade"
    app_symbol = _binance_to_app_symbol(symbol)
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                _mark_ok(st, {"symbol": symbol}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if d.get("e") != "aggTrade":
                        continue
                    px = float(d["p"])
                    qty = float(d["q"])
                    await hub.add_trade(
                        {
                            "source": "binance_perps",
                            "symbol": app_symbol,
                            "exchange": "binance_perps",
                            "price": px,
                            "side": "sell" if bool(d.get("m")) else "buy",
                            "size_usd": _notional_from_size("binance_perps", px, qty),
                            "ts_ms": _to_ms(d.get("T") or _now_ms()),
                            "trade_id": str(d.get("a") or ""),
                        }
                    )
                    st.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(st, e)
            await _sleep_retry(st)


async def _bybit_linear_loop(symbol: str = "BTCUSDT") -> None:
    st = hub.states["bybit_linear"]
    if not st.enabled:
        return
    url = "wss://stream.bybit.com/v5/public/linear"
    sub = {"op": "subscribe", "args": [f"publicTrade.{symbol}"]}
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(st, {"symbol": symbol}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if not str(d.get("topic", "")).startswith("publicTrade."):
                        continue
                    for tr in d.get("data", []):
                        px = float(tr.get("p") or 0)
                        qty = float(tr.get("v") or 0)
                        if px <= 0 or qty <= 0:
                            continue
                        await hub.add_trade(
                            {
                                "source": "bybit_linear",
                                "symbol": APP_SYMBOL,
                                "exchange": "bybit_perps",
                                "price": px,
                                "side": "buy" if str(tr.get("S", "")).lower().startswith("b") else "sell",
                                "size_usd": _notional_from_size("bybit_perps", px, qty),
                                "ts_ms": _to_ms(tr.get("T") or _now_ms()),
                                "trade_id": str(tr.get("i") or ""),
                            }
                        )
                        st.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(st, e)
            await _sleep_retry(st)


async def _okx_swap_loop(inst_id: str = "BTC-USDT-SWAP") -> None:
    st = hub.states["okx_swap"]
    if not st.enabled:
        return
    url = "wss://ws.okx.com:8443/ws/v5/public"
    sub = {"op": "subscribe", "args": [{"channel": "trades", "instId": inst_id}]}
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(st, {"instId": inst_id}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    for tr in d.get("data", []):
                        px = float(tr.get("px") or 0)
                        qty = float(tr.get("sz") or 0)
                        if px <= 0 or qty <= 0:
                            continue
                        await hub.add_trade(
                            {
                                "source": "okx_swap",
                                "symbol": APP_SYMBOL,
                                "exchange": "okx_perps",
                                "price": px,
                                "side": str(tr.get("side") or "buy").lower(),
                                "size_usd": _notional_from_size("okx_perps", px, qty),
                                "ts_ms": _to_ms(tr.get("ts") or _now_ms()),
                                "trade_id": str(tr.get("tradeId") or ""),
                            }
                        )
                        st.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(st, e)
            await _sleep_retry(st)


async def _hyperliquid_loop() -> None:
    st = hub.states["hyperliquid_perps"]
    if not st.enabled:
        return
    url = "wss://api.hyperliquid.xyz/ws"
    sub = {"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}}
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(st, {"coin": "BTC"}, connected_event=True)
                async for msg in ws:
                    d = json.loads(msg)
                    if str(d.get("channel")) != "trades":
                        continue
                    data = d.get("data")
                    if not isinstance(data, list):
                        continue
                    for tr in data:
                        if not isinstance(tr, dict):
                            continue
                        px = float(tr.get("px") or 0)
                        qty = float(tr.get("sz") or 0)
                        if px <= 0 or qty <= 0:
                            continue
                        await hub.add_trade(
                            {
                                "source": "hyperliquid_perps",
                                "symbol": APP_SYMBOL,
                                "exchange": "hyperliquid_perps",
                                "price": px,
                                "side": "sell" if str(tr.get("side") or "").lower() in {"a", "ask", "sell", "s"} else "buy",
                                "size_usd": _notional_from_size("hyperliquid_perps", px, qty),
                                "ts_ms": _to_ms(tr.get("time") or _now_ms()),
                                "trade_id": str(tr.get("tid") or tr.get("hash") or ""),
                            }
                        )
                        st.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(st, e)
            await _sleep_retry(st)


async def _bitfinex_loop(symbol: str = "tBTCUSD") -> None:
    st = hub.states["bitfinex_spot"]
    if not st.enabled:
        return
    url = "wss://api-pub.bitfinex.com/ws/2"
    sub = {"event": "subscribe", "channel": "trades", "symbol": symbol}
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, ssl=SSL_CTX, proxy=None) as ws:
                await ws.send(json.dumps(sub))
                _mark_ok(st, {"symbol": symbol}, connected_event=True)
                chan_id: int | None = None
                async for msg in ws:
                    d = json.loads(msg)
                    if isinstance(d, dict):
                        if d.get("event") == "subscribed" and d.get("channel") == "trades":
                            chan_id = int(d.get("chanId") or 0)
                        continue
                    if not isinstance(d, list) or len(d) < 2:
                        continue
                    if chan_id and d[0] != chan_id:
                        continue
                    payload = d[1]
                    rows: list[Any] = []
                    if payload == "te" and len(d) >= 3 and isinstance(d[2], list):
                        rows = [d[2]]
                    elif isinstance(payload, list) and payload and isinstance(payload[0], list):
                        rows = payload
                    for tr in rows:
                        px = float(tr[3] or 0)
                        amt = float(tr[2] or 0)
                        if px <= 0 or amt == 0:
                            continue
                        await hub.add_trade(
                            {
                                "source": "bitfinex_spot",
                                "symbol": APP_SYMBOL,
                                "exchange": "bitfinex_spot",
                                "price": px,
                                "side": "buy" if amt > 0 else "sell",
                                "size_usd": _notional_from_size("bitfinex_spot", px, abs(amt)),
                                "ts_ms": _to_ms(tr[1]),
                                "trade_id": str(tr[0]),
                            }
                        )
                        st.last_ok_ms = _now_ms()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _mark_err(st, e)
            await _sleep_retry(st)


async def _engine_loop() -> None:
    while True:
        await engine.process_symbol(APP_SYMBOL)
        await asyncio.sleep(0.12)


@app.on_event("startup")
async def _startup() -> None:
    global http_client, _feeds_started
    if _feeds_started:
        return
    _feeds_started = True
    http_client = httpx.AsyncClient(timeout=25.0, trust_env=False)
    tasks.append(asyncio.create_task(_engine_loop(), name="abs-engine"))
    if ENABLE_BEA and bool(BEA_TOKEN):
        tasks.append(asyncio.create_task(_bea_loop(), name="bea"))
    if ENABLE_BINANCE_PERPS:
        tasks.append(asyncio.create_task(_binance_perps_loop(), name="binance"))
    if ENABLE_BYBIT_LINEAR:
        tasks.append(asyncio.create_task(_bybit_linear_loop(), name="bybit"))
    if ENABLE_OKX_SWAP:
        tasks.append(asyncio.create_task(_okx_swap_loop(), name="okx"))
    if ENABLE_HYPERLIQUID:
        tasks.append(asyncio.create_task(_hyperliquid_loop(), name="hyperliquid"))
    if ENABLE_BITFINEX:
        tasks.append(asyncio.create_task(_bitfinex_loop(), name="bitfinex"))


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


@app.get("/api/capabilities")
async def capabilities() -> dict[str, Any]:
    return {
        "module": "whale_absorption",
        "available": True,
        "sources": {
            "bea": {"available": ENABLE_BEA and bool(BEA_TOKEN), "reason": "" if (ENABLE_BEA and bool(BEA_TOKEN)) else "missing_api_key_or_disabled"},
            "binance_perps": {"available": ENABLE_BINANCE_PERPS},
            "bybit_linear": {"available": ENABLE_BYBIT_LINEAR},
            "okx_swap": {"available": ENABLE_OKX_SWAP},
            "hyperliquid_perps": {"available": ENABLE_HYPERLIQUID},
            "bitfinex_spot": {"available": ENABLE_BITFINEX},
        },
    }


def _filter_sources(rows: list[dict[str, Any]], sources: str | None) -> list[dict[str, Any]]:
    if not sources:
        return rows
    allowed = {s.strip() for s in sources.split(",") if s.strip()}
    if not allowed:
        return rows
    return [x for x in rows if str(x.get("source", "")) in allowed or str(x.get("exchange", "")) in allowed]


@app.get("/api/absorption/debug")
async def absorption_debug(symbol: str = APP_SYMBOL, tf: str = "5m") -> dict[str, Any]:
    """Inbound trades for the absorption engine: if `hub_snapshot_count` is 0, feeds are not filling this process."""
    tf_key = _resolve_tf(tf)
    snap = await hub.snapshot(symbol, limit=3000)
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
    ev = await engine.events(symbol, limit=20, tf=tf_key)
    return {
        "symbol": symbol.upper(),
        "tf": tf_key,
        "global_deque_count": len(hub.trades),
        "hub_snapshot_count": len(snap),
        "emitted_events_queued": len(ev),
        "feeds_started": _feeds_started,
        "background_tasks": len(tasks),
        "http_client_ready": http_client is not None,
        "sources": src_out,
    }


@app.get("/api/absorption/live")
async def absorption_live(
    symbol: str = APP_SYMBOL,
    limit: int = 100,
    sources: str | None = None,
    tf: str = "5m",
    min_burst_usd: float | None = None,
) -> dict[str, Any]:
    tf_key = _resolve_tf(tf)
    await engine.process_symbol(symbol, source_filter=sources, tf=tf_key, min_burst_override=min_burst_usd)
    rows = await engine.events(symbol, limit=limit, tf=tf_key)
    rows = _filter_sources(rows, sources)
    return {"symbol": symbol, "tf": tf_key, "events": rows}


@app.get("/api/absorption/stats")
async def absorption_stats(symbol: str = APP_SYMBOL, tf: str = "5m", min_burst_usd: float | None = None) -> dict[str, Any]:
    tf_key = _resolve_tf(tf)
    await engine.process_symbol(symbol, tf=tf_key, min_burst_override=min_burst_usd)
    return {"symbol": symbol, "tf": tf_key, "stats": await engine.stats(symbol, tf=tf_key)}


@app.get("/api/absorption/matrix")
async def absorption_matrix(symbol: str = APP_SYMBOL, tf: str = "5m", min_burst_usd: float | None = None) -> dict[str, Any]:
    tf_key = _resolve_tf(tf)
    await engine.process_symbol(symbol, tf=tf_key, min_burst_override=min_burst_usd)
    return {"symbol": symbol, "tf": tf_key, "matrix": await engine.matrix(symbol, tf=tf_key)}


@app.get("/api/absorption/ladder")
async def absorption_ladder(
    symbol: str = APP_SYMBOL,
    tf: str = "5m",
    sources: str | None = None,
    half_levels: int = 18,
) -> dict[str, Any]:
    tf_key = _resolve_tf(tf)
    hl = max(6, min(32, int(half_levels)))
    lad = await engine.ladder(symbol, tf=tf_key, source_filter=sources, half_levels=hl)
    return {"symbol": symbol, "tf": tf_key, "ladder": lad}


@app.get("/api/absorption/stream")
async def absorption_stream(
    symbol: str = APP_SYMBOL,
    limit: int = 100,
    sources: str | None = None,
    tf: str = "5m",
    stream_ms: int = 120,
    min_burst_usd: float | None = None,
) -> StreamingResponse:
    tf_key = _resolve_tf(tf)
    lim = max(1, min(1000, limit))
    wait_ms = max(50, min(1000, stream_ms))

    async def _gen() -> Any:
        last = ""
        while True:
            await engine.process_symbol(symbol, source_filter=sources, tf=tf_key, min_burst_override=min_burst_usd)
            rows = await engine.events(symbol, limit=lim, tf=tf_key)
            rows = _filter_sources(rows, sources)
            sig = f"{tf_key}|{len(rows)}|{(rows[0].get('event_id') if rows else '')}"
            if sig != last:
                last = sig
                yield f"data: {json.dumps({'symbol': symbol, 'tf': tf_key, 'events': rows}, separators=(',', ':'))}\n\n"
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
    out: dict[str, Any] = {"now_ms": now, "sources": {}}
    for k, st in hub.states.items():
        while st.msg_times_ms and now - st.msg_times_ms[0] > 10000:
            st.msg_times_ms.popleft()
        mps = round(len(st.msg_times_ms) / 10.0, 2)
        score = 100.0 if st.enabled else 0.0
        if st.enabled:
            age_s = 0.0 if st.last_ok_ms <= 0 else max(0.0, (now - st.last_ok_ms) / 1000.0)
            score -= min(45.0, age_s * 2.5)
            score -= min(35.0, st.errors * 1.2)
            score -= min(20.0, st.reconnects * 0.8)
            score -= min(20.0, st.anomaly_drops * 0.6)
            score = max(0.0, min(100.0, score))
        out["sources"][k] = {
            "enabled": st.enabled,
            "connected": st.connected,
            "last_ok_age_s": None if st.last_ok_ms <= 0 else max(0, (now - st.last_ok_ms) // 1000),
            "errors": st.errors,
            "reconnects": st.reconnects,
            "msg_per_s_10s": mps,
            "anomaly_drops": st.anomaly_drops,
            "lag_ewma_ms": int(st.lag_ewma_ms),
            "quality_score": round(score, 1),
            "last_error": st.last_error,
            "extra": st.extra,
        }
    return out


@app.get("/api/bea/tape")
async def bea_tape(symbol: str = APP_SYMBOL) -> dict | list:
    if not BEA_TOKEN:
        raise HTTPException(503, detail="Set BEA_TOKEN in whale_absorption_standalone/.env")
    assert http_client is not None
    r = await http_client.get(
        f"{BEA_REST_BASE}/market/tape",
        params={"symbol": symbol},
        headers={"Authorization": f"Bearer {BEA_TOKEN}"},
    )
    if r.status_code != 200:
        raise HTTPException(r.status_code, detail=r.text[:800] if r.text else "BEA error")
    return r.json()


app.mount("/", StaticFiles(directory=str(PUBLIC), html=True), name="site")
