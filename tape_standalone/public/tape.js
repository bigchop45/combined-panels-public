/**
 * Canvas tape + optional BEA poll (via same-origin proxy — never put BEA_TOKEN in the browser).
 */
/** @typedef {Object} TapePrint
 * @property {boolean} isBuy
 * @property {number} price
 * @property {number} size_usd
 * @property {string} exchange
 * @property {boolean} isMulti
 * @property {number} age_seconds
 * @property {number} bps
 * @property {number} [t_ms]
 * @property {number} [born_ms]
 * @property {string} [source]
 */

const BG = "#060e1a";
const SEP = "#06090d";

/** Default min notional (USD). Can be overridden live from settings panel. */
const DEFAULT_MIN_PRINT_USD = 400_000;
const DEFAULT_BURST_WINDOW_MS = 220;
const DEFAULT_BURST_PRICE_BPS = 2.0;
const NEW_PRINT_SHIMMER_MS = 180;

/** Single-trade notional above this (USD) is almost always a unit bug (e.g. contracts vs BTC). */
const NAIVE_NOTIONAL_CAP_USD = 12_000_000;

/** Resolve API paths against `<base href>` / document URL (required for EventSource under `/tape/` in combined mode). */
export function apiUrl(path) {
  const p = path.startsWith("/") ? path.slice(1) : path;
  try {
    return new URL(p, document.baseURI).href;
  } catch {
    return `/${p}`;
  }
}

/**
 * BEA tape `size` is not always base BTC: some venues (e.g. mexc_perps) send **contracts**
 * (often 0.001 BTC per contract). Multiplying `size * price` as if size were BTC explodes notional.
 * Prefer explicit quote fields when present.
 * @param {Record<string, unknown>} row
 * @param {number} price
 */
function computeNotionalUsd(row, price) {
  const quoteKeys = [
    "quote_qty",
    "quoteQty",
    "quote_size",
    "quote_qty_usd",
    "usd",
    "size_usd",
    "notional",
    "notional_usd",
    "value_usd",
    "cash_qty",
    "quoteQtyUsd",
  ];
  for (const k of quoteKeys) {
    const v = row[k];
    if (v == null) continue;
    const u = parseFloat(String(v));
    if (Number.isFinite(u) && u > 0 && u < 1e12) return u;
  }

  const sz = parseFloat(String(row.size ?? row.qty ?? row.quantity ?? row.q ?? 0));
  if (!Number.isFinite(sz) || sz <= 0) return 0;

  const ex = String(row.exchange ?? row.venue ?? "").toLowerCase();

  /** MEXC BTC linear perps: `size` is contract count; 1 contract = 0.001 BTC (see BEA samples). */
  if (ex.includes("mexc") && ex.includes("perp")) {
    return sz * 0.001 * price;
  }

  const naiveBtc = sz * price;

  /** Other perps: if naive implies absurd USD, try standard 0.001 BTC/contract. */
  if ((ex.includes("perp") || ex.includes("_perp")) && naiveBtc > NAIVE_NOTIONAL_CAP_USD) {
    const alt = sz * 0.001 * price;
    if (alt > 0 && alt < naiveBtc && alt <= NAIVE_NOTIONAL_CAP_USD * 2) return alt;
  }

  /** Spot / mixed: size is base BTC (typical). */
  return naiveBtc;
}

/** @param {unknown} raw */
function normalizeTapeRows(raw) {
  if (raw == null) return [];
  if (Array.isArray(raw)) return raw;
  if (typeof raw !== "object") return [];
  const o = /** @type {Record<string, unknown>} */ (raw);
  const keys = ["trades", "prints", "data", "tape", "rows", "items", "events", "lines", "tape_trades"];
  for (const k of keys) {
    if (Array.isArray(o[k])) return o[k];
  }
  const d = o.data;
  if (d && typeof d === "object") {
    const dd = /** @type {Record<string, unknown>} */ (d);
    for (const k of keys) {
      if (Array.isArray(dd[k])) return dd[k];
    }
  }
  return [];
}

/** @param {Record<string, unknown>} row @param {number} nowMs */
function rowToTapePrint(row, nowMs) {
  const price = parseFloat(
    String(row.price ?? row.px ?? row.Price ?? row.p ?? 0),
  );
  if (!Number.isFinite(price) || price <= 0) return null;

  const sizeUsd = computeNotionalUsd(
    /** @type {Record<string, unknown>} */ (row),
    price,
  );
  if (!Number.isFinite(sizeUsd) || sizeUsd <= 0) return null;

  const sideRaw = row.side ?? row.Side ?? row.is_buy ?? row.aggressor;
  let isBuy = true;
  if (typeof sideRaw === "string") {
    const s = sideRaw.toUpperCase();
    if (s.includes("SELL") || s === "S" || s === "ASK" || s === "A") isBuy = false;
    else if (s.includes("BUY") || s === "B" || s === "BID") isBuy = true;
    else if (s === "-1") isBuy = false;
  } else if (typeof sideRaw === "boolean") {
    isBuy = sideRaw;
  } else if (typeof sideRaw === "number") {
    isBuy = sideRaw >= 0;
  }

  let tMs = parseInt(
    String(row.t ?? row.ts ?? row.ts_ms ?? row.time ?? row.T ?? row.timestamp_ms ?? 0),
    10,
  );
  if (tMs > 0 && tMs < 1e12) tMs *= 1000;

  let exchange = "";
  const ex = row.exchange ?? row.exchange_code ?? row.venue ?? row.v ?? row.src;
  if (Array.isArray(ex)) {
    exchange = ex
      .map((x) => String(x || "").trim())
      .filter(Boolean)
      .join("+");
  } else if (ex != null) {
    exchange = String(ex).trim();
  }
  if (!exchange) exchange = "—";

  const isMulti = Boolean(row.is_multi ?? row.multi ?? exchange.includes("+"));

  const age_seconds =
    tMs > 0 ? Math.max(0, (nowMs - tMs) / 1000) : parseFloat(String(row.age_seconds ?? 0)) || 0;

  const bps = parseFloat(String(row.bps ?? 0)) || 0;
  const source = String(row.source ?? row.src ?? "").trim();

  return {
    isBuy,
    price,
    size_usd: sizeUsd,
    exchange,
    isMulti,
    age_seconds,
    bps,
    t_ms: tMs > 0 ? tMs : undefined,
    source: source || undefined,
  };
}

function emojiForExchange(code) {
  const c = String(code || "").trim().toLowerCase();
  if (!c) return "•";
  if (c.includes("binance")) return "🍍";
  if (c.includes("bybit")) return "🟠";
  if (c.includes("okx")) return "✖️";
  if (c.includes("htx")) return "🦁";
  if (c.includes("hyperliquid")) return "💧";
  if (c.includes("bitfinex")) return "💠";
  if (c.includes("coinbase")) return "🔵";
  if (c.includes("kraken")) return "🐙";
  if (c.includes("bitstamp")) return "♦️";
  if (c.includes("bitmex")) return "⚫";
  if (c.includes("deribit")) return "🟣";
  if (c.includes("mexc")) return "🐝";
  if (c.includes("aster")) return "⭐";
  if (c.includes("btcc")) return "🧱";
  return "•";
}

function drawSingleExchange(ctx, code, cy) {
  const emoji = emojiForExchange(code);
  ctx.fillStyle = "#0e1827";
  ctx.beginPath();
  ctx.arc(12, cy, 8, 0, Math.PI * 2);
  ctx.fill();
  ctx.font = '400 11px "Apple Color Emoji","Segoe UI Emoji","Noto Color Emoji","JetBrains Mono",sans-serif';
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillStyle = "#ffffff";
  ctx.fillText(emoji, 12, cy + 0.5);
}

function drawExchangeIcon(ctx, exchange, cy) {
  const ex = String(exchange || "").trim();
  if (ex.includes("+")) {
    const parts = ex.split("+").map((s) => s.trim()).filter(Boolean);
    const n = parts.length;
    if (n >= 3) {
      drawSingleExchange(ctx, "mix3", cy);
      ctx.font = '400 11px "Apple Color Emoji","Segoe UI Emoji","Noto Color Emoji","JetBrains Mono",sans-serif';
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = "#ffffff";
      ctx.fillText("🔷", 12, cy + 0.5);
    } else if (n === 2) {
      drawSingleExchange(ctx, "mix2", cy);
      ctx.font = '400 11px "Apple Color Emoji","Segoe UI Emoji","Noto Color Emoji","JetBrains Mono",sans-serif';
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = "#ffffff";
      ctx.fillText("🔶", 12, cy + 0.5);
    } else {
      drawSingleExchange(ctx, parts[0] || "", cy);
    }
  } else {
    drawSingleExchange(ctx, ex, cy);
  }
  ctx.textAlign = "left";
  ctx.textBaseline = "middle";
}

/**
 * Normalize verbose venue names to compact 3-letter labels.
 * @param {string} exchange
 */
function shortExchangeLabel(exchange) {
  const ex = String(exchange || "").trim().toLowerCase();
  if (!ex) return "---";
  if (ex.includes("+")) return "MIX";
  if (ex.includes("binance")) return "BIN";
  if (ex.includes("bybit")) return "BYB";
  if (ex.includes("okx")) return "OKX";
  if (ex.includes("htx")) return "HTX";
  if (ex.includes("hyperliquid")) return "HYP";
  if (ex.includes("bitfinex")) return "BFX";
  if (ex.includes("coinbase")) return "CBS";
  if (ex.includes("kraken")) return "KRK";
  if (ex.includes("bitstamp")) return "BSP";
  if (ex.includes("bitmex")) return "BMX";
  if (ex.includes("deribit")) return "DRB";
  if (ex.includes("mexc")) return "MXC";
  if (ex.includes("aster")) return "AST";
  if (ex.includes("btcc")) return "BTC";
  return ex.slice(0, 3).toUpperCase();
}

/**
 * Visual tiers: high prints ($1M+) get saturated full-row blocks; size column scales up.
 * @param {number} usd
 */
function getTier(usd, minPrintUsd) {
  if (usd >= 2.5e6) {
    return {
      buyBg: "#5bb36d",
      sellBg: "#ff3e57",
      stripeBuy: "#d2ffe1",
      stripeSell: "#ffd1d8",
      priceFs: 12,
      sizeFs: 18,
      exchFs: 10,
      ageFs: 10,
      priceFw: "600",
      sizeFw: "700",
      priceColor: "#ffffff",
      sizeColor: "#ffffff",
      exchColor: "rgba(255,255,255,0.95)",
      ageColor: "rgba(255,255,255,0.82)",
      highPrint: true,
    };
  }
  if (usd >= 1.5e6) {
    return {
      buyBg: "#4ca865",
      sellBg: "#f2364c",
      stripeBuy: "#b9ffd3",
      stripeSell: "#ffbdc8",
      priceFs: 12,
      sizeFs: 17,
      exchFs: 10,
      ageFs: 9,
      priceFw: "600",
      sizeFw: "700",
      priceColor: "#ffffff",
      sizeColor: "#ffffff",
      exchColor: "rgba(255,255,255,0.92)",
      ageColor: "rgba(255,255,255,0.78)",
      highPrint: true,
    };
  }
  if (usd >= 1e6) {
    return {
      buyBg: "#3f9057",
      sellBg: "#de3147",
      stripeBuy: "#95f3b6",
      stripeSell: "#ff9eae",
      priceFs: 12,
      sizeFs: 16,
      exchFs: 10,
      ageFs: 9,
      priceFw: "600",
      sizeFw: "700",
      priceColor: "#f8fffb",
      sizeColor: "#ffffff",
      exchColor: "rgba(255,255,255,0.9)",
      ageColor: "rgba(255,255,255,0.76)",
      highPrint: true,
    };
  }
  if (usd >= 700_000) {
    return {
      buyBg: "#29583c",
      sellBg: "#9d2535",
      stripeBuy: "#68d492",
      stripeSell: "#f17285",
      priceFs: 11,
      sizeFs: 15,
      exchFs: 9,
      ageFs: 9,
      priceFw: "600",
      sizeFw: "700",
      priceColor: "#eefbf2",
      sizeColor: "#ffffff",
      exchColor: "rgba(226,244,234,0.92)",
      ageColor: "rgba(214,232,221,0.72)",
      highPrint: false,
    };
  }
  if (usd >= 450_000) {
    return {
      buyBg: "#234736",
      sellBg: "#782332",
      stripeBuy: "#49b97a",
      stripeSell: "#e35d72",
      priceFs: 11,
      sizeFs: 14,
      exchFs: 9,
      ageFs: 9,
      priceFw: "500",
      sizeFw: "700",
      priceColor: "#e7f4ec",
      sizeColor: "#f7fffb",
      exchColor: "#b4d1c1",
      ageColor: "#95ad9f",
      highPrint: false,
    };
  }
  if (usd >= 200_000) {
    return {
      buyBg: "#1f2f2b",
      sellBg: "#4f2830",
      stripeBuy: "#2f8d5f",
      stripeSell: "#ba495e",
      priceFs: 11,
      sizeFs: 13,
      exchFs: 9,
      ageFs: 9,
      priceFw: "500",
      sizeFw: "700",
      priceColor: "#e0ece7",
      sizeColor: "#eef6f2",
      exchColor: "#98afa4",
      ageColor: "#80938a",
      highPrint: false,
    };
  }
  if (usd >= minPrintUsd) {
    return {
      buyBg: "#202a2a",
      sellBg: "#3b242c",
      stripeBuy: "#226547",
      stripeSell: "#8e3747",
      priceFs: 11,
      sizeFs: 12,
      exchFs: 9,
      ageFs: 9,
      priceFw: "500",
      sizeFw: "600",
      priceColor: "#d8e3e7",
      sizeColor: "#e8f0f3",
      exchColor: "#8ea0a8",
      ageColor: "#6f8088",
      highPrint: false,
    };
  }
  return {
    buyBg: "#1f2427",
    sellBg: "#2b2225",
    stripeBuy: "#1d5a3f",
    stripeSell: "#7f2c3d",
    priceFs: 11,
    sizeFs: 11,
    exchFs: 9,
    ageFs: 9,
    priceFw: "500",
    sizeFw: "500",
    priceColor: "#dce4ec",
    sizeColor: "#eef3f8",
    exchColor: "#6d8aa0",
    ageColor: "#4d6275",
    highPrint: false,
  };
}

function formatSize(usd) {
  if (usd >= 1e6) return `$${(usd / 1e6).toFixed(2)}M`;
  if (usd >= 1e3) return `$${(usd / 1e3).toFixed(0)}K`;
  return `$${usd.toFixed(0)}`;
}

function formatAge(sec) {
  if (sec < 60) return `${Math.floor(sec)}s`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m`;
  return `${Math.floor(sec / 3600)}h`;
}

export default class TapeRenderer {
  /**
   * @param {HTMLCanvasElement} canvas
   * @param {{
   *   footerBuy?: HTMLElement,
   *   footerCvd?: HTMLElement,
   *   footerSell?: HTMLElement,
   *   dominanceTrackEl?: HTMLElement,
   *   dominanceBuyEl?: HTMLElement,
   *   dominanceSellEl?: HTMLElement,
   *   dominanceLabelEl?: HTMLElement,
   *   simulate?: boolean,
   *   onBeaStatus?: (ok: boolean, message: string) => void,
   *   minPrintUsd?: number,
  *   aggregateMode?: "raw" | "burst",
  *   burstWindowMs?: number,
   * }} [opts]
   */
  constructor(canvas, opts = {}) {
    this.canvas = canvas;
    const ctx = canvas.getContext("2d");
    if (!ctx) throw new Error("2d context required");
    this.ctx = ctx;
    this.maxPrints = 220;
    /** @type {TapePrint[]} */
    this.prints = [];
    this._mid = 68120;
    this._simulate = !!opts.simulate;
    this._nextSimAt = 0;
    this.footerBuy = opts.footerBuy || null;
    this.footerCvd = opts.footerCvd || null;
    this.footerSell = opts.footerSell || null;
    this._dominanceTrackEl = opts.dominanceTrackEl || null;
    this._dominanceBuyEl = opts.dominanceBuyEl || null;
    this._dominanceSellEl = opts.dominanceSellEl || null;
    this._dominanceLabelEl = opts.dominanceLabelEl || null;
    this._onBeaStatus = typeof opts.onBeaStatus === "function" ? opts.onBeaStatus : null;
    this._cvdWindow = [];
    this._buyVolUsd = 0;
    this._sellVolUsd = 0;
    this._minPrintUsd =
      Number.isFinite(opts.minPrintUsd) && opts.minPrintUsd > 0
        ? Number(opts.minPrintUsd)
        : DEFAULT_MIN_PRINT_USD;
    this._aggregateMode = opts.aggregateMode === "raw" ? "raw" : "burst";
    this._burstWindowMs =
      Number.isFinite(opts.burstWindowMs) && opts.burstWindowMs > 0
        ? Number(opts.burstWindowMs)
        : DEFAULT_BURST_WINDOW_MS;
    this._burstPriceBps =
      Number.isFinite(opts.burstPriceBps) && opts.burstPriceBps > 0
        ? Number(opts.burstPriceBps)
        : DEFAULT_BURST_PRICE_BPS;
    this._running = true;
    this._raf = 0;
    this._beaPollId = null;
    this._beaStream = null;
    this._emptyPolls = 0;
    this._lastNonEmptyMs = 0;
    this._lastPollMs = 0;
    this._pollInFlight = false;
    this._pollSeq = 0;
    this._appliedPollSeq = 0;
    /** @type {string} */
    this._beaEndpoint = "api/bea/tape";
    /** @type {string} */
    this._beaSymbol = "BTC-USD";
    /** @type {string} */
    this._beaMarket = "all";
    /** @type {string[]} */
    this._beaSources = [];
    this._lastBeaPayload = null;
    this._seenTradeKeys = new Set();
    this._seenKeyQueue = [];
    this._lastTapeTsMs = 0;
    this._canvasW = 0;
    this._canvasH = 0;
    this._loop = this._loop.bind(this);
    this.draw = this.draw.bind(this);
    const parent = canvas.parentElement;
    if (parent) {
      this._resizeObs = new ResizeObserver(() => this.draw());
      this._resizeObs.observe(parent);
    } else {
      this._resizeObs = null;
    }
    this._raf = requestAnimationFrame(this._loop);
  }

  /**
   * Merge micro-prints into short burst rows for a more tape-like flow.
   * Groups by exchange + side + time bucket + coarse price bucket.
   * @param {TapePrint[]} prints
   * @returns {TapePrint[]}
   */
  _aggregateBursts(prints) {
    if (!Array.isArray(prints) || prints.length <= 1) return prints;
    const winMs = Math.max(20, this._burstWindowMs | 0);
    /** @type {Map<string, {sumUsd:number, sumPxUsd:number, latestTs:number, firstTs:number, exchange:string, isBuy:boolean}>} */
    const groups = new Map();
    for (const p of prints) {
      const ts = Number.isFinite(p.t_ms) ? p.t_ms : 0;
      const tb = ts > 0 ? Math.floor(ts / winMs) : 0;
      const priceBand = Math.max(0.5, p.price * (this._burstPriceBps / 10000));
      const pb = Math.round(p.price / priceBand);
      const key = `${p.exchange}|${p.isBuy ? "B" : "S"}|${tb}|${pb}`;
      const g = groups.get(key);
      if (g) {
        g.sumUsd += p.size_usd;
        g.sumPxUsd += p.price * p.size_usd;
        if (ts > g.latestTs) g.latestTs = ts;
      } else {
        groups.set(key, {
          sumUsd: p.size_usd,
          sumPxUsd: p.price * p.size_usd,
          latestTs: ts,
          firstTs: ts,
          exchange: p.exchange,
          isBuy: p.isBuy,
        });
      }
    }
    /** @type {TapePrint[]} */
    const out = [];
    for (const g of groups.values()) {
      const px = g.sumUsd > 0 ? g.sumPxUsd / g.sumUsd : 0;
      out.push({
        isBuy: g.isBuy,
        price: px,
        size_usd: g.sumUsd,
        exchange: g.exchange,
        isMulti: false,
        age_seconds: 0,
        bps: 0,
        t_ms: g.latestTs > 0 ? g.latestTs : g.firstTs,
        born_ms: Date.now(),
      });
    }
    return out;
  }

  _emitSimPrint() {
    const r = Math.random();
    let sizeUsd;
    if (r < 0.65) {
      sizeUsd = this._minPrintUsd + Math.random() * (1e6 - this._minPrintUsd);
    } else if (r < 0.88) {
      sizeUsd = 1e6 + Math.random() * (2.5e6 - 1e6);
    } else if (r < 0.97) {
      sizeUsd = 2.5e6 + Math.random() * (5e6 - 2.5e6);
    } else {
      sizeUsd = 5e6 + Math.random() * 3e6;
    }
    const isBuy = Math.random() < 0.52;
    this._mid += (Math.random() - 0.48) * 12;
    const price = this._mid + (Math.random() - 0.5) * 8;
    let exchange;
    const exR = Math.random();
    if (exR < 0.45) exchange = "BIN";
    else if (exR < 0.6) exchange = "BYB";
    else if (exR < 0.72) exchange = "OKX";
    else if (exR < 0.84) exchange = "BIN+BYB";
    else if (exR < 0.94) exchange = "BIN+OKX+BYB";
    else exchange = "BINANCE";
    /** @type {TapePrint} */
    const p = {
      isBuy,
      price,
      size_usd: sizeUsd,
      exchange,
      isMulti: Math.random() < 0.12,
      age_seconds: Math.random() * 120,
      bps: (Math.random() - 0.5) * 4,
      born_ms: Date.now(),
    };
    this.addPrint(p);
  }

  /** @param {TapePrint} p */
  _tradeKey(p) {
    const side = p.isBuy ? "B" : "S";
    return `${p.t_ms || 0}|${p.source || ""}|${p.exchange}|${side}|${p.price}|${p.size_usd}`;
  }

  _pruneSeenKeys() {
    const keep = Math.max(this.maxPrints * 12, 2000);
    while (this._seenKeyQueue.length > keep) {
      const k = this._seenKeyQueue.shift();
      if (k) this._seenTradeKeys.delete(k);
    }
  }

  /** @param {TapePrint} print */
  addPrint(print) {
    if (print.size_usd < this._minPrintUsd) return;
    this.prints.unshift(print);
    if (this.prints.length > this.maxPrints) {
      this.prints.length = this.maxPrints;
    }
    const u = print.size_usd;
    if (print.isBuy) this._buyVolUsd += u;
    else this._sellVolUsd += u;
    const signed = print.isBuy ? u : -u;
    const now = Date.now();
    this._cvdWindow.push({ t: now, signed });
    let i = 0;
    const _MS_5M = 5 * 60 * 1000;
    while (i < this._cvdWindow.length && now - this._cvdWindow[i].t > _MS_5M) i++;
    if (i > 0) this._cvdWindow.splice(0, i);
    this._updateFooter();
  }

  _updateFooter() {
    const cvd5m = this._cvdWindow.reduce((a, x) => a + x.signed, 0);
    const fmtM = (v) => {
      const a = Math.abs(v);
      return `${v >= 0 ? "" : "-"}$${(a / 1e6).toFixed(1)}M`;
    };
    if (this.footerBuy) {
      this.footerBuy.textContent = `Buy ${fmtM(this._buyVolUsd)}`;
      this.footerBuy.style.color = "#00b868";
    }
    if (this.footerSell) {
      this.footerSell.textContent = `Sell ${fmtM(this._sellVolUsd)}`;
      this.footerSell.style.color = "#d03540";
    }
    if (this.footerCvd) {
      const sign = cvd5m >= 0 ? "+" : "";
      this.footerCvd.textContent = `5m CVD ${sign}${fmtM(cvd5m)}`;
      this.footerCvd.style.color = "#38b6f0";
    }
    const total = this._buyVolUsd + this._sellVolUsd;
    const buyShare = total > 0 ? this._buyVolUsd / total : 0.5;
    const sellShare = total > 0 ? this._sellVolUsd / total : 0.5;
    if (this._dominanceBuyEl) this._dominanceBuyEl.style.width = `${(buyShare * 100).toFixed(1)}%`;
    if (this._dominanceSellEl) this._dominanceSellEl.style.width = `${(sellShare * 100).toFixed(1)}%`;
    if (this._dominanceLabelEl) {
      const bp = Math.round(buyShare * 100);
      const sp = Math.round(sellShare * 100);
      this._dominanceLabelEl.textContent = `Buy ${bp}% | Sell ${sp}%`;
      this._dominanceLabelEl.style.color = bp >= sp ? "#8de3b4" : "#ff99aa";
    }
    const isNeutral = buyShare >= 0.48 && buyShare <= 0.52;
    if (this._dominanceTrackEl) this._dominanceTrackEl.classList.toggle("neutral", isNeutral);
    if (this._dominanceLabelEl) this._dominanceLabelEl.classList.toggle("neutral", isNeutral);
  }

  /** @param {number} now */
  _rebuildFooterFromPrints(now) {
    this._buyVolUsd = 0;
    this._sellVolUsd = 0;
    this._cvdWindow = [];
    const _MS_5M = 5 * 60 * 1000;
    for (const p of this.prints) {
      const u = p.size_usd;
      if (p.isBuy) this._buyVolUsd += u;
      else this._sellVolUsd += u;
      const t = p.t_ms ?? now;
      if (now - t <= _MS_5M) this._cvdWindow.push({ t, signed: p.isBuy ? u : -u });
    }
    this._updateFooter();
  }

  /**
   * Replace tape from BEA JSON (array or wrapped). Rebuilds footer from visible window.
   * @param {unknown} payload
   */
  applyBeaPayload(payload) {
    this._lastBeaPayload = payload;
    const now = Date.now();
    const rows = normalizeTapeRows(payload);
    /** @type {TapePrint[]} */
    let mapped = [];
    for (const row of rows) {
      if (!row || typeof row !== "object") continue;
      const p = rowToTapePrint(/** @type {Record<string, unknown>} */ (row), now);
      if (p) mapped.push(p);
    }
    if (this._aggregateMode === "burst") {
      mapped = this._aggregateBursts(mapped);
    }
    // Stable rolling behavior: append only new rows from snapshot polls.
    mapped.sort((a, b) => (a.t_ms || 0) - (b.t_ms || 0)); // oldest -> newest
    /** @type {TapePrint[]} */
    const fresh = [];
    for (const p of mapped) {
      const ts = p.t_ms || 0;
      if (ts > 0 && this._lastTapeTsMs > 0) {
        const src = String(p.source || "").toLowerCase();
        const allowedSkewMs = src === "bea" ? 15_000 : 3_000;
        if (ts < this._lastTapeTsMs - allowedSkewMs) continue;
      }
      const k = this._tradeKey(p);
      if (this._seenTradeKeys.has(k)) continue;
      this._seenTradeKeys.add(k);
      this._seenKeyQueue.push(k);
      if (p.size_usd < this._minPrintUsd) continue;
      p.born_ms = now;
      fresh.push(p);
      if (ts > this._lastTapeTsMs) this._lastTapeTsMs = ts;
    }
    this._pruneSeenKeys();

    if (fresh.length > 0) {
      fresh.sort((a, b) => (b.t_ms || 0) - (a.t_ms || 0));
      this.prints = fresh.concat(this.prints);
      if (this.prints.length > this.maxPrints) {
        this.prints.length = this.maxPrints;
      }
    }

    this._rebuildFooterFromPrints(now);
  }

  /**
   * Poll same-origin proxy that forwards to BEA /market/tape.
   * @param {{ endpoint?: string, symbol?: string, pollMs?: number }} [cfg]
   */
  startBeaFeed(cfg = {}) {
    this.stopBeaStream();
    this.stopBeaFeed();
    this.stopSimulation();
    this._simulate = false;
    if (cfg.endpoint) this._beaEndpoint = cfg.endpoint;
    if (cfg.symbol) this._beaSymbol = cfg.symbol;
    if (cfg.market) this._beaMarket = cfg.market;
    if (Array.isArray(cfg.sources)) this._beaSources = cfg.sources;
    const pollMs = cfg.pollMs ?? 1200;
    this._beaPollId = window.setInterval(() => {
      void this._pollBeaOnce();
    }, pollMs);
    void this._pollBeaOnce();
  }

  stopBeaFeed() {
    if (this._beaPollId != null) {
      clearInterval(this._beaPollId);
      this._beaPollId = null;
    }
  }

  /**
   * Stream tape using SSE for lower latency.
   * @param {{ endpoint?: string, symbol?: string, market?: string, sources?: string[], limit?: number, streamMs?: number }} [cfg]
   */
  startBeaStream(cfg = {}) {
    this.stopBeaFeed();
    this.stopSimulation();
    this._simulate = false;
    if (cfg.endpoint) this._beaEndpoint = cfg.endpoint;
    if (cfg.symbol) this._beaSymbol = cfg.symbol;
    if (cfg.market) this._beaMarket = cfg.market;
    if (Array.isArray(cfg.sources)) this._beaSources = cfg.sources;
    this.stopBeaStream();
    const params = new URLSearchParams();
    params.set("symbol", this._beaSymbol);
    params.set("market", this._beaMarket || "all");
    params.set("limit", String(cfg.limit || 500));
    params.set("stream_ms", String(cfg.streamMs || 120));
    if (this._beaSources.length) params.set("sources", this._beaSources.join(","));
    const url = apiUrl(`${this._beaEndpoint}?${params.toString()}`);
    try {
      this._beaStream = new EventSource(url);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (this._onBeaStatus) this._onBeaStatus(false, `Stream init error: ${msg.slice(0, 120)}`);
      return;
    }
    this._beaStream.onopen = () => {
      if (this._onBeaStatus) {
        this._onBeaStatus(true, `STREAM · ${this._beaSymbol} · connected`);
      }
    };
    this._beaStream.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        this._lastPollMs = Date.now();
        const rawRows = normalizeTapeRows(data);
        const rawCount = rawRows.length;
        this.applyBeaPayload(data);
        if (rawCount > 0) {
          this._emptyPolls = 0;
          this._lastNonEmptyMs = Date.now();
        } else {
          this._emptyPolls += 1;
        }
        if (this._onBeaStatus) {
          const shown = this.prints.length;
          const minLbl =
            this._minPrintUsd >= 1e6
              ? `$${(this._minPrintUsd / 1e6).toFixed(1)}M`
              : `$${Math.round(this._minPrintUsd / 1e3)}K`;
          this._onBeaStatus(
            true,
            `STREAM · ${this._beaSymbol} · raw ${rawCount} · shown ${shown} (>= ${minLbl}) · ${this._aggregateMode.toUpperCase()}`,
          );
        }
      } catch {}
    };
    this._beaStream.onerror = () => {
      const es = this._beaStream;
      if (!es) return;
      // Browsers fire error while reconnecting; only surface a hard failure when permanently closed.
      if (es.readyState === EventSource.CLOSED && this._onBeaStatus) {
        this._onBeaStatus(false, `Stream closed · ${this._beaSymbol}`);
      }
    };
  }

  stopBeaStream() {
    if (this._beaStream) {
      this._beaStream.close();
      this._beaStream = null;
    }
  }

  async _pollBeaOnce() {
    if (this._pollInFlight) return;
    this._pollInFlight = true;
    const reqSeq = ++this._pollSeq;
    const params = new URLSearchParams();
    params.set("symbol", this._beaSymbol);
    if (this._beaMarket) params.set("market", this._beaMarket);
    if (this._beaSources.length) params.set("sources", this._beaSources.join(","));
    const url = apiUrl(`${this._beaEndpoint}?${params.toString()}`);
    try {
      this._lastPollMs = Date.now();
      const res = await fetch(url);
      if (!res.ok) {
        const t = await res.text();
        throw new Error(t || res.statusText);
      }
      const data = await res.json();
      if (reqSeq < this._appliedPollSeq) return;
      this._appliedPollSeq = reqSeq;
      const rawRows = normalizeTapeRows(data);
      const rawCount = rawRows.length;
      this.applyBeaPayload(data);
      if (rawCount > 0) {
        this._emptyPolls = 0;
        this._lastNonEmptyMs = Date.now();
      } else {
        this._emptyPolls += 1;
      }
      if (this._onBeaStatus) {
        const shown = this.prints.length;
        const minLbl =
          this._minPrintUsd >= 1e6
            ? `$${(this._minPrintUsd / 1e6).toFixed(1)}M`
            : `$${Math.round(this._minPrintUsd / 1e3)}K`;
        if (rawCount > 0) {
          this._onBeaStatus(
            true,
            `BEA · ${this._beaSymbol} · raw ${rawCount} · shown ${shown} (>= ${minLbl}) · ${this._aggregateMode.toUpperCase()}`,
          );
        } else {
          const age = this._lastNonEmptyMs
            ? `${Math.max(0, Math.floor((Date.now() - this._lastNonEmptyMs) / 1000))}s ago`
            : "never";
          this._onBeaStatus(
            false,
            `BEA idle · ${this._beaSymbol} · 0 rows (${this._emptyPolls} polls) · last non-empty ${age}`,
          );
        }
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      this._emptyPolls += 1;
      if (this._onBeaStatus) {
        this._onBeaStatus(false, `Tape error: ${msg.slice(0, 120)}`);
      } else {
        console.warn("BEA tape poll:", e);
      }
    } finally {
      this._pollInFlight = false;
    }
  }

  _loop() {
    if (!this._running) return;
    if (this._simulate) {
      const now = performance.now();
      if (now >= this._nextSimAt) {
        this._emitSimPrint();
        this._nextSimAt = now + 600 + Math.random() * 400;
      }
    }
    this.draw();
    this._raf = requestAnimationFrame(this._loop);
  }

  draw() {
    const canvas = this.canvas;
    const parent = canvas.parentElement;
    if (!parent || !this.ctx) return;
    const W = parent.clientWidth | 0;
    const H = parent.clientHeight | 0;
    if (W < 2 || H < 2) return;
    if (W !== this._canvasW || H !== this._canvasH) {
      canvas.width = W;
      canvas.height = H;
      this._canvasW = W;
      this._canvasH = H;
    }
    const ctx = this.ctx;
    const ROW_H = Math.max(26, Math.floor(H / 21));
    const visible = Math.floor(H / ROW_H);
    const slice = this.prints.slice(0, visible);
    const nowMs = Date.now();
    ctx.fillStyle = BG;
    ctx.fillRect(0, 0, W, H);
    for (let i = 0; i < slice.length; i++) {
      const print = slice[i];
      const y = i * ROW_H;
      const tier = getTier(print.size_usd, this._minPrintUsd);
      const isBuy = print.isBuy;
      const bg = isBuy ? tier.buyBg : tier.sellBg;
      const stripe = isBuy ? tier.stripeBuy : tier.stripeSell;
      const rowH = ROW_H - 1;
      ctx.fillStyle = bg;
      ctx.fillRect(0, y, W, rowH);
      const bornMs = Number(print.born_ms || 0);
      const shimmerAge = bornMs > 0 ? nowMs - bornMs : Number.POSITIVE_INFINITY;
      if (shimmerAge >= 0 && shimmerAge <= NEW_PRINT_SHIMMER_MS) {
        const t = shimmerAge / NEW_PRINT_SHIMMER_MS;
        const alpha = 0.26 * (1 - t);
        ctx.fillStyle = `rgba(255,255,255,${alpha.toFixed(3)})`;
        ctx.fillRect(0, y, W, rowH);
        const sweepW = Math.max(34, Math.floor(W * 0.18));
        const sweepX = Math.floor((W + sweepW) * t) - sweepW;
        const sheen = ctx.createLinearGradient(sweepX, 0, sweepX + sweepW, 0);
        sheen.addColorStop(0, "rgba(255,255,255,0)");
        sheen.addColorStop(0.5, `rgba(255,255,255,${(alpha * 0.9).toFixed(3)})`);
        sheen.addColorStop(1, "rgba(255,255,255,0)");
        ctx.fillStyle = sheen;
        ctx.fillRect(sweepX, y, sweepW, rowH);
      }
      ctx.fillStyle = stripe;
      ctx.fillRect(0, y, tier.highPrint ? 4 : 3, rowH);
      const cy = y + ROW_H / 2;
      drawExchangeIcon(ctx, print.exchange, cy);
      ctx.font = `${tier.priceFw} ${tier.priceFs}px "JetBrains Mono", ui-monospace, monospace`;
      ctx.fillStyle = tier.priceColor;
      ctx.textAlign = "left";
      ctx.textBaseline = "middle";
      ctx.fillText(`$${print.price.toFixed(1)}`, 26, cy);
      const sizeX = W - 128;
      const exchX = W - 56;
      const ageX = W - 4;
      ctx.font = `${tier.sizeFw} ${tier.sizeFs}px "JetBrains Mono", ui-monospace, monospace`;
      ctx.fillStyle = tier.sizeColor;
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      ctx.fillText(formatSize(print.size_usd), sizeX, cy);
      ctx.font = `400 ${tier.exchFs}px "JetBrains Mono", ui-monospace, monospace`;
      ctx.fillStyle = tier.exchColor;
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      ctx.fillText(shortExchangeLabel(String(print.exchange || "")), exchX, cy);
      ctx.font = `400 ${tier.ageFs}px "JetBrains Mono", ui-monospace, monospace`;
      ctx.fillStyle = tier.ageColor;
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      const ageSec =
        print.t_ms != null ? Math.max(0, (nowMs - print.t_ms) / 1000) : print.age_seconds;
      ctx.fillText(formatAge(ageSec), ageX, cy);
      ctx.fillStyle = SEP;
      ctx.fillRect(0, y + ROW_H - 1, W, 1);
    }
  }

  startSimulation() {
    this.stopBeaFeed();
    if (this._simulate) return;
    this._simulate = true;
    this._nextSimAt = performance.now();
  }

  stopSimulation() {
    this._simulate = false;
  }

  /** @param {number} usd */
  setMinPrintUsd(usd) {
    if (!Number.isFinite(usd) || usd <= 0) return;
    this._minPrintUsd = usd;
    this.prints = this.prints.filter((p) => p.size_usd >= this._minPrintUsd);
    // Reset dedupe state so new threshold can repopulate on next payload.
    this._seenTradeKeys.clear();
    this._seenKeyQueue.length = 0;
    this._lastTapeTsMs = 0;
    if (this._lastBeaPayload != null) {
      this.prints = [];
      this.applyBeaPayload(this._lastBeaPayload);
    } else {
      this._rebuildFooterFromPrints(Date.now());
    }
  }

  getMinPrintUsd() {
    return this._minPrintUsd;
  }

  /** @param {"raw" | "burst"} mode */
  setAggregationMode(mode) {
    const m = mode === "raw" ? "raw" : "burst";
    this._aggregateMode = m;
    this._seenTradeKeys.clear();
    this._seenKeyQueue.length = 0;
    this._lastTapeTsMs = 0;
    if (this._lastBeaPayload != null) {
      this.prints = [];
      this.applyBeaPayload(this._lastBeaPayload);
    }
  }

  /** @param {number} ms */
  setBurstWindowMs(ms) {
    if (!Number.isFinite(ms) || ms < 20) return;
    this._burstWindowMs = ms;
    if (this._lastBeaPayload != null && this._aggregateMode === "burst") {
      this.prints = [];
      this._seenTradeKeys.clear();
      this._seenKeyQueue.length = 0;
      this._lastTapeTsMs = 0;
      this.applyBeaPayload(this._lastBeaPayload);
    }
  }

  /** @param {number} bps */
  setBurstPriceBps(bps) {
    if (!Number.isFinite(bps) || bps <= 0) return;
    this._burstPriceBps = bps;
    if (this._lastBeaPayload != null && this._aggregateMode === "burst") {
      this.prints = [];
      this._seenTradeKeys.clear();
      this._seenKeyQueue.length = 0;
      this._lastTapeTsMs = 0;
      this.applyBeaPayload(this._lastBeaPayload);
    }
  }

  destroy() {
    this._running = false;
    this._simulate = false;
    this.stopBeaFeed();
    this.stopBeaStream();
    cancelAnimationFrame(this._raf);
    if (this._resizeObs) {
      this._resizeObs.disconnect();
    }
  }
}
