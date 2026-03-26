/** Resolve API paths relative to `<base href>` so this app works standalone and under `/absorption/` in combined mode. */
function apiUrl(path) {
  const p = path.startsWith("/") ? path.slice(1) : path;
  try {
    return new URL(p, document.baseURI).href;
  } catch {
    return `/${p}`;
  }
}

const statusEl = document.getElementById("status");
const symbolEl = document.getElementById("symbol");
const tfEl = document.getElementById("tf");
const windowValEl = document.getElementById("window-val");
const analyticsToggleEl = document.getElementById("analytics-toggle");
const matrixEl = document.getElementById("matrix");
const domLadderEl = document.getElementById("dom-ladder");
const domMetaEl = document.getElementById("dom-meta");
const sceneNarrativeEl = document.getElementById("scene-narrative");
const canvas = document.getElementById("attractor-canvas");
const metaEl = document.getElementById("attractor-meta");
const minBurstEl = document.getElementById("min-burst");
const minBurstValEl = document.getElementById("min-burst-val");
const sourceBtns = Array.from(document.querySelectorAll(".pill"));
const footEvents = document.getElementById("f-events");
const footHigh = document.getElementById("f-high");
const footConfirmed = document.getElementById("f-confirmed");
const footScore = document.getElementById("f-score");
const footNotional = document.getElementById("f-notional");
const mFlow = document.getElementById("m-flow");
const mResp = document.getElementById("m-resp");
const mVenue = document.getElementById("m-venue");
const mFail = document.getElementById("m-fail");
const banner = document.getElementById("banner");
const bannerMsg = document.getElementById("banner-msg");
const sideChip = document.getElementById("side-chip");
const heatTrack = document.getElementById("heat-track");
const whalePointer = document.getElementById("whale-pointer");
const btnLong = document.getElementById("btn-long");
const btnShort = document.getElementById("btn-short");
const controlNote = document.getElementById("control-note");
const confirmBadge = document.getElementById("confirm-badge");
const failBadge = document.getElementById("fail-badge");
const levelsNote = document.getElementById("levels-note");
const railAggr = document.getElementById("rail-aggr");
const railSqueeze = document.getElementById("rail-squeeze");
const railConfirm = document.getElementById("rail-confirm");
const railConviction = document.getElementById("rail-conviction");
const railAggrV = document.getElementById("rail-aggr-v");
const railSqueezeV = document.getElementById("rail-squeeze-v");
const railConfirmV = document.getElementById("rail-confirm-v");
const railConvictionV = document.getElementById("rail-conviction-v");
const sceneRailAggr = document.getElementById("scene-rail-aggr");
const sceneRailHold = document.getElementById("scene-rail-hold");
const sceneRailFail = document.getElementById("scene-rail-fail");
const sceneRailRisk = document.getElementById("scene-rail-risk");
const sceneRailAggrV = document.getElementById("scene-rail-aggr-v");
const sceneRailHoldV = document.getElementById("scene-rail-hold-v");
const sceneRailFailV = document.getElementById("scene-rail-fail-v");
const sceneRailRiskV = document.getElementById("scene-rail-risk-v");

let sourceMode = "";
let es = null;
let selectedTf = "5m";
let selectedMinBurst = 1000000;
let sideMode = "neutral";
let lastRows = [];
let aggressionState = { side: "neutral", pct: 0 };
let matrixState = {
  flow_pressure: 0,
  response_efficiency: 0,
  venue_agreement: 0,
  failure_confirmation: 0,
  state: "IDLE",
};

class AttractorView {
  constructor(c, meta) {
    this.canvas = c;
    this.metaEl = meta;
    this.ctx = c.getContext("2d");
    this.points = [];
    this.maxPoints = 4200;
    this.x = 0.12;
    this.y = 0.0;
    this.z = 0.0;
    this.targetScale = 1.0;
    this.scale = 1.0;
    this.phase = 0;
    this.sideMode = "neutral";
    this.metrics = matrixState;
    this.sFlow = 0;
    this.sResp = 0;
    this.sFail = 0;
    this.sVenue = 0;
    this.aggression = aggressionState;
    this.squeezeX = 1.0;
    this.squeezeY = 1.0;
    this.hudState = "BUILDING";
    this.motionBlur = 0.08;
    this.glow = 0.75;
    this.noiseSeeds = Array.from({ length: 96 }, (_, i) => i * 0.31 + Math.random() * 8.0);
    this.loop = this.loop.bind(this);
    requestAnimationFrame(this.loop);
  }

  updateMetrics(metrics, mode, aggression) {
    this.metrics = metrics || this.metrics;
    this.sideMode = mode || "neutral";
    this.aggression = aggression || this.aggression;
  }

  _step() {
    const m = this.metrics || {};
    const tFlow = Number(m.flow_pressure || 0) / 100;
    const tResp = Number(m.response_efficiency || 0) / 100;
    const tFail = Number(m.failure_confirmation || 0) / 100;
    const tVenue = Number(m.venue_agreement || 0) / 100;
    this.sFlow += (tFlow - this.sFlow) * 0.06;
    this.sResp += (tResp - this.sResp) * 0.06;
    this.sFail += (tFail - this.sFail) * 0.08;
    this.sVenue += (tVenue - this.sVenue) * 0.05;
    const flow = this.sFlow;
    const resp = this.sResp;
    const fail = this.sFail;
    const venue = this.sVenue;
    const state = String(m.state || "IDLE");

    const a = 0.95 + flow * 0.28;
    const b = 0.72 - resp * 0.12;
    const c = 0.62 + venue * 0.2;
    const d = 3.52 + fail * 1.15;
    const e = 0.25 + flow * 0.18;
    const f = 0.09 + fail * 0.22;
    const dt = 0.0078 + flow * 0.0026;

    const x = this.x;
    const y = this.y;
    const z = this.z;
    const dx = ((z - b) * x - d * y) * dt;
    const dy = (d * x + (z - b) * y) * dt;
    const dz = (c + a * z - (z ** 3) / 3 - (x * x + y * y) * (1 + e * z) + f * z * (x ** 3)) * dt;
    this.x += dx;
    this.y += dy;
    this.z += dz;

    const pulse = state === "CONFIRMED_FAILURE" ? 1.32 : state === "ABSORBING" ? 1.15 : 1.0;
    this.targetScale = 0.88 + flow * 0.55 + resp * 0.18;
    this.scale += (this.targetScale - this.scale) * 0.12;
    this.phase += 0.012 + fail * 0.02;

    // Long-side absorption: horizontal squeeze (buyers absorbed, move stalls).
    // Short-side absorption: vertical squeeze.
    if (this.sideMode === "long") {
      this.squeezeX += ((0.78 + (1 - resp) * 0.08) - this.squeezeX) * 0.08;
      this.squeezeY += ((1.22 + flow * 0.14 * pulse) - this.squeezeY) * 0.08;
    } else if (this.sideMode === "short") {
      this.squeezeX += ((1.22 + flow * 0.14 * pulse) - this.squeezeX) * 0.08;
      this.squeezeY += ((0.78 + (1 - resp) * 0.08) - this.squeezeY) * 0.08;
    } else {
      this.squeezeX += (1.0 - this.squeezeX) * 0.08;
      this.squeezeY += (1.0 - this.squeezeY) * 0.08;
    }

    this.hudState = state === "CONFIRMED_FAILURE" ? "MOVE DEATH" : state === "ABSORBING" ? "ABSORBING" : "BUILDING";
    this.points.push({ x: this.x, y: this.y, z: this.z, t: performance.now(), state, side: this.sideMode });
    if (this.points.length > this.maxPoints) this.points.splice(0, this.points.length - this.maxPoints);
  }

  _color(p, ageMs) {
    const t = Math.max(0, Math.min(1, 1 - ageMs / 2600));
    const state = p.state || "";
    if (state === "CONFIRMED_FAILURE") return `rgba(250, 105, 96, ${(0.32 + 0.66 * t).toFixed(3)})`;
    if (state === "ABSORBING" || state === "HEAVY_ABSORBING") return `rgba(252, 210, 86, ${(0.26 + 0.62 * t).toFixed(3)})`;
    if (p.side === "long") return `rgba(238, 206, 92, ${(0.2 + 0.45 * t).toFixed(3)})`;
    if (p.side === "short") return `rgba(78, 225, 180, ${(0.2 + 0.45 * t).toFixed(3)})`;
    return `rgba(96, 163, 255, ${(0.16 + 0.4 * t).toFixed(3)})`;
  }

  _drawHud(ctx, W) {
    ctx.font = '600 11px "Inter", sans-serif';
    ctx.fillStyle = "rgba(228, 244, 255, 0.88)";
    ctx.fillText("Absorption Liquid Orb (2D)", 12, 20);
    ctx.font = '500 10px "JetBrains Mono", monospace';
    ctx.fillStyle = "rgba(115, 232, 182, 0.78)";
    ctx.fillText("Orb squeezes as absorption strength increases", 12, 35);

    const text = this.hudState;
    const tw = ctx.measureText(text).width + 18;
    const x = W - tw - 12;
    ctx.fillStyle = "rgba(36, 49, 72, 0.5)";
    ctx.strokeStyle = "rgba(138, 161, 196, 0.45)";
    ctx.lineWidth = 1;
    ctx.beginPath();
    if (typeof ctx.roundRect === "function") {
      ctx.roundRect(x, 10, tw, 26, 6);
    } else {
      ctx.rect(x, 10, tw, 26);
    }
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = "rgba(224, 238, 255, 0.92)";
    ctx.font = '700 10px "Inter", sans-serif';
    ctx.fillText(text, x + 9, 27);

    // Aggression + squeeze meters
    const m = this.metrics || {};
    const aggPct = Math.max(0, Math.min(100, Number(this.aggression?.pct || 0)));
    const squeezePct = Math.max(0, Math.min(100, Math.max(Number(m.flow_pressure || 0), Number(m.failure_confirmation || 0))));
    const meterX = 12;
    const meterY = 44;
    const meterW = 176;
    const meterH = 6;
    const side = String(this.aggression?.side || "neutral");
    const aggColor = side === "long" ? "#ff7b6e" : side === "short" ? "#79e784" : "#8db8ea";
    const sqColor = "#f2c84f";
    ctx.font = '500 9px "Inter", sans-serif';
    ctx.fillStyle = "rgba(171, 191, 211, 0.9)";
    ctx.fillText(`Aggression (${side.toUpperCase()})`, meterX, meterY);
    ctx.fillStyle = "rgba(28, 43, 63, 0.9)";
    ctx.fillRect(meterX, meterY + 4, meterW, meterH);
    ctx.fillStyle = aggColor;
    ctx.fillRect(meterX, meterY + 4, (aggPct / 100) * meterW, meterH);
    ctx.fillStyle = "rgba(220, 233, 245, 0.9)";
    ctx.fillText(`${aggPct.toFixed(0)}%`, meterX + meterW + 8, meterY + 10);

    const y2 = meterY + 20;
    ctx.fillStyle = "rgba(171, 191, 211, 0.9)";
    ctx.fillText("Squeeze / Orb Size", meterX, y2);
    ctx.fillStyle = "rgba(28, 43, 63, 0.9)";
    ctx.fillRect(meterX, y2 + 4, meterW, meterH);
    ctx.fillStyle = sqColor;
    ctx.fillRect(meterX, y2 + 4, (squeezePct / 100) * meterW, meterH);
    ctx.fillStyle = "rgba(220, 233, 245, 0.9)";
    ctx.fillText(`${squeezePct.toFixed(0)}%`, meterX + meterW + 8, y2 + 10);
  }

  draw() {
    const c = this.canvas;
    const p = c.parentElement;
    if (!p || !this.ctx) return;
    const W = p.clientWidth | 0;
    const H = p.clientHeight | 0;
    if (W < 2 || H < 2) return;
    if (c.width !== W || c.height !== H) {
      c.width = W;
      c.height = H;
    }
    const ctx = this.ctx;
    // Soft frame-to-frame fade for trailing motion blur.
    ctx.fillStyle = `rgba(6,12,24,${this.motionBlur.toFixed(3)})`;
    ctx.fillRect(0, 0, W, H);
    const now = performance.now();
    const cx = W * 0.5;
    const cy = H * 0.56;
    const m = this.metrics || {};
    const flow = Number(m.flow_pressure || 0) / 100;
    const resp = Number(m.response_efficiency || 0) / 100;
    const fail = Number(m.failure_confirmation || 0) / 100;
    const state = String(m.state || "IDLE");

    // subtle radial dark vignette
    const bg = ctx.createRadialGradient(cx, cy, Math.min(W, H) * 0.1, cx, cy, Math.min(W, H) * 0.75);
    bg.addColorStop(0, "rgba(14,28,52,0.35)");
    bg.addColorStop(1, "rgba(3,8,16,0)");
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, W, H);

    const baseR = Math.min(W, H) * (0.17 + 0.16 * this.scale);
    const amp = baseR * (0.12 + fail * 0.14);
    const rot = 0; // no spinning

    // Orb tint by side regime; squeeze magnitude by absorption pressure.
    let c0 = "rgba(85,150,245,0.28)";
    let c1 = "rgba(62,104,190,0.20)";
    if (this.sideMode === "long") {
      c0 = "rgba(244,104,96,0.30)";
      c1 = "rgba(152,58,56,0.22)";
    } else if (this.sideMode === "short") {
      c0 = "rgba(112,232,122,0.30)";
      c1 = "rgba(66,140,70,0.22)";
    }
    if (state === "CONFIRMED_FAILURE") {
      c0 = "rgba(255,92,86,0.36)";
      c1 = "rgba(170,58,54,0.28)";
    }
    const g = ctx.createRadialGradient(cx, cy, baseR * 0.15, cx, cy, baseR * 1.28);
    g.addColorStop(0, c0);
    g.addColorStop(1, c1);

    const squeeze = Math.max(flow, fail);
    const orbRx = baseR * (1.12 - squeeze * 0.52) * this.squeezeX;
    const orbRy = baseR * (1.12 - squeeze * 0.18) * this.squeezeY;

    // Outer orb shell
    ctx.beginPath();
    ctx.ellipse(cx, cy, orbRx, orbRy, 0, 0, Math.PI * 2);
    ctx.fillStyle = g;
    ctx.fill();

    // Clip to orb and draw liquid fill with animated wave level.
    ctx.save();
    ctx.beginPath();
    ctx.ellipse(cx, cy, orbRx, orbRy, 0, 0, Math.PI * 2);
    ctx.clip();
    const domSide = String(this.aggression?.side || "neutral");
    const domPct = Math.max(0, Math.min(1, Number(this.aggression?.pct || 0) / 100));
    // Side-aware fill: stronger dominant aggression drives deeper fill dynamics.
    const sideBias = domSide === "long" ? 0.08 : domSide === "short" ? -0.08 : 0;
    const baseSlosh = 0.03 + Math.sin(now * 0.0013) * 0.03;
    const level = cy + orbRy * (0.43 - flow * (0.52 + domPct * 0.34) + (resp - 0.5) * 0.13 + sideBias + baseSlosh);
    const waveAmp = Math.max(3, 7 + fail * 18 + flow * 10);
    const waveFreq = 0.014 + squeeze * 0.01;
    const wavePhase = now * (0.0025 + fail * 0.0045);
    const liquidTop = Math.max(cy - orbRy, Math.min(cy + orbRy, level));
    const liquidG = ctx.createLinearGradient(0, cy - orbRy, 0, cy + orbRy);
    if (domSide === "long") {
      liquidG.addColorStop(0, "rgba(255,120,102,0.78)");
      liquidG.addColorStop(1, "rgba(140,42,44,0.82)");
    } else if (domSide === "short") {
      liquidG.addColorStop(0, "rgba(132,245,142,0.78)");
      liquidG.addColorStop(1, "rgba(44,118,54,0.82)");
    } else {
      liquidG.addColorStop(0, "rgba(140,202,255,0.76)");
      liquidG.addColorStop(1, "rgba(52,86,160,0.82)");
    }
    ctx.beginPath();
    ctx.moveTo(cx - orbRx - 4, cy + orbRy + 6);
    for (let x = cx - orbRx - 4; x <= cx + orbRx + 4; x += 4) {
      const y = liquidTop + Math.sin((x - cx) * waveFreq + wavePhase) * waveAmp;
      ctx.lineTo(x, y);
    }
    ctx.lineTo(cx + orbRx + 4, cy + orbRy + 6);
    ctx.closePath();
    ctx.fillStyle = liquidG;
    ctx.fill();

    // Subtle highlights/bubbles
    const bubbles = 14;
    for (let i = 0; i < bubbles; i++) {
      const bx = cx + Math.sin(i * 1.9 + wavePhase * 1.2) * orbRx * 0.6;
      const by = liquidTop + (i / bubbles) * (cy + orbRy - liquidTop) + Math.cos(i * 2.1 + wavePhase) * 3;
      const br = 1.2 + ((i % 3) * 0.5);
      ctx.fillStyle = "rgba(235,246,255,0.18)";
      ctx.beginPath();
      ctx.arc(bx, by, br, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();

    // Bloom rim for squeezed orb
    ctx.save();
    ctx.globalCompositeOperation = "lighter";
    ctx.shadowBlur = (16 + 24 * squeeze) * this.glow;
    ctx.shadowColor = this.sideMode === "long"
      ? "rgba(255,86,76,0.78)"
      : this.sideMode === "short"
        ? "rgba(106,240,114,0.78)"
        : "rgba(128,186,255,0.72)";
    ctx.strokeStyle = this.sideMode === "long"
      ? "rgba(255,154,140,0.58)"
      : this.sideMode === "short"
        ? "rgba(180,255,186,0.58)"
        : "rgba(186,220,255,0.54)";
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.ellipse(cx, cy, orbRx, orbRy, 0, 0, Math.PI * 2);
    ctx.stroke();
    ctx.restore();

    this._drawHud(ctx, W);

    if (this.metaEl) {
      const m = this.metrics || {};
      const state = String(m.state || "IDLE");
      this.hudState = state === "CONFIRMED_FAILURE" ? "MOVE DEATH" : state === "ABSORBING" ? "ABSORBING" : "BUILDING";
      this.metaEl.textContent =
        `state: ${state}\n` +
        `flow: ${Math.round(Number(m.flow_pressure || 0))}  eff: ${Math.round(Number(m.response_efficiency || 0))}\n` +
        `venue: ${Math.round(Number(m.venue_agreement || 0))}  fail: ${Math.round(Number(m.failure_confirmation || 0))}\n` +
        `mode: ${this.sideMode.toUpperCase()}  orb squeeze: ${(squeeze * 100).toFixed(0)}%`;
    }
  }

  loop() {
    for (let i = 0; i < 4; i++) this._step();
    this.draw();
    requestAnimationFrame(this.loop);
  }
}

const attractor = new AttractorView(canvas, metaEl);

const fmtUsd = (v) => {
  const n = Number(v) || 0;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e3) return `$${Math.round(n / 1e3)}K`;
  return `$${n.toFixed(0)}`;
};
function updateBadgesFromEvents(rows) {
  if (!rows.length) {
    if (confirmBadge) confirmBadge.textContent = "Confirmation: n/a";
    if (failBadge) failBadge.textContent = "Failure: n/a";
    if (levelsNote) levelsNote.textContent = "Side: n/a";
    return;
  }
  const top = rows[0];
  if (top && levelsNote) {
    const c = Number(top.confirmation_level || 0);
    const f = Number(top.failure_level || 0);
    const side = String(top.dominant_side || "").toUpperCase();
    if (confirmBadge) confirmBadge.textContent = `Confirmation: $${c.toFixed(1)}`;
    if (failBadge) failBadge.textContent = `Failure: $${f.toFixed(1)}`;
    levelsNote.textContent = `Side: ${side || "N/A"}`;
  }
}

function renderLadder(lad) {
  if (!domLadderEl) return;
  domLadderEl.innerHTML = "";
  if (!lad || !Array.isArray(lad.rows)) {
    if (domMetaEl) domMetaEl.textContent = "No ladder data.";
    return;
  }
  const active = lad.rows
    .map((r) => {
      const buy = Number(r.buy_usd || 0);
      const sell = Number(r.sell_usd || 0);
      const total = buy + sell;
      const absorbUsd = Math.min(buy, sell) * 2;
      const pressure = absorbUsd * 0.7 + total * 0.3;
      return { ...r, _buy: buy, _sell: sell, _total: total, _absorb: absorbUsd, _pressure: pressure };
    })
    .filter((r) => r._total > 25_000)
    .sort((a, b) => b._pressure - a._pressure)
    .slice(0, 14);
  if (!active.length) {
    if (domMetaEl) domMetaEl.textContent = "No active absorption bands in current lookback.";
    return;
  }
  const maxUsd = Math.max(1, ...active.map((r) => Math.max(r._buy, r._sell)));
  const anchor = Number(lad.anchor_price || 0);
  const mk = lad.markers || {};
  const cLv = Number(mk.confirmation_level || 0);
  const fLv = Number(mk.failure_level || 0);
  const zLo = Number(mk.zone_low || 0);
  const zHi = Number(mk.zone_high || 0);
  const bw = Number(lad.band_width || 0);
  if (domMetaEl) {
    const lb = Math.round((lad.lookback_ms || 0) / 1000);
    const top = active[0];
    const dir = top._buy >= top._sell ? "BUY pressure" : "SELL pressure";
    domMetaEl.textContent =
      `Active absorption scope · top band $${Number(top.price || 0).toFixed(1)} (${dir}) · lookback ${lb}s · scale ${fmtUsd(maxUsd)}\n` +
      "Rows are ranked by pressure score. Highlight: anchor band, confirmation/failure markers, and latest zone band.";
  }
  const near = (px, lv) => lv > 0 && Math.abs(px - lv) <= Math.max(bw * 1.2, 1.0);

  for (const r of active) {
    const px = Number(r.price || 0);
    const sell = Number(r._sell || 0);
    const buy = Number(r._buy || 0);
    const hits = Number(r.hits || 0);
    const side = buy >= sell ? "BUY" : "SELL";
    const sideColor = buy >= sell ? "#86e39d" : "#f19a9a";
    const sp = Math.min(100, (sell / maxUsd) * 100);
    const bp = Math.min(100, (buy / maxUsd) * 100);
    const row = document.createElement("div");
    let cls = "dom-row";
    if (anchor > 0 && Math.abs(px - anchor) <= Math.max(bw * 0.6, 0.5)) cls += " anchor";
    if (near(px, cLv)) cls += " marker-confirm";
    if (near(px, fLv)) cls += " marker-fail";
    if (zLo > 0 && zHi > 0 && px >= zLo - 1e-6 && px <= zHi + 1e-6) cls += " marker-zone";
    row.className = cls;
    row.innerHTML = `
      <span class="dom-price">$${px.toFixed(1)}</span>
      <div class="dom-bars">
        <div class="dom-bar sell" title="Sell aggressor $${sell.toFixed(0)}"><i style="width:${sp.toFixed(1)}%"></i></div>
        <div class="dom-bar buy" title="Buy aggressor $${buy.toFixed(0)}"><i style="width:${bp.toFixed(1)}%"></i></div>
      </div>
      <span class="dom-mini" title="Sell aggressor">${fmtUsd(sell)}</span>
      <span class="dom-mini" title="Buy aggressor">${fmtUsd(buy)}</span>
      <span class="dom-mini" title="Dominant side / hits" style="color:${sideColor}">${side}/${hits}</span>
    `;
    domLadderEl.appendChild(row);
  }
}

async function refreshStats() {
  const symbol = (symbolEl.value || "BTC-USD").trim() || "BTC-USD";
  const q = new URLSearchParams({
    symbol,
    tf: selectedTf,
    min_burst_usd: String(selectedMinBurst),
  });
  const r = await fetch(apiUrl(`api/absorption/stats?${q.toString()}`));
  if (!r.ok) return;
  const d = await r.json();
  const s = d.stats || {};
  footEvents.textContent = `${selectedTf} Events ${s.events_window || 0}`;
  footHigh.textContent = `High ${s.high_conf_window || 0}`;
  footConfirmed.textContent = `Confirmed ${s.confirmed_window || 0}`;
  footScore.textContent = `Avg ${Number(s.avg_score_window || 0).toFixed(1)}`;
  footNotional.textContent = `Notional ${fmtUsd(s.notional_window || 0)}`;
}

function tone(el, v) {
  const n = Number(v) || 0;
  if (n >= 75) el.style.color = "#f0df9d";
  else if (n >= 55) el.style.color = "#9bd9ff";
  else el.style.color = "#a7b6c6";
}

function renderHeat(zones) {
  heatTrack.innerHTML = "";
  const rows = Array.isArray(zones) ? zones.slice(0, 14) : [];
  if (!rows.length) {
    for (let i = 0; i < 14; i++) {
      const b = document.createElement("div");
      b.className = "heat-bar";
      heatTrack.appendChild(b);
    }
    return;
  }
  for (const z of rows) {
    const b = document.createElement("div");
    const s = Number(z.score || 0);
    b.className = "heat-bar active";
    // Requested red/yellow/green heat scale:
    // green = low absorption, yellow = medium, red = high.
    if (s >= 85) b.style.background = "#e24c4b";
    else if (s >= 70) b.style.background = "#f2c84f";
    else b.style.background = "#4caf68";
    if (String(z.state || "") === "CONFIRMED_FAILURE") {
      b.style.boxShadow = "0 0 0 1px rgba(232,197,255,0.65), 0 0 10px rgba(232,197,255,0.28)";
    }
    b.title = `$${Number(z.zone_price || 0).toFixed(1)} · ${Math.round(s)} · ${z.state || ""}`;
    heatTrack.appendChild(b);
  }
}

function updateSideChip(zones) {
  const rows = Array.isArray(zones) ? zones.slice(0, 16) : [];
  if (!rows.length) {
    sideChip.className = "side-chip";
    sideChip.textContent = "SIDE: NEUTRAL";
    sideMode = "neutral";
    aggressionState = { side: "neutral", pct: 0 };
    updateControlPane("neutral", 0);
    return;
  }
  let buyW = 0;
  let sellW = 0;
  for (const z of rows) {
    const w = Number(z.score || 0);
    if (String(z.dominant_side || "").toLowerCase() === "buy") buyW += w;
    else if (String(z.dominant_side || "").toLowerCase() === "sell") sellW += w;
  }
  if (buyW === sellW) {
    sideChip.className = "side-chip";
    sideChip.textContent = "SIDE: NEUTRAL";
    sideMode = "neutral";
    aggressionState = { side: "neutral", pct: 50 };
    updateControlPane("neutral", 50);
    return;
  }
  const total = Math.max(1, buyW + sellW);
  if (buyW > sellW) {
    // Buy-side aggression getting absorbed.
    sideChip.className = "side-chip long";
    sideChip.textContent = "LONG-SIDE ABSORPTION";
    sideMode = "long";
    aggressionState = { side: "long", pct: (buyW / total) * 100 };
    updateControlPane("long", aggressionState.pct);
    return;
  }
  sideChip.className = "side-chip short";
  sideChip.textContent = "SHORT-SIDE ABSORPTION";
  sideMode = "short";
  aggressionState = { side: "short", pct: (sellW / total) * 100 };
  updateControlPane("short", aggressionState.pct);
}

function updateControlPane(side, pct) {
  if (!btnLong || !btnShort || !controlNote) return;
  btnLong.classList.toggle("active", side === "long");
  btnShort.classList.toggle("active", side === "short");
  if (whalePointer) {
    if (side === "long") whalePointer.style.left = "25%";
    else if (side === "short") whalePointer.style.left = "75%";
    else whalePointer.style.left = "50%";
  }

  if (side === "long") {
    controlNote.textContent = `Buyers are getting absorbed: upside push is being capped (${Math.round(pct)}% long-side pressure).`;
  } else if (side === "short") {
    controlNote.textContent = `Sellers are getting absorbed: downside push is being held (${Math.round(pct)}% short-side pressure).`;
  } else {
    controlNote.textContent = "Neutral flow. No side is controlling absorption.";
  }
}

function setRail(el, valEl, v) {
  if (!el || !valEl) return;
  const n = Math.max(0, Math.min(100, Number(v || 0)));
  el.style.width = `${n.toFixed(1)}%`;
  valEl.textContent = `${Math.round(n)}`;
}

async function refreshLadder() {
  const symbol = (symbolEl.value || "BTC-USD").trim() || "BTC-USD";
  const q = new URLSearchParams({ symbol, tf: selectedTf });
  if (sourceMode) q.set("sources", sourceMode);
  const r = await fetch(apiUrl(`api/absorption/ladder?${q.toString()}`));
  if (!r.ok) return;
  const d = await r.json();
  renderLadder(d.ladder || {});
}

async function refreshMatrix() {
  const symbol = (symbolEl.value || "BTC-USD").trim() || "BTC-USD";
  const q = new URLSearchParams({
    symbol,
    tf: selectedTf,
    min_burst_usd: String(selectedMinBurst),
  });
  const r = await fetch(apiUrl(`api/absorption/matrix?${q.toString()}`));
  if (!r.ok) return;
  const d = await r.json();
  const m = d.matrix || {};
  const flow = Number(m.flow_pressure || 0);
  const resp = Number(m.response_efficiency || 0);
  const venue = Number(m.venue_agreement || 0);
  const fail = Number(m.failure_confirmation || 0);
  mFlow.textContent = `${Math.round(flow)}`;
  mResp.textContent = `${Math.round(resp)}`;
  mVenue.textContent = `${Math.round(venue)}`;
  mFail.textContent = `${Math.round(fail)}`;
  tone(mFlow, flow);
  tone(mResp, resp);
  tone(mVenue, venue);
  tone(mFail, fail);
  const state = String(m.state || "BUILDING");
  banner.className = `banner state-${state}`;
  const msg = String(m.message || "Absorption state unavailable.");
  if (bannerMsg) bannerMsg.textContent = msg;
  if (sceneNarrativeEl) sceneNarrativeEl.textContent = msg;
  updateSideChip(m.top_zones || []);
  renderHeat(m.top_zones || []);
  matrixState = {
    flow_pressure: flow,
    response_efficiency: resp,
    venue_agreement: venue,
    failure_confirmation: fail,
    state,
  };
  const aggr = Math.max(0, Math.min(100, Number(aggressionState.pct || 0)));
  const squeeze = Math.max(flow, fail);
  const conviction = Math.max(0, Math.min(100, flow * 0.42 + resp * 0.28 + venue * 0.3));
  const continuationRisk = Math.max(
    0,
    Math.min(100, flow * 0.45 + fail * 0.35 + (100 - resp) * 0.2),
  );
  setRail(railAggr, railAggrV, aggr);
  setRail(railSqueeze, railSqueezeV, squeeze);
  setRail(railConfirm, railConfirmV, fail);
  setRail(railConviction, railConvictionV, conviction);
  setRail(sceneRailAggr, sceneRailAggrV, aggr);
  setRail(sceneRailHold, sceneRailHoldV, resp);
  setRail(sceneRailFail, sceneRailFailV, fail);
  setRail(sceneRailRisk, sceneRailRiskV, continuationRisk);
  attractor.updateMetrics(matrixState, sideMode, aggressionState);
  await refreshLadder();
}

function start() {
  const symbol = (symbolEl.value || "BTC-USD").trim() || "BTC-USD";
  if (es) es.close();
  const q = new URLSearchParams({
    symbol,
    tf: selectedTf,
    limit: "220",
    stream_ms: "120",
    min_burst_usd: String(selectedMinBurst),
  });
  if (sourceMode) q.set("sources", sourceMode);
  es = new EventSource(apiUrl(`api/absorption/stream?${q.toString()}`));
  es.onmessage = (ev) => {
    try {
      const d = JSON.parse(ev.data);
      const rows = Array.isArray(d.events) ? d.events : [];
      lastRows = rows;
      updateBadgesFromEvents(rows);
      statusEl.textContent = `STREAM · ${symbol} · ${selectedTf} · events ${rows.length}${sourceMode ? ` · ${sourceMode.toUpperCase()}` : ""}`;
    } catch {}
  };
  es.onerror = () => {
    if (es.readyState === EventSource.CLOSED) {
      statusEl.textContent = `Stream closed · ${symbol}`;
      return;
    }
    const t = statusEl.textContent || "";
    if (!t.includes("STREAM")) {
      statusEl.textContent = `Stream reconnecting… ${symbol}`;
    }
  };
}

symbolEl.addEventListener("change", start);
if (tfEl) {
  tfEl.addEventListener("change", () => {
    selectedTf = String(tfEl.value || "5m");
    if (windowValEl) windowValEl.textContent = selectedTf;
    start();
    void refreshStats();
    void refreshMatrix();
  });
}
if (analyticsToggleEl && matrixEl) {
  analyticsToggleEl.addEventListener("click", () => {
    matrixEl.classList.toggle("hidden");
    const hidden = matrixEl.classList.contains("hidden");
    analyticsToggleEl.textContent = hidden ? "Show Analytics" : "Hide Analytics";
  });
}
for (const b of sourceBtns) {
  b.addEventListener("click", () => {
    sourceMode = b.dataset.sources || "";
    for (const x of sourceBtns) x.classList.toggle("active", x === b);
    start();
  });
}
minBurstEl.addEventListener("input", () => {
  selectedMinBurst = Number(minBurstEl.value || 0) || 1000000;
  minBurstValEl.textContent = fmtUsd(selectedMinBurst);
  start();
  void refreshStats();
  void refreshMatrix();
});

setInterval(() => {
  void refreshStats();
  void refreshMatrix();
}, 1000);

if (windowValEl) windowValEl.textContent = selectedTf;
selectedMinBurst = Number(minBurstEl?.value || 1000000) || 1000000;
if (minBurstValEl) minBurstValEl.textContent = fmtUsd(selectedMinBurst);
start();
void refreshStats();
void refreshMatrix();
