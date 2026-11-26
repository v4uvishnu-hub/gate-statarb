const express = require("express");
const ccxt = require("ccxt");
const { createClient } = require("redis");

// ==========================================
// CONFIGURATION (PUBLIC API / NO KEY NEEDED)
// ==========================================
const API_KEY = (process.env.GATE_API_KEY || "").trim();
const API_SECRET = (process.env.GATE_API_SECRET || "").trim();
const USE_TESTNET = String(process.env.USE_TESTNET || "false").toLowerCase() === "true";

const HAS_PRIVATE_KEYS = Boolean(API_KEY && API_SECRET);
const PAPER_MODE =
  String(process.env.PAPER_MODE || (!HAS_PRIVATE_KEYS ? "true" : "false")).toLowerCase() === "true" ||
  !HAS_PRIVATE_KEYS;

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379/0";
const REDIS_STATE_KEY = "atr_trend_bot:state";
const REDIS_STATS_KEY = "atr_trend_bot:stats";
const REDIS_HISTORY_KEY = "atr_trend_bot:history";
const MAX_TRADE_HISTORY = 100;

const TELEGRAM_BOT_TOKEN = (process.env.TELEGRAM_BOT_TOKEN || "").trim();
const TELEGRAM_CHAT_ID = (process.env.TELEGRAM_CHAT_ID || "").trim();

const WEB_UI_HOST = process.env.WEB_UI_HOST || "0.0.0.0";
const WEB_UI_PORT = Number(process.env.PORT || process.env.WEB_UI_PORT || 8080);
const WEB_UI_TOKEN = (process.env.WEB_UI_TOKEN || "").trim();

const TOP_N_COINS = Number(process.env.TOP_N_COINS || 100);
const MAX_PAIRS_PER_CYCLE = Number(process.env.MAX_PAIRS_PER_CYCLE || 25);
const UNIVERSE_REFRESH_SEC = Number(process.env.UNIVERSE_REFRESH_SEC || 1800);

const rawPinned =
  process.env.PINNED_SYMBOLS ||
  process.env.TRADE_SYMBOLS ||
  process.env.TRADE_SYMBOL ||
  "BTC/USDT:USDT,ETH/USDT:USDT";
const PINNED_SYMBOLS = rawPinned.split(",").map((s) => s.trim()).filter(Boolean);

const MAX_OPEN_POSITIONS = Number(process.env.MAX_OPEN_POSITIONS || 2);
const TOTAL_CAPITAL_USDT = Number(process.env.TOTAL_CAPITAL_USDT || 500.0);

const TREND_TF = "1h";
const ENTRY_TF = "15m";
const EMA_FAST_LEN = 20;
const EMA_SLOW_LEN = 50;
const ATR_LEN = 14;

const RISK_PER_TRADE_USDT = Number(process.env.RISK_PER_TRADE_USDT || 25.0);
const STOP_ATR_MULT = Number(process.env.STOP_ATR_MULT || 1.5);
const TRAIL_ATR_MULT = Number(process.env.TRAIL_ATR_MULT || 2.0);
const TP_ATR_MULT = Number(process.env.TP_ATR_MULT || 3.0);
const PULLBACK_ATR_BAND = Number(process.env.PULLBACK_ATR_BAND || 0.75);
const EMA20_GUARD_PCT = Number(process.env.EMA20_GUARD_PCT || 0.003);
const MAX_HOLD_HOURS = Number(process.env.MAX_HOLD_HOURS || 24.0);
const POLL_INTERVAL_SEC = 60;

// ==========================================
// CLIENTS & STATE
// ==========================================
const redisClient = createClient({ url: REDIS_URL });
redisClient.on("error", (err) => console.error("[REDIS ERROR]", err.message));

const exchangeCfg = {
  enableRateLimit: true,
  options: { defaultType: "swap" },
};
if (HAS_PRIVATE_KEYS) {
  exchangeCfg.apiKey = API_KEY;
  exchangeCfg.secret = API_SECRET;
}
const exchange = new ccxt.gate(exchangeCfg);
if (USE_TESTNET) {
  exchange.setSandboxMode(true);
}

let cachedMarkets = {};
let cachedTopSymbols = [...PINNED_SYMBOLS];
let lastUniverseRefreshMs = 0;
let scanBusy = false;
let warnedMissingChatId = false;

function modeLabel() {
  if (PAPER_MODE) return "PUBLIC-PAPER";
  return USE_TESTNET ? "TESTNET" : "LIVE";
}

function nowIstString() {
  return new Date().toLocaleString("en-GB", {
    timeZone: "Asia/Kolkata",
    hour12: false,
  }) + " IST";
}

function fmtPx(v) {
  const n = Number(v);
  if (Number.isNaN(n)) return "--";
  return Math.abs(n) < 50 ? n.toFixed(4) : n.toFixed(2);
}

async function loadState() {
  const raw = await redisClient.get(REDIS_STATE_KEY);
  if (raw) {
    try {
      const data = JSON.parse(raw);
      const trades = Array.isArray(data.active_trades)
        ? data.active_trades
        : data.active_trade && typeof data.active_trade === "object"
        ? [data.active_trade]
        : [];
      return {
        active_trades: trades,
        paused: Boolean(data.paused || false),
        last_scan: data.last_scan || null,
        scan_cursor: Number(data.scan_cursor || 0),
        max_open_positions: Number(data.max_open_positions ?? MAX_OPEN_POSITIONS),
        total_capital_usdt: Number(data.total_capital_usdt ?? TOTAL_CAPITAL_USDT),
        pullback_atr_band: Number(data.pullback_atr_band ?? PULLBACK_ATR_BAND),
        risk_per_trade_usdt: Number(data.risk_per_trade_usdt ?? RISK_PER_TRADE_USDT),
      };
    } catch (_) {
      console.warn("Corrupted Redis state JSON; using defaults.");
    }
  }
  return {
    active_trades: [],
    paused: false,
    last_scan: null,
    scan_cursor: 0,
    max_open_positions: MAX_OPEN_POSITIONS,
    total_capital_usdt: TOTAL_CAPITAL_USDT,
    pullback_atr_band: PULLBACK_ATR_BAND,
    risk_per_trade_usdt: RISK_PER_TRADE_USDT,
  };
}

async function saveState(state) {
  const normalized = {
    ...state,
    active_trades: Array.isArray(state.active_trades) ? state.active_trades : [],
  };
  await redisClient.set(REDIS_STATE_KEY, JSON.stringify(normalized));
}

async function loadStats() {
  const raw = await redisClient.get(REDIS_STATS_KEY);
  if (raw) {
    try {
      return JSON.parse(raw);
    } catch (_) {}
  }
  return { total_trades: 0, wins: 0, losses: 0, realized_pnl: 0.0 };
}

async function resetStatsAndHistory() {
  const empty = { total_trades: 0, wins: 0, losses: 0, realized_pnl: 0.0 };
  await redisClient.set(REDIS_STATS_KEY, JSON.stringify(empty));
  await redisClient.del(REDIS_HISTORY_KEY);
  return empty;
}

async function recordClosedTrade(trade, exitPrice, realizedPnl, exitReason) {
  const stats = await loadStats();
  stats.total_trades = Number(stats.total_trades || 0) + 1;
  if (realizedPnl >= 0) stats.wins = Number(stats.wins || 0) + 1;
  else stats.losses = Number(stats.losses || 0) + 1;
  stats.realized_pnl = Number((Number(stats.realized_pnl || 0) + realizedPnl).toFixed(4));
  await redisClient.set(REDIS_STATS_KEY, JSON.stringify(stats));

  const row = {
    symbol: trade.symbol,
    side: trade.side,
    amount: trade.amount,
    entry_price: Number(Number(trade.entry_price || 0).toFixed(4)),
    exit_price: Number(Number(exitPrice || 0).toFixed(4)),
    realized_pnl: Number(realizedPnl.toFixed(4)),
    exit_reason: exitReason,
    opened_at: trade.opened_at,
    closed_at: new Date().toISOString(),
  };
  await redisClient.lPush(REDIS_HISTORY_KEY, JSON.stringify(row));
  await redisClient.lTrim(REDIS_HISTORY_KEY, 0, MAX_TRADE_HISTORY - 1);
}

async function loadHistory(limit = 25) {
  const items = await redisClient.lRange(REDIS_HISTORY_KEY, 0, Math.max(0, limit - 1));
  return items.map((x) => {
    try {
      return JSON.parse(x);
    } catch (_) {
      return null;
    }
  }).filter(Boolean);
}

function calcCapitalSummary(trades, totalCapitalUsdt) {
  let used = 0;
  for (const tr of trades || []) {
    const amt = Number(tr.amount || 0);
    const csize = Number(tr.contract_size || 1);
    const entry = Number(tr.entry_price || 0);
    if (!Number.isNaN(amt) && !Number.isNaN(csize) && !Number.isNaN(entry)) {
      used += Math.abs(amt * csize * entry);
    }
  }
  const total = Math.max(0, Number(totalCapitalUsdt || 0));
  const free = Math.max(0, total - used);
  const usedPct = total > 0 ? Number(Math.min(100, (used / total) * 100).toFixed(1)) : 0;
  return {
    capital_used_usdt: Number(used.toFixed(2)),
    total_capital_usdt: Number(total.toFixed(2)),
    free_capital_usdt: Number(free.toFixed(2)),
    capital_used_pct: usedPct,
  };
}

async function sendTelegram(htmlText) {
  if (!TELEGRAM_BOT_TOKEN) return;
  if (!TELEGRAM_CHAT_ID) {
    if (!warnedMissingChatId) {
      console.warn("[TELEGRAM] TELEGRAM_CHAT_ID is empty; skipping outbound alerts.");
      warnedMissingChatId = true;
    }
    return;
  }
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: htmlText,
        parse_mode: "HTML",
        disable_web_page_preview: true,
      }),
    });
  } catch (err) {
    console.error("[TELEGRAM SEND ERROR]", err.message);
  }
}

function calcEma(values, period) {
  const k = 2 / (period + 1);
  let ema = Number(values[0]);
  for (let i = 1; i < values.length; i += 1) {
    ema = Number(values[i]) * k + ema * (1 - k);
  }
  return ema;
}

function calcAtr(ohlcv, period = 14) {
  if (!Array.isArray(ohlcv) || ohlcv.length < period + 1) return 0;
  const trs = [];
  for (let i = 1; i < ohlcv.length; i += 1) {
    const high = Number(ohlcv[i][2]);
    const low = Number(ohlcv[i][3]);
    const prevClose = Number(ohlcv[i - 1][4]);
    trs.push(Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose)));
  }
  const recent = trs.slice(-period);
  return recent.reduce((a, b) => a + b, 0) / recent.length;
}

function calcUnrealizedPnl(trade, markPrice) {
  const entry = Number(trade.entry_price);
  const amt = Number(trade.amount);
  const csize = Number(trade.contract_size || 1);
  return trade.side === "long" ? (markPrice - entry) * amt * csize : (entry - markPrice) * amt * csize;
}

async function getTopGatePerpSymbols(markets, forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && cachedTopSymbols.length && now - lastUniverseRefreshMs < UNIVERSE_REFRESH_SEC * 1000) {
    return [...cachedTopSymbols];
  }
  let tickers = {};
  try {
    tickers = await exchange.fetchTickers();
  } catch (err) {
    console.warn("[UNIVERSE] fetchTickers() failed:", err.message);
  }

  const ranked = [];
  for (const [sym, m] of Object.entries(markets || {})) {
    if (!m || !m.active || !m.swap) continue;
    if (m.linear === false || m.settle !== "USDT" || m.quote !== "USDT") continue;
    const t = tickers[sym] || {};
    const vol = Number(t.quoteVolume || (Number(t.baseVolume || 0) * Number(t.last || 0)) || 0);
    ranked.push([vol, sym]);
  }
  ranked.sort((a, b) => b[0] - a[0]);
  const topSyms = ranked.slice(0, TOP_N_COINS).map((x) => x[1]);

  const ordered = [];
  const seen = new Set();
  for (const sym of [...PINNED_SYMBOLS, ...topSyms]) {
    if ((markets || {})[sym] && !seen.has(sym)) {
      seen.add(sym);
      ordered.push(sym);
    }
  }
  if (ordered.length) {
    cachedTopSymbols = ordered;
    lastUniverseRefreshMs = now;
  }
  return [...cachedTopSymbols];
}

async function getNextScanSlice(markets) {
  const topSyms = await getTopGatePerpSymbols(markets);
  const pinnedSet = new Set(PINNED_SYMBOLS);
  const pinnedActive = PINNED_SYMBOLS.filter((s) => markets[s] && markets[s].active);
  const rotatable = topSyms.filter((s) => !pinnedSet.has(s));

  if (!rotatable.length) {
    return { slice: pinnedActive.slice(0, MAX_PAIRS_PER_CYCLE), nextCursor: 0, rotatableTotal: 0 };
  }
  const st = await loadState();
  const cursor = ((Number(st.scan_cursor || 0) % rotatable.length) + rotatable.length) % rotatable.length;
  const take = Math.min(rotatable.length, Math.max(1, MAX_PAIRS_PER_CYCLE - pinnedActive.length));
  const slice = [];
  for (let i = 0; i < take; i += 1) {
    slice.push(rotatable[(cursor + i) % rotatable.length]);
  }
  const nextCursor = (cursor + take) % rotatable.length;
  return { slice: [...pinnedActive, ...slice], nextCursor, rotatableTotal: rotatable.length };
}

async function closeSinglePosition(trade, reason, exitPriceHint = null) {
  const sym = trade.symbol;
  let exitPx = Number(exitPriceHint);
  if (!exitPx || Number.isNaN(exitPx)) {
    const tk = await exchange.fetchTicker(sym);
    exitPx = Number(tk.last || tk.mark || trade.entry_price);
  }
  if (!PAPER_MODE) {
    const closeSide = trade.side === "long" ? "sell" : "buy";
    const order = await exchange.createOrder(sym, "market", closeSide, Number(trade.amount));
    exitPx = Number(order.average || order.price || exitPx);
  }
  const pnl = calcUnrealizedPnl(trade, exitPx);
  await recordClosedTrade(trade, exitPx, pnl, reason);

  const st = await loadState();
  st.active_trades = (st.active_trades || []).filter(
    (t) => !(t.symbol === sym && t.opened_at === trade.opened_at)
  );
  await saveState(st);
  await sendTelegram(
    `<b>Closed — ${reason}</b>\n<code>${sym}</code> ${String(trade.side).toUpperCase()} | PnL: <code>${pnl >= 0 ? "+" : ""}${pnl.toFixed(4)} USDT</code>`
  );
  return pnl;
}

async function performScanCycle() {
  if (scanBusy) {
    const st = await loadState();
    return { ok: true, last_scan: st.last_scan };
  }
  scanBusy = true;
  try {
    if (!Object.keys(cachedMarkets).length) {
      cachedMarkets = await exchange.loadMarkets();
    }
    const state = await loadState();
    const maxSlots = Math.max(1, Math.min(20, Number(state.max_open_positions || MAX_OPEN_POSITIONS)));
    const pullbackBand = Math.max(0.05, Math.min(10, Number(state.pullback_atr_band || PULLBACK_ATR_BAND)));
    const riskUsdt = Math.max(1, Number(state.risk_per_trade_usdt || RISK_PER_TRADE_USDT));
    const remaining = [];

    for (const tr of state.active_trades || []) {
      try {
        const m15 = await exchange.fetchOHLCV(tr.symbol, ENTRY_TF, undefined, ATR_LEN + 15);
        const spot = Number(m15[m15.length - 1][4]);
        const atr = Math.max(calcAtr(m15, ATR_LEN), Number(tr.atr_at_entry || 1));
        let stopPx = Number(tr.stop_price);
        const tpPx = Number(tr.tp_price);

        if (tr.side === "long") {
          stopPx = Math.max(stopPx, spot - TRAIL_ATR_MULT * atr);
        } else {
          stopPx = Math.min(stopPx, spot + TRAIL_ATR_MULT * atr);
        }
        tr.stop_price = Number(stopPx.toFixed(4));

        const heldHours = (Date.now() - Date.parse(tr.opened_at)) / 3600000;
        let exitReason = null;
        if (tr.side === "long") {
          if (spot <= stopPx) exitReason = "ATR Trailing/Hard Stop";
          else if (spot >= tpPx) exitReason = `Take Profit (${TP_ATR_MULT}x ATR)`;
        } else {
          if (spot >= stopPx) exitReason = "ATR Trailing/Hard Stop";
          else if (spot <= tpPx) exitReason = `Take Profit (${TP_ATR_MULT}x ATR)`;
        }
        if (!exitReason && heldHours >= MAX_HOLD_HOURS) {
          exitReason = `Max Hold Time (${MAX_HOLD_HOURS}h)`;
        }

        if (exitReason) {
          await closeSinglePosition(tr, exitReason, spot);
        } else {
          remaining.push(tr);
        }
      } catch (err) {
        console.error(`[MONITOR ERROR] ${tr.symbol}:`, err.message);
        remaining.push(tr);
      }
    }

    const afterCloseState = await loadState();
    afterCloseState.active_trades = remaining;
    await saveState(afterCloseState);

    const openSyms = new Set(remaining.map((t) => t.symbol));
    let scanInfo;

    if (afterCloseState.paused) {
      scanInfo = {
        checked_at_ist: nowIstString(),
        status: "PAUSED",
        reason: `Bot is PAUSED (${remaining.length}/${maxSlots} open). Skipping new entries.`,
        pair_summaries: [],
      };
    } else if (remaining.length >= maxSlots) {
      scanInfo = {
        checked_at_ist: nowIstString(),
        status: "MAX_POSITIONS",
        reason: `Max open slots reached (${remaining.length}/${maxSlots}). Monitoring active trades.`,
        pair_summaries: [],
      };
    } else {
      const { slice, nextCursor, rotatableTotal } = await getNextScanSlice(cachedMarkets);
      afterCloseState.scan_cursor = nextCursor;
      await saveState(afterCloseState);

      const pairSummaries = [];
      let openedNew = false;
      let checkedCount = 0;

      for (const sym of slice) {
        if (openSyms.has(sym) || openSyms.size >= maxSlots) break;
        const mkt = cachedMarkets[sym];
        if (!mkt) continue;

        try {
          const [h1, m15] = await Promise.all([
            exchange.fetchOHLCV(sym, TREND_TF, undefined, EMA_SLOW_LEN + 15),
            exchange.fetchOHLCV(sym, ENTRY_TF, undefined, Math.max(EMA_FAST_LEN, ATR_LEN) + 15),
          ]);
          checkedCount += 1;
          const h1Closes = h1.map((c) => Number(c[4]));
          const m15Closes = m15.map((c) => Number(c[4]));
          const spot = m15Closes[m15Closes.length - 1];
          const ema50 = calcEma(h1Closes, EMA_SLOW_LEN);
          const ema20 = calcEma(m15Closes, EMA_FAST_LEN);
          const atr = calcAtr(m15, ATR_LEN);
          if (atr <= 0) continue;

          const band = Math.max(1e-8, pullbackBand * atr);
          const distTo20 = Math.abs(spot - ema20);
          let setupSide = null;
          if (spot > ema50 && distTo20 <= band && spot >= ema20 * (1 - EMA20_GUARD_PCT)) {
            setupSide = "long";
          } else if (spot < ema50 && distTo20 <= band && spot <= ema20 * (1 + EMA20_GUARD_PCT)) {
            setupSide = "short";
          }

          if (!setupSide) {
            const ratio = Math.max(0, Math.min(1, 1 - distTo20 / (3 * band)));
            if (pairSummaries.length < 12) {
              pairSummaries.push({
                symbol: sym,
                open_readiness_pct: Number((ratio * 99).toFixed(1)),
                detail: `${spot > ema50 ? "Bull 1h" : "Bear 1h"} · Spot=${fmtPx(spot)} vs 15mEMA20=${fmtPx(ema20)}`,
              });
            }
            continue;
          }

          const stopDist = STOP_ATR_MULT * atr;
          const csize = Number(mkt.contractSize || 1);
          const minAmt = Number((mkt.limits && mkt.limits.amount && mkt.limits.amount.min) || 0.001);
          const rawQty = Math.max(minAmt, riskUsdt / (stopDist * csize));
          const amt = Number(exchange.amountToPrecision(sym, rawQty));
          if (amt <= 0) continue;

          let entryPx = spot;
          if (!PAPER_MODE) {
            const order = await exchange.createOrder(sym, "market", setupSide === "long" ? "buy" : "sell", amt);
            entryPx = Number(order.average || order.price || spot);
          }

          const stopPx = setupSide === "long" ? entryPx - STOP_ATR_MULT * atr : entryPx + STOP_ATR_MULT * atr;
          const tpPx = setupSide === "long" ? entryPx + TP_ATR_MULT * atr : entryPx - TP_ATR_MULT * atr;

          const newTrade = {
            symbol: sym,
            side: setupSide,
            amount: amt,
            contract_size: csize,
            entry_price: Number(entryPx.toFixed(4)),
            stop_price: Number(stopPx.toFixed(4)),
            tp_price: Number(tpPx.toFixed(4)),
            atr_at_entry: Number(atr.toFixed(4)),
            opened_at: new Date().toISOString(),
          };
          remaining.push(newTrade);
          afterCloseState.active_trades = remaining;
          await saveState(afterCloseState);
          openSyms.add(sym);
          openedNew = true;

          await sendTelegram(
            `<b>New ${setupSide.toUpperCase()} Opened [${modeLabel()}]</b>\n<code>${sym}</code> @ <code>${fmtPx(entryPx)}</code>`
          );
          scanInfo = {
            checked_at_ist: nowIstString(),
            status: "OPENED",
            reason: `Opened ${setupSide.toUpperCase()} on ${sym} (${remaining.length}/${maxSlots} active).`,
            pair_summaries: pairSummaries.slice(0, 12),
          };
          break;
        } catch (_) {}
      }

      if (!openedNew) {
        scanInfo = {
          checked_at_ist: nowIstString(),
          status: "SKIPPED",
          reason: `Rotated ${checkedCount} pairs (cursor->${nextCursor}/${rotatableTotal} of Top ${TOP_N_COINS}, ${remaining.length}/${maxSlots} slots) — no entry triggers.`,
          pair_summaries: pairSummaries.slice(0, 12),
        };
      }
    }

    const latest = await loadState();
    latest.last_scan = scanInfo;
    await saveState(latest);
    return { ok: true, last_scan: scanInfo };
  } finally {
    scanBusy = false;
  }
}

// ==========================================
// EXPRESS WEB UI & ROUTES
// ==========================================
const app = express();
app.use(express.json());

function verifyToken(req, res, next) {
  if (WEB_UI_TOKEN && req.query.token !== WEB_UI_TOKEN) {
    return res.status(401).json({ detail: "Invalid dashboard token" });
  }
  return next();
}

app.get("/healthz", async (_req, res) => {
  let redisOk = false;
  try {
    redisOk = (await redisClient.ping()) === "PONG";
  } catch (_) {}
  const st = await loadState();
  const cap = calcCapitalSummary(st.active_trades, st.total_capital_usdt);
  res.json({
    status: redisOk ? "ok" : "degraded",
    redis: redisOk,
    mode: modeLabel(),
    has_api_keys: HAS_PRIVATE_KEYS,
    port: WEB_UI_PORT,
    top_n_coins: TOP_N_COINS,
    max_pairs_per_cycle: MAX_PAIRS_PER_CYCLE,
    max_open_positions: st.max_open_positions,
    active_positions_count: (st.active_trades || []).length,
    ...cap,
    pullback_atr_band: st.pullback_atr_band,
    risk_per_trade_usdt: st.risk_per_trade_usdt,
    paused: st.paused,
    time_ist: nowIstString(),
  });
});

app.get("/api/state", async (_req, res) => {
  const st = await loadState();
  const stats = await loadStats();
  const history = await loadHistory(25);
  const trades = st.active_trades || [];
  const cap = calcCapitalSummary(trades, st.total_capital_usdt);
  let totalUnrealized = 0;
  let hasPnl = false;

  const liveTrades = [];
  for (const tr of trades) {
    let mark = null;
    let uPnl = null;
    try {
      const tk = await exchange.fetchTicker(tr.symbol);
      mark = Number(tk.last || tk.mark || tr.entry_price);
      uPnl = Number(calcUnrealizedPnl(tr, mark).toFixed(4));
      totalUnrealized += uPnl;
      hasPnl = true;
    } catch (_) {}

    const elapsedH = Math.max(0, (Date.now() - Date.parse(tr.opened_at)) / 3600000);
    const remH = Math.max(0, MAX_HOLD_HOURS - elapsedH);
    const remPct = Number(Math.min(100, Math.max(0, (remH / Math.max(0.1, MAX_HOLD_HOURS)) * 100)).toFixed(1));
    liveTrades.push({
      ...tr,
      mark_price: mark !== null ? Number(mark.toFixed(4)) : null,
      unrealized_pnl: uPnl,
      hours_left: Number(remH.toFixed(2)),
      remaining_pct: remPct,
    });
  }

  res.json({
    server_time_ist: nowIstString(),
    mode: modeLabel(),
    has_api_keys: HAS_PRIVATE_KEYS,
    top_n_coins: TOP_N_COINS,
    max_pairs_per_cycle: MAX_PAIRS_PER_CYCLE,
    pinned_symbols: PINNED_SYMBOLS,
    max_open_positions: st.max_open_positions,
    ...cap,
    pullback_atr_band: st.pullback_atr_band,
    risk_per_trade_usdt: st.risk_per_trade_usdt,
    paused: st.paused,
    last_scan: st.last_scan,
    active_trades: liveTrades,
    total_unrealized_pnl: hasPnl ? Number(totalUnrealized.toFixed(4)) : null,
    stats,
    trade_history: history,
  });
});

app.post("/api/scan-now", verifyToken, async (_req, res) => {
  res.json(await performScanCycle());
});

app.post("/api/capacity-config", verifyToken, async (req, res) => {
  const slots = Number(req.body.max_open_positions);
  const cap = Number(req.body.total_capital_usdt);
  if (Number.isNaN(slots) || slots < 1 || slots > 20 || Number.isNaN(cap) || cap < 10) {
    return res.status(400).json({ detail: "Invalid slots or total capital" });
  }
  const st = await loadState();
  st.max_open_positions = slots;
  st.total_capital_usdt = Number(cap.toFixed(2));
  await saveState(st);
  res.json({ ok: true, max_open_positions: st.max_open_positions, total_capital_usdt: st.total_capital_usdt });
});

app.post("/api/strategy-config", verifyToken, async (req, res) => {
  const band = Number(req.body.pullback_atr_band);
  const risk = Number(req.body.risk_per_trade_usdt);
  if (Number.isNaN(band) || band < 0.05 || band > 10 || Number.isNaN(risk) || risk < 1) {
    return res.status(400).json({ detail: "Invalid strategy parameters" });
  }
  const st = await loadState();
  st.pullback_atr_band = Number(band.toFixed(4));
  st.risk_per_trade_usdt = Number(risk.toFixed(2));
  await saveState(st);
  res.json({ ok: true, pullback_atr_band: st.pullback_atr_band, risk_per_trade_usdt: st.risk_per_trade_usdt });
});

app.post("/api/reset-stats", verifyToken, async (_req, res) => {
  const stats = await resetStatsAndHistory();
  res.json({ ok: true, stats });
});

app.post("/api/close-position", verifyToken, async (req, res) => {
  const sym = String(req.query.symbol || (req.body && req.body.symbol) || "").trim();
  if (!sym) return res.status(400).json({ detail: "Missing symbol" });
  const st = await loadState();
  const target = (st.active_trades || []).find((t) => t.symbol === sym);
  if (!target) return res.json({ ok: true, closed: false, symbol: sym });
  const pnl = await closeSinglePosition(target, "Manual Web UI Close");
  res.json({ ok: true, closed: true, symbol: sym, realized_pnl: Number(pnl.toFixed(4)) });
});

app.post("/api/toggle-pause", verifyToken, async (_req, res) => {
  const st = await loadState();
  st.paused = !Boolean(st.paused);
  await saveState(st);
  res.json({ ok: true, paused: st.paused });
});

app.post("/api/closeall", verifyToken, async (_req, res) => {
  const st = await loadState();
  st.paused = true;
  await saveState(st);
  let closed = 0;
  let totalPnl = 0;
  for (const tr of [...(st.active_trades || [])]) {
    totalPnl += await closeSinglePosition(tr, "Emergency /closeall");
    closed += 1;
  }
  res.json({ ok: true, closed_count: closed, realized_pnl: Number(totalPnl.toFixed(4)) });
});

app.get("/", (_req, res) => {
  res.type("html").send(`<!DOCTYPE html><html><head><meta charset="utf-8"/><title>ATR Top-100 Node Bot</title>
  <style>body{font-family:sans-serif;background:#0b0f17;color:#e6edf7;padding:20px;max-width:960px;margin:auto}
  .card{background:#131a26;border:1px solid #222d40;border-radius:10px;padding:14px;margin-bottom:14px}
  button{background:#3b82f6;color:#fff;border:none;border-radius:7px;padding:8px 12px;cursor:pointer;margin-right:8px}
  input{background:#0f1520;color:#e6edf7;border:1px solid #222d40;border-radius:7px;padding:7px;width:120px;margin-right:8px}</style></head>
  <body><h2>ATR Top-100 Bot (Node.js)</h2>
  <div class="card"><button onclick="post('/api/scan-now')">Scan Now</button><button onclick="post('/api/toggle-pause')">Pause / Resume</button><button onclick="post('/api/reset-stats')">Reset Stats</button><button onclick="post('/api/closeall')">Close All & Pause</button></div>
  <div class="card"><pre id="out">Loading...</pre></div>
  <script>
  async function post(p){await fetch(p,{method:'POST'});refresh();}
  async function refresh(){const r=await fetch('/api/state');document.getElementById('out').textContent=JSON.stringify(await r.json(),null,2);}
  refresh();setInterval(refresh,3000);
  </script></body></html>`);
});

async function main() {
  await redisClient.connect();
  cachedMarkets = await exchange.loadMarkets();
  await getTopGatePerpSymbols(cachedMarkets, true);

  app.listen(WEB_UI_PORT, WEB_UI_HOST, () => {
    console.log(`[NODE BOT] Listening on http://${WEB_UI_HOST}:${WEB_UI_PORT} (${modeLabel()})`);
  });

  await performScanCycle();
  setInterval(() => {
    performScanCycle().catch((e) => console.error("[LOOP ERROR]", e.message));
  }, POLL_INTERVAL_SEC * 1000);
}

main().catch((err) => {
  console.error("[FATAL]", err);
  process.exit(1);
});
