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
  return (
    new Date().toLocaleString("en-GB", {
      timeZone: "Asia/Kolkata",
      hour12: false,
    }) + " IST"
  );
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
  return items
    .map((x) => {
      try {
        return JSON.parse(x);
      } catch (_) {
        return null;
      }
    })
    .filter(Boolean);
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

async function sendTelegramTo(chatId, htmlText) {
  if (!TELEGRAM_BOT_TOKEN || !chatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: String(chatId),
        text: htmlText,
        parse_mode: "HTML",
        disable_web_page_preview: true,
      }),
    });
  } catch (err) {
    console.error("[TELEGRAM SEND ERROR]", err.message);
  }
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
  await sendTelegramTo(TELEGRAM_CHAT_ID, htmlText);
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
    const vol = Number(t.quoteVolume || Number(t.baseVolume || 0) * Number(t.last || 0) || 0);
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

async function closeAllPositionsNow(reason) {
  const st = await loadState();
  let closed = 0;
  let totalPnl = 0;
  for (const tr of [...(st.active_trades || [])]) {
    totalPnl += await closeSinglePosition(tr, reason);
    closed += 1;
  }
  return { closed, totalPnl };
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
// TELEGRAM POLLING LISTENER
// ==========================================
async function buildStatusHtml() {
  const st = await loadState();
  const stats = await loadStats();
  const trades = st.active_trades || [];
  const cap = calcCapitalSummary(trades, st.total_capital_usdt);
  const maxSlots = Math.max(1, Math.min(20, Number(st.max_open_positions || MAX_OPEN_POSITIONS)));
  const statusFlag = st.paused ? "PAUSED" : "RUNNING";

  let posBlock;
  if (trades.length > 0) {
    const lines = [`<b>Active Positions (${trades.length}/${maxSlots}):</b>`];
    let totU = 0;
    for (const tr of trades) {
      let u = 0;
      let markPx = Number(tr.entry_price);
      try {
        const tk = await exchange.fetchTicker(tr.symbol);
        markPx = Number(tk.last || tk.mark || tr.entry_price);
        u = calcUnrealizedPnl(tr, markPx);
        totU += u;
      } catch (_) {}
      lines.push(
        `• <code>${tr.symbol}</code> ${String(tr.side || "").toUpperCase()} <code>${tr.amount}</code> ` +
          `(@ <code>${fmtPx(markPx)}</code> | PnL: <code>${u >= 0 ? "+" : ""}${u.toFixed(4)}</code>)`
      );
    }
    lines.push(`Total Unrealized: <code>${totU >= 0 ? "+" : ""}${totU.toFixed(4)} USDT</code>`);
    posBlock = lines.join("\n");
  } else {
    const scanNote = st.last_scan && st.last_scan.reason ? `\n<i>Last Scan: ${st.last_scan.reason}</i>` : "";
    posBlock = `<b>Active Positions:</b> 0/${maxSlots} (Flat / Scanning)${scanNote}`;
  }

  const total = Number(stats.total_trades || 0);
  const wins = Number(stats.wins || 0);
  const losses = Number(stats.losses || 0);
  const wr = total > 0 ? ((wins / total) * 100).toFixed(1) : "0.0";
  const realized = Number(stats.realized_pnl || 0);

  return (
    `<b>ATR Top-${TOP_N_COINS} [${modeLabel()}] [${statusFlag}]</b>\n` +
    `${posBlock}\n\n` +
    `<b>Capital:</b> <code>${cap.capital_used_usdt.toFixed(2)}</code> / ` +
    `<code>${cap.total_capital_usdt.toFixed(2)} USDT</code> (${cap.capital_used_pct}%)\n` +
    `<b>Realized Stats:</b> ${wins}W / ${losses}L (${wr}%) | PnL: <code>${realized >= 0 ? "+" : ""}${realized.toFixed(4)}</code>\n` +
    `<i>${nowIstString()}</i>`
  );
}

async function startTelegramPolling() {
  if (!TELEGRAM_BOT_TOKEN) {
    console.warn("[TELEGRAM] TELEGRAM_BOT_TOKEN not set; polling listener disabled.");
    return;
  }

  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/deleteWebhook`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ drop_pending_updates: false }),
    });
  } catch (_) {}

  let offset = null;
  console.log("[TELEGRAM] Command polling listener started.");

  while (true) {
    try {
      const qs = new URLSearchParams({ timeout: "20" });
      if (offset !== null) qs.set("offset", String(offset));

      const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates?${qs.toString()}`);
      if (!resp.ok) {
        await new Promise((r) => setTimeout(r, 3000));
        continue;
      }

      const data = await resp.json();
      for (const upd of data.result || []) {
        offset = Number(upd.update_id) + 1;
        const msg = upd.message || upd.edited_message || {};
        const chatId = String((msg.chat && msg.chat.id) || "");
        const rawText = String(msg.text || "").trim();
        if (!chatId || !rawText.startsWith("/")) continue;

        const parts = rawText.split(/\s+/);
        const cmd = parts[0].split("@")[0].toLowerCase();

        if (cmd === "/chatid") {
          await sendTelegramTo(chatId, `Chat ID: <code>${chatId}</code>`);
          continue;
        }

        if (!TELEGRAM_CHAT_ID || chatId !== String(TELEGRAM_CHAT_ID)) {
          continue;
        }

        if (cmd === "/status") {
          await sendTelegramTo(chatId, await buildStatusHtml());
        } else if (cmd === "/scannow") {
          await sendTelegramTo(chatId, `<b>Scanning Top ${TOP_N_COINS} slice (${MAX_PAIRS_PER_CYCLE}/cycle)...</b>`);
          const res = await performScanCycle();
          const scan = (res && res.last_scan) || {};
          const topPairs = (scan.pair_summaries || [])
            .slice(0, 8)
            .map((p) => `• <code>${p.symbol}</code> [${Number(p.open_readiness_pct || 0).toFixed(0)}%]: ${p.detail}`)
            .join("\n");
          await sendTelegramTo(
            chatId,
            `<b>Scan [${scan.status || "UNKNOWN"}]</b>\n` +
              `Summary: ${scan.reason || "--"}` +
              (topPairs ? `\n${topPairs}` : "") +
              `\n<i>${scan.checked_at_ist || nowIstString()}</i>`
          );
        } else if (cmd === "/pause") {
          const st = await loadState();
          st.paused = true;
          await saveState(st);
          await sendTelegramTo(chatId, "<b>Bot Paused.</b> New entries disabled.");
        } else if (cmd === "/resume") {
          const st = await loadState();
          st.paused = false;
          await saveState(st);
          await sendTelegramTo(chatId, "<b>Bot Resumed.</b> Scanning Top-100 slice.");
        } else if (cmd === "/setslots") {
          const n = Number(parts[1]);
          if (!parts[1] || Number.isNaN(n) || n < 1 || n > 20) {
            await sendTelegramTo(chatId, "Usage: <code>/setslots &lt;1-20&gt;</code>");
          } else {
            const st = await loadState();
            st.max_open_positions = Math.floor(n);
            await saveState(st);
            await sendTelegramTo(chatId, `Open slots updated to <b>${st.max_open_positions}</b>.`);
          }
        } else if (cmd === "/resetstats") {
          await resetStatsAndHistory();
          await sendTelegramTo(chatId, "<b>Stats &amp; History Reset.</b>");
        } else if (cmd === "/closeall") {
          const st = await loadState();
          st.paused = true;
          await saveState(st);
          const { closed, totalPnl } = await closeAllPositionsNow("Emergency /closeall");
          await sendTelegramTo(
            chatId,
            `<b>Bot PAUSED.</b> Closed <b>${closed}</b> position(s) (PnL: <code>${totalPnl >= 0 ? "+" : ""}${totalPnl.toFixed(4)}</code>).`
          );
        }
      }
    } catch (err) {
      console.error("[TELEGRAM POLL ERROR]", err.message);
      await new Promise((r) => setTimeout(r, 4000));
    }
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
  st.max_open_positions = Math.floor(slots);
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
  const { closed, totalPnl } = await closeAllPositionsNow("Emergency /closeall");
  res.json({ ok: true, closed_count: closed, realized_pnl: Number(totalPnl.toFixed(4)) });
});

app.get("/", (_req, res) => {
  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>ATR Top-100 Node Bot</title>
<style>
  :root {
    --bg: #0b0f17; --card: #131a26; --border: #222d40;
    --text: #e6edf7; --muted: #8b9bb4; --green: #22c55e;
    --red: #ef4444; --amber: #f59e0b; --blue: #3b82f6;
  }
  * { box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  body { margin: 0; background: var(--bg); color: var(--text); padding: 20px; }
  .wrap { max-width: 1020px; margin: 0 auto; }
  .topbar { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 18px; }
  .title { font-size: 1.25rem; font-weight: 700; display: flex; align-items: center; gap: 10px; }
  .badge { font-size: 0.75rem; padding: 3px 8px; border-radius: 999px; font-weight: 600; }
  .badge-run { background: rgba(34,197,94,.15); color: var(--green); border: 1px solid rgba(34,197,94,.4); }
  .badge-pause { background: rgba(245,158,11,.15); color: var(--amber); border: 1px solid rgba(245,158,11,.4); }
  .badge-mode { background: rgba(59,130,246,.15); color: var(--blue); border: 1px solid rgba(59,130,246,.4); }
  .meta { font-size: 0.85rem; color: var(--muted); }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(195px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }
  .label { font-size: 0.78rem; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }
  .val { font-size: 1.35rem; font-weight: 700; margin-top: 6px; font-variant-numeric: tabular-nums; }
  .sub-meta { margin-top: 4px; font-size: 0.78rem; color: var(--muted); }
  .pos { color: var(--green); } .neg { color: var(--red); }
  .scan-reason { margin-top: 8px; font-size: 0.9rem; color: var(--amber); }
  .pair-list { margin-top: 10px; display: grid; gap: 8px; font-size: 0.84rem; color: var(--muted); }
  .coin-item { border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; background: #0f1520; }
  .coin-top { display: flex; justify-content: space-between; align-items: center; gap: 8px; font-size: 0.84rem; }
  .coin-bar-bg { width: 100%; height: 6px; background: #1c2536; border-radius: 999px; overflow: hidden; margin-top: 6px; }
  .coin-bar-fill { height: 100%; background: linear-gradient(90deg, var(--amber), var(--blue), var(--green)); transition: width .35s ease; }
  .pos-item { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-top: 10px; background: #0f1520; }
  .bar-bg { width: 100%; height: 8px; background: #1c2536; border-radius: 999px; overflow: hidden; margin-top: 8px; }
  .bar-fill { height: 100%; background: linear-gradient(90deg, var(--blue), var(--green)); transition: width .4s ease; }
  .controls { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-top: 14px; }
  .param-form {
    display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end;
    margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--border);
  }
  .param-field { display: flex; flex-direction: column; gap: 4px; font-size: 0.78rem; color: var(--muted); }
  .param-field input {
    background: #0f1520; border: 1px solid var(--border); color: var(--text);
    padding: 8px 10px; border-radius: 8px; width: 145px; font-size: 0.85rem;
  }
  .feedback { margin-top: 10px; font-size: 0.83rem; color: var(--muted); min-height: 18px; }
  button { cursor: pointer; border: none; border-radius: 8px; padding: 9px 14px; font-weight: 600; font-size: 0.85rem; }
  button:disabled { opacity: 0.6; cursor: not-allowed; }
  .btn-primary { background: var(--blue); color: #fff; }
  .btn-warn { background: var(--amber); color: #111; }
  .btn-danger { background: var(--red); color: #fff; }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.88rem; }
  td, th { padding: 8px 6px; border-bottom: 1px solid var(--border); text-align: left; white-space: nowrap; }
  th { color: var(--muted); font-weight: 600; font-size: 0.76rem; text-transform: uppercase; }
  code { background: #0f1520; padding: 2px 6px; border-radius: 4px; font-size: 0.83rem; }
</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="title">
      ATR Top-100 Node Bot
      <span id="modeBadge" class="badge badge-mode">PUBLIC-PAPER</span>
      <span id="statusBadge" class="badge badge-run">RUNNING</span>
    </div>
    <div class="meta">
      <span id="istClock">--</span> · Refresh in <b id="countdown">3</b>s
    </div>
  </div>

  <div class="grid">
    <div class="card"><div class="label">Total Unrealized PnL</div><div id="livePnl" class="val">--</div></div>
    <div class="card"><div class="label">Realized PnL (USDT)</div><div id="realizedPnl" class="val">--</div></div>
    <div class="card"><div class="label">Win / Loss Record</div><div id="winLoss" class="val">0W / 0L</div></div>
    <div class="card"><div class="label">Open Slots</div><div id="slotsVal" class="val">0 / 2</div></div>
    <div class="card">
      <div class="label">Capital Used / Total</div>
      <div id="capitalVal" class="val">0.00 / 500.00</div>
      <div id="capitalSub" class="sub-meta">0.0% used · Free: 500.00 USDT</div>
    </div>
  </div>

  <div class="card" style="margin-bottom:16px;">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <div class="label">Live Scanner Diagnostics (<span id="universeLabel">--</span>)</div>
      <span style="font-size:0.82rem; color:var(--muted);" id="scanTime">--</span>
    </div>
    <div id="scanReason" class="scan-reason">Waiting for first scan cycle...</div>
    <div id="pairSummaries" class="pair-list"></div>
  </div>

  <div class="card" style="margin-bottom:16px;">
    <div class="label">Active Positions</div>
    <div id="noTradeMsg" style="margin-top:10px; color:var(--muted);">No active positions open.</div>
    <div id="tradesList"></div>
  </div>

  <div class="card">
    <div class="label">Controls &amp; Runtime Configuration</div>
    <div class="controls">
      <button id="scanNowBtn" type="button" class="btn-primary" onclick="scanNow()">Scan Now</button>
      <button id="toggleBtn" type="button" class="btn-warn" onclick="togglePause()">Pause Bot</button>
      <button id="resetStatsBtn" type="button" class="btn-warn" onclick="resetStats()">Reset Stats</button>
      <button type="button" class="btn-danger" onclick="emergencyCloseAll()">Close All &amp; Pause</button>
    </div>

    <form id="capacityForm" class="param-form" onsubmit="saveCapacityConfig(event)">
      <div class="param-field">
        <label for="maxSlotsInput">OPEN_SLOTS (1–20)</label>
        <input id="maxSlotsInput" type="number" step="1" min="1" max="20" value="2" required />
      </div>
      <div class="param-field">
        <label for="totalCapInput">TOTAL_CAPITAL_USDT</label>
        <input id="totalCapInput" type="number" step="10" min="10" max="10000000" value="500" required />
      </div>
      <button id="saveCapBtn" type="submit" class="btn-primary">Update Slots &amp; Capital</button>
    </form>

    <form id="strategyForm" class="param-form" onsubmit="saveStrategyConfig(event)">
      <div class="param-field">
        <label for="pullbackBandInput">PULLBACK_ATR_BAND</label>
        <input id="pullbackBandInput" type="number" step="0.05" min="0.05" max="10" value="0.75" required />
      </div>
      <div class="param-field">
        <label for="riskUsdtInput">RISK_PER_TRADE_USDT</label>
        <input id="riskUsdtInput" type="number" step="1" min="1" max="100000" value="25" required />
      </div>
      <button id="saveStrategyBtn" type="submit" class="btn-primary">Apply Strategy Params</button>
    </form>

    <div id="feedback" class="feedback">Ready.</div>
  </div>

  <div class="card" style="margin-top:16px;">
    <div class="label">Recent Closed Trades</div>
    <div id="noHistoryMsg" style="margin-top:10px; color:var(--muted);">No closed trades recorded yet.</div>
    <div id="historyWrap" style="display:none; overflow-x:auto;">
      <table>
        <thead>
          <tr><th>Closed At</th><th>Pair</th><th>Side</th><th>Entry → Exit</th><th>Reason</th><th>Realized PnL</th></tr>
        </thead>
        <tbody id="historyBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const token = new URLSearchParams(window.location.search).get("token") || "";
function apiUrl(path) { return token ? \`\${path}?token=\${encodeURIComponent(token)}\` : path; }
let secLeft = 3;

function fmtPx(v) {
  const n = Number(v);
  if (Number.isNaN(n)) return "--";
  return Math.abs(n) < 50 ? n.toFixed(4) : n.toFixed(2);
}

function setFeedback(msg, isErr = false) {
  const el = document.getElementById("feedback");
  if (!el) return;
  el.textContent = msg;
  el.style.color = isErr ? "var(--red)" : "var(--green)";
}

async function fetchState() {
  try {
    const res = await fetch("/api/state");
    const data = await res.json();
    document.getElementById("istClock").textContent = data.server_time_ist || "--";
    document.getElementById("modeBadge").textContent = data.mode || "PUBLIC-PAPER";
    const pinnedStr = (data.pinned_symbols || []).join(", ");
    document.getElementById("universeLabel").textContent =
      \`Top \${data.top_n_coins || 100} Gate USDT Perps · \${data.max_pairs_per_cycle || 25}/cycle\` + (pinnedStr ? \` · Pins: \${pinnedStr}\` : "");

    const st = document.getElementById("statusBadge");
    const tBtn = document.getElementById("toggleBtn");
    if (data.paused) {
      st.textContent = "PAUSED"; st.className = "badge badge-pause";
      tBtn.textContent = "Resume Bot"; tBtn.className = "btn-primary";
    } else {
      st.textContent = "RUNNING"; st.className = "badge badge-run";
      tBtn.textContent = "Pause Bot"; tBtn.className = "btn-warn";
    }

    const activeTrades = data.active_trades || [];
    const maxPos = data.max_open_positions || 2;
    document.getElementById("slotsVal").textContent = \`\${activeTrades.length} / \${maxPos}\`;

    const usedCap = Number(data.capital_used_usdt || 0);
    const totCap = Number(data.total_capital_usdt || 500);
    const freeCap = Number(data.free_capital_usdt ?? Math.max(0, totCap - usedCap));
    const usedPct = Number(data.capital_used_pct || 0);
    document.getElementById("capitalVal").textContent = \`\${usedCap.toFixed(2)} / \${totCap.toFixed(2)}\`;
    document.getElementById("capitalSub").textContent = \`\${usedPct.toFixed(1)}% used · Free: \${freeCap.toFixed(2)} USDT\`;

    const slotsEl = document.getElementById("maxSlotsInput");
    const capEl = document.getElementById("totalCapInput");
    if (slotsEl && document.activeElement !== slotsEl && data.max_open_positions !== undefined) {
      slotsEl.value = data.max_open_positions;
    }
    if (capEl && document.activeElement !== capEl && data.total_capital_usdt !== undefined) {
      capEl.value = data.total_capital_usdt;
    }

    const bandEl = document.getElementById("pullbackBandInput");
    const riskEl = document.getElementById("riskUsdtInput");
    if (bandEl && document.activeElement !== bandEl && data.pullback_atr_band !== undefined) {
      bandEl.value = data.pullback_atr_band;
    }
    if (riskEl && document.activeElement !== riskEl && data.risk_per_trade_usdt !== undefined) {
      riskEl.value = data.risk_per_trade_usdt;
    }

    const scan = data.last_scan;
    if (scan) {
      document.getElementById("scanTime").textContent = scan.checked_at_ist || "--";
      const rEl = document.getElementById("scanReason");
      rEl.textContent = \`[\${scan.status || "INFO"}] \${scan.reason || ""}\`;
      rEl.style.color = scan.status === "OPENED" ? "var(--green)" : (scan.status === "ERROR" ? "var(--red)" : "var(--amber)");
      const ps = scan.pair_summaries || [];
      document.getElementById("pairSummaries").innerHTML = ps.map(p => {
        const pct = Math.max(0, Math.min(100, Number(p.open_readiness_pct ?? 0)));
        return \`<div class="coin-item">
          <div class="coin-top">
            <div><code>\${p.symbol}</code> — <span>\${p.detail}</span></div>
            <b>\${pct.toFixed(0)}% to open</b>
          </div>
          <div class="coin-bar-bg"><div class="coin-bar-fill" style="width:\${pct}%;"></div></div>
        </div>\`;
      }).join("");
    }

    const stats = data.stats || {};
    const rPnl = Number(stats.realized_pnl || 0);
    const rEl = document.getElementById("realizedPnl");
    rEl.textContent = (rPnl >= 0 ? "+" : "") + rPnl.toFixed(4);
    rEl.className = "val " + (rPnl >= 0 ? "pos" : "neg");

    const total = Number(stats.total_trades || 0);
    const wins = Number(stats.wins || 0);
    const losses = Number(stats.losses || 0);
    const wr = total > 0 ? ((wins / total) * 100).toFixed(1) : "0.0";
    document.getElementById("winLoss").textContent = \`\${wins}W / \${losses}L (\${wr}%)\`;

    const liveEl = document.getElementById("livePnl");
    const listEl = document.getElementById("tradesList");
    if (activeTrades.length > 0) {
      document.getElementById("noTradeMsg").style.display = "none";
      const totU = Number(data.total_unrealized_pnl || 0);
      liveEl.textContent = (totU >= 0 ? "+" : "") + totU.toFixed(4);
      liveEl.className = "val " + (totU >= 0 ? "pos" : "neg");

      listEl.innerHTML = activeTrades.map(t => {
        const u = Number(t.unrealized_pnl || 0);
        const uCls = u >= 0 ? "pos" : "neg";
        return \`<div class="pos-item">
          <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">
            <div><code>\${t.symbol}</code> <b>\${(t.side || "").toUpperCase()}</b> × <code>\${t.amount}</code></div>
            <div style="display:flex; align-items:center; gap:10px;">
              <span class="\${uCls}"><b>PnL: \${(u >= 0 ? "+" : "") + u.toFixed(4)} USDT</b></span>
              <button type="button" class="btn-danger" style="padding:5px 10px; font-size:0.78rem;" onclick="closeSinglePosition(this, \${JSON.stringify(t.symbol)})">Close</button>
            </div>
          </div>
          <div style="display:flex; flex-wrap:wrap; gap:14px; margin-top:8px; font-size:0.84rem; color:var(--muted);">
            <span>Entry: <b style="color:var(--text);">\${fmtPx(t.entry_price)}</b></span>
            <span>Mark: <b style="color:var(--text);">\${t.mark_price !== null && t.mark_price !== undefined ? fmtPx(t.mark_price) : "--"}</b></span>
            <span>Stop: <b style="color:var(--amber);">\${fmtPx(t.stop_price)}</b></span>
            <span>TP: <b style="color:var(--green);">\${fmtPx(t.tp_price)}</b></span>
            <span style="margin-left:auto;">Hold left: <b>\${t.hours_left}h</b> (\${t.remaining_pct}%)</span>
          </div>
          <div class="bar-bg"><div class="bar-fill" style="width:\${t.remaining_pct}%;"></div></div>
        </div>\`;
      }).join("");
    } else {
      document.getElementById("noTradeMsg").style.display = "block";
      listEl.innerHTML = "";
      liveEl.textContent = "Flat";
      liveEl.className = "val";
    }

    const hist = data.trade_history || [];
    const hBody = document.getElementById("historyBody");
    if (hist.length > 0) {
      document.getElementById("noHistoryMsg").style.display = "none";
      document.getElementById("historyWrap").style.display = "block";
      hBody.innerHTML = hist.map(row => {
        const pnlVal = Number(row.realized_pnl || 0);
        const cls = pnlVal >= 0 ? "pos" : "neg";
        const pnlStr = (pnlVal >= 0 ? "+" : "") + pnlVal.toFixed(4);
        const closedLabel = row.closed_at ? new Date(row.closed_at).toLocaleString("en-GB", { timeZone: "Asia/Kolkata", hour12: false }) : "--";
        return \`<tr>
          <td><code>\${closedLabel}</code></td>
          <td><code>\${row.symbol || "--"}</code></td>
          <td><b>\${(row.side || "").toUpperCase()}</b></td>
          <td>\${fmtPx(row.entry_price)} → \${fmtPx(row.exit_price)}</td>
          <td>\${row.exit_reason || "--"}</td>
          <td class="\${cls}"><b>\${pnlStr}</b></td>
        </tr>\`;
      }).join("");
    } else {
      document.getElementById("noHistoryMsg").style.display = "block";
      document.getElementById("historyWrap").style.display = "none";
    }
  } catch (err) {
    console.error("State fetch error:", err);
  }
}

async function saveCapacityConfig(e) {
  e.preventDefault();
  const btn = document.getElementById("saveCapBtn");
  const slots = parseInt(document.getElementById("maxSlotsInput").value, 10);
  const cap = parseFloat(document.getElementById("totalCapInput").value);
  if (Number.isNaN(slots) || slots < 1 || slots > 20 || Number.isNaN(cap) || cap < 10) {
    setFeedback("Enter valid slots (1–20) and total capital (>= 10 USDT).", true);
    return;
  }
  btn.disabled = true;
  btn.textContent = "Updating...";
  try {
    const res = await fetch(apiUrl("/api/capacity-config"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ max_open_positions: slots, total_capital_usdt: cap }),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) {
      setFeedback(out.detail || "Failed to update slots & capital.", true);
      return;
    }
    setFeedback(\`Updated: OPEN_SLOTS=\${out.max_open_positions}, TOTAL_CAPITAL_USDT=\${Number(out.total_capital_usdt).toFixed(2)}\`);
    await fetchState();
  } finally {
    btn.disabled = false;
    btn.textContent = "Update Slots & Capital";
  }
}

async function saveStrategyConfig(e) {
  e.preventDefault();
  const btn = document.getElementById("saveStrategyBtn");
  const band = parseFloat(document.getElementById("pullbackBandInput").value);
  const risk = parseFloat(document.getElementById("riskUsdtInput").value);
  if (Number.isNaN(band) || band <= 0 || Number.isNaN(risk) || risk <= 0) {
    setFeedback("Enter valid positive numbers for pullback band and risk USDT.", true);
    return;
  }
  btn.disabled = true;
  btn.textContent = "Applying...";
  try {
    const res = await fetch(apiUrl("/api/strategy-config"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pullback_atr_band: band, risk_per_trade_usdt: risk }),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) {
      setFeedback(out.detail || "Failed to update strategy config.", true);
      return;
    }
    setFeedback(\`Applied: PULLBACK_ATR_BAND=\${out.pullback_atr_band}, RISK_PER_TRADE_USDT=\${Number(out.risk_per_trade_usdt).toFixed(2)}\`);
    await fetchState();
  } finally {
    btn.disabled = false;
    btn.textContent = "Apply Strategy Params";
  }
}

async function closeSinglePosition(btn, symbol) {
  if (!confirm(\`Close \${symbol} position now? Bot will remain running.\`)) return;
  const orig = btn.textContent;
  btn.disabled = true;
  btn.textContent = "Closing...";
  try {
    const qs = new URLSearchParams({ symbol });
    if (token) qs.set("token", token);
    const res = await fetch(\`/api/close-position?\${qs.toString()}\`, { method: "POST" });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      setFeedback(err.detail || \`Failed to close \${symbol}\`, true);
    } else {
      setFeedback(\`Closed \${symbol}.\`);
    }
    await fetchState();
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
  }
}

async function resetStats() {
  if (!confirm("Clear all realized PnL stats and closed trade history?")) return;
  const btn = document.getElementById("resetStatsBtn");
  btn.disabled = true;
  btn.textContent = "Resetting...";
  try {
    const res = await fetch(apiUrl("/api/reset-stats"), { method: "POST" });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      setFeedback(err.detail || "Failed to reset stats", true);
    } else {
      setFeedback("Cleared realized stats and trade history.");
    }
    await fetchState();
  } finally {
    btn.disabled = false;
    btn.textContent = "Reset Stats";
  }
}

async function scanNow() {
  const btn = document.getElementById("scanNowBtn");
  btn.disabled = true;
  btn.textContent = "Scanning...";
  try {
    await fetch(apiUrl("/api/scan-now"), { method: "POST" });
    await fetchState();
    secLeft = 3;
    document.getElementById("countdown").textContent = secLeft;
  } finally {
    btn.disabled = false;
    btn.textContent = "Scan Now";
  }
}

async function togglePause() { await fetch(apiUrl("/api/toggle-pause"), { method: "POST" }); fetchState(); }

async function emergencyCloseAll() {
  if (!confirm("Close all open positions immediately and pause the bot?")) return;
  await fetch(apiUrl("/api/closeall"), { method: "POST" });
  fetchState();
}

fetchState();
setInterval(() => {
  secLeft -= 1;
  if (secLeft <= 0) { secLeft = 3; fetchState(); }
  document.getElementById("countdown").textContent = secLeft;
}, 1000);
</script>
</body>
</html>`);
});

async function main() {
  await redisClient.connect();
  cachedMarkets = await exchange.loadMarkets();
  await getTopGatePerpSymbols(cachedMarkets, true);

  startTelegramPolling().catch((e) => console.error("[TELEGRAM FATAL]", e.message));

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
