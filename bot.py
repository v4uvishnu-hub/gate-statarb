import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
import ccxt.async_support as ccxt
from aiohttp import web, ClientSession, ClientTimeout
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from strategy import StatArbEngine

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger("GateArb")

IST = timezone(timedelta(hours=5, minutes=30))

GATE_API_KEY = os.getenv("GATE_API_KEY", "")
GATE_API_SECRET = os.getenv("GATE_API_SECRET", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "3000"))

TOP_N_COINS = int(os.getenv("TOP_N_COINS", "100"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "3"))
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
NOTIONAL_USDT = float(os.getenv("NOTIONAL_USDT", "100"))
SIM_START_EQUITY = float(os.getenv("SIM_START_EQUITY", "1000"))
TAKER_FEE_RATE = float(os.getenv("TAKER_FEE_RATE", "0.0005"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# State persistence options
PERSIST_STATE = os.getenv("PERSIST_STATE", "true").lower() == "true"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
RESET_STATE = os.getenv("RESET_STATE", "false").lower() == "true"

GATE_FUTURES_BASE = "https://api.gateio.ws/api/v4/futures/usdt"

engine = StatArbEngine(
    lookback=int(os.getenv("LOOKBACK", "100")),
    z_entry=float(os.getenv("Z_ENTRY", "2.0")),
    z_exit=float(os.getenv("Z_EXIT", "0.2")),
    z_stop=float(os.getenv("Z_STOP", "3.8")),
)

state = {
    "running": True,
    "scan_status": "Initializing...",
    "last_scan_sec": 0.0,
    "universe_count": 0,
    "ready_count": 0,
    "active_pair_y": "ETH/USDT:USDT",
    "active_pair_x": "BTC/USDT:USDT",
    "positions": [],
    "unrealized_pnl": 0.0,
    "sim_equity": SIM_START_EQUITY,
    "realized_pnl": 0.0,
    "wins": 0,
    "losses": 0,
    "trades": [],
    "top_pairs": [],
    "metrics": {},
    "updated_at": None,
    "events": [],
}

candle_cache: dict[str, pd.DataFrame] = {}


def now_ist() -> datetime:
    return datetime.now(IST)


def save_state():
    if not PERSIST_STATE:
        return
    try:
        parent = os.path.dirname(STATE_FILE)
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = {
            "running": state["running"],
            "z_entry": engine.z_entry,
            "active_pair_y": state["active_pair_y"],
            "active_pair_x": state["active_pair_x"],
            "positions": state["positions"],
            "sim_equity": state["sim_equity"],
            "realized_pnl": state["realized_pnl"],
            "wins": state["wins"],
            "losses": state["losses"],
            "trades": state["trades"],
            "events": state["events"],
        }
        tmp_path = f"{STATE_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, STATE_FILE)
    except Exception as e:
        log.error(f"Failed to save state to {STATE_FILE}: {e}")


def load_state():
    if not PERSIST_STATE or RESET_STATE:
        if RESET_STATE and os.path.exists(STATE_FILE):
            try:
                os.remove(STATE_FILE)
                log.info(f"RESET_STATE=true: removed {STATE_FILE}")
            except Exception as e:
                log.warning(f"Could not remove {STATE_FILE}: {e}")
        return
    if not os.path.exists(STATE_FILE):
        log.info(f"No saved state at {STATE_FILE}; starting fresh.")
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        state["running"] = bool(saved.get("running", True))
        if "z_entry" in saved:
            engine.z_entry = float(saved["z_entry"])
        state["active_pair_y"] = saved.get("active_pair_y", state["active_pair_y"])
        state["active_pair_x"] = saved.get("active_pair_x", state["active_pair_x"])
        state["positions"] = list(saved.get("positions", []))
        state["sim_equity"] = float(saved.get("sim_equity", SIM_START_EQUITY))
        state["realized_pnl"] = float(saved.get("realized_pnl", 0.0))
        state["wins"] = int(saved.get("wins", 0))
        state["losses"] = int(saved.get("losses", 0))
        state["trades"] = list(saved.get("trades", []))
        state["events"] = list(saved.get("events", []))
        state["unrealized_pnl"] = sum(float(p.get("unrealized_pnl", 0.0)) for p in state["positions"])
        log.info(
            f"Restored state from {STATE_FILE}: {len(state['positions'])} open pos, "
            f"{len(state['trades'])} trades, equity={state['sim_equity']:.2f}"
        )
    except Exception as e:
        log.error(f"Failed to load state from {STATE_FILE}: {e}")


def add_event(msg: str, persist: bool = True):
    ts = now_ist().strftime("%H:%M:%S IST")
    state["events"].insert(0, f"[{ts}] {msg}")
    state["events"] = state["events"][:25]
    if persist:
        save_state()


def make_exchange():
    cfg = {
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    }
    if GATE_API_KEY and GATE_API_SECRET:
        cfg["apiKey"] = GATE_API_KEY
        cfg["secret"] = GATE_API_SECRET
    return ccxt.gate(cfg)


def to_gate_contract(ccxt_symbol: str) -> str:
    base = ccxt_symbol.split("/")[0]
    return f"{base}_USDT"


def auth(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID)


def leg_unrealized_pnl(pos: dict, price_y: float, price_x: float) -> float:
    if pos["side"] == "LONG_SPREAD":
        pnl_y = (price_y - pos["entry_y"]) * pos["qty_y"]
        pnl_x = (pos["entry_x"] - price_x) * pos["qty_x"]
    else:
        pnl_y = (pos["entry_y"] - price_y) * pos["qty_y"]
        pnl_x = (price_x - pos["entry_x"]) * pos["qty_x"]
    return float(pnl_y + pnl_x)


def refresh_unrealized_pnl(ohlcv_map: dict[str, pd.DataFrame]):
    total_u = 0.0
    for pos in state["positions"]:
        py, px = pos["pair_y"], pos["pair_x"]
        if py in ohlcv_map and px in ohlcv_map:
            cy = float(ohlcv_map[py]["close"].iloc[-1])
            cx = float(ohlcv_map[px]["close"].iloc[-1])
            pos["current_y"] = cy
            pos["current_x"] = cx
            pos["unrealized_pnl"] = round(leg_unrealized_pnl(pos, cy, cx), 3)
        total_u += float(pos.get("unrealized_pnl", 0.0))
    state["unrealized_pnl"] = float(total_u)


def busy_symbols() -> set[str]:
    used = set()
    for pos in state["positions"]:
        used.add(pos["pair_y"])
        used.add(pos["pair_x"])
    return used


async def fetch_top_usdt_contracts(session: ClientSession, limit: int = 100) -> list[str]:
    url = f"{GATE_FUTURES_BASE}/tickers"
    async with session.get(url) as resp:
        data = await resp.json()
    ranked = []
    for item in data:
        contract = item.get("contract", "")
        if not contract.endswith("_USDT"):
            continue
        vol = float(item.get("volume_24h_settle") or item.get("volume_24h_quote") or 0.0)
        if vol > 0:
            base = contract[:-5]
            ranked.append((vol, f"{base}/USDT:USDT"))
    ranked.sort(reverse=True)
    return [sym for _, sym in ranked[:limit]]


async def fetch_candles_fast(session: ClientSession, symbols: list[str]) -> dict[str, pd.DataFrame]:
    sem = asyncio.Semaphore(16)
    needed = engine.lookback + 15

    async def worker(sym: str):
        contract = to_gate_contract(sym)
        cached = candle_cache.get(sym)
        fetch_limit = 6 if (cached is not None and len(cached) >= needed) else needed
        url = f"{GATE_FUTURES_BASE}/candlesticks?contract={contract}&interval={TIMEFRAME}&limit={fetch_limit}"

        async with sem:
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return
                    rows = await resp.json()
                    if not rows:
                        return
                    new_df = pd.DataFrame([
                        {
                            "ts": int(r["t"]),
                            "close": float(r["c"]),
                        }
                        for r in rows
                    ])
                    if cached is not None and fetch_limit < needed:
                        merged = pd.concat([cached, new_df], ignore_index=True)
                        merged = merged.drop_duplicates(subset=["ts"], keep="last").sort_values("ts").tail(needed)
                        candle_cache[sym] = merged.reset_index(drop=True)
                    else:
                        candle_cache[sym] = new_df.sort_values("ts").tail(needed).reset_index(drop=True)
            except Exception:
                pass

    await asyncio.gather(*(worker(s) for s in symbols))
    return {s: candle_cache[s] for s in symbols if s in candle_cache and len(candle_cache[s]) >= engine.lookback}


async def open_spread(ex, pair_y: str, pair_x: str, sig: str, m: dict) -> dict | None:
    mkt_y = ex.markets.get(pair_y, {})
    mkt_x = ex.markets.get(pair_x, {})

    min_y = float((((mkt_y.get("limits") or {}).get("amount") or {}).get("min")) or 1.0)
    min_x = float((((mkt_x.get("limits") or {}).get("amount") or {}).get("min")) or 1.0)

    raw_qy = max(NOTIONAL_USDT / max(m["price_y"], 1e-12), min_y)
    raw_qx = max((NOTIONAL_USDT * abs(m["beta"])) / max(m["price_x"], 1e-12), min_x)

    try:
        qy = float(ex.amount_to_precision(pair_y, raw_qy))
        qx = float(ex.amount_to_precision(pair_x, raw_qx))
    except Exception as e:
        log.warning(f"Precision error for {pair_y}/{pair_x}: {e}")
        return None

    if qy < min_y or qx < min_x or qy <= 0 or qx <= 0:
        log.warning(f"Skipping {pair_y}/{pair_x}: qty below min ({qy}<{min_y} or {qx}<{min_x})")
        return None

    sy, sx = ("buy", "sell") if sig == "LONG_SPREAD" else ("sell", "buy")

    if not DRY_RUN:
        await asyncio.gather(
            ex.create_market_order(pair_y, sy, qy),
            ex.create_market_order(pair_x, sx, qx),
        )

    entry_fee = (qy * m["price_y"] + qx * m["price_x"]) * TAKER_FEE_RATE
    state["sim_equity"] -= entry_fee
    state["realized_pnl"] -= entry_fee

    pos_rec = {
        "pair_y": pair_y,
        "pair_x": pair_x,
        "label": f"{pair_y.split('/')[0]}/{pair_x.split('/')[0]}",
        "side": sig,
        "side_y": sy,
        "side_x": sx,
        "qty_y": qy,
        "qty_x": qx,
        "entry_y": m["price_y"],
        "entry_x": m["price_x"],
        "current_y": m["price_y"],
        "current_x": m["price_x"],
        "entry_z": round(m["z_score"], 2),
        "current_z": round(m["z_score"], 2),
        "unrealized_pnl": 0.0,
        "entry_time": now_ist().strftime("%d-%m %H:%M IST"),
    }
    state["positions"].append(pos_rec)
    state["active_pair_y"] = pair_y
    state["active_pair_x"] = pair_x
    save_state()
    return pos_rec


async def close_single_position(ex, pos: dict, price_y: float | None = None, price_x: float | None = None, exit_z: float = 0.0, reason: str = "SIGNAL") -> dict:
    py, px = pos["pair_y"], pos["pair_x"]
    sy, sx = ("sell", "buy") if pos["side"] == "LONG_SPREAD" else ("buy", "sell")

    if price_y is None or price_x is None:
        ty, tx = await asyncio.gather(ex.fetch_ticker(py), ex.fetch_ticker(px))
        price_y = float(ty["last"])
        price_x = float(tx["last"])

    if not DRY_RUN:
        await asyncio.gather(
            ex.create_market_order(py, sy, pos["qty_y"], params={"reduceOnly": True}),
            ex.create_market_order(px, sx, pos["qty_x"], params={"reduceOnly": True}),
        )

    gross_pnl = leg_unrealized_pnl(pos, price_y, price_x)
    exit_fee = (pos["qty_y"] * price_y + pos["qty_x"] * price_x) * TAKER_FEE_RATE
    net_pnl = gross_pnl - exit_fee

    state["sim_equity"] += net_pnl
    state["realized_pnl"] += net_pnl
    if net_pnl >= 0:
        state["wins"] += 1
    else:
        state["losses"] += 1

    trade_rec = {
        "time": now_ist().strftime("%d-%m %H:%M"),
        "pair": pos["label"],
        "side": pos["side"],
        "entry_z": pos["entry_z"],
        "exit_z": round(exit_z if exit_z else pos.get("current_z", 0.0), 2),
        "pnl": round(net_pnl, 3),
        "reason": reason,
    }
    state["trades"].insert(0, trade_rec)
    state["trades"] = state["trades"][:40]

    state["positions"] = [p for p in state["positions"] if p is not pos]
    save_state()
    return trade_rec


async def close_all_positions(ex, reason: str = "MANUAL") -> list[dict]:
    closed = []
    for pos in list(state["positions"]):
        tr = await close_single_position(ex, pos, reason=reason)
        closed.append(tr)
    state["unrealized_pnl"] = 0.0
    save_state()
    return closed


# ---------------- Web Dashboard & API ----------------

DASHBOARD_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Gate.io Top-100 Stat-Arb Simulator (IST)</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }
    .wrap { max-width: 1080px; margin: 0 auto; }
    .top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; gap: 12px; flex-wrap: wrap; }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; }
    .live { background: #065f46; color: #6ee7b7; }
    .dry { background: #1e3a8a; color: #93c5fd; }
    .ready-pill { background: #065f46; color: #6ee7b7; padding: 2px 7px; border-radius: 6px; font-size: 11px; font-weight: 700; }
    .watch-pill { background: #334155; color: #cbd5e1; padding: 2px 7px; border-radius: 6px; font-size: 11px; }
    .controls { display: flex; gap: 10px; margin-bottom: 18px; flex-wrap: wrap; align-items: center; }
    button { border: 1px solid #334155; background: #1e293b; color: #f8fafc; padding: 9px 16px; border-radius: 8px; font-weight: 600; cursor: pointer; font-size: 14px; }
    button:hover:not(:disabled) { background: #334155; }
    button:disabled { opacity: 0.45; cursor: not-allowed; }
    .btn-pause { border-color: #d97706; color: #fcd34d; }
    .btn-resume { border-color: #059669; color: #6ee7b7; }
    .btn-close { border-color: #dc2626; color: #fca5a5; }
    .status-meta { margin-left: auto; display: flex; gap: 14px; align-items: center; font-family: monospace; font-size: 13px; color: #94a3b8; flex-wrap: wrap; }
    .refresh-pill { background: #1e293b; border: 1px solid #334155; color: #38bdf8; padding: 4px 10px; border-radius: 6px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .card { background: #1e293b; padding: 14px; border-radius: 10px; border: 1px solid #334155; }
    .label { font-size: 12px; color: #94a3b8; margin-bottom: 4px; }
    .val { font-size: 20px; font-weight: 700; }
    .pos-green { color: #4ade80; }
    .neg-red { color: #f87171; }
    .cols { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 18px; }
    @media (max-width: 820px) { .cols { grid-template-columns: 1fr; } .status-meta { margin-left: 0; } }
    table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 10px; overflow: hidden; font-size: 13px; border: 1px solid #334155; margin-bottom: 16px; }
    th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #334155; }
    th { color: #94a3b8; font-weight: 600; background: #162032; }
    ul { list-style: none; padding: 12px; margin: 0; background: #1e293b; border-radius: 10px; border: 1px solid #334155; font-family: monospace; font-size: 12px; max-height: 210px; overflow-y: auto; }
    li { padding: 4px 0; border-bottom: 1px solid #334155; }
    li:last-child { border-bottom: none; }
  </style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div>
      <h2 style="margin:0">Gate.io Top-100 Stat-Arb (Auto-Entry)</h2>
      <small id="pair" style="color:#94a3b8">Initializing fast scan...</small>
    </div>
    <span id="mode" class="badge dry">SIMULATOR</span>
  </div>
  <div class="controls">
    <button id="btnPause" class="btn-pause" onclick="sendAction('pause')">Pause</button>
    <button id="btnResume" class="btn-resume" onclick="sendAction('resume')">Resume</button>
    <button id="btnClose" class="btn-close" onclick="sendAction('closeall')">Close All</button>
    <div class="status-meta">
      <span id="refreshTimer" class="refresh-pill">Refresh in: 3s</span>
      <span id="istClock">IST: --:--:--</span>
    </div>
  </div>
  <div class="grid">
    <div class="card"><div class="label">Sim Equity (USDT)</div><div class="val" id="eq">1000.00</div></div>
    <div class="card"><div class="label">Realized / Unrealized</div><div class="val" id="pnl">0.00 / 0.00</div></div>
    <div class="card"><div class="label">Open Pairs</div><div class="val" id="openCount">0 / 3</div></div>
    <div class="card"><div class="label">Entry-Ready Pairs</div><div class="val" id="readyCount">0</div></div>
    <div class="card"><div class="label">Win / Loss</div><div class="val" id="wl">0 / 0</div></div>
    <div class="card"><div class="label">Scan Speed</div><div class="val" id="univ">-</div></div>
  </div>

  <h4 style="margin:0 0 8px 0;color:#94a3b8">Open Positions</h4>
  <table>
    <thead><tr><th>Pair</th><th>Side</th><th>Entry Z</th><th>Now Z</th><th>Unrealized ($)</th><th>Opened (IST)</th></tr></thead>
    <tbody id="openPos"><tr><td colspan="6">No open positions</td></tr></tbody>
  </table>

  <div class="cols">
    <div>
      <h4 style="margin:0 0 8px 0;color:#94a3b8">Top Ranked Pairs (Entry Criteria Sorted)</h4>
      <table>
        <thead><tr><th>Pair</th><th>Status</th><th>Z</th><th>Beta</th><th>ADF</th><th>HL</th></tr></thead>
        <tbody id="topPairs"><tr><td colspan="6">Scanning...</td></tr></tbody>
      </table>
    </div>
    <div>
      <h4 style="margin:0 0 8px 0;color:#94a3b8">Simulated Trade History (IST)</h4>
      <table>
        <thead><tr><th>Time (IST)</th><th>Pair</th><th>Side</th><th>Z In/Out</th><th>PnL ($)</th></tr></thead>
        <tbody id="trades"><tr><td colspan="5">No trades yet</td></tr></tbody>
      </table>
    </div>
  </div>

  <h4 style="margin:0 0 8px 0;color:#94a3b8">Recent Activity (IST)</h4>
  <ul id="events"><li>Starting fast scan...</li></ul>
</div>
<script>
const REFRESH_INTERVAL_SEC = 3;
let remainingSec = REFRESH_INTERVAL_SEC;

function fmtPnl(v) {
  const cls = v > 0 ? 'pos-green' : (v < 0 ? 'neg-red' : '');
  return `<span class="${cls}">${v >= 0 ? '+' : ''}${v.toFixed(2)}</span>`;
}

function updateClockAndTimer() {
  const istStr = new Date().toLocaleTimeString('en-IN', {
    timeZone: 'Asia/Kolkata',
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
  document.getElementById('istClock').textContent = `IST: ${istStr}`;
  document.getElementById('refreshTimer').textContent = `Refresh in: ${Math.max(0, remainingSec)}s`;
}

function applyState(s) {
  const posList = s.positions || [];
  document.getElementById('pair').textContent = `Focus: ${s.pair_y} vs ${s.pair_x} (${s.timeframe}) • ${s.scan_status}`;
  const badge = document.getElementById('mode');
  badge.textContent = s.dry_run ? 'SIMULATED PAPER' : 'LIVE TRADING';
  badge.className = 'badge ' + (s.dry_run ? 'dry' : 'live');
  document.getElementById('eq').textContent = s.sim_equity.toFixed(2);
  document.getElementById('pnl').innerHTML = `${fmtPnl(s.realized_pnl)} / ${fmtPnl(s.unrealized_pnl)}`;
  document.getElementById('openCount').textContent = `${posList.length} / ${s.max_positions}`;
  document.getElementById('readyCount').textContent = `${s.ready_count} ready`;
  document.getElementById('wl').textContent = `${s.wins} / ${s.losses}`;
  document.getElementById('univ').textContent = `${s.universe_count}c / ${s.last_scan_sec}s`;
  document.getElementById('btnPause').disabled = !s.running;
  document.getElementById('btnResume').disabled = s.running;
  document.getElementById('btnClose').disabled = posList.length === 0;

  document.getElementById('openPos').innerHTML = posList.length
    ? posList.map(p => `<tr><td><b>${p.label}</b></td><td>${p.side.replace('_SPREAD','')}</td><td>${p.entry_z}</td><td>${p.current_z}</td><td>${fmtPnl(p.unrealized_pnl)}</td><td>${p.entry_time}</td></tr>`).join('')
    : '<tr><td colspan="6">No open positions</td></tr>';

  if (s.top_pairs && s.top_pairs.length) {
    document.getElementById('topPairs').innerHTML = s.top_pairs.map(p => {
      const tag = p.entry_ready
        ? '<span class="ready-pill">ENTRY MET</span>'
        : (p.is_cointegrated ? '<span class="watch-pill">COINT</span>' : '<span class="watch-pill">WATCH</span>');
      return `<tr><td>${p.pair_y.split('/')[0]}/${p.pair_x.split('/')[0]}</td><td>${tag}</td><td>${p.z_score.toFixed(2)}</td><td>${p.beta.toFixed(2)}</td><td>${p.adf_stat.toFixed(2)}</td><td>${p.half_life.toFixed(0)}b</td></tr>`;
    }).join('');
  }
  if (s.trades && s.trades.length) {
    document.getElementById('trades').innerHTML = s.trades.map(t =>
      `<tr><td>${t.time}</td><td>${t.pair}</td><td>${t.side.replace('_SPREAD','')}</td><td>${t.entry_z} → ${t.exit_z}</td><td>${fmtPnl(t.pnl)}</td></tr>`
    ).join('');
  }
  if (s.events && s.events.length) {
    document.getElementById('events').innerHTML = s.events.map(e => `<li>${e}</li>`).join('');
  }
}

async function sendAction(action) {
  if (action === 'closeall' && !confirm('Close ALL open positions now?')) return;
  ['btnPause', 'btnResume', 'btnClose'].forEach(id => document.getElementById(id).disabled = true);
  try {
    const r = await fetch(`/api/${action}`, { method: 'POST' });
    const d = await r.json();
    if (d.state) applyState(d.state);
  } finally {
    remainingSec = REFRESH_INTERVAL_SEC;
    await refresh();
  }
}

async function refresh() {
  try {
    const r = await fetch('/api/state');
    applyState(await r.json());
  } catch (e) {}
  remainingSec = REFRESH_INTERVAL_SEC;
  updateClockAndTimer();
}

updateClockAndTimer();
refresh();
setInterval(() => {
  remainingSec = Math.max(0, remainingSec - 1);
  updateClockAndTimer();
}, 1000);
setInterval(refresh, REFRESH_INTERVAL_SEC * 1000);
</script>
</body>
</html>"""


def snapshot_state() -> dict:
    return {
        "running": state["running"],
        "persist_state": PERSIST_STATE,
        "state_file": STATE_FILE,
        "scan_status": state["scan_status"],
        "last_scan_sec": state["last_scan_sec"],
        "dry_run": DRY_RUN,
        "universe_count": state["universe_count"],
        "ready_count": state["ready_count"],
        "max_positions": MAX_POSITIONS,
        "pair_y": state["active_pair_y"],
        "pair_x": state["active_pair_x"],
        "timeframe": TIMEFRAME,
        "positions": state["positions"],
        "z_entry": engine.z_entry,
        "sim_equity": state["sim_equity"],
        "realized_pnl": state["realized_pnl"],
        "unrealized_pnl": state["unrealized_pnl"],
        "wins": state["wins"],
        "losses": state["losses"],
        "trades": state["trades"][:12],
        "top_pairs": state["top_pairs"][:10],
        "metrics": state["metrics"],
        "updated_at": state["updated_at"],
        "events": state["events"],
    }


async def handle_dashboard(request: web.Request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_health(request: web.Request):
    return web.json_response({"status": "ok"})


async def handle_api_state(request: web.Request):
    return web.json_response(snapshot_state())


async def handle_api_pause(request: web.Request):
    state["running"] = False
    add_event("Paused via Web Dashboard")
    return web.json_response({"ok": True, "state": snapshot_state()})


async def handle_api_resume(request: web.Request):
    state["running"] = True
    add_event("Resumed via Web Dashboard")
    return web.json_response({"ok": True, "state": snapshot_state()})


async def handle_api_closeall(request: web.Request):
    ex = make_exchange()
    try:
        closed = await close_all_positions(ex, reason="MANUAL_WEB")
        if closed:
            total_p = sum(t["pnl"] for t in closed)
            add_event(f"Closed {len(closed)} position(s) via Web | Net PnL: {total_p:+.2f} USDT")
        return web.json_response({"ok": True, "state": snapshot_state()})
    finally:
        await ex.close()


async def start_web():
    app = web.Application()
    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/state", handle_api_state)
    app.router.add_post("/api/pause", handle_api_pause)
    app.router.add_post("/api/resume", handle_api_resume)
    app.router.add_post("/api/closeall", handle_api_closeall)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info(f"Dashboard listening on 0.0.0.0:{PORT}")


# ---------------- Telegram Commands ----------------

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    m = state["metrics"]
    pos_lines = [
        f"• {p['label']} ({p['side']}): Z {p['entry_z']}→{p['current_z']} | uPnL {p['unrealized_pnl']:+.2f}"
        for p in state["positions"]
    ]
    pos_block = "\n".join(pos_lines) if pos_lines else "None (FLAT)"
    z_txt = f"{m['z_score']:.2f} (±{engine.z_entry:.2f}) | ADF: {m['adf_stat']:.2f}" if m else state["scan_status"]
    await update.message.reply_text(
        f"Mode: {'SIMULATOR' if DRY_RUN else 'LIVE'} | {'ON' if state['running'] else 'PAUSED'}\n"
        f"Persist: {'ON' if PERSIST_STATE else 'OFF'} ({STATE_FILE})\n"
        f"Scan: {state['universe_count']} coins in {state['last_scan_sec']}s ({state['ready_count']} entry-ready)\n"
        f"Open Pairs: {len(state['positions'])}/{MAX_POSITIONS} | Total uPnL: {state['unrealized_pnl']:+.2f} USDT\n"
        f"Focus ({state['active_pair_y'].split('/')[0]}/{state['active_pair_x'].split('/')[0]}): {z_txt}\n"
        f"Positions:\n{pos_block}"
    )


async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    total = state["wins"] + state["losses"]
    wr = (state["wins"] / total * 100.0) if total else 0.0
    await update.message.reply_text(
        f"Simulated Account Summary\n"
        f"Equity: {state['sim_equity']:.2f} USDT (Start: {SIM_START_EQUITY:.2f})\n"
        f"Realized PnL: {state['realized_pnl']:+.2f} USDT\n"
        f"Unrealized PnL: {state['unrealized_pnl']:+.2f} USDT\n"
        f"Trades: {total} ({state['wins']}W / {state['losses']}L, {wr:.1f}% WR)"
    )


async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    pairs = state["top_pairs"][:8]
    if not pairs:
        await update.message.reply_text(f"Scan status: {state['scan_status']}")
        return
    lines = [f"Top Ranked Pairs ({state['ready_count']} Entry-Ready):"]
    for p in pairs:
        y_n = p["pair_y"].split("/")[0]
        x_n = p["pair_x"].split("/")[0]
        tag = "[ENTRY]" if p.get("entry_ready") else ("[COINT]" if p.get("is_cointegrated") else "[WATCH]")
        lines.append(f"• {tag} {y_n}/{x_n}: Z={p['z_score']:+.2f} | ADF={p['adf_stat']:.2f} | HL={p['half_life']:.0f}b")
    await update.message.reply_text("\n".join(lines))


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    if not state["trades"]:
        await update.message.reply_text("No closed simulated trades yet.")
        return
    lines = ["Recent Simulated Trades (IST):"]
    for t in state["trades"][:8]:
        lines.append(f"• {t['time']} {t['pair']} {t['side']} ({t['entry_z']}→{t['exit_z']}): {t['pnl']:+.2f} USDT")
    await update.message.reply_text("\n".join(lines))


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    ex = make_exchange()
    try:
        bal = await ex.fetch_balance()
        usdt = bal.get("USDT", {})
        await update.message.reply_text(
            f"Gate.io Live USDT Futures\n"
            f"Free: {usdt.get('free') or 0:.2f} | Total: {usdt.get('total') or 0:.2f} USDT"
        )
    except Exception as e:
        await update.message.reply_text(f"Balance check failed: {e}")
    finally:
        await ex.close()


async def cmd_setz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    try:
        val = float(context.args[0])
        if val <= 0 or val >= engine.z_stop:
            await update.message.reply_text(f"Enter a value > 0 and < {engine.z_stop}.")
            return
        engine.z_entry = val
        add_event(f"Z-entry updated to ±{val:.2f}")
        await update.message.reply_text(f"Z-score entry threshold set to ±{val:.2f}")
    except (IndexError, ValueError):
        await update.message.reply_text(f"Usage: /setz 2.0 (Current: ±{engine.z_entry:.2f})")


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = False
    add_event("Paused via Telegram")
    await update.message.reply_text("Paused.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = True
    add_event("Resumed via Telegram")
    await update.message.reply_text("Resumed.")


async def cmd_closeall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    ex = make_exchange()
    try:
        closed = await close_all_positions(ex, reason="MANUAL_TG")
        if not closed:
            await update.message.reply_text("No active positions.")
            return
        total_p = sum(t["pnl"] for t in closed)
        msg = f"Closed {len(closed)} position(s) | Net PnL: {total_p:+.2f} USDT"
        add_event(msg)
        await update.message.reply_text(msg)
    finally:
        await ex.close()


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    await update.message.reply_text(
        f"Commands (Auto-selecting top coins, max {MAX_POSITIONS} pairs):\n"
        "/status - Open positions, Z-score, unrealized PnL\n"
        "/pnl - Simulated equity, realized PnL, win/loss stats\n"
        "/top - Top entry-ready & cointegrated pairs\n"
        "/trades - Recent simulated trade history (IST)\n"
        "/balance - Gate.io live USDT futures balance\n"
        "/setz <val> - Set Z-score entry threshold\n"
        "/pause | /resume | /closeall"
    )


async def cmd_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    await update.message.reply_text("Unknown command. Send /help.")


async def tg_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error(f"Telegram error: {context.error}", exc_info=context.error)


# ---------------- Fast Top-100 Auto-Selection Trading Loop ----------------

async def trader_loop(tg_app: Application):
    ex = make_exchange()
    await ex.load_markets()
    timeout = ClientTimeout(total=12)
    top_symbols: list[str] = []
    cycle = 0

    try:
        async with ClientSession(timeout=timeout) as session:
            while True:
                t0 = asyncio.get_running_loop().time()
                try:
                    if not top_symbols or cycle % 20 == 0:
                        state["scan_status"] = "Fetching Top 100 tickers..."
                        top_symbols = await fetch_top_usdt_contracts(session, TOP_N_COINS)
                        state["universe_count"] = len(top_symbols)
                    cycle += 1

                    state["scan_status"] = f"Fetching {len(top_symbols)} candles..."
                    ohlcv_map = await fetch_candles_fast(session, top_symbols)

                    state["scan_status"] = "Scoring cointegration..."
                    ranked_pairs = await asyncio.to_thread(engine.scan_universe, ohlcv_map, 8)

                    elapsed = round(asyncio.get_running_loop().time() - t0, 1)
                    state["last_scan_sec"] = elapsed
                    state["top_pairs"] = ranked_pairs[:15]
                    state["ready_count"] = sum(1 for r in ranked_pairs if r.get("entry_ready"))
                    state["updated_at"] = now_ist().isoformat()
                    state["scan_status"] = f"{'ACTIVE' if state['running'] else 'PAUSED'} • Scanned in {elapsed}s"

                    if cycle == 1:
                        add_event(
                            f"Scan started in {elapsed}s ({len(ohlcv_map)} coins, {len(state['positions'])} open pos restored)"
                        )

                    for pos in list(state["positions"]):
                        py, px = pos["pair_y"], pos["pair_x"]
                        if py in ohlcv_map and px in ohlcv_map:
                            m = engine.compute_metrics(ohlcv_map[py], ohlcv_map[px])
                            pos["current_z"] = round(m["z_score"], 2)
                            pos["current_y"] = m["price_y"]
                            pos["current_x"] = m["price_x"]
                            pos["unrealized_pnl"] = round(leg_unrealized_pnl(pos, m["price_y"], m["price_x"]), 3)

                            if state["running"] and engine.get_signal(m, pos["side"]) == "CLOSE":
                                reason = "STOP_LOSS" if abs(m["z_score"]) >= engine.z_stop else "MEAN_REVERT"
                                tr = await close_single_position(
                                    ex, pos, m["price_y"], m["price_x"], m["z_score"], reason=reason
                                )
                                msg = (
                                    f"[SIM CLOSED - {reason}] {tr['pair']} ({tr['side']}) • {tr['time']} IST\n"
                                    f"Z: {tr['entry_z']} → {tr['exit_z']} | Net PnL: {tr['pnl']:+.2f} USDT\n"
                                    f"Sim Equity: {state['sim_equity']:.2f} USDT"
                                )
                                add_event(f"CLOSED {tr['pair']} {tr['pnl']:+.2f} USDT ({reason})")
                                await tg_app.bot.send_message(ADMIN_ID, msg)

                    refresh_unrealized_pnl(ohlcv_map)

                    if state["positions"]:
                        focus = state["positions"][0]
                        py, px = focus["pair_y"], focus["pair_x"]
                        state["active_pair_y"], state["active_pair_x"] = py, px
                        if py in ohlcv_map and px in ohlcv_map:
                            state["metrics"] = engine.compute_metrics(ohlcv_map[py], ohlcv_map[px])
                    elif ranked_pairs:
                        best = ranked_pairs[0]
                        state["active_pair_y"], state["active_pair_x"] = best["pair_y"], best["pair_x"]
                        state["metrics"] = best

                    if state["running"] and len(state["positions"]) < MAX_POSITIONS:
                        used = busy_symbols()
                        for cand in ranked_pairs:
                            if len(state["positions"]) >= MAX_POSITIONS:
                                break
                            if not cand.get("entry_ready"):
                                break
                            if cand["pair_y"] in used or cand["pair_x"] in used:
                                continue
                            sig = engine.get_signal(cand, None)
                            if sig in ("LONG_SPREAD", "SHORT_SPREAD"):
                                pos_rec = await open_spread(
                                    ex, cand["pair_y"], cand["pair_x"], sig, cand
                                )
                                if not pos_rec:
                                    continue
                                used.add(cand["pair_y"])
                                used.add(cand["pair_x"])
                                state["active_pair_y"], state["active_pair_x"] = cand["pair_y"], cand["pair_x"]
                                state["metrics"] = cand
                                msg = (
                                    f"[AUTO ENTRY {len(state['positions'])}/{MAX_POSITIONS}] {sig} on {pos_rec['label']} • {pos_rec['entry_time']}\n"
                                    f"Z: {cand['z_score']:+.2f} | Beta: {cand['beta']:.3f} | ADF: {cand['adf_stat']:.2f} | HL: {cand['half_life']:.0f}b\n"
                                    f"{pos_rec['side_y'].upper()} {pos_rec['qty_y']} {cand['pair_y']} @ {cand['price_y']}\n"
                                    f"{pos_rec['side_x'].upper()} {pos_rec['qty_x']} {cand['pair_x']} @ {cand['price_x']}"
                                )
                                add_event(f"AUTO OPENED {sig} {pos_rec['label']} @ Z={cand['z_score']:+.2f}")
                                await tg_app.bot.send_message(ADMIN_ID, msg)

                except Exception as e:
                    log.error(f"Loop error: {e}", exc_info=True)
                    state["scan_status"] = f"Error: {e}"
                    add_event(f"Loop error: {e}", persist=False)

                await asyncio.sleep(POLL_SECONDS)
    finally:
        await ex.close()


async def post_init(tg_app: Application):
    await start_web()
    asyncio.create_task(trader_loop(tg_app))


def main():
    load_state()
    tg_app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    tg_app.add_handler(CommandHandler("status", cmd_status))
    tg_app.add_handler(CommandHandler("pnl", cmd_pnl))
    tg_app.add_handler(CommandHandler("top", cmd_top))
    tg_app.add_handler(CommandHandler("trades", cmd_trades))
    tg_app.add_handler(CommandHandler("balance", cmd_balance))
    tg_app.add_handler(CommandHandler("setz", cmd_setz))
    tg_app.add_handler(CommandHandler("pause", cmd_pause))
    tg_app.add_handler(CommandHandler("resume", cmd_resume))
    tg_app.add_handler(CommandHandler("closeall", cmd_closeall))
    tg_app.add_handler(CommandHandler("help", cmd_help))
    tg_app.add_handler(MessageHandler(filters.COMMAND, cmd_unknown))
    tg_app.add_error_handler(tg_error_handler)
    tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
