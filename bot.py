import os
import asyncio
import logging
from datetime import datetime, timezone
import pandas as pd
import ccxt.async_support as ccxt
from aiohttp import web
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from strategy import StatArbEngine

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger("GateArb")

GATE_API_KEY = os.getenv("GATE_API_KEY", "")
GATE_API_SECRET = os.getenv("GATE_API_SECRET", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "3000"))

TOP_N_COINS = int(os.getenv("TOP_N_COINS", "100"))
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
NOTIONAL_USDT = float(os.getenv("NOTIONAL_USDT", "100"))
SIM_START_EQUITY = float(os.getenv("SIM_START_EQUITY", "1000"))
TAKER_FEE_RATE = float(os.getenv("TAKER_FEE_RATE", "0.0005"))  # 0.05% Gate.io futures taker
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "45"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

engine = StatArbEngine(
    lookback=int(os.getenv("LOOKBACK", "100")),
    z_entry=float(os.getenv("Z_ENTRY", "2.0")),
    z_exit=float(os.getenv("Z_EXIT", "0.2")),
    z_stop=float(os.getenv("Z_STOP", "3.8")),
)

state = {
    "running": True,
    "universe_count": 0,
    "active_pair_y": "ETH/USDT:USDT",
    "active_pair_x": "BTC/USDT:USDT",
    "position": None,  # None | 'LONG_SPREAD' | 'SHORT_SPREAD'
    "qty_y": 0.0,
    "qty_x": 0.0,
    "entry_y": 0.0,
    "entry_x": 0.0,
    "entry_z": 0.0,
    "entry_time": None,
    "unrealized_pnl": 0.0,
    "sim_equity": SIM_START_EQUITY,
    "realized_pnl": 0.0,
    "wins": 0,
    "losses": 0,
    "trades": [],      # Completed simulated/live trades
    "top_pairs": [],   # Top screened cointegrated pairs
    "metrics": {},
    "updated_at": None,
    "events": [],
}


def add_event(msg: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    state["events"].insert(0, f"[{ts}] {msg}")
    state["events"] = state["events"][:25]


def make_exchange():
    cfg = {
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    }
    if GATE_API_KEY and GATE_API_SECRET:
        cfg["apiKey"] = GATE_API_KEY
        cfg["secret"] = GATE_API_SECRET
    return ccxt.gate(cfg)


def auth(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID)


def compute_unrealized_pnl(price_y: float, price_x: float) -> float:
    if not state["position"]:
        return 0.0
    if state["position"] == "LONG_SPREAD":
        # Long Y, Short X
        pnl_y = (price_y - state["entry_y"]) * state["qty_y"]
        pnl_x = (state["entry_x"] - price_x) * state["qty_x"]
    else:
        # Short Y, Long X
        pnl_y = (state["entry_y"] - price_y) * state["qty_y"]
        pnl_x = (price_x - state["entry_x"]) * state["qty_x"]
    return float(pnl_y + pnl_x)


async def get_top_usdt_swaps(ex, limit: int = 100) -> list[str]:
    tickers = await ex.fetch_tickers()
    ranked = []
    for sym, t in tickers.items():
        mkt = ex.markets.get(sym)
        if not mkt or not mkt.get("swap") or mkt.get("quote") != "USDT" or not mkt.get("active", True):
            continue
        vol = float(t.get("quoteVolume") or 0.0)
        if vol > 0:
            ranked.append((vol, sym))
    ranked.sort(reverse=True)
    return [sym for _, sym in ranked[:limit]]


async def fetch_ohlcv_universe(ex, symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Fetches OHLCV for top 100 symbols in controlled concurrent batches."""
    sem = asyncio.Semaphore(10)
    out: dict[str, pd.DataFrame] = {}

    async def worker(sym: str):
        async with sem:
            try:
                rows = await ex.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=engine.lookback + 10)
                if rows and len(rows) >= engine.lookback:
                    out[sym] = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
            except Exception:
                pass

    await asyncio.gather(*(worker(s) for s in symbols))
    return out


async def open_spread(ex, pair_y: str, pair_x: str, sig: str, m: dict):
    qy = float(ex.amount_to_precision(pair_y, NOTIONAL_USDT / m["price_y"]))
    qx = float(ex.amount_to_precision(pair_x, (NOTIONAL_USDT * abs(m["beta"])) / m["price_x"]))
    sy, sx = ("buy", "sell") if sig == "LONG_SPREAD" else ("sell", "buy")

    if not DRY_RUN:
        await asyncio.gather(
            ex.create_market_order(pair_y, sy, qy),
            ex.create_market_order(pair_x, sx, qx),
        )

    entry_fee = (qy * m["price_y"] + qx * m["price_x"]) * TAKER_FEE_RATE
    state["sim_equity"] -= entry_fee
    state["realized_pnl"] -= entry_fee

    state.update({
        "active_pair_y": pair_y,
        "active_pair_x": pair_x,
        "position": sig,
        "qty_y": qy,
        "qty_x": qx,
        "entry_y": m["price_y"],
        "entry_x": m["price_x"],
        "entry_z": m["z_score"],
        "entry_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "unrealized_pnl": 0.0,
    })
    return sy, sx, qy, qx


async def close_spread(ex, price_y: float | None = None, price_x: float | None = None, exit_z: float = 0.0, reason: str = "SIGNAL"):
    if not state["position"]:
        return None
    py = state["active_pair_y"]
    px = state["active_pair_x"]
    sy, sx = ("sell", "buy") if state["position"] == "LONG_SPREAD" else ("buy", "sell")

    if price_y is None or price_x is None:
        ty, tx = await asyncio.gather(ex.fetch_ticker(py), ex.fetch_ticker(px))
        price_y = float(ty["last"])
        price_x = float(tx["last"])

    if not DRY_RUN:
        await asyncio.gather(
            ex.create_market_order(py, sy, state["qty_y"], params={"reduceOnly": True}),
            ex.create_market_order(px, sx, state["qty_x"], params={"reduceOnly": True}),
        )

    gross_pnl = compute_unrealized_pnl(price_y, price_x)
    exit_fee = (state["qty_y"] * price_y + state["qty_x"] * price_x) * TAKER_FEE_RATE
    net_pnl = gross_pnl - exit_fee

    state["sim_equity"] += net_pnl
    state["realized_pnl"] += net_pnl
    if net_pnl >= 0:
        state["wins"] += 1
    else:
        state["losses"] += 1

    trade_rec = {
        "time": datetime.now(timezone.utc).strftime("%m-%d %H:%M"),
        "pair": f"{py.split('/')[0]}/{px.split('/')[0]}",
        "side": state["position"],
        "entry_z": round(state["entry_z"], 2),
        "exit_z": round(exit_z, 2),
        "pnl": round(net_pnl, 3),
        "reason": reason,
    }
    state["trades"].insert(0, trade_rec)
    state["trades"] = state["trades"][:30]

    state.update({
        "position": None,
        "qty_y": 0.0,
        "qty_x": 0.0,
        "entry_y": 0.0,
        "entry_x": 0.0,
        "entry_z": 0.0,
        "entry_time": None,
        "unrealized_pnl": 0.0,
    })
    return trade_rec


# ---------------- Web Dashboard & API ----------------

DASHBOARD_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Gate.io Top-100 Stat-Arb Simulator</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }
    .wrap { max-width: 1020px; margin: 0 auto; }
    .top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; gap: 12px; flex-wrap: wrap; }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; }
    .live { background: #065f46; color: #6ee7b7; }
    .dry { background: #1e3a8a; color: #93c5fd; }
    .controls { display: flex; gap: 10px; margin-bottom: 18px; flex-wrap: wrap; }
    button { border: 1px solid #334155; background: #1e293b; color: #f8fafc; padding: 9px 16px; border-radius: 8px; font-weight: 600; cursor: pointer; font-size: 14px; }
    button:hover:not(:disabled) { background: #334155; }
    button:disabled { opacity: 0.45; cursor: not-allowed; }
    .btn-pause { border-color: #d97706; color: #fcd34d; }
    .btn-resume { border-color: #059669; color: #6ee7b7; }
    .btn-close { border-color: #dc2626; color: #fca5a5; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .card { background: #1e293b; padding: 14px; border-radius: 10px; border: 1px solid #334155; }
    .label { font-size: 12px; color: #94a3b8; margin-bottom: 4px; }
    .val { font-size: 20px; font-weight: 700; }
    .pos-green { color: #4ade80; }
    .neg-red { color: #f87171; }
    svg { width: 100%; height: 130px; background: #1e293b; border-radius: 10px; border: 1px solid #334155; margin-bottom: 18px; }
    .cols { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 18px; }
    @media (max-width: 760px) { .cols { grid-template-columns: 1fr; } }
    table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 10px; overflow: hidden; font-size: 13px; border: 1px solid #334155; }
    th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #334155; }
    th { color: #94a3b8; font-weight: 600; background: #162032; }
    ul { list-style: none; padding: 12px; margin: 0; background: #1e293b; border-radius: 10px; border: 1px solid #334155; font-family: monospace; font-size: 12px; max-height: 220px; overflow-y: auto; }
    li { padding: 4px 0; border-bottom: 1px solid #334155; }
    li:last-child { border-bottom: none; }
  </style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div>
      <h2 style="margin:0">Gate.io Top-100 Stat-Arb</h2>
      <small id="pair" style="color:#94a3b8">Scanning top 100 coins...</small>
    </div>
    <span id="mode" class="badge dry">SIMULATOR</span>
  </div>
  <div class="controls">
    <button id="btnPause" class="btn-pause" onclick="sendAction('pause')">Pause</button>
    <button id="btnResume" class="btn-resume" onclick="sendAction('resume')">Resume</button>
    <button id="btnClose" class="btn-close" onclick="sendAction('closeall')">Close All</button>
  </div>
  <div class="grid">
    <div class="card"><div class="label">Sim Equity (USDT)</div><div class="val" id="eq">1000.00</div></div>
    <div class="card"><div class="label">Realized / Unrealized</div><div class="val" id="pnl">0.00 / 0.00</div></div>
    <div class="card"><div class="label">Active Z-Score</div><div class="val" id="z">-</div></div>
    <div class="card"><div class="label">Position</div><div class="val" id="pos">FLAT</div></div>
    <div class="card"><div class="label">Win / Loss</div><div class="val" id="wl">0 / 0</div></div>
    <div class="card"><div class="label">Universe Scanned</div><div class="val" id="univ">0</div></div>
  </div>
  <svg id="chart" viewBox="0 0 600 130" preserveAspectRatio="none"></svg>

  <div class="cols">
    <div>
      <h4 style="margin:0 0 8px 0;color:#94a3b8">Top Cointegrated Pairs (Top 100 Scan)</h4>
      <table>
        <thead><tr><th>Pair</th><th>Z</th><th>Beta</th><th>ADF</th><th>HL</th></tr></thead>
        <tbody id="topPairs"><tr><td colspan="5">Scanning...</td></tr></tbody>
      </table>
    </div>
    <div>
      <h4 style="margin:0 0 8px 0;color:#94a3b8">Simulated Trade History</h4>
      <table>
        <thead><tr><th>Time</th><th>Pair</th><th>Side</th><th>Z In/Out</th><th>PnL ($)</th></tr></thead>
        <tbody id="trades"><tr><td colspan="5">No trades yet</td></tr></tbody>
      </table>
    </div>
  </div>

  <h4 style="margin:0 0 8px 0;color:#94a3b8">Recent Activity</h4>
  <ul id="events"><li>Initializing top-100 market scan...</li></ul>
</div>
<script>
function fmtPnl(v) {
  const cls = v > 0 ? 'pos-green' : (v < 0 ? 'neg-red' : '');
  return `<span class="${cls}">${v >= 0 ? '+' : ''}${v.toFixed(2)}</span>`;
}
function drawChart(pts, entry) {
  const svg = document.getElementById('chart');
  if (!pts || pts.length < 2) return;
  const maxZ = Math.max(3.5, entry + 0.5, ...pts.map(Math.abs));
  const toY = z => 65 - (z / maxZ) * 55;
  const step = 600 / (pts.length - 1);
  const d = pts.map((z, i) => `${i === 0 ? 'M' : 'L'}${(i * step).toFixed(1)},${toY(z).toFixed(1)}`).join(' ');
  svg.innerHTML = `
    <line x1="0" y1="65" x2="600" y2="65" stroke="#475569" stroke-dasharray="4" />
    <line x1="0" y1="${toY(entry)}" x2="600" y2="${toY(entry)}" stroke="#ef4444" stroke-dasharray="3" />
    <line x1="0" y1="${toY(-entry)}" x2="600" y2="${toY(-entry)}" stroke="#22c55e" stroke-dasharray="3" />
    <path d="${d}" fill="none" stroke="#38bdf8" stroke-width="2.5" />
  `;
}
function applyState(s) {
  document.getElementById('pair').textContent = `Active: ${s.pair_y} vs ${s.pair_x} (${s.timeframe}) • ${s.running ? 'RUNNING' : 'PAUSED'}`;
  const badge = document.getElementById('mode');
  badge.textContent = s.dry_run ? 'SIMULATED PAPER' : 'LIVE TRADING';
  badge.className = 'badge ' + (s.dry_run ? 'dry' : 'live');
  document.getElementById('eq').textContent = s.sim_equity.toFixed(2);
  document.getElementById('pnl').innerHTML = `${fmtPnl(s.realized_pnl)} / ${fmtPnl(s.unrealized_pnl)}`;
  document.getElementById('pos').textContent = s.position || 'FLAT';
  document.getElementById('wl').textContent = `${s.wins} / ${s.losses}`;
  document.getElementById('univ').textContent = `${s.universe_count} coins`;
  document.getElementById('btnPause').disabled = !s.running;
  document.getElementById('btnResume').disabled = s.running;
  document.getElementById('btnClose').disabled = !s.position;

  if (s.metrics && s.metrics.z_score !== undefined) {
    document.getElementById('z').textContent = `${s.metrics.z_score.toFixed(2)} (±${s.z_entry.toFixed(1)})`;
    drawChart(s.metrics.z_history, s.z_entry);
  }
  if (s.top_pairs && s.top_pairs.length) {
    document.getElementById('topPairs').innerHTML = s.top_pairs.map(p =>
      `<tr><td>${p.pair_y.split('/')[0]}/${p.pair_x.split('/')[0]}</td><td>${p.z_score.toFixed(2)}</td><td>${p.beta.toFixed(2)}</td><td>${p.adf_stat.toFixed(2)}</td><td>${p.half_life.toFixed(0)}b</td></tr>`
    ).join('');
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
  if (action === 'closeall' && !confirm('Close active simulated/live legs now?')) return;
  ['btnPause', 'btnResume', 'btnClose'].forEach(id => document.getElementById(id).disabled = true);
  try {
    const r = await fetch(`/api/${action}`, { method: 'POST' });
    const d = await r.json();
    if (d.state) applyState(d.state);
  } finally {
    await refresh();
  }
}
async function refresh() {
  try {
    const r = await fetch('/api/state');
    applyState(await r.json());
  } catch (e) {}
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


def snapshot_state() -> dict:
    return {
        "running": state["running"],
        "dry_run": DRY_RUN,
        "universe_count": state["universe_count"],
        "pair_y": state["active_pair_y"],
        "pair_x": state["active_pair_x"],
        "timeframe": TIMEFRAME,
        "position": state["position"],
        "z_entry": engine.z_entry,
        "sim_equity": state["sim_equity"],
        "realized_pnl": state["realized_pnl"],
        "unrealized_pnl": state["unrealized_pnl"],
        "wins": state["wins"],
        "losses": state["losses"],
        "trades": state["trades"][:10],
        "top_pairs": state["top_pairs"][:8],
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
        tr = await close_spread(ex, reason="MANUAL_WEB")
        if tr:
            add_event(f"Manual Web Close {tr['pair']} | Net PnL: {tr['pnl']:+.2f} USDT")
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
    if not m:
        await update.message.reply_text("Scanning top 100 Gate.io coins, try again in a few seconds...")
        return
    await update.message.reply_text(
        f"Mode: {'SIMULATOR' if DRY_RUN else 'LIVE'} | {'ON' if state['running'] else 'PAUSED'}\n"
        f"Universe: {state['universe_count']} coins ({TIMEFRAME})\n"
        f"Active: {state['active_pair_y']} vs {state['active_pair_x']}\n"
        f"Pos: {state['position'] or 'FLAT'} | Unrealized: {state['unrealized_pnl']:+.2f} USDT\n"
        f"Z: {m['z_score']:.2f} (Entry: ±{engine.z_entry:.2f}) | Beta: {m['beta']:.3f} | ADF: {m['adf_stat']:.2f}"
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
    pairs = state["top_pairs"][:6]
    if not pairs:
        await update.message.reply_text("Top-100 scan still running...")
        return
    lines = ["Top Cointegrated Pairs (Top 100 Scan):"]
    for p in pairs:
        y_n = p["pair_y"].split("/")[0]
        x_n = p["pair_x"].split("/")[0]
        lines.append(f"• {y_n}/{x_n}: Z={p['z_score']:+.2f} | ADF={p['adf_stat']:.2f} | HL={p['half_life']:.0f}b")
    await update.message.reply_text("\n".join(lines))


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    if not state["trades"]:
        await update.message.reply_text("No closed simulated trades yet.")
        return
    lines = ["Recent Simulated Trades:"]
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
        tr = await close_spread(ex, reason="MANUAL_TG")
        msg = f"Closed {tr['pair']} | Net PnL: {tr['pnl']:+.2f} USDT" if tr else "No active position."
        add_event(msg)
        await update.message.reply_text(msg)
    finally:
        await ex.close()


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    await update.message.reply_text(
        "Commands:\n"
        "/status - Active pair, Z-score, beta, unrealized PnL\n"
        "/pnl - Simulated equity, realized PnL, win/loss stats\n"
        "/top - Top cointegrated pairs from Top-100 scan\n"
        "/trades - Recent simulated trade history\n"
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


# ---------------- Top-100 Trading & Simulation Loop ----------------

async def trader_loop(tg_app: Application):
    ex = make_exchange()
    await ex.load_markets()
    top_symbols = await get_top_usdt_swaps(ex, TOP_N_COINS)
    state["universe_count"] = len(top_symbols)
    add_event(f"Loaded Top {len(top_symbols)} Gate.io USDT Perpetuals")
    cycle = 0

    try:
        while True:
            try:
                # Refresh top 100 liquidity ranking every 20 cycles
                if cycle > 0 and cycle % 20 == 0:
                    top_symbols = await get_top_usdt_swaps(ex, TOP_N_COINS)
                    state["universe_count"] = len(top_symbols)
                cycle += 1

                ohlcv_map = await fetch_ohlcv_universe(ex, top_symbols)
                ranked_pairs = engine.scan_universe(ohlcv_map, anchor_count=8)
                state["top_pairs"] = ranked_pairs[:15]
                state["updated_at"] = datetime.now(timezone.utc).isoformat()

                # 1. Manage existing position first
                if state["position"]:
                    py, px = state["active_pair_y"], state["active_pair_x"]
                    if py in ohlcv_map and px in ohlcv_map:
                        m = engine.compute_metrics(ohlcv_map[py], ohlcv_map[px])
                        state["metrics"] = m
                        state["unrealized_pnl"] = compute_unrealized_pnl(m["price_y"], m["price_x"])

                        if state["running"]:
                            sig = engine.get_signal(m, state["position"])
                            if sig == "CLOSE":
                                reason = "STOP_LOSS" if abs(m["z_score"]) >= engine.z_stop else "MEAN_REVERT"
                                tr = await close_spread(ex, m["price_y"], m["price_x"], m["z_score"], reason=reason)
                                msg = (
                                    f"[SIM CLOSED - {reason}] {tr['pair']} ({tr['side']})\n"
                                    f"Z: {tr['entry_z']} → {tr['exit_z']} | Net PnL: {tr['pnl']:+.2f} USDT\n"
                                    f"Sim Equity: {state['sim_equity']:.2f} USDT"
                                )
                                add_event(f"CLOSED {tr['pair']} {tr['pnl']:+.2f} USDT ({reason})")
                                await tg_app.bot.send_message(ADMIN_ID, msg)

                # 2. Scan for best new simulated entry if flat
                else:
                    best = ranked_pairs[0] if ranked_pairs else None
                    if best:
                        state["active_pair_y"] = best["pair_y"]
                        state["active_pair_x"] = best["pair_x"]
                        state["metrics"] = best

                    if state["running"]:
                        for cand in ranked_pairs:
                            sig = engine.get_signal(cand, None)
                            if sig in ("LONG_SPREAD", "SHORT_SPREAD"):
                                sy, sx, qy, qx = await open_spread(
                                    ex, cand["pair_y"], cand["pair_x"], sig, cand
                                )
                                state["metrics"] = cand
                                pair_lbl = f"{cand['pair_y'].split('/')[0]}/{cand['pair_x'].split('/')[0]}"
                                msg = (
                                    f"[SIM OPENED] {sig} on {pair_lbl}\n"
                                    f"Z: {cand['z_score']:+.2f} | Beta: {cand['beta']:.3f} | ADF: {cand['adf_stat']:.2f}\n"
                                    f"{sy.upper()} {qy} {cand['pair_y']} @ {cand['price_y']}\n"
                                    f"{sx.upper()} {qx} {cand['pair_x']} @ {cand['price_x']}"
                                )
                                add_event(f"OPENED {sig} {pair_lbl} @ Z={cand['z_score']:+.2f}")
                                await tg_app.bot.send_message(ADMIN_ID, msg)
                                break

            except Exception as e:
                log.error(f"Loop error: {e}", exc_info=True)
                add_event(f"Loop error: {e}")

            await asyncio.sleep(POLL_SECONDS)
    finally:
        await ex.close()


async def post_init(tg_app: Application):
    await start_web()
    asyncio.create_task(trader_loop(tg_app))


def main():
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
