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

GATE_API_KEY = os.getenv("GATE_API_KEY")
GATE_API_SECRET = os.getenv("GATE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "3000"))

PAIR_Y = os.getenv("PAIR_Y", "ETH/USDT:USDT")
PAIR_X = os.getenv("PAIR_X", "BTC/USDT:USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
NOTIONAL_USDT = float(os.getenv("NOTIONAL_USDT", "100"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

engine = StatArbEngine()
state = {
    "running": True,
    "position": None,
    "qty_y": 0.0,
    "qty_x": 0.0,
    "metrics": {},
    "updated_at": None,
    "events": [],
}


def add_event(msg: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    state["events"].insert(0, f"[{ts}] {msg}")
    state["events"] = state["events"][:20]


def make_exchange():
    return ccxt.gate({
        "apiKey": GATE_API_KEY,
        "secret": GATE_API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })


def auth(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID)


async def fetch_df(ex, symbol):
    rows = await ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=engine.lookback + 10)
    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])


async def close_spread(ex):
    if not state["position"]:
        return
    sy, sx = ("sell", "buy") if state["position"] == "LONG_SPREAD" else ("buy", "sell")
    if not DRY_RUN:
        await asyncio.gather(
            ex.create_market_order(PAIR_Y, sy, state["qty_y"], params={"reduceOnly": True}),
            ex.create_market_order(PAIR_X, sx, state["qty_x"], params={"reduceOnly": True}),
        )
    state.update({"position": None, "qty_y": 0.0, "qty_x": 0.0})


# ---------------- Web Dashboard & API ----------------

DASHBOARD_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Gate.io Stat-Arb Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }
    .wrap { max-width: 900px; margin: 0 auto; }
    .top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; }
    .badge { padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; }
    .live { background: #065f46; color: #6ee7b7; }
    .dry { background: #78350f; color: #fcd34d; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .card { background: #1e293b; padding: 14px; border-radius: 10px; border: 1px solid #334155; }
    .label { font-size: 12px; color: #94a3b8; margin-bottom: 4px; }
    .val { font-size: 22px; font-weight: 700; }
    svg { width: 100%; height: 140px; background: #1e293b; border-radius: 10px; border: 1px solid #334155; margin-bottom: 18px; }
    ul { list-style: none; padding: 12px; margin: 0; background: #1e293b; border-radius: 10px; border: 1px solid #334155; font-family: monospace; font-size: 13px; }
    li { padding: 4px 0; border-bottom: 1px solid #334155; }
    li:last-child { border-bottom: none; }
  </style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div>
      <h2 style="margin:0">Gate.io Stat-Arb</h2>
      <small id="pair" style="color:#94a3b8">Loading...</small>
    </div>
    <span id="mode" class="badge dry">LOADING</span>
  </div>
  <div class="grid">
    <div class="card"><div class="label">Z-Score</div><div class="val" id="z">-</div></div>
    <div class="card"><div class="label">Position</div><div class="val" id="pos">FLAT</div></div>
    <div class="card"><div class="label">Hedge Beta</div><div class="val" id="beta">-</div></div>
    <div class="card"><div class="label">Entry Threshold</div><div class="val" id="zentry">-</div></div>
  </div>
  <svg id="chart" viewBox="0 0 600 140" preserveAspectRatio="none"></svg>
  <h4 style="margin:0 0 8px 0;color:#94a3b8">Recent Activity</h4>
  <ul id="events"><li>Waiting for first scan...</li></ul>
</div>
<script>
function drawChart(pts, entry) {
  const svg = document.getElementById('chart');
  if (!pts || pts.length < 2) return;
  const maxZ = Math.max(3.5, entry + 0.5, ...pts.map(Math.abs));
  const toY = z => 70 - (z / maxZ) * 60;
  const step = 600 / (pts.length - 1);
  const d = pts.map((z, i) => `${i === 0 ? 'M' : 'L'}${(i * step).toFixed(1)},${toY(z).toFixed(1)}`).join(' ');
  svg.innerHTML = `
    <line x1="0" y1="70" x2="600" y2="70" stroke="#475569" stroke-dasharray="4" />
    <line x1="0" y1="${toY(entry)}" x2="600" y2="${toY(entry)}" stroke="#ef4444" stroke-dasharray="3" />
    <line x1="0" y1="${toY(-entry)}" x2="600" y2="${toY(-entry)}" stroke="#22c55e" stroke-dasharray="3" />
    <path d="${d}" fill="none" stroke="#38bdf8" stroke-width="2.5" />
  `;
}
async function refresh() {
  try {
    const r = await fetch('/api/state');
    const s = await r.json();
    document.getElementById('pair').textContent = `${s.pair_y} vs ${s.pair_x} (${s.timeframe}) • ${s.running ? 'ACTIVE' : 'PAUSED'}`;
    const badge = document.getElementById('mode');
    badge.textContent = s.dry_run ? 'DRY RUN' : 'LIVE';
    badge.className = 'badge ' + (s.dry_run ? 'dry' : 'live');
    document.getElementById('pos').textContent = s.position || 'FLAT';
    document.getElementById('zentry').textContent = '±' + s.z_entry.toFixed(2);
    if (s.metrics && s.metrics.z_score !== undefined) {
      document.getElementById('z').textContent = s.metrics.z_score.toFixed(3);
      document.getElementById('beta').textContent = s.metrics.beta.toFixed(4);
      drawChart(s.metrics.z_history, s.z_entry);
    }
    if (s.events.length) {
      document.getElementById('events').innerHTML = s.events.map(e => `<li>${e}</li>`).join('');
    }
  } catch (e) {}
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


async def handle_dashboard(request: web.Request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_health(request: web.Request):
    return web.json_response({"status": "ok"})


async def handle_api_state(request: web.Request):
    return web.json_response({
        "running": state["running"],
        "dry_run": DRY_RUN,
        "pair_y": PAIR_Y,
        "pair_x": PAIR_X,
        "timeframe": TIMEFRAME,
        "position": state["position"],
        "z_entry": engine.z_entry,
        "metrics": state["metrics"],
        "updated_at": state["updated_at"],
        "events": state["events"],
    })


async def start_web():
    app = web.Application()
    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/state", handle_api_state)
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
        await update.message.reply_text("Starting up, try again in a moment.")
        return
    await update.message.reply_text(
        f"Mode: {'DRY' if DRY_RUN else 'LIVE'} | {'ON' if state['running'] else 'PAUSED'}\n"
        f"Pair: {PAIR_Y} vs {PAIR_X} ({TIMEFRAME})\n"
        f"Pos: {state['position'] or 'FLAT'}\n"
        f"Z: {m['z_score']:.2f} (Entry: ±{engine.z_entry:.2f}) | Beta: {m['beta']:.3f}"
    )


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    ex = make_exchange()
    try:
        bal = await ex.fetch_balance()
        usdt = bal.get("USDT", {})
        await update.message.reply_text(
            f"Gate.io USDT Futures Balance\n"
            f"Free: {usdt.get('free') or 0:.2f} USDT\n"
            f"Used: {usdt.get('used') or 0:.2f} USDT\n"
            f"Total: {usdt.get('total') or 0:.2f} USDT"
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
            await update.message.reply_text(f"Enter a value greater than 0 and less than {engine.z_stop}.")
            return
        engine.z_entry = val
        add_event(f"Z-entry updated to ±{val:.2f}")
        await update.message.reply_text(f"Z-score entry threshold set to ±{val:.2f}")
    except (IndexError, ValueError):
        await update.message.reply_text(f"Usage: /setz 2.2 (Current: ±{engine.z_entry:.2f})")


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = False
    add_event("Engine paused via Telegram")
    await update.message.reply_text("Paused.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = True
    add_event("Engine resumed via Telegram")
    await update.message.reply_text("Resumed.")


async def cmd_closeall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    ex = make_exchange()
    try:
        await close_spread(ex)
        add_event("Emergency /closeall executed")
        await update.message.reply_text("Closed all legs.")
    finally:
        await ex.close()


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    await update.message.reply_text(
        "Commands:\n"
        "/status - Z-score, entry threshold, beta, position\n"
        "/balance - Gate.io USDT futures balance\n"
        "/setz <val> - Set Z-score entry threshold\n"
        "/pause - Pause new entries\n"
        "/resume - Resume trading loop\n"
        "/closeall - Close active legs\n"
        "/help - Show commands"
    )


async def cmd_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    await update.message.reply_text("Unknown command. Send /help.")


async def tg_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error(f"Telegram error: {context.error}", exc_info=context.error)


# ---------------- Trading Loop ----------------

async def trader_loop(tg_app: Application):
    ex = make_exchange()
    await ex.load_markets()
    add_event(f"Connected to Gate.io ({PAIR_Y} / {PAIR_X})")
    try:
        while True:
            try:
                df_y, df_x = await asyncio.gather(fetch_df(ex, PAIR_Y), fetch_df(ex, PAIR_X))
                m = engine.compute_metrics(df_y, df_x)
                state["metrics"] = m
                state["updated_at"] = datetime.now(timezone.utc).isoformat()

                if state["running"]:
                    sig = engine.get_signal(m, state["position"])
                    if sig == "CLOSE" and state["position"]:
                        prev = state["position"]
                        await close_spread(ex)
                        msg = f"CLOSED {prev} @ Z={m['z_score']:.2f}"
                        add_event(msg)
                        await tg_app.bot.send_message(ADMIN_ID, msg)
                    elif sig in ("LONG_SPREAD", "SHORT_SPREAD") and not state["position"]:
                        qy = float(ex.amount_to_precision(PAIR_Y, NOTIONAL_USDT / m["price_y"]))
                        qx = float(ex.amount_to_precision(PAIR_X, (NOTIONAL_USDT * abs(m["beta"])) / m["price_x"]))
                        sy, sx = ("buy", "sell") if sig == "LONG_SPREAD" else ("sell", "buy")
                        if not DRY_RUN:
                            await asyncio.gather(
                                ex.create_market_order(PAIR_Y, sy, qy),
                                ex.create_market_order(PAIR_X, sx, qx),
                            )
                        state.update({"position": sig, "qty_y": qy, "qty_x": qx})
                        msg = f"OPENED {sig} | Z={m['z_score']:.2f} | {sy} {qy} {PAIR_Y} / {sx} {qx} {PAIR_X}"
                        add_event(msg)
                        await tg_app.bot.send_message(ADMIN_ID, msg)
            except Exception as e:
                log.error(f"Loop error: {e}")
                add_event(f"Error: {e}")
            await asyncio.sleep(POLL_SECONDS)
    finally:
        await ex.close()


async def post_init(tg_app: Application):
    await start_web()
    asyncio.create_task(trader_loop(tg_app))


def main():
    tg_app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    tg_app.add_handler(CommandHandler("status", cmd_status))
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
