import os
import asyncio
import logging
import pandas as pd
import ccxt.async_support as ccxt
from aiohttp import web
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from strategy import StatArbEngine

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger("GateArb")

GATE_API_KEY = os.getenv("GATE_API_KEY")
GATE_API_SECRET = os.getenv("GATE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "10000"))

PAIR_Y = os.getenv("PAIR_Y", "ETH/USDT:USDT")
PAIR_X = os.getenv("PAIR_X", "BTC/USDT:USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
NOTIONAL_USDT = float(os.getenv("NOTIONAL_USDT", "100"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

engine = StatArbEngine()
state = {"running": True, "position": None, "qty_y": 0.0, "qty_x": 0.0, "metrics": {}}


def make_exchange():
    return ccxt.gateio({
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
        f"Z: {m['z_score']:.2f} | Beta: {m['beta']:.3f} | ADF: {m['adf_stat']:.2f}"
    )


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    ex = make_exchange()
    try:
        bal = await ex.fetch_balance()
        usdt = bal.get("USDT", {})
        free_amt = usdt.get("free") or 0.0
        used_amt = usdt.get("used") or 0.0
        total_amt = usdt.get("total") or 0.0
        await update.message.reply_text(
            f"Gate.io USDT Futures Balance\n"
            f"Free: {free_amt:.2f} USDT\n"
            f"Used: {used_amt:.2f} USDT\n"
            f"Total: {total_amt:.2f} USDT"
        )
    except Exception as e:
        await update.message.reply_text(f"Balance check failed: {e}")
    finally:
        await ex.close()


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = False
    await update.message.reply_text("Paused.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = True
    await update.message.reply_text("Resumed.")


async def cmd_closeall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    ex = make_exchange()
    try:
        await close_spread(ex)
        await update.message.reply_text("Closed all legs.")
    finally:
        await ex.close()


async def start_web():
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="OK"))
    app.router.add_get("/health", lambda r: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def trader_loop(tg_app: Application):
    ex = make_exchange()
    await ex.load_markets()
    try:
        while True:
            try:
                df_y, df_x = await asyncio.gather(fetch_df(ex, PAIR_Y), fetch_df(ex, PAIR_X))
                m = engine.compute_metrics(df_y, df_x)
                state["metrics"] = m

                if state["running"]:
                    sig = engine.get_signal(m, state["position"])
                    if sig == "CLOSE" and state["position"]:
                        prev = state["position"]
                        await close_spread(ex)
                        await tg_app.bot.send_message(ADMIN_ID, f"CLOSED {prev} @ Z={m['z_score']:.2f}")
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
                        await tg_app.bot.send_message(
                            ADMIN_ID, f"OPENED {sig} | Z={m['z_score']:.2f} | {sy} {qy} {PAIR_Y} / {sx} {qx} {PAIR_X}"
                        )
            except Exception as e:
                log.error(f"Loop error: {e}")
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
    tg_app.add_handler(CommandHandler("pause", cmd_pause))
    tg_app.add_handler(CommandHandler("resume", cmd_resume))
    tg_app.add_handler(CommandHandler("closeall", cmd_closeall))
    tg_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
