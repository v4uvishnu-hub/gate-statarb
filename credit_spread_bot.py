import os
import json
import math
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
import requests
import redis
import ccxt
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

# ==========================================
# CONFIGURATION
# ==========================================
API_KEY = os.getenv("GATE_API_KEY", "YOUR_GATE_API_KEY")
API_SECRET = os.getenv("GATE_API_SECRET", "YOUR_GATE_API_SECRET")
USE_TESTNET = os.getenv("USE_TESTNET", "true").lower() == "true"

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_STATE_KEY = "spread_bot:state"
REDIS_STATS_KEY = "spread_bot:stats"
REDIS_HISTORY_KEY = "spread_bot:history"
MAX_TRADE_HISTORY = 100

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

WEB_UI_HOST = os.getenv("WEB_UI_HOST", "0.0.0.0")
WEB_UI_PORT = int(os.getenv("PORT") or os.getenv("WEB_UI_PORT") or "8080")
WEB_UI_TOKEN = os.getenv("WEB_UI_TOKEN", "")

UNDERLYING = "BTC/USDT"
CONTRACTS = float(os.getenv("CONTRACTS", "1"))
MIN_DTE = 3
MAX_DTE = 7
MIN_CREDIT_RATIO = 0.15

TARGET_SHORT_PUT_DELTA = -0.25
TARGET_LONG_PUT_DELTA = -0.10
MAX_DELTA_DIFF = 0.08
RISK_FREE_RATE = 0.0

LEG_FILL_TIMEOUT_SEC = 15
LEG_POLL_INTERVAL_SEC = 0.5
MAX_CHASE_STEPS = 3
MAX_SLIPPAGE_PCT = 0.015
UNWIND_TIMEOUT_SEC = 10
QTY_EPSILON = 1e-8

TAKE_PROFIT_PCT = 0.50
STOP_LOSS_MULT = 1.50
EXIT_HOURS_BEFORE_EXP = 12
POLL_INTERVAL_SEC = 60

PIN_INTERVAL_DEFAULT_SEC = 30
PIN_INTERVAL_MIN_SEC = 10
PIN_INTERVAL_MAX_SEC = 3600

IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ==========================================
# CLIENTS & THREAD LOCK
# ==========================================
state_lock = threading.Lock()
rds = redis.from_url(REDIS_URL, decode_responses=True)

exchange = ccxt.gate({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "enableRateLimit": True,
    "options": {"defaultType": "option"},
})

if USE_TESTNET:
    exchange.set_sandbox_mode(True)


# ==========================================
# REDIS PERSISTENCE HELPERS
# ==========================================
def load_state() -> dict:
    raw = rds.get(REDIS_STATE_KEY)
    if raw:
        try:
            data = json.loads(raw)
            data.setdefault("active_trade", None)
            data.setdefault("paused", False)
            data.setdefault("pin_interval_sec", PIN_INTERVAL_DEFAULT_SEC)
            data.setdefault("pin_message_id", None)
            return data
        except json.JSONDecodeError:
            logging.error("Corrupted Redis state JSON; resetting to default.")
    return {
        "active_trade": None,
        "paused": False,
        "pin_interval_sec": PIN_INTERVAL_DEFAULT_SEC,
        "pin_message_id": None,
    }


def save_state(state: dict):
    rds.set(REDIS_STATE_KEY, json.dumps(state))


def load_stats() -> dict:
    raw = rds.get(REDIS_STATS_KEY)
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return {"total_trades": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0}


def update_stats(pnl: float):
    stats = load_stats()
    stats["total_trades"] = int(stats.get("total_trades", 0)) + 1
    if pnl >= 0:
        stats["wins"] = int(stats.get("wins", 0)) + 1
    else:
        stats["losses"] = int(stats.get("losses", 0)) + 1
    stats["realized_pnl"] = round(float(stats.get("realized_pnl", 0.0)) + float(pnl), 4)
    rds.set(REDIS_STATS_KEY, json.dumps(stats))


def record_closed_trade(trade: dict, realized_pnl: float, exit_reason: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    record = {
        "short_sym": trade.get("short_sym"),
        "long_sym": trade.get("long_sym"),
        "short_strike": trade.get("short_strike"),
        "long_strike": trade.get("long_strike"),
        "short_delta": trade.get("short_delta"),
        "long_delta": trade.get("long_delta"),
        "entry_credit": round(float(trade.get("entry_credit", 0.0)), 4),
        "realized_pnl": round(float(realized_pnl), 4),
        "exit_reason": exit_reason,
        "opened_at": trade.get("opened_at"),
        "closed_at": now_iso,
    }
    pipe = rds.pipeline()
    pipe.lpush(REDIS_HISTORY_KEY, json.dumps(record))
    pipe.ltrim(REDIS_HISTORY_KEY, 0, MAX_TRADE_HISTORY - 1)
    pipe.execute()


def load_trade_history(limit: int = 25) -> list[dict]:
    raw_items = rds.lrange(REDIS_HISTORY_KEY, 0, max(0, limit - 1))
    rows = []
    for item in raw_items:
        try:
            rows.append(json.loads(item))
        except json.JSONDecodeError:
            logging.warning("Skipping malformed trade record in Redis history.")
    return rows


# ==========================================
# TELEGRAM API, LIVE CARD & COMMANDS
# ==========================================
def _tg_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN) and "YOUR_TELEGRAM" not in TELEGRAM_BOT_TOKEN


def _tg_api(method: str, payload: dict):
    if not _tg_configured():
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    for _ in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 429:
                retry_after = float(resp.json().get("parameters", {}).get("retry_after", 3))
                time.sleep(max(1.0, retry_after))
                continue
            data = resp.json()
            if not resp.ok or not data.get("ok"):
                logging.warning(f"Telegram {method} non-OK response: {data}")
                return None
            return data.get("result")
        except Exception as e:
            logging.error(f"Telegram {method} request error: {e}")
            time.sleep(2)
    return None


def send_telegram_to(chat_id: str, text: str):
    if not _tg_configured() or not chat_id:
        return
    _tg_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )


def send_telegram(text: str):
    send_telegram_to(str(TELEGRAM_CHAT_ID), text)


def get_unrealized_pnl(trade: dict) -> tuple[float, float]:
    short_ob = exchange.fetch_order_book(trade["short_sym"], limit=5)
    long_ob = exchange.fetch_order_book(trade["long_sym"], limit=5)
    cost_to_close = float(short_ob["asks"][0][0]) - float(long_ob["bids"][0][0])
    pnl = float(trade["entry_credit"]) - cost_to_close
    return pnl, cost_to_close


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_put_delta(spot: float, strike: float, t_years: float, iv: float, r: float = RISK_FREE_RATE) -> float:
    if t_years <= 0.0 or iv <= 0.0 or spot <= 0.0 or strike <= 0.0:
        return -1.0 if spot < strike else 0.0
    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * t_years) / (iv * math.sqrt(t_years))
    return _norm_cdf(d1) - 1.0


def _estimate_live_put_delta(trade: dict, side: str, now_ms: int) -> float | None:
    key = "short_delta" if side == "short" else "long_delta"
    if trade.get(key) is not None:
        return float(trade[key])
    try:
        strike = float(trade.get("short_strike" if side == "short" else "long_strike") or 0.0)
        expiry_ms = int(trade.get("expiry_ms") or 0)
        t_years = max((expiry_ms - now_ms) / (1000 * 60 * 60 * 24 * 365.0), 1e-6)
        ticker = exchange.fetch_ticker(UNDERLYING)
        spot = float(ticker.get("last") or 0.0)
        if spot > 0 and strike > 0:
            return _bs_put_delta(spot, strike, t_years, 0.60)
    except Exception:
        pass
    return None


def build_pinned_status_text(state: dict, stats: dict) -> str:
    now_ms = int(time.time() * 1000)
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    status_flag = "PAUSED" if state.get("paused") else "RUNNING"
    pin_sec = int(state.get("pin_interval_sec", PIN_INTERVAL_DEFAULT_SEC))
    trade = state.get("active_trade")

    if trade:
        try:
            pnl, cost_to_close = get_unrealized_pnl(trade)
            pnl_line = f"Live Spread PnL: <code>{pnl:+.4f}</code> (Close: <code>{cost_to_close:.4f}</code>)"
        except Exception as e:
            pnl_line = f"Live Spread PnL: <i>Quote unavailable ({e})</i>"

        hours_left = max(0.0, (int(trade.get("expiry_ms", now_ms)) - now_ms) / 3600000.0)
        s_delta = _estimate_live_put_delta(trade, "short", now_ms)
        l_delta = _estimate_live_put_delta(trade, "long", now_ms)
        s_delta_str = f"{s_delta:+.3f}" if s_delta is not None else "N/A"
        l_delta_str = f"{l_delta:+.3f}" if l_delta is not None else "N/A"
        net_delta_str = f"{-(s_delta - l_delta):+.3f}" if (s_delta is not None and l_delta is not None) else "N/A"

        position_block = (
            f"<b>Active Spread:</b> Bull Put\n"
            f"Short: <code>{trade.get('short_sym')}</code> (Δ <code>{s_delta_str}</code>)\n"
            f"Long:  <code>{trade.get('long_sym')}</code> (Δ <code>{l_delta_str}</code>)\n"
            f"Net Spread Δ: <code>{net_delta_str}</code>\n"
            f"Entry Credit: <code>{float(trade.get('entry_credit', 0.0)):.4f}</code> | "
            f"Max Loss: <code>{float(trade.get('max_loss', 0.0)):.4f}</code>\n"
            f"{pnl_line}\n"
            f"Time to Expiry: <code>{hours_left:.1f}h</code>"
        )
    else:
        position_block = "<b>Active Spread:</b> None (Flat / Scanning)"

    total = int(stats.get("total_trades", 0))
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    win_rate = (wins / total * 100.0) if total > 0 else 0.0
    realized = float(stats.get("realized_pnl", 0.0))

    return (
        f"<b>BTC Credit Spread Live Card [{status_flag}]</b>\n"
        f"{position_block}\n\n"
        f"<b>Realized Stats:</b> {wins}W / {losses}L ({win_rate:.1f}%) | "
        f"PnL: <code>{realized:+.4f}</code>\n"
        f"<i>Refresh: {pin_sec}s · {now_ist}</i>"
    )


def ensure_and_update_pinned_card(force_new: bool = False):
    if not _tg_configured():
        return

    with state_lock:
        state = load_state()
        stats = load_stats()
        msg_id = None if force_new else state.get("pin_message_id")
        text = build_pinned_status_text(state, stats)

    if msg_id:
        edited = _tg_api(
            "editMessageText",
            {
                "chat_id": TELEGRAM_CHAT_ID,
                "message_id": msg_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        )
        if edited:
            return

    sent = _tg_api(
        "sendMessage",
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )
    if sent and sent.get("message_id"):
        new_msg_id = sent["message_id"]
        _tg_api(
            "pinChatMessage",
            {
                "chat_id": TELEGRAM_CHAT_ID,
                "message_id": new_msg_id,
                "disable_notification": True,
            },
        )
        with state_lock:
            state = load_state()
            state["pin_message_id"] = new_msg_id
            save_state(state)


def pinned_card_worker():
    logging.info("Pinned Telegram status card worker started.")
    while True:
        try:
            ensure_and_update_pinned_card(force_new=False)
        except Exception as e:
            logging.error(f"Pinned card refresh error: {e}")

        with state_lock:
            interval = int(load_state().get("pin_interval_sec", PIN_INTERVAL_DEFAULT_SEC))
        time.sleep(max(PIN_INTERVAL_MIN_SEC, interval))


def telegram_command_worker():
    if not _tg_configured():
        logging.warning("Telegram token not configured; command listener disabled.")
        return

    offset = None
    base_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    logging.info("Telegram command listener started.")

    while True:
        try:
            params = {"timeout": 25}
            if offset is not None:
                params["offset"] = offset
            resp = requests.get(base_url, params=params, timeout=30)
            if resp.status_code != 200:
                time.sleep(3)
                continue

            for upd in resp.json().get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = (msg.get("text") or "").strip()

                if not chat_id or not text.startswith("/"):
                    continue

                cmd = text.split()[0].lower()

                if cmd == "/chatid":
                    send_telegram_to(chat_id, f"Chat ID: <code>{chat_id}</code>")
                    continue

                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                if cmd == "/status":
                    with state_lock:
                        state = load_state()
                        stats = load_stats()
                    send_telegram(build_pinned_status_text(state, stats))

                elif cmd == "/pause":
                    with state_lock:
                        state = load_state()
                        state["paused"] = True
                        save_state(state)
                    send_telegram("<b>Bot Paused.</b> New entries disabled.")
                    ensure_and_update_pinned_card(force_new=False)

                elif cmd == "/resume":
                    with state_lock:
                        state = load_state()
                        state["paused"] = False
                        save_state(state)
                    send_telegram("<b>Bot Resumed.</b> Scanning for setups.")
                    ensure_and_update_pinned_card(force_new=False)

                elif cmd == "/setpinsec":
                    parts = text.split()
                    if len(parts) < 2 or not parts[1].isdigit():
                        send_telegram(
                            f"Usage: <code>/setpinsec &lt;seconds&gt;</code> "
                            f"({PIN_INTERVAL_MIN_SEC}–{PIN_INTERVAL_MAX_SEC}s)"
                        )
                    else:
                        clamped = max(PIN_INTERVAL_MIN_SEC, min(PIN_INTERVAL_MAX_SEC, int(parts[1])))
                        with state_lock:
                            state = load_state()
                            state["pin_interval_sec"] = clamped
                            save_state(state)
                        send_telegram(f"Pinned card refresh set to <b>{clamped}s</b>.")
                        ensure_and_update_pinned_card(force_new=False)

                elif cmd == "/closeall":
                    with state_lock:
                        state = load_state()
                        trade = state.get("active_trade")
                        state["paused"] = True
                        if not trade:
                            save_state(state)
                            send_telegram("<b>/closeall:</b> No active spread open. Bot is now <b>PAUSED</b>.")
                        else:
                            try:
                                pnl = close_spread(trade["short_sym"], trade["long_sym"], trade["entry_credit"])
                                update_stats(pnl)
                                record_closed_trade(trade, pnl, "Emergency /closeall")
                                state["active_trade"] = None
                                save_state(state)
                                send_telegram(
                                    f"<b>EMERGENCY /closeall EXECUTED</b>\n"
                                    f"Closed: <code>{trade['short_sym']}</code> / <code>{trade['long_sym']}</code>\n"
                                    f"Realized PnL: <code>{pnl:+.4f}</code>\n"
                                    f"Bot is now <b>PAUSED</b>."
                                )
                            except Exception as e:
                                save_state(state)
                                send_telegram(f"<b>CRITICAL: /closeall failed!</b> Check exchange manually: {e}")
                    ensure_and_update_pinned_card(force_new=False)

        except Exception as e:
            logging.error(f"Telegram polling error: {e}")
            time.sleep(5)


# ==========================================
# DELTA STRIKE SELECTION & CHASE/UNWIND EXECUTION
# ==========================================
def get_spot_and_ema(ema_period: int = 50):
    ohlcv = exchange.fetch_ohlcv(UNDERLYING, timeframe="1h", limit=ema_period + 10)
    closes = [candle[4] for candle in ohlcv]
    spot = float(closes[-1])
    multiplier = 2 / (ema_period + 1)
    ema = float(closes[0])
    for price in closes[1:]:
        ema = (float(price) - ema) * multiplier + ema
    return spot, ema


def _bs_put_price(spot: float, strike: float, t_years: float, iv: float, r: float = RISK_FREE_RATE) -> float:
    if t_years <= 0.0 or iv <= 0.0:
        return max(0.0, strike - spot)
    sqrt_t = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * t_years) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t
    return strike * math.exp(-r * t_years) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def _implied_put_vol(price: float, spot: float, strike: float, t_years: float, r: float = RISK_FREE_RATE) -> float:
    intrinsic = max(0.0, strike * math.exp(-r * t_years) - spot)
    if price <= intrinsic + 1e-8 or t_years <= 0.0:
        return 0.60
    low, high = 0.05, 3.0
    for _ in range(50):
        mid = 0.5 * (low + high)
        px = _bs_put_price(spot, strike, t_years, mid, r)
        if abs(px - price) < 1e-6:
            return mid
        if px > price:
            high = mid
        else:
            low = mid
    return 0.5 * (low + high)


def _extract_put_delta(market: dict, ticker: dict | None, spot: float, t_years: float) -> float | None:
    info = (ticker or {}).get("info") or {}
    greeks = (ticker or {}).get("greeks") or info.get("greeks") or {}

    raw_delta = None
    for candidate in (
        greeks.get("delta"),
        (ticker or {}).get("delta"),
        info.get("delta"),
        market.get("info", {}).get("delta") if isinstance(market.get("info"), dict) else None,
    ):
        if candidate is not None and candidate != "":
            try:
                raw_delta = float(candidate)
                break
            except (TypeError, ValueError):
                pass

    if raw_delta is not None:
        return raw_delta if raw_delta <= 0 else -raw_delta

    strike = float(market.get("strike") or 0.0)
    bid = (ticker or {}).get("bid")
    ask = (ticker or {}).get("ask")
    last = (ticker or {}).get("last") or info.get("mark_price") or info.get("markPrice")

    ref_price = None
    if bid is not None and ask is not None and float(bid) > 0 and float(ask) > 0:
        ref_price = 0.5 * (float(bid) + float(ask))
    elif last is not None and float(last) > 0:
        ref_price = float(last)

    if ref_price and strike > 0 and t_years > 0:
        iv = _implied_put_vol(ref_price, spot, strike, t_years)
        return _bs_put_delta(spot, strike, t_years, iv)
    return None


def find_bull_put_spread_legs_by_delta(markets: dict, spot: float):
    now_ms = int(time.time() * 1000)
    candidates_by_expiry = {}

    for symbol, m in markets.items():
        if not m.get("active") or m.get("type") != "option":
            continue
        if m.get("base") != "BTC" or m.get("optionType") != "put":
            continue
        expiry_ms = m.get("expiry")
        if not expiry_ms or not m.get("strike"):
            continue
        dte = (expiry_ms - now_ms) / (1000 * 60 * 60 * 24)
        if MIN_DTE <= dte <= MAX_DTE:
            candidates_by_expiry.setdefault(expiry_ms, []).append((symbol, m, dte))

    if not candidates_by_expiry:
        return None

    chosen_expiry = sorted(candidates_by_expiry.keys())[0]
    expiry_contracts = candidates_by_expiry[chosen_expiry]
    symbols = [item[0] for item in expiry_contracts]

    try:
        tickers = exchange.fetch_tickers(symbols)
    except Exception as e:
        logging.warning(f"fetch_tickers failed ({e}); falling back to price IV estimate.")
        tickers = {}

    scored_puts = []
    for symbol, m, dte in expiry_contracts:
        t_years = max(dte, 1e-6) / 365.0
        delta = _extract_put_delta(m, tickers.get(symbol), spot, t_years)
        if delta is not None and -0.80 <= delta <= -0.02:
            scored_puts.append({
                **m,
                "symbol": symbol,
                "strike": float(m["strike"]),
                "delta": delta,
            })

    if len(scored_puts) < 2:
        return None

    short_leg = min(scored_puts, key=lambda x: abs(x["delta"] - TARGET_SHORT_PUT_DELTA))
    long_leg = min(scored_puts, key=lambda x: abs(x["delta"] - TARGET_LONG_PUT_DELTA))

    if abs(short_leg["delta"] - TARGET_SHORT_PUT_DELTA) > MAX_DELTA_DIFF:
        return None
    if abs(long_leg["delta"] - TARGET_LONG_PUT_DELTA) > MAX_DELTA_DIFF:
        return None
    if short_leg["strike"] <= long_leg["strike"]:
        return None

    return short_leg, long_leg, chosen_expiry


def check_spread_edge(short_sym: str, long_sym: str, strike_width: float):
    short_ob = exchange.fetch_order_book(short_sym, limit=5)
    long_ob = exchange.fetch_order_book(long_sym, limit=5)
    if not short_ob.get("bids") or not long_ob.get("asks"):
        return None

    short_bid = float(short_ob["bids"][0][0])
    long_ask = float(long_ob["asks"][0][0])
    net_credit = short_bid - long_ask

    if net_credit <= 0 or (net_credit / strike_width) < MIN_CREDIT_RATIO:
        return None

    return {
        "short_bid": short_bid,
        "long_ask": long_ask,
        "net_credit": net_credit,
        "max_loss": strike_width - net_credit,
    }


def _remaining_qty(order: dict, target_amount: float) -> float:
    filled = float(order.get("filled") or 0.0)
    if order.get("remaining") is not None:
        return max(0.0, float(order["remaining"]))
    return max(0.0, target_amount - filled)


def _is_order_filled(order: dict, target_amount: float) -> bool:
    status = (order.get("status") or "").lower()
    filled = float(order.get("filled") or 0.0)
    rem = _remaining_qty(order, target_amount)
    return status == "closed" and filled + QTY_EPSILON >= target_amount and rem <= QTY_EPSILON


def _safe_cancel_order(order_id: str, symbol: str) -> dict:
    try:
        return exchange.cancel_order(order_id, symbol)
    except Exception as e:
        logging.warning(f"Cancel order {order_id} ({symbol}) error: {e}; fetching order state.")
        return exchange.fetch_order(order_id, symbol)


def _avg_fill_price(order: dict, fallback_price: float) -> float:
    avg = order.get("average")
    if avg is not None and float(avg) > 0:
        return float(avg)
    cost = order.get("cost")
    filled = order.get("filled")
    if cost is not None and filled is not None and float(filled) > QTY_EPSILON:
        return float(cost) / float(filled)
    price = order.get("price")
    if price is not None and float(price) > 0:
        return float(price)
    return float(fallback_price)


def _compute_next_chase_price(symbol: str, side: str, anchor_price: float, current_price: float, step_idx: int, total_steps: int) -> float:
    frac = min(1.0, step_idx / max(1, total_steps))
    if side == "buy":
        max_allowed = anchor_price * (1.0 + MAX_SLIPPAGE_PCT)
        target = anchor_price + (max_allowed - anchor_price) * frac
        try:
            ob = exchange.fetch_order_book(symbol, limit=3)
            if ob.get("asks"):
                target = min(max_allowed, max(target, float(ob["asks"][0][0])))
        except Exception:
            pass
        next_px = max(current_price, min(max_allowed, target))
    else:
        min_allowed = anchor_price * (1.0 - MAX_SLIPPAGE_PCT)
        target = anchor_price - (anchor_price - min_allowed) * frac
        try:
            ob = exchange.fetch_order_book(symbol, limit=3)
            if ob.get("bids"):
                target = max(min_allowed, min(target, float(ob["bids"][0][0])))
        except Exception:
            pass
        next_px = min(current_price, max(min_allowed, target))
    return float(exchange.price_to_precision(symbol, next_px))


def _update_or_replace_remainder(order_id: str, symbol: str, side: str, rem_amount: float, new_price: float) -> dict | None:
    if exchange.has.get("editOrder"):
        try:
            return exchange.edit_order(order_id, symbol, "limit", side, rem_amount, new_price)
        except Exception:
            pass
    cancelled = _safe_cancel_order(order_id, symbol)
    already_filled = float(cancelled.get("filled") or 0.0)
    actual_rem = max(0.0, rem_amount - already_filled)
    if actual_rem <= QTY_EPSILON:
        return cancelled
    return exchange.create_order(symbol, "limit", side, actual_rem, new_price)


def _unwind_partial_fill(symbol: str, side_opened: str, filled_qty: float, ref_price: float):
    if filled_qty <= QTY_EPSILON:
        return
    unwind_side = "sell" if side_opened == "buy" else "buy"
    try:
        ob = exchange.fetch_order_book(symbol, limit=3)
        if unwind_side == "sell" and ob.get("bids"):
            unwind_px = float(ob["bids"][0][0]) * (1.0 - MAX_SLIPPAGE_PCT)
        elif unwind_side == "buy" and ob.get("asks"):
            unwind_px = float(ob["asks"][0][0]) * (1.0 + MAX_SLIPPAGE_PCT)
        else:
            unwind_px = ref_price * (0.98 if unwind_side == "sell" else 1.02)
    except Exception:
        unwind_px = ref_price * (0.98 if unwind_side == "sell" else 1.02)

    unwind_px = float(exchange.price_to_precision(symbol, unwind_px))
    order = exchange.create_order(symbol, "limit", unwind_side, filled_qty, unwind_px)
    order_id = order.get("id")
    if not order_id:
        return

    deadline = time.monotonic() + UNWIND_TIMEOUT_SEC
    latest = order
    while time.monotonic() < deadline:
        if _is_order_filled(latest, filled_qty):
            return
        time.sleep(LEG_POLL_INTERVAL_SEC)
        try:
            latest = exchange.fetch_order(order_id, symbol)
        except Exception:
            pass
    _safe_cancel_order(order_id, symbol)


def _execute_leg_with_fill_check(symbol: str, side: str, amount: float, price: float, timeout_sec: float = LEG_FILL_TIMEOUT_SEC):
    anchor_price = float(price)
    current_price = float(exchange.price_to_precision(symbol, anchor_price))
    order = exchange.create_order(symbol, "limit", side, amount, current_price)
    order_id = order.get("id")
    if not order_id:
        return None

    cumulative_filled = 0.0
    latest_order = order

    for step in range(MAX_CHASE_STEPS + 1):
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            try:
                latest_order = exchange.fetch_order(order_id, symbol)
            except Exception as e:
                logging.warning(f"Transient fetch_order error ({order_id}): {e}")
            if _is_order_filled(latest_order, amount):
                return latest_order
            time.sleep(LEG_POLL_INTERVAL_SEC)

        try:
            latest_order = exchange.fetch_order(order_id, symbol)
        except Exception:
            pass
        if _is_order_filled(latest_order, amount):
            return latest_order

        cumulative_filled = float(latest_order.get("filled") or 0.0)
        rem_qty = max(0.0, amount - cumulative_filled)
        if rem_qty <= QTY_EPSILON:
            return latest_order

        if step < MAX_CHASE_STEPS:
            next_price = _compute_next_chase_price(symbol, side, anchor_price, current_price, step + 1, MAX_CHASE_STEPS)
            logging.info(f"[CHASE {step + 1}/{MAX_CHASE_STEPS}] {symbol} ({side}) stepping {current_price} -> {next_price}")
            current_price = next_price
            updated = _update_or_replace_remainder(order_id, symbol, side, rem_qty, current_price)
            if updated and updated.get("id"):
                order_id = updated["id"]
                latest_order = updated
        else:
            break

    cancelled = _safe_cancel_order(order_id, symbol)
    if _is_order_filled(cancelled, amount):
        return cancelled

    final_filled = float(cancelled.get("filled") or cumulative_filled)
    if final_filled > QTY_EPSILON:
        _unwind_partial_fill(symbol, side, final_filled, _avg_fill_price(cancelled, current_price))
    return None


def open_spread(short_sym: str, long_sym: str, quote: dict):
    long_order = _execute_leg_with_fill_check(long_sym, "buy", CONTRACTS, quote["long_ask"])
    if not long_order:
        return None
    long_fill = _avg_fill_price(long_order, quote["long_ask"])

    short_order = _execute_leg_with_fill_check(short_sym, "sell", CONTRACTS, quote["short_bid"])
    if not short_order:
        return None
    short_fill = _avg_fill_price(short_order, quote["short_bid"])

    return {
        "short_fill_price": short_fill,
        "long_fill_price": long_fill,
        "actual_credit": short_fill - long_fill,
    }


def close_spread(short_sym: str, long_sym: str, entry_credit: float) -> float:
    short_ob = exchange.fetch_order_book(short_sym, limit=5)
    long_ob = exchange.fetch_order_book(long_sym, limit=5)
    if not short_ob.get("asks") or not long_ob.get("bids"):
        raise RuntimeError(f"Insufficient order book liquidity to close {short_sym} / {long_sym}")

    buy_back_px = float(short_ob["asks"][0][0])
    sell_out_px = float(long_ob["bids"][0][0])

    short_close = _execute_leg_with_fill_check(short_sym, "buy", CONTRACTS, buy_back_px)
    if not short_close:
        raise RuntimeError(f"Failed to completely buy back short leg {short_sym}")
    actual_buy_back = _avg_fill_price(short_close, buy_back_px)

    long_close = _execute_leg_with_fill_check(long_sym, "sell", CONTRACTS, sell_out_px)
    if not long_close:
        raise RuntimeError(f"Short leg closed, but failed to completely sell long leg {long_sym}")
    actual_sell_out = _avg_fill_price(long_close, sell_out_px)

    return float(entry_credit) - (actual_buy_back - actual_sell_out)


# ==========================================
# FASTAPI WEB UI & HEALTHZ
# ==========================================
app = FastAPI(title="BTC Credit Spread Bot Dashboard")


class PinIntervalPayload(BaseModel):
    seconds: int


def _verify_token(token: str | None):
    if WEB_UI_TOKEN and token != WEB_UI_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid dashboard token")


def _fmt_ist(iso_str: str | None) -> str:
    if not iso_str:
        return "--"
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).strftime("%d %b %H:%M:%S")
    except Exception:
        return str(iso_str)


@app.get("/healthz")
def healthz():
    redis_ok = False
    try:
        redis_ok = bool(rds.ping())
    except Exception:
        redis_ok = False

    with state_lock:
        try:
            state = load_state()
            paused = bool(state.get("paused", False))
            has_trade = state.get("active_trade") is not None
        except Exception:
            paused = False
            has_trade = False

    return {
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "mode": "TESTNET" if USE_TESTNET else "LIVE",
        "port": WEB_UI_PORT,
        "paused": paused,
        "has_active_trade": has_trade,
        "time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    }


@app.get("/api/state")
def api_state():
    with state_lock:
        state = load_state()
        stats = load_stats()
        history_raw = load_trade_history(limit=25)

    now_ms = int(time.time() * 1000)
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    trade = state.get("active_trade")
    live_trade = None

    if trade:
        try:
            pnl, cost_to_close = get_unrealized_pnl(trade)
        except Exception:
            pnl, cost_to_close = None, None

        expiry_ms = int(trade.get("expiry_ms", now_ms))
        opened_iso = trade.get("opened_at")
        try:
            opened_ms = int(datetime.fromisoformat(opened_iso).timestamp() * 1000) if opened_iso else (expiry_ms - 5 * 86400000)
        except Exception:
            opened_ms = expiry_ms - 5 * 86400000

        total_dur_ms = max(1, expiry_ms - opened_ms)
        rem_ms = max(0, expiry_ms - now_ms)
        rem_pct = round(min(100.0, max(0.0, (rem_ms / total_dur_ms) * 100.0)), 1)

        s_delta = _estimate_live_put_delta(trade, "short", now_ms)
        l_delta = _estimate_live_put_delta(trade, "long", now_ms)

        live_trade = {
            **trade,
            "unrealized_pnl": round(pnl, 4) if pnl is not None else None,
            "cost_to_close": round(cost_to_close, 4) if cost_to_close is not None else None,
            "hours_left": round(rem_ms / 3600000.0, 2),
            "remaining_pct": rem_pct,
            "short_delta": round(s_delta, 3) if s_delta is not None else None,
            "long_delta": round(l_delta, 3) if l_delta is not None else None,
            "net_delta": round(-(s_delta - l_delta), 3) if (s_delta is not None and l_delta is not None) else None,
        }

    formatted_history = [
        {
            **h,
            "entry_ist": _fmt_ist(h.get("opened_at")),
            "exit_ist": _fmt_ist(h.get("closed_at")),
        }
        for h in history_raw
    ]

    return {
        "server_time_ist": now_ist,
        "mode": "TESTNET" if USE_TESTNET else "LIVE",
        "paused": bool(state.get("paused", False)),
        "pin_interval_sec": int(state.get("pin_interval_sec", PIN_INTERVAL_DEFAULT_SEC)),
        "active_trade": live_trade,
        "stats": stats,
        "trade_history": formatted_history,
    }


@app.post("/api/toggle-pause")
def api_toggle_pause(token: str | None = None):
    _verify_token(token)
    with state_lock:
        state = load_state()
        state["paused"] = not bool(state.get("paused", False))
        save_state(state)
        paused = state["paused"]
    send_telegram(f"<b>Web UI:</b> Bot is now <b>{'PAUSED' if paused else 'RUNNING'}</b>.")
    ensure_and_update_pinned_card(force_new=False)
    return {"ok": True, "paused": paused}


@app.post("/api/setpinsec")
def api_set_pin_sec(payload: PinIntervalPayload, token: str | None = None):
    _verify_token(token)
    clamped = max(PIN_INTERVAL_MIN_SEC, min(PIN_INTERVAL_MAX_SEC, int(payload.seconds)))
    with state_lock:
        state = load_state()
        state["pin_interval_sec"] = clamped
        save_state(state)
    ensure_and_update_pinned_card(force_new=False)
    return {"ok": True, "pin_interval_sec": clamped}


@app.post("/api/closeall")
def api_close_all(token: str | None = None):
    _verify_token(token)
    with state_lock:
        state = load_state()
        trade = state.get("active_trade")
        state["paused"] = True
        if not trade:
            save_state(state)
            send_telegram("<b>Web UI /closeall:</b> No active spread open. Bot is now <b>PAUSED</b>.")
            ensure_and_update_pinned_card(force_new=False)
            return {"ok": True, "closed": False}
        try:
            pnl = close_spread(trade["short_sym"], trade["long_sym"], trade["entry_credit"])
            update_stats(pnl)
            record_closed_trade(trade, pnl, "Emergency /closeall")
            state["active_trade"] = None
            save_state(state)
            send_telegram(
                f"<b>WEB UI EMERGENCY /closeall EXECUTED</b>\n"
                f"Closed: <code>{trade['short_sym']}</code> / <code>{trade['long_sym']}</code>\n"
                f"Realized PnL: <code>{pnl:+.4f}</code>\n"
                f"Bot is now <b>PAUSED</b>."
            )
            ensure_and_update_pinned_card(force_new=False)
            return {"ok": True, "closed": True, "realized_pnl": round(pnl, 4)}
        except Exception as e:
            save_state(state)
            send_telegram(f"<b>CRITICAL: Web UI /closeall failed!</b> Check exchange manually: {e}")
            raise HTTPException(status_code=500, detail=str(e))


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>BTC Credit Spread Bot</title>
<style>
  :root {
    --bg: #0b0f17; --card: #131a26; --border: #222d40;
    --text: #e6edf7; --muted: #8b9bb4; --green: #22c55e;
    --red: #ef4444; --amber: #f59e0b; --blue: #3b82f6;
  }
  * { box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  body { margin: 0; background: var(--bg); color: var(--text); padding: 20px; }
  .wrap { max-width: 980px; margin: 0 auto; }
  .topbar { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 18px; }
  .title { font-size: 1.25rem; font-weight: 700; display: flex; align-items: center; gap: 10px; }
  .badge { font-size: 0.75rem; padding: 3px 8px; border-radius: 999px; font-weight: 600; }
  .badge-run { background: rgba(34,197,94,.15); color: var(--green); border: 1px solid rgba(34,197,94,.4); }
  .badge-pause { background: rgba(245,158,11,.15); color: var(--amber); border: 1px solid rgba(245,158,11,.4); }
  .badge-mode { background: rgba(59,130,246,.15); color: var(--blue); border: 1px solid rgba(59,130,246,.4); }
  .meta { font-size: 0.85rem; color: var(--muted); }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }
  .label { font-size: 0.78rem; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }
  .val { font-size: 1.4rem; font-weight: 700; margin-top: 6px; font-variant-numeric: tabular-nums; }
  .pos { color: var(--green); } .neg { color: var(--red); }
  .bar-bg { width: 100%; height: 10px; background: #1c2536; border-radius: 999px; overflow: hidden; margin-top: 10px; }
  .bar-fill { height: 100%; background: linear-gradient(90deg, var(--blue), var(--green)); transition: width .4s ease; }
  .controls { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-top: 14px; }
  button { cursor: pointer; border: none; border-radius: 8px; padding: 9px 14px; font-weight: 600; font-size: 0.85rem; }
  .btn-primary { background: var(--blue); color: #fff; }
  .btn-warn { background: var(--amber); color: #111; }
  .btn-danger { background: var(--red); color: #fff; }
  input[type=number] { background: #0f1520; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; width: 90px; }
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
      BTC Bull Put Credit Spread
      <span id="modeBadge" class="badge badge-mode">TESTNET</span>
      <span id="statusBadge" class="badge badge-run">RUNNING</span>
    </div>
    <div class="meta">
      <span id="istClock">--</span> · Refresh in <b id="countdown">3</b>s
    </div>
  </div>

  <div class="grid">
    <div class="card"><div class="label">Unrealized Spread PnL</div><div id="livePnl" class="val">--</div></div>
    <div class="card"><div class="label">Realized Cumulative PnL</div><div id="realizedPnl" class="val">--</div></div>
    <div class="card"><div class="label">Win / Loss Record</div><div id="winLoss" class="val">0W / 0L</div></div>
    <div class="card"><div class="label">Max Loss Cap (Active)</div><div id="maxLoss" class="val">--</div></div>
  </div>

  <div class="card" style="margin-bottom:16px;">
    <div class="label">Active Spread Position</div>
    <div id="noTradeMsg" style="margin-top:10px; color:var(--muted);">No active spread open. Scanning 1h trend &amp; option chain...</div>
    <div id="tradePanel" style="display:none;">
      <table>
        <thead><tr><th>Leg</th><th>Contract</th><th>Strike</th><th>Fill Price</th><th>Delta (Δ)</th></tr></thead>
        <tbody>
          <tr><td><b>Short Put</b></td><td><code id="shortSym">--</code></td><td id="shortStrike">--</td><td id="shortFill">--</td><td id="shortDelta">--</td></tr>
          <tr><td><b>Long Put (Hedge)</b></td><td><code id="longSym">--</code></td><td id="longStrike">--</td><td id="longFill">--</td><td id="longDelta">--</td></tr>
        </tbody>
      </table>
      <div style="display:flex; justify-content:space-between; margin-top:12px; font-size:0.85rem; color:var(--muted);">
        <span>Entry Credit: <b id="entryCredit" style="color:var(--text);">--</b> | Net Δ: <b id="netDelta" style="color:var(--text);">--</b></span>
        <span>Remaining Hold: <b id="hoursLeft" style="color:var(--text);">--</b>h (<span id="remPct">--</span>%)</span>
      </div>
      <div class="bar-bg"><div id="holdBar" class="bar-fill" style="width:0%;"></div></div>
    </div>
  </div>

  <div class="card">
    <div class="label">Controls</div>
    <div class="controls">
      <button id="toggleBtn" class="btn-warn" onclick="togglePause()">Pause Bot</button>
      <button class="btn-danger" onclick="emergencyCloseAll()">Emergency /closeall</button>
      <div style="margin-left:auto; display:flex; align-items:center; gap:8px;">
        <span style="font-size:0.85rem; color:var(--muted);">TG Pin Refresh (s):</span>
        <input id="pinSecInput" type="number" min="10" max="3600" value="30" />
        <button class="btn-primary" onclick="updatePinSec()">Update</button>
      </div>
    </div>
  </div>

  <div class="card" style="margin-top:16px;">
    <div class="label">Recent Closed Trades (IST)</div>
    <div id="noHistoryMsg" style="margin-top:10px; color:var(--muted);">No closed trades recorded yet.</div>
    <div id="historyWrap" style="display:none; overflow-x:auto;">
      <table>
        <thead>
          <tr><th>Entry → Exit (IST)</th><th>Strikes (S / L)</th><th>Deltas (S / L)</th><th>Reason</th><th>Realized PnL</th></tr>
        </thead>
        <tbody id="historyBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const token = new URLSearchParams(window.location.search).get("token") || "";
function apiUrl(path) { return token ? `${path}?token=${encodeURIComponent(token)}` : path; }
let secLeft = 3;

async function fetchState() {
  try {
    const res = await fetch("/api/state");
    const data = await res.json();
    document.getElementById("istClock").textContent = data.server_time_ist;
    document.getElementById("modeBadge").textContent = data.mode;

    const st = document.getElementById("statusBadge");
    const tBtn = document.getElementById("toggleBtn");
    if (data.paused) {
      st.textContent = "PAUSED"; st.className = "badge badge-pause";
      tBtn.textContent = "Resume Bot"; tBtn.className = "btn-primary";
    } else {
      st.textContent = "RUNNING"; st.className = "badge badge-run";
      tBtn.textContent = "Pause Bot"; tBtn.className = "btn-warn";
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
    document.getElementById("winLoss").textContent = `${wins}W / ${losses}L (${wr}%)`;

    if (document.activeElement !== document.getElementById("pinSecInput")) {
      document.getElementById("pinSecInput").value = data.pin_interval_sec;
    }

    const t = data.active_trade;
    const liveEl = document.getElementById("livePnl");
    if (t) {
      document.getElementById("noTradeMsg").style.display = "none";
      document.getElementById("tradePanel").style.display = "block";
      document.getElementById("shortSym").textContent = t.short_sym;
      document.getElementById("longSym").textContent = t.long_sym;
      document.getElementById("shortStrike").textContent = t.short_strike;
      document.getElementById("longStrike").textContent = t.long_strike;
      document.getElementById("shortFill").textContent = Number(t.short_fill_price || 0).toFixed(4);
      document.getElementById("longFill").textContent = Number(t.long_fill_price || 0).toFixed(4);
      document.getElementById("shortDelta").textContent = t.short_delta !== null ? t.short_delta.toFixed(3) : "N/A";
      document.getElementById("longDelta").textContent = t.long_delta !== null ? t.long_delta.toFixed(3) : "N/A";
      document.getElementById("netDelta").textContent = t.net_delta !== null ? t.net_delta.toFixed(3) : "N/A";
      document.getElementById("entryCredit").textContent = Number(t.entry_credit || 0).toFixed(4);
      document.getElementById("maxLoss").textContent = Number(t.max_loss || 0).toFixed(4);
      document.getElementById("hoursLeft").textContent = t.hours_left;
      document.getElementById("remPct").textContent = t.remaining_pct;
      document.getElementById("holdBar").style.width = `${t.remaining_pct}%`;

      if (t.unrealized_pnl !== null) {
        const u = Number(t.unrealized_pnl);
        liveEl.textContent = (u >= 0 ? "+" : "") + u.toFixed(4);
        liveEl.className = "val " + (u >= 0 ? "pos" : "neg");
      } else {
        liveEl.textContent = "Quote N/A"; liveEl.className = "val";
      }
    } else {
      document.getElementById("noTradeMsg").style.display = "block";
      document.getElementById("tradePanel").style.display = "none";
      liveEl.textContent = "Flat"; liveEl.className = "val";
      document.getElementById("maxLoss").textContent = "--";
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
        const sdStr = (row.short_delta !== null && row.short_delta !== undefined) ? Number(row.short_delta).toFixed(3) : "N/A";
        const ldStr = (row.long_delta !== null && row.long_delta !== undefined) ? Number(row.long_delta).toFixed(3) : "N/A";
        return `<tr>
          <td><code>${row.entry_ist}</code> → <code>${row.exit_ist}</code></td>
          <td><b>${row.short_strike ?? "--"}</b> / ${row.long_strike ?? "--"}</td>
          <td><code>${sdStr}</code> / <code>${ldStr}</code></td>
          <td>${row.exit_reason || "--"}</td>
          <td class="${cls}"><b>${pnlStr}</b></td>
        </tr>`;
      }).join("");
    } else {
      document.getElementById("noHistoryMsg").style.display = "block";
      document.getElementById("historyWrap").style.display = "none";
    }
  } catch (err) {
    console.error("State fetch error:", err);
  }
}

async function togglePause() { await fetch(apiUrl("/api/toggle-pause"), { method: "POST" }); fetchState(); }
async function updatePinSec() {
  const sec = parseInt(document.getElementById("pinSecInput").value, 10);
  if (!sec || sec < 10) return;
  await fetch(apiUrl("/api/setpinsec"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ seconds: sec }) });
  fetchState();
}
async function emergencyCloseAll() {
  if (!confirm("Close all open option legs immediately and pause the bot?")) return;
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
</html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard_page():
    return DASHBOARD_HTML


def start_web_ui():
    uvicorn.run(app, host=WEB_UI_HOST, port=WEB_UI_PORT, log_level="warning")


# ==========================================
# MAIN LOOP
# ==========================================
def run_bot():
    threading.Thread(target=telegram_command_worker, daemon=True).start()
    threading.Thread(target=pinned_card_worker, daemon=True).start()
    threading.Thread(target=start_web_ui, daemon=True).start()

    markets = exchange.load_markets()
    state = load_state()

    send_telegram(
        f"<b>Bull Put Spread Bot Started</b>\n"
        f"Mode: <code>{'TESTNET' if USE_TESTNET else 'LIVE'}</code> | "
        f"State: <code>{'PAUSED' if state['paused'] else 'RUNNING'}</code>\n"
        f"Web UI: <code>http://{WEB_UI_HOST}:{WEB_UI_PORT}</code>"
    )
    ensure_and_update_pinned_card(force_new=False)
    logging.info("Bull Put Credit Spread Bot started.")

    while True:
        try:
            with state_lock:
                state = load_state()
                active_trade = state.get("active_trade")
                now_ms = int(time.time() * 1000)

                if active_trade:
                    hours_to_exp = (int(active_trade["expiry_ms"]) - now_ms) / (1000 * 60 * 60)
                    pnl, _ = get_unrealized_pnl(active_trade)

                    tp_target = float(active_trade["entry_credit"]) * TAKE_PROFIT_PCT
                    sl_limit = -float(active_trade["entry_credit"]) * STOP_LOSS_MULT

                    exit_reason = None
                    if pnl >= tp_target:
                        exit_reason = "Take Profit (50% Max Credit)"
                    elif pnl <= sl_limit:
                        exit_reason = "Stop Loss (1.5x Credit)"
                    elif hours_to_exp <= EXIT_HOURS_BEFORE_EXP:
                        exit_reason = "Time Exit (Pre-Expiry Pin Protection)"

                    if exit_reason:
                        realized = close_spread(
                            active_trade["short_sym"],
                            active_trade["long_sym"],
                            active_trade["entry_credit"],
                        )
                        update_stats(realized)
                        record_closed_trade(active_trade, realized, exit_reason)
                        state["active_trade"] = None
                        save_state(state)

                        send_telegram(
                            f"<b>Spread Closed — {exit_reason}</b>\n"
                            f"Short: <code>{active_trade['short_sym']}</code>\n"
                            f"Long: <code>{active_trade['long_sym']}</code>\n"
                            f"Realized PnL: <code>{realized:+.4f}</code>"
                        )
                        ensure_and_update_pinned_card(force_new=False)
                    else:
                        logging.info(
                            f"Monitoring {active_trade['short_sym']} | "
                            f"PnL: {pnl:+.4f} | Exp: {hours_to_exp:.1f}h"
                        )

                elif not state.get("paused"):
                    spot, ema_50 = get_spot_and_ema(50)
                    if spot > ema_50:
                        legs = find_bull_put_spread_legs_by_delta(markets, spot)
                        if legs:
                            short_leg, long_leg, expiry_ms = legs
                            width = float(short_leg["strike"]) - float(long_leg["strike"])
                            quote = check_spread_edge(short_leg["symbol"], long_leg["symbol"], width)
                            if quote:
                                fills = open_spread(short_leg["symbol"], long_leg["symbol"], quote)
                                if fills:
                                    actual_credit = float(fills["actual_credit"])
                                    actual_max_loss = width - actual_credit
                                    new_trade = {
                                        "short_sym": short_leg["symbol"],
                                        "long_sym": long_leg["symbol"],
                                        "short_strike": short_leg["strike"],
                                        "long_strike": long_leg["strike"],
                                        "short_delta": short_leg["delta"],
                                        "long_delta": long_leg["delta"],
                                        "short_fill_price": fills["short_fill_price"],
                                        "long_fill_price": fills["long_fill_price"],
                                        "entry_credit": actual_credit,
                                        "max_loss": actual_max_loss,
                                        "expiry_ms": expiry_ms,
                                        "opened_at": datetime.now(timezone.utc).isoformat(),
                                    }
                                    state["active_trade"] = new_trade
                                    save_state(state)

                                    send_telegram(
                                        f"<b>New Bull Put Spread Opened</b>\n"
                                        f"Spot: <code>{spot:.2f}</code> (EMA50: <code>{ema_50:.2f}</code>)\n"
                                        f"Short Put: <code>{short_leg['strike']}</code> (Δ <code>{short_leg['delta']:+.3f}</code>)\n"
                                        f"Long Put:  <code>{long_leg['strike']}</code> (Δ <code>{long_leg['delta']:+.3f}</code>)\n"
                                        f"Actual Credit: <code>{actual_credit:.4f}</code>\n"
                                        f"Max Loss Cap: <code>{actual_max_loss:.4f}</code>"
                                    )
                                    ensure_and_update_pinned_card(force_new=False)

        except Exception as e:
            logging.error(f"Main loop error: {e}")
            send_telegram(f"<b>Bot Loop Error:</b> <code>{e}</code>")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_bot()
