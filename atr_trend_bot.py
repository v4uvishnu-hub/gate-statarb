import os
import json
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
# CONFIGURATION (PUBLIC API / NO KEY NEEDED)
# ==========================================
API_KEY = os.getenv("GATE_API_KEY", "").strip()
API_SECRET = os.getenv("GATE_API_SECRET", "").strip()
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"

# Automatically runs in paper-execution mode when API keys are omitted
HAS_PRIVATE_KEYS = bool(API_KEY and API_SECRET)
PAPER_MODE = os.getenv("PAPER_MODE", "true" if not HAS_PRIVATE_KEYS else "false").lower() == "true" or not HAS_PRIVATE_KEYS

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_STATE_KEY = "atr_trend_bot:state"
REDIS_STATS_KEY = "atr_trend_bot:stats"
REDIS_HISTORY_KEY = "atr_trend_bot:history"
MAX_TRADE_HISTORY = 100

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WEB_UI_HOST = os.getenv("WEB_UI_HOST", "0.0.0.0")
WEB_UI_PORT = int(os.getenv("PORT") or os.getenv("WEB_UI_PORT") or "8080")
WEB_UI_TOKEN = os.getenv("WEB_UI_TOKEN", "").strip()

# Universe, Rotation & Concurrent Position Settings
TOP_N_COINS = int(os.getenv("TOP_N_COINS", "100"))
MAX_PAIRS_PER_CYCLE = int(os.getenv("MAX_PAIRS_PER_CYCLE", "25"))
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "1800"))

raw_pinned = (
    os.getenv("PINNED_SYMBOLS")
    or os.getenv("TRADE_SYMBOLS")
    or os.getenv("TRADE_SYMBOL")
    or "BTC/USDT:USDT,ETH/USDT:USDT"
)
PINNED_SYMBOLS = [s.strip() for s in raw_pinned.split(",") if s.strip()]

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "2"))

# Strategy Parameters (1h EMA trend + 15m ATR pullback/continuation)
TREND_TF = "1h"
ENTRY_TF = "15m"
EMA_FAST_LEN = 20
EMA_SLOW_LEN = 50
ATR_LEN = 14

RISK_PER_TRADE_USDT = float(os.getenv("RISK_PER_TRADE_USDT", "25.0"))
STOP_ATR_MULT = float(os.getenv("STOP_ATR_MULT", "1.5"))
TRAIL_ATR_MULT = float(os.getenv("TRAIL_ATR_MULT", "2.0"))
TP_ATR_MULT = float(os.getenv("TP_ATR_MULT", "3.0"))
PULLBACK_ATR_BAND = float(os.getenv("PULLBACK_ATR_BAND", "0.75"))
MAX_HOLD_HOURS = float(os.getenv("MAX_HOLD_HOURS", "24.0"))

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
# CLIENTS, THREAD LOCKS & MARKET CACHE
# ==========================================
state_lock = threading.Lock()
scan_lock = threading.Lock()
universe_lock = threading.Lock()

cached_markets: dict = {}
cached_top_symbols: list[str] = list(PINNED_SYMBOLS)
last_universe_refresh_ts: float = 0.0

rds = redis.from_url(REDIS_URL, decode_responses=True)

exchange_cfg: dict = {
    "enableRateLimit": True,
    "options": {"defaultType": "swap"},
}
if HAS_PRIVATE_KEYS:
    exchange_cfg["apiKey"] = API_KEY
    exchange_cfg["secret"] = API_SECRET

exchange = ccxt.gate(exchange_cfg)
if USE_TESTNET:
    exchange.set_sandbox_mode(True)


def mode_label() -> str:
    if PAPER_MODE:
        return "PUBLIC-PAPER"
    return "TESTNET" if USE_TESTNET else "LIVE"


# ==========================================
# REDIS PERSISTENCE HELPERS
# ==========================================
def load_state() -> dict:
    raw = rds.get(REDIS_STATE_KEY)
    if raw:
        try:
            data = json.loads(raw)
            trades = data.get("active_trades")
            if not isinstance(trades, list):
                legacy = data.get("active_trade")
                trades = [legacy] if isinstance(legacy, dict) else []
            data["active_trades"] = trades
            data.setdefault("paused", False)
            data.setdefault("pin_interval_sec", PIN_INTERVAL_DEFAULT_SEC)
            data.setdefault("pin_message_id", None)
            data.setdefault("last_scan", None)
            data.setdefault("scan_cursor", 0)
            return data
        except json.JSONDecodeError:
            logging.error("Corrupted Redis state JSON; resetting to default.")
    return {
        "active_trades": [],
        "paused": False,
        "pin_interval_sec": PIN_INTERVAL_DEFAULT_SEC,
        "pin_message_id": None,
        "last_scan": None,
        "scan_cursor": 0,
    }


def save_state(state: dict):
    state["active_trades"] = list(state.get("active_trades") or [])
    state["active_trade"] = state["active_trades"][0] if state["active_trades"] else None
    rds.set(REDIS_STATE_KEY, json.dumps(state))


def record_scan_diagnostics(
    status: str,
    reason: str,
    symbol: str | None = None,
    spot: float | None = None,
    ema_50: float | None = None,
    atr_15m: float | None = None,
    setup_side: str | None = None,
    pair_summaries: list[dict] | None = None,
):
    now_ist = datetime.now(IST).strftime("%d %b %H:%M:%S IST")
    scan_info = {
        "checked_at_ist": now_ist,
        "status": status,
        "reason": reason,
        "symbol": symbol,
        "spot": round(float(spot), 4) if spot is not None else None,
        "ema_50": round(float(ema_50), 4) if ema_50 is not None else None,
        "atr_15m": round(float(atr_15m), 4) if atr_15m is not None else None,
        "setup_side": setup_side,
        "pair_summaries": pair_summaries or [],
    }
    with state_lock:
        state = load_state()
        state["last_scan"] = scan_info
        save_state(state)


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


def record_closed_trade(trade: dict, exit_price: float, realized_pnl: float, exit_reason: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    record = {
        "symbol": trade.get("symbol"),
        "side": trade.get("side"),
        "amount": trade.get("amount"),
        "entry_price": round(float(trade.get("entry_price", 0.0)), 4),
        "exit_price": round(float(exit_price), 4),
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
# UNIVERSE DISCOVERY & ROTATION (PUBLIC)
# ==========================================
def _quote_volume_24h(ticker: dict | None) -> float:
    if not ticker:
        return 0.0
    qv = ticker.get("quoteVolume")
    if qv is not None:
        try:
            val = float(qv)
            if val > 0:
                return val
        except (TypeError, ValueError):
            pass
    bv = ticker.get("baseVolume")
    px = ticker.get("last") or ticker.get("close") or ticker.get("mark")
    if bv is not None and px is not None:
        try:
            return max(0.0, float(bv) * float(px))
        except (TypeError, ValueError):
            pass
    info = ticker.get("info") or {}
    for k in ("quote_volume", "usdt_vol", "volume_24h_quote"):
        if info.get(k) is not None:
            try:
                return max(0.0, float(info[k]))
            except (TypeError, ValueError):
                pass
    return 0.0


def get_top_gate_perp_symbols(markets: dict, force_refresh: bool = False) -> list[str]:
    global cached_top_symbols, last_universe_refresh_ts
    now = time.monotonic()
    with universe_lock:
        if not force_refresh and cached_top_symbols and (now - last_universe_refresh_ts) < UNIVERSE_REFRESH_SEC:
            return list(cached_top_symbols)

        try:
            tickers = exchange.fetch_tickers()
        except Exception as e:
            logging.warning(f"Public fetch_tickers() failed ({e}); using cached/pinned symbols.")
            tickers = {}

        ranked: list[tuple[float, str]] = []
        for sym, m in (markets or {}).items():
            if not m.get("active") or not m.get("swap"):
                continue
            if m.get("linear") is False or m.get("settle") != "USDT" or m.get("quote") != "USDT":
                continue
            vol = _quote_volume_24h(tickers.get(sym))
            ranked.append((vol, sym))

        ranked.sort(key=lambda item: item[0], reverse=True)
        top_syms = [sym for _, sym in ranked[:TOP_N_COINS]]

        ordered: list[str] = []
        seen: set[str] = set()
        for sym in PINNED_SYMBOLS + top_syms:
            if sym in (markets or {}) and sym not in seen:
                seen.add(sym)
                ordered.append(sym)

        if ordered:
            cached_top_symbols = ordered
            last_universe_refresh_ts = now
            logging.info(f"Refreshed Gate Top {TOP_N_COINS} USDT perp universe: {len(ordered)} active symbols.")
        elif not cached_top_symbols:
            cached_top_symbols = list(PINNED_SYMBOLS)

        return list(cached_top_symbols)


def get_next_scan_slice(markets: dict) -> tuple[list[str], int, int]:
    top_symbols = get_top_gate_perp_symbols(markets)
    pinned_set = set(PINNED_SYMBOLS)

    pinned_active = [s for s in PINNED_SYMBOLS if s in markets and markets[s].get("active")]
    rotatable = [s for s in top_symbols if s not in pinned_set]

    if not rotatable:
        return pinned_active[:MAX_PAIRS_PER_CYCLE], 0, 0

    with state_lock:
        state = load_state()
        cursor = int(state.get("scan_cursor") or 0) % len(rotatable)

    slots_for_rotatable = max(1, MAX_PAIRS_PER_CYCLE - len(pinned_active))
    take = min(len(rotatable), slots_for_rotatable)

    slice_syms: list[str] = []
    for offset in range(take):
        slice_syms.append(rotatable[(cursor + offset) % len(rotatable)])

    next_cursor = (cursor + take) % len(rotatable)
    combined = pinned_active + slice_syms
    return combined, next_cursor, len(rotatable)


# ==========================================
# INDICATORS & PUBLIC MARKET DATA
# ==========================================
def calc_ema(values: list[float], period: int) -> float:
    mult = 2.0 / (period + 1)
    ema = float(values[0])
    for v in values[1:]:
        ema = (float(v) - ema) * mult + ema
    return ema


def calc_atr(ohlcv: list, period: int = 14) -> float:
    if len(ohlcv) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(ohlcv)):
        high = float(ohlcv[i][2])
        low = float(ohlcv[i][3])
        prev_close = float(ohlcv[i - 1][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    recent = trs[-period:]
    return sum(recent) / len(recent)


def fetch_market_snapshot(symbol: str):
    h1 = exchange.fetch_ohlcv(symbol, timeframe=TREND_TF, limit=EMA_SLOW_LEN + 15)
    m15 = exchange.fetch_ohlcv(symbol, timeframe=ENTRY_TF, limit=max(EMA_FAST_LEN, ATR_LEN) + 15)

    h1_closes = [float(c[4]) for c in h1]
    m15_closes = [float(c[4]) for c in m15]

    spot = m15_closes[-1]
    ema_50_1h = calc_ema(h1_closes, EMA_SLOW_LEN)
    ema_20_15m = calc_ema(m15_closes, EMA_FAST_LEN)
    atr_15m = calc_atr(m15, ATR_LEN)

    return {
        "symbol": symbol,
        "spot": spot,
        "ema_50_1h": ema_50_1h,
        "ema_20_15m": ema_20_15m,
        "atr_15m": atr_15m,
    }


def fetch_mark_price(symbol: str, fallback: float) -> float:
    ticker = exchange.fetch_ticker(symbol)
    px = ticker.get("last") or ticker.get("mark") or fallback
    return float(px)


def calc_unrealized_pnl(trade: dict, mark_price: float) -> float:
    entry = float(trade["entry_price"])
    amt = float(trade["amount"])
    contract_size = float(trade.get("contract_size", 1.0))
    if trade["side"] == "long":
        return (mark_price - entry) * amt * contract_size
    return (entry - mark_price) * amt * contract_size


def compute_order_amount(market: dict, symbol: str, spot: float, stop_distance: float) -> float:
    if stop_distance <= 0 or spot <= 0:
        return 0.0
    contract_size = float(market.get("contractSize") or 1.0)
    raw_qty = RISK_PER_TRADE_USDT / (stop_distance * contract_size)
    min_amt = float((market.get("limits") or {}).get("amount", {}).get("min") or 0.001)
    qty = max(min_amt, raw_qty)
    return float(exchange.amount_to_precision(symbol, qty))


def place_order(symbol: str, side: str, amount: float, ref_price: float | None = None) -> dict:
    if PAPER_MODE:
        px = float(ref_price) if ref_price and ref_price > 0 else fetch_mark_price(symbol, 0.0)
        return {
            "id": f"paper-{int(time.time() * 1000)}",
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "average": px,
            "status": "closed",
        }
    return exchange.create_order(symbol, "market", side, amount)


def _fmt_px(val: float) -> str:
    return f"{val:.4f}" if abs(val) < 50 else f"{val:.2f}"


# ==========================================
# TELEGRAM API, LIVE CARD & COMMANDS
# ==========================================
def _tg_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN)


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


def build_pinned_status_text(state: dict, stats: dict) -> str:
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    status_flag = "PAUSED" if state.get("paused") else "RUNNING"
    pin_sec = int(state.get("pin_interval_sec", PIN_INTERVAL_DEFAULT_SEC))
    trades = state.get("active_trades") or []
    last_scan = state.get("last_scan")

    if trades:
        lines = [f"<b>Active Positions ({len(trades)}/{MAX_OPEN_POSITIONS}):</b>"]
        total_u_pnl = 0.0
        for tr in trades:
            sym = tr["symbol"]
            try:
                mark = fetch_mark_price(sym, float(tr["entry_price"]))
                u_pnl = calc_unrealized_pnl(tr, mark)
                total_u_pnl += u_pnl
                pnl_part = f"PnL <code>{u_pnl:+.4f}</code> @ <code>{_fmt_px(mark)}</code>"
            except Exception as e:
                pnl_part = f"PnL <i>N/A ({e})</i>"
            lines.append(
                f"• <code>{sym}</code> {tr.get('side', '').upper()} <code>{tr.get('amount')}</code> "
                f"(Stop <code>{_fmt_px(float(tr.get('stop_price', 0.0)))}</code> / "
                f"TP <code>{_fmt_px(float(tr.get('tp_price', 0.0)))}</code>) → {pnl_part}"
            )
        lines.append(f"Total Unrealized: <code>{total_u_pnl:+.4f} USDT</code>")
        position_block = "\n".join(lines)
    else:
        scan_note = f"\n<i>Last Scan: {last_scan.get('reason')}</i>" if last_scan else ""
        position_block = f"<b>Active Positions:</b> 0/{MAX_OPEN_POSITIONS} (Flat / Scanning){scan_note}"

    total = int(stats.get("total_trades", 0))
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    win_rate = (wins / total * 100.0) if total > 0 else 0.0
    realized = float(stats.get("realized_pnl", 0.0))

    return (
        f"<b>ATR Top-{TOP_N_COINS} [{mode_label()}] [{status_flag}]</b>\n"
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


def close_single_position(trade: dict, reason: str, exit_price_hint: float | None = None) -> float:
    sym = trade["symbol"]
    close_side = "sell" if trade["side"] == "long" else "buy"
    ref_px = exit_price_hint if exit_price_hint is not None else fetch_mark_price(sym, float(trade["entry_price"]))
    order = place_order(sym, close_side, float(trade["amount"]), ref_price=ref_px)
    exit_px = float(order.get("average") or order.get("price") or ref_px)

    pnl = calc_unrealized_pnl(trade, exit_px)
    update_stats(pnl)
    record_closed_trade(trade, exit_px, pnl, reason)

    with state_lock:
        state = load_state()
        remaining = [
            t for t in (state.get("active_trades") or [])
            if not (t.get("symbol") == sym and t.get("opened_at") == trade.get("opened_at"))
        ]
        state["active_trades"] = remaining
        save_state(state)

    send_telegram(
        f"<b>Position Closed — {reason}</b>\n"
        f"Pair: <code>{sym}</code> | Side: <code>{trade['side'].upper()}</code> | Qty: <code>{trade['amount']}</code>\n"
        f"Entry: <code>{_fmt_px(float(trade['entry_price']))}</code> → Exit: <code>{_fmt_px(exit_px)}</code>\n"
        f"Realized PnL: <code>{pnl:+.4f} USDT</code>"
    )
    ensure_and_update_pinned_card(force_new=False)
    return pnl


def close_position_by_symbol(symbol: str, reason: str = "Manual Web UI Close") -> dict:
    with scan_lock:
        with state_lock:
            state = load_state()
            trades = list(state.get("active_trades") or [])
            target = next((t for t in trades if t.get("symbol") == symbol), None)

        if not target:
            return {"ok": True, "closed": False, "symbol": symbol, "detail": "Position not active"}

        pnl = close_single_position(target, reason)
        return {"ok": True, "closed": True, "symbol": symbol, "realized_pnl": round(pnl, 4)}


def close_all_positions_now(reason: str) -> tuple[int, float]:
    with state_lock:
        state = load_state()
        trades = list(state.get("active_trades") or [])

    if not trades:
        return 0, 0.0

    total_pnl = 0.0
    closed_count = 0
    for tr in trades:
        try:
            pnl = close_single_position(tr, reason)
            total_pnl += pnl
            closed_count += 1
        except Exception as e:
            logging.error(f"Failed closing {tr.get('symbol')}: {e}")
            send_telegram(f"<b>Error closing {tr.get('symbol')}:</b> <code>{e}</code>")

    return closed_count, total_pnl


def telegram_command_worker():
    if not _tg_configured():
        logging.warning("Telegram token not configured; command listener disabled.")
        return

    _tg_api("deleteWebhook", {"drop_pending_updates": False})
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
                logging.warning(f"Telegram getUpdates HTTP {resp.status_code}: {resp.text[:200]}")
                time.sleep(3)
                continue

            for upd in resp.json().get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message") or {}
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = (msg.get("text") or "").strip()

                if not chat_id or not text.startswith("/"):
                    continue

                cmd = text.split()[0].split("@")[0].lower()

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

                elif cmd == "/scannow":
                    send_telegram(
                        f"<b>Scanning Top {TOP_N_COINS} slice ({MAX_PAIRS_PER_CYCLE}/cycle)...</b> "
                        f"(Max concurrent: <code>{MAX_OPEN_POSITIONS}</code>)"
                    )
                    res = perform_scan_cycle()
                    scan = (res or {}).get("last_scan") or {}
                    pairs_txt = ""
                    for p in scan.get("pair_summaries") or []:
                        pct = p.get("open_readiness_pct")
                        pct_str = f" [{float(pct):.0f}%]" if pct is not None else ""
                        pairs_txt += f"\n• <code>{p.get('symbol')}</code>{pct_str}: {p.get('detail')}"
                    send_telegram(
                        f"<b>Scan [{scan.get('status', 'UNKNOWN')}]</b>\n"
                        f"Summary: {scan.get('reason', '--')}{pairs_txt}\n"
                        f"<i>{scan.get('checked_at_ist', '--')}</i>"
                    )

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
                    send_telegram("<b>Bot Resumed.</b> Scanning Top-100 slice for setups.")
                    ensure_and_update_pinned_card(force_new=False)

                elif cmd == "/setpinsec":
                    parts = text.split()
                    if len(parts) < 2 or not parts[1].isdigit():
                        send_telegram(f"Usage: <code>/setpinsec &lt;seconds&gt;</code> ({PIN_INTERVAL_MIN_SEC}–{PIN_INTERVAL_MAX_SEC}s)")
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
                        state["paused"] = True
                        save_state(state)
                    closed_cnt, total_pnl = close_all_positions_now("Emergency /closeall")
                    if closed_cnt == 0:
                        send_telegram("<b>/closeall:</b> No active positions open. Bot is now <b>PAUSED</b>.")
                    else:
                        send_telegram(
                            f"<b>Bot PAUSED</b> after closing {closed_cnt} position(s) "
                            f"(Total PnL: <code>{total_pnl:+.4f}</code>)."
                        )

        except Exception as e:
            logging.error(f"Telegram polling error: {e}")
            time.sleep(5)


# ==========================================
# SCAN & MONITOR CYCLE (ROTATING TOP-100)
# ==========================================
def perform_scan_cycle(markets: dict | None = None) -> dict:
    global cached_markets
    with scan_lock:
        active_markets = markets or cached_markets
        if not active_markets:
            active_markets = exchange.load_markets()
            cached_markets = active_markets

        try:
            with state_lock:
                state = load_state()
                trades = list(state.get("active_trades") or [])
                paused = bool(state.get("paused", False))

            updated_trades: list[dict] = []
            for tr in trades:
                sym = tr["symbol"]
                try:
                    snap = fetch_market_snapshot(sym)
                    spot = snap["spot"]
                    atr = max(snap["atr_15m"], float(tr.get("atr_at_entry") or 1.0))
                    side = tr["side"]
                    stop_px = float(tr["stop_price"])
                    tp_px = float(tr["tp_price"])

                    if side == "long":
                        new_stop = max(stop_px, spot - TRAIL_ATR_MULT * atr)
                        if new_stop > stop_px:
                            tr["stop_price"] = round(new_stop, 4)
                            stop_px = tr["stop_price"]
                    else:
                        new_stop = min(stop_px, spot + TRAIL_ATR_MULT * atr)
                        if new_stop < stop_px:
                            tr["stop_price"] = round(new_stop, 4)
                            stop_px = tr["stop_price"]

                    opened_dt = datetime.fromisoformat(tr["opened_at"])
                    held_hours = (datetime.now(timezone.utc) - opened_dt).total_seconds() / 3600.0

                    exit_reason = None
                    if side == "long":
                        if spot <= stop_px:
                            exit_reason = "ATR Trailing/Hard Stop"
                        elif spot >= tp_px:
                            exit_reason = f"Take Profit ({TP_ATR_MULT}x ATR)"
                    else:
                        if spot >= stop_px:
                            exit_reason = "ATR Trailing/Hard Stop"
                        elif spot <= tp_px:
                            exit_reason = f"Take Profit ({TP_ATR_MULT}x ATR)"

                    if not exit_reason and held_hours >= MAX_HOLD_HOURS:
                        exit_reason = f"Max Hold Time ({MAX_HOLD_HOURS:.0f}h)"

                    u_pnl = calc_unrealized_pnl(tr, spot)
                    if exit_reason:
                        close_single_position(tr, exit_reason, exit_price_hint=spot)
                    else:
                        logging.info(
                            f"Monitoring {sym} {side.upper()} | Spot={_fmt_px(spot)} "
                            f"Stop={_fmt_px(stop_px)} TP={_fmt_px(tp_px)} PnL={u_pnl:+.4f}"
                        )
                        updated_trades.append(tr)
                except Exception as e:
                    logging.error(f"Error monitoring {sym}: {e}")
                    updated_trades.append(tr)

            with state_lock:
                st = load_state()
                st["active_trades"] = updated_trades
                save_state(st)

            open_count = len(updated_trades)
            open_syms = {t["symbol"] for t in updated_trades}

            if paused:
                msg = f"Bot is PAUSED ({open_count}/{MAX_OPEN_POSITIONS} open). Skipping new entries."
                logging.info(f"[SCAN] {msg}")
                record_scan_diagnostics("PAUSED", msg)
            elif open_count >= MAX_OPEN_POSITIONS:
                msg = f"Max concurrent positions reached ({open_count}/{MAX_OPEN_POSITIONS}). Monitoring active trades."
                logging.info(f"[SCAN] {msg}")
                record_scan_diagnostics("MAX_POSITIONS", msg)
            else:
                scan_symbols, next_cursor, rotatable_total = get_next_scan_slice(active_markets)
                with state_lock:
                    st = load_state()
                    st["scan_cursor"] = next_cursor
                    save_state(st)

                pair_summaries: list[dict] = []
                opened_new = False
                checked_count = 0

                for sym in scan_symbols:
                    if sym in open_syms:
                        continue
                    if len(open_syms) >= MAX_OPEN_POSITIONS:
                        break

                    mkt = active_markets.get(sym)
                    if not mkt:
                        continue

                    try:
                        snap = fetch_market_snapshot(sym)
                    except Exception as e:
                        logging.debug(f"Skipping {sym} snapshot error: {e}")
                        continue

                    checked_count += 1
                    spot = snap["spot"]
                    ema_50 = snap["ema_50_1h"]
                    ema_20_15m = snap["ema_20_15m"]
                    atr = snap["atr_15m"]

                    if atr <= 0:
                        continue

                    band = max(1e-8, PULLBACK_ATR_BAND * atr)
                    trend_bullish = spot > ema_50
                    trend_bearish = spot < ema_50
                    dist_to_20 = abs(spot - ema_20_15m)

                    setup_side = None
                    if trend_bullish and dist_to_20 <= band and spot >= ema_20_15m * 0.997:
                        setup_side = "long"
                    elif trend_bearish and dist_to_20 <= band and spot <= ema_20_15m * 1.003:
                        setup_side = "short"

                    if not setup_side:
                        if trend_bullish or trend_bearish:
                            pullback_ratio = max(0.0, min(1.0, 1.0 - (dist_to_20 / (3.0 * band))))
                            readiness_pct = round(pullback_ratio * 99.0, 1)
                            regime_tag = "Bull 1h" if trend_bullish else "Bear 1h"
                        else:
                            readiness_pct = 0.0
                            regime_tag = "Flat 1h"

                        if len(pair_summaries) < 12:
                            pair_summaries.append({
                                "symbol": sym,
                                "open_readiness_pct": readiness_pct,
                                "detail": (
                                    f"{regime_tag} · Spot={_fmt_px(spot)} vs 1hEMA50={_fmt_px(ema_50)}, "
                                    f"15mEMA20={_fmt_px(ema_20_15m)}"
                                ),
                            })
                        continue

                    stop_dist = STOP_ATR_MULT * atr
                    amt = compute_order_amount(mkt, sym, spot, stop_dist)
                    if amt <= 0:
                        continue

                    order_side = "buy" if setup_side == "long" else "sell"
                    order = place_order(sym, order_side, amt, ref_price=spot)
                    entry_px = float(order.get("average") or order.get("price") or spot)
                    stop_px = entry_px - STOP_ATR_MULT * atr if setup_side == "long" else entry_px + STOP_ATR_MULT * atr
                    tp_px = entry_px + TP_ATR_MULT * atr if setup_side == "long" else entry_px - TP_ATR_MULT * atr

                    new_trade = {
                        "symbol": sym,
                        "side": setup_side,
                        "amount": amt,
                        "contract_size": float(mkt.get("contractSize") or 1.0),
                        "entry_price": round(entry_px, 4),
                        "stop_price": round(stop_px, 4),
                        "tp_price": round(tp_px, 4),
                        "atr_at_entry": round(atr, 4),
                        "opened_at": datetime.now(timezone.utc).isoformat(),
                    }
                    with state_lock:
                        st = load_state()
                        current_list = list(st.get("active_trades") or [])
                        current_list.append(new_trade)
                        st["active_trades"] = current_list
                        save_state(st)

                    open_syms.add(sym)
                    opened_new = True
                    pair_summaries.insert(0, {
                        "symbol": sym,
                        "open_readiness_pct": 100.0,
                        "detail": f"OPENED {setup_side.upper()} @ {_fmt_px(entry_px)}",
                    })

                    send_telegram(
                        f"<b>New {setup_side.upper()} Opened [{mode_label()}] ({len(open_syms)}/{MAX_OPEN_POSITIONS})</b>\n"
                        f"Pair: <code>{sym}</code> | Qty: <code>{amt}</code>\n"
                        f"Entry: <code>{_fmt_px(entry_px)}</code> | Stop: <code>{_fmt_px(stop_px)}</code> | "
                        f"TP: <code>{_fmt_px(tp_px)}</code>\n"
                        f"Risk Cap: <code>{RISK_PER_TRADE_USDT:.2f} USDT</code>"
                    )
                    ensure_and_update_pinned_card(force_new=False)

                    record_scan_diagnostics(
                        "OPENED",
                        f"Opened {setup_side.upper()} on {sym} ({len(open_syms)}/{MAX_OPEN_POSITIONS} active).",
                        symbol=sym,
                        spot=spot,
                        ema_50=ema_50,
                        atr_15m=atr,
                        setup_side=setup_side,
                        pair_summaries=pair_summaries[:12],
                    )

                if not opened_new:
                    summary_line = (
                        f"Rotated {checked_count} pairs (cursor->{next_cursor}/{rotatable_total} of Top {TOP_N_COINS}, "
                        f"{len(open_syms)}/{MAX_OPEN_POSITIONS} open) — no entry triggers."
                    )
                    logging.info(f"[SCAN] {summary_line}")
                    record_scan_diagnostics("SKIPPED", summary_line, pair_summaries=pair_summaries[:12])

        except Exception as e:
            logging.error(f"Scan cycle error: {e}")
            record_scan_diagnostics("ERROR", str(e))
            send_telegram(f"<b>Bot Loop Error:</b> <code>{e}</code>")

    with state_lock:
        return {"ok": True, "last_scan": load_state().get("last_scan")}


# ==========================================
# FASTAPI WEB UI & HEALTHZ
# ==========================================
app = FastAPI(title="ATR Top-100 Bot Dashboard")


class PinIntervalPayload(BaseModel):
    seconds: int


class CloseSymbolPayload(BaseModel):
    symbol: str


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
        st = load_state()
        paused = bool(st.get("paused", False))
        active_cnt = len(st.get("active_trades") or [])
    return {
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "mode": mode_label(),
        "has_api_keys": HAS_PRIVATE_KEYS,
        "port": WEB_UI_PORT,
        "top_n_coins": TOP_N_COINS,
        "max_pairs_per_cycle": MAX_PAIRS_PER_CYCLE,
        "max_open_positions": MAX_OPEN_POSITIONS,
        "active_positions_count": active_cnt,
        "paused": paused,
        "time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    }


@app.get("/api/state")
def api_state():
    with state_lock:
        state = load_state()
        stats = load_stats()
        history_raw = load_trade_history(limit=25)

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    trades = state.get("active_trades") or []
    live_trades = []
    total_unrealized = 0.0
    has_any_pnl = False

    for tr in trades:
        sym = tr["symbol"]
        try:
            mark = fetch_mark_price(sym, float(tr["entry_price"]))
            u_pnl = round(calc_unrealized_pnl(tr, mark), 4)
            total_unrealized += u_pnl
            has_any_pnl = True
        except Exception:
            mark, u_pnl = None, None

        opened_dt = datetime.fromisoformat(tr["opened_at"])
        elapsed_h = max(0.0, (datetime.now(timezone.utc) - opened_dt).total_seconds() / 3600.0)
        rem_h = max(0.0, MAX_HOLD_HOURS - elapsed_h)
        rem_pct = round(min(100.0, max(0.0, (rem_h / max(0.1, MAX_HOLD_HOURS)) * 100.0)), 1)

        live_trades.append({
            **tr,
            "mark_price": round(mark, 4) if mark is not None else None,
            "unrealized_pnl": u_pnl,
            "hours_left": round(rem_h, 2),
            "remaining_pct": rem_pct,
        })

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
        "mode": mode_label(),
        "has_api_keys": HAS_PRIVATE_KEYS,
        "top_n_coins": TOP_N_COINS,
        "max_pairs_per_cycle": MAX_PAIRS_PER_CYCLE,
        "pinned_symbols": PINNED_SYMBOLS,
        "max_open_positions": MAX_OPEN_POSITIONS,
        "paused": bool(state.get("paused", False)),
        "pin_interval_sec": int(state.get("pin_interval_sec", PIN_INTERVAL_DEFAULT_SEC)),
        "last_scan": state.get("last_scan"),
        "active_trades": live_trades,
        "total_unrealized_pnl": round(total_unrealized, 4) if has_any_pnl else None,
        "stats": stats,
        "trade_history": formatted_history,
    }


@app.post("/api/scan-now")
def api_scan_now(token: str | None = None):
    _verify_token(token)
    return perform_scan_cycle()


@app.post("/api/close-position")
def api_close_position(
    symbol: str | None = None,
    payload: CloseSymbolPayload | None = None,
    token: str | None = None,
):
    _verify_token(token)
    target_symbol = (symbol or (payload.symbol if payload else "") or "").strip()
    if not target_symbol:
        raise HTTPException(status_code=400, detail="Missing symbol")
    try:
        return close_position_by_symbol(target_symbol, "Manual Web UI Close")
    except Exception as e:
        logging.error(f"Manual close failed for {target_symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
        state["paused"] = True
        save_state(state)
    closed_cnt, total_pnl = close_all_positions_now("Emergency /closeall")
    ensure_and_update_pinned_card(force_new=False)
    return {"ok": True, "closed_count": closed_cnt, "realized_pnl": round(total_pnl, 4)}


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>ATR Top-100 Bot (Public API)</title>
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
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }
  .label { font-size: 0.78rem; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }
  .val { font-size: 1.4rem; font-weight: 700; margin-top: 6px; font-variant-numeric: tabular-nums; }
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
  button { cursor: pointer; border: none; border-radius: 8px; padding: 9px 14px; font-weight: 600; font-size: 0.85rem; }
  button:disabled { opacity: 0.6; cursor: not-allowed; }
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
      ATR Top-100 Pullback Bot
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
    <div class="label">Controls</div>
    <div class="controls">
      <button id="scanNowBtn" class="btn-primary" onclick="scanNow()">Scan Now</button>
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
          <tr><th>Entry → Exit (IST)</th><th>Pair</th><th>Side</th><th>Entry → Exit</th><th>Reason</th><th>Realized PnL</th></tr>
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

function fmtPx(v) {
  const n = Number(v);
  if (Number.isNaN(n)) return "--";
  return Math.abs(n) < 50 ? n.toFixed(4) : n.toFixed(2);
}

async function fetchState() {
  try {
    const res = await fetch("/api/state");
    const data = await res.json();
    document.getElementById("istClock").textContent = data.server_time_ist;
    document.getElementById("modeBadge").textContent = data.mode;
    const pinnedStr = (data.pinned_symbols || []).join(", ");
    document.getElementById("universeLabel").textContent =
      `Top ${data.top_n_coins || 100} Gate USDT Perps · ${data.max_pairs_per_cycle || 25}/cycle` + (pinnedStr ? ` · Pins: ${pinnedStr}` : "");

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
    document.getElementById("slotsVal").textContent = `${activeTrades.length} / ${maxPos}`;

    const scan = data.last_scan;
    if (scan) {
      document.getElementById("scanTime").textContent = scan.checked_at_ist || "--";
      const rEl = document.getElementById("scanReason");
      rEl.textContent = `[${scan.status || "INFO"}] ${scan.reason || ""}`;
      rEl.style.color = scan.status === "OPENED" ? "var(--green)" : (scan.status === "ERROR" ? "var(--red)" : "var(--amber)");
      const ps = scan.pair_summaries || [];
      document.getElementById("pairSummaries").innerHTML = ps.map(p => {
        const pct = Math.max(0, Math.min(100, Number(p.open_readiness_pct ?? 0)));
        return `<div class="coin-item">
          <div class="coin-top">
            <div><code>${p.symbol}</code> — <span>${p.detail}</span></div>
            <b>${pct.toFixed(0)}% to open</b>
          </div>
          <div class="coin-bar-bg"><div class="coin-bar-fill" style="width:${pct}%;"></div></div>
        </div>`;
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
    document.getElementById("winLoss").textContent = `${wins}W / ${losses}L (${wr}%)`;

    if (document.activeElement !== document.getElementById("pinSecInput")) {
      document.getElementById("pinSecInput").value = data.pin_interval_sec;
    }

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
        return `<div class="pos-item">
          <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">
            <div><code>${t.symbol}</code> <b>${(t.side || "").toUpperCase()}</b> × <code>${t.amount}</code></div>
            <div style="display:flex; align-items:center; gap:10px;">
              <span class="${uCls}"><b>PnL: ${(u >= 0 ? "+" : "") + u.toFixed(4)} USDT</b></span>
              <button class="btn-danger" style="padding:5px 10px; font-size:0.78rem;" onclick="closeSinglePosition(this, ${JSON.stringify(t.symbol)})">Close</button>
            </div>
          </div>
          <div style="display:flex; flex-wrap:wrap; gap:14px; margin-top:8px; font-size:0.84rem; color:var(--muted);">
            <span>Entry: <b style="color:var(--text);">${fmtPx(t.entry_price)}</b></span>
            <span>Mark: <b style="color:var(--text);">${t.mark_price !== null ? fmtPx(t.mark_price) : "--"}</b></span>
            <span>Stop: <b style="color:var(--amber);">${fmtPx(t.stop_price)}</b></span>
            <span>TP: <b style="color:var(--green);">${fmtPx(t.tp_price)}</b></span>
            <span style="margin-left:auto;">Hold left: <b>${t.hours_left}h</b> (${t.remaining_pct}%)</span>
          </div>
          <div class="bar-bg"><div class="bar-fill" style="width:${t.remaining_pct}%;"></div></div>
        </div>`;
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
        return `<tr>
          <td><code>${row.entry_ist}</code> → <code>${row.exit_ist}</code></td>
          <td><code>${row.symbol || "--"}</code></td>
          <td><b>${(row.side || "").toUpperCase()}</b></td>
          <td>${fmtPx(row.entry_price)} → ${fmtPx(row.exit_price)}</td>
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

async function closeSinglePosition(btn, symbol) {
  if (!confirm(`Close ${symbol} position now? Bot will remain running.`)) return;
  const orig = btn.textContent;
  btn.disabled = true;
  btn.textContent = "Closing...";
  try {
    const qs = new URLSearchParams({ symbol });
    if (token) qs.set("token", token);
    const res = await fetch(`/api/close-position?${qs.toString()}`, { method: "POST" });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      alert(err.detail || `Failed to close ${symbol}`);
    }
    await fetchState();
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
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
async function updatePinSec() {
  const sec = parseInt(document.getElementById("pinSecInput").value, 10);
  if (!sec || sec < 10) return;
  await fetch(apiUrl("/api/setpinsec"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ seconds: sec }) });
  fetchState();
}
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
    global cached_markets
    threading.Thread(target=telegram_command_worker, daemon=True).start()
    threading.Thread(target=pinned_card_worker, daemon=True).start()
    threading.Thread(target=start_web_ui, daemon=True).start()

    cached_markets = exchange.load_markets()
    get_top_gate_perp_symbols(cached_markets, force_refresh=True)
    state = load_state()

    send_telegram(
        f"<b>ATR Top-{TOP_N_COINS} Bot Started [{mode_label()}]</b>\n"
        f"Max Positions: <code>{MAX_OPEN_POSITIONS}</code> | Slice: <code>{MAX_PAIRS_PER_CYCLE}/cycle</code>\n"
        f"Risk/Trade: <code>{RISK_PER_TRADE_USDT:.2f} USDT</code> | Port: <code>{WEB_UI_PORT}</code>"
    )
    ensure_and_update_pinned_card(force_new=False)
    logging.info(f"ATR Top-{TOP_N_COINS} Bot started in {mode_label()} mode on port {WEB_UI_PORT}.")

    while True:
        perform_scan_cycle(cached_markets)
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_bot()
