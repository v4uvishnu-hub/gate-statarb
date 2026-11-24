import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import redis.asyncio as aioredis
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
MAX_HOLD_MINUTES = int(os.getenv("MAX_HOLD_MINUTES", "720"))
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
NOTIONAL_USDT = float(os.getenv("NOTIONAL_USDT", "100"))
SIM_START_EQUITY = float(os.getenv("SIM_START_EQUITY", "1000"))
TAKER_FEE_RATE = float(os.getenv("TAKER_FEE_RATE", "0.0005"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# Local fallback + Redis persistence
PERSIST_STATE = os.getenv("PERSIST_STATE", "true").lower() == "true"
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
RESET_STATE = os.getenv("RESET_STATE", "false").lower() == "true"

REDIS_URL = os.getenv("REDIS_URL", "").strip()
REDIS_STATE_KEY = os.getenv("REDIS_STATE_KEY", "gate_arb:state:v1").strip()
_redis_client: aioredis.Redis | None = None
_trader_task: asyncio.Task | None = None
_active_exchange = None

GATE_FUTURES_BASE = "https://api.gateio.ws/api/v4/futures/usdt"

engine = StatArbEngine(
    lookback=int(os.getenv("LOOKBACK", "100")),
    z_entry=float(os.getenv("Z_ENTRY", "2.0")),
    z_exit=float(os.getenv("Z_EXIT", "0.2")),
    z_stop=float(os.getenv("Z_STOP", "3.8")),
    notional_usdt=NOTIONAL_USDT,
    round_trip_fee_rate=TAKER_FEE_RATE * 4.0,
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


def redis_enabled() -> bool:
    return bool(REDIS_URL)


async def get_redis() -> aioredis.Redis | None:
    global _redis_client
    if not REDIS_URL:
        return None
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    return _redis_client


def build_state_payload() -> dict:
    return {
        "saved_at_ist": now_ist().isoformat(),
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


def apply_saved_payload(saved: dict, source_label: str):
    if not isinstance(saved, dict) or not saved:
        return
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
    state["unrealized_pnl"] = round(
        sum(float(p.get("unrealized_pnl") or 0.0) for p in state["positions"]), 3
    )
    log.info(
        f"Restored state from {source_label}: {len(state['positions'])} open pos, "
        f"{len(state['trades'])} trades, equity={state['sim_equity']:.2f}"
    )


def save_local_state(payload: dict):
    if not PERSIST_STATE:
        return
    try:
        parent = os.path.dirname(STATE_FILE)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp_path = f"{STATE_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, STATE_FILE)
    except Exception as e:
        log.error(f"Failed to save local state to {STATE_FILE}: {e}")


def load_local_state() -> bool:
    if not PERSIST_STATE or RESET_STATE:
        if RESET_STATE and os.path.exists(STATE_FILE):
            try:
                os.remove(STATE_FILE)
            except Exception:
                pass
        return False
    if not os.path.exists(STATE_FILE):
        return False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict) and ("positions" in saved or "trades" in saved or "sim_equity" in saved):
            apply_saved_payload(saved, f"local:{STATE_FILE}")
            return True
    except Exception as e:
        log.error(f"Failed to load local state from {STATE_FILE}: {e}")
    return False


async def save_redis_state(payload: dict):
    global _redis_client
    client = await get_redis()
    if not client:
        return
    try:
        await client.ping()
        await client.set(REDIS_STATE_KEY, json.dumps(payload))
        log.info(
            f"Saved state to Redis key '{REDIS_STATE_KEY}' "
            f"(pos={len(payload.get('positions', []))}, trades={len(payload.get('trades', []))}, eq={payload.get('sim_equity')})"
        )
    except Exception as e:
        log.error(f"Redis save failed for key '{REDIS_STATE_KEY}': {repr(e)}")
        try:
            await client.aclose()
        except Exception:
            pass
        _redis_client = None


async def load_redis_state() -> bool:
    global _redis_client
    client = await get_redis()
    if not client:
        return False
    try:
        await client.ping()
        if RESET_STATE:
            await client.delete(REDIS_STATE_KEY)
            log.info(f"RESET_STATE=true: deleted Redis key '{REDIS_STATE_KEY}'")
            return False
        raw = await client.get(REDIS_STATE_KEY)
        if not raw:
            log.info(f"Redis key '{REDIS_STATE_KEY}' is empty; nothing to restore yet.")
            return False
        saved = json.loads(raw)
        if isinstance(saved, dict) and ("positions" in saved or "trades" in saved or "sim_equity" in saved):
            apply_saved_payload(saved, f"redis:{REDIS_STATE_KEY}")
            save_local_state(saved)
            return True
    except Exception as e:
        log.error(f"Redis load failed for key '{REDIS_STATE_KEY}': {repr(e)}")
        try:
            await client.aclose()
        except Exception:
            pass
        _redis_client = None
    return False


async def persist_now(sync_remote: bool = False):
    payload = build_state_payload()
    save_local_state(payload)
    if sync_remote and redis_enabled():
        await save_redis_state(payload)


def add_event(msg: str, persist: bool = False, sync_remote: bool = False):
    ts = now_ist().strftime("%H:%M:%S IST")
    state["events"].insert(0, f"[{ts}] {msg}")
    state["events"] = state["events"][:25]
    if persist or sync_remote:
        asyncio.create_task(persist_now(sync_remote=sync_remote))


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
    qy = float(pos.get("qty_y") or 0.0)
    qx = float(pos.get("qty_x") or 0.0)
    ey = float(pos.get("entry_y") or 0.0)
    ex_p = float(pos.get("entry_x") or 0.0)
    if pos.get("side") == "LONG_SPREAD":
        pnl_y = (price_y - ey) * qy
        pnl_x = (ex_p - price_x) * qx
    else:
        pnl_y = (ey - price_y) * qy
        pnl_x = (price_x - ex_p) * qx
    return float(pnl_y + pnl_x)


def compute_position_live_edge(pos: dict, m: dict, cy: float, cx: float) -> tuple[float, float]:
    z_now = float(m["z_score"])
    spread_std = float(m.get("spread_std") or 0.01)
    if pos.get("side") == "LONG_SPREAD":
        revert_z = max(0.0, (-engine.z_exit) - z_now)
    else:
        revert_z = max(0.0, z_now - engine.z_exit)
    gross_pct = (np.exp(min(revert_z * spread_std, 1.0)) - 1.0) * 100.0
    exit_fee_pct = TAKER_FEE_RATE * 2.0 * 100.0
    edge_pct = float(gross_pct - exit_fee_pct)
    qy = float(pos.get("qty_y") or 0.0)
    qx = float(pos.get("qty_x") or 0.0)
    notional_now = qy * cy + qx * cx
    edge_usdt = float((edge_pct / 100.0) * notional_now)
    return round(edge_pct, 3), round(edge_usdt, 3)


def enrich_position_timers():
    now_ts = now_ist().timestamp()
    max_sec = max(60, MAX_HOLD_MINUTES * 60)
    for pos in state["positions"]:
        entry_ts = float(pos.get("entry_ts") or now_ts)
        pos["entry_ts"] = entry_ts
        pos["max_hold_min"] = MAX_HOLD_MINUTES
        elapsed_sec = max(0.0, now_ts - entry_ts)
        rem_sec = max(0.0, max_sec - elapsed_sec)
        pos["remaining_sec"] = int(rem_sec)
        pos["remaining_pct"] = round(min(100.0, max(0.0, (rem_sec / max_sec) * 100.0)), 1)


def is_expired(pos: dict) -> bool:
    enrich_position_timers()
    return pos.get("remaining_sec", 1) <= 0


def refresh_positions_from_ohlcv(ohlcv_map: dict[str, pd.DataFrame]):
    total_u = 0.0
    for pos in state["positions"]:
        py, px = pos["pair_y"], pos["pair_x"]
        cy = float(pos.get("current_y") or pos.get("entry_y") or 0.0)
        cx = float(pos.get("current_x") or pos.get("entry_x") or 0.0)
        if py in ohlcv_map and px in ohlcv_map:
            m = engine.compute_metrics(ohlcv_map[py], ohlcv_map[px])
            cy = float(ohlcv_map[py]["close"].iloc[-1])
            cx = float(ohlcv_map[px]["close"].iloc[-1])
            pos["current_y"] = cy
            pos["current_x"] = cx
            pos["current_z"] = round(m["z_score"], 2)
            pos["beta"] = round(m["beta"], 3)
            pos["adf_stat"] = round(m["adf_stat"], 2)
            pos["half_life"] = round(m["half_life"], 1)
            pos["corr"] = round(m["corr"], 2)
            e_pct, e_usdt = compute_position_live_edge(pos, m, cy, cx)
            pos["edge_pct"] = e_pct
            pos["edge_usdt"] = e_usdt
        else:
            if py in ohlcv_map:
                cy = float(ohlcv_map[py]["close"].iloc[-1])
                pos["current_y"] = cy
            if px in ohlcv_map:
                cx = float(ohlcv_map[px]["close"].iloc[-1])
                pos["current_x"] = cx
        if cy > 0 and cx > 0:
            u_pnl = leg_unrealized_pnl(pos, cy, cx)
            pos["unrealized_pnl"] = round(u_pnl, 3)
        else:
            pos["unrealized_pnl"] = round(float(pos.get("unrealized_pnl") or 0.0), 3)
        total_u += float(pos["unrealized_pnl"])
    state["unrealized_pnl"] = round(float(total_u), 3)
    enrich_position_timers()


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

    now_dt = now_ist()
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
        "beta": round(m["beta"], 3),
        "adf_stat": round(m["adf_stat"], 2),
        "half_life": round(m["half_life"], 1),
        "corr": round(m["corr"], 2),
        "edge_pct": m.get("edge_pct", 0.0),
        "edge_usdt": m.get("edge_usdt", 0.0),
        "unrealized_pnl": 0.0,
        "entry_ts": now_dt.timestamp(),
        "max_hold_min": MAX_HOLD_MINUTES,
        "remaining_sec": MAX_HOLD_MINUTES * 60,
        "remaining_pct": 100.0,
        "entry_time": now_dt.strftime("%d-%m %H:%M IST"),
    }
    state["positions"].append(pos_rec)
    state["active_pair_y"] = pair_y
    state["active_pair_x"] = pair_x
    await persist_now(sync_remote=True)
    return pos_rec


async def close_single_position(
    ex,
    pos: dict,
    price_y: float | None = None,
    price_x: float | None = None,
    exit_z: float = 0.0,
    reason: str = "SIGNAL",
    sync_remote: bool = True,
) -> dict:
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
        "edge_pct": pos.get("edge_pct", 0.0),
        "pnl": round(net_pnl, 3),
        "reason": reason,
    }
    state["trades"].insert(0, trade_rec)
    state["trades"] = state["trades"][:40]

    state["positions"] = [p for p in state["positions"] if p is not pos]
    if sync_remote:
        await persist_now(sync_remote=True)
    return trade_rec


async def close_all_positions(ex, reason: str = "MANUAL") -> list[dict]:
    closed = []
    for pos in list(state["positions"]):
        tr = await close_single_position(ex, pos, reason=reason, sync_remote=False)
        closed.append(tr)
    state["unrealized_pnl"] = 0.0
    await persist_now(sync_remote=True)
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
    .wrap { max-width: 1240px; margin: 0 auto; }
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
    .edge-normal { color: #e2e8f0; }
    .rem-box { min-width: 135px; }
    .rem-text { font-family: monospace; font-size: 11px; color: #cbd5e1; margin-bottom: 4px; display: flex; justify-content: space-between; }
    .rem-track { width: 100%; height: 8px; background: #0f172a; border-radius: 999px; overflow: hidden; border: 1px solid #334155; }
    .rem-fill { height: 100%; border-radius: 999px; transition: width 0.5s linear, background-color 0.3s ease; }
    .table-wrap { overflow-x: auto; margin-bottom: 16px; border-radius: 10px; border: 1px solid #334155; background: #1e293b; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; white-space: nowrap; }
    th, td { padding: 8px 9px; text-align: left; border-bottom: 1px solid #334155; vertical-align: middle; }
    th { color: #94a3b8; font-weight: 600; background: #162032; }
    ul { list-style: none; padding: 12px; margin: 0; background: #1e293b; border-radius: 10px; border: 1px solid #334155; font-family: monospace; font-size: 12px; max-height: 210px; overflow-y: auto; }
    li { padding: 4px 0; border-bottom: 1px solid #334155; }
    li:last-child { border-bottom: none; }
    @media (max-width: 820px) { .status-meta { margin-left: 0; } }
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

  <h4 style="margin:0 0 8px 0;color:#94a3b8">Open Positions (Edge & All Metrics)</h4>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Pair</th><th>Side</th><th>Qty Y / X</th><th>Entry Y / X</th><th>Now Y / X</th>
          <th>Z (In→Now)</th><th>Edge (% / $)</th><th>uPnL ($)</th><th>Beta</th><th>ADF</th>
          <th>HL</th><th>Corr</th><th>Remaining Time</th><th>Opened (IST)</th>
        </tr>
      </thead>
      <tbody id="openPos"><tr><td colspan="14">No open positions</td></tr></tbody>
    </table>
  </div>

  <h4 style="margin:0 0 8px 0;color:#94a3b8">Top Ranked Pairs (Edge & All Metrics)</h4>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Pair</th><th>Status</th><th>Z-Score</th><th>Edge (% / $)</th><th>Price Y / X</th>
          <th>Beta</th><th>Alpha</th><th>Spread / Mean</th><th>ADF</th><th>HL</th><th>Corr</th><th>Score</th>
        </tr>
      </thead>
      <tbody id="topPairs"><tr><td colspan="12">Scanning...</td></tr></tbody>
    </table>
  </div>

  <h4 style="margin:0 0 8px 0;color:#94a3b8">Simulated Trade History (IST)</h4>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Time (IST)</th><th>Pair</th><th>Side</th><th>Z In/Out</th><th>Edge %</th><th>PnL ($)</th><th>Reason</th></tr></thead>
      <tbody id="trades"><tr><td colspan="7">No trades yet</td></tr></tbody>
    </table>
  </div>

  <h4 style="margin:0 0 8px 0;color:#94a3b8">Recent Activity (IST)</h4>
  <ul id="events"><li>Starting fast scan...</li></ul>
</div>
<script>
const REFRESH_INTERVAL_SEC = 3;
let remainingSec = REFRESH_INTERVAL_SEC;
let latestPositions = [];

function fmtSigned(v, dec=2, suffix='') {
  const num = Number(v || 0);
  if (!Number.isFinite(num)) return `0.${'0'.repeat(dec)}${suffix}`;
  const cls = num > 0 ? 'pos-green' : (num < 0 ? 'neg-red' : '');
  return `<span class="${cls}">${num >= 0 ? '+' : ''}${num.toFixed(dec)}${suffix}</span>`;
}

function fmtNeutral(v, dec=2, suffix='') {
  const num = Number(v);
  if (!Number.isFinite(num)) return `<span class="edge-normal">0.${'0'.repeat(dec)}${suffix}</span>`;
  return `<span class="edge-normal">${num >= 0 ? '+' : ''}${num.toFixed(dec)}${suffix}</span>`;
}

function fmtPrice(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n) || n === 0) return '-';
  if (n >= 1000) return n.toFixed(1);
  if (n >= 1) return n.toFixed(3);
  return n.toFixed(5);
}

function fmtDuration(sec) {
  const s = Math.max(0, Math.floor(sec));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${r}s`;
  return `${r}s`;
}

function renderRemainingBar(p) {
  const maxSec = Math.max(60, (p.max_hold_min || 720) * 60);
  const remSec = Math.max(0, Number(p.remaining_sec ?? maxSec));
  const pct = Math.min(100, Math.max(0, (remSec / maxSec) * 100));
  const color = pct > 50 ? '#22c55e' : (pct > 20 ? '#f59e0b' : '#ef4444');
  return `
    <div class="rem-box">
      <div class="rem-text"><span>${fmtDuration(remSec)}</span><span>${pct.toFixed(0)}%</span></div>
      <div class="rem-track"><div class="rem-fill" style="width:${pct.toFixed(1)}%;background:${color};"></div></div>
    </div>
  `;
}

function renderOpenPositions() {
  const tbody = document.getElementById('openPos');
  if (!latestPositions || latestPositions.length === 0) {
    tbody.innerHTML = '<tr><td colspan="14">No open positions</td></tr>';
    return;
  }
  tbody.innerHTML = latestPositions.map(p => `
    <tr>
      <td><b>${p.label}</b></td>
      <td>${(p.side || '').replace('_SPREAD','')}</td>
      <td>${p.qty_y} / ${p.qty_x}</td>
      <td>${fmtPrice(p.entry_y)} / ${fmtPrice(p.entry_x)}</td>
      <td>${fmtPrice(p.current_y)} / ${fmtPrice(p.current_x)}</td>
      <td>${p.entry_z} → ${p.current_z}</td>
      <td>${fmtNeutral(p.edge_pct, 2, '%')} (${fmtNeutral(p.edge_usdt, 2, '$')})</td>
      <td>${fmtSigned(p.unrealized_pnl, 2)}</td>
      <td>${Number(p.beta || 0).toFixed(3)}</td>
      <td>${Number(p.adf_stat || 0).toFixed(2)}</td>
      <td>${Number(p.half_life || 0).toFixed(0)}b</td>
      <td>${Number(p.corr || 0).toFixed(2)}</td>
      <td>${renderRemainingBar(p)}</td>
      <td>${p.entry_time}</td>
    </tr>
  `).join('');
}

function tickLocalRemainingTimers() {
  if (!latestPositions || latestPositions.length === 0) return;
  latestPositions.forEach(p => {
    if (typeof p.remaining_sec === 'number') {
      p.remaining_sec = Math.max(0, p.remaining_sec - 1);
    }
  });
  renderOpenPositions();
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
  latestPositions = s.positions || [];
  document.getElementById('pair').textContent = `Focus: ${s.pair_y} vs ${s.pair_x} (${s.timeframe}) • ${s.scan_status}`;
  const badge = document.getElementById('mode');
  badge.textContent = s.dry_run ? 'SIMULATED PAPER' : 'LIVE TRADING';
  badge.className = 'badge ' + (s.dry_run ? 'dry' : 'live');
  document.getElementById('eq').textContent = Number(s.sim_equity || 0).toFixed(2);
  document.getElementById('pnl').innerHTML = `${fmtSigned(s.realized_pnl, 2)} / ${fmtSigned(s.unrealized_pnl, 2)}`;
  document.getElementById('openCount').textContent = `${latestPositions.length} / ${s.max_positions}`;
  document.getElementById('readyCount').textContent = `${s.ready_count} ready`;
  document.getElementById('wl').textContent = `${s.wins} / ${s.losses}`;
  document.getElementById('univ').textContent = `${s.universe_count}c / ${s.last_scan_sec}s`;
  document.getElementById('btnPause').disabled = !s.running;
  document.getElementById('btnResume').disabled = s.running;
  document.getElementById('btnClose').disabled = latestPositions.length === 0;

  renderOpenPositions();

  if (s.top_pairs && s.top_pairs.length) {
    document.getElementById('topPairs').innerHTML = s.top_pairs.map(p => {
      const tag = p.entry_ready
        ? '<span class="ready-pill">ENTRY MET</span>'
        : (p.is_cointegrated ? '<span class="watch-pill">COINT</span>' : '<span class="watch-pill">WATCH</span>');
      return `
        <tr>
          <td><b>${p.pair_y.split('/')[0]}/${p.pair_x.split('/')[0]}</b></td>
          <td>${tag}</td>
          <td>${Number(p.z_score || 0).toFixed(2)}</td>
          <td>${fmtNeutral(p.edge_pct, 2, '%')} (${fmtNeutral(p.edge_usdt, 2, '$')})</td>
          <td>${fmtPrice(p.price_y)} / ${fmtPrice(p.price_x)}</td>
          <td>${Number(p.beta || 0).toFixed(3)}</td>
          <td>${Number(p.alpha || 0).toFixed(3)}</td>
          <td>${Number(p.spread_now || 0).toFixed(3)} / ${Number(p.spread_mean || 0).toFixed(3)}</td>
          <td>${Number(p.adf_stat || 0).toFixed(2)}</td>
          <td>${Number(p.half_life || 0).toFixed(0)}b</td>
          <td>${Number(p.corr || 0).toFixed(2)}</td>
          <td>${Number(p.score || 0).toFixed(2)}</td>
        </tr>
      `;
    }).join('');
  }
  if (s.trades && s.trades.length) {
    document.getElementById('trades').innerHTML = s.trades.map(t =>
      `<tr><td>${t.time}</td><td>${t.pair}</td><td>${t.side.replace('_SPREAD','')}</td><td>${t.entry_z} → ${t.exit_z}</td><td>${fmtNeutral(t.edge_pct, 2, '%')}</td><td>${fmtSigned(t.pnl, 2)}</td><td>${t.reason || '-'}</td></tr>`
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
  tickLocalRemainingTimers();
}, 1000);
setInterval(refresh, REFRESH_INTERVAL_SEC * 1000);
</script>
</body>
</html>"""


def snapshot_state() -> dict:
    enrich_position_timers()
    return {
        "running": state["running"],
        "persist_state": PERSIST_STATE,
        "redis_enabled": redis_enabled(),
        "state_file": STATE_FILE,
        "max_hold_min": MAX_HOLD_MINUTES,
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
    return web.json_response({"status": "ok", "redis": redis_enabled()})


async def handle_api_state(request: web.Request):
    return web.json_response(snapshot_state())


async def handle_api_pause(request: web.Request):
    state["running"] = False
    add_event("Paused via Web Dashboard")
    await persist_now(sync_remote=True)
    return web.json_response({"ok": True, "state": snapshot_state()})


async def handle_api_resume(request: web.Request):
    state["running"] = True
    add_event("Resumed via Web Dashboard")
    await persist_now(sync_remote=True)
    return web.json_response({"ok": True, "state": snapshot_state()})


async def handle_api_closeall(request: web.Request):
    ex = make_exchange()
    try:
        closed = await close_all_positions(ex, reason="MANUAL_WEB")
        if closed:
            total_p = sum(t["pnl"] for t in closed)
            add_event(f"Closed {len(closed)} position(s) via Web | Net PnL: {total_p:+.2f} USDT")
            await persist_now(sync_remote=True)
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
    enrich_position_timers()
    m = state["metrics"]
    pos_lines = [
        f"• {p['label']} ({p['side']}): Z {p['entry_z']}→{p['current_z']} | Edge {p.get('edge_pct',0):+.2f}% ({p.get('edge_usdt',0):+.2f}$) | uPnL {p['unrealized_pnl']:+.2f} | β={p.get('beta',0)} | ADF={p.get('adf_stat',0)} | HL={p.get('half_life',0)}b | Rem {p['remaining_sec']//60}m"
        for p in state["positions"]
    ]
    pos_block = "\n".join(pos_lines) if pos_lines else "None (FLAT)"
    z_txt = f"{m['z_score']:.2f} (±{engine.z_entry:.2f}) | Edge {m.get('edge_pct',0):+.2f}% | ADF: {m['adf_stat']:.2f}" if m else state["scan_status"]
    await update.message.reply_text(
        f"Mode: {'SIMULATOR' if DRY_RUN else 'LIVE'} | {'ON' if state['running'] else 'PAUSED'} | Redis: {'ON' if redis_enabled() else 'OFF'}\n"
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
        lines.append(
            f"• {tag} {y_n}/{x_n}: Z={p['z_score']:+.2f} | Edge={p.get('edge_pct',0):+.2f}% ({p.get('edge_usdt',0):+.2f}$) | "
            f"β={p['beta']:.2f} | ADF={p['adf_stat']:.2f} | HL={p['half_life']:.0f}b | Corr={p['corr']:.2f} | Sc={p['score']:.2f}"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    if not state["trades"]:
        await update.message.reply_text("No closed simulated trades yet.")
        return
    lines = ["Recent Simulated Trades (IST):"]
    for t in state["trades"][:8]:
        lines.append(f"• {t['time']} {t['pair']} {t['side']} ({t['entry_z']}→{t['exit_z']}) Edge {t.get('edge_pct',0):+.2f}%: {t['pnl']:+.2f} USDT [{t.get('reason','')}]")
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
            await update.message.reply_text(f"Enter a value greater than 0 and less than {engine.z_stop}.")
            return
        engine.z_entry = val
        add_event(f"Z-entry updated to ±{val:.2f}")
        await persist_now(sync_remote=True)
        await update.message.reply_text(f"Z-score entry threshold set to ±{val:.2f}")
    except (IndexError, ValueError):
        await update.message.reply_text(f"Usage: /setz 2.0 (Current: ±{engine.z_entry:.2f})")


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = False
    add_event("Paused via Telegram")
    await persist_now(sync_remote=True)
    await update.message.reply_text("Paused.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    state["running"] = True
    add_event("Resumed via Telegram")
    await persist_now(sync_remote=True)
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
        await persist_now(sync_remote=True)
        await update.message.reply_text(msg)
    finally:
        await ex.close()


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not auth(update):
        return
    await update.message.reply_text(
        f"Commands (Auto-selecting top coins, max {MAX_POSITIONS} pairs):\n"
        "/status - Open positions, Edge, Z-score, metrics, remaining time\n"
        "/pnl - Simulated equity, realized PnL, win/loss stats\n"
        "/top - Top ranked pairs with Edge & all metrics\n"
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
    global _active_exchange
    ex = make_exchange()
    _active_exchange = ex
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
                            cy = float(ohlcv_map[py]["close"].iloc[-1])
                            cx = float(ohlcv_map[px]["close"].iloc[-1])
                            pos["current_z"] = round(m["z_score"], 2)
                            pos["current_y"] = cy
                            pos["current_x"] = cx
                            pos["beta"] = round(m["beta"], 3)
                            pos["adf_stat"] = round(m["adf_stat"], 2)
                            pos["half_life"] = round(m["half_life"], 1)
                            pos["corr"] = round(m["corr"], 2)
                            e_pct, e_usdt = compute_position_live_edge(pos, m, cy, cx)
                            pos["edge_pct"] = e_pct
                            pos["edge_usdt"] = e_usdt
                            pos["unrealized_pnl"] = round(leg_unrealized_pnl(pos, cy, cx), 3)

                            timed_out = is_expired(pos)
                            sig = engine.get_signal(m, pos["side"])
                            if state["running"] and (sig == "CLOSE" or timed_out):
                                if timed_out:
                                    reason = "MAX_HOLD_TIME"
                                elif abs(m["z_score"]) >= engine.z_stop:
                                    reason = "STOP_LOSS"
                                else:
                                    reason = "MEAN_REVERT"
                                tr = await close_single_position(
                                    ex, pos, m["price_y"], m["price_x"], m["z_score"], reason=reason, sync_remote=True
                                )
                                msg = (
                                    f"[SIM CLOSED - {reason}] {tr['pair']} ({tr['side']}) • {tr['time']} IST\n"
                                    f"Z: {tr['entry_z']} → {tr['exit_z']} | Edge: {tr.get('edge_pct',0):+.2f}% | Net PnL: {tr['pnl']:+.2f} USDT\n"
                                    f"Sim Equity: {state['sim_equity']:.2f} USDT"
                                )
                                add_event(f"CLOSED {tr['pair']} {tr['pnl']:+.2f} USDT ({reason})")
                                await tg_app.bot.send_message(ADMIN_ID, msg)

                    refresh_positions_from_ohlcv(ohlcv_map)

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
                                    f"Z: {cand['z_score']:+.2f} | Edge: {cand['edge_pct']:+.2f}% ({cand['edge_usdt']:+.2f}$)\n"
                                    f"β={cand['beta']:.3f} | ADF={cand['adf_stat']:.2f} | HL={cand['half_life']:.0f}b | Corr={cand['corr']:.2f}\n"
                                    f"{pos_rec['side_y'].upper()} {pos_rec['qty_y']} {cand['pair_y']} @ {cand['price_y']}\n"
                                    f"{pos_rec['side_x'].upper()} {pos_rec['qty_x']} {cand['pair_x']} @ {cand['price_x']}"
                                )
                                add_event(f"AUTO OPENED {sig} {pos_rec['label']} @ Z={cand['z_score']:+.2f} Edge={cand['edge_pct']:+.2f}%")
                                await tg_app.bot.send_message(ADMIN_ID, msg)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.error(f"Loop error: {e}", exc_info=True)
                    state["scan_status"] = f"Error: {e}"
                    add_event(f"Loop error: {e}", persist=False)

                await asyncio.sleep(POLL_SECONDS)
    except asyncio.CancelledError:
        log.info("trader_loop cancelled cleanly.")
    finally:
        try:
            await ex.close()
        except Exception:
            pass
        if _active_exchange is ex:
            _active_exchange = None


async def post_init(tg_app: Application):
    global _trader_task
    loaded = await load_redis_state()
    if not loaded:
        load_local_state()
    await start_web()
    _trader_task = asyncio.create_task(trader_loop(tg_app))


async def post_stop(tg_app: Application):
    global _trader_task, _active_exchange, _redis_client
    if _trader_task and not _trader_task.done():
        _trader_task.cancel()
        try:
            await _trader_task
        except asyncio.CancelledError:
            pass
    _trader_task = None

    if _active_exchange is not None:
        try:
            await _active_exchange.close()
        except Exception:
            pass
        _active_exchange = None

    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception:
            pass
        _redis_client = None


def main():
    tg_app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_stop(post_stop)
        .build()
    )
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
