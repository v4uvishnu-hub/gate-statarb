import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web
import numpy as np
import redis.asyncio as redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("gate_statarb")

IST = timezone(timedelta(hours=5, minutes=30))
GATE_BASE = "https://api.gateio.ws/api/v4"
STATE_FILE = "bot_state.json"
REDIS_STATE_KEY = "gate_statarb:state_v1"


def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() not in ("0", "false", "no", "off", "")


def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except (ValueError, TypeError):
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


def get_env_config_defaults() -> Dict[str, Any]:
    return {
        "DRY_RUN": env_bool("DRY_RUN", True),
        "TOP_N_COINS": max(10, min(200, env_int("TOP_N_COINS", 100))),
        "LOOKBACK": max(30, min(300, env_int("LOOKBACK", 100))),
        "MAX_POSITIONS": max(1, min(10, env_int("MAX_POSITIONS", 3))),
        "MAX_HOLD_MINUTES": max(15, min(4320, env_int("MAX_HOLD_MINUTES", 720))),
        "NOTIONAL_USDT": max(10.0, min(10000.0, env_float("NOTIONAL_USDT", 100.0))),
        "Z_ENTRY": max(0.5, min(6.0, env_float("Z_ENTRY", 2.0))),
        "Z_EXIT": max(0.0, min(3.0, env_float("Z_EXIT", 0.5))),
        "Z_STOP": max(1.5, min(10.0, env_float("Z_STOP", 3.5))),
    }


CANDLE_INTERVAL = os.getenv("CANDLE_INTERVAL", "5m")
SCAN_INTERVAL_SEC = max(15, env_int("SCAN_INTERVAL_SEC", 45))
INITIAL_EQUITY = max(50.0, env_float("INITIAL_EQUITY", 1000.0))
TAKER_FEE = max(0.0, env_float("TAKER_FEE", 0.0005))
FETCH_CONCURRENCY = max(4, min(30, env_int("FETCH_CONCURRENCY", 15)))
CANDIDATE_POOL_LIMIT = max(20, min(80, env_int("CANDIDATE_POOL_LIMIT", 45)))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_ADMIN_ID = os.getenv("TELEGRAM_ADMIN_ID", "").strip()
GATE_API_KEY = os.getenv("GATE_API_KEY", "").strip()
GATE_API_SECRET = os.getenv("GATE_API_SECRET", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()


def fmt_ist(ts_ms: Optional[int]) -> str:
    if not ts_ms:
        return "-"
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=IST)
    return dt.strftime("%Y-%m-%d %H:%M:%S IST")


def now_ms() -> int:
    return int(time.time() * 1000)


class StateStore:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.redis_client: Optional[redis.Redis] = None
        self.state: Dict[str, Any] = self._default_state()
        self.candle_cache: Dict[str, List[Tuple[int, float]]] = {}
        self.contracts_meta: Dict[str, Dict[str, float]] = {}
        self.contracts_meta_ts: float = 0.0

    def _default_state(self) -> Dict[str, Any]:
        return {
            "paused": False,
            "config": get_env_config_defaults(),
            "sim_equity": INITIAL_EQUITY,
            "realized_pnl": 0.0,
            "wins": 0,
            "losses": 0,
            "positions": [],
            "ranked_pairs": [],
            "trades": [],
            "events": [
                {
                    "ts": now_ms(),
                    "type": "SYSTEM",
                    "msg": "Bot state initialized",
                }
            ],
            "last_tick_ts": None,
            "last_error": None,
        }

    async def init(self) -> None:
        if REDIS_URL:
            try:
                self.redis_client = redis.from_url(
                    REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                )
                await self.redis_client.ping()
                log.info("Connected to Redis for persistent state")
            except Exception as exc:
                log.warning("Redis connection failed (%s); falling back to %s", exc, STATE_FILE)
                self.redis_client = None

        loaded: Optional[Dict[str, Any]] = None
        if self.redis_client is not None:
            try:
                raw = await self.redis_client.get(REDIS_STATE_KEY)
                if raw:
                    loaded = json.loads(raw)
                    log.info("Loaded state from Redis")
            except Exception as exc:
                log.warning("Redis read failed: %s", exc)

        if loaded is None and os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                log.info("Loaded state from %s", STATE_FILE)
            except Exception as exc:
                log.warning("Local JSON read failed: %s", exc)

        if isinstance(loaded, dict):
            base = self._default_state()
            merged_cfg = get_env_config_defaults()
            if isinstance(loaded.get("config"), dict):
                for k in merged_cfg:
                    if k in loaded["config"]:
                        merged_cfg[k] = loaded["config"][k]
            base.update(loaded)
            base["config"] = merged_cfg
            base["positions"] = loaded.get("positions") if isinstance(loaded.get("positions"), list) else []
            base["ranked_pairs"] = loaded.get("ranked_pairs") if isinstance(loaded.get("ranked_pairs"), list) else []
            base["trades"] = loaded.get("trades") if isinstance(loaded.get("trades"), list) else []
            base["events"] = loaded.get("events") if isinstance(loaded.get("events"), list) else base["events"]
            self.state = base

        await self.save()

    def add_event(self, ev_type: str, msg: str) -> None:
        self.state["events"] = [
            {"ts": now_ms(), "type": ev_type, "msg": msg},
            *self.state.get("events", []),
        ][:100]

    async def save(self) -> None:
        payload = json.dumps(self.state, separators=(",", ":"))
        if self.redis_client is not None:
            try:
                await self.redis_client.set(REDIS_STATE_KEY, payload)
            except Exception as exc:
                log.warning("Redis save error: %s", exc)
        try:
            tmp = f"{STATE_FILE}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp, STATE_FILE)
        except Exception as exc:
            log.warning("Local state save error: %s", exc)

    async def close(self) -> None:
        if self.redis_client is not None:
            await self.redis_client.aclose()


class GateClient:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{GATE_BASE}{path}"
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Gate GET {path} {resp.status}: {text[:180]}")
            return json.loads(text)

    async def signed_post(self, path: str, body: Dict[str, Any]) -> Any:
        if not GATE_API_KEY or not GATE_API_SECRET:
            raise RuntimeError("Missing GATE_API_KEY or GATE_API_SECRET")
        ts = str(int(time.time()))
        payload_str = json.dumps(body, separators=(",", ":"))
        hashed_payload = hashlib.sha512(payload_str.encode("utf-8")).hexdigest()
        sign_str = f"POST\n/api/v4{path}\n\n{hashed_payload}\n{ts}"
        signature = hmac.new(
            GATE_API_SECRET.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": GATE_API_KEY,
            "Timestamp": ts,
            "SIGN": signature,
        }
        url = f"{GATE_BASE}{path}"
        async with self.session.post(
            url, data=payload_str, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Gate POST {path} {resp.status}: {text[:200]}")
            return json.loads(text)


def round_to_step(val: float, step: float) -> float:
    if not math.isfinite(val) or val <= 0:
        return 0.0
    if not math.isfinite(step) or step <= 0:
        return float(math.floor(val))
    inv = round(1.0 / step) if step < 1 else 1
    if step < 1 and abs(1.0 / step - inv) < 1e-6:
        rounded = math.floor(val * inv + 1e-9) / inv
        decimals = max(0, min(8, len(str(step).split(".")[1]) if "." in str(step) else 0))
        return round(rounded, decimals)
    units = math.floor((val / step) + 1e-9)
    return round(units * step, 8)


def size_contract_order(
    contract: str,
    target_notional: float,
    price: float,
    meta_map: Dict[str, Dict[str, float]],
) -> Tuple[bool, float, str]:
    info = meta_map.get(contract, {"min_size": 1.0, "max_size": 1_000_000.0, "quanto": 1.0})
    min_size = float(info.get("min_size") or 1.0)
    max_size = float(info.get("max_size") or 1_000_000.0)
    quanto = float(info.get("quanto") or 1.0)
    if price <= 0 or quanto <= 0:
        return False, 0.0, f"{contract} invalid price/quanto"
    raw_contracts = target_notional / (price * quanto)
    qty = round_to_step(raw_contracts, min_size)
    if qty < min_size:
        return False, 0.0, f"{contract} qty {qty} < min {min_size}"
    if qty > max_size:
        return False, 0.0, f"{contract} qty {qty} > max {max_size}"
    return True, qty, ""


def format_order_size_field(qty: float, signed_dir: int) -> Any:
    signed = qty * signed_dir
    if abs(signed - round(signed)) < 1e-9:
        return int(round(signed))
    return str(round(signed, 8))


def adf_t_stat(spread: np.ndarray) -> float:
    n = len(spread) - 1
    if n < 20:
        return 0.0
    lag = spread[:-1]
    diff = np.diff(spread)
    mean_l = float(np.mean(lag))
    mean_d = float(np.mean(diff))
    dl = lag - mean_l
    s_ll = float(np.dot(dl, dl))
    if s_ll <= 1e-12:
        return 0.0
    gamma = float(np.dot(dl, diff - mean_d)) / s_ll
    c = mean_d - gamma * mean_l
    resid = diff - (c + gamma * lag)
    rss = float(np.dot(resid, resid))
    dof = max(1, n - 2)
    sigma2 = rss / dof
    se_gamma = math.sqrt(max(1e-15, sigma2 / s_ll))
    return gamma / se_gamma


def ou_half_life_bars(spread: np.ndarray) -> float:
    n = len(spread) - 1
    if n < 15:
        return float("inf")
    prev = spread[:-1]
    curr = spread[1:]
    mean_p = float(np.mean(prev))
    mean_c = float(np.mean(curr))
    dp = prev - mean_p
    den = float(np.dot(dp, dp))
    if den <= 1e-12:
        return float("inf")
    phi = float(np.dot(dp, curr - mean_c)) / den
    if phi <= 0.001 or phi >= 0.999:
        return float("inf")
    hl = -math.log(2.0) / math.log(phi)
    return hl if (hl > 0 and math.isfinite(hl)) else float("inf")


def evaluate_pair(
    sym_y: str,
    sym_x: str,
    bars_y: List[Tuple[int, float]],
    bars_x: List[Tuple[int, float]],
    cfg: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    lookback = int(cfg["LOOKBACK"])
    map_x = dict(bars_x)
    aligned = [(t, cy, map_x[t]) for (t, cy) in bars_y if t in map_x and cy > 0 and map_x[t] > 0]
    if len(aligned) < max(35, int(lookback * 0.7)):
        return None
    aligned = aligned[-lookback:]

    py = np.array([row[1] for row in aligned], dtype=np.float64)
    px = np.array([row[2] for row in aligned], dtype=np.float64)
    ly = np.log(py)
    lx = np.log(px)

    mean_x = float(np.mean(lx))
    mean_y = float(np.mean(ly))
    dx = lx - mean_x
    den = float(np.dot(dx, dx))
    if den <= 1e-12:
        return None
    beta = float(np.dot(dx, ly - mean_y)) / den
    alpha = mean_y - beta * mean_x

    spread = ly - (alpha + beta * lx)
    roll_w = min(len(spread), max(20, min(60, lookback)))
    tail = spread[-roll_w:]
    s_mean = float(np.mean(tail))
    s_std = float(np.std(tail, ddof=1)) if len(tail) > 1 else 0.0
    z_now = float((spread[-1] - s_mean) / s_std) if s_std > 1e-8 else 0.0

    adf = adf_t_stat(spread)
    hl = ou_half_life_bars(spread)

    ret_y = np.diff(ly)
    ret_x = np.diff(lx)
    if len(ret_y) >= 10 and np.std(ret_y) > 1e-12 and np.std(ret_x) > 1e-12:
        corr = float(np.corrcoef(ret_y, ret_x)[0, 1])
    else:
        corr = 0.0

    abs_beta = abs(beta)
    z_entry = float(cfg["Z_ENTRY"])
    z_exit = float(cfg["Z_EXIT"])
    notional = float(cfg["NOTIONAL_USDT"])

    is_coint = (
        adf <= -2.35
        and 1.0 < hl <= 120.0
        and corr >= 0.45
        and 0.15 <= abs_beta <= 5.0
    )

    if z_now <= -z_entry:
        signal = "LONG_SPREAD"
    elif z_now >= z_entry:
        signal = "SHORT_SPREAD"
    else:
        signal = "NONE"

    capture_z = max(0.0, abs(z_now) - z_exit)
    gross_move = capture_z * s_std
    total_notional = notional * (1.0 + abs_beta)
    fee_drag_usdt = total_notional * TAKER_FEE * 2.0
    expected_gross_usdt = notional * gross_move
    edge_usdt = expected_gross_usdt - fee_drag_usdt
    edge_pct = (edge_usdt / total_notional * 100.0) if total_notional > 0 else 0.0

    status = "ENTRY MET" if (is_coint and signal != "NONE" and edge_usdt > 0) else ("COINT" if is_coint else "WATCH")

    score = (
        (50.0 if is_coint else 0.0)
        + min(25.0, max(0.0, (-adf - 1.5) * 8.0))
        + min(15.0, max(0.0, corr * 15.0))
        + min(15.0, abs(z_now) * 4.0)
    )

    return {
        "pair": f"{sym_y}/{sym_x}",
        "legY": sym_y,
        "legX": sym_x,
        "priceY": float(py[-1]),
        "priceX": float(px[-1]),
        "alpha": float(alpha),
        "beta": float(beta),
        "zNow": float(z_now),
        "adf": float(adf),
        "halfLifeBars": round(hl, 1) if math.isfinite(hl) else None,
        "corr": float(corr),
        "signal": signal,
        "status": status,
        "edgePct": float(edge_pct),
        "edgeUsdt": float(edge_usdt),
        "score": float(score),
    }


class StatArbBot:
    def __init__(self, store: StateStore, gate: GateClient, session: aiohttp.ClientSession) -> None:
        self.store = store
        self.gate = gate
        self.session = session
        self.tg_offset = 0

    async def send_telegram(self, text: str, chat_id: Optional[str] = None) -> None:
        target = chat_id or TELEGRAM_ADMIN_ID
        if not TELEGRAM_BOT_TOKEN or not target:
            return
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try:
            async with self.session.post(
                url,
                json={"chat_id": target, "text": text, "parse_mode": "Markdown"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                await resp.read()
        except Exception as exc:
            log.warning("Telegram send error: %s", exc)

    async def refresh_contracts_meta(self) -> None:
        if self.store.contracts_meta and (time.time() - self.store.contracts_meta_ts) < 6 * 3600:
            return
        items = await self.gate.public_get("/futures/usdt/contracts")
        meta: Dict[str, Dict[str, float]] = {}
        for item in items if isinstance(items, list) else []:
            name = item.get("name")
            if not name or item.get("in_delisting"):
                continue
            meta[name] = {
                "min_size": float(item.get("order_size_min") or 1.0),
                "max_size": float(item.get("order_size_max") or 1_000_000.0),
                "quanto": float(item.get("quanto_multiplier") or 1.0),
            }
        if meta:
            self.store.contracts_meta = meta
            self.store.contracts_meta_ts = time.time()

    async def fetch_universe_and_candles(self, top_n: int, lookback: int) -> List[str]:
        tickers = await self.gate.public_get("/futures/usdt/tickers")
        ranked = []
        for t in tickers if isinstance(tickers, list) else []:
            c = t.get("contract", "")
            last = float(t.get("last") or 0.0)
            if not c.endswith("_USDT") or last <= 0:
                continue
            vol = float(t.get("volume_24h_quote") or t.get("volume_24h_settle") or t.get("volume_24h") or 0.0)
            ranked.append((c, vol))
        ranked.sort(key=lambda x: x[1], reverse=True)
        selected = [c for c, _ in ranked[:top_n]]

        sem = asyncio.Semaphore(FETCH_CONCURRENCY)
        full_limit = max(120, lookback + 25)

        async def fetch_one(contract: str) -> None:
            prev = self.store.candle_cache.get(contract, [])
            limit = 15 if len(prev) >= lookback else full_limit
            async with sem:
                raw = await self.gate.public_get(
                    "/futures/usdt/candlesticks",
                    {"contract": contract, "interval": CANDLE_INTERVAL, "limit": limit},
                )
            merged: Dict[int, float] = dict(prev)
            for bar in raw if isinstance(raw, list) else []:
                ts = int(bar.get("t") or 0)
                close = float(bar.get("c") or 0.0)
                if ts > 0 and close > 0:
                    merged[ts] = close
            ordered = sorted(merged.items(), key=lambda x: x[0])[-(lookback + 35) :]
            if ordered:
                self.store.candle_cache[contract] = ordered

        await asyncio.gather(*(fetch_one(sym) for sym in selected), return_exceptions=True)
        return selected

    async def place_pair_orders(
        self,
        leg_y: str,
        leg_x: str,
        side: str,
        qty_y: float,
        qty_x: float,
        reduce_only: bool,
        dry_run: bool,
    ) -> None:
        if dry_run:
            return
        if not reduce_only:
            dir_y = 1 if side == "LONG_SPREAD" else -1
            dir_x = -1 if side == "LONG_SPREAD" else 1
        else:
            dir_y = -1 if side == "LONG_SPREAD" else 1
            dir_x = 1 if side == "LONG_SPREAD" else -1

        await self.gate.signed_post(
            "/futures/usdt/orders",
            {
                "contract": leg_y,
                "size": format_order_size_field(qty_y, dir_y),
                "price": "0",
                "tif": "ioc",
                "reduce_only": reduce_only,
            },
        )
        await self.gate.signed_post(
            "/futures/usdt/orders",
            {
                "contract": leg_x,
                "size": format_order_size_field(qty_x, dir_x),
                "price": "0",
                "tif": "ioc",
                "reduce_only": reduce_only,
            },
        )

    async def close_all_positions(self, reason: str = "MANUAL_CLOSEALL") -> float:
        alerts: List[str] = []
        total_pnl = 0.0
        async with self.store.lock:
            state = self.store.state
            cfg = state["config"]
            positions = state.get("positions", [])
            if not positions:
                return 0.0
            ts_exit = now_ms()
            for pos in positions:
                try:
                    await self.place_pair_orders(
                        pos["legY"],
                        pos["legX"],
                        pos["side"],
                        float(pos["qtyY"]),
                        float(pos["qtyX"]),
                        reduce_only=True,
                        dry_run=bool(cfg["DRY_RUN"]),
                    )
                except Exception as exc:
                    self.store.add_event("ERROR", f"Close order failed on {pos['pair']}: {exc}")

                pnl = float(pos.get("unrealizedPnl") or 0.0)
                total_pnl += pnl
                state["realized_pnl"] = float(state.get("realized_pnl", 0.0)) + pnl
                state["sim_equity"] = float(state.get("sim_equity", INITIAL_EQUITY)) + pnl
                if pnl >= 0:
                    state["wins"] = int(state.get("wins", 0)) + 1
                else:
                    state["losses"] = int(state.get("losses", 0)) + 1

                state["trades"] = [
                    {
                        "pair": pos["pair"],
                        "side": pos["side"],
                        "qtyY": pos["qtyY"],
                        "qtyX": pos["qtyX"],
                        "entryPriceY": pos["entryPriceY"],
                        "exitPriceY": pos.get("curPriceY", pos["entryPriceY"]),
                        "entryPriceX": pos["entryPriceX"],
                        "exitPriceX": pos.get("curPriceX", pos["entryPriceX"]),
                        "zEntry": pos["zEntry"],
                        "zExit": pos.get("zNow", 0.0),
                        "pnl": pnl,
                        "reason": reason,
                        "entryTs": pos["entryTs"],
                        "exitTs": ts_exit,
                    },
                    *state.get("trades", []),
                ][:150]

                self.store.add_event(
                    "EXIT",
                    f"Closed {pos['pair']} ({pos['side']}) [{reason}] PnL: {pnl:+.2f} USDT",
                )

            state["positions"] = []
            await self.store.save()
            alerts.append(
                f"🛑 *Closed All Positions* (`{reason}`)\n"
                f"Net Realized PnL: *{total_pnl:+.2f} USDT*\n"
                f"Equity: *{state['sim_equity']:.2f} USDT*"
            )

        for msg in alerts:
            await self.send_telegram(msg)
        return total_pnl

    async def run_tick(self) -> None:
        alerts: List[str] = []
        async with self.store.lock:
            cfg = dict(self.store.state["config"])

        try:
            await self.refresh_contracts_meta()
            universe = await self.fetch_universe_and_candles(int(cfg["TOP_N_COINS"]), int(cfg["LOOKBACK"]))
        except Exception as exc:
            async with self.store.lock:
                self.store.state["last_tick_ts"] = now_ms()
                self.store.state["last_error"] = str(exc)
                self.store.add_event("ERROR", f"Market fetch failed: {exc}")
                await self.store.save()
            return

        async with self.store.lock:
            state = self.store.state
            cfg = state["config"]
            state["last_tick_ts"] = now_ms()
            state["last_error"] = None

            lookback = int(cfg["LOOKBACK"])
            z_exit = float(cfg["Z_EXIT"])
            z_stop = float(cfg["Z_STOP"])
            max_hold_min = float(cfg["MAX_HOLD_MINUTES"])
            max_positions = int(cfg["MAX_POSITIONS"])
            notional_usdt = float(cfg["NOTIONAL_USDT"])
            dry_run = bool(cfg["DRY_RUN"])

            remaining_positions: List[Dict[str, Any]] = []
            now = now_ms()
            for pos in state.get("positions", []):
                bars_y = self.store.candle_cache.get(pos["legY"], [])
                bars_x = self.store.candle_cache.get(pos["legX"], [])
                cur_y = bars_y[-1][1] if bars_y else float(pos["entryPriceY"])
                cur_x = bars_x[-1][1] if bars_x else float(pos["entryPriceX"])

                z_now = float(pos.get("zNow", pos["zEntry"]))
                if bars_y and bars_x:
                    map_x = dict(bars_x)
                    aligned = [(t, cy, map_x[t]) for (t, cy) in bars_y if t in map_x][-lookback:]
                    if len(aligned) >= 15:
                        sp = np.array(
                            [math.log(cy) - (pos["alpha"] + pos["beta"] * math.log(cx)) for _, cy, cx in aligned],
                            dtype=np.float64,
                        )
                        tail = sp[-min(len(sp), 60) :]
                        s_mean = float(np.mean(tail))
                        s_std = float(np.std(tail, ddof=1)) if len(tail) > 1 else 0.0
                        cur_sp = math.log(cur_y) - (pos["alpha"] + pos["beta"] * math.log(cur_x))
                        if s_std > 1e-8:
                            z_now = float((cur_sp - s_mean) / s_std)

                direction = 1.0 if pos["side"] == "LONG_SPREAD" else -1.0
                ret_y = (cur_y - float(pos["entryPriceY"])) / float(pos["entryPriceY"])
                ret_x = (cur_x - float(pos["entryPriceX"])) / float(pos["entryPriceX"])
                leg_yn = float(pos.get("notionalY") or notional_usdt)
                leg_xn = float(pos.get("notionalX") or notional_usdt * abs(float(pos["beta"])))
                gross_pnl = direction * (leg_yn * ret_y - leg_xn * ret_x)
                fee_drag = (leg_yn + leg_xn) * TAKER_FEE * 2.0
                net_pnl = gross_pnl - fee_drag

                capture_z = max(0.0, abs(z_now) - z_exit)
                edge_usdt = max(0.0, capture_z * 0.015 * leg_yn - fee_drag)
                edge_pct = (edge_usdt / (leg_yn + leg_xn) * 100.0) if (leg_yn + leg_xn) > 0 else 0.0

                elapsed_min = (now - int(pos["entryTs"])) / 60000.0
                exit_reason: Optional[str] = None
                if abs(z_now) <= z_exit:
                    exit_reason = "MEAN_REVERT"
                elif abs(z_now) >= z_stop:
                    exit_reason = "STOP_LOSS"
                elif elapsed_min >= max_hold_min:
                    exit_reason = "MAX_HOLD"

                pos["curPriceY"] = cur_y
                pos["curPriceX"] = cur_x
                pos["zNow"] = z_now
                pos["unrealizedPnl"] = net_pnl
                pos["edgePct"] = edge_pct
                pos["edgeUsdt"] = edge_usdt

                if exit_reason:
                    try:
                        await self.place_pair_orders(
                            pos["legY"],
                            pos["legX"],
                            pos["side"],
                            float(pos["qtyY"]),
                            float(pos["qtyX"]),
                            reduce_only=True,
                            dry_run=dry_run,
                        )
                    except Exception as exc:
                        self.store.add_event("ERROR", f"Live exit failed {pos['pair']}: {exc}")

                    state["realized_pnl"] = float(state.get("realized_pnl", 0.0)) + net_pnl
                    state["sim_equity"] = float(state.get("sim_equity", INITIAL_EQUITY)) + net_pnl
                    if net_pnl >= 0:
                        state["wins"] = int(state.get("wins", 0)) + 1
                    else:
                        state["losses"] = int(state.get("losses", 0)) + 1

                    state["trades"] = [
                        {
                            "pair": pos["pair"],
                            "side": pos["side"],
                            "qtyY": pos["qtyY"],
                            "qtyX": pos["qtyX"],
                            "entryPriceY": pos["entryPriceY"],
                            "exitPriceY": cur_y,
                            "entryPriceX": pos["entryPriceX"],
                            "exitPriceX": cur_x,
                            "zEntry": pos["zEntry"],
                            "zExit": z_now,
                            "pnl": net_pnl,
                            "reason": exit_reason,
                            "entryTs": pos["entryTs"],
                            "exitTs": now,
                        },
                        *state.get("trades", []),
                    ][:150]

                    self.store.add_event(
                        "EXIT",
                        f"{pos['pair']} {pos['side']} exit [{exit_reason}] PnL {net_pnl:+.2f} USDT",
                    )
                    alerts.append(
                        f"📤 *EXIT {pos['pair']}* ({pos['side']})\n"
                        f"Reason: `{exit_reason}` | Z: `{z_now:.2f}`\n"
                        f"PnL: *{net_pnl:+.2f} USDT* | Equity: *{state['sim_equity']:.2f} USDT*"
                    )
                else:
                    remaining_positions.append(pos)

            state["positions"] = remaining_positions

            valid_syms = [s for s in universe if len(self.store.candle_cache.get(s, [])) >= 35][:CANDIDATE_POOL_LIMIT]
            candidates: List[Dict[str, Any]] = []
            for i in range(len(valid_syms)):
                for j in range(i + 1, len(valid_syms)):
                    a, b = valid_syms[i], valid_syms[j]
                    ev_ab = evaluate_pair(a, b, self.store.candle_cache[a], self.store.candle_cache[b], cfg)
                    if ev_ab:
                        candidates.append(ev_ab)
                    ev_ba = evaluate_pair(b, a, self.store.candle_cache[b], self.store.candle_cache[a], cfg)
                    if ev_ba:
                        candidates.append(ev_ba)

            candidates.sort(key=lambda x: x["score"], reverse=True)
            state["ranked_pairs"] = candidates[:50]

            if not state.get("paused", False) and len(state["positions"]) < max_positions:
                busy = set()
                for p in state["positions"]:
                    busy.add(p["legY"])
                    busy.add(p["legX"])

                for cand in candidates:
                    if len(state["positions"]) >= max_positions:
                        break
                    if cand["status"] != "ENTRY MET" or cand["signal"] == "NONE":
                        continue
                    if cand["legY"] in busy or cand["legX"] in busy:
                        continue

                    notional_y = notional_usdt
                    notional_x = notional_usdt * abs(float(cand["beta"]))
                    ok_y, qty_y, err_y = size_contract_order(
                        cand["legY"], notional_y, float(cand["priceY"]), self.store.contracts_meta
                    )
                    ok_x, qty_x, err_x = size_contract_order(
                        cand["legX"], notional_x, float(cand["priceX"]), self.store.contracts_meta
                    )
                    if not ok_y or not ok_x:
                        self.store.add_event("GUARD", f"Skipped {cand['pair']}: {err_y or err_x}")
                        continue

                    try:
                        await self.place_pair_orders(
                            cand["legY"],
                            cand["legX"],
                            cand["signal"],
                            qty_y,
                            qty_x,
                            reduce_only=False,
                            dry_run=dry_run,
                        )
                    except Exception as exc:
                        self.store.add_event("ERROR", f"Live entry failed {cand['pair']}: {exc}")
                        continue

                    init_fee = -(notional_y + notional_x) * TAKER_FEE * 2.0
                    new_pos = {
                        "id": f"{now_ms()}_{cand['legY']}_{cand['legX']}",
                        "pair": cand["pair"],
                        "legY": cand["legY"],
                        "legX": cand["legX"],
                        "side": cand["signal"],
                        "qtyY": qty_y,
                        "qtyX": qty_x,
                        "notionalY": notional_y,
                        "notionalX": notional_x,
                        "entryPriceY": cand["priceY"],
                        "curPriceY": cand["priceY"],
                        "entryPriceX": cand["priceX"],
                        "curPriceX": cand["priceX"],
                        "alpha": cand["alpha"],
                        "beta": cand["beta"],
                        "zEntry": cand["zNow"],
                        "zNow": cand["zNow"],
                        "adf": cand["adf"],
                        "halfLifeBars": cand["halfLifeBars"],
                        "corr": cand["corr"],
                        "edgePct": cand["edgePct"],
                        "edgeUsdt": cand["edgeUsdt"],
                        "unrealizedPnl": init_fee,
                        "entryTs": now_ms(),
                    }
                    state["positions"].append(new_pos)
                    busy.add(cand["legY"])
                    busy.add(cand["legX"])

                    self.store.add_event(
                        "ENTRY",
                        f"Opened {cand['pair']} {cand['signal']} Z={cand['zNow']:.2f} β={cand['beta']:.2f} qty={qty_y}/{qty_x}",
                    )
                    alerts.append(
                        f"📥 *ENTRY {cand['pair']}* ({cand['signal']})\n"
                        f"Z: `{cand['zNow']:.2f}` | β: `{cand['beta']:.2f}` | ADF: `{cand['adf']:.2f}` | HL: `{cand['halfLifeBars']}b`\n"
                        f"Edge: *{cand['edgePct']:.2f}% ({cand['edgeUsdt']:.2f} USDT)*"
                    )

            await self.store.save()

        for msg in alerts:
            await self.send_telegram(msg)

    async def process_telegram_update(self, update: Dict[str, Any]) -> None:
        msg = update.get("message") or {}
        text = str(msg.get("text") or "").strip()
        user_id = str((msg.get("from") or {}).get("id") or "")
        chat_id = str((msg.get("chat") or {}).get("id") or user_id)

        if not text or not TELEGRAM_ADMIN_ID or user_id != TELEGRAM_ADMIN_ID:
            return

        parts = text.split()
        cmd = parts[0].lower().split("@")[0]
        arg1 = parts[1] if len(parts) > 1 else ""
        arg2 = parts[2] if len(parts) > 2 else ""

        if cmd in ("/help", "/start"):
            await self.send_telegram(
                "*Gate.io Top-100 Stat-Arb Admin Commands*\n"
                "`/status` - Bot state, mode, open slots & last IST tick\n"
                "`/pnl` - Realized, unrealized PnL, equity & W/L record\n"
                "`/top` - Top ranked cointegrated pairs & live Z-scores\n"
                "`/trades` - Recent closed spread trades\n"
                "`/balance` - Equity, notional & hold settings\n"
                "`/config` - View all 9 editable runtime parameters\n"
                "`/setdryrun <true|false> [confirm]` - Toggle paper/live mode\n"
                "`/settopn <10-200>` - Set `TOP_N_COINS`\n"
                "`/setlookback <30-300>` - Set `LOOKBACK`\n"
                "`/setmaxpos <1-10>` - Set `MAX_POSITIONS`\n"
                "`/setmaxhold <15-4320>` - Set `MAX_HOLD_MINUTES`\n"
                "`/setnotional <10-10000>` - Set `NOTIONAL_USDT`\n"
                "`/setz <0.5-6.0>` - Set `Z_ENTRY`\n"
                "`/setzexit <0.0-3.0>` - Set `Z_EXIT`\n"
                "`/setzstop <1.5-10.0>` - Set `Z_STOP`\n"
                "`/resetconfig` - Reset all 9 parameters to defaults\n"
                "`/pause` | `/resume` | `/closeall`",
                chat_id,
            )
            return

        if cmd == "/closeall":
            await self.close_all_positions("TELEGRAM_CLOSEALL")
            return

        async with self.store.lock:
            state = self.store.state
            cfg = state["config"]
            defaults = get_env_config_defaults()

            if cmd == "/status":
                u_pnl = sum(float(p.get("unrealizedPnl") or 0.0) for p in state.get("positions", []))
                mode = "PAPER (DRY_RUN)" if cfg["DRY_RUN"] else "LIVE TRADING"
                reply = (
                    f"*Status:* {'⏸ PAUSED' if state.get('paused') else '▶️ RUNNING'} | *{mode}*\n"
                    f"*Positions:* `{len(state.get('positions', []))}/{cfg['MAX_POSITIONS']}` | "
                    f"*Z Entry/Exit/Stop:* `{cfg['Z_ENTRY']}/{cfg['Z_EXIT']}/{cfg['Z_STOP']}`\n"
                    f"*Unrealized PnL:* `{u_pnl:+.2f} USDT`\n"
                    f"*Last Tick:* {fmt_ist(state.get('last_tick_ts'))}"
                )
                if state.get("last_error"):
                    reply += f"\n*Last Error:* `{state['last_error']}`"

            elif cmd == "/pnl":
                u_pnl = sum(float(p.get("unrealizedPnl") or 0.0) for p in state.get("positions", []))
                reply = (
                    f"*Realized PnL:* `{float(state.get('realized_pnl', 0.0)):+.2f} USDT`\n"
                    f"*Unrealized PnL:* `{u_pnl:+.2f} USDT`\n"
                    f"*Equity:* `{float(state.get('sim_equity', INITIAL_EQUITY)):.2f} USDT`\n"
                    f"*Record:* `{state.get('wins', 0)}W / {state.get('losses', 0)}L`"
                )

            elif cmd == "/top":
                lines = []
                for idx, p in enumerate(state.get("ranked_pairs", [])[:6], start=1):
                    lines.append(
                        f"{idx}. *{p['pair']}* [{p['status']}] Z=`{p['zNow']:.2f}` "
                        f"β=`{p['beta']:.2f}` ADF=`{p['adf']:.2f}` Edge=`{p['edgePct']:.2f}%`"
                    )
                reply = "\n".join(lines) if lines else "No ranked pairs yet. Wait for the first scan tick."

            elif cmd == "/trades":
                lines = []
                for t in state.get("trades", [])[:6]:
                    lines.append(
                        f"• *{t['pair']}* {t['side']} `{float(t['pnl']):+.2f}` [{t['reason']}] ({fmt_ist(t.get('exitTs'))})"
                    )
                reply = "\n".join(lines) if lines else "No closed trades yet."

            elif cmd == "/balance":
                reply = (
                    f"*Mode:* {'Paper (`DRY_RUN=true`)' if cfg['DRY_RUN'] else 'Live (`DRY_RUN=false`)'}\n"
                    f"*Equity:* `{float(state.get('sim_equity', INITIAL_EQUITY)):.2f} USDT`\n"
                    f"*Leg Y Notional:* `{cfg['NOTIONAL_USDT']} USDT` | *Max Hold:* `{cfg['MAX_HOLD_MINUTES']}m`"
                )

            elif cmd == "/config":
                rows = ["*Active Runtime Configuration:*"]
                for key in [
                    "DRY_RUN",
                    "TOP_N_COINS",
                    "LOOKBACK",
                    "MAX_POSITIONS",
                    "MAX_HOLD_MINUTES",
                    "NOTIONAL_USDT",
                    "Z_ENTRY",
                    "Z_EXIT",
                    "Z_STOP",
                ]:
                    tag = " _(edited)_" if cfg[key] != defaults[key] else ""
                    rows.append(f"• `{key}` = `{cfg[key]}`{tag}")
                reply = "\n".join(rows)

            elif cmd == "/resetconfig":
                state["config"] = get_env_config_defaults()
                self.store.add_event("CONFIG", "All 9 runtime parameters reset to defaults via Telegram")
                await self.store.save()
                reply = "✅ All 9 parameters reset to built-in defaults."

            elif cmd == "/pause":
                state["paused"] = True
                self.store.add_event("CONTROL", "Bot paused via Telegram")
                await self.store.save()
                reply = "⏸ Bot *paused*. Existing positions remain monitored."

            elif cmd == "/resume":
                state["paused"] = False
                self.store.add_event("CONTROL", "Bot resumed via Telegram")
                await self.store.save()
                reply = "▶️ Bot *resumed*."

            elif cmd == "/setdryrun":
                v = arg1.lower()
                if v == "true":
                    cfg["DRY_RUN"] = True
                    self.store.add_event("CONFIG", "DRY_RUN set to True via Telegram")
                    await self.store.save()
                    reply = "✅ `DRY_RUN` set to `True` (Paper Trading)."
                elif v == "false":
                    if arg2.lower() != "confirm":
                        reply = "⚠️ Live mode places real Gate.io orders. Send `/setdryrun false confirm` to activate."
                    elif not GATE_API_KEY or not GATE_API_SECRET:
                        reply = "❌ Cannot enable live mode: `GATE_API_KEY` or `GATE_API_SECRET` is missing in Voroa."
                    else:
                        cfg["DRY_RUN"] = False
                        self.store.add_event("CONFIG", "DRY_RUN set to False (LIVE) via Telegram")
                        await self.store.save()
                        reply = "🚨 `DRY_RUN` set to `False` (*LIVE TRADING ENABLED*)."
                else:
                    reply = "Usage: `/setdryrun true` or `/setdryrun false confirm`"

            elif cmd == "/settopn":
                try:
                    val = int(arg1)
                    if not (10 <= val <= 200):
                        raise ValueError
                    cfg["TOP_N_COINS"] = val
                    self.store.add_event("CONFIG", f"TOP_N_COINS updated to {val}")
                    await self.store.save()
                    reply = f"✅ `TOP_N_COINS` updated to `{val}`."
                except ValueError:
                    reply = "Usage: `/settopn <10-200>` (e.g. `/settopn 80`)"

            elif cmd == "/setlookback":
                try:
                    val = int(arg1)
                    if not (30 <= val <= 300):
                        raise ValueError
                    cfg["LOOKBACK"] = val
                    self.store.add_event("CONFIG", f"LOOKBACK updated to {val}")
                    await self.store.save()
                    reply = f"✅ `LOOKBACK` updated to `{val}` bars."
                except ValueError:
                    reply = "Usage: `/setlookback <30-300>` (e.g. `/setlookback 120`)"

            elif cmd == "/setmaxpos":
                try:
                    val = int(arg1)
                    if not (1 <= val <= 10):
                        raise ValueError
                    cfg["MAX_POSITIONS"] = val
                    self.store.add_event("CONFIG", f"MAX_POSITIONS updated to {val}")
                    await self.store.save()
                    reply = f"✅ `MAX_POSITIONS` updated to `{val}`."
                except ValueError:
                    reply = "Usage: `/setmaxpos <1-10>` (e.g. `/setmaxpos 4`)"

            elif cmd == "/setmaxhold":
                try:
                    val = int(arg1)
                    if not (15 <= val <= 4320):
                        raise ValueError
                    cfg["MAX_HOLD_MINUTES"] = val
                    self.store.add_event("CONFIG", f"MAX_HOLD_MINUTES updated to {val}")
                    await self.store.save()
                    reply = f"✅ `MAX_HOLD_MINUTES` updated to `{val}` min."
                except ValueError:
                    reply = "Usage: `/setmaxhold <15-4320>` (e.g. `/setmaxhold 480`)"

            elif cmd == "/setnotional":
                try:
                    val = float(arg1)
                    if not (10.0 <= val <= 10000.0):
                        raise ValueError
                    cfg["NOTIONAL_USDT"] = round(val, 2)
                    self.store.add_event("CONFIG", f"NOTIONAL_USDT updated to {cfg['NOTIONAL_USDT']}")
                    await self.store.save()
                    reply = f"✅ `NOTIONAL_USDT` updated to `{cfg['NOTIONAL_USDT']}` USDT."
                except ValueError:
                    reply = "Usage: `/setnotional <10-10000>` (e.g. `/setnotional 150`)"

            elif cmd == "/setz":
                try:
                    val = float(arg1)
                    if not (0.5 <= val <= 6.0):
                        raise ValueError
                    if not (float(cfg["Z_EXIT"]) < val < float(cfg["Z_STOP"])):
                        reply = f"❌ Must satisfy `Z_EXIT ({cfg['Z_EXIT']}) < Z_ENTRY < Z_STOP ({cfg['Z_STOP']})`."
                    else:
                        cfg["Z_ENTRY"] = round(val, 4)
                        self.store.add_event("CONFIG", f"Z_ENTRY updated to {cfg['Z_ENTRY']}")
                        await self.store.save()
                        reply = f"✅ `Z_ENTRY` updated to `{cfg['Z_ENTRY']}`."
                except ValueError:
                    reply = "Usage: `/setz <0.5-6.0>` (e.g. `/setz 2.2`)"

            elif cmd == "/setzexit":
                try:
                    val = float(arg1)
                    if not (0.0 <= val <= 3.0):
                        raise ValueError
                    if val >= float(cfg["Z_ENTRY"]):
                        reply = f"❌ Must be strictly less than `Z_ENTRY ({cfg['Z_ENTRY']})`."
                    else:
                        cfg["Z_EXIT"] = round(val, 4)
                        self.store.add_event("CONFIG", f"Z_EXIT updated to {cfg['Z_EXIT']}")
                        await self.store.save()
                        reply = f"✅ `Z_EXIT` updated to `{cfg['Z_EXIT']}`."
                except ValueError:
                    reply = "Usage: `/setzexit <0.0-3.0>` (e.g. `/setzexit 0.4`)"

            elif cmd == "/setzstop":
                try:
                    val = float(arg1)
                    if not (1.5 <= val <= 10.0):
                        raise ValueError
                    if val <= float(cfg["Z_ENTRY"]):
                        reply = f"❌ Must be strictly greater than `Z_ENTRY ({cfg['Z_ENTRY']})`."
                    else:
                        cfg["Z_STOP"] = round(val, 4)
                        self.store.add_event("CONFIG", f"Z_STOP updated to {cfg['Z_STOP']}")
                        await self.store.save()
                        reply = f"✅ `Z_STOP` updated to `{cfg['Z_STOP']}`."
                except ValueError:
                    reply = "Usage: `/setzstop <1.5-10.0>` (e.g. `/setzstop 4.0`)"

            else:
                reply = "Unknown command. Send `/help` to view all commands."

        await self.send_telegram(reply, chat_id)

    async def telegram_polling_loop(self) -> None:
        if not TELEGRAM_BOT_TOKEN:
            log.info("TELEGRAM_BOT_TOKEN not set; Telegram polling disabled")
            return
        try:
            async with self.session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                await resp.read()
        except Exception as exc:
            log.warning("deleteWebhook failed: %s", exc)

        while True:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
                params = {"offset": self.tg_offset, "timeout": 25}
                async with self.session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=35)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for upd in data.get("result", []):
                            uid = int(upd.get("update_id", 0))
                            if uid >= self.tg_offset:
                                self.tg_offset = uid + 1
                            await self.process_telegram_update(upd)
                    else:
                        await asyncio.sleep(3)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("Telegram poll error: %s", exc)
                await asyncio.sleep(3)

    async def scanner_loop(self) -> None:
        while True:
            try:
                await self.run_tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("Scanner tick error: %s", exc)
            await asyncio.sleep(SCAN_INTERVAL_SEC)


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Gate.io Top-100 Stat-Arb Bot (IST)</title>
<style>
  :root {
    --bg: #0b0f17; --panel: #121826; --border: #222c42;
    --text: #e6edf7; --muted: #8fa0bd;
    --green: #16a34a; --amber: #d97706; --red: #dc2626; --blue: #2563eb;
  }
  * { box-sizing: border-box; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
  body { margin: 0; background: var(--bg); color: var(--text); padding: 16px; }
  .topbar { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; background: var(--panel); border: 1px solid var(--border); padding: 12px 16px; border-radius: 10px; margin-bottom: 14px; }
  .title-group h1 { margin: 0; font-size: 18px; }
  .sub { font-size: 12px; color: var(--muted); margin-top: 3px; }
  .right-controls { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
  .badge { background: #1a2438; border: 1px solid var(--border); padding: 6px 10px; border-radius: 6px; font-size: 12px; font-variant-numeric: tabular-nums; }
  button { border: none; border-radius: 6px; padding: 7px 12px; font-size: 12px; font-weight: 600; cursor: pointer; color: #fff; }
  .btn-pause { background: var(--amber); }
  .btn-resume { background: var(--green); }
  .btn-close { background: var(--red); }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); gap: 10px; margin-bottom: 14px; }
  .card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
  .card .k { font-size: 11px; color: var(--muted); text-transform: uppercase; }
  .card .v { font-size: 17px; font-weight: 700; margin-top: 4px; font-variant-numeric: tabular-nums; }
  .section { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 12px; margin-bottom: 14px; }
  .section h2 { margin: 0 0 10px 0; font-size: 14px; }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; font-variant-numeric: tabular-nums; }
  th, td { text-align: left; padding: 7px 8px; border-bottom: 1px solid var(--border); white-space: nowrap; }
  th { color: var(--muted); font-weight: 600; }
  .pos { color: #4ade80; }
  .neg { color: #f87171; }
  .hold-wrap { width: 105px; background: #1e293b; border-radius: 4px; height: 10px; overflow: hidden; display: inline-block; vertical-align: middle; }
  .hold-fill { height: 100%; transition: width 0.25s linear; }
  .hold-txt { font-size: 11px; margin-left: 6px; color: var(--muted); }
  .pill { padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 600; background: #1e293b; }
  .pill-entry { background: rgba(22,163,74,0.22); color: #4ade80; }
  .pill-coint { background: rgba(37,99,235,0.22); color: #93c5fd; }
  .err { background: rgba(220,38,38,0.15); border: 1px solid rgba(220,38,38,0.4); color: #fca5a5; padding: 8px 12px; border-radius: 8px; font-size: 12px; margin-bottom: 12px; display: none; }
</style>
</head>
<body>
  <div class="topbar">
    <div class="title-group">
      <h1>Gate.io Top-100 Stat-Arb Bot</h1>
      <div class="sub" id="modeLine">Loading state...</div>
    </div>
    <div class="right-controls">
      <div class="badge" id="istClock">--:--:-- IST</div>
      <div class="badge" id="countdownBadge">Refresh in: 3s</div>
      <button class="btn-pause" id="btnTogglePause" onclick="togglePause()">Pause</button>
      <button class="btn-close" onclick="closeAll()">Close All</button>
    </div>
  </div>

  <div class="err" id="errBanner"></div>
  <div class="cards" id="kpiCards"></div>

  <div class="section">
    <h2>Open Positions</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Pair</th><th>Side</th><th>Qty (Y / X)</th><th>Entry / Cur Y</th><th>Entry / Cur X</th>
            <th>Z (In / Now)</th><th>Edge % / $</th><th>Unrealized PnL</th><th>Beta</th><th>ADF</th>
            <th>Half-Life</th><th>Corr</th><th>Remaining Hold</th><th>Entry Time (IST)</th>
          </tr>
        </thead>
        <tbody id="posBody"></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Top Ranked Pairs</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Pair</th><th>Status</th><th>Signal</th><th>Z Now</th><th>Beta</th>
            <th>ADF</th><th>Half-Life (bars)</th><th>Corr</th><th>Edge %</th><th>Edge $</th>
          </tr>
        </thead>
        <tbody id="pairsBody"></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Simulated Trade History</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Pair</th><th>Side</th><th>Qty (Y / X)</th><th>Z In / Out</th><th>PnL (USDT)</th>
            <th>Reason</th><th>Entry (IST)</th><th>Exit (IST)</th>
          </tr>
        </thead>
        <tbody id="tradesBody"></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Recent Activity</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Time (IST)</th><th>Type</th><th>Message</th></tr></thead>
        <tbody id="eventsBody"></tbody>
      </table>
    </div>
  </div>

<script>
let latestState = null;
let countdownSec = 3;

function fmtIST(ts) {
  if (!ts) return "-";
  return new Intl.DateTimeFormat("en-IN", {
    timeZone: "Asia/Kolkata",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false
  }).format(new Date(ts)) + " IST";
}

function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}

function tickClockAndBars() {
  document.getElementById("istClock").textContent = fmtIST(Date.now());
  document.getElementById("countdownBadge").textContent = "Refresh in: " + countdownSec + "s";

  const maxMin = Number(latestState?.config?.MAX_HOLD_MINUTES) || 720;
  const now = Date.now();
  document.querySelectorAll("[data-entry-ts]").forEach(el => {
    const entryTs = Number(el.getAttribute("data-entry-ts"));
    const elapsedMin = Math.max(0, (now - entryTs) / 60000);
    const remMin = Math.max(0, maxMin - elapsedMin);
    const pct = Math.max(0, Math.min(100, (remMin / maxMin) * 100));
    const fill = el.querySelector(".hold-fill");
    const txt = el.querySelector(".hold-txt");
    if (fill) {
      fill.style.width = pct.toFixed(1) + "%";
      fill.style.background = pct > 50 ? "#16a34a" : pct > 20 ? "#d97706" : "#dc2626";
    }
    if (txt) {
      txt.textContent = Math.floor(remMin) + "m (" + pct.toFixed(0) + "%)";
    }
  });
}

function renderState(s) {
  latestState = s;
  const cfg = s.config || {};
  const errEl = document.getElementById("errBanner");
  if (s.last_error) {
    errEl.style.display = "block";
    errEl.textContent = "Last Error: " + s.last_error;
  } else {
    errEl.style.display = "none";
  }

  document.getElementById("modeLine").textContent =
    (s.paused ? "PAUSED" : "RUNNING") +
    " | Mode: " + (cfg.DRY_RUN ? "PAPER" : "LIVE") +
    " | Top-" + (cfg.TOP_N_COINS || 100) +
    " | Lookback: " + (cfg.LOOKBACK || 100) +
    " | Z (In/Out/Stop): " + cfg.Z_ENTRY + "/" + cfg.Z_EXIT + "/" + cfg.Z_STOP +
    " | Notional: $" + cfg.NOTIONAL_USDT +
    " | Last Tick: " + fmtIST(s.last_tick_ts);

  const btn = document.getElementById("btnTogglePause");
  btn.textContent = s.paused ? "Resume" : "Pause";
  btn.className = s.paused ? "btn-resume" : "btn-pause";

  const uPnl = (s.positions || []).reduce((acc, p) => acc + (Number(p.unrealizedPnl) || 0), 0);
  const kpis = [
    ["Equity (USDT)", Number(s.sim_equity || 0).toFixed(2), ""],
    ["Realized PnL", (s.realized_pnl >= 0 ? "+" : "") + Number(s.realized_pnl || 0).toFixed(2), s.realized_pnl >= 0 ? "pos" : "neg"],
    ["Unrealized PnL", (uPnl >= 0 ? "+" : "") + uPnl.toFixed(2), uPnl >= 0 ? "pos" : "neg"],
    ["Win / Loss", (s.wins || 0) + "W / " + (s.losses || 0) + "L", ""],
    ["Open Slots", (s.positions || []).length + " / " + (cfg.MAX_POSITIONS || 3), ""],
    ["Max Hold", (cfg.MAX_HOLD_MINUTES || 720) + " min", ""]
  ];
  document.getElementById("kpiCards").innerHTML = kpis.map(([k, v, cls]) =>
    '<div class="card"><div class="k">' + esc(k) + '</div><div class="v ' + cls + '">' + esc(v) + '</div></div>'
  ).join("");

  const posBody = document.getElementById("posBody");
  if (!s.positions || !s.positions.length) {
    posBody.innerHTML = '<tr><td colspan="14" style="color:#8fa0bd">No open spread positions</td></tr>';
  } else {
    posBody.innerHTML = s.positions.map(p => {
      const pnlCls = (p.unrealizedPnl || 0) >= 0 ? "pos" : "neg";
      return '<tr>' +
        '<td><strong>' + esc(p.pair) + '</strong></td>' +
        '<td>' + esc(p.side) + '</td>' +
        '<td>' + esc(p.qtyY) + ' / ' + esc(p.qtyX) + '</td>' +
        '<td>' + Number(p.entryPriceY).toFixed(4) + ' / ' + Number(p.curPriceY).toFixed(4) + '</td>' +
        '<td>' + Number(p.entryPriceX).toFixed(4) + ' / ' + Number(p.curPriceX).toFixed(4) + '</td>' +
        '<td>' + Number(p.zEntry).toFixed(2) + ' / ' + Number(p.zNow).toFixed(2) + '</td>' +
        '<td>' + Number(p.edgePct).toFixed(2) + '% / $' + Number(p.edgeUsdt).toFixed(2) + '</td>' +
        '<td class="' + pnlCls + '">' + (p.unrealizedPnl >= 0 ? "+" : "") + Number(p.unrealizedPnl).toFixed(2) + '</td>' +
        '<td>' + Number(p.beta).toFixed(2) + '</td>' +
        '<td>' + Number(p.adf).toFixed(2) + '</td>' +
        '<td>' + esc(p.halfLifeBars ?? "-") + '</td>' +
        '<td>' + Number(p.corr).toFixed(2) + '</td>' +
        '<td><div data-entry-ts="' + Number(p.entryTs) + '"><div class="hold-wrap"><div class="hold-fill"></div></div><span class="hold-txt"></span></div></td>' +
        '<td>' + fmtIST(p.entryTs) + '</td>' +
      '</tr>';
    }).join("");
  }

  const pairsBody = document.getElementById("pairsBody");
  if (!s.ranked_pairs || !s.ranked_pairs.length) {
    pairsBody.innerHTML = '<tr><td colspan="10" style="color:#8fa0bd">Waiting for first market scan</td></tr>';
  } else {
    pairsBody.innerHTML = s.ranked_pairs.slice(0, 25).map(p => {
      const pillCls = p.status === "ENTRY MET" ? "pill-entry" : p.status === "COINT" ? "pill-coint" : "";
      return '<tr>' +
        '<td><strong>' + esc(p.pair) + '</strong></td>' +
        '<td><span class="pill ' + pillCls + '">' + esc(p.status) + '</span></td>' +
        '<td>' + esc(p.signal) + '</td>' +
        '<td>' + Number(p.zNow).toFixed(2) + '</td>' +
        '<td>' + Number(p.beta).toFixed(2) + '</td>' +
        '<td>' + Number(p.adf).toFixed(2) + '</td>' +
        '<td>' + esc(p.halfLifeBars ?? "-") + '</td>' +
        '<td>' + Number(p.corr).toFixed(2) + '</td>' +
        '<td>' + Number(p.edgePct).toFixed(2) + '%</td>' +
        '<td>$' + Number(p.edgeUsdt).toFixed(2) + '</td>' +
      '</tr>';
    }).join("");
  }

  const tradesBody = document.getElementById("tradesBody");
  if (!s.trades || !s.trades.length) {
    tradesBody.innerHTML = '<tr><td colspan="8" style="color:#8fa0bd">No closed trades yet</td></tr>';
  } else {
    tradesBody.innerHTML = s.trades.slice(0, 30).map(t => {
      const cls = (t.pnl || 0) >= 0 ? "pos" : "neg";
      return '<tr>' +
        '<td><strong>' + esc(t.pair) + '</strong></td>' +
        '<td>' + esc(t.side) + '</td>' +
        '<td>' + esc(t.qtyY) + ' / ' + esc(t.qtyX) + '</td>' +
        '<td>' + Number(t.zEntry).toFixed(2) + ' / ' + Number(t.zExit).toFixed(2) + '</td>' +
        '<td class="' + cls + '">' + (t.pnl >= 0 ? "+" : "") + Number(t.pnl).toFixed(2) + '</td>' +
        '<td>' + esc(t.reason) + '</td>' +
        '<td>' + fmtIST(t.entryTs) + '</td>' +
        '<td>' + fmtIST(t.exitTs) + '</td>' +
      '</tr>';
    }).join("");
  }

  const eventsBody = document.getElementById("eventsBody");
  eventsBody.innerHTML = (s.events || []).slice(0, 30).map(e =>
    '<tr><td>' + fmtIST(e.ts) + '</td><td><span class="pill">' + esc(e.type) + '</span></td><td>' + esc(e.msg) + '</td></tr>'
  ).join("");

  tickClockAndBars();
}

async function fetchState() {
  try {
    const res = await fetch("/api/state");
    if (res.ok) {
      const data = await res.json();
      renderState(data);
    }
  } catch (e) {}
  countdownSec = 3;
}

async function sendControl(payload) {
  await fetch("/api/control", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  await fetchState();
}

function togglePause() {
  sendControl({ action: latestState?.paused ? "resume" : "pause" });
}

function closeAll() {
  if (confirm("Close all open spread positions now?")) {
    sendControl({ action: "closeall" });
  }
}

setInterval(() => {
  countdownSec -= 1;
  if (countdownSec <= 0) {
    fetchState();
  } else {
    tickClockAndBars();
  }
}, 1000);

fetchState();
</script>
</body>
</html>
"""


async def main() -> None:
    session = aiohttp.ClientSession()
    store = StateStore()
    await store.init()
    gate = GateClient(session)
    bot = StatArbBot(store, gate, session)

    async def handle_index(_request: web.Request) -> web.Response:
        return web.Response(text=DASHBOARD_HTML, content_type="text/html")

    async def handle_state(_request: web.Request) -> web.Response:
        async with store.lock:
            return web.json_response(store.state)

    async def handle_control(request: web.Request) -> web.Response:
        body = await request.json()
        action = str(body.get("action") or "").lower()
        if action == "closeall":
            await bot.close_all_positions("DASHBOARD_CLOSEALL")
            return web.json_response({"ok": True})
        async with store.lock:
            if action == "pause":
                store.state["paused"] = True
                store.add_event("CONTROL", "Bot paused via Web Dashboard")
            elif action == "resume":
                store.state["paused"] = False
                store.add_event("CONTROL", "Bot resumed via Web Dashboard")
            await store.save()
        return web.json_response({"ok": True})

    async def handle_health(_request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "ist": fmt_ist(now_ms())})

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/state", handle_state)
    app.router.add_post("/api/control", handle_control)
    app.router.add_get("/health", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Web dashboard listening on 0.0.0.0:%d", port)

    scanner_task = asyncio.create_task(bot.scanner_loop())
    tg_task = asyncio.create_task(bot.telegram_polling_loop())

    try:
        await asyncio.gather(scanner_task, tg_task)
    finally:
        scanner_task.cancel()
        tg_task.cancel()
        await runner.cleanup()
        await store.close()
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
