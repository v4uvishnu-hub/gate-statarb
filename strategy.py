import numpy as np
import pandas as pd


def _adf_tstat(series: np.ndarray) -> float:
    """Fast pure-NumPy Augmented Dickey-Fuller (lag=1) t-statistic."""
    dy = np.diff(series)
    y_lag = series[:-1]
    if len(dy) < 15:
        return 0.0
    dy_lag = dy[:-1]
    y_lag_trim = y_lag[1:]
    target = dy[1:]
    X = np.column_stack([np.ones(len(target)), y_lag_trim, dy_lag])
    try:
        beta, _, _, _ = np.linalg.lstsq(X, target, rcond=None)
        resid = target - X @ beta
        s2 = np.sum(resid ** 2) / max(1, len(target) - 3)
        cov = s2 * np.linalg.inv(X.T @ X)
        se = np.sqrt(max(cov[1, 1], 1e-18))
        return float(beta[1] / se)
    except np.linalg.LinAlgError:
        return 0.0


def _half_life(spread: np.ndarray) -> float:
    """Ornstein-Uhlenbeck half-life in bars."""
    lag = spread[:-1]
    delta = np.diff(spread)
    X = np.column_stack([np.ones(len(lag)), lag])
    try:
        beta, _, _, _ = np.linalg.lstsq(X, delta, rcond=None)
        lam = beta[1]
        return float(-np.log(2.0) / lam) if lam < -1e-6 else float("inf")
    except np.linalg.LinAlgError:
        return float("inf")


class StatArbEngine:
    def __init__(
        self,
        lookback: int = 100,
        z_entry: float = 2.0,
        z_exit: float = 0.2,
        z_stop: float = 3.8,
        adf_threshold: float = -2.86,
        min_half_life: float = 2.0,
        max_half_life: float = 60.0,
    ):
        self.lookback = lookback
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.z_stop = z_stop
        self.adf_threshold = adf_threshold
        self.min_half_life = min_half_life
        self.max_half_life = max_half_life

    def compute_metrics(self, df_y: pd.DataFrame, df_x: pd.DataFrame) -> dict:
        n = min(self.lookback, len(df_y), len(df_x))
        y = np.log(df_y["close"].values[-n:])
        x = np.log(df_x["close"].values[-n:])

        x_mean = np.mean(x)
        y_mean = np.mean(y)
        denom = np.sum((x - x_mean) ** 2) + 1e-12
        beta = np.sum((x - x_mean) * (y - y_mean)) / denom
        alpha = y_mean - beta * x_mean

        spread = y - (alpha + beta * x)
        z_series = (spread - np.mean(spread)) / (np.std(spread, ddof=1) + 1e-12)
        adf_stat = _adf_tstat(spread)
        hl = _half_life(spread)
        corr = float(np.corrcoef(np.diff(y), np.diff(x))[0, 1]) if n > 5 else 0.0

        is_cointegrated = (
            0.3 <= abs(beta) <= 3.0
            and corr >= 0.55
            and adf_stat <= self.adf_threshold
            and self.min_half_life <= hl <= self.max_half_life
        )

        return {
            "beta": float(beta),
            "z_score": float(z_series[-1]),
            "z_history": [round(float(v), 3) for v in z_series[-40:]],
            "adf_stat": float(adf_stat),
            "half_life": float(hl) if np.isfinite(hl) else 999.0,
            "corr": float(corr),
            "is_cointegrated": bool(is_cointegrated),
            "price_y": float(df_y["close"].iloc[-1]),
            "price_x": float(df_x["close"].iloc[-1]),
        }

    def scan_universe(self, ohlcv_map: dict[str, pd.DataFrame], anchor_count: int = 8) -> list[dict]:
        """
        Screens pairs across the top liquid coins.
        Tests each coin against the top `anchor_count` most liquid leaders (BTC, ETH, SOL, etc.)
        to keep O(N * anchor_count) fast (~750 pairs across 100 coins in <0.2s).
        """
        symbols = list(ohlcv_map.keys())
        anchors = symbols[:anchor_count]
        results = []
        seen = set()

        for y_sym in symbols:
            for x_sym in anchors:
                if y_sym == x_sym:
                    continue
                pair_key = tuple(sorted((y_sym, x_sym)))
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                m = self.compute_metrics(ohlcv_map[y_sym], ohlcv_map[x_sym])
                m["pair_y"] = y_sym
                m["pair_x"] = x_sym
                # Score rewards strong stationarity (negative ADF) + active |Z|
                m["score"] = (-m["adf_stat"]) + min(abs(m["z_score"]), 3.5) * 0.4
                results.append(m)

        results.sort(key=lambda r: (r["is_cointegrated"], r["score"]), reverse=True)
        return results

    def get_signal(self, metrics: dict, side: str | None) -> str:
        z = metrics["z_score"]
        if side == "LONG_SPREAD":
            return "CLOSE" if (z >= -self.z_exit or z <= -self.z_stop) else "HOLD"
        if side == "SHORT_SPREAD":
            return "CLOSE" if (z <= self.z_exit or z >= self.z_stop) else "HOLD"
        if not metrics.get("is_cointegrated", False):
            return "HOLD"
        if z <= -self.z_entry:
            return "LONG_SPREAD"
        if z >= self.z_entry:
            return "SHORT_SPREAD"
        return "HOLD"
