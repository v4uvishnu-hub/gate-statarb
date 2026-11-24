import numpy as np
import pandas as pd


class StatArbEngine:
    def __init__(self, lookback=100, z_entry=2.0, z_exit=0.2, z_stop=3.8):
        self.lookback = lookback
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.z_stop = z_stop

    def compute_metrics(self, df_y: pd.DataFrame, df_x: pd.DataFrame) -> dict:
        y = np.log(df_y["close"].values[-self.lookback:])
        x = np.log(df_x["close"].values[-self.lookback:])

        x_mean = np.mean(x)
        y_mean = np.mean(y)
        denom = np.sum((x - x_mean) ** 2) + 1e-12
        beta = np.sum((x - x_mean) * (y - y_mean)) / denom
        alpha = y_mean - beta * x_mean

        spread = y - (alpha + beta * x)
        z_series = (spread - np.mean(spread)) / (np.std(spread, ddof=1) + 1e-12)

        return {
            "beta": float(beta),
            "z_score": float(z_series[-1]),
            "z_history": [round(float(v), 3) for v in z_series[-40:]],
            "price_y": float(df_y["close"].iloc[-1]),
            "price_x": float(df_x["close"].iloc[-1]),
        }

    def get_signal(self, metrics: dict, side: str | None) -> str:
        z = metrics["z_score"]
        if side == "LONG_SPREAD":
            return "CLOSE" if (z >= -self.z_exit or z <= -self.z_stop) else "HOLD"
        if side == "SHORT_SPREAD":
            return "CLOSE" if (z <= self.z_exit or z >= self.z_stop) else "HOLD"
        if z <= -self.z_entry:
            return "LONG_SPREAD"
        if z >= self.z_entry:
            return "SHORT_SPREAD"
        return "HOLD"
