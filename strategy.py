import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

class StatArbEngine:
    def __init__(self, lookback=100, z_entry=2.0, z_exit=0.2, z_stop=3.8):
        self.lookback = lookback
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.z_stop = z_stop

    def compute_metrics(self, df_y: pd.DataFrame, df_x: pd.DataFrame) -> dict:
        y = np.log(df_y["close"].values[-self.lookback:])
        x = np.log(df_x["close"].values[-self.lookback:])

        x_const = sm.add_constant(x)
        alpha, beta = sm.OLS(y, x_const).fit().params

        spread = y - (alpha + beta * x)
        z_score = (spread[-1] - np.mean(spread)) / (np.std(spread, ddof=1) + 1e-12)
        adf_stat, p_value = adfuller(spread, maxlag=1)[:2]
        is_cointegrated = adf_stat <= -2.86

        return {
            "beta": float(beta),
            "z_score": float(z_score),
            "adf_stat": float(adf_stat),
            "p_value": float(p_value),
            "is_cointegrated": bool(is_cointegrated),
            "price_y": float(df_y["close"].iloc[-1]),
            "price_x": float(df_x["close"].iloc[-1]),
        }

    def get_signal(self, metrics: dict, side: str | None) -> str:
        z = metrics["z_score"]
        if side == "LONG_SPREAD":
            return "CLOSE" if (z >= -self.z_exit or z <= -self.z_stop) else "HOLD"
        if side == "SHORT_SPREAD":
            return "CLOSE" if (z <= self.z_exit or z >= self.z_stop) else "HOLD"
        if not metrics["is_cointegrated"]:
            return "HOLD"
        if z <= -self.z_entry:
            return "LONG_SPREAD"
        if z >= self.z_entry:
            return "SHORT_SPREAD"
        return "HOLD"
