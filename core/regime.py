import numpy as np
import pandas as pd
from core.signals_whale import clamp


def calc_atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def compute_regime_signals(price_df: pd.DataFrame) -> dict:
    """
    用 120~250D 日K建立中期 Regime：bull/bear/range/transition
    回傳：regime_score(0~100), regime_trend, regime_tags
    """
    if price_df is None or price_df.empty or len(price_df) < 140:
        return {"regime_score": 0, "regime_trend": "unknown", "regime_tags": ["insufficient_price_data"]}

    df = price_df.copy()
    df = df.tail(260).reset_index(drop=True)

    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["atr14"] = calc_atr(df, 14)
    df["atrp14"] = (df["atr14"] / df["close"].replace(0, np.nan)).fillna(0.0)

    last = df.iloc[-1]
    if pd.isna(last["ma20"]) or pd.isna(last["ma60"]):
        return {"regime_score": 0, "regime_trend": "unknown", "regime_tags": ["ma_not_ready"]}

    ma60_tail = df["ma60"].tail(10).dropna()
    slope60 = 0.0
    if len(ma60_tail) >= 5:
        x = np.arange(len(ma60_tail), dtype=float)
        y = ma60_tail.values.astype(float)
        denom = (((x - x.mean()) ** 2).sum() or 1.0)
        slope60 = float(((x - x.mean()) * (y - y.mean())).sum() / denom)

    lb = 120
    prev = df.iloc[-lb-1:-1] if len(df) > lb else df.iloc[:-1]
    hh120 = float(prev["high"].max()) if not prev.empty else float(df["high"].max())
    ll120 = float(prev["low"].min()) if not prev.empty else float(df["low"].min())

    close_last = float(last["close"])
    ma20 = float(last["ma20"])
    ma60 = float(last["ma60"])
    atrp = float(last["atrp14"])

    tags = []
    dir_score = 0
    if ma20 > ma60:
        dir_score = 35
        tags.append("ma20>ma60")
    else:
        dir_score = 15
        tags.append("ma20<ma60")

    if slope60 > 0:
        dir_score += 10
        tags.append("ma60_up")
    elif slope60 < 0:
        dir_score -= 5
        tags.append("ma60_down")

    struct_score = 0
    if close_last > hh120:
        struct_score = 30
        tags.append("break_high_120d")
    elif close_last < ll120:
        struct_score = 5
        tags.append("break_low_120d")
    else:
        struct_score = 15
        tags.append("inside_120d_range")

    risk_score = 20
    if atrp > 0.06:
        risk_score = 5
        tags.append("atr_high")
    elif atrp > 0.045:
        risk_score = 10
        tags.append("atr_mid")
    else:
        tags.append("atr_low")

    score = int(round(clamp(dir_score + struct_score + risk_score, 0, 100)))

    if score >= 70 and ma20 > ma60 and slope60 > 0:
        trend = "bull"
    elif score <= 35 and ma20 < ma60 and slope60 < 0:
        trend = "bear"
    elif 40 <= score <= 60 and abs(ma20 - ma60) / (ma60 or 1.0) < 0.01:
        trend = "range"
        tags.append("ma_converge")
    else:
        trend = "transition"
        tags.append("regime_transition")

    return {"regime_score": score, "regime_trend": trend, "regime_tags": tags}
