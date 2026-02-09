import numpy as np
import pandas as pd


def _rolling_slope(arr: np.ndarray) -> float:
    x = np.arange(len(arr), dtype=float)
    x_mean = x.mean()
    y = arr.astype(float)
    y_mean = y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return 0.0
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def calc_vwap(df: pd.DataFrame, window: int = 50) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = tp * df["volume"]
    denom = df["volume"].rolling(window).sum().replace(0, np.nan)
    vwap = pv.rolling(window).sum() / denom
    return vwap


def calc_obv(df: pd.DataFrame) -> pd.Series:
    direction = np.sign(df["close"].diff().fillna(0.0))
    return (direction * df["volume"]).cumsum()


def calc_ad(df: pd.DataFrame) -> pd.Series:
    hl = (df["high"] - df["low"]).replace(0, np.nan)
    mfm = (((df["close"] - df["low"]) - (df["high"] - df["close"])) / hl).fillna(0.0)
    mfv = mfm * df["volume"]
    return mfv.cumsum()


def compute_tv_radar_signals(ohlcv: pd.DataFrame, debug: bool = False) -> dict:
    """
    用近 20 日 OHLCV 算 5 個 Smart Money proxy 訊號（台股尺度調參版）
    回傳：sig_* + tv_score + tv_grade (+ tv_debug 可選)
    """
    if ohlcv is None or ohlcv.empty or len(ohlcv) < 20:
        out = {
            "sig_vwap": 0,
            "sig_accumulation": 0,
            "sig_rotation": 0,
            "sig_profile_zone": 0,
            "sig_structure_break": 0,
            "tv_score": 0,
            "tv_grade": "weak",
        }
        if debug:
            out["tv_debug"] = {"reason": "ohlcv_empty_or_lt_20"}
        return out

    df = ohlcv.copy().reset_index(drop=True)

    # 1) VWAP control
    vwap = calc_vwap(df, window=min(50, max(10, len(df))))
    vwap_tail = vwap.tail(20).dropna()

    sig_vwap = 0
    slope_vwap10 = 0.0
    vwap_last = float(vwap.iloc[-1]) if pd.notna(vwap.iloc[-1]) else None
    close_last = float(df["close"].iloc[-1])

    if len(vwap_tail) >= 10 and vwap_last is not None and vwap_last != 0:
        slope_vwap10 = _rolling_slope(vwap_tail.values[-10:])
        near = abs(close_last - vwap_last) / vwap_last < 0.006  # 0.6%
        sig_vwap = int((close_last >= vwap_last or near) and slope_vwap10 >= 0)

    # 2) Accumulation
    obv = calc_obv(df)
    ad = calc_ad(df)
    obv_s = _rolling_slope(obv.values[-20:])
    ad_s = _rolling_slope(ad.values[-20:])

    hl = (df["high"] - df["low"]).astype(float)
    atr20 = float(hl.rolling(20).mean().iloc[-1]) if pd.notna(hl.rolling(20).mean().iloc[-1]) else 0.0
    atr10 = float(hl.rolling(10).mean().iloc[-1]) if pd.notna(hl.rolling(10).mean().iloc[-1]) else 0.0
    compression = (atr20 > 0) and ((atr10 / atr20) < 0.92)

    sig_acc = int(obv_s > 0 and ad_s > 0 and compression)

    # 3) Rotation
    sig_rot = 0
    z = 0.0
    body_pct = 0.0
    med_body_pct = 0.0

    vol = df["volume"].astype(float)
    vol_last = float(vol.iloc[-1])
    if vol.sum() > 0:
        mu = float(vol.rolling(20).mean().iloc[-1]) if pd.notna(vol.rolling(20).mean().iloc[-1]) else 0.0
        sd = float(vol.rolling(20).std().iloc[-1]) if pd.notna(vol.rolling(20).std().iloc[-1]) else 0.0
        z = (vol_last - mu) / sd if sd > 0 else 0.0

        body = abs(float(df["close"].iloc[-1]) - float(df["open"].iloc[-1]))
        body_pct = body / close_last if close_last else 0.0

        body_pct_series = (abs(df["close"] - df["open"]) / df["close"].replace(0, np.nan)).astype(float)
        mb = body_pct_series.rolling(20).median().iloc[-1]
        med_body_pct = float(mb) if pd.notna(mb) else 0.0

        sig_rot = int(z > 1.3 and med_body_pct > 0 and body_pct < med_body_pct * 1.10)

    # 4) Profile HVN
    sig_profile = 0
    near_hvn = False
    vol_shrink = False
    hl_shrink = False
    hvn_low = hvn_high = None

    if vol.sum() > 0:
        sub = df.tail(20).copy()
        pmin, pmax = float(sub["close"].min()), float(sub["close"].max())
        if pmax > pmin:
            bins = np.linspace(pmin, pmax, 21)
            idx = np.digitize(sub["close"].values, bins) - 1
            idx = np.clip(idx, 0, len(bins) - 2)
            vol_by_bin = np.zeros(len(bins) - 1)
            np.add.at(vol_by_bin, idx, sub["volume"].values.astype(float))
            hvn_bin = int(np.argmax(vol_by_bin))
            hvn_low, hvn_high = float(bins[hvn_bin]), float(bins[hvn_bin + 1])

            hvn_mid = (hvn_low + hvn_high) / 2.0
            near_hvn = (hvn_low <= close_last <= hvn_high) or (hvn_mid and abs(close_last - hvn_mid) / hvn_mid < 0.005)

            vol_med = float(sub["volume"].median())
            vol_shrink = vol_last < vol_med

            hl10 = float((sub["high"] - sub["low"]).rolling(10).mean().iloc[-1])
            hl20 = float((sub["high"] - sub["low"]).rolling(20).mean().iloc[-1])
            hl_shrink = (hl20 > 0) and ((hl10 / hl20) < 0.92)

            sig_profile = int(near_hvn and (vol_shrink or hl_shrink))

    # 5) BOS
    sig_bos = 0
    prev = df.iloc[:-1].tail(20)
    if len(prev) >= 5:
        hh = float(prev["high"].max())
        ll = float(prev["low"].min())
        sig_bos = int(close_last > hh or close_last < ll)

    tv_score = int(sig_vwap + sig_acc + sig_rot + sig_profile + sig_bos)
    tv_grade = "weak" if tv_score <= 1 else ("watch" if tv_score <= 3 else "strong")

    out = {
        "sig_vwap": int(sig_vwap),
        "sig_accumulation": int(sig_acc),
        "sig_rotation": int(sig_rot),
        "sig_profile_zone": int(sig_profile),
        "sig_structure_break": int(sig_bos),
        "tv_score": int(tv_score),
        "tv_grade": tv_grade,
    }

    if debug:
        out["tv_debug"] = {
            "close_last": close_last,
            "vwap_last": vwap_last,
            "slope_vwap10": float(slope_vwap10),
            "z_vol20": float(z),
            "body_pct_last": float(body_pct),
            "med_body_pct20": float(med_body_pct),
            "obv_slope20": float(obv_s),
            "ad_slope20": float(ad_s),
            "atr10": float(atr10),
            "atr20": float(atr20),
            "compression": bool(compression),
            "hvn_low": hvn_low,
            "hvn_high": hvn_high,
            "near_hvn": bool(near_hvn),
            "vol_shrink": bool(vol_shrink),
            "hl_shrink": bool(hl_shrink),
        }

    return out
