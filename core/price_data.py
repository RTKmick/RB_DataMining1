from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from core.adapter_tw import TaiwanStockAdapter


def _normalize_price_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    col_map = {}
    if "max" in df.columns:
        col_map["max"] = "high"
    if "min" in df.columns:
        col_map["min"] = "low"
    df.rename(columns=col_map, inplace=True)

    vol_col = None
    for c in ["Trading_Volume", "Trading_turnover", "trading_volume", "turnover", "volume"]:
        if c in df.columns:
            vol_col = c
            break
    if vol_col is None:
        df["volume"] = 0
    else:
        df["volume"] = pd.to_numeric(df[vol_col], errors="coerce").fillna(0)

    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            return pd.DataFrame()
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["date"] = df["date"].astype(str)
    df = df.sort_values("date").reset_index(drop=True)
    return df[["date", "open", "high", "low", "close", "volume"]]


def fetch_ohlcv_20d(adapter: TaiwanStockAdapter, stock_id: str, target_dates: list[str], debug: bool = False) -> pd.DataFrame:
    """
    抓近 20 交易日 OHLCV（FinMind dataset：TaiwanStockPrice）
    回傳欄位：date, open, high, low, close, volume
    """
    if not target_dates:
        return pd.DataFrame()

    start_date = target_dates[0]
    end_date = target_dates[-1]

    df = adapter.client.request_data("TaiwanStockPrice", data_id=stock_id, start_date=start_date, end_date=end_date)
    if df.empty:
        return pd.DataFrame()

    if debug:
        print("TaiwanStockPrice raw cols=", list(df.columns))

    out = _normalize_price_df(df)
    if out.empty:
        return pd.DataFrame()

    out = out[out["date"].isin(target_dates)].copy()
    out = out.sort_values("date").reset_index(drop=True)
    return out


def fetch_price_nd(adapter: TaiwanStockAdapter, stock_id: str, lookback_days: int = 420) -> pd.DataFrame:
    """
    拉較長期間日K（用 TaiwanStockPrice）。
    lookback_days 用日曆天給 buffer，避免交易日不足。
    回傳欄位：date, open, high, low, close, volume
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    df = adapter.client.request_data("TaiwanStockPrice", data_id=stock_id, start_date=start_date, end_date=end_date)
    return _normalize_price_df(df)
