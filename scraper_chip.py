# scraper_chip.py
# 目的：抓 FinMind 分點明細（TaiwanStockTradingDailyReport），輸出：
#   ./data/{stock_id}_whale_track.json  （dashboard 使用）
#   ./data/{stock_id}_boss_list.csv     （Top20 彙總表）
#
# 特色：
# - Top6 大戶 10日累積軌跡：whale_data / total_whale_values / top6_details
# - signals（主力訊號）
#     score / trend / tags
#     concentration_5d / concentration_20d
#     netbuy_1d_lot / netbuy_5d_lot / netbuy_20d_lot
#     buy_count_5d / sell_count_5d / breadth_5d / breadth_ratio_5d
#     buy_count_20d / sell_count_20d / breadth_20d / breadth_ratio_20d
#     foreign_net_5d / local_net_5d
#     top_buy_15 / top_sell_15（含 streak_buy/streak_sell + city + broker_org_type）
#     labels_20d / buy_count_series_20d / sell_count_series_20d / breadth_series_20d / breadth_ratio_series_20d
#     ★ TV Smart Money Radar:
#         sig_vwap / sig_accumulation / sig_rotation / sig_profile_zone / sig_structure_break
#         tv_score / tv_grade
#         tv_debug（debug_tv=True 才會輸出/印出）
#
# 依賴：
#   pip install requests pandas python-dotenv numpy
# .env：
#   FINMIND_API_TOKEN=xxxx

import os
import time
import json
import math
import argparse
import requests
import pandas as pd
import numpy as np
import urllib3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATA_PATH = "./data"
os.makedirs(DATA_PATH, exist_ok=True)

FINMIND_V4_DATA_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TDR_URL = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================== FinMind Client =====================
class FinMindClient:
    def __init__(self, token: str, verify_ssl: bool = True):
        self.token = (token or "").strip()
        self.verify_ssl = verify_ssl
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.0,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        if self.token:
            session.headers.update({"Authorization": f"Bearer {self.token}"})
        return session

    def _log(self, tag: str, params: dict, http_status: int, api_status, msg: str, latency: float):
        dataset = params.get("dataset", "")
        data_id = params.get("data_id", "")
        date = params.get("date", "")
        start_date = params.get("start_date", "")
        end_date = params.get("end_date", "")
        print(
            f"[{now_str()}] {tag} | http={http_status} api={api_status} "
            f"| dataset={dataset} data_id={data_id} date={date} "
            f"| start={start_date} end={end_date} | latency={latency:.2f}s | msg={msg}"
        )

    def request_data(self, dataset: str, data_id: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        params = {"dataset": dataset}
        if data_id:
            params["data_id"] = data_id
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        t0 = time.time()
        try:
            resp = self.session.get(FINMIND_V4_DATA_URL, params=params, timeout=30, verify=self.verify_ssl)
            latency = time.time() - t0
            try:
                js = resp.json()
            except Exception:
                self._log("DATA", params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")
            self._log("DATA", params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))
            return pd.DataFrame()
        except Exception as e:
            latency = time.time() - t0
            self._log("DATA", params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()

    def request_trading_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        params = {"data_id": stock_id, "date": date_yyyy_mm_dd}
        t0 = time.time()
        try:
            resp = self.session.get(FINMIND_TDR_URL, params=params, timeout=30, verify=self.verify_ssl)
            latency = time.time() - t0
            try:
                js = resp.json()
            except Exception:
                self._log("TDR", params, resp.status_code, "N/A", "non-json response", latency)
                return pd.DataFrame()

            api_status = js.get("status")
            msg = js.get("msg", "")
            self._log("TDR", params, resp.status_code, api_status, msg, latency)

            if resp.status_code == 200 and api_status == 200:
                return pd.DataFrame(js.get("data", []))

            if api_status in (401, 402, 403) or resp.status_code in (401, 402, 403):
                print(f"⚠️ 分點資料需要 sponsor 權限或 token 無效 | api_status={api_status} | msg={msg}")
            return pd.DataFrame()

        except Exception as e:
            latency = time.time() - t0
            self._log("TDR", params, -1, "EXC", repr(e), latency)
            return pd.DataFrame()


# ===================== Adapter =====================
class TaiwanStockAdapter:
    def __init__(self, client: FinMindClient):
        self.client = client

    def get_trading_dates(self, lookback=120) -> list:
        start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
        df = self.client.request_data("TaiwanStockTradingDate", start_date=start_date)
        if df.empty or "date" not in df.columns:
            return []
        return df["date"].astype(str).tolist()

    def get_daily_report(self, stock_id: str, date_yyyy_mm_dd: str) -> pd.DataFrame:
        return self.client.request_trading_daily_report(stock_id, date_yyyy_mm_dd)

    def get_stock_name(self, stock_id: str) -> str:
        name = stock_id
        df = self.client.request_data("TaiwanStockInfo", data_id=stock_id)
        if not df.empty:
            if "stock_name" in df.columns and str(df["stock_name"].iloc[0]).strip():
                name = str(df["stock_name"].iloc[0]).strip()
            elif "name" in df.columns and str(df["name"].iloc[0]).strip():
                name = str(df["name"].iloc[0]).strip()
        return name


# ===================== Broker master (enriched) =====================
def load_broker_master_enriched(data_path: str) -> dict:
    """
    讀 ./data/broker_master_enriched.csv
    回傳：
      broker_id -> {city, broker_org_type, is_proprietary, seat_type, broker_name(optional)}
    """
    path = os.path.join(data_path, "broker_master_enriched.csv")
    if not os.path.exists(path):
        print(f"⚠️ 找不到 {path}，城市/外本/席位資訊將顯示為空")
        return {}

    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception as e:
        print(f"⚠️ broker_master_enriched.csv 讀取失敗: {repr(e)}")
        return {}

    keep_cols = set(df.columns)
    out = {}
    for _, r in df.iterrows():
        bid = str(r.get("broker_id", "")).strip()
        if not bid:
            continue
        out[bid] = {
            "city": str(r.get("city", "")).strip(),
            "broker_org_type": str(r.get("broker_org_type", "")).strip() or "unknown",  # foreign/local/unknown
            "is_proprietary": str(r.get("is_proprietary", "")).strip(),
            "seat_type": str(r.get("seat_type", "")).strip(),
        }
        if "broker_name" in keep_cols:
            out[bid]["broker_name"] = str(r.get("broker_name", "")).strip()
    return out


# ===================== 抓 OHLCV（20交易日） =====================
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

    df = df.copy()
    if debug:
        print("TaiwanStockPrice raw cols=", list(df.columns))

    if "date" not in df.columns:
        return pd.DataFrame()

    # 欄位對齊（FinMind 常用 max/min）
    col_map = {}
    if "max" in df.columns:
        col_map["max"] = "high"
    if "min" in df.columns:
        col_map["min"] = "low"
    df.rename(columns=col_map, inplace=True)

    # volume 欄位可能叫 Trading_Volume / Trading_turnover / ...（取第一個命中的）
    vol_col = None
    for c in ["Trading_Volume", "Trading_turnover", "trading_volume", "turnover", "volume"]:
        if c in df.columns:
            vol_col = c
            break
    if vol_col is None:
        return pd.DataFrame()

    df["volume"] = pd.to_numeric(df[vol_col], errors="coerce").fillna(0)

    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            return pd.DataFrame()
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["date"] = df["date"].astype(str)

    # 只取 target_dates（確保與分點日期一致）
    df = df[df["date"].isin(target_dates)].copy()
    df = df.sort_values("date").reset_index(drop=True)

    return df[["date", "open", "high", "low", "close", "volume"]]


# ===================== Core math =====================
def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def calc_breadth(df_period: pd.DataFrame) -> dict:
    """
    df_period: 必須包含 broker_id, broker_name, net
    回傳買超家數/賣超家數/廣度比
    """
    if df_period.empty:
        return {"buy_count": 0, "sell_count": 0, "breadth": 0, "breadth_ratio": 0.0}

    g = df_period.groupby(["broker_id", "broker_name"], as_index=False)["net"].sum()
    buy_cnt = int((g["net"] > 0).sum())
    sell_cnt = int((g["net"] < 0).sum())
    total = buy_cnt + sell_cnt
    ratio = round(buy_cnt / total, 4) if total > 0 else 0.0
    return {"buy_count": buy_cnt, "sell_count": sell_cnt, "breadth": buy_cnt - sell_cnt, "breadth_ratio": ratio}


def compute_streaks(net_matrix: pd.DataFrame) -> dict:
    """
    net_matrix: index=date, columns=broker_id, values=net (股數)
    回傳 dict broker_id -> (streak_buy, streak_sell) 連續到最後一日
    """
    if net_matrix is None or net_matrix.empty:
        return {}

    dates = list(net_matrix.index)
    last_i = len(dates) - 1
    out = {}

    for bid in net_matrix.columns:
        s = net_matrix[bid].fillna(0).astype(float).tolist()

        sb = 0
        for i in range(last_i, -1, -1):
            if s[i] > 0:
                sb += 1
            else:
                break

        ss = 0
        for i in range(last_i, -1, -1):
            if s[i] < 0:
                ss += 1
            else:
                break

        out[str(bid)] = {"streak_buy": sb, "streak_sell": ss}

    return out


def compute_master_trend(signals: dict) -> dict:
    """
    主力走向新規則（集中度 + 廣度 + 外/本分歧）
    回傳：score(0~100), trend(文字), tags(list)
    """
    c5 = float(signals.get("concentration_5d", 0) or 0)
    c20 = float(signals.get("concentration_20d", 0) or 0)
    nb5 = float(signals.get("netbuy_5d_lot", 0) or 0)
    nb20 = float(signals.get("netbuy_20d_lot", 0) or 0)
    br5 = float(signals.get("breadth_ratio_5d", 0) or 0)
    br20 = float(signals.get("breadth_ratio_20d", 0) or 0)
    f5 = float(signals.get("foreign_net_5d", 0) or 0)  # 股數
    l5 = float(signals.get("local_net_5d", 0) or 0)    # 股數

    # 1) 強度（0~1）
    s_c5 = clamp(c5 / 12.0, 0.0, 1.0)
    s_c20 = clamp(c20 / 15.0, 0.0, 1.0)
    strength = 0.55 * s_c5 + 0.45 * s_c20

    # 2) 方向（-1~+1）
    dir5 = math.tanh(nb5 / 2500.0)
    dir20 = math.tanh(nb20 / 6000.0)
    direction = 0.6 * dir5 + 0.4 * dir20

    # 3) 廣度（-1~+1）
    b5 = clamp((br5 - 0.5) * 2.0, -1.0, 1.0)
    b20 = clamp((br20 - 0.5) * 2.0, -1.0, 1.0)
    breadth = 0.6 * b5 + 0.4 * b20

    # 4) 外/本分歧（-1~+1）
    if f5 == 0 and l5 == 0:
        div = 0.0
    else:
        same = (f5 >= 0 and l5 >= 0) or (f5 <= 0 and l5 <= 0)
        denom = abs(f5) + abs(l5)
        gap = abs(abs(f5) - abs(l5)) / denom if denom > 0 else 0.0
        div = (1.0 - gap) if same else -(0.5 + 0.5 * gap)

    score = (
        40.0 * strength +
        25.0 * ((direction + 1.0) / 2.0) +
        25.0 * ((breadth + 1.0) / 2.0) +
        10.0 * ((div + 1.0) / 2.0)
    )
    score = int(round(clamp(score, 0.0, 100.0)))

    # Gate：集中度很低 -> 觀察中
    if c20 < 4.0 and c5 < 4.0:
        return {"score": score, "trend": "觀察中", "tags": ["集中度偏低"]}

    tags = []
    if div > 0.3:
        tags.append("外/本協同")
    elif div < -0.3:
        tags.append("外/本分歧")

    if breadth > 0.25:
        tags.append("買盤擴散")
    elif breadth < -0.25:
        tags.append("賣盤擴散")

    if direction > 0.25:
        tags.append("淨買主導")
    elif direction < -0.25:
        tags.append("淨賣主導")

    if strength >= 0.70 and direction > 0.20 and breadth > 0.10:
        trend = "多" if div > 0.10 else "偏多"
        tags.append("強度高")
    elif strength >= 0.70 and direction < -0.20 and breadth < -0.10:
        trend = "空" if div > 0.10 else "偏空"
        tags.append("強度高")
    elif direction > 0.10 and breadth > 0.10 and strength >= 0.45:
        trend = "吸籌"
        tags.append("中等強度")
    elif direction < -0.10 and breadth < -0.10 and strength >= 0.45:
        trend = "派發"
        tags.append("中等強度")
    else:
        trend = "震盪"
        tags.append("方向拉扯")

    return {"score": score, "trend": trend, "tags": tags}


# ===================== TV Radar Signals (OHLCV 20D) =====================
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

    # ---------- 1) VWAP control (放寬: close >= vwap 或接近 vwap；vwap slope >= 0) ----------
    vwap = calc_vwap(df, window=min(50, max(10, len(df))))
    vwap_tail = vwap.tail(20).dropna()

    sig_vwap = 0
    slope_vwap10 = 0.0
    vwap_last = float(vwap.iloc[-1]) if pd.notna(vwap.iloc[-1]) else None
    close_last = float(df["close"].iloc[-1])

    if len(vwap_tail) >= 10 and vwap_last is not None and vwap_last != 0:
        slope_vwap10 = _rolling_slope(vwap_tail.values[-10:])
        near = abs(close_last - vwap_last) / vwap_last < 0.006  # 0.6% 內算控制
        sig_vwap = int((close_last >= vwap_last or near) and slope_vwap10 >= 0)

    # ---------- 2) Accumulation (OBV + A/D slope + volatility compression 放寬) ----------
    obv = calc_obv(df)
    ad = calc_ad(df)

    obv_s = _rolling_slope(obv.values[-20:])
    ad_s = _rolling_slope(ad.values[-20:])

    hl = (df["high"] - df["low"]).astype(float)
    atr20 = float(hl.rolling(20).mean().iloc[-1]) if pd.notna(hl.rolling(20).mean().iloc[-1]) else 0.0
    atr10 = float(hl.rolling(10).mean().iloc[-1]) if pd.notna(hl.rolling(10).mean().iloc[-1]) else 0.0
    compression = (atr20 > 0) and ((atr10 / atr20) < 0.92)

    sig_acc = int(obv_s > 0 and ad_s > 0 and compression)

    # ---------- 3) Volume spike rotation (z 門檻放寬 + body 判斷放寬) ----------
    sig_rot = 0
    z = 0.0
    body_pct = 0.0
    med_body_pct = 0.0

    vol = df["volume"].astype(float)
    if vol.sum() > 0:
        mu = float(vol.rolling(20).mean().iloc[-1]) if pd.notna(vol.rolling(20).mean().iloc[-1]) else 0.0
        sd = float(vol.rolling(20).std().iloc[-1]) if pd.notna(vol.rolling(20).std().iloc[-1]) else 0.0
        vol_last = float(vol.iloc[-1])

        z = (vol_last - mu) / sd if sd > 0 else 0.0

        body = abs(float(df["close"].iloc[-1]) - float(df["open"].iloc[-1]))
        body_pct = body / close_last if close_last else 0.0

        body_pct_series = (abs(df["close"] - df["open"]) / df["close"].replace(0, np.nan)).astype(float)
        mb = body_pct_series.rolling(20).median().iloc[-1]
        med_body_pct = float(mb) if pd.notna(mb) else 0.0

        sig_rot = int(z > 1.3 and med_body_pct > 0 and body_pct < med_body_pct * 1.10)

    # ---------- 4) Simplified Volume Profile HVN zone (放寬: HVN 附近 + 量縮或價差縮) ----------
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

            price = close_last
            # 放寬：允許落在 HVN 區間或距離 HVN 中心 0.5% 內
            hvn_mid = (hvn_low + hvn_high) / 2.0
            near_hvn = (hvn_low <= price <= hvn_high) or (hvn_mid and abs(price - hvn_mid) / hvn_mid < 0.005)

            vol_med = float(sub["volume"].median())
            vol_shrink = vol_last < vol_med

            # 量未縮也可用「價差縮」替代
            hl10 = float((sub["high"] - sub["low"]).rolling(10).mean().iloc[-1])
            hl20 = float((sub["high"] - sub["low"]).rolling(20).mean().iloc[-1])
            hl_shrink = (hl20 > 0) and ((hl10 / hl20) < 0.92)

            sig_profile = int(near_hvn and (vol_shrink or hl_shrink))

    # ---------- 5) Structure break (BOS) (保留原本：破 20 日高/低) ----------
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


# ===================== Analysis utils =====================
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()

    if "securities_trader_id" in df.columns and "broker_id" not in df.columns:
        df.rename(columns={"securities_trader_id": "broker_id"}, inplace=True)
    if "securities_trader" in df.columns and "broker_name" not in df.columns:
        df.rename(columns={"securities_trader": "broker_name"}, inplace=True)

    need = {"date", "broker_id", "broker_name", "buy", "sell"}
    if not need.issubset(set(df.columns)):
        return pd.DataFrame()

    df["broker_id"] = df["broker_id"].astype(str)
    df["broker_name"] = df["broker_name"].astype(str)

    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
    df["net"] = df["buy"] - df["sell"]

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

    return df


def compute_concentration(df_period: pd.DataFrame, top_n: int = 15) -> float:
    """
    集中度：TopN（以 net 彙總排序的買方）之 buy 量 / 全市場 buy 量
    """
    if df_period.empty:
        return 0.0
    total_buy = float(df_period["buy"].sum())
    if total_buy <= 0:
        return 0.0

    agg = df_period.groupby(["broker_id", "broker_name"], as_index=False).agg(
        net=("net", "sum"),
        buy=("buy", "sum"),
        sell=("sell", "sum"),
    )
    top_buyers = agg.sort_values("net", ascending=False).head(top_n)
    top_buy = float(top_buyers["buy"].sum())
    return round((top_buy / total_buy) * 100, 2)


def build_top15_tables(df_20d: pd.DataFrame, broker_map: dict, date_list: list[str]) -> dict:
    """
    產生 signals.top_buy_15 / top_sell_15（含連買/連賣、城市、外本）
    """
    if df_20d.empty:
        return {"top_buy_15": [], "top_sell_15": []}

    agg = df_20d.groupby(["broker_id", "broker_name"], as_index=False).agg(
        buy=("buy", "sum"),
        sell=("sell", "sum"),
        net=("net", "sum"),
    )
    agg["net_lot"] = (agg["net"] / 1000.0).round(1)

    pivot = df_20d.pivot_table(index="date", columns="broker_id", values="net", aggfunc="sum").fillna(0)
    pivot = pivot.reindex(date_list).fillna(0)
    streaks = compute_streaks(pivot)

    def enrich_row(r: pd.Series) -> dict:
        bid = str(r["broker_id"])
        meta = broker_map.get(bid, {})
        org = meta.get("broker_org_type", "unknown") or "unknown"
        city = meta.get("city", "")
        st = streaks.get(bid, {"streak_buy": 0, "streak_sell": 0})
        return {
            "broker_id": bid,
            "broker_name": str(r["broker_name"]),
            "net_lot": float(r["net_lot"]),
            "city": city,
            "broker_org_type": org,
            "streak_buy": int(st.get("streak_buy", 0)),
            "streak_sell": int(st.get("streak_sell", 0)),
        }

    top_buy = agg.sort_values("net", ascending=False).head(15).copy()
    top_sell = agg.sort_values("net", ascending=True).head(15).copy()

    top_buy_15 = [enrich_row(r) for _, r in top_buy.iterrows()]
    top_sell_15 = [enrich_row(r) for _, r in top_sell.iterrows()]

    # 保證 top_sell 是負數（lot）
    for x in top_sell_15:
        x["net_lot"] = -abs(float(x["net_lot"]))

    return {"top_buy_15": top_buy_15, "top_sell_15": top_sell_15}


def compute_foreign_local_net(df_period: pd.DataFrame, broker_map: dict) -> dict:
    """
    以 broker_org_type 分 foreign/local，計算期間 net（股數）
    """
    if df_period.empty:
        return {"foreign_net": 0.0, "local_net": 0.0}

    tmp = df_period[["broker_id", "net"]].copy()
    tmp["broker_id"] = tmp["broker_id"].astype(str)
    tmp["org"] = tmp["broker_id"].map(lambda x: (broker_map.get(x, {}).get("broker_org_type", "unknown") or "unknown"))

    foreign_net = float(tmp[tmp["org"] == "foreign"]["net"].sum())
    local_net = float(tmp[tmp["org"] == "local"]["net"].sum())
    return {"foreign_net": foreign_net, "local_net": local_net}


def build_breadth_series(df_all: pd.DataFrame, date_list: list[str]) -> dict:
    """
    逐日廣度序列：每一日看當日各券商 net>0 / net<0 的家數
    """
    labels = []
    buy_series = []
    sell_series = []
    breadth_series = []
    ratio_series = []

    if df_all.empty or not date_list:
        return {
            "labels_20d": [],
            "buy_count_series_20d": [],
            "sell_count_series_20d": [],
            "breadth_series_20d": [],
            "breadth_ratio_series_20d": [],
        }

    for d in date_list:
        df_day = df_all[df_all["date"] == d]
        b = calc_breadth(df_day)
        labels.append(d[5:])  # MM-DD
        buy_series.append(int(b["buy_count"]))
        sell_series.append(int(b["sell_count"]))
        breadth_series.append(int(b["breadth"]))
        ratio_series.append(float(b["breadth_ratio"]))

    return {
        "labels_20d": labels,
        "buy_count_series_20d": buy_series,
        "sell_count_series_20d": sell_series,
        "breadth_series_20d": breadth_series,
        "breadth_ratio_series_20d": ratio_series,
    }


# ===================== Main Analysis =====================
def analyze_whale_trajectory(
    frames: list[pd.DataFrame],
    target_dates: list[str],
    broker_map: dict,
    adapter: TaiwanStockAdapter,
    stock_id: str,
    debug_tv: bool = False,
):
    """
    - Top6 軌跡（10日）
    - signals（20日 + 5日 + 廣度序列 + TV radar）
    """
    if not frames:
        return None, None

    combined = pd.concat(frames, ignore_index=True)
    combined = standardize_columns(combined)
    if combined.empty:
        return None, None

    combined["date"] = combined["date"].astype(str)

    date_20d = list(target_dates)
    date_10d = date_20d[-10:] if len(date_20d) >= 10 else date_20d
    date_5d = date_20d[-5:] if len(date_20d) >= 5 else date_20d
    last_1d = date_20d[-1]

    df_20d = combined[combined["date"].isin(date_20d)].copy()
    df_10d = combined[combined["date"].isin(date_10d)].copy()
    df_5d = combined[combined["date"].isin(date_5d)].copy()

    # ===================== Top6（10日用 net 買方）=====================
    agg_10d = df_10d.groupby(["broker_id", "broker_name"], as_index=False).agg(
        buy=("buy", "sum"),
        sell=("sell", "sum"),
        net_buy=("net", "sum"),
    )
    top6 = agg_10d.sort_values("net_buy", ascending=False).head(6).copy()
    top6_ids = top6["broker_id"].astype(str).tolist()

    # Top6 details
    has_price = "price" in combined.columns
    top6_details = []
    for _, r in top6.iterrows():
        bid = str(r["broker_id"])
        bname = str(r["broker_name"])
        bdata = combined[combined["broker_id"] == bid]

        n10d = float(r["net_buy"]) / 1000.0
        n5d = float(bdata[bdata["date"].isin(date_5d)]["net"].sum()) / 1000.0
        n1d = float(bdata[bdata["date"] == last_1d]["net"].sum()) / 1000.0

        avg_p = 0.0
        if has_price:
            buy_only = bdata[bdata["buy"] > 0]
            if not buy_only.empty and float(buy_only["buy"].sum()) > 0:
                avg_p = float((buy_only["buy"] * buy_only["price"]).sum() / buy_only["buy"].sum())

        meta = broker_map.get(bid, {})
        top6_details.append({
            "broker_id": bid,
            "broker_name": bname,
            "net_10d": round(n10d, 1),
            "net_5d": round(n5d, 1),
            "net_1d": round(n1d, 1),
            "avg_price": round(avg_p, 2),
            "city": meta.get("city", ""),
            "broker_org_type": meta.get("broker_org_type", "unknown") or "unknown",
            "is_proprietary": meta.get("is_proprietary", ""),
            "seat_type": meta.get("seat_type", ""),
        })

    # 軌跡矩陣（Top6 累積 net）
    whale_detail = df_10d[df_10d["broker_id"].isin(top6_ids)].copy()
    pivot_net = whale_detail.pivot_table(index="date", columns="broker_name", values="net", aggfunc="sum").fillna(0)
    pivot_cumsum = pivot_net.reindex(date_10d).fillna(0).cumsum()

    colors = ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF", "#FF9F40"]
    whale_data = []
    for i, name in enumerate(pivot_cumsum.columns):
        whale_data.append({
            "name": name,
            "values": (pivot_cumsum[name] / 1000.0).round(1).tolist(),
            "color": colors[i % len(colors)],
        })

    total_whale_values = (pivot_cumsum.sum(axis=1) / 1000.0).round(1).tolist()

    # ===================== signals =====================
    c20 = compute_concentration(df_20d, top_n=15)
    c5 = compute_concentration(df_5d, top_n=15)

    net_1d_lot = round(float(combined[combined["date"] == last_1d]["net"].sum()) / 1000.0, 1)
    net_5d_lot = round(float(df_5d["net"].sum()) / 1000.0, 1)
    net_20d_lot = round(float(df_20d["net"].sum()) / 1000.0, 1)

    b20 = calc_breadth(df_20d)
    b5 = calc_breadth(df_5d)

    fl5 = compute_foreign_local_net(df_5d, broker_map)
    top15_pack = build_top15_tables(df_20d, broker_map, date_20d)
    breadth_series_pack = build_breadth_series(df_20d, date_20d)

    signals = {
        "concentration_5d": c5,
        "concentration_20d": c20,

        "netbuy_1d_lot": net_1d_lot,
        "netbuy_5d_lot": net_5d_lot,
        "netbuy_20d_lot": net_20d_lot,

        "buy_count_5d": b5["buy_count"],
        "sell_count_5d": b5["sell_count"],
        "breadth_5d": b5["breadth"],
        "breadth_ratio_5d": b5["breadth_ratio"],

        "buy_count_20d": b20["buy_count"],
        "sell_count_20d": b20["sell_count"],
        "breadth_20d": b20["breadth"],
        "breadth_ratio_20d": b20["breadth_ratio"],

        "foreign_net_5d": fl5["foreign_net"],  # 股數
        "local_net_5d": fl5["local_net"],      # 股數

        "top_buy_15": top15_pack["top_buy_15"],
        "top_sell_15": top15_pack["top_sell_15"],

        # series（dashboard 廣度趨勢）
        **breadth_series_pack,
    }

    # ===================== TV Smart Money Radar（一次就好）=====================
    ohlcv_20d = fetch_ohlcv_20d(adapter, stock_id, date_20d, debug=debug_tv)
    if debug_tv:
        print("OHLCV rows=", 0 if ohlcv_20d is None else len(ohlcv_20d))
        print("OHLCV cols=", [] if ohlcv_20d is None else list(ohlcv_20d.columns))
        print("OHLCV tail=\n", ohlcv_20d.tail(3) if ohlcv_20d is not None and not ohlcv_20d.empty else None)

    tv_pack = compute_tv_radar_signals(ohlcv_20d, debug=debug_tv)
    if debug_tv:
        print("TV_PACK=", tv_pack)

    signals.update(tv_pack)

    # ===================== 最後算主力走向（一次）=====================
    trend_pack = compute_master_trend(signals)
    signals["score"] = trend_pack["score"]
    signals["trend"] = trend_pack["trend"]
    signals["tags"] = trend_pack["tags"]

    insight = {
        "history_labels": [d[5:] for d in date_10d],  # MM-DD
        "whale_data": whale_data,
        "total_whale_values": total_whale_values,
        "top6_details": top6_details,
        "signals": signals,
    }

    boss_list_df = agg_10d.sort_values("net_buy", ascending=False).head(20).reset_index(drop=True)
    boss_list_df.rename(columns={"net_buy": "net"}, inplace=True)
    boss_list_df["net_lot"] = (boss_list_df["net"] / 1000.0).round(1)

    return insight, boss_list_df


# ===================== Runner =====================
def run_strategy(stock_id: str, days: int = 20, throttle_sec: float = 0.6, verify_ssl: bool = True, debug_tv: bool = False):
    load_dotenv()
    token = os.getenv("FINMIND_API_TOKEN", "").strip()
    if not token:
        print("❌ 找不到 FINMIND_API_TOKEN，請在 .env 設定 FINMIND_API_TOKEN=你的token")
        return

    client = FinMindClient(token=token, verify_ssl=verify_ssl)
    adapter = TaiwanStockAdapter(client)
    broker_map = load_broker_master_enriched(DATA_PATH)

    # 1) 交易日（排除今天）
    all_dates = adapter.get_trading_dates(lookback=180)
    today_str = datetime.now().strftime("%Y-%m-%d")
    target_dates = [d for d in all_dates if d < today_str][-days:]

    if not target_dates:
        print("❌ 無法取得交易日，終止。")
        return

    if len(target_dates) < days:
        print(f"⚠️ 交易日不足 {days} 天，實際取得 {len(target_dates)} 天: {target_dates[0]} ~ {target_dates[-1]}")

    # 2) 抓分點明細
    stock_name = adapter.get_stock_name(stock_id)
    print(f"🎯 開始追蹤 {stock_id} {stock_name} | 近 {len(target_dates)} 日分點...")

    frames = []
    for d in target_dates:
        report = adapter.get_daily_report(stock_id, d)
        if not report.empty:
            report["date"] = d
            frames.append(report)
            print(f"✅ 已載入 {d} 數據: {len(report)} rows")
        else:
            print(f"⚠️ {d} 分點數據為空（可能權限不足/尚未入庫/假日）")
        time.sleep(throttle_sec)

    if not frames:
        print("❌ 全部日期分點都為空：請檢查 sponsor 權限或 token 是否正確。")
        return

    # 3) 分析與輸出
    insight, boss_list_df = analyze_whale_trajectory(frames, target_dates, broker_map, adapter, stock_id, debug_tv=debug_tv)
    if insight is None or boss_list_df is None:
        print("❌ 分析失敗：分點欄位不齊或資料格式異常。")
        return

    # 把 stock_name 寫進 JSON，讓 dashboard 正確顯示「6239 力成」
    insight["stock_id"] = stock_id
    insight["stock_name"] = stock_name
    insight["probe_date"] = target_dates[-1]
    insight["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    json_path = os.path.join(DATA_PATH, f"{stock_id}_whale_track.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(insight, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON輸出完成: {json_path}")

    csv_path = os.path.join(DATA_PATH, f"{stock_id}_boss_list.csv")
    boss_list_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"💾 CSV輸出完成: {csv_path}")

    sig = insight.get("signals", {})
    print(
        f"📊 完成：Score={sig.get('score',0)} | Trend={sig.get('trend','')} | "
        f"TV={sig.get('tv_score',0)}/5({sig.get('tv_grade','')}) | "
        f"5D集中度={sig.get('concentration_5d',0)}% | 20D集中度={sig.get('concentration_20d',0)}% | "
        f"20D買超家數={sig.get('buy_count_20d',0)} | 20D賣超家數={sig.get('sell_count_20d',0)} | "
        f"series_len={len(sig.get('labels_20d',[]))}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock_id", default="6239", help="股票代號，如 6239")
    parser.add_argument("--days", type=int, default=20, help="回溯交易日數（建議 20，對應 dashboard 的 20日）")
    parser.add_argument("--throttle", type=float, default=0.6, help="每次請求間隔秒數")
    parser.add_argument("--no_ssl_verify", action="store_true", help="關閉 SSL verify（公司網路憑證問題才用）")
    parser.add_argument("--debug_tv", action="store_true", help="印出 OHLCV / TV debug 資訊（並把 tv_debug 寫進 JSON）")
    args = parser.parse_args()

    run_strategy(
        stock_id=args.stock_id,
        days=args.days,
        throttle_sec=args.throttle,
        verify_ssl=not args.no_ssl_verify,
        debug_tv=args.debug_tv,
    )


if __name__ == "__main__":
    main()
