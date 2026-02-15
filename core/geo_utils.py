# core/geo_utils.py
# 目的：提供距離（km）與地緣特徵計算。只做純數學與輕量特徵，不碰 API。

from __future__ import annotations
import math
from functools import lru_cache
from typing import Any


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    計算兩點球面距離（公里）
    """
    # 地球半徑 (km)
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * (math.sin(dlambda / 2.0) ** 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c


def _to_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def compute_geo_topn_features(
    top_rows: list[dict],
    hq_lat: float | None,
    hq_lon: float | None,
    broker_map: dict,
    top_n: int = 5,
) -> dict:
    """
    top_rows：通常是 signals["top_buy_15"] 之類的 list[dict]，必須包含 broker_id 與 net_lot
    回傳：
      geo_top5_avg_km / geo_top5_min_km / geo_top5_wavg_km / geo_affinity_score / geo_top5_detail
    """
    if hq_lat is None or hq_lon is None:
        return {
            "geo_top5_avg_km": None,
            "geo_top5_min_km": None,
            "geo_top5_wavg_km": None,
            "geo_affinity_score": None,
            "geo_top5_detail": [],
        }

    # 取 TopN 買超（net_lot > 0）前 N 筆
    buys = [r for r in (top_rows or []) if float(r.get("net_lot", 0) or 0) > 0]
    buys = buys[: max(1, int(top_n))]

    dists = []
    wsum = 0.0
    wdist = 0.0
    detail = []

    for r in buys:
        bid = str(r.get("broker_id", "")).strip()
        meta = (broker_map.get(bid, {}) or {})
        lat = _to_float(meta.get("lat"))
        lon = _to_float(meta.get("lon"))
        if lat is None or lon is None:
            continue

        km = haversine_km(float(hq_lat), float(hq_lon), float(lat), float(lon))
        dists.append(km)

        w = abs(float(r.get("net_lot", 0) or 0))
        wsum += w
        wdist += w * km

        detail.append({
            "broker_id": bid,
            "broker_name": r.get("broker_name", ""),
            "net_lot": float(r.get("net_lot", 0) or 0),
            "km_to_hq": round(km, 1),
        })

    if not dists:
        return {
            "geo_top5_avg_km": None,
            "geo_top5_min_km": None,
            "geo_top5_wavg_km": None,
            "geo_affinity_score": None,
            "geo_top5_detail": detail,
        }

    avg_km = sum(dists) / len(dists)
    min_km = min(dists)
    wavg_km = (wdist / wsum) if wsum > 0 else avg_km

    # affinity：距離越近分數越高（0~100），用簡單的倒數衰減
    # 0km -> 100, 30km ~ 50~70, 100km -> 20~40 (可自行調整 k)
    k = 50.0
    affinity = 100.0 * (1.0 / (1.0 + (wavg_km / k)))

    return {
        "geo_top5_avg_km": round(avg_km, 1),
        "geo_top5_min_km": round(min_km, 1),
        "geo_top5_wavg_km": round(wavg_km, 1),
        "geo_affinity_score": round(affinity, 1),
        "geo_top5_detail": detail,
    }
