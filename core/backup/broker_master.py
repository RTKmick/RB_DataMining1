import os
import pandas as pd


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
