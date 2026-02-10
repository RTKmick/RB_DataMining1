# core/pipeline.py
import pandas as pd

from core.signals_whale import (
    standardize_columns,
    compute_concentration,
    calc_breadth,
    compute_foreign_local_net,
    build_top15_tables,
    build_breadth_series,
    compute_master_trend,
)
from core.price_data import fetch_ohlcv_20d, fetch_price_nd
from core.indicators_tv import compute_tv_radar_signals
from core.regime import compute_regime_signals

from core.config import PipelineConfig
from core.aggregate import compute_final_pack
from core.types import Insight


def analyze_whale_trajectory(
    frames: list[pd.DataFrame],
    target_dates: list[str],
    broker_map: dict,
    adapter,
    stock_id: str,
    debug_tv: bool = False,
    cfg: PipelineConfig | None = None,
) -> tuple[Insight | None, pd.DataFrame | None]:
    """
    - Top6 軌跡（10日）
    - signals（20日 + 5日 + 廣度序列 + TV radar + Regime + Final aggregation）
    """
    cfg = cfg or PipelineConfig()

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

    # Top6（10日用 net 買方）
    agg_10d = df_10d.groupby(["broker_id", "broker_name"], as_index=False).agg(
        buy=("buy", "sum"),
        sell=("sell", "sum"),
        net_buy=("net", "sum"),
    )
    top6 = agg_10d.sort_values("net_buy", ascending=False).head(6).copy()
    top6_ids = top6["broker_id"].astype(str).tolist()

    # 讓 Top6 也有 streak
    from core.signals_whale import compute_streaks  # 若尚未 import
    pivot10 = (
        df_10d.pivot_table(index="date", columns="broker_id", values="net", aggfunc="sum")
        .fillna(0)
        .reindex(date_10d)
        .fillna(0)
    )
    streaks10 = compute_streaks(pivot10)

    # Top6 details
    has_price = "price" in combined.columns
    top6_details = []

    # 先做一次清洗，避免型別/空白造成對不到
    combined["broker_id"] = combined["broker_id"].astype(str).str.strip()

    # 確保 streaks10 一定是 dict（避免 None / 其他型別）
    if not isinstance(streaks10, dict):
        streaks10 = {}

    for _, r in top6.iterrows():
        bid = str(r.get("broker_id", "")).strip()
        if not bid:
            continue

        bname = str(r.get("broker_name", "")).strip()

        # streaks（用 bid 查）
        st = streaks10.get(bid, {}) or {}
        sb = int(st.get("streak_buy", 0) or 0)
        ss = int(st.get("streak_sell", 0) or 0)

        bdata = combined[combined["broker_id"] == bid]

        n10d = float(r.get("net_buy", 0) or 0) / 1000.0
        n5d = float(bdata[bdata["date"].isin(date_5d)]["net"].sum()) / 1000.0
        n1d = float(bdata[bdata["date"] == last_1d]["net"].sum()) / 1000.0

        avg_p = 0.0
        if has_price and not bdata.empty:
            buy_only = bdata[bdata["buy"] > 0]
            if not buy_only.empty and float(buy_only["buy"].sum()) > 0:
                avg_p = float((buy_only["buy"] * buy_only["price"]).sum() / buy_only["buy"].sum())

        meta = broker_map.get(bid, {}) or {}
        print(f"🔎 TOP6 bid={bid} name={bname} in_broker_map={bool(meta)} meta_keys={list(meta.keys())[:6]}")

        top6_details.append({
            "broker_id": bid,
            "broker_name": bname,
            "net_10d": round(n10d, 1),
            "net_5d": round(n5d, 1),
            "net_1d": round(n1d, 1),
            "avg_price": round(avg_p, 2),
            "city": meta.get("city", "") or "",
            "broker_org_type": meta.get("broker_org_type", "unknown") or "unknown",
            "is_proprietary": meta.get("is_proprietary", "") or "",
            "seat_type": meta.get("seat_type", "") or "",
            "streak_buy": sb,
            "streak_sell": ss,
        })


    # Top6 軌跡矩陣（累積 net）
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

    # signals（短期）
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

        "foreign_net_5d": fl5["foreign_net"],
        "local_net_5d": fl5["local_net"],

        "top_buy_15": top15_pack["top_buy_15"],
        "top_sell_15": top15_pack["top_sell_15"],

        **breadth_series_pack,
    }

    import math

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _tanh_0_100(x):# x=0 -> 50；正向上升 -> 越接近100；負向 -> 越接近0
        return round(50.0 + 50.0 * math.tanh(x), 1)

    # === A. ΔMajorFlow20：買方 Top15 張數 - 賣方 Top15 張數 ===
    buy_sum_20 = 0.0
    sell_sum_20 = 0.0

    if "top_buy_15" in top15_pack and top15_pack["top_buy_15"]:
        buy_sum_20 = sum([float(r["net_lot"]) for r in top15_pack["top_buy_15"]])

    if "top_sell_15" in top15_pack and top15_pack["top_sell_15"]:
        # r["net_lot"] 是負值，需取絕對值
        sell_sum_20 = sum([abs(float(r["net_lot"])) for r in top15_pack["top_sell_15"]])

    delta_major_flow_20 = round(buy_sum_20 - sell_sum_20, 1)

    signals["delta_major_flow_20"] = delta_major_flow_20
    signals["top15_buy_sum_20"] = round(buy_sum_20, 1)
    signals["top15_sell_sum_20"] = round(sell_sum_20, 1)
    # ----------------------------------------------------------
    # === B. 主力拐點偵測 Turning Points ===
    tp = {}

    # 1) 外資 / 本土 轉折
    foreign_5 = float(fl5.get("foreign_net", 0.0))
    local_5 = float(fl5.get("local_net", 0.0))

    # 1-1 外資過去 5 日為賣方（總 net <0） 且 今日 net >0
    today_net = float(combined[combined["date"] == last_1d]["net"].sum())
    foreign_today = float(df_1d[df_1d["org"] == "foreign"]["net"].sum()) if 'df_1d' in locals() else 0
    local_today = float(df_1d[df_1d["org"] == "local"]["net"].sum()) if 'df_1d' in locals() else 0

    tp["foreign_buy_switch"] = int(foreign_5 < 0 and foreign_today > 0)
    tp["local_buy_switch"] = int(local_5 < 0 and local_today > 0)

    # 2) Top6 大戶反轉（連賣 -> 首日轉買）
    tp["whale_reversal"] = 0
    for bid in top6_ids:
        seq = df_20d[df_20d["broker_id"] == bid].sort_values("date")["net"].tolist()
        if len(seq) >= 4:
            if seq[-4] < 0 and seq[-3] < 0 and seq[-2] < 0 and seq[-1] > 0:
                tp["whale_reversal"] = 1
                break

    # 3) 異常爆量買超（買超 >= 過去20日平均 * 3）
    mean_buy20 = df_20d["buy"].mean()
    today_buy = df_20d[df_20d["date"] == last_1d]["buy"].sum()
    tp["abnormal_buy_spike"] = int(today_buy >= mean_buy20 * 3)

    # 4) 主力成本帶（Top6 均價）防守
    avg_prices = [float(r["avg_price"]) for r in top6_details if r["avg_price"] > 0]
    if avg_prices:
        cost_low = min(avg_prices)
        close_recent = price_df_tail["close"].tail(3).min() if "price_df_tail" in locals() else None
        tp["cost_zone_defended"] = int(close_recent is not None and close_recent >= cost_low)
    else:
        tp["cost_zone_defended"] = 0

    signals["turning_points"] = tp
    # ----------------------------------------------------------
    # === C1: 群聚買超 Coherence ===
    # 今日所有大戶買超家數
    df_today = df_20d[df_20d["date"] == last_1d]
    g_today = df_today.groupby("broker_id")["net"].sum()
    buy_cnt_today = int((g_today > 0).sum())

    # 過去 20 日平均買超家數
    g_all = df_20d.groupby("date", group_keys=False).apply(lambda d: (d.groupby("broker_id")["net"].sum() > 0).sum())
    avg_buy_cnt = float(g_all.mean()) if len(g_all) > 0 else 1.0

    coherence = buy_cnt_today / avg_buy_cnt if avg_buy_cnt > 0 else 0
    coherence = round(min(max(coherence, 0.0), 3.0), 2)

    enhanced = {}
    enhanced["coherence_today"] = coherence
    enhanced["buy_cnt_today"] = buy_cnt_today
    enhanced["avg_buy_cnt_20d"] = round(avg_buy_cnt, 1)
    # ----------------------------------------------------------

    # ========= Whale Radar (0~100) =========
    c20_val = float(signals.get("concentration_20d", 0) or 0)
    c20_score = round(_clamp(c20_val, 0, 100), 1)

    net1 = float(signals.get("netbuy_1d_lot", 0) or 0)
    net5 = float(signals.get("netbuy_5d_lot", 0) or 0)
    net20 = float(signals.get("netbuy_20d_lot", 0) or 0)

    # 2) 淨買方向：用 20D 尺度做 normalize，避免量級差
    scale = max(10.0, abs(net20) / 4.0)   # 你可調：/3 /5 都行
    net_dir_score = _tanh_0_100(net5 / scale)

    # 3) 買盤廣度：breadth_ratio_5d (0~1) -> 0~100
    br5 = float(signals.get("breadth_ratio_5d", 0) or 0)
    breadth_score = round(_clamp(br5, 0, 1) * 100.0, 1)

    # 4) 外本協同：同向加分、不同向扣分，並以幅度做加權
    f5 = float(signals.get("foreign_net_5d", 0) or 0)
    l5 = float(signals.get("local_net_5d", 0) or 0)
    same_sign = (f5 == 0 and l5 == 0) or (f5 > 0 and l5 > 0) or (f5 < 0 and l5 < 0)

    mag = abs(f5) + abs(l5)
    mag_score = _clamp(mag / max(1.0, mag + 20000.0), 0, 1)  # 這個 20000 可調：越小越敏感
    base = 65.0 if same_sign else 35.0
    align_score = round(_clamp(base + 35.0 * mag_score, 0, 100), 1)

    # 5) 短期加速：1D vs 5D日均（避免 5D=0 爆炸）
    avg5 = net5 / 5.0
    den = max(5.0, abs(avg5))  # 保底
    acc_score = _tanh_0_100(net1 / den)

    signals["whale_radar"] = {
        "labels": ["集中度", "淨買方向", "買盤廣度", "外本協同", "短期加速"],
        "values": [c20_score, net_dir_score, breadth_score, align_score, acc_score],
        "debug": {
            "c20": c20_val,
            "net1": net1, "net5": net5, "net20": net20, "scale": scale,
            "breadth_ratio_5d": br5,
            "foreign_5d": f5, "local_5d": l5, "same_sign": int(same_sign),
            "avg5": avg5, "den": den
        }
    }
    print("🔎 whale_radar(pipeline)=", signals["whale_radar"])

    # === C2: 主力成本帶 Range + 乖離率 ===
    avg_prices = [float(r["avg_price"]) for r in top6_details if r["avg_price"] > 0]
    if avg_prices:
        cost_low = min(avg_prices)
        cost_high = max(avg_prices)
        enhanced["cost_low"] = round(cost_low, 2)
        enhanced["cost_high"] = round(cost_high, 2)

        if "price_df" in locals():
            close_last = float(price_df.iloc[-1]["close"])
            enhanced["close_last"] = close_last

            # 乖離率
            cost_dev = (close_last - cost_low) / cost_low if cost_low > 0 else 0
            enhanced["cost_deviation"] = round(cost_dev, 3)

            # 區域分類
            if close_last > cost_high:
                enhanced["cost_zone_status"] = "above_zone"
            elif close_last >= cost_low:
                enhanced["cost_zone_status"] = "inside_zone"
            else:
                enhanced["cost_zone_status"] = "below_zone"
    else:
        enhanced["cost_zone_status"] = "unknown"
    # ----------------------------------------------------------
    # === C3: 連買強度 ===
    streak_strength = 0.0
    for r in top6_details:
        sb = int(r["streak_buy"])
        if sb <= 0:
            continue
        # 該大戶五日買超張數
        bid = r["broker_id"]
        df_b = df_5d[df_5d["broker_id"] == bid]
        net5 = float(df_b["net"].sum()) / 1000.0
        avg5 = float(df_5d["net"].sum()) / (len(top6_ids) * 1000.0)
        if avg5 <= 0:
            continue
        strength = (math.log(sb + 1)) * (net5 / avg5)
        streak_strength = max(streak_strength, strength)

    enhanced["streak_strength"] = round(streak_strength, 3)
    # ----------------------------------------------------------

    # Regime（中期趨勢底座）
    price_250d = fetch_price_nd(adapter, stock_id, lookback_days=int(cfg.regime.lookback_days))
    regime_pack = compute_regime_signals(price_250d)
    signals.update(regime_pack)

    # TV Radar（短期價格行為）
    ohlcv_20d = fetch_ohlcv_20d(adapter, stock_id, date_20d, debug=debug_tv)
    if debug_tv:
        print("OHLCV rows=", 0 if ohlcv_20d is None else len(ohlcv_20d))
        print("OHLCV cols=", [] if ohlcv_20d is None else list(ohlcv_20d.columns))
        print("OHLCV tail=\n", ohlcv_20d.tail(3) if ohlcv_20d is not None and not ohlcv_20d.empty else None)

    tv_pack = compute_tv_radar_signals(ohlcv_20d, debug=debug_tv)
    if debug_tv:
        print("TV_PACK=", tv_pack)

    signals.update(tv_pack)

    # 最後算主力走向（一次）
    trend_pack = compute_master_trend(signals)
    signals["score"] = float(trend_pack.get("score", 0) or 0)
    signals["trend"] = trend_pack.get("trend", "")
    signals["tags"] = trend_pack.get("tags", [])
    # NEW: radar pack
    signals["whale_radar"] = trend_pack.get("whale_radar", {})

    # ---------------------------------
    def norm_pct(x):
        try:
            v = float(x or 0)
        except Exception:
            return 0.0
        # 若 v <= 1，推定是 0~1，轉成 0~100
        if 0 <= v <= 1:
            v *= 100.0
        return max(0.0, min(100.0, v))
    # ---------------------------------

    # Aggregation（final_score / grade / weights）
    signals.update(compute_final_pack(signals, cfg))

    insight: Insight = {
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
