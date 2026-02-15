# core/aggregate.py
from typing import Dict
from .config import PipelineConfig
from typing import Dict
from core.config import PipelineConfig

def grade_from_score(score: float, cfg: PipelineConfig) -> str:
    fs = cfg.final_score
    if score >= fs.grade_a:
        return "A"
    if score >= fs.grade_b:
        return "B"
    if score >= fs.grade_c:
        return "C"
    return "D"

def compute_final_pack(signals: Dict, cfg: PipelineConfig) -> Dict:
    score = float(signals.get("score", 0) or 0)
    regime_score = float(signals.get("regime_score", 0) or 0)

    w_whale = float(cfg.final_score.weight_whale)
    w_regime = float(cfg.final_score.weight_regime)
    denom = (w_whale + w_regime) if (w_whale + w_regime) != 0 else 1.0

    base = (w_whale * score + w_regime * regime_score) / denom
    base = round(base, 1)

    # ✅ 策略A：折減式 geo_adjust（避免 baseline_all 直接腰斬）
    # gw 建議範圍 0.5~1.0；若 gw=0.7 => geo_adjust=0.85
    try:
        gw = float(signals.get("geo_baseline_weight", 1.0) or 1.0)
    except Exception:
        gw = 1.0
    gw = max(0.0, min(1.0, gw))

    geo_adjust = 1.0 - (1.0 - gw) * 0.5
    geo_adjust = round(geo_adjust, 3)

    final_score = round(base * geo_adjust, 1)

    return {
        "final_score": final_score,
        "final_grade": grade_from_score(final_score, cfg),
        "weights": {"whale": w_whale, "regime": w_regime},
        "geo_adjust": geo_adjust,
    }

