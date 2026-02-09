# core/aggregate.py
from typing import Dict
from .config import PipelineConfig


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

    final_score = round((w_whale * score + w_regime * regime_score) / denom, 1)

    return {
        "final_score": final_score,
        "final_grade": grade_from_score(final_score, cfg),
        "weights": {"whale": w_whale, "regime": w_regime},
    }
