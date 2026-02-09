# core/config.py
from dataclasses import dataclass


@dataclass(frozen=True)
class RegimeConfig:
    # fetch_price_nd 用的日曆天 buffer（420 ≈ 250 交易日）
    lookback_days: int = 420


@dataclass(frozen=True)
class FinalScoreConfig:
    # final_score 權重
    weight_whale: float = 0.6
    weight_regime: float = 0.4

    # final_grade 門檻
    grade_a: float = 75.0
    grade_b: float = 60.0
    grade_c: float = 45.0


@dataclass(frozen=True)
class PipelineConfig:
    regime: RegimeConfig = RegimeConfig()
    final_score: FinalScoreConfig = FinalScoreConfig()
