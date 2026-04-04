from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import numpy as np
import polars as pl


@dataclass(frozen=True, slots=True)
class PipelinePaths:
    pipeline_root: Path
    artifacts_dir: Path
    sql_path: Path
    setup_json_path: Path


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    history_months: int
    product_ids: tuple[int, ...]
    min_train_points: int
    max_history: int
    model_id: str
    forecast_batch_size: int
    timesfm_max_context: int
    timesfm_max_horizon: int
    timesfm_per_core_batch_size: int
    result_chunk_size: int
    xgboost_feature_window_months: int
    xgboost_category_history_months: int
    xgboost_share_history_months: int
    xgboost_category_holdout_days: int
    xgboost_share_holdout_months: int
    xgboost_max_depth: int
    xgboost_estimators: int
    xgboost_learning_rate: float
    xgboost_subsample: float
    xgboost_colsample_bytree: float
    xgboost_min_child_weight: float
    xgboost_reg_lambda: float
    xgboost_random_state: int
    xgboost_objective_prediction: str


@dataclass(frozen=True, slots=True)
class ForecastSpec:
    group_cols: tuple[str, ...]
    time_col: str
    value_col: str
    prediction_col: str


@dataclass(frozen=True, slots=True)
class RollingForecastStats:
    series_count: int
    rolling_points: int
    model_calls: int


@dataclass(frozen=True, slots=True)
class RollingForecastBuffers:
    batch_inputs: list[np.ndarray]
    batch_meta: list[tuple[tuple[object, ...], list[object], list[float]]]
    result_frames: list[pl.DataFrame]
    buffer_columns: dict[str, list[object]]


class TimesFmLike(Protocol):
    def forecast(self, horizon: int, inputs: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
        """
        Forecasts future values for a batch of numeric histories.

        Args:
            horizon (int): Number of future steps predicted for each series.
            inputs (list[np.ndarray]): Historical numeric series passed to the forecaster.

        Returns:
            tuple: Forecast means and auxiliary model outputs for the requested horizon.
        """


@dataclass(frozen=True, slots=True)
class TrainValidationSplit:
    cutoff: object
    train_rows: int
    validation_rows: int


@dataclass(frozen=True, slots=True)
class ModelMetrics:
    model_name: str
    target_col: str
    train_rows: int
    validation_rows: int
    mae: float
    rmse: float


@dataclass(frozen=True, slots=True)
class ResidualModelArtifacts:
    features: pl.DataFrame
    predictions: pl.DataFrame
    audit: pl.DataFrame
    metrics: ModelMetrics


@dataclass(frozen=True, slots=True)
class HistoricalExtracts:
    sales: pl.DataFrame
    quotations: pl.DataFrame
    stock: pl.DataFrame
