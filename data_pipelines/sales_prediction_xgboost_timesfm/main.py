import json
import logging
import os
from pathlib import Path

import polars as pl

from core import domain_timesfm, domain_xgboost, infrastructure, schemas

LOGGER = logging.getLogger("sales_prediction_xgboost_timesfm")
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PIPELINE_ROOT = Path(__file__).resolve().parent
ARTIFACTS_DIR = PIPELINE_ROOT / "artifacts"
SQL_PATH = PIPELINE_ROOT / "sql"
JSON_SETUP_PATH = PIPELINE_ROOT / "json" / "setup_sales_prediction_xgboost_timesfm.json"

DEFAULT_PARAMETERS = {
    "history_months": 24,
    "product_ids": [],
    "min_train_points": 3,
    "max_history": 0,
    "model_id": "google/timesfm-2.5-200m-pytorch",
    "forecast_batch_size": 512,
    "timesfm_max_context": 2048,
    "timesfm_max_horizon": 180,
    "timesfm_per_core_batch_size": 64,
    "result_chunk_size": 50000,
    "xgboost_feature_window_months": 6,
    "xgboost_category_history_months": 24,
    "xgboost_share_history_months": 24,
    "xgboost_category_holdout_days": 30,
    "xgboost_share_holdout_months": 3,
    "xgboost_max_depth": 6,
    "xgboost_estimators": 300,
    "xgboost_learning_rate": 0.05,
    "xgboost_subsample": 0.9,
    "xgboost_colsample_bytree": 0.9,
    "xgboost_min_child_weight": 1.0,
    "xgboost_reg_lambda": 1.0,
    "xgboost_random_state": 42,
    "xgboost_objective_prediction": "reg:squarederror",
}


def get_pipeline_paths() -> schemas.PipelinePaths:
    """
    Resolves the local paths used by the portfolio DAG.

    Returns:
        schemas.PipelinePaths: Structured local paths used by the portfolio pipeline.
    """

    return schemas.PipelinePaths(
        pipeline_root=PIPELINE_ROOT,
        artifacts_dir=ARTIFACTS_DIR,
        sql_path=SQL_PATH,
        setup_json_path=JSON_SETUP_PATH,
    )


def build_runtime_config() -> schemas.RuntimeConfig:
    """
    Builds the runtime configuration from environment parameters and defaults.

    Returns:
        schemas.RuntimeConfig: Runtime configuration merged from defaults and environment overrides.
    """

    env_value = os.getenv("SALES_PREDICTION_XGBOOST_TIMESFM_PARAMS", "").strip()
    loaded = json.loads(env_value) if env_value else {}
    params = {**DEFAULT_PARAMETERS, **loaded}
    return schemas.RuntimeConfig(
        history_months=int(params["history_months"]),
        product_ids=tuple(int(product_id) for product_id in params["product_ids"]),
        min_train_points=int(params["min_train_points"]),
        max_history=int(params["max_history"]),
        model_id=str(params["model_id"]),
        forecast_batch_size=int(params["forecast_batch_size"]),
        timesfm_max_context=int(params["timesfm_max_context"]),
        timesfm_max_horizon=int(params["timesfm_max_horizon"]),
        timesfm_per_core_batch_size=int(params["timesfm_per_core_batch_size"]),
        result_chunk_size=int(params["result_chunk_size"]),
        xgboost_feature_window_months=int(params["xgboost_feature_window_months"]),
        xgboost_category_history_months=int(params["xgboost_category_history_months"]),
        xgboost_share_history_months=int(params["xgboost_share_history_months"]),
        xgboost_category_holdout_days=int(params["xgboost_category_holdout_days"]),
        xgboost_share_holdout_months=int(params["xgboost_share_holdout_months"]),
        xgboost_max_depth=int(params["xgboost_max_depth"]),
        xgboost_estimators=int(params["xgboost_estimators"]),
        xgboost_learning_rate=float(params["xgboost_learning_rate"]),
        xgboost_subsample=float(params["xgboost_subsample"]),
        xgboost_colsample_bytree=float(params["xgboost_colsample_bytree"]),
        xgboost_min_child_weight=float(params["xgboost_min_child_weight"]),
        xgboost_reg_lambda=float(params["xgboost_reg_lambda"]),
        xgboost_random_state=int(params["xgboost_random_state"]),
        xgboost_objective_prediction=str(params["xgboost_objective_prediction"]),
    )


def _get_xgboost_params(config: schemas.RuntimeConfig) -> dict[str, object]:
    """
    Builds the XGBoost regressor parameter dictionary from the runtime config.

    Args:
        config (schemas.RuntimeConfig): Runtime configuration containing XGBoost hyperparameters.

    Returns:
        dict[str, object]: Parameter dictionary passed to the XGBoost regressor.
    """

    return {
        "max_depth": config.xgboost_max_depth,
        "n_estimators": config.xgboost_estimators,
        "learning_rate": config.xgboost_learning_rate,
        "subsample": config.xgboost_subsample,
        "colsample_bytree": config.xgboost_colsample_bytree,
        "min_child_weight": config.xgboost_min_child_weight,
        "reg_lambda": config.xgboost_reg_lambda,
        "random_state": config.xgboost_random_state,
        "objective": config.xgboost_objective_prediction,
        "enable_categorical": True,
        "tree_method": "hist",
        "eval_metric": "rmse",
    }


def extract_historical_inputs(paths: schemas.PipelinePaths, config: schemas.RuntimeConfig) -> schemas.HistoricalExtracts:
    """
    Extracts the full historical inputs required by the portfolio pipeline.

    Args:
        paths (schemas.PipelinePaths): Local pipeline paths containing the SQL directory.
        config (schemas.RuntimeConfig): Runtime configuration controlling the extraction scope.

    Returns:
        schemas.HistoricalExtracts: Non-sensitive sales, quotation, and stock extracts.
    """

    extracts = infrastructure.extract_historical_inputs(paths, config)
    if extracts.sales.is_empty():
        raise RuntimeError("Sales extraction returned no rows.")
    if extracts.quotations.is_empty():
        raise RuntimeError("Quotation extraction returned no rows.")
    if extracts.stock.is_empty():
        raise RuntimeError("Stock extraction returned no rows.")
    return extracts


def run_timesfm_pipeline(
    sales_raw: pl.DataFrame,
    config: schemas.RuntimeConfig,
    paths: schemas.PipelinePaths,
) -> dict[str, object]:
    """
    Builds the historical TimesFM baselines and persists the baseline artifacts locally.

    Args:
        sales_raw (pl.DataFrame): Raw sales extract returned by the generic connector.
        config (schemas.RuntimeConfig): Runtime configuration controlling model execution.
        paths (schemas.PipelinePaths): Local paths used to persist the TimesFM artifacts.

    Returns:
        dict[str, object]: Persisted TimesFM outputs and execution statistics.
    """

    sales = domain_timesfm.normalize_sales_dataframe(sales_raw)
    monthly_share = domain_timesfm.build_monthly_share_base(sales)
    daily_gmv = domain_timesfm.build_daily_gmv_base(sales)
    model = infrastructure.load_timesfm_model(
        model_id=config.model_id,
        max_context=config.timesfm_max_context,
        max_horizon=config.timesfm_max_horizon,
        per_core_batch_size=config.timesfm_per_core_batch_size,
    )

    share_predictions, share_stats = domain_timesfm.run_rolling_timesfm_baseline(
        df=monthly_share,
        spec=domain_timesfm.get_share_forecast_spec(),
        model=model,
        min_train_points=config.min_train_points,
        max_history=config.max_history,
        forecast_batch_size=config.forecast_batch_size,
        forecast_horizon=1,
        step_size=1,
        result_chunk_size=config.result_chunk_size,
    )
    share_predictions = domain_timesfm.clip_predictions(share_predictions, "timesfm_share_pred", low=0.0, high=1.0)
    share_predictions = domain_timesfm.reconcile_monthly_share(share_predictions)
    share_errors = domain_timesfm.build_error_dataframe(share_predictions, "real_share", "timesfm_share_pred", "share_error")

    gmv_predictions, gmv_stats = domain_timesfm.run_rolling_timesfm_baseline(
        df=daily_gmv,
        spec=domain_timesfm.get_gmv_forecast_spec(),
        model=model,
        min_train_points=config.min_train_points,
        max_history=config.max_history,
        forecast_batch_size=config.forecast_batch_size,
        forecast_horizon=1,
        step_size=1,
        result_chunk_size=config.result_chunk_size,
    )
    gmv_predictions = domain_timesfm.clip_predictions(gmv_predictions, "timesfm_gmv_pred", low=0.0, high=None)
    gmv_errors = domain_timesfm.build_error_dataframe(gmv_predictions, "real_gmv", "timesfm_gmv_pred", "gmv_error")

    infrastructure.ensure_directory(paths.artifacts_dir)
    for file_stem, dataframe in {
        "timesfm_share_predictions": share_predictions,
        "timesfm_share_errors": share_errors,
        "timesfm_gmv_predictions": gmv_predictions,
        "timesfm_gmv_errors": gmv_errors,
    }.items():
        infrastructure.write_parquet(dataframe, paths.artifacts_dir / f"{file_stem}.parquet")
        infrastructure.write_csv(dataframe, paths.artifacts_dir / f"{file_stem}.csv")

    return {
        "share_predictions": share_predictions,
        "share_errors": share_errors,
        "gmv_predictions": gmv_predictions,
        "gmv_errors": gmv_errors,
        "share_stats": share_stats,
        "gmv_stats": gmv_stats,
    }


def run_xgboost_pipelines(
    extracts: schemas.HistoricalExtracts,
    timesfm_outputs: dict[str, object],
    config: schemas.RuntimeConfig,
    paths: schemas.PipelinePaths,
) -> dict[str, object]:
    """
    Builds residual features, trains the XGBoost residual models, and persists all artifacts locally.

    Args:
        extracts (schemas.HistoricalExtracts): Historical sales, quotation, and stock extracts.
        timesfm_outputs (dict[str, object]): TimesFM baseline outputs used as residual targets.
        config (schemas.RuntimeConfig): Runtime configuration controlling feature engineering and model training.
        paths (schemas.PipelinePaths): Local paths used to persist the residual artifacts.

    Returns:
        dict[str, object]: Corrected output path, residual artifacts, and metrics dataframe.
    """

    daily_base, monthly_base = domain_xgboost.build_product_xgboost_base(
        sales_raw=extracts.sales,
        quotations_raw=extracts.quotations,
        stock_raw=extracts.stock,
        feature_window_months=config.xgboost_feature_window_months,
    )

    category_features = domain_xgboost.build_category_xgboost_features(
        daily_base=daily_base,
        gmv_errors=timesfm_outputs["gmv_errors"],
        category_history_months=config.xgboost_category_history_months,
    )
    share_features = domain_xgboost.build_share_xgboost_features(
        monthly_base=monthly_base,
        share_errors=timesfm_outputs["share_errors"],
        share_history_months=config.xgboost_share_history_months,
    )

    category_model = infrastructure.load_xgboost_regressor(_get_xgboost_params(config))
    share_model = infrastructure.load_xgboost_regressor(_get_xgboost_params(config))

    category_artifacts = domain_xgboost.train_residual_model(
        df=category_features,
        feature_cols=domain_xgboost.get_category_feature_columns(),
        target_col="gmv_error",
        time_col="date_ref",
        actual_col="real_gmv",
        baseline_col="timesfm_gmv_pred",
        model_name="category_gmv_xgboost",
        regressor=category_model,
        holdout_size=config.xgboost_category_holdout_days,
        prediction_col="xgboost_pred_error",
        corrected_prediction_col="corrected_gmv_pred",
        corrected_error_col="corrected_gmv_error",
        corrected_low=0.0,
        corrected_high=None,
    )
    share_artifacts = domain_xgboost.train_residual_model(
        df=share_features,
        feature_cols=domain_xgboost.get_share_feature_columns(),
        target_col="share_error",
        time_col="month_ref",
        actual_col="real_share",
        baseline_col="timesfm_share_pred",
        model_name="product_share_xgboost",
        regressor=share_model,
        holdout_size=config.xgboost_share_holdout_months,
        prediction_col="xgboost_pred_error",
        corrected_prediction_col="corrected_share_pred",
        corrected_error_col="corrected_share_error",
        corrected_low=0.0,
        corrected_high=1.0,
        reconcile_prediction_group_cols=["month_ref", "category_name", "sales_channel"],
    )
    metrics_df = domain_xgboost.metrics_to_frame([category_artifacts.metrics, share_artifacts.metrics])

    artifact_frames = {
        "xgboost_category_features": category_artifacts.features,
        "xgboost_category_predictions": category_artifacts.predictions,
        "xgboost_category_audit": category_artifacts.audit,
        "xgboost_share_features": share_artifacts.features,
        "xgboost_share_predictions": share_artifacts.predictions,
        "xgboost_share_audit": share_artifacts.audit,
        "xgboost_metrics": metrics_df,
        "corrected_gmv_output": category_artifacts.predictions,
    }
    for file_stem, dataframe in artifact_frames.items():
        infrastructure.write_parquet(dataframe, paths.artifacts_dir / f"{file_stem}.parquet")
        infrastructure.write_csv(dataframe, paths.artifacts_dir / f"{file_stem}.csv")

    corrected_share_path = infrastructure.save_portfolio_output(
        share_artifacts.predictions,
        paths,
        file_stem="corrected_share_output",
    )
    infrastructure.write_parquet(share_artifacts.predictions, paths.artifacts_dir / "corrected_share_output.parquet")
    infrastructure.write_pickle(category_model, paths.artifacts_dir / "xgboost_category_model.pkl")
    infrastructure.write_pickle(share_model, paths.artifacts_dir / "xgboost_share_model.pkl")

    return {
        "corrected_share_path": corrected_share_path,
        "category_artifacts": category_artifacts,
        "share_artifacts": share_artifacts,
        "metrics": metrics_df,
    }


def run_pipeline_locally() -> dict[str, object]:
    """
    Runs the full portfolio pipeline locally and persists all outputs.

    Returns:
        dict[str, object]: TimesFM outputs and XGBoost outputs produced by the local run.
    """

    paths = get_pipeline_paths()
    config = build_runtime_config()
    LOGGER.info("Extracting generic historical inputs.")
    extracts = extract_historical_inputs(paths, config)
    LOGGER.info("Running TimesFM baselines.")
    timesfm_outputs = run_timesfm_pipeline(extracts.sales, config, paths)
    LOGGER.info("Running XGBoost residual models.")
    xgboost_outputs = run_xgboost_pipelines(extracts, timesfm_outputs, config, paths)
    return {
        "timesfm_outputs": timesfm_outputs,
        "xgboost_outputs": xgboost_outputs,
    }


def main() -> None:
    """
    Local entrypoint for running the portfolio-safe pipeline.

    Returns:
        None: This function executes the full pipeline and logs the final row counts.
    """

    outputs = run_pipeline_locally()
    LOGGER.info(
        "Pipeline finished with %s share rows and %s GMV rows.",
        outputs["timesfm_outputs"]["share_predictions"].height,
        outputs["timesfm_outputs"]["gmv_predictions"].height,
    )


if __name__ == "__main__":
    main()
