import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from core.domain import (
    adjust_units_for_seasonality,
    build_price_tiers,
    calculate_seasonal_index,
    filter_low_price_variation,
    flag_spike_days,
    impute_elasticities,
    run_log_log_regression,
)
from core.infrastructure import (
    ensure_artifacts_dir,
    extract_master_data,
    get_analysis_window,
    load_dataframe,
    save_dataframe,
    save_results,
)
from core.schemas import PipelinePaths

AIRFLOW_AVAILABLE = False

try:
    from airflow.decorators import dag, task
except ImportError:
    AIRFLOW_AVAILABLE = False

    def dag(**_kwargs):
        """
        Local fallback for environments without Airflow installed.
        """

        def decorator(function):
            return function

        return decorator

    def task(**_kwargs):
        """
        Local fallback for environments without Airflow installed.
        """

        def decorator(function):
            return function

        return decorator


LOGGER = logging.getLogger("elasticity_regression_loglog")
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PIPELINE_ROOT = Path(__file__).resolve().parent
FREE_TABLES_ROOT = PIPELINE_ROOT / "olist_free_tables"
ARTIFACTS_DIR = PIPELINE_ROOT / "artifacts"
SQL_PATH = PIPELINE_ROOT / "sql"
JSON_SETUP_PATH = PIPELINE_ROOT / "json" / "setup_elasticity_regression_loglog.json"

DEFAULT_PARAMETERS = {
    "min_observations": 30,
    "max_p_value": 0.25,
    "min_r_squared": 0.05,
    "price_variation_threshold": 0.02,
    "months_back": 24,
}


def get_pipeline_paths() -> PipelinePaths:
    """
    Resolves the local paths used by the portfolio DAG.

    Returns:
        PipelinePaths: Structured paths used by the pipeline.
    """
    final_paths = PipelinePaths(
        pipeline_root=PIPELINE_ROOT,
        artifacts_dir=ARTIFACTS_DIR,
        sql_path=SQL_PATH,
        setup_json_path=JSON_SETUP_PATH,
        orders_csv_path=FREE_TABLES_ROOT / "olist_orders_dataset.csv",
        order_items_csv_path=FREE_TABLES_ROOT / "olist_order_items_dataset.csv",
        products_csv_path=FREE_TABLES_ROOT / "olist_products_dataset.csv",
        translation_csv_path=FREE_TABLES_ROOT / "product_category_name_translation.csv",
        customers_csv_path=FREE_TABLES_ROOT / "olist_customers_dataset.csv",
    )
    return final_paths


def get_runtime_parameters() -> dict:
    """
    Loads runtime parameters from ELASTICITY_REGRESSION_LOGLOG_PARAMS when available.

    Returns:
        dict: Runtime parameters merged with defaults.
    """
    env_value = os.getenv("ELASTICITY_REGRESSION_LOGLOG_PARAMS", "").strip()
    if not env_value:
        final_params = DEFAULT_PARAMETERS.copy()
        return final_params

    loaded_params = json.loads(env_value)
    final_params = {**DEFAULT_PARAMETERS, **loaded_params}
    return final_params


def extract_master_dataset(paths: PipelinePaths, parameters: dict) -> pd.DataFrame:
    """
    Executes the DuckDB SQL extraction and validates the resulting master dataset.

    Args:
        paths (PipelinePaths): Collection of pipeline file paths.
        parameters (dict): Runtime parameters for windowing and filtering.

    Returns:
        pd.DataFrame: Master dataset ready for the domain pipeline.
    """
    window = get_analysis_window(paths.orders_csv_path, months_back=int(parameters["months_back"]))
    master_df = extract_master_data(paths, window)

    if master_df.empty:
        raise RuntimeError("DuckDB extraction returned no rows for the selected Olist analysis window.")

    master_df["data_date"] = pd.to_datetime(master_df["data_date"])
    final_df = filter_low_price_variation(
        master_df,
        threshold=float(parameters["price_variation_threshold"]),
    )

    if final_df.empty:
        raise RuntimeError("No product-location pairs passed the minimum price variation threshold.")

    return final_df


def calculate_elasticity_frame(master_df: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """
    Runs the business pipeline from seasonality adjustment through fallback imputation.

    Args:
        master_df (pd.DataFrame): Extracted master dataset.
        parameters (dict): Runtime parameters for the regression criteria.

    Returns:
        pd.DataFrame: Final elasticity output DataFrame.
    """
    staged_df = flag_spike_days(master_df)
    seasonal_index = calculate_seasonal_index(staged_df)
    staged_df = adjust_units_for_seasonality(staged_df, seasonal_index)
    staged_df = build_price_tiers(staged_df)

    grouped_frames = [
        group_df.reset_index(drop=True)
        for _, group_df in staged_df.groupby(["product_id", "sale_location"], sort=False)
    ]
    results = [
        run_log_log_regression(
            product_data=group_df,
            min_observations=int(parameters["min_observations"]),
            max_p_value=float(parameters["max_p_value"]),
            min_r_squared=float(parameters["min_r_squared"]),
        )
        for group_df in grouped_frames
    ]
    final_df = impute_elasticities(results)
    final_df["data_calculo"] = datetime.now().date().isoformat()
    return final_df


def run_pipeline_locally(parameters: dict | None = None) -> pd.DataFrame:
    """
    Runs the full pipeline outside Airflow and saves the final output locally.

    Args:
        parameters (dict | None): Optional runtime parameter override.

    Returns:
        pd.DataFrame: Final elasticity output DataFrame.
    """
    runtime_parameters = parameters or get_runtime_parameters()
    paths = get_pipeline_paths()
    ensure_artifacts_dir(paths.artifacts_dir)

    LOGGER.info("Extracting portfolio master data from Olist free tables via DuckDB SQL.")
    master_df = extract_master_dataset(paths, runtime_parameters)
    save_dataframe(master_df, paths.artifacts_dir / "master_dataset.pkl")

    LOGGER.info("Calculating elasticity outputs with pandas domain logic.")
    final_df = calculate_elasticity_frame(master_df, runtime_parameters)
    save_dataframe(final_df, paths.artifacts_dir / "final_dataset.pkl")
    save_results(final_df, paths)
    return final_df


@dag(
    dag_id="elasticity_regression_loglog",
    schedule="30 3 1 * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pricing", "portfolio", "olist", "elasticity"],
    doc_md="""
    Real TaskFlow DAG that recreates the `elasticity` business flow with free Olist CSVs.
    The extraction layer mimics warehouse SQL using DuckDB over local files, while the domain
    layer keeps the regression, seasonality, price-tier, and confidence fallback logic in pandas.
    """,
)
def elasticity_regression_loglog():
    """
    Defines the TaskFlow DAG for the portfolio version of the elasticity pipeline.

    Returns:
        None: Airflow uses the decorated function to build the DAG.
    """

    @task(task_id="extract_master_data")
    def extract_master_data_task() -> str:
        """
        Extracts the master dataset and stages it locally for downstream tasks.

        Returns:
            str: Saved path to the staged master dataset.
        """
        paths = get_pipeline_paths()
        parameters = get_runtime_parameters()
        final_df = extract_master_dataset(paths, parameters)
        final_path = save_dataframe(final_df, paths.artifacts_dir / "master_dataset.pkl")
        return final_path

    @task(task_id="calculate_elasticities")
    def calculate_elasticities_task(master_dataset_path: str) -> str:
        """
        Applies the regression logic and stages the final elasticity frame locally.

        Args:
            master_dataset_path (str): Path to the staged master dataset.

        Returns:
            str: Saved path to the staged final dataset.
        """
        parameters = get_runtime_parameters()
        paths = get_pipeline_paths()
        master_df = load_dataframe(master_dataset_path)
        final_df = calculate_elasticity_frame(master_df, parameters)
        final_path = save_dataframe(final_df, paths.artifacts_dir / "final_dataset.pkl")
        return final_path

    @task(task_id="load_results")
    def load_results_task(final_dataset_path: str) -> str:
        """
        Persists the final output locally, mimicking a serving-table load.

        Args:
            final_dataset_path (str): Path to the staged final dataset.

        Returns:
            str: Saved path to the final CSV output.
        """
        paths = get_pipeline_paths()
        final_df = load_dataframe(final_dataset_path)
        final_path = save_results(final_df, paths)
        return final_path

    master_dataset_path = extract_master_data_task()
    final_dataset_path = calculate_elasticities_task(master_dataset_path)
    load_results_task(final_dataset_path)

dag_instance = elasticity_regression_loglog() if AIRFLOW_AVAILABLE else None


def main() -> None:
    """
    Local entrypoint for running the pipeline without an Airflow installation.

    Returns:
        None: This function executes the pipeline and logs the result.
    """
    final_df = run_pipeline_locally()
    LOGGER.info("Local execution finished with %s rows.", len(final_df))


if __name__ == "__main__":
    main()
