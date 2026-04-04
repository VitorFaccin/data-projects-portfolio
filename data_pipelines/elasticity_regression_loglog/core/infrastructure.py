import json
from pathlib import Path

import duckdb
import pandas as pd

from core.schemas import AnalysisWindow, PipelinePaths


def ensure_artifacts_dir(artifacts_dir: Path) -> None:
    """
    Creates the local artifacts directory used to mimic staging and serving layers.

    Args:
        artifacts_dir (Path): Directory where local artifacts are stored.

    Returns:
        None: This function creates the directory when needed.
    """
    artifacts_dir.mkdir(parents=True, exist_ok=True)


def get_analysis_window(orders_csv_path: Path, months_back: int = 24) -> AnalysisWindow:
    """
    Builds a rolling analysis window based on the latest date available in the free dataset.

    Args:
        orders_csv_path (Path): Path to the Olist orders CSV.
        months_back (int): Number of months to include in the analysis window. Defaults to 24.

    Returns:
        AnalysisWindow: Start and end dates for the extraction window.
    """
    orders_df = pd.read_csv(orders_csv_path, usecols=["order_purchase_timestamp"])
    orders_df["order_purchase_timestamp"] = pd.to_datetime(
        orders_df["order_purchase_timestamp"], errors="coerce"
    )
    latest_date = orders_df["order_purchase_timestamp"].dropna().max()
    start_date = latest_date - pd.DateOffset(months=months_back)
    final_window = AnalysisWindow(
        start_date=start_date.date().isoformat(),
        end_date=latest_date.date().isoformat(),
    )
    return final_window


def load_sql(sql_path: Path) -> str:
    """
    Reads a SQL file used to mimic warehouse extraction over local CSV files.

    Args:
        sql_path (Path): Path to the SQL file.

    Returns:
        str: SQL text loaded from disk.
    """
    sql_text = sql_path.read_text(encoding="utf-8")
    return sql_text


def extract_master_data(paths: PipelinePaths, window: AnalysisWindow) -> pd.DataFrame:
    """
    Executes the initial extraction query with DuckDB against the free Olist CSVs.

    Args:
        paths (PipelinePaths): Collection of pipeline file paths.
        window (AnalysisWindow): Date window used in the extraction query.

    Returns:
        pd.DataFrame: Daily master dataset with sales, price, and delivery proxy.
    """
    query = load_sql(paths.sql_path / "extract_daily_sales_pricing.sql").format(
        order_items_csv_path=paths.order_items_csv_path.as_posix(),
        orders_csv_path=paths.orders_csv_path.as_posix(),
        products_csv_path=paths.products_csv_path.as_posix(),
        translation_csv_path=paths.translation_csv_path.as_posix(),
        customers_csv_path=paths.customers_csv_path.as_posix(),
        start_date=window.start_date,
        end_date=window.end_date,
    )
    connection = duckdb.connect(database=":memory:")
    try:
        master_df = connection.execute(query).df()
    finally:
        connection.close()
    return master_df


def save_dataframe(df: pd.DataFrame, output_path: Path) -> str:
    """
    Saves an intermediate DataFrame to pickle for task-to-task exchange.

    Args:
        df (pd.DataFrame): DataFrame to persist.
        output_path (Path): Output pickle path.

    Returns:
        str: String path to the saved file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(output_path)
    final_path = str(output_path)
    return final_path


def load_dataframe(input_path: str) -> pd.DataFrame:
    """
    Loads an intermediate DataFrame from pickle.

    Args:
        input_path (str): Pickle path to load.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    final_df = pd.read_pickle(input_path)
    return final_df


def _apply_column_types(df: pd.DataFrame, setup_file_path: Path) -> pd.DataFrame:
    """
    Casts and orders the output columns according to the setup JSON contract.

    Args:
        df (pd.DataFrame): Final DataFrame before persistence.
        setup_file_path (Path): Path to the setup JSON file.

    Returns:
        pd.DataFrame: DataFrame with ordered and cast columns.
    """
    with setup_file_path.open("r", encoding="utf-8") as file_pointer:
        setup = json.load(file_pointer)

    type_map = {
        "int": "Int64",
        "string": "string",
        "float": "float64",
    }
    output_columns = list(setup["column_types"].keys())
    final_df = df[[column for column in output_columns if column in df.columns]].copy()
    casts = {
        column: type_map[config["column_type"]]
        for column, config in setup["column_types"].items()
        if column in final_df.columns and config["column_type"] in type_map
    }
    final_df = final_df.astype(casts)
    return final_df


def save_results(df: pd.DataFrame, paths: PipelinePaths) -> str:
    """
    Persists the final result locally, mimicking a serving-layer load for a portfolio project.

    Args:
        df (pd.DataFrame): Final elasticity DataFrame.
        paths (PipelinePaths): Collection of pipeline file paths.

    Returns:
        str: String path to the saved CSV file.
    """
    ensure_artifacts_dir(paths.artifacts_dir)
    final_df = _apply_column_types(df, paths.setup_json_path)
    csv_path = paths.artifacts_dir / "pricing_elasticity_regression_loglog.csv"
    final_df.to_csv(csv_path, index=False)
    metadata_path = paths.artifacts_dir / "pricing_elasticity_regression_loglog.schema.json"
    metadata_path.write_text(paths.setup_json_path.read_text(encoding="utf-8"), encoding="utf-8")
    final_path = str(csv_path)
    return final_path
