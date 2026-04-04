import json
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

import pandas as pd

from core.schemas import ExtractedFrames, PipelinePaths, RuntimeConfig


def ensure_directory(path: Path) -> None:
    """
    Creates a directory tree when it does not already exist.

    Args:
        path (Path): Directory path that should exist before local writes.

    Returns:
        None: This function only guarantees that the directory exists.
    """

    path.mkdir(parents=True, exist_ok=True)


def load_sql(sql_path: Path) -> str:
    """
    Loads one SQL template from disk.

    Args:
        sql_path (Path): Path to the SQL file stored under the pipeline `sql/` folder.

    Returns:
        str: Raw SQL template text loaded from disk.
    """

    return sql_path.read_text(encoding="utf-8")


def generic_data_warehouse_connection(query: str) -> pd.DataFrame:
    """
    Placeholder generic warehouse connector for portfolio-safe extraction examples.

    Args:
        query (str): Rendered SQL query that should be executed by the runtime environment.

    Returns:
        pd.DataFrame: Query result returned by the external warehouse connector.
    """

    raise NotImplementedError("generic_data_warehouse_connection must be implemented by the runtime environment.")


def get_extraction_condition(config: RuntimeConfig, execution_dt: datetime | None = None) -> str:
    """
    Builds the SQL extraction condition based on the runtime execution mode.

    Args:
        config (RuntimeConfig): Runtime configuration containing manual or incremental extraction parameters.
        execution_dt (datetime | None): Optional execution timestamp used for deterministic testing.

    Returns:
        str: SQL extraction condition string ready to be injected into the SQL templates.
    """

    reference_dt = execution_dt or datetime.utcnow()
    if config.is_incremental:
        start_date = (reference_dt - timedelta(days=config.days_back)).strftime("%Y-%m-%d")
        return f">= '{start_date}'"
    return f"BETWEEN '{config.start_date}' AND '{config.end_date}'"


def execute_sql_template(
    sql_file_path: Path,
    config: RuntimeConfig,
    use_condition: bool = False,
    retry: int = 0,
) -> pd.DataFrame:
    """
    Renders and executes one SQL template through the generic connector with retry logic.

    Args:
        sql_file_path (Path): SQL template file path.
        config (RuntimeConfig): Runtime configuration with retry and extraction settings.
        use_condition (bool): Whether the SQL file expects a `{date_condition}` placeholder.
        retry (int): Current retry attempt number.

    Returns:
        pd.DataFrame: Extracted dataset returned by the generic connector.
    """

    try:
        sql = load_sql(sql_file_path)
        if use_condition:
            sql = sql.format(date_condition=get_extraction_condition(config))
        return generic_data_warehouse_connection(sql)
    except Exception:
        if retry < config.max_retry:
            sleep(config.retry_sleep_seconds)
            return execute_sql_template(sql_file_path, config, use_condition=use_condition, retry=retry + 1)
        raise


def extract_source_frames(paths: PipelinePaths, config: RuntimeConfig) -> ExtractedFrames:
    """
    Extracts all source dataframes required by the tickets conversion pipeline.

    Args:
        paths (PipelinePaths): Local pipeline paths containing the SQL directory.
        config (RuntimeConfig): Runtime configuration controlling the extraction scope and retries.

    Returns:
        ExtractedFrames: Interactions, customers, agents, and orders dataframes.
    """

    return ExtractedFrames(
        interactions_df=execute_sql_template(paths.sql_path / "extract_interactions.sql", config, use_condition=True),
        customers_df=execute_sql_template(paths.sql_path / "extract_customers.sql", config),
        agents_df=execute_sql_template(paths.sql_path / "extract_agent_roster.sql", config),
        orders_df=execute_sql_template(paths.sql_path / "extract_orders.sql", config, use_condition=True),
    )


def add_processed_at(df: pd.DataFrame, execution_dt: datetime | None = None) -> pd.DataFrame:
    """
    Adds the processing timestamp column to the final dataframe.

    Args:
        df (pd.DataFrame): Final dataframe before persistence.
        execution_dt (datetime | None): Optional deterministic execution timestamp used for tests.

    Returns:
        pd.DataFrame: Dataframe with the `processed_at` column added.
    """

    final_df = df.copy()
    processed_at = (execution_dt or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")
    final_df["processed_at"] = processed_at
    return final_df


def apply_output_schema(df: pd.DataFrame, setup_json_path: Path) -> pd.DataFrame:
    """
    Selects and casts the final output columns according to the setup contract.

    Args:
        df (pd.DataFrame): Dataframe that should be aligned to the public output contract.
        setup_json_path (Path): Setup JSON used as the source of truth for output columns and types.

    Returns:
        pd.DataFrame: Contract-aligned dataframe ready for local persistence.
    """

    setup = json.loads(setup_json_path.read_text(encoding="utf-8"))
    output_columns = list(setup["column_types"].keys())
    final_df = df[[column for column in output_columns if column in df.columns]].copy()

    for column, info in setup["column_types"].items():
        if column not in final_df.columns:
            continue
        if info["column_type"] == "int":
            final_df[column] = final_df[column].astype("Int64")
        elif info["column_type"] == "float":
            final_df[column] = final_df[column].astype(float)
        else:
            final_df[column] = final_df[column].astype(str)
    return final_df


def save_output(df: pd.DataFrame, paths: PipelinePaths) -> str:
    """
    Saves the final portfolio output locally as CSV and copies the schema beside it.

    Args:
        df (pd.DataFrame): Final dataframe that should be exposed in the portfolio.
        paths (PipelinePaths): Local paths containing the artifacts directory and setup JSON.

    Returns:
        str: String path to the saved CSV output.
    """

    ensure_directory(paths.artifacts_dir)
    final_df = apply_output_schema(df, paths.setup_json_path)
    csv_path = paths.artifacts_dir / "tickets_conversion.csv"
    final_df.to_csv(csv_path, index=False)
    schema_path = paths.artifacts_dir / "tickets_conversion.schema.json"
    schema_path.write_text(paths.setup_json_path.read_text(encoding="utf-8"), encoding="utf-8")
    return str(csv_path)
