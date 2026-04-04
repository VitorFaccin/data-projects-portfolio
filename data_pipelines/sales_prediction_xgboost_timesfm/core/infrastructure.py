import json
import pickle
from pathlib import Path
from typing import Any

import polars as pl

from core.schemas import HistoricalExtracts, PipelinePaths, RuntimeConfig


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


def build_product_filter(product_ids: tuple[int, ...], alias: str) -> str:
    """
    Builds an optional SQL predicate for a restricted product universe.

    Args:
        product_ids (tuple[int, ...]): Product identifiers used to restrict the extraction scope.
        alias (str): Table alias used in the SQL file for the product column reference.

    Returns:
        str: Optional SQL `AND ... IN (...)` clause or an empty string when no filter is needed.
    """

    if not product_ids:
        return ""
    values = ", ".join(str(int(product_id)) for product_id in product_ids)
    return f"AND {alias}.product_id IN ({values})"


def generic_data_warehouse_connection(query: str) -> pl.DataFrame:
    """
    Placeholder generic warehouse connector for portfolio-safe extraction examples.

    Args:
        query (str): Rendered SQL query that should be executed by the runtime environment.

    Returns:
        pl.DataFrame: Query result returned by the external warehouse connector.
    """

    raise NotImplementedError(
        "generic_data_warehouse_connection must be implemented by the runtime environment."
    )


def execute_sql_template(sql_file_path: Path, history_months: int, product_filter: str) -> pl.DataFrame:
    """
    Renders and executes one SQL template through the generic connector.

    Args:
        sql_file_path (Path): SQL template file path.
        history_months (int): Number of historical months injected into the SQL template.
        product_filter (str): Optional rendered product predicate injected into the SQL template.

    Returns:
        pl.DataFrame: Extracted dataset returned by the generic connector.
    """

    query = load_sql(sql_file_path).format(
        history_months=history_months,
        product_filter=product_filter,
    )
    return generic_data_warehouse_connection(query)


def extract_historical_inputs(paths: PipelinePaths, config: RuntimeConfig) -> HistoricalExtracts:
    """
    Extracts sales, quotation, and stock history directly from generic SQL sources.

    Args:
        paths (PipelinePaths): Local pipeline paths containing the SQL directory.
        config (RuntimeConfig): Runtime parameters controlling history depth and product scope.

    Returns:
        HistoricalExtracts: Portfolio-safe sales, quotation, and stock history frames.
    """

    sales = execute_sql_template(
        paths.sql_path / "extract_sales_history.sql",
        history_months=config.history_months,
        product_filter=build_product_filter(config.product_ids, alias="s"),
    )
    quotations = execute_sql_template(
        paths.sql_path / "extract_quotations_history.sql",
        history_months=config.history_months,
        product_filter=build_product_filter(config.product_ids, alias="q"),
    )
    stock = execute_sql_template(
        paths.sql_path / "extract_stock_history.sql",
        history_months=config.history_months,
        product_filter=build_product_filter(config.product_ids, alias="st"),
    )
    return HistoricalExtracts(sales=sales, quotations=quotations, stock=stock)


def write_parquet(df: pl.DataFrame, path: Path) -> None:
    """
    Writes a Polars DataFrame to parquet.

    Args:
        df (pl.DataFrame): DataFrame that should be persisted locally.
        path (Path): Destination parquet path.

    Returns:
        None: This function writes the parquet artifact to disk.
    """

    ensure_directory(path.parent)
    df.write_parquet(path)


def write_csv(df: pl.DataFrame, path: Path) -> None:
    """
    Writes a Polars DataFrame to CSV.

    Args:
        df (pl.DataFrame): DataFrame that should be persisted locally.
        path (Path): Destination CSV path.

    Returns:
        None: This function writes the CSV artifact to disk.
    """

    ensure_directory(path.parent)
    df.write_csv(path)


def write_pickle(obj: Any, path: Path) -> None:
    """
    Serializes a Python object to disk.

    Args:
        obj (Any): Python object to serialize.
        path (Path): Destination pickle path.

    Returns:
        None: This function writes the serialized object to disk.
    """

    ensure_directory(path.parent)
    with path.open("wb") as file_pointer:
        pickle.dump(obj, file_pointer)


def _apply_output_schema(df: pl.DataFrame, setup_json_path: Path) -> pl.DataFrame:
    """
    Selects and casts the final output columns according to the setup contract.

    Args:
        df (pl.DataFrame): DataFrame that should be aligned to the public output contract.
        setup_json_path (Path): Setup JSON used as the source of truth for output columns and types.

    Returns:
        pl.DataFrame: Contract-aligned DataFrame ready for local persistence.
    """

    with setup_json_path.open("r", encoding="utf-8") as file_pointer:
        setup = json.load(file_pointer)

    type_map = {
        "int": pl.Int64,
        "string": pl.String,
        "float": pl.Float64,
        "date": pl.Date,
    }
    output_columns = list(setup["column_types"].keys())
    selected = df.select([column for column in output_columns if column in df.columns])
    casts = [
        pl.col(column).cast(type_map[config["column_type"]], strict=False).alias(column)
        for column, config in setup["column_types"].items()
        if column in selected.columns and config["column_type"] in type_map
    ]
    if not casts:
        return selected
    return selected.with_columns(casts)


def save_portfolio_output(df: pl.DataFrame, paths: PipelinePaths, file_stem: str) -> str:
    """
    Saves one portfolio output as CSV and copies the public schema contract beside it.

    Args:
        df (pl.DataFrame): Final output DataFrame that should be exposed in the portfolio.
        paths (PipelinePaths): Local paths containing the artifacts directory and setup JSON.
        file_stem (str): Base filename used for the generated CSV and schema files.

    Returns:
        str: String path to the saved CSV output.
    """

    ensure_directory(paths.artifacts_dir)
    final_df = _apply_output_schema(df, paths.setup_json_path)
    csv_path = paths.artifacts_dir / f"{file_stem}.csv"
    final_df.write_csv(csv_path)
    schema_path = paths.artifacts_dir / f"{file_stem}.schema.json"
    schema_path.write_text(paths.setup_json_path.read_text(encoding="utf-8"), encoding="utf-8")
    return str(csv_path)


def load_timesfm_model(
    model_id: str,
    max_context: int,
    max_horizon: int,
    per_core_batch_size: int,
) -> Any:
    """
    Loads a TimesFM model lazily.

    Args:
        model_id (str): Hugging Face model identifier used to download the TimesFM weights.
        max_context (int): Maximum context length configured for the model.
        max_horizon (int): Maximum forecast horizon configured for the model.
        per_core_batch_size (int): Per-core inference batch size.

    Returns:
        Any: Compiled TimesFM model ready for inference.
    """

    import os

    from huggingface_hub import snapshot_download
    from safetensors.torch import load_file
    import timesfm
    from timesfm.configs import ForecastConfig

    model = timesfm.TimesFM_2p5_200M_torch(
        torch_compile=False,
        config={
            "context_len": max_context,
            "horizon_len": max_horizon,
            "per_core_batch_size": per_core_batch_size,
        },
    )
    model_dir = snapshot_download(repo_id=model_id)
    weights = load_file(os.path.join(model_dir, "model.safetensors"))
    if hasattr(model, "model"):
        model.model.load_state_dict(weights, strict=False)
    elif hasattr(model, "_model"):
        model._model.load_state_dict(weights, strict=False)

    try:
        forecast_config = ForecastConfig(
            max_context=max_context,
            max_horizon=max_horizon,
            per_core_batch_size=per_core_batch_size,
        )
    except TypeError:
        forecast_config = ForecastConfig(
            context_len=max_context,
            horizon_len=max_horizon,
            per_core_batch_size=per_core_batch_size,
        )
    model.compile(forecast_config=forecast_config)
    return model


def load_xgboost_regressor(params: dict[str, Any]) -> Any:
    """
    Loads an XGBoost regressor lazily.

    Args:
        params (dict[str, Any]): Parameter dictionary passed to `XGBRegressor`.

    Returns:
        Any: Configured XGBoost regressor instance.
    """

    try:
        from xgboost import XGBRegressor
    except ImportError as exc:
        raise ImportError("XGBoost is required to run the residual pipeline.") from exc
    return XGBRegressor(**params)
