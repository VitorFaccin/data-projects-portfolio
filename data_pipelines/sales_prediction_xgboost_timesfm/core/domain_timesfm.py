import numpy as np
import polars as pl

from core import schemas


def _flush_result_buffer(
    state: schemas.RollingForecastBuffers,
    spec: schemas.ForecastSpec,
    empty_schema: dict[str, pl.DataType],
) -> None:
    """
    Materializes buffered forecast rows into one Polars chunk.

    Args:
        state (schemas.RollingForecastBuffers): Mutable buffer state holding batched forecast results.
        spec (schemas.ForecastSpec): Forecast column specification for the current execution.
        empty_schema (dict[str, pl.DataType]): Output schema used to materialize the buffered rows.

    Returns:
        None: This function appends one materialized chunk to the result buffer when needed.
    """

    if not state.buffer_columns[spec.time_col]:
        return
    state.result_frames.append(
        pl.DataFrame(
            {
                **{column: state.buffer_columns[column] for column in spec.group_cols},
                spec.time_col: state.buffer_columns[spec.time_col],
                spec.value_col: state.buffer_columns[spec.value_col],
                spec.prediction_col: state.buffer_columns[spec.prediction_col],
            },
            schema=empty_schema,
        )
    )
    for column in state.buffer_columns:
        state.buffer_columns[column].clear()


def _flush_batch(
    state: schemas.RollingForecastBuffers,
    spec: schemas.ForecastSpec,
    model: schemas.TimesFmLike,
    empty_schema: dict[str, pl.DataType],
    forecast_horizon: int,
    result_chunk_size: int,
) -> int:
    """
    Runs one batched TimesFM call and appends the predictions to the result buffers.

    Args:
        state (schemas.RollingForecastBuffers): Mutable buffer state holding pending model inputs and outputs.
        spec (schemas.ForecastSpec): Forecast column specification for the current execution.
        model (schemas.TimesFmLike): TimesFM-compatible model used for inference.
        empty_schema (dict[str, pl.DataType]): Output schema used when buffered rows are materialized.
        forecast_horizon (int): Number of forecast steps requested from the model.
        result_chunk_size (int): Maximum number of buffered rows before a chunk is flushed.

    Returns:
        int: `1` when a model call was executed, otherwise `0`.
    """

    if not state.batch_inputs:
        return 0
    forecast_mean, _ = model.forecast(horizon=forecast_horizon, inputs=state.batch_inputs)
    pred_values = np.asarray(forecast_mean).reshape(-1, forecast_horizon)

    for index, pred_block in enumerate(pred_values):
        key_values, time_values, real_values = state.batch_meta[index]
        valid_horizon = min(len(time_values), len(real_values), pred_block.shape[0])
        for pred_idx in range(valid_horizon):
            for col_idx, col_name in enumerate(spec.group_cols):
                state.buffer_columns[col_name].append(key_values[col_idx])
            state.buffer_columns[spec.time_col].append(time_values[pred_idx])
            state.buffer_columns[spec.value_col].append(real_values[pred_idx])
            state.buffer_columns[spec.prediction_col].append(float(pred_block[pred_idx]))
            if len(state.buffer_columns[spec.time_col]) >= result_chunk_size:
                _flush_result_buffer(state, spec, empty_schema)

    state.batch_inputs.clear()
    state.batch_meta.clear()
    return 1


def normalize_sales_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizes the extracted sales dataframe into the generic contract used by the pipeline.

    Args:
        df (pl.DataFrame): Raw sales extract returned by the generic warehouse connector.

    Returns:
        pl.DataFrame: Typed sales history restricted to the columns required by the pipeline.
    """

    required = [
        "date_ref",
        "product_id",
        "category_group",
        "category_name",
        "sales_channel",
        "gmv",
    ]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return (
        df.select(required)
        .with_columns(
            pl.col("date_ref").cast(pl.Date, strict=False),
            pl.col("product_id").cast(pl.Int64, strict=False),
            pl.col("category_group").cast(pl.String, strict=False),
            pl.col("category_name").cast(pl.String, strict=False),
            pl.col("sales_channel").cast(pl.String, strict=False),
            pl.col("gmv").cast(pl.Float64, strict=False),
        )
        .drop_nulls(["date_ref", "product_id", "category_name", "sales_channel", "gmv"])
    )


def build_monthly_share_base(df: pl.DataFrame) -> pl.DataFrame:
    """
    Builds the monthly product-share table from the normalized sales history.

    Args:
        df (pl.DataFrame): Normalized sales history produced by `normalize_sales_dataframe`.

    Returns:
        pl.DataFrame: Monthly product share base with one row per product/category/channel/month.
    """

    monthly = (
        df.with_columns(month_ref=pl.col("date_ref").dt.truncate("1mo"))
        .group_by(["month_ref", "category_name", "sales_channel", "product_id"])
        .agg(product_gmv=pl.col("gmv").sum())
    )
    totals = (
        monthly.group_by(["month_ref", "category_name", "sales_channel"])
        .agg(category_gmv=pl.col("product_gmv").sum())
    )
    return (
        monthly.join(totals, on=["month_ref", "category_name", "sales_channel"], how="inner")
        .with_columns(
            real_share=pl.when(pl.col("category_gmv") > 0)
            .then(pl.col("product_gmv") / pl.col("category_gmv"))
            .otherwise(0.0)
        )
        .sort(["category_name", "sales_channel", "product_id", "month_ref"])
    )


def build_daily_gmv_base(df: pl.DataFrame) -> pl.DataFrame:
    """
    Builds the daily category GMV table from the normalized sales history.

    Args:
        df (pl.DataFrame): Normalized sales history produced by `normalize_sales_dataframe`.

    Returns:
        pl.DataFrame: Daily category GMV base with one row per category/channel/day.
    """

    return (
        df.group_by(["date_ref", "category_name", "sales_channel"])
        .agg(real_gmv=pl.col("gmv").sum())
        .sort(["category_name", "sales_channel", "date_ref"])
    )


def run_rolling_timesfm_baseline(
    df: pl.DataFrame,
    spec: schemas.ForecastSpec,
    model: schemas.TimesFmLike,
    min_train_points: int,
    max_history: int,
    forecast_batch_size: int,
    forecast_horizon: int = 1,
    step_size: int = 1,
    result_chunk_size: int = 50_000,
) -> tuple[pl.DataFrame, schemas.RollingForecastStats]:
    """
    Runs a batched rolling TimesFM baseline for each grouped time series.

    Args:
        df (pl.DataFrame): Historical time-series base used for the forecast run.
        spec (schemas.ForecastSpec): Grouping, time, value, and prediction columns used by the run.
        model (schemas.TimesFmLike): TimesFM-compatible model used for inference.
        min_train_points (int): Minimum number of observed points required before forecasting.
        max_history (int): Maximum history length passed to the model. `0` keeps full history.
        forecast_batch_size (int): Number of rolling histories sent per model call.
        forecast_horizon (int): Number of future points predicted per rolling origin.
        step_size (int): Stride between forecast origins.
        result_chunk_size (int): Maximum number of buffered rows before materialization.

    Returns:
        tuple: Predicted baseline dataframe and rolling forecast execution statistics.
    """

    if min_train_points < 1:
        raise ValueError("min_train_points must be >= 1")
    if forecast_batch_size < 1:
        raise ValueError("forecast_batch_size must be >= 1")
    if forecast_horizon < 1:
        raise ValueError("forecast_horizon must be >= 1")
    if step_size < 1:
        raise ValueError("step_size must be >= 1")
    if result_chunk_size < 1:
        raise ValueError("result_chunk_size must be >= 1")

    buffer_columns: dict[str, list[object]] = {column: [] for column in spec.group_cols}
    buffer_columns[spec.time_col] = []
    buffer_columns[spec.value_col] = []
    buffer_columns[spec.prediction_col] = []
    state = schemas.RollingForecastBuffers(
        batch_inputs=[],
        batch_meta=[],
        result_frames=[],
        buffer_columns=buffer_columns,
    )

    grouped = (
        df.group_by(list(spec.group_cols))
        .agg(
            pl.col(spec.time_col).sort_by(spec.time_col).alias("__times"),
            pl.col(spec.value_col).sort_by(spec.time_col).alias("__values"),
        )
        .sort(list(spec.group_cols))
    )
    group_columns = [grouped.get_column(column).to_list() for column in spec.group_cols]
    times_column = grouped.get_column("__times").to_list()
    values_column = grouped.get_column("__values").to_list()

    series_count = 0
    rolling_points = 0
    model_calls = 0

    empty_schema = {column: df.schema[column] for column in spec.group_cols}
    empty_schema[spec.time_col] = df.schema[spec.time_col]
    empty_schema[spec.value_col] = pl.Float64
    empty_schema[spec.prediction_col] = pl.Float64

    for row_idx in range(grouped.height):
        times = times_column[row_idx]
        values = np.asarray(values_column[row_idx], dtype=np.float32)
        series_count += 1
        if len(values) <= min_train_points:
            continue

        key_payload = tuple(group_columns[col_idx][row_idx] for col_idx in range(len(spec.group_cols)))
        for index in range(min_train_points, len(values), step_size):
            block_end = min(index + forecast_horizon, len(values))
            history = values[:index] if max_history <= 0 or index <= max_history else values[index - max_history:index]
            state.batch_inputs.append(history)
            state.batch_meta.append(
                (
                    key_payload,
                    list(times[index:block_end]),
                    values[index:block_end].astype(np.float64).tolist(),
                )
            )
            rolling_points += block_end - index
            if len(state.batch_inputs) >= forecast_batch_size:
                model_calls += _flush_batch(
                    state=state,
                    spec=spec,
                    model=model,
                    empty_schema=empty_schema,
                    forecast_horizon=forecast_horizon,
                    result_chunk_size=result_chunk_size,
                )

    model_calls += _flush_batch(
        state=state,
        spec=spec,
        model=model,
        empty_schema=empty_schema,
        forecast_horizon=forecast_horizon,
        result_chunk_size=result_chunk_size,
    )
    _flush_result_buffer(state, spec, empty_schema)

    stats = schemas.RollingForecastStats(
        series_count=series_count,
        rolling_points=rolling_points,
        model_calls=model_calls,
    )
    if not state.result_frames:
        return pl.DataFrame(schema=empty_schema), stats
    return pl.concat(state.result_frames, how="vertical_relaxed").sort([*spec.group_cols, spec.time_col]), stats


def clip_predictions(df: pl.DataFrame, prediction_col: str, low: float, high: float | None = None) -> pl.DataFrame:
    """
    Clips one prediction column to the configured bounds.

    Args:
        df (pl.DataFrame): DataFrame containing the prediction column.
        prediction_col (str): Prediction column that should be clipped.
        low (float): Lower bound for the prediction values.
        high (float | None): Optional upper bound for the prediction values.

    Returns:
        pl.DataFrame: DataFrame with the clipped prediction column.
    """

    expression = pl.col(prediction_col).clip(lower_bound=low)
    if high is not None:
        expression = expression.clip(upper_bound=high)
    return df.with_columns(expression.alias(prediction_col))


def reconcile_monthly_share(df: pl.DataFrame) -> pl.DataFrame:
    """
    Renormalizes monthly predicted shares so each category-channel-month sums to one.

    Args:
        df (pl.DataFrame): Monthly share prediction dataframe.

    Returns:
        pl.DataFrame: Monthly share predictions normalized within each category/channel/month group.
    """

    group_cols = ["month_ref", "category_name", "sales_channel"]
    return (
        df.with_columns(group_prediction_sum=pl.col("timesfm_share_pred").sum().over(group_cols))
        .with_columns(
            pl.when(pl.col("group_prediction_sum") > 0)
            .then(pl.col("timesfm_share_pred") / pl.col("group_prediction_sum"))
            .otherwise(0.0)
            .alias("timesfm_share_pred")
        )
        .drop("group_prediction_sum")
    )


def build_error_dataframe(df: pl.DataFrame, real_col: str, pred_col: str, error_col: str) -> pl.DataFrame:
    """
    Builds a residual dataframe from one real-prediction pair.

    Args:
        df (pl.DataFrame): DataFrame containing the real and predicted columns.
        real_col (str): Column with the observed value.
        pred_col (str): Column with the predicted value.
        error_col (str): Name of the new residual column.

    Returns:
        pl.DataFrame: DataFrame with the residual column added.
    """

    return df.with_columns((pl.col(real_col) - pl.col(pred_col)).alias(error_col))


def get_share_forecast_spec() -> schemas.ForecastSpec:
    """
    Returns the grouped forecast specification for monthly share.

    Returns:
        schemas.ForecastSpec: Grouping and column contract used by the monthly share baseline.
    """

    return schemas.ForecastSpec(
        group_cols=("category_name", "sales_channel", "product_id"),
        time_col="month_ref",
        value_col="real_share",
        prediction_col="timesfm_share_pred",
    )


def get_gmv_forecast_spec() -> schemas.ForecastSpec:
    """
    Returns the grouped forecast specification for daily GMV.

    Returns:
        schemas.ForecastSpec: Grouping and column contract used by the daily GMV baseline.
    """

    return schemas.ForecastSpec(
        group_cols=("category_name", "sales_channel"),
        time_col="date_ref",
        value_col="real_gmv",
        prediction_col="timesfm_gmv_pred",
    )
