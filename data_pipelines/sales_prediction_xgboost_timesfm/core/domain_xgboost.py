from __future__ import annotations

from dataclasses import asdict
import math

import pandas as pd
import polars as pl

from core import domain_timesfm, schemas

KPI_COLUMN_MAP = {
    "price": "price",
    "freight": "freight",
    "leadtime": "leadtime",
}
CHANNEL_FALLBACK_MAP = {
    "digital_app": "digital",
}


def _safe_z_score_expr(source_col: str, mean_col: str, std_col: str, output_col: str) -> pl.Expr:
    """
    Builds a z-score expression that collapses invalid results to zero.

    Args:
        source_col (str): Source KPI column used in the z-score calculation.
        mean_col (str): Rolling mean column associated with the KPI.
        std_col (str): Rolling standard deviation column associated with the KPI.
        output_col (str): Output z-score column name.

    Returns:
        pl.Expr: Safe Polars expression that returns a finite z-score.
    """

    std_expr = pl.col(std_col)
    safe_division = (pl.col(source_col) - pl.col(mean_col).fill_null(pl.col(source_col))) / std_expr
    return (
        pl.when(std_expr.is_null() | (std_expr == 0))
        .then(0.0)
        .otherwise(safe_division)
        .fill_nan(0.0)
        .fill_null(0.0)
        .replace([math.inf, -math.inf], [0.0, 0.0])
        .alias(output_col)
    )


def _channel_fallback_expr(source_col: str = "sales_channel") -> pl.Expr:
    """
    Builds the channel fallback expression used when direct quotation matches fail.

    Args:
        source_col (str): Channel column that should be mapped to a generic fallback channel.

    Returns:
        pl.Expr: Polars expression that applies the configured channel fallback mapping.
    """

    expression = pl.col(source_col)
    for raw_channel, fallback_channel in CHANNEL_FALLBACK_MAP.items():
        expression = pl.when(pl.col(source_col) == raw_channel).then(pl.lit(fallback_channel)).otherwise(expression)
    return expression


def _print_unmatched_debug_stats(
    df: pl.DataFrame,
    match_stage_col: str,
    debug_label: str,
    weighted_col: str | None = None,
) -> None:
    """
    Prints compact diagnostics about unmatched KPI joins.

    Args:
        df (pl.DataFrame): Joined dataframe containing match-stage diagnostics.
        match_stage_col (str): Column that stores the match-stage labels.
        debug_label (str): Prefix used in the printed diagnostic message.
        weighted_col (str | None): Optional weighted business column used to quantify unmatched coverage.

    Returns:
        None: This function only prints compact debug diagnostics.
    """

    total_rows = df.height
    if total_rows == 0:
        print(f"{debug_label}: rows=0")
        return
    unmatched = df.filter(pl.col(match_stage_col) == "unmatched")
    total_products = df.select("product_id").unique().height if "product_id" in df.columns else 0
    unmatched_products = unmatched.select("product_id").unique().height if "product_id" in unmatched.columns else 0
    message = (
        f"{debug_label}: unmatched_rows={unmatched.height}, "
        f"unmatched_products={unmatched_products}/{total_products}"
    )
    if weighted_col and weighted_col in df.columns:
        total_weight = df.get_column(weighted_col).fill_null(0.0).sum()
        unmatched_weight = unmatched.get_column(weighted_col).fill_null(0.0).sum() if unmatched.height else 0.0
        message = f"{message}, unmatched_weight={unmatched_weight:.4f}/{total_weight:.4f}"
    print(message)


def join_adjusted_features(
    left_df: pl.DataFrame,
    quotations: pl.DataFrame,
    time_col: str,
    debug_label: str,
    weighted_col: str | None = None,
    drop_unmatched: bool = True,
) -> pl.DataFrame:
    """
    Attaches quotation KPIs using direct, channel-fallback, and product-only fallback joins.

    Args:
        left_df (pl.DataFrame): Target dataframe that needs quotation KPI columns.
        quotations (pl.DataFrame): Normalized quotation history used as the KPI source.
        time_col (str): Time column that anchors the as-of join.
        debug_label (str): Label used in debug diagnostics.
        weighted_col (str | None): Optional business weight column used in debug diagnostics.
        drop_unmatched (bool): Whether rows without any quotation match should be removed.

    Returns:
        pl.DataFrame: Target dataframe enriched with price, freight, and leadtime features.
    """

    if left_df.is_empty():
        return left_df
    if time_col not in left_df.columns:
        raise ValueError(f"Missing time column '{time_col}' in target dataframe.")

    prepared = left_df.with_row_index("__join_row_id").with_columns(
        pl.col(time_col).cast(pl.Date, strict=False),
        (
            (pl.col(time_col).cast(pl.Date, strict=False).dt.offset_by("1mo")) - pl.duration(days=1)
            if time_col == "month_ref"
            else pl.col(time_col).cast(pl.Date, strict=False)
        ).alias("__cutoff_date"),
    )
    quotations_prepared = quotations.select(
        ["date_ref", "category_name", "sales_channel", "product_id", "price", "freight", "leadtime"]
    ).with_columns(pl.col("date_ref").cast(pl.Date, strict=False).alias("quote_date_ref"))
    result_cols = [*prepared.columns, "quote_date_ref", "price", "freight", "leadtime", "__match_stage"]

    direct_source = quotations_prepared.sort(["category_name", "sales_channel", "product_id", "quote_date_ref"])
    direct_join = (
        prepared.sort(["category_name", "sales_channel", "product_id", "__cutoff_date"])
        .join_asof(
            direct_source,
            left_on="__cutoff_date",
            right_on="quote_date_ref",
            by=["category_name", "sales_channel", "product_id"],
            strategy="backward",
        )
    )
    matched_direct = (
        direct_join.filter(pl.col("price").is_not_null())
        .with_columns(pl.lit("direct").alias("__match_stage"))
        .select(result_cols)
    )
    remaining = direct_join.filter(pl.col("price").is_null()).select(prepared.columns)

    fallback_source = quotations_prepared.select(
        [
            "quote_date_ref",
            "category_name",
            pl.col("sales_channel").alias("__fallback_sales_channel"),
            "product_id",
            "price",
            "freight",
            "leadtime",
        ]
    ).sort(["category_name", "__fallback_sales_channel", "product_id", "quote_date_ref"])
    fallback_join = (
        remaining
        .with_columns(_channel_fallback_expr().alias("__fallback_sales_channel"))
        .sort(["category_name", "__fallback_sales_channel", "product_id", "__cutoff_date"])
        .join_asof(
            fallback_source,
            left_on="__cutoff_date",
            right_on="quote_date_ref",
            by=["category_name", "__fallback_sales_channel", "product_id"],
            strategy="backward",
        )
    )
    matched_fallback = (
        fallback_join.filter(pl.col("price").is_not_null())
        .with_columns(pl.lit("channel_fallback").alias("__match_stage"))
        .select(result_cols)
    )
    remaining = fallback_join.filter(pl.col("price").is_null()).select(prepared.columns)

    product_source = (
        quotations_prepared.group_by(["quote_date_ref", "product_id"])
        .agg(
            pl.col("price").mean().alias("price"),
            pl.col("freight").mean().alias("freight"),
            pl.col("leadtime").mean().alias("leadtime"),
        )
        .sort(["product_id", "quote_date_ref"])
    )
    product_join = (
        remaining.sort(["product_id", "__cutoff_date"])
        .join_asof(
            product_source,
            left_on="__cutoff_date",
            right_on="quote_date_ref",
            by=["product_id"],
            strategy="backward",
        )
    )
    matched_product = (
        product_join.filter(pl.col("price").is_not_null())
        .with_columns(pl.lit("product_fallback").alias("__match_stage"))
        .select(result_cols)
    )
    unmatched = (
        product_join.filter(pl.col("price").is_null())
        .with_columns(pl.lit("unmatched").alias("__match_stage"))
        .select(result_cols)
    )
    combined = pl.concat([matched_direct, matched_fallback, matched_product, unmatched], how="vertical_relaxed")
    combined = combined.sort("__join_row_id")
    _print_unmatched_debug_stats(combined, "__match_stage", debug_label, weighted_col=weighted_col)
    if drop_unmatched:
        combined = combined.filter(pl.col("__match_stage") != "unmatched")
    return combined.drop(
        ["__join_row_id", "__cutoff_date", "__match_stage", "__fallback_sales_channel", "quote_date_ref"],
        strict=False,
    )


def normalize_xgboost_support_data(
    sales_raw: pl.DataFrame,
    quotations_raw: pl.DataFrame,
    stock_raw: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Normalizes sales, quotations, and stock history for residual feature generation.

    Args:
        sales_raw (pl.DataFrame): Raw sales extract.
        quotations_raw (pl.DataFrame): Raw quotation extract.
        stock_raw (pl.DataFrame): Raw stock extract.

    Returns:
        tuple: Normalized sales, quotations, and stock dataframes used by the residual pipeline.
    """

    sales = domain_timesfm.normalize_sales_dataframe(sales_raw)
    product_dimension = (
        sales.sort(["product_id", "date_ref"])
        .group_by("product_id")
        .agg(
            pl.col("category_name").drop_nulls().last().alias("category_name"),
            pl.col("sales_channel").drop_nulls().last().alias("sales_channel"),
        )
    )
    quotations = (
        quotations_raw.with_columns(
            pl.col("date_ref").cast(pl.Date, strict=False),
            pl.col("product_id").cast(pl.Int64, strict=False),
            pl.col("sales_channel").cast(pl.String, strict=False),
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("leadtime").cast(pl.Float64, strict=False),
            pl.col("freight").cast(pl.Float64, strict=False),
        )
        .join(product_dimension, on="product_id", how="left")
        .drop_nulls(["date_ref", "product_id", "sales_channel", "category_name"])
        .group_by(["date_ref", "product_id", "sales_channel", "category_name"])
        .agg(
            pl.col("price").mean().alias("price"),
            pl.col("leadtime").mean().alias("leadtime"),
            pl.col("freight").mean().alias("freight"),
        )
        .sort(["product_id", "sales_channel", "date_ref"])
    )
    stock = (
        stock_raw.with_columns(
            pl.col("date_ref").cast(pl.Date, strict=False),
            pl.col("product_id").cast(pl.Int64, strict=False),
            pl.col("stock_available").cast(pl.Float64, strict=False),
        )
        .drop_nulls(["date_ref", "product_id", "stock_available"])
        .with_columns((pl.col("stock_available") <= 0).cast(pl.Float64).alias("stockout_flag"))
        .select(["date_ref", "product_id", "stockout_flag"])
        .join(product_dimension, on="product_id", how="left")
        .drop_nulls(["category_name", "sales_channel"])
    )
    return sales, quotations, stock


def _rolling_monthly_weights(sales: pl.DataFrame, feature_window_months: int) -> pl.DataFrame:
    """
    Computes trailing monthly GMV weights per product.

    Args:
        sales (pl.DataFrame): Normalized sales history.
        feature_window_months (int): Number of trailing months used to compute the product weights.

    Returns:
        pl.DataFrame: Monthly product weights used to aggregate residual features.
    """

    monthly_sum = (
        sales.with_columns(month_ref=pl.col("date_ref").dt.truncate("1mo"))
        .group_by(["month_ref", "category_name", "sales_channel", "product_id"])
        .agg(gmv=pl.col("gmv").sum())
        .sort(["category_name", "sales_channel", "product_id", "month_ref"])
    )
    weighted = monthly_sum.with_columns(
        weighted_gmv=pl.col("gmv")
        .shift(1)
        .rolling_sum(window_size=feature_window_months, min_periods=1)
        .over(["category_name", "sales_channel", "product_id"])
        .fill_null(0.0)
    )
    return (
        weighted.with_columns(
            total_weighted_gmv=pl.col("weighted_gmv").sum().over(["month_ref", "category_name", "sales_channel"])
        )
        .with_columns(
            sales_weight=pl.when(pl.col("total_weighted_gmv") > 0)
            .then(pl.col("weighted_gmv") / pl.col("total_weighted_gmv"))
            .otherwise(0.0)
        )
        .select(["month_ref", "category_name", "sales_channel", "product_id", "weighted_gmv", "sales_weight"])
    )


def _add_daily_trailing_stats(df: pl.DataFrame, feature_window_months: int) -> pl.DataFrame:
    """
    Adds trailing daily KPI statistics per product.

    Args:
        df (pl.DataFrame): Daily product-level feature base before trailing statistics are added.
        feature_window_months (int): Number of trailing months converted into a daily rolling window.

    Returns:
        pl.DataFrame: Daily feature base enriched with KPI z-scores and best-moment flags.
    """

    window_days = max(feature_window_months * 31, 1)
    group_cols = ["category_name", "sales_channel", "product_id"]
    df = df.sort(group_cols + ["date_ref"])
    stats_exprs: list[pl.Expr] = []
    for short_name, source_col in KPI_COLUMN_MAP.items():
        history = pl.col(source_col).shift(1).over(group_cols)
        stats_exprs.extend(
            [
                history.rolling_mean(window_size=window_days).over(group_cols).alias(f"{short_name}_mean"),
                history.rolling_std(window_size=window_days).over(group_cols).alias(f"{short_name}_std"),
                history.rolling_min(window_size=window_days).over(group_cols).alias(f"{short_name}_best_value"),
            ]
        )
    df_with_stats = df.with_columns(stats_exprs)
    derived_exprs: list[pl.Expr] = []
    for short_name, source_col in KPI_COLUMN_MAP.items():
        derived_exprs.extend(
            [
                _safe_z_score_expr(source_col, f"{short_name}_mean", f"{short_name}_std", f"z_{short_name}"),
                (
                    (pl.col(source_col) <= (pl.col(f"{short_name}_best_value").fill_null(pl.col(source_col)) + 1e-9))
                    .cast(pl.Float64)
                    .alias(f"best_{short_name}_moment")
                ),
            ]
        )
    return df_with_stats.with_columns(derived_exprs)


def _add_monthly_trailing_stats(df: pl.DataFrame, feature_window_months: int) -> pl.DataFrame:
    """
    Adds trailing monthly KPI statistics per product.

    Args:
        df (pl.DataFrame): Monthly product-level feature base before trailing statistics are added.
        feature_window_months (int): Number of trailing months used by the monthly rolling window.

    Returns:
        pl.DataFrame: Monthly feature base enriched with KPI z-scores and best-moment flags.
    """

    group_cols = ["category_name", "sales_channel", "product_id"]
    df = df.sort(group_cols + ["month_ref"])
    stats_exprs: list[pl.Expr] = []
    for short_name, source_col in KPI_COLUMN_MAP.items():
        history = pl.col(source_col).shift(1).over(group_cols)
        stats_exprs.extend(
            [
                history.rolling_mean(window_size=feature_window_months, min_periods=1).over(group_cols).alias(f"{short_name}_mean"),
                history.rolling_std(window_size=feature_window_months, min_periods=1).over(group_cols).alias(f"{short_name}_std"),
                history.rolling_min(window_size=feature_window_months, min_periods=1).over(group_cols).alias(f"{short_name}_best_value"),
            ]
        )
    df_with_stats = df.with_columns(stats_exprs)
    derived_exprs: list[pl.Expr] = []
    for short_name, source_col in KPI_COLUMN_MAP.items():
        derived_exprs.extend(
            [
                _safe_z_score_expr(source_col, f"{short_name}_mean", f"{short_name}_std", f"z_{short_name}"),
                (
                    (pl.col(source_col) <= (pl.col(f"{short_name}_best_value").fill_null(pl.col(source_col)) + 1e-9))
                    .cast(pl.Float64)
                    .alias(f"best_{short_name}_moment")
                ),
            ]
        )
    return df_with_stats.with_columns(derived_exprs)


def build_product_xgboost_base(
    sales_raw: pl.DataFrame,
    quotations_raw: pl.DataFrame,
    stock_raw: pl.DataFrame,
    feature_window_months: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Builds the daily and monthly product-level feature bases used by the residual models.

    Args:
        sales_raw (pl.DataFrame): Raw sales extract.
        quotations_raw (pl.DataFrame): Raw quotation extract.
        stock_raw (pl.DataFrame): Raw stock extract.
        feature_window_months (int): Number of trailing months used by the feature engineering logic.

    Returns:
        tuple: Daily and monthly product-level feature bases used by the residual models.
    """

    sales, quotations, stock = normalize_xgboost_support_data(sales_raw, quotations_raw, stock_raw)
    monthly_weights = _rolling_monthly_weights(sales, feature_window_months)

    daily_targets = (
        stock.with_columns(month_ref=pl.col("date_ref").dt.truncate("1mo"))
        .join(monthly_weights, on=["month_ref", "category_name", "sales_channel", "product_id"], how="inner")
    )
    daily_base = join_adjusted_features(
        left_df=daily_targets,
        quotations=quotations,
        time_col="date_ref",
        debug_label="historical_daily_kpi_join",
        weighted_col="weighted_gmv",
    )
    daily_base = _add_daily_trailing_stats(daily_base, feature_window_months)

    monthly_stock = (
        stock.with_columns(month_ref=pl.col("date_ref").dt.truncate("1mo"))
        .group_by(["month_ref", "product_id"])
        .agg(pl.col("stockout_flag").mean().alias("stockout_rate"))
    )
    monthly_base = (
        monthly_weights.join(monthly_stock, on=["month_ref", "product_id"], how="left")
        .pipe(
            lambda frame: join_adjusted_features(
                left_df=frame,
                quotations=quotations,
                time_col="month_ref",
                debug_label="historical_monthly_kpi_join",
                weighted_col="weighted_gmv",
            )
        )
        .with_columns(
            pl.col("stockout_rate").fill_null(1.0),
            pl.col("weighted_gmv").fill_null(0.0),
            pl.col("sales_weight").fill_null(0.0),
        )
    )
    monthly_base = _add_monthly_trailing_stats(monthly_base, feature_window_months)
    return daily_base, monthly_base


def _build_category_feature_aggregates(daily_base: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregates daily product features to the category-level feature frame.

    Args:
        daily_base (pl.DataFrame): Daily product-level feature base.

    Returns:
        pl.DataFrame: Category-level aggregated feature frame used by the GMV residual model.
    """

    group_keys = ["date_ref", "category_name", "sales_channel"]
    prepared = daily_base.with_columns(
        pl.col("weighted_gmv").fill_null(0.0),
        pl.col("sales_weight").fill_null(0.0),
    )
    for short_name in KPI_COLUMN_MAP:
        prepared = prepared.with_columns(
            pl.col(f"z_{short_name}")
            .qcut(5, labels=[str(index) for index in range(1, 6)], allow_duplicates=True)
            .over(group_keys)
            .alias(f"{short_name}_bucket")
        )
    aggregates: list[pl.Expr] = [
        (
            (pl.col("weighted_gmv") * pl.col("stockout_flag")).sum() /
            pl.when(pl.col("weighted_gmv").sum() > 0).then(pl.col("weighted_gmv").sum()).otherwise(1.0)
        ).alias("stock_sales_rep")
    ]
    for short_name in KPI_COLUMN_MAP:
        total_weight = pl.when(pl.col("weighted_gmv").sum() > 0).then(pl.col("weighted_gmv").sum()).otherwise(1.0)
        aggregates.append(((pl.col("weighted_gmv") * pl.col(f"z_{short_name}")).sum() / total_weight).alias(f"weighted_z_{short_name}"))
        aggregates.append(((pl.col("weighted_gmv") * pl.col(f"best_{short_name}_moment")).sum() / total_weight).alias(f"best_{short_name}_moment"))
        for bucket in range(1, 6):
            aggregates.append(
                (pl.col("weighted_gmv").filter(pl.col(f"{short_name}_bucket") == str(bucket)).sum() / total_weight).alias(f"{short_name}_q{bucket}")
            )
    return prepared.group_by(group_keys).agg(aggregates)


def build_category_xgboost_features(
    daily_base: pl.DataFrame,
    gmv_errors: pl.DataFrame,
    category_history_months: int,
) -> pl.DataFrame:
    """
    Builds the category-level residual feature table for GMV correction.

    Args:
        daily_base (pl.DataFrame): Daily product-level feature base.
        gmv_errors (pl.DataFrame): TimesFM GMV residual dataframe.
        category_history_months (int): Number of recent months kept for model training and validation.

    Returns:
        pl.DataFrame: Category-level GMV residual feature dataset.
    """

    features = _build_category_feature_aggregates(daily_base).with_columns(pl.col("date_ref").cast(pl.Date, strict=False))
    targets = gmv_errors.with_columns(pl.col("date_ref").cast(pl.Date, strict=False))
    final_df = features.join(targets, on=["date_ref", "category_name", "sales_channel"], how="inner")
    if category_history_months > 0 and not final_df.is_empty():
        max_date = final_df.get_column("date_ref").max()
        cutoff = max_date - pl.duration(days=category_history_months * 30)
        final_df = final_df.filter(pl.col("date_ref") >= cutoff)
    return final_df.with_columns(
        pl.col("category_name").cast(pl.Categorical),
        pl.col("sales_channel").cast(pl.Categorical),
    ).sort(["category_name", "sales_channel", "date_ref"])


def build_share_xgboost_features(
    monthly_base: pl.DataFrame,
    share_errors: pl.DataFrame,
    share_history_months: int,
) -> pl.DataFrame:
    """
    Builds the product-level residual feature table for share correction.

    Args:
        monthly_base (pl.DataFrame): Monthly product-level feature base.
        share_errors (pl.DataFrame): TimesFM share residual dataframe.
        share_history_months (int): Number of recent months kept for model training and validation.

    Returns:
        pl.DataFrame: Product-level share residual feature dataset.
    """

    feature_cols = [
        "price",
        "freight",
        "leadtime",
        "z_price",
        "z_freight",
        "z_leadtime",
        "best_price_moment",
        "best_freight_moment",
        "best_leadtime_moment",
        "stockout_rate",
    ]
    features = monthly_base.select(["month_ref", "category_name", "sales_channel", "product_id", *feature_cols])
    final_df = (
        share_errors.with_columns(pl.col("month_ref").cast(pl.Date, strict=False))
        .join(features, on=["month_ref", "category_name", "sales_channel", "product_id"], how="left")
        .with_columns([pl.col(column).fill_null(0.0) for column in feature_cols])
    )
    if share_history_months > 0 and not final_df.is_empty():
        max_date = pd.Timestamp(final_df.get_column("month_ref").max())
        cutoff = (max_date - pd.DateOffset(months=share_history_months)).date()
        final_df = final_df.filter(pl.col("month_ref") >= pl.lit(cutoff))
    return final_df.sort(["category_name", "sales_channel", "product_id", "month_ref"])


def _time_based_split(df: pl.DataFrame, time_col: str, holdout_size: int) -> schemas.TrainValidationSplit:
    """
    Builds a time-based train-validation split.

    Args:
        df (pl.DataFrame): Feature dataframe that should be split by time.
        time_col (str): Time column used to define the split boundary.
        holdout_size (int): Number of most recent periods reserved for validation.

    Returns:
        schemas.TrainValidationSplit: Split metadata with cutoff and row counts.
    """

    unique_times = sorted(df.get_column(time_col).unique().to_list())
    if not unique_times:
        raise ValueError("Cannot split an empty dataframe.")
    effective_holdout = min(max(holdout_size, 1), len(unique_times))
    cutoff = unique_times[-effective_holdout]
    train_rows = df.filter(pl.col(time_col) < pl.lit(cutoff)).height
    validation_rows = df.filter(pl.col(time_col) >= pl.lit(cutoff)).height
    if train_rows == 0:
        cutoff = unique_times[-1]
        train_rows = df.filter(pl.col(time_col) < pl.lit(cutoff)).height
        validation_rows = df.filter(pl.col(time_col) >= pl.lit(cutoff)).height
    return schemas.TrainValidationSplit(cutoff=cutoff, train_rows=train_rows, validation_rows=validation_rows)


def _build_metrics(model_name: str, target_col: str, y_true: pl.Series, y_pred: pl.Series) -> schemas.ModelMetrics:
    """
    Computes MAE and RMSE for one residual model.

    Args:
        model_name (str): Logical model name used in the metrics output.
        target_col (str): Residual target column name.
        y_true (pl.Series): Validation residual targets.
        y_pred (pl.Series): Validation residual predictions.

    Returns:
        schemas.ModelMetrics: Validation metric summary for the residual model.
    """

    errors = y_true - y_pred
    mae = float(errors.abs().mean()) if not errors.is_empty() else 0.0
    rmse = float((errors.pow(2).mean()) ** 0.5) if not errors.is_empty() else 0.0
    return schemas.ModelMetrics(
        model_name=model_name,
        target_col=target_col,
        train_rows=0,
        validation_rows=len(y_true),
        mae=mae,
        rmse=rmse,
    )


def _reconcile_grouped_prediction(df: pl.DataFrame, prediction_col: str, group_cols: list[str]) -> pl.DataFrame:
    """
    Renormalizes grouped corrected predictions so each group sums to one.

    Args:
        df (pl.DataFrame): Prediction dataframe containing the corrected prediction column.
        prediction_col (str): Corrected prediction column that should be normalized.
        group_cols (list[str]): Grouping columns used to enforce the unit-sum constraint.

    Returns:
        pl.DataFrame: Prediction dataframe with normalized corrected predictions.
    """

    return (
        df.with_columns(pl.col(prediction_col).sum().over(group_cols).alias("__group_prediction_sum"))
        .with_columns(
            pl.when(pl.col("__group_prediction_sum") > 0)
            .then(pl.col(prediction_col) / pl.col("__group_prediction_sum"))
            .otherwise(0.0)
            .alias(prediction_col)
        )
        .drop("__group_prediction_sum")
    )


def train_residual_model(
    df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str,
    time_col: str,
    actual_col: str,
    baseline_col: str,
    model_name: str,
    regressor: object,
    holdout_size: int,
    prediction_col: str,
    corrected_prediction_col: str,
    corrected_error_col: str,
    corrected_low: float,
    corrected_high: float | None = None,
    reconcile_prediction_group_cols: list[str] | None = None,
) -> schemas.ResidualModelArtifacts:
    """
    Fits one residual model and returns features, predictions, audit output, and validation metrics.

    Args:
        df (pl.DataFrame): Residual feature dataframe used for training and scoring.
        feature_cols (list[str]): Predictor columns passed to the regressor.
        target_col (str): Residual target column.
        time_col (str): Time column used for the train-validation split.
        actual_col (str): Observed business target column.
        baseline_col (str): TimesFM baseline prediction column.
        model_name (str): Logical model name.
        regressor (object): Fitted-compatible regressor implementing `fit` and `predict`.
        holdout_size (int): Number of periods reserved for validation.
        prediction_col (str): Residual prediction output column.
        corrected_prediction_col (str): Corrected business prediction output column.
        corrected_error_col (str): Corrected residual error output column.
        corrected_low (float): Lower clipping bound applied to corrected predictions.
        corrected_high (float | None): Optional upper clipping bound applied to corrected predictions.
        reconcile_prediction_group_cols (list[str] | None): Optional grouping columns used to normalize corrected predictions.

    Returns:
        schemas.ResidualModelArtifacts: Features, scored predictions, audit view, and validation metrics.
    """

    if df.is_empty():
        raise ValueError(f"{model_name} feature dataframe is empty.")
    split = _time_based_split(df, time_col, holdout_size)
    train_df = df.filter(pl.col(time_col) < split.cutoff)
    validation_df = df.filter(pl.col(time_col) >= split.cutoff)
    if train_df.is_empty():
        raise ValueError(f"{model_name} requires at least one training row before the validation cutoff.")

    X_train = train_df.select(feature_cols).fill_null(0.0)
    y_train = train_df.get_column(target_col)
    regressor.fit(X_train, y_train)

    X_all = df.select(feature_cols).fill_null(0.0)
    y_pred_all = regressor.predict(X_all)
    predictions = (
        df.with_columns(pl.Series(prediction_col, y_pred_all))
        .with_columns((pl.col(baseline_col) + pl.col(prediction_col)).alias(corrected_prediction_col))
        .with_columns(
            pl.col(corrected_prediction_col).clip(lower_bound=corrected_low, upper_bound=corrected_high).alias(corrected_prediction_col)
        )
    )
    if reconcile_prediction_group_cols:
        predictions = _reconcile_grouped_prediction(predictions, corrected_prediction_col, reconcile_prediction_group_cols)
    predictions = predictions.with_columns((pl.col(actual_col) - pl.col(corrected_prediction_col)).alias(corrected_error_col))

    valid_predictions = predictions.filter(pl.col(time_col) >= split.cutoff)
    metrics_raw = _build_metrics(
        model_name=model_name,
        target_col=target_col,
        y_true=valid_predictions.get_column(target_col),
        y_pred=valid_predictions.get_column(prediction_col),
    )
    metrics = schemas.ModelMetrics(
        model_name=metrics_raw.model_name,
        target_col=metrics_raw.target_col,
        train_rows=train_df.height,
        validation_rows=validation_df.height,
        mae=metrics_raw.mae,
        rmse=metrics_raw.rmse,
    )
    preserved_audit_cols = {time_col, "category_name", "sales_channel", "product_id"}
    audit_cols = [column for column in predictions.columns if column not in feature_cols or column in preserved_audit_cols]
    return schemas.ResidualModelArtifacts(
        features=df,
        predictions=predictions,
        audit=predictions.select(audit_cols),
        metrics=metrics,
    )


def get_category_feature_columns() -> list[str]:
    """
    Returns the feature columns used by the category residual model.

    Returns:
        list[str]: Predictor columns passed to the category-level GMV residual model.
    """

    columns = ["category_name", "sales_channel"]
    for prefix in ("price", "freight", "leadtime"):
        columns.extend([f"{prefix}_q{index}" for index in range(1, 6)])
        columns.append(f"weighted_z_{prefix}")
        columns.append(f"best_{prefix}_moment")
    columns.extend(["stock_sales_rep", "timesfm_gmv_pred"])
    return columns


def get_share_feature_columns() -> list[str]:
    """
    Returns the feature columns used by the share residual model.

    Returns:
        list[str]: Predictor columns passed to the product-level share residual model.
    """

    return [
        "price",
        "freight",
        "leadtime",
        "z_price",
        "z_freight",
        "z_leadtime",
        "best_price_moment",
        "best_freight_moment",
        "best_leadtime_moment",
        "stockout_rate",
        "timesfm_share_pred",
    ]


def metrics_to_frame(metrics: list[schemas.ModelMetrics]) -> pl.DataFrame:
    """
    Converts metric dataclasses into a dataframe.

    Args:
        metrics (list[schemas.ModelMetrics]): Residual model metric dataclasses.

    Returns:
        pl.DataFrame: Tabular metrics output ready for persistence.
    """

    return pl.from_dicts([asdict(item) for item in metrics])
