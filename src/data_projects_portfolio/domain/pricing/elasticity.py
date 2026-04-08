import math
from dataclasses import asdict
from datetime import date, timedelta

import numpy as np
import pandas as pd

from data_projects_portfolio.domain.pricing.models import ElasticityResult, RegressionFit, SeasonalKey

# Business rule constants — set by data science, not per-environment config
MIN_OBSERVATIONS: int = 30
MAX_P_VALUE: float = 0.25
MIN_R_SQUARED: float = 0.05
PRICE_VARIATION_THRESHOLD: float = 0.02


def _get_nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    first_day = date(year, month, 1)
    delta = (weekday - first_day.weekday()) % 7
    nth_date = first_day + timedelta(days=delta) + timedelta(weeks=n - 1)
    return nth_date


def _build_spike_dates(years: list[int]) -> set[date]:
    spike_dates: set[date] = set()
    for year in years:
        for month in range(1, 13):
            try:
                spike_dates.add(date(year, month, month))
            except ValueError:
                continue
        spike_dates.add(_get_nth_weekday_of_month(year, 5, 6, 2))
        spike_dates.add(_get_nth_weekday_of_month(year, 8, 6, 2))
        spike_dates.add(date(year, 12, 24))
        spike_dates.add(date(year, 12, 25))
    return spike_dates


def flag_spike_days(df: pd.DataFrame) -> pd.DataFrame:
    final_df = df.copy()
    final_df["data_date"] = pd.to_datetime(final_df["data_date"])
    years = final_df["data_date"].dt.year.unique().tolist()
    spike_dates = _build_spike_dates(years)
    final_df["is_spike_day"] = final_df["data_date"].dt.date.isin(spike_dates).astype(int)
    return final_df


def filter_low_price_variation(df: pd.DataFrame, threshold: float = PRICE_VARIATION_THRESHOLD) -> pd.DataFrame:
    variation_df = (
        df.groupby(["product_id", "sale_location"])["preco"]
        .agg(avg_price="mean", std_price="std")
        .reset_index()
    )
    variation_df["coeficiente_variacao"] = np.where(
        variation_df["avg_price"] > 0,
        variation_df["std_price"].fillna(0.0) / variation_df["avg_price"],
        0.0,
    )
    valid_pairs = variation_df[variation_df["coeficiente_variacao"] >= threshold][
        ["product_id", "sale_location"]
    ]
    final_df = df.merge(valid_pairs, on=["product_id", "sale_location"], how="inner")
    return final_df


def calculate_seasonal_index(df: pd.DataFrame) -> dict[SeasonalKey, float]:
    clean_df = df[df["is_spike_day"] == 0].copy()
    clean_df["month"] = pd.to_datetime(clean_df["data_date"]).dt.month

    monthly_totals = (
        clean_df.groupby(["product_category", "month"])["quantidade_produto"]
        .sum()
        .reset_index()
    )
    category_monthly_avg = (
        clean_df.groupby("product_category")["quantidade_produto"]
        .sum()
        .div(12)
        .reset_index()
        .rename(columns={"quantidade_produto": "avg_monthly"})
    )
    merged = monthly_totals.merge(category_monthly_avg, on="product_category", how="left")
    merged["seasonal_factor"] = merged["quantidade_produto"] / merged["avg_monthly"].replace(0, 1.0)

    final_index = {
        SeasonalKey(product_category=category, month=month): float(factor)
        for (category, month), factor in merged.set_index(["product_category", "month"])[
            "seasonal_factor"
        ].to_dict().items()
    }
    return final_index


def adjust_units_for_seasonality(
    df: pd.DataFrame, seasonal_index: dict[SeasonalKey, float]
) -> pd.DataFrame:
    final_df = df.copy()
    final_df["month"] = pd.to_datetime(final_df["data_date"]).dt.month

    if seasonal_index:
        seasonal_df = pd.DataFrame(
            [
                {
                    "product_category": key.product_category,
                    "month": key.month,
                    "seasonal_factor": value,
                }
                for key, value in seasonal_index.items()
            ]
        )
        final_df = final_df.merge(seasonal_df, on=["product_category", "month"], how="left")
    else:
        final_df["seasonal_factor"] = float("nan")

    final_df["seasonal_factor"] = final_df["seasonal_factor"].fillna(1.0)
    final_df["quantidade_ajustada"] = (
        final_df["quantidade_produto"] / final_df["seasonal_factor"].replace(0, 1.0)
    )
    return final_df


def build_price_tiers(df: pd.DataFrame) -> pd.DataFrame:
    enriched_df = df.copy()
    avg_price_df = (
        enriched_df.groupby(["product_id", "sale_location"])["preco"]
        .mean()
        .reset_index()
        .rename(columns={"preco": "avg_price"})
    )
    enriched_df = enriched_df.merge(avg_price_df, on=["product_id", "sale_location"], how="left")

    tiers: list[pd.Series] = []
    for _, group_df in enriched_df.groupby(["sale_location", "product_category"], sort=False):
        unique_prices = group_df["avg_price"].nunique(dropna=True)
        if len(group_df) < 5 or unique_prices < 5:
            tier_series = pd.Series([3] * len(group_df), index=group_df.index, dtype="int64")
        else:
            tier_series = pd.qcut(
                group_df["avg_price"].rank(method="first"),
                q=5,
                labels=[1, 2, 3, 4, 5],
            ).astype(int)
        tiers.append(tier_series)

    enriched_df["nivel_de_preco"] = pd.concat(tiers).sort_index().astype(int)
    final_df = enriched_df.drop(columns=["avg_price"])
    return final_df


def _linear_regression(
    log_quantity: np.ndarray,
    log_price: np.ndarray,
    log_delivery_time: np.ndarray,
    is_spike_day: np.ndarray,
) -> RegressionFit:
    null_fit = RegressionFit(coefficients=None, p_values=None, r_squared=None)
    intercept = np.ones_like(log_quantity)
    design_matrix = np.column_stack([intercept, log_price, log_delivery_time, is_spike_day])

    n_observations, n_features = design_matrix.shape
    if n_observations <= n_features:
        return null_fit

    try:
        coefficients, _, _, _ = np.linalg.lstsq(design_matrix, log_quantity, rcond=None)
    except np.linalg.LinAlgError:
        return null_fit

    predicted = design_matrix @ coefficients
    residuals = log_quantity - predicted
    sum_sq_errors = float(np.dot(residuals, residuals))
    centered = log_quantity - log_quantity.mean()
    total_sum_sq = float(np.dot(centered, centered))

    if total_sum_sq == 0:
        return null_fit

    r_squared = 1.0 - (sum_sq_errors / total_sum_sq)
    mean_sq_error = sum_sq_errors / (n_observations - n_features)
    covariance_matrix = mean_sq_error * np.linalg.pinv(design_matrix.T @ design_matrix)
    standard_errors = np.sqrt(np.diag(covariance_matrix))

    t_statistics = np.zeros_like(coefficients)
    valid_errors = standard_errors > 0
    t_statistics[valid_errors] = coefficients[valid_errors] / standard_errors[valid_errors]
    p_values = np.array([math.erfc(abs(value) / math.sqrt(2)) for value in t_statistics])

    return RegressionFit(coefficients=coefficients, p_values=p_values, r_squared=r_squared)


def run_log_log_regression(
    product_data: pd.DataFrame,
    min_observations: int = MIN_OBSERVATIONS,
    max_p_value: float = MAX_P_VALUE,
    min_r_squared: float = MIN_R_SQUARED,
) -> ElasticityResult:
    product_id = str(product_data["product_id"].iloc[0])
    sale_location = str(product_data["sale_location"].iloc[0])
    product_category = str(product_data["product_category"].iloc[0])
    nivel_de_preco = int(product_data["nivel_de_preco"].iloc[0])
    qtde_observacoes = int(len(product_data))

    null_result = ElasticityResult(
        product_id=product_id,
        sale_location=sale_location,
        product_category=product_category,
        nivel_de_preco=nivel_de_preco,
        elasticidade_preco=None,
        elasticidade_prazo=None,
        qtde_observacoes=qtde_observacoes,
        nivel_confianca="baixo",
    )

    if qtde_observacoes < min_observations:
        return null_result

    valid_df = product_data[
        (product_data["preco"] > 0)
        & (product_data["prazo"] > 0)
        & (product_data["quantidade_ajustada"] > 0)
    ].copy()

    if len(valid_df) < min_observations:
        return null_result

    fit = _linear_regression(
        log_quantity=np.log(valid_df["quantidade_ajustada"].to_numpy(dtype=float)),
        log_price=np.log(valid_df["preco"].to_numpy(dtype=float)),
        log_delivery_time=np.log(valid_df["prazo"].to_numpy(dtype=float)),
        is_spike_day=valid_df["is_spike_day"].to_numpy(dtype=float),
    )

    if fit.coefficients is None or fit.p_values is None or fit.r_squared is None:
        return null_result

    elasticity_price = float(fit.coefficients[1])
    elasticity_delivery = float(fit.coefficients[2])
    p_value_price = float(fit.p_values[1])
    r_squared = float(fit.r_squared)

    if p_value_price > max_p_value or r_squared < min_r_squared or elasticity_price >= 0:
        return null_result

    return ElasticityResult(
        product_id=product_id,
        sale_location=sale_location,
        product_category=product_category,
        nivel_de_preco=nivel_de_preco,
        elasticidade_preco=round(elasticity_price, 4),
        elasticidade_prazo=round(elasticity_delivery, 4),
        qtde_observacoes=qtde_observacoes,
        nivel_confianca="alto",
    )


def impute_elasticities(results: list[ElasticityResult]) -> pd.DataFrame:
    result_df = pd.DataFrame([asdict(result) for result in results])
    valid_df = result_df[result_df["elasticidade_preco"].notna()].copy()

    tier_avg_df = (
        valid_df.groupby(["sale_location", "product_category", "nivel_de_preco"])[
            ["elasticidade_preco", "elasticidade_prazo"]
        ]
        .mean()
        .rename(columns={"elasticidade_preco": "_tier_price", "elasticidade_prazo": "_tier_prazo"})
        .reset_index()
    )
    category_avg_df = (
        valid_df.groupby(["sale_location", "product_category"])[
            ["elasticidade_preco", "elasticidade_prazo"]
        ]
        .mean()
        .rename(
            columns={"elasticidade_preco": "_category_price", "elasticidade_prazo": "_category_prazo"}
        )
        .reset_index()
    )

    result_df = result_df.merge(
        tier_avg_df, on=["sale_location", "product_category", "nivel_de_preco"], how="left"
    )
    result_df = result_df.merge(
        category_avg_df, on=["sale_location", "product_category"], how="left"
    )

    tier_mask = result_df["elasticidade_preco"].isna() & result_df["_tier_price"].notna()
    result_df.loc[tier_mask, "elasticidade_preco"] = result_df.loc[tier_mask, "_tier_price"]
    result_df.loc[tier_mask, "elasticidade_prazo"] = result_df.loc[tier_mask, "_tier_prazo"]
    result_df.loc[tier_mask, "nivel_confianca"] = "medio"

    category_mask = result_df["elasticidade_preco"].isna() & result_df["_category_price"].notna()
    result_df.loc[category_mask, "elasticidade_preco"] = result_df.loc[category_mask, "_category_price"]
    result_df.loc[category_mask, "elasticidade_prazo"] = result_df.loc[category_mask, "_category_prazo"]
    result_df.loc[category_mask, "nivel_confianca"] = "baixo"

    return result_df.drop(
        columns=["_tier_price", "_tier_prazo", "_category_price", "_category_prazo"]
    )
