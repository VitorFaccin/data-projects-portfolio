"""
Unit tests for domain/pricing/elasticity.py.

These tests require no Docker, no MinIO, no Spark — pure Python only.
Run with: pytest tests/unit/
"""

import numpy as np
import pandas as pd
import pytest

from data_projects_portfolio.domain.pricing.models import ElasticityResult, SeasonalKey
from data_projects_portfolio.domain.pricing.elasticity import (
    adjust_units_for_seasonality,
    build_price_tiers,
    calculate_seasonal_index,
    filter_low_price_variation,
    flag_spike_days,
    impute_elasticities,
    run_log_log_regression,
)


def make_df(**kwargs) -> pd.DataFrame:
    return pd.DataFrame(kwargs)


def make_elastic_product(
    n: int = 60,
    seed: int = 42,
    sale_location: str = "SP",
    product_category: str = "bed_bath_table",
) -> pd.DataFrame:
    np.random.seed(seed)
    prices = np.random.uniform(80, 120, n)
    prazo = np.random.uniform(3, 10, n)
    units = np.exp(5 - 1.5 * np.log(prices) - 0.3 * np.log(prazo) + np.random.normal(0, 0.1, n))
    return pd.DataFrame(
        {
            "product_id": ["produto_1"] * n,
            "sale_location": [sale_location] * n,
            "product_category": [product_category] * n,
            "nivel_de_preco": [3] * n,
            "preco": prices,
            "prazo": prazo,
            "quantidade_ajustada": units,
            "is_spike_day": [0] * n,
        }
    )


def make_null_result(
    product_id: str,
    nivel_de_preco: int = 3,
    product_category: str = "bed_bath_table",
    sale_location: str = "SP",
) -> ElasticityResult:
    return ElasticityResult(
        product_id=product_id,
        sale_location=sale_location,
        product_category=product_category,
        nivel_de_preco=nivel_de_preco,
        elasticidade_preco=None,
        elasticidade_prazo=None,
        qtde_observacoes=5,
        nivel_confianca="baixo",
    )


def make_valid_result(
    product_id: str,
    elasticidade_preco: float,
    nivel_de_preco: int = 3,
    product_category: str = "bed_bath_table",
    sale_location: str = "SP",
) -> ElasticityResult:
    return ElasticityResult(
        product_id=product_id,
        sale_location=sale_location,
        product_category=product_category,
        nivel_de_preco=nivel_de_preco,
        elasticidade_preco=elasticidade_preco,
        elasticidade_prazo=-0.5,
        qtde_observacoes=60,
        nivel_confianca="alto",
    )


class TestFlagSpikeDays:
    def test_double_date_is_flagged(self):
        df = make_df(
            data_date=["2024-03-03"],
            product_id=["produto_1"],
            quantidade_produto=[10.0],
            product_category=["bed_bath_table"],
        )
        assert flag_spike_days(df)["is_spike_day"].iloc[0] == 1

    def test_regular_day_is_not_flagged(self):
        df = make_df(
            data_date=["2024-03-15"],
            product_id=["produto_1"],
            quantidade_produto=[10.0],
            product_category=["bed_bath_table"],
        )
        assert flag_spike_days(df)["is_spike_day"].iloc[0] == 0


class TestFilterLowPriceVariation:
    def test_removes_constant_price_pair(self):
        df = make_df(
            product_id=["a", "a", "a", "b", "b", "b"],
            sale_location=["SP", "SP", "SP", "RJ", "RJ", "RJ"],
            preco=[100.0, 100.0, 100.0, 100.0, 120.0, 140.0],
        )
        result = filter_low_price_variation(df, threshold=0.02)
        assert "a" not in result["product_id"].values
        assert "b" in result["product_id"].values


class TestBuildPriceTiers:
    def test_creates_five_tiers(self):
        rows = [
            {"product_id": f"p{i}", "sale_location": "SP", "product_category": "cat", "preco": float(i * 10)}
            for i in range(1, 26)
        ]
        result = build_price_tiers(pd.DataFrame(rows))
        assert set(result["nivel_de_preco"].unique()) == {1, 2, 3, 4, 5}

    def test_fallback_to_tier_three_for_small_groups(self):
        df = pd.DataFrame(
            [{"product_id": "p1", "sale_location": "SP", "product_category": "cat", "preco": 100.0}]
        )
        assert build_price_tiers(df)["nivel_de_preco"].iloc[0] == 3


class TestSeasonality:
    def test_adjusted_quantity_divides_by_factor(self):
        df = make_df(
            data_date=["2024-12-15"],
            product_id=["p1"],
            product_category=["bed_bath_table"],
            quantidade_produto=[120.0],
            is_spike_day=[0],
        )
        seasonal_index = {SeasonalKey("bed_bath_table", 12): 1.2}
        result = adjust_units_for_seasonality(df, seasonal_index)
        assert abs(result["quantidade_ajustada"].iloc[0] - 100.0) < 0.01

    def test_seasonal_index_reflects_high_month(self):
        rows = [
            {
                "data_date": f"2024-{month:02d}-15",
                "product_id": "p1",
                "product_category": "bed_bath_table",
                "quantidade_produto": 200.0 if month == 12 else 100.0,
                "is_spike_day": 0,
            }
            for month in range(1, 13)
        ]
        index = calculate_seasonal_index(pd.DataFrame(rows))
        assert index[SeasonalKey("bed_bath_table", 12)] > 1.0


class TestRunLogLogRegression:
    _params = dict(min_observations=30, max_p_value=0.10, min_r_squared=0.05)

    def test_valid_product_returns_negative_elasticity(self):
        result = run_log_log_regression(make_elastic_product(), **self._params)
        assert result.elasticidade_preco is not None
        assert result.elasticidade_preco < 0

    def test_insufficient_observations_returns_null(self):
        result = run_log_log_regression(make_elastic_product(n=10), **self._params)
        assert result.elasticidade_preco is None


class TestImputeElasticities:
    def test_tier_fallback_sets_medium_confidence(self):
        results = [
            make_valid_result("p1", -1.2, nivel_de_preco=3),
            make_valid_result("p2", -1.4, nivel_de_preco=3),
            make_null_result("p3", nivel_de_preco=3),
        ]
        df = impute_elasticities(results)
        row = df[df["product_id"] == "p3"].iloc[0]
        assert row["nivel_confianca"] == "medio"
        assert row["elasticidade_preco"] is not None

    def test_category_fallback_sets_low_confidence(self):
        results = [
            make_valid_result("p1", -1.5, nivel_de_preco=2),
            make_null_result("p2", nivel_de_preco=5),
        ]
        df = impute_elasticities(results)
        row = df[df["product_id"] == "p2"].iloc[0]
        assert row["nivel_confianca"] == "baixo"
        assert abs(row["elasticidade_preco"] - (-1.5)) < 0.01
