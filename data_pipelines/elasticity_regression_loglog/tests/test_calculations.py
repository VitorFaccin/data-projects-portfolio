import numpy as np
import pandas as pd
import sys
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from core.domain import (
    adjust_units_for_seasonality,
    build_price_tiers,
    calculate_seasonal_index,
    filter_low_price_variation,
    flag_spike_days,
    impute_elasticities,
    run_log_log_regression,
)
from core.schemas import ElasticityResult, SeasonalKey


def make_df(**kwargs) -> pd.DataFrame:
    final_df = pd.DataFrame(kwargs)
    return final_df


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
    final_df = pd.DataFrame(
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
    return final_df


def make_null_result(
    product_id: str,
    nivel_de_preco: int = 3,
    product_category: str = "bed_bath_table",
    sale_location: str = "SP",
) -> ElasticityResult:
    final_result = ElasticityResult(
        product_id=product_id,
        sale_location=sale_location,
        product_category=product_category,
        nivel_de_preco=nivel_de_preco,
        elasticidade_preco=None,
        elasticidade_prazo=None,
        qtde_observacoes=5,
        nivel_confianca="baixo",
    )
    return final_result


def make_valid_result(
    product_id: str,
    elasticidade_preco: float,
    nivel_de_preco: int = 3,
    product_category: str = "bed_bath_table",
    sale_location: str = "SP",
) -> ElasticityResult:
    final_result = ElasticityResult(
        product_id=product_id,
        sale_location=sale_location,
        product_category=product_category,
        nivel_de_preco=nivel_de_preco,
        elasticidade_preco=elasticidade_preco,
        elasticidade_prazo=-0.5,
        qtde_observacoes=60,
        nivel_confianca="alto",
    )
    return final_result


class TestFlagSpikeDays:
    def test_double_date_is_flagged(self):
        base_df = make_df(
            data_date=["2024-03-03"],
            product_id=["produto_1"],
            quantidade_produto=[10.0],
            product_category=["bed_bath_table"],
        )
        final_df = flag_spike_days(base_df)
        assert final_df["is_spike_day"].iloc[0] == 1

    def test_regular_day_is_not_flagged(self):
        base_df = make_df(
            data_date=["2024-03-15"],
            product_id=["produto_1"],
            quantidade_produto=[10.0],
            product_category=["bed_bath_table"],
        )
        final_df = flag_spike_days(base_df)
        assert final_df["is_spike_day"].iloc[0] == 0


class TestFilterLowPriceVariation:
    def test_removes_constant_product_location_pair(self):
        base_df = make_df(
            product_id=["a", "a", "a", "b", "b", "b"],
            sale_location=["SP", "SP", "SP", "RJ", "RJ", "RJ"],
            preco=[100.0, 100.0, 100.0, 100.0, 120.0, 140.0],
        )
        final_df = filter_low_price_variation(base_df, threshold=0.02)
        assert "a" not in final_df["product_id"].values
        assert "b" in final_df["product_id"].values


class TestBuildPriceTiers:
    def test_creates_five_tiers(self):
        rows = []
        for idx in range(1, 26):
            rows.append(
                {
                    "product_id": f"produto_{idx}",
                    "sale_location": "SP",
                    "product_category": "bed_bath_table",
                    "preco": float(idx * 10),
                }
            )
        base_df = pd.DataFrame(rows)
        final_df = build_price_tiers(base_df)
        assert set(final_df["nivel_de_preco"].unique()) == {1, 2, 3, 4, 5}

    def test_fallback_to_tier_three_for_small_groups(self):
        base_df = pd.DataFrame(
            [
                {
                    "product_id": "produto_1",
                    "sale_location": "SP",
                    "product_category": "bed_bath_table",
                    "preco": 100.0,
                }
            ]
        )
        final_df = build_price_tiers(base_df)
        assert final_df["nivel_de_preco"].iloc[0] == 3


class TestSeasonality:
    def test_adjusted_quantity_uses_factor(self):
        base_df = make_df(
            data_date=["2024-12-15"],
            product_id=["produto_1"],
            product_category=["bed_bath_table"],
            quantidade_produto=[120.0],
            is_spike_day=[0],
        )
        seasonal_index = {SeasonalKey("bed_bath_table", 12): 1.2}
        final_df = adjust_units_for_seasonality(base_df, seasonal_index)
        assert abs(final_df["quantidade_ajustada"].iloc[0] - 100.0) < 0.01

    def test_seasonal_index_returns_category_month_key(self):
        rows = []
        for month in range(1, 13):
            rows.append(
                {
                    "data_date": f"2024-{month:02d}-15",
                    "product_id": "produto_1",
                    "product_category": "bed_bath_table",
                    "quantidade_produto": 200.0 if month == 12 else 100.0,
                    "is_spike_day": 0,
                }
            )
        base_df = pd.DataFrame(rows)
        final_index = calculate_seasonal_index(base_df)
        assert final_index[SeasonalKey("bed_bath_table", 12)] > 1.0


class TestRunLogLogRegression:
    _params = dict(min_observations=30, max_p_value=0.10, min_r_squared=0.05)

    def test_valid_product_returns_negative_elasticity(self):
        base_df = make_elastic_product()
        final_result = run_log_log_regression(base_df, **self._params)
        assert final_result.elasticidade_preco is not None
        assert final_result.elasticidade_preco < 0

    def test_insufficient_observations_returns_null_elasticity(self):
        base_df = make_elastic_product(n=10)
        final_result = run_log_log_regression(base_df, **self._params)
        assert final_result.elasticidade_preco is None


class TestImputeElasticities:
    def test_tier_fallback_sets_medium_confidence(self):
        results = [
            make_valid_result("produto_1", -1.2, nivel_de_preco=3),
            make_valid_result("produto_2", -1.4, nivel_de_preco=3),
            make_null_result("produto_3", nivel_de_preco=3),
        ]
        final_df = impute_elasticities(results)
        row = final_df[final_df["product_id"] == "produto_3"].iloc[0]
        assert row["nivel_confianca"] == "medio"
        assert row["elasticidade_preco"] is not None

    def test_category_fallback_sets_low_confidence(self):
        results = [
            make_valid_result("produto_1", -1.5, nivel_de_preco=2),
            make_null_result("produto_2", nivel_de_preco=5),
        ]
        final_df = impute_elasticities(results)
        row = final_df[final_df["product_id"] == "produto_2"].iloc[0]
        assert row["nivel_confianca"] == "baixo"
        assert abs(row["elasticidade_preco"] - (-1.5)) < 0.01
