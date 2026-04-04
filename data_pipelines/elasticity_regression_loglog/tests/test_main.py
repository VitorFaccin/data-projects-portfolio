import json
import sys
import types
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest


PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))


if "duckdb" not in sys.modules:
    duckdb_stub = types.ModuleType("duckdb")
    duckdb_stub.connect = lambda database=":memory:": None
    sys.modules["duckdb"] = duckdb_stub


from core.schemas import AnalysisWindow  # noqa: E402
from main import calculate_elasticity_frame, extract_master_dataset, get_pipeline_paths  # noqa: E402


def make_master_df() -> pd.DataFrame:
    final_df = pd.DataFrame(
        {
            "data_date": ["2018-08-01", "2018-08-02", "2018-08-03", "2018-08-04"] * 10,
            "product_id": ["produto_1"] * 20 + ["produto_2"] * 20,
            "sale_location": ["SP"] * 40,
            "product_category": ["bed_bath_table"] * 40,
            "quantidade_produto": [1, 2, 1, 3] * 10,
            "preco": [100, 95, 105, 98] * 10,
            "prazo": [5, 6, 5, 7] * 10,
        }
    )
    return final_df


class TestPaths:
    def test_pipeline_paths_exist_for_required_inputs(self):
        paths = get_pipeline_paths()
        assert paths.orders_csv_path.name == "olist_orders_dataset.csv"
        assert paths.order_items_csv_path.name == "olist_order_items_dataset.csv"


class TestExtractMasterDataset:
    @patch("main.extract_master_data")
    @patch("main.get_analysis_window")
    def test_extract_master_dataset_filters_low_variation(self, mock_window, mock_extract):
        mock_window.return_value = AnalysisWindow(start_date="2017-01-01", end_date="2018-10-17")
        mock_extract.return_value = pd.DataFrame(
            {
                "data_date": ["2018-08-01"] * 6,
                "product_id": ["a", "a", "a", "b", "b", "b"],
                "sale_location": ["SP", "SP", "SP", "RJ", "RJ", "RJ"],
                "product_category": ["bed_bath_table"] * 6,
                "quantidade_produto": [1, 1, 1, 1, 1, 1],
                "preco": [100.0, 100.0, 100.0, 90.0, 110.0, 130.0],
                "prazo": [5.0, 5.0, 5.0, 4.0, 5.0, 6.0],
            }
        )

        final_df = extract_master_dataset(
            get_pipeline_paths(),
            {
                "months_back": 24,
                "price_variation_threshold": 0.02,
            },
        )

        assert "a" not in final_df["product_id"].values
        assert "b" in final_df["product_id"].values

    @patch("main.extract_master_data", return_value=pd.DataFrame())
    @patch(
        "main.get_analysis_window",
        return_value=AnalysisWindow(start_date="2017-01-01", end_date="2018-10-17"),
    )
    def test_extract_master_dataset_raises_for_empty_extract(self, mock_window, mock_extract):
        with pytest.raises(RuntimeError, match="DuckDB extraction returned no rows"):
            extract_master_dataset(
                get_pipeline_paths(),
                {
                    "months_back": 24,
                    "price_variation_threshold": 0.02,
                },
            )


class TestCalculateElasticityFrame:
    def test_calculate_elasticity_frame_returns_expected_columns(self):
        master_df = make_master_df()
        final_df = calculate_elasticity_frame(
            master_df,
            {
                "min_observations": 5,
                "max_p_value": 1.0,
                "min_r_squared": -1.0,
            },
        )

        for column in [
            "product_id",
            "sale_location",
            "product_category",
            "nivel_de_preco",
            "elasticidade_preco",
            "elasticidade_prazo",
            "qtde_observacoes",
            "nivel_confianca",
            "data_calculo",
        ]:
            assert column in final_df.columns


class TestSetupJson:
    def test_numeric_columns_use_none_max_length(self):
        setup_path = PIPELINE_ROOT / "json" / "setup_elasticity_regression_loglog.json"
        with setup_path.open("r", encoding="utf-8") as file_pointer:
            metadata = json.load(file_pointer)

        for column, config in metadata["column_types"].items():
            if config["column_type"] in {"int", "float"}:
                assert config["max_length"] == "None", column
