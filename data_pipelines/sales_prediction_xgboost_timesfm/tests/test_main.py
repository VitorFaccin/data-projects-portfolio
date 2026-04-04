import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

import main
from core import infrastructure


class FakeTimesFm:
    def forecast(self, horizon: int, inputs: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
        preds = []
        for series in inputs:
            preds.append([float(series[-1])] * horizon)
        return np.asarray(preds, dtype=float), np.empty((len(inputs), horizon, 0))


class FakeRegressor:
    def fit(self, X, y) -> None:
        self.mean_value = float(np.mean(y))

    def predict(self, X):
        return np.full(X.height, self.mean_value, dtype=float)


def make_extracts() -> main.schemas.HistoricalExtracts:
    sales = pl.DataFrame(
        {
            "date_ref": [
                "2026-01-01",
                "2026-01-02",
                "2026-02-01",
                "2026-02-02",
                "2026-03-01",
                "2026-03-02",
            ],
            "product_id": [1, 2, 1, 2, 1, 2],
            "category_group": ["home"] * 6,
            "category_name": ["chairs"] * 6,
            "sales_channel": ["digital"] * 6,
            "gmv": [100.0, 50.0, 120.0, 80.0, 150.0, 90.0],
        }
    ).with_columns(pl.col("date_ref").str.strptime(pl.Date, "%Y-%m-%d"))
    quotations = pl.DataFrame(
        {
            "date_ref": ["2026-02-10", "2026-03-10", "2026-03-11"],
            "product_id": [1, 1, 2],
            "sales_channel": ["digital", "digital", "digital"],
            "price": [100.0, 110.0, 120.0],
            "leadtime": [5.0, 4.0, 6.0],
            "freight": [10.0, 12.0, 11.0],
        }
    ).with_columns(pl.col("date_ref").str.strptime(pl.Date, "%Y-%m-%d"))
    stock = pl.DataFrame(
        {
            "date_ref": ["2026-02-10", "2026-03-10", "2026-03-11"],
            "product_id": [1, 1, 2],
            "stock_available": [3.0, 2.0, 0.0],
        }
    ).with_columns(pl.col("date_ref").str.strptime(pl.Date, "%Y-%m-%d"))
    return main.schemas.HistoricalExtracts(sales=sales, quotations=quotations, stock=stock)


def test_build_runtime_config_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("SALES_PREDICTION_XGBOOST_TIMESFM_PARAMS", json.dumps({"history_months": 12, "product_ids": [1, 2]}))
    config = main.build_runtime_config()

    assert config.history_months == 12
    assert config.product_ids == (1, 2)


def test_extract_historical_inputs_raises_for_empty_sales(monkeypatch) -> None:
    monkeypatch.setattr(
        infrastructure,
        "extract_historical_inputs",
        lambda paths, config: main.schemas.HistoricalExtracts(
            sales=pl.DataFrame(),
            quotations=pl.DataFrame({"a": [1]}),
            stock=pl.DataFrame({"a": [1]}),
        ),
    )

    with pytest.raises(RuntimeError, match="Sales extraction returned no rows"):
        main.extract_historical_inputs(main.get_pipeline_paths(), main.build_runtime_config())


def test_run_pipeline_locally_persists_expected_artifacts(monkeypatch, tmp_path) -> None:
    extracts = make_extracts()
    recorded_csv_paths: list[str] = []
    recorded_pickle_paths: list[str] = []

    monkeypatch.setattr(main, "ARTIFACTS_DIR", tmp_path)
    monkeypatch.setattr(
        infrastructure,
        "extract_historical_inputs",
        lambda paths, config: extracts,
    )
    monkeypatch.setattr(
        infrastructure,
        "load_timesfm_model",
        lambda **kwargs: FakeTimesFm(),
    )
    monkeypatch.setattr(
        infrastructure,
        "load_xgboost_regressor",
        lambda params: FakeRegressor(),
    )
    monkeypatch.setattr(
        infrastructure,
        "write_csv",
        lambda df, path: recorded_csv_paths.append(path.name),
    )
    monkeypatch.setattr(
        infrastructure,
        "write_parquet",
        lambda df, path: None,
    )
    monkeypatch.setattr(
        infrastructure,
        "write_pickle",
        lambda obj, path: recorded_pickle_paths.append(path.name),
    )
    monkeypatch.setattr(
        infrastructure,
        "save_portfolio_output",
        lambda df, paths, file_stem: str(paths.artifacts_dir / f"{file_stem}.csv"),
    )

    outputs = main.run_pipeline_locally()

    assert outputs["timesfm_outputs"]["share_predictions"].height > 0
    assert "timesfm_share_predictions.csv" in recorded_csv_paths
    assert "timesfm_gmv_predictions.csv" in recorded_csv_paths
    assert "xgboost_share_predictions.csv" in recorded_csv_paths
    assert "xgboost_category_predictions.csv" in recorded_csv_paths
    assert "xgboost_share_model.pkl" in recorded_pickle_paths
    assert "xgboost_category_model.pkl" in recorded_pickle_paths


def test_extract_sales_sql_uses_explicit_group_by_names() -> None:
    sql_text = (PIPELINE_ROOT / "sql" / "extract_sales_history.sql").read_text(encoding="utf-8")

    assert "GROUP BY\n    CAST(s.order_date AS DATE)," in sql_text
    assert "GROUP BY\n    1," not in sql_text
    assert "ORDER BY" not in sql_text
