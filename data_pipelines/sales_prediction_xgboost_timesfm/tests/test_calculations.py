import datetime as dt
import sys
from pathlib import Path

import numpy as np
import polars as pl

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from core import domain_timesfm, domain_xgboost


class FakeTimesFm:
    def forecast(self, horizon: int, inputs: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
        preds = []
        for series in inputs:
            preds.append([float(series[-1])] * horizon)
        return np.asarray(preds, dtype=float), np.empty((len(inputs), horizon, 0))


def make_sales_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date_ref": [
                dt.date(2026, 1, 1),
                dt.date(2026, 1, 2),
                dt.date(2026, 2, 1),
                dt.date(2026, 2, 2),
            ],
            "product_id": [1, 2, 1, 2],
            "category_group": ["home", "home", "home", "home"],
            "category_name": ["chairs", "chairs", "chairs", "chairs"],
            "sales_channel": ["digital", "digital", "digital", "digital"],
            "gmv": [100.0, 50.0, 120.0, 80.0],
        }
    )


def test_build_monthly_share_base_generates_product_shares() -> None:
    base = domain_timesfm.build_monthly_share_base(domain_timesfm.normalize_sales_dataframe(make_sales_df()))

    assert base.filter(pl.col("month_ref") == dt.date(2026, 1, 1)).get_column("real_share").to_list() == [2.0 / 3.0, 1.0 / 3.0]


def test_build_daily_gmv_base_aggregates_category_gmv() -> None:
    base = domain_timesfm.build_daily_gmv_base(domain_timesfm.normalize_sales_dataframe(make_sales_df()))

    assert base.height == 4
    assert "real_gmv" in base.columns


def test_run_rolling_timesfm_baseline_repeats_last_value_with_fake_model() -> None:
    df = pl.DataFrame(
        {
            "month_ref": [dt.date(2026, 1, 1), dt.date(2026, 2, 1), dt.date(2026, 3, 1)],
            "category_name": ["chairs"] * 3,
            "sales_channel": ["digital"] * 3,
            "product_id": [1] * 3,
            "real_share": [0.2, 0.3, 0.5],
        }
    )
    pred, stats = domain_timesfm.run_rolling_timesfm_baseline(
        df=df,
        spec=domain_timesfm.get_share_forecast_spec(),
        model=FakeTimesFm(),
        min_train_points=1,
        max_history=0,
        forecast_batch_size=16,
        forecast_horizon=1,
        step_size=1,
        result_chunk_size=100,
    )

    assert pred.get_column("timesfm_share_pred").to_list() == [0.2, 0.3]
    assert stats.model_calls == 1


def test_join_adjusted_features_falls_back_to_product_level_average() -> None:
    target = pl.DataFrame(
        {
            "month_ref": [dt.date(2026, 2, 1)],
            "category_name": ["chairs"],
            "sales_channel": ["marketplace"],
            "product_id": [1],
        }
    )
    quotations = pl.DataFrame(
        {
            "date_ref": [dt.date(2026, 2, 10), dt.date(2026, 2, 10)],
            "category_name": ["chairs", "chairs"],
            "sales_channel": ["digital", "partners"],
            "product_id": [1, 1],
            "price": [200.0, 100.0],
            "freight": [10.0, 20.0],
            "leadtime": [5.0, 7.0],
        }
    )

    joined = domain_xgboost.join_adjusted_features(
        left_df=target,
        quotations=quotations,
        time_col="month_ref",
        debug_label="test_product_fallback",
    )

    assert joined.get_column("price").to_list() == [150.0]
    assert joined.get_column("freight").to_list() == [15.0]
    assert joined.get_column("leadtime").to_list() == [6.0]


def test_build_product_xgboost_base_keeps_generic_kpis() -> None:
    sales_raw = pl.DataFrame(
        {
            "date_ref": [
                dt.date(2026, 1, 5),
                dt.date(2026, 1, 6),
                dt.date(2026, 1, 7),
                dt.date(2026, 2, 5),
                dt.date(2026, 2, 6),
                dt.date(2026, 2, 7),
            ],
            "product_id": [1, 1, 1, 1, 1, 1],
            "category_group": ["home"] * 6,
            "category_name": ["chairs"] * 6,
            "sales_channel": ["digital"] * 6,
            "gmv": [100.0, 120.0, 110.0, 130.0, 140.0, 150.0],
        }
    )
    quotations_raw = pl.DataFrame(
        {
            "date_ref": [dt.date(2026, 2, 10), dt.date(2026, 2, 11)],
            "product_id": [1, 1],
            "sales_channel": ["digital", "digital"],
            "price": [200.0, 210.0],
            "leadtime": [5.0, 5.0],
            "freight": [0.0, 0.0],
        }
    )
    stock_raw = pl.DataFrame(
        {
            "date_ref": [dt.date(2026, 2, 10)],
            "product_id": [1],
            "stock_available": [3.0],
        }
    )

    daily_base, monthly_base = domain_xgboost.build_product_xgboost_base(
        sales_raw=sales_raw,
        quotations_raw=quotations_raw,
        stock_raw=stock_raw,
        feature_window_months=2,
    )

    assert daily_base.height == 1
    assert monthly_base.height == 1
    assert "price" in monthly_base.columns
    assert "z_price" in monthly_base.columns


def test_build_share_xgboost_features_keeps_generic_monthly_kpis() -> None:
    monthly_base = pl.DataFrame(
        {
            "month_ref": [dt.date(2026, 1, 1)],
            "category_name": ["chairs"],
            "sales_channel": ["digital"],
            "product_id": [1],
            "price": [100.0],
            "freight": [10.0],
            "leadtime": [5.0],
            "z_price": [1.2],
            "z_freight": [0.5],
            "z_leadtime": [-0.3],
            "best_price_moment": [0.0],
            "best_freight_moment": [1.0],
            "best_leadtime_moment": [0.0],
            "stockout_rate": [0.2],
        }
    )
    share_errors = pl.DataFrame(
        {
            "month_ref": [dt.date(2026, 1, 1)],
            "category_name": ["chairs"],
            "sales_channel": ["digital"],
            "product_id": [1],
            "real_share": [0.4],
            "timesfm_share_pred": [0.35],
            "share_error": [0.05],
        }
    )

    features = domain_xgboost.build_share_xgboost_features(
        monthly_base=monthly_base,
        share_errors=share_errors,
        share_history_months=0,
    )

    assert features.get_column("price").to_list() == [100.0]
    assert features.get_column("leadtime").to_list() == [5.0]
