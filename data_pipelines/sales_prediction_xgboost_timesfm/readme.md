# Sales Prediction XGBoost TimesFM

## Objective

This pipeline is a portfolio-safe version of a real sales prediction flow. It extracts sales, quotation, and stock history directly from generic SQL sources, builds TimesFM historical baselines, and trains XGBoost residual models that correct those baselines (In Category level the real code right now achieves 75% to 80% of precision - wmape between 25% and 20%).

This is a first version that is already being built and refined. The goal of the current implementation is to show the architecture, extraction pattern, and business logic split without exposing sensitive production details.

The implementation keeps the repo DAG structure while preserving the meaningful split between:

- `main.py`: orchestration
- `core/domain_timesfm.py`: TimesFM preparation and baseline logic
- `core/domain_xgboost.py`: residual feature engineering and model training
- `core/infrastructure.py`: SQL loading, generic extraction, model loading, and local persistence

## Generic source model

This portfolio version removes planning-file staging and sensitive source references.

The pipeline reads directly from generic source queries:

- `sql/extract_sales_history.sql`
- `sql/extract_quotations_history.sql`
- `sql/extract_stock_history.sql`

Production authentication and platform-specific details are intentionally abstracted behind placeholder connector functions such as `generic_data_warehouse_connection`.

## Business flow

1. Extract historical sales, quotations, and stock data.
2. Build TimesFM monthly share and daily GMV baselines.
3. Persist TimesFM predictions and residuals locally.
4. Build XGBoost feature bases from quotations and stock.
5. Train:
   - a category-level GMV residual model
   - a product-level share residual model
6. Persist corrected outputs, audits, metrics, and model artifacts locally.

## Generic naming contract

The portfolio pipeline uses generic English names:

- `date_ref`
- `product_id`
- `category_group`
- `category_name`
- `sales_channel`
- `gmv`
- `price`
- `leadtime`
- `freight`
- `stock_available`

## Local outputs

Artifacts are written under `artifacts/`:

- `timesfm_share_predictions.*`
- `timesfm_share_errors.*`
- `timesfm_gmv_predictions.*`
- `timesfm_gmv_errors.*`
- `xgboost_category_*`
- `xgboost_share_*`
- `corrected_share_output.*`
- `corrected_gmv_output.*`
- `sales_prediction_corrected_share.schema.json`

## Run locally

```bash
python data_pipelines/sales_prediction_xgboost_timesfm/main.py
```

Optional parameters can be passed with:

```text
SALES_PREDICTION_XGBOOST_TIMESFM_PARAMS='{"history_months": 24, "min_train_points": 3}'
```
