# Elasticity Regression Log-Log with Free Olist Tables

## Objective

`dataset link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce`

This pipeline recreates the business idea of `elasticity_calculation` in a portfolio-friendly format using public Olist marketplace tables instead of internal company sources. It estimates monthly product elasticity with a log-log regression and produces a table that pricing or commercial teams can use to simulate the impact of price changes on demand.

The implementation keeps the same DDD split used in the company version:

- `main.py`: orchestration and TaskFlow DAG definition
- `core/domain.py`: business logic and regression logic
- `core/schemas.py`: typed internal contracts
- `core/infrastructure.py`: I/O, DuckDB SQL extraction, and local persistence

## Main adaptation from the company DAG

The original DAG uses internal sales and quote tables plus a DAG factory. This version is a real Airflow TaskFlow DAG with `@dag` and `@task`, and it uses free CSVs under `olist_free_tables/`.

The extraction layer explicitly mimics warehouse SQL:

- DuckDB reads the CSVs
- the first extraction lives in `sql/extract_daily_sales_pricing.sql`
- the rest of the pipeline follows normal pandas domain code

This keeps the portfolio project close to a real production pattern without pretending the local CSVs are a real warehouse.

## Free tables used

This version only uses the files that are needed to rebuild the logic:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`
- `product_category_name_translation.csv`
- `olist_customers_dataset.csv`

The other available Olist files remain in the folder for context, but they are not required for this regression pipeline.

## Business mapping

The original pipeline models elasticity by product and `emp_venda`. Olist does not have an equivalent `empresa_venda`, so this version replaces it with `sale_location`, represented by the customer state.

The resulting grain is:

- `product_id`
- `sale_location`

The grouping dimensions used for seasonality and fallback are:

- `product_category`
- `sale_location`
- `nivel_de_preco`

## Model

The model follows the same log-log idea:

```text
ln(quantidade_ajustada) = beta0 + beta1*ln(preco) + beta2*ln(prazo) + beta3*is_spike_day + erro
```

- `beta1`: price elasticity
- `beta2`: delivery-time elasticity

The regression is only accepted when:

- the product-location pair has at least `30` valid observations
- the price coefficient p-value is `<= 0.25`
- `R² >= 0.05`
- price elasticity is negative

## Seasonal adjustment

Sales are deseasonalized by category-month before the regression. Spike dates are excluded from the seasonality index so commercial peaks are not mixed with normal demand behavior.

## Spike dates

The pipeline flags:

- double dates such as `01/01`, `02/02`, ..., `12/12`
- second Sunday of May
- second Sunday of August
- December 24 and 25
- November is excluded from the extraction because of major promotional distortion

## Price tier and confidence fallback

`nivel_de_preco` is still part of the output because it is useful when a product does not have enough history to support its own regression.

Price tiers are built by quintiles inside each `sale_location` and `product_category`:

- `1`: lowest average-price range
- `5`: highest average-price range

Confidence fallback works like this:

1. `alto`: valid regression from the product itself
2. `medio`: category + price-tier average
3. `baixo`: category average

This is important in portfolio and real scenarios because many products do not have enough price variation or enough daily observations to support a stable own-product elasticity.

## Delivery-time feature

The original company DAG uses quote lead time from an internal quotation source. The free Olist dataset does not have that source, so this version approximates `prazo` with the number of days between purchase timestamp and estimated delivery date. The README and code treat this as a proxy feature rather than a perfect replacement.

## Historical window

Because the free Olist data is historical and static, the pipeline does not use the machine current date as the analysis anchor. Instead, it uses the latest `order_purchase_timestamp` available in the CSVs and rolls back `24` months from that point.

## Local output

This project is not connected to a real warehouse, so the load step mimics a serving-table write by saving the final table to:

- `artifacts/pricing_elasticity_regression_loglog.csv`
- `artifacts/pricing_elasticity_regression_loglog.schema.json`

## Running locally

```bash
python data_pipelines/elasticity_regression_loglog/main.py
```

Optional runtime parameters can be passed through the environment variable:

```text
ELASTICITY_REGRESSION_LOGLOG_PARAMS='{"min_observations": 20, "price_variation_threshold": 0.03}'
```

## Output table

| Column | Type | Description |
|---|---|---|
| `product_id` | string | Olist product identifier |
| `sale_location` | string | Customer state used as sale location |
| `product_category` | string | Product category translated to English when available |
| `nivel_de_preco` | int | Price tier from 1 to 5 within location and category |
| `elasticidade_preco` | float | Price elasticity coefficient |
| `elasticidade_prazo` | float | Delivery-time elasticity coefficient |
| `qtde_observacoes` | int | Number of daily observations used for the regression |
| `nivel_confianca` | string | `alto`, `medio`, or `baixo` |
| `data_calculo` | string | Execution date |
