# Tickets Conversion

## Objective

This pipeline is a portfolio-safe version of a conversion-analysis flow that links service interactions to converted orders. It extracts generic interaction, customer, agent roster, and order history sources; matches interactions to customers; associates them with agents; and identifies transactions that happened after the interaction window.

This is a first version that is already being built and refined. The current implementation is meant to preserve the business logic and matching flow while removing sensitive warehouse names, private connectors, and company-specific identifiers.

## Architecture

The portfolio version follows the standard DAG structure:

- `main.py`: orchestration
- `core/domain.py`: deterministic matching and conversion logic
- `core/infrastructure.py`: generic SQL loading, extraction, and local persistence
- `core/schemas.py`: typed internal contracts

## Generic source model

The pipeline reads directly from generic SQL files:

- `sql/extract_interactions.sql`
- `sql/extract_customers.sql`
- `sql/extract_agent_roster.sql`
- `sql/extract_orders.sql`

Production-specific systems were intentionally replaced by generic placeholders such as `generic_data_warehouse_connection`.

## Business flow

1. Extract interaction history from a generic support platform source.
2. Extract customer reference data.
3. Match interactions to customers using email and phone rules.
4. Enrich interactions with agent roster information by month.
5. Extract order history.
6. Match orders to interactions using customer keys, agent keys, and a conversion date window.
7. Emit both service-event and purchase-event rows in one consolidated output.

## Generic naming contract

The portfolio pipeline uses generic names such as:

- `interaction_id`
- `customer_id`
- `service_channel`
- `interaction_type`
- `agent_id`
- `order_group_id`
- `order_value`
- `event_type`
- `event_timestamp`

## Local outputs

Artifacts are written under `artifacts/`:

- `tickets_conversion.csv`
- `tickets_conversion.schema.json`

## Run locally

```bash
python data_pipelines/tickets_conversion/main.py
```

Optional runtime parameters can be passed through:

```text
TICKETS_CONVERSION_PARAMS='{"is_incremental": true, "days_back": 30}'
```
