import json
import logging
import os
from pathlib import Path

from core import domain, infrastructure, schemas

LOGGER = logging.getLogger("tickets_conversion")
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PIPELINE_ROOT = Path(__file__).resolve().parent
ARTIFACTS_DIR = PIPELINE_ROOT / "artifacts"
SQL_PATH = PIPELINE_ROOT / "sql"
JSON_SETUP_PATH = PIPELINE_ROOT / "json" / "setup_tickets_conversion.json"

DEFAULT_PARAMETERS = {
    "is_incremental": True,
    "days_back": 30,
    "start_date": "2026-01-01",
    "end_date": "2026-01-31",
    "max_retry": 2,
    "retry_sleep_seconds": 1
}


def get_pipeline_paths() -> schemas.PipelinePaths:
    """
    Resolves the local paths used by the portfolio DAG.

    Returns:
        schemas.PipelinePaths: Structured local paths used by the portfolio pipeline.
    """

    return schemas.PipelinePaths(
        pipeline_root=PIPELINE_ROOT,
        artifacts_dir=ARTIFACTS_DIR,
        sql_path=SQL_PATH,
        setup_json_path=JSON_SETUP_PATH,
    )


def build_runtime_config() -> schemas.RuntimeConfig:
    """
    Builds the runtime configuration from environment parameters and defaults.

    Returns:
        schemas.RuntimeConfig: Runtime configuration merged from defaults and environment overrides.
    """

    env_value = os.getenv("TICKETS_CONVERSION_PARAMS", "").strip()
    loaded = json.loads(env_value) if env_value else {}
    params = {**DEFAULT_PARAMETERS, **loaded}
    return schemas.RuntimeConfig(
        is_incremental=bool(params["is_incremental"]),
        days_back=int(params["days_back"]),
        start_date=str(params["start_date"]),
        end_date=str(params["end_date"]),
        max_retry=int(params["max_retry"]),
        retry_sleep_seconds=int(params["retry_sleep_seconds"]),
    )


def extract_source_frames(paths: schemas.PipelinePaths, config: schemas.RuntimeConfig) -> schemas.ExtractedFrames:
    """
    Extracts all source dataframes required by the tickets conversion pipeline.

    Args:
        paths (schemas.PipelinePaths): Local pipeline paths containing the SQL directory.
        config (schemas.RuntimeConfig): Runtime configuration controlling extraction scope and retries.

    Returns:
        schemas.ExtractedFrames: Interactions, customers, agents, and orders dataframes.
    """

    frames = infrastructure.extract_source_frames(paths, config)
    if frames.interactions_df.empty:
        raise RuntimeError("Interaction extraction returned no rows.")
    if frames.customers_df.empty:
        raise RuntimeError("Customer extraction returned no rows.")
    if frames.agents_df.empty:
        raise RuntimeError("Agent roster extraction returned no rows.")
    if frames.orders_df.empty:
        raise RuntimeError("Order extraction returned no rows.")
    return frames


def run_pipeline_locally() -> str:
    """
    Runs the full portfolio pipeline locally and persists the final output.

    Returns:
        str: String path to the saved CSV output.
    """

    paths = get_pipeline_paths()
    config = build_runtime_config()
    LOGGER.info("Extracting source frames.")
    frames = extract_source_frames(paths, config)

    LOGGER.info("Matching interactions with customers.")
    interactions_df = domain.match_interactions_with_customers(frames.interactions_df, frames.customers_df)
    interactions_df = domain.add_interaction_duration_column(interactions_df)

    LOGGER.info("Enriching interactions with agent roster.")
    interactions_with_agents_df = domain.enrich_interactions_with_agents(interactions_df, frames.agents_df)
    interactions_for_orders_df = domain.clean_customer_id(
        interactions_with_agents_df.drop(
            ["agent_name", "team_name", "supervisor_name", "agent_email", "customer_key"],
            axis=1,
            errors="ignore",
        )
    )

    LOGGER.info("Matching orders with interactions.")
    orders_with_interactions_df = domain.enrich_orders_with_interactions(frames.orders_df, interactions_for_orders_df)
    orders_with_interactions_df = domain.add_immediate_conversion_flag(orders_with_interactions_df)

    LOGGER.info("Consolidating final output.")
    final_df = domain.consolidate_conversion_output(interactions_with_agents_df, orders_with_interactions_df)
    final_df = infrastructure.add_processed_at(final_df)

    LOGGER.info("Saving output.")
    return infrastructure.save_output(final_df, paths)


def main() -> None:
    """
    Local entrypoint for running the portfolio-safe pipeline.

    Returns:
        None: This function executes the full pipeline and logs the saved output path.
    """

    output_path = run_pipeline_locally()
    LOGGER.info("Pipeline finished. Output saved to %s.", output_path)


if __name__ == "__main__":
    main()
