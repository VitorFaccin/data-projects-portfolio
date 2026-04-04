import json
import sys
from pathlib import Path

import pandas as pd
import pytest

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

import main
from core import infrastructure


def make_frames() -> main.schemas.ExtractedFrames:
    interactions_df = pd.DataFrame(
        {
            "interaction_id": ["1"],
            "created_at": ["2026-01-01 10:00:00"],
            "updated_at": ["2026-01-01 10:20:00"],
            "customer_key": ["1"],
            "customer_phone": ["41999999999"],
            "system_customer_email": ["a@test.com"],
            "contact_email": ["a@test.com"],
            "agent_email": ["agent@test.com"],
            "raw_service_channel": ["service_channel_phone"],
            "month_ref": [pd.Timestamp("2026-01-01")],
            "interaction_type": ["inbound"],
        }
    )
    customers_df = pd.DataFrame(
        {
            "customer_id": ["1"],
            "email": ["a@test.com"],
            "phone_primary": ["41999999999"],
            "phone_secondary": [None],
        }
    )
    agents_df = pd.DataFrame(
        {
            "agent_id": ["7"],
            "agent_name": ["alice"],
            "team_name": ["inside sales"],
            "supervisor_name": ["bob"],
            "agent_email": ["agent@test.com"],
            "month_ref": [pd.Timestamp("2026-01-01")],
        }
    )
    orders_df = pd.DataFrame(
        {
            "purchase_timestamp": pd.to_datetime(["2026-01-10 10:05:00"]),
            "order_agent_id": ["7"],
            "order_agent_name": ["alice"],
            "order_supervisor_name": ["bob"],
            "order_team_name": ["inside sales"],
            "approval_timestamp": pd.to_datetime(["2026-01-10 10:10:00"]),
            "order_group_id": [123],
            "customer_id": ["1"],
            "payment_type": ["card"],
            "order_status": ["approved"],
            "order_value": [100.0],
        }
    )
    return main.schemas.ExtractedFrames(
        interactions_df=interactions_df,
        customers_df=customers_df,
        agents_df=agents_df,
        orders_df=orders_df,
    )


def test_build_runtime_config_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("TICKETS_CONVERSION_PARAMS", json.dumps({"is_incremental": False, "start_date": "2026-01-01", "end_date": "2026-01-05"}))
    config = main.build_runtime_config()

    assert config.is_incremental is False
    assert config.start_date == "2026-01-01"


def test_extract_source_frames_raises_for_empty_interactions(monkeypatch) -> None:
    monkeypatch.setattr(
        infrastructure,
        "extract_source_frames",
        lambda paths, config: main.schemas.ExtractedFrames(
            interactions_df=pd.DataFrame(),
            customers_df=pd.DataFrame({"a": [1]}),
            agents_df=pd.DataFrame({"a": [1]}),
            orders_df=pd.DataFrame({"a": [1]}),
        ),
    )

    with pytest.raises(RuntimeError, match="Interaction extraction returned no rows"):
        main.extract_source_frames(main.get_pipeline_paths(), main.build_runtime_config())


def test_run_pipeline_locally_saves_output(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(main, "ARTIFACTS_DIR", tmp_path)
    monkeypatch.setattr(infrastructure, "extract_source_frames", lambda paths, config: make_frames())
    saved = {}
    monkeypatch.setattr(
        infrastructure,
        "save_output",
        lambda df, paths: saved.setdefault("path", str(paths.artifacts_dir / "tickets_conversion.csv")),
    )

    output_path = main.run_pipeline_locally()

    assert output_path.endswith("tickets_conversion.csv")
    assert "path" in saved


def test_extract_interactions_sql_uses_generic_sources() -> None:
    sql_text = (PIPELINE_ROOT / "sql" / "extract_interactions.sql").read_text(encoding="utf-8")

    assert "generic.support_interactions" in sql_text
    assert "GROUP BY\n        i.interaction_id" in sql_text
    assert "GROUP BY\n    1," not in sql_text
