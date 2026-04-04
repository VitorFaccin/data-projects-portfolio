import sys
from pathlib import Path

import pandas as pd

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from core import domain


def test_match_interactions_with_customers_by_key_matches_email() -> None:
    customers_df = pd.DataFrame({"customer_id": ["10"], "email": ["a@test.com"]})
    interactions_df = pd.DataFrame({"contact_email": ["a@test.com"], "interaction_id": ["1"]})

    matched, unmatched = domain.match_interactions_with_customers_by_key(
        customers_df=customers_df,
        interactions_df=interactions_df,
        left_key="contact_email",
        right_key="email",
    )

    assert matched["customer_id"].tolist() == ["10"]
    assert unmatched.empty


def test_add_interaction_duration_column_creates_minutes() -> None:
    interactions_df = pd.DataFrame(
        {
            "created_at": ["2026-01-01 10:00:00"],
            "updated_at": ["2026-01-01 10:30:00"],
        }
    )

    final_df = domain.add_interaction_duration_column(interactions_df)

    assert final_df["interaction_duration_minutes"].tolist() == [30.0]


def test_merge_with_id_and_date_window_keeps_order_within_thirty_days() -> None:
    orders_df = pd.DataFrame(
        {
            "customer_id": ["1"],
            "order_agent_id": ["7"],
            "purchase_timestamp": pd.to_datetime(["2026-02-10"]),
            "order_group_id": [100],
            "order_status": ["approved"],
        }
    )
    interactions_df = pd.DataFrame(
        {
            "customer_id": ["1"],
            "agent_id": ["7"],
            "created_at": pd.to_datetime(["2026-02-01"]),
            "interaction_id": ["abc"],
        }
    )

    final_df = domain.merge_with_id_and_date_window(
        left_df=orders_df,
        right_df=interactions_df,
        left_equal_id=["customer_id", "order_agent_id"],
        right_equal_id=["customer_id", "agent_id"],
        left_date_col="purchase_timestamp",
        right_date_col="created_at",
    )

    assert final_df["interaction_id"].tolist() == ["abc"]


def test_add_immediate_conversion_flag_marks_orders_inside_interaction_window() -> None:
    input_df = pd.DataFrame(
        {
            "purchase_timestamp": pd.to_datetime(["2026-02-01 10:10:00"]),
            "created_at": pd.to_datetime(["2026-02-01 10:00:00"]),
            "updated_at": pd.to_datetime(["2026-02-01 10:30:00"]),
        }
    )

    final_df = domain.add_immediate_conversion_flag(input_df)

    assert final_df["immediate_conversion_flag"].tolist() == [1]
