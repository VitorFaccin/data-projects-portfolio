from __future__ import annotations

import pandas as pd


def match_interactions_with_customers_by_key(
    customers_df: pd.DataFrame,
    interactions_df: pd.DataFrame,
    left_key: str,
    right_key: str,
    id_column: str = "customer_id",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Matches interactions with customers using the specified key pair and separates matched and unmatched rows.

    Args:
        customers_df (pd.DataFrame): Customer reference dataframe containing the customer identifier.
        interactions_df (pd.DataFrame): Interaction dataframe that should be matched to customers.
        left_key (str): Column name in `interactions_df` used for matching.
        right_key (str): Column name in `customers_df` used for matching.
        id_column (str): Customer identifier column in the customer reference dataframe.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Matched interactions with `customer_id` and the remaining unmatched interactions.
    """

    lookup_map = (
        customers_df[[right_key, id_column]]
        .dropna(subset=[right_key, id_column])
        .assign(**{right_key: lambda frame: frame[right_key].astype(str).str.lower()})
        .drop_duplicates(subset=[right_key], keep="first")
        .set_index(right_key)[id_column]
    )
    enriched = interactions_df.copy()
    enriched["matched_customer_id"] = enriched[left_key].astype(str).str.lower().map(lookup_map)

    matched = (
        enriched[enriched["matched_customer_id"].notna()]
        .rename(columns={"matched_customer_id": "customer_id"})
        .reset_index(drop=True)
    )
    unmatched = (
        enriched[enriched["matched_customer_id"].isna()]
        .drop(columns=["matched_customer_id"])
        .reset_index(drop=True)
    )
    return matched, unmatched


def match_interactions_with_customers(interactions_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Matches interactions to customers using email and phone rules in priority order.

    Args:
        interactions_df (pd.DataFrame): Interaction dataframe before customer enrichment.
        customers_df (pd.DataFrame): Customer reference dataframe.

    Returns:
        pd.DataFrame: Interaction dataframe enriched with matched customer identifiers when available.
    """

    join_rules = [
        {"left_on": "contact_email", "right_on": "email"},
        {"left_on": "system_customer_email", "right_on": "email"},
        {"left_on": "customer_phone", "right_on": "phone_primary"},
        {"left_on": "customer_phone", "right_on": "phone_secondary"},
    ]
    remaining = interactions_df.copy()
    matched_frames: list[pd.DataFrame] = []
    for rule in join_rules:
        matched, remaining = match_interactions_with_customers_by_key(
            customers_df=customers_df,
            interactions_df=remaining,
            left_key=rule["left_on"],
            right_key=rule["right_on"],
        )
        matched_frames.append(matched)
    if matched_frames:
        matched_df = pd.concat(matched_frames, ignore_index=True)
        return pd.concat([matched_df, remaining], ignore_index=True)
    return remaining


def add_interaction_duration_column(
    interactions_df: pd.DataFrame,
    start_col: str = "created_at",
    end_col: str = "updated_at",
    new_col_name: str = "interaction_duration_minutes",
) -> pd.DataFrame:
    """
    Adds a duration column in minutes calculated from the interaction start and end timestamps.

    Args:
        interactions_df (pd.DataFrame): Interaction dataframe containing the timestamp columns.
        start_col (str): Start timestamp column.
        end_col (str): End timestamp column.
        new_col_name (str): Name of the duration column to be created.

    Returns:
        pd.DataFrame: Interaction dataframe with the duration column added.
    """

    final_df = interactions_df.copy()
    for column in [start_col, end_col]:
        final_df[column] = pd.to_datetime(final_df[column], errors="coerce").dt.tz_localize(None)
    final_df[new_col_name] = (final_df[end_col] - final_df[start_col]).dt.total_seconds() / 60
    return final_df


def enrich_interactions_with_agents(interactions_df: pd.DataFrame, agents_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches interactions with the monthly agent roster using agent email and month reference.

    Args:
        interactions_df (pd.DataFrame): Interaction dataframe that contains `agent_email` and `month_ref`.
        agents_df (pd.DataFrame): Agent roster dataframe.

    Returns:
        pd.DataFrame: Interaction dataframe enriched with agent metadata.
    """

    return interactions_df.merge(agents_df, how="left", on=["agent_email", "month_ref"])


def clean_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the matched customer identifier by removing trailing decimal artifacts and whitespace.

    Args:
        df (pd.DataFrame): Dataframe containing the `customer_id` column.

    Returns:
        pd.DataFrame: Dataframe with a normalized `customer_id`.
    """

    final_df = df.copy()
    final_df["customer_id"] = (
        final_df["customer_id"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )
    return final_df


def merge_with_id_and_date_window(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_equal_id: list[str],
    right_equal_id: list[str],
    left_date_col: str,
    right_date_col: str,
) -> pd.DataFrame:
    """
    Merges two dataframes by keys and keeps only rows where the right-side event is within a valid conversion window.

    Args:
        left_df (pd.DataFrame): Left dataframe to merge.
        right_df (pd.DataFrame): Right dataframe to merge.
        left_equal_id (list[str]): Join key columns from the left dataframe.
        right_equal_id (list[str]): Join key columns from the right dataframe.
        left_date_col (str): Event timestamp column from the left dataframe.
        right_date_col (str): Event timestamp column from the right dataframe.

    Returns:
        pd.DataFrame: Filtered merged dataframe with one best match per group.
    """

    final_left = left_df.copy()
    final_right = right_df.copy()
    final_left[left_equal_id[0]] = final_left[left_equal_id[0]].astype(str)
    final_right[right_equal_id[0]] = final_right[right_equal_id[0]].astype(str)

    merged_df = pd.merge(
        final_left,
        final_right,
        left_on=left_equal_id,
        right_on=right_equal_id,
        how="left",
    ).reset_index(drop=True)
    merged_df = merged_df[~merged_df[right_date_col].isna()].copy()
    merged_df["thirty_days_later"] = merged_df[right_date_col] + pd.Timedelta(days=30)

    filtered_df = merged_df[
        (merged_df["thirty_days_later"].dt.date >= merged_df[left_date_col].dt.date)
        & (merged_df[right_date_col].dt.date <= merged_df[left_date_col].dt.date)
    ].copy()
    filtered_df.drop("thirty_days_later", axis=1, inplace=True)

    group_cols = left_equal_id.copy()
    group_cols.extend(["order_group_id", left_date_col])
    return (
        filtered_df
        .sort_values(by=[right_date_col, "order_status"], ascending=[True, True])
        .groupby(group_cols, as_index=False)
        .first()
    )


def get_unmatched_orders(matched_orders_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns orders that were not used in the first conversion match and adds empty agent fields.

    Args:
        matched_orders_df (pd.DataFrame): Orders already matched to interactions.
        orders_df (pd.DataFrame): Full orders dataframe.

    Returns:
        pd.DataFrame: Remaining orders with empty agent metadata placeholders.
    """

    used_ids = matched_orders_df["order_group_id"].dropna().unique().tolist()
    orders_no_match = orders_df[~orders_df["order_group_id"].isin(used_ids)].copy()
    orders_no_match["order_agent_id"] = None
    orders_no_match["order_agent_name"] = None
    orders_no_match["order_supervisor_name"] = None
    orders_no_match["order_team_name"] = None
    return orders_no_match


def enrich_orders_with_interactions(orders_df: pd.DataFrame, interactions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches orders with matched interactions using a two-step key strategy.

    Args:
        orders_df (pd.DataFrame): Orders dataframe.
        interactions_df (pd.DataFrame): Interaction dataframe enriched with customer and agent information.

    Returns:
        pd.DataFrame: Orders dataframe enriched with interaction linkage.
    """

    first_merge = merge_with_id_and_date_window(
        left_df=orders_df,
        right_df=interactions_df,
        left_equal_id=["customer_id", "order_agent_id"],
        right_equal_id=["customer_id", "agent_id"],
        left_date_col="purchase_timestamp",
        right_date_col="created_at",
    )
    first_merge.drop("agent_id_y", axis=1, inplace=True, errors="ignore")
    orders_no_match = get_unmatched_orders(first_merge, orders_df)

    second_merge = merge_with_id_and_date_window(
        left_df=orders_no_match,
        right_df=interactions_df,
        left_equal_id=["customer_id"],
        right_equal_id=["customer_id"],
        left_date_col="purchase_timestamp",
        right_date_col="created_at",
    )
    second_merge.drop("agent_id_y", axis=1, inplace=True, errors="ignore")
    return pd.concat([first_merge, second_merge], ignore_index=True)


def add_immediate_conversion_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the immediate conversion flag and removes columns not needed after order matching.

    Args:
        df (pd.DataFrame): Orders dataframe enriched with interaction fields.

    Returns:
        pd.DataFrame: Orders dataframe with the immediate conversion flag and reduced columns.
    """

    final_df = df.copy()
    final_df["immediate_conversion_flag"] = (
        (final_df["purchase_timestamp"] >= final_df["created_at"])
        & (final_df["purchase_timestamp"] <= final_df["updated_at"])
    ).astype(int)
    drop_cols = [
        "contact_email",
        "created_at",
        "updated_at",
        "raw_service_channel",
        "system_customer_email",
        "customer_phone",
        "month_ref",
        "interaction_duration_minutes",
        "interaction_type",
    ]
    final_df.drop(drop_cols, axis=1, inplace=True, errors="ignore")
    return final_df


def merge_and_unpivot_interactions_with_orders(
    interactions_with_agents_df: pd.DataFrame,
    orders_with_interactions_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merges interactions and orders, then unpivots interaction and purchase timestamps into event rows.

    Args:
        interactions_with_agents_df (pd.DataFrame): Interactions enriched with agent data.
        orders_with_interactions_df (pd.DataFrame): Orders enriched with interaction matches.

    Returns:
        pd.DataFrame: Event-level dataframe containing both interaction and purchase timestamps.
    """

    merged_df = pd.merge(
        interactions_with_agents_df,
        orders_with_interactions_df,
        how="left",
        on="interaction_id",
    ).reset_index(drop=True)
    pivot_cols = ["created_at", "purchase_timestamp"]
    id_vars = [column for column in merged_df.columns if column not in pivot_cols]
    final_df = pd.melt(
        merged_df,
        id_vars=id_vars,
        value_vars=pivot_cols,
        var_name="event_type",
        value_name="event_timestamp",
    )
    return final_df[final_df["event_timestamp"].notna()].copy()


def consolidate_conversion_output(
    interactions_with_agents_df: pd.DataFrame,
    orders_with_interactions_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Consolidates the final event output and renames columns to the public portfolio contract.

    Args:
        interactions_with_agents_df (pd.DataFrame): Interactions enriched with agent metadata.
        orders_with_interactions_df (pd.DataFrame): Orders enriched with interaction matches.

    Returns:
        pd.DataFrame: Consolidated conversion output ready for persistence.
    """

    final_df = merge_and_unpivot_interactions_with_orders(interactions_with_agents_df, orders_with_interactions_df)
    if "customer_id_x" in final_df.columns or "customer_id_y" in final_df.columns:
        final_df["customer_id"] = final_df.get("customer_id_x", final_df.get("customer_id_y"))
        if "customer_id_x" in final_df.columns and "customer_id_y" in final_df.columns:
            final_df["customer_id"] = final_df["customer_id_x"].fillna(final_df["customer_id_y"])
        final_df.drop(["customer_id_x", "customer_id_y"], axis=1, inplace=True, errors="ignore")

    service_agent_cols = ["agent_name", "supervisor_name", "team_name", "agent_id"]
    order_agent_cols = ["order_agent_name", "order_supervisor_name", "order_team_name", "order_agent_id"]

    for column in ["order_agent_name", "order_supervisor_name", "order_team_name"]:
        if column in final_df.columns:
            final_df[column] = final_df[column].astype(str).str.title()
    mask_has_order = final_df["order_group_id"].notnull()
    final_df.loc[mask_has_order, service_agent_cols] = final_df.loc[mask_has_order, order_agent_cols].values
    final_df.drop(order_agent_cols, axis=1, inplace=True, errors="ignore")

    clear_columns = [
        "order_group_id",
        "approval_timestamp",
        "payment_type",
        "order_status",
        "order_value",
        "immediate_conversion_flag",
    ]
    final_df.loc[final_df["event_type"] == "created_at", clear_columns] = None
    final_df["event_type"] = final_df["event_type"].map({
        "created_at": "interaction_timestamp",
        "purchase_timestamp": "purchase_timestamp",
    })
    final_df["service_channel"] = final_df["raw_service_channel"].map({
        "service_channel_whatsapp": "whatsapp",
        "service_channel_phone": "phone",
    }).fillna("chat")

    final_df.drop(["contact_email", "customer_phone", "month_ref"], axis=1, inplace=True, errors="ignore")
    for date_col in ["updated_at", "event_timestamp", "approval_timestamp"]:
        if date_col in final_df.columns:
            final_df[date_col] = pd.to_datetime(final_df[date_col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    final_df = final_df.rename(
        columns={
            "updated_at": "interaction_updated_at",
        }
    )
    return final_df
