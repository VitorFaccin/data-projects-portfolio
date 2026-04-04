from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PipelinePaths:
    pipeline_root: Path
    artifacts_dir: Path
    sql_path: Path
    setup_json_path: Path


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    is_incremental: bool
    days_back: int
    start_date: str
    end_date: str
    max_retry: int
    retry_sleep_seconds: int


@dataclass(frozen=True, slots=True)
class ExtractedFrames:
    interactions_df: object
    customers_df: object
    agents_df: object
    orders_df: object
