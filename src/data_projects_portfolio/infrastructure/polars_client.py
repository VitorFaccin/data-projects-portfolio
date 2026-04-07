import polars as pl

from data_projects_portfolio.config import Settings


class PolarsClient:
    """
    Reads Delta Lake tables from MinIO using Polars.

    Polars is chosen for the analytical Gold layer because the silver master dataset
    is already filtered and aggregated — medium-sized, single-node friendly.
    No need to spin up Spark executors for analytical aggregations.

    This strategy (Spark for raw joins, Polars for analytics) demonstrates cost
    awareness: right tool for the right scale.
    """

    def __init__(self, settings: Settings) -> None:
        self._storage_options = {
            "endpoint_url": settings.minio_endpoint,
            "aws_access_key_id": settings.minio_access_key,
            "aws_secret_access_key": settings.minio_secret_key,
            "region_name": "us-east-1",
            "allow_http": "true",
        }

    def read_delta(self, path: str) -> pl.DataFrame:
        return pl.read_delta(path, storage_options=self._storage_options)
