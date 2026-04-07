import io

import boto3
import pandas as pd
from botocore.client import Config

from data_projects_portfolio.config import Settings


class S3Client:
    """
    Uploads files to MinIO/S3.

    Encapsulates all boto3 usage for write operations, keeping the service layer
    free of infrastructure details. The gold layer outputs a CSV — if that changes
    (e.g., to Parquet), only this class needs to change.
    """

    def __init__(self, settings: Settings) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def upload_csv(self, df: pd.DataFrame, bucket: str, key: str) -> None:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue().encode("utf-8"),
        )
