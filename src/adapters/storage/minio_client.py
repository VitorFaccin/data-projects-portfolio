import os
from pathlib import Path

import boto3
from botocore.client import Config

from src.config import Settings


class MinioClient:
    def __init__(self, settings: Settings) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def upload_file(self, local_path: str, bucket: str, key: str) -> None:
        self._client.upload_file(local_path, bucket, key)

    def download_file(self, bucket: str, key: str, local_path: str) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(bucket, key, local_path)

    def upload_directory(self, local_dir: str, bucket: str, prefix: str = "") -> list[str]:
        uploaded: list[str] = []
        for filename in os.listdir(local_dir):
            local_path = os.path.join(local_dir, filename)
            if not os.path.isfile(local_path):
                continue
            key = f"{prefix}/{filename}".lstrip("/") if prefix else filename
            self.upload_file(local_path, bucket, key)
            uploaded.append(key)
        return uploaded

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
