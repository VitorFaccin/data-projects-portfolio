from src.config import get_settings
from src.service_layer.pricing_pipeline import run_bronze_ingestion


def run() -> None:
    settings = get_settings()
    run_bronze_ingestion(settings)
