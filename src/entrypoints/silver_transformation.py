from src.config import get_settings
from src.service_layer.pricing_pipeline import run_silver_transformation


def run() -> None:
    settings = get_settings()
    run_silver_transformation(settings)
