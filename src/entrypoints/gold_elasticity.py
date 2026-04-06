from src.config import get_settings
from src.service_layer.pricing_pipeline import run_gold_elasticity


def run() -> None:
    settings = get_settings()
    run_gold_elasticity(settings)
