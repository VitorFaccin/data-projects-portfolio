from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True, slots=True)
class ElasticityResult:
    product_id: str
    sale_location: str
    product_category: str
    nivel_de_preco: int
    elasticidade_preco: Optional[float]
    elasticidade_prazo: Optional[float]
    qtde_observacoes: int
    nivel_confianca: str


@dataclass(frozen=True, slots=True)
class AnalysisWindow:
    start_date: str
    end_date: str


@dataclass(frozen=True, slots=True)
class RegressionFit:
    coefficients: Optional[Any]
    p_values: Optional[Any]
    r_squared: Optional[float]


@dataclass(frozen=True, slots=True)
class SeasonalKey:
    product_category: str
    month: int


@dataclass(frozen=True, slots=True)
class PipelinePaths:
    pipeline_root: Path
    artifacts_dir: Path
    sql_path: Path
    setup_json_path: Path
    orders_csv_path: Path
    order_items_csv_path: Path
    products_csv_path: Path
    translation_csv_path: Path
    customers_csv_path: Path
