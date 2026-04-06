from dataclasses import dataclass
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
class RegressionFit:
    coefficients: Optional[Any]
    p_values: Optional[Any]
    r_squared: Optional[float]


@dataclass(frozen=True, slots=True)
class SeasonalKey:
    product_category: str
    month: int
