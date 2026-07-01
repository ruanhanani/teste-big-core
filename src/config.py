"""Configuracao central do pipeline.

Tudo que varia por ambiente (caminhos, memoria do Spark, limites de qualidade)
vem de variaveis de ambiente, nunca hardcoded. Um `.env` opcional e carregado
para facilitar a execucao local fora do Docker.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _path(env: str, default: str) -> Path:
    return Path(os.getenv(env, default)).resolve()


@dataclass(frozen=True)
class BoundingBox:
    """Retangulo geografico usado para descartar coordenadas fora do Brasil."""

    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


@dataclass(frozen=True)
class Config:
    data_dir: Path
    lakehouse_dir: Path
    duckdb_path: Path
    spark_master: str
    spark_driver_memory: str
    spark_app_name: str
    brasil_bbox: BoundingBox
    velocidade_max_kmh: int

    @property
    def bronze_dir(self) -> Path:
        return self.lakehouse_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.lakehouse_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.lakehouse_dir / "gold"

    @property
    def rejeitados_dir(self) -> Path:
        return self.lakehouse_dir / "rejeitados"


def load_config() -> Config:
    return Config(
        data_dir=_path("DATA_DIR", "data"),
        lakehouse_dir=_path("LAKEHOUSE_DIR", "lakehouse"),
        duckdb_path=_path("DUCKDB_PATH", "lakehouse/warehouse.duckdb"),
        spark_master=os.getenv("SPARK_MASTER", "local[*]"),
        spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        spark_app_name=os.getenv("SPARK_APP_NAME", "teste-big-core"),
        brasil_bbox=BoundingBox(
            lat_min=float(os.getenv("BR_LAT_MIN", "-34.0")),
            lat_max=float(os.getenv("BR_LAT_MAX", "5.5")),
            lon_min=float(os.getenv("BR_LON_MIN", "-74.0")),
            lon_max=float(os.getenv("BR_LON_MAX", "-34.0")),
        ),
        velocidade_max_kmh=int(os.getenv("VELOCIDADE_MAX_KMH", "140")),
    )
