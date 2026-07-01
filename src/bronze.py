"""Camada BRONZE — extracao das 5 fontes brutas para Parquet.

O bronze e um espelho fiel da origem: nao limpamos nem tipamos aqui, apenas
padronizamos o formato de armazenamento (Parquet particionado por dataset).
A unica excecao e o GeoJSON, cuja geometria e convertida para WKT para poder
ser materializada em coluna tabular — o conteudo (coordenadas) permanece intacto.
"""
from __future__ import annotations

import json

from shapely.geometry import shape
from pyspark.sql import DataFrame, SparkSession

from .config import Config
from .logging_conf import get_logger

logger = get_logger("bronze")

# Fontes lidas como texto puro (inferSchema desligado) para preservar o dado cru.
_CSV_OPTS = {"header": "true", "inferSchema": "false", "mode": "PERMISSIVE"}


def _read_veiculos(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.read.options(**_CSV_OPTS).csv(str(cfg.data_dir / "veiculos" / "veiculos.csv"))


def _read_viagens(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.read.options(**_CSV_OPTS).csv(str(cfg.data_dir / "viagens" / "viagens.csv"))


def _read_motoristas(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.read.option("multiLine", "true").json(
        str(cfg.data_dir / "motoristas" / "motoristas.json")
    )


def _read_posicoes(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.read.parquet(str(cfg.data_dir / "rastreamento" / "posicoes.parquet"))


def _read_geocercas(spark: SparkSession, cfg: Config) -> DataFrame:
    """GeoJSON -> linhas tabulares com a geometria serializada em WKT."""
    path = cfg.data_dir / "geocercas" / "geocercas.geojson"
    with open(path, encoding="utf-8") as fh:
        collection = json.load(fh)

    rows = []
    for feature in collection.get("features", []):
        props = feature.get("properties", {}) or {}
        geometry = feature.get("geometry")
        geometry_wkt = shape(geometry).wkt if geometry else None
        rows.append(
            {
                "geocerca_id": props.get("geocerca_id"),
                "nome": props.get("nome"),
                "tipo": props.get("tipo"),
                "uf": props.get("uf"),
                "raio_km": props.get("raio_km"),
                "ativo": props.get("ativo"),
                "geometry_wkt": geometry_wkt,
            }
        )
    return spark.createDataFrame(rows)


_READERS = {
    "veiculos": _read_veiculos,
    "motoristas": _read_motoristas,
    "geocercas": _read_geocercas,
    "viagens": _read_viagens,
    "posicoes": _read_posicoes,
}


def run(spark: SparkSession, cfg: Config) -> None:
    """Le as 5 fontes e materializa cada uma em bronze/<dataset> (idempotente)."""
    cfg.bronze_dir.mkdir(parents=True, exist_ok=True)
    for name, reader in _READERS.items():
        df = reader(spark, cfg)
        destino = cfg.bronze_dir / name
        df.write.mode("overwrite").parquet(str(destino))
        logger.info("bronze.%s -> %s linhas", name, df.count())
