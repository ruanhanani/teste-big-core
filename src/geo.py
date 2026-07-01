"""Enriquecimento geoespacial (point-in-polygon) sobre o rastreamento.

Estrategia: as ~38 geocercas cabem folgadamente na memoria, entao sao
transmitidas (broadcast) para os executores e indexadas em uma STRtree do
Shapely. Um `pandas_udf` vetorizado resolve o point-in-polygon de cada posicao
GPS distribuidamente no Spark — sem depender de jars externos.

Saidas:
  * posicoes_geo: cada ponto classificado como `em_geocerca` (com o id/tipo da
    cerca) ou `em_rota`;
  * eventos_geocerca: visitas consolidadas (entrada, saida e tempo parado),
    derivadas da deteccao de entrada/saida ao longo de cada viagem.

Para volumes muito maiores, a troca natural seria Apache Sedona (spatial join
distribuido nativo) — ver README.
"""
from __future__ import annotations

import pandas as pd
from shapely import STRtree, points
from shapely.wkt import loads as wkt_loads
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

from .config import Config
from .logging_conf import get_logger

logger = get_logger("geo")

# Cache por worker: evita reconstruir a STRtree a cada batch do pandas_udf.
_TREE_CACHE: dict[str, tuple] = {}


def build_tree(polygons: list[tuple[str, str]]):
    """Constroi (uma vez por worker) a STRtree e a lista de ids paralela."""
    cached = _TREE_CACHE.get("tree")
    if cached is None:
        geoms = [wkt_loads(wkt) for _id, wkt in polygons]
        ids = [gid for gid, _wkt in polygons]
        cached = (STRtree(geoms), ids)
        _TREE_CACHE["tree"] = cached
    return cached


def match_point(tree, ids, point) -> str | None:
    """Retorna o id da primeira geocerca que contem o ponto, ou None."""
    # STRtree aplica o predicado como point.predicado(poligono) -> "within".
    match = tree.query(point, predicate="within")
    return ids[int(match[0])] if len(match) > 0 else None


def _make_pip_udf(broadcast_polys):
    @pandas_udf("string")
    def _pip(lon: pd.Series, lat: pd.Series) -> pd.Series:
        tree, ids = build_tree(broadcast_polys.value)
        pts = points(lon.to_numpy(), lat.to_numpy())
        out = [match_point(tree, ids, pt) for pt in pts]
        return pd.Series(out, dtype="object")

    return _pip


def _classificar_posicoes(
    spark: SparkSession, cfg: Config, posicoes: DataFrame, geocercas: DataFrame
) -> DataFrame:
    polys = [(r["geocerca_id"], r["geometry_wkt"]) for r in geocercas.collect()]
    broadcast_polys = spark.sparkContext.broadcast(polys)
    pip_udf = _make_pip_udf(broadcast_polys)

    tipos = geocercas.select(
        F.col("geocerca_id"),
        F.col("tipo").alias("tipo_geocerca"),
    )

    return (
        posicoes.withColumn("geocerca_id", pip_udf(F.col("longitude"), F.col("latitude")))
        .join(tipos, "geocerca_id", "left")
        .withColumn(
            "classificacao",
            F.when(F.col("geocerca_id").isNotNull(), F.lit("em_geocerca")).otherwise(
                F.lit("em_rota")
            ),
        )
    )


def _detectar_visitas(posicoes_geo: DataFrame) -> DataFrame:
    """Agrupa pings consecutivos numa mesma geocerca em visitas (islands)."""
    ordem = Window.partitionBy("viagem_id").orderBy("timestamp")
    corrida = ordem.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    anterior = F.lag("geocerca_id").over(ordem)
    inicio_visita = (~F.col("geocerca_id").eqNullSafe(anterior)).cast("int")

    marcado = posicoes_geo.withColumn(
        "visita_id", F.sum(inicio_visita).over(corrida)
    )

    return (
        marcado.filter(F.col("geocerca_id").isNotNull())
        .groupBy("viagem_id", "visita_id", "geocerca_id", "tipo_geocerca")
        .agg(
            F.min("timestamp").alias("entrada_ts"),
            F.max("timestamp").alias("saida_ts"),
            F.count("*").alias("qtd_pontos"),
        )
        .withColumn(
            "duracao_segundos",
            F.col("saida_ts").cast("long") - F.col("entrada_ts").cast("long"),
        )
        .drop("visita_id")
    )


def run(spark: SparkSession, cfg: Config) -> None:
    posicoes = spark.read.parquet(str(cfg.silver_dir / "posicoes"))
    geocercas = spark.read.parquet(str(cfg.silver_dir / "geocercas"))

    posicoes_geo = _classificar_posicoes(spark, cfg, posicoes, geocercas).cache()
    posicoes_geo.write.mode("overwrite").parquet(str(cfg.silver_dir / "posicoes_geo"))
    em_cerca = posicoes_geo.filter(F.col("classificacao") == "em_geocerca").count()
    logger.info(
        "posicoes_geo -> %s pontos (%s em geocerca)", posicoes_geo.count(), em_cerca
    )

    visitas = _detectar_visitas(posicoes_geo)
    visitas.write.mode("overwrite").parquet(str(cfg.silver_dir / "eventos_geocerca"))
    logger.info("eventos_geocerca -> %s visitas detectadas", visitas.count())
    posicoes_geo.unpersist()
