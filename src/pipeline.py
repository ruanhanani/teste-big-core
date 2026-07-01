"""Orquestrador do pipeline ETL.

Executa as camadas em ordem: bronze -> silver -> geo -> gold -> lakehouse.
Cada etapa e idempotente (overwrite), entao o pipeline pode ser reexecutado
sem gerar duplicacao. Uso: `python -m src.pipeline`.
"""
from __future__ import annotations

import time

from . import bronze, geo, gold, lakehouse, silver
from .config import load_config
from .logging_conf import get_logger
from .spark_session import build_spark

logger = get_logger("pipeline")


def main() -> None:
    cfg = load_config()
    logger.info("iniciando pipeline | data_dir=%s | lakehouse=%s", cfg.data_dir, cfg.lakehouse_dir)
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    etapas_spark = (
        ("bronze", bronze.run),
        ("silver", silver.run),
        ("geo", geo.run),
        ("gold", gold.run),
    )
    try:
        for nome, etapa in etapas_spark:
            inicio = time.perf_counter()
            etapa(spark, cfg)
            logger.info("etapa '%s' concluida em %.1fs", nome, time.perf_counter() - inicio)
    finally:
        spark.stop()

    # DuckDB nao usa Spark: registra as views apos o processamento.
    lakehouse.run(cfg)
    logger.info("pipeline finalizado com sucesso")


if __name__ == "__main__":
    main()
