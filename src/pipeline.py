"""Orquestrador do pipeline ETL.

Executa as camadas em ordem: bronze -> silver -> geo -> gold -> lakehouse.
Cada etapa e idempotente (overwrite), entao o pipeline pode ser reexecutado
sem gerar duplicacao.

Uso:
  python -m src.pipeline              # pipeline completo
  python -m src.pipeline --stage bronze
  python -m src.pipeline --from-stage silver   # silver -> lakehouse
"""
from __future__ import annotations

import argparse
import time
from collections.abc import Callable

from pyspark.sql import SparkSession

from . import bronze, geo, gold, lakehouse, silver
from .config import Config, load_config
from .logging_conf import get_logger
from .spark_session import build_spark

logger = get_logger("pipeline")

STAGES_SPARK: tuple[tuple[str, Callable], ...] = (
    ("bronze", bronze.run),
    ("silver", silver.run),
    ("geo", geo.run),
    ("gold", gold.run),
)
STAGE_ORDER = [name for name, _ in STAGES_SPARK] + ["lakehouse"]


def _run_spark_stage(spark: SparkSession, cfg: Config, nome: str, fn: Callable) -> None:
    inicio = time.perf_counter()
    fn(spark, cfg)
    logger.info("etapa '%s' concluida em %.1fs", nome, time.perf_counter() - inicio)


def run_stages(cfg: Config, stages: list[str]) -> None:
    spark_stages = [s for s in stages if s != "lakehouse"]
    spark = build_spark(cfg) if spark_stages else None
    if spark:
        spark.sparkContext.setLogLevel("WARN")
    try:
        for nome, fn in STAGES_SPARK:
            if nome in spark_stages:
                _run_spark_stage(spark, cfg, nome, fn)
        if "lakehouse" in stages:
            inicio = time.perf_counter()
            lakehouse.run(cfg)
            logger.info("etapa 'lakehouse' concluida em %.1fs", time.perf_counter() - inicio)
    finally:
        if spark:
            spark.stop()


def _resolve_stages(stage: str | None, from_stage: str | None) -> list[str]:
    if stage and from_stage:
        raise SystemExit("Use --stage OU --from-stage, nao ambos.")
    if stage:
        if stage not in STAGE_ORDER:
            raise SystemExit(f"Etapa invalida: {stage}. Opcoes: {', '.join(STAGE_ORDER)}")
        return [stage]
    if from_stage:
        if from_stage not in STAGE_ORDER:
            raise SystemExit(f"Etapa invalida: {from_stage}. Opcoes: {', '.join(STAGE_ORDER)}")
        idx = STAGE_ORDER.index(from_stage)
        return STAGE_ORDER[idx:]
    return STAGE_ORDER


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline ETL medallion (PySpark + DuckDB)")
    parser.add_argument(
        "--stage",
        choices=STAGE_ORDER,
        help="Executa somente uma etapa.",
    )
    parser.add_argument(
        "--from-stage",
        choices=STAGE_ORDER,
        help="Executa a partir desta etapa ate lakehouse.",
    )
    args = parser.parse_args(argv)

    cfg = load_config()
    stages = _resolve_stages(args.stage, args.from_stage)
    logger.info(
        "iniciando pipeline | etapas=%s | data_dir=%s | lakehouse=%s",
        " -> ".join(stages),
        cfg.data_dir,
        cfg.lakehouse_dir,
    )
    run_stages(cfg, stages)
    logger.info("pipeline finalizado com sucesso")


if __name__ == "__main__":
    main()
