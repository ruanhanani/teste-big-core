"""Camada de servico do datalake local via DuckDB.

Registra schemas bronze / silver / gold / rejeitados com views sobre Parquet.
Um unico arquivo (medallion.duckdb) serve DBeaver e dashboard — sem duplicar
dados nem aliases em main.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from .config import Config
from .logging_conf import get_logger

logger = get_logger("lakehouse")

_LAYERS = ("bronze", "silver", "gold", "rejeitados")


def _layer_root(cfg: Config, layer: str) -> Path:
    return getattr(cfg, f"{layer}_dir")


def _ensure_schema(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def _register_layer(
    con: duckdb.DuckDBPyConnection, schema: str, root: Path, registrados: list[str]
) -> None:
    if not root.exists():
        return
    _ensure_schema(con, schema)
    for sub in sorted(root.iterdir()):
        if not sub.is_dir():
            continue
        glob = f"{sub.as_posix()}/**/*.parquet"
        fq = f'"{schema}"."{sub.name}"'
        con.execute(
            f"CREATE OR REPLACE VIEW {fq} AS "
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
        )
        registrados.append(f"{schema}.{sub.name}")


def run(cfg: Config) -> None:
    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(cfg.duckdb_path))
    try:
        registrados: list[str] = []
        for layer in _LAYERS:
            _register_layer(con, layer, _layer_root(cfg, layer), registrados)
        n_gold = con.execute("SELECT COUNT(*) FROM gold.viagens_enriquecidas").fetchone()[0]
        assert n_gold > 0, "gold.viagens_enriquecidas ausente ou vazio"
        logger.info(
            "duckdb -> %s views em %s | gold.viagens_enriquecidas=%s",
            len(registrados),
            cfg.duckdb_path.name,
            n_gold,
        )
    finally:
        con.close()
