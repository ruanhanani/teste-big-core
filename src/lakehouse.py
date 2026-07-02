"""Camada de servico do datalake local via DuckDB.

O DuckDB atua como "query engine" do lakehouse: em vez de subir um data
warehouse externo, expomos os Parquets das camadas gold/silver como VIEWs em
um catalogo `.duckdb`. Assim o dashboard consulta os dados tratados com SQL
analitico rapido, sem reprocessar nada no Spark.
"""
from __future__ import annotations

import duckdb

from .config import Config
from .logging_conf import get_logger

logger = get_logger("lakehouse")

# datasets extras (fora da gold) uteis para o dashboard.
_SILVER_VIEWS = ("posicoes_geo", "eventos_geocerca", "geocercas")
_REJEITADOS_VIEWS = ("viagens", "posicoes")


def _register(con: duckdb.DuckDBPyConnection, view: str, path) -> None:
    glob = f"{path.as_posix()}/**/*.parquet"
    con.execute(
        f'CREATE OR REPLACE VIEW "{view}" AS '
        f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
    )


def run(cfg: Config) -> None:
    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(cfg.duckdb_path))
    try:
        registrados = []
        for sub in sorted(cfg.gold_dir.iterdir()):
            if sub.is_dir():
                _register(con, sub.name, sub)
                registrados.append(sub.name)
        for view in _SILVER_VIEWS:
            path = cfg.silver_dir / view
            if path.exists():
                _register(con, view, path)
                registrados.append(view)
        for view in _REJEITADOS_VIEWS:
            path = cfg.rejeitados_dir / view
            if path.exists():
                nome = f"rejeitados_{view}"
                _register(con, nome, path)
                registrados.append(nome)
        logger.info("duckdb -> %s views: %s", len(registrados), ", ".join(registrados))
    finally:
        con.close()
