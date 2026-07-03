"""Valida queries do dashboard contra medallion.duckdb."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.config import load_config  # noqa: E402

QUERIES = [
    "SELECT COUNT(*) FROM gold.viagens_enriquecidas",
    "SELECT COUNT(*) FROM gold.taxa_atraso_por_mes",
    "SELECT COUNT(*) FROM gold.top10_motoristas",
    "SELECT COUNT(*) FROM silver.geocercas",
    "SELECT COUNT(*) FROM silver.posicoes_geo WHERE classificacao='em_geocerca'",
    "SELECT COUNT(*) FROM rejeitados.viagens",
]


def main() -> int:
    cfg = load_config()
    con = duckdb.connect(str(cfg.duckdb_path), read_only=True)
    for sql in QUERIES:
        n = con.execute(sql).fetchone()[0]
        print(f"OK {n:>6}  {sql}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
