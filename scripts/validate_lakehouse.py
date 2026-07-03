"""Smoke test pos-ETL — valida enunciado e integridade do lakehouse."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import load_config  # noqa: E402

# ponytail: limites esperados da base sintetica; ajuste se a fonte mudar.
EXPECT = {
    "bronze.viagens": (2900, 3100),
    "silver.viagens": (2800, 2950),
    "gold.viagens_enriquecidas": (2800, 2950),
    "silver.em_geocerca": (100, 200),
    "gold.top10_motoristas": (10, 10),
    "rejeitados.viagens": (50, 200),
    "gold.metricas": (7, 8),  # tabelas gold exceto fato
}


def main() -> int:
    cfg = load_config()
    db = cfg.duckdb_path
    if not db.exists():
        print(f"FAIL: {db} nao encontrado. Rode o pipeline primeiro.")
        return 1

    con = duckdb.connect(str(db), read_only=True)
    errors: list[str] = []

    def count(sql: str) -> int:
        return int(con.execute(sql).fetchone()[0])

    checks = {
        "bronze.viagens": count("SELECT COUNT(*) FROM bronze.viagens"),
        "silver.viagens": count("SELECT COUNT(*) FROM silver.viagens"),
        "gold.viagens_enriquecidas": count("SELECT COUNT(*) FROM gold.viagens_enriquecidas"),
        "silver.em_geocerca": count(
            "SELECT COUNT(*) FROM silver.posicoes_geo WHERE classificacao='em_geocerca'"
        ),
        "gold.top10_motoristas": count("SELECT COUNT(*) FROM gold.top10_motoristas"),
        "rejeitados.viagens": count("SELECT COUNT(*) FROM rejeitados.viagens"),
        "gold.metricas": count(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='gold' AND table_name != 'viagens_enriquecidas'"
        ),
    }

    gold_tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='gold'"
        ).fetchall()
    }
    required_gold = {
        "viagens_enriquecidas",
        "viagens_por_mes_status",
        "tempo_medio_por_rota",
        "velocidade_media_por_viagem",
        "taxa_atraso_por_mes",
        "top10_motoristas",
        "utilizacao_frota_por_mes",
        "tempo_medio_parado_por_tipo",
    }
    missing_gold = required_gold - gold_tables

    print(f"Catalogo: {db}")
    for key, val in checks.items():
        lo, hi = EXPECT[key]
        ok = lo <= val <= hi
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {key} = {val} (esperado {lo}-{hi})")
        if not ok:
            errors.append(key)

    for tbl in sorted(required_gold):
        ok = tbl in gold_tables
        print(f"  [{'OK' if ok else 'FAIL'}] gold.{tbl}")
        if not ok:
            errors.append(f"gold.{tbl}")

    if missing_gold:
        errors.extend(f"missing gold.{t}" for t in missing_gold)

    con.close()
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
