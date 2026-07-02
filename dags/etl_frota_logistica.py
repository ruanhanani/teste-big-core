"""DAG ETL medallion — frota logistica (PySpark + DuckDB).

Orquestra as mesmas etapas de `python -m src.pipeline`, uma task por camada,
com retries e timeouts. Idempotente: pode reexecutar a DAG inteira ou uma task.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

PIPELINE = "cd /app && python -m src.pipeline"
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

DOC_MD = """
## ETL Frota Logistica

Pipeline medallion executado via PySpark (`local[*]`) e catalogo DuckDB.

| Task | Camada | Saida |
|------|--------|-------|
| bronze | Extracao | Parquet bruto |
| silver | Limpeza + quarentena | Parquet confiavel |
| geo | Point-in-polygon | posicoes_geo, eventos |
| gold | Metricas de negocio | 8 tabelas gold |
| lakehouse | DuckDB views | warehouse.duckdb |
| validar_gold | QA | contagem viagens gold |

**Idempotente:** cada etapa usa `overwrite`. Reprocessar nao duplica dados.
"""

with DAG(
    dag_id="etl_frota_logistica",
    description="ETL medallion frota logistica (bronze -> gold + DuckDB)",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["etl", "pyspark", "medallion", "frota"],
    doc_md=DOC_MD,
    max_active_runs=1,
) as dag:
    with TaskGroup(group_id="medallion") as medallion:
        bronze = BashOperator(
            task_id="bronze",
            bash_command=f"{PIPELINE} --stage bronze",
            execution_timeout=timedelta(minutes=30),
        )
        silver = BashOperator(
            task_id="silver",
            bash_command=f"{PIPELINE} --stage silver",
            execution_timeout=timedelta(minutes=30),
        )
        geo = BashOperator(
            task_id="geo",
            bash_command=f"{PIPELINE} --stage geo",
            execution_timeout=timedelta(minutes=45),
        )
        gold = BashOperator(
            task_id="gold",
            bash_command=f"{PIPELINE} --stage gold",
            execution_timeout=timedelta(minutes=30),
        )
        bronze >> silver >> geo >> gold

    lakehouse = BashOperator(
        task_id="lakehouse",
        bash_command=f"{PIPELINE} --stage lakehouse",
        execution_timeout=timedelta(minutes=10),
    )

    validar_gold = BashOperator(
        task_id="validar_gold",
        bash_command=(
            "cd /app && python - <<'PY'\n"
            "import duckdb\n"
            "from pathlib import Path\n"
            "import os\n"
            "db = Path(os.environ['DUCKDB_PATH'])\n"
            "if not db.exists():\n"
            "    raise SystemExit(f'DuckDB ausente: {db}')\n"
            "con = duckdb.connect(str(db), read_only=True)\n"
            "n = con.execute('SELECT COUNT(*) FROM viagens_enriquecidas').fetchone()[0]\n"
            "if n < 2800:\n"
            "    raise SystemExit(f'viagens_enriquecidas={n}, esperado ~2892')\n"
            "views = {r[0] for r in con.execute('SHOW TABLES').fetchall()}\n"
            "for v in ('geocercas', 'posicoes_geo', 'viagens_enriquecidas'):\n"
            "    if v not in views:\n"
            "        raise SystemExit(f'view ausente: {v}')\n"
            "print(f'OK: {n} viagens, {len(views)} views DuckDB')\n"
            "PY"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    medallion >> lakehouse >> validar_gold
