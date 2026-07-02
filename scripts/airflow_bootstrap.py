#!/usr/bin/env python3
"""Dispara a DAG ETL e aguarda conclusao (Airflow 3.x nao tem --wait no trigger)."""
from __future__ import annotations

import json
import subprocess
import sys
import time

DAG = "etl_frota_logistica"
TIMEOUT_SEC = 3600
POLL_SEC = 15


def _run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout


def _json_array(stdout: str) -> list[dict]:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            return json.loads(line)
    raise ValueError(f"JSON nao encontrado na saida: {stdout[:300]!r}")


def main() -> int:
    run_id = f"bootstrap_{int(time.time())}"
    print(f"Disparando DAG {DAG} run_id={run_id}")
    _run(["airflow", "dags", "trigger", DAG, "-r", run_id, "-o", "plain"])

    deadline = time.time() + TIMEOUT_SEC
    while time.time() < deadline:
        raw = _run(["airflow", "dags", "list-runs", DAG, "-o", "json"])
        runs = _json_array(raw)
        for run in runs:
            if run.get("run_id") != run_id:
                continue
            state = run.get("state", "")
            print(f"  estado: {state}")
            if state == "success":
                print("ETL concluido com sucesso.")
                return 0
            if state == "failed":
                print("ETL falhou.", file=sys.stderr)
                return 1
        time.sleep(POLL_SEC)

    print("Timeout aguardando DAG.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
