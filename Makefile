.PHONY: up down pipeline dashboard test clean airflow-trigger airflow-logs env

# Sobe stack completo: Airflow + bootstrap ETL + dashboard.
up:
	docker compose up --build

down:
	docker compose down

env:
	@if not exist .env copy .env.example .env

# Pipeline direto (sem Airflow).
pipeline:
	python -m src.pipeline

pipeline-stage:
	python -m src.pipeline --stage $(STAGE)

# Pipeline legado via Docker (profile legacy).
pipeline-docker:
	docker compose --profile legacy run --rm pipeline

dashboard:
	streamlit run dashboard/app.py --server.port=8502

test:
	pytest -q

clean:
	rm -rf lakehouse logs/airflow

# Dispara a DAG manualmente (stack Airflow ja rodando).
airflow-trigger:
	docker compose exec airflow-scheduler python /app/scripts/airflow_bootstrap.py

airflow-logs:
	docker compose logs -f airflow-worker airflow-scheduler

# Flower (monitor Celery).
flower:
	docker compose --profile flower up -d flower
