.PHONY: up down pipeline dashboard test clean airflow-trigger airflow-logs env flower pipeline-stage validate

# Sobe stack completo: Airflow + bootstrap ETL + dashboard.
up:
	docker compose up --build

down:
	docker compose down

env:
	@if not exist .env copy .env.example .env

pipeline:
	python -m src.pipeline

pipeline-stage:
	python -m src.pipeline --stage $(STAGE)

dashboard:
	streamlit run dashboard/app.py --server.port=8502

test:
	pytest -q

clean:
	rm -rf lakehouse logs/airflow

airflow-trigger:
	docker compose exec airflow-scheduler python /app/scripts/airflow_bootstrap.py

airflow-logs:
	docker compose logs -f airflow-worker airflow-scheduler

flower:
	docker compose --profile flower up -d flower

validate:
	python scripts/validate_lakehouse.py
	python scripts/validate_dashboard.py
