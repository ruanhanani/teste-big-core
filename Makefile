.PHONY: up down pipeline dashboard test clean

# Sobe tudo: pipeline + dashboard (um unico comando).
up:
	docker compose up --build

down:
	docker compose down

# Execucao local (sem Docker), util para desenvolvimento.
pipeline:
	python -m src.pipeline

dashboard:
	streamlit run dashboard/app.py --server.port=8502

test:
	pytest -q

clean:
	rm -rf lakehouse
