.PHONY: sim-up sim-down test validate bootstrap

sim-up:
	docker compose --env-file source-simulator/.env -f source-simulator/compose.yaml up -d --build

sim-down:
	docker compose --env-file source-simulator/.env -f source-simulator/compose.yaml down

test:
	python -m pytest tests/ -v

validate:
	docker compose --env-file source-simulator/.env -f source-simulator/compose.yaml config --quiet

bootstrap:
	@echo "Run these idempotent migrations in lexical order through Databricks SQL Editor:"
	@python -c "from pathlib import Path; print(*sorted(Path('pipelines/bootstrap').glob('v[0-9][0-9][0-9]_*.sql')), sep='\n')"
