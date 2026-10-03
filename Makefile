# AtlasLake developer entry points. `make help` lists targets.
SHELL := /bin/bash
export UV_PROJECT_ENVIRONMENT ?= $(HOME)/.venvs/atlaslake
# Host shells may export a foreign PYTHONPATH (e.g. ROS) that breaks pytest plugin loading.
unexport PYTHONPATH

UV      ?= uv
RUN     := $(UV) run --no-sync
ATLAS   := $(RUN) atlas
PYTEST  := $(RUN) python -m pytest -p no:cacheprovider
SCALE   ?= small
COMPOSE ?= docker compose
PY_PACKAGES := core simulator contracts producers ingestion spark baseline quality reconciliation \
               lineage monitoring cli benchmarks lakehouse

.DEFAULT_GOAL := help
.PHONY: images help setup up up-all down init lint fmt typecheck test test-unit test-integration test-failure \
        dbt-build demo benchmark chaos clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Install all dependencies (incl. extras + dev) and git hooks
	$(UV) sync --all-extras
	$(RUN) pre-commit install

up: ## Start the core stack (postgres, minio, kafka, schema-registry, kafka-ui)
	$(COMPOSE) up -d

up-all: ## Start every profile (monitoring, lineage, orchestration)
	$(COMPOSE) --profile monitoring --profile lineage --profile orchestration up -d

down: ## Stop all services (data volumes are kept)
	$(COMPOSE) --profile monitoring --profile lineage --profile orchestration down

init: ## Create bucket, source schema, contracts, topics and Iceberg tables
	$(ATLAS) init

images: ## Build app + airflow images (host network: this host's dockerd runs with iptables=false)
	docker build --network host -f docker/app/Dockerfile -t atlaslake/app:local .
	docker build --network host -f docker/airflow/Dockerfile -t atlaslake/airflow:local .

lint: ## Ruff lint + format check
	$(RUN) ruff check .
	$(RUN) ruff format --check .

fmt: ## Auto-format and auto-fix
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

typecheck: ## mypy over all platform packages
	$(RUN) mypy $(PY_PACKAGES)

test-unit: ## Unit + contract tests (no docker needed)
	$(PYTEST) tests/unit tests/contracts

test: test-unit ## Fast test suite (unit + contracts)

test-integration: ## Integration tests against the docker stack (RESETS local data)
	$(PYTEST) tests/integration

test-failure: ## Failure-injection experiments (slow, RESETS local data)
	$(PYTEST) tests/failure

dbt-build: ## dbt build (run + test) of the Gold layer
	$(ATLAS) gold build

demo: ## Small end-to-end run: simulate → relay → stream → stores → Gold → reconcile
	$(ATLAS) simulate --orders 5000 --reset-source
	$(ATLAS) relay
	$(ATLAS) stream --available-now --metrics-port 0
	$(ATLAS) silver ingest-stores
	$(ATLAS) gold build
	$(ATLAS) reconcile

benchmark: ## Reproducible benchmarks into evaluation/*.json (SCALE=small|medium|large)
	$(ATLAS) benchmark --suite all --scale $(SCALE)

chaos: ## Final chaos test (Phase 45)
	$(RUN) python -m benchmarks.chaos

clean: ## Remove caches and build artefacts (keeps data/ and docker volumes)
	rm -rf .ruff_cache .mypy_cache .pytest_cache dbt/target dbt/logs logs
	find . -name __pycache__ -type d -prune -not -path './data/*' -exec rm -rf {} +
