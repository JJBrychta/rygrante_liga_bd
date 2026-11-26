# ==============================================================================
# rygrante_liga_bd — developer shortcuts
#
# Run `make` (or `make help`) to list targets.
# The CI targets mirror .github/workflows/ci.yml so you can reproduce CI locally
# before pushing. Security scanners run in Docker, so nothing extra to install.
# Requires: uv, Docker, GNU make. Arch: use Makefile.arch. Windows: use make.ps1.
# ==============================================================================

.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE          ?= docker compose
GITLEAKS_VERSION ?= v8.30.1
SEMGREP_IMAGE    ?= semgrep/semgrep:latest
TRIVY_IMAGE      ?= aquasec/trivy:latest

# Local-only paths that CI never sees (secrets, data, build state) — keep them
# out of the Trivy scan so it doesn't flag your own .env credentials.
TRIVY_SKIP_DIRS  := data,lakehouse,logs,.venv,.idea,.ruff_cache,.pytest_cache
TRIVY_SKIP_FILES := .env

.PHONY: help ci ci-fast lint uv-check fix test test-host gitleaks semgrep trivy \
        up down restart ps logs build

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "; printf "\nUsage: make \033[36m<target>\033[0m\n"} \
		/^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} \
		/^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ CI (mirrors .github/workflows/ci.yml)

ci: lint test gitleaks semgrep trivy ## Run the full CI pipeline locally (all 5 jobs)
	@echo -e "\n\033[32m✔ All CI jobs passed\033[0m"

ci-fast: lint test ## Run only lint + tests (quick pre-push check)
	@echo -e "\n\033[32m✔ lint + tests passed\033[0m"

lint: uv-check ## Job lint-and-uv: uv lock, Ruff lint/format, script + Compose syntax
	uv run ruff check .
	uv run ruff format --check .
	bash -n scripts/download_data.sh
	$(COMPOSE) config --quiet
	@echo -e "\033[32m✔ lint-and-uv passed\033[0m"

uv-check:
	uv lock --check
	uv sync --frozen --dev --quiet

fix: ## Auto-fix Ruff lint issues and reformat code
	uv run ruff check --fix .
	uv run ruff format .

test: ## Job unit-tests: pytest inside the Spark container (no local Java needed)
	$(COMPOSE) run --rm spark-master pytest -v

test-host: ## Job unit-tests exactly like CI (needs Java 17 on your machine)
	uv run pytest -v

gitleaks: ## Job gitleaks: scan committed Git history for secrets
	docker run --rm -v "$(CURDIR):/repo" zricethezav/gitleaks:$(GITLEAKS_VERSION) \
		git --verbose --redact --config /repo/.gitleaks.toml /repo

semgrep: ## Job semgrep: static security analysis (Python, Docker, Compose, secrets)
	docker run --rm -v "$(CURDIR):/src" -w /src $(SEMGREP_IMAGE) \
		semgrep scan --config p/python --config p/dockerfile \
		--config p/docker-compose --config p/secrets --error --metrics=off

trivy: ## Job trivy: CRITICAL/HIGH vulnerability, secret and IaC scans
	docker run --rm -v "$(CURDIR):/src" -v trivy-cache:/root/.cache $(TRIVY_IMAGE) \
		fs --scanners vuln,secret --severity CRITICAL,HIGH --ignore-unfixed --exit-code 1 \
		--skip-dirs $(TRIVY_SKIP_DIRS) --skip-files $(TRIVY_SKIP_FILES) /src
	docker run --rm -v "$(CURDIR):/src" -v trivy-cache:/root/.cache $(TRIVY_IMAGE) \
		config --severity CRITICAL,HIGH --exit-code 1 --skip-dirs $(TRIVY_SKIP_DIRS) /src

##@ Docker stack

up: ## Start the stack (Spark, Airflow, Streamlit) in the background
	$(COMPOSE) up -d

build: ## Rebuild images and start the stack
	$(COMPOSE) up -d --build

down: ## Stop the stack (keeps data and volumes)
	$(COMPOSE) down

restart: down up ## Restart the stack

ps: ## Show container status
	$(COMPOSE) ps -a

logs: ## Follow logs (all services, or one: make logs s=airflow-webserver)
	$(COMPOSE) logs -f --tail 100 $(s)
