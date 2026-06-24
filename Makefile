# ==============================================================================
# AgenticAI-DataEngineering — Developer Makefile
# ==============================================================================
# Prerequisites (install once):
#   - Python 3.11+
#   - GNU Make   (Windows: via Chocolatey `choco install make`, or use WSL/Git Bash)
#   - Databricks CLI v0.200+   https://docs.databricks.com/dev-tools/cli/index.html
#   - Terraform >= 1.8.0       https://developer.hashicorp.com/terraform/downloads
#   - Azure CLI                https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
#
# Quick start for a new developer:
#   1. cp .env.example .env   &&  edit .env with real values
#   2. make install
#   3. make seed-local
#   4. make test
#   5. make deploy-dev        (after `az login` and configuring Databricks CLI)
# ==============================================================================

SHELL        := /bin/bash
.DEFAULT_GOAL := help

# Load .env if it exists (skips CI where variables are set by the pipeline)
-include .env
export

PYTHON       := python3
VENV         := .venv
VENV_BIN     := $(VENV)/bin
PIP          := $(VENV_BIN)/pip
PYTHON_VENV  := $(VENV_BIN)/python
RUFF         := $(VENV_BIN)/ruff
BLACK        := $(VENV_BIN)/black
MYPY         := $(VENV_BIN)/mypy
BANDIT       := $(VENV_BIN)/bandit
PYTEST       := $(VENV_BIN)/pytest
PRE_COMMIT   := $(VENV_BIN)/pre-commit

SRC_DIRS     := src tests scripts
TERRAFORM    := infra/terraform

# Colour helpers
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[0;32m
CYAN  := \033[0;36m

# ==============================================================================
# Help
# ==============================================================================
.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "$(BOLD)AgenticAI-DataEngineering — Available Targets$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-25s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ==============================================================================
# Environment Setup
# ==============================================================================
.PHONY: install
install: $(VENV)/pyvenv.cfg install-hooks ## Create venv, install all dependencies, enable git hooks
	@echo "$(GREEN)✓ Environment ready. Run 'make test' to verify.$(RESET)"

$(VENV)/pyvenv.cfg:
	@echo "Creating virtual environment..."
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip setuptools wheel

.PHONY: install-deps
install-deps: $(VENV)/pyvenv.cfg ## Install runtime + dev dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -e ".[dev]"

.PHONY: install-hooks
install-hooks: install-deps ## Install pre-commit hooks
	$(PRE_COMMIT) install
	$(PRE_COMMIT) install --hook-type commit-msg
	@echo "$(GREEN)✓ Pre-commit hooks installed$(RESET)"

.PHONY: update-deps
update-deps: ## Upgrade all dependencies to latest compatible versions
	$(PIP) install --upgrade -r requirements.txt
	$(PIP) install --upgrade -e ".[dev]"

# ==============================================================================
# Code Quality
# ==============================================================================
.PHONY: lint
lint: ## Run ruff linter on all source files
	@echo "$(BOLD)Running ruff...$(RESET)"
	$(RUFF) check $(SRC_DIRS)

.PHONY: lint-fix
lint-fix: ## Run ruff with auto-fix
	$(RUFF) check --fix $(SRC_DIRS)

.PHONY: format
format: ## Format code with black
	@echo "$(BOLD)Formatting with black...$(RESET)"
	$(BLACK) $(SRC_DIRS)

.PHONY: format-check
format-check: ## Check formatting without modifying files (used in CI)
	$(BLACK) --check $(SRC_DIRS)

.PHONY: typecheck
typecheck: ## Run mypy static type checker
	@echo "$(BOLD)Running mypy...$(RESET)"
	$(MYPY) src/

.PHONY: security
security: ## Run bandit security scan + pip-audit vulnerability check
	@echo "$(BOLD)Running bandit...$(RESET)"
	$(BANDIT) -r src/ -ll -x src/dlt/ -f txt
	@echo "$(BOLD)Running pip-audit...$(RESET)"
	$(VENV_BIN)/pip-audit -r requirements.txt --ignore-vuln PYSEC-2022-43012

.PHONY: check
check: lint format-check typecheck security ## Run all quality checks (CI equivalent)

# ==============================================================================
# Testing
# ==============================================================================
.PHONY: test
test: test-unit test-integration ## Run all tests

.PHONY: test-unit
test-unit: ## Run fast unit tests (no Spark cluster required)
	@echo "$(BOLD)Running unit tests...$(RESET)"
	$(PYTEST) tests/unit/ -m "not integration" --tb=short -v

.PHONY: test-integration
test-integration: ## Run integration tests with local Spark + Delta Lake
	@echo "$(BOLD)Running integration tests (local Spark)...$(RESET)"
	$(PYTEST) tests/integration/ -m "not slow" --tb=short -v

.PHONY: test-remote
test-remote: ## Run integration tests against a live Databricks cluster (requires .env)
	@echo "$(BOLD)Running remote integration tests on Databricks...$(RESET)"
	DATABRICKS_HOST=$(DATABRICKS_HOST) \
	DATABRICKS_TOKEN=$(DATABRICKS_TOKEN) \
	$(PYTEST) tests/integration/ --tb=short -v

.PHONY: test-coverage
test-coverage: ## Run all tests with coverage report
	$(PYTEST) tests/unit/ tests/integration/ \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html:build/coverage \
		--cov-fail-under=70
	@echo "$(GREEN)Coverage report: build/coverage/index.html$(RESET)"

# ==============================================================================
# Local Development Data
# ==============================================================================
.PHONY: seed-local
seed-local: ## Seed local Delta tables with synthetic data for local development
	@echo "$(BOLD)Seeding local Delta tables...$(RESET)"
	$(PYTHON_VENV) scripts/seed_local.py
	@echo "$(GREEN)✓ Local Delta tables seeded at $(LOCAL_DELTA_BASE)$(RESET)"

.PHONY: serve-api
serve-api: ## Start the Gold REST API locally (port 8000)
	@echo "$(BOLD)Starting Gold API at http://localhost:8000$(RESET)"
	@echo "Docs available at http://localhost:8000/docs"
	DATABRICKS_HOST=$(DATABRICKS_HOST) \
	DATABRICKS_TOKEN=$(DATABRICKS_TOKEN) \
	DATABRICKS_HTTP_PATH=$(DATABRICKS_HTTP_PATH) \
	$(VENV_BIN)/uvicorn src.api.gold_api:app --reload --port 8000

# ==============================================================================
# Databricks Bundle — Deploy
# ==============================================================================
.PHONY: bundle-validate
bundle-validate: ## Validate databricks.yml without deploying
	databricks bundle validate

.PHONY: deploy-dev
deploy-dev: bundle-validate ## Deploy all jobs/pipelines to the DEV workspace
	@echo "$(BOLD)Deploying to DEV...$(RESET)"
	DATABRICKS_HOST=$(DEV_DATABRICKS_HOST) \
	DATABRICKS_TOKEN=$(DATABRICKS_TOKEN) \
	databricks bundle deploy --target dev
	@echo "$(GREEN)✓ DEV deployment complete$(RESET)"

.PHONY: deploy-test
deploy-test: bundle-validate ## Deploy to the TEST workspace (requires approval in CI)
	@echo "$(BOLD)Deploying to TEST...$(RESET)"
	DATABRICKS_HOST=$(TEST_DATABRICKS_HOST) \
	DATABRICKS_TOKEN=$(DATABRICKS_TOKEN) \
	databricks bundle deploy --target test
	@echo "$(GREEN)✓ TEST deployment complete$(RESET)"

.PHONY: deploy-prod
deploy-prod: ## Deploy to PROD — confirms before proceeding
	@read -p "Deploy to PROD? This is irreversible. Type 'yes' to continue: " confirm; \
	[ "$$confirm" = "yes" ] || (echo "Aborted." && exit 1)
	@echo "$(BOLD)Deploying to PROD...$(RESET)"
	DATABRICKS_HOST=$(PROD_DATABRICKS_HOST) \
	DATABRICKS_TOKEN=$(DATABRICKS_TOKEN) \
	databricks bundle deploy --target prod
	@echo "$(GREEN)✓ PROD deployment complete$(RESET)"

.PHONY: run-dev
run-dev: ## Trigger all scheduled jobs once in DEV for a smoke test
	DATABRICKS_HOST=$(DEV_DATABRICKS_HOST) \
	DATABRICKS_TOKEN=$(DATABRICKS_TOKEN) \
	databricks bundle run --target dev lakehouse_ingestion_job

# ==============================================================================
# Terraform Infrastructure
# ==============================================================================
.PHONY: infra-init
infra-init: ## Initialise Terraform (run once per environment)
	cd $(TERRAFORM) && terraform init \
		-backend-config="resource_group_name=$(TF_STATE_RESOURCE_GROUP)" \
		-backend-config="storage_account_name=$(TF_STATE_STORAGE_ACCOUNT)" \
		-backend-config="container_name=$(TF_STATE_CONTAINER)" \
		-backend-config="key=$(LAKEHOUSE_ENV)/terraform.tfstate"

.PHONY: infra-plan
infra-plan: ## Terraform plan for the target environment
	cd $(TERRAFORM) && terraform plan \
		-var="environment=$(LAKEHOUSE_ENV)" \
		-out=$(LAKEHOUSE_ENV).tfplan

.PHONY: infra-apply
infra-apply: ## Apply Terraform plan (run infra-plan first)
	cd $(TERRAFORM) && terraform apply $(LAKEHOUSE_ENV).tfplan

# ==============================================================================
# Maintenance
# ==============================================================================
.PHONY: pre-commit
pre-commit: ## Run all pre-commit hooks against all files
	$(PRE_COMMIT) run --all-files

.PHONY: clean
clean: ## Remove build artefacts, coverage reports, and __pycache__
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache"   -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache"   -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ *.egg-info htmlcov/ .coverage
	@echo "$(GREEN)✓ Clean$(RESET)"

.PHONY: clean-local-delta
clean-local-delta: ## Remove local Delta tables seeded by seed-local
	rm -rf $(LOCAL_DELTA_BASE)
	@echo "$(GREEN)✓ Local Delta tables removed$(RESET)"

.PHONY: clean-all
clean-all: clean clean-local-delta ## Remove everything including the virtual environment
	rm -rf $(VENV)
	@echo "$(GREEN)✓ Full clean complete$(RESET)"
