# ══════════════════════════════════════════════════════════════════════════════
#  Makefile — one-command dev operations
#  Usage: make <target>
# ══════════════════════════════════════════════════════════════════════════════


.PHONY: help install install-dev lint format typecheck test test-unit test-integration \
        db.reset db.migrate up down demo worker intel dashboard clean

# Print help by default
help:
	@echo ""
	@echo "  twilio-comm-intelligence — available commands"
	@echo "  ──────────────────────────────────────────────"
	@echo "  make install          Install production dependencies"
	@echo "  make install-dev      Install all dependencies including dev/test tools"
	@echo "  make lint             Run ruff linter"
	@echo "  make format           Run black formatter"
	@echo "  make typecheck        Run mypy type checker"
	@echo "  make test             Run all tests with coverage"
	@echo "  make test-unit        Run unit tests only"
	@echo "  make test-integration Run integration tests only"
	@echo "  make db.reset         Drop and recreate all tables (runs all migrations)"
	@echo "  make db.migrate       Run any pending migrations"
	@echo "  make up               Start all services with docker-compose"
	@echo "  make down             Stop all services"
	@echo "  make worker           Start the delivery worker"
	@echo "  make intel            Start the intelligence layer (enrichment + embedding)"
	@echo "  make dashboard        Start the Streamlit operator dashboard"
	@echo "  make demo             Run the chaos demo script"
	@echo "  make clean            Remove caches and temp files"
	@echo ""

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check .

format:
	black .

typecheck:
	mypy comm_layer delivery_worker intelligence_layer

# Run all tests with coverage — equivalent to what CI runs
test:
	pytest

# Run only fast unit tests (no DB needed)
test-unit:
	pytest tests/unit -v

# Run integration tests (needs DB — set DATABASE_URL first)
test-integration:
	pytest tests/integration -v

# Apply all SQL migrations in numbered order
db.migrate:
	@echo "Applying migrations..."
	@set -a; . ./.env; set +a; for f in migrations/*.sql; do \
		echo "  → $$f"; \
		psql "$$DATABASE_URL" -f "$$f"; \
	done
	@echo "Migrations applied."

# Drop all tables and reapply from scratch — use in dev only
db.reset:
	@echo "WARNING: This will DELETE all data. Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
	psql "$$DATABASE_URL" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	$(MAKE) db.migrate
	psql "$$DATABASE_URL" -f seeds/number_registry_seed.sql
	@echo "Database reset complete."

up:
	docker-compose up --build

down:
	docker-compose down

worker:
	python delivery_worker/main.py

intel:
	python -m intelligence_layer.main

dashboard:
	streamlit run dashboard/main.py

demo:
	@echo "Running chaos demo..."
	bash scripts/demo.sh

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	@echo "Clean complete."
