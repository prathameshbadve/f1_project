.PHONY: start stop test lint format clean

# Default target
.DEFAULT_GOAL := help

# Color output
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)


##@ General

# Show this help message
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${WHITE}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)

## Start all services (MinIO, PostgreSQL, MLflow, Dagster)
start-services:
	docker-compose -f docker/docker-compose.local.yml up -d
	@echo ""
	@echo "${GREEN}Services started successfully!${RESET}"
	@echo ""
	@echo "Access your services:"
	@echo "  ${YELLOW}Dagster UI:${RESET}  http://localhost:3000"
	@echo "  ${YELLOW}MinIO UI:${RESET}    http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  ${YELLOW}MLflow UI:${RESET}   http://localhost:8080"
	@echo "  ${YELLOW}PostgreSQL:${RESET}  localhost:5434 (f1user/f1pass)"
	@echo ""
	@echo "${YELLOW}Waiting for services to be healthy...${RESET}"
	@sleep 10
	@echo "${GREEN}Setting up MinIO buckets...${RESET}"
	@python scripts/minio/setup_minio_buckets.py || echo "${YELLOW}MinIO buckets setup failed - run 'make setup-buckets' manually${RESET}"


# start minio only
start-minio:
	docker-compose -f docker/docker-compose.local.yml up -d minio
	@echo "${GREEN}MinIO started:${RESET} http://localhost:9001"

# start postgres only
start-postgres:
	docker-compose -f docker/docker-compose.local.yml up -d postgres
	@echo "${GREEN}PostgreSQL started:${RESET} localhost:5434"

# Start MLflow only
start-mlflow: 
	docker-compose -f docker/docker-compose.local.yml up -d mlflow
	@echo "${GREEN}MLflow started:${RESET} http://localhost:8080"

# Start Dagster services only (requires other services running)
start-dagster:
	docker-compose -f docker/docker-compose.local.yml up -d dagster-webserver dagster-daemon
	@echo "${GREEN}Dagster started:${RESET} http://localhost:3000"

# Stop all services
stop-services:
	docker-compose -f docker/docker-compose.local.yml down
	@echo "${GREEN}All services stopped${RESET}"

# Restart all services
restart: stop-services start-services

# Restart Dagster services only (useful during development)
restart-dagster:
	docker-compose -f docker/docker-compose.local.yml restart dagster-webserver dagster-daemon
	@echo "${GREEN}Dagster services restarted${RESET}"

# Stop all services and remove volumes (WARNING: deletes all data!)
clean:
	@echo "${YELLOW}WARNING: This will delete all data including MinIO files and PostgreSQL database!${RESET}"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose -f docker/docker-compose.local.yml down -v; \
		echo "${GREEN}All services stopped and volumes removed${RESET}"; \
	else \
		echo "${YELLOW}Cancelled${RESET}"; \
	fi


##@ Building

# Build all Docker images
build:
	docker-compose -f docker/docker-compose.local.yml build
	@echo "${GREEN}All images built successfully${RESET}"

# Build Dagster image only
build-dagster:
	docker-compose -f docker/docker-compose.local.yml build dagster-webserver dagster-daemon
	@echo "${GREEN}Dagster image built successfully${RESET}"

# Clean rebuild - removes volumes, rebuilds images, and starts services
rebuild: clean build start


##@ Logs & Monitoring

# View logs from all services
logs:
	docker-compose -f docker/docker-compose.local.yml logs -f

# View Dagster logs only
logs-dagster:
	docker-compose -f docker/docker-compose.local.yml logs -f dagster-webserver dagster-daemon

# View MinIO logs only
logs-minio:
	docker-compose -f docker/docker-compose.local.yml logs -f minio

# View PostgreSQL logs only
logs-postgres:
	docker-compose -f docker/docker-compose.local.yml logs -f postgres

# View MLflow logs only
logs-mlflow:
	docker-compose -f docker/docker-compose.local.yml logs -f mlflow

# Check health status of all services
health:
	@echo "${YELLOW}Checking service health...${RESET}"
	@echo ""
	@echo -n "MinIO:      "
	@curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1 && echo "${GREEN}✓ Healthy${RESET}" || echo "${YELLOW}✗ Not responding${RESET}"
	@echo -n "PostgreSQL: "
	@docker exec $$(docker ps -q -f name=postgres) pg_isready -U f1user > /dev/null 2>&1 && echo "${GREEN}✓ Healthy${RESET}" || echo "${YELLOW}✗ Not responding${RESET}"
	@echo -n "MLflow:     "
	@curl -f http://localhost:8080/health > /dev/null 2>&1 && echo "${GREEN}✓ Healthy${RESET}" || echo "${YELLOW}✗ Not responding${RESET}"
	@echo -n "Dagster:    "
	@curl -f http://localhost:3000/server_info > /dev/null 2>&1 && echo "${GREEN}✓ Healthy${RESET}" || echo "${YELLOW}✗ Not responding${RESET}"

# Show running containers
ps:
	docker-compose -f docker/docker-compose.local.yml ps


##@ Setup & Configuration

# Create MinIO buckets
setup-buckets:
	python scripts/minio/setup_minio_buckets.py
	@echo "${GREEN}MinIO buckets created${RESET}"

# Test database connection
test-db:
	python scripts/postgresdb/test_db_connection.py

# Full setup - start services and configure buckets
setup: start setup-buckets
	@echo ""
	@echo "${GREEN}Setup complete! Access Dagster at http://localhost:3000${RESET}"


##@ Development

# Open a shell in the Dagster webserver container
shell-dagster:
	docker exec -it f1-dagster-webserver /bin/bash

# Open a PostgreSQL shell
shell-postgres:
	docker exec -it f1-postgres psql -U f1user -d f1_data

# Install Python dependencies locally
install:
	pip install -r requirements/dagster.txt
	pip insatll -r requirements/test.txt
	@echo "${GREEN}Dependencies installed${RESET}"

# Run Dagster webserver locally (not in Docker)
dagster-dev:
	dagster dev -w workspace.yaml


##@ Testing

# Run all tests
test:
	pytest tests/ -v --cov=dagster_project --cov=config --cov=src --cov-report=html --cov-report=term
	@echo "${GREEN}Tests completed. View coverage report at htmlcov/index.html${RESET}"

# Run unit tests only
test-unit:
	pytest tests/unit -v
	@echo "${GREEN}Unit tests completed${RESET}"

# Run integration tests only
test-integration:
	pytest tests/integration -v
	@echo "${GREEN}Integration tests completed${RESET}"

# Test Dagster assets
test-assets:
	pytest tests/dagster_project/test_assets.py -v
	@echo "${GREEN}Asset tests completed${RESET}"


##@ Code Quality

# Lint code with ruff and mypy
lint:
	@echo "${YELLOW}Running ruff...${RESET}"
	ruff check dagster_project/ config/ scripts/ src/ tests/
	@echo "${YELLOW}Running mypy...${RESET}"
	mypy dagster_project/ config/ scripts/ src/ tests/
	@echo "${GREEN}Linting completed${RESET}"

# Format code with black and ruff
format:
	@echo "${YELLOW}Formatting with black...${RESET}"
	black dagster_project/ config/ scripts/ src/ tests/
	@echo "${YELLOW}Fixing with ruff...${RESET}"
	ruff check --fix dagster_project/ config/ scripts/ src/ tests/
	@echo "${GREEN}Formatting completed${RESET}"

# Check if code is formatted correctly (for CI)
format-check:
	black --check dagster_project/ config/ scripts/ src/ tests/
	ruff check dagster_project/ config/ scripts/ src/ tests/

# Run type checking with mypy
type-check:
	mypy dagster_project/ config/ scripts/ src/ tests/
	@echo "${GREEN}Type checking completed${RESET}"


##@ Dagster Operations

# Reload Dagster after code changes (alias for restart-dagster)
dagster-reload: restart-dagster

# Open Dagster UI to materialize assets
dagster-materialize:
	@echo "${YELLOW}Opening Dagster UI...${RESET}"
	@echo "Navigate to Assets tab and click 'Materialize selected'"
	@open http://localhost:3000 2>/dev/null || xdg-open http://localhost:3000 2>/dev/null || echo "Open http://localhost:3000 in your browser"


##@ Cleanup

# Remove Dagster log files
clean-logs:
	rm -rf /tmp/dagster/logs/*
	@echo "${GREEN}Dagster logs cleaned${RESET}"

# Remove Python cache files
clean-cache:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "${GREEN}Python cache files removed${RESET}"

# Remove everything (volumes, cache, logs)
clean-all: clean clean-cache clean-logs
	@echo "${GREEN}Complete cleanup finished${RESET}"

# Clean project log files
clean-logfiles:
	rm -rf monitoring/logs


##@ Information

version: ## Show versions of key tools
	@echo "${YELLOW}Tool Versions:${RESET}"
	@echo "Docker:      $$(docker --version)"
	@echo "Docker Compose: $$(docker-compose --version)"
	@echo "Python:      $$(python --version)"
	@echo "Dagster:     $$(dagster --version 2>/dev/null || echo 'Not installed locally')"

urls: ## Show all service URLs
	@echo ""
	@echo "${YELLOW}Service URLs:${RESET}"
	@echo "  Dagster UI:  ${GREEN}http://localhost:3000${RESET}"
	@echo "  MinIO UI:    ${GREEN}http://localhost:9001${RESET} (minioadmin/minioadmin)"
	@echo "  MLflow UI:   ${GREEN}http://localhost:8080${RESET}"
	@echo "  PostgreSQL:  ${GREEN}localhost:5434${RESET} (f1user/f1pass)"
	@echo ""