.PHONY: start stop test lint format clean

# start all services
start:
	docker-compose -f docker/docker-compose.local.yml up -d
	@echo "Services started. Access:"
	@echo "  - MinIO: http://localhost:9001"
	@echo "  - MLflow: http://localhost:8080"
	@echo "  - Postgres: localhost:5434"

# start minio only
start-minio:
	docker-compose -f docker/docker-compose.local.yml up -d minio
	@echo "  - MinIO: http://localhost:9001"

# start postgres only
start-postgres:
	docker-compose -f docker/docker-compose.local.yml up -d postgres
	@echo "  - Postgres: localhost:5434"

# Stop all services
stop:
	docker-compose -f docker/docker-compose.local.yml down

# Run tests
test:
	pytest tests/ -v --cov=src --cov-report=html

# Run unit tests only
test-unit:
	pytest tests/unit -v

# Run integration tests only
test-integration:
	pytest tests/integration -v

# Lint code
lint:
	ruff check src/
	mypy src/

# Format code
format:
	black src/ tests/
	ruff check --fix src/
