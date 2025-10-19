#!/bin/bash
# Test runner script for F1 Race Prediction project

set -e  # Exit on error

echo "====================================================================="
echo "F1 Race Prediction - Test Suite"
echo "====================================================================="
echo ""
echo "Test configuration:"
echo "  Type: $TEST_TYPE"
echo "  Coverage: $COVERAGE"
echo ""

# Create test bucket in MinIO if it doesn't exist
echo "Setting up test environment..."
python -c "
from minio import Minio
client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
if not client.bucket_exists('f1-test-rwa-data'):
    client.make_bucket('f1-test-rwa-data')
    print('Created test bucket: f1-test-raw-data')
else:
    print('Test bucket exists: f1-test-raw-data')
"

echo ""
echo "====================================================================="
echo "Running tests..."
echo "====================================================================="
echo ""

# Base pytest command
PYTEST_CMD="pytest -v"

# Add coverage if requested
if [ "$COVERAGE" = "yes" ]; then
    PYTEST_CMD="$PYTEST_CMD --cov=src --cov-report=html --cov-report=term"
fi

# Run tests based on type
case $TEST_TYPE in
    unit)
        echo "Running unit tests only..."
        $PYTEST_CMD -m unit tests/unit/
        EXIT_CODE=$?
        ;;
    
    integration)
        echo "Running integration tests only..."
        echo "Note: This will make real API calls and may take several minutes."
        $PYTEST_CMD -m integration tests/integration/
        EXIT_CODE=$?
        ;;
    
    fast)
        echo "Running fast tests only (excluding slow)..."
        $PYTEST_CMD -m "not slow" tests/
        EXIT_CODE=$?
        ;;
    
    schemas)
        echo "Running schema tests only..."
        $PYTEST_CMD tests/unit/test_schemas.py
        EXIT_CODE=$?
        ;;
    
    storage)
        echo "Running storage tests only..."
        $PYTEST_CMD tests/unit/test_storage_client.py
        EXIT_CODE=$?
        ;;
    
    fastf1)
        echo "Running FastF1 client tests only..."
        $PYTEST_CMD tests/unit/test_fastf1_client.py
        EXIT_CODE=$?
        ;;
    
    pipeline)
        echo "Running pipeline tests only..."
        $PYTEST_CMD tests/integration/test_ingestion_flow.py
        EXIT_CODE=$?
        ;;
    
    all)
        echo "Running all tests..."
        $PYTEST_CMD tests/
        EXIT_CODE=$?
        ;;
    
    *)
        echo -e "${RED}Unknown test type: $TEST_TYPE${NC}"
        echo "Usage: $0 [unit|integration|fast|schemas|storage|fastf1|pipeline|all] [yes|no]"
        exit 1
        ;;
esac

echo ""
echo "====================================================================="
echo "Test Results"
echo "====================================================================="
echo ""

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    
    if [ "$COVERAGE" = "yes" ]; then
        echo ""
        echo "Coverage report generated in htmlcov/index.html"
        echo "Open with: open htmlcov/index.html (Mac) or xdg-open htmlcov/index.html (Linux)"
    fi
else
    echo -e "${RED}✗ Some tests failed. Exit code: $EXIT_CODE${NC}"
    echo ""
    echo "Check the output above for details."
    echo "Logs available in: monitoring/logs/test.log"
fi

echo ""
echo "====================================================================="

exit $EXIT_CODE


# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
    fi
}

# Check if Docker is running
echo "Checking prerequisites..."
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running. Please start Docker.${NC}"
    exit 1
fi
print_status 0 "Docker is running"

# Check if MinIO container is running
if ! docker ps | grep -q minio; then
    echo -e "${YELLOW}⚠ MinIO container not running. Starting...${NC}"
    docker-compose -f docker/docker-compose.local.yml up -d minio
    sleep 5
fi
print_status 0 "MinIO is running"

# Parse command line arguments
TEST_TYPE=${1:-all}
COVERAGE=${2:-yes}

echo ""