#!/bin/bash

set -e

echo "=========================================="
echo "  Spark + MinIO + Hive Stack Starter"
echo "=========================================="
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

print_status "Docker is running"

# Detect host architecture and set default platform for builds
HOST_UNAME_S=$(uname -s | tr '[:upper:]' '[:lower:]')
HOST_UNAME_M=$(uname -m)
case "$HOST_UNAME_M" in
  x86_64|amd64)
    DEFAULT_PLATFORM="linux/amd64"
    ;;
  arm64|aarch64)
    DEFAULT_PLATFORM="linux/arm64"
    ;;
  *)
    DEFAULT_PLATFORM="linux/amd64"
    ;;
esac
export DEFAULT_PLATFORM
print_status "Using platform: $DEFAULT_PLATFORM (host: ${HOST_UNAME_S}/${HOST_UNAME_M})"

# Clean up old containers if requested
if [ "$1" == "--clean" ]; then
    print_warning "Cleaning up old containers and volumes..."
    docker-compose down -v
    rm -rf volumes/
    print_status "Cleanup complete"
fi

# Build the custom Spark image
echo
echo "Building custom Spark image with S3A and Hive support..."
DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose build spark-master spark-worker
print_status "Spark image built successfully"

# Start the stack
echo
echo "Starting services..."
DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose up -d

echo
echo "Waiting for services to be healthy..."
echo "This may take 30-60 seconds..."

# Wait for services to be healthy
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    attempt=$((attempt + 1))

    # Check MinIO
    if DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps minio | grep -q "healthy"; then
        minio_status="${GREEN}✓${NC}"
    else
        minio_status="${YELLOW}...${NC}"
    fi

    # Check PostgreSQL
    if DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps postgres | grep -q "healthy"; then
        postgres_status="${GREEN}✓${NC}"
    else
        postgres_status="${YELLOW}...${NC}"
    fi

    # Check Hive Metastore
    if DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps hive-metastore | grep -q "healthy"; then
        hive_status="${GREEN}✓${NC}"
    else
        hive_status="${YELLOW}...${NC}"
    fi

    # Check Spark Master
    if DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps spark-master | grep -q "Up"; then
        spark_status="${GREEN}✓${NC}"
    else
        spark_status="${YELLOW}...${NC}"
    fi

    echo -ne "\r  MinIO: ${minio_status}  PostgreSQL: ${postgres_status}  Hive: ${hive_status}  Spark: ${spark_status}  "

    # Check if all services are healthy
    if DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps minio | grep -q "healthy" && \
       DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps postgres | grep -q "healthy" && \
       DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps hive-metastore | grep -q "healthy" && \
       DEFAULT_PLATFORM="$DEFAULT_PLATFORM" docker-compose ps spark-master | grep -q "Up"; then
        echo
        print_status "All services are healthy!"
        break
    fi

    if [ $attempt -eq $max_attempts ]; then
        echo
        print_error "Services did not become healthy in time. Check logs with: docker-compose logs"
        exit 1
    fi

    sleep 2
done

echo
echo "=========================================="
echo "  Stack is ready!"
echo "=========================================="
echo
echo "Service URLs:"
echo "  • Spark Master UI:    http://localhost:8080"
echo "  • Spark Worker UI:    http://localhost:8081"
echo "  • Spark Jobs UI:      http://localhost:4040 (when job is running)"
echo "  • MinIO Console:      http://localhost:9001 (admin/admin123)"
echo "  • MinIO API:          http://localhost:9000"
echo
echo "Run the test script:"
echo "  ./test.sh"
echo
echo "Run custom scripts:"
echo "  ./submit.sh your_script.py"
echo
echo "View logs:"
echo "  docker-compose logs -f [service_name]"
echo
echo "Stop the stack:"
echo "  docker-compose down"
echo
