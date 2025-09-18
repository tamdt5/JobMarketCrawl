# Makefile for Job Market Crawl with Spark Preprocessing
# Makefile cho dự án Job Market Crawl với tiền xử lý Spark

.PHONY: help install setup start-spark stop-spark run-preprocessing run-pipeline clean test

# Default target
help:
	@echo "Job Market Crawl - Spark Preprocessing Pipeline"
	@echo "=============================================="
	@echo ""
	@echo "Available commands:"
	@echo "  install          - Install Python dependencies"
	@echo "  setup            - Setup environment and check dependencies"
	@echo "  start-spark      - Start Spark cluster (Docker)"
	@echo "  stop-spark       - Stop Spark cluster"
	@echo "  run-preprocessing - Run Spark preprocessing only"
	@echo "  run-pipeline     - Run complete pipeline (Spark + Kafka)"
	@echo "  run-scraper      - Run web scraper to get new data"
	@echo "  clean            - Clean temporary files"
	@echo "  test             - Run tests"
	@echo ""

# Install dependencies
install:
	@echo "Installing Python dependencies..."
	pip install -r requirements.txt
	@echo "Dependencies installed successfully!"

# Setup environment
setup: install
	@echo "Setting up environment..."
	@echo "Checking Python version..."
	python --version
	@echo "Checking Spark installation..."
	python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
	@echo "Checking Kafka Python client..."
	python -c "import kafka; print('Kafka Python version:', kafka.__version__)"
	@echo "Environment setup complete!"

# Start Spark cluster using Docker
start-spark:
	@echo "Starting Spark cluster..."
	cd ../docker && docker-compose up -d spark-master spark-worker
	@echo "Waiting for Spark cluster to be ready..."
	sleep 30
	@echo "Spark cluster started! Master UI: http://localhost:8080"

# Stop Spark cluster
stop-spark:
	@echo "Stopping Spark cluster..."
	cd ../docker && docker-compose stop spark-master spark-worker
	@echo "Spark cluster stopped!"

# Run web scraper
run-scraper:
	@echo "Running web scraper..."
	python vieclamtot_spider.py
	@echo "Scraping completed!"

# Run Spark preprocessing only
run-preprocessing:
	@echo "Running Spark preprocessing..."
	python spark_preprocessing.py
	@echo "Preprocessing completed!"

# Run complete pipeline
run-pipeline:
	@echo "Running complete pipeline (Spark + Kafka)..."
	python spark_ingest_to_kafka.py
	@echo "Pipeline completed!"

# Run pipeline with fresh data
run-full-pipeline: run-scraper run-pipeline
	@echo "Full pipeline with fresh data completed!"

# Clean temporary files
clean:
	@echo "Cleaning temporary files..."
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.log" -delete
	find . -name ".pytest_cache" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleanup completed!"

# Run tests
test:
	@echo "Running tests..."
	python -m pytest tests/ -v || echo "No tests found or pytest not installed"

# Development setup
dev-setup: setup
	@echo "Setting up development environment..."
	pip install pytest black flake8 mypy
	@echo "Development setup completed!"

# Format code
format:
	@echo "Formatting code..."
	black *.py
	@echo "Code formatting completed!"

# Lint code
lint:
	@echo "Linting code..."
	flake8 *.py
	@echo "Linting completed!"

# Type check
type-check:
	@echo "Running type checks..."
	mypy *.py
	@echo "Type checking completed!"

# Check all (format, lint, type-check)
check-all: format lint type-check
	@echo "All checks completed!"

# Monitor Spark cluster
monitor-spark:
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark Worker logs:"
	docker logs spark-worker -f

# Monitor Kafka
monitor-kafka:
	@echo "Kafka topics:"
	cd ../docker && docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Show cluster status
status:
	@echo "=== Docker Services Status ==="
	cd ../docker && docker-compose ps
	@echo ""
	@echo "=== Spark Cluster Status ==="
	curl -s http://localhost:8080/api/v1/applications | python -m json.tool || echo "Spark Master not accessible"

# Quick start (setup + start services + run pipeline)
quick-start: setup start-spark
	@echo "Waiting for services to be ready..."
	sleep 60
	@echo "Running pipeline..."
	make run-pipeline

# Production deployment
deploy-prod:
	@echo "Deploying to production..."
	@echo "Make sure to update spark_config.py for production settings"
	@echo "Starting production services..."
	cd ../docker && docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
	@echo "Production deployment completed!"
