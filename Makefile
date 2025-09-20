# COMMAND cài đặt các package "make install" - Định nghĩa biến PYTHON để chọn python3 hoặc python
PYTHON := $(shell command -v python3 >/dev/null 2>&1 && echo python3 || echo python)

.PHONY: install
install:
	@echo "Installing python dependencies..."
	@pip install -r requirements.txt

.PHONY: reset start-consume list-topic create-topic delete-topic remove-migration migrate process-stream setup crawl ingest

## COMMNAD KHỞI TẠO: "make setup"
# Thiết lập toàn bộ hệ thống (tạo topic, migrate pinot)
setup: reset
	@echo "Waiting 30 seconds for all services to be healthy..."
	@sleep 30
	@make create-topic TOPIC=raw_job_postings
	@make create-topic TOPIC=cleansed_job_postings
	@make migrate
	@echo "✅ System setup complete! Ready for data streaming."

# Dừng và khởi động lại toàn bộ hệ thống
reset:
	@echo "Stopping and removing containers..."
	@docker-compose -f infrastructure/docker-compose.yml down
	@echo "Starting containers..."
	@docker-compose -f infrastructure/docker-compose.yml up -d
	@echo "Done!"

# Tạo một Kafka topic mới
create-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Error: TOPIC not specified. Usage: make create-topic TOPIC=<topic_name>"; \
		exit 1; \
	else \
		echo "Creating Kafka topic $(TOPIC)..."; \
		docker exec kafka kafka-topics --create --topic "$(TOPIC)" --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1; \
		echo "Done!"; \
	fi

# Rule migrate bây giờ sẽ tự động xóa cái cũ trước
migrate: remove-migration
	@echo "Applying Pinot schema and table configuration..."
	@docker exec pinot-controller \
		bin/pinot-admin.sh AddTable \
		-tableConfigFile /var/pinot/data/table-config.json \
		-schemaFile /var/pinot/data/schema.json \
		-controllerHost localhost \
		-controllerPort 9000 \
		-controllerProtocol http \
		-exec
	@echo "Done!"

# Rule để xóa table/schema cũ trong Pinot
remove-migration:
	@echo "Removing old Pinot table and schema if they exist..."
	@if docker exec pinot-controller bin/pinot-admin.sh ListTables \
		-controllerHost localhost \
		-controllerPort 9000 \
		-controllerProtocol http \
		-user null \
		-password null \
		-exec | grep -q "job_postings"; then \
		echo "Table job_postings found, deleting..."; \
		docker exec pinot-controller bin/pinot-admin.sh DeleteTable \
			-tableName job_postings \
			-type REALTIME \
			-controllerHost localhost \
			-controllerPort 9000 \
			-controllerProtocol http \
			-user null \
			-password null \
			-exec; \
	else \
		echo "Table job_postings does not exist, skipping deletion."; \
	fi
	@if docker exec pinot-controller bin/pinot-admin.sh ListSchemas \
		-controllerHost localhost \
		-controllerPort 9000 \
		-controllerProtocol http \
		-user null \
		-password null \
		-exec | grep -q "job_postings"; then \
		echo "Schema job_postings found, deleting..."; \
		docker exec pinot-controller bin/pinot-admin.sh DeleteSchema \
			-schemaName job_postings \
			-controllerHost localhost \
			-controllerPort 9000 \
			-controllerProtocol http \
			-user null \
			-password null \
			-exec; \
	else \
		echo "Schema job_postings does not exist, skipping deletion."; \
	fi
	@echo "Remove migration Done!"

## Kết thúc COMMAND khởi tạo 

## Các COMMAND để theo dõi topic và thao tác với kafka topic
# List topic hiện có trong Kafka
list-topic:
	@echo "Topic listing:..."
	@docker exec kafka kafka-topics --list --bootstrap-server localhost:9092                                   
	@echo "Done!"

# Xóa một Kafka topic
delete-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Error: TOPIC not specified. Usage: make delete-topic TOPIC=<topic_name>"; \
		exit 1; \
	fi
	@echo "Deleting Kafka topic $(TOPIC)..."
	@docker exec kafka kafka-topics --delete --topic $(TOPIC) --bootstrap-server kafka:9092
	@echo "Done!";

# Xem dữ liệu đang chảy vào Kafka topic
start-consume:
	@echo "Start Consume... Usage: make start-consume TOPIC=<topic_name>"
	@docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic ${TOPIC} --from-beginning
## Kết thúc COMMAND

# Build spark-master và spark-worker với --no-cache nếu chưa build
build-spark:
	@if docker images | grep -q "infrastructure-spark-master" && docker images | grep -q "infrastructure-spark-worker"; then \
		echo "Spark images already exist, skipping build."; \
	else \
		echo "Building spark-master and spark-worker..."; \
		docker-compose -f infrastructure/docker-compose.yml build --no-cache spark-master spark-worker | tee build.log; \
		echo "Build complete. Log saved to build.log."; \
	fi

# Chạy job Spark Streaming để xử lý dữ liệu
process-stream:
	@make build-spark
	@echo "Waiting for Spark worker to be ready..."
	@timeout=60; \
	while [ $$timeout -gt 0 ]; do \
		if docker exec spark-master curl -f http://spark-master:8080 >/dev/null 2>&1; then \
			echo "Spark master is ready! Checking workers..."; \
			if docker exec spark-master curl -s http://spark-master:8080 | grep -q "ALIVE"; then \
				echo "Spark worker is registered and alive!"; \
				break; \
			fi; \
		fi; \
		echo "Waiting for Spark worker ($$timeout seconds remaining)..."; \
		sleep 5; \
		timeout=$$((timeout - 5)); \
	done; \
	if [ $$timeout -le 0 ]; then \
		echo "Error: Spark worker not ready after 60 seconds."; \
		exit 1; \
	fi
	@echo "Submitting Spark streaming job..."
	@docker exec \
		--user sparkuser \
		--env HADOOP_USER_NAME=sparkuser \
		spark-master spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.jars.ivy=/tmp/ivy \
		/opt/bitnami/spark/scripts/process_with_spark.py

# Chạy script crawl dữ liệu
crawl:
	@echo "Crawling data from vieclamtot.com..."
	@$(PYTHON) scripts/crawl_data.py
	@echo "✅ Crawling complete! Saved multiple jobs_00x.csv and jobs_00x.json files."

# Chạy script ingest dữ liệu vào Kafka
ingest:
	@echo "Ingesting data from all jobs_*.csv files to Kafka topic 'raw_job_postings'..."
	@$(PYTHON) scripts/ingest_to_kafka.py
	@echo "✅ Ingestion complete!"

.PHONY: deep-clean
deep-clean:
	@echo "Stopping and removing project containers, volumes, and orphans..."
	@docker-compose -f infrastructure/docker-compose.yml down -v --remove-orphans
	@echo "Pruning all unused Docker system data (containers, networks, volumes)..."
	@docker system prune -a -f --volumes
	@echo "Docker deep clean complete."