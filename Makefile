.PHONY: install
install:
	@echo "Installing python dependencies..."
	@pip install -r requirements.txt

.PHONY: reset start-consume create-topic delete-topic remove-migration migrate process-stream setup crawl ingest

# Dừng và khởi động lại toàn bộ hệ thống
reset:
	@echo "Stopping and removing containers..."
	@docker-compose -f infrastructure/docker-compose.yml down
	@echo "Starting containers..."
	@docker-compose -f infrastructure/docker-compose.yml up -d
	@echo "Done!"

# Thiết lập toàn bộ hệ thống (tạo topic, migrate pinot)
setup: reset
	@echo "Waiting 30 seconds for all services to be healthy..."
	@sleep 30
	@make create-topic TOPIC=raw_job_postings
	@make create-topic TOPIC=cleansed_job_postings
	@make migrate
	@echo "✅ System setup complete! Ready for data streaming."

# Xem dữ liệu đang chảy vào Kafka topic
start-consume:
	@echo "Start Consume... Usage: make start-consume TOPIC=<topic_name>"
	@docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic ${TOPIC} --from-beginning

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

# Xóa một Kafka topic
delete-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Error: TOPIC not specified. Usage: make delete-topic TOPIC=<topic_name>"; \
		exit 1; \
	fi
	@echo "Deleting Kafka topic $(TOPIC)..."
	@docker exec kafka kafka-topics --delete --topic $(TOPIC) --bootstrap-server kafka:9092
	@echo "Done!";

# Rule để xóa table/schema cũ trong Pinot
remove-migration:
	@echo "Removing old Pinot table and schema if they exist..."
	@docker exec pinot-controller bin/pinot-admin.sh DeleteTable -tableName cleansed_job_postings -type REALTIME -exec || true
	@docker exec pinot-controller bin/pinot-admin.sh DeleteSchema -schemaName job_postings -exec || true

# Rule migrate bây giờ sẽ tự động xóa cái cũ trước
migrate: remove-migration
	@echo "Applying Pinot schema and table configuration..."
	@docker exec pinot-controller \
		bin/pinot-admin.sh AddTable \
		-tableConfigFile /var/pinot/data/table-config.json \
		-schemaFile /var/pinot/data/schema.json \
		-exec
	@echo "Done!"

# Chạy job Spark Streaming để xử lý dữ liệu
process-stream:
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
	@python scripts/crawl_data.py
	@echo "✅ Crawling complete!"

# Chạy script ingest dữ liệu vào Kafka
ingest:
	@echo "Ingesting data to Kafka topic 'raw_job_postings'..."
	@python scripts/ingest_to_kafka.py
	@echo "✅ Ingestion complete!"

.PHONY: deep-clean
deep-clean:
	@echo "Stopping and removing project containers, volumes, and orphans..."
	@docker-compose -f infrastructure/docker-compose.yml down -v --remove-orphans
	@echo "Pruning all unused Docker system data (containers, networks, volumes)..."
	@docker system prune -a -f --volumes
	@echo "Docker deep clean complete."