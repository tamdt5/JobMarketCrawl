.PHONY: reset
reset:
	@echo "Stopping and removing containers..."
	@docker-compose down
	@echo "Starting containers..."
	@docker-compose up -d
	@echo "Done!"

.PHONY: start-consume
start-consume:
	@echo "Start Consume... (default: crawl_res)"
	@docker exec docker-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic crawl_res --from-beginning
	@echo "Done!"

.PHONY: list-topic
list-topic:
	@echo "Topic listing:..."
	@docker exec docker-kafka-1 kafka-topics --list --bootstrap-server localhost:9092                                   
	@echo "Done!"

.PHONY: create-topic
create-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Error: TOPIC not specified. Usage: make create-topic TOPIC=<topic_name>"; \
		exit 1; \
	else \
		echo "Creating Kafka topic $(TOPIC)..."; \
		docker exec docker-kafka-1 kafka-topics --create --topic "$(TOPIC)" --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1; \
		echo "Done!"; \
	fi

PHONY: delete-topic
delete-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Error: TOPIC not specified. Usage: make delete-topic TOPIC=<topic_name>"; \
		exit 1; \
	fi
	@echo "Deleting Kafka topic $(TOPIC)..."
	@docker exec docker-kafka-1 kafka-topics --delete --topic $(TOPIC) --bootstrap-server kafka:9092
	@echo "Done!";

PHONY: remove-migration
remove-migration:
	@echo "Remove old migrate" 
	@docker exec pinot-controller bin/pinot-admin.sh DeleteTable -tableName crawl_res -exec 
	 docker exec pinot-controller bin/pinot-admin.sh DeleteSchema -schemaName crawl_res -exec 
	@echo "Done!"

# hardcode: /var/pinot/data/schema.json
PHONY: migrate 
migrate:
	@make remove-migration
	@echo "Migration..."
	@docker exec -it pinot-controller \
		bin/pinot-admin.sh AddTable \
		-tableConfigFile /var/pinot/data/table-config.json \
		-schemaFile /var/pinot/data/schema.json \
		-controllerHost localhost \
		-controllerPort 9000 \
		-exec
	@echo "Done!"

# reset -> start-consume -> create-topic (if not exist) -> migrate