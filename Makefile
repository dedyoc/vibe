# Makefile for Bluesky Streaming Pipeline
# Provides convenient commands for managing the Docker Compose stack

.PHONY: help up down logs build clean status health dev-tools

# Default target
help: ## Show this help message
	@echo "Bluesky Streaming Pipeline - Docker Compose Management"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start all services
	docker-compose up -d
	@echo "Services starting... Use 'make logs' to view logs or 'make status' to check health"

down: ## Stop all services
	docker-compose down

logs: ## View logs from all services
	docker-compose logs -f

build: ## Build application services
	docker-compose build

clean: ## Stop services and remove volumes (WARNING: This will delete all data)
	docker-compose down -v --remove-orphans
	docker system prune -f

status: ## Show status of all services
	docker-compose ps

health: ## Check health of all services
	@echo "Checking service health..."
	@echo "Kafka: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9092 || echo "DOWN")"
	@echo "Flink JobManager: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/overview || echo "DOWN")"
	@echo "MinIO: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/minio/health/live || echo "DOWN")"
	@echo "Redis: $$(redis-cli -h localhost -p 6379 ping 2>/dev/null || echo "DOWN")"
	@echo "Prometheus: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/-/healthy || echo "DOWN")"
	@echo "Grafana: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health || echo "DOWN")"
	@echo "Bluesky Producer: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health || echo "DOWN")"
	@echo "Trend Processor: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8001/health || echo "DOWN")"

dev-tools: ## Start development tools (Kafka UI, Redis Commander)
	docker-compose --profile dev-tools up -d kafka-ui redis-commander
	@echo "Development tools started:"
	@echo "  Kafka UI: http://localhost:8080"
	@echo "  Redis Commander: http://localhost:8081"

restart: ## Restart all services
	docker-compose restart

rebuild: ## Rebuild and restart application services
	docker-compose build bluesky-producer trend-processor
	docker-compose up -d bluesky-producer trend-processor

# Service-specific commands
kafka-logs: ## View Kafka logs
	docker-compose logs -f kafka

flink-logs: ## View Flink logs
	docker-compose logs -f flink-jobmanager flink-taskmanager

app-logs: ## View application service logs
	docker-compose logs -f bluesky-producer trend-processor

monitoring-logs: ## View monitoring service logs
	docker-compose logs -f prometheus grafana

# Quick access to service UIs
urls: ## Show URLs for all web interfaces
	@echo "Service URLs:"
	@echo "  Flink Dashboard:    http://localhost:8081"
	@echo "  MinIO Console:      http://localhost:9001 (admin/minioadmin123)"
	@echo "  Prometheus:         http://localhost:9090"
	@echo "  Grafana:           http://localhost:3000 (admin/admin123)"
	@echo "  Bluesky Producer:   http://localhost:8000"
	@echo "  Trend Processor:    http://localhost:8001"
	@echo ""
	@echo "Development Tools (use 'make dev-tools' first):"
	@echo "  Kafka UI:          http://localhost:8080"
	@echo "  Redis Commander:   http://localhost:8081"