.PHONY: up down restart logs shell test-unit test-int init

up:
	docker-compose up -d --build

down:
	docker-compose down -v

restart:
	docker-compose restart

logs:
	docker-compose logs -f

shell:
	docker exec -it $$(docker-compose ps -q airflow-scheduler) bash

test-unit:
	pytest tests/unit/ -v

test-int:
	pytest tests/integration/ -v

init:
	cp -n .env.example .env || true
	mkdir -p logs plugins data/raw data/minio
	@echo "Edit .env with your Kaggle credentials, then run: make up"
