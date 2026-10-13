# Makefile for Endangered Species Real-Time Lakehouse

.PHONY: up-lite up-full down ps logs test benchmark demo clean

up-lite:
	docker compose -f docker-compose.yml -f docker-compose.lite.yml up -d

up-full:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	pytest -v --cov=phase1.etl_pipeline --cov=phase1.api_extractor --cov=common.rules

benchmark:
	python -m benchmarks.run_benchmark

demo:
	python -m scripts.run_demo

clean:
	docker compose down -v

