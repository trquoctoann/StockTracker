.PHONY: up down logs api collector ui

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f

api:
	$(MAKE) -C StockTracker.API run

collector:
	python -m stocktracker_collector.app health

ui:
	cd StockTracker.UI && npm run dev

