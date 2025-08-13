.PHONY: install dev test lint format clean run docker-build coverage help pre-commit-install pre-commit-run

install:
	pip install -e .

dev:
	pip install -e .[dev]

test:
	pytest -v

test-coverage:
	pytest --cov=src --cov-report=html --cov-report=term-missing

lint:
	ruff check .
	black --check .
	mypy src

format:
	ruff check --fix .
	black .

clean:
	rm -rf dist/ build/ *.egg-info/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage htmlcov/ .pytest_cache/ .ruff_cache/

run:
	collector health

docker-build:
	docker build -t stocktracker-collector .

coverage: test-coverage
	@echo "Coverage report generated in htmlcov/"

pre-commit-install:
	pre-commit install

pre-commit-run:
	pre-commit run --all-files

help:
	@echo "Available commands:"
	@echo "  install      - Install package in development mode"
	@echo "  dev          - Install development dependencies"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage report"
	@echo "  lint         - Check code style and quality"
	@echo "  format       - Format code automatically"
	@echo "  clean        - Clean build artifacts and cache"
	@echo "  run          - Run health check"
	@echo "  docker-build - Build Docker image"
	@echo "  coverage     - Generate coverage report"
	@echo "  pre-commit-install - Install pre-commit hooks"
	@echo "  pre-commit-run     - Run pre-commit on all files"
	@echo "  help         - Show this help message"
