.PHONY: setup lint typecheck test format format-check check

setup:
	uv sync --group dev

lint:
	uv run ruff check .

typecheck:
	uv run mypy

test:
	uv run pytest

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

check: lint typecheck test format-check
