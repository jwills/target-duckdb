.PHONY: venv pylint unit_test integration_test test clean

venv:
	uv sync --extra test

pylint:
	uv run pylint target_duckdb/

unit_test:
	uv run pytest tests/unit -v

integration_test:
	uv run pytest tests/integration -v

test: unit_test integration_test

clean:
	rm -rf .venv dist *.egg-info .pytest_cache .coverage
