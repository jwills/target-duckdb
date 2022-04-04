venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile .pylintrc target_duckdb/

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=target_duckdb  --cov-fail-under=44 tests/unit -v

integration_test:
	. ./venv/bin/activate ;\
	pytest --cov=target_duckdb  --cov-fail-under=44 tests/integration -v

