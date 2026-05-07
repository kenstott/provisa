.PHONY: typecheck lint lint-fix test test-unit pip-audit lint-imports

typecheck:
	pyright

lint:
	ruff check provisa tests

lint-fix:
	ruff check --fix provisa tests

test:
	pytest

test-unit:
	pytest tests/unit -p no:cov

pip-audit:
	pip-audit

lint-imports:
	lint-contracts

coverage-reqs:
	python scripts/coverage_reqs.py
