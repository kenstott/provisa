.PHONY: typecheck lint lint-fix test test-unit pip-audit lint-imports sync-reqs screenshots

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

sync-reqs:
	python scripts/gen_requirements_md.py
	python scripts/gen_features.py
	python scripts/gen_step_stubs.py
	python scripts/gen_feature_matrix.py
	python scripts/gen_traceability_matrix.py
	python scripts/gen_roadmap.py

coverage-reqs:
	python scripts/coverage_reqs.py

screenshots:
	cd provisa-ui && npm run screenshots
