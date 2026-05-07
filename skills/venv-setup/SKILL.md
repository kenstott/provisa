---
name: venv-setup
description: Python virtual environment and tooling setup. Auto-triggers when setting up development environment.
---

# Environment Setup

## Python Version
Python 3.12 via `.venv/`

## Setup
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Formatting / Linting
- **ruff** — linter
- **black** — formatter
- Line length: 100
- Target: Python 3.12+

## Verification
```bash
python -m pytest tests/ -x -q        # backend tests
uvicorn main:app --reload             # dev server
```

## Git Hooks
```bash
git config core.hooksPath .githooks
```

Post-commit hook runs canary stamp + Cloudflare deploy.
