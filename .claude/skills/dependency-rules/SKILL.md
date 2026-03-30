---
name: dependency-rules
description: Package governance and dependency management rules. Auto-triggers when adding or modifying dependencies.
---

# Dependency Rules

## Approved Core Dependencies
- `pydantic>=2` — data validation
- `fastapi` — HTTP server
- `uvicorn` — ASGI server
- `sqlglot` — SQL dialect transpilation
- `graphql-core` — GraphQL AST parsing and validation
- `grpcio` — gRPC server for Arrow Flight
- `pyarrow` — Arrow columnar format

## Banned
- `flask` — use FastAPI
- `django` — use FastAPI
- `strawberry-graphql` — purpose-built compiler, no third-party GraphQL frameworks
- `graphene` — purpose-built compiler, no third-party GraphQL frameworks
- `ariadne` — purpose-built compiler, no third-party GraphQL frameworks

## Package Manager
- `pip` with `pyproject.toml`
- No poetry, no conda, no pipenv

## Rules
- No new dependencies without explicit user approval
- Prefer stdlib over third-party when reasonable
- Pin major versions in pyproject.toml
- Check license compatibility (BSL 1.1 project)
