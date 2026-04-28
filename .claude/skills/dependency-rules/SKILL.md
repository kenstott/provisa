---
name: dependency-rules
description: Package governance, dependency management, and BSL 1.1 license compatibility rules. Auto-triggers when adding or modifying dependencies.
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
- `pip` with `pyproject.toml` only
- No poetry, no conda, no pipenv

## Rules
- No new dependencies without explicit user approval
- Prefer stdlib over third-party when reasonable
- Pin major versions in pyproject.toml

## BSL 1.1 License Compatibility

Provisa is licensed under BSL 1.1. GPL-licensed production dependencies are incompatible pre-Change Date.

### License compatibility
| License | Compatible | Notes |
|---|---|---|
| MIT, BSD, Apache-2.0, UPL-1.0 | Yes | All permissive — no restrictions |
| LGPL | Conditional | Compatible if dynamically linked (Python imports satisfy this) |
| GPL-2.0, GPL-3.0 | No | Incompatible with BSL 1.1 in production artifacts |
| AGPL | No | Incompatible |

### Production vs dev dependency distinction
- **Production dependencies** (`[project.dependencies]`) ship in the artifact — must be license-audited
- **Dev dependencies** (`[project.optional-dependencies] dev`) never ship — no licensing concern
- Test frameworks (pytest, playwright, etc.) are dev-only — not subject to this rule

### Known incompatible packages
- `igraph` — GPL-2.0 — cannot bundle; sideload only
- `leidenalg` — depends on igraph — GPL-2.0 by inheritance — cannot bundle; sideload only

### Native client sideloads (not Python package licensing — distribution restrictions)
- Oracle Instant Client — proprietary Oracle redistribution terms; customer installs separately
- Microsoft ODBC Driver — redistributable under Microsoft terms; customer installs separately
- The Python packages (`oracledb`, `aioodbc`) are permissively licensed and can be bundled
