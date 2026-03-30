---
name: project-layout
description: Module ownership map and architecture overview. Auto-triggers when navigating or modifying project structure.
---

# Project Layout

## Backend (`provisa/`)
| Module | Purpose | Key Files |
|--------|---------|-----------|
| `api/` | FastAPI app factory, routers | `__init__.py`, `app.py` |
| `compiler/` | GraphQL compiler (SDL generation + query compilation) | `schema.py`, `query.py` |
| `core/` | Config, models, types | `config.py`, `models.py` |
| `executor/` | Query execution, routing, RLS injection | `router.py`, `rls.py` |
| `registry/` | Persisted query registry, approval workflow | `store.py`, `approval.py` |
| `registration/` | Source, table, relationship registration | `source.py`, `table.py`, `relationship.py` |
| `security/` | Auth, column visibility, RLS rules | `auth.py`, `visibility.py` |
| `server/` | Server entry point, middleware | `__init__.py` |
| `transpiler/` | SQLGlot dialect transpilation | `transpile.py` |

## Frontend (`provisa-ui/src/`)
| Module | Purpose |
|--------|---------|
| `components/` | React components (registration, query builder, approval queue) |
| `api/` | HTTP/GraphQL clients |
| `types/` | TypeScript type definitions |

## Entry Points
- Server: `uvicorn main:app --reload`
- Tests: `python -m pytest tests/ -x -q`

## Dependency Graph
- `api/` → `executor/`, `registry/`, `registration/`, `security/`
- `executor/` → `compiler/`, `transpiler/`, `registry/`
- `compiler/` → `registration/` (registration model for SDL generation)
- `transpiler/` → SQLGlot (external)

## Component Stack
```
GraphQL Request
    → Compiler (GraphQL → PG-style SQL)
    → Transpiler (PG SQL → target dialect via SQLGlot)
    → Router (single-source → direct RDBMS, cross-source → Trino)
    → Executor (RLS injection, column security, execution)
    → Response
```
