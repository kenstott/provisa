---
name: layer-contracts
description: Compiler pipeline stage boundaries, key types at each handoff, and import rules — auto-triggers on any change to provisa/compiler/ or provisa/executor/
---

# Layer Contracts

## Pipeline Stages (in order)

```
GraphQL resolver
  → compile_query(doc: DocumentNode, ctx: CompilationContext) → list[CompiledQuery]
  → inject_rls(compiled: CompiledQuery, ctx: CompilationContext, rls: RLSContext) → CompiledQuery
  → executor (receives governed SQL string + params list)
```

## Key Types at Each Boundary

### Input to `sql_gen.compile_query`

`CompilationContext` (`provisa/compiler/sql_gen.py`, line 101):
- `tables: dict[str, TableMeta]` — root field_name → physical table metadata
- `joins: dict[tuple[str, str], JoinMeta]` — (source_type_name, rel_field_name) → join metadata
- Additional dicts: `column_paths`, `aggregate_columns`, `pk_columns`, `gql_to_physical`, `native_filter_columns`, `virtual_columns`, `gql_json_columns`

`TableMeta` (`provisa/compiler/sql_gen.py`, line 68) — `frozen=True`:
| Field | Type | Notes |
|---|---|---|
| `table_id` | `int` | DB primary key |
| `field_name` | `str` | snake_case GraphQL field |
| `type_name` | `str` | PascalCase GraphQL type |
| `source_id` | `str` | e.g. `"sales-pg"` |
| `catalog_name` | `str` | Trino catalog (hyphens → underscores) |
| `schema_name` | `str` | |
| `table_name` | `str` | post-alias physical name |

### Output of `sql_gen.compile_query`

`CompiledQuery` (`provisa/compiler/sql_gen.py`, line 139):
- `sql: str` — double-quoted identifiers, `$1`-style positional params, no RLS applied yet
- `params: list` — positional param values corresponding to `$N` placeholders
- `columns: list[ColumnRef]` — SELECT list with serialization metadata
- `sources: set[str]` — source_ids involved (used for routing)

### Input to `rls.inject_rls`

```python
inject_rls(compiled: CompiledQuery, ctx: CompilationContext, rls: RLSContext) -> CompiledQuery
```

`RLSContext` (`provisa/compiler/rls.py`, line 29):
- `rules: dict[int, str]` — table_id → raw SQL predicate (e.g. `"tenant_id = 42"`)
- `domain_rules: dict[str, str]` — domain_id → raw SQL predicate
- `RLSContext.empty()` returns zero-rule instance (no-op injection)

`inject_rls` returns a new `CompiledQuery` with the RLS predicates injected into the WHERE clause. Empty rules → SQL unchanged.

### Input to Executor

`CompiledQuery.sql` (governed SQL string) + `CompiledQuery.params` (list).
Executor type: `QueryResult` (`provisa/executor/trino.py`):
- `rows: list` — result tuples
- `column_names: list[str]`

## Contract Tests

`tests/unit/test_layer_contracts.py` — 28 tests covering:
- `TableMeta` required fields: `table_id`, `field_name`, `source_id`, `catalog_name`, `schema_name`, `table_name`
- `CompilationContext` has `tables: dict`, `joins: dict`
- `RLSContext` has `rules: dict[int, str]`, `.empty()` factory
- `QueryResult` has `rows: list`, `column_names: list`
- `CompiledQuery` has `sql: str`, `params: list`
- Round-trip: `compile_query` → `inject_rls` — output is `CompiledQuery`, SQL is non-empty string, RLS rule appears in SQL, empty RLS returns unchanged SQL

Run before any refactor touching these types:
```
pytest tests/unit/test_layer_contracts.py
```

## Import Boundary Contracts (`pyproject.toml` `[tool.importlinter]`)

| Source | Must NOT import |
|---|---|
| `provisa.compiler` | `provisa.executor`, `provisa.api` |
| `provisa.cypher` | `provisa.compiler`, `provisa.executor`, `provisa.api` |

Verify:
```
make lint-imports
```

When a new cross-boundary call is genuinely necessary: add an explicit `[[tool.importlinter.contracts]]` exemption with an inline comment explaining the architectural reason. Never silently bypass.

Note: `sql_gen.py` line 28–32 imports `provisa.api.app.state` inside a function body (not at module level) — this is the one existing intentional exception, scoped to `_get_default_row_limit()`.
