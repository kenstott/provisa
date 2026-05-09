---
name: fixture-factory
description: How to build test fixtures for compiler domain objects; auto-triggers when writing tests that need TableMeta, CompilationContext, RLSContext, or JoinMeta.
---

# Fixture Factory

## Problem

Inline `_make_*` helpers are duplicated across test files:
- `tests/unit/test_sql_to_cypher.py` — `_make_simple_ctx_and_label_map`, `_make_prefixed_ctx_and_label_map`
- `tests/unit/test_sql_gen.py` — `_build_schema_and_ctx`

Do not add more inline factories to individual test files. Shared factories belong in `tests/conftest.py` (as pytest fixtures) or `tests/factories.py` (as plain functions for non-fixture use).

## Key Domain Objects

| Class | Module | Mutable? |
|---|---|---|
| `TableMeta` | `provisa.compiler.sql_gen` | No (`frozen=True`) |
| `JoinMeta` | `provisa.compiler.sql_gen` | No (`frozen=True`) |
| `CompilationContext` | `provisa.compiler.sql_gen` | Yes |
| `RLSContext` | `provisa.compiler.rls` | Yes |
| `NodeMapping` | `provisa.cypher.label_map` | — |

## TableMeta Fields (all required unless noted)

From `provisa/compiler/sql_gen.py` lines 68–81:

```python
@dataclass(frozen=True)
class TableMeta:
    table_id: int
    field_name: str       # snake_case GraphQL field name
    type_name: str        # PascalCase GraphQL type name
    source_id: str
    catalog_name: str     # hyphens → underscores of source_id
    schema_name: str
    table_name: str       # physical table name (post-alias)
    domain_id: str = ""   # semantic domain; empty string is valid default
    column_presets: list = field(default_factory=list)
    source_type: str = ""
    original_table_name: str = ""
```

`domain_id` has a default of `""` — it is NOT required. Never pass `domain_id=None`; the field is typed `str`.

## Minimal CompilationContext (one table)

```python
from provisa.compiler.sql_gen import TableMeta, CompilationContext

orders_meta = TableMeta(
    table_id=1,
    field_name="orders",
    type_name="Orders",
    source_id="sales-pg",
    catalog_name="sales_pg",
    schema_name="public",
    table_name="orders",
    domain_id="sales",
)

ctx = CompilationContext()
ctx.tables = {"orders": orders_meta}
ctx.joins = {}
```

`CompilationContext` is a plain `@dataclass` (not frozen) — all dict fields default to `{}` via `field(default_factory=dict)`. You only need to set the fields your test exercises.

## RLSContext

```python
from provisa.compiler.rls import RLSContext

# Empty (no rules)
rls = RLSContext.empty()

# With a table-scoped rule
rls = RLSContext(rules={1: "tenant_id = 'acme'"}, domain_rules={})

# With a domain-scoped rule
rls = RLSContext(rules={}, domain_rules={"sales": "region = 'US'"})
```

## Shared Fixture Pattern (conftest.py)

```python
# tests/conftest.py
import pytest
from provisa.compiler.sql_gen import TableMeta, CompilationContext

@pytest.fixture
def orders_ctx() -> CompilationContext:
    meta = TableMeta(
        table_id=1, field_name="orders", type_name="Orders",
        source_id="sales-pg", catalog_name="sales_pg",
        schema_name="public", table_name="orders", domain_id="sales",
    )
    ctx = CompilationContext()
    ctx.tables = {"orders": meta}
    return ctx
```

Consume in tests via parameter injection — no import needed.

## polyfactory

`polyfactory` is NOT in `pyproject.toml` dev dependencies. Do not use `ModelFactory`. Build instances directly as shown above.

## Anti-Patterns

- `domain_id=None` — field is `str`, not `Optional[str]`; crashes at runtime.
- Adding `_make_*` functions inside a test class or module when the same object is needed in another test file — put it in `conftest.py`.
- Constructing `CompilationContext` with positional args — the dataclass has 9 fields with defaults; always use keyword args.
- Setting `ctx.tables["orders"] = {...}` (dict literal) instead of `TableMeta(...)` — the compiler calls `.table_id`, `.source_id`, etc. as attributes.
- Copying a `_make_*` factory verbatim from another test file instead of extracting it to `conftest.py`.
