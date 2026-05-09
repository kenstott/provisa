---
name: sql-debug
description: How to compile GraphQL to SQL in tests, assert on output correctly, and diagnose wrong SQL in Provisa's compiler.
---

# SQL Debugging

## `compile_query` Signature (`provisa/compiler/sql_gen.py:1813`)

```python
def compile_query(
    document: DocumentNode,
    ctx: CompilationContext,
    variables: dict | None = None,
    use_catalog: bool = False,
    flat: bool = False,
) -> list[CompiledQuery]:
```

Returns one `CompiledQuery` per root query field in the document.

## `CompiledQuery` Fields (`sql_gen.py:140`)

| Field | Type | Notes |
|---|---|---|
| `sql` | `str` | Final SQL string with `$1`-style positional params |
| `params` | `list` | Positional parameter values in `$N` order |
| `root_field` | `str` | GraphQL field name (alias if present) |
| `columns` | `list[ColumnRef]` | Output column metadata |
| `sources` | `set[str]` | `source_id`s involved (for routing) |
| `nodes_sql` | `str \| None` | Aggregate queries only — plain SELECT for `nodes` subfield |
| `nodes_params` | `list` | Params for `nodes_sql` |
| `result_limit` | `int \| None` | Python-level row cap when LATERAL ops joins are present |

There is no `.semantic_sql` or `.compiled_cypher` attribute on `CompiledQuery`. Those do not exist in this class.

## SQL Assertion Helpers (`tests/helpers.py`)

### `_normalize_sql(sql)`

Replaces generated numeric aliases before comparison:

- Regex `\b(t|a|j|n|sub|cte)\d+\b` → `__alias__` (unquoted)
- Regex `"(t|a|j|n|sub|cte)\d+"` → `__alias__` (quoted)
- Collapses all whitespace runs to a single space

Alias prefixes covered: `t`, `a`, `j`, `n`, `sub`, `cte` (case-insensitive).

### `assert_sql_contains(sql, fragment)`

Both `sql` and `fragment` are normalized before the `in` check. Failure message prints both normalized forms.

```python
# Wrong — alias number is position-dependent
assert '"t0"."id"' in results[0].sql

# Right — table name is stable
assert_sql_contains(results[0].sql, '"orders"."id"')
```

### `assert_sql_matches(sql, pattern)`

Same normalization as `assert_sql_contains`, then `re.search(pattern, norm_sql, re.IGNORECASE)`.

Use when the fragment has variable whitespace or optional clauses.

## Parameterized SQL

The compiler emits `$1`, `$2`, … placeholders — never literal values in the SQL string.

```python
# Wrong
assert "LIMIT 10" in results[0].sql

# Right
assert "LIMIT $1" in results[0].sql
assert results[0].params[0] == 10
```

## `flat` Parameter

| `flat` | Join strategy for one-to-many relationships |
|---|---|
| `False` (default) | `ARRAY_AGG` subquery — one row per parent |
| `True` | `LATERAL` join — one row per child (flat row set) |

`flat=True` is forced automatically when any field has `where`, `order_by`, `limit`, or `offset` args (`_has_nested_db_args` check inside the compiler). Check this before concluding `flat` is the wrong setting.

## Diagnosing Wrong SQL

1. Print `results[0].sql` raw first to see actual alias numbers and structure before normalization strips them.
2. Confirm `flat` matches the route under test — the `/graphql` REST endpoint passes `flat=True`; direct compiler unit tests default to `flat=False`.
3. If a field is missing from the SELECT list, check `columns` — the compiler skips columns not in `ctx.tables[field_name]`.
4. If a JOIN is missing, check `sources` — cross-source queries may be split into multiple `CompiledQuery` results (one per source).
5. For aggregate queries, `sql` is the aggregation SELECT; `nodes_sql` is the rows SELECT. Assert on both when testing `_aggregate` fields.

## Reproducing a Compiler Regression as a Unit Test

```python
# tests/unit/test_sql_gen.py
from graphql import build_schema, parse
from provisa.compiler.sql_gen import compile_query, CompilationContext

def test_regression():
    schema = build_schema(SCHEMA_SDL)
    document = parse("{ orders(limit: 5) { id status } }")
    ctx = CompilationContext(...)   # build from fixture or minimal inline dict
    results = compile_query(document, ctx, flat=True)
    assert len(results) == 1
    assert_sql_contains(results[0].sql, '"orders"."status"')
    assert "LIMIT $1" in results[0].sql
    assert results[0].params == [5]
```

Copy the GraphQL query and fixture tables from the failing e2e test verbatim. Use `flat=True` if the original failure came through the REST `/graphql` route.

## Anti-Patterns

- **Never assert raw alias tokens** (`"t0"`, `"a1"`). Use `assert_sql_contains` or `assert_sql_matches`.
- **Never assert literal parameter values in the SQL string.** Parameterized queries use `$N`; the value is in `params[N-1]`.
- **Never add fallback values or silent error handling to the compiler.** If a field is missing, raise — never default to empty string or `None` silently. (CLAUDE.md: CRITICAL)
