---
name: snapshot-testing
description: When and how to use syrupy snapshots vs assert_sql_contains for SQL output tests; auto-triggers when writing or reviewing SQL compiler tests.
---

# Snapshot Testing

## Tool Selection

| Situation | Tool |
|---|---|
| Full SQL output — structure and shape both matter | `assert result == snapshot` (syrupy) |
| Fragment presence — survive alias renaming | `assert_sql_contains(sql, fragment)` |
| Regex structural check | `assert_sql_matches(sql, pattern)` |

Both helpers live in `tests/helpers.py`. Import:
```python
from tests.helpers import assert_sql_contains, assert_sql_matches, _normalize_sql
```

## Alias Stability

Generated aliases (`t0`, `t1`, `a2`, `j3`, `sub0`, `cte1`, `n0`) are positional — they change when join order changes.

`_normalize_sql` replaces all matches of `\b(t|a|j|n|sub|cte)\d+\b` (quoted and unquoted) with `__alias__` and collapses whitespace.

**Rule: always call `_normalize_sql(result)` before passing to `assert result == snapshot`.**

```python
def test_join_sql(snapshot):
    sql = compile_query(ctx, gql_ast)
    assert _normalize_sql(sql) == snapshot
```

Skipping normalization makes snapshots break on unrelated compiler refactors.

## Regenerating Baselines

```bash
pytest --snapshot-update tests/unit/test_sql_gen.py
```

- Review the printed diff before committing — syrupy shows old vs new.
- Never run `--snapshot-update` across the whole suite in one pass; scope to the file being changed.
- Committing updated snapshots without reviewing the diff masks regressions silently.

## Snapshot File Location

Syrupy writes `__snapshots__/<test_file>.ambr` next to the test file.

```
tests/unit/
  test_sql_gen.py
  __snapshots__/
    test_sql_gen.ambr
```

Do not hand-edit `.ambr` files. Regenerate via `--snapshot-update`.

## When NOT to Use Snapshots

- The output contains timestamps, UUIDs, or request IDs → normalize first or use `assert_sql_contains`.
- The test only cares that a JOIN clause exists → `assert_sql_contains` is less brittle.
- The compiler is mid-refactor and snapshots would need daily updates → use fragment assertions until output stabilizes.

## Anti-Patterns

- `assert result == snapshot` without `_normalize_sql` — breaks on alias reorder, not on real regressions.
- `--snapshot-update` without reading the diff — silently accepts wrong SQL.
- Snapshotting raw `compile_query` output that includes execution-time data (e.g., RLS injected values from mutable state).
- Using snapshots for error message strings — error text changes often; assert the exception type instead.
