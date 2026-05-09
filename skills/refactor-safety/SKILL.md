---
name: refactor-safety
description: Pre/post checklist and scope rules for safe refactors in the compiler pipeline — auto-triggers on any proposed rename, move, or restructure of provisa/compiler/ files
---

# Refactor Safety

## Pre-Refactor Checklist (must all pass before touching code)

1. `pytest tests/unit/test_layer_contracts.py` — green
2. `make lint-imports` — zero boundary violations
3. Locate snapshot files: `tests/unit/__snapshots__/` — if your change touches snapshot-protected tests, plan to run with `--snapshot-update` and manually diff every changed snapshot
4. For compiler changes where time allows: `mutmut run --paths-to-mutate provisa/compiler/` — review surviving mutants before proceeding

## Safe Refactor Scope

These are low-risk without additional contract work:
- Rename a symbol **within** a single module (no cross-module callers)
- Extract a private helper function (`_foo`) within a module
- Simplify a condition or early-return without changing the function signature
- Add a new optional field with a default to a dataclass (append only — never reorder or remove)

## Unsafe Without Contracts Green First

Any change to these layer-boundary types requires `test_layer_contracts.py` green before AND after:

| Type | Module |
|---|---|
| `TableMeta` | `provisa/compiler/sql_gen.py:68` |
| `CompilationContext` | `provisa/compiler/sql_gen.py:101` |
| `JoinMeta` | `provisa/compiler/sql_gen.py:84` |
| `CompiledQuery` | `provisa/compiler/sql_gen.py:139` |
| `ColumnRef` | `provisa/compiler/sql_gen.py:128` |
| `RLSContext` | `provisa/compiler/rls.py:29` |
| `QueryResult` | `provisa/executor/trino.py` |

Renaming a field on any of these: update the contract test in the same commit.

## Cross-Boundary Moves

Moving a module that creates a new import path across a `[tool.importlinter]` boundary:
1. Update `pyproject.toml` `[[tool.importlinter.contracts]]` in the same commit
2. Add a comment to the exemption explaining the architectural justification
3. Run `make lint-imports` again after the update

Current boundaries:
- `provisa.compiler` must not import `provisa.executor` or `provisa.api`
- `provisa.cypher` must not import `provisa.compiler`, `provisa.executor`, or `provisa.api`

## Post-Refactor Verification

```
pytest tests/unit/ -q
```

All 45+ unit tests must pass. No skips, no xfails added to paper over failures.

If snapshot tests fail unexpectedly:
```
pytest tests/unit/ --snapshot-update -q
```
Then `git diff tests/unit/__snapshots__/` — review every line before committing.

## Anti-Patterns

- **Never** fix a cross-boundary import violation by adding the import to importlinter's forbidden list as an allow — that removes the guard entirely. Add a scoped exemption with justification instead.
- **Never** move a module and repair broken imports by wiring `provisa.compiler → provisa.executor`. Fix the design: either keep the module in its current layer or introduce an abstraction at the boundary.
- **Never** delete or skip a contract test to make a rename "work". Rename the test to match the new name.
- **Never** add fallback values to handle missing fields on boundary types — find the upstream source or fix the design guarantee (per CLAUDE.md).
