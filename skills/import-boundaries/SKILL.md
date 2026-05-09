---
name: import-boundaries
description: Provisa module layering — enforced importlinter contracts, allowed dependency directions, and how to fix violations without suppression flags.
---

# Import Boundaries

## Enforcement

```
make lint-imports   →   lint-contracts
```

Contracts live in `pyproject.toml` under `[tool.importlinter]`.
`root_package = "provisa"`

## Enforced Contracts

| Contract name | Source | Forbidden from importing |
|---|---|---|
| `compiler must not import executor` | `provisa.compiler` | `provisa.executor`, `provisa.api` |
| `cypher must not import compiler` | `provisa.cypher` | `provisa.compiler`, `provisa.executor`, `provisa.api` |

## Derived Dependency Directions

From what is NOT forbidden:

```
provisa.api
  └── can import: compiler, executor, cypher, anything

provisa.executor
  └── can import: compiler, cypher

provisa.compiler
  └── can import: cypher

provisa.cypher
  └── deepest layer — imports nothing above it
```

Layering (high → low):

```
api → executor → compiler → cypher
```

`api` sits above all. `cypher` sits below all.

## Fixing a Violation

Never add `--ignore-dependency` or any suppression flag. Two valid fixes:

### Option A: Move shared code down
If `compiler` needs something from `executor`, the shared piece belongs in a lower layer (`provisa.types`, `provisa.cypher`, or a new `provisa.shared` module). Move it there.

### Option B: Dependency injection via protocol
If the dependency is behavioral (e.g., `compiler` needs to call an executor function):
1. Define a protocol/interface in the lower layer or `provisa.types`
2. Pass the concrete implementation in from `api` or `executor` at call time
3. `compiler` depends only on the protocol, not the concrete module

## Adding a New Contract

1. Add a `[[tool.importlinter.contracts]]` block to `pyproject.toml`:

```toml
[[tool.importlinter.contracts]]
name = "descriptive name"
type = "forbidden"
source_modules = ["provisa.new_module"]
forbidden_modules = ["provisa.api", "provisa.executor"]
```

2. Run `make lint-imports` to verify the contract is recognized and passing.
3. Write an ADR if this changes module boundary design (see `adr` skill).

## Anti-Patterns

| Anti-pattern | Consequence | Fix |
|---|---|---|
| `compiler` imports `executor` for a shared data class | Contract violation | Move data class to `provisa.types` |
| `cypher` imports `compiler` for a utility | Contract violation | Move utility to `provisa.cypher` or lower |
| Circular import between `compiler` ↔ `executor` | Import error + contract violation | Extract shared types to `provisa.types` |
| Adding `--ignore-dependency` to suppress violation | Masks real design problem | Restructure instead |
| Patching with try/except ImportError around a forbidden import | Silent violation at runtime | Restructure instead |

## Shared Types Pattern

If multiple layers need the same data class or constant:

```
provisa/
  types.py        ← shared data classes, enums, protocols
  cypher/         ← imports types.py only
  compiler/       ← imports cypher/, types.py
  executor/       ← imports compiler/, cypher/, types.py
  api/            ← imports everything
```

`provisa.types` has no forbidden imports — it is the shared base.

## Quick Reference

```
# Check contracts
make lint-imports

# See all contracts
grep -A5 'tool.importlinter.contracts' pyproject.toml
```
