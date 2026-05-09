---
name: type-coverage
description: Covers pyright standard-mode config, known suppressions, and pre-commit enforcement — auto-triggers when editing Python files, adding type annotations, or resolving type errors
---

# Type Coverage

## Configuration (`pyproject.toml` `[tool.pyright]`)

| Setting | Value | Effect |
|---|---|---|
| `pythonVersion` | `3.12` | Enables 3.12 type narrowing features |
| `typeCheckingMode` | `standard` | Stricter than basic; see below |
| `include` | `["provisa"]` | Only production code checked |
| `exclude` | `["provisa/proto", "**/__pycache__"]` | Proto-generated files excluded |
| `reportMissingImports` | `"warning"` | Proto stubs trigger this; expected, not actionable |
| `reportMissingModuleSource` | `"none"` | Suppresses stubs noise entirely |

## Run

- `make typecheck` → `pyright` (no args; picks up `pyproject.toml` config)
- `pyright provisa` → equivalent explicit form
- Pre-commit hook runs `pyright provisa` on every commit — all errors must be fixed before committing

## What `standard` Mode Checks That `basic` Missed

- **Type narrowing correctness** — incomplete narrowing chains are errors
- **Override compatibility** — subclass method signatures must be compatible with base
- **Return type completeness** — missing return annotations on public functions flagged
- **`__init__` type inference** — attribute types inferred from `__init__` assignments; inconsistent reassignment is an error
- **Unbound variable detection** — stricter path analysis

## Legitimate Suppressions in This Codebase

| Suppression | Where | Reason |
|---|---|---|
| `# type: ignore[override]` | Strawberry resolver subclasses | Strawberry's generated base types are not co-variant-safe |
| `# type: ignore[assignment]` | asyncpg `Record` → `dict` patterns | asyncpg `Record` does not satisfy `dict` protocol; cast is safe at runtime |

Rules for any suppression:
- Must be the narrowest `[error-code]` form — never bare `# type: ignore`
- Must appear on the exact line causing the error
- Add an inline comment explaining why if the reason is not obvious from context

## Anti-Patterns

- **`Any` as a silence mechanism** — adding `Any` to make an error disappear without understanding the root cause violates CLAUDE.md; find the correct type or fix the upstream signature
- **`cast()` without verification** — `cast(X, val)` is a lie to the type checker; use only when you have confirmed at runtime that `val` is `X`
- **Bare `# type: ignore`** — always narrow to `[error-code]`
- **`--ignore-errors` flag** — never pass this to pyright; it defeats the pre-commit hook's purpose

## Expected Warnings (Not Errors)

- `reportMissingImports` warnings on anything under `provisa/proto/` — proto-generated files have no stubs; these are excluded from `include` but may still surface via import chains; treat as noise
