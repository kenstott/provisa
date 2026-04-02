---
name: code-review
description: Structured code review checklist and standards. Auto-triggers during code reviews.
---

# Code Review Checklist

## Review Order
1. Type safety
2. DRY violations
3. Security (OWASP)
4. Error handling

## Type Safety
- Type hints on all public function signatures
- `list[str]` not `List[str]`, `X | None` not `Optional[X]`
- No excessive `Any`
- `isinstance()` not `type()`
- Dataclasses/Pydantic for structured data

## DRY
Flag: identical code blocks (3+ lines, 2+ occurrences), similar code with minor variations, magic numbers/strings repeated.
Suggest: extract function, extract constant, decorator, base class.

## Security (OWASP Top 10)
- **SQL injection**: parameterized queries only (`?` or `:param`), never f-strings
- **Command injection**: `subprocess.run([...], shell=False)`, never `shell=True` with user input
- **Path traversal**: validate user-supplied paths
- **Deserialization**: no `pickle.load()` on untrusted data
- **Secrets**: no hardcoded credentials; `yaml.SafeLoader` only

## Error Handling (CRITICAL)
- No bare `except:` or `except Exception: pass`
- **No fallback values that mask failures** — this is the #1 production issue pattern
- Distinguish "not found" (may return default) from "failure" (must propagate)
- Fallbacks are explicit architectural decisions, not defensive reflexes

## Prompt Review
- Goal: information density, not just brevity
- Critical instructions at start and end (attention U-curve)
- Flag: restated foundational knowledge, filler words, prose that could be bullets
- Token budget: ~3,000 tokens max; estimate chars / 4
- Global instructions stable; turn-specific logic injected dynamically

## UI Audit
When reviewing UI features, audit includes:
- **Vitest**: component rendering and behavior tests
- **Playwright**: e2e browser tests for user-visible workflows
- Flag UI code lacking either test type

## Also Check
- Unused imports/variables
- Functions >50 lines
- **Files >1000 lines — flag for splitting by separation of concerns**
- `print()` that should be `logging`
- `assert` for validation (use explicit checks)
- Global mutable state
- Wildcard imports
