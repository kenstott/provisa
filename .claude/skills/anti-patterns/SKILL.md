---
name: anti-patterns
description: Prohibited patterns and quality rules. Auto-triggers when writing or reviewing any code.
---

# Anti-Patterns

## Never Do

### Error handling
- No fallback values that mask failures — let errors surface and propagate
- No bare `except:` or `except Exception:` without re-raise or explicit logging
- No silent swallowing of errors in helpers, fixtures, or middleware
- Distinguish "not found" (expected) from "failure" (unexpected) — different error paths

### Tests
- Never remove or skip tests to make the suite pass
- Never use `pytest.skip` for services available in docker-compose
- Never mock a service that docker-compose can provide — mocks belong in unit tests only
- Never write tests after implementation — red first, always

### Code structure
- No files over 1000 lines — split by separation of concerns (applies to Python and TypeScript)
- No magic numbers repeated — extract as named constants
- No mutable default arguments in Python functions

### UI
- "Audit" for a UI feature means browser rendering + functionality — vitest for components, Playwright for e2e. Code review alone is not an audit.
- All Playwright specs must import `test` from `./coverage`, never directly from `@playwright/test`
