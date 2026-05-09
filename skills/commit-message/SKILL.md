---
name: commit-message
description: Conventional commit message format for this project. Auto-triggers when creating commits.
---

# Commit Message Format

## Structure
```
type: short description (<72 chars)

Body explains WHY, not WHAT. The diff shows what changed.

Refs: #123
```

## Types
- `feat` — new feature
- `fix` — bug fix
- `refactor` — restructure without behavior change
- `test` — add or update tests
- `docs` — documentation only
- `chore` — build, deps, CI, tooling

## Rules
- Subject line: imperative mood, lowercase, no period, <72 chars
- Body: wrap at 100 chars, blank line after subject
- Reference issue numbers when applicable
- No emoji
- One logical change per commit
