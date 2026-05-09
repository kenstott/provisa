---
name: security-review
description: Covers bandit, pip-audit, and Provisa-specific security patterns — auto-triggers on any new dependency, SQL/RLS expression construction, JWT handling, or subprocess usage
---

# Security Review

## Bandit (pre-commit)

- Hook: `bandit -r provisa -ll`
- `-ll` = report LOW severity + LOW confidence and above (catches everything)
- Runs automatically on commit via `.pre-commit-config.yaml` rev `1.8.3`
- Run locally before pushing: `bandit -r provisa -ll`
- Fix all findings; do not accumulate suppressions

## pip-audit

- Run: `make pip-audit`
- Required before adding any new dependency to `pyproject.toml`
- Required before bumping an existing dependency version
- A clean audit output is a prerequisite for merging dependency changes

## Provisa-Specific Patterns — Always Flag

### SQL injection
- Never concatenate user input or dynamic values into `compile_query`, query executor calls, or raw SQL strings
- Use `$1`, `$2`, … positional placeholders with asyncpg parameter binding
- sqlglot AST construction is acceptable; string concatenation is not

### RLS filter expressions
- RLS filter strings are config-defined, never user-supplied at runtime
- Must be injected via sqlglot AST transformation only — never `eval`'d, never interpolated into a query string

### JWT secrets
- Secret must be sourced from environment variable only
- Never hardcode a JWT secret or signing key anywhere in `provisa/`
- `PyJWT[crypto]` is the approved library (already in `pyproject.toml`)

### subprocess
- Approved pattern only: `subprocess.run(["docker", "compose", ..., "up", "<single-service>", "-d"], ...)`
- Single named service — never `up` with no service argument (starts entire stack)
- `shell=True` is never permitted
- Any other subprocess invocation requires explicit justification and security review

## False Positive Suppressions

- `# nosec B101` — permitted only for `assert` statements in test files under `tests/`
- Never use `# nosec` in `provisa/` production code without a written justification in the same PR

## Checklist

- [ ] `bandit -r provisa -ll` clean
- [ ] `make pip-audit` clean (if dependencies changed)
- [ ] No SQL string concatenation
- [ ] No RLS expression eval
- [ ] JWT secret from env only
- [ ] subprocess: single service, no `shell=True`
- [ ] `# nosec` only in test files, only for B101
