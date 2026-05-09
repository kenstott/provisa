---
name: adr
description: Architecture Decision Records — when to write one, filename format, required sections, and how to wire REQ-NNN traceability so `make coverage-reqs` passes.
---

# Architecture Decision Records (ADRs)

## Location
`docs/arch/adr-NNN-short-title.md`

Same directory as `requirements.md`.

## When to Write an ADR

Write one when the decision involves any of:
- Module boundary change (adds or removes an importlinter contract)
- New third-party dependency added to `pyproject.toml`
- Change to the compiler pipeline (parse → compile → execute path)
- Override or relaxation of an existing importlinter contract
- New storage backend, protocol, or external service dependency

Do NOT write ADRs for: bug fixes, refactors within a single module, test additions, or config tweaks.

## Filename Format

```
adr-NNN-short-title.md
```

- `NNN` is zero-padded to 3 digits: `001`, `002`, `023`
- Short title: lowercase, hyphen-separated, no more than 5 words
- Examples: `adr-001-no-postgraphile-dependency.md`, `adr-012-cypher-layer-isolation.md`

## Required Structure

```markdown
# ADR-NNN: Title

## Status: Proposed | Accepted | Deprecated

## REQ: REQ-NNN

## Context
Why this decision was needed. What problem exists.

## Decision
What was decided. Active voice, one or two sentences.

## Consequences
What changes as a result. Include trade-offs.
```

Rules:
- `Status` must be one of the three listed values — no others
- `REQ:` line must reference at least one `REQ-NNN` from `docs/arch/requirements.md`
- If no existing REQ applies, append a new one to `requirements.md` first

## REQ-NNN Traceability

### How `make coverage-reqs` works

`scripts/coverage_reqs.py`:
1. Reads all `## REQ-NNN` headings from `docs/arch/requirements.md`
2. Scans `tests/unit/`, `tests/integration/`, `tests/e2e/` for `REQ-NNN` string occurrences
3. Exits non-zero if any REQ-NNN has no test file referencing it

**ADR files are not scanned. Only test files count as coverage.**

### Required follow-up after writing an ADR

For every `REQ-NNN` referenced in the ADR:
1. Find the test file(s) listed in the REQ's table row in `requirements.md`
2. Add a comment to that test file: `# REQ-NNN` (top of file or near the relevant test)
3. Run `make coverage-reqs` — must exit 0

### REQ table format in `requirements.md`

```markdown
## REQ-NNN

| REQ-NNN | Category | Description | Use Case | Code | Test |
|---------|----------|-------------|----------|------|------|
```

The `## REQ-NNN` heading is what `coverage_reqs.py` scans. The table row is human context only.

## Checklist Before Committing an ADR

- [ ] File at `docs/arch/adr-NNN-short-title.md`
- [ ] Status is one of: Proposed, Accepted, Deprecated
- [ ] At least one `REQ-NNN` in the `## REQ:` line
- [ ] That REQ exists in `docs/arch/requirements.md`
- [ ] A test file contains `# REQ-NNN` for each referenced REQ
- [ ] `make coverage-reqs` exits 0
- [ ] `make lint-imports` exits 0 if the ADR touches module boundaries
