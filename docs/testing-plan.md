# Requirements-as-Code: Testing & Requirements Plan

## Overview

Requirements live in a single YAML file (`docs/arch/requirements.yaml`). All other artifacts — markdown documentation, Gherkin feature files, feature matrices, traceability matrices, roadmap — are generated from it. No artifact is hand-edited after generation.

This gives three things simultaneously:
- RFC 2119 normative language for structural/constraint requirements
- Gherkin scenarios for behavioral requirements, driving pytest-bdd
- Machine-verifiable coverage: CI fails if a `MUST` behavioral requirement has no passing test

---

## YAML Schema

File: `docs/arch/requirements.yaml`

```yaml
- id: REQ-001                          # assigned immediately, never reused
  status: complete                     # proposed | accepted | in-progress | complete | rejected
  group: "1. Access Governance & Security"
  category: Query Governance
  priority: MUST                       # RFC 2119: MUST | SHOULD | MAY
  type: behavioral                     # behavioral | structural | constraint | ui | infrastructure
  description: >
    Any authenticated identity can query using any supported language.
    Data returned is governed solely by table/column visibility, RLS, and masking.
  use_case: >
    Removing the query-execution capability gate means governance is expressed
    entirely through data-layer controls, not access lists.
  code:
    - provisa/security/
    - provisa/compiler/
  tests:
    - tests/unit/test_governance.py
    - tests/integration/test_governance_integration.py
  scenario: |
    Given an authenticated identity with role "analyst"
    When a GraphQL query is submitted
    Then data returned is filtered by RLS and masking rules only
    And no capability gate rejects the query
  since: "2026-03"                     # when status moved to complete (complete only)
  target: "2026-Q3"                    # expected delivery (proposed/accepted only)
  rejection_reason: >                  # required when status: rejected
    Deliberate: superseded by REQ-266.
```

### Field rules

| Field | Required | Notes |
|---|---|---|
| `id` | always | Assigned on creation, never reused. Sequential. |
| `status` | always | `proposed` and `rejected` exempt from coverage requirements. |
| `priority` | always | RFC 2119. `MUST` triggers coverage enforcement. |
| `type` | always | Only `behavioral` and `constraint` get `scenario:` fields. |
| `description` | always | |
| `scenario` | if `type: behavioral` and `status` not `proposed\|rejected` | Gherkin Given/When/Then. |
| `rejection_reason` | if `status: rejected` | |
| `since` | if `status: complete` | |
| `target` | if `status: proposed\|accepted` | Quarter format: `2026-Q3`. |

### Status lifecycle

```
proposed → accepted → in-progress → complete
         → rejected
```

IDs are assigned at `proposed`. Requirements are never deleted — `rejected` preserves the audit trail.

---

## Scripts

All scripts live in `scripts/`. All read `docs/arch/requirements.yaml` via the shared Pydantic model at `provisa/tools/req_schema.py`.

| Script | Output | Purpose |
|---|---|---|
| `validate_requirements.py` | exit code | Schema enforcement (see CI section) |
| `gen_requirements_md.py` | `docs/arch/requirements.md` | Human-readable table; never hand-edited |
| `gen_features.py` | `tests/features/REQ-NNN.feature` | One file per behavioral REQ with scenario |
| `gen_step_stubs.py` | `tests/steps/` | Stub step functions for unimplemented steps |
| `gen_feature_matrix.py` | `docs/exports/feature_matrix.csv` | Buyer-facing feature list |
| `gen_traceability_matrix.py` | `docs/exports/traceability_matrix.csv` | Auditor-facing REQ↔test map |
| `gen_roadmap.py` | `docs/exports/roadmap.md` | Proposed + accepted REQs by target quarter |
| `match_rfp.py <file.csv>` | stdout CSV | Fuzzy-matches buyer RFP rows against YAML |

`gen_requirements_md.py --check` and `gen_features.py --check` diff generated output against committed files and exit non-zero on drift — used in CI without writing files.

---

## pytest-bdd Wiring

Generated `.feature` files live in `tests/features/` and are committed (IDE-navigable, grep-friendly). Step definitions live in `tests/steps/` organized by domain:

```
tests/
  features/
    REQ-001.feature
    REQ-002.feature
    ...
  steps/
    governance_steps.py
    auth_steps.py
    compiler_steps.py
    pgwire_steps.py
    ...
  conftest.py           ← configures pytest-bdd feature discovery
```

Unimplemented steps are stubbed with `pytest.skip("step not implemented")` so CI collects all scenarios and reports skipped rather than erroring — gives visibility without blocking.

---

## Agents, Hooks & Skills

### Agents

**`requirements-tracker`** (replaces current)
Appends a structured YAML entry. Infers `type`, `priority`, `status` from description language. Assigns the next sequential ID. Regenerates `requirements.md` after appending.

**`feature-writer`** (new)
Given one or more REQ-IDs, generates `.feature` file(s) and step stubs. Transitions `proposed` → `accepted` if not already. Does not write step implementations.

**`test-strategist`** (modified)
Reads YAML filtered to `type: behavioral`, `status: accepted|in-progress|complete`, `tests: []` as primary input. Reports the component boundary for each uncovered REQ rather than scanning the codebase heuristically.

### Hooks

**PostToolUse on `requirements.yaml` writes**
Runs `validate_requirements.py` then `gen_requirements_md.py` automatically. Keeps markdown in sync without manual intervention. Fails the write if validation errors exist.

**PostToolUse on new test file creation**
Scans new file for `# REQ-NNN` comments. Back-fills `tests:` list in YAML for those REQs. Sets `since:` and `status: complete` on newly-complete requirements.

### Skills

| Skill | Action |
|---|---|
| `/req-audit` | Reports: MUST behavioral with no scenario; complete with no tests; `.feature` files with no step implementation; proposed REQs with no status change in 90+ days |
| `/req-new` | Classifies description, assigns next ID, appends to YAML |
| `/req-accept REQ-NNN` | Transitions `proposed` → `accepted`; optionally sets `target:` |
| `/req-complete REQ-NNN` | Transitions → `complete`; sets `since:` to current month |

---

## CI Gates

Added to every PR and merge to `main`:

1. `validate_requirements.py` — fails on schema violations
2. `gen_requirements_md.py --check` — fails if committed markdown differs from generated
3. `gen_features.py --check` — fails if committed `.feature` files differ from generated
4. `pytest tests/features/` — fails on unmatched scenarios (step functions missing and not stubbed)
5. `validate_requirements.py --coverage-check` — fails if any `MUST complete behavioral` REQ has empty `tests:`

On release tag (additional):

6. `gen_feature_matrix.py` → attached to GitHub Release
7. `gen_traceability_matrix.py` → attached to GitHub Release
8. `gen_roadmap.py` → attached to GitHub Release

---

## Downstream Exports

| Artifact | Audience | Trigger |
|---|---|---|
| `docs/exports/feature_matrix.csv` | Procurement reviewers, buyers | Release tag |
| `docs/exports/traceability_matrix.csv` | Auditors (SOC 2, ISO 27001, FedRAMP) | Release tag |
| `docs/exports/roadmap.md` | Prospects, investors | Release tag |
| RFP response sheet | Specific buyer evaluation | Manual: `match_rfp.py <buyer.csv>` |

The traceability matrix maps every `accepted|in-progress|complete` REQ to its code paths and test files. Test coverage is machine-verifiable against CI run history — a buyer or auditor can confirm claimed features have passing tests, not just checkboxes.

`rejected` REQs with `rejection_reason` document deliberate non-features. When a competitor claims Provisa "doesn't support X," the rejection record shows it was an intentional design decision with a dated rationale.

---

## Phased Delivery

```
Phase 1 (Schema)
    └── Phase 2 (Migration)
            ├── Phase 3 (Generators)
            │       └── Phase 4 (pytest-bdd)
            │               └── Phase 6 (CI)
            └── Phase 5 (Agents/Hooks/Skills)
                        └── Phase 7 (Exports)
```

| Phase | Deliverables | Dependency |
|---|---|---|
| 1. Schema & Validator | `req_schema.py`, `validate_requirements.py` | None |
| 2. Migration | `requirements.yaml` (350 rows migrated) | Phase 1 |
| 3. Generator Scripts | All `gen_*.py` scripts | Phase 2 |
| 4. pytest-bdd Wiring | `tests/features/`, `tests/steps/`, `conftest.py` | Phase 3 |
| 5. Agents/Hooks/Skills | Updated agents, 2 hooks, 4 skills | Phase 2 |
| 6. CI Integration | 5 PR gates + 3 release gates | Phases 3, 4 |
| 7. Downstream Exports | CSV/MD export scripts + release workflow | Phase 3 |

Phases 4 and 5 run in parallel after Phase 3. Phase 7 starts as soon as Phase 3 scripts exist.
