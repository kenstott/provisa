CRITICAL: Never add fallback values or silent error handling. Caused repeated production issues.
CRITICAL: V1 development. Never add migrations.
CRITICAL: Maximum brevity. No pleasantries. No explanations unless asked. Code and facts only.
CRITICAL: Files must stay under 1000 lines. If a file approaches or exceeds this, split it by separation of concerns. This applies to all languages (Python, TypeScript, etc.).
CRITICAL: "Audit" for UI features must include browser rendering and functionality testing (vitest + Playwright), not just code review.
CRITICAL: All Playwright tests must catch uncaught browser exceptions (pageerror). The base fixture in `provisa-ui/e2e/coverage.ts` handles this automatically — all specs must import `test` from `./coverage`, never directly from `@playwright/test`.
CRITICAL: Test errors must be resolved whether they are preexisting or not. Never skip or ignore failing tests.

# Code Audits
When asked to audit code against a spec, requirements, or standards: spawn parallel Explore subagents (split by phase/module range) to compare implementation against the spec. Gather results, then synthesize into a single report categorized as: completed to spec, not added, added but incomplete, added but not to spec.

# Requirements Tracking
On new requirement/constraint/feature/design decision: spawn background haiku agent to append to `docs/arch/requirements.md`. Agent reads `.claude/agents/requirements-tracker.md` for format, then current file, then appends. Silent. Skip implementation details, bugs, questions.

# Architecture
## Backend (`provisa/`)
| Module | Purpose |
|--------|---------|
| `api/` | FastAPI app factory, routers |
| `core/` | Config, models, types |
| `server/` | Entry point, middleware |

Patterns: FastAPI async handlers, Pydantic validation. Storage TBD.

# Verification
- Tests: `python -m pytest tests/ -x -q`
- Server: `uvicorn main:app --reload`

# Module Boundaries
Define as project grows.

# Swarm Mode & Teammate Spawning
@.claude/refs/swarm-mode.md