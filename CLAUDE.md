CRITICAL: Never add fallback values or silent error handling. Caused repeated production issues.
CRITICAL: V1 development. Never add migrations.
CRITICAL: Maximum brevity. No pleasantries. No explanations unless asked. Code and facts only.

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