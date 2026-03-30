CRITICAL: Never add fallback values or silent error handling. This has caused repeated production issues.
CRITICAL: We are currently in version 1 development. Never add migrations.
CRITICAL:  Maximum brevity. No pleasantries. No explanations unless asked. Code and facts only.

# Requirements Tracking
When the user states a new requirement, constraint, feature request, or design decision, spawn a general-purpose agent (model: haiku) in the background to append it to `docs/arch/requirements.md`. The agent should first read `.claude/agents/requirements-tracker.md` for format rules, then read the current requirements file, then append the new requirement(s). Do this silently — no confirmation needed. Do NOT spawn for implementation details, bug reports, or questions.

# Architecture

## Backend (Python — `provisa/`)
| Module | Purpose | Key files |
|--------|---------|-----------|
| `api/` | FastAPI app factory, routers | `__init__.py`, `app.py` |
| `core/` | Config, models, types | `config.py`, `models.py` |
| `server/` | Server entry point, middleware | `__init__.py` |

## Storage
- TBD — define as project evolves

## Key Patterns
- FastAPI with async route handlers
- Pydantic models for request/response validation

# Verification Commands
- Backend tests: `python -m pytest tests/ -x -q`
- Server: `uvicorn main:app --reload`

# Module Boundaries (for parallel work)
- Define as project grows

# Swarm Mode (Self-Claim)

All agents operate autonomously. No lead assignment required.

## After completing any task:
1. Call `TaskList` to see all tasks
2. Find the first task (lowest ID) that is **unblocked** AND has **no owner**
3. Call `TaskUpdate` to set yourself as owner and status to "in_progress"
4. Begin work immediately

## Rules:
- Never wait for assignment — self-claim
- Prefer lowest task ID among unblocked/unowned tasks
- If no tasks available, report idle and stop
- Respect module boundaries (see below) — only claim tasks in your domain
- Run the verification command for your module when done
- After completing a task, loop back to step 1 above — always pull the next task

## TeammateIdle Pattern:
When a teammate finishes and becomes idle, they must immediately self-claim the next available task rather than reporting back to a lead. The lead spawns initial teammates; after that, agents are self-sustaining.

# Teammate Spawn Context
When spawning teammates for parallel work, include:
1. Which module(s) they own (from Module Boundaries above)
2. Specific files to read first
3. Verification command to run when done
4. What NOT to touch (other teammate's modules)