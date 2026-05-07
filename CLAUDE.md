CRITICAL: Never add fallback values or silent error handling. Caused repeated production issues. When a value might be missing, find the design guarantee or fix the upstream source — never patch around it.
CRITICAL: V1 development. Never add migrations.
CRITICAL: Maximum brevity. No pleasantries. No explanations unless asked. Code and facts only.
CRITICAL: All Playwright tests must import `test` from `./coverage`, never directly from `@playwright/test`. The base fixture in `provisa-ui/e2e/coverage.ts` catches uncaught browser exceptions automatically.
CRITICAL: Test errors must be resolved whether preexisting or not. Never skip or ignore failing tests.
CRITICAL: Before answering any question about what existing code does, run a Grep or Read tool call first. No exceptions. Do not answer from memory.

# Requirements Tracking
On any new requirement, constraint, feature, or design decision: spawn a background haiku agent. It reads `.claude/agents/requirements-tracker.md` for format, then appends to `docs/arch/requirements.md`. Silent — skip implementation details, bugs, questions.

# Swarm Mode & Teammate Spawning
@.claude/refs/swarm-mode.md
