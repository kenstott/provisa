CRITICAL: Never add fallback values or silent error handling. Caused repeated production issues.
CRITICAL: V1 development. Never add migrations.
CRITICAL: Maximum brevity. No pleasantries. No explanations unless asked. Code and facts only.
CRITICAL: All Playwright tests must import `test` from `./coverage`, never directly from `@playwright/test`. The base fixture in `provisa-ui/e2e/coverage.ts` catches uncaught browser exceptions automatically.
CRITICAL: Test errors must be resolved whether preexisting or not. Never skip or ignore failing tests.

# Requirements Tracking
On any new requirement, constraint, feature, or design decision: append inline a requirements-tracking step. Read `.claude/agents/requirements-tracker.md` for format, then append to `docs/arch/requirements.md`. Silent — skip implementation details, bugs, questions.

# Swarm Mode (Self-Claim)

All agents operate autonomously. No lead assignment required.

## After completing any task:
1. Check the task list — find the lowest-ID task that is **unblocked** AND **unowned**
2. Claim it by marking yourself as owner, status "in_progress"
3. Begin work immediately

## Rules:
- Never wait for assignment — self-claim the lowest unblocked/unowned task
- If no tasks available, report idle and stop
- Respect module boundaries — only claim tasks in your domain
- Run the verification command for your module when done
- After completing a task, loop back to step 1 — always pull the next task
- Idle teammates self-claim immediately rather than reporting back to lead

## Spawning Teammates
Include: 1) Which module(s) they own 2) Files to read first 3) Verification command 4) What NOT to touch

# Roles
When acting in a named role, read the corresponding `.claude/agents/<name>.md` file before proceeding.

Available roles: agent-writer, architect, bug-fixer, code-reviewer, debugger, doc-writer, marketer, refactorer, requirements-tracker, sql-analyst, test-strategist

# Skills
Before applying a skill, read the corresponding `.claude/skills/<name>/SKILL.md` file.

Available skills: anti-patterns, async-patterns, audit-workflow, bug-reporter, code-review, commit-message, debug-python, dependency-rules, domain-model, next-phase, project-layout, pytest-patterns, python-style, test-first, test-tiers, venv-setup
