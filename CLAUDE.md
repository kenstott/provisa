CRITICAL: Never add fallback values or silent error handling. Caused repeated production issues. When a value might be missing, find the design guarantee or fix the upstream source — never patch around it.
CRITICAL: V1 development. Never add migrations.
CRITICAL: Maximum brevity. No pleasantries. No explanations unless asked. Code and facts only. Output only what was explicitly requested — no observations, no opinions, no unsolicited notes.
# Brevity examples
# BAD:  "I found the bug — it's on line 42 where the null check is missing. Want me to fix it?"
# GOOD: "Line 42: missing null check. Fix?"
# BAD:  "The feature is already implemented in provisa/compiler/sql_gen.py. Want me to mark it complete?"
# GOOD: "Implemented at provisa/compiler/sql_gen.py:64. Mark complete?"
# BAD:  "I've updated both files to reflect the completed status of the feature."
# GOOD: [no text — the edit tool calls are the answer]
CRITICAL: All Playwright tests must import `test` from `./coverage`, never directly from `@playwright/test`. The base fixture in `provisa-ui/e2e/coverage.ts` catches uncaught browser exceptions automatically.
CRITICAL: Test errors must be resolved whether preexisting or not. Never skip or ignore failing tests.
CRITICAL: Before answering any question about what existing code does, run a Grep or Read tool call first. No exceptions. Do not answer from memory.

# Requirements Tracking
On any new requirement, constraint, feature, or design decision: spawn a background haiku agent. It reads `.claude/agents/requirements-tracker.md` for format, then appends to `docs/arch/requirements.md`. Silent — skip implementation details, bugs, questions.

# Documentation
When features are complete and need documentation, or existing docs are stale: spawn the doc-writer agent (`.claude/agents/doc-writer.md`). It reads source before writing, tags claims as `[tool-verified]` or `[inferred]`, and follows the prose rules in `.claude/refs/prose-quality.md`.

When reviewing inherited READMEs, vendor docs, or PRs with doc changes: spawn the doc-reviewer agent (`.claude/agents/doc-reviewer.md`). It audits for prose quality, accuracy, completeness, and structure — does not rewrite unless asked.

Both agents read `.claude/refs/prose-quality.md` before acting. The PostToolUse prose hook (`hooks/prose-check.py`) blocks writes to `.md` files containing banned AI-smell phrases.

# Bug Reporting
After confirming a bug (reproduced or root-caused): use the `github-issue` skill (`.claude/skills/github-issue/SKILL.md`) to deduplicate and file. Never create an issue for an unverified bug.

# Swarm Mode & Teammate Spawning
@.claude/refs/swarm-mode.md
