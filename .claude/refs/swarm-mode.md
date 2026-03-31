# Swarm Mode (Self-Claim)

All agents operate autonomously. No lead assignment required.

## After completing any task:
1. `TaskList` → find lowest-ID task that is **unblocked** AND **unowned**
2. `TaskUpdate` → set yourself as owner, status "in_progress"
3. Begin work immediately

## Rules:
- Never wait for assignment — self-claim lowest unblocked/unowned task
- If no tasks available, report idle and stop
- Respect module boundaries — only claim tasks in your domain
- Run verification command for your module when done
- After completing a task, loop back to step 1 — always pull next task
- Idle teammates self-claim immediately rather than reporting back to lead

## Spawning Teammates
Include: 1) Which module(s) they own 2) Files to read first 3) Verification command 4) What NOT to touch