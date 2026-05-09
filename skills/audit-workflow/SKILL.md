---
name: audit-workflow
description: Code audit workflow using parallel subagents. Auto-triggers when asked to audit code against a spec, requirements document, or standards.
---

# Audit Workflow

When asked to audit code against a spec, requirements, or standards:

1. **Split** the scope by phase or module range into independent segments
2. **Spawn parallel Explore subagents** — one per segment — each comparing implementation against the spec
3. **Gather** all subagent results
4. **Synthesize** into a single report categorised as:
   - **Completed to spec** — implemented and matches requirements
   - **Not added** — required but missing entirely
   - **Added but incomplete** — partially implemented
   - **Added but not to spec** — implemented differently than specified

## Notes

- Split segments so subagents can work independently with no overlap
- Each subagent receives: the spec section it covers + the relevant code paths to examine
- The synthesis step is done in the main context after all subagents complete
- For UI features: audit must include vitest coverage and Playwright e2e — code review alone is not sufficient
