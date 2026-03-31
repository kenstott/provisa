---
name: next-phase
description: Compact context to phase specs only, then implement next phase, then audit.
---

Before implementing, perform a context reduction step:
1. Identify which phase is next from the plan
2. Summarize conversation history down to: phase specifications, interfaces, 
   and decisions only. Discard implementation discussion, error logs, 
   and completed phase output from active context.
3. Implement the next phase
4. Audit the implementation for correctness, completeness, and consistency 
   with prior phases
5. Report: what was implemented, what was audited, any issues found
```

Then instead of typing "implement next phase. audit after completion" you type:
```
/next-phase
