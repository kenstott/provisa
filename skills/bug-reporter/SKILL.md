---
name: bug-reporter
description: After confirming a bug's root cause (fix applied or recommended), file a GitHub issue at kenstott/provisa using mcp__github__create_issue.
---

## Trigger

Invoke only when:
- Root cause is confirmed (not speculative)
- Fix is applied or clearly recommended

Do not invoke for: hypotheses, regressions not yet diagnosed, feature gaps.

## Extract from context

| Field | Source |
|---|---|
| Title | One-line imperative: what broke and where |
| Root cause | Exact mechanism — data shape, logic error, wrong variable, etc. |
| Affected files/lines | Absolute paths preferred; line numbers if known |
| Fix | Diff summary, function changed, or recommended change |
| Related bugs | Same class of bug found elsewhere in codebase |
| Notes | Caveats, follow-up risk, migration impact, test gaps |

## Issue body format

Use exactly these sections, in this order:

```
## Bug
<one to three sentences: observable symptom>

## Root Cause
<mechanism: what code did wrong and why>
Affected: `path/to/file.py` (line N)

## Fix
<what was changed or what must be changed>

## Notes
- <caveat or follow-up item>
- <related occurrence elsewhere, if any>
```

Omit `## Notes` only if there is nothing to add.

## File the issue

Call:

```
mcp__github__create_issue(
  owner="kenstott",
  repo="provisa",
  title="<title>",
  body="<formatted body>",
  labels=["bug"]
)
```

## Report back

After the call succeeds, output:

```
Filed: <issue URL>
```

If the call fails, output the error verbatim — no silent handling.
