---
name: github-issue
description: Report confirmed bugs to GitHub Issues via `gh issue create`. Use after a bug is confirmed — not before. Deduplicates against open issues.
---

# GitHub Issue Reporting

## When to Use

After a bug is **confirmed** (reproduced or traced to root cause). Never create an issue for a suspected or unverified bug.

## Step 1 — Deduplicate

```bash
gh issue list --state open --label bug --json number,title --limit 100
```

Compare the candidate title against existing titles. Skip if same root cause already exists. Err on the side of skipping.

## Step 2 — Determine Labels

Primary label: `bug`

Module label (add if applicable): `compiler`, `api`, `otel`, `graphql`, `sql-gen`, `auth`, `ui`

## Step 3 — Create Issue

```bash
gh issue create \
  --title "<concise: what breaks and where>" \
  --body "<see template>" \
  --label "bug,<module-label>"
```

### Body Template

```
## Summary
One sentence: what is broken.

## Steps to Reproduce
1. ...
2. ...

## Expected Behavior
...

## Actual Behavior
...

## Relevant Code
File: `<path>:<line>`
\`\`\`python
<snippet — max 20 lines>
\`\`\`
```

## Step 4 — Report Back

```
Created: #<number> <url> — <one-line summary>
Skipped (duplicate): <title>
```

## Hard Rules

- One issue per distinct root cause — no bundling
- Never create without reading the code first
- If no confirmed bug: output "No confirmed bugs found."
