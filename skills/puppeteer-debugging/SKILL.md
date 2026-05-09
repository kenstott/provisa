---
name: puppeteer-debugging
description: When and how to use the MCP puppeteer tools for UI verification and debugging. Distinct from writing Playwright e2e tests. Auto-triggers when verifying UI changes, debugging frontend regressions, or checking that a feature looks/works correctly in the browser.
---

# Puppeteer MCP — UI Verification and Debugging

## Two distinct tools — don't confuse them

| | Puppeteer MCP tools | Playwright e2e tests |
|---|---|---|
| **What** | Claude's own live browser session | Automated test suite in `provisa-ui/e2e/` |
| **When** | Verify a UI change looks correct right now | Permanent regression coverage |
| **Who runs it** | Claude, inline during a task | CI + `npx playwright test` |
| **Output** | Screenshot / DOM state returned to Claude | Pass/fail in test report |
| **State** | Ephemeral — browser closes after session | Repeatable from cold start |

## When to use puppeteer MCP

Use it whenever you cannot verify a UI change by reading code alone:

- After editing a React component — screenshot it to confirm the layout is right
- When a user reports "X looks broken" — navigate and screenshot before diagnosing
- After fixing a CSS/styling bug — confirm the fix visually
- When writing a new Playwright spec — use puppeteer first to find the right selectors, then encode them in the spec
- When a Playwright test fails with "element not found" — use puppeteer to inspect what's actually in the DOM

**Do not** just report "the code looks correct" after a frontend change without at least one puppeteer screenshot of the golden path.

## Prerequisites — dev server must be running

Puppeteer MCP navigates to `http://localhost:3000`. The Vite dev server must be running:

```
# In provisa-ui/
npm run dev
```

Check first:
```javascript
// mcp__puppeteer__puppeteer_navigate
{ url: "http://localhost:3000" }
// if it times out or shows "connection refused", the dev server is not running
// tell the user: "! npm run dev" in provisa-ui/
```

The backend (uvicorn) must also be running for API calls to work — `http://localhost:8000`.

## Core workflow

### 1. Navigate
```javascript
mcp__puppeteer__puppeteer_navigate({ url: "http://localhost:3000/admin" })
```

### 2. Screenshot to see current state
```javascript
mcp__puppeteer__puppeteer_screenshot({ name: "admin-page-before" })
```

### 3. Interact
```javascript
// Click
mcp__puppeteer__puppeteer_click({ selector: "button[data-testid='save']" })

// Fill input
mcp__puppeteer__puppeteer_fill({ selector: "input[name='threshold']", value: "5000" })

// Select dropdown
mcp__puppeteer__puppeteer_select({ selector: "select#role", value: "admin" })

// Hover (for tooltips)
mcp__puppeteer__puppeteer_hover({ selector: ".info-icon" })
```

### 4. Screenshot after to verify result
```javascript
mcp__puppeteer__puppeteer_screenshot({ name: "admin-page-after-save" })
```

### 5. Evaluate JS to inspect DOM state
```javascript
mcp__puppeteer__puppeteer_evaluate({
  script: "document.querySelector('.stat-card').textContent"
})

// Check for errors in console
mcp__puppeteer__puppeteer_evaluate({
  script: "window.__provisa_errors__ ?? []"
})
```

## Selector strategy — prefer in this order

1. `data-testid` attributes: `[data-testid="submit-btn"]`
2. ARIA roles: `button[aria-label="Save"]`
3. Text content (via evaluate): `document.querySelector('button:has-text("Save")')` — but only in evaluate, not in click/fill
4. CSS class: `.stat-card` — last resort, fragile

To find a selector when unsure:
```javascript
mcp__puppeteer__puppeteer_evaluate({
  script: `
    [...document.querySelectorAll('button')].map(b => ({
      text: b.textContent.trim(),
      testId: b.dataset.testid,
      class: b.className
    }))
  `
})
```

## Provisa-specific routes

| Route | Page |
|---|---|
| `/` | Landing / role selector |
| `/admin` | Admin dashboard |
| `/graphiql` | GraphiQL + Provisa plugin panel |
| `/sql` | SQL query runner |
| `/graph` | Cypher / graph explorer |
| `/sources` | Source management |
| `/tables` | Table management |
| `/relationships` | Relationship management |
| `/roles` | Role management |
| `/approvals` | Governed query approvals |
| `/schema` | Schema explorer |

## Mocks vs live backend

Playwright e2e tests call `setupMocks(page)` from `e2e/mocks.ts` to intercept GraphQL calls. Puppeteer MCP hits the **live backend** — so:

- If the backend is running with real data, you see real data
- If you need predictable state for debugging, navigate to the page and call `setupMocks` manually via evaluate (not practical — just be aware the data will vary)

## After using puppeteer for selector discovery

Once you've found the right selectors via puppeteer, encode them permanently:

1. Add `data-testid` attributes to the component if none exist
2. Write or update the Playwright spec in `provisa-ui/e2e/`
3. Import `test` from `./coverage`, never from `@playwright/test` directly
4. Run: `cd provisa-ui && npx playwright test <spec-file> --headed` to verify

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| Navigate returns immediately, screenshot is blank | Dev server not running | `! npm run dev` in provisa-ui/ |
| Click does nothing | Wrong selector / element not in viewport | Use evaluate to inspect DOM; scroll into view |
| GraphQL data missing | Backend not running or not seeded | Check `http://localhost:8000/health` |
| Page shows login redirect | Auth cookie expired | Navigate to `/` first and select a role |
