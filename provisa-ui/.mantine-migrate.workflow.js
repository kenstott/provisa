export const meta = {
  name: 'mantine-ui-migrate',
  description: 'Migrate every provisa-ui page/component to Mantine + i18n + a11y, migrating tests, in parallel',
  phases: [
    { title: 'Migrate', detail: 'one agent per file: Mantine swap + i18n + a11y + test migration' },
    { title: 'Verify', detail: 're-run each migrated file’s test to confirm green' },
  ],
}

const WT = '/Volumes/main/Users/kennethstott/PycharmProjects/provisa-ui-upgrade/provisa-ui'
const files = Array.isArray(args) ? args : []
log(`Migrating ${files.length} files across the provisa-ui tree`)

const RULES = `
You are migrating ONE file in the provisa-ui React app to the new UI system on branch ui-upgrade.
CRITICAL: All work happens in this package directory: ${WT}
Every path below is relative to ${WT}. When you Read/Edit/Write, use the ABSOLUTE path ${WT}/<relative-path>. When you run tests: cd ${WT} && npx vitest run <test-file>. Do NOT touch the copy of this repo at any other path.

GOAL for your file: replace hand-rolled HTML/CSS UI with Mantine components, extract user-facing strings to i18n, meet WCAG 2.1 AA, and migrate its test to role/text selectors. Keep behavior identical and PUBLIC EXPORTS/PROPS unchanged.

HARD CONSTRAINTS (violating these breaks other parallel agents):
- Edit ONLY these paths: (a) your target file, (b) its co-located test file if one exists, (c) a NEW locale file src/i18n/locales/en/<namespace>.json that you own.
- NEVER edit: src/App.css, src/index.css, src/i18n/locales/en.json, src/i18n/index.ts, src/theme/*, src/test-utils/*, src/test-setup.ts, package.json, any other component/page, or any file another agent might own.
- Do NOT run 'npm run typecheck' or 'npm run build' (project-wide, races other agents). You MAY run 'npx vitest run <your-test-file>' only.

HOW TO MIGRATE:
- Read these already-migrated references first for the exact pattern: src/components/admin/LocalUsersTab.tsx (table+form+select+notifications), src/components/ConfirmDialog.tsx (Modal), src/components/RoleSelector.tsx (Menu), src/components/MultiSelect.tsx (wrapper preserving API).
- Read src/theme/theme.ts and src/theme/tokens.css for tokens. Prefer Mantine components (Button, TextInput, PasswordInput, Select, MultiSelect, Table, Modal, Menu, Group, Stack, Badge, Tabs, Pagination, ActionIcon, Alert, Text, Title, Checkbox, Switch, Tooltip, Card, etc.). Keep existing var(--token) references working; do not hardcode hex.
- i18n: import { useTranslation } from 'react-i18next'; replace literal user-facing strings with t('<namespace>.<key>'). Put the keys in src/i18n/locales/en/<namespace>.json as { "<namespace>": { "<key>": "<English text>" } }. Choose <namespace> = camelCase of the component/file name; it MUST be globally unique (auto-merged via glob). Some basenames repeat across directories (ResultsPanel, JoinCanvas, CanvasTableCard exist under both src/pages/sql and src/components/sql-modeling) — if your basename is not unique, prefix with the parent dir (e.g. sqlResultsPanel vs sqlModelingResultsPanel) so the locale filename and namespace never collide with another agent's.
- a11y: dialogs use Mantine Modal (role=dialog, focus-trap). Menus/dropdowns use Mantine Menu. Every icon-only control gets aria-label. Inputs get labels. Selected menu items get aria-current. Add data-testid to key interactive controls.

TEST MIGRATION (only if a test file already exists for your file):
- Import { render, screen, ... } from the provider helper: relative path to src/test-utils/render (it wraps MantineProvider + i18n). Do NOT import from '@testing-library/react' directly.
- Replace CSS-class selectors (locator/querySelector('.x')) and literal-text assertions with getByRole + data-testid, and for text use the i18n value via i18n.getFixedT('en').
- Mantine Modal/Menu mount async in jsdom: use findBy* for opened content; for Menu items query by TEXT within role="menu" (within(await screen.findByRole('menu')).getByText(...)), NOT by accessible name; add transitionProps={{ duration: 0 }} to Menu. Modal content asserted via findByRole('dialog').
- Run 'npx vitest run <test-file>' until green.
- If NO test file exists, do NOT create one; note it as a followup.

SKIP (return action=skipped) if the file is: a pure data-visualization/canvas renderer (cytoscape/SVG/d3 canvas, e.g. GraphCanvas, *SvgOverlay, GraphIcons, JoinCanvas, NodeRingMenuOverlay), a CodeMirror/Monaco editor wrapper, or has no user-facing UI to restyle. Give the reason.

Return the structured result. Be honest about testPassed (true only if you actually ran the test and it passed).`

const MIGRATE_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['file', 'action', 'reason', 'changedFiles', 'testPassed'],
  properties: {
    file: { type: 'string' },
    action: { type: 'string', enum: ['migrated', 'skipped', 'failed'] },
    reason: { type: 'string' },
    changedFiles: { type: 'array', items: { type: 'string' } },
    localeNamespace: { type: ['string', 'null'] },
    testFile: { type: ['string', 'null'] },
    testPassed: { type: ['boolean', 'null'] },
    followups: { type: 'string' },
  },
}

const VERIFY_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['file', 'ok', 'detail'],
  properties: {
    file: { type: 'string' },
    ok: { type: 'boolean' },
    detail: { type: 'string' },
  },
}

const results = await pipeline(
  files,
  (file) =>
    agent(`${RULES}\n\nYOUR TARGET FILE (absolute): ${WT}/${file}\nMigrate it now.`, {
      label: `migrate:${file.replace(/^src\//, '')}`,
      phase: 'Migrate',
      model: 'sonnet',
      schema: MIGRATE_SCHEMA,
    }),
  (res, file) => {
    if (!res || res.action !== 'migrated' || !res.testFile) {
      return { file, ok: res?.action !== 'failed', detail: res?.reason ?? 'no-op', skippedVerify: true, migrate: res }
    }
    return agent(
      `Re-run ONLY this test to independently confirm the migration is green: cd ${WT} && npx vitest run ${res.testFile}\n` +
        `Do not edit any file. Report ok=true only if the command exits with all tests passing. Include the failing summary in detail if not.`,
      { label: `verify:${file.replace(/^src\//, '')}`, phase: 'Verify', model: 'sonnet', effort: 'low', schema: VERIFY_SCHEMA },
    ).then((v) => ({ ...(v ?? { file, ok: false, detail: 'verify agent died' }), migrate: res }))
  },
)

const clean = results.filter(Boolean)
const migrated = clean.filter((r) => r.migrate?.action === 'migrated')
const skipped = clean.filter((r) => r.migrate?.action === 'skipped')
const failed = clean.filter((r) => r.migrate?.action === 'failed' || (r.migrate?.action === 'migrated' && r.ok === false))

log(`Done: ${migrated.length} migrated, ${skipped.length} skipped, ${failed.length} need attention`)

return {
  counts: { total: files.length, migrated: migrated.length, skipped: skipped.length, failed: failed.length },
  migrated: migrated.map((r) => ({ file: r.migrate.file, ns: r.migrate.localeNamespace, verify: r.ok, followups: r.migrate.followups })),
  skipped: skipped.map((r) => ({ file: r.migrate.file, reason: r.migrate.reason })),
  failed: failed.map((r) => ({ file: r.migrate?.file ?? r.file, detail: r.detail, reason: r.migrate?.reason })),
}
