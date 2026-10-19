# provisa-ui

The Provisa web console: React and TypeScript, built with Vite. It talks to the Provisa API for
everything it shows, so run the backend too. From the repository root, `./start-ui.sh` starts
both and serves the console at http://localhost:3000.

It needs Node.js 22.12 or later.

## Commands

Run these from this directory.

| Command | What it does |
| --- | --- |
| `npm run dev` | Vite dev server with hot reload |
| `npm run build` | Type-check, then build to `dist/`. The `prebuild` step first builds the MkDocs site into `public/docs-site/`, the offline copy the in-app docs reader falls back to; set `SKIP_DOCS_SITE=1` to skip it |
| `npm run typecheck` | `tsc -b --noEmit` |
| `npm run lint` | ESLint |
| `npm run test:unit` | Vitest, with coverage |
| `npm run test:e2e` | Playwright |

## Tests

Unit tests sit beside the code in `__tests__` folders. Browser tests are under `e2e/`, and every
Playwright test imports `test` from `./coverage`, whose fixture fails a test on any uncaught
browser exception. The e2e suites start the services they need, so they run in CI.

## Text and languages

No user-facing string is written in a component. English lives in `src/i18n/locales/en.json`
and `src/i18n/locales/en/<namespace>.json`, and each other language mirrors it.
