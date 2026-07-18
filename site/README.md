# provisa.dev landing page

Static, framework-free landing page for provisa.dev. No build step.

```text
site/
  index.html            hero, interfaces, governance, sources, download, CTA + signup
  styles.css            brand tokens mirror provisa-ui/src/theme/tokens.css (dark)
  favicon.svg           the Provisa "P" mark
  _headers              Cloudflare Pages caching + security headers
  assets/               product screenshots (graph view, query explorer)
  wrangler.jsonc        Pages project config (name, output dir, D1 binding)
  functions/api/        Pages Functions — subscribe.js (mailing-list endpoint)
  schema.sql            D1 table for the mailing list
  deploy.sh             manual Cloudflare Pages deploy
```

## Preview

```bash
python3 -m http.server 8080 --directory site
# open http://localhost:8080
```

## Downloads

The three installer cards resolve to the latest GitHub Release assets at runtime
via the GitHub API (`Provisa-<tag>-macOS.dmg`, `-linux-x86_64.AppImage`,
`-windows-x64.exe`). If the API is unreachable or no release exists yet, each card
falls back to its default href — the releases page. The visitor's OS is highlighted
client-side. No build step; the resolution is plain fetch + regex in `index.html`.

## Deploy — Cloudflare Pages (provisa.dev)

Run `./deploy.sh` to publish `site/` to the Cloudflare Pages project
**provisa-dev** (bound to `provisa.dev`):

```bash
./site/deploy.sh              # production
./site/deploy.sh --preview    # preview branch
```

It shells out to `npx wrangler pages deploy`. Authenticate one of two ways:

- `wrangler login` once, or
- export `CLOUDFLARE_API_TOKEN` (Pages: Edit) and `CLOUDFLARE_ACCOUNT_ID`.

One-time: create the `provisa-dev` Pages project and bind the `provisa.dev`
custom domain in the Cloudflare dashboard. `_headers` sets caching + security
headers (honored by Cloudflare Pages).

The page is also portable to any static host (GitHub Pages, Netlify, S3, or the
app's own static mount) — but the mailing-list signup only runs on Cloudflare
Pages (it needs the Pages Function + D1).

## Mailing list (private, account-only)

The signup form POSTs `{name, email}` to `/api/subscribe`
([functions/api/subscribe.js](functions/api/subscribe.js)), which appends a row
to a **D1** database — a private SQLite file only your Cloudflare account can read.
A honeypot field silently drops bots; emails are de-duplicated (email is the
primary key).

One-time setup:

```bash
# 1. Create the database, then paste the printed id into wrangler.jsonc (database_id)
npx --yes wrangler d1 create provisa-subscribers

# 2. Apply the schema
npx --yes wrangler d1 execute provisa-subscribers --remote --file=site/schema.sql

# 3. Deploy (the Function + binding ship with the site)
./site/deploy.sh
```

Read the list any time:

```bash
# CSV to stdout
npx --yes wrangler d1 execute provisa-subscribers --remote \
  --command "SELECT created_at, name, email FROM subscribers ORDER BY created_at" --json

# or a full SQL backup file
npx --yes wrangler d1 export provisa-subscribers --remote --output=subscribers.sql
```

Local dev with a working endpoint (needs a local D1):

```bash
cd site && npx --yes wrangler pages dev . --d1 DB=provisa-subscribers
```

The API token used by `deploy.sh` needs both **Pages: Edit** and **D1: Edit**.
Spam is handled by the honeypot; for a stronger guard, add
[Turnstile](https://developers.cloudflare.com/turnstile/) to the form and verify
the token in `subscribe.js`.
