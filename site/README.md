# provisa.dev landing page

Static, framework-free landing page for provisa.dev. No build step.

```text
site/
  index.html            hero, interfaces, governance, sources, download, CTA + signup
  register.html         free license registration form (REQ-1793)
  styles.css            brand tokens mirror provisa-ui/src/theme/tokens.css (dark)
  favicon.svg           the Provisa "P" mark
  _headers              Cloudflare Pages caching + security headers
  assets/               product screenshots (graph view, query explorer)
  wrangler.jsonc        Pages project config (name, output dir, D1 binding)
  functions/_lib/       license.js — canonicalization/signing shared by the two endpoints below
  functions/api/        Pages Functions — subscribe.js (mailing list), register.js +
                         register/confirm.js (double opt-in license registration)
  schema.sql            D1 tables for the mailing list and license registrations
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

## License registration (REQ-1793, double opt-in)

`/register` collects the fields `provisa/licensing/license.py` requires (company, position,
role, name, email, machine ID), stores them unconfirmed in D1, and emails a confirmation link
([functions/api/register.js](functions/api/register.js)). Clicking the link
([functions/api/register/confirm.js](functions/api/register/confirm.js)) signs the license with
the production Ed25519 key and emails `license.json` — nothing is signed or sent before that
click, so an email you don't control can never receive a working license.

One-time setup (in addition to the mailing-list steps above, same D1 database):

```bash
# 1. Generate the ONE production signing keypair (do this once, ever, offline)
python3 scripts/generate_license_keypair.py

# 2. Paste its public key into provisa/licensing/keys.py's _DEFAULT_LICENSE_PUBKEY_HEX
#    and ship that change — every Provisa install verifies against it offline.

# 3. Set the two secrets this Pages project needs (never in wrangler.jsonc)
npx --yes wrangler pages secret put RESEND_API_KEY --project-name provisa-dev
npx --yes wrangler pages secret put LICENSE_PRIVATE_KEY_PKCS8_B64 --project-name provisa-dev

# 4. Apply the schema addition (same command as the mailing list; schema.sql now
#    has both tables, and CREATE TABLE IF NOT EXISTS is idempotent)
npx --yes wrangler d1 execute provisa-subscribers --remote --file=site/schema.sql

# 5. Deploy
./site/deploy.sh
```

`scripts/generate_license_keypair.py` also prints the raw private-key seed hex for
`PROVISA_LICENSE_PRIVKEY`, Provisa's own manual issuance path
([scripts/issue_license.py](../scripts/issue_license.py)) for a license requested outside this
form (e.g. by email). Both paths sign for the same public key — keep the seed and the PKCS8
secret in sync if either is ever rotated.
