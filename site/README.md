# provisa.dev landing page

Static, framework-free landing page for provisa.dev. No build step.

```text
site/
  index.html            hero, job cards, one-model list and role proof, top sources, download, FAQ
  register.html         free license registration form (REQ-1793)
  start/                one page per home-page problem card: the pain, then a five-minute quick start
                         (the hero lines link to them; each carries the home page's navigation)
  styles.css            brand tokens mirror provisa-ui/src/theme/tokens.css (dark)
  favicon.svg           the Provisa "P" mark
  _headers              Cloudflare Pages caching + security headers
  assets/               product screenshots (graph view, query explorer)
  wrangler.jsonc        Pages project config (name, output dir, D1 binding)
  functions/_lib/       license.js — canonicalization/signing for license registration;
                         releases.js — pending release signups and the Resend release list
  functions/api/        Pages Functions — subscribe.js + subscribe/confirm.js (double opt-in
                         release emails, REQ-2362), register.js + register/confirm.js (double
                         opt-in license registration, REQ-1793)
  schema.sql            D1 tables for release signups and license registrations
  deploy.sh             manual Cloudflare Pages deploy
```

## Source picker

The source-type block at the top of `/why/sources` is the product's source picker rendered as HTML
and CSS, and the one-line row on the home page is a short selection from it (`HOME_ROW`). It is generated, not typed: `scripts/gen-site-source-picker.mjs`
reads `provisa-ui/src/pages/sources/constants.ts` (types, order, categories, the hosted Postgres
services in `managed`) and `provisa-ui/src/components/SourceLogo.tsx` (which types have a
simple-icons mark; the rest are lettered tiles), adds the requirement-only entries and tile
sub-lists named at the top of the script, and writes the fragment between the
`<!-- source-picker:start -->` and `<!-- source-picker:end -->` markers of that page, and the row
between the `source-row` markers of the home page. The marks
are written once to `assets/source-marks.<hash>.svg` and referenced with `<use>`.

```bash
node scripts/gen-site-source-picker.mjs           # regenerate after adding or renaming a source
node scripts/gen-site-source-picker.mjs --check   # exit 1 when the site is out of step
```

`provisa-ui/src/__tests__/siteSourcePicker.test.ts` runs the check in the UI's test suite.

## Preview

```bash
python3 -m http.server 8080 --directory site
# open http://localhost:8080
```

## Downloads

The three installer cards resolve to the latest GitHub Release assets at runtime
via the GitHub API (`Provisa-<tag>-macOS.dmg`, `-linux-x86_64.AppImage`,
`-windows-x64.exe`). If the API is unreachable or no release exists yet, each card
falls back to its default href, the releases page. The visitor's OS is highlighted
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

The page is also portable to any static host (GitHub Pages, Netlify, S3, or the app's own static mount), but the release-email signup only runs on Cloudflare Pages (it needs the Pages Function + D1).

## Release emails (double opt-in, REQ-2362)

The form under the download section (footer link: Release emails) POSTs `{name, email}` to
`/api/subscribe` ([functions/api/subscribe.js](functions/api/subscribe.js)), which stores a
pending row in the private **D1** table `release_signups` and emails a confirmation link from
`releases@provisa.dev`. Clicking it ([functions/api/subscribe/confirm.js](functions/api/subscribe/confirm.js))
adds the address to the **Resend** release segment and marks the row confirmed. Nobody joins the
list without that click. The license form's optional release box does the same, confirmed by the
license confirmation link instead of a second email. A honeypot field silently drops bots; one row
per email.

Release emails themselves are Resend **Broadcasts** sent to that segment. Resend adds the
per-recipient unsubscribe link (`{{{RESEND_UNSUBSCRIBE_URL}}}`) and skips unsubscribed contacts,
so Resend, not D1, is the list to send to. Each broadcast's footer needs a postal address
(CAN-SPAM).

One-time setup:

```bash
# 1. Create the database, then paste the printed id into wrangler.jsonc (database_id)
npx --yes wrangler d1 create provisa-subscribers

# 2. Apply the schema (adds release_signups; existing tables are untouched)
npx --yes wrangler d1 execute provisa-subscribers --remote --file=site/schema.sql

# 3. In Resend: create a segment for release emails, and make sure RESEND_API_KEY has full
#    access (a sending-only key can't manage contacts). Then set the segment id:
npx --yes wrangler pages secret put RESEND_RELEASES_SEGMENT_ID --project-name provisa-dev

# 4. Deploy (the Functions + binding ship with the site)
./site/deploy.sh
```

Read the signups any time:

```bash
npx --yes wrangler d1 execute provisa-subscribers --remote \
  --command "SELECT created_at, confirmed_at, name, email FROM release_signups ORDER BY created_at" --json

# or a full SQL backup file
npx --yes wrangler d1 export provisa-subscribers --remote --output=subscribers.sql
```

The older `subscribers` table holds signups from the original "Keep me posted" form. Those
addresses were never confirmed, so they aren't added to the Resend list automatically.

Local dev with a working endpoint (needs a local D1):

```bash
cd site && npx --yes wrangler pages dev . --d1 DB=provisa-subscribers
```

The API token used by `deploy.sh` needs both **Pages: Edit** and **D1: Edit**.
Spam is handled by the honeypot; for a stronger guard, add
[Turnstile](https://developers.cloudflare.com/turnstile/) to the form and verify
the token in `subscribe.js`.

## License registration (double opt-in)

`/register` collects the fields `provisa/licensing/license.py` requires (company, position,
role, name, email, machine ID), stores them unconfirmed in D1, and emails a confirmation link
([functions/api/register.js](functions/api/register.js)). Clicking the link
([functions/api/register/confirm.js](functions/api/register/confirm.js)) signs the license with
the production Ed25519 key and emails `license.json`. Nothing is signed or sent before that click, so an email you don't control can never receive a working license.

One-time setup (in addition to the release-email steps above, same D1 database):

```bash
# 1. Generate the ONE production signing keypair (do this once, ever, offline)
python3 scripts/generate_license_keypair.py

# 2. Paste its public key into provisa/licensing/keys.py's _DEFAULT_LICENSE_PUBKEY_HEX
#    and ship that change — every Provisa install verifies against it offline.

# 3. Set the two secrets this Pages project needs (never in wrangler.jsonc)
npx --yes wrangler pages secret put RESEND_API_KEY --project-name provisa-dev
npx --yes wrangler pages secret put LICENSE_PRIVATE_KEY_PKCS8_B64 --project-name provisa-dev

# 4. Apply the schema addition (same command as for release emails; CREATE TABLE IF
#    NOT EXISTS is idempotent)
npx --yes wrangler d1 execute provisa-subscribers --remote --file=site/schema.sql

# 5. Deploy
./site/deploy.sh
```

`scripts/generate_license_keypair.py` also prints the raw private-key seed hex for
`PROVISA_LICENSE_PRIVKEY`, Provisa's own manual issuance path
([scripts/issue_license.py](../scripts/issue_license.py)) for a license requested outside this
form (e.g. by email). Both paths sign for the same public key, so keep the seed and the PKCS8
secret in sync if either is ever rotated.
