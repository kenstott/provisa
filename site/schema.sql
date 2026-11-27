-- D1 schema for the release-email signup and license registration. Apply after creating the
-- database, and again whenever a table is added (CREATE TABLE IF NOT EXISTS is idempotent):
--   wrangler d1 execute provisa-subscribers --remote --file=site/schema.sql

-- Legacy: rows from the original "Keep me posted" form, which never confirmed the address.
-- Nothing writes here since REQ-2361; release signups live in release_signups below.
CREATE TABLE IF NOT EXISTS subscribers (
  email      TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Double opt-in license registration (REQ-1793). `token` is the confirmation-link secret — a
-- registrant who never clicks the emailed link never gets a signed license, and the row simply
-- expires unconfirmed. One license per (email, machine_id): re-registering the same machine
-- before confirming replaces the pending row instead of piling up duplicates.
CREATE TABLE IF NOT EXISTS license_registrations (
  token         TEXT PRIMARY KEY,
  email         TEXT NOT NULL,
  company       TEXT NOT NULL,
  position      TEXT NOT NULL,
  role          TEXT NOT NULL,
  first_name    TEXT NOT NULL,
  last_name     TEXT NOT NULL,
  phone         TEXT,
  machine_id    TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT (datetime('now')),
  confirmed_at  TEXT,
  UNIQUE (email, machine_id)
);

-- Double opt-in release-email signup (REQ-2361). `token` is the confirmation-link secret; a
-- signup joins the Resend release list only once confirmed. One row per email: signing up again
-- replaces the token. `license_token` is set when the signup came from the license form's
-- release box, whose license confirmation link confirms it too.
CREATE TABLE IF NOT EXISTS release_signups (
  token          TEXT PRIMARY KEY,
  email          TEXT NOT NULL UNIQUE,
  name           TEXT NOT NULL,
  license_token  TEXT,
  created_at     TEXT NOT NULL DEFAULT (datetime('now')),
  confirmed_at   TEXT
);
