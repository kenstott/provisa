-- D1 schema for the mailing-list signup. Apply once after creating the database:
--   wrangler d1 execute provisa-subscribers --remote --file=site/schema.sql
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
