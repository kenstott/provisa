-- D1 schema for the mailing-list signup. Apply once after creating the database:
--   wrangler d1 execute provisa-subscribers --remote --file=site/schema.sql
CREATE TABLE IF NOT EXISTS subscribers (
  email      TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
