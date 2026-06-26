# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-589 — pgwire Server
  # The extended-query protocol (Bind/Execute) supports binary-encoded parameters. Supported OIDs for binary decode: 16 (boo…

  Scenario: REQ-589 default behaviour
    Given psycopg2 or asyncpg sending binary-encoded parameters via Bind/Execute
    When the server decodes them
    Then supported OIDs are decoded correctly; unsupported OIDs raise an error
