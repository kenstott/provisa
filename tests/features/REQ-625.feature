# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-625 — Hasura Migration Converters
  # [SUPERSEDED by REQ-417, 2026-10-17 -- no placeholder is substituted; an unparseable URL fails the conversion by name. Ke…

  Scenario: REQ-625 default behaviour
    Given a Hasura v2 source with database_url as an env var reference
    When the v2 converter runs
    Then no connection value is invented, the report says to enter it, and the import's source settings supply it
