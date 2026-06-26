# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-624 — Hasura Migration Converters
  # v2 converter upgrades a role to `write` capability when that role has any `delete_permissions` entry on any table. No pe…

  Scenario: REQ-624 default behaviour
    Given a Hasura v2 role with delete_permissions on any table
    When the v2 converter runs
    Then the role is upgraded to write capability with no per-table delete mapping produced
