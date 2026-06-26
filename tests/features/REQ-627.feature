# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-627 — Hasura Migration Converters
  # v2 converter derives the Provisa table `alias` from `custom_root_fields.select` (first priority), then `custom_root_fiel…

  Scenario: REQ-627 default behaviour
    Given a Hasura v2 table with custom_root_fields or custom_name defined
    When the v2 converter runs
    Then the Provisa table alias is derived with select > select_by_pk > custom_name priority order
