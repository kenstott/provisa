# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-186 — Hasura Migration Converters
  # v2 converter maps each role's `insert/update/delete_permissions` -> the table's write grants (REQ-2074) -- columns, filt…

  Scenario: REQ-186 default behaviour
    Given a Hasura v2 metadata export with insert/update_permissions[].columns per role
    When the v2 converter runs
    Then each role's insert and update grants hold the role's insert/update column list
