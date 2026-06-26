# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-186 — Hasura Migration Converters
  # v2 converter maps `insert/update_permissions[].columns` per role -> Provisa column `writable_by`.

  Scenario: REQ-186 default behaviour
    Given a Hasura v2 metadata export with insert/update_permissions[].columns per role
    When the v2 converter runs
    Then each column's writable_by is populated from the role's insert/update column list
