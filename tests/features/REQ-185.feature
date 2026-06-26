# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-185 — Hasura Migration Converters
  # v2 converter maps `select_permissions[].columns` per role -> Provisa column `visible_to`. `columns: "*"` means all colum…

  Scenario: REQ-185 default behaviour
    Given a Hasura v2 metadata export with select_permissions[].columns per role
    When the v2 converter runs
    Then each column's visible_to is populated from the role's column list, with "*" meaning all columns
