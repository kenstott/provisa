# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-187 — Hasura Migration Converters
  # v2 converter maps `select_permissions[].filter` -> Provisa `rls_rules[]` via boolean expression-to-SQL conversion. `filt…

  Scenario: REQ-187 default behaviour
    Given a Hasura v2 select_permissions[].filter boolean expression
    When the v2 converter runs
    Then rls_rules[] are generated via boolean expression-to-SQL conversion, with empty filter producing no RLS rule
