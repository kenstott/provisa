# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-191 — Hasura Migration Converters
  # DDN AggregateExpression metadata preserved in sidecar `provisa-aggregates.yaml` and converted to Provisa aggregate confi…

  Scenario: REQ-191 default behaviour
    Given a DDN project with AggregateExpression metadata
    When the DDN converter runs
    Then aggregate config is emitted in provisa-aggregates.yaml as valid Provisa aggregate config
