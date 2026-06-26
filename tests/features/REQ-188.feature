# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-188 — Hasura Migration Converters
  # v2 converter maps `object_relationships` -> cardinality=many-to-one and `array_relationships` -> cardinality=one-to-many…

  Scenario: REQ-188 default behaviour
    Given a Hasura v2 metadata export with object_relationships and array_relationships
    When the v2 converter runs
    Then object_relationships become cardinality=many-to-one and array_relationships become cardinality=one-to-many
