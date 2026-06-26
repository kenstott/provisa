# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-597 — GraphQL Remote Schema Connector (REQ-307–313)
  # GraphQL remote source registration accepts a `field_overrides` map (`{fieldName: "query" | "mutation"}`) that is applied…

  Scenario: REQ-597 default behaviour
    Given a GQL remote source with a query-type field that behaves as a mutation
    When field_overrides maps that field to "mutation"
    Then the field is registered as a tracked function and the override takes priority over structural classification
