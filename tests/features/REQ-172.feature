# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-172 — Dataset Change Events
  # Mutations emit a dataset change event to a Kafka topic — no row-level detail, just `{table, source, timestamp}`.

  Scenario: REQ-172 default behaviour
    Given a mutation is executed against a registered table
    When the mutation completes
    Then a change event containing table, source, and timestamp is emitted to the configured Kafka topic
