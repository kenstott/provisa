# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-180 — Kafka Sinks (Table/View Publishing)
  # Sinks can be added to or removed from a table/view at any time, independently of other config.

  Scenario: REQ-180 default behaviour
    Given a table with an existing Kafka sink
    When the steward removes the sink configuration
    Then subsequent triggers produce no Kafka messages and other table config is unchanged
