# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-178 — Kafka Sinks (Table/View Publishing)
  # Sinks are opt-in per registered table or view, configured by the steward.

  Scenario: REQ-178 default behaviour
    Given a registered table has no Kafka sink configured
    When the table data changes
    Then no Kafka messages are published for that table
