# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-176 — Kafka Sinks (Table/View Publishing)
  # Registered tables and views can optionally have a Kafka sink — results published to a topic on trigger. Sinks may use ch…

  Scenario: REQ-176 default behaviour
    Given a registered table or view has a Kafka sink configured
    When the configured trigger fires
    Then the query results are published to the configured Kafka topic
