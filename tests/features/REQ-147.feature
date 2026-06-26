# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-147 — Kafka Sources
  # Kafka topics queryable via Trino Kafka connector. Routed through Trino (TRINO_ONLY source).

  Scenario: REQ-147 default behaviour
    Given a Kafka topic registered as a TRINO_ONLY source
    When a consumer queries the topic via Provisa
    Then the query is routed through Trino and returns results from the Kafka topic
