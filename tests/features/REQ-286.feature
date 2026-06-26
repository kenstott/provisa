# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-286 — Live Query Engine (Unified Subscription & Sink Delivery)
  # SSE subscription and Kafka sink are equivalent output mechanisms for a table or view. A single live definition may list…

  Scenario: REQ-286 default behaviour
    Given a table with both sse_subscription and kafka_sink outputs configured
    When the Kafka consumer falls behind
    Then SSE delivery continues at normal intervals unaffected by the Kafka consumer lag
