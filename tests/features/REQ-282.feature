# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-282 — Live Query Engine (Unified Subscription & Sink Delivery)
  # A single Live Query Engine powers all poll-based live delivery. It is the common implementation for: (a) SSE subscriptio…

  Scenario: REQ-282 default behaviour
    Given a table configured for poll-based delivery with both SSE subscription and Kafka sink outputs
    When the poll interval fires
    Then the Live Query Engine executes the query once and delivers results to both SSE and Kafka outputs
