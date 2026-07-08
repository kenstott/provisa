# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-922 — Change Data Capture
  # Debezium CDC provider (provisa/subscriptions/debezium_provider.py) uses a stable sentinel (datetime.min) for missing or…

  Scenario: REQ-922 default behaviour
    Given a Debezium envelope with missing or unparseable ts_ms
    When the CDC provider processes the record
    Then it uses datetime.min as the event timestamp
    And the watermark does not advance beyond real events
