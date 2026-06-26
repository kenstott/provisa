# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-331 — Ingest Sources — Governed HTTP Push Receiver (REQ-331–337)
  # A new `ingest` source type allows external services (OTEL Collector, Fluentd, custom webhooks, etc.) to POST JSON events…

  Scenario: REQ-331 default behaviour
    Given an external service configured to POST JSON events to Provisa
    When a POST is made to /events/ingest/{source_id}/{table}
    Then Provisa persists the events to the steward-configured backing relational store
