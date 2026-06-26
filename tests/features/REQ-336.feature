# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-336 — Ingest Sources — Governed HTTP Push Receiver (REQ-331–337)
  # Ingest tables are subscribable via the standard SSE endpoint (`GET /data/subscribe/{table}`). The subscription provider…

  Scenario: REQ-336 default behaviour
    Given an ingest table with SSE subscription active
    When new events are ingested and the _updated_at watermark advances
    Then subscribers receive new rows via SSE with RLS and column masking applied
