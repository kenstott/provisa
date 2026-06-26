# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-335 — Ingest Sources — Governed HTTP Push Receiver (REQ-331–337)
  # The ingest endpoint accepts a single JSON object or a JSON array of objects per request. Each event in the array is extr…

  Scenario: REQ-335 default behaviour
    Given a POST to the ingest endpoint with a JSON array of events
    When all events are written
    Then HTTP 202 is returned with the count of inserted rows; 404 for unknown source/table, 503 for unavailable engine
