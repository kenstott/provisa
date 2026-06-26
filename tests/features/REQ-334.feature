# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-334 — Ingest Sources — Governed HTTP Push Receiver (REQ-331–337)
  # The `path` field on `table_columns` uses dot-notation to walk nested JSON payloads. Array index segments are supported (…

  Scenario: REQ-334 default behaviour
    Given an ingest column with path "resourceLogs.0.resource.attributes"
    When a POST payload is received
    Then the value at that nested path is extracted into the column and missing paths yield NULL
