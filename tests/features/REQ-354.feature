# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-354 — Natural Language Query Service (Phase AV)
  # Provisa exposes a `POST /query/nl` endpoint accepting a natural language question. The service submits an async job and…

  Scenario: REQ-354 default behaviour
    Given a non-technical user submitting a natural language question to POST /query/nl
    When the service receives it
    Then it returns a job_id immediately and the result is available via polling or SSE
