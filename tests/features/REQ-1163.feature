# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1163 — Materialization Store
  # A bitemporal materialized view (REQ-1162) is READABLE through the query languages: consumers get current state by defaul…

  Scenario: REQ-1163 default behaviour
    Given a materialized bitemporal view that has been refreshed across several changes
    When it is queried with no header, then with X-Provisa-As-Of set to a past time
    Then the plain read returns current reconstructed state and the as-of read returns the state that was effective at that time; a malformed header is rejected with HTTP 400
