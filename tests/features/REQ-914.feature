# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-914 — Pipeline Stage Tracing
  # Each query-pipeline boundary (govern.in, govern.rls, govern.mask, govern.out, and transpile.<dialect>) emits an OpenTele…

  Scenario: REQ-914 default behaviour
    Given the query pipeline processes a request
    When PROVISA_TRACE_SQL is unset or off
    Then stage span events carry the stage name but no SQL text
    When PROVISA_TRACE_SQL is redacted
    Then each stage event carries SQL with every literal replaced by a placeholder
    And no literal value (e.g. an RLS predicate value or SSN) appears in any attribute
    When the SQL is unparseable in redacted mode
    Then the failure is recorded as a span attribute and the request is not aborted
