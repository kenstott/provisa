# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-607 — SQL & Multi-Protocol Client Access
  # ProvisaClient error contract: `query()` raises `httpx.HTTPStatusError` on HTTP-level errors (4xx/5xx). `query_df()` rais…

  Scenario: REQ-607 default behaviour
    Given a ProvisaClient caller
    When query() receives a 4xx/5xx response it raises httpx.HTTPStatusError; when query_df() receives a GraphQL errors field it raises RuntimeError
    Then callers can handle transport and schema errors separately
