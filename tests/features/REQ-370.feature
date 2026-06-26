# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-370 — Rate Limiting
  # The NL query service (`POST /query/nl`) has an independent rate limit configurable via `nl.rate_limit` (requests per min…

  Scenario: REQ-370 default behaviour
    Given a role with a configured nl.rate_limit
    When NL query requests exceed the per-minute limit
    Then requests are rejected before any LLM call is made
