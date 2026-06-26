# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-369 — Rate Limiting
  # Per-role rate limits configurable in `provisa.yaml`: max requests per second, max concurrent SSE subscriptions, max conc…

  Scenario: REQ-369 default behaviour
    Given a role with configured rate limits
    When requests exceed the rate limit
    Then requests are rejected with HTTP 429 and a Retry-After header before compilation or execution
