# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-539 — Infrastructure
  # The `GET /health` (or `HEAD /health`) and `GET /setup/status` endpoints are always unauthenticated — they bypass the `Au…

  Scenario: REQ-539 default behaviour
    Given an auth provider is configured
    When GET /health or GET /setup/status is called without an Authorization header
    Then the request succeeds; all other endpoints return 401 without a valid bearer token
