# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-594 — Security
  # AuthMiddleware defines a skip-path set `{/billing/signup, /billing/webhook, /health, /live, /ready, /data/openapi/*, /au…

  Scenario: REQ-594 default behaviour
    Given a request to /billing/signup, /billing/webhook, /health, /docs, or /openapi.json
    When AuthMiddleware processes the request
    Then the bearer-token gate is bypassed and no JWT is required
