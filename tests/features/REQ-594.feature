# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-594 — Security
  # TenantMiddleware defines a skip-path set `{/billing/signup, /billing/webhook, /health, /docs, /openapi.json}`. Requests…

  Scenario: REQ-594 default behaviour
    Given a request to /billing/signup, /billing/webhook, /health, /docs, or /openapi.json
    When TenantMiddleware processes the request
    Then tenant resolution is bypassed and no JWT tenant_id claim is required
