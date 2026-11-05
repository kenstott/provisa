# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-594 — Security
  # [SUPERSEDED by REQ-594, 2026-11-05 -- the set grew with later requirements; the full list is in the amendment below. Kep…

  Scenario: REQ-594 default behaviour
    Given a request to /billing/signup, /billing/webhook, /health, /docs, or /openapi.json
    When AuthMiddleware processes the request
    Then the bearer-token gate is bypassed and no JWT is required
