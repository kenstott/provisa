# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-606 — SQL & Multi-Protocol Client Access
  # ProvisaClient accepts a bearer token (`token` parameter) as an alternative to username/password for authentication. When…

  Scenario: REQ-606 default behaviour
    Given a ProvisaClient instantiated with a token parameter
    When a request is made
    Then Authorization: Bearer <token> is sent on every request
