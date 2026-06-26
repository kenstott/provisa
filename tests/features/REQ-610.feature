# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-610 — Domain Model
  # A field access grant belongs to the requesting domain, not to the specific view that prompted it. Any subsequent view in…

  Scenario: REQ-610 default behaviour
    Given a domain that has received a cross-domain field access grant
    When a new view in that domain uses the granted fields
    Then no additional approval is required; only new fields outside the grant trigger a new request
