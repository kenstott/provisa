# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-209 — Tracked Functions & Custom Mutations
  # Webhook-backed mutations — external HTTP endpoint called as a GraphQL mutation (Part of Phase AC). Config includes name,…

  Scenario: REQ-209 default behaviour
    Given a webhook mutation configured with governance: requires_approval
    When a client invokes it
    Then steward approval is required and the external HTTP endpoint is called
