# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-638 — Registration & Governance
  # The UI calls one availableSchemas endpoint and one availableTables endpoint. Backend routing selects the correct introsp…

  Scenario: REQ-638 default behaviour
    Given a UI requesting schema and table lists for any source type
    When it calls availableSchemas and availableTables
    Then the backend selects the correct strategy internally without exposing per-type endpoints
