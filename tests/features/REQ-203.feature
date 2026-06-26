# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-203 — ABAC Approval Hook
  # Pluggable operation approval hook for enterprises with complex ABAC that can't be expressed as static RLS rules. Evaluat…

  Scenario: REQ-203 default behaviour
    Given a table with an approval hook configured
    When a query references that table
    Then the approval hook is called after RLS injection and before execution with the query context
