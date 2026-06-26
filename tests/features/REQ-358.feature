# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-358 — Natural Language Query Service (Phase AV)
  # The NL query service is differentiated from commodity text-to-SQL tools by: (1) three-target generation (SQL, GraphQL, C…

  Scenario: REQ-358 default behaviour
    Given the NL query service
    When compared to commodity text-to-SQL tools
    Then it provides three-target output, role-scoped prompts, and compiler-driven refinement
