# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-811 — Federation Performance
  # The `# @provisa key=value` GraphQL comment hint vocabulary includes a `route=federated|direct` directive parsed by extra…

  Scenario: REQ-811 default behaviour
    Given a GraphQL query annotated with the comment hint "# @provisa route=direct"
    When the query is compiled
    Then the query is routed to single-source direct execution
    And a query annotated with "# @provisa route=federated" is routed through the federation engine
