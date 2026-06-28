# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-012 — Registration & Governance
  # Source registration is privileged; validates connection, calls Trino dynamic catalog API, no restart required, available…

  Scenario: REQ-012 default behaviour
    Given a privileged steward with registration rights
    When they submit a new source registration
    Then Provisa validates the connection, calls the Trino dynamic catalog API, and makes the source
    available within seconds without a server restart
