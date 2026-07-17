# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1106 — Served Protocol Surface
  # Provisa serves the DuckDB Airport Arrow-Flight protocol as an outbound data-as-a-service surface, allowing external Duck…

  Scenario: REQ-1106 default behaviour
    Given a DuckDB client with the airport community extension and a role
    When it ATTACHes Provisa (TYPE AIRPORT) and runs SELECT against a federated table
    Then governed rows stream back over the airport protocol via the shared query pipeline
    And row-level security is enforced server-side (deny-by-default when no session var)
