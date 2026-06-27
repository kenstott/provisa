# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-796 — Neo4j Export
  # X-Role header (e.g., "DEV") grants access to /data/cypher and /data/graph-schema endpoints. Requests without valid role…

  Scenario: REQ-796 default behaviour
    Given an export client with X-Role: DEV header
    When POST /data/cypher is called
    Then the request succeeds
    And a request without X-Role header is rejected
