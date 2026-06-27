# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-738 — NoSQL Adapters
  # MongoDB and Elasticsearch source adapters support live connections to running services. MongoDB adapter queries document…

  Scenario: REQ-738 default behaviour
    Given a running MongoDB service with seeded test documents
    When the adapter queries the collection with filter criteria
    Then documents matching the filter are returned
