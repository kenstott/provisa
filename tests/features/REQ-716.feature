# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-716 — Neo4j Export
  # Edges are MERGE'd by matching source and target nodes on _provisa_id, with relationship type preserved from the source g…

  Scenario: REQ-716 default behaviour
    Given an edge with start: 101, end: 202, type: "CONNECTS_TO"
    When the edge is exported
    Then Neo4j contains a relationship matching (a:Label{_provisa_id: 101})-[r:CONNECTS_TO]->(b:Label{_provisa_id: 202})
