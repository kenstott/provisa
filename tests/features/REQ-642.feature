# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-642 — Graph Analytics Pipeline
  # A POST /data/graph-analytics endpoint accepts a Cypher query and algorithm name, executes the query via the existing cyp…

  Scenario: REQ-642 default behaviour
    Given a POST /data/graph-analytics request with a Cypher query and algorithm name
    When the endpoint processes it
    Then it builds a NetworkX DiGraph, runs the algorithm, and returns augmented nodes and edges with elapsed_ms
