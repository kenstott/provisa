# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-673 — Source Connectors
  # GQL remote sources support an optional per-source config field (e.g. `count_query` in provisa.yaml) that specifies a que…

  Scenario: REQ-673 default behaviour
    Given a GQL remote source with count_query configured and a cold Trino cache
    When the graph-counts endpoint is called
    Then the remote GraphQL API is queried to return node counts instead of returning no count
