# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-739 — API & Integration
  # API source discovery endpoint introspects OpenAPI specs and stores discovered endpoint candidates (operation_id, path, m…

  Scenario: REQ-739 default behaviour
    Given an OpenAPI spec URL
    When the discovery endpoint introspects it
    Then discovered operation candidates are stored and queryable via admin API
    And stewards can accept or reject each candidate
