# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-635 — Registration & Governance
  # The schema name presented to users must be the name the data source itself uses to group datasets. For relational databa…

  Scenario: REQ-635 default behaviour
    Given a relational database source
    When available schemas are listed
    Then the native schema names are presented; for flat/API sources a fixed source-type constant is used
