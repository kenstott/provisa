# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-430 — Vector Search
  # Query-time vectorization must be supported: when a similarity search is expressed with a text string rather than a raw v…

  Scenario: REQ-430 default behaviour
    Given a similarity search expressed with a text string
    When the query is executed
    Then Provisa calls the declared embedding model to generate the query vector before running the search
