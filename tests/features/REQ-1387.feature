# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1387 — Business Glossary & Ontology
  # Dictionary/ontology of business terms over the semantic layer. Every physical field name in the semantic layer normalize…

  Scenario: REQ-1387 default behaviour
    Given tables with columns cust_id, customerId, and CUSTOMER_KEY registered in the semantic layer
    When deterministic normalization runs
    Then the fields resolve to deduplicated terms each listing its physical refs
    And a user can move a physical ref to another term, rename the term, and record a definition and experts
    And a user can create an abstract term linked to a rooted term via KIND_OF
    When the last physical ref of a term is deleted from the semantic layer
    Then the term is removed
    But if an abstract term is connected to the rooted graph through the term
    Then the term is deprecated instead and the abstract term is not left dangling
