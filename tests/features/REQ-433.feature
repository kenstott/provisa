# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-433 — Registration & Governance
  # A datasource may be associated with multiple domains. Any domain owner may register any unclaimed table from that source…

  Scenario: REQ-433 default behaviour
    Given tables registered in domains A and B When a table named orders is registered in domain A and a table named orders in domain B Then both registrations succeed, each name being unique only within its own domain When a second table named orders is registered in domain A Then it is refused by name When the table orders in domain A is deleted and the same source table is registered again in domain A Then the retired table is restored with its id, relationships and tags And the register form does not grey out a table another domain registered
