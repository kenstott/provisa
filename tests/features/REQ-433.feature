# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-433 — Registration & Governance
  # A datasource may be associated with multiple domains. Any domain owner may register any unclaimed table from that source…

  Scenario: REQ-433 default behaviour
    Given a datasource shared across multiple domains
    When a domain owner claims a table
    Then no other domain may claim that same physical table; the UI greys it out for all other
    domains
