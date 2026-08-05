# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1368 — Data Catalog Integration
  # Metadata egress user documentation: a published docs page (docs/metadata-egress.md, navigated under Security & Governanc…

  Scenario: REQ-1368 default behaviour
    Given the published documentation set
    When a reader looks for how to publish metadata to an external catalog
    Then the metadata egress page is reachable from the Security and Governance navigation
    And the page states that publication is outbound only
    And the page names every registered egress provider
    And the page documents each configuration setting the egress config accepts
    And the page describes the sync model the scheduler actually runs
