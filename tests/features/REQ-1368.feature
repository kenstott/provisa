# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1368 — Data Catalog Integration
  # Metadata export user documentation: a published docs page (docs/metadata-export.md, navigated under Security & Governanc…

  Scenario: REQ-1368 default behaviour
    Given the published documentation set
    When a reader looks for how to publish metadata to an external catalog
    Then the metadata export page is reachable from the Security and Governance navigation
    And the page states that publication is outbound only
    And the page names every registered export provider
    And the page documents each configuration setting the export config accepts
    And the page describes the sync model the scheduler actually runs
