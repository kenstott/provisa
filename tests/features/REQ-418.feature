# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-418 — Domain Model
  # Report authoring workflow: analysts pull cross-domain data into their own domain via views (data-import adapters), then…

  Scenario: REQ-418 default behaviour
    Given an analyst building a cross-domain report
    When they import cross-domain data
    Then it must be done via views; all calculations and relationships are defined only within the
    analyst's own domain
