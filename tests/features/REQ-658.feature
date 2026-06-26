# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-658 — JSON:API Remote Schema Connector
  # JSON:API sparse fieldsets via `?fields[type]=col1,col2` integrate with Provisa column projection — the compiler injects…

  Scenario: REQ-658 default behaviour
    Given a query requesting specific columns from a JSON:API source
    When the compiler generates the remote request
    Then sparse fieldset parameters are injected to reduce the upstream payload to only requested columns
