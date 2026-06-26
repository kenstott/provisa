# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-174 — Dataset Change Events
  # Producers running complex ETL outside Provisa can signal changes via a trivial mutation (touch operation).

  Scenario: REQ-174 default behaviour
    Given an external ETL process has modified source data outside Provisa
    When the ETL calls a touch mutation on the relevant table
    Then Provisa fires the mutation hook and emits a change event as if data had changed directly
