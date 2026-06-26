# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-271 — SQL & Multi-Protocol Client Access
  # ADBC (Arrow Database Connectivity) interface in `provisa-client`. `provisa_client.adbc_connect(url, user, password)` ret…

  Scenario: REQ-271 default behaviour
    Given an analytics tool connecting via adbc_connect()
    When a query executes over Arrow Flight
    Then results stream as Arrow RecordBatches with zero-copy columnar delivery
