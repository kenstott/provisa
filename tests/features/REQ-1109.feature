# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1109 — Transaction Management
  # Airport transactions use a real in-memory coordinator (provisa/api/airport/transactions.py). create_transaction mints a…

  Scenario: REQ-1109 default behaviour
    Given a DuckDB client with airport extension connected to Provisa
    When the client calls create_transaction
    Then a unique transaction UUID is minted and returned
    And get_transaction_status reports the transaction as active
    And mutations within the transaction auto-commit with read-committed isolation
