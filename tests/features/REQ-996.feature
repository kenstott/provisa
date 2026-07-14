# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-996 — Physical Data Transfer
  # Support one-time physical data move expressed as standard SQL CREATE TABLE {schema}.{table} AS SELECT ... FROM {schema2}…

  Scenario: REQ-996 default behaviour
    Given a federated view SELECT col1, col2 FROM schema_a.table_a
    When a user executes CREATE TABLE schema_b.new_table AS SELECT col1, col2 FROM schema_a.table_a
    Then a physical table is created in the source owning schema_b
    And the new table does not appear in the Domain or Table model until explicitly registered
