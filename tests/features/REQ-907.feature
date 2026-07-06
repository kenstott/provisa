# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-907 — Source Connectors
  # SqliteFdwConnector and MysqlFdwConnector enable the embedded/BYO Postgres federation engine to attach SQLite files and r…

  Scenario: REQ-907 default behaviour
    Given a source configured with sqlite_fdw (source_type "sqlite", key "sqlite_fdw") or mysql_fdw (source_type "mysql", key "mysql_fdw")
    When the federation engine initializes
    Then the connector probes functional availability (sqlite_fdw via loaded FDW, mysql_fdw via probe and library bundling)
    And if available, the connector attaches the SQLite file (CREATE SERVER OPTIONS(database path) + IMPORT FOREIGN SCHEMA) or remote MySQL (CREATE SERVER + CREATE FOREIGN TABLE IMPORT FOREIGN SCHEMA)
    And queries route through the attached foreign schema to the source.
