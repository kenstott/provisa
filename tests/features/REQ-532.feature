# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-532 — pgwire Server
  # Queries against `information_schema` and `pg_catalog` are intercepted and answered from an in-memory DuckDB database bui…

  Scenario: REQ-532 default behaviour
    Given a BI tool querying information_schema or pg_catalog via pgwire
    When the query is intercepted
    Then it is answered from an in-memory DuckDB built from the role's compilation context
