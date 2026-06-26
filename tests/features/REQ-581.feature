# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-581 — pgwire Server
  # Parameterized queries using `$1`, `$2`, ... positional parameters are supported in both simple-query and extended-query…

  Scenario: REQ-581 default behaviour
    Given a JDBC or psycopg2 client using $1, $2 positional parameters
    When the query executes
    Then parameters are substituted as SQL literals before reaching the upstream engine
