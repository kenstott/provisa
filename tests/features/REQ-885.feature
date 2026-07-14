# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-885 — Extensible Functions
  # Generalize tracked functions (REQ-205–208: source-resident stored procedures exposed as GraphQL mutations) to support mu…

  Scenario: REQ-885 default behaviour
    Given a user registers a function with implementation kind=http and argument kinds=[table_ref, column_value] and a view references it as SELECT * FROM myfn(tbl, param) WITH (materialize=true)
    When Provisa materializes the view (e.g. via CTAS or scheduled MV refresh)
    Then the function invokes the http endpoint as admin (definer), passing table metadata/hint and row-wise scalar, receives Arrow buffer, materializes as a relation, and applies row-level access governance on output before serving to end users
