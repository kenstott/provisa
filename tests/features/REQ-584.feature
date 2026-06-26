# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-584 — pgwire Server
  # DDL write target is resolved from the domain's `ddl_catalog` and `ddl_schema` config fields. If `ddl_catalog` is unset,…

  Scenario: REQ-584 default behaviour
    Given a role with domain_access configured
    When DDL executes without specifying catalog or schema
    Then ddl_catalog defaults to Iceberg and ddl_schema defaults to the domain ID
