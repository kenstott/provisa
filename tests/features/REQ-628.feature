# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-628 — Hasura Migration Converters
  # When converting DDN HML, if a Model references an ObjectType name that is not found in any scanned .hml file, that table…

  Scenario: REQ-628 default behaviour
    Given a DDN HML project where some ObjectType HML files are missing
    When the DDN converter runs
    Then missing ObjectType tables are skipped with a warning and conversion continues
