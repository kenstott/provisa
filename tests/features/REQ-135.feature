# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-135 — Views (Governed Computed Datasets)
  # Views with `materialize: true` are backed by a periodically refreshed MV (CTAS). Views without materialization run as li…

  Scenario: REQ-135 default behaviour
    Given a view registered with materialize: true
    When the view is queried
    Then it is served from the periodically refreshed materialized view; views without that flag run as live subqueries
