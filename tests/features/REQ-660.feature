# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-660 — JSON:API Remote Schema Connector
  # JSON:API filtering via `?filter[field]=value` maps to Provisa native filter columns (with `_nf_` prefix, `native_filter_…

  Scenario: REQ-660 default behaviour
    Given a filter on a JSON:API source column with native_filter_type query_param
    When the query is executed
    Then the filter is passed as ?filter[field]=value to the remote API rather than applied post-fetch
