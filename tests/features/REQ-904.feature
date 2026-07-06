# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-904 — PostgreSQL Deployment
  # Federation connectors are self-describing prebuilt definitions; each has an async probe() reporting FUNCTIONAL truth (FD…

  Scenario: REQ-904 default behaviour
    Given a FederationEngine with pg_duckdb not in shared_preload_libraries
    When discover() is called
    Then pg_duckdb's probe reports unavailable
    And CSV sources fall back to file_fdw
    And post-preload, discover() re-probes and activates pg_duckdb.
