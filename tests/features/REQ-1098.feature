# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1098 — Served Protocol Surface
  # Provisa serves the DuckDB Airport Arrow-Flight protocol (msgpack/zstd payloads + Arrow Flight protobufs, matching airpor…

  Scenario: REQ-1098 default behaviour
    Given an external DuckDB client with `airport` extension
    When client sets PROVISA_AIRPORT_PORT env var and attaches Provisa via ATTACH 'grpc://host:port' AS db (TYPE AIRPORT)
    And queries the federated catalog with authorization Bearer token containing a role name
    Then server applies row-level and column-level governance rules for that role
    And results reflect governed access (RLS-filtered rows, masked columns) per the role's permissions
