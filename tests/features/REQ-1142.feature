# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1142 — Embedded Tier Deployment
  # DuckDB extensions (sqlite_scanner, postgres_scanner, iceberg, delta, community) required by the embedded engine are ship…

  Scenario: REQ-1142 default behaviour
    Given an air-gapped environment with only PyPI proxy access
    And provisa[embedded] installed (which depends on provisa-duckdb-ext)
    When `provisa run --demo` launches the embedded DuckDB engine
    Then stage_bundled_extensions copies platform-specific .duckdb_extension blobs from the package into ~/.provisa/native/duckdb-ext (idempotent)
    And PROVISA_DUCKDB_EXT_DIR is set to that directory
    And the engine connects with extension_directory=PROVISA_DUCKDB_EXT_DIR and autoinstall_known_extensions=false
    And all required extensions load from local blobs without network access
    And a missing extension raises BundledExtensionsMissing, never a silent fallback
