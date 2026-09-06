# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1635 — Data Catalog Integration
  # New engine-native MetadataExport adapter(s), alongside the vendor-neutral ones (openmetadata.py, atlan.py, collibra.py,…

  Scenario: REQ-1635 default behaviour
    Given a DataProduct "customer_360" with member tables exists in Provisa, when metadata export runs with Snowflake as the configured engine and the Snowflake landing terminal exists for the member tables, then the DataProduct appears as a first-class listing in Snowflake Horizon Catalog and as a Snowflake Data Product / Marketplace listing backed by a share over the product's member tables.
