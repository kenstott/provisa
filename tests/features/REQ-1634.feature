# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1634 — Data Catalog Integration
  # New DataProduct entity, domain-scoped: id, domain_id (required FK), name, owner, description. Tables replace
  # the boolean data_product flag with product_id: str | None (FK -> DataProduct.id). A DataProduct's member tables
  # must share its domain_id. MetadataSnapshot gains a data_products: list[DataProductAsset] section. Each vendor adapter
  # maps DataProductAsset to native constructs. TableEditForm presents a product_id picker scoped to domain-matching
  # DataProducts.

  Scenario: REQ-1634.1 — DataProduct metadata export unifies member tables
    Given a DataProduct "customer_360" owned by "alice" in domain "sales"
    And two tables in domain "sales" both set product_id="customer_360"
    When metadata is exported
    Then both tables publish as one data product named "customer_360" attributed to owner "alice"

  Scenario: REQ-1634.2 — Cross-domain DataProduct assignment is rejected
    Given a DataProduct "customer_360" owned by "alice" in domain "sales"
    And a table in domain "marketing"
    When an attempt is made to set product_id="customer_360" on the marketing table
    And the change is saved
    Then it is rejected because the table's domain_id does not match the DataProduct's domain_id
