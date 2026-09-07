# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1634 — Data Catalog Integration
  # New DataProduct entity, domain-scoped: id, domain_id (required FK — a data product has exactly one owning domain), name,…

  Scenario: REQ-1634 default behaviour
    Given a DataProduct "customer_360" owned by "alice" in domain "sales", and two tables in domain "sales" both set product_id="customer_360", when metadata is exported, then both tables publish as one data product named "customer_360" attributed to owner "alice" instead of two separate products; given an attempt to set product_id="customer_360" on a table in domain "marketing", when saved, then it is rejected because the table's domain_id does not match the DataProduct's domain_id.
