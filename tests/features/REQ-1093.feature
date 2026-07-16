# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1093 — Unique Constraints
  # A registered Table carries a table-level list of UNIQUE constraints (`unique_constraints: [{name, columns[]}]`), each an…

  Scenario: REQ-1093 default behaviour
    Given a PostgreSQL source table with a declared UNIQUE constraint on (tenant_id, email)
    When the table is registered in Provisa
    Then the table's unique_constraints list is seeded with {name, columns: [tenant_id, email]} from the source
    And the admin register/edit UI shows the constraint in the expandable "Uniques" panel with its name and checked columns
    And pgwire exposes it as a pg_constraint row with contype 'u' and an information_schema.table_constraints row of type 'UNIQUE'
    And the MCP describe_table response for the table includes the unique_constraints entry
