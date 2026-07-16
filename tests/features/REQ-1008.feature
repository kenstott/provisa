# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1008 — MCP Server
  # Provisa exposes a Model Context Protocol (MCP) server so external AI agents (Claude Desktop, claude.ai connectors, Claud…

  Scenario: REQ-1008 default behaviour
    Given a Provisa instance whose curated catalog is indexed and an MCP client authenticated as role r with access to a subset of domains
    When the agent calls search_catalog("customer lifetime value") then describe_table on the top hit and finally run_sql("SELECT ... LIMIT 100") against that table
    Then search_catalog returns only entities in domains r may access, ranked by column-detail relevance with {schema,table,column} provenance, describe_table returns the authoritative table structure from the meta views, and run_sql executes through _govern_and_route under role r — enforcing the same domain access policy as pgwire and returning a row-limited result
