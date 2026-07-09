# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-943 — Live Data & Events
  # The event-loop source-land path fetches current rows from a materialized source table through the federation engine's SQ…

  Scenario: REQ-943 default behaviour
    Given a materialized source with current rows in the federation catalog
    When SourceRowLoader.load(source, table) is invoked
    Then engine.execute_engine issues SELECT * FROM "<catalog>"."<schema>"."<table>" and returns row dicts
    Given a row-oriented API/push source type with no engine-scannable table
    When SourceRowLoader.load(source, table) is invoked
    Then UnsupportedSourceFetch is raised and no scan is issued
