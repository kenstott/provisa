# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-910 — Multi-Route Write Dispatch
  # Preference-ordered write-path resolution routes mutations across three tiers: NATIVE (bespoke async driver, direct), SQL…

  Scenario: REQ-910 default behaviour
    Given a federation engine with connectors for multiple source types (postgresql, sqlite, cassandra)
    When resolve_write_path is called for each source with the engine
    Then postgresql returns NATIVE (native asyncpg driver + dialect available)
    And sqlite returns SQLALCHEMY (no native driver, SQLAlchemy fallback + dialect available)
    And cassandra returns ENGINE (no direct driver/dialect, only connector write=True)
    And if engine is None, only NATIVE and SQLALCHEMY remain as possible routes
