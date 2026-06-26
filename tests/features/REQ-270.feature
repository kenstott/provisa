# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-270 — SQL & Multi-Protocol Client Access
  # SQLAlchemy dialect for Provisa. `create_engine("provisa+http://user:password@host:8001")`. Dialect maps SQLAlchemy Core…

  Scenario: REQ-270 default behaviour
    Given a pandas or ORM user creating a SQLAlchemy engine with the Provisa dialect
    When they call read_sql() or inspector.get_table_names()
    Then governed data is returned using standard SQLAlchemy patterns
