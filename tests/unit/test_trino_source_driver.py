# REQ-994: Trino/Presto clusters are first-class remote SOURCES read via the SQLAlchemy trino
# dialect and landed as REPLICA tables — reachable on ANY engine like GraphQL/REST/gRPC sources,
# classified by read mechanism, not data-at-rest residency.
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.drivers.registry import create_driver, has_driver
from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver
from provisa.federation.engine import reachable_source_types


def test_trino_has_direct_source_driver():
    assert has_driver("trino")
    driver = create_driver("trino")
    assert isinstance(driver, DirectDriver)
    # Read path is the SQLAlchemy trino dialect (read-then-land), not a Trino-engine detour.
    assert isinstance(driver, SQLAlchemyDriver)


def test_trino_source_reachable_as_replica_on_every_engine():
    for engine in ("duckdb", "trino", "pg", "clickhouse"):
        assert "trino" in reachable_source_types(engine), engine
