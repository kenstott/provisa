# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8e5f14-7a6c-4d39-9e01-c4f3a7d8b562
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Spark source's script is validated by its own host in three steps (REQ-2225).

The host is the connector's real front end over TLS running the connector project's code, its
validator among it; Spark is stood in for as in tests/unit/test_connector_script.py. A table
argument is made readable here as table_reads would (its Flight address and statement), and the
cluster is the one spark_cluster.resolved is stood in to give. Each step is shown passing and
failing: a bad tag or type, a schema mismatch, a dry-run error, and a missing key."""

# Requirements: REQ-2225, REQ-2249

from __future__ import annotations

import textwrap
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.codesource import script_validation, table_reads
from provisa.codesource.script_validation import Asked, Target
from provisa.core import spark_cluster
from provisa.pgwire.minted import read_statement
from tests.unit import test_connector_script as _script
from tests.unit.test_connector_script import _join, _joined, _mapping, _worker

pytestmark = pytest.mark.unit

# The connector's real host and the join's stand-ins, the fixtures of test_connector_script.
host = _script.host
wired = _script.wired

_CLUSTER = "sc://spark.example:15002"

_HEAD = '''
from dataclasses import dataclass

from provisa_services import TableRef
from provisa_spark import session


@dataclass
class Order:
    id: int
    amount: float


@dataclass
class Read:
    statement: str


@dataclass
class Store:
    id: int


@dataclass
class Region:
    id: int


@dataclass
class Match:
    store_id: int | None
    region_id: int | None
    score: float


def orders() -> list[Order]:
    """Orders, from the cluster.

    @table
    """
    return [Order(**row.asDict()) for row in session().sql("SELECT orders").collect()]
'''

_GOOD = (
    _HEAD
    + '''

def read_of(orders: TableRef[Order]) -> list[Read]:
    """What the call was given to read its table with.

    @query
    """
    return [Read(statement=orders.sql)]


def match_stores(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
    """Each store matched to its region.

    @query
    """
    return [Match(store_id=1, region_id=7, score=1.0), Match(store_id=2, region_id=8, score=0.5)]
'''
)


def _changed(head: str, old: str, new: str) -> str:
    assert old in head
    return head.replace(old, new)


@pytest.fixture
async def joined(host, wired):
    """The source joined with :data:`_GOOD` and its validation target."""
    _joined(wired, host)
    mapping = await _join(_mapping(host, textwrap.dedent(_GOOD)))
    worker = _worker(mapping, wired)
    worker.start()
    target = Target(
        worker=worker,
        remote={**mapping["remote"], "language": "python"},
        setup=lambda: {"script": mapping["settings"]["script"], "cluster": _CLUSTER},
        own_cluster=_CLUSTER,
        offered={"script", "cluster"},
        script_name="script",
    )
    yield target
    worker.stop()


@pytest.fixture
def made_readable(monkeypatch):
    """Table arguments made readable as table_reads makes them for a remote Python host: each
    given its Flight address and the statement that reads its declared columns."""
    given: list[dict] = []

    @asynccontextmanager
    async def readable(state, session, payload, *, remote, local):
        assert remote["language"] == "python" and local is None
        for reference in payload.values():
            reference["flight"] = {
                "uri": "grpc+tls://provisa.example:8815",
                "token": "minted",
                "call": session.correlation_id,
            }
            reference["sql"] = read_statement(reference["ref"], reference["columns"])
            given.append(dict(reference))
        yield

    monkeypatch.setattr(table_reads, "readable", readable)
    return given


@pytest.fixture
def cluster(monkeypatch):
    async def resolved(admin_db, org_id, own):
        assert own == _CLUSTER
        return spark_cluster.Resolved(own, spark_cluster.ORIGIN_SOURCE)

    monkeypatch.setattr(spark_cluster, "resolved", resolved)


_INPUTS = {
    "read_of": {"orders": "sales.orders"},
    "match_stores": {"stores": "geo.stores", "regions": "geo.regions"},
}
_KEYS = {"match_stores": ("store_id", "region_id")}


async def _validated(target: Target, script: str | None = None, **asked) -> dict:
    asked.setdefault("inputs", _INPUTS)
    asked.setdefault("keys", _KEYS)
    given = None if script is None else textwrap.dedent(script)
    return await script_validation.validate(
        SimpleNamespace(admin_db=None), target, Asked(script=given, **asked), "analyst"
    )


def _by_function(step: dict) -> dict[str, dict]:
    return {f["function"]: f for f in step["functions"]}


# -- all three pass -----------------------------------------------------------------------------------


async def test_a_good_script_passes_all_three_steps(joined, made_readable, cluster):
    report = await _validated(joined)
    assert report["static"]["status"] == "ok", report["static"]
    described = {f["name"]: f for f in report["static"]["functions"]}
    assert described["read_of"]["tables"] == ["orders"]
    assert described["match_stores"]["resolver"] is True
    assert described["orders"]["columns"] == ["id", "amount"]
    assert report["cluster"] == {"address": _CLUSTER, "origin": "source"}
    assert report["dry_run"]["status"] == "ok", report["dry_run"]
    ran = _by_function(report["dry_run"])
    assert ran["orders"]["rows"] == 2
    assert ran["orders"]["declared"] == [
        {"name": "id", "type": "int64"},
        {"name": "amount", "type": "double"},
    ]
    assert ran["match_stores"]["rows"] == 2
    assert report["keys"]["status"] == "ok", report["keys"]
    assert _by_function(report["keys"])["match_stores"]["rows"] == 2


async def test_a_table_argument_is_a_sample_read_over_flight_as_the_caller(
    joined, made_readable, cluster
):
    report = await _validated(joined)
    assert {r["ref"] for r in made_readable} == {"sales.orders", "geo.stores", "geo.regions"}
    assert all(r["flight"]["token"] == "minted" for r in made_readable)
    assert report["dry_run"]["status"] == "ok"
    # What the function was given to read with: the declared columns, at most a sample of rows.
    script = textwrap.dedent(
        _changed(_GOOD, "return [Read(statement=orders.sql)]", "raise ValueError(orders.sql)")
    )
    failed = await _validated(joined, script)
    said = _by_function(failed["dry_run"])["read_of"]["error"]
    assert 'SELECT "id", "amount" FROM' in said
    assert said.endswith(f"LIMIT {script_validation.SAMPLE_ROWS}")


async def test_the_served_script_is_validated_when_none_is_given(joined, made_readable, cluster):
    report = await _validated(joined, None)
    assert report["static"]["hash"] == _script._script_hash(textwrap.dedent(_GOOD))
    assert report["dry_run"]["status"] == "ok"


# -- step 1: static -----------------------------------------------------------------------------------


async def test_a_bad_tag_fails_the_static_check_and_nothing_runs(joined, made_readable, cluster):
    script = _changed(
        _GOOD, '    @query\n    """\n    return [Read', '    @qurey\n    """\n    return [Read'
    )
    report = await _validated(joined, script)
    assert report["static"]["status"] == "failed"
    assert "read_of: unknown tag @qurey" in report["static"]["problems"]
    assert report["dry_run"] == {
        "status": "not_run",
        "functions": [],
        "reason": "the static check failed",
    }
    assert report["keys"]["status"] == "not_run"
    assert made_readable == []


async def test_a_bad_declared_type_fails_the_static_check(joined, made_readable, cluster):
    script = _changed(_GOOD, "    amount: float\n", "    amount: complex\n")
    report = await _validated(joined, script)
    assert report["static"]["status"] == "failed"
    assert any("complex" in p for p in report["static"]["problems"]), report["static"]


async def test_a_table_argument_on_a_table_fails_the_static_check(joined, made_readable, cluster):
    script = _changed(
        _GOOD,
        "def orders() -> list[Order]:",
        "def orders(source: TableRef[Order]) -> list[Order]:",
    )
    report = await _validated(joined, script)
    assert report["static"]["status"] == "failed"
    assert any(p.startswith("orders") for p in report["static"]["problems"]), report["static"]


async def test_a_script_that_does_not_import_fails_the_static_check(joined, made_readable, cluster):
    report = await _validated(joined, "import not_a_module_anywhere\n")
    assert report["static"]["status"] == "failed"
    assert any("not_a_module_anywhere" in p for p in report["static"]["problems"])


# -- step 2: the dry run ------------------------------------------------------------------------------


async def test_rows_that_do_not_match_the_declared_type_are_a_mismatch(
    joined, made_readable, cluster
):
    script = _changed(
        _GOOD,
        "Match(store_id=1, region_id=7, score=1.0)",
        "Match(store_id=1, region_id=7, score='high')",
    )
    report = await _validated(joined, script)
    assert report["dry_run"]["status"] == "failed"
    found = _by_function(report["dry_run"])["match_stores"]
    assert found["status"] == "mismatch"
    assert found["rows"] == 2
    assert any("do not convert to the declared types" in m for m in found["mismatches"])
    assert {"name": "score", "type": "float | str"} in found["actual"]


async def test_a_function_that_raises_is_a_dry_run_error(joined, made_readable, cluster):
    script = _changed(
        _GOOD,
        "    return [Order(**row.asDict())",
        "    raise RuntimeError('the cluster refused the job')\n    return [Order(**row.asDict())",
    )
    report = await _validated(joined, script)
    assert report["dry_run"]["status"] == "failed"
    found = _by_function(report["dry_run"])["orders"]
    raised = "raise RuntimeError('the cluster refused the job')"
    line = textwrap.dedent(script).splitlines().index(f"    {raised}") + 1
    # The error names the function, the script's line that failed and what it raised.
    assert found == {
        "function": "orders",
        "rows": 0,
        "status": "error",
        "error": f"orders, line {line}: {raised}: RuntimeError: the cluster refused the job",
    }
    assert _by_function(report["dry_run"])["read_of"]["status"] == "ok"


async def test_a_dry_run_error_names_the_expression_that_failed(joined, made_readable, cluster):
    script = _changed(
        _GOOD, "return [Read(statement=orders.sql)]", "return [Read(statement=orders.sql())]"
    )
    report = await _validated(joined, script)
    found = _by_function(report["dry_run"])["read_of"]
    assert found["status"] == "error"
    assert found["error"].startswith("read_of, line "), found
    assert "orders.sql()" in found["error"]
    assert found["error"].endswith("TypeError: 'str' object is not callable")


_COMMAND = '''

def purge(orders: TableRef[Order]) -> int:
    """Delete the orders' rows: it writes, so validation never runs it.

    @command
    """
    raise RuntimeError("a @command ran during validation")
'''


async def test_a_command_is_checked_statically_and_not_run(joined, made_readable, cluster):
    report = await _validated(
        joined, _GOOD + _COMMAND, inputs={**_INPUTS, "purge": {"orders": "sales.orders"}}
    )
    assert report["static"]["status"] == "ok", report["static"]
    assert {f["name"]: f["kind"] for f in report["static"]["functions"]}["purge"] == "command"
    assert report["dry_run"]["status"] == "ok", report["dry_run"]
    assert _by_function(report["dry_run"])["purge"] == {
        "function": "purge",
        "status": "not_run_by_design",
        "reason": "a @command may write or call out: it is checked statically and not run",
    }
    # Its table is not read: only the @query's tables are made readable.
    assert sorted(r["ref"] for r in made_readable) == ["geo.regions", "geo.stores", "sales.orders"]


async def test_a_function_whose_tables_are_not_named_needs_them(joined, made_readable, cluster):
    report = await _validated(joined, inputs={"match_stores": _INPUTS["match_stores"]})
    assert report["dry_run"]["status"] == "incomplete"
    assert _by_function(report["dry_run"])["read_of"] == {
        "function": "read_of",
        "status": "needs_input",
        "missing": ["orders"],
    }


async def test_with_no_cluster_the_dry_run_does_not_run(joined, made_readable, monkeypatch):
    async def resolved(admin_db, org_id, own):
        raise spark_cluster.SparkClusterRefused("not_configured", "No Spark cluster is configured")

    monkeypatch.setattr(spark_cluster, "resolved", resolved)
    report = await _validated(joined)
    assert report["static"]["status"] == "ok"
    assert report["cluster"] is None
    assert report["dry_run"]["status"] == "not_run"
    assert report["dry_run"]["reason"] == "No Spark cluster is configured"


# -- step 3: the keys ---------------------------------------------------------------------------------


async def test_a_key_the_resolver_does_not_declare_is_a_mismatch(joined, made_readable, cluster):
    report = await _validated(joined, keys={"match_stores": ("store_id", "region_key")})
    assert report["keys"]["status"] == "failed"
    found = _by_function(report["keys"])["match_stores"]
    assert found["status"] == "mismatch"
    assert "region_key" in found["problems"][0]


async def test_a_null_key_in_the_rows_is_a_mismatch(joined, made_readable, cluster):
    script = _changed(_GOOD, "Match(store_id=2, region_id=8", "Match(store_id=2, region_id=None")
    report = await _validated(joined, script)
    assert report["dry_run"]["status"] == "ok"
    found = _by_function(report["keys"])["match_stores"]
    assert found["status"] == "mismatch"
    assert found["problems"] == ["1 of 2 rows have a null region_id"]


async def test_a_resolver_whose_keys_are_not_named_needs_them(joined, made_readable, cluster):
    report = await _validated(joined, keys={})
    assert report["keys"]["status"] == "incomplete"
    assert _by_function(report["keys"])["match_stores"]["status"] == "needs_input"
