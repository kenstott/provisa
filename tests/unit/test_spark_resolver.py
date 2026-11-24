# Copyright (c) 2026 Kenneth Stott
# Canary: 8c1f4b27-6e93-4d05-b2a8-0d7e5c9f13a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Spark script's two-table @query is a code resolver, as a Python code source's is (REQ-2225,
REQ-2249).

The connector's real host reads the script it is given at pairing (Spark stood in for, as in
tests/unit/test_connector_script.py); the manifest the join keeps carries the @query with its two
table parameters and the @change keeping a set's bridge. The command registered from it has a
resolver's shape and is held to both keys as any resolver is, and the incremental option is open
only for the table the script's @change names, with the reason shown otherwise."""

# Requirements: REQ-2225, REQ-2249

from __future__ import annotations

import textwrap
from types import SimpleNamespace

import pytest

from provisa.codesource import commands
from provisa.codesource.commands import CODE_COMMAND, contract
from provisa.codesource.manifest import CHANGE, TableOf, manifest_from_json
from provisa.codesource.resolver import ResolverRefused, check_resolver, is_resolver
from provisa.mv.change_refresh import NO_CHANGE, incremental_option
from tests.unit import test_connector_script as _script
from tests.unit.test_connector_script import _join, _joined, _mapping

pytestmark = pytest.mark.unit

# The connector's real host and the join's stand-ins, the fixtures of test_connector_script.
host = _script.host
wired = _script.wired

_RESOLVER = '''
from dataclasses import dataclass

from provisa_services import TableRef
from provisa_spark import dataframe


@dataclass
class Store:
    id: int
    region: int


@dataclass
class Region:
    id: int


@dataclass
class Match:
    store_id: int
    region_id: int
    score: float


@dataclass
class Changed:
    store_id: int
    region_id: int
    score: float
    removed: bool
    seq: int


def match_on_cluster(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
    """Each store matched to its region on the cluster.

    @query
    """
    frame = dataframe(stores).join(dataframe(regions))
    return [Match(r.store_id, r.region_id, 1.0) for r in frame.collect()]
'''

_CHANGE = '''

def store_regions_changes(
    stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
) -> list[Changed]:
    """The matches changed since ``since``.

    @change table=store_regions
    @change key=store_id,region_id
    @change removed=removed
    @change cursor=seq
    """
    return []
'''


async def _manifest(host, wired, script: str):
    _joined(wired, host)
    mapping = await _join(_mapping(host, textwrap.dedent(script)))
    return mapping, manifest_from_json(mapping["remote"]["manifest"])


def _command(manifest, name: str) -> dict:
    """The command registering ``name``, as actions_router._as_code_command saves it."""
    (export,) = [e for e in manifest.exports if e.name == name]
    arguments, outputs = contract(manifest, export)
    return {
        "name": name,
        "function_name": name,
        "source_id": "spark_prod",
        "impl_kind": CODE_COMMAND,
        "kind": commands.command_kind(export),
        "arguments": arguments,
        "output_columns": outputs,
    }


@pytest.fixture
def prepared(monkeypatch):
    """The source's prepared manifest, as commands._prepared reads it for a registered source."""
    held: dict = {}

    async def _prepared(_state, source_id, _function):
        assert source_id == "spark_prod"
        return SimpleNamespace(manifest=held["manifest"])

    monkeypatch.setattr(commands, "_prepared", _prepared)
    return held


async def test_the_scripts_two_table_query_and_change_are_in_the_joined_manifest(host, wired):
    mapping, manifest = await _manifest(host, wired, _RESOLVER + _CHANGE)
    kinds = {e.name: e.kind for e in manifest.exports}
    assert kinds["match_on_cluster"] == "query"
    assert kinds["store_regions_changes"] == CHANGE
    (query,) = [e for e in manifest.exports if e.name == "match_on_cluster"]
    assert [isinstance(p.shape, TableOf) for p in query.parameters] == [True, True]
    (change,) = [e for e in manifest.exports if e.kind == CHANGE]
    assert change.change is not None
    assert change.change.table == "store_regions"
    assert change.change.key == ("store_id", "region_id")
    assert "match_on_cluster" in mapping["remote"]["exports"]


async def test_the_registered_spark_query_has_a_resolvers_shape(host, wired):
    _, manifest = await _manifest(host, wired, _RESOLVER)
    command = _command(manifest, "match_on_cluster")
    assert [a["arg_kind"] for a in command["arguments"]] == ["table_ref", "table_ref"]
    assert [c["name"] for c in command["output_columns"]] == ["store_id", "region_id", "score"]
    assert is_resolver(command)
    check_resolver(command, "store_id", "region_id")


async def test_a_spark_resolver_is_held_to_both_keys(host, wired):
    _, manifest = await _manifest(host, wired, _RESOLVER)
    with pytest.raises(ResolverRefused, match="carry no 'shop_id'"):
        check_resolver(_command(manifest, "match_on_cluster"), "shop_id", "region_id")


async def test_the_incremental_option_is_open_for_the_table_the_scripts_change_names(
    host, wired, prepared
):
    _, prepared["manifest"] = await _manifest(host, wired, _RESOLVER + _CHANGE)
    command = _command(prepared["manifest"], "match_on_cluster")
    option = await incremental_option(SimpleNamespace(), command, "store_regions")
    assert option.available


async def test_a_spark_resolver_with_no_change_has_the_incremental_option_disabled(
    host, wired, prepared
):
    _, prepared["manifest"] = await _manifest(host, wired, _RESOLVER)
    command = _command(prepared["manifest"], "match_on_cluster")
    option = await incremental_option(SimpleNamespace(), command, "store_regions")
    assert not option.available
    assert option.code == NO_CHANGE
    assert option.reason is not None and "@change table=store_regions" in option.reason
