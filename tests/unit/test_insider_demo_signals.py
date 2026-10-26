# Copyright (c) 2026 Kenneth Stott
# Canary: b521283a-1bcb-4380-aefe-32740c2c2591
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The insider demo matches its mail with a code resolver, not host code (REQ-2056, REQ-2134).

deal_signals, a column transform (REQ-2131) of the insider_signals code source, is applied in
message_deal_signals over every message and deal; match_deals is a statement that writes the rows
a signal ties to deal_match, after the view is stored. The config holds no host code. The code
archive is the one the config names by its content hash and holds the function's source as the
demo's insider_signals/ directory has it. The signals are what the demo's matching always said:
a name a letter or two off, an alias, the ticker in its case, the code name, and the meaning
above its floor."""

# Requirements: REQ-2056, REQ-2131, REQ-2134
from __future__ import annotations

import importlib.util
import sys
import tarfile
import tempfile
from pathlib import Path

import pytest
import yaml

from provisa.codesource.bundle import content_hash, unpack

pytestmark = pytest.mark.unit

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "insider"
_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_SOURCES = {s["id"]: s for s in _CONFIG["sources"]}
_FUNCTIONS = {f["name"]: f for f in _CONFIG["functions"]}
_VIEWS = {v["id"]: v for v in _CONFIG["views"]}


def _module():
    spec = importlib.util.spec_from_file_location(
        "insider_signals", _DEMO / "insider_signals" / "insider_signals.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # its dataclass looks its module up while it is made
    spec.loader.exec_module(module)
    return module


_SIGNALS = _module()
_FALCON = {
    "legal_name": "Halcyon Biosciences, Inc.",
    "aliases": "Halcyon;Halcyon Bio",
    "ticker": "HLCB",
    "code_name": "Falcon",
}


def _one(subject: str, body: str, message_vector: list[float], deal_vector: list[float], **deal):
    deal = {**_FALCON, **deal}
    [found] = _SIGNALS.deal_signals(
        [subject],
        [body],
        [deal["legal_name"]],
        [deal["aliases"]],
        [deal["ticker"]],
        [deal["code_name"]],
        [message_vector],
        [deal_vector],
    )
    return found


def test_the_config_holds_no_host_code():
    assert not [f["name"] for f in _CONFIG["functions"] if f["impl_kind"] in {"python", "script"}]


def test_deal_signals_is_a_column_transform_of_the_code_resolver():
    command = _FUNCTIONS["deal_signals"]
    assert (command["impl_kind"], command["source_id"], command["function_name"]) == (
        "code_columns",
        "insider_signals",
        "deal_signals",
    )
    assert [a["name"] for a in command["arguments"]] == [
        "subject",
        "body",
        "legal_name",
        "aliases",
        "ticker",
        "code_name",
        "message_vector",
        "deal_vector",
    ]
    assert [c["name"] for c in command["output_columns"]] == [
        "name_match",
        "meaning_match",
        "signals",
    ]


def test_the_view_applies_it_and_is_stored_before_match_deals_reads_it():
    view = _VIEWS["message_deal_signals"]
    assert "deal_signals(" in view["sql"]
    # The pairing is the declared screening scope, joined along its relationships (V002).
    assert 'FROM "bank"."screening" AS s' in view["sql"]
    assert "CROSS JOIN" not in view["sql"]
    relationships = {r["id"]: r for r in _CONFIG["relationships"]}
    assert relationships["screening-message"]["target_table_id"] == "message"
    assert relationships["screening-deal"]["target_table_id"] == "deal"
    assert view["materialize"] is True
    assert _FUNCTIONS["match_deals"]["impl_kind"] == "statement"
    assert '"bank"."view_message_deal_signals"' in _FUNCTIONS["match_deals"]["binding"]["sql"]
    steps = yaml.safe_load((_DEMO / "setup.yaml").read_text())["steps"]
    assert steps.index({"refresh": "message_deal_signals"}) < steps.index(
        {"command": "match_deals"}
    )


def test_the_code_archive_is_the_one_the_config_names():
    archive = _DEMO / "code" / "insider_signals.tar.gz"
    with tempfile.TemporaryDirectory() as scratch:
        found = content_hash(unpack(archive.name, archive.read_bytes(), Path(scratch)))
    assert _SOURCES["insider_signals"]["mapping"]["code"] == {
        "kind": "upload",
        "upload": found,
        "filename": archive.name,
    }


def test_the_archive_holds_the_function_as_the_demo_shows_it():
    with tarfile.open(_DEMO / "code" / "insider_signals.tar.gz") as archive:
        names = set(archive.getnames())
        shipped = archive.extractfile("insider_signals.py")
        assert shipped is not None
        source = shipped.read().decode()
    assert names == {"insider_signals.py", "pyproject.toml"}
    assert source == (_DEMO / "insider_signals" / "insider_signals.py").read_text()


def test_names_its_deal_by_code_name_ticker_and_company():
    found = _one(
        "Project Falcon - draft merger agreement",
        "Attached is the latest draft merger agreement for Halcyon Biosciences. Please send "
        "comments on the HLCB tender offer mechanics and the Falcon timetable by Friday.",
        [1.0, 0.0],
        [0.0, 1.0],
    )
    assert found.signals == "legal_name, alias, ticker, code_name"
    assert found.name_match == 1.0
    assert found.meaning_match == 0.0


def test_a_name_a_letter_off_still_names_the_company():
    found = _one("note", "Halcyon Bioscienses is moving.", [1.0, 0.0], [0.0, 1.0], aliases="x")
    assert found.signals == "legal_name"
    assert 0.85 <= found.name_match < 1.0


def test_the_ticker_counts_only_in_its_case():
    assert _one("hlcb", "about hlcb", [1.0, 0.0], [0.0, 1.0], aliases="x").signals == ""


def test_a_paraphrase_that_avoids_every_name_scores_by_meaning_alone():
    found = _one(
        "fyi",
        "the gene therapy outfit is about to be bought out",
        [0.8, 0.6],
        [1.0, 0.0],
    )
    assert (found.signals, found.name_match, found.meaning_match) == ("meaning", 0.0, 0.8)


def test_a_meaning_below_its_floor_is_no_signal():
    found = _one("lunch", "see you at noon", [0.39, 0.92], [1.0, 0.0])
    assert (found.signals, found.meaning_match) == ("", 0.0)


def test_match_deals_resolves_against_the_demos_own_tables_and_views():
    """The statement names the stored view as it is registered, view_<id> (config_loader)."""
    import copy

    from provisa.compiler.statement_contract import derive
    from provisa.core.config_loader import views_as_tables

    raw = views_as_tables(copy.deepcopy(_CONFIG))
    tables = [
        {
            "id": i,
            "domain_id": t.get("domain_id"),
            "schema_name": t.get("schema"),
            "table_name": t["table"],
            "alias": t.get("alias"),
            "view_sql": t.get("view_sql"),
            "columns": [
                {"column_name": c["name"], "data_type": c["data_type"]}
                for c in t.get("columns", [])
            ],
        }
        for i, t in enumerate(raw["tables"])
    ]
    command = _FUNCTIONS["match_deals"]
    contract = derive(command["name"], command["binding"], [], tables)
    assert contract.broken is None
    assert (contract.kind, contract.writes) == ("mutation", "bank.deal_match")


def _governed_violations(sql: str) -> list:
    """The V-rule violations of ``sql`` as the pipeline judges it for org_admin, the role the
    setup runs as: the demo's own tables, views and relationships, each view it reads replaced by
    its SQL first (provisa.mv.view_read.semantic_statement), then validate_sql."""
    import copy

    from provisa.compiler import naming
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput
    from provisa.compiler.sql_validator import validate_sql
    from provisa.compiler.stage2 import build_governance_context
    from provisa.compiler.view_expand import expand_view_refs
    from provisa.core.config_loader import views_as_tables
    from provisa.core.models import config_table_name

    config = views_as_tables(copy.deepcopy(_CONFIG))
    tables, column_types = [], {}
    for i, t in enumerate(config["tables"], start=1):
        tables.append(
            {
                "id": i,
                "source_id": t["source_id"],
                "domain_id": t["domain_id"],
                "schema_name": t.get("schema", "default"),
                "table_name": config_table_name(t),
                "alias": t.get("alias"),
                **({"view_sql": t["view_sql"]} if "view_sql" in t else {}),
                "enable_aggregates": False,
                "enable_group_by": False,
                "columns": [
                    {
                        "column_name": c["name"],
                        "visible_to": c["visible_to"],
                        "data_type": c["data_type"],
                        **({"is_primary_key": True} if c.get("is_primary_key") else {}),
                    }
                    for c in t["columns"]
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        )
        column_types[i] = [
            ColumnMetadata(column_name=c["name"], data_type=c["data_type"], is_nullable=True)
            for c in t["columns"]
        ]
    ids = {t["table_name"]: t["id"] for t in tables}
    relationships = [
        {
            **r,
            "source_table_id": ids[r["source_table_id"]],
            "target_table_id": ids[r["target_table_id"]],
        }
        for r in _CONFIG["relationships"]
        if r["source_table_id"] in ids and r["target_table_id"] in ids
    ]
    # REQ-2155: the grants folded over each role's ancestors, as the server's build does.
    from provisa.core.db import fold_config_grants

    fold_config_grants(tables, config.get("roles") or [])
    role = {"id": "org_admin", "domain_access": ["*"], "capabilities": []}
    naming.configure(gql="apollo_graphql")
    schema_input = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=config["domains"],
        domain_prefix=config["naming"]["domain_prefix"],
    )
    ctx = build_context(schema_input)
    gov = build_governance_context(
        "org_admin",
        None,
        {},
        ctx,
        tables,
        role=role,
        relationships=relationships,
        source_types={s["id"]: s["type"] for s in config["sources"]},
    )
    views = {t["table_name"]: t["view_sql"] for t in tables if "view_sql" in t}
    return validate_sql(
        expand_view_refs(sql, views, mark=True),
        ctx,
        gov,
        role,
        tables,
        bypass_uncovered_relationships=True,
    )


def test_match_deals_joins_only_along_the_demos_relationships():
    """What match_deals reads, with the view it reads expanded, is governed clean (V002): the
    team through the sender's employee row, the counterparty along recipient-counterparty."""
    import sqlglot

    statement = sqlglot.parse_one(_FUNCTIONS["match_deals"]["binding"]["sql"], read="postgres")
    assert _governed_violations(statement.expression.sql(dialect="postgres")) == []


def test_the_view_pairs_messages_and_deals_only_through_the_screening_scope():
    assert _governed_violations(_VIEWS["message_deal_signals"]["sql"]) == []
