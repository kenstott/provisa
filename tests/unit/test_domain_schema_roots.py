# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8f1a62-5b7e-4c09-9e2a-71c4b0d5e8f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A per-domain schema (``/data/introspection?domain=...``, the GraphQL explorer's tree) has the
requested domains' root fields and no others. meta, ops and ontology are listed in the Domains
menu like any domain, so they filter like any domain: with only pet-store and shelter checked the
explorer showed a meta group holding ``m__resolve``, meta's resolver, because it was added to every
schema whatever domains were asked for. A command or a subscription of a domain not asked for is
the same leak: the requested domains chose only the tables' Query fields."""

# Requirements: REQ-039, REQ-525, REQ-1365, REQ-1723

from __future__ import annotations

import pytest

from provisa.compiler.introspect import ColumnMetadata

pytestmark = pytest.mark.unit

#: (domain, table)
_TABLES = [
    ("meta", "registered_tables"),
    ("ops", "queries"),
    ("ontology", "_terms"),
    ("pet-store", "pets"),
    ("shelter", "kennels"),
    ("finance", "ledger"),
]
_ID = {domain: i for i, (domain, _) in enumerate(_TABLES, 1)}


def _command(name: str, domain: str, kind: str) -> dict:
    return {
        "name": name,
        "arguments": [],
        "returns": "",
        "domain_id": domain,
        "kind": kind,
        "visible_to": ["admin"],
    }


def _schema_input(root_domains: set[str] | None):
    from provisa.compiler.schema_gen import SchemaInput

    return SchemaInput(
        tables=[
            {
                "id": i,
                "source_id": "pg",
                "domain_id": domain,
                "schema_name": "public",
                "table_name": name,
                "watermark_column": "id",  # every table offers a subscription
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for i, (domain, name) in enumerate(_TABLES, 1)
        ],
        relationships=[],
        column_types={
            i: [ColumnMetadata(column_name="id", data_type="varchar", is_nullable=True)]
            for i in range(1, len(_TABLES) + 1)
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": d} for d, _ in _TABLES],
        domain_prefix=True,
        functions=[
            _command("count_pets", "pet-store", "query"),
            _command("close_books", "finance", "mutation"),
            _command("audit_queries", "ops", "query"),
        ],
        root_table_ids=(None if root_domains is None else {_ID[d] for d in root_domains}),
        root_domain_ids=root_domains,
    )


def _roots(schema) -> dict[str, set[str]]:
    return {
        "query": set(schema.query_type.fields),
        "mutation": set(schema.mutation_type.fields) if schema.mutation_type else set(),
        "subscription": (
            set(schema.subscription_type.fields) if schema.subscription_type else set()
        ),
    }


def _domains_of(names: set[str]) -> set[str]:
    """The domain each root field is named for: its alias before ``__``, or the ontology's on_ and
    the ops schema's op_ (REQ-2163). The resolver, a field with neither, names no domain."""
    out = set()
    for n in names:
        if n.startswith(("on_", "op_")):
            out.add(n[:2])
        elif "__" in n:
            out.add(n.split("__", 1)[0])
    return out


def _has(names: set[str], command: str) -> bool:
    """Whether a root field is ``command``'s, under the active naming convention."""
    flat = command.replace("_", "")
    return any(n.lower().replace("_", "").endswith(flat) for n in names)


def test_unchecked_meta_gives_the_explorer_no_meta_root_field():
    from provisa.compiler.schema_gen import generate_schema

    roots = _roots(generate_schema(_schema_input({"pet-store", "shelter"})))
    assert "m__resolve" not in roots["query"]
    for kind, names in roots.items():
        assert _domains_of(names) <= {"ps", "s"}, f"{kind}: {sorted(names)}"


def test_checked_meta_gives_the_explorer_meta_s_resolver():
    from provisa.compiler.schema_gen import generate_schema, resolve_field

    schema = generate_schema(_schema_input({"meta", "pet-store"}))
    assert resolve_field(schema) == "m__resolve"


def test_a_command_of_a_domain_not_asked_for_is_not_a_root_field():
    from provisa.compiler.schema_gen import generate_schema

    roots = _roots(generate_schema(_schema_input({"pet-store"})))
    assert _has(roots["query"], "count_pets"), sorted(roots["query"])
    assert not _has(roots["query"], "audit_queries")
    assert not _has(roots["mutation"], "close_books")


def test_a_subscription_of_a_domain_not_asked_for_is_not_a_root_field():
    from provisa.compiler.schema_gen import generate_schema

    roots = _roots(generate_schema(_schema_input({"shelter"})))
    assert _domains_of(roots["subscription"]) == {"s"}, sorted(roots["subscription"])


def test_the_role_schema_with_no_domain_asked_keeps_every_root_field():
    from provisa.compiler.schema_gen import generate_schema, resolve_field

    schema = generate_schema(_schema_input(None))
    roots = _roots(schema)
    assert resolve_field(schema) == "m__resolve"
    assert _has(roots["mutation"], "close_books")
    assert _has(roots["query"], "audit_queries")
    assert len(_domains_of(roots["subscription"])) == len(_TABLES)


# --- the gRPC page's per-domain proto (/data/proto/{role}?domains=...) is the same rule ----------


def test_the_per_domain_proto_has_only_the_asked_domains_commands_and_no_resolve():
    from provisa.grpc.proto_gen import RESOLVE_RPC, generate_proto

    proto = generate_proto(_schema_input({"pet-store", "shelter"}))
    assert f"rpc {RESOLVE_RPC}(" not in proto
    assert f"message {RESOLVE_RPC}AddressRequest" not in proto
    rpcs = {line.split()[1].split("(")[0] for line in proto.splitlines() if "  rpc " in line}
    assert any(r.lower().endswith("countpets") for r in rpcs), sorted(rpcs)
    assert not any(r.lower().endswith(("auditqueries", "closebooks")) for r in rpcs)


@pytest.mark.parametrize("root_domains", [None, {"meta"}])
def test_the_proto_keeps_resolve_where_meta_is_offered(root_domains):
    from provisa.grpc.proto_gen import RESOLVE_RPC, generate_proto

    proto = generate_proto(_schema_input(root_domains))
    assert f"rpc {RESOLVE_RPC}({RESOLVE_RPC}AddressRequest)" in proto
