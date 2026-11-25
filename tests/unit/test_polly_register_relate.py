# Copyright (c) 2026 Kenneth Stott
# Canary: 7eb07dfd-33c7-491a-881f-270952764c15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2114 (amended 2026-11-24): Polly registers tables by each kind of registration a source
type uses, and creates relationships with their full input. The descriptor's registration part
and the prompt are held to the real admin GraphQL schema, so neither can drift from it."""

from __future__ import annotations

import pytest
from graphql import OperationDefinitionNode, parse, validate
from graphql.execution.values import get_variable_values

from provisa.api.admin import source_types as st
from provisa.api.mcp import admin_api
from provisa.api.mcp import chat as chat_mod
from provisa.core.models import Cardinality
from provisa.core.paging import PaginationType


def _flat(text: str) -> str:
    return " ".join(text.split())


def _schema():
    return admin_api._graphql_schema()


def _input_fields(name: str) -> list[str]:
    return [f["name"] for f in admin_api.schema_type(name)["fields"]]


def _root(kind: str) -> set[str]:
    return {f["name"] for f in admin_api.schema_fields(kind)}


def _checked(example: tuple[str, dict]) -> dict:
    """The example's document validated against the admin schema and its variables coerced by
    the schema's input types: an unknown field, a wrong type or a missing required one fails."""
    document = parse(example[0])
    assert validate(_schema(), document) == []
    (operation,) = [d for d in document.definitions if isinstance(d, OperationDefinitionNode)]
    coerced = get_variable_values(_schema(), operation.variable_definitions or [], example[1])
    assert not isinstance(coerced, list), coerced
    return coerced


_EXAMPLES = {
    "register_tables": chat_mod._REGISTER_TABLES_EXAMPLE,
    "foreign_key": chat_mod._RELATE_FOREIGN_KEY_EXAMPLE,
    "junction": chat_mod._RELATE_JUNCTION_EXAMPLE,
    "computed": chat_mod._RELATE_COMPUTED_EXAMPLE,
    "code_resolver": chat_mod._RELATE_CODE_RESOLVER_EXAMPLE,
}


class TestTheDescriptorsRegistration:
    def test_its_mutations_queries_and_fields_are_the_schemas(self):
        mutations, queries = _root("mutation"), _root("query")
        table_input = set(_input_fields("TableInput"))
        assert set(st.REGISTER_BASE_FIELDS) <= table_input
        for kind in st.registration_kinds():
            assert set(kind.mutations) <= mutations, kind.kind
            assert set(kind.find) <= queries, kind.kind
            assert set(kind.fields) <= table_input, kind.kind

    def test_every_offered_type_registers_tables_somehow(self):
        used = set()
        for d in st.source_types():
            kinds = st.registration(d)
            assert kinds, d.name
            used |= {k.kind for k in kinds}
        assert used == {k.kind for k in st.registration_kinds()}

    @pytest.mark.parametrize(
        ("type_name", "kinds"),
        [
            ("postgresql", ["tables"]),
            ("files", ["tables", "file_glob"]),
            ("openapi", ["openapi_operation"]),
            ("python", ["code"]),
            ("spark", ["code"]),
            ("neo4j", ["cypher"]),
            ("graphql", ["connection"]),
            ("github", ["connection"]),
        ],
    )
    def test_each_type_names_its_kinds(self, type_name, kinds):
        assert [k.kind for k in st.registration(st.source_type(type_name))] == kinds

    def test_an_openapi_brand_registers_operations(self):
        brand = next(d for d in st.source_types() if d.brand and d.carrier == "openapi")
        assert [k.kind for k in st.registration(brand)] == ["openapi_operation"]

    def test_openapi_paging_names_every_paging_type_and_real_paging_fields(self):
        (kind,) = st.registration(st.source_type("openapi"))
        note = _flat(kind.note)
        for paging_type in PaginationType:
            assert paging_type.value in note
        for name in _input_fields("PagingInput"):
            if name != "maxRows":  # a connection table's bound, not an endpoint's
                assert name in note, name

    def test_code_registration_covers_discover(self):
        (kind,) = st.registration(st.source_type("python"))
        assert "@discover" in kind.note
        assert "@table" in kind.note

    async def test_the_source_types_tool_returns_the_registration(self):
        result = await chat_mod._dispatch_tool(
            None, "admin", "source_types", {"type": "files"}, scope=None
        )
        registration = result["registration"]
        assert registration["base_fields"] == list(st.REGISTER_BASE_FIELDS)
        glob = next(k for k in registration["kinds"] if k["kind"] == "file_glob")
        assert glob["fields"] == ["fileGlob", "sourceFileColumn"]
        assert "fileGlobPreview" in glob["find"]
        assert glob["mutations"] == ["registerTable", "registerTables"]


class TestThePrompt:
    def test_it_names_every_registration_kind_and_its_fields(self):
        s = _flat(chat_mod._SYSTEM)
        for name in st.REGISTER_MUTATIONS + st.REGISTER_BASE_FIELDS:
            assert name in s, name
        for kind in st.registration_kinds():
            assert f"'{kind.kind}'" in s, kind.kind
            for name in kind.fields:
                assert name in s, (kind.kind, name)
        for name in ("fileGlobPreview", "sampleOpenapiColumns", "@discover", "@table"):
            assert name in s, name

    def test_it_names_every_relationship_input_field(self):
        s = chat_mod._SYSTEM
        for name in _input_fields("RelationshipInput"):
            assert name in s, name
        for name in _input_fields("RelationshipParameterInput"):
            assert name in s, name

    def test_it_names_every_code_resolver_input_field(self):
        s = chat_mod._SYSTEM
        assert "createCodeResolver" in s
        for name in _input_fields("CodeResolverInput"):
            assert name in s, name

    def test_it_names_every_cardinality(self):
        s = chat_mod._SYSTEM
        for cardinality in Cardinality:
            assert cardinality.value in s

    def test_every_example_is_in_the_prompt(self):
        s = chat_mod._SYSTEM
        for example in _EXAMPLES.values():
            assert chat_mod._example(example) in s


class TestTheExamplesAreValidAdminMutations:
    @pytest.mark.parametrize("name", sorted(_EXAMPLES))
    def test_the_example_validates_against_the_admin_schema(self, name):
        _checked(_EXAMPLES[name])

    def test_the_batch_example_covers_glob_paging_and_arguments(self):
        tables = _checked(chat_mod._REGISTER_TABLES_EXAMPLE)["inputs"]
        assert any(t.get("fileGlob") for t in tables)
        assert any(t.get("pagination") and t.get("arguments") for t in tables)

    @pytest.mark.parametrize("name", ["foreign_key", "junction", "computed"])
    def test_its_cardinality_is_one_the_server_takes(self, name):
        Cardinality(_EXAMPLES[name][1]["input"]["cardinality"])

    def test_the_junctions_key_lists_pair_with_its_own(self):
        rel = chat_mod._RELATE_JUNCTION_EXAMPLE[1]["input"]
        assert len(rel["viaSourceColumn"].split(",")) == len(rel["sourceColumn"].split(","))
        assert len(rel["viaTargetColumn"].split(",")) == len(rel["targetColumn"].split(","))


class TestThroughAdminMutation:
    """A createRelationship with a junction, a computed one and a code resolver, each proposed as
    Polly proposes it -- an admin_mutation client call -- and executed by the admin schema up to
    the save, which receives the input the user's confirmation would send."""

    @staticmethod
    def _call(example: tuple[str, dict], summary: str) -> dict:
        return {
            "id": "call-1",
            "name": "admin_mutation",
            "input": {"mutation": example[0], "variables": example[1], "summary": summary},
        }

    @staticmethod
    def _handed(call: dict) -> dict:
        client, server = chat_mod._split_tool_calls([call])
        assert server == [] and client == [call]
        prepared, note = chat_mod._prepare_client_call(call)
        assert note is None
        assert prepared == call
        return prepared["input"]

    @staticmethod
    async def _execute(handed: dict) -> dict:
        from provisa.api.admin.schema import admin_schema

        result = await admin_schema.execute(
            handed["mutation"], variable_values=handed["variables"], context_value={}
        )
        assert result.errors is None, result.errors
        return result.data

    @pytest.fixture
    def saved(self, monkeypatch):
        from provisa.api.admin import code_resolver, schema_mutation
        from provisa.api.admin.types import MutationResult

        seen: list = []

        async def save_relationship(info, input, *, creating):
            seen.append(("relationship", input, creating))
            return MutationResult(success=True, message="saved")

        async def save_code_resolver(info, input, *, creating):
            seen.append(("code_resolver", input, creating))
            return MutationResult(success=True, message="saved")

        monkeypatch.setattr(schema_mutation, "require_capability", lambda *a, **k: None)
        monkeypatch.setattr(schema_mutation, "_save_relationship_as_caller", save_relationship)
        monkeypatch.setattr(code_resolver, "save", save_code_resolver)
        return seen

    async def test_a_junction_relationship(self, saved):
        handed = self._handed(
            self._call(chat_mod._RELATE_JUNCTION_EXAMPLE, "Relate students to courses.")
        )
        data = await self._execute(handed)
        assert data["createRelationship"]["success"] is True
        ((kind, rel, creating),) = saved
        assert (kind, creating) == ("relationship", True)
        assert rel.via_table == "enrollments"
        assert (rel.via_source_column, rel.via_target_column) == ("student_id", "course_id")
        assert (rel.via_type_column, rel.via_type_value) == ("role", "enrolled")
        assert rel.via_label_source == "column"
        assert Cardinality(rel.cardinality) is Cardinality.one_to_many

    async def test_a_computed_relationship(self, saved):
        handed = self._handed(
            self._call(chat_mod._RELATE_COMPUTED_EXAMPLE, "Relate customers to their score.")
        )
        await self._execute(handed)
        ((_, rel, _),) = saved
        assert (rel.target_function_name, rel.function_arg) == ("credit_score", "customer_id")
        assert rel.target_table_id == "" and rel.target_column == ""

    async def test_a_code_resolver(self, saved):
        handed = self._handed(
            self._call(chat_mod._RELATE_CODE_RESOLVER_EXAMPLE, "Match leads to accounts.")
        )
        data = await self._execute(handed)
        assert data["createCodeResolver"]["success"] is True
        ((kind, resolver, creating),) = saved
        assert (kind, creating) == ("code_resolver", True)
        assert resolver.resolver == "match_names"
        assert (resolver.source_match, resolver.target_match) == ("lead_company", "account_name")
        assert resolver.refresh_interval == 3600
