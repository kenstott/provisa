# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-20, HASURA NAMES REPRODUCED EXACTLY): the hasura-default and
graphql-default presets give the names Hasura v2 generates, compared name by name against
tests/fixtures/hasura_naming/expected_names.json (each entry cites the Hasura doc section it
rests on), and importing Hasura metadata picks the convention its source customization names."""

# Requirements: REQ-194, REQ-195

import json
from pathlib import Path

import pytest

from provisa.compiler import naming as nh
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.parser import parse_metadata_dir, parse_metadata_document

_FIXTURE = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "hasura_naming" / "expected_names.json").read_text()
)
_CONVENTIONS = ("hasura-default", "graphql-default")


def _table_cases():
    for entry in _FIXTURE["tables"]:
        for convention, names in entry["expected"].items():
            label = f"{entry['schema']}.{entry['table']}"
            if entry.get("custom_name"):
                label += f"~{entry['custom_name']}"
            if entry.get("custom_root_fields"):
                label += "~root"
            yield pytest.param(entry, convention, names, id=f"{label}-{convention}")


@pytest.mark.parametrize(("entry", "convention", "expected"), list(_table_cases()))
def test_table_root_fields_and_types_are_hasuras(entry, convention, expected):
    names = nh.hasura_table_names(
        convention,
        entry["table"],
        schema=entry["schema"],
        custom_name=entry.get("custom_name"),
        custom_root_fields=entry.get("custom_root_fields"),
    )
    got = {kind: getattr(names, kind) for kind in expected}
    assert got == expected


@pytest.mark.parametrize("convention", _CONVENTIONS)
def test_column_field_names_are_hasuras_and_custom_names_kept_as_written(convention):
    for case in _FIXTURE["fields"]["cases"]:
        got = nh.hasura_field_name(convention, case["column"], custom_name=case.get("custom_name"))
        assert got == case[convention], case


def test_arguments_and_response_fields_are_camel_under_graphql_default():
    for case in _FIXTURE["arguments"]["cases"]:
        assert nh.hasura_argument_name("graphql-default", case["name"]) == case["graphql-default"]
        assert nh.hasura_argument_name("hasura-default", case["name"]) == case["name"]


@pytest.mark.parametrize("convention", _CONVENTIONS)
def test_enum_values_are_hasuras(convention):
    for case in _FIXTURE["enum_values"]["cases"]:
        got = nh.hasura_enum_value(convention, case["value"], case["kind"])
        assert got == case[convention], case


@pytest.mark.parametrize("convention", _CONVENTIONS)
def test_snake_case_exceptions_hold_under_both_presets(convention):
    exceptions = _FIXTURE["snake_case_exceptions"]
    for case in exceptions["function_args"]:
        assert nh.hasura_function_args_type(convention, case["function"]) == case[convention]
    for case in exceptions["cast_exp"]:
        assert nh.hasura_cast_exp_type(convention, case["scalar"]) == case[convention]


@pytest.mark.parametrize("convention", _CONVENTIONS)
def test_relationship_fields_keep_their_metadata_name(convention):
    for case in _FIXTURE["relationships"]["cases"]:
        expected = case[convention]
        assert nh.hasura_relationship_field(case["name"]) == expected["field"]
        assert nh.hasura_relationship_aggregate(convention, case["name"]) == expected["aggregate"]


def test_an_unknown_convention_is_refused_by_name():
    with pytest.raises(ValueError, match="graphql-camel"):
        nh.hasura_table_names("graphql-camel", "my_table")


# ── the importer picks the convention its metadata names ──────────────────────────────────


def _document(customization: dict | None) -> dict:
    source: dict = {
        "name": "default",
        "kind": "postgres",
        "configuration": {"connection_info": {"database_url": "postgres://u:p@h:5432/d"}},
        "tables": [{"table": {"schema": "public", "name": "my_table"}}],
    }
    if customization is not None:
        source["customization"] = customization
    return {"version": 3, "sources": [source]}


def _source_convention(doc: dict, env: dict[str, str] | None = None) -> str | None:
    config = convert_metadata(parse_metadata_document(doc), auth_env=env)
    return next(s for s in config.sources if s.id == "default").gql_naming_convention


def test_import_reads_graphql_default_from_source_customization():
    doc = _document({"naming_convention": "graphql-default"})
    assert parse_metadata_document(doc).sources[0].naming_convention == "graphql-default"
    assert _source_convention(doc) == nh.preset_of("graphql-default")


def test_import_reads_hasura_default_from_source_customization():
    doc = _document({"naming_convention": "hasura-default"})
    assert _source_convention(
        doc, {"HASURA_GRAPHQL_DEFAULT_NAMING_CONVENTION": "graphql-default"}
    ) == (nh.preset_of("hasura-default"))


def test_import_without_customization_takes_the_deployment_default():
    doc = _document(None)
    env = {"HASURA_GRAPHQL_DEFAULT_NAMING_CONVENTION": "graphql-default"}
    assert _source_convention(doc, env) == nh.preset_of("graphql-default")


def test_import_without_customization_or_deployment_default_is_hasura_default():
    assert _source_convention(_document({"root_fields": {"prefix": "x_"}})) == nh.preset_of(
        "hasura-default"
    )


def test_import_refuses_an_unknown_naming_convention():
    with pytest.raises(ValueError, match="camel-case"):
        _source_convention(_document({"naming_convention": "camel-case"}))


def test_import_reads_customization_from_databases_yaml(tmp_path):
    md = tmp_path / "metadata"
    (md / "databases" / "default" / "tables").mkdir(parents=True)
    (md / "databases" / "databases.yaml").write_text(
        "- name: default\n"
        "  kind: postgres\n"
        "  configuration:\n"
        "    connection_info:\n"
        "      database_url: postgres://u:p@h:5432/d\n"
        "  customization:\n"
        "    naming_convention: graphql-default\n"
        '  tables: "!include default/tables/tables.yaml"\n'
    )
    (md / "databases" / "default" / "tables" / "tables.yaml").write_text(
        "- table:\n    schema: public\n    name: my_table\n"
    )
    metadata = parse_metadata_dir(md)
    assert metadata.sources[0].naming_convention == "graphql-default"
    src = convert_metadata(metadata).sources[0]
    assert src.gql_naming_convention == nh.preset_of("graphql-default")
