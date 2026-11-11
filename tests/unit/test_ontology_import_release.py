# Copyright (c) 2026 Kenneth Stott
# Canary: 0e5c8b27-f413-4a96-b7d2-6c19a3e40f85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2237 (amended: A WHOLE RELEASE IMPORTS, AND NOTHING IS LOST). A whole ontology release --
every module file of it, as one archive -- imports, resolving the release's own imports between its
files locally and never fetching; an import of a module outside the archive is counted as left
out. What Provisa's terms do not model is kept on the term it belongs to, shown on its page and
written back out on export: restrictions and cardinalities, disjointness, inverse and transitive
properties, property chains, named individuals with properties of their own, datatype ranges and
annotations. An object property keeps its domain and range rather than becoming a part-of edge;
a plain enumerated individual still comes in as a valid value.

The release is tests/fixtures/ontology_release/release.zip, three modules in two formats and a
catalog (its sources beside it, in release/)."""

# Requirements: REQ-2237

from __future__ import annotations

import base64
import socket
import types
import urllib.request
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
import yaml
from rdflib import Graph
from sqlalchemy import insert

from provisa.api.admin import glossary_router
from provisa.api.admin import ontology_import_router as router
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import domains, sources
from provisa.ontology_import import kept, parse
from provisa.ontology_import.export import graph_of, serialize
from tests.unit.gate_identity import serve_every_column

if TYPE_CHECKING:
    from fastapi import Request

FIXTURE = Path(__file__).parents[1] / "fixtures" / "ontology_release"
RELEASE = FIXTURE / "release.zip"

FND = "https://example.org/mini-fibo/foundations/Parties/"
AGR = "https://example.org/mini-fibo/foundations/Agreements/"
ACC = "https://example.org/mini-fibo/business/Accounts/"
OWL = "http://www.w3.org/2002/07/owl#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
SKOS = "http://www.w3.org/2004/02/skos/core#"
XSD = "http://www.w3.org/2001/XMLSchema#"


@pytest.fixture
def no_network(monkeypatch):
    """Every way out of the process refused: an import that reached for the network fails."""
    reached: list[Any] = []

    def refuse(*args, **kwargs):
        reached.append(args)
        raise AssertionError(f"the import reached for the network: {args!r}")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(urllib.request, "urlopen", refuse)
    return reached


def _release() -> parse.Parsed:
    return parse.parse("mini-fibo.zip", RELEASE.read_bytes())


def _by_iri(parsed: parse.Parsed) -> dict[str, parse.Item]:
    return {iri: item for item in parsed.items for iri in item.keys}


def _kinds(item: parse.Item) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for statement in item.statements:
        out.setdefault(statement["kind"], []).append(statement)
    return out


def _edges(parsed: parse.Parsed) -> set[tuple[str, str, str]]:
    return {
        (parsed.items[e.source].name, e.rel_type, parsed.items[e.target].name) for e in parsed.edges
    }


# -- the release ---------------------------------------------------------------------------------


def test_the_checked_in_archive_holds_exactly_the_release_sources():
    with zipfile.ZipFile(RELEASE) as archive:
        members = {i.filename: archive.read(i) for i in archive.infolist() if not i.is_dir()}
    sources_ = {
        str(path.relative_to(FIXTURE / "release")): path.read_bytes()
        for path in (FIXTURE / "release").rglob("*")
        if path.is_file()
    }
    assert members == sources_


def test_every_module_of_a_release_is_read_and_its_imports_resolve_inside_it(no_network):
    parsed = _release()
    assert parsed.format == parse.ARCHIVE
    assert parsed.modules == [
        "business/Accounts.ttl",
        "foundations/Agreements.rdf",
        "foundations/Parties.ttl",
    ]
    # Accounts imports Parties by its version IRI, Agreements by its ontology IRI; Agreements
    # imports Parties. Commons is no module of the archive: counted, never fetched.
    assert parsed.imports == {
        "https://example.org/mini-fibo/2026Q4/foundations/Parties/": "foundations/Parties.ttl",
        AGR: "foundations/Agreements.rdf",
        FND: "foundations/Parties.ttl",
        "https://www.omg.org/spec/Commons/Classifiers/": None,
    }
    assert parsed.left_out["import of a module outside the archive"] == 1
    assert parsed.left_out["archive catalog"] == 1
    assert parsed.left_out["archive file that is no module"] == 1
    items = _by_iri(parsed)
    # Items of all three modules, statements across them: an account is a kind of agreement.
    assert {f"{FND}Party", f"{AGR}Agreement", f"{ACC}Account"} <= set(items)
    assert ("account", "KIND_OF", "agreement") in _edges(parsed)
    assert no_network == []


def test_the_catalog_resolves_an_import_no_module_declares(no_network, tmp_path):
    archive = tmp_path / "release.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.writestr(
            "catalog-v001.xml",
            '<catalog xmlns="urn:oasis:names:tc:entity:xmlns:xml:catalog">'
            '<uri name="https://example.org/alias/" uri="m/a.ttl"/></catalog>',
        )
        out.writestr(
            "m/a.ttl",
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
            "<https://example.org/a> a owl:Ontology .\n"
            "<https://example.org/A> a owl:Class .\n",
        )
        out.writestr(
            "m/b.ttl",
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
            "<https://example.org/b> a owl:Ontology ; owl:imports <https://example.org/alias/> .\n"
            "<https://example.org/B> a owl:Class .\n",
        )
    parsed = parse.parse("release.zip", archive.read_bytes())
    assert parsed.imports == {"https://example.org/alias/": "m/a.ttl"}
    assert "import of a module outside the archive" not in parsed.left_out


def test_an_archive_reaching_outside_itself_is_refused(tmp_path):
    archive = tmp_path / "evil.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.writestr("../escape.ttl", "")
    with pytest.raises(parse.ImportRefused, match="names a file outside itself"):
        parse.parse("evil.zip", archive.read_bytes())


def test_a_catalog_declaring_an_entity_is_refused_and_nothing_is_read(tmp_path, no_network):
    archive = tmp_path / "release.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.writestr(
            "catalog-v001.xml",
            '<?xml version="1.0"?><!DOCTYPE c [<!ENTITY x SYSTEM "https://example.org/x">]>'
            '<catalog xmlns="urn:oasis:names:tc:entity:xmlns:xml:catalog">&x;</catalog>',
        )
        out.writestr(
            "a.ttl",
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n<https://example.org/A> a owl:Class .\n",
        )
    with pytest.raises(parse.ImportRefused, match="is not an XML catalog"):
        parse.parse("release.zip", archive.read_bytes())


def test_bytes_that_are_no_zip_are_refused_naming_the_file():
    with pytest.raises(parse.ImportRefused, match="'x.zip' is not a zip archive"):
        parse.parse("x.zip", b"not a zip")


# -- nothing is lost -----------------------------------------------------------------------------


def test_restrictions_and_cardinalities_are_kept_on_the_class():
    account = _by_iri(_release())[f"{ACC}Account"]
    kinds = _kinds(account)
    (restriction,) = kinds[kept.RESTRICTION]
    assert restriction["subject"] == f"{ACC}Account"
    assert restriction["predicate"] == f"{RDFS}subClassOf"
    assert restriction["object"] == {
        "node": [
            [f"{RDF}type", {"iri": f"{OWL}Restriction"}],
            [f"{OWL}onProperty", {"iri": f"{ACC}isHeldBy"}],
            [f"{OWL}someValuesFrom", {"iri": f"{FND}Party"}],
        ]
    }
    (cardinality,) = kinds[kept.CARDINALITY]
    assert [
        f"{OWL}qualifiedCardinality",
        {"value": "1", "datatype": f"{XSD}nonNegativeInteger"},
    ] in (cardinality["object"]["node"])
    assert [f"{OWL}onDataRange", {"iri": f"{XSD}string"}] in cardinality["object"]["node"]


def test_disjointness_and_annotations_are_kept_with_labels_by_language():
    parsed = _release()
    organization = _kinds(_by_iri(parsed)[f"{FND}Organization"])
    assert [(s["predicate"], s["object"]) for s in organization[kept.DISJOINTNESS]] == [
        (f"{OWL}disjointWith", {"iri": f"{FND}Person"})
    ]
    annotations = {(s["predicate"], str(s["object"])) for s in organization[kept.ANNOTATION]}
    assert (
        f"{SKOS}editorialNote",
        str({"value": "Kept apart from person on purpose.", "lang": "en"}),
    ) in annotations
    assert (
        "http://purl.org/dc/terms/source",
        str({"iri": "https://www.iso.org/standard/12345.html"}),
    ) in (annotations)
    account = _kinds(_by_iri(parsed)[f"{ACC}Account"])
    assert {
        (s["object"]["value"], s["object"].get("lang"))
        for s in account[kept.ANNOTATION]
        if s["predicate"] == f"{RDFS}label"
    } == {("account", "en"), ("compte", "fr")}


def test_inverse_transitive_and_chained_properties_are_kept_on_the_property():
    items = _by_iri(_release())
    held_by = _kinds(items[f"{ACC}isHeldBy"])
    assert [s["object"] for s in held_by[kept.INVERSE]] == [{"iri": f"{ACC}holds"}]
    part_of = _kinds(items[f"{AGR}isPartOfAgreement"])
    assert [s["object"] for s in part_of[kept.CHARACTERISTIC]] == [
        {"iri": f"{OWL}TransitiveProperty"}
    ]
    controller = _kinds(items[f"{ACC}hasUltimateController"])
    assert [s["object"] for s in controller[kept.PROPERTY_CHAIN]] == [
        {"list": [{"iri": f"{ACC}isHeldBy"}, {"iri": f"{FND}isControlledBy"}]}
    ]


def test_an_object_property_keeps_its_domain_and_range_and_is_no_part_of_edge():
    parsed = _release()
    held_by = _kinds(_by_iri(parsed)[f"{ACC}isHeldBy"])
    assert [s["object"] for s in held_by[kept.DOMAIN]] == [{"iri": f"{ACC}Account"}]
    assert [s["object"] for s in held_by[kept.RANGE]] == [{"iri": f"{FND}Party"}]
    edges = _edges(parsed)
    assert not any(source == "is held by" for source, _, _ in edges)
    # A datatype property is still part of the class it describes, its range kept.
    assert ("has account number", "PART_OF", "account") in edges


def test_datatype_ranges_are_kept_plain_or_restricted():
    items = _by_iri(_release())
    number = _kinds(items[f"{ACC}hasAccountNumber"])
    assert [s["object"] for s in number[kept.DATATYPE_RANGE]] == [{"iri": f"{XSD}string"}]
    (balance,) = _kinds(items[f"{ACC}hasBalance"])[kept.DATATYPE_RANGE]
    node = dict((p, v) for p, v in balance["object"]["node"])
    assert node[f"{OWL}onDatatype"] == {"iri": f"{XSD}decimal"}
    assert node[f"{OWL}withRestrictions"] == {
        "list": [{"node": [[f"{XSD}minInclusive", {"value": "0", "datatype": f"{XSD}decimal"}]]}]
    }


def test_a_plain_enumerated_individual_is_a_valid_value_of_its_class():
    parsed = _release()
    items = _by_iri(parsed)
    open_ = items[f"{ACC}Open"]
    assert (open_.name, open_.kind, open_.definition) == (
        "open",
        parse.INDIVIDUAL,
        "The account can be used.",
    )
    assert items[f"{ACC}Closed"].kind == parse.INDIVIDUAL
    edges = _edges(parsed)
    assert ("open", "VALID_VALUE_OF", "account status") in edges
    assert ("closed", "VALID_VALUE_OF", "account status") in edges
    # The enumeration itself stays on the class, so the class still says it is closed.
    (oneof,) = _kinds(items[f"{ACC}AccountStatus"])[kept.CLASS_EXPRESSION]
    assert [f"{OWL}oneOf", {"list": [{"iri": f"{ACC}Open"}, {"iri": f"{ACC}Closed"}]}] in (
        oneof["object"]["node"]
    )


def test_a_named_individual_with_properties_of_its_own_is_kept_on_its_class():
    items = _by_iri(_release())
    assert f"{ACC}FlagshipBank" not in items
    individual = _kinds(items[f"{FND}Organization"])[kept.INDIVIDUAL]
    assert {(s["subject"], s["predicate"], str(s["object"])) for s in individual} >= {
        (f"{ACC}FlagshipBank", f"{FND}hasLegalName", str({"value": "Flagship Bank, N.A."})),
        (f"{ACC}FlagshipBank", f"{FND}isControlledBy", str({"iri": f"{ACC}FlagshipHoldings"})),
        (f"{ACC}FlagshipBank", f"{RDF}type", str({"iri": f"{FND}Organization"})),
    }


def test_a_blank_subject_axiom_is_kept_on_the_first_item_it_names():
    parsed = parse.parse(
        "x.ttl",
        b"""@prefix owl: <http://www.w3.org/2002/07/owl#> .
<https://example.org/A> a owl:Class . <https://example.org/B> a owl:Class .
[] a owl:AllDisjointClasses ; owl:members ( <https://example.org/A> <https://example.org/B> ) .
""",
    )
    (disjoint,) = _kinds(_by_iri(parsed)["https://example.org/A"])[kept.DISJOINTNESS]
    assert set(disjoint) == {"kind", "axiom"}
    assert [
        f"{OWL}members",
        {"list": [{"iri": "https://example.org/A"}, {"iri": "https://example.org/B"}]},
    ] in disjoint["axiom"]["node"]


# -- written back out ----------------------------------------------------------------------------


def _by_value(value: Any) -> Any:
    """A statement with each typed literal by the value it spells: a writer may spell a value
    its own way (Turtle writes the decimal ``0`` as ``0.0``)."""
    from decimal import Decimal

    from rdflib import Literal, URIRef

    if isinstance(value, dict):
        if "datatype" in value:
            spelled = Literal(value["value"], datatype=URIRef(value["datatype"])).toPython()
            if isinstance(spelled, Decimal):
                spelled = spelled.normalize()
            return {**value, "value": str(spelled)}
        return {k: _by_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_by_value(v) for v in value]
    return value


def _canonical(parsed: parse.Parsed) -> dict[str, Any]:
    """What a round trip must keep: every item by its IRI, with its kind, definition and
    statements, and every edge between items by their IRIs."""
    import json

    by_ordinal = {n: item.keys[0] for n, item in enumerate(parsed.items)}
    return {
        "items": {
            item.iri: (
                item.name,
                item.kind,
                item.definition,
                sorted(json.dumps(_by_value(s), sort_keys=True) for s in item.statements),
            )
            for item in parsed.items
        },
        "edges": {(by_ordinal[e.source], e.rel_type, by_ordinal[e.target]) for e in parsed.edges},
    }


def _as_export(parsed: parse.Parsed):
    terms = [
        {"id": n, "name": item.name, "iri": item.iri, "definition": item.definition}
        for n, item in enumerate(parsed.items)
    ]
    edges = [(e.source, e.target, e.rel_type) for e in parsed.edges]
    return graph_of(terms, edges, {n: item.statements for n, item in enumerate(parsed.items)})


@pytest.mark.parametrize("fmt, file_name", [("turtle", "x.ttl"), ("xml", "x.rdf"), ("nt", "x.nt")])
def test_what_is_kept_is_written_back_out_and_reads_back_the_same(fmt, file_name, no_network):
    original = _release()
    exported = _as_export(original)
    assert exported.left_out == {}
    again = parse.parse(file_name, serialize(exported, fmt))
    assert _canonical(again) == _canonical(original)


def test_the_export_holds_every_kept_triple_of_the_release():
    """Graph-level: every triple the release states about an item is in the export, blank
    structure and all (rdflib's isomorphism over each item's description)."""
    from rdflib.compare import isomorphic

    with zipfile.ZipFile(RELEASE) as archive:
        source = Graph()
        for name in archive.namelist():
            if name.endswith(".ttl"):
                source.parse(data=archive.read(name).decode(), format="turtle")
            elif name.endswith(".rdf"):
                source.parse(data=archive.read(name).decode(), format="xml")
    exported = _as_export(_release()).graph
    for subject in (f"{ACC}Account", f"{ACC}hasBalance", f"{ACC}hasUltimateController"):
        assert isomorphic(_description(source, subject), _description(exported, subject))


def _ossie_ontology_schema():
    """Ossie's ontology schema; it refers to Ossie's core schema by its URL on main, which the
    pinned copy beside it answers."""
    import json

    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource

    ossie = Path(__file__).parents[1] / "fixtures" / "ossie"
    core = "https://raw.githubusercontent.com/apache/ossie/main/core-spec/ossie-schema.json"
    return Draft202012Validator(
        json.loads((ossie / "ontology.json").read_text()),
        registry=Registry().with_resource(
            core, Resource.from_contents(json.loads((ossie / "ossie-schema.json").read_text()))
        ),
    )


def _description(graph: Graph, iri: str) -> Graph:
    from rdflib import BNode, URIRef

    out = Graph()
    todo: list[Any] = [URIRef(iri)]
    while todo:
        node = todo.pop()
        for p, o in graph.predicate_objects(node):
            out.add((node, p, o))
            if isinstance(o, BNode):
                todo.append(o)
    return out


# -- through the API -----------------------------------------------------------------------------


def _request(body: dict | None = None) -> Request:
    async def _json() -> dict:
        return body or {}

    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="default",
            role="steward",
        ),
        json=_json,
    )
    return cast("Request", fake)


@pytest.fixture
def api(control_plane, monkeypatch):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(sources).values(id="__derived__", type="postgresql"))
    db = control_plane.org()

    async def _notify(org_id: str, reason: str) -> None:
        return None

    async def _pool():
        return db

    monkeypatch.setattr(router, "_pool", lambda: db)
    monkeypatch.setattr(router, "require_active_org_id", lambda request: "default")
    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_glossary_read", lambda request: None)
    # Which columns a caller is served is REQ-2152's own tests; here every one is.
    serve_every_column(monkeypatch)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_is_org_curator", lambda request: False)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    return types.SimpleNamespace(db=db)


async def _import_and_apply(api) -> tuple[dict, dict[str, dict]]:
    review = await router.import_ontology(
        _request(
            {
                "file_name": "mini-fibo.zip",
                "content_b64": base64.b64encode(RELEASE.read_bytes()).decode(),
                "domains": ["d"],
            }
        )
    )
    ontology = [
        a["id"] for item in review["items"] for a in item["alignments"] if a["basis"] == "ontology"
    ]
    await router.bulk_approve(review["import"]["id"], _request({"alignment_ids": ontology}))
    await router.apply_import(review["import"]["id"], _request())
    async with api.db.acquire() as conn:
        terms = {t["name"]: t for t in await glossary_repo.list_terms(conn)}
    return review, terms


async def test_a_release_uploaded_as_an_archive_is_reviewed_with_what_each_item_keeps(
    api, no_network
):
    review, _ = await _import_and_apply(api)
    assert review["import"]["format"] == "archive"
    summary = review["import"]["summary"]
    assert summary["modules"] == [
        "business/Accounts.ttl",
        "foundations/Agreements.rdf",
        "foundations/Parties.ttl",
    ]
    assert summary["left_out"]["import of a module outside the archive"] == 1
    account = next(i for i in review["items"] if i["name"] == "account")
    assert {s["kind"] for s in account["statements"]} >= {kept.RESTRICTION, kept.CARDINALITY}
    assert next(i for i in review["items"] if i["name"] == "open")["kind"] == "individual"
    assert no_network == []


async def test_the_term_page_shows_what_the_ontology_stated_about_the_term(api, no_network):
    _, terms = await _import_and_apply(api)
    page = await glossary_router.get_term(_request(), terms["account"]["id"])
    kinds = {s["kind"] for s in page["statements"]}
    assert {kept.RESTRICTION, kept.CARDINALITY, kept.ANNOTATION, kept.DECLARATION} <= kinds
    organization = await glossary_router.get_term(_request(), terms["organization"]["id"])
    assert {s["kind"] for s in organization["statements"]} >= {
        kept.DISJOINTNESS,
        kept.INDIVIDUAL,
        kept.ANNOTATION,
    }
    held_by = await glossary_router.get_term(_request(), terms["is held by"]["id"])
    assert {s["kind"] for s in held_by["statements"]} >= {kept.DOMAIN, kept.RANGE, kept.INVERSE}
    assert not any(e["rel_type"] == "PART_OF" for e in held_by["edges_out"])
    status = await glossary_router.get_term(_request(), terms["open"]["id"])
    assert [(e["rel_type"], e["name"]) for e in status["edges_out"]] == [
        ("VALID_VALUE_OF", "account status")
    ]
    transitive = await glossary_router.get_term(_request(), terms["is part of agreement"]["id"])
    assert kept.CHARACTERISTIC in {s["kind"] for s in transitive["statements"]}
    chain = await glossary_router.get_term(_request(), terms["has ultimate controller"]["id"])
    assert kept.PROPERTY_CHAIN in {s["kind"] for s in chain["statements"]}
    balance = await glossary_router.get_term(_request(), terms["has balance"]["id"])
    assert kept.DATATYPE_RANGE in {s["kind"] for s in balance["statements"]}


async def test_the_imported_ontology_exported_as_rdf_reads_back_as_it_was_imported(api, no_network):
    review, _ = await _import_and_apply(api)
    response = await router.export_import(review["import"]["id"], _request(), format="turtle")
    assert response.media_type == "text/turtle"
    again = parse.parse("again.ttl", bytes(response.body))
    assert _canonical(again) == _canonical(_release())


async def test_an_export_in_a_format_no_import_reads_is_refused(api):
    review, _ = await _import_and_apply(api)
    from provisa.api.errors import ApiError

    with pytest.raises(ApiError, match="exported as one of"):
        await router.export_import(review["import"]["id"], _request(), format="owl-functional")


async def test_the_ossie_export_writes_the_kept_statements_back_and_an_import_reads_them(
    api, no_network
):
    """The Ossie ontology a data product's terms export (provisa.ossie.ontology) carries each
    term's kept statements in its ai_context, and importing that document keeps them again."""
    from provisa.api.metadata_export.model import GroundedTerm

    _, terms = await _import_and_apply(api)
    async with api.db.acquire() as conn:
        statements = await glossary_repo.term_statements(conn)
    account = terms["account"]
    from provisa.ossie.ontology import ossie_ontology

    snapshot = types.SimpleNamespace(
        terms_reached=lambda product: (
            [
                GroundedTerm(
                    term_id=account["id"],
                    name="account",
                    definition=account["definition"],
                    status="draft",
                    semantic_uri="provisa://default/terms/account",
                    role="class",
                    iri=account["iri"],
                    statements=tuple(statements[account["id"]]),
                )
            ],
            [],
        )
    )
    product = types.SimpleNamespace(id="accounts", name="Accounts")
    document = ossie_ontology(cast("Any", snapshot), cast("Any", product))
    assert not list(_ossie_ontology_schema().iter_errors(document))
    again = parse.parse("accounts.yaml", yaml.safe_dump(document).encode())
    (item,) = again.items
    assert item.iri == f"{ACC}Account"
    assert item.statements == statements[account["id"]]
