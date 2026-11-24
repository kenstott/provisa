# Copyright (c) 2026 Kenneth Stott
# Canary: 6c5633ac-0662-440b-8135-612843273532
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sparql source's reasoner serves its inferred classes and properties as tables (REQ-2138).

The reasoner program and the endpoint are stood in for here; the program itself is run against
a live endpoint by tests/integration/test_sparql_reasoner.py.
"""

# Requirements: REQ-2138

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api_source.models import ApiColumn, ApiColumnType
from provisa.core.config_loader import _validate_neo4j_sources, parse_config_dict
from provisa.runtime_deps.code_runtimes import RuntimeUnavailable
from provisa.sparql import reasoning
from provisa.sparql.reasoner_bundle import JARS, MAIN_CLASS, REASONER, ReasonerBundle
from provisa.sparql.source import SparqlSourceConfig, build_endpoint

KYC = "http://example.org/kyc#"
EXPOSED, SANCTIONED, CONTROLS, STAKE = (
    KYC + "SanctionsExposed",
    KYC + "SanctionedParty",
    KYC + "controls",
    KYC + "stake",
)
SIGNATURE = f"class {SANCTIONED}\nclass {EXPOSED}\nobject {CONTROLS}\ndata {STAKE}\n"
INFERRED = (
    f"<{KYC}A> <{reasoning.RDF_TYPE}> <{SANCTIONED}> .\n"
    f"<{KYC}B> <{reasoning.RDF_TYPE}> <{EXPOSED}> .\n"
    f"<{KYC}C> <{reasoning.RDF_TYPE}> <{EXPOSED}> .\n"
    f"<{KYC}A> <{CONTROLS}> <{KYC}B> .\n"
    f"<{KYC}A> <{CONTROLS}> <{KYC}C> .\n"
    f'<{KYC}B> <{STAKE}> "0.6"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n'
    f'<{KYC}B> <{KYC}name> "B \\"co\\""@en .\n'
)
AXIOMS = "@prefix : <http://example.org/kyc#> . :controls a <http://www.w3.org/2002/07/owl#TransitiveProperty> ."


class _Program:
    """The reasoner program, as its two commands answer."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, command: str, *args: str) -> str:
        self.calls.append(command)
        if command == "signature":
            return SIGNATURE
        Path(args[2]).write_text(INFERRED)
        return ""


def _endpoint(query: str, columns: list[str]):
    cfg = SparqlSourceConfig(source_id="kyc", endpoint_url="http://graph.test/sparql")
    return build_endpoint(
        cfg,
        "t",
        query,
        [ApiColumn(name=c, type=ApiColumnType.string) for c in columns],
        ttl=300,
        reasoner=("hermit", AXIOMS),
    )


@pytest.fixture(autouse=True)
def _no_classifications():
    """No classification kept between tests, and a source whose tables produce nothing its
    axioms read (``own_products`` reads the control plane; the self-read tests give their own)."""
    reasoning._classified.clear()
    with patch("provisa.sparql.reasoning.own_products", new=AsyncMock(return_value={})):
        yield
    reasoning._classified.clear()


def test_the_slice_reads_every_property_and_class_the_axioms_name():
    query = reasoning.slice_query(reasoning.parse_signature(SIGNATURE))
    assert query.startswith("CONSTRUCT { ?s ?p ?o } WHERE {")
    assert f"VALUES ?p {{ <{CONTROLS}> <{STAKE}> }} ?s ?p ?o" in query
    assert f"VALUES ?o {{ <{SANCTIONED}> <{EXPOSED}> }} ?s a ?o" in query
    with pytest.raises(reasoning.ReasonerFailed, match="no class or property"):
        reasoning.slice_query(reasoning.parse_signature(""))


def test_the_reasoners_triples_are_read_as_facts_by_class_and_property():
    facts = reasoning.parse_inferred(INFERRED)
    assert facts[EXPOSED] == [(KYC + "B",), (KYC + "C",)]
    assert facts[CONTROLS] == [(KYC + "A", KYC + "B"), (KYC + "A", KYC + "C")]
    assert facts[STAKE] == [(KYC + "B", "0.6")]
    assert facts[KYC + "name"] == [(KYC + "B", 'B "co"')]
    with pytest.raises(reasoning.ReasonerFailed, match="unreadable line"):
        reasoning.parse_inferred("not a triple")


@pytest.mark.asyncio
async def test_a_class_table_is_its_members_and_a_property_table_its_pairs():
    program, fetch = _Program(), AsyncMock(return_value=b"<a> <b> <c> .\n")
    found = await reasoning.classification(
        "kyc", AXIOMS, "http://graph.test/sparql", {}, 300, run=program, fetch=fetch
    )
    exposed = _endpoint(f"<{EXPOSED}>", ["party"])
    controls = _endpoint(f"  <{CONTROLS}> ", ["controller", "controlled"])
    assert reasoning.rows_of(exposed, found) == [{"party": KYC + "B"}, {"party": KYC + "C"}]
    assert reasoning.rows_of(controls, found) == [
        {"controller": KYC + "A", "controlled": KYC + "B"},
        {"controller": KYC + "A", "controlled": KYC + "C"},
    ]
    assert fetch.await_args is not None
    sent = fetch.await_args.args
    assert sent[0] == "http://graph.test/sparql" and sent[1].startswith("CONSTRUCT")


@pytest.mark.asyncio
async def test_a_table_is_refused_when_its_iri_or_its_width_is_not_the_axioms():
    found = await reasoning.classification(
        "kyc", AXIOMS, "u", {}, 300, run=_Program(), fetch=AsyncMock(return_value=b"")
    )
    with pytest.raises(reasoning.ReasonerFailed, match="do not"):
        reasoning.rows_of(_endpoint(f"<{KYC}Unknown>", ["x"]), found)
    with pytest.raises(reasoning.ReasonerFailed, match="has 2 columns; it declares 1"):
        reasoning.rows_of(_endpoint(f"<{CONTROLS}>", ["controller"]), found)


@pytest.mark.asyncio
async def test_one_classification_answers_every_table_until_its_ttl_passes():
    program, fetch = _Program(), AsyncMock(return_value=b"")
    for _ in range(3):
        await reasoning.classification("kyc", AXIOMS, "u", {}, 300, run=program, fetch=fetch)
    assert program.calls == ["signature", "classify"]
    await reasoning.classification("kyc", AXIOMS, "u", {}, 0, run=program, fetch=fetch)
    assert program.calls == ["signature", "classify"] * 2  # a TTL of 0 keeps nothing
    await reasoning.classification("kyc", AXIOMS + " ", "u", {}, -1, run=program, fetch=fetch)
    assert len(program.calls) == 6  # other axioms are another classification
    await reasoning.classification("kyc", AXIOMS + " ", "u", {}, -1, run=program, fetch=fetch)
    assert len(program.calls) == 6  # -1 classifies once


@pytest.mark.asyncio
async def test_a_source_keeps_one_classification_replaced_when_its_axioms_or_endpoint_change():
    """One per source: a classification made from other axioms or another endpoint replaces the
    source's last, which is never read again -- kept, each edit of the axioms grew the cache."""
    program, fetch = _Program(), AsyncMock(return_value=b"")
    await reasoning.classification("kyc", AXIOMS, "u", {}, -1, run=program, fetch=fetch)
    await reasoning.classification("kyc", AXIOMS + " ", "u", {}, -1, run=program, fetch=fetch)
    await reasoning.classification("kyc", AXIOMS + " ", "u2", {}, -1, run=program, fetch=fetch)
    assert len(program.calls) == 6
    assert len(reasoning._classified) == 1 and len(reasoning._locks) == 1


@pytest.mark.asyncio
async def test_an_inferred_tables_call_is_answered_by_the_reasoner():
    from provisa.api_source.caller import answer_rows, call_api

    endpoint = _endpoint(f"<{EXPOSED}>", ["party"])
    rows = AsyncMock(return_value=[{"party": KYC + "B"}])
    with patch("provisa.sparql.reasoning.inferred_rows", new=rows):
        answer = await call_api(endpoint, dict(endpoint.default_params), "http://graph.test")
    assert answer_rows(endpoint, answer) == ([{"party": KYC + "B"}], None)
    assert rows.await_args is not None
    _, params, url, _ = rows.await_args.args
    assert params[reasoning.AXIOMS_PARAM] == AXIOMS and url == "http://graph.test/sparql"


def test_an_inferred_table_carries_its_reasoner_and_axioms():
    endpoint = _endpoint(f"<{EXPOSED}>", ["party"])
    assert endpoint.response_normalizer == reasoning.NORMALIZER
    assert endpoint.default_params == {
        reasoning.REASONER_PARAM: "hermit",
        reasoning.AXIOMS_PARAM: AXIOMS,
    }
    cfg = SparqlSourceConfig(source_id="kyc", endpoint_url="http://graph.test/sparql")
    with pytest.raises(ValueError, match="names no reasoner"):
        build_endpoint(cfg, "t", f"<{EXPOSED}>", [], 300)
    plain = build_endpoint(cfg, "t", "SELECT ?x WHERE { ?x ?p ?o }", [], 300, ("hermit", AXIOMS))
    assert plain.response_normalizer == "sparql_bindings" and plain.default_params == {}


def _config(mapping: dict, query: str) -> dict:
    return {
        "sources": [
            {"id": "kyc", "type": "sparql", "host": "http://graph.test/sparql", "mapping": mapping}
        ],
        "domains": [{"id": "d"}],
        "roles": [],
        "naming": {"domain_prefix": False, "rules": []},
        "tables": [
            {
                "source_id": "kyc",
                "domain_id": "d",
                "schema": "kyc",
                "table": "exposed",
                "query_template": query,
                "columns": [{"name": "party", "data_type": "text", "visible_to": ["org_admin"]}],
            }
        ],
    }


def test_config_checks_the_reasoner_and_the_tables_that_read_it():
    _validate_neo4j_sources(
        parse_config_dict(_config({"reasoner": "hermit", "axioms": AXIOMS}, f"<{EXPOSED}>"))
    )
    with pytest.raises(ValueError, match="names no reasoner"):
        _validate_neo4j_sources(parse_config_dict(_config({}, f"<{EXPOSED}>")))
    with pytest.raises(ValueError, match="is not one of"):
        _validate_neo4j_sources(
            parse_config_dict(_config({"reasoner": "pellet", "axioms": AXIOMS}, f"<{EXPOSED}>"))
        )
    with pytest.raises(ValueError, match="needs its OWL axioms"):
        _validate_neo4j_sources(parse_config_dict(_config({"reasoner": "hermit"}, f"<{EXPOSED}>")))
    with pytest.raises(ValueError, match="no reasoner"):
        _validate_neo4j_sources(parse_config_dict(_config({"axioms": AXIOMS}, "SELECT ?x {}")))


def test_the_bundle_keeps_only_jars_whose_sums_are_the_pinned_ones(tmp_path):
    jar = JARS[0]
    served = {j.url: b"jar" for j in (REASONER, *JARS)}

    def fetch(url: str) -> bytes:
        return served[url]

    with pytest.raises(RuntimeUnavailable, match="not the pinned"):
        ReasonerBundle(cache_root=tmp_path, fetcher=fetch).lib()
    assert not list((tmp_path / "reasoner" / "lib").glob("*.jar"))

    def pinned(j):
        return type(j)(j.group, j.artifact, j.version, hashlib.sha256(b"jar").hexdigest())

    with (
        patch("provisa.sparql.reasoner_bundle.JARS", tuple(pinned(j) for j in JARS)),
        patch("provisa.sparql.reasoner_bundle.REASONER", pinned(REASONER)),
    ):
        lib = ReasonerBundle(cache_root=tmp_path, fetcher=fetch).lib()
        assert len(list(lib.glob("*.jar"))) == len(JARS) + 1
        again = ReasonerBundle(cache_root=tmp_path, fetcher=lambda url: pytest.fail(url)).lib()
        assert again == lib  # a cache hit never downloads
    assert jar.url == (
        "https://repo1.maven.org/maven2/net/sourceforge/owlapi/org.semanticweb.hermit/"
        "1.4.5.519/org.semanticweb.hermit-1.4.5.519.jar"
    )


def test_the_reasoner_jar_is_fetched_from_maven_central_and_checked(tmp_path):
    """REQ-2138 (amended 2026-11-23): the reasoner is the precompiled jar
    io.simpleishard:provisa-sparql-reasoner on Maven Central, kept only when its sum is pinned."""
    assert REASONER.url == (
        "https://repo1.maven.org/maven2/io/simpleishard/provisa-sparql-reasoner/"
        f"{REASONER.version}/provisa-sparql-reasoner-{REASONER.version}.jar"
    )
    good = {j.url: b"jar" for j in JARS} | {REASONER.url: b"tampered"}
    pinned = tuple(
        type(j)(j.group, j.artifact, j.version, hashlib.sha256(b"jar").hexdigest()) for j in JARS
    )
    with patch("provisa.sparql.reasoner_bundle.JARS", pinned):
        with pytest.raises(RuntimeUnavailable, match="provisa-sparql-reasoner.*not the pinned"):
            ReasonerBundle(cache_root=tmp_path, fetcher=good.__getitem__).lib()
        assert not ReasonerBundle(cache_root=tmp_path).kept(REASONER).exists()
        served = good | {REASONER.url: b"reasoner"}
        reasoner = type(REASONER)(
            REASONER.group,
            REASONER.artifact,
            REASONER.version,
            hashlib.sha256(b"reasoner").hexdigest(),
        )
        with patch("provisa.sparql.reasoner_bundle.REASONER", reasoner):
            bundle = ReasonerBundle(cache_root=tmp_path, fetcher=served.__getitem__)
            bundle.lib()
            assert bundle.kept(reasoner).read_bytes() == b"reasoner"


def test_no_url_the_reasoner_fetches_is_on_github():
    for jar in (REASONER, *JARS):
        assert jar.url.startswith("https://repo1.maven.org/maven2/"), jar.url


def test_the_reasoner_runs_on_jdk4pys_java_with_only_the_pinned_jars(tmp_path):
    import jdk4py

    bundle = ReasonerBundle(cache_root=tmp_path, fetcher=lambda url: pytest.fail(url))
    for jar in (REASONER, *JARS):
        bundle.kept(jar).parent.mkdir(parents=True, exist_ok=True)
        bundle.kept(jar).write_bytes(b"")
    command = bundle.command("signature", "axioms.ttl")
    assert command[0] == str(jdk4py.JAVA)
    assert command[1] == "-cp"
    assert command[2].split(os.pathsep) == [str(bundle.kept(j)) for j in (REASONER, *JARS)]
    assert command[3:] == [MAIN_CLASS, "signature", "axioms.ttl"]


def test_without_jdk4py_the_reasoner_is_unavailable_naming_the_extra(tmp_path):
    with patch.dict(sys.modules, {"jdk4py": None}):
        with pytest.raises(RuntimeUnavailable, match=r"provisa\[sparql\]"):
            ReasonerBundle(cache_root=tmp_path, fetcher=lambda url: pytest.fail(url)).command()


@pytest.mark.asyncio
async def test_the_program_is_run_as_jdk4pys_java_in_its_own_process(tmp_path):
    import jdk4py

    process = AsyncMock()
    process.returncode = 0
    process.communicate = AsyncMock(return_value=(b"class x\n", b""))
    bundle = ReasonerBundle(cache_root=tmp_path, fetcher=lambda url: pytest.fail(url))
    for jar in (REASONER, *JARS):
        bundle.kept(jar).parent.mkdir(parents=True, exist_ok=True)
        bundle.kept(jar).write_bytes(b"")
    spawn = AsyncMock(return_value=process)
    with (
        patch("provisa.sparql.reasoner_bundle.default_cache_root", return_value=tmp_path),
        patch("asyncio.create_subprocess_exec", new=spawn),
    ):
        assert await reasoning.run_program("signature", "a.ttl") == "class x\n"
    args = spawn.await_args.args
    assert args[0] == str(jdk4py.JAVA) and args[-3:] == (MAIN_CLASS, "signature", "a.ttl")


# -- REQ-2138: a source that would read what its own inferred tables produce is refused ---------

BASE = "https://provisa.test"
OWN_CONTROLLER = f"{BASE}/vocab/kyc/controls#controller"
OWN_CLASS = f"{BASE}/vocab/kyc/controls"
TERM_CONTROLS = f"{BASE}/vocab/term/controls"


def test_what_a_sources_tables_produce_is_their_class_and_predicates_on_provisas_endpoint():
    """Its tables' classes, their columns' predicates (a column's alias names it), and the term
    of a grounded table or column in place of the column's own -- as Provisa's endpoint serves
    them (provisa.sparql_query.vocabulary)."""
    tables = [
        reasoning.OwnTable(
            7, "kyc", "controls", (("controller", "controller"), ("b", "controlled"))
        ),
    ]
    produced = reasoning.produced_iris(BASE, tables, {7: "Control"}, {(7, "b"): ["controls"]})
    assert produced == {
        OWN_CLASS: "controls",
        f"{BASE}/vocab/term/Control": "controls",
        OWN_CONTROLLER: "controls",
        TERM_CONTROLS: "controls",
    }


def test_a_slice_that_reads_a_predicate_of_the_sources_own_table_is_refused_by_name():
    signature = reasoning.parse_signature(f"class {EXPOSED}\nobject {OWN_CONTROLLER}\n")
    with pytest.raises(reasoning.SelfReadRefused) as refused:
        reasoning.refuse_self_read("kyc", signature, {OWN_CONTROLLER: "controls"})
    said = str(refused.value)
    assert "'kyc'" in said and f"<{OWN_CONTROLLER}>" in said and "'controls'" in said
    assert "reads itself" in said
    reasoning.refuse_self_read("kyc", signature, {OWN_CLASS: "controls"})  # not read: no refusal


@pytest.mark.asyncio
async def test_a_source_reading_its_own_tables_output_is_refused_before_its_slice_is_read():
    signature = f"class {EXPOSED}\nobject {TERM_CONTROLS}\n"

    async def program(command: str, *args: str) -> str:
        return signature

    fetch = AsyncMock(return_value=b"")
    own = AsyncMock(return_value={TERM_CONTROLS: "controls"})
    with patch("provisa.sparql.reasoning.own_products", new=own):
        with pytest.raises(reasoning.SelfReadRefused, match="controls"):
            await reasoning.classification("kyc", AXIOMS, "u", {}, 0, run=program, fetch=fetch)
    own.assert_awaited_once_with("kyc")
    fetch.assert_not_awaited()
