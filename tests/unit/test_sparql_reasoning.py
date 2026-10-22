# Copyright (c) 2026 Kenneth Stott
# Canary: 452185a6-cc3b-4a0e-9abf-2ed7f12645d3
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
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api_source.models import ApiColumn, ApiColumnType
from provisa.core.config_loader import _validate_neo4j_sources, parse_config_dict
from provisa.runtime_deps.code_runtimes import RuntimeUnavailable
from provisa.sparql import reasoning
from provisa.sparql.reasoner_bundle import JARS, ReasonerBundle
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
    reasoning._classified.clear()
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
    served = {j.url: b"jar" for j in JARS}

    def fetch(url: str) -> bytes:
        return served[url]

    with pytest.raises(RuntimeUnavailable, match="not the pinned"):
        ReasonerBundle(cache_root=tmp_path, fetcher=fetch).lib()
    assert not list((tmp_path / "reasoner" / "lib").glob("*.jar"))

    with patch(
        "provisa.sparql.reasoner_bundle.JARS",
        tuple(
            type(j)(j.group, j.artifact, j.version, hashlib.sha256(b"jar").hexdigest())
            for j in JARS
        ),
    ):
        lib = ReasonerBundle(cache_root=tmp_path, fetcher=fetch).lib()
        assert len(list(lib.glob("*.jar"))) == len(JARS)
        again = ReasonerBundle(cache_root=tmp_path, fetcher=lambda url: pytest.fail(url)).lib()
        assert again == lib  # a cache hit never downloads
    assert jar.url == (
        "https://repo1.maven.org/maven2/net/sourceforge/owlapi/org.semanticweb.hermit/"
        "1.4.5.519/org.semanticweb.hermit-1.4.5.519.jar"
    )
