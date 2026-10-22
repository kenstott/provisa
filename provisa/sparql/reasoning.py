# Copyright (c) 2026 Kenneth Stott
# Canary: d90273dd-f119-4b8c-9dec-7dbbe8dae293
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sparql source's reasoner (REQ-2138): its inferred class memberships and properties, served
as tables.

A sparql source may name a reasoner (``hermit``) and an OWL axioms document in its options. A
table under it whose query is one IRI in angle brackets, ``<http://example.org/kyc#controls>``,
is that class or property as the axioms infer it: a class is one column, its members; an object
or data property is two, each subject and its value.

To answer one, the reasoner:
1. reads the classes and properties the axioms name (the reasoner program's ``signature``);
2. pulls the slice of the endpoint's graph about them by CONSTRUCT, as N-Triples;
3. classifies the slice under the axioms (``classify``), in its own process;
4. serves the inferred facts of the table's class or property as rows.

Every inferred table of a source is answered by one classification. It is reused while the
table's cache TTL allows, so the source's freshness declaration (cache_ttl, replica) is how often
it reasons. A replica's TTL of -1 classifies once.
"""

# Requirements: REQ-2138

from __future__ import annotations

import asyncio
import hashlib
import re
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from provisa.api_source.models import ApiEndpoint

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

#: The source option naming its reasoner, and the one reasoner there is.
REASONER_OPTION = "reasoner"
AXIOMS_OPTION = "axioms"
REASONERS = ("hermit",)

#: The response normalizer an inferred table's endpoint carries, and the default parameters its
#: reasoner and axioms travel in (part of what its answer is kept under: changed axioms are a
#: different answer).
NORMALIZER = "owl_inferred"
REASONER_PARAM = "owl_reasoner"
AXIOMS_PARAM = "owl_axioms"

_INFERRED_IRI = re.compile(r"\A\s*<([^<>\s]+)>\s*\Z")
_TIMEOUT = 600.0


class ReasonerFailed(Exception):
    """The reasoner could not answer: its program failed, or the endpoint refused the slice."""


def source_reasoner(source_id: str, options: dict) -> tuple[str, str] | None:
    """The reasoner and axioms a sparql source's options name (``reasoner``, ``axioms``), or None
    when it names no reasoner. A reasoner that is not one there is, or one without axioms, is
    refused by name."""
    reasoner = options.get(REASONER_OPTION)
    if reasoner is None:
        if options.get(AXIOMS_OPTION):
            raise ValueError(f"sparql source {source_id!r}: axioms are given but no reasoner")
        return None
    if reasoner not in REASONERS:
        raise ValueError(
            f"sparql source {source_id!r}: reasoner {reasoner!r} is not one of {list(REASONERS)}"
        )
    axioms = options.get(AXIOMS_OPTION)
    if not isinstance(axioms, str) or not axioms.strip():
        raise ValueError(
            f"sparql source {source_id!r}: reasoner {reasoner!r} needs its OWL axioms (axioms)"
        )
    return reasoner, axioms


def inferred_iri(query_template: str | None) -> str | None:
    """The class or property IRI a table's query names, when its query is one IRI."""
    found = _INFERRED_IRI.match(query_template or "")
    return found.group(1) if found else None


@dataclass(frozen=True)
class Signature:
    classes: tuple[str, ...]
    object_properties: tuple[str, ...]
    data_properties: tuple[str, ...]

    def kind(self, iri: str) -> str | None:
        if iri in self.classes:
            return "class"
        if iri in self.object_properties or iri in self.data_properties:
            return "property"
        return None


def parse_signature(text: str) -> Signature:
    found: dict[str, list[str]] = {"class": [], "object": [], "data": []}
    for line in text.splitlines():
        if not line.strip():
            continue
        kind, _, iri = line.partition(" ")
        if kind not in found or not iri:
            raise ReasonerFailed(f"the reasoner's signature has an unreadable line: {line!r}")
        found[kind].append(iri.strip())
    return Signature(tuple(found["class"]), tuple(found["object"]), tuple(found["data"]))


def slice_query(signature: Signature) -> str:
    """The CONSTRUCT that pulls the facts the axioms reason about: every triple of one of their
    properties, and every membership of one of their classes."""
    properties = signature.object_properties + signature.data_properties
    branches = []
    if properties:
        values = " ".join(f"<{p}>" for p in properties)
        branches.append(f"{{ VALUES ?p {{ {values} }} ?s ?p ?o }}")
    if signature.classes:
        values = " ".join(f"<{c}>" for c in signature.classes)
        branches.append(f"{{ VALUES ?o {{ {values} }} ?s a ?o BIND(<{RDF_TYPE}> AS ?p) }}")
    if not branches:
        raise ReasonerFailed("the axioms name no class or property to reason about")
    return "CONSTRUCT { ?s ?p ?o } WHERE {\n  " + "\n  UNION\n  ".join(branches) + "\n}"


# -- the reasoner's answer ---------------------------------------------------------------------

_TRIPLE = re.compile(r"\A<([^>]*)> <([^>]*)> (.+) \.\Z")
_ESCAPES = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f", '"': '"', "'": "'", "\\": "\\"}


def _object(text: str) -> str:
    """An object term's value: an IRI's text, a literal's lexical form."""
    if text.startswith("<") and text.endswith(">"):
        return text[1:-1]
    if not text.startswith('"'):
        raise ReasonerFailed(f"the reasoner wrote an unreadable term: {text!r}")
    out: list[str] = []
    i = 1
    while i < len(text):
        ch = text[i]
        if ch == '"':
            return "".join(out)
        if ch == "\\":
            i += 1
            out.append(_ESCAPES[text[i]])
        else:
            out.append(ch)
        i += 1
    raise ReasonerFailed(f"the reasoner wrote an unclosed literal: {text!r}")


def parse_inferred(text: str) -> dict[str, list[tuple[str, ...]]]:
    """The reasoner's N-Triples as facts by class or property IRI: ``(member,)`` for a class,
    ``(subject, value)`` for a property."""
    facts: dict[str, list[tuple[str, ...]]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        found = _TRIPLE.match(line)
        if found is None:
            raise ReasonerFailed(f"the reasoner wrote an unreadable line: {line!r}")
        subject, predicate, obj = found.group(1), found.group(2), _object(found.group(3))
        if predicate == RDF_TYPE:
            facts.setdefault(obj, []).append((subject,))
        else:
            facts.setdefault(predicate, []).append((subject, obj))
    return facts


@dataclass
class Classification:
    signature: Signature
    facts: dict[str, list[tuple[str, ...]]]
    at: float = field(default_factory=time.monotonic)


# -- running it --------------------------------------------------------------------------------


async def run_program(*args: str) -> str:
    """Run the reasoner program (``reasoner_bundle``) in its own process."""
    from provisa.sparql.reasoner_bundle import ReasonerBundle

    command = await asyncio.to_thread(ReasonerBundle().command, *args)
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await asyncio.wait_for(process.communicate(), timeout=_TIMEOUT)
    if process.returncode != 0:
        raise ReasonerFailed(
            f"the reasoner exited {process.returncode}: {err.decode(errors='replace')[-2000:]}"
        )
    return out.decode()


async def fetch_slice(url: str, query: str, headers: dict[str, str]) -> bytes:
    """The endpoint's answer to the CONSTRUCT, as N-Triples."""
    from provisa.security import sandbox_egress

    async with httpx.AsyncClient(
        timeout=_TIMEOUT, event_hooks=sandbox_egress.http_event_hooks()
    ) as client:
        resp = await client.post(
            url,
            data={"query": query},
            headers={**headers, "Accept": "application/n-triples"},
        )
    if resp.status_code != httpx.codes.OK:
        raise ReasonerFailed(
            f"the endpoint answered the slice's CONSTRUCT with HTTP {resp.status_code}: "
            f"{resp.text[:500]}"
        )
    return resp.content


_classified: dict[tuple[str, str], Classification] = {}
_locks: dict[tuple[str, str], asyncio.Lock] = {}
_locks_guard = threading.Lock()


def _lock(key: tuple[str, str]) -> asyncio.Lock:
    with _locks_guard:
        return _locks.setdefault(key, asyncio.Lock())


def _fresh(found: Classification | None, ttl: int) -> bool:
    if found is None or ttl == 0:
        return False
    return ttl < 0 or time.monotonic() - found.at < ttl


async def classification(
    source_id: str,
    axioms: str,
    url: str,
    headers: dict[str, str],
    ttl: int,
    *,
    run: Callable[..., Any] = run_program,
    fetch: Callable[..., Any] = fetch_slice,
) -> Classification:
    """The source's classification, reasoned again only once ``ttl`` (seconds; -1 never) has
    passed since the last. ``run`` and ``fetch`` are injected so tests need no Java and no
    endpoint."""
    key = (source_id, hashlib.sha256(axioms.encode()).hexdigest())
    async with _lock(key):
        found = _classified.get(key)
        if _fresh(found, ttl):
            assert found is not None
            return found
        with tempfile.TemporaryDirectory(prefix="provisa-reasoner-") as work:
            axioms_file = Path(work) / "axioms.owl"
            axioms_file.write_text(axioms)
            signature = parse_signature(await run("signature", str(axioms_file)))
            slice_file = Path(work) / "slice.nt"
            slice_file.write_bytes(await fetch(url, slice_query(signature), headers))
            out_file = Path(work) / "inferred.nt"
            await run("classify", str(axioms_file), str(slice_file), str(out_file))
            facts = parse_inferred(out_file.read_text())
        found = Classification(signature, facts)
        _classified[key] = found
        return found


def rows_of(endpoint: ApiEndpoint, found: Classification) -> list[dict]:
    """The rows of an inferred table: its class's members, or its property's pairs."""
    iri = inferred_iri(endpoint.query_template)
    assert iri is not None
    kind = found.signature.kind(iri)
    if kind is None:
        raise ReasonerFailed(
            f"table {endpoint.table_name!r} names <{iri}>, which its source's axioms do not"
        )
    names = [c.name for c in endpoint.columns if c.param_type is None]
    width = 1 if kind == "class" else 2
    if len(names) != width:
        raise ReasonerFailed(
            f"table {endpoint.table_name!r} reads the {kind} <{iri}>, which has {width} "
            f"column{'s' if width > 1 else ''}; it declares {len(names)}"
        )
    return [dict(zip(names, fact)) for fact in found.facts.get(iri, [])]


async def inferred_rows(
    endpoint: ApiEndpoint, params: dict, url: str, headers: dict[str, str]
) -> list[dict]:
    """An inferred table's rows, from its source's classification."""
    found = await classification(
        endpoint.source_id, params[AXIOMS_PARAM], url, headers, endpoint.ttl
    )
    return rows_of(endpoint, found)
