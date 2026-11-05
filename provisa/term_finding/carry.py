# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2f17-9c3a-4d61-a0f5-7e2d19c8b634
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's scans for terms, carried in its bundle (REQ-2201).

A scan reads a document with the org's AI model, a call after a call for minutes; a demo's tour
cannot wait on one, and a sandbox may have no key. So the bundle build runs the demo's setup.yaml
``scan`` steps on a throwaway control plane, and :func:`export_scans` writes what they left out as
``seeded/term_scans.json`` beside the demo's seed: each library document scanned (its passages
and their embeddings, its domains) and each scan of it, with the drafts it queued. A template of
the bundle then loads them with :func:`load_scans`, and every sandbox is cloned with them
(env_copy.copy_term_scans), as the profile runs are carried (provisa.profiler.carry, REQ-2052).

A document is keyed by its source (the URL it was read from), and a glossary term a draft matched
by its name: the template's ids are its own.
"""

# Requirements: REQ-2201, REQ-2179, REQ-2185

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from provisa.core.schema_org import glossary_term_drafts as drafts_t
from provisa.core.schema_org import glossary_term_scans as scans_t
from provisa.core.schema_org import glossary_terms as terms_t
from provisa.core.schema_org import reference_document_domains as domains_t
from provisa.core.schema_org import reference_documents as documents_t
from provisa.core.schema_org import reference_passages as passages_t

#: Where a bundle carries its demo's scans, under the demo's directory.
SCANS_FILE = Path("seeded") / "term_scans.json"

_DOCUMENT_FIELDS = (
    "title",
    "source_kind",
    "source",
    "media_type",
    "content_sha256",
    "status",
    "embedding_model",
    "added_by",
    "added_at",
    "refreshed_at",
    "replaced_at",
)
_PASSAGE_FIELDS = ("ordinal", "section", "page", "text", "vector")
_SCAN_FIELDS = (
    "document_title",
    "document_source",
    "domains",
    "status",
    "error",
    "summary",
    "started_by",
    "started_at",
    "finished_at",
    "model",
    "step",
    "batch",
    "batches",
    "budget_limit",
)
_DRAFT_FIELDS = (
    "ordinal",
    "name",
    "category",
    "sense",
    "definition",
    "definition_kind",
    "proposal",
    "match_kind",
    "payload",
    "status",
    "decided_by",
    "decided_at",
)
_TIMESTAMPS = {"added_at", "refreshed_at", "replaced_at", "started_at", "finished_at", "decided_at"}


def scans_path(demo_directory: Path) -> Path:
    return demo_directory / SCANS_FILE


def _encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"a scan holds a {type(value).__name__}, which has no JSON form")


def _decoded(row: dict[str, Any]) -> dict[str, Any]:
    """A row as JSON carried it, its timestamps parsed back."""
    return {
        name: datetime.fromisoformat(value) if name in _TIMESTAMPS and value is not None else value
        for name, value in row.items()
    }


def export_scans(url: str, schema: str, out: Path) -> int:
    """Write every library document of control-plane ``schema`` (an org's) that a scan read, with
    its passages and domains, and each of those scans with its drafts, to ``out``; the scans
    written. A glossary term a draft names is named by its name."""
    engine = sa.create_engine(url)
    try:
        with engine.connect() as raw:
            conn = raw.execution_options(schema_translate_map={None: schema})
            names = {r.id: r.name for r in conn.execute(sa.select(terms_t.c.id, terms_t.c.name))}
            scans = [
                dict(r._mapping) for r in conn.execute(sa.select(scans_t).order_by(scans_t.c.id))
            ]
            read = sorted({s["document_id"] for s in scans if s["document_id"] is not None})
            documents = []
            for document in conn.execute(
                sa.select(documents_t).where(documents_t.c.id.in_(read)).order_by(documents_t.c.id)
            ):
                d = dict(document._mapping)
                domains = [
                    r[0]
                    for r in conn.execute(
                        sa.select(domains_t.c.domain_id)
                        .where(domains_t.c.document_id == d["id"])
                        .order_by(domains_t.c.domain_id)
                    )
                ]
                passages = [
                    {f: p._mapping[f] for f in _PASSAGE_FIELDS}
                    for p in conn.execute(
                        sa.select(passages_t)
                        .where(passages_t.c.document_id == d["id"])
                        .order_by(passages_t.c.ordinal)
                    )
                ]
                documents.append(
                    {
                        **{f: d[f] for f in _DOCUMENT_FIELDS},
                        "domains": domains,
                        "passages": passages,
                    }
                )
            carried = []
            for s in scans:
                drafts = [
                    {
                        **{f: r._mapping[f] for f in _DRAFT_FIELDS},
                        "match_term": names.get(r.match_term_id),
                        "term": names.get(r.term_id),
                    }
                    for r in conn.execute(
                        sa.select(drafts_t)
                        .where(drafts_t.c.scan_id == s["id"])
                        .order_by(drafts_t.c.ordinal)
                    )
                ]
                carried.append({**{f: s[f] for f in _SCAN_FIELDS}, "drafts": drafts})
    finally:
        engine.dispose()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps({"documents": documents, "scans": carried}, default=_encode, indent=1)
    )
    return len(carried)


def _term_id(names: dict[str, int], name: str | None, where: str) -> int | None:
    if name is None:
        return None
    if name not in names:
        raise ValueError(
            f"the bundle's scan of {where} names the glossary term {name!r}, "
            "which this org does not hold"
        )
    return names[name]


def _matches(payload: str, names: dict[str, int], where: str) -> str:
    """A draft's payload with each matched term's id the org's own, found by its name."""
    held = json.loads(payload)
    held["matches"] = [
        {**m, "term_id": _term_id(names, m["name"], where)} for m in held.get("matches") or []
    ]
    return json.dumps(held)


async def load_scans(conn: Any, path: Path) -> list[str]:
    """Load the scans a bundle carries at ``path`` into the org ``conn`` is scoped to: each library
    document replaced whole by its source (and every scan of that source with it), then each scan
    with its drafts, a matched glossary term found by its name; the sources loaded. A term the
    org does not hold is refused by name: the bundle and the demo's config disagree."""
    from provisa.reference_library import store

    carried = json.loads(path.read_text())
    names = {
        r.name: r.id
        for r in (await conn.execute_core(sa.select(terms_t.c.id, terms_t.c.name))).fetchall()
    }
    ids: dict[str, int] = {}
    for document in carried["documents"]:
        source = document["source"]
        await conn.execute_core(sa.delete(scans_t).where(scans_t.c.document_source == source))
        await conn.execute_core(sa.delete(documents_t).where(documents_t.c.source == source))
        held = _decoded({f: document[f] for f in _DOCUMENT_FIELDS})
        ids[source] = int(
            (
                await conn.execute_core(
                    sa.insert(documents_t).values(**held).returning(documents_t.c.id)
                )
            ).fetchone()[0]
        )
        await store.set_domains(conn, ids[source], document["domains"])
        if document["passages"]:
            await conn.execute_core_many(
                sa.insert(passages_t),
                [{**p, "document_id": ids[source]} for p in document["passages"]],
            )
    # REQ-2185: the loaded passages searchable under their dimension's vector index.
    await store.index_vectors(conn)
    for scan in carried["scans"]:
        source = scan["document_source"]
        values = _decoded({f: scan[f] for f in _SCAN_FIELDS})
        scan_id = int(
            (
                await conn.execute_core(
                    sa.insert(scans_t)
                    .values(**values, document_id=ids.get(source))
                    .returning(scans_t.c.id)
                )
            ).fetchone()[0]
        )
        if scan["drafts"]:
            await conn.execute_core_many(
                sa.insert(drafts_t),
                [
                    {
                        **_decoded({f: d[f] for f in _DRAFT_FIELDS}),
                        "payload": _matches(d["payload"], names, source),
                        "scan_id": scan_id,
                        "match_term_id": _term_id(names, d["match_term"], source),
                        "term_id": _term_id(names, d["term"], source),
                    }
                    for d in scan["drafts"]
                ],
            )
    return sorted(ids)


async def unheld_scans(conn: Any, path: Path, schema: str) -> list[str]:
    """The documents and scans a bundle carries at ``path`` that control-plane ``schema`` (an
    org's) does not hold, passages and drafts all, each saying why; empty when it holds every
    one. A template is ready only when it holds what its bundle carries: one that never loaded
    them gave every sandbox of it an empty review queue."""
    carried = json.loads(path.read_text())
    documents = sa.table("reference_documents", sa.column("id"), sa.column("source"), schema=schema)
    passages = sa.table("reference_passages", sa.column("document_id"), schema=schema)
    scans = sa.table(
        "glossary_term_scans",
        sa.column("id"),
        sa.column("document_source"),
        sa.column("status"),
        schema=schema,
    )
    drafts = sa.table("glossary_term_drafts", sa.column("scan_id"), schema=schema)
    unheld = []
    for document in carried["documents"]:
        source = document["source"]
        held = (
            await conn.execute_core(
                sa.select(sa.func.count())
                .select_from(passages.join(documents, passages.c.document_id == documents.c.id))
                .where(documents.c.source == source)
            )
        ).scalar_one()
        if held != len(document["passages"]):
            unheld.append(
                f"{source} holds {held} passages of the {len(document['passages'])} carried"
            )
    held_scans: dict[tuple[str, str], list[int]] = {}
    rows = (
        await conn.execute_core(
            sa.select(scans.c.document_source, scans.c.status, sa.func.count(drafts.c.scan_id))
            .select_from(scans.outerjoin(drafts, drafts.c.scan_id == scans.c.id))
            .group_by(scans.c.id, scans.c.document_source, scans.c.status)
        )
    ).fetchall()
    for source, status, n in rows:
        held_scans.setdefault((source, status), []).append(int(n))
    for scan in carried["scans"]:
        key = (scan["document_source"], scan["status"])
        counts = held_scans.get(key, [])
        if len(scan["drafts"]) in counts:
            counts.remove(len(scan["drafts"]))
        else:
            unheld.append(
                f"the {scan['status']} scan of {scan['document_source']} with "
                f"{len(scan['drafts'])} drafts is not there"
            )
    return unheld


async def load_bundle_scans(conn: Any, directory: Path | None) -> list[str]:
    """Load the scans the bundle of the demo this server was started as carries (``directory``,
    :func:`provisa.core.demo.started_demo_directory`) into the org ``conn`` is scoped to: a bundle
    started on its own server has the scans its build made, as its hosted template has. Refused
    by name when the server was started as no named demo or the bundle carries none."""
    if directory is None:
        raise ValueError("this server was started as no named demo: there are no carried scans")
    path = scans_path(directory)
    if not path.is_file():
        raise ValueError(f"the bundle carries no scans ({path.name}): rebuild it")
    return await load_scans(conn, path)
