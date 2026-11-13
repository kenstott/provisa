# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f61a2-5c07-4e9b-b2a4-1f6e0c9d7a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's scans for terms carried in its bundle (REQ-2201): written out by the build with the
library documents they read, loaded into its template with each matched glossary term found by
its name, checked held, and copied into every sandbox cloned from the template."""

# Requirements: REQ-2201, REQ-2185

from __future__ import annotations

import json

import pytest
import sqlalchemy as sa
from sqlalchemy import delete, insert, select

from provisa.core import env_copy
from provisa.core.database import Database
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.schema_org import domains
from provisa.core.schema_org import glossary_term_drafts as drafts_t
from provisa.core.schema_org import glossary_term_scans as scans_t
from provisa.core.schema_org import reference_documents as documents_t
from provisa.reference_library import library, store
from provisa.term_finding import carry, queue, scan

SCHEMA = "org_default"
RULE = (
    "https://www.ecfr.gov/api/versioner/v1/full/2026-10-15/title-31.xml?part=1010&section=1010.311"
)
PASSAGES = [
    store.StoredPassage(
        0, "1010.311", None, "Each financial institution shall file a report.", [1.0, 0.0]
    ),
    store.StoredPassage(
        1, "1010.311", None, "A transaction in currency of more than $10,000.", [0.0, 1.0]
    ),
]


def _candidate(name: str, *, matched: int | None = None) -> scan.Candidate:
    found = scan.Candidate(
        name=name,
        category="business concept",
        sense="",
        definition="cash in or out",
        definition_kind=scan.EXPLICIT,
        passages=[{"ordinal": 1, "section": "1010.311", "page": None, "quote": "transaction"}],
        variants=[],
        conflicts=[],
    )
    if matched is not None:
        found.match_term_id, found.match_kind = matched, scan.SYNONYM
        found.matches = [{"term_id": matched, "name": "cash transaction", "via": name}]
    return found


async def _scanned(db) -> None:
    """The org as the build's setup left it: the rule in the library, scanned into two drafts,
    one matching the glossary's own term."""
    async with db.acquire() as conn, conn.transaction():
        await conn.execute_core(insert(domains).values(id="bank", description=""))
        term = await glossary_repo.create_abstract_term(conn, "cash transaction", domains={"bank"})
        document = await store.insert_document(
            conn,
            title="31 CFR 1010.311",
            source_kind=store.URL,
            source=RULE,
            media_type="application/xml",
            content_sha256="ab" * 32,
            status=store.PROPOSED,
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            added_by="org_admin",
            domains=["bank"],
            passages=PASSAGES,
        )
        scan_id = await queue.start_scan(
            conn,
            document_id=document,
            document_title="31 CFR 1010.311",
            document_source=RULE,
            domains=["bank"],
            started_by="org_admin",
            model="openrouter:some-model",
            launch="build",
        )
        await queue.finish_scan(
            conn,
            scan_id,
            [_candidate("transaction in currency", matched=term), _candidate("business day")],
            {"read": 2},
        )


async def _drafts(db) -> list[queue.DraftRow]:
    async with db.acquire() as conn:
        (only,) = await queue.scans(conn)
        return await queue.drafts(conn, only.id)


async def _forget(db) -> None:
    """The library and scans gone, and the term made again under another id, as a template's
    rebuild makes every glossary term afresh."""
    async with db.acquire() as conn, conn.transaction():
        await conn.execute_core(delete(scans_t))
        await conn.execute_core(delete(documents_t))
        for term in await glossary_repo.list_terms(conn):
            await glossary_repo.delete_term(conn, term["id"])
        await glossary_repo.create_abstract_term(conn, "filler", domains={"bank"})
        await glossary_repo.create_abstract_term(conn, "cash transaction", domains={"bank"})


@pytest.fixture
def db(control_plane):
    return control_plane.org()


async def test_the_build_s_scan_is_written_out_and_loaded_under_the_template_s_own_ids(
    control_plane, db, tmp_path
):
    await _scanned(db)
    out = carry.scans_path(tmp_path)
    assert carry.export_scans(control_plane.url, SCHEMA, out) == 1
    carried = json.loads(out.read_text())
    assert [d["source"] for d in carried["documents"]] == [RULE]
    # a setup scan step without keep: true leaves its document proposed, and it travels so
    assert [d["status"] for d in carried["documents"]] == [store.PROPOSED]
    # each passage's embedding travels as a list of floats
    assert [p["vector"] for p in carried["documents"][0]["passages"]] == [
        pytest.approx(list(p.vector), rel=1e-14) for p in PASSAGES
    ]
    assert [d["match_term"] for d in carried["scans"][0]["drafts"]] == ["cash transaction", None]

    await _forget(db)
    async with db.acquire() as conn:
        assert len(await carry.unheld_scans(conn, out, SCHEMA)) == 2
        assert await carry.load_scans(conn, out) == [RULE]
        assert await carry.unheld_scans(conn, out, SCHEMA) == []
        names = {t["name"]: t["id"] for t in await glossary_repo.list_terms(conn)}
        (document,) = await store.documents(conn)
        passages = await store.passages(conn, document.id)

    assert (document.source, document.domains, document.passages) == (RULE, ("bank",), 2)
    assert [p[3] for p in passages] == [p.text for p in PASSAGES]
    # loaded, the passages are searchable by their embeddings again, under the index
    async with db.acquire() as conn:
        hits = await store.nearest_passages(conn, [0.1, 0.9], [document.id], 2)
        indexed = await _vector_indexes(conn, SCHEMA)
    assert [h.ordinal for h in hits] == [1, 0]
    assert indexed == {"reference_passages_vector_2"}
    (scanned,) = await _scans(db)
    assert (scanned.status, scanned.document_id, scanned.summary) == (
        "done",
        document.id,
        {"read": 2},
    )
    matched, new = await _drafts(db)
    assert (matched.name, matched.match_term_id) == (
        "transaction in currency",
        names["cash transaction"],
    )
    assert matched.payload["matches"][0]["term_id"] == names["cash transaction"]
    assert (new.name, new.match_term_id, new.status) == ("business day", None, queue.DRAFT)


async def _scans(db) -> list[queue.ScanRow]:
    async with db.acquire() as conn:
        return await queue.scans(conn)


async def test_loading_again_replaces_the_document_and_its_scans_whole(control_plane, db, tmp_path):
    await _scanned(db)
    out = carry.scans_path(tmp_path)
    carry.export_scans(control_plane.url, SCHEMA, out)
    async with db.acquire() as conn:
        await carry.load_scans(conn, out)
        await carry.load_scans(conn, out)
        assert len(await store.documents(conn)) == 1
        assert await carry.unheld_scans(conn, out, SCHEMA) == []
    assert len(await _scans(db)) == 1


async def test_a_matched_term_the_org_does_not_hold_is_refused_by_name(control_plane, db, tmp_path):
    await _scanned(db)
    out = carry.scans_path(tmp_path)
    carry.export_scans(control_plane.url, SCHEMA, out)
    async with db.acquire() as conn, conn.transaction():
        for term in await glossary_repo.list_terms(conn):
            await glossary_repo.delete_term(conn, term["id"])
    async with db.acquire() as conn:
        with pytest.raises(ValueError, match="'cash transaction', which this org does not hold"):
            await carry.load_scans(conn, out)


async def test_a_template_missing_a_carried_scan_is_not_held(control_plane, db, tmp_path):
    await _scanned(db)
    out = carry.scans_path(tmp_path)
    carry.export_scans(control_plane.url, SCHEMA, out)
    async with db.acquire() as conn:
        await conn.execute_core(delete(drafts_t))
        assert await carry.unheld_scans(conn, out, SCHEMA) == [
            f"the done scan of {RULE} with 2 drafts is not there"
        ]


#: The sandbox's tables a copy of the template's scans reads or writes: the scan tables, and the
#: glossary term embeddings whose vector indexes it fits to the rows held (store.index_vectors).
_SANDBOX_TABLES = (*env_copy._SCAN_TABLES, store.TERM_VECTORS)


def _lay_out_sandbox(control_plane) -> None:
    """A sandbox's schema as schema.sql lays it out, for the tables the copy touches: no vector
    index until it holds rows (LIKE copied the template's under a name of its own)."""
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('CREATE SCHEMA "org_sandbox"')
        for table in _SANDBOX_TABLES:
            c.exec_driver_sql(
                f'CREATE TABLE "org_sandbox"."{table}" (LIKE "{SCHEMA}"."{table}" INCLUDING ALL)'
            )
        for (name,) in c.exec_driver_sql(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'org_sandbox' "
            "AND indexdef LIKE '%%USING hnsw%%'"
        ).fetchall():
            c.exec_driver_sql(f'DROP INDEX "org_sandbox"."{name}"')


async def test_a_sandbox_is_cloned_with_its_template_s_library_and_scans(control_plane, db):
    await _scanned(db)
    _lay_out_sandbox(control_plane)
    async with db.acquire() as conn:
        copied = await env_copy.copy_term_scans(conn, SCHEMA, "org_sandbox")
        assert copied == list(env_copy._SCAN_TABLES)
        held = {
            table: (
                await conn.execute_core(
                    select(env_copy._scoped(env_copy.org_metadata.tables[table], "org_sandbox"))
                )
            ).fetchall()
            for table in env_copy._SCAN_TABLES
        }
    # REQ-2208: the template's document carries no tags and no citations here, so none are copied
    assert [len(held[t]) for t in env_copy._SCAN_TABLES] == [1, 1, 2, 0, 0, 1, 2]
    async with db.acquire() as conn:
        assert await _vector_indexes(conn, "org_sandbox") == {"reference_passages_vector_2"}
    sandbox = Database(
        control_plane.engine(f"{control_plane.url}?options=-csearch_path%3Dorg_sandbox"),
        name="org-model",
        search_path="org_sandbox",
    )
    async with sandbox.acquire() as conn:
        (document,) = await store.documents(conn)
        hits = await store.nearest_passages(conn, [1.0, 0.1], [document.id], 1)
    assert [h.text for h in hits] == [PASSAGES[0].text]


async def test_an_assistant_in_a_sandbox_finds_the_passages_of_the_regulation_its_demo_scanned(
    control_plane, db, tmp_path
):
    """REQ-2185's acceptance in the bank secrecy demo: the regulation the bundle's build scanned
    is what Polly cites in a sandbox. Through retrieve(), as search_reference_library calls it,
    not store.nearest_passages: the library's own filter (kept documents a role reads) applies."""
    from provisa.reference_library.library import Embedder
    from provisa.reference_library.retrieve import retrieve

    await _scanned(db)
    # the build's keep: setup.yaml's keep: true, through the keep a steward's PATCH runs
    async with db.acquire() as conn, conn.transaction():
        (scanned,) = await store.documents(conn)
        await library.keep(conn, scanned.id)
    out = carry.scans_path(tmp_path)
    carry.export_scans(control_plane.url, SCHEMA, out)
    assert [d["status"] for d in json.loads(out.read_text())["documents"]] == [store.KEPT]
    await _forget(db)
    async with db.acquire() as conn, conn.transaction():
        await carry.load_scans(conn, out)
    _lay_out_sandbox(control_plane)
    async with db.acquire() as conn:
        await env_copy.copy_term_scans(conn, SCHEMA, "org_sandbox")
    sandbox = Database(
        control_plane.engine(f"{control_plane.url}?options=-csearch_path%3Dorg_sandbox"),
        name="org-model",
        search_path="org_sandbox",
    )

    async def embed(texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.1] for _ in texts]

    embedder = Embedder(model="sentence-transformers/all-MiniLM-L6-v2", embed=embed)
    async with sandbox.acquire() as conn:
        found = await retrieve(
            conn, "Which cash needs a currency transaction report?", {"bank"}, embedder, k=2
        )
    assert [(c.source, c.section, c.quote) for c in found] == [
        (RULE, "1010.311", PASSAGES[0].text),
        (RULE, "1010.311", PASSAGES[1].text),
    ]


async def _vector_indexes(conn, schema: str) -> set[str]:
    rows = await conn.execute_core(
        sa.text(
            "SELECT indexname FROM pg_indexes WHERE schemaname = :schema "
            "AND tablename = 'reference_passages' AND indexname LIKE 'reference_passages_vector_%'"
        ).bindparams(schema=schema)
    )
    return {r[0] for r in rows.fetchall()}


def test_a_value_with_no_json_form_is_refused_by_its_type():
    with pytest.raises(TypeError, match="set"):
        carry._encode({1})


def test_the_bundle_carries_them_beside_the_seed(tmp_path):
    assert carry.scans_path(tmp_path) == tmp_path / "seeded" / "term_scans.json"


async def test_a_server_started_as_no_demo_or_a_bundle_without_scans_is_refused_by_name(
    tmp_path,
):
    with pytest.raises(ValueError, match="started as no named demo"):
        await carry.load_bundle_scans(None, None)
    with pytest.raises(ValueError, match=r"carries no scans \(term_scans\.json\)"):
        await carry.load_bundle_scans(None, tmp_path)
