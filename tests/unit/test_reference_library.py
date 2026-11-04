# Copyright (c) 2026 Kenneth Stott
# Canary: 909702f3-d57a-40e2-9dac-b083d69153bb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2185: the reference library -- documents read into passages, embedded, held in the org's
control plane and searched for a role, each passage with what to cite; and the Admin page's API
that adds, keeps, rescopes, refreshes, replaces and removes them."""

# Requirements: REQ-2185

from __future__ import annotations

import base64
import math
import re
import struct
import types
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
import sqlalchemy as sa
from sqlalchemy import insert, select

from provisa.api.admin import reference_library_router as router
from provisa.api.errors import ApiError
from provisa.core.schema_org import domains, reference_passages
from provisa.reference_library import extract, library, retrieve, store
from provisa.reference_library.passages import LIMIT, TARGET, cut

if TYPE_CHECKING:
    from fastapi import Request

SCHEMA = "org_default"

# -- reading a document -------------------------------------------------------------------------

POLICY = b"""<html><head><title>Client policy</title><script>var x=1;</script></head>
<body><nav><a href="/">Home</a></nav>
<h1>Clients</h1><p>A Client is a party that holds an account with the bank.</p>
<h2>Retention</h2><p>Client records are kept for seven years.</p>
<ul><li>Statements</li><li>Contracts <b>and</b> notices</li></ul>
<footer>Copyright</footer></body></html>"""

SPEC = b"""# Payments standard

Every payment carries an amount and a currency.

## Settlement

A payment settles on the second business day.
"""


def test_html_is_read_by_its_headings_without_its_chrome():
    document = extract.read(POLICY, extract.HTML, "policy.html")
    assert document.title == "Client policy"
    assert [(s.heading, s.paragraphs) for s in document.sections] == [
        ("Clients", ["A Client is a party that holds an account with the bank."]),
        (
            "Retention",
            ["Client records are kept for seven years.", "Statements", "Contracts and notices"],
        ),
    ]


def test_markdown_is_read_by_its_headings_and_titled_by_its_first():
    document = extract.read(SPEC, extract.MARKDOWN, "spec.md")
    assert document.title == "Payments standard"
    assert [s.heading for s in document.sections] == ["Payments standard", "Settlement"]


def test_plain_text_is_read_as_paragraphs():
    document = extract.read(b"one\nline\n\ntwo", extract.PLAIN, "notes.txt")
    assert document.title is None
    assert document.sections[0].paragraphs == ["one line", "two"]


@pytest.mark.parametrize(
    ("content", "media", "name", "says"),
    [
        (b"x", "application/zip", "a.zip", "does not read application/zip"),
        (b"\xff\xfe\x00", extract.PLAIN, "a.txt", "is not utf-8 text"),
        (b"<html><body><script>x</script></body></html>", extract.HTML, "a.html", "holds no text"),
    ],
)
def test_a_document_that_cannot_be_read_is_refused_by_name(content, media, name, says):
    with pytest.raises(extract.UnreadableDocument, match=says):
        extract.read(content, media, name)


def test_a_file_is_read_as_its_name_says_and_an_unknown_kind_is_refused():
    assert extract.media_type_of_name("Policy.HTML") == extract.HTML
    assert extract.media_type_of_name("spec.md") == extract.MARKDOWN
    assert extract.media_type_of_name("contract.PDF") == extract.PDF
    assert extract.media_type_of_name("standard.docx") == extract.WORD
    with pytest.raises(extract.UnreadableDocument, match="'deck.pptx' is not a document"):
        extract.media_type_of_name("deck.pptx")


def test_a_response_is_read_as_its_content_type_says():
    assert extract.media_type_of_response("text/html; charset=ISO-8859-1", "https://x/p") == (
        extract.HTML,
        "ISO-8859-1",
    )
    assert extract.media_type_of_response(None, "https://x/a/spec.md?v=1") == (
        extract.MARKDOWN,
        None,
    )
    with pytest.raises(extract.UnreadableDocument, match="serves image/png"):
        extract.media_type_of_response("image/png", "https://x/logo")


def _pdf(pages: list[str], title: str) -> bytes:
    """A PDF of ``pages``, each one line of Helvetica text, with ``title`` in its information."""
    objects = ["<< /Type /Catalog /Pages 2 0 R >>"]
    kids = " ".join(f"{4 + 2 * i} 0 R" for i in range(len(pages)))
    objects.append(f"<< /Type /Pages /Kids [{kids}] /Count {len(pages)} >>")
    objects.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    for i, text in enumerate(pages):
        stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET"
        objects.append(
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {5 + 2 * i} 0 R >>"
        )
        objects.append(f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream")
    objects.append(f"<< /Title ({title}) >>")
    out = b"%PDF-1.4\n"
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n{body}\nendobj\n".encode()
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R /Info {len(objects)} 0 R >>\n"
        f"startxref\n{xref}\n%%EOF\n"
    ).encode()
    return out


def test_a_pdf_is_read_by_its_pages():
    document = extract.read(
        _pdf(["A Client holds an account.", "Records are kept seven years."], "Client policy"),
        extract.PDF,
        "policy.pdf",
    )
    assert document.title == "Client policy"
    assert [(s.page, s.paragraphs) for s in document.sections] == [
        (1, ["A Client holds an account."]),
        (2, ["Records are kept seven years."]),
    ]
    assert [(p.page, p.section) for p in cut(document)] == [(1, None), (2, None)]


def test_a_word_document_is_read_by_its_headings_with_its_tables_in_place():
    import io

    import docx

    word = docx.Document()
    word.add_paragraph("Client policy", style="Title")
    word.add_heading("Clients", 1)
    word.add_paragraph("A Client holds an account.")
    table = word.add_table(rows=1, cols=2)
    table.rows[0].cells[0].text = "Term"
    table.rows[0].cells[1].text = "Client"
    word.add_heading("Retention", 1)
    word.add_paragraph("Records are kept seven years.")
    data = io.BytesIO()
    word.save(data)
    document = extract.read(data.getvalue(), extract.WORD, "policy.docx")
    assert document.title == "Client policy"
    assert [(s.heading, s.paragraphs) for s in document.sections] == [
        ("Clients", ["A Client holds an account.", "Term | Client"]),
        ("Retention", ["Records are kept seven years."]),
    ]


@pytest.mark.parametrize(
    ("media", "name", "says"),
    [
        (extract.PDF, "x.pdf", "'x.pdf' is not a PDF that can be read"),
        (extract.WORD, "x.docx", "'x.docx' is not a Word document that can be read"),
    ],
)
def test_a_pdf_or_word_file_that_is_not_one_is_refused_by_name(media, name, says):
    with pytest.raises(extract.UnreadableDocument, match=says):
        extract.read(b"not a document", media, name)


ECFR = Path(__file__).resolve().parents[1] / "fixtures" / "ecfr"


def test_ecfr_xml_is_read_by_its_section_heading():
    """31 CFR 1010.311 as eCFR's API serves it (versioner/v1/full/<date>/title-31.xml)."""
    document = extract.read(
        (ECFR / "31-cfr-1010.311.xml").read_bytes(), extract.XML, "title-31.xml"
    )
    heading = "§ 1010.311 Filing obligations for reports of transactions in currency."
    assert document.title == heading
    assert [s.heading for s in document.sections] == [heading]
    [paragraph] = document.sections[0].paragraphs
    assert paragraph.startswith(
        "Each financial institution other than a casino shall file a report of each deposit,"
    )
    assert paragraph.endswith("the purchase of postage or philatelic products.")
    [passage] = cut(document)
    assert passage.section == heading and passage.text == paragraph


def test_ecfr_definitions_are_each_read_under_their_own_heading():
    document = extract.read(
        (ECFR / "31-cfr-1010.100.xml").read_bytes(), extract.XML, "title-31.xml"
    )
    sections = {s.heading: s.paragraphs for s in document.sections}
    assert document.title == "§ 1010.100 General definitions."
    assert sections["§ 1010.100 General definitions."][0].startswith(
        "When used in this chapter and in forms prescribed under this chapter"
    )
    assert sections["§ 1010.100 General definitions — (i) Business day"] == [
        "(i) Business day. As used in this chapter with respect to banks, business day means "
        "that day, as normally communicated to its depository customers, on which a bank "
        "routinely posts a particular transaction to its customer's account."
    ]
    institution = sections["§ 1010.100 General definitions — (t) Financial institution"]
    assert institution[0].startswith("(t) Financial institution. Each agent, agency, branch")
    assert "(3) A money services business as defined in paragraph (ff) of this section;" in (
        institution
    )
    # a term defined inside another definition is headed by its own term
    assert "§ 1010.100 General definitions — (6)(i) Card club" in sections
    text = " ".join(p for paragraphs in sections.values() for p in paragraphs)
    # eCFR's amendment links and source citation are not the regulation's text
    assert "Link to an amendment" not in text and "75 FR 65812" not in text


async def test_an_ecfr_api_address_is_read_into_passages_with_their_sections():
    url = (
        "https://www.ecfr.gov/api/versioner/v1/full/2026-10-15/title-31.xml"
        "?part=1010&section=1010.311"
    )
    media, charset = extract.media_type_of_response("text/xml", url)
    content = library.Content((ECFR / "31-cfr-1010.311.xml").read_bytes(), media, charset, url)
    read = await library._read(content, EMBEDDER)
    assert read.title == "§ 1010.311 Filing obligations for reports of transactions in currency."
    assert [p.section for p in read.passages] == [read.title]
    assert read.passages[0].text.startswith("Each financial institution other than a casino")


def test_xml_is_read_as_its_content_type_or_name_says():
    assert extract.media_type_of_response("text/xml", "https://x/t.xml") == (extract.XML, None)
    assert extract.media_type_of_response("application/xml; charset=utf-8", "https://x/t") == (
        extract.XML,
        "utf-8",
    )
    assert extract.media_type_of_name("title-31.XML") == extract.XML


@pytest.mark.parametrize(
    ("content", "says"),
    [
        (
            b"<rss><channel><title>News</title></channel></rss>",
            "'feed.xml' is XML the library does not read",
        ),
        (b"<DIV8><HEAD>x</HEAD><P>unclosed", "'feed.xml' is not XML that can be read"),
        (b"<DIV8 N='1'><HEAD>Empty</HEAD></DIV8>", "'feed.xml' holds no text"),
    ],
)
def test_xml_the_reader_cannot_read_is_refused_by_name(content, says):
    with pytest.raises(extract.UnreadableDocument, match=says):
        extract.read(content, extract.XML, "feed.xml")


def test_ecfr_xml_does_not_resolve_its_entities(tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("do not read me")
    content = (
        f'<?xml version="1.0"?><!DOCTYPE DIV8 [<!ENTITY x SYSTEM "file://{secret}">]>'
        "<DIV8 N='1'><HEAD>H</HEAD><P>before &x; after</P></DIV8>"
    ).encode()
    document = extract.read(content, extract.XML, "t.xml")
    assert "do not read me" not in " ".join(document.sections[0].paragraphs)


def test_passages_keep_to_one_section_and_a_long_paragraph_is_cut_at_its_sentences():
    long = " ".join(f"Sentence {i} is about settlement terms." for i in range(120))
    document = extract.Document(
        title=None,
        sections=[
            extract.Section("A", None, ["short one", "short two"]),
            extract.Section("B", None, [long]),
        ],
    )
    passages = cut(document)
    assert passages[0].section == "A" and passages[0].text == "short one\n\nshort two"
    rest = passages[1:]
    assert {p.section for p in rest} == {"B"}
    assert all(len(p.text) <= max(LIMIT, TARGET) for p in rest)
    assert all(p.text.rstrip().endswith(".") for p in rest)
    assert [p.ordinal for p in passages] == list(range(len(passages)))


# -- the library in the control plane -----------------------------------------------------------

VOCABULARY = ("client", "account", "record", "payment", "currency", "settle", "contract")


def _vector(text: str) -> list[float]:
    words = re.findall(r"[a-z]+", text.lower())
    return [float(sum(w.startswith(v) for w in words)) + 0.01 for v in VOCABULARY]


async def _embed(texts: list[str]) -> list[list[float]]:
    return [_vector(t) for t in texts]


EMBEDDER = library.Embedder(model="test-embed", embed=_embed)


@pytest.fixture
def db(control_plane):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(domains).values(id="finance", description=""))
    return control_plane.org()


def _file(name: str, data: bytes) -> library.Content:
    return library.file_content(name, data)


async def _add(conn, name: str, data: bytes, scope: list[str], status: str = store.KEPT) -> int:
    return await library.add(
        conn,
        _file(name, data),
        source_kind=store.FILE,
        domains=scope,
        status=status,
        embedder=EMBEDDER,
        added_by="alice",
    )


async def test_a_role_draws_on_the_documents_whose_domains_it_reads_and_every_whole_org_one(db):
    async with db.acquire() as conn:
        policy = await _add(conn, "policy.html", POLICY, ["sales"])
        await _add(conn, "spec.md", SPEC, ["finance"])
        whole = await _add(conn, "glossary.txt", b"A contract binds a client.", ["*"])

        sales = await retrieve.retrieve(conn, "What is a client?", frozenset({"sales"}), EMBEDDER)
        assert {c.document_id for c in sales} == {policy, whole}
        top = sales[0]
        assert (top.document_id, top.document_title, top.source, top.section) == (
            policy,
            "Client policy",
            "policy.html",
            "Clients",
        )
        assert top.quote == "A Client is a party that holds an account with the bank."

        finance = await retrieve.retrieve(conn, "client", frozenset({"finance"}), EMBEDDER)
        assert policy not in {c.document_id for c in finance}

        everyone = await retrieve.retrieve(conn, "payment currency", None, EMBEDDER, k=1)
        assert [c.document_title for c in everyone] == ["Payments standard"]


async def test_a_proposed_document_is_not_drawn_on_until_it_is_kept(db):
    async with db.acquire() as conn:
        proposed = await _add(conn, "policy.html", POLICY, ["*"], status=store.PROPOSED)
        assert await retrieve.retrieve(conn, "client", None, EMBEDDER) == []
        await library.keep(conn, proposed)
        assert {c.document_id for c in await retrieve.retrieve(conn, "client", None, EMBEDDER)} == {
            proposed
        }
        with pytest.raises(library.LibraryRefused, match="already kept"):
            await library.keep(conn, proposed)


async def test_a_document_already_in_the_library_is_not_added_again(db):
    async with db.acquire() as conn:
        first = await _add(conn, "policy.html", POLICY, ["sales"])
        with pytest.raises(library.AlreadyInLibrary) as refused:
            await _add(conn, "copy-of-policy.html", POLICY, ["finance"])
        assert refused.value.document_id == first
        assert len(await store.documents(conn)) == 1


async def test_a_new_version_replaces_the_content_and_keeps_place_status_and_domains(db):
    async with db.acquire() as conn:
        document_id = await _add(conn, "policy.html", POLICY, ["sales"])
        before = await store.document(conn, document_id)
        await library.new_version(
            conn, document_id, _file("policy-v2.md", b"# Policy\n\nA payment settles."), EMBEDDER
        )
        after = await store.document(conn, document_id)
        assert after is not None and before is not None
        assert (after.title, after.status, after.domains, after.added_at) == (
            before.title,
            before.status,
            ("sales",),
            before.added_at,
        )
        assert after.source == "policy-v2.md" and after.replaced_at is not None
        assert after.refreshed_at is None
        texts = [t for _, _, _, t in await store.passages(conn, document_id)]
        assert texts == ["A payment settles."]
        with pytest.raises(library.LibraryRefused, match="uploaded as a file"):
            await library.refresh(conn, document_id, EMBEDDER, None)


async def test_refresh_fetches_a_url_document_again(db, monkeypatch):
    served = {"body": SPEC}

    async def fetch(url, org_id):
        return library.Content(served["body"], extract.MARKDOWN, None, url)

    monkeypatch.setattr(library, "fetch", fetch)
    url = "https://example.com/payments.md"
    async with db.acquire() as conn:
        document_id = await library.add(
            conn,
            await library.fetch(url, None),
            source_kind=store.URL,
            domains=["finance"],
            status=store.KEPT,
            embedder=EMBEDDER,
            added_by=None,
        )
        with pytest.raises(library.AlreadyInLibrary):
            await library.add(
                conn,
                await library.fetch(url, None),
                source_kind=store.URL,
                domains=["finance"],
                status=store.KEPT,
                embedder=EMBEDDER,
                added_by=None,
            )
        served["body"] = b"# Payments standard\n\nA payment settles on the same day."
        await library.refresh(conn, document_id, EMBEDDER, None)
        held = await store.document(conn, document_id)
        assert held is not None and held.refreshed_at is not None and held.source == url
        assert [t for *_, t in await store.passages(conn, document_id)] == [
            "A payment settles on the same day."
        ]
        with pytest.raises(library.LibraryRefused, match="added from its URL"):
            await library.new_version(conn, document_id, _file("x.txt", b"other"), EMBEDDER)


async def test_removing_a_document_takes_its_passages_with_it(db):
    async with db.acquire() as conn:
        document_id = await _add(conn, "policy.html", POLICY, ["*"])
        await library.remove(conn, document_id)
        assert await retrieve.retrieve(conn, "client", None, EMBEDDER) == []
        held = (await conn.execute_core(select(reference_passages.c.id))).fetchall()
        assert held == []
        with pytest.raises(library.DocumentNotFound):
            await library.remove(conn, document_id)


async def test_a_document_embedded_by_another_model_is_named_rather_than_compared(db):
    async with db.acquire() as conn:
        await _add(conn, "policy.html", POLICY, ["*"])
        other = library.Embedder(model="other-embed", embed=_embed)
        with pytest.raises(retrieve.LibraryModelChanged, match="'Client policy'.*'other-embed'"):
            await retrieve.retrieve(conn, "client", None, other)


@pytest.mark.parametrize(
    ("raw", "says"),
    [
        ([], "list of domain ids"),
        ("sales", "list of domain ids"),
        (["*", "sales"], "cannot be combined"),
        (["marketing"], "no domain 'marketing'"),
    ],
)
def test_a_documents_domains_are_the_orgs_or_the_whole_org(raw, says):
    with pytest.raises(library.LibraryRefused, match=says):
        library.check_domains(raw, {"sales", "finance"})
    assert library.check_domains(["finance", "sales", "sales"], {"sales", "finance"}) == [
        "finance",
        "sales",
    ]
    assert library.check_domains(["*"], set()) == ["*"]


def test_a_url_is_an_http_address():
    assert library.check_url("https://example.com/a") == "https://example.com/a"
    for bad in ("ftp://example.com/a", "example.com/a", "https://", 3):
        with pytest.raises(library.LibraryRefused, match="http"):
            library.check_url(bad)


# -- the Admin page's API -----------------------------------------------------------------------


def _request() -> Request:
    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="default",
        )
    )
    return cast("Request", fake)


@pytest.fixture
def api(db, monkeypatch):
    monkeypatch.setattr(router, "_pool", lambda: db)
    monkeypatch.setattr(router, "_guard", lambda request: "default")

    async def embedder():
        return EMBEDDER

    monkeypatch.setattr(router, "_embedder", embedder)
    return db


def _upload(name: str, data: bytes, **extra: Any) -> dict:
    return {"file_name": name, "content_b64": base64.b64encode(data).decode(), **extra}


async def test_the_admin_api_adds_lists_rescopes_and_removes_a_document(api):
    added = await router.add_document(_upload("policy.html", POLICY, domains=["sales"]), _request())
    assert (added["title"], added["source_kind"], added["status"], added["domains"]) == (
        "Client policy",
        "file",
        "kept",
        ["sales"],
    )
    assert added["passages"] == 2 and added["added_by"] == "alice"
    listed = await router.list_documents(_request())
    assert [d["id"] for d in listed["documents"]] == [added["id"]]

    changed = await router.change_document(
        added["id"], {"domains": ["*"], "title": "Clients"}, _request()
    )
    assert (changed["domains"], changed["title"]) == (["*"], "Clients")

    detail = await router.get_document(added["id"], _request())
    assert [p["section"] for p in detail["passage_list"]] == ["Clients", "Retention"]

    assert await router.remove_document(added["id"], _request()) == {"removed": added["id"]}
    with pytest.raises(ApiError) as gone:
        await router.get_document(added["id"], _request())
    assert gone.value.status_code == 404 and gone.value.code == "reference_library.not_found"


async def test_the_admin_api_refuses_by_name(api):
    async def refused(body: dict) -> ApiError:
        with pytest.raises(ApiError) as caught:
            await router.add_document(body, _request())
        return caught.value

    unknown = await refused(_upload("policy.html", POLICY, domains=["marketing"]))
    assert unknown.status_code == 400 and "marketing" in str(unknown.detail)
    both = await refused({**_upload("a.txt", b"x"), "url": "https://x/a", "domains": ["*"]})
    assert "not both" in str(both.detail)
    unreadable = await refused(_upload("deck.pptx", b"x", domains=["*"]))
    assert "'deck.pptx' is not a document" in str(unreadable.detail)
    added = await router.add_document(_upload("policy.html", POLICY, domains=["*"]), _request())
    again = await refused(_upload("policy.html", POLICY, domains=["*"]))
    assert again.status_code == 409 and again.params == {"document_id": added["id"]}
    with pytest.raises(ApiError, match="discarding it removes it"):
        await router.change_document(added["id"], {"status": "proposed"}, _request())


async def test_the_admin_api_uploads_a_new_version(api):
    added = await router.add_document(_upload("spec.md", SPEC, domains=["finance"]), _request())
    replaced = await router.upload_version(
        added["id"], _upload("spec-v2.txt", b"Payments settle daily."), _request()
    )
    assert replaced["source"] == "spec-v2.txt" and replaced["replaced_at"] is not None
    assert replaced["passages"] == 1


# -- what an assistant reaches ------------------------------------------------------------------


async def test_the_mcp_tool_searches_for_the_callers_role(db, monkeypatch):
    from provisa.api.mcp import tools

    async with db.acquire() as conn:
        await _add(conn, "policy.html", POLICY, ["sales"])
        await _add(conn, "spec.md", SPEC, ["finance"])

    async def embedder(state):
        return EMBEDDER

    monkeypatch.setattr(retrieve, "org_embedder", embedder)
    state = types.SimpleNamespace(
        model_db=db,
        roles={"analyst": {"id": "analyst", "domain_access": ["sales"], "capabilities": []}},
    )
    monkeypatch.setattr(tools, "require_role", lambda role, state: role)
    found = await tools.search_reference_library(cast(Any, state), "analyst", "client account")
    assert {f["document_title"] for f in found} == {"Client policy"}
    assert set(found[0]) == {
        "document_id",
        "document_title",
        "source",
        "section",
        "page",
        "quote",
        "score",
        "ordinal",
        "reached",
    }


# -- neighbours (REQ-2185, THE LIBRARY'S STORE AND RETRIEVAL) -----------------------------------

ACCOUNTS = b"""# Accounts

A client holds an account.

## Records

Account records are kept for a client.

## Payments

A payment carries a currency.

## Settlement

A payment settles in its currency.

## Contracts

A contract binds the client.
"""

SETTLEMENT = b"""# Settlement terms

A payment settles in its currency.

## Fees

A contract fee is charged.
"""


async def test_a_passage_s_adjacent_passages_are_read_from_its_document(db):
    async with db.acquire() as conn:
        accounts = await _add(conn, "accounts.md", ACCOUNTS, ["*"])
        other = await _add(conn, "spec.md", SPEC, ["*"])
        ids = [accounts, other]
        assert [o for o, *_ in await store.passages(conn, accounts)] == [0, 1, 2, 3, 4]

        async def beside(ordinal: int, n: int = 1) -> list[tuple[int, int]]:
            found = await store.adjacent_passages(conn, accounts, ordinal, ids, n)
            return [(p.document_id, p.ordinal) for p in found]

        assert await beside(2) == [(accounts, 1), (accounts, 3)]
        assert await beside(0) == [(accounts, 1)]
        assert await beside(4) == [(accounts, 3)]
        assert await beside(2, 2) == [(accounts, o) for o in (0, 1, 3, 4)]
        (after,) = [
            p for p in await store.adjacent_passages(conn, accounts, 2, ids) if p.ordinal == 3
        ]
        assert (after.section, after.text) == ("Settlement", "A payment settles in its currency.")
        with pytest.raises(ValueError, match="not among the documents being searched"):
            await store.adjacent_passages(conn, accounts, 2, [other])
        with pytest.raises(ValueError, match="n must be a positive integer"):
            await store.adjacent_passages(conn, accounts, 2, ids, 0)

        from sqlalchemy.dialects import postgresql

        query = store.adjacent_query(accounts, 2, 1).compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
        async with conn.transaction():
            await conn.execute_core(sa.text("SET LOCAL enable_seqscan = off"))
            explained = await conn.execute_core(sa.text(f"EXPLAIN {query}"))
            plan = "\n".join(r[0] for r in explained.fetchall())
        assert "reference_passages_document_id_ordinal" in plan


async def test_the_passages_nearest_a_passage_are_those_the_cosine_says_but_itself(db):
    async with db.acquire() as conn:
        ids = [
            await _add(conn, "accounts.md", ACCOUNTS, ["*"]),
            await _add(conn, "policy.html", POLICY, ["*"]),
        ]
        held = [
            (document_id, ordinal, text_)
            for document_id in ids
            for ordinal, _, _, text_ in await store.passages(conn, document_id)
        ]
        for seed_document, seed_ordinal, seed_text in held[:3]:
            seed = _float4(_vector(seed_text))
            expected = sorted(
                (-_old_cosine(seed, _float4(_vector(t))), d, o, t)
                for d, o, t in held
                if (d, o) != (seed_document, seed_ordinal)
            )[:3]
            hits = await store.near_passage(conn, seed_document, seed_ordinal, ids, 3)
            assert [(h.document_id, h.ordinal, h.text) for h in hits] == [
                (d, o, t) for _, d, o, t in expected
            ]
            for hit, (negative, *_rest) in zip(hits, expected, strict=True):
                assert hit.score == pytest.approx(-negative, rel=FLOAT4)

        # only the documents searched are reached, and the seed's must be one of them
        only = await store.near_passage(conn, ids[0], 2, [ids[0]], 10)
        assert {h.document_id for h in only} == {ids[0]}
        assert [h.ordinal for h in only if h.ordinal == 2] == []
        with pytest.raises(ValueError, match="not among the documents being searched"):
            await store.near_passage(conn, ids[0], 2, [ids[1]], 3)
        with pytest.raises(ValueError, match="has no passage 99"):
            await store.near_passage(conn, ids[0], 99, ids, 3)

        sql, params = store.near_passage_sql(ids[0], 2, len(VOCABULARY), ids, 3)
        async with conn.transaction():
            await conn.execute_core(sa.text("SET LOCAL enable_seqscan = off"))
            explained = await conn.execute_core(sa.text(f"EXPLAIN {sql}").bindparams(**params))
            plan = "\n".join(r[0] for r in explained.fetchall())
        assert f"reference_passages_vector_{len(VOCABULARY)}" in plan


def _keys(found: list[retrieve.Citation]) -> list[tuple[int, int]]:
    return [(c.document_id, c.ordinal) for c in found]


async def test_neighbours_fill_the_budget_after_the_question_s_passages_in_order(db):
    async with db.acquire() as conn:
        accounts = await _add(conn, "accounts.md", ACCOUNTS, ["*"])
        question = "When does a payment settle?"
        (hit,) = await retrieve.retrieve(conn, question, None, EMBEDDER, k=1)
        assert (hit.ordinal, hit.reached) == (3, retrieve.NEAREST_QUESTION)
        near = await store.near_passage(conn, accounts, 3, [accounts], retrieve.NEAR_PER_PASSAGE)
        # the passage nearest the settlement passage is the one before it, already given
        assert near[0].ordinal == 2

        found = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=5
        )
        named = "passage 3 in 'Accounts'"
        assert [(c.ordinal, c.reached) for c in found] == [
            (3, retrieve.NEAREST_QUESTION),
            (4, f"after {named}"),
            (2, f"before {named}"),
            *[(h.ordinal, f"near {named}") for h in near[1:]],
        ]
        assert found[0] == hit
        assert all(c.score is None for c in found[1:])
        # four, under the budget of five: the passage before was also the nearest, given once
        assert len(found) == 4 and len(set(_keys(found))) == len(found)
        assert found[1].quote == "A contract binds the client." and found[1].section == "Contracts"

        # the budget cuts the neighbours, in their order
        cut_short = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=2
        )
        assert _keys(cut_short) == _keys(found)[:2]


async def test_neighbours_never_displace_the_question_s_passages_nor_repeat_them(db):
    async with db.acquire() as conn:
        await _add(conn, "accounts.md", ACCOUNTS, ["*"])
        question = "payment currency"
        hits = await retrieve.retrieve(conn, question, None, EMBEDDER, k=2)
        # the two nearest the question sit side by side: each is the other's neighbour
        assert sorted(c.ordinal for c in hits) == [2, 3]

        full = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=2, budget=2
        )
        assert full == hits
        found = await retrieve.retrieve_with_neighbours(conn, question, None, EMBEDDER, k=2)
        assert found[:2] == hits
        assert len(found) == 4
        assert len(set(_keys(found))) == len(found)
        assert {c.ordinal for c in found[2:]} == {1, 4}
        assert all(c.reached != retrieve.NEAREST_QUESTION for c in found[2:])
        with pytest.raises(ValueError, match="cannot hold the 2 nearest the question"):
            await retrieve.retrieve_with_neighbours(conn, question, None, EMBEDDER, k=2, budget=1)


async def test_a_role_never_gets_a_neighbour_from_a_document_it_cannot_read(db):
    async with db.acquire() as conn:
        accounts = await _add(conn, "accounts.md", ACCOUNTS, ["sales"])
        settlement = await _add(conn, "settlement.md", SETTLEMENT, ["finance"])
        question = "When does a payment settle?"

        everyone = await retrieve.retrieve_with_neighbours(
            conn, question, None, EMBEDDER, k=1, budget=20
        )
        # with both documents readable, the other document's passages are reached
        assert settlement in {c.document_id for c in everyone}

        sales = await retrieve.retrieve_with_neighbours(
            conn, question, frozenset({"sales"}), EMBEDDER, k=2, budget=20
        )
        assert sales and {c.document_id for c in sales} == {accounts}
        finance = await retrieve.retrieve_with_neighbours(
            conn, question, frozenset({"finance"}), EMBEDDER, k=1, budget=20
        )
        assert finance and {c.document_id for c in finance} == {settlement}


async def _labelled_state(db, monkeypatch) -> Any:
    from provisa.api.mcp import tools

    async def embedder(state):
        return EMBEDDER

    monkeypatch.setattr(retrieve, "org_embedder", embedder)
    monkeypatch.setattr(tools, "require_role", lambda role, state: role)
    return types.SimpleNamespace(
        model_db=db,
        roles={"analyst": {"id": "analyst", "domain_access": ["sales"], "capabilities": []}},
    )


async def test_the_labels_reach_the_assistant_in_the_tool_result(db, monkeypatch):
    from provisa.api.mcp import chat, tools

    async with db.acquire() as conn:
        await _add(conn, "accounts.md", ACCOUNTS, ["sales"])
        await _add(conn, "settlement.md", SETTLEMENT, ["finance"])
    state = await _labelled_state(db, monkeypatch)
    question = "When does a payment settle?"

    over_mcp = await tools.search_reference_library(state, "analyst", question, k=1)
    in_chat = await chat._execute_tool(
        state, "analyst", "search_reference_library", {"question": question, "k": 1}
    )
    assert in_chat == over_mcp
    # k=1: the passage nearest the question and one neighbour, within the default budget of 2
    assert [(f["ordinal"], f["reached"]) for f in in_chat] == [
        (3, "nearest the question"),
        (4, "after passage 3 in 'Accounts'"),
    ]
    assert {f["document_title"] for f in in_chat} == {"Accounts"}


# -- re-embedding (REQ-2185, RE-EMBED) ----------------------------------------------------------


def _other_vector(text: str) -> list[float]:
    return [float(len(text)), float(text.count(" ")) + 0.5, 1.0]


async def _embed_other(texts: list[str]) -> list[list[float]]:
    return [_other_vector(t) for t in texts]


OTHER = library.Embedder(model="other-embed", embed=_embed_other)


async def _unreadable(*_a: Any, **_k: Any) -> Any:
    raise AssertionError("re-embedding must not read the file or fetch the URL again")


def _no_rereading(monkeypatch) -> None:
    monkeypatch.setattr(library, "fetch", _unreadable)
    monkeypatch.setattr(extract, "read", _unreadable)


async def test_a_document_embedded_by_an_earlier_model_is_named_as_needing_re_embedding(db):
    async with db.acquire() as conn:
        old = await _add(conn, "policy.html", POLICY, ["*"])
        assert [d.id for d in await library.needing_re_embedding(conn, EMBEDDER)] == []
        assert [d.id for d in await library.needing_re_embedding(conn, OTHER)] == [old]


async def test_re_embedding_one_document_rebuilds_its_vectors_from_the_stored_text(db, monkeypatch):
    async with db.acquire() as conn:
        document_id = await _add(conn, "policy.html", POLICY, ["sales"])
        before = await store.document(conn, document_id)
        texts = [t for _, _, _, t in await store.passages(conn, document_id)]
        _no_rereading(monkeypatch)

        await library.reembed(conn, document_id, OTHER)

        after = await store.document(conn, document_id)
        assert before is not None and after is not None
        assert after.embedding_model == "other-embed"
        assert (after.title, after.status, after.domains, after.source, after.content_sha256) == (
            before.title,
            before.status,
            before.domains,
            before.source,
            before.content_sha256,
        )
        assert (after.added_at, after.refreshed_at, after.replaced_at) == (
            before.added_at,
            before.refreshed_at,
            before.replaced_at,
        )
        assert [t for _, _, _, t in await store.passages(conn, document_id)] == texts
        hits = await store.nearest_passages(conn, _other_vector(texts[0]), [document_id], 5)
        assert [h.text for h in hits][0] == texts[0] and len(hits) == len(texts)
        assert hits[0].score == pytest.approx(1.0)
        # the org's current model now draws on it without complaint
        assert await retrieve.retrieve(conn, "A Client holds an account", None, OTHER)


async def test_re_embedding_all_rebuilds_only_the_documents_of_an_earlier_model(db, monkeypatch):
    async with db.acquire() as conn:
        first = await _add(conn, "policy.html", POLICY, ["*"])
        second = await _add(conn, "spec.md", SPEC, ["*"])
        await library.reembed(conn, second, OTHER)  # already on the current model
        _no_rereading(monkeypatch)

        outcome = await library.reembed_all(conn, OTHER)

        assert outcome.reembedded == [first]
        assert outcome.failed == []
        models = {d.id: d.embedding_model for d in await store.documents(conn)}
        assert models == {first: "other-embed", second: "other-embed"}
        assert await library.needing_re_embedding(conn, OTHER) == []


async def test_a_failed_embedding_is_reported_by_document_and_leaves_that_document_as_it_was(
    db, monkeypatch
):
    async with db.acquire() as conn:
        good = await _add(conn, "spec.md", SPEC, ["*"])
        bad = await _add(conn, "policy.html", POLICY, ["*"])
        bad_before = await store.passages(conn, bad)

        async def embed(texts: list[str]) -> list[list[float]]:
            if any("Client" in t for t in texts):
                raise RuntimeError("quota exceeded")
            return [_other_vector(t) for t in texts]

        flaky = library.Embedder(model="other-embed", embed=embed)
        _no_rereading(monkeypatch)

        outcome = await library.reembed_all(conn, flaky)

        assert outcome.reembedded == [good]
        assert [(f.document_id, f.title) for f in outcome.failed] == [(bad, "Client policy")]
        assert "quota exceeded" in outcome.failed[0].reason
        kept = await store.document(conn, bad)
        assert kept is not None and kept.embedding_model == "test-embed"
        assert await store.passages(conn, bad) == bad_before
        with pytest.raises(library.EmbeddingFailed, match="'Client policy'.*quota exceeded"):
            await library.reembed(conn, bad, flaky)


async def test_re_embedding_a_missing_document_is_refused_by_name(db):
    async with db.acquire() as conn:
        with pytest.raises(library.DocumentNotFound):
            await library.reembed(conn, 999, OTHER)


def _use_embedder(monkeypatch, embedder: library.Embedder) -> None:
    async def current():
        return embedder

    monkeypatch.setattr(router, "_embedder", current)


async def test_the_admin_api_names_documents_needing_re_embedding_and_re_embeds_one(
    api, monkeypatch
):
    added = await router.add_document(_upload("policy.html", POLICY, domains=["*"]), _request())
    assert added["needs_reembedding"] is False
    _use_embedder(monkeypatch, OTHER)

    listed = await router.list_documents(_request())
    assert listed["embedding_model"] == "other-embed"
    assert [(d["id"], d["needs_reembedding"]) for d in listed["documents"]] == [(added["id"], True)]

    _no_rereading(monkeypatch)
    one = await router.reembed_document(added["id"], _request())
    assert (one["embedding_model"], one["needs_reembedding"]) == ("other-embed", False)


async def test_the_admin_api_re_embeds_all_and_reports_each_failed_document(api, monkeypatch):
    first = await router.add_document(_upload("spec.md", SPEC, domains=["*"]), _request())
    second = await router.add_document(_upload("policy.html", POLICY, domains=["*"]), _request())
    _no_rereading(monkeypatch)

    async def embed(texts: list[str]) -> list[list[float]]:
        if any("Client" in t for t in texts):
            raise RuntimeError("quota exceeded")
        return [_other_vector(t) for t in texts]

    _use_embedder(monkeypatch, library.Embedder(model="other-embed", embed=embed))
    outcome = await router.reembed_documents(_request())
    assert outcome["reembedded"] == [first["id"]]
    assert [(f["document_id"], f["title"]) for f in outcome["failed"]] == [
        (second["id"], "Client policy")
    ]
    assert "quota exceeded" in outcome["failed"][0]["reason"]

    with pytest.raises(ApiError) as failed:
        await router.reembed_document(second["id"], _request())
    assert failed.value.status_code == 502 and failed.value.code == "reference_library.embed_failed"
    with pytest.raises(ApiError) as gone:
        await router.reembed_document(999, _request())
    assert gone.value.status_code == 404


# -- the passages' vector index (REQ-2185, THE LIBRARY'S STORE AND RETRIEVAL) ---------------------


def _old_cosine(a: list[float], b: list[float]) -> float:
    """The library's search before the index: cosine similarity, in Python, over every passage."""
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return 0.0 if norm == 0 else dot / norm


def _float4(vector: list[float]) -> list[float]:
    """A vector as pgvector holds it: each component a single-precision float."""
    return list(struct.unpack(f"{len(vector)}f", struct.pack(f"{len(vector)}f", *vector)))


async def _vector_indexes(conn) -> set[str]:
    rows = await conn.execute_core(
        sa.text(
            "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() "
            "AND tablename = 'reference_passages' AND indexname LIKE 'reference_passages_vector_%'"
        )
    )
    return {r[0] for r in rows.fetchall()}


#: Stored with pgvector's single-precision floats, a similarity agrees with the double-precision
#: cosine to about float32's epsilon (1.2e-7) per operation, not to 1e-14.
FLOAT4 = 1e-6
#: Ordered by the half-precision index, a similarity agrees to half precision's 2**-11 per value.
HALF = 5e-3


async def test_the_passages_vector_is_a_pgvector_column(db):
    async with db.acquire() as conn:
        found = await conn.execute_core(
            sa.text(
                "SELECT udt_schema, udt_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = 'reference_passages' "
                "AND column_name = 'vector'"
            )
        )
        assert tuple(found.fetchone()) == ("public", "vector")


async def test_indexed_search_returns_the_passages_the_cosine_did(db):
    async with db.acquire() as conn:
        ids = [
            await _add(conn, "policy.html", POLICY, ["*"]),
            await _add(conn, "spec.md", SPEC, ["*"]),
            await _add(
                conn, "glossary.txt", b"A contract binds a client.\n\nA client pays.", ["*"]
            ),
        ]
        held = [
            (document_id, ordinal, text_)
            for document_id in ids
            for ordinal, _, _, text_ in await store.passages(conn, document_id)
        ]
        for question in ("What is a client?", "When does a payment settle?", "contract records"):
            query = _vector(question)
            expected = sorted(
                ((-_old_cosine(query, _float4(_vector(t))), d, o, t) for d, o, t in held),
            )[:4]
            hits = await store.nearest_passages(conn, query, ids, 4)
            assert [(h.document_id, h.ordinal, h.text) for h in hits] == [
                (d, o, t) for _, d, o, t in expected
            ]
            for hit, (negative, *_rest) in zip(hits, expected, strict=True):
                assert hit.score == pytest.approx(-negative, rel=FLOAT4)


async def test_the_search_is_served_by_the_dimension_s_index(db):
    async with db.acquire() as conn:
        document_id = await _add(conn, "policy.html", POLICY, ["*"])
        assert await _vector_indexes(conn) == {f"reference_passages_vector_{len(VOCABULARY)}"}
        sql, params = store.nearest_sql(_vector("client"), [document_id], 3)
        async with conn.transaction():
            await conn.execute_core(sa.text("SET LOCAL enable_seqscan = off"))
            explained = await conn.execute_core(sa.text(f"EXPLAIN {sql}").bindparams(**params))
            plan = "\n".join(r[0] for r in explained.fetchall())
        assert f"reference_passages_vector_{len(VOCABULARY)}" in plan


async def test_a_search_only_reads_the_documents_it_is_given(db):
    async with db.acquire() as conn:
        policy = await _add(conn, "policy.html", POLICY, ["*"])
        await _add(conn, "spec.md", SPEC, ["*"])
        hits = await store.nearest_passages(conn, _vector("payment currency"), [policy], 10)
        assert {h.document_id for h in hits} == {policy}
        assert await store.nearest_passages(conn, _vector("payment"), [], 10) == []


async def test_a_question_of_another_dimension_is_refused_by_name(db):
    async with db.acquire() as conn:
        document_id = await _add(conn, "policy.html", POLICY, ["*"])
        with pytest.raises(ValueError, match=r"7 dimensions; the question's have 3"):
            await store.nearest_passages(conn, [1.0, 2.0, 3.0], [document_id], 3)


async def test_re_embedding_with_another_dimension_indexes_that_dimension(db, monkeypatch):
    async with db.acquire() as conn:
        first = await _add(conn, "policy.html", POLICY, ["*"])
        second = await _add(conn, "spec.md", SPEC, ["*"])
        _no_rereading(monkeypatch)

        await library.reembed(conn, first, OTHER)
        # both dimensions are in use while the second document still has the earlier model's
        assert await _vector_indexes(conn) == {
            "reference_passages_vector_3",
            f"reference_passages_vector_{len(VOCABULARY)}",
        }
        await library.reembed(conn, second, OTHER)
        assert await _vector_indexes(conn) == {"reference_passages_vector_3"}

        texts = [t for _, _, _, t in await store.passages(conn, second)]
        hits = await store.nearest_passages(conn, _other_vector(texts[-1]), [first, second], 3)
        assert hits[0].text == texts[-1]
        assert hits[0].score == pytest.approx(1.0, rel=FLOAT4)


async def test_removing_the_last_document_of_a_dimension_drops_its_index(db):
    async with db.acquire() as conn:
        document_id = await _add(conn, "policy.html", POLICY, ["*"])
        await library.remove(conn, document_id)
        assert await _vector_indexes(conn) == set()


async def test_embeddings_beyond_the_half_precision_limit_are_refused_by_name(db):
    async def wide(texts: list[str]) -> list[list[float]]:
        return [[1.0] * 4097 for _ in texts]

    async with db.acquire() as conn:
        with pytest.raises(ValueError, match=r"4097 dimensions.*at most 4000"):
            await library.add(
                conn,
                _file("glossary.txt", b"A contract binds a client."),
                source_kind=store.FILE,
                domains=["*"],
                status=store.KEPT,
                embedder=library.Embedder(model="wide", embed=wide),
                added_by="alice",
            )
        assert await store.documents(conn) == []


WIDE = 3072


def _wide_vector(text: str) -> list[float]:
    """The vocabulary vector in the first dimensions of a 3072-dimension space, the rest zero:
    cosine between two of them is that of the narrow vectors."""
    return _vector(text) + [0.0] * (WIDE - len(VOCABULARY))


async def _embed_wide(texts: list[str]) -> list[list[float]]:
    return [_wide_vector(t) for t in texts]


async def test_a_3072_dimension_model_is_held_under_a_half_precision_index(db):
    wide = library.Embedder(model="wide-embed", embed=_embed_wide)
    async with db.acquire() as conn:
        ids = [
            await library.add(
                conn,
                _file(name, data),
                source_kind=store.FILE,
                domains=["*"],
                status=store.KEPT,
                embedder=wide,
                added_by="alice",
            )
            for name, data in (("policy.html", POLICY), ("spec.md", SPEC))
        ]
        assert await _vector_indexes(conn) == {f"reference_passages_vector_{WIDE}"}
        held = [
            (document_id, ordinal, text_)
            for document_id in ids
            for ordinal, _, _, text_ in await store.passages(conn, document_id)
        ]
        for question in ("What is a client?", "When does a payment settle?"):
            query = _wide_vector(question)
            expected = sorted((-_old_cosine(query, _wide_vector(t)), d, o, t) for d, o, t in held)[
                :3
            ]
            hits = await store.nearest_passages(conn, query, ids, 3)
            assert [(h.document_id, h.ordinal, h.text) for h in hits] == [
                (d, o, t) for _, d, o, t in expected
            ]
            for hit, (negative, *_rest) in zip(hits, expected, strict=True):
                assert hit.score == pytest.approx(-negative, rel=HALF)

        sql, params = store.nearest_sql(_wide_vector("client"), ids, 3)
        async with conn.transaction():
            await conn.execute_core(sa.text("SET LOCAL enable_seqscan = off"))
            await conn.execute_core(sa.text("SET LOCAL enable_sort = off"))
            explained = await conn.execute_core(sa.text(f"EXPLAIN {sql}").bindparams(**params))
            plan = "\n".join(r[0] for r in explained.fetchall())
        assert f"reference_passages_vector_{WIDE}" in plan
        indexed = await conn.execute_core(
            sa.text(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() "
                f"AND indexname = 'reference_passages_vector_{WIDE}'"
            )
        )
        definition = indexed.scalar()
        assert "halfvec_cosine_ops" in definition
        assert f"halfvec({WIDE})" in definition

        await library.remove(conn, ids[0])
        await library.remove(conn, ids[1])
        assert await _vector_indexes(conn) == set()


def test_the_library_refuses_to_start_without_pgvector(embedded_postgres):
    """A control plane whose role cannot create pgvector -- a managed PostgreSQL without it, or a
    role short of the right -- is refused at its layout, naming pgvector."""
    import asyncio
    import os

    import psycopg

    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.pgvector import PgvectorUnavailable
    from tests.pg_control_plane import SCHEMA_SQL

    role = f"no_pgvector_{os.getpid()}"
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{role}" WITH (FORCE)')  # type: ignore[arg-type]
        admin.execute(f'DROP ROLE IF EXISTS "{role}"')  # type: ignore[arg-type]
        admin.execute(f"CREATE ROLE \"{role}\" LOGIN NOSUPERUSER PASSWORD 'x'")  # type: ignore[arg-type]
        admin.execute(f'CREATE DATABASE "{role}" OWNER "{role}"')  # type: ignore[arg-type]
    engine = create_engine_from_url(
        f"postgresql+psycopg://{role}:x@127.0.0.1:{embedded_postgres.port}/{role}"
    )
    try:
        with pytest.raises(PgvectorUnavailable, match=r"pgvector.*permission denied"):
            asyncio.run(init_schema(Database(engine, name="layout"), SCHEMA_SQL, org_id="default"))
        with engine.connect() as c:
            assert (
                c.exec_driver_sql("SELECT to_regclass('org_default.reference_passages')").scalar()
                is None
            )
    finally:
        engine.dispose()
        with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{role}" WITH (FORCE)')  # type: ignore[arg-type]
            admin.execute(f'DROP ROLE IF EXISTS "{role}"')  # type: ignore[arg-type]
