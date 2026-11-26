# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1c9a52-7d4e-4b8a-a6e1-0c25d9b7e814
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's files (REQ-2114, amended 2026-11-25): what the person attaches in the chat, and what
she hands back for them to save.

A file reaches Polly only as bytes the person's browser sent with the chat request (the
``attachments`` of the request body, resent with every turn as its history is); it leaves only as
a download the chat panel offers. Nothing here opens, reads or writes a path: an attachment is
named by the id the chat gave it, an archive member by its name inside the archive's bytes, and a
download by a file name the browser saves under.
"""

# Requirements: REQ-2114

from __future__ import annotations

import base64
import binascii
import csv
import io
import json
import tarfile
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import yaml

#: The largest file a person may attach, and the largest download Polly may hand back, in bytes.
MAX_FILE_BYTES = 10 * 1024 * 1024
#: The most files one conversation may hold attached.
MAX_ATTACHMENTS = 5
#: The most characters one read_attachment call returns; ``offset`` reads on from there.
READ_CHARS = 50_000

CSV = "csv"
API_SPEC = "api_spec"
CODE_ARCHIVE = "code_archive"
DOCUMENT = "document"

#: The kind of attachment each file name ending names. Longest endings first, so ``.tar.gz`` is an
#: archive and not a ``.gz``.
_KINDS: tuple[tuple[str, str], ...] = (
    (".tar.gz", CODE_ARCHIVE),
    (".tgz", CODE_ARCHIVE),
    (".tar", CODE_ARCHIVE),
    (".zip", CODE_ARCHIVE),
    (".csv", CSV),
    (".tsv", CSV),
    (".json", API_SPEC),
    (".yaml", API_SPEC),
    (".yml", API_SPEC),
    (".graphql", API_SPEC),
    (".graphqls", API_SPEC),
    (".proto", API_SPEC),
    (".pdf", DOCUMENT),
    (".docx", DOCUMENT),
    (".xlsx", DOCUMENT),
    (".md", DOCUMENT),
    (".markdown", DOCUMENT),
    (".txt", DOCUMENT),
    (".html", DOCUMENT),
    (".htm", DOCUMENT),
)

#: What each file name ending is when Polly hands it back as a download.
_CONTENT_TYPES: dict[str, str] = {
    ".py": "text/x-python",
    ".ts": "text/plain",
    ".tsx": "text/plain",
    ".js": "text/javascript",
    ".java": "text/plain",
    ".go": "text/plain",
    ".rs": "text/plain",
    ".sql": "application/sql",
    ".graphql": "application/graphql",
    ".cypher": "text/plain",
    ".sparql": "application/sparql-query",
    ".proto": "text/plain",
    ".ipynb": "application/x-ipynb+json",
    ".json": "application/json",
    ".yaml": "application/x-yaml",
    ".yml": "application/x-yaml",
    ".md": "text/markdown",
    ".txt": "text/plain",
    ".html": "text/html",
    ".csv": "text/csv",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

#: The kinds of download offer_download hands back.
DOWNLOAD_KINDS = ("code", "report", "model_export", "query_result")


class FileRefused(ValueError):
    """An attachment or a download refused, said naming why."""


class FileTooLarge(FileRefused):
    """An attachment or a download over the stated size limit, or more attachments than allowed."""


@dataclass(frozen=True)
class Attachment:
    """One file the person attached: the id the chat gave it, its name, its kind and its bytes."""

    id: str
    name: str
    kind: str
    content: bytes

    def listing(self) -> dict[str, Any]:
        return {"id": self.id, "name": self.name, "kind": self.kind, "size": len(self.content)}


#: A chat turn with no files attached.
NO_ATTACHMENTS: Mapping[str, Attachment] = MappingProxyType({})


def _megabytes(n: int) -> str:
    return f"{n / (1024 * 1024):.1f} MB"


LIMIT_TEXT = (
    f"An attachment may be at most {_megabytes(MAX_FILE_BYTES)}, and a conversation may hold "
    f"{MAX_ATTACHMENTS} attachments."
)


def kind_of_name(name: str) -> str:
    """The attachment kind ``name``'s ending names; any other file is refused by name."""
    lowered = name.lower()
    for ending, kind in _KINDS:
        if lowered.endswith(ending):
            return kind
    endings = ", ".join(e for e, _ in _KINDS)
    raise FileRefused(
        f"{name!r} is not a file Polly reads: she reads a CSV, an API spec, a code archive or a "
        f"document ({endings})"
    )


def _is_bare_name(name: str) -> bool:
    return bool(name) and "/" not in name and "\\" not in name and name not in (".", "..")


def parse_attachments(value: Any) -> dict[str, Attachment]:
    """The request body's ``attachments``, by id: each ``{id, name, data}`` with ``data`` the
    file's bytes in base64. Absent is none. A file over :data:`MAX_FILE_BYTES`, more than
    :data:`MAX_ATTACHMENTS` files, a kind Polly does not read, or a malformed entry is refused by
    name (:class:`FileRefused`)."""
    if value is None:
        return {}
    if not isinstance(value, list):
        raise FileRefused("attachments must be a list of {id, name, data}")
    if len(value) > MAX_ATTACHMENTS:
        raise FileTooLarge(f"{len(value)} files are attached. {LIMIT_TEXT}")
    parsed: dict[str, Attachment] = {}
    for entry in value:
        if not isinstance(entry, dict) or not all(
            isinstance(entry.get(k), str) for k in ("id", "name", "data")
        ):
            raise FileRefused("each attachment must be {id, name, data} strings")
        att_id, name, data = entry["id"], entry["name"], entry["data"]
        if not _is_bare_name(name):
            raise FileRefused(f"{name!r} is not a file name: an attachment is named, never a path")
        if att_id in parsed:
            raise FileRefused(f"two attachments have the id {att_id!r}")
        # Refused before it is decoded: base64 holds three bytes in every four characters.
        if len(data) * 3 // 4 > MAX_FILE_BYTES + 2:
            raise FileTooLarge(
                f"{name!r} is larger than {_megabytes(MAX_FILE_BYTES)}. {LIMIT_TEXT}"
            )
        try:
            content = base64.b64decode(data, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise FileRefused(f"{name!r} is not base64: {exc}") from None
        if len(content) > MAX_FILE_BYTES:
            raise FileTooLarge(
                f"{name!r} is {_megabytes(len(content))}, larger than "
                f"{_megabytes(MAX_FILE_BYTES)}. {LIMIT_TEXT}"
            )
        parsed[att_id] = Attachment(att_id, name, kind_of_name(name), content)
    return parsed


def prompt_listing(attachments: Mapping[str, Attachment]) -> str | None:
    """The system prompt's line naming the files attached to this conversation, or None when
    there are none."""
    if not attachments:
        return None
    files = json.dumps([a.listing() for a in attachments.values()])
    return (
        "The user has attached these files in the chat; read one with read_attachment by its id: "
        f"{files}"
    )


def _text(content: bytes, name: str) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise FileRefused(f"{name!r} is not UTF-8 text: {exc}") from None


def _window(text: str, offset: int) -> dict[str, Any]:
    if offset < 0 or offset > len(text):
        raise FileRefused(f"offset {offset} is outside the text's {len(text)} characters")
    end = min(offset + READ_CHARS, len(text))
    return {
        "text": text[offset:end],
        "offset": offset,
        "total_chars": len(text),
        "next_offset": end if end < len(text) else None,
    }


def _document_text(content: bytes, name: str) -> str:
    from provisa.reference_library import extract

    try:
        document = extract.read(content, extract.media_type_of_name(name), name)
    except extract.UnreadableDocument as exc:
        raise FileRefused(str(exc)) from None
    parts = [f"# {document.title}"] if document.title else []
    for section in document.sections:
        label = section.heading or (f"Page {section.page}" if section.page is not None else None)
        if label:
            parts.append(f"## {label}")
        parts.extend(section.paragraphs)
    return "\n\n".join(parts)


def _spec_format(text: str, name: str) -> str | None:
    """What an API spec says it is: openapi, swagger or asyncapi (by its version key), graphql or
    protobuf (by its ending), or None when a JSON or YAML file names none of them."""
    lowered = name.lower()
    if lowered.endswith((".graphql", ".graphqls")):
        return "graphql"
    if lowered.endswith(".proto"):
        return "protobuf"
    try:
        doc = yaml.safe_load(text)  # JSON is YAML
    except yaml.YAMLError as exc:
        raise FileRefused(f"{name!r} is not JSON or YAML: {exc}") from None
    if isinstance(doc, dict):
        for key in ("openapi", "swagger", "asyncapi"):
            if key in doc:
                return key
    return None


def _csv_facts(text: str, name: str) -> dict[str, Any]:
    dialect = csv.excel_tab if name.lower().endswith(".tsv") else csv.excel
    rows = list(csv.reader(io.StringIO(text), dialect))
    return {"columns": rows[0] if rows else [], "row_count": max(len(rows) - 1, 0)}


def _members(content: bytes, name: str) -> list[dict[str, Any]]:
    if name.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            return [
                {"name": i.filename, "size": i.file_size} for i in zf.infolist() if not i.is_dir()
            ]
    with tarfile.open(fileobj=io.BytesIO(content)) as tf:
        return [{"name": m.name, "size": m.size} for m in tf.getmembers() if m.isfile()]


def _member(content: bytes, name: str, member: str) -> bytes:
    """One member of an archive, read from the archive's bytes. A member larger than
    :data:`MAX_FILE_BYTES` unpacked is refused, so a small archive cannot unpack without bound."""
    try:
        if name.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                info = zf.getinfo(member)
                if info.file_size > MAX_FILE_BYTES:
                    raise FileTooLarge(
                        f"{member!r} unpacks larger than {_megabytes(MAX_FILE_BYTES)}"
                    )
                with zf.open(info) as fh:
                    body = fh.read(MAX_FILE_BYTES + 1)
                if len(body) > MAX_FILE_BYTES:  # a header can understate what a member unpacks to
                    raise FileTooLarge(
                        f"{member!r} unpacks larger than {_megabytes(MAX_FILE_BYTES)}"
                    )
                return body
        with tarfile.open(fileobj=io.BytesIO(content)) as tf:
            info = tf.getmember(member)
            if not info.isfile():
                raise FileRefused(f"{member!r} in {name!r} is not a file")
            if info.size > MAX_FILE_BYTES:
                raise FileTooLarge(f"{member!r} unpacks larger than {_megabytes(MAX_FILE_BYTES)}")
            fh = tf.extractfile(info)
            assert fh is not None  # isfile() above guarantees a regular member
            return fh.read(MAX_FILE_BYTES + 1)
    except KeyError:
        raise FileRefused(
            f"{name!r} has no member {member!r}; read the attachment without a member to list them"
        ) from None


def read_attachment(
    attachments: Mapping[str, Attachment], attachment: str, member: str | None, offset: int
) -> dict[str, Any]:
    """read_attachment: one attached file, by the id the chat gave it, as text a window at a time
    from ``offset``. A CSV adds its columns and row count; an API spec what it says it is; a code
    archive lists its members, or with ``member`` reads one; a document is read by its headings
    or pages. Only attached files are read: anything else (a path included) is refused by name."""
    found = attachments.get(attachment)
    if found is None:
        attached = sorted(attachments) or "none"
        raise FileRefused(
            f"{attachment!r} is not a file attached in this chat (attached: {attached}). "
            "Polly reads only the files the user attaches; ask them to attach it."
        )
    head: dict[str, Any] = {"id": found.id, "name": found.name, "kind": found.kind}
    if found.kind == CODE_ARCHIVE:
        try:
            if member is None:
                return {**head, "members": _members(found.content, found.name)}
            body = _member(found.content, found.name, member)
        except (zipfile.BadZipFile, tarfile.TarError) as exc:
            raise FileRefused(f"{found.name!r} is not a readable archive: {exc}") from None
        return {**head, "member": member, **_window(_text(body, member), offset)}
    if member is not None:
        raise FileRefused(f"{found.name!r} is not an archive; it has no members")
    if found.kind == DOCUMENT:
        return {**head, **_window(_document_text(found.content, found.name), offset)}
    text = _text(found.content, found.name)
    if found.kind == CSV:
        return {**head, **_csv_facts(text, found.name), **_window(text, offset)}
    return {**head, "format": _spec_format(text, found.name), **_window(text, offset)}


def content_type_of_name(filename: str) -> str:
    """The media type a download named ``filename`` is saved as; an ending with none is refused."""
    if not _is_bare_name(filename):
        raise FileRefused(f"{filename!r} is not a file name: a download is named, never a path")
    dot = filename.rfind(".")
    content_type = _CONTENT_TYPES.get(filename[dot:].lower()) if dot > 0 else None
    if content_type is None:
        raise FileRefused(
            f"{filename!r} has no ending Polly hands back; use one of {', '.join(_CONTENT_TYPES)}"
        )
    return content_type


def download(filename: str, content: bytes) -> dict[str, Any]:
    """A tool result handing ``content`` back as a download named ``filename``: the model gets
    what was handed back, the chat panel the bytes (the ``artifact``, never in the model's
    context). Larger than :data:`MAX_FILE_BYTES` is refused."""
    content_type = content_type_of_name(filename)
    if len(content) > MAX_FILE_BYTES:
        raise FileTooLarge(
            f"{filename!r} would be {_megabytes(len(content))}, larger than the "
            f"{_megabytes(MAX_FILE_BYTES)} a download may be"
        )
    return {
        "filename": filename,
        "content_type": content_type,
        "size": len(content),
        "shown": "The user has a download for this file in the chat.",
        "artifact": {
            "kind": "download",
            "filename": filename,
            "content_type": content_type,
            "size": len(content),
            "data": base64.b64encode(content).decode("ascii"),
        },
    }


def csv_bytes(columns: list[str], rows: list[dict[str, Any]]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(["" if row[c] is None else row[c] for c in columns])
    return out.getvalue().encode("utf-8")


def _cell(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, default=str)


def xlsx_bytes(columns: list[str], rows: list[dict[str, Any]]) -> bytes:
    """A workbook of one sheet, written in memory: XlsxWriter's in_memory mode opens no
    temporary file (openpyxl writes each worksheet through one)."""
    import xlsxwriter

    out = io.BytesIO()
    wb = xlsxwriter.Workbook(out, {"in_memory": True})
    ws = wb.add_worksheet()
    ws.write_row(0, 0, columns)
    for r, row in enumerate(rows, start=1):
        ws.write_row(r, 0, [_cell(row[c]) for c in columns])
    wb.close()
    return out.getvalue()
