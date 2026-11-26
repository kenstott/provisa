# Copyright (c) 2026 Kenneth Stott
# Canary: be758c37-dbff-4f61-802a-05a8fac1c5cb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The local result sink: a large result written to a file in the org's folder and served over
HTTP to the user who asked for it (REQ-1191, REQ-1192, REQ-1193, REQ-1195).

A delivery that no object store takes is written here (``redirect.run_materialize`` chooses):
in ``<org folder>/results/<name>``, by the engine itself where it writes the format to a file
(DuckDB's ``COPY ... TO``, Postgres's ``COPY ... TO STDOUT``), otherwise by Provisa from the
result's rows. Beside each result is its record, ``<name>.owner``: the user who asked for it and
its media type. ``GET /data/redirect-file/<name>`` serves it to that user alone, checked from
the request's own identity on every fetch. A result older than ``redirect.ttl`` is gone: refused
when fetched, and deleted -- at its expiry by a timer, as the object store's results are
(``redirect.schedule_s3_cleanup``), and by the sweep each new result makes of the org's results,
which takes the ones whose timer a restart ended."""

# Requirements: REQ-1191, REQ-1192, REQ-1193, REQ-1195

from __future__ import annotations

import contextlib
import json
import logging
import re
import time
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger(__name__)

#: The org-folder directory results are written in (REQ-1992).
RESULTS_DIR = "results"
#: The path a result is served at; its name follows.
SERVE_PATH = "/data/redirect-file/"
_OWNER_SUFFIX = ".owner"

#: The formats a result file is written in, with each one's media type and file extension.
FILE_FORMATS: dict[str, tuple[str, str]] = {
    "parquet": ("application/vnd.apache.parquet", ".parquet"),
    "orc": ("application/x-orc", ".orc"),
    "csv": ("text/csv", ".csv"),
    "arrow": ("application/vnd.apache.arrow.stream", ".arrow"),
    "json": ("application/json", ".json"),
    "ndjson": ("application/x-ndjson", ".ndjson"),
}

#: A result's name: a random id and its format's extension. Nothing else is served.
_NAME = re.compile(r"^[0-9a-f]{32}\.(parquet|orc|csv|arrow|json|ndjson)$")


class ResultFormatNotWritable(ValueError):
    """A result was asked for in a format no result file is written in."""

    def __init__(self, output_format: str) -> None:
        super().__init__(
            f"a result file is not written as {output_format!r}; it is written as one of "
            f"{', '.join(sorted(FILE_FORMATS))}"
        )


class ResultAddressUnknown(RuntimeError):
    """A result was to be written to a file with no address to serve it at."""

    def __init__(self) -> None:
        super().__init__(
            "a result kept on this server is served at the address its request came in on, and "
            "this request came in over no HTTP; set server.public_url to the address HTTP "
            "clients reach this server at"
        )


class ResultOwnerUnknown(RuntimeError):
    """A result was to be written to a file with no user to serve it to."""


class ResultFileNotFound(LookupError):
    """No result of that name is held for the org, or it is past its time."""


class NotResultOwner(PermissionError):
    """A user asked for a result another user asked for."""


#: The base URI of the HTTP request being served (scheme, host, port and root path), bound by the
#: HTTP middleware for every request; None on a request that came in over no HTTP.
_request_base: ContextVar[str | None] = ContextVar("result_request_base", default=None)


@contextlib.contextmanager
def bound_request_base(base_uri: str) -> Iterator[None]:
    """Name the base URI of the HTTP request being served, for the enclosed work."""
    token = _request_base.set(base_uri.rstrip("/"))
    try:
        yield
    finally:
        _request_base.reset(token)


def serve_base() -> str:
    """The base a result kept here is served under: the HTTP request's own base URI, so the link
    reaches this server the way the caller did (REQ-1191). A request on a protocol that is not
    HTTP (pgwire, Flight SQL, Bolt, gRPC) has no base URI of its own; its link names the address
    HTTP clients reach this server at, ``server.public_url`` (REQ-1191, REQ-2135), and with that
    unset the result is refused by name (:class:`ResultAddressUnknown`)."""
    bound = _request_base.get()
    if bound is not None:
        return bound
    from provisa.core import settings_registry

    public = settings_registry.value("server.public_url")
    if not public:
        raise ResultAddressUnknown()
    return public.rstrip("/")


def results_dir() -> Path:
    """The bound org's results directory (REQ-1992), made if it is not there."""
    from provisa.core.org_files import org_path

    return org_path(RESULTS_DIR)


def new_result_path(output_format: str) -> Path:
    """A fresh path in the bound org's results directory for a result in ``output_format``."""
    fmt = output_format.lower()
    if fmt not in FILE_FORMATS:
        raise ResultFormatNotWritable(output_format)
    return results_dir() / f"{uuid.uuid4().hex}{FILE_FORMATS[fmt][1]}"


def current_owner() -> str:
    """The user the request runs as: the principal every caller-facing surface binds (the audit
    identity), which the fetch compares with its own request's."""
    from provisa.audit.context import current_audit_identity

    identity = current_audit_identity()
    if identity is None:
        raise ResultOwnerUnknown(
            "a result is written to a file only for a request with a user to serve it to"
        )
    return identity.user_id


def record_owner(path: Path, owner: str, content_type: str, row_count: int) -> None:
    """Write ``path``'s record: who may fetch it, and what it is."""
    record = {"owner": owner, "content_type": content_type, "row_count": row_count}
    path.with_name(path.name + _OWNER_SUFFIX).write_text(json.dumps(record))


def keep(path: Path, owner: str, base: str, content_type: str, row_count: int, ttl: int) -> dict:
    """Record a written result for its owner, have it deleted after ``ttl``, sweep the org's
    expired ones, and return its delivery handle: the absolute URL it is served at (``base``,
    :func:`serve_base`), its rows, how long it is kept and its media type -- the object store's
    handle, with ``sink: local``."""
    record_owner(path, owner, content_type, row_count)
    schedule_removal(path, ttl)
    reap_expired(path.parent, ttl)
    return {
        "sink": "local",
        "redirect_url": f"{base}{SERVE_PATH}{path.name}",
        "row_count": row_count,
        "expires_in": ttl,
        "content_type": content_type,
    }


def store_bytes(
    body: bytes, output_format: str, row_count: int, ttl: int, *, content_type: str | None = None
) -> dict:
    """A result Provisa has already serialized, kept in the bound org's folder for the user the
    request runs as. ``content_type``: the body's own when it is not the format's (an encrypted
    payload, REQ-687)."""
    owner = current_owner()
    base = serve_base()
    path = new_result_path(output_format)
    path.write_bytes(body)
    return keep(
        path, owner, base, content_type or FILE_FORMATS[output_format.lower()][0], row_count, ttl
    )


def result_table(state: Any, physical_sql: str, params: list | None):  # -> pa.Table
    """Run the statement on the bound engine and return its rows as an Arrow table: the result
    of an engine that does not write the asked-for format itself."""
    import pyarrow as pa

    stream = state.federation_engine.execute_engine_sync(physical_sql, params)
    names = list(stream.column_names)
    rows = stream.rows
    return pa.table({name: [row[i] for row in rows] for i, name in enumerate(names)})


def serialize_table(table, output_format: str) -> bytes:
    """``table`` (an Arrow table) as the bytes of a result file in ``output_format``."""
    fmt = output_format.lower()
    if fmt not in FILE_FORMATS:
        raise ResultFormatNotWritable(output_format)
    if fmt == "orc":
        import io

        import pyarrow.orc as orc

        buf = io.BytesIO()
        orc.write_table(table, buf)
        return buf.getvalue()
    from provisa.executor.redirect import _serialize_arrow_table

    return _serialize_arrow_table(table, fmt)[0]


def write_from_rows(
    state: Any, physical_sql: str, output_format: str, params: list | None, path: Path
) -> int:
    """Run the statement and write its rows to ``path`` in ``output_format``: the result file of
    an engine that does not write one itself. Returns the rows written."""
    table = result_table(state, physical_sql, params)
    path.write_bytes(serialize_table(table, output_format))
    return table.num_rows


def write(state: Any, physical_sql: str, output_format: str, params: list | None, ttl: int) -> dict:
    """The local tier of ``redirect.run_materialize``: write the statement's result to a file in
    the bound org's folder and return its delivery handle. The engine writes it where it writes
    the format to a file; Provisa writes it from the rows otherwise."""
    owner = current_owner()
    base = serve_base()
    path = new_result_path(output_format)
    fmt = output_format.lower()
    engine = state.federation_engine
    try:
        if engine.writes_result_file(fmt):
            rows = engine.write_result_file(physical_sql, fmt, params, str(path))
        else:
            rows = write_from_rows(state, physical_sql, fmt, params, path)
    except BaseException:
        remove(path)
        raise
    return keep(path, owner, base, FILE_FORMATS[fmt][0], rows, ttl)


def _expired(path: Path, ttl: int, now: float) -> bool:
    return now - path.stat().st_mtime > ttl


def open_for(name: str, user_id: str, ttl: int) -> tuple[Path, str]:
    """The bound org's result ``name`` and its media type, for ``user_id``: refused when there is
    no such result or it is past ``ttl`` (:class:`ResultFileNotFound`), and when another user
    asked for it (:class:`NotResultOwner`)."""
    if not _NAME.match(name):
        raise ResultFileNotFound(name)
    path = results_dir() / name
    record_path = path.with_name(name + _OWNER_SUFFIX)
    if not path.is_file() or not record_path.is_file() or _expired(path, ttl, time.time()):
        raise ResultFileNotFound(name)
    record = json.loads(record_path.read_text())
    if record["owner"] != user_id:
        raise NotResultOwner(name)
    return path, record["content_type"]


def remove(path: Path) -> None:
    """Delete a result and its record, whichever of them is there."""
    for each in (path, path.with_name(path.name + _OWNER_SUFFIX)):
        each.unlink(missing_ok=True)


def reap_expired(directory: Path, ttl: int, now: float | None = None) -> int:
    """Delete every result in ``directory`` older than ``ttl`` seconds, with its record, and every
    record whose result is gone; the number of results deleted (REQ-1193)."""
    at = time.time() if now is None else now
    reaped = 0
    for path in directory.iterdir():
        if path.name.endswith(_OWNER_SUFFIX):
            if not path.with_name(path.name[: -len(_OWNER_SUFFIX)]).exists():
                path.unlink(missing_ok=True)
            continue
        if not _NAME.match(path.name):
            continue
        try:
            expired = _expired(path, ttl, at)
        except FileNotFoundError:
            continue  # its own timer deleted it since the directory was listed
        if expired:
            remove(path)
            reaped += 1
    if reaped:
        log.info("[REDIRECT CLEANUP] deleted %d expired result files in %s", reaped, directory)
    return reaped


def schedule_removal(path: Path, ttl: int) -> None:
    """Delete the result once its ``ttl`` has passed (REQ-1193), as the object store's results
    are (``redirect.schedule_s3_cleanup``): the delay is held by the background timer thread."""
    from provisa.core.connection_loop import spawn_after

    async def _remove() -> None:
        remove(path)

    spawn_after(ttl, _remove(), name=f"result-file-cleanup:{path.name}")
