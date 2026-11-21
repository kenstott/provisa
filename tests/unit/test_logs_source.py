# Copyright (c) 2026 Kenneth Stott
# Canary: 868b1e7c-ba97-4c8e-a230-7687584635f2
#
# This source code is licensed under the Business Source License 1.1
"""REQ-1996: a Logs source -- where its files may be read, how a file is known across its
rotations, what of it each pass reads, and its rows as a replica kept in place. The syslog
lines are macOS lines with the host renamed."""

from __future__ import annotations

import bz2
import gzip
import json
import os
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import secrets_providers
from provisa.federation.mail_units import Part, ReplicaUnitJob
from provisa.logs import ENTRY, FILE_ID, LANDED_AT, LINE_OFFSET, PARSE_ERROR, RAW
from provisa.logs.files import FileState, Table, identify, matched, read_file
from provisa.logs.loader import make_logs_loader
from provisa.logs.roots import LogPathRefused, checked_file, checked_glob
from provisa.logs.settings import InvalidLogsSource, parse
from provisa.logparse import ParseError, format_for
from provisa.logparse.formats import JsonLines

LINES = [
    "Oct 12 00:09:29 host-a syslogd[603]: ASL Sender Statistics",
    "Oct 12 00:19:29 host-a syslogd[603]: ASL Sender Statistics",
    "Oct 12 00:29:46 host-a kernel[0]: AppleALC: found codec",
]
SYSLOG_COLUMNS = {
    FILE_ID: "text",
    "_file_name": "text",
    LINE_OFFSET: "bigint",
    ENTRY: "bigint",
    RAW: "text",
    PARSE_ERROR: "text",
    "timestamp": "timestamp",
    "host": "text",
    "process": "text",
    "pid": "bigint",
    "message": "text",
    LANDED_AT: "timestamp",
}


@pytest.fixture
def root(tmp_path, monkeypatch):
    """A directory log files may be read from, on a server of one organization."""
    logs = tmp_path / "logs"
    logs.mkdir()
    monkeypatch.setenv("PROVISA_LOGS_ALLOWED_ROOTS", str(logs))
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    return logs


def _write(path, lines, *, end="\n", written_ago=60.0):
    """``lines`` as the file, last written ``written_ago`` seconds ago: a log not written to
    lately, unless the test says otherwise."""
    path.write_text("\n".join(lines) + end)
    _written(path, written_ago)


def _written(path, ago=60.0):
    at = time.time() - ago
    os.utime(path, (at, at))


def _table(columns=None, max_line_bytes=65536):
    return Table("syslog", dict(columns or SYSLOG_COLUMNS), max_line_bytes)


def _read_all(glob, kept, fmt=None, table=None):
    """One pass: every file's new rows, and what is kept of each after it."""
    files, gone = identify(matched(glob), kept)
    rows = []
    for found in files:
        for batch in read_file(found, fmt or format_for("rfc3164"), table or _table(), 2):
            rows.extend(batch)
    return rows, {f.state.id: FileState.of(f.state.token()) for f in files}, gone


# --- where files may be read ------------------------------------------------------------------


def test_a_glob_inside_an_allowed_root_is_read_and_one_outside_is_refused_by_name(root, tmp_path):
    assert checked_glob(f"{root}/system.log*") == f"{root}/system.log*"
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    with pytest.raises(LogPathRefused, match=f"{outside}/\\*.log resolves to {outside}, outside"):
        checked_glob(f"{outside}/*.log")
    with pytest.raises(LogPathRefused, match="full path"):
        checked_glob("system.log*")


def test_a_symlink_escaping_the_root_is_refused_by_name(root, tmp_path):
    secret = tmp_path / "secret.log"
    _write(secret, LINES)
    (root / "innocent.log").symlink_to(secret)
    with pytest.raises(LogPathRefused, match=f"innocent.log resolves to {secret}, outside"):
        matched(f"{root}/*.log")
    # A directory link out of the root is refused at the glob itself.
    (root / "away").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(LogPathRefused, match="away/\\*.log resolves to"):
        checked_glob(f"{root}/away/*.log")


@pytest.fixture
def many(monkeypatch):
    """A server of many organizations whose deployment's own org is ``default``; ``acme`` is
    bound, an org it holds (REQ-1997)."""
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.security import sandbox_egress

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: "default")
    token = set_current_org("acme")
    yield
    reset_current_org(token)


def test_a_held_org_on_a_server_of_many_reads_no_log_file_of_the_servers_own(root, many):
    with pytest.raises(LogPathRefused, match="outside its own folder"):
        checked_glob(f"{root}/system.log*")
    with pytest.raises(LogPathRefused, match="outside its own folder"):
        checked_file(str(root / "system.log"))


def test_a_demo_bundles_seeding_run_reads_its_demos_logs_inside_the_allowed_roots_only(
    root, many, monkeypatch, tmp_path
):
    """REQ-2049: the bundle build's throwaway sandbox tier lands a demo's Logs sources from the
    files its traffic wrote; the roots still bound it, and no held org outside a seeding run
    reads a log file of the server's own."""
    monkeypatch.setenv("PROVISA_DEMO_SEED_EXPORT", "true")
    assert checked_glob(f"{root}/system.log*") == f"{root}/system.log*"
    (root / "system.log").write_text("x\n")
    assert checked_file(str(root / "system.log")) == os.path.realpath(root / "system.log")
    with pytest.raises(LogPathRefused, match="outside the directories log files may be read"):
        checked_glob(f"{tmp_path}/*.log")
    monkeypatch.setenv("PROVISA_DEMO_SEED_EXPORT", "false")
    with pytest.raises(LogPathRefused, match="outside its own folder"):
        checked_glob(f"{root}/system.log*")


def test_a_sandbox_tier_demo_org_reads_the_logs_its_demo_declares_and_nothing_else(
    root, monkeypatch
):
    """REQ-2049, REQ-1997: a demo's logs are its own source, read live from its bundle -- by its
    template and its sandboxes, the files its Logs source's glob names and nothing else, not
    even the rest of the demo's directory; any other held org of a many-organization server
    still reads no log file of the server's own."""
    from provisa.core.request_context import current_org
    from provisa.security import sandbox_egress

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: "default")
    monkeypatch.setenv("PROVISA_DEMO_SEED_EXPORT", "false")
    demo = root / "demo-bundles" / "weblogs" / "202610141747-0a3ad1932"
    (demo / "logs").mkdir(parents=True)
    (demo / "logs" / "access.json").write_text("{}\n")
    (demo / "logs" / "notes.txt").write_text("x\n")
    (root / "demo-bundles" / "other" / "1" / "logs").mkdir(parents=True)
    declared = {"type": "logs", "mapping": {"glob": f"{demo}/logs/*.json", "format": "jsonl"}}

    for org in ("sbx0123456789ab", "sbxtplweblogs"):
        sandbox_egress.set_demo_destinations(org, [declared])
        token = current_org.set(org)
        try:
            assert checked_glob(f"{demo}/logs/*.json") == f"{demo}/logs/*.json"
            assert checked_file(str(demo / "logs" / "access.json"))
            with pytest.raises(LogPathRefused, match="outside its own folder"):
                checked_file(str(demo / "logs" / "notes.txt"))
            with pytest.raises(LogPathRefused, match="outside its own folder"):
                checked_glob(f"{demo}/logs/*")
            with pytest.raises(LogPathRefused, match="outside its own folder"):
                checked_glob(f"{root}/demo-bundles/other/1/logs/*.json")
        finally:
            current_org.reset(token)
            sandbox_egress.forget(org)
    token = current_org.set("acme")
    try:
        with pytest.raises(LogPathRefused, match="outside its own folder"):
            checked_glob(f"{demo}/logs/*.json")
    finally:
        current_org.reset(token)


def test_the_save_refuses_a_logs_source_by_name_and_on_the_saas_plane(root, monkeypatch, tmp_path):
    from provisa.api.admin.schema_common import logs_refusal

    def refusal(mapping):
        source: Any = SimpleNamespace(id="syslog", type="logs", mapping_json=json.dumps(mapping))
        return logs_refusal(source)

    assert refusal({"glob": f"{root}/system.log*", "format": "rfc3164"}) is None
    refused = refusal({"glob": f"{tmp_path}/*.log", "format": "rfc3164"})
    assert refused is not None and refused.code == "schema.logs_path_refused"
    assert str(tmp_path) in refused.params["reason"]
    invalid = refusal({"glob": f"{root}/x"})
    assert invalid is not None and invalid.code == "schema.logs_invalid"
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.security import sandbox_egress

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: "default")
    token = set_current_org("acme")
    try:
        saas = refusal({"glob": f"{root}/system.log*", "format": "rfc3164"})
    finally:
        reset_current_org(token)
    assert saas is not None and saas.code == "schema.logs_path_refused"
    assert "outside its own folder" in saas.params["reason"]


def test_the_settings_name_a_glob_and_a_format():
    assert (
        parse({"glob": "/var/log/x", "format": "regex", "pattern": "(?P<a>.*)"}).pattern
        == "(?P<a>.*)"
    )
    with pytest.raises(InvalidLogsSource, match="name the files"):
        parse({"format": "jsonl"})
    with pytest.raises(InvalidLogsSource, match="no log format named"):
        parse({"glob": "/var/log/x", "format": "nope"})
    with pytest.raises(InvalidLogsSource, match="no setting colour"):
        parse({"glob": "/var/log/x", "format": "jsonl", "colour": "red"})


def test_a_glob_naming_a_variable_reads_where_it_points(root, monkeypatch):
    """REQ-1919: a source's mapping keeps the reference as written (a config's
    ``${env:LOG_DIR}/*.log``); the glob is resolved where the settings are read, so the files read
    are the ones the variable names, inside the allowed root like any other."""
    monkeypatch.setenv("WEBLOGS_TEST_LOG_DIR", str(root))
    _write(root / "system.log", LINES)
    settings = parse({"glob": "${env:WEBLOGS_TEST_LOG_DIR}/system.log*", "format": "rfc3164"})
    assert settings.glob == f"{root}/system.log*"
    rows, _, _ = _read_all(checked_glob(settings.glob), {})
    assert [r[RAW] for r in rows] == LINES


def test_a_glob_naming_an_unset_variable_is_refused_naming_it(monkeypatch):
    monkeypatch.delenv("WEBLOGS_TEST_UNSET", raising=False)
    with pytest.raises(InvalidLogsSource, match="WEBLOGS_TEST_UNSET"):
        parse({"glob": "${env:WEBLOGS_TEST_UNSET}/app.log", "format": "jsonl"})


def test_a_pattern_is_read_as_written_not_as_a_reference():
    pattern = r"(?P<a>\$\{x:y\}.*)"
    assert parse({"glob": "/var/log/x", "format": "regex", "pattern": pattern}).pattern == pattern


# --- a file across its rotations ------------------------------------------------------------------


def test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice(root):
    live = root / "system.log"
    _write(live, LINES[:2])
    rows, kept, _gone = _read_all(f"{root}/system.log*", {})
    assert [r["message"] for r in rows] == ["ASL Sender Statistics"] * 2
    first_id = rows[0][FILE_ID]
    # More is written; the file is rotated by rename and compressed, and a new one started.
    with open(live, "a") as f:
        f.write(LINES[2] + "\n")
    with open(live, "rb") as f, gzip.open(root / "system.log.0.gz", "wb") as z:
        z.write(f.read())
    live.unlink()
    _write(live, ["Oct 13 00:00:01 host-a syslogd[603]: restarted"])
    rows, kept, gone = _read_all(f"{root}/system.log*", kept)
    assert gone == []
    by_file = {(r[FILE_ID], r[LINE_OFFSET]) for r in rows}
    assert len(by_file) == len(rows) == 2
    assert [r["message"] for r in rows if r[FILE_ID] == first_id] == ["AppleALC: found codec"]
    assert [r["message"] for r in rows if r[FILE_ID] != first_id] == ["restarted"]
    # Nothing new: nothing is read.
    rows, kept, _gone = _read_all(f"{root}/system.log*", kept)
    assert rows == []


def test_a_rotated_copy_beside_its_compressed_one_is_read_once(root):
    _write(root / "app.log.0", LINES)
    with bz2.open(root / "app.log.0.bz2", "wb") as z:
        z.write(("\n".join(LINES) + "\n").encode())
    rows, kept, _gone = _read_all(f"{root}/app.log*", {})
    assert len(rows) == 3 and len(kept) == 1
    assert {r["_file_name"] for r in rows} == {str(root / "app.log.0.bz2")}


def test_a_truncated_file_is_a_new_file_read_from_its_start(root):
    live = root / "app.log"
    _write(live, LINES)
    rows, kept, _gone = _read_all(f"{root}/app.log", {})
    first_id = rows[0][FILE_ID]
    live.write_text(LINES[0] + "\n")  # truncated in place, the same first line written again
    _written(live)
    rows, kept, gone = _read_all(f"{root}/app.log", kept)
    assert [r[LINE_OFFSET] for r in rows] == [0]
    assert rows[0][FILE_ID] != first_id and gone == [first_id]


def test_two_short_files_that_start_alike_are_two_files(root):
    _write(root / "a.log", [LINES[0]])
    rows, kept, _gone = _read_all(f"{root}/*.log", {})
    _write(root / "b.log", [LINES[0], LINES[1]])
    rows, kept, _gone = _read_all(f"{root}/*.log", kept)
    assert [r["_file_name"] for r in rows] == [str(root / "b.log")] * 2


def test_a_growing_file_s_last_line_waits_for_its_end(root):
    live = root / "app.log"
    _write(live, LINES[:2], end="")  # the second line not yet ended
    rows, kept, _gone = _read_all(f"{root}/app.log", {})
    assert len(rows) == 1
    with open(live, "a") as f:
        f.write(" more\n")
    _written(live)
    rows, kept, _gone = _read_all(f"{root}/app.log", kept)
    assert [r["message"] for r in rows] == ["ASL Sender Statistics more"]


def test_a_record_spanning_lines_waits_for_the_next_record_to_start(root):
    live = root / "app.log"
    # Being written now: its last record may go on.
    _write(
        live,
        ["2026-10-12 12:00:00 ERROR boom", "Traceback (most recent call last):"],
        written_ago=0,
    )
    fmt = format_for("regex", r"(?P<when>\S+ \S+) (?P<level>\w+) (?P<message>.*)", r"\d{4}-")
    table = _table(
        {
            FILE_ID: "text",
            "_file_name": "text",
            LINE_OFFSET: "bigint",
            RAW: "text",
            PARSE_ERROR: "text",
            "when": "text",
            "level": "text",
            "message": "text",
        }
    )
    rows, kept, _gone = _read_all(f"{root}/app.log", {}, fmt, table)
    assert rows == []  # the trace may go on
    with open(live, "a") as f:
        f.write('  File "x.py", line 1\n2026-10-12 12:00:01 INFO fine\n')
    rows, kept, _gone = _read_all(f"{root}/app.log", kept, fmt, table)
    assert [r["level"] for r in rows] == ["ERROR"]
    assert rows[0]["message"].endswith('File "x.py", line 1') and rows[0][LINE_OFFSET] == 0
    # Once the file goes quiet, the record it ends with is whole.
    _written(live)
    rows, kept, _gone = _read_all(f"{root}/app.log", kept, fmt, table)
    assert [r["level"] for r in rows] == ["INFO"]


def test_a_w3c_file_read_on_keeps_its_header_s_fields(root):
    live = root / "u_ex261012.log"
    _write(live, ["#Fields: date time c-ip sc-status", "2026-10-12 12:00:01 192.0.2.7 200"])
    table = _table(
        {
            FILE_ID: "text",
            "_file_name": "text",
            LINE_OFFSET: "bigint",
            RAW: "text",
            PARSE_ERROR: "text",
            "timestamp": "timestamp",
            "c_ip": "text",
            "sc_status": "bigint",
            "extra": "json",
        }
    )
    rows, kept, _gone = _read_all(f"{root}/*.log", {}, format_for("w3c"), table)
    assert [r["sc_status"] for r in rows] == [200]
    with open(live, "a") as f:
        f.write("2026-10-12 12:00:02 192.0.2.8 404\n")
    rows, kept, _gone = _read_all(f"{root}/*.log", kept, format_for("w3c"), table)
    assert [(r["c_ip"], r["sc_status"], r[PARSE_ERROR]) for r in rows] == [("192.0.2.8", 404, None)]


# --- rows -------------------------------------------------------------------------------------


def test_a_line_the_format_cannot_read_or_a_value_its_column_cannot_hold_is_kept_saying_why(root):
    _write(root / "app.log", ["not syslog at all", LINES[0]])
    table = _table({**SYSLOG_COLUMNS, "host": "bigint"})
    rows, _kept, _gone = _read_all(f"{root}/app.log", {}, table=table)
    assert rows[0][RAW] == "not syslog at all" and "BSD syslog" in rows[0][PARSE_ERROR]
    assert rows[1]["host"] is None and rows[1][PARSE_ERROR].startswith("host:")
    assert rows[1]["timestamp"].tzinfo is None  # landed as UTC


class _Batched(JsonLines):
    """A record that is a batch of entries, as a CloudTrail file is: a row per entry."""

    def parse_many(self, record, ctx):
        value = self.fields(record)
        if "Records" not in value:
            raise ParseError("no Records")
        return list(value["Records"])


def test_a_record_of_several_entries_is_a_row_per_entry_each_with_its_own_key(root):
    _write(
        root / "trail.json",
        [
            json.dumps({"Records": [{"eventName": "GetObject"}, {"eventName": "PutObject"}]}),
            json.dumps({"Records": []}),
            json.dumps({"nothing": 1}),
        ],
    )
    table = _table(
        {
            FILE_ID: "text",
            "_file_name": "text",
            LINE_OFFSET: "bigint",
            ENTRY: "bigint",
            RAW: "text",
            PARSE_ERROR: "text",
            "eventName": "text",
        }
    )
    rows, _kept, _gone = _read_all(f"{root}/trail.json", {}, _Batched(), table)
    assert [(r[LINE_OFFSET], r[ENTRY], r.get("eventName"), r[PARSE_ERROR]) for r in rows] == [
        (0, 0, "GetObject", None),
        (0, 1, "PutObject", None),
        (rows[2][LINE_OFFSET], 0, None, "no Records"),  # an empty batch has no rows
    ]
    assert rows[0][RAW] == rows[1][RAW]


def test_a_record_longer_than_the_limit_keeps_its_first_bytes(root):
    _write(root / "app.log", [LINES[0] + " " + "x" * 400])
    rows, _kept, _gone = _read_all(f"{root}/app.log", {}, table=_table(max_line_bytes=300))
    assert len(rows[0][RAW]) == 300 and "longer than the 300" in rows[0][PARSE_ERROR]


# --- the replica, kept in place ------------------------------------------------------------------


class _StateStore:
    """The organisation's state store as the loader uses it: what each file was read to."""

    def __init__(self):
        self.kept: dict[str, dict[str, str]] = {}

    @asynccontextmanager
    async def acquire(self):
        yield self


@pytest.fixture
def kept_state(monkeypatch):
    store = _StateStore()
    from provisa.federation import mail_sync_state

    async def links_of(_conn, _source_id):
        return {a: dict(s) for a, s in store.kept.items()}

    async def keep_links(_conn, _source_id, account, links, *, kind=mail_sync_state.DELTA):
        store.kept.setdefault(account, {}).update(links)

    async def forget_account(_conn, _source_id, account):
        store.kept.pop(account, None)

    monkeypatch.setattr(mail_sync_state, "links_of", links_of)
    monkeypatch.setattr(mail_sync_state, "keep_links", keep_links)
    monkeypatch.setattr(mail_sync_state, "forget_account", forget_account)
    return store


async def _pass(loader, source, landed):
    columns = list(SYSLOG_COLUMNS.items())
    reader = loader.replica_unit_source(source, ("syslog",), {"syslog": columns})
    part = Part(
        "logs", "syslog", columns, loader.replica_key_columns("syslog"), loader.replica_stamp_column
    )

    async def apply(group):
        for changes in group:
            for event in changes.events:
                landed[tuple(event.row[k] for k in changes.pk_columns)] = event.row

    done = await ReplicaUnitJob(reader, {"syslog": part}, apply).run()
    return reader, done


async def test_each_pass_lands_only_new_lines_keyed_by_file_and_offset(root, kept_state):
    loader = make_logs_loader(SimpleNamespace(tenant_db=kept_state))
    source = SimpleNamespace(
        id="syslog", mapping={"glob": f"{root}/system.log*", "format": "rfc3164"}
    )
    live = root / "system.log"
    _write(live, LINES[:2])
    landed: dict = {}
    reader, done = await _pass(loader, source, landed)
    assert len(landed) == 2 and reader.total == 1 and done.mailboxes == 1
    assert reader.unit == "files"
    assert all(row[LANDED_AT] is not None for row in landed.values())
    with open(live, "rb") as f, gzip.open(root / "system.log.0.gz", "wb") as z:
        z.write(f.read() + (LINES[2] + "\n").encode())
    live.unlink()
    await _pass(loader, source, landed)
    assert sorted(row["message"] for row in landed.values()) == [
        "ASL Sender Statistics",
        "ASL Sender Statistics",
        "AppleALC: found codec",
    ]
    # The rotated copy is the same file: one file is kept, read to its end.
    (file_id,) = kept_state.kept
    assert FileState.of(kept_state.kept[file_id][""]).offset == len(
        ("\n".join(LINES) + "\n").encode()
    )


async def test_a_file_no_longer_matched_is_forgotten_and_its_rows_stay(root, kept_state):
    loader = make_logs_loader(SimpleNamespace(tenant_db=kept_state))
    source = SimpleNamespace(
        id="syslog", mapping={"glob": f"{root}/system.log*", "format": "rfc3164"}
    )
    _write(root / "system.log", LINES)
    landed: dict = {}
    await _pass(loader, source, landed)
    (root / "system.log").unlink()
    await _pass(loader, source, landed)
    assert kept_state.kept == {} and len(landed) == 3


async def test_the_files_token_changes_when_a_file_is_written(root):
    from provisa.logs.loader import files_token

    source = SimpleNamespace(id="syslog", mapping={"glob": f"{root}/*.log", "format": "rfc3164"})
    _write(root / "a.log", LINES[:1])
    before = files_token(source)
    assert files_token(source) == before
    with open(root / "a.log", "a") as f:
        f.write(LINES[1] + "\n")
    assert files_token(source) != before


# --- encodings and binary formats ----------------------------------------------------------------


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be", "utf-8-sig"])
def test_a_file_in_another_encoding_is_read_by_its_byte_order_mark_and_read_on_exactly(
    root, encoding
):
    # SQL Server writes its ERRORLOG in UTF-16 LE with a byte-order mark.
    live = root / "ERRORLOG"
    mark = {"utf-16-le": b"\xff\xfe", "utf-16-be": b"\xfe\xff", "utf-8-sig": b"\xef\xbb\xbf"}
    codec = "utf-8" if encoding == "utf-8-sig" else encoding
    live.write_bytes(mark[encoding] + ("\r\n".join(LINES[:2]) + "\r\n").encode(codec))
    _written(live)
    rows, kept, _gone = _read_all(f"{root}/ERRORLOG", {})
    assert [r["message"] for r in rows] == ["ASL Sender Statistics"] * 2
    assert rows[0][LINE_OFFSET] == len(mark[encoding])
    assert rows[1][LINE_OFFSET] == len(mark[encoding]) + len((LINES[0] + "\r\n").encode(codec))
    with open(live, "ab") as f:
        f.write((LINES[2] + "\r\n").encode(codec))
    _written(live)
    rows, kept, _gone = _read_all(f"{root}/ERRORLOG", kept)
    assert [r["message"] for r in rows] == ["AppleALC: found codec"]
    (state,) = kept.values()
    assert state.offset == live.stat().st_size


class _Frames(JsonLines):
    """A format that reads its records from the file's bytes itself: here, length-prefixed
    JSON records, as a binary format such as .evtx is read by its own reader."""

    frames_bytes = True

    def frame(self, path, stream, start, whole):
        stream.seek(start)
        offset = start
        while True:
            size = stream.read(2)
            if len(size) < 2:
                return
            body = stream.read(int.from_bytes(size, "big"))
            end = offset + 2 + len(body)
            yield offset, body.decode(), end
            offset = end


def test_a_format_that_frames_its_own_records_is_read_on_from_where_it_reached(root):
    def record(value):
        body = json.dumps(value).encode()
        return len(body).to_bytes(2, "big") + body

    log = root / "events.bin"
    log.write_bytes(record({"id": 1}) + record({"id": 2}))
    _written(log)
    table = _table(
        {
            FILE_ID: "text",
            "_file_name": "text",
            LINE_OFFSET: "bigint",
            ENTRY: "bigint",
            RAW: "text",
            PARSE_ERROR: "text",
            "id": "bigint",
        }
    )
    rows, kept, _gone = _read_all(f"{root}/events.bin", {}, _Frames(), table)
    assert [(r[LINE_OFFSET], r["id"]) for r in rows] == [(0, 1), (len(record({"id": 1})), 2)]
    with open(log, "ab") as f:
        f.write(record({"id": 3}))
    _written(log)
    rows, kept, _gone = _read_all(f"{root}/events.bin", kept, _Frames(), table)
    assert [r["id"] for r in rows] == [3]
