# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2b6f41-0a3c-4e97-b5d8-71c9e2a4f063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2114 (amended 2026-11-25): Polly reads the files the person attaches in the chat and hands
back downloads, with no access to the server's file system."""

from __future__ import annotations

import base64
import builtins
import io
import json
import pathlib
import tarfile
import zipfile
from types import SimpleNamespace

import pytest

from provisa.api.mcp import chat as chat_mod
from provisa.api.mcp import polly_files as pf


def _b64(content: bytes) -> str:
    return base64.b64encode(content).decode("ascii")


def _attach(*files: tuple[str, bytes]) -> dict[str, pf.Attachment]:
    return pf.parse_attachments(
        [{"id": f"a{i}", "name": name, "data": _b64(body)} for i, (name, body) in enumerate(files)]
    )


def _zip(members: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return out.getvalue()


def _tgz(members: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tf:
        for name, body in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tf.addfile(info, io.BytesIO(body))
    return out.getvalue()


OPENAPI = b"openapi: 3.0.0\ninfo: {title: Pets, version: '1'}\npaths: {/pets: {get: {}}}\n"


class TestTheTools:
    def test_both_are_server_tools(self):
        names = {t["name"] for t in chat_mod._TOOLS}
        assert {"read_attachment", "offer_download"} <= names
        assert not {"read_attachment", "offer_download"} & set(chat_mod._CLIENT_TOOL_NAMES)

    def test_read_attachment_takes_an_attachment_id_never_a_path(self):
        tool = next(t for t in chat_mod._TOOLS if t["name"] == "read_attachment")
        assert set(tool["input_schema"]["properties"]) == {"attachment", "member", "offset"}
        assert "path" not in json.dumps(tool["input_schema"])
        assert pf.LIMIT_TEXT in tool["description"]

    def test_the_stated_limit_is_the_enforced_one(self):
        assert pf.MAX_FILE_BYTES == 10 * 1024 * 1024
        assert "10.0 MB" in pf.LIMIT_TEXT
        assert f"{pf.MAX_ATTACHMENTS} attachments" in pf.LIMIT_TEXT


class TestAttaching:
    def test_absent_is_none_attached(self):
        assert pf.parse_attachments(None) == {}

    def test_each_kind_is_named_by_its_ending(self):
        atts = _attach(
            ("orders.csv", b"id\n1\n"),
            ("pets.yaml", OPENAPI),
            ("src.zip", _zip({"a.py": b"x = 1\n"})),
            ("notes.md", b"# Notes\n\nhello\n"),
        )
        assert [a.kind for a in atts.values()] == [
            pf.CSV,
            pf.API_SPEC,
            pf.CODE_ARCHIVE,
            pf.DOCUMENT,
        ]

    def test_a_file_over_the_limit_is_refused(self):
        big = b"x" * (pf.MAX_FILE_BYTES + 1)
        with pytest.raises(pf.FileTooLarge, match="larger than 10.0 MB"):
            _attach(("big.csv", big))

    def test_a_file_at_the_limit_is_taken(self):
        exact = b"x" * pf.MAX_FILE_BYTES
        assert len(_attach(("exact.txt", exact))["a0"].content) == pf.MAX_FILE_BYTES

    def test_more_files_than_the_limit_are_refused(self):
        files = [("f%d.csv" % i, b"a\n") for i in range(pf.MAX_ATTACHMENTS + 1)]
        with pytest.raises(pf.FileTooLarge, match="files are attached"):
            _attach(*files)

    def test_a_kind_polly_does_not_read_is_refused_by_name(self):
        with pytest.raises(pf.FileRefused, match="'run.exe' is not a file Polly reads"):
            _attach(("run.exe", b"MZ"))

    @pytest.mark.parametrize("name", ["/etc/passwd.txt", "../secrets.csv", "dir\\a.csv", ".."])
    def test_a_path_is_refused_as_a_name(self, name):
        with pytest.raises(pf.FileRefused, match="never a path"):
            _attach((name, b"a\n"))

    def test_bytes_that_are_not_base64_are_refused(self):
        with pytest.raises(pf.FileRefused, match="not base64"):
            pf.parse_attachments([{"id": "a", "name": "a.csv", "data": "!!!"}])


class TestReading:
    def test_a_csv_reads_with_its_columns_and_row_count(self):
        atts = _attach(("orders.csv", b"id,total\n1,9.5\n2,3\n"))
        got = pf.read_attachment(atts, "a0", None, 0)
        assert got["columns"] == ["id", "total"]
        assert got["row_count"] == 2
        assert got["text"].startswith("id,total")
        assert got["next_offset"] is None

    def test_an_api_spec_says_what_it_is(self):
        got = pf.read_attachment(_attach(("pets.yaml", OPENAPI)), "a0", None, 0)
        assert got["format"] == "openapi"
        assert "/pets" in got["text"]

    def test_a_zip_lists_its_members_then_reads_one(self):
        atts = _attach(("src.zip", _zip({"pkg/a.py": b"x = 1\n", "README.md": b"hi"})))
        listed = pf.read_attachment(atts, "a0", None, 0)
        assert {m["name"] for m in listed["members"]} == {"pkg/a.py", "README.md"}
        got = pf.read_attachment(atts, "a0", "pkg/a.py", 0)
        assert got["text"] == "x = 1\n"

    def test_a_tar_gz_reads_a_member(self):
        atts = _attach(("src.tar.gz", _tgz({"main.go": b"package main\n"})))
        assert pf.read_attachment(atts, "a0", None, 0)["members"] == [
            {"name": "main.go", "size": 13}
        ]
        assert pf.read_attachment(atts, "a0", "main.go", 0)["text"] == "package main\n"

    def test_a_member_the_archive_lacks_is_refused(self):
        atts = _attach(("src.zip", _zip({"a.py": b""})))
        with pytest.raises(pf.FileRefused, match="no member '/etc/passwd'"):
            pf.read_attachment(atts, "a0", "/etc/passwd", 0)

    def test_a_member_that_unpacks_over_the_limit_is_refused(self):
        bomb = _zip({"big.txt": b"0" * (pf.MAX_FILE_BYTES + 1)})
        with pytest.raises(pf.FileTooLarge, match="unpacks larger"):
            pf.read_attachment(_attach(("bomb.zip", bomb)), "a0", "big.txt", 0)

    def test_a_document_reads_by_its_headings(self):
        got = pf.read_attachment(
            _attach(("notes.md", b"# Plan\n\n## Goals\n\nShip it.\n")), "a0", None, 0
        )
        assert "## Goals" in got["text"]
        assert "Ship it." in got["text"]

    def test_long_text_reads_a_window_at_a_time(self):
        body = ("a" * pf.READ_CHARS + "tail").encode()
        atts = _attach(("long.txt", body))
        first = pf.read_attachment(atts, "a0", None, 0)
        assert len(first["text"]) == pf.READ_CHARS
        assert first["next_offset"] == pf.READ_CHARS
        rest = pf.read_attachment(atts, "a0", None, first["next_offset"])
        assert rest["text"] == "tail"
        assert rest["next_offset"] is None


class TestNoPathAccess:
    @pytest.mark.parametrize("ref", ["/etc/passwd", "~/.ssh/id_rsa", "../provisa.yaml", "a9"])
    def test_anything_not_attached_is_refused(self, ref):
        atts = _attach(("orders.csv", b"id\n1\n"))
        with pytest.raises(pf.FileRefused, match="is not a file attached in this chat"):
            pf.read_attachment(atts, ref, None, 0)

    def test_with_nothing_attached_every_read_is_refused(self, tmp_path):
        real = tmp_path / "server.csv"
        real.write_text("secret\n1\n")
        with pytest.raises(pf.FileRefused, match=r"attached: none"):
            pf.read_attachment(pf.NO_ATTACHMENTS, str(real), None, 0)

    def test_reading_and_offering_never_open_a_path(self, monkeypatch):
        atts = _attach(
            ("orders.csv", b"id\n1\n"),
            ("pets.yaml", OPENAPI),
            ("src.zip", _zip({"a.py": b"x = 1\n"})),
            ("notes.md", b"# Notes\n\nhello\n"),
        )
        # Warm the lazy imports first, so only the reads themselves run with paths closed off.
        for att_id in atts:
            pf.read_attachment(atts, att_id, None, 0)
        pf.xlsx_bytes(["a"], [{"a": 1}])

        def no_path(*_a, **_k):
            raise AssertionError("a path was opened")

        monkeypatch.setattr(builtins, "open", no_path)
        monkeypatch.setattr(pathlib.Path, "open", no_path)
        monkeypatch.setattr(pathlib.Path, "read_bytes", no_path)
        monkeypatch.setattr(pathlib.Path, "read_text", no_path)
        monkeypatch.setattr(pathlib.Path, "write_bytes", no_path)
        monkeypatch.setattr(pathlib.Path, "write_text", no_path)
        for att_id in atts:
            pf.read_attachment(atts, att_id, None, 0)
        pf.read_attachment(atts, "a2", "a.py", 0)
        pf.download("report.md", b"# r")
        pf.download("r.xlsx", pf.xlsx_bytes(["a"], [{"a": 1}]))


class TestThroughTheChat:
    async def test_dispatch_reads_only_this_chats_attachments(self):
        atts = _attach(("orders.csv", b"id\n1\n"))
        got = await chat_mod._dispatch_tool(
            None, "admin", "read_attachment", {"attachment": "a0"}, scope=None, attachments=atts
        )
        assert got["columns"] == ["id"]
        with pytest.raises(pf.FileRefused):
            await chat_mod._dispatch_tool(
                None, "admin", "read_attachment", {"attachment": "a0"}, scope=None
            )

    def test_the_system_prompt_lists_the_attached_files(self):
        atts = _attach(("orders.csv", b"id\n1\n"))
        system = chat_mod._system_prompt(None, atts)
        assert '"id": "a0"' in system
        assert '"name": "orders.csv"' in system
        assert "read_attachment" in system
        assert "read_attachment by its id" not in chat_mod._system_prompt(None)


class TestDownloads:
    async def _offer(self, args: dict, **kw):
        return await chat_mod._dispatch_tool(
            None, "admin", "offer_download", args, scope=None, **kw
        )

    async def test_code_is_handed_back_as_a_download_the_model_never_holds(self):
        result = await self._offer(
            {"kind": "code", "filename": "orders.py", "content": "print('hi')\n"}
        )
        kept, artifact = chat_mod._split_artifact(result)
        assert artifact is not None
        assert artifact["type"] == "artifact"
        assert artifact["kind"] == "download"
        assert artifact["filename"] == "orders.py"
        assert artifact["content_type"] == "text/x-python"
        assert base64.b64decode(artifact["data"]) == b"print('hi')\n"
        assert "data" not in json.dumps(kept)
        assert kept["shown"] == "The user has a download for this file in the chat."

    async def test_a_report_needs_its_content(self):
        with pytest.raises(ValueError, match="needs content"):
            await self._offer({"kind": "report", "filename": "r.md"})

    @pytest.mark.parametrize("name", ["/tmp/x.py", "../x.py", "a\\b.py"])
    async def test_a_download_is_named_never_a_path(self, name):
        with pytest.raises(pf.FileRefused, match="never a path"):
            await self._offer({"kind": "code", "filename": name, "content": "x"})

    async def test_an_ending_polly_does_not_hand_back_is_refused(self):
        with pytest.raises(pf.FileRefused, match="no ending Polly hands back"):
            await self._offer({"kind": "code", "filename": "run.exe", "content": "x"})

    async def test_a_download_over_the_limit_is_refused(self):
        with pytest.raises(pf.FileTooLarge, match="larger than the 10.0 MB"):
            await self._offer(
                {"kind": "report", "filename": "r.txt", "content": "x" * (pf.MAX_FILE_BYTES + 1)}
            )

    async def test_a_query_result_is_saved_as_xlsx(self, monkeypatch):
        async def run_sql(_state, role, sql, *_a, **_k):
            assert role == "admin"
            return {
                "columns": ["id", "name"],
                "rows": [{"id": 1, "name": "a"}, {"id": 2, "name": None}],
                "row_count": 2,
                "total_rows": 2,
                "truncated": False,
            }

        monkeypatch.setattr(chat_mod.mcp_tools, "run_sql", run_sql)
        result = await self._offer(
            {"kind": "query_result", "filename": "o.xlsx", "sql": "SELECT 1", "format": "xlsx"}
        )
        from openpyxl import load_workbook

        wb = load_workbook(io.BytesIO(base64.b64decode(result["artifact"]["data"])))
        ws = wb.active
        assert ws is not None
        assert [list(r) for r in ws.iter_rows(values_only=True)] == [
            ["id", "name"],
            [1, "a"],
            [2, None],
        ]

    async def test_a_query_result_is_saved_as_csv(self, monkeypatch):
        async def run_sql(*_a, **_k):
            return {
                "columns": ["id"],
                "rows": [{"id": 1}],
                "row_count": 1,
                "total_rows": 1,
                "truncated": False,
            }

        monkeypatch.setattr(chat_mod.mcp_tools, "run_sql", run_sql)
        result = await self._offer(
            {"kind": "query_result", "filename": "o.csv", "sql": "SELECT 1", "format": "csv"}
        )
        assert base64.b64decode(result["artifact"]["data"]) == b"id\r\n1\r\n"

    async def test_a_result_over_the_row_cap_is_refused_never_cut(self, monkeypatch):
        async def run_sql(*_a, **_k):
            return {
                "columns": ["id"],
                "rows": [{"id": 1}],
                "row_count": 1,
                "total_rows": 5,
                "truncated": True,
            }

        monkeypatch.setattr(chat_mod.mcp_tools, "run_sql", run_sql)
        with pytest.raises(ValueError, match="has 5 rows"):
            await self._offer(
                {"kind": "query_result", "filename": "o.csv", "sql": "SELECT 1", "format": "csv"}
            )

    async def test_a_model_export_is_the_settings_export_for_its_people(self, monkeypatch):
        from provisa.api.admin import _platform_guard, config_export

        checked = []
        monkeypatch.setattr(_platform_guard, "require_deployment_settings", checked.append)

        async def build(sections):
            return f"sections: {sections}\n"

        monkeypatch.setattr(config_export, "build_live_config_yaml", build)
        request = SimpleNamespace()
        result = await self._offer(
            {"kind": "model_export", "sections": ["sources"]}, request=request
        )
        assert checked == [request]
        assert result["filename"] == "provisa.live.yaml"
        assert base64.b64decode(result["artifact"]["data"]) == b"sections: ['sources']\n"

    async def test_a_model_export_is_refused_to_whom_the_settings_page_refuses(self, monkeypatch):
        from fastapi import HTTPException

        from provisa.api.admin import _platform_guard

        def refuse(_request):
            raise HTTPException(status_code=403, detail="platform administrators only")

        monkeypatch.setattr(_platform_guard, "require_deployment_settings", refuse)
        with pytest.raises(HTTPException):
            await self._offer({"kind": "model_export"}, request=SimpleNamespace())
