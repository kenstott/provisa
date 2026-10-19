# Copyright (c) 2026 Kenneth Stott
# Canary: e24d0c1f-7ba1-4916-9e71-24c5d1e5a8d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2089: a config's declared replica rows land as its replica, recorded built, and each
declaration that cannot is refused by its file's name."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.federation import declared_replica_rows as drr
from provisa.federation.declared_replica_rows import DeclaredRowsRefused

COLUMNS = [
    ("provider", "text"),
    ("id", "text"),
    ("position", "bigint"),
    ("is_read", "boolean"),
    ("to_addresses", "json"),
    ("sent_at", "timestamp"),
    ("score", "double"),
]
HEADER = "\t".join(name for name, _ in COLUMNS)


def _write(directory: Path, source: str, name: str, lines: list[str]) -> Path:
    path = directory / drr.ROWS_DIR / source / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class TestReading:
    def test_a_cell_is_read_as_its_columns_type(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path,
            "mail",
            "default.messages.tsv",
            [
                HEADER,
                'microsoft\tM1\t2\ttrue\t["a@x.com"]\t2026-03-02T09:15:00Z\t0.5',
                "microsoft\tM2\t\\N\tfalse\t[]\t\\N\t\\N",
            ],
        )
        rows = drr.typed_rows(drr.read_file(path), COLUMNS)
        assert rows[0] == {
            "provider": "microsoft",
            "id": "M1",
            "position": 2,
            "is_read": True,
            "to_addresses": '["a@x.com"]',
            "sent_at": "2026-03-02T09:15:00Z",
            "score": 0.5,
        }
        assert rows[1]["position"] is None and rows[1]["sent_at"] is None
        assert rows[1]["is_read"] is False

    def test_escapes_stand_for_a_tab_a_newline_and_a_backslash(self, tmp_path: Path) -> None:
        path = _write(
            tmp_path,
            "mail",
            "default.messages.tsv",
            [HEADER, "microsoft\tline one\\nline two\\ttab \\\\ end\t1\ttrue\t[]\t\\N\t\\N"],
        )
        (row,) = drr.typed_rows(drr.read_file(path), COLUMNS)
        assert row["id"] == "line one\nline two\ttab \\ end"

    def test_its_key_is_its_place(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "mail", "default.message_recipients.tsv", [HEADER])
        assert drr.read_file(path).key == ("mail", "default", "message_recipients")

    def test_a_file_not_named_schema_dot_table_is_refused(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "mail", "messages.tsv", [HEADER])
        with pytest.raises(DeclaredRowsRefused, match=r"messages\.tsv: name it"):
            drr.read_file(path)

    def test_a_header_that_is_not_the_tables_columns_is_refused(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "mail", "default.messages.tsv", ["provider\tid\tsubject"])
        with pytest.raises(DeclaredRowsRefused, match=r"default\.messages\.tsv: the header"):
            drr.typed_rows(drr.read_file(path), COLUMNS)

    def test_a_row_of_another_width_is_refused_by_its_line(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "mail", "default.messages.tsv", [HEADER, "microsoft\tM1"])
        with pytest.raises(DeclaredRowsRefused, match=r"default\.messages\.tsv:2: 2 cells"):
            drr.read_file(path)

    @pytest.mark.parametrize(
        ("cells", "why"),
        [
            ("microsoft\tM1\ttwo\ttrue\t[]\t\\N\t\\N", "not a whole number"),
            ("microsoft\tM1\t1\tyes\t[]\t\\N\t\\N", "not true or false"),
            ("microsoft\tM1\t1\ttrue\t[a\t\\N\t\\N", "not JSON"),
            ("microsoft\tM1\t1\ttrue\t[]\t\\N\tlots", "not a number"),
        ],
    )
    def test_a_cell_not_of_its_columns_type_is_refused(
        self, tmp_path: Path, cells: str, why: str
    ) -> None:
        path = _write(tmp_path, "mail", "default.messages.tsv", [HEADER, cells])
        with pytest.raises(DeclaredRowsRefused, match=why):
            drr.typed_rows(drr.read_file(path), COLUMNS)


class _Target:
    caps = SimpleNamespace(load=SimpleNamespace(value="bulk_stream"))

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.batches: list[Any] = []

    async def begin(self) -> None:
        self.calls.append("begin")

    async def write(self, batch: Any, rows: list[dict]) -> None:
        self.calls.append("write")
        self.batches.append(batch)

    async def swap(self) -> None:
        self.calls.append("swap")

    async def abort(self) -> None:
        self.calls.append("abort")


class _Db:
    def __init__(self) -> None:
        self.records: dict = {}

    @asynccontextmanager
    async def acquire(self):
        yield self


def _state(target: _Target) -> Any:
    backend = SimpleNamespace(
        dialect="duckdb",
        replica_address=lambda state, **k: SimpleNamespace(schema="org_x_replicas", table="t"),
        replica_engine=lambda state, source, table, **k: object(),
        replica_target=lambda state, **k: target,
    )
    return SimpleNamespace(
        federation_engine=SimpleNamespace(engine=SimpleNamespace(backend=backend)),
        tenant_db=_Db(),
    )


@pytest.fixture
def model(monkeypatch: pytest.MonkeyPatch) -> dict:
    """The model a test declares rows against: its sources and tables, and what is recorded."""
    from provisa.federation import replica_builds, replica_converge, replica_state, residency

    held: dict = {"sources": {}, "tables": set(), "recorded": [], "read": None}

    async def model_row(state, key):
        if key not in held["tables"] or key[0] not in held["sources"]:
            raise replica_builds.ReplicaTableGone(key)
        return held["sources"][key[0]], SimpleNamespace(schema_name=key[1], table_name=key[2]), []

    async def read(conn, key):
        return held["read"]

    async def record_adopted(conn, rows):
        held["recorded"].extend(rows)

    monkeypatch.setattr(replica_builds, "_model_row", model_row)
    monkeypatch.setattr(replica_builds, "store_identity", lambda state: "store-1")
    monkeypatch.setattr(replica_converge, "definition_hash", lambda *a, **k: "def-1")
    monkeypatch.setattr(
        residency,
        "resolve_landing_args",
        lambda source, table, platform: SimpleNamespace(
            columns=COLUMNS, pk_columns=["id"], landing_filter=None
        ),
    )
    monkeypatch.setattr(replica_state, "read", read)
    monkeypatch.setattr(replica_state, "record_adopted", record_adopted)
    return held


def _declare(model: dict, source: str, cache_ttl: int | None) -> None:
    model["sources"][source] = SimpleNamespace(id=source, cache_ttl=cache_ttl)
    model["tables"].add((source, "default", "messages"))


class TestAdopting:
    def test_declared_rows_are_written_and_recorded_built(
        self, tmp_path: Path, model: dict
    ) -> None:
        _declare(model, "mail", -1)
        _write(
            tmp_path,
            "mail",
            "default.messages.tsv",
            [HEADER, "microsoft\tM1\t1\ttrue\t[]\t2026-03-02T09:15:00\t0.5"],
        )
        target = _Target()
        landed = asyncio.run(drr.adopt_declared_rows(_state(target), tmp_path))
        assert landed == [("mail", "default", "messages")]
        assert target.calls == ["begin", "write", "swap"]
        assert target.batches[0].num_rows == 1
        (record,) = model["recorded"]
        assert record["built_store"] == "store-1" and record["definition_hash"] == "def-1"
        assert record["completed_rows"] == 1 and record["build_state"] == "idle"
        assert record["built_columns"] == [[n, t] for n, t in COLUMNS]
        assert record["build_method"] == "stream_batches" and record["load_kind"] == "bulk_stream"
        assert record["content_hash"]

    def test_rows_already_landed_here_are_not_written_again(
        self, tmp_path: Path, model: dict
    ) -> None:
        _declare(model, "mail", -1)
        _write(
            tmp_path,
            "mail",
            "default.messages.tsv",
            [HEADER, "microsoft\tM1\t1\ttrue\t[]\t2026-03-02T09:15:00\t0.5"],
        )
        first = _Target()
        asyncio.run(drr.adopt_declared_rows(_state(first), tmp_path))
        recorded = model["recorded"][0]
        model["read"] = SimpleNamespace(
            built_store="store-1",
            definition_hash="def-1",
            content_hash=recorded["content_hash"],
        )
        again = _Target()
        assert asyncio.run(drr.adopt_declared_rows(_state(again), tmp_path)) == []
        assert again.calls == []

    def test_a_table_the_config_does_not_serve_is_refused(
        self, tmp_path: Path, model: dict
    ) -> None:
        _declare(model, "mail", -1)
        _write(tmp_path, "mail", "default.threads.tsv", [HEADER])
        with pytest.raises(DeclaredRowsRefused, match=r"default\.threads\.tsv: the config serves"):
            asyncio.run(drr.adopt_declared_rows(_state(_Target()), tmp_path))

    def test_a_source_that_refreshes_is_refused(self, tmp_path: Path, model: dict) -> None:
        _declare(model, "mail", 300)
        _write(tmp_path, "mail", "default.messages.tsv", [HEADER])
        with pytest.raises(DeclaredRowsRefused, match=r"must declare cache_ttl: -1"):
            asyncio.run(drr.adopt_declared_rows(_state(_Target()), tmp_path))

    def test_a_declared_table_with_no_rows_is_refused(self, tmp_path: Path, model: dict) -> None:
        _declare(model, "mail", -1)
        _write(tmp_path, "mail", "default.messages.tsv", [HEADER])
        with pytest.raises(DeclaredRowsRefused, match=r"default\.messages\.tsv: no rows"):
            asyncio.run(drr.adopt_declared_rows(_state(_Target()), tmp_path))

    def test_a_config_with_no_declared_rows_lands_nothing(
        self, tmp_path: Path, model: dict
    ) -> None:
        assert asyncio.run(drr.adopt_declared_rows(_state(_Target()), tmp_path)) == []


class TestTheSchemaBuild:
    """The declared rows land before the routes are published, so a source of them alone is kept
    (and never contacted) from the first read on."""

    def _record(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
        from provisa.api import demo_templates, model_reload

        calls: list[str] = []

        async def adopt(state, directory):
            calls.append(f"adopt {directory.name}")
            return []

        async def publish(rt):
            calls.append("publish")

        monkeypatch.setattr(demo_templates, "declared_rows_directory", lambda org_id: tmp_path)
        monkeypatch.setattr(drr, "adopt_declared_rows", adopt)
        monkeypatch.setattr(model_reload, "publish_replica_routes", publish)
        return calls

    def test_the_rows_land_before_the_routes_are_published(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from provisa.api.model_reload import publish_routes_over_declared_rows
        from provisa.core.environments import PROD

        calls = self._record(monkeypatch, tmp_path)
        asyncio.run(publish_routes_over_declared_rows(SimpleNamespace(env=PROD, org_id="o")))
        assert calls == [f"adopt {tmp_path.name}", "publish"]

    def test_another_environment_publishes_its_routes_alone(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from provisa.api.model_reload import publish_routes_over_declared_rows

        calls = self._record(monkeypatch, tmp_path)
        asyncio.run(publish_routes_over_declared_rows(SimpleNamespace(env="dev", org_id="o")))
        assert calls == ["publish"]

    def test_the_rebuild_publishes_its_routes_through_that_one_step(self) -> None:
        import inspect

        from provisa.api import app

        body = inspect.getsource(app._rebuild_schemas_impl)
        assert "publish_routes_over_declared_rows(" in body
        assert "publish_replica_routes(" not in body
        assert "adopt_declared_rows(" not in body
