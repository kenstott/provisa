# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2d4e61-7a3b-4f19-9e05-3b6a1d0c7f42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox's Test (synthetic) environment is given its template's rows without building the
template's runtimes (REQ-1942, REQ-2053).

The template's generation records are rows of its environment's schema. Reading them through the
template environment's runtime built that runtime, and its prod's, inside the visitor's open: on the
first open after a start, served as built, nothing of the template was built yet, and the profiling
demo's open spent 412 s there and was answered 504 by the 480 s upstream limit."""

# Requirements: REQ-1942, REQ-2053

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from provisa.api import demo_synthetic
from provisa.core.environments import org_schema

_TEMPLATE = "sbxtplprofiling"
_SANDBOX = "sbx0123456789ab"
_ENV = "synthetic"


class _Row:
    def __init__(self, values: dict) -> None:
        self._mapping = values
        for key, value in values.items():
            setattr(self, key, value)


class _Conn:
    """Answers a SELECT of a table by its schema-qualified name; each read is recorded."""

    capabilities = SimpleNamespace(schemas=True)

    def __init__(self, held: dict[str, list[dict]], read: list[str]) -> None:
        self.held = held
        self.read = read

    async def execute_core(self, stmt):
        (table,) = stmt.get_final_froms()
        self.read.append(table.fullname)
        rows = [_Row(r) for r in self.held.get(table.fullname, [])]
        return SimpleNamespace(fetchall=lambda: rows)


class _Pool:
    def __init__(self, held: dict[str, list[dict]], read: list[str]) -> None:
        self.held = held
        self.read = read

    @asynccontextmanager
    async def acquire(self):
        yield _Conn(self.held, self.read)


def test_rows_are_given_without_building_the_templates_runtime(monkeypatch) -> None:
    schema = org_schema(_TEMPLATE, _ENV)
    held = {
        f"{schema}.registered_tables": [
            {"id": 7, "source_id": "lines", "schema_name": "public", "table_name": "orders"}
        ],
        f"{schema}.synthetic_datasets": [{"id": "d1"}],
        f"{schema}.synthetic_dataset_tables": [{"dataset_id": "d1", "table_id": 7}],
        f"{schema}.synthetic_report": [],
        f"{schema}.synthetic_generations": [],
    }
    read: list[str] = []
    built: list[tuple[str, str]] = []
    landed: list[tuple] = []

    async def _runtime(org_id: str, env: str = "prod"):
        built.append((org_id, env))
        return SimpleNamespace(model_db=_Pool(held, read))

    async def _ready(org_id: str, env: str):
        return {"data_status": "ready", "data_error": None}

    async def _land(org_id, env, store, from_schema, records):
        landed.append((org_id, env, store, from_schema, records))

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _runtime)
    monkeypatch.setattr(demo_synthetic, "_tenant", lambda: _Pool(held, read), raising=False)
    monkeypatch.setattr(demo_synthetic, "_env_row", _ready)
    monkeypatch.setattr(demo_synthetic, "_land", _land)
    monkeypatch.setattr("provisa.api.demo_replicas._store_path", lambda org: f"/stores/{org}")

    asyncio.run(demo_synthetic.copy_to_sandbox(_TEMPLATE, _SANDBOX, [_ENV]))

    assert built == [], f"the open built the template's runtimes: {built}"
    assert f"{schema}.synthetic_datasets" in read, read
    ((org_id, env, store, _from, records),) = landed
    assert (org_id, env, store) == (_SANDBOX, _ENV, f"/stores/{_TEMPLATE}")
    assert records["synthetic_datasets"] == [{"id": "d1"}]
    # A table is carried by its source, schema and name: the sandbox numbers it its own way.
    assert records["synthetic_dataset_tables"] == [
        {"dataset_id": "d1", "table": ["lines", "public", "orders"]}
    ]
