# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2d4e61-7a3b-4f19-9e05-3b6a1d0c7f42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox's Test (synthetic) environment is given its template's prepared environment without
building a runtime of either (REQ-1942, REQ-2053, REQ-2006).

The template's generation records are rows of its environment's schema. Reading them through the
template environment's runtime built that runtime, and its prod's, inside the visitor's open: on the
first open after a start, served as built, nothing of the template was built yet, and the profiling
demo's open spent 412 s there and was answered 504 by the 480 s upstream limit. And landing them
through the sandbox environment's runtime built that runtime twice (amended 2026-11-04): on cloud
"its environments' rows" took 32.6 s, its built-in sources, sources and schemas rebuilt. The
sandbox's environment is given the template's rows, records and bindings as the rest of the
template is copied: through its schema. Its runtime is built once after, by the open, cloned
(test_sandbox_env_runtime_built_at_open); the template's never is."""

# Requirements: REQ-1942, REQ-2053, REQ-2006

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from provisa.api import demo_synthetic
from provisa.core.environments import org_schema
from provisa.federation.replica_address import synthetic_schema
from provisa.synthetic.env_model import DATASET_ID

_TEMPLATE = "sbxtplprofiling"
_SANDBOX = "sbx0123456789ab"
_ENV = "synthetic"


class _Row:
    def __init__(self, values: dict) -> None:
        self._mapping = values
        for key, value in values.items():
            setattr(self, key, value)


class _Conn:
    """Answers a SELECT of a table by its schema-qualified name, keeping the columns selected;
    each read, and each insert and delete, is recorded."""

    capabilities = SimpleNamespace(schemas=True)

    def __init__(self, held: dict[str, list[dict]], read: list[str], wrote: list[tuple]) -> None:
        self.held = held
        self.read = read
        self.wrote = wrote

    @asynccontextmanager
    async def transaction(self):
        yield

    async def execute_core(self, stmt):
        if stmt.is_select:
            (table,) = stmt.get_final_froms()
            self.read.append(table.fullname)
            names = [c.name for c in stmt.selected_columns]
            rows = [
                _Row({n: r[n] for n in names if n in r}) for r in self.held.get(table.fullname, [])
            ]
            return SimpleNamespace(fetchall=lambda: rows)
        if stmt.is_insert:
            # One row each here: a multi-row insert names its first row's values ``<column>_m0``.
            values = {k.removesuffix("_m0"): v for k, v in stmt.compile().params.items()}
            self.wrote.append(("insert", stmt.table.fullname, values))
        else:
            self.wrote.append(("delete", stmt.table.fullname, None))
        return SimpleNamespace()


class _Pool:
    def __init__(self, held: dict[str, list[dict]], read: list[str], wrote: list[tuple]) -> None:
        self.held = held
        self.read = read
        self.wrote = wrote

    @asynccontextmanager
    async def acquire(self):
        yield _Conn(self.held, self.read, self.wrote)


def test_a_sandbox_environment_is_given_the_templates_without_building_a_runtime(
    monkeypatch,
) -> None:
    template = org_schema(_TEMPLATE, _ENV)
    sandbox = org_schema(_SANDBOX, _ENV)
    template_store = synthetic_schema(_TEMPLATE, _ENV, DATASET_ID)
    sandbox_store = synthetic_schema(_SANDBOX, _ENV, DATASET_ID)
    binding = {
        "schema": template_store,
        "tables": [["public", "orders"]],
        "parameters": {},
        "model_type": "postgresql",
    }
    held = {
        f"{template}.registered_tables": [
            {"id": 7, "source_id": "lines", "schema_name": "public", "table_name": "orders"}
        ],
        # The sandbox numbers its tables its own way.
        f"{sandbox}.registered_tables": [
            {"id": 31, "source_id": "lines", "schema_name": "public", "table_name": "orders"}
        ],
        f"{template}.synthetic_datasets": [{"id": DATASET_ID, "store_schema": template_store}],
        f"{template}.synthetic_dataset_tables": [{"dataset_id": DATASET_ID, "table_id": 7}],
        f"{template}.synthetic_report": [],
        f"{template}.synthetic_generations": [],
        f"{template}.sources": [
            {"id": "lines", "type": "duckdb", "synthetic": binding, "binding": "synthetic"},
        ],
    }
    read: list[str] = []
    wrote: list[tuple] = []
    built: list[tuple] = []
    copied: list[tuple] = []
    bound: list[dict] = []
    marked: list[tuple] = []

    async def _runtime(org_id: str, env: str = "prod", **kwargs):
        built.append((org_id, env))
        raise AssertionError(f"a runtime of {org_id}/{env} was built")

    async def _ready(org_id: str, env: str):
        return {"data_status": "ready", "data_error": None}

    async def _bind(conn, schema, **kwargs):
        bound.append({"schema": schema, **kwargs})
        return sorted(kwargs["generated"])

    async def _set_data(_admin, org_id, env, **values):
        marked.append((org_id, env, values["data_status"], values["synthetic_dataset"]))

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _runtime)
    monkeypatch.setattr(demo_synthetic, "_model_db", _runtime)
    monkeypatch.setattr(demo_synthetic, "_tenant", lambda: _Pool(held, read, wrote))
    monkeypatch.setattr(demo_synthetic, "_admin", lambda: object())
    monkeypatch.setattr(demo_synthetic, "_env_row", _ready)
    monkeypatch.setattr("provisa.core.env_data.bind_synthetic", _bind)
    monkeypatch.setattr("provisa.core.env_store.set_data", _set_data)
    monkeypatch.setattr(
        "provisa.federation.materialize_broker.copy_schema", lambda *a: copied.append(a)
    )
    monkeypatch.setattr("provisa.api.demo_replicas._store_path", lambda org: f"/stores/{org}")

    asyncio.run(demo_synthetic.copy_to_sandbox(_TEMPLATE, _SANDBOX, [_ENV]))

    assert built == [], f"the open built runtimes: {built}"
    # Its rows: the template's store schema, into the sandbox's own store.
    assert copied == [
        (f"/stores/{_TEMPLATE}", template_store, f"/stores/{_SANDBOX}", sandbox_store)
    ]
    # Its records: the template's, written into the sandbox environment's schema, naming its
    # own store schema and its own table ids.
    assert f"{template}.synthetic_datasets" in read, read
    inserted = {name: values for verb, name, values in wrote if verb == "insert"}
    assert inserted[f"{sandbox}.synthetic_datasets"]["store_schema"] == sandbox_store
    assert inserted[f"{sandbox}.synthetic_dataset_tables"]["table_id"] == 31
    assert all(name.startswith(f"{sandbox}.") for _verb, name, _v in wrote), wrote
    # Its sources: bound as the template's are, to its own store schema.
    assert bound == [
        {
            "schema": sandbox,
            "store_type": "duckdb",
            "store_schema": sandbox_store,
            "generated": {"lines": [["public", "orders"]]},
            "parameters": {"lines": {}},
        }
    ]
    assert marked == [(_SANDBOX, _ENV, "ready", DATASET_ID)]
