# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2b8d17-3a94-4c50-b7f6-9d1c0e4a5f38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""EVIDENCE, not a regression suite: does a change made through one worker reach the others?

Boots `uvicorn --workers 4` on a fresh control plane, makes one model or governance change
through the admin API on ONE worker, and then asks EACH worker — on a connection pinned to it —
what it serves, at once and again later. A worker is identified by /data/schema-version
(``<boot id>-<schema version>``; the boot id is per process), and a keep-alive connection stays
on the worker that accepted it.

    .venv/bin/python3 -m tests.integration.cross_worker_evidence --pg-port <port> [--second-launch]

Prints one row per change type: which workers serve the change at 0 s / 20 s / 70 s. Not named
test_*: every row that is not "all workers" is a known gap this file exists to measure, and a
collected test that fails by design would hide real failures."""

from __future__ import annotations

import argparse
import http.client
import json
import time

from tests.helpers import registered_id, release_mutation
from tests.integration.worker_boot_harness import WorkerBoot

ADMIN = "/admin/graphql"
DATA = "/data/graphql"


class Worker:
    """One keep-alive connection, i.e. one worker process."""

    def __init__(self, port: int) -> None:
        self.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
        self.boot_id, self.version = self.schema_version()

    def _post(self, path: str, body: dict, headers: dict | None = None):
        self.conn.request(
            "POST",
            path,
            body=json.dumps(body),
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        resp = self.conn.getresponse()
        return resp, json.loads(resp.read() or b"{}")

    def schema_version(self) -> tuple[str, str]:
        self.conn.request("GET", "/data/schema-version")
        boot, _, version = json.loads(self.conn.getresponse().read())["version"].rpartition("-")
        return boot, version

    def admin(self, query: str) -> dict:
        _, body = self._post(ADMIN, {"query": query})
        return body

    def data(self, query: str, role: str = "analyst") -> dict:
        _, body = self._post(DATA, {"query": query}, {"x-provisa-role": role})
        return body


def one_connection_per_worker(port: int, workers: int) -> list[Worker]:
    seen: dict[str, Worker] = {}
    for _ in range(400):
        w = Worker(port)
        if w.boot_id in seen:
            w.conn.close()
        else:
            seen[w.boot_id] = w
        if len(seen) == workers:
            break
    return list(seen.values())


_printed: set = set()


def _refused(body: dict) -> bool:
    """A query the server refused: a GraphQL error list, or the 400 body `{"detail": ...}`."""
    return "errors" in body or "detail" in body


def observe(workers: list[Worker], probe, label: str, rows: list) -> None:
    """Record, per worker, whether `probe(worker)` shows the change — now, at 20 s and at 70 s."""
    started = time.monotonic()
    line = [label]
    for at in (0, 20, 70):
        # The server closes a keep-alive connection idle for 5 s, and a reopened one may land on
        # another worker: keep each connection busy while waiting, and check whose it still is.
        while time.monotonic() - started < at:
            for w in workers:
                w.schema_version()
            time.sleep(2)
        seen = []
        for w in workers:
            try:
                assert w.schema_version()[0] == w.boot_id, "connection moved to another worker"
                shows, raw = probe(w)
                seen.append("Y" if shows else "n")
                answer = json.dumps(raw)[:150]
                if (label, answer) not in _printed:
                    _printed.add((label, answer))
                    print(f"    [{at:>2}s {w.boot_id[:6]}] {answer}")
            except Exception as exc:  # the probe's failure is the observation
                seen.append(f"E({type(exc).__name__})")
                print(f"    probe error on {w.boot_id[:6]}: {type(exc).__name__}: {str(exc)[:160]}")
        line.append(" ".join(seen))
    rows.append(line)


def run(order: list[Worker], rows: list, tag: str) -> None:
    """Make each change through ``order[0]`` and observe it on every worker in ``order``."""
    author = order[0]
    print(f"[{tag}] workers reached: {len(order)} (boot ids {[w.boot_id[:6] for w in order]})")

    def versions() -> str:
        return " ".join(w.schema_version()[1] for w in order)

    print(f"[{tag}] schema_version per worker before: {versions()}")

    # 1. register a table --------------------------------------------------------------------
    res = author.admin(
        'mutation { registerTable(input: {sourceId: "sales-pg", domainId: "sales", '
        'schemaName: "public", tableName: "customers", columns: ['
        '{name: "id", visibleTo: ["org_admin", "analyst"], dataType: "integer"}, '
        '{name: "name", visibleTo: ["org_admin", "analyst"], dataType: "varchar"}]}) '
        "{ success message } }"
    )
    print(f"[{tag}] registerTable -> {json.dumps(res)[:200]}")
    # REQ-1921: registered through the admin, it starts as draft; released, it is read.
    res = author.admin(release_mutation(registered_id(res["data"]["registerTable"]["message"])))
    print(f"[{tag}] setTableDraft -> {json.dumps(res)[:200]}")
    observe(
        order,
        lambda w: (lambda r: (not _refused(r), r))(w.data("{ s__customers { id } }")),
        f"{tag}: register table (new field queryable)",
        rows,
    )

    # 2. column visibility: analyst loses `region` on orders ----------------------------------
    res = author.admin(
        'mutation { updateTable(input: {sourceId: "sales-pg", domainId: "sales", '
        'schemaName: "public", tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin", "analyst"], dataType: "integer"}, '
        '{name: "region", visibleTo: ["org_admin"], dataType: "varchar"}]}) '
        "{ success message } }"
    )
    print(f"[{tag}] updateTable(visibility) -> {json.dumps(res)[:200]}")
    observe(
        order,
        lambda w: (lambda r: (_refused(r), r))(w.data("{ s__orders { id region } }")),
        f"{tag}: column hidden from role (analyst refused `region`)",
        rows,
    )

    # 3. RLS: analyst sees only id = 1 --------------------------------------------------------
    res = author.admin(
        'mutation { upsertRlsRule(input: {tableId: "orders", roleId: "analyst", '
        'filterExpr: "id = 1"}) { success message } }'
    )
    print(f"[{tag}] upsertRlsRule -> {json.dumps(res)[:200]}")
    observe(
        order,
        lambda w: (lambda r: (len((r.get("data") or {}).get("s__orders") or []) == 1, r))(
            w.data("{ s__orders { id } }")
        ),
        f"{tag}: RLS rule (analyst sees 1 of 2 rows)",
        rows,
    )

    # 4. masking: `name` on customers masked for analyst --------------------------------------
    res = author.admin(
        'mutation { updateTable(input: {sourceId: "sales-pg", domainId: "sales", '
        'schemaName: "public", tableName: "customers", columns: ['
        '{name: "id", visibleTo: ["org_admin", "analyst"], dataType: "integer"}, '
        '{name: "name", visibleTo: ["org_admin", "analyst"], unmaskedTo: ["org_admin"], '
        'maskType: "constant", maskValue: "MASKED", dataType: "varchar"}]}) '
        "{ success message } }"
    )
    print(f"[{tag}] updateTable(mask) -> {json.dumps(res)[:200]}")

    def masked(w: Worker):
        body = w.data("{ s__customers { name } }")
        rows_ = (body.get("data") or {}).get("s__customers") or []
        return bool(rows_) and all(r["name"] == "MASKED" for r in rows_), body

    observe(order, masked, f"{tag}: masking rule (analyst sees MASKED)", rows)

    # 5. relationship -------------------------------------------------------------------------
    res = author.admin(
        'mutation { upsertRelationship(input: {id: "orders-customers", sourceTableId: "orders", '
        'targetTableId: "customers", sourceColumn: "id", targetColumn: "id", '
        'cardinality: "many-to-one"}) { success message } }'
    )
    print(f"[{tag}] upsertRelationship -> {json.dumps(res)[:200]}")
    observe(
        order,
        lambda w: (lambda r: (not _refused(r), r))(
            w.data("{ s__orders { id customer { id } } }", "org_admin")
        ),
        f"{tag}: relationship (nested field queryable)",
        rows,
    )

    print(f"[{tag}] schema_version per worker after:  {versions()}")
    for w in order:
        w.conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-port", type=int, required=True)
    ap.add_argument("--pg-host", default="127.0.0.1")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--second-launch", action="store_true")
    args = ap.parse_args()

    import sqlalchemy as sa

    boot = WorkerBoot(args.workers, pg_host=args.pg_host, pg_port=args.pg_port)
    boot.create_database()
    own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        conn.execute(sa.text("CREATE TABLE public.customers (id integer PRIMARY KEY, name text)"))
        conn.execute(sa.text("INSERT INTO public.customers VALUES (1, 'ann'), (2, 'bob')"))
    own.dispose()
    rows: list = []
    second = None
    try:
        boot.start()
        boot.wait_all_ready()
        if args.second_launch:
            # A second instance on the SAME control plane, up before any change is made.
            second = WorkerBoot(
                1,
                pg_host=args.pg_host,
                pg_port=args.pg_port,
                database=boot.database,
                data_dir=boot.data_dir,
            )
            second.start()
            second.wait_all_ready()
            mine = one_connection_per_worker(boot.ports["http"], boot.workers)
            # Changes are authored on launch A's first worker; observed there, on one sibling
            # worker of launch A, and on launch B.
            print("[two launches] columns: A.author  A.sibling  B")
            run([mine[0], mine[1], Worker(second.ports["http"])], rows, "two launches")
        else:
            print("[one launch] columns: author worker, then the other three")
            run(one_connection_per_worker(boot.ports["http"], boot.workers), rows, "one launch")
        print("\nchange".ljust(62) + "at 0s".ljust(14) + "at 20s".ljust(14) + "at 70s")
        for label, *cells in rows:
            print(label.ljust(61) + "".join(c.ljust(14) for c in cells))
    finally:
        if second is not None:
            second.stop()
        boot.cleanup()


if __name__ == "__main__":
    main()
