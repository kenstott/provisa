# Copyright (c) 2026 Kenneth Stott
# Canary: f0ee5c0d-42e9-4613-b23e-479c1d748b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Engine-agnostic, APPEND-ONLY bitemporal materialization for MVs (REQ-1159).

Time travel is a simple idea — keep every version of a row instead of overwriting it — but
engines realize it with very different efficiency. This module expresses the idea ONCE, as
ordinary SQL the underlying engine runs, so the same MV definition works on any materializing
backend.

TWO HARD RULES shape the design:

1. PURE APPEND — never an UPDATE and never a DELETE-of-history. A version, once written, is
   immutable. "Which version was in effect at time T" is DERIVED at read time, never stamped by
   mutating an earlier row. This keeps materialization to the one write every engine does cheaply
   (INSERT ... SELECT) and makes the store an immutable log.

2. EXACTLY TWO WRITE CHOICES per refresh:
     - SNAPSHOT — append the COMPLETE fresh dataset, stamped with this refresh's system time.
       No diff at all. History is the sequence of whole snapshots; a delete is simply a key's
       absence from a later snapshot. Simple and universal; storage grows by full size per refresh.
     - DELTA — append ONLY what changed since the current reconstructed state: new/changed rows as
       upserts, disappeared keys as tombstones. The DELTA IS COMPUTED BY THE ENGINE (anti-joins in
       an INSERT ... SELECT), never folded in Provisa. Smaller storage; needs a business key.

SYSTEM time (transaction time — when Provisa recorded it) is Provisa-managed via one append stamp.
VALID time (business time — when a fact is true) is NOT computed here: the view's own SELECT
supplies the ``valid_from``/``valid_to`` columns and this module only preserves and queries them.

Efficiency varies by engine — a MERGE/UPDATE store could maintain fewer rows, an Iceberg store
could lean on native snapshots — which is exactly why the strategy is DECLARED, not hardcoded.
The append-only path here is the portable floor that is correct on every engine.
"""

from __future__ import annotations

from dataclasses import dataclass

# The two write choices (REQ-1159).
MODE_SNAPSHOT = "snapshot"
MODE_DELTA = "delta"
MODES = frozenset({MODE_SNAPSHOT, MODE_DELTA})

# Dialects whose null-safe equality operator is `<=>` rather than ANSI `IS NOT DISTINCT FROM`.
_SPACESHIP_DIALECTS = frozenset({"mysql", "mariadb"})
_OP_UPSERT = "upsert"
_OP_DELETE = "delete"


@dataclass(frozen=True)
class BitemporalSpec:
    """How an MV keeps history. ``key`` is the business identity a version belongs to.
    ``mode`` is snapshot vs delta. ``system_column`` is the Provisa-managed append stamp.
    ``op_column`` (delta only) marks upsert vs tombstone. The valid-time columns, when named,
    are supplied by the view SELECT and only preserved/queried here."""

    key: tuple[str, ...]
    mode: str = MODE_SNAPSHOT
    system_column: str = "sys_recorded_at"
    op_column: str = "sys_op"
    valid_from: str | None = None
    valid_to: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"invalid bitemporal mode {self.mode!r}; expected one of {sorted(MODES)}")
        if self.mode == MODE_DELTA and not self.key:
            raise ValueError("delta bitemporal mode requires a business key (no key ⇒ no delta)")
        managed = [self.system_column, *([self.op_column] if self.mode == MODE_DELTA else [])]
        valid = [c for c in (self.valid_from, self.valid_to) if c]
        allcols = managed + valid
        if len(set(allcols)) != len(allcols):
            raise ValueError(f"bitemporal column names must be distinct: {allcols}")

    @property
    def is_delta(self) -> bool:
        return self.mode == MODE_DELTA


def _q(ident: str) -> str:
    """Double-quote an identifier (ANSI). Doubles embedded quotes."""
    return '"' + ident.replace('"', '""') + '"'


def _null_safe_eq(left: str, right: str, dialect: str) -> str:
    if dialect in _SPACESHIP_DIALECTS:
        return f"{left} <=> {right}"
    return f"{left} IS NOT DISTINCT FROM {right}"


def _match(cols: list[str], la: str, ra: str, dialect: str) -> str:
    lq, rq = _q(la), _q(ra)
    return " AND ".join(_null_safe_eq(f"{lq}.{_q(c)}", f"{rq}.{_q(c)}", dialect) for c in cols)


def system_columns_ddl(spec: BitemporalSpec) -> list[tuple[str, str]]:
    """The Provisa-managed columns to add to the landed table, as (name, sql_type) pairs."""
    cols = [(spec.system_column, "TIMESTAMP")]
    if spec.is_delta:
        cols.append((spec.op_column, "VARCHAR"))
    return cols


def create_sql(target: str, select_sql: str, spec: BitemporalSpec, now_ts: str) -> str:
    """First materialization: append the whole fresh dataset as the initial state (both modes)."""
    stamp = f"{now_ts} AS {_q(spec.system_column)}"
    if spec.is_delta:
        stamp += f", '{_OP_UPSERT}' AS {_q(spec.op_column)}"
    return f"CREATE TABLE {target} AS SELECT _s.*, {stamp} FROM ({select_sql}) _s"


def append_sql(
    target: str,
    select_sql: str,
    spec: BitemporalSpec,
    business_cols: list[str],
    now_ts: str,
    dialect: str,
) -> list[str]:
    """Ordered APPEND-ONLY statements advancing the versioned table by one refresh.

    Snapshot: one INSERT of the whole fresh dataset. Delta: one INSERT of upsert rows (fresh rows
    with no identical currently-effective row — the engine computes this via anti-join) and one
    INSERT of tombstones (currently-effective keys absent from the fresh set). No UPDATE, no DELETE.
    """
    if not spec.is_delta:
        return [_append_snapshot(target, select_sql, spec, business_cols, now_ts)]
    return _append_delta(target, select_sql, spec, business_cols, now_ts, dialect)


def _append_snapshot(
    target: str, select_sql: str, spec: BitemporalSpec, business_cols: list[str], now_ts: str
) -> str:
    cols = ", ".join(_q(c) for c in business_cols)
    f_cols = ", ".join(f'{_q("f")}.{_q(c)}' for c in business_cols)
    sys = _q(spec.system_column)
    return (
        f"INSERT INTO {target} ({cols}, {sys}) "
        f"SELECT {f_cols}, {now_ts} FROM ({select_sql}) {_q('f')}"
    )


def _append_delta(
    target: str,
    select_sql: str,
    spec: BitemporalSpec,
    business_cols: list[str],
    now_ts: str,
    dialect: str,
) -> list[str]:
    cols = ", ".join(_q(c) for c in business_cols)
    sys, op = _q(spec.system_column), _q(spec.op_column)
    current = current_state_sql(target, spec, business_cols)  # the engine reconstructs "now"

    # Upserts: fresh rows not identical to any currently-effective row (new keys + changed rows).
    f_cols = ", ".join(f'{_q("f")}.{_q(c)}' for c in business_cols)
    upserts = (
        f"INSERT INTO {target} ({cols}, {sys}, {op}) "
        f"SELECT {f_cols}, {now_ts}, '{_OP_UPSERT}' "
        f"FROM ({select_sql}) {_q('f')} "
        f"WHERE NOT EXISTS (SELECT 1 FROM ({current}) {_q('c')} "
        f"WHERE {_match(business_cols, 'c', 'f', dialect)})"
    )

    # Tombstones: currently-effective keys with no matching fresh row. Key columns carried, the
    # rest NULL (a tombstone records identity, not attributes); op = delete.
    key = list(spec.key)
    non_key = [c for c in business_cols if c not in spec.key]
    # Bare NULL (not CAST): the INSERT's target column already types it. An explicit cast to a
    # filler type is rejected by strict engines — Postgres won't assign varchar to an int column.
    tomb_select = ", ".join(
        [f'{_q("c")}.{_q(c)}' for c in key] + [f"NULL AS {_q(c)}" for c in non_key]
    )
    ordered = ", ".join(_q(c) for c in (key + non_key))
    tombstones = (
        f"INSERT INTO {target} ({ordered}, {sys}, {op}) "
        f"SELECT {tomb_select}, {now_ts}, '{_OP_DELETE}' "
        f"FROM ({current}) {_q('c')} "
        f"WHERE NOT EXISTS (SELECT 1 FROM ({select_sql}) {_q('f')} "
        f"WHERE {_match(key, 'c', 'f', dialect)})"
    )
    return [upserts, tombstones]


def current_state_sql(target: str, spec: BitemporalSpec, business_cols: list[str]) -> str:
    """A SELECT of the business columns as CURRENTLY effective (system-time now), engine-computed.

    Snapshot: the rows of the latest appended batch (max system stamp). Delta: the latest version
    per key (window over system stamp), excluding tombstones."""
    return reconstruct_as_of_sql(target, spec, business_cols, ts_sql=None)


def reconstruct_as_of_sql(
    target: str, spec: BitemporalSpec, business_cols: list[str], ts_sql: str | None
) -> str:
    """A SELECT of the business columns as effective at system time ``ts_sql`` (None ⇒ now).

    Reconstruction is READ-ONLY and derives the effective set from the immutable append log — no
    stored ``system_to``, no UPDATE ever having run."""
    cols = ", ".join(_q(c) for c in business_cols)
    sys = _q(spec.system_column)
    upto = f" WHERE {sys} <= {ts_sql}" if ts_sql else ""

    if not spec.is_delta:
        # Latest whole snapshot at-or-before ts: the batch whose stamp is the max ≤ ts.
        max_batch = f"SELECT MAX({sys}) FROM {target}{upto}"
        return f"SELECT {cols} FROM {target} WHERE {sys} = ({max_batch})"

    # Delta: newest version per key ≤ ts, dropping tombstones.
    op = _q(spec.op_column)
    part = ", ".join(_q(c) for c in spec.key)
    ranked = (
        f"SELECT {cols}, {op}, ROW_NUMBER() OVER "
        f"(PARTITION BY {part} ORDER BY {sys} DESC) AS _rn "
        f"FROM {target}{upto}"
    )
    return f"SELECT {cols} FROM ({ranked}) _v WHERE _v._rn = 1 AND _v.{op} <> '{_OP_DELETE}'"


def as_of_valid_predicate(spec: BitemporalSpec, ts_sql: str, alias: str | None = None) -> str:
    """Filter to rows valid (business time) at ``ts_sql``. Requires the view to supply valid-time
    columns; raises rather than silently ignoring the request."""
    if not (spec.valid_from and spec.valid_to):
        raise ValueError(
            "as_of_valid requires the view to declare valid_from/valid_to columns, but the "
            "BitemporalSpec has none"
        )
    p = f"{_q(alias)}." if alias else ""
    vf, vt = f"{p}{_q(spec.valid_from)}", f"{p}{_q(spec.valid_to)}"
    return f"{vf} <= {ts_sql} AND ({vt} IS NULL OR {vt} > {ts_sql})"
