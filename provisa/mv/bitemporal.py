# Copyright (c) 2026 Kenneth Stott
# Canary: f0ee5c0d-42e9-4613-b23e-479c1d748b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Engine-agnostic, APPEND-ONLY bitemporal materialization for MVs (REQ-1162).

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
       an INSERT ... SELECT), never folded in Provisa. Smaller storage; needs an entity key.

SYSTEM time (transaction time — when Provisa recorded it) is Provisa-managed via one append stamp.
VALID time (business time — when a fact is true) is the clock reads travel on (REQ-1163 as amended
2026-10-20, REQ-2064, REQ-2109): the view's own column holding it -- the ``valid_time`` it declares,
or for a view that inherits its versions from the one versioned table it groups
(:mod:`provisa.mv.bitemporal_inherit`), its output column that is that table's valid time
unchanged. A read answers, per key, the latest recorded version among those valid at or before a
point, or every version valid within a range. The RECORDED clock is a second axis (as recorded):
the view's declared ``recorded_time`` column, else the append stamp.

Efficiency varies by engine — a MERGE/UPDATE store could maintain fewer rows, an Iceberg store
could lean on native snapshots — which is exactly why the strategy is DECLARED, not hardcoded.
The append-only path here is the portable floor that is correct on every engine.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

# The two write choices (REQ-1162).
MODE_SNAPSHOT = "snapshot"
MODE_DELTA = "delta"
MODES = frozenset({MODE_SNAPSHOT, MODE_DELTA})

# Dialects whose null-safe equality operator is `<=>` rather than ANSI `IS NOT DISTINCT FROM`.
_SPACESHIP_DIALECTS = frozenset({"mysql", "mariadb"})
_OP_UPSERT = "upsert"
_OP_DELETE = "delete"
#: REQ-2109: the managed column an inherited view's every appended row carries -- the latest
#: recorded time of its input that append read. Its maximum over the log is the view's watermark,
#: so the watermark advances only with an append that commits and survives a restart.
WATERMARK_COLUMN = "sys_input_recorded"


@dataclass(frozen=True)
class InheritedInput:
    """REQ-2109: the one versioned table a view inherits its versions from: its reference as the
    view's SQL spells it, and its own valid and recorded time columns."""

    ref: str
    valid_time: str
    recorded_time: str


@dataclass(frozen=True)
class BitemporalSpec:
    """How an MV keeps history. ``key`` is the business identity a version belongs to.
    ``mode`` is snapshot vs delta. ``system_column`` is the Provisa-managed append stamp.
    ``op_column`` (delta only) marks upsert vs tombstone. The valid-time columns, when named,
    are supplied by the view SELECT and only preserved/queried here."""

    key: tuple[str, ...]
    mode: str = MODE_SNAPSHOT
    # REQ-1163: the view's column holding the source's own change time -- the clock of its reads.
    source_column: str | None = None
    system_column: str = "sys_recorded_at"
    op_column: str = "sys_op"
    valid_from: str | None = None
    valid_to: str | None = None
    # REQ-2109: the view's column holding its valid time -- the clock its reads travel on.
    valid_column: str | None = None
    # REQ-2109: set when the view inherits its versions from one versioned table (no declaration).
    inherited: InheritedInput | None = None
    # The view's declared columns: what a snapshot view's read compares to tell a row recorded
    # again unchanged from a correction (the read columns and system columns are left out).
    columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError(
                f"invalid bitemporal mode {self.mode!r}; expected one of {sorted(MODES)}"
            )
        if self.mode == MODE_DELTA and not self.key:
            raise ValueError("delta bitemporal mode requires an entity key (no key ⇒ no delta)")
        if self.inherited is not None and not (self.is_delta and self.valid_column):
            raise ValueError("an inherited bitemporal view appends deltas on its valid column")
        managed = [
            self.system_column,
            *([self.op_column] if self.mode == MODE_DELTA else []),
            *([self.watermark_column] if self.watermark_column else []),
        ]
        valid = [c for c in (self.valid_from, self.valid_to, self.source_column) if c]
        allcols = managed + valid
        if len(set(allcols)) != len(allcols):
            raise ValueError(f"bitemporal column names must be distinct: {allcols}")

    @property
    def is_delta(self) -> bool:
        return self.mode == MODE_DELTA

    @property
    def watermark_column(self) -> str | None:
        """The managed watermark column (REQ-2109): an inherited view's only."""
        return WATERMARK_COLUMN if self.inherited is not None else None

    @property
    def recorded_clock(self) -> str:
        """The column an as-recorded read cuts the log on: the declared recorded time, else the
        append stamp."""
        return self.source_column or self.system_column


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
    if spec.watermark_column:
        cols.append((spec.watermark_column, "TIMESTAMP"))
    return cols


def _watermark(spec: BitemporalSpec, watermark_sql: str | None) -> str | None:
    """The watermark stamp an inherited view's append carries, as SQL; None for any other view.
    An inherited view's append without one is a defect, refused."""
    if spec.watermark_column is None:
        return None
    if watermark_sql is None:
        raise ValueError("an inherited bitemporal view's append carries its input's watermark")
    return f"CAST({watermark_sql} AS TIMESTAMP)"


def create_sql(
    target: str,
    select_sql: str,
    spec: BitemporalSpec,
    now_ts: str,
    watermark_sql: str | None = None,
) -> str:
    """First materialization: append the whole fresh dataset as the initial state (both modes).
    ``watermark_sql`` is an inherited view's watermark stamp (REQ-2109)."""
    stamp = f"{now_ts} AS {_q(spec.system_column)}"
    if spec.is_delta:
        stamp += f", '{_OP_UPSERT}' AS {_q(spec.op_column)}"
    wm = _watermark(spec, watermark_sql)
    if wm is not None:
        stamp += f", {wm} AS {_q(WATERMARK_COLUMN)}"
    return f"CREATE TABLE {target} AS SELECT _s.*, {stamp} FROM ({select_sql}) _s"


def append_sql(
    target: str,
    select_sql: str,
    spec: BitemporalSpec,
    business_cols: list[str],
    now_ts: str,
    dialect: str,
    *,
    watermark_sql: str | None = None,
    scope: str | None = None,
) -> list[str]:
    """Ordered APPEND-ONLY statements advancing the versioned table by one refresh.

    Snapshot: one INSERT of the whole fresh dataset. Delta: one INSERT of upsert rows (fresh rows
    with no identical currently-effective row — the engine computes this via anti-join) and one
    INSERT of tombstones (currently-effective keys absent from the fresh set). No UPDATE, no DELETE.

    REQ-2109, an inherited view: ``watermark_sql`` stamps its upserts; ``scope`` (a condition on
    the current state, alias ``c``) confines its tombstones to the valid dates the refresh rebuilt,
    since the fresh set holds only those.
    """
    if not spec.is_delta:
        return [_append_snapshot(target, select_sql, spec, business_cols, now_ts)]
    return _append_delta(
        target, select_sql, spec, business_cols, now_ts, dialect, watermark_sql, scope
    )


def _append_snapshot(
    target: str, select_sql: str, spec: BitemporalSpec, business_cols: list[str], now_ts: str
) -> str:
    cols = ", ".join(_q(c) for c in business_cols)
    f_cols = ", ".join(f"{_q('f')}.{_q(c)}" for c in business_cols)
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
    watermark_sql: str | None = None,
    scope: str | None = None,
) -> list[str]:
    cols = ", ".join(_q(c) for c in business_cols)
    sys, op = _q(spec.system_column), _q(spec.op_column)
    current = current_state_sql(target, spec, business_cols)  # the engine reconstructs "now"
    wm = _watermark(spec, watermark_sql)
    # REQ-2109: an inherited view's watermark is carried by its upserts alone, appended last: a
    # failed upsert leaves the watermark where it was, so the next refresh reads the same changes
    # again; a tombstone carries none (NULL).
    wm_col = f", {_q(WATERMARK_COLUMN)}" if wm is not None else ""

    # Upserts: fresh rows not identical to any currently-effective row (new keys + changed rows).
    f_cols = ", ".join(f"{_q('f')}.{_q(c)}" for c in business_cols)
    upserts = (
        f"INSERT INTO {target} ({cols}, {sys}, {op}{wm_col}) "
        f"SELECT {f_cols}, {now_ts}, '{_OP_UPSERT}'{f', {wm}' if wm is not None else ''} "
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
        [f"{_q('c')}.{_q(c)}" for c in key] + [f"NULL AS {_q(c)}" for c in non_key]
    )
    ordered = ", ".join(_q(c) for c in (key + non_key))
    scoped = f"({scope}) AND " if scope is not None else ""
    tombstones = (
        f"INSERT INTO {target} ({ordered}, {sys}, {op}) "
        f"SELECT {tomb_select}, {now_ts}, '{_OP_DELETE}' "
        f"FROM ({current}) {_q('c')} "
        f"WHERE {scoped}NOT EXISTS (SELECT 1 FROM ({select_sql}) {_q('f')} "
        f"WHERE {_match(key, 'c', 'f', dialect)})"
    )
    if wm is not None:
        return [tombstones, upserts]
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


#: REQ-2064: the columns every read of a bitemporal view answers beside its own: the version's
#: valid time, the valid time of its key's next version (NULL for the last), and when Provisa
#: recorded it.
SOURCE_FROM = "source_from"
SOURCE_TO = "source_to"
RECORDED_AT = "recorded_at"
READ_COLUMNS = (SOURCE_FROM, SOURCE_TO, RECORDED_AT)
_READ_DESCRIPTIONS = {
    SOURCE_FROM: "When this version took effect",
    SOURCE_TO: "When the next version took effect; empty while this one is in force",
    RECORDED_AT: "When Provisa recorded this version",
}


def declared_spec(
    business_key: list[str], valid_time: str, recorded_time: str, columns: list[str]
) -> BitemporalSpec:
    """REQ-2109: how a view declaring business_key, valid_time and recorded_time keeps its
    versions. Provisa picks the mechanism: each refresh appends what changed (delta) keyed on the
    business key; its reads travel on the valid time and are cut as recorded on the recorded
    time."""
    return BitemporalSpec(
        key=tuple(business_key),
        mode=MODE_DELTA,
        source_column=recorded_time,
        valid_column=valid_time,
        columns=tuple(columns),
    )


def with_read_columns(columns: list[dict], key: list[str], source_time: str | None) -> list[dict]:
    """A bitemporal view's columns (as a config or a save gives them, ``name`` and ``visible_to``
    each) with the three read columns it answers (REQ-2064) appended where missing, granted as its
    clock column is -- its source-time column, else the first of its key."""
    names = {c["name"] for c in columns}
    clock = source_time or (key[0] if key else None)
    granted = next((c.get("visible_to") for c in columns if c["name"] == clock), None)
    if granted is None:
        return columns
    return columns + [
        {
            "name": name,
            "data_type": "timestamp",
            "visible_to": list(granted),
            "description": _READ_DESCRIPTIONS[name],
        }
        for name in READ_COLUMNS
        if name not in names
    ]


@dataclass(frozen=True)
class AsOf:
    """A request's time travel (REQ-1163 as amended 2026-10-20, REQ-2064), as validated SQL
    timestamp literals.

    On the view's valid time: a point (``end_sql`` None) answers, of each key's versions valid at
    or before it, the latest recorded; a range ``[start_sql, end_sql)`` answers every version
    whose valid time falls in it; neither (``start_sql`` None) the latest version of each key.
    ``recorded_sql``, when given, is a second axis: the log as it was recorded then, every version
    recorded later left out first -- so a correction recorded since does not change what is
    answered about the past."""

    start_sql: str | None
    end_sql: str | None = None
    recorded_sql: str | None = None

    @property
    def is_range(self) -> bool:
        return self.end_sql is not None

    def key(self) -> str:
        """Its canonical form, the one a request's as-of travels the pipeline in -- its cache
        keys, its kept statements, its record -- turned back by :meth:`from_key` where it is read:
        ``<start>[/<end>]`` on the view's clock, then ``@<recorded>`` when recorded is given."""
        travel = "" if self.start_sql is None else self.start_sql
        if self.end_sql is not None:
            travel = f"{travel}/{self.end_sql}"
        return travel if self.recorded_sql is None else f"{travel}@{self.recorded_sql}"

    @classmethod
    def from_key(cls, key: str) -> "AsOf":
        """The as-of a canonical form names; anything else is refused (never spliced)."""
        travel, at, recorded = key.partition("@")
        parts = travel.split("/") if travel else []
        literals = [*parts, *([recorded] if at else [])]
        if (
            len(parts) > 2
            or (not parts and not at)
            or not all(_LITERAL.fullmatch(p) for p in literals)
        ):
            raise ValueError(f"not a canonical as-of: {key!r}")
        return cls(
            parts[0] if parts else None,
            parts[1] if len(parts) == 2 else None,
            recorded if at else None,
        )


def _content_columns(spec: BitemporalSpec) -> list[str]:
    """A snapshot view's declared business columns: what tells a row recorded again unchanged
    from a new version, and what its derived tombstones are written over."""
    content = [
        c
        for c in spec.columns
        if c not in READ_COLUMNS and c not in (spec.system_column, spec.op_column)
    ]
    if not content:
        raise ValueError(
            "a snapshot bitemporal view's read names no columns: its declared columns tell a "
            "row recorded again unchanged from a correction"
        )
    return content


def _snapshot_log_sql(mv_ref: str, spec: BitemporalSpec) -> str:
    """A snapshot view's log as versions and tombstones, as a delta view's is: of each key's
    recordings, those whose content differs from the one before it (or that follow a batch the key
    was absent from), and a tombstone at the first batch a key is absent from after one it was
    in."""
    content = _content_columns(spec)
    sys = _q(spec.system_column)
    part = ", ".join(_q(c) for c in spec.key)
    by_key = f"PARTITION BY {part} ORDER BY {sys}"
    changed = " OR ".join(
        ["_bn._bt_prev IS DISTINCT FROM _bn._bt_batch - 1"]
        + [f"_bn.{_q(c)} IS DISTINCT FROM _bn.{_q('_bt_lag_' + c)}" for c in content]
    )
    lags = ", ".join(f"LAG({_q(c)}) OVER ({by_key}) AS {_q('_bt_lag_' + c)}" for c in content)
    batches = f"SELECT {sys}, DENSE_RANK() OVER (ORDER BY {sys}) AS _bt_batch FROM {mv_ref}"
    numbered = (
        f"SELECT _bl.*, _bb._bt_batch FROM {mv_ref} _bl "
        f"JOIN (SELECT DISTINCT * FROM ({batches}) _bd) _bb ON _bb.{sys} = _bl.{sys}"
    )
    marked = (
        f"SELECT *, LAG(_bt_batch) OVER ({by_key}) AS _bt_prev, "
        f"LEAD(_bt_batch) OVER ({by_key}) AS _bt_next, {lags} FROM ({numbered}) _bm"
    )
    versions = (
        f"SELECT {', '.join(f'_bn.{_q(c)}' for c in content)}, _bn.{sys}, 0 AS _bt_gone "
        f"FROM ({marked}) _bn WHERE {changed}"
    )
    tomb_cols = ", ".join(f"_bn.{_q(c)}" if c in spec.key else f"NULL AS {_q(c)}" for c in content)
    tombstones = (
        f"SELECT {tomb_cols}, _bx.{sys}, 1 AS _bt_gone FROM ({marked}) _bn "
        f"JOIN (SELECT DISTINCT * FROM ({batches}) _bd) _bx ON _bx._bt_batch = _bn._bt_batch + 1 "
        f"WHERE _bn._bt_next IS DISTINCT FROM _bn._bt_batch + 1"
    )
    return f"{versions} UNION ALL {tombstones}"


def _versions_sql(mv_ref: str, spec: BitemporalSpec, recorded_sql: str | None = None) -> str:
    """Every version and tombstone of the log, each with ``_bt_gone`` (1 for a tombstone) and
    ``_bt_clock`` (when it was recorded: the view's recorded clock, a tombstone's append stamp --
    a disappearance is known only when it was recorded). With ``recorded_sql`` the log is read as
    it stood then: every row recorded later is left out first (REQ-1163, as recorded)."""
    sys = _q(spec.system_column)
    src = _q(spec.recorded_clock)
    if spec.is_delta:
        op = _q(spec.op_column)
        log = (
            f"SELECT *, CASE WHEN {op} = '{_OP_DELETE}' THEN 1 ELSE 0 END AS _bt_gone "
            f"FROM {mv_ref} _bl"
        )
    else:
        log = _snapshot_log_sql(mv_ref, spec)
    clocked = (
        f"SELECT *, CASE WHEN _bg._bt_gone = 1 THEN _bg.{sys} ELSE _bg.{src} END AS _bt_clock "
        f"FROM ({log}) _bg"
    )
    if recorded_sql is None:
        return clocked
    return f"SELECT * FROM ({clocked}) _bc WHERE _bc._bt_clock <= {recorded_sql}"


def view_read_sql(mv_ref: str, spec: BitemporalSpec, as_of: AsOf | None = None) -> str:
    """A SELECT over the append log that yields the versions a read answers, on valid time
    (REQ-1163 as amended 2026-10-20, REQ-2064, REQ-2109): the latest recorded version of each key
    (``as_of`` None), of those valid at or before a point, or every version valid within a range
    -- each with its valid time, its key's next version's and when Provisa recorded it
    (``READ_COLUMNS``). A key whose latest is a tombstone is answered by none. Used where a
    bitemporal view's read is expanded inline.

    It projects ``*`` (business and system columns), because the caller inlines it as
    ``(<this>) alias`` and the outer consumer query selects columns by name."""
    sys = _q(spec.system_column)
    # A spec that names no valid column travels on when Provisa recorded each version, as it did
    # before valid time was declared (REQ-1163 as amended 2026-10-16).
    valid = sys if spec.valid_column is None else _q(spec.valid_column)
    part = ", ".join(_q(c) for c in spec.key)
    versions = _versions_sql(mv_ref, spec, as_of.recorded_sql if as_of else None)
    # REQ-2064: a tombstone of a key that does not hold the valid time has none of its own -- a
    # disappearance is known only when it was recorded -- so it takes effect at its append stamp.
    effective = (
        f"CASE WHEN _be._bt_gone = 1 AND _be.{valid} IS NULL THEN _be.{sys} ELSE _be.{valid} END"
    )
    timed = f"SELECT *, {effective} AS _bt_eff FROM ({versions}) _be"
    # Each version's valid time, and the time its key's next version (or tombstone) took effect.
    spans = (
        f"SELECT *, _bs.{valid} AS _bt_from, CASE WHEN _bs._bt_gone = 0 THEN LEAD(_bs._bt_eff) "
        f"OVER (PARTITION BY {part} ORDER BY _bs._bt_eff, _bs.{sys}) END AS _bt_to "
        f"FROM ({timed}) _bs"
    )
    read = (
        f"SELECT *, _bt._bt_from AS {_q(SOURCE_FROM)}, _bt._bt_to AS {_q(SOURCE_TO)}, "
        f"_bt.{sys} AS {_q(RECORDED_AT)} FROM "
    )
    if as_of is not None and as_of.is_range:
        return (
            f"{read}({spans}) _bt WHERE _bt._bt_gone = 0 "
            f"AND _bt.{valid} >= {as_of.start_sql} AND _bt.{valid} < {as_of.end_sql}"
        )
    upto = (
        f" WHERE _bp._bt_eff <= {as_of.start_sql}"
        if as_of is not None and as_of.start_sql is not None
        else ""
    )
    ranked = (
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY _bp.{sys} DESC, "
        f"_bp._bt_gone DESC) AS _bt_rank FROM ({spans}) _bp{upto}"
    )
    return f"{read}({ranked}) _bt WHERE _bt._bt_rank = 1 AND _bt._bt_gone = 0"


_TS_FMT = "%Y-%m-%d %H:%M:%S.%f"
#: A literal system_ts_literal writes, and nothing else.
_LITERAL = re.compile(r"TIMESTAMP '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}'")


def system_ts_literal(instant: datetime) -> str:
    """A SAFE SQL TIMESTAMP literal for a Provisa-managed append stamp (REQ-1162), fixed-format so
    there is no injection surface.

    When the stamp is a CALENDAR BOUNDARY (``window.end``) rather than wall-clock, the appended
    snapshot is DETERMINISTIC and addressable AS-OF that window — the basis for calendar-addressed
    time travel (REQ-1166/1167): a period sealed at its boundary is reproducible and a read as-of the
    boundary returns exactly that period's dataset."""
    return f"TIMESTAMP '{instant.strftime(_TS_FMT)}'"


def parse_as_of(value: str) -> AsOf:
    """Validate a request-level as-of value -- an ISO 8601 time, or a range ``start/end``
    (REQ-2064) -- into SAFE SQL TIMESTAMP literals.

    The value is spliced into SQL, so it is never trusted raw: each part is parsed to a datetime
    (raising on anything malformed) and re-emitted in a fixed format — no injection surface
    (REQ-1163)."""
    start, sep, end = value.strip().partition("/")
    if not sep:
        return AsOf(system_ts_literal(datetime.fromisoformat(start)))  # raises on bad input
    begin, finish = datetime.fromisoformat(start.strip()), datetime.fromisoformat(end.strip())
    if finish <= begin:
        raise ValueError(f"the range's end {end.strip()} is not after its start {start.strip()}")
    return AsOf(system_ts_literal(begin), system_ts_literal(finish))


def parse_as_recorded(value: str) -> str:
    """Validate a request-level as-recorded value -- one ISO 8601 time -- into a SAFE SQL
    TIMESTAMP literal (REQ-1163). A replay is of the record as it stood at one time, so a range is
    refused."""
    if "/" in value:
        raise ValueError(f"as recorded is a point in time, not a range: {value.strip()}")
    return system_ts_literal(datetime.fromisoformat(value.strip()))  # raises on bad input


def request_as_of(as_of: str | None, as_recorded: str | None) -> str | None:
    """The canonical as-of a request's two values travel the pipeline in (``AsOf.key``), or None
    when it gives neither. Each is validated here; a malformed one raises ValueError."""
    if as_of is None and as_recorded is None:
        return None
    travel = parse_as_of(as_of) if as_of is not None else AsOf(None)
    recorded = parse_as_recorded(as_recorded) if as_recorded is not None else None
    return AsOf(travel.start_sql, travel.end_sql, recorded).key()


def as_of_view_map(
    view_sql_map: dict[str, str],
    bitemporal_reads: dict[str, tuple[str, BitemporalSpec]],
    as_of: AsOf | str,
) -> dict[str, str]:
    """Overlay a request-level as-of onto the inline-expansion map (REQ-1163): each bitemporal view's
    entry is replaced with its read over its append log under ``as_of`` (:func:`view_read_sql`, on
    valid time), so the whole query reads as of ``as_of``. Non-bitemporal views are untouched.
    Returns a new map (input not mutated)."""
    if not bitemporal_reads:
        return view_sql_map
    point = AsOf.from_key(as_of) if isinstance(as_of, str) else as_of
    out = dict(view_sql_map)
    for name, (mv_ref, spec) in bitemporal_reads.items():
        out[name] = view_read_sql(mv_ref, spec, point)
    return out


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
