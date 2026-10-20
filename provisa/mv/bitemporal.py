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
SOURCE time (REQ-1163 as amended, REQ-2064) is the clock reads travel on: the view declares the
column holding the source's own change time (an updated_at, or the commit time a CDC landing
carries), and a version is in force from its source time to the next version's. The recorded time
stays beside it as the answer to when Provisa knew.
VALID time (business time — when a fact is true) is NOT computed here: the view's own SELECT
supplies the ``valid_from``/``valid_to`` columns and this module only preserves and queries them.

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
        managed = [self.system_column, *([self.op_column] if self.mode == MODE_DELTA else [])]
        valid = [c for c in (self.valid_from, self.valid_to, self.source_column) if c]
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
) -> list[str]:
    cols = ", ".join(_q(c) for c in business_cols)
    sys, op = _q(spec.system_column), _q(spec.op_column)
    current = current_state_sql(target, spec, business_cols)  # the engine reconstructs "now"

    # Upserts: fresh rows not identical to any currently-effective row (new keys + changed rows).
    f_cols = ", ".join(f"{_q('f')}.{_q(c)}" for c in business_cols)
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
        [f"{_q('c')}.{_q(c)}" for c in key] + [f"NULL AS {_q(c)}" for c in non_key]
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


#: REQ-2064: the columns every read of a bitemporal view answers beside its own: the version's
#: source time, the next version's (NULL while it is in force), and when Provisa recorded it.
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
    business_key: list[str], recorded_time: str | None, columns: list[str]
) -> BitemporalSpec:
    """REQ-2109: how a view declaring business_key, valid_time and recorded_time keeps its
    versions. Provisa picks the mechanism: each refresh appends what changed (delta) keyed on the
    business key, read on the recorded time."""
    return BitemporalSpec(
        key=tuple(business_key),
        mode=MODE_DELTA,
        source_column=recorded_time,
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
    """A request's time travel (REQ-1163, REQ-2064), as validated SQL timestamp literals.

    On the view's clock (its source time, else its recorded time): a point (``end_sql`` None)
    answers the version of each key in force then; a range ``[start_sql, end_sql)`` answers every
    version in force at any time within it; neither (``start_sql`` None) the version in force now.
    ``recorded_sql``, when given, is a second axis: the view as Provisa had recorded it at that
    time, every version recorded later left out -- so a correction recorded since does not change
    what is answered about the past."""

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


def _versions_sql(mv_ref: str, spec: BitemporalSpec, recorded_sql: str | None = None) -> str:
    """Each version of each key with the span it is in force on source time: ``_bt_from`` its
    source time, ``_bt_to`` the next version's (NULL while current). Tombstones end a span and are
    not versions themselves. With ``recorded_sql`` the log is read as it stood then: every row
    recorded later is left out before any span is drawn (REQ-1163, as recorded)."""
    sys = _q(spec.system_column)
    if recorded_sql is not None:
        mv_ref = f"(SELECT * FROM {mv_ref} WHERE {sys} <= {recorded_sql})"
    # REQ-1163 (amended): a view that names no source-time column travels on Provisa's recorded
    # time instead -- its declared clock, which depends on refresh timing, not on when the source
    # changed.
    src = sys if spec.source_column is None else _q(spec.source_column)
    part = ", ".join(_q(c) for c in spec.key)
    if spec.is_delta:
        op = _q(spec.op_column)
        # REQ-2064: a tombstone has no source change time -- a disappearance is known only when it
        # was recorded -- so it takes effect, ending the version before it, at its recorded time.
        eff = f"CASE WHEN {op} = '{_OP_DELETE}' THEN {sys} ELSE {src} END"
        spans = (
            f"SELECT *, {eff} AS _bt_from, LEAD({eff}) OVER "
            f"(PARTITION BY {part} ORDER BY {eff}, {sys}) AS _bt_to FROM {mv_ref} _bl"
        )
        return f"SELECT * FROM ({spans}) _bv WHERE _bv.{op} <> '{_OP_DELETE}'"
    # Snapshot: each refresh appends the whole dataset, so a row recorded again unchanged is no
    # new version. A recording whose content differs from the one before it at the same (key,
    # source time) is a correction; of those, the latest recorded is in force -- as a delta view's
    # is -- and its record is when that content was first recorded. A key absent from a later
    # batch is gone from that batch's recorded time.
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
    same_version = f"PARTITION BY {part}, {src} ORDER BY {sys}"
    changed = " OR ".join(
        [f"LAG({sys}) OVER ({same_version}) IS NULL"]
        + [f"{_q(c)} IS DISTINCT FROM LAG({_q(c)}) OVER ({same_version})" for c in content]
    )
    marked = (
        f"SELECT *, CASE WHEN {changed} THEN 1 ELSE 0 END AS _bt_changed, "
        f"MAX({sys}) OVER (PARTITION BY {part}) AS _bt_last_seen FROM {mv_ref} _bl"
    )
    latest = (
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {part}, {src} ORDER BY {sys} DESC) "
        f"AS _bt_latest FROM ({marked}) _bm WHERE _bm._bt_changed = 1"
    )
    gone = f"(SELECT MIN(_bb.{sys}) FROM {mv_ref} _bb WHERE _bb.{sys} > _bd._bt_last_seen)"
    return (
        f"SELECT *, {src} AS _bt_from, COALESCE(LEAD({src}) OVER "
        f"(PARTITION BY {part} ORDER BY {src}, {sys}), {gone}) AS _bt_to "
        f"FROM ({latest}) _bd WHERE _bd._bt_latest = 1"
    )


def view_read_sql(mv_ref: str, spec: BitemporalSpec, as_of: AsOf | None = None) -> str:
    """A SELECT over the append log that yields the versions a read answers, on source time
    (REQ-1163, REQ-2064): the version of each key in force now (``as_of`` None) or at a point, or
    every version in force within a range -- each with its source time, the next version's and its
    recorded time (``READ_COLUMNS``). Used where a bitemporal view's read is expanded inline.

    It projects ``*`` (business and system columns), because the caller inlines it as
    ``(<this>) alias`` and the outer consumer query selects columns by name."""
    if as_of is None or as_of.start_sql is None:
        when = "_bt._bt_to IS NULL"
    elif as_of.end_sql is None:
        t = as_of.start_sql
        when = f"_bt._bt_from <= {t} AND (_bt._bt_to IS NULL OR _bt._bt_to > {t})"
    else:
        when = (
            f"_bt._bt_from < {as_of.end_sql} "
            f"AND (_bt._bt_to IS NULL OR _bt._bt_to > {as_of.start_sql})"
        )
    return (
        f"SELECT *, _bt._bt_from AS {_q(SOURCE_FROM)}, _bt._bt_to AS {_q(SOURCE_TO)}, "
        f"_bt.{_q(spec.system_column)} AS {_q(RECORDED_AT)} "
        f"FROM ({_versions_sql(mv_ref, spec, as_of.recorded_sql if as_of else None)}) _bt "
        f"WHERE {when}"
    )


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
    entry is replaced with an as-of reconstruction over its append log, so the whole query reads as of
    ``as_of``. Non-bitemporal views are untouched. Returns a new map (input not mutated)."""
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
