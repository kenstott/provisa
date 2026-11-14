# Copyright (c) 2026 Kenneth Stott
# Canary: e81f0b1a-4844-4bb0-b915-2d58e4da9966
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The customer360 demo's entity-resolution job (REQ-2034): it writes the narrow bridges.

    .venv/bin/python3 demo/named/customer360/entity_resolution/er_job.py seed
    .venv/bin/python3 demo/named/customer360/entity_resolution/er_job.py run \\
        --dsn postgresql://er_writer:er_writer@localhost:30532/customer360 \\
        --provisa http://localhost:8001

It reads every table entity_resolution.ENTITY_SOURCES declares, matches each mention to a record
of its entity type's authority, and replaces the contents of er.entity_org_bridge and
er.entity_person_bridge with one row per mention for a new match_run_id. ``run`` writes them in
the demo's Postgres and then tells Provisa, with the ``signal`` change signal
(POST /data/sources/er/signal), that they changed. ``seed`` writes db/er.sql instead: the bridges
the demo ships, one run of the job over the rows it declares (replica_rows/), so the demo starts
with them and does not match live. Both read the rows from replica_rows/.

The design is govdata's EntityBridgeListener / EntityBridgeOrganizer, its columns and tiers kept:

  organizations  identifier exact (the web domain, or the email domain unless a personal
                 mailbox's), then the normalised name exact, then Jaro-Winkler on what follows the
                 name's first word, blocked on that word
  people         exact email; else, within the person's resolved organization, last name exact
                 and then the first name exact or by Jaro-Winkler

A fuzzy score of HIGH or above is ``high``, of FLOOR or above ``low``, and below FLOOR no match.
Matching is star-shaped: each mention is matched to one authority record (the anchor), never
clustered pairwise, so the canonical id is the anchor's key and a rerun over unchanged rows gives
the same ids. An organization's anchor is the authority record of its web domain, the earliest if
several (the entry's order_by). A mention matched to no authority record keeps a synthetic id,
'h:' || md5 of its source identity, and is unresolved with its reason: no_candidate, below_floor
(with its best candidate and score), ambiguous, or excluded (a placeholder such as "TBD", or a
personal mailbox and nothing else to match by). No model is asked anything: the matching is
deterministic similarity against thresholds.
"""

# Requirements: REQ-2034

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.request
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from entity_resolution import (
    ENTITY_SOURCES,
    ORGANIZATION,
    PERSON,
    EntitySource,
    authority,
    check_columns,
    check_registry,
    entries,
    render,
)

#: A column's value as a row holds it.
Cell = str | int | float | bool | None

HERE = Path(__file__).resolve().parent
DEMO = HERE.parent

#: A fuzzy score at or above this is a ``high`` match (govdata: hand-adjudicated precision jump).
HIGH = 0.95
#: A fuzzy score below this is no match.
FLOOR = 0.85
#: Scores are written to this many places, so a rerun writes the same numbers.
SCORE_PLACES = 4
#: Mailboxes anyone can open: their domain says nothing of an organization.
PERSONAL_DOMAINS = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "outlook.com",
        "hotmail.com",
        "live.com",
        "yahoo.com",
        "icloud.com",
        "aol.com",
        "proton.me",
    }
)
#: Values a person types when they do not know the name: never matched.
PLACEHOLDERS = frozenset({"tbd", "tba", "n a", "na", "none", "unknown", "test", "todo"})
#: The run the shipped bridges (db/er.sql) are.
SEED_RUN_ID = "er-2026-11-07T06:30:00Z"
#: The Provisa source the narrow bridges are registered as (config.yaml).
BRIDGE_SOURCE = "er"

# govdata's legal suffixes, stripped up to three deep ("drilling co inc" -> "drilling").
_LEGAL_SUFFIX = re.compile(
    r"\s+(inc|llc|corp|corporation|co|company|ltd|llp|lp|plc|pc|pa|na|partnership|holdings|partners"
    r"|fund|trust|group|management)$"
)

HIGH_CONFIDENCE, LOW_CONFIDENCE, UNRESOLVED = "high", "low", "unresolved"
NO_CANDIDATE, BELOW_FLOOR, AMBIGUOUS, EXCLUDED = (
    "no_candidate",
    "below_floor",
    "ambiguous",
    "excluded",
)


# --------------------------------------------------------------------------------------------- #
# Similarity and normalisation.                                                                 #
# --------------------------------------------------------------------------------------------- #


def jaro_winkler(a: str, b: str) -> float:
    """Jaro-Winkler similarity, prefix scale 0.1 over at most four characters (as DuckDB's
    jaro_winkler_similarity, which govdata scores with). Two empty strings score 0."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    window = max(max(len(a), len(b)) // 2 - 1, 0)
    used_b = [False] * len(b)
    matched_a: list[str] = []
    for i, ch in enumerate(a):
        for j in range(max(0, i - window), min(len(b), i + window + 1)):
            if not used_b[j] and b[j] == ch:
                used_b[j] = True
                matched_a.append(ch)
                break
    if not matched_a:
        return 0.0
    matched_b = [b[j] for j in range(len(b)) if used_b[j]]
    m = len(matched_a)
    transpositions = sum(x != y for x, y in zip(matched_a, matched_b)) // 2
    jaro = (m / len(a) + m / len(b) + (m - transpositions) / m) / 3
    prefix = 0
    for x, y in zip(a[:4], b[:4]):
        if x != y:
            break
        prefix += 1
    return jaro + prefix * 0.1 * (1 - jaro)


def norm_org_name(name: str) -> str:
    """govdata's norm_org_name: lower case, '&' as 'and', punctuation as space, a leading article
    and up to three trailing legal suffixes dropped, spaces collapsed."""
    text = re.sub(r"[^a-z0-9]+", " ", name.lower().replace("&", " and ")).strip()
    text = re.sub(r"^(the|a|an) +", "", text)
    for _ in range(3):
        text = _LEGAL_SUFFIX.sub("", text)
    return re.sub(r"\s+", " ", text).strip()


def norm_person_part(name: str) -> str:
    """A first or last name compared: upper case letters only."""
    return re.sub(r"[^A-Z]", "", name.upper())


def domain_of(value: str | None, *, email: bool) -> str | None:
    """The domain of a website (scheme, www. and path dropped) or of an email address."""
    if not value or not value.strip():
        return None
    text = value.strip().lower()
    if email:
        return text.rsplit("@", 1)[1] if "@" in text else None
    text = re.sub(r"^(https?://)?(www\.)?", "", text)
    return text.split("/", 1)[0] or None


def _score(value: float) -> float:
    return round(value, SCORE_PLACES)


def synthetic_id(entry: EntitySource, key: str) -> str:
    """govdata's id for a mention matched to no authority record: 'h:' || md5(source identity)."""
    identity = f"{entry.source}.{entry.table}.{entry.key.column}.{key}"
    return "h:" + hashlib.md5(identity.encode()).hexdigest()  # noqa: S324 -- an id, not a secret


# --------------------------------------------------------------------------------------------- #
# Mentions.                                                                                     #
# --------------------------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Mention:
    """One distinct key value of a registry entry's table, read from one row of it."""

    entry: EntitySource
    key: str
    row: Mapping[str, Cell]

    def text(self, role: str) -> str | None:
        column = self.entry.reference.get(role)
        value = None if column is None else self.row.get(column)
        return None if value is None or str(value).strip() == "" else str(value).strip()

    @property
    def match_text(self) -> str:
        return render(self.entry.match_template, self.row)

    @property
    def label(self) -> str:
        return render(self.entry.label_template, self.row)


def _row_order(row: Mapping[str, Cell]) -> tuple:
    return tuple((k, "" if v is None else str(v)) for k, v in sorted(row.items()))


#: The reference roles that name the entity: a row with none of them is no mention.
_NAME_ROLES = ("name", "first_name", "last_name")


def mentions(entry: EntitySource, rows: Iterable[Mapping[str, Cell]]) -> list[Mention]:
    """The entry's mentions: one per distinct non-empty key whose row names someone, read from the
    first of its rows in a fixed order (so the same rows give the same mention in any read order), sorted by key."""
    by_key: dict[str, list[Mapping[str, Cell]]] = defaultdict(list)
    names = [entry.reference[r] for r in _NAME_ROLES if r in entry.reference]
    for row in rows:
        value = row.get(entry.key.column)
        if value is None or str(value).strip() == "":
            continue
        if all(row.get(n) is None or str(row.get(n)).strip() == "" for n in names):
            continue  # names no one: a Stripe customer with no billing contact holds no person
        by_key[str(value)].append(row)
    return [
        Mention(entry, key, min(group, key=_row_order)) for key, group in sorted(by_key.items())
    ]


@dataclass(frozen=True)
class Match:
    """What a mention was matched to, or why it was not."""

    canonical_id: str
    method: str | None
    confidence: str
    score: float | None
    reason: str | None = None
    candidate_id: str | None = None
    candidate_score: float | None = None

    @property
    def resolved(self) -> bool:
        return self.confidence != UNRESOLVED


def _matched(anchor: str, method: str, score: float) -> Match:
    confidence = HIGH_CONFIDENCE if score >= HIGH else LOW_CONFIDENCE
    return Match(anchor, method, confidence, _score(score))


def _unresolved(
    mention: Mention, reason: str, candidate: str | None = None, score: float | None = None
) -> Match:
    return Match(
        synthetic_id(mention.entry, mention.key),
        None,
        UNRESOLVED,
        None,
        reason,
        candidate,
        None if score is None else _score(score),
    )


def _best(scored: Mapping[str, float], mention: Mention, method: str, *, excluded: bool) -> Match:
    """The fuzzy tier's verdict over its candidates' best scores, by anchor."""
    if not scored:
        return _unresolved(mention, EXCLUDED if excluded else NO_CANDIDATE)
    top = max(scored.values())
    leaders = sorted(a for a, s in scored.items() if s == top)
    if top < FLOOR:
        return _unresolved(mention, BELOW_FLOOR, leaders[0], top)
    if len(leaders) > 1:
        return _unresolved(mention, AMBIGUOUS, leaders[0], top)
    return _matched(leaders[0], method, top)


# --------------------------------------------------------------------------------------------- #
# The organization track.                                                                       #
# --------------------------------------------------------------------------------------------- #


def _org_domain(mention: Mention) -> str | None:
    website = domain_of(mention.text("domain"), email=False)
    return website or domain_of(mention.text("email"), email=True)


class OrgAuthority:
    """The authority's organizations: each record's anchor (the earliest record of its domain), and
    what the tiers look an anchor up by."""

    def __init__(self, records: Sequence[Mention]) -> None:
        self.anchor_of: dict[str, str] = {}  # authority key -> its anchor
        self.by_domain: dict[str, str] = {}
        self.by_name: dict[str, set[str]] = defaultdict(set)
        self.by_block: dict[str, list[tuple[str, str]]] = defaultdict(list)  # (remainder, anchor)
        order_by = records[0].entry.order_by if records else None
        ordered = sorted(
            records,
            key=lambda m: ("" if order_by is None else str(m.row.get(order_by) or ""), m.key),
        )
        for m in ordered:
            domain = _org_domain(m)
            anchor = self.by_domain.get(domain, m.key) if domain else m.key
            if domain and domain not in self.by_domain and domain not in PERSONAL_DOMAINS:
                self.by_domain[domain] = m.key
            self.anchor_of[m.key] = anchor
        for m in records:
            norm = norm_org_name(m.match_text)
            if not norm:
                continue
            anchor = self.anchor_of[m.key]
            self.by_name[norm].add(anchor)
            block, _, remainder = norm.partition(" ")
            self.by_block[block].append((remainder, anchor))

    def own(self, mention: Mention) -> Match:
        """An authority record: its own anchor, or the earlier record of its domain."""
        anchor = self.anchor_of[mention.key]
        if anchor == mention.key:
            return Match(anchor, "authority", HIGH_CONFIDENCE, 1.0)
        return Match(anchor, "exact_domain", HIGH_CONFIDENCE, 1.0)

    def match(self, mention: Mention) -> Match:
        """A mention of another system, through the tiers in order."""
        norm = norm_org_name(mention.match_text)
        if not norm or norm in PLACEHOLDERS:
            return _unresolved(mention, EXCLUDED)
        domain = _org_domain(mention)
        personal = domain in PERSONAL_DOMAINS
        if domain and not personal and domain in self.by_domain:
            return Match(self.by_domain[domain], "exact_domain", HIGH_CONFIDENCE, 1.0)
        named = sorted(self.by_name.get(norm, ()))
        if len(named) == 1:
            return Match(named[0], "exact_normalized", HIGH_CONFIDENCE, 1.0)
        if len(named) > 1:
            return _unresolved(mention, AMBIGUOUS, named[0], 1.0)
        block, _, remainder = norm.partition(" ")
        scored: dict[str, float] = {}
        for candidate_remainder, anchor in self.by_block.get(block, ()):
            score = jaro_winkler(remainder, candidate_remainder)
            scored[anchor] = max(score, scored.get(anchor, 0.0))
        return _best(scored, mention, "fuzzy", excluded=personal)


# --------------------------------------------------------------------------------------------- #
# The person track.                                                                             #
# --------------------------------------------------------------------------------------------- #


def person_name(mention: Mention) -> tuple[str, str]:
    """A person's first and last name as compared: from first_name and last_name, or from name's
    first and last word."""
    if "name" in mention.entry.reference:
        words = (mention.text("name") or "").split()
        first, last = (words[0], words[-1]) if len(words) > 1 else ("", words[0] if words else "")
    else:
        first, last = mention.text("first_name") or "", mention.text("last_name") or ""
    return norm_person_part(first), norm_person_part(last)


class PersonAuthority:
    """The authority's people: each record its own anchor, looked up by email and, within its
    organization, by last name."""

    def __init__(self, records: Sequence[Mention], org_of: Mapping[str, str | None]) -> None:
        self.by_email: dict[str, set[str]] = defaultdict(set)
        self.by_org_last: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        for m in records:
            email = (m.text("email") or "").lower()
            if email:
                self.by_email[email].add(m.key)
            first, last = person_name(m)
            org = org_of.get(m.key)
            if org is not None and last:
                self.by_org_last[(org, last)].append((first, m.key))

    def match(self, mention: Mention, org: str | None) -> Match:
        """A person of another system: exact email, then within ``org`` (the person's resolved
        organization, None when it has none) last name exact and first name exact or fuzzy."""
        first, last = person_name(mention)
        if not last or last.lower() in PLACEHOLDERS:
            return _unresolved(mention, EXCLUDED)
        email = (mention.text("email") or "").lower()
        by_email = sorted(self.by_email.get(email, ())) if email else []
        if len(by_email) == 1:
            return Match(by_email[0], "exact_email", HIGH_CONFIDENCE, 1.0)
        if len(by_email) > 1:
            return _unresolved(mention, AMBIGUOUS, by_email[0], 1.0)
        candidates = self.by_org_last.get((org, last), []) if org is not None else []
        exact = sorted(anchor for cand_first, anchor in candidates if cand_first == first)
        if len(exact) == 1:
            return Match(exact[0], "exact_normalized", HIGH_CONFIDENCE, 1.0)
        if len(exact) > 1:
            return _unresolved(mention, AMBIGUOUS, exact[0], 1.0)
        scored: dict[str, float] = {}
        for cand_first, anchor in candidates:
            scored[anchor] = max(jaro_winkler(first, cand_first), scored.get(anchor, 0.0))
        return _best(scored, mention, "fuzzy", excluded=False)


# --------------------------------------------------------------------------------------------- #
# The run.                                                                                      #
# --------------------------------------------------------------------------------------------- #

ORG_COLUMNS = [
    "source_schema",
    "source_table",
    "source_column",
    "source_key",
    "name_raw",
    "name_norm",
    "match_text",
    "label",
    "canonical_id",
    "golden_source",
    "match_method",
    "match_confidence",
    "match_score",
    "unresolved_reason",
    "candidate_id",
    "candidate_score",
    "match_run_id",
]
PERSON_COLUMNS = [*ORG_COLUMNS[:10], "org_canonical_id", *ORG_COLUMNS[10:]]

Rows = Mapping[tuple[str, str], Sequence[Mapping[str, Cell]]]
Columns = Mapping[tuple[str, str], Sequence[str]]


@dataclass(frozen=True)
class Bridges:
    """One run's narrow bridges: a row per mention of each entity type."""

    run_id: str
    org: list[dict]
    person: list[dict]


def _bridge_row(
    mention: Mention, match: Match, golden: EntitySource, run_id: str, norm: str
) -> dict:
    return {
        "source_schema": mention.entry.source,
        "source_table": mention.entry.table,
        "source_column": mention.entry.key.column,
        "source_key": mention.key,
        "name_raw": mention.label,
        "name_norm": norm,
        "match_text": mention.match_text,
        "label": mention.label,
        "canonical_id": match.canonical_id,
        "golden_source": golden.identity if match.resolved else None,
        "match_method": match.method,
        "match_confidence": match.confidence,
        "match_score": match.score,
        "unresolved_reason": match.reason,
        "candidate_id": match.candidate_id,
        "candidate_score": match.candidate_score,
        "match_run_id": run_id,
    }


def _rows_of(entry: EntitySource, rows: Rows, columns: Columns) -> list[Mapping[str, Cell]]:
    where = (entry.source, entry.table)
    if where not in columns:
        raise ValueError(f"entity source {entry.identity}: its table's rows were not read")
    check_columns(entry, columns[where])
    return list(rows.get(where, ()))


def run(
    rows: Rows, columns: Columns, run_id: str, registry: Sequence[EntitySource] = ()
) -> Bridges:
    """Match every mention of every registry entry and return the bridges for ``run_id``.
    ``rows`` and ``columns`` hold each (source, table)'s rows and its column names."""
    registry = list(registry or ENTITY_SOURCES)
    check_registry(registry)
    for entry in registry:
        _rows_of(entry, rows, columns)  # every template and column checked before any match

    org_golden = authority(ORGANIZATION, registry)
    org_auth = OrgAuthority(mentions(org_golden, _rows_of(org_golden, rows, columns)))
    org_rows: list[dict] = []
    org_canonical: dict[tuple[str, str, str], str] = {}  # (source, table, key) -> canonical id
    for entry in entries(ORGANIZATION, registry):
        for m in mentions(entry, _rows_of(entry, rows, columns)):
            match = org_auth.own(m) if entry.authority else org_auth.match(m)
            org_canonical[(entry.source, entry.table, m.key)] = match.canonical_id
            org_rows.append(_bridge_row(m, match, org_golden, run_id, norm_org_name(m.match_text)))
    resolved_orgs = {r["canonical_id"] for r in org_rows if r["match_confidence"] != UNRESOLVED}

    def org_context(m: Mention) -> str | None:
        """The canonical id of the person's organization, synthetic when it is unresolved."""
        ref = m.entry.organization
        if ref is None:
            return None
        value = m.row.get(ref.column)
        return None if value is None else org_canonical.get((ref.source, ref.table, str(value)))

    person_golden = authority(PERSON, registry)
    golden_people = mentions(person_golden, _rows_of(person_golden, rows, columns))
    person_auth = PersonAuthority(
        golden_people,
        {m.key: org_context(m) for m in golden_people if org_context(m) in resolved_orgs},
    )
    person_rows: list[dict] = []
    for entry in entries(PERSON, registry):
        for m in mentions(entry, _rows_of(entry, rows, columns)):
            context = org_context(m)
            if entry.authority:
                match = Match(m.key, "authority", HIGH_CONFIDENCE, 1.0)
            else:
                match = person_auth.match(m, context if context in resolved_orgs else None)
            first, last = person_name(m)
            row = _bridge_row(m, match, person_golden, run_id, f"{first} {last}".strip())
            row["org_canonical_id"] = context
            person_rows.append({c: row[c] for c in PERSON_COLUMNS})
    return Bridges(run_id, org_rows, person_rows)


# --------------------------------------------------------------------------------------------- #
# Reading the declared rows, writing the bridges.                                               #
# --------------------------------------------------------------------------------------------- #

_UNESCAPE = {"\\\\": "\\", "\\t": "\t", "\\n": "\n", "\\r": "\r"}


def _cell(text: str) -> str | None:
    if text == "\\N":
        return None
    return re.sub(r"\\[\\tnr]", lambda m: _UNESCAPE[m.group(0)], text)


def read_replica_rows(demo: Path, registry: Sequence[EntitySource] = ()) -> tuple[dict, dict]:
    """Each registry table's rows and column names, as the demo declares them in
    replica_rows/<source>/<schema>.<table>.tsv."""
    rows: dict[tuple[str, str], list[dict]] = {}
    columns: dict[tuple[str, str], list[str]] = {}
    for entry in registry or ENTITY_SOURCES:
        where = (entry.source, entry.table)
        if where in rows:
            continue
        [path] = sorted((demo / "replica_rows" / entry.source).glob(f"*.{entry.table}.tsv"))
        header, *lines = path.read_text(encoding="utf-8").splitlines()
        names = header.split("\t")
        columns[where] = names
        rows[where] = [dict(zip(names, (_cell(c) for c in line.split("\t")))) for line in lines]
    return rows, columns


SCHEMA_SQL = """\
CREATE SCHEMA er;

-- One row per mention of an organization in a registered table (entity_resolution.ENTITY_SOURCES).
CREATE TABLE er.entity_org_bridge (
  source_schema varchar NOT NULL,
  source_table varchar NOT NULL,
  source_column varchar NOT NULL,
  source_key varchar NOT NULL,
  name_raw varchar NOT NULL,
  name_norm varchar NOT NULL,
  match_text varchar NOT NULL,
  label varchar NOT NULL,
  canonical_id varchar NOT NULL,
  golden_source varchar,
  match_method varchar CHECK (match_method IN ('authority', 'exact_domain', 'exact_normalized', 'fuzzy')),
  match_confidence varchar NOT NULL CHECK (match_confidence IN ('high', 'low', 'unresolved')),
  match_score double precision,
  unresolved_reason varchar CHECK (unresolved_reason IN ('no_candidate', 'below_floor', 'ambiguous', 'excluded')),
  candidate_id varchar,
  candidate_score double precision,
  match_run_id varchar NOT NULL,
  PRIMARY KEY (source_schema, source_table, source_column, source_key),
  CHECK ((match_confidence = 'unresolved') = (unresolved_reason IS NOT NULL))
);

-- One row per mention of a person, with the canonical id of the organization it was matched in.
CREATE TABLE er.entity_person_bridge (
  source_schema varchar NOT NULL,
  source_table varchar NOT NULL,
  source_column varchar NOT NULL,
  source_key varchar NOT NULL,
  name_raw varchar NOT NULL,
  name_norm varchar NOT NULL,
  match_text varchar NOT NULL,
  label varchar NOT NULL,
  canonical_id varchar NOT NULL,
  golden_source varchar,
  org_canonical_id varchar,
  match_method varchar CHECK (match_method IN ('authority', 'exact_email', 'exact_normalized', 'fuzzy')),
  match_confidence varchar NOT NULL CHECK (match_confidence IN ('high', 'low', 'unresolved')),
  match_score double precision,
  unresolved_reason varchar CHECK (unresolved_reason IN ('no_candidate', 'below_floor', 'ambiguous', 'excluded')),
  candidate_id varchar,
  candidate_score double precision,
  match_run_id varchar NOT NULL,
  PRIMARY KEY (source_schema, source_table, source_column, source_key),
  CHECK ((match_confidence = 'unresolved') = (unresolved_reason IS NOT NULL))
);
"""

SEED_HEADER = """\
-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The customer360 demo's narrow entity bridges (schema er; REQ-2034), as one run of the matching
-- job wrote them over the rows the demo declares: written by entity_resolution/er_job.py seed. Do
-- not edit; rerun the job. Only the job writes these tables (role er_writer, db/reader.sql).
"""


def _sql(value: Cell) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def replace_sql(bridges: Bridges) -> str:
    """The statements that replace both bridges' contents with ``bridges``."""
    out = []
    for table, cols, rows in (
        ("er.entity_org_bridge", ORG_COLUMNS, bridges.org),
        ("er.entity_person_bridge", PERSON_COLUMNS, bridges.person),
    ):
        out.append(f"DELETE FROM {table};")
        if rows:
            values = ",\n".join("  (" + ", ".join(_sql(r[c]) for c in cols) + ")" for r in rows)
            out.append(f"INSERT INTO {table} ({', '.join(cols)}) VALUES\n{values};")
    return "\n".join(out) + "\n"


def seed_sql(bridges: Bridges) -> str:
    """db/er.sql: the schema and one run's bridges."""
    return SEED_HEADER + "\n" + SCHEMA_SQL + "\n" + replace_sql(bridges)


def write_postgres(dsn: str, bridges: Bridges) -> None:
    """Replace the bridges' contents in one transaction."""
    import psycopg

    with psycopg.connect(dsn) as conn, conn.transaction():
        conn.execute(replace_sql(bridges).encode())


def signal(provisa: str, token: str | None, role: str | None) -> dict:
    """Tell Provisa the bridges changed: POST /data/sources/er/signal (the ``signal`` change
    signal). Its answer names the tables it marked written."""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if role:
        headers["X-Provisa-Role"] = role
    request = urllib.request.Request(
        f"{provisa.rstrip('/')}/data/sources/{BRIDGE_SOURCE}/signal",
        data=b"{}",
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as answer:  # noqa: S310 -- the operator's Provisa
        return json.loads(answer.read() or b"{}")


def summary(bridges: Bridges) -> str:
    def count(rows: list[dict]) -> str:
        unresolved = sum(r["match_confidence"] == UNRESOLVED for r in rows)
        return f"{len(rows)} mentions, {unresolved} unresolved"

    return f"{bridges.run_id}: organizations {count(bridges.org)}; people {count(bridges.person)}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rerun the customer360 demo's entity resolution.")
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("seed", help="write db/er.sql, the bridges the demo ships")
    live = sub.add_parser("run", help="replace the bridges in Postgres, then signal Provisa")
    live.add_argument(
        "--dsn", required=True, help="the demo database, as a role that may write schema er"
    )
    live.add_argument("--provisa", required=True, help="Provisa's base URL, for the signal")
    live.add_argument("--token", help="a bearer token, when Provisa has auth")
    live.add_argument("--role", help="the role to signal as, in dev mode")
    args = parser.parse_args(argv)
    rows, columns = read_replica_rows(DEMO)
    if args.action == "seed":
        bridges = run(rows, columns, SEED_RUN_ID)
        (DEMO / "db" / "er.sql").write_text(seed_sql(bridges), encoding="utf-8")
        print(summary(bridges))
        return 0
    run_id = "er-" + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bridges = run(rows, columns, run_id)
    write_postgres(args.dsn, bridges)
    print(summary(bridges))
    print("signalled", signal(args.provisa, args.token, args.role))
    return 0


if __name__ == "__main__":
    sys.exit(main())
