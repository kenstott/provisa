# Copyright (c) 2026 Kenneth Stott
# Canary: 8978e423-e7e5-4969-b6fb-5f400c6972de
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The mask: a person's reach as a bitmask over the object ids of a role's schema (REQ-2219).

It is computed once per combination of the identity provider's lists, the version of the
Provisa-held lists and the model version, and cached by those; per request it is a lookup and a
bit test. Several roles combine by union: the mask of a set of acting roles is the OR of each
member's, each within its own schema.
"""

# Requirements: REQ-2219

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime

from provisa.entitlements.index import (
    COLUMN,
    COMMAND,
    DATA_PRODUCT,
    LIST_OF,
    METRIC,
    RELATIONSHIP,
    SEMANTIC_LISTS,
    TABLE,
    Finding,
    IndexedObject,
    ObjectIndex,
    matches,
)
from provisa.entitlements.lists import IDP, EffectiveList, ExpiryRule

_MAX_ENTRIES = 4096

#: What an expiry's capability ends, by object kind (REQ-2219: each capability is scoped to one
#: kind of object). A capability of any other kind has no bit in this mask.
_CAPABILITY_KINDS: dict[str, frozenset[str]] = {
    "read": frozenset({TABLE, COLUMN, METRIC}),
    "traverse": frozenset({RELATIONSHIP}),
    "execute": frozenset({COMMAND}),
    "consume": frozenset({DATA_PRODUCT}),
    "*": frozenset({TABLE, COLUMN, METRIC, RELATIONSHIP, COMMAND, DATA_PRODUCT}),
}

#: The lists an object's reach passes through, nearest first.
_CHAIN: dict[str, tuple[str, ...]] = {
    TABLE: ("tables", "domains"),
    COLUMN: ("columns", "tables", "domains"),
    METRIC: ("metrics", "tables", "domains"),
    RELATIONSHIP: ("relationships", "tables", "domains"),
    COMMAND: ("commands", "domains"),
    DATA_PRODUCT: ("data_products", "domains"),
}


@dataclass(frozen=True)
class MemberInput:
    """One acting role's part of the mask: its lists, within its own schema's index."""

    role_id: str
    lists: Mapping[str, EffectiveList]
    index: ObjectIndex
    expiries: tuple[ExpiryRule, ...] = ()


@dataclass(frozen=True)
class EntitlementMask:
    """The bits of one combination, over ``index``."""

    role_id: str
    index: ObjectIndex
    bits: int
    model_version: tuple
    next_expiry: datetime | None = None
    findings: tuple[Finding, ...] = ()

    @property
    def full(self) -> bool:
        """Every object of the role's schema: no narrowing, the same as no mask at all."""
        return self.bits == (1 << self.index.size) - 1

    @property
    def hash(self) -> str:
        """The mask's hash for cache keys. A full mask has none, so it shares what no mask does."""
        if self.full:
            return ""
        return hashlib.sha256(
            f"{self.role_id}|{self.model_version}|{self.bits:x}".encode()
        ).hexdigest()

    def _has(self, bit: int | None) -> bool:
        return bit is not None and bool(self.bits >> bit & 1)

    def covers(self, kind: str, oid: str) -> bool:
        return self._has(self.index.bit_of(kind, oid))

    def knows_table(self, table_id: int) -> bool:
        """Whether the index holds this table: a table the names table does not name (a built-in
        of the system domains) has no bit, and is no entitlement's to select."""
        return table_id in self.index.table_ids

    def has_table(self, table_id: int) -> bool:
        return self.covers(TABLE, str(table_id))

    def has_column(self, table_id: int, physical: str) -> bool:
        return self.covers(COLUMN, f"{table_id}:{physical}") or self.covers(
            METRIC, f"{table_id}:{physical}"
        )

    def knows_relationship(self, key: tuple[int, int, str, str]) -> bool:
        return self.index.relationship_bit(key) is not None

    def has_relationship(self, *key: int | str) -> bool:
        return self._has(self.index.relationship_bit(key))  # pyright: ignore[reportArgumentType]


def _ends(rule: ExpiryRule, obj: IndexedObject) -> bool:
    """Whether a passed ``rule`` removes ``obj``."""
    if not rule.ends:
        return True
    for capability, identifiers in rule.ends.items():
        if capability == "*" and rule.list_kind not in (None, *SEMANTIC_LISTS):
            continue  # set with a list outside the mask: its identifiers name none of its objects
        if obj.kind in _CAPABILITY_KINDS.get(capability, frozenset()) and any(
            matches(i, obj.paths) for i in identifiers
        ):
            return True
    return False


def _member_reach(
    member: MemberInput, now: datetime
) -> tuple[set[tuple[str, str]], datetime | None]:
    """What ``member``'s lists select of its schema, by (kind, oid), and its next expiry."""
    lists = member.lists

    def allowed(list_name: str, obj: IndexedObject) -> bool:
        listed = lists.get(list_name)
        return listed is None or any(matches(i, obj.paths) for i in listed.identifiers)

    objects = member.index.objects
    domains = lists.get("domains")

    def domain_ok(obj: IndexedObject) -> bool:
        return domains is None or any(matches(i, (obj.paths[0][:1],)) for i in domains.identifiers)

    tables_ok = {
        o.table_id for o in objects if o.kind == TABLE and domain_ok(o) and allowed("tables", o)
    }

    def own(o: IndexedObject) -> bool:
        if o.kind == TABLE:
            return o.table_id in tables_ok
        if o.kind in (COLUMN, METRIC):
            return o.table_id in tables_ok and allowed(LIST_OF[o.kind], o)
        if o.kind in (COMMAND, DATA_PRODUCT):
            return domain_ok(o) and allowed(LIST_OF[o.kind], o)
        return True  # a relationship: judged below, once the tables it joins are settled

    reach = {(o.kind, o.oid) for o in objects if own(o)}
    next_expiry = None
    for rule in member.expiries:
        if rule.ends_at > now:
            next_expiry = min(next_expiry or rule.ends_at, rule.ends_at)
            continue
        for o in objects:
            spoken_for_by_idp = any(
                lists[name].origin == IDP for name in _CHAIN[o.kind] if name in lists
            )
            if not spoken_for_by_idp and _ends(rule, o):
                reach.discard((o.kind, o.oid))
    columns_left = {
        o.table_id for o in objects if o.kind in (COLUMN, METRIC) and (o.kind, o.oid) in reach
    }
    has_columns = {o.table_id for o in objects if o.kind in (COLUMN, METRIC)}
    for o in objects:
        if o.kind == TABLE and o.table_id in has_columns and o.table_id not in columns_left:
            reach.discard((o.kind, o.oid))
    table_reached = {o.table_id for o in objects if o.kind == TABLE and (o.kind, o.oid) in reach}
    for o in objects:
        if o.kind != RELATIONSHIP or (o.kind, o.oid) not in reach:
            continue
        if not (
            domain_ok(o)
            and allowed(LIST_OF[RELATIONSHIP], o)
            and o.table_id in table_reached
            and o.target_table_id in table_reached
        ):
            reach.discard((o.kind, o.oid))
    return reach, next_expiry


def compute_mask(
    role_id: str,
    index: ObjectIndex,
    members: Iterable[MemberInput],
    model_version: tuple,
    now: datetime,
    findings: tuple[Finding, ...] = (),
) -> EntitlementMask:
    """The mask of ``role_id`` over ``index``: the union of each member's reach."""
    bits = 0
    next_expiry: datetime | None = None
    for member in members:
        reach, member_next = _member_reach(member, now)
        if member_next is not None:
            next_expiry = min(next_expiry or member_next, member_next)
        for kind, oid in reach:
            bit = index.bit_of(kind, oid)
            if bit is not None:
                bits |= 1 << bit
    return EntitlementMask(role_id, index, bits, model_version, next_expiry, findings)


class MaskCache:
    """Masks and the indexes they are over, bounded, per org. Key: (role, IdP lists hash,
    Provisa-held version, model version). A mask past its next expiry is recomputed."""

    def __init__(self, max_entries: int = _MAX_ENTRIES) -> None:
        self._max = max_entries
        self._lock = threading.Lock()
        self._masks: dict[tuple, EntitlementMask] = {}
        self._indexes: dict[tuple, ObjectIndex] = {}

    def get_or_build(
        self, key: tuple, build: Callable[[], EntitlementMask], *, now: datetime
    ) -> EntitlementMask:
        with self._lock:
            kept = self._masks.pop(key, None)
            if kept is not None and (kept.next_expiry is None or now < kept.next_expiry):
                self._masks[key] = kept
                return kept
        mask = build()
        with self._lock:
            self._masks[key] = mask
            self._bound(self._masks)
        return mask

    def index(self, key: tuple, build: Callable[[], ObjectIndex]) -> ObjectIndex:
        with self._lock:
            kept = self._indexes.get(key)
        if kept is not None:
            return kept
        built = build()
        with self._lock:
            self._indexes[key] = built
            self._bound(self._indexes)
        return built

    def _bound(self, entries: dict) -> None:
        while len(entries) > self._max:
            entries.pop(next(iter(entries)))
