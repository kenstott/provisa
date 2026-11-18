# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c91-5b3d-4f08-a6e2-8c1d9f04b7a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413: the rules of a discovery run, each its own test -- type groups, class intersection,
the contexts sent, table pairs batched into calls sized from the model's limits, re-asking and
splitting, the request count, the direction rules in order, the removals, and Stop."""

# Requirements: REQ-413, REQ-464

from __future__ import annotations

import json

import pytest

from provisa.discovery import context
from provisa.discovery.columns import (
    DECIMAL,
    GROUPS,
    TEXT,
    UNKNOWN,
    UUID,
    WHOLE,
    ColumnFacts,
    TableFacts,
    classes_meet,
    classes_of,
    comparable,
    type_groups,
)
from provisa.discovery.direction import (
    Known,
    Proposal,
    Sampled,
    decide,
    from_answer,
    inferred_role,
    settle,
)
from provisa.discovery.runner import KeyValues, Tally, execute
from provisa.discovery.windows import TOKENS_PER_PAIR, bound, per_call, plan
from provisa.llm.client import Completion
from provisa.llm.limits import CHARS_PER_TOKEN, DEFAULT_LIMITS, limits_for
from tests.unit.discovery_answers import from_ids, listed_pairs


def col(
    name: str,
    data_type: str = "integer",
    *,
    classes: tuple[str, ...] = (UNKNOWN,),
    pk: bool = False,
    sensitive: bool = False,
    samples: tuple[str, ...] = (),
    tags: tuple[str, ...] = (),
    description: str | None = None,
    subtype: str | None = None,
    array_depth: int = 0,
) -> ColumnFacts:
    return ColumnFacts(
        name=name,
        data_type=data_type,
        description=description,
        tags=tags,
        classes=classes,
        sensitive=sensitive,
        samples=samples,
        is_primary_key=pk,
        groups=type_groups(data_type, subtype=subtype, array_depth=array_depth),
    )


def tbl(
    table_id: int,
    name: str,
    *columns: ColumnFacts,
    domain: str = "sales",
    role: str | None = None,
    description: str | None = None,
    tags: tuple[str, ...] = (),
) -> TableFacts:
    return TableFacts(
        table_id=table_id,
        domain_id=domain,
        schema_name="public",
        table_name=name,
        description=description,
        tags=tags,
        role=role,
        columns=tuple(columns),
    )


# --- type groups ---------------------------------------------------------------------------------


@pytest.mark.parametrize("spelled", ["smallint", "integer", "bigint"])
def test_integer_types_are_whole_numbers(spelled):
    assert type_groups(spelled, subtype=None, array_depth=0) == {WHOLE}


@pytest.mark.parametrize("spelled", ["numeric(10,0)", "numeric(18)", "numeric(12, 0)"])
def test_a_zero_scale_decimal_is_in_both_whole_numbers_and_decimals(spelled):
    assert type_groups(spelled, subtype=None, array_depth=0) == {WHOLE, DECIMAL}


@pytest.mark.parametrize("spelled", ["numeric(10,2)", "numeric"])
def test_a_decimal_with_places_or_no_stated_scale_is_a_decimal_only(spelled):
    assert type_groups(spelled, subtype=None, array_depth=0) == {DECIMAL}


def test_a_zero_scale_decimal_is_compared_with_integers_and_with_decimals():
    zero_scale = col("amount_units", "numeric(10,0)")
    assert comparable(zero_scale, col("id", "bigint"), WHOLE)
    assert comparable(zero_scale, col("price", "numeric(10,2)"), DECIMAL)
    assert not comparable(col("id", "bigint"), col("price", "numeric(10,2)"), WHOLE)
    assert not comparable(col("id", "bigint"), col("price", "numeric(10,2)"), DECIMAL)


def test_character_columns_are_text_and_a_text_based_subtype_is_not():
    assert type_groups("text", subtype=None, array_depth=0) == {TEXT}
    assert type_groups("text", subtype="inet", array_depth=0) == frozenset()


@pytest.mark.parametrize(
    ("spelled", "groups"),
    [
        ("varchar", {TEXT}),
        ('varchar collate "default"', {TEXT}),
        ('text collate "c"', {TEXT}),
        ("VARCHAR(255)", {TEXT}),
        ("character varying", {TEXT}),
        ("int4", {WHOLE}),
        ("decimal(10,2)", {DECIMAL}),
        ("timestamp with time zone", frozenset()),
        ("timestamp without time zone", frozenset()),
        ("double precision", frozenset()),
        ("real", frozenset()),
        ("inet", frozenset()),
        ("oid", frozenset()),
        ("", frozenset()),
    ],
)
def test_a_column_is_grouped_by_the_type_its_sources_spelling_names(spelled, groups):
    """A live source's column keeps the spelling its source reports: the default demo's string
    columns are registered ``varchar`` and ``varchar collate "default"``, and took part in no
    group while only the spelling ``text`` was a character type."""
    assert type_groups(spelled, subtype=None, array_depth=0) == groups


def test_uuid_is_a_group_of_its_own():
    assert type_groups("uuid", subtype=None, array_depth=0) == {UUID}


@pytest.mark.parametrize(
    "spelled",
    [
        "date",
        "timestamp",
        "timestamptz",
        "timestamp with time zone",
        "timestamp without time zone",
    ],
)
def test_a_date_and_each_timestamp_type_are_in_no_group(spelled):
    """REQ-413 (amended 2026-11-16): dates are never key candidates."""
    assert type_groups(spelled, subtype=None, array_depth=0) == frozenset()


@pytest.mark.parametrize("spelled", ["date", "timestamp", "timestamptz"])
def test_two_date_or_timestamp_columns_are_compared_in_no_group(spelled):
    assert not any(comparable(col("at", spelled), col("at", spelled), g) for g in GROUPS)


@pytest.mark.parametrize(
    ("spelled", "subtype", "depth"),
    [
        ("boolean", None, 0),
        ("float", None, 0),
        ("double", None, 0),
        ("json", None, 0),
        ("bytea", None, 0),
        ("vector", None, 0),
        ("text", "geometry", 0),
        ("text", "point", 0),
        ("integer", None, 1),
    ],
)
def test_boolean_float_json_binary_vector_geometry_and_arrays_are_never_compared(
    spelled, subtype, depth
):
    assert type_groups(spelled, subtype=subtype, array_depth=depth) == frozenset()


def test_a_column_not_compared_still_appears_by_name_in_its_tables_context():
    t = tbl(1, "orders", col("id", pk=True), col("is_paid", "boolean"))
    sent = context.table_context(t, "orders")
    assert sent["columns"] == ["id", "is_paid"]
    assert [c["name"] for c in sent["comparable_columns"]] == ["id"]


# --- classes ---------------------------------------------------------------------------------------


def test_a_columns_classes_are_every_kind_matched():
    assert classes_of({"world_city", "country_name"}, set()) == ("country_name", "world_city")


def test_a_decided_class_replaces_the_matched_ones():
    assert classes_of({"world_city", "country_name"}, {"world_city"}) == ("world_city",)


def test_a_column_with_no_class_is_unknown():
    assert classes_of(set(), set()) == (UNKNOWN,)


def test_two_unknown_columns_intersect():
    assert classes_meet(col("a"), col("b"))


def test_columns_are_compared_only_where_their_classes_intersect():
    city = col("city", "text", classes=("world_city",))
    city_or_country = col("place", "text", classes=("country_name", "world_city"))
    unknown = col("note", "text")
    assert comparable(city, city_or_country, TEXT)
    assert not comparable(city, unknown, TEXT)


def test_a_table_pair_with_no_intersecting_classes_is_not_asked():
    source = tbl(1, "stores", col("city", "text", classes=("world_city",)))
    target = tbl(2, "currencies", col("code", "text", classes=("iso4217_currency",)))
    assert plan([source], [target], *DEFAULT_LIMITS).calls == []


# --- contexts -------------------------------------------------------------------------------------


def test_the_table_context_is_name_description_tags_role_and_all_column_names():
    t = tbl(
        7,
        "orders",
        col("id", pk=True),
        col("customer_id"),
        col("notes", "json"),
        description="One row per order",
        tags=("finance",),
        role="fact",
    )
    assert context.table_context(t, "orders") == {
        "name": "orders",
        "description": "One row per order",
        "tags": ["finance"],
        "role": "fact",
        "columns": ["id", "customer_id", "notes"],
        "comparable_columns": [
            context.column_context(t.columns[0]),
            context.column_context(t.columns[1]),
        ],
    }


def test_the_column_context_is_name_description_classes_tags_and_samples_with_no_type():
    c = col(
        "customer_id",
        "bigint",
        description="Who placed it",
        tags=("pii",),
        samples=("17", "42"),
    )
    assert context.column_context(c) == {
        "name": "customer_id",
        "description": "Who placed it",
        "classes": [UNKNOWN],
        "tags": ["pii"],
        "sample_values": ["17", "42"],
    }


def test_a_sensitive_class_is_sent_in_place_of_sample_values():
    c = col("tax_id", "text", classes=("us_ssn",), sensitive=True, samples=("078-05-1120",))
    sent = context.column_context(c)
    assert sent["classes"] == ["us_ssn"]
    assert "sample_values" not in sent
    assert "078-05-1120" not in json.dumps(sent)


def test_the_prompt_says_a_shared_pii_tag_alone_is_weak_evidence():
    text = context.instructions(10, threshold=0.7, either_direction=False)
    assert "A shared pii tag alone is weak evidence" in text


# --- table pairs and calls -------------------------------------------------------------------------


def test_table_pairs_per_call_are_the_output_limit_at_150_tokens_each():
    assert TOKENS_PER_PAIR == 150
    assert per_call(4_096) == 27
    assert per_call(128_000) == 853


def test_an_unknown_model_sizes_from_the_default_pair():
    limits = limits_for("acme", "never-heard-of-it", {})
    assert (limits.input_tokens, limits.output_tokens) == DEFAULT_LIMITS == (32_000, 4_096)


def test_a_calls_room_is_the_input_limit_in_characters_less_the_instructions():
    k = per_call(4_096)
    # The instructions are weighed at the widest threshold they can state.
    fixed = len(context.instructions(k, threshold=0.9999, either_direction=False))
    assert fixed == context.instructions_chars(k, either_direction=False)
    assert CHARS_PER_TOKEN == 3
    t = tbl(1, "t", col("id"))
    assert plan([t], [t], 32_000, 4_096).room == 32_000 * 3 - fixed - context.HEADERS


def _wide(table_id: int, name: str, n: int) -> TableFacts:
    return tbl(
        table_id,
        name,
        *[col(f"{name}_c{i}", samples=(str(i) * 40,)) for i in range(n)],
    )


def test_pairs_are_batched_in_order_within_the_room_each_listed_once():
    tables = [_wide(i, f"t{i}", 6) for i in range(1, 6)]
    planned = plan(tables, None, 1_500, 4_096)
    assert len(planned.calls) > 1
    assert all(len(c.prompt(0.9999)) <= 1_500 * CHARS_PER_TOKEN for c in planned.calls)
    listed = [p.key for c in planned.calls for p in c.pairs]
    assert len(listed) == len(set(listed)) == planned.table_pairs == 15  # 5 tables, 5 + 10 pairs


def test_a_larger_input_limit_makes_fewer_requests():
    tables = [_wide(i, f"t{i}", 8) for i in range(1, 9)]
    small = plan(tables, None, 3_000, 4_096).calls
    large = plan(tables, None, 300_000, 4_096).calls
    assert len(large) < len(small)
    assert len(large) == 2  # 36 pairs, 27 to a call


def test_every_request_fits_the_input_limit():
    tables = [_wide(i, f"t{i}", 8) for i in range(1, 9)]
    calls = plan(tables, [*tables[:3]], 4_000, 4_096).calls
    assert calls
    assert all(len(c.prompt(0.9999)) <= 4_000 * CHARS_PER_TOKEN for c in calls)


def test_the_request_count_counts_calls_and_table_pairs():
    t1 = tbl(1, "orders", col("id", pk=True), col("customer_ref", "uuid"), col("day", "date"))
    t2 = tbl(2, "customers", col("id", pk=True), col("ref", "uuid"))
    planned = plan([t1, t2], None, *DEFAULT_LIMITS)
    # customers x orders shares whole numbers and uuids; neither table pairs with itself.
    assert [p.key for c in planned.calls for p in c.pairs] == [(2, 1)]
    # The count before Run: the date column is in no group.
    assert bound([t1, t2], None, *DEFAULT_LIMITS) == (27, 1, 1)


async def test_tables_sharing_only_dates_ask_the_model_nothing_and_measure_nothing():
    """REQ-413 (amended 2026-11-16): date and timestamp columns are never key candidates, so a
    run over tables whose only columns in common are dates and timestamps has no request."""
    orders = tbl(
        1,
        "orders",
        col("placed_on", "date"),
        col("placed_at", "timestamp"),
        col("seen_at", "timestamptz"),
    )
    shipments = tbl(
        2,
        "shipments",
        col("shipped_on", "date"),
        col("shipped_at", "timestamp"),
        col("logged_at", "timestamp with time zone"),
    )
    planned = plan([orders, shipments], None, *DEFAULT_LIMITS)
    assert planned.calls == []
    assert bound([orders, shipments], None, *DEFAULT_LIMITS)[1:] == (0, 0)
    assert bound([orders], [shipments], *DEFAULT_LIMITS)[1:] == (0, 0)
    asked: list[str] = []
    measured: list = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        return _answer(prompt, [])

    async def measure(found: list) -> list:
        measured.append(found)
        return found

    tally, proposals = await execute(
        planned,
        4_096,
        ask,
        _Watcher(),
        threshold=0.7,
        key_values=KeyValues(measure=measure, cutoff=0.0),
        to_thread=_direct,
    )
    assert asked == []
    assert measured == []
    assert proposals == []
    assert tally.total == 0


# --- re-asking and splitting, request count, stop --------------------------------------------------


class _Watcher:
    def __init__(self, stop_after: int | None = None):
        self.returns = 0
        self.stop_after = stop_after

    async def stop_asked(self) -> bool:
        return self.stop_after is not None and self.returns >= self.stop_after

    async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
        self.returns += 1


async def _direct(fn, *args):
    return fn(*args)


#: The names of the tables these tests make, by id.
_NAMES = {1: "orders", 2: "customers", 3: "shipments", **{i: f"t{i}" for i in range(4, 10)}}


def _answer(prompt: str, items: list[dict], *, cut_off: bool = False) -> Completion:
    """The model answering each listed pair with the column pairs of ``items`` (tables by id),
    every other "none"."""
    return Completion(from_ids(prompt, json.dumps(items), _NAMES), "acme", "m", None, cut_off)


def _item(src: tuple[int, str], tgt: tuple[int, str], confidence: float = 0.9) -> dict:
    return {
        "source_table_id": src[0],
        "source_column": src[1],
        "target_table_id": tgt[0],
        "target_column": tgt[1],
        "cardinality": "many-to-one",
        "confidence": confidence,
        "reasoning": "names match",
    }


def _pair_tables() -> tuple[TableFacts, TableFacts]:
    orders = tbl(1, "orders", col("id", pk=True), col("customer_id"), col("store_id"))
    customers = tbl(2, "customers", col("id", pk=True), col("region_id"))
    return orders, customers


def _two_pairs():
    """One call of two table pairs: orders x customers, shipments x customers."""
    orders, customers = _pair_tables()
    shipments = tbl(3, "shipments", col("id", pk=True), col("customer_id"))
    planned = plan([orders, shipments], [customers], *DEFAULT_LIMITS)
    assert [len(c.pairs) for c in planned.calls] == [2]
    return planned


async def test_the_request_count_equals_the_requests_the_run_then_makes():
    tables = [_wide(i, f"t{i}", 8) for i in range(4, 10)]
    planned = plan(tables, None, 3_000, 4_096)
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        return _answer(prompt, [])

    tally, _ = await execute(
        planned, 4_096, ask, _Watcher(), threshold=0.7, key_values=None, to_thread=_direct
    )
    assert len(asked) == len(planned.calls)
    assert tally.done == tally.total == len(planned.calls)


async def test_a_cut_off_answer_keeps_its_complete_pairs_and_asks_the_rest_again():
    planned = _two_pairs()
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        if len(asked) == 1:
            pairs = listed_pairs(prompt)
            first = {
                "pair": 1,
                "column_pairs": [
                    {
                        **_item((1, "customer_id"), (2, "id")),
                        "source_table": pairs[0]["source"],
                        "target_table": pairs[0]["target"],
                    }
                ],
            }
            text = "[" + json.dumps(first) + ', {"pair": 2, "column_pa'
            return Completion(text, "acme", "m", None, True)
        return _answer(prompt, [_item((3, "customer_id"), (2, "id"))])

    tally, proposals = await execute(
        planned, 4_096, ask, _Watcher(), threshold=0.7, key_values=None, to_thread=_direct
    )
    assert len(asked) == 2
    assert [(p["source"], p["target"]) for p in listed_pairs(asked[1])] == [
        ("shipments", "customers")
    ]
    assert tally.cut_short == 1 and tally.asked_again == 1 and tally.unanswered == []
    assert {p.edge for p in proposals} == {
        (1, "customer_id", 2, "id"),
        (3, "customer_id", 2, "id"),
    }


async def test_a_call_lists_no_more_pairs_than_the_output_limit_answers():
    orders, customers = _pair_tables()
    shipments = tbl(3, "shipments", col("id", pk=True), col("customer_id"))
    planned = plan([orders, shipments], [customers], DEFAULT_LIMITS[0], 150)
    assert planned.k == 1
    assert [len(c.pairs) for c in planned.calls] == [1, 1]


async def test_a_request_the_vendor_refuses_as_too_long_is_split_and_asked_again():
    planned = _two_pairs()
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        if len(asked) == 1:
            raise RuntimeError("Error code: 400 - prompt is too long: 250000 tokens > 200000")
        return _answer(prompt, [])

    tally, _ = await execute(
        planned, 4_096, ask, _Watcher(), threshold=0.7, key_values=None, to_thread=_direct
    )
    assert tally.split == 1
    assert tally.failed == []
    assert len(asked) == 3


async def test_a_cut_off_answer_that_finished_no_pair_asks_it_again():
    single = tbl(1, "orders", col("x_id"))
    other = tbl(2, "customers", col("id", pk=True))
    planned = plan([single], [other], *DEFAULT_LIMITS)
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        if len(asked) == 1:
            return Completion('[{"pair": 1, "column_pairs": [{"sour', "acme", "m", None, True)
        return _answer(prompt, [_item((1, "x_id"), (2, "id"))])

    tally, proposals = await execute(
        planned, 4_096, ask, _Watcher(), threshold=0.7, key_values=None, to_thread=_direct
    )
    assert tally.split == 0 and tally.cut_short == 1 and tally.asked_again == 1
    assert [p.edge for p in proposals] == [(1, "x_id", 2, "id")]


async def test_one_failed_request_is_reported_and_the_rest_continue():
    tables = [_wide(i, f"t{i}", 8) for i in range(4, 10)]
    planned = plan(tables, None, 3_000, 4_096)
    assert len(planned.calls) > 2
    calls = 0

    def ask(prompt: str, max_tokens: int) -> Completion:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("vendor unavailable")
        return _answer(prompt, [])

    tally, _ = await execute(
        planned, 4_096, ask, _Watcher(), threshold=0.7, key_values=None, to_thread=_direct
    )
    first = planned.calls[0]
    # The failed call's pairs are asked again, in one more call.
    assert calls == len(planned.calls) + 1
    assert tally.asked_again == len(first.pairs) and tally.unanswered == []
    assert tally.failed == [
        {
            "table_pairs": [first.named(p) for p in first.pairs],
            "code": "llm.request_failed",
            "params": {"reason": "vendor unavailable"},
            "message": "The request to the model failed: vendor unavailable",
        }
    ]


async def test_an_unreadable_answer_is_a_failed_request_with_its_own_message():
    orders, customers = _pair_tables()
    planned = plan([orders], [customers], *DEFAULT_LIMITS)

    tally, _ = await execute(
        planned,
        4_096,
        lambda p, m: Completion("no list here", "v", "m", None, False),
        _Watcher(),
        threshold=0.7,
        key_values=None,
        to_thread=_direct,
    )
    assert tally.unreadable == 2  # asked, then asked again
    assert tally.unanswered == ["orders × customers"]
    assert tally.failed[0] == {
        "table_pairs": ["orders × customers"],
        "code": "discovery.answer_unreadable",
        "params": {},
        "message": "The model's answer held no readable list.",
    }


async def test_a_request_in_flight_says_it_is_still_going_until_it_ends(monkeypatch):
    import asyncio

    from provisa.discovery import run as discovery_run

    monkeypatch.setattr(discovery_run, "STILL_GOING_EVERY_S", 0.01)
    said = 0

    async def still_going() -> None:
        nonlocal said
        said += 1

    async def work() -> str:
        await asyncio.sleep(0.1)
        return "answer"

    assert await discovery_run.while_saying_still_going(work(), still_going) == "answer"
    assert said >= 2


async def test_a_run_that_has_ended_leaves_the_request_in_flight(monkeypatch):
    import asyncio

    from provisa.discovery import run as discovery_run

    monkeypatch.setattr(discovery_run, "STILL_GOING_EVERY_S", 0.01)

    class Ended(Exception):
        pass

    async def ended() -> None:
        raise Ended

    with pytest.raises(Ended):
        await discovery_run.while_saying_still_going(asyncio.sleep(5), ended)


async def test_stop_ends_the_run_after_the_request_in_flight_and_keeps_what_was_found():
    tables = [_wide(i, f"t{i}", 8) for i in range(4, 10)]
    planned = plan(tables, None, 3_000, 4_096)
    pair = planned.calls[0].pairs[0]
    a, b = pair.column_pairs()[0]
    edge = (pair.source.table_id, a.name, pair.target.table_id, b.name)
    asked = 0

    def ask(prompt: str, max_tokens: int) -> Completion:
        nonlocal asked
        asked += 1
        return _answer(prompt, [_item(edge[:2], edge[2:])])

    tally, proposals = await execute(
        planned,
        4_096,
        ask,
        _Watcher(stop_after=1),
        threshold=0.7,
        key_values=None,
        to_thread=_direct,
    )
    assert asked == 1
    assert tally.stopped
    assert [p.edge for p in proposals] == [edge]


# --- direction --------------------------------------------------------------------------------------


class _Samples:
    def __init__(self, repeats: dict[tuple[int, str], bool], rows: int = 500):
        self.repeats = repeats
        self.rows = rows
        self.read: list[tuple[int, str]] = []

    async def __call__(self, table: TableFacts, column: str) -> Sampled:
        self.read.append((table.table_id, column))
        return Sampled(self.rows, self.repeats[(table.table_id, column)])


def _sem(src, tgt, confidence=0.9, cardinality="many-to-one") -> Proposal:
    return from_answer("semantics", src, tgt, cardinality, confidence, "model")


def _fk(src, tgt) -> Proposal:
    return from_answer("foreign_keys", src, tgt, "many-to-one", 1.0, "Foreign key constraint")


def _tables(*ts: TableFacts) -> dict[int, TableFacts]:
    return {t.table_id: t for t in ts}


async def test_a_foreign_key_constraint_decides_first():
    # The primary key rule would choose the other way: b.ref is b's key.
    a = tbl(1, "a", col("ref", pk=False))
    b = tbl(2, "b", col("ref", pk=True))
    samples = _Samples({})
    d = await decide(
        [_fk((2, "ref"), (1, "ref")), _sem((1, "ref"), (2, "ref"))], _tables(a, b), Known(), samples
    )
    assert d is not None and (d.rule, d.start) == ("foreign_key", (2, "ref"))
    assert samples.read == []


async def test_a_primary_key_is_the_one_side_before_roles():
    # Recorded roles would choose the other way: a is the dimension.
    a = tbl(1, "a", col("code", pk=True), role="dimension")
    b = tbl(2, "b", col("code"), role="fact")
    a_fact = tbl(1, "a", col("code"), role="fact")
    b_dim = tbl(2, "b", col("code", pk=True), role="dimension")
    props = [_sem((1, "code"), (2, "code")), _sem((2, "code"), (1, "code"))]
    d = await decide(props, _tables(a_fact, b_dim), Known(), _Samples({}))
    assert d is not None and (d.rule, d.start) == ("primary_key", (1, "code"))
    d2 = await decide(props, _tables(a, b), Known(), _Samples({}))
    assert d2 is not None and (d2.rule, d2.start) == ("primary_key", (2, "code"))


async def test_recorded_roles_start_at_the_fact_before_the_models_statement():
    fact = tbl(1, "sales", col("store_code"), role="fact")
    dim = tbl(2, "stores", col("store_code"), role="dimension")
    # Both answers say the dimension is the many side; the roles decide first.
    props = [
        _sem((2, "store_code"), (1, "store_code")),
        _sem((1, "store_code"), (2, "store_code"), cardinality="one-to-many"),
    ]
    d = await decide(props, _tables(fact, dim), Known(), _Samples({}))
    assert d is not None and (d.rule, d.start) == ("roles", (1, "store_code"))


def test_roles_are_inferred_per_pair_where_none_is_recorded():
    stores = tbl(2, "stores", col("store_id", pk=True))
    products = tbl(3, "products", col("product_id", pk=True))
    sales = tbl(1, "sales", col("store_id"), col("product_id"), col("qty"))
    universe = [sales, stores, products]
    assert inferred_role(sales, universe, Known()) == "fact"
    assert inferred_role(stores, universe, Known()) == "dimension"
    lonely = tbl(4, "lonely", col("lonely_id", pk=True))
    assert inferred_role(lonely, [lonely], Known()) is None
    assert inferred_role(lonely, [lonely], Known(relationships_ending={4})) == "dimension"


async def test_inferred_roles_decide_when_none_is_recorded():
    stores = tbl(2, "stores", col("store_id", pk=True), col("city_code"))
    products = tbl(3, "products", col("product_id", pk=True))
    sales = tbl(1, "sales", col("store_id"), col("product_id"), col("city_code"))
    props = [_sem((2, "city_code"), (1, "city_code")), _sem((1, "city_code"), (2, "city_code"))]
    d = await decide(props, _tables(sales, stores, products), Known(), _Samples({}))
    assert d is not None and (d.rule, d.start) == ("roles", (1, "city_code"))


async def test_the_models_statement_decides_before_confidence():
    a = tbl(1, "a", col("k"))
    b = tbl(2, "b", col("k"))
    # Written both ways, both reading a.k as the many side.
    props = [
        _sem((1, "k"), (2, "k"), confidence=0.75),
        _sem((2, "k"), (1, "k"), confidence=0.95, cardinality="one-to-many"),
    ]
    samples = _Samples({})
    d = await decide(props, _tables(a, b), Known(), samples)
    assert d is not None and (d.rule, d.start) == ("model_statement", (1, "k"))
    assert samples.read == []


async def test_the_higher_confidence_decides_before_any_sample_is_read():
    a = tbl(1, "a", col("k"))
    b = tbl(2, "b", col("k"))
    props = [_sem((1, "k"), (2, "k"), 0.8), _sem((2, "k"), (1, "k"), 0.9)]
    samples = _Samples({})
    d = await decide(props, _tables(a, b), Known(), samples)
    assert d is not None and (d.rule, d.start) == ("confidence", (2, "k"))
    assert samples.read == []


async def test_only_then_a_sample_decides_the_unique_side_is_the_one_side():
    a = tbl(1, "a", col("k"))
    b = tbl(2, "b", col("k"))
    props = [_sem((1, "k"), (2, "k"), 0.9), _sem((2, "k"), (1, "k"), 0.9)]
    samples = _Samples({(1, "k"): True, (2, "k"): False}, rows=321)
    d = await decide(props, _tables(a, b), Known(), samples)
    assert d is not None and (d.rule, d.start) == ("uniqueness_sample", (1, "k"))
    assert d.evidence == "k unique in a sample of 321 rows"


async def test_last_the_table_whose_domain_and_name_sort_first():
    a = tbl(1, "zebra", col("k"), domain="alpha")
    b = tbl(2, "apple", col("k"), domain="beta")
    props = [_sem((1, "k"), (2, "k"), 0.9), _sem((2, "k"), (1, "k"), 0.9)]
    samples = _Samples({(1, "k"): False, (2, "k"): False})
    d = await decide(props, _tables(a, b), Known(), samples)
    assert d is not None and (d.rule, d.start) == ("fixed_order", (1, "k"))


async def test_the_sample_evidence_is_reported_on_the_kept_suggestion():
    a = tbl(1, "a", col("k"))
    b = tbl(2, "b", col("k"))
    props = [_sem((1, "k"), (2, "k"), 0.9), _sem((2, "k"), (1, "k"), 0.9)]
    samples = _Samples({(1, "k"): True, (2, "k"): False}, rows=40)
    out = await settle(props, _tables(a, b), {1, 2}, {1, 2}, Known(), samples)
    assert [p.edge for p in out.kept] == [(1, "k", 2, "k")]
    assert out.kept[0].reasoning.endswith("k unique in a sample of 40 rows")
    assert out.decided_by == {"uniqueness_sample": 1}


# --- removals ----------------------------------------------------------------------------------------


async def _settle(props, *ts, known=None, source=None, target=None, samples=None):
    tables = _tables(*ts)
    ids = set(tables)
    return await settle(
        props,
        tables,
        source or ids,
        target or ids,
        known or Known(),
        samples or _Samples({}),
    )


async def test_a_column_to_that_same_column_is_removed():
    a = tbl(1, "a", col("id", pk=True), col("parent_id"))
    out = await _settle([_sem((1, "parent_id"), (1, "parent_id"))], a)
    assert out.kept == [] and out.removed.same_column == 1


async def test_two_different_columns_of_one_table_are_kept():
    a = tbl(1, "employees", col("id", pk=True), col("manager_id"))
    out = await _settle([_sem((1, "manager_id"), (1, "id"))], a)
    assert [p.edge for p in out.kept] == [(1, "manager_id", 1, "id")]


async def test_primary_key_to_primary_key_is_removed():
    a = tbl(1, "a", col("id", pk=True))
    b = tbl(2, "b", col("id", pk=True))
    out = await _settle([_sem((1, "id"), (2, "id"))], a, b)
    assert out.kept == [] and out.removed.primary_key_to_primary_key == 1


async def test_both_sides_repeating_in_the_sample_is_removed():
    a = tbl(1, "a", col("k"))
    b = tbl(2, "b", col("k"))
    props = [_sem((1, "k"), (2, "k"), 0.9), _sem((2, "k"), (1, "k"), 0.9)]
    samples = _Samples({(1, "k"): True, (2, "k"): True})
    out = await _settle(props, a, b, samples=samples)
    assert out.kept == [] and out.removed.both_repeating == 1


@pytest.mark.parametrize("edge", [(1, "customer_id", 2, "id"), (2, "id", 1, "customer_id")])
async def test_a_relationship_that_exists_in_either_direction_is_removed(edge):
    a, b = _pair_tables()
    out = await _settle([_sem((1, "customer_id"), (2, "id"))], a, b, known=Known(existing={edge}))
    assert out.kept == [] and out.removed.existing == 1


async def test_a_pending_suggestion_is_removed():
    a, b = _pair_tables()
    known = Known(pending={(1, "customer_id", 2, "id")})
    out = await _settle([_sem((1, "customer_id"), (2, "id"))], a, b, known=known)
    assert out.kept == [] and out.removed.pending == 1


async def test_a_rejected_suggestion_is_removed():
    a, b = _pair_tables()
    known = Known(rejected={(1, "customer_id", 2, "id")})
    out = await _settle([_sem((1, "customer_id"), (2, "id"))], a, b, known=known)
    assert out.kept == [] and out.removed.rejected == 1


async def test_a_semantic_suggestion_matching_a_foreign_key_of_the_run_is_removed_and_the_key_kept():
    a, b = _pair_tables()
    fk = _fk((1, "customer_id"), (2, "id"))
    out = await _settle([fk, _sem((2, "id"), (1, "customer_id"), cardinality="one-to-many")], a, b)
    assert [(p.kind, p.edge) for p in out.kept] == [("foreign_keys", (1, "customer_id", 2, "id"))]
    assert out.removed.matches_foreign_key == 1


async def test_only_relationships_starting_in_the_source_set_and_ending_in_the_target_set_are_kept():
    a, b = _pair_tables()
    out = await _settle(
        [_sem((1, "customer_id"), (2, "id")), _sem((2, "region_id"), (1, "id"))],
        a,
        b,
        source={1},
        target={2},
    )
    assert [p.edge for p in out.kept] == [(1, "customer_id", 2, "id")]
    assert out.removed.out_of_scope == 1


# --- nothing read is dropped: what is left out, and why ---------------------------------------------


def test_the_instructions_say_what_a_confidence_states_and_where_a_suggestion_is_kept():
    text = context.instructions(5, threshold=0.7, either_direction=True)
    assert context.rubric("0.7") in text
    assert "A relationship is suggested at 0.7 or more" in text
    for band in ("0.9 to 1.0", "0.7 to 0.89", "0.4 to 0.69", "below 0.4"):
        assert band in text
    # The bands are tied to what a request shows: the columns' names, a key, sample values.
    assert "sample values overlap or are of one form" in text
    assert "the same name on a column that is a key of neither table" in text


async def test_every_element_of_an_answer_is_kept_with_why_it_is_left_out():
    from provisa.discovery.runner import NOT_NAMED, judged, read_answers

    orders = tbl(
        1,
        "orders",
        col("id", pk=True),
        col("customer_id"),
        col("store_id"),
        col("card", classes=("payment_card",)),
    )
    customers = tbl(2, "customers", col("id", pk=True), col("region_id"))
    (call,) = plan([orders], [customers], *DEFAULT_LIMITS).calls
    tally = Tally(total=1, threshold=0.7)

    def named(src: tuple[str, str], tgt: tuple[str, str], confidence: float = 0.9) -> dict:
        return {
            "source_table": src[0],
            "source_column": src[1],
            "target_table": tgt[0],
            "target_column": tgt[1],
            "cardinality": "many-to-one",
            "confidence": confidence,
            "reasoning": "names match",
        }

    column_pairs = [
        named(("orders", "customer_id"), ("customers", "id"), 0.95),
        named(("orders", "store_id"), ("customers", "region_id"), 0.4),
        named(("orders", "card"), ("customers", "id"), 0.9),
        named(("orders", "customer_id"), ("nowhere", "id"), 0.9),
        {"source_table": "orders", "source_column": "customer_id"},
        {**named(("orders", "customer_id"), ("customers", "id")), "cardinality": "many-to-many"},
        "orders.customer_id -> customers.id",
    ]
    answered, found, apart, zeros = read_answers(
        call, [{"pair": 1, "column_pairs": column_pairs}], tally
    )
    assert answered == {1}
    kept = await judged(found, apart, None, None, tally, zeros)
    assert [p.edge for p in kept] == [(1, "customer_id", 2, "id")]
    assert tally.proposed == 7
    # Every compared column pair the answer did not name scores zero, below the threshold.
    unnamed = {
        (1, "id", 2, "id"),
        (1, "id", 2, "region_id"),
        (1, "customer_id", 2, "region_id"),
        (1, "store_id", 2, "id"),
    }
    assert tally.zero_scored == len(unnamed)
    assert (tally.below_threshold, tally.not_comparable, tally.invalid) == (1 + 4, 1, 4)
    left = [(r.left_out, None if r.proposal is None else r.proposal.edge) for r in tally.left_out]
    assert left[:6] == [
        ("unknown_column", None),
        ("invalid_shape", None),
        ("invalid_shape", None),
        ("invalid_shape", None),
        ("not_comparable", (1, "card", 2, "id")),
        ("below_threshold", (1, "store_id", 2, "region_id")),
    ]
    assert {e for reason, e in left[6:]} == unnamed
    assert all(reason == "below_threshold" for reason, _ in left[6:])
    assert all(
        r.proposal is not None and (r.proposal.confidence, r.proposal.reasoning) == (0.0, NOT_NAMED)
        for r in tally.left_out[6:]
    )
    below = tally.left_out[5].proposal
    assert below is not None and below.confidence == 0.4 and below.reasoning == "names match"
    # An element that made no proposal is kept as the model wrote it.
    assert tally.left_out[0].answer is not None and '"nowhere"' in tally.left_out[0].answer
    assert tally.left_out[3].answer == '"orders.customer_id -> customers.id"'


def test_a_second_scorers_confidence_is_kept_beside_the_models_and_removes_nothing():
    from provisa.discovery.runner import rescored

    found = [
        _sem((1, "customer_id"), (2, "id"), 0.95),
        _sem((1, "store_id"), (2, "region_id"), 0.9),
        _sem((1, "note"), (2, "name"), 0.8),
    ]
    tally = Tally(total=1, threshold=0.7)
    kept = rescored(found, [0.75, 0.31, None], tally)
    assert [(p.edge, p.confidence, p.scored) for p in kept] == [
        ((1, "customer_id", 2, "id"), 0.95, 0.75),
        ((1, "store_id", 2, "region_id"), 0.9, 0.31),
        ((1, "note", 2, "name"), 0.8, None),  # given no score
    ]
    assert (tally.scored, tally.scored_below_threshold, tally.below_threshold) == (2, 1, 0)
    assert tally.left_out == []


def test_a_second_scorer_that_could_not_be_asked_keeps_the_models_confidence_and_is_counted():
    from provisa.discovery.runner import rescored

    found = [_sem((1, "customer_id"), (2, "id"), 0.95)]
    tally = Tally(total=1, threshold=0.7)
    assert rescored(found, None, tally) == found
    assert tally.scoring_failed == 1 and tally.scored == 0 and tally.left_out == []


async def test_every_removed_proposal_is_given_back_with_its_reason():
    orders = tbl(1, "orders", col("id", pk=True), col("customer_id"), col("ref"), col("code"))
    customers = tbl(2, "customers", col("id", pk=True), col("ref"), col("code"))
    props = [
        _sem((1, "customer_id"), (1, "customer_id")),
        _sem((1, "id"), (2, "id")),
        _fk((1, "customer_id"), (2, "id")),
        _sem((1, "customer_id"), (2, "id"), 0.8),
        _sem((1, "ref"), (2, "ref"), 0.9),
        _sem((1, "ref"), (2, "ref"), 0.75),
        _sem((1, "code"), (2, "code"), 0.9),
    ]
    out = await _settle(props, orders, customers, known=Known(rejected={(1, "code", 2, "code")}))
    assert [p.edge for p in out.kept] == [(1, "customer_id", 2, "id"), (1, "ref", 2, "ref")]
    assert [(o.reason, o.proposal.edge, o.proposal.confidence) for o in out.left_out] == [
        ("same_column", (1, "customer_id", 1, "customer_id"), 0.9),
        ("primary_key_to_primary_key", (1, "id", 2, "id"), 0.9),
        ("matches_foreign_key", (1, "customer_id", 2, "id"), 0.8),
        ("weaker", (1, "ref", 2, "ref"), 0.75),
        ("rejected", (1, "code", 2, "code"), 0.9),
    ]
    assert out.removed.weaker == 1 and out.removed.rejected == 1


async def test_a_pair_whose_both_sides_repeat_leaves_out_each_of_its_proposals():
    a = tbl(1, "a", col("code"))
    b = tbl(2, "b", col("code"))
    props = [_sem((1, "code"), (2, "code"), 0.9), _sem((2, "code"), (1, "code"), 0.9)]
    samples = _Samples({(1, "code"): True, (2, "code"): True})
    out = await _settle(props, a, b, samples=samples)
    assert out.kept == [] and out.removed.both_repeating == 1
    assert [o.reason for o in out.left_out] == ["both_repeating", "both_repeating"]


# --- the second scorer is asked with what the run knows ---------------------------------------------


def _permit_tables() -> dict[int, TableFacts]:
    inspections = tbl(
        313,
        "inspections",
        col("inspection_id", pk=True),
        col("permit_id", "varchar", samples=("BP-0096", "BP-0081"), description="The permit"),
        domain="city",
        description="Inspections of permitted work",
    )
    permits = tbl(
        310,
        "building_permits",
        col("permit_id", "varchar", pk=True, samples=("BP-0001", "BP-0002")),
        col("address", "varchar"),
        domain="city",
    )
    return _tables(inspections, permits)


def test_the_second_scorer_is_asked_by_name_and_shown_the_tables_and_columns_without_values():
    from provisa.discovery import rescoring

    tables = _permit_tables()
    found = [_sem((313, "permit_id"), (310, "permit_id"), 0.95)]
    asked = rescoring.question(0, found[0], tables)
    assert asked["id"] == "0" and asked["type"] == "noul"
    assert "column 'permit_id' of table 'inspections' (id 313)" in asked["instructions"]
    assert "column 'permit_id' of table 'building_permits' (id 310)" in asked["instructions"]
    shown = rescoring.evidence(found, tables)
    assert [t["table"]["name"] for t in shown["tables"]] == ["building_permits", "inspections"]
    assert shown["tables"][1]["table"]["description"] == "Inspections of permitted work"
    assert shown["tables"][1]["table"]["columns"] == ["inspection_id", "permit_id"]
    (column,) = shown["tables"][1]["columns"]
    assert column == {
        "name": "permit_id",
        "description": "The permit",
        "classes": ["unknown"],
        "tags": [],
    }
    assert "BP-0096" not in json.dumps(shown)  # sample values stay with the run's own model


async def test_jevs_scores_come_back_in_order_and_a_failed_call_gives_none(monkeypatch):
    from provisa.discovery import rescoring

    tables = _permit_tables()
    found = [
        _sem((313, "permit_id"), (310, "permit_id"), 0.95),
        _sem((313, "permit_id"), (310, "address"), 0.8),
    ]
    calls: list[tuple] = []

    async def evaluate(api_key, state, questions):
        calls.append((api_key, state, questions))
        return {"answers": {"0": {"noul": 0.93}}}

    monkeypatch.setattr("provisa.jev.client.evaluate", evaluate)
    assert await rescoring.jev_scores(found, tables, "ts-key") == [0.93, None]
    ((key, state, questions),) = calls
    assert key == "ts-key" and [q["id"] for q in questions] == ["0", "1"]
    assert state == rescoring.evidence(found, tables)

    async def failing(api_key, state, questions):
        raise RuntimeError("unreachable")

    monkeypatch.setattr("provisa.jev.client.evaluate", failing)
    assert await rescoring.jev_scores(found, tables, "ts-key") is None
