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
the contexts sent, windows sized from the model's limits, split and retry, the request count, the
direction rules in order, the removals, and Stop."""

# Requirements: REQ-413, REQ-464

from __future__ import annotations

import json
import pytest

from provisa.discovery import context
from provisa.discovery.columns import (
    DATE,
    DECIMAL,
    TEXT,
    TIMESTAMP,
    TIMESTAMPTZ,
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
from provisa.discovery.runner import Tally, execute
from provisa.discovery.windows import (
    TOKENS_PER_SUGGESTION,
    at_most,
    count_by_group,
    pack,
    plan,
    window_chars,
)
from provisa.llm.client import Completion
from provisa.llm.limits import CHARS_PER_TOKEN, DEFAULT_LIMITS, limits_for


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
    ("spelled", "group"),
    [("uuid", UUID), ("date", DATE), ("timestamp", TIMESTAMP), ("timestamptz", TIMESTAMPTZ)],
)
def test_uuid_date_and_each_timestamp_type_are_groups_of_their_own(spelled, group):
    assert type_groups(spelled, subtype=None, array_depth=0) == {group}


def test_the_two_timestamp_types_are_not_compared_with_each_other():
    assert not comparable(col("at", "timestamp"), col("at", "timestamptz"), TIMESTAMP)
    assert not comparable(col("at", "timestamp"), col("at", "timestamptz"), TIMESTAMPTZ)


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
    windows = pack([t], WHOLE, 10_000)
    sent = windows[0].blocks()[0]
    assert sent["table"]["columns"] == ["id", "is_paid"]
    assert [c["name"] for c in sent["compared_columns"]] == ["id"]


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


def test_a_window_pair_with_no_intersecting_classes_is_not_asked():
    source = tbl(1, "stores", col("city", "text", classes=("world_city",)))
    target = tbl(2, "currencies", col("code", "text", classes=("iso4217_currency",)))
    _, requests = plan([source], [target], *DEFAULT_LIMITS)
    assert requests == []


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
    assert context.table_context(t) == {
        "id": 7,
        "name": "orders",
        "description": "One row per order",
        "tags": ["finance"],
        "role": "fact",
        "columns": ["id", "customer_id", "notes"],
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
    text = context.instructions(WHOLE, 10, either_direction=False)
    assert "A shared pii tag alone is weak evidence" in text


# --- windows ---------------------------------------------------------------------------------------


def test_suggestions_per_request_are_the_output_limit_at_150_tokens_each():
    assert TOKENS_PER_SUGGESTION == 150
    assert at_most(4_096) == 27
    assert at_most(128_000) == 853


def test_an_unknown_model_sizes_from_the_default_pair():
    limits = limits_for("acme", "never-heard-of-it", {})
    assert (limits.input_tokens, limits.output_tokens) == DEFAULT_LIMITS == (32_000, 4_096)


def test_a_window_is_half_the_input_limit_in_characters_less_the_instructions():
    k = at_most(4_096)
    fixed = len(context.instructions(WHOLE, k, either_direction=False))
    headers = len("\n## SOURCE window\n") + len("\n## TARGET window\n")
    assert CHARS_PER_TOKEN == 3
    assert (
        window_chars(32_000, WHOLE, k, either_direction=False)
        == (32_000 * 3 - fixed - headers) // 2
    )


def _wide(table_id: int, name: str, n: int) -> TableFacts:
    return tbl(
        table_id,
        name,
        *[col(f"{name}_c{i}", samples=(str(i) * 40,)) for i in range(n)],
    )


def test_windows_are_packed_by_character_weight_within_the_room():
    tables = [_wide(i, f"t{i}", 6) for i in range(1, 6)]
    room = 2_000
    windows = pack(tables, WHOLE, room)
    assert len(windows) > 1
    assert all(w.weight <= room for w in windows)
    packed = [(t.table_id, c.name) for w in windows for t, c in w.columns()]
    assert packed == [(t.table_id, c.name) for t in tables for c in t.columns]


def test_a_larger_input_limit_makes_fewer_requests():
    tables = [_wide(i, f"t{i}", 8) for i in range(1, 9)]
    _, small = plan(tables, None, 3_000, 4_096)
    _, large = plan(tables, None, 300_000, 4_096)
    assert len(large) < len(small)
    assert len(large) == 1


def test_every_request_fits_the_input_limit():
    tables = [_wide(i, f"t{i}", 8) for i in range(1, 9)]
    k, requests = plan(tables, [*tables[:3]], 4_000, 4_096)
    assert requests
    assert all(len(r.prompt(k)) <= 4_000 * CHARS_PER_TOKEN for r in requests)


def test_the_request_count_is_given_by_group():
    t1 = tbl(1, "orders", col("id", pk=True), col("customer_ref", "uuid"), col("day", "date"))
    t2 = tbl(2, "customers", col("id", pk=True), col("ref", "uuid"))
    _, requests = plan([t1, t2], None, *DEFAULT_LIMITS)
    assert count_by_group(requests) == {WHOLE: 1, UUID: 1}


# --- split and retry, request count, stop ----------------------------------------------------------


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


def _answer(items: list[dict], *, cut_off: bool = False, closed: bool = True) -> Completion:
    text = json.dumps(items)
    if not closed:
        text = text[:-1] + ', {"source_table_id": 1, "sou'
    return Completion(text, "acme", "m", None, cut_off)


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


async def test_the_request_count_equals_the_requests_the_run_then_makes():
    tables = [_wide(i, f"t{i}", 8) for i in range(1, 7)]
    k, requests = plan(tables, None, 3_000, 4_096)
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        return _answer([])

    tally, _ = await execute(requests, k, 4_096, ask, _Watcher(), to_thread=_direct)
    assert len(asked) == len(requests) == sum(count_by_group(requests).values())
    assert tally.done == tally.total == len(requests)


async def test_a_cut_off_answer_splits_the_request_and_keeps_its_complete_suggestions():
    orders, customers = _pair_tables()
    k, requests = plan([orders], [customers], *DEFAULT_LIMITS)
    assert len(requests) == 1
    answers = [
        _answer([_item((1, "customer_id"), (2, "id"))], cut_off=True, closed=False),
        _answer([]),
        _answer([_item((1, "store_id"), (2, "region_id"))]),
    ]
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        return answers.pop(0)

    tally, proposals = await execute(requests, k, 4_096, ask, _Watcher(), to_thread=_direct)
    assert len(asked) == 3
    assert tally.split == 1
    assert tally.total == 3 and tally.done == 3
    assert {p.edge for p in proposals} == {
        (1, "customer_id", 2, "id"),
        (1, "store_id", 2, "region_id"),
    }


async def test_a_full_answer_splits_the_request():
    orders, customers = _pair_tables()
    _, requests = plan([orders], [customers], *DEFAULT_LIMITS)
    k = 1
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        return _answer([_item((1, "customer_id"), (2, "id"))] if len(asked) == 1 else [])

    tally, proposals = await execute(requests, k, 150, ask, _Watcher(), to_thread=_direct)
    assert tally.split >= 1
    assert len(asked) > 1
    assert (1, "customer_id", 2, "id") in {p.edge for p in proposals}


async def test_a_request_the_vendor_refuses_as_too_long_is_split_and_asked_again():
    orders, customers = _pair_tables()
    k, requests = plan([orders], [customers], *DEFAULT_LIMITS)
    asked: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        asked.append(prompt)
        if len(asked) == 1:
            raise RuntimeError("Error code: 400 - prompt is too long: 250000 tokens > 200000")
        return _answer([])

    tally, _ = await execute(requests, k, 4_096, ask, _Watcher(), to_thread=_direct)
    assert tally.split == 1
    assert tally.failed == []
    assert len(asked) == 3


async def test_a_cut_off_answer_that_cannot_split_keeps_what_it_finished():
    single = tbl(1, "a", col("x_id"))
    other = tbl(2, "b", col("id", pk=True))
    k, requests = plan([single], [other], *DEFAULT_LIMITS)
    tally, proposals = await execute(
        requests,
        k,
        4_096,
        lambda p, m: _answer([_item((1, "x_id"), (2, "id"))], cut_off=True, closed=False),
        _Watcher(),
        to_thread=_direct,
    )
    assert tally.split == 0 and tally.cut_short == 1
    assert [p.edge for p in proposals] == [(1, "x_id", 2, "id")]


async def test_one_failed_request_is_reported_and_the_rest_continue():
    tables = [_wide(i, f"t{i}", 8) for i in range(1, 7)]
    k, requests = plan(tables, None, 3_000, 4_096)
    assert len(requests) > 2
    calls = 0

    def ask(prompt: str, max_tokens: int) -> Completion:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("vendor unavailable")
        return _answer([])

    tally, _ = await execute(requests, k, 4_096, ask, _Watcher(), to_thread=_direct)
    assert calls == len(requests)
    assert tally.failed == [{"group": requests[0].group, "error": "vendor unavailable"}]


async def test_stop_ends_the_run_after_the_request_in_flight_and_keeps_what_was_found():
    tables = [_wide(i, f"t{i}", 8) for i in range(1, 7)]
    k, requests = plan(tables, None, 3_000, 4_096)
    first = requests[0].source.columns()
    a, b = first[0], first[1]
    asked = 0

    def ask(prompt: str, max_tokens: int) -> Completion:
        nonlocal asked
        asked += 1
        return _answer([_item((a[0].table_id, a[1].name), (b[0].table_id, b[1].name))])

    tally, proposals = await execute(
        requests, k, 4_096, ask, _Watcher(stop_after=1), to_thread=_direct
    )
    assert asked == 1
    assert tally.stopped
    assert [p.edge for p in proposals] == [(a[0].table_id, a[1].name, b[0].table_id, b[1].name)]


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
