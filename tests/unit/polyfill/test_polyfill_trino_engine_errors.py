# Copyright (c) 2026 Kenneth Stott
# Canary: 8e41d2c6-0b7f-4a93-b15d-6c2f9e07a4d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The engine errors the function support measure found on Trino (run 43307392116), each a row of
the polyfill grid (REQ-2236): Trino's form of the call, or its refusal by name where Trino cannot
answer as PostgreSQL does. Each statement is the one the measure sent."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.transpile import transpile, transpile_to_trino

_TRINO = ("trino", "presto", "athena")


def _sent(sql: str) -> exp.Expr:
    return sqlglot.parse_one(transpile_to_trino(sql), read="trino")


def _refused(sql: str, function: str, dialect: str = "trino") -> None:
    with pytest.raises(RowRefused) as refused:
        transpile_to_trino(sql) if dialect == "trino" else transpile(sql, dialect)
    assert refused.value.sqlstate == "0A000"
    assert str(refused.value) == f"{function}() has no form on {dialect}"


def _calls(tree: exp.Expr) -> set[str]:
    """The names of the functions a Trino statement calls."""
    names = {node.name.lower() for node in tree.find_all(exp.Anonymous)}
    names |= {
        type(node).sql_names()[0].lower()
        for node in tree.find_all(exp.Func)
        if not isinstance(node, exp.Anonymous)
    }
    return names


def _call(function: str, args: str, over: str = "") -> str:
    return f'SELECT pg_catalog."{function}"({args}){over} AS v'


# --- the database's collation: Trino orders text by its code points --------------------------

_TEXTS = "NULL::text, NULL::text"
_CHARACTERS = "NULL::character, NULL::character"
_COLLATED = [
    *((f, _TEXTS) for f in ("text_lt", "text_le", "text_gt", "text_ge")),
    *((f, _TEXTS) for f in ("text_larger", "text_smaller")),
    *((f, _CHARACTERS) for f in ("bpcharlt", "bpcharle", "bpchargt", "bpcharge", "bpcharcmp")),
    *((f, _CHARACTERS) for f in ("bpchar_larger", "bpchar_smaller")),
]


@pytest.mark.parametrize(("function", "args"), _COLLATED)
@pytest.mark.parametrize("dialect", _TRINO)
def test_an_order_of_the_databases_collation_is_refused_on_trino(function, args, dialect):
    _refused(_call(function, args), function, dialect)


def test_duckdb_keeps_its_collated_order():
    assert "COLLATE" in transpile(_call("text_ge", "'a'::text, 'B'::text"), "duckdb").upper()


def test_an_order_by_bytes_is_still_answered_on_trino():
    assert "text_pattern_ge" not in _calls(_sent(_call("text_pattern_ge", _TEXTS)))


# --- records: Trino's row comparison refuses a NULL field ----------------------------------------

_RECORDS = "ROW(NULL::integer, 'c'::text)::record, ROW(NULL::integer, 'c'::text)::record"


@pytest.mark.parametrize(
    "function",
    [f"record_{o}" for o in ("lt", "le", "gt", "ge")]
    + [f"record_image_{o}" for o in ("lt", "le", "gt", "ge")],
)
def test_a_records_order_is_refused_on_trino(function):
    _refused(_call(function, _RECORDS), function)


@pytest.mark.parametrize("operator", [exp.GTE, exp.LT])
def test_duckdb_keeps_its_records_order(operator):
    function = "record_ge" if operator is exp.GTE else "record_lt"
    tree = sqlglot.parse_one(transpile(_call(function, _RECORDS), "duckdb"), read="duckdb")
    assert tree.find(operator) is not None


# --- arrays: Trino's array comparison refuses a NULL element -------------------------------------

_INTEGERS = "'{-7,NULL,42}'::integer[], '{-7,NULL,42}'::integer[]"
_ORDERS = ("array_lt", "array_le", "array_gt", "array_ge")


@pytest.mark.parametrize("function", [*_ORDERS, "array_larger", "array_smaller", "btarraycmp"])
def test_integer_arrays_are_ordered_element_by_element_on_trino(function):
    """A NULL element is larger than any other and equal to a NULL, as btree's array_cmp has it:
    the arrays are never compared by Trino's own operator."""
    tree = _sent(_call(function, _INTEGERS))
    compared = [
        node
        for node in tree.find_all(exp.GT, exp.GTE, exp.LT, exp.LTE)
        if isinstance(node.this, exp.Cast) and node.this.to.is_type(exp.DataType.Type.ARRAY)
    ]
    assert compared == []
    assert tree.find(exp.Lambda) is not None


@pytest.mark.parametrize("function", [*_ORDERS, "array_larger", "array_smaller", "btarraycmp"])
@pytest.mark.parametrize("args", ["'{a,B}'::text[], '{a}'::text[]", "a, b"])
def test_arrays_of_text_or_of_no_said_type_are_refused_on_trino(function, args):
    """Text orders by the database's collation, and a column's elements may be text."""
    _refused(f'SELECT pg_catalog."{function}"({args}) AS v FROM t', function)


def test_duckdb_keeps_its_array_order():
    tree = sqlglot.parse_one(transpile(_call("array_ge", _INTEGERS), "duckdb"), read="duckdb")
    assert tree.find(exp.GTE) is not None


# --- inet: Trino's forms of two addresses are subqueries it reads as correlated ------------------

_ADDRESSES = "'192.168.0.1/24'::inet, '192.168.0.1/24'::inet"
_NETWORKS = (
    "inetand",
    "inetor",
    *(
        f"network_{name}"
        for name in (
            "cmp", "eq", "ge", "gt", "larger", "le", "lt", "ne", "overlap", "smaller", "sub",
            "subeq", "sup", "supeq",
        )
    ),
)  # fmt: skip


@pytest.mark.parametrize("function", _NETWORKS)
def test_an_operation_on_two_addresses_is_refused_on_trino(function):
    _refused(_call(function, _ADDRESSES), function)


@pytest.mark.parametrize("function", ["broadcast", "family", "host", "network", "max", "min"])
def test_an_address_trino_holds_no_type_of_is_refused_there(function):
    _refused(_call(function, "NULL::inet"), function)


def test_max_of_a_number_is_still_trinos_own():
    assert "max" in _calls(_sent(_call("max", "1::integer")))


def test_duckdb_keeps_its_network_forms():
    assert transpile(_call("network_eq", _ADDRESSES), "duckdb")


# --- macaddr: Trino's to_hex reads bytes, not a number -------------------------------------------


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("macaddr_and", "NULL::macaddr, NULL::macaddr"),
        ("macaddr_or", "NULL::macaddr, NULL::macaddr"),
        ("macaddr_not", "NULL::macaddr"),
    ],
)
def test_a_macaddrs_bits_are_its_hex_digits_read_as_a_number_on_trino(function, args):
    calls = _calls(_sent(_call(function, args)))
    assert {"from_base", "to_base"} <= calls
    assert "to_hex" not in calls


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("macaddr8_and", "NULL::macaddr8, NULL::macaddr8"),
        ("macaddr8_or", "NULL::macaddr8, NULL::macaddr8"),
        ("macaddr8_not", "NULL::macaddr8"),
        ("macaddr8_set7bit", "NULL::macaddr8"),
    ],
)
def test_a_macaddr8s_bits_past_a_bigint_are_refused_on_trino(function, args):
    _refused(_call(function, args), function)


@pytest.mark.parametrize("type_name", ["macaddr", "macaddr8"])
def test_trunc_of_a_mac_address_keeps_its_first_three_bytes_on_trino(type_name):
    calls = _calls(_sent(_call("trunc", f"'08:00:2b:01:02:03'::{type_name}")))
    assert "truncate" not in calls and "trunc" not in calls


def test_trunc_of_a_number_is_still_trinos_own():
    calls = _calls(
        _sent(_call("trunc", "1.5::double precision"))
    )  # sqlglot reads truncate as trunc
    assert "trunc" in calls and "regexp_replace" not in calls


# --- a value of a type Trino holds none of -------------------------------------------------------


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("max", "NULL::xid8"),
        ("min", "NULL::xid8"),
        ("xid", "NULL::xid8"),
        ("xid8cmp", "NULL::xid8, NULL::xid8"),
        ("xid8_larger", "NULL::xid8, NULL::xid8"),
        ("xid8_smaller", "NULL::xid8, NULL::xid8"),
        ("pg_xact_status", "NULL::xid8"),
        ("lower", "NULL::int4range"),
        ("upper", "NULL::int4range"),
        ("lower", "NULL::int4multirange"),
        ("upper", "NULL::int4multirange"),
        ("unnest", "NULL::int4multirange"),
    ],
)
def test_a_value_trino_has_no_type_of_is_refused_by_name(function, args):
    _refused(_call(function, args), function)


def test_lower_of_text_is_still_trinos_own():
    assert "lower" in _calls(_sent(_call("lower", "'A'::text")))


# --- numbers past a bigint, which Trino reads as no literal --------------------------------------


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("int2_sum", "NULL::bigint, NULL::smallint"),
        ("int4_sum", "NULL::bigint, NULL::integer"),
        ("int8shl", "NULL::bigint, NULL::integer"),
    ],
)
def test_a_wrapped_bigint_has_no_literal_past_a_bigint_on_trino(function, args):
    sent = transpile_to_trino(_call(function, args))
    assert "9223372036854775808 " not in sent.replace("'9223372036854775808'", "")
    assert " 18446744073709551616" not in sent
    tree = sqlglot.parse_one(sent, read="trino")
    big = [
        n for n in tree.find_all(exp.Literal) if not n.is_string and abs(int(n.this)) > 2**63 - 1
    ]
    assert big == []


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("numeric_pl_pg_lsn", "NULL::numeric, NULL::pg_lsn"),
        ("pg_lsn_pli", "NULL::pg_lsn, NULL::numeric"),
        ("pg_lsn_mii", "NULL::pg_lsn, NULL::numeric"),
        ("pg_lsn_larger", "NULL::pg_lsn, NULL::pg_lsn"),
        ("pg_lsn_smaller", "NULL::pg_lsn, NULL::pg_lsn"),
    ],
)
def test_a_pg_lsns_position_past_a_bigint_is_refused_on_trino(function, args):
    _refused(_call(function, args), function)


# --- shifts: Trino's shift takes an integer count, and keeps the sign arithmetically -------------


@pytest.mark.parametrize(
    ("function", "args", "width"),
    [
        ("int2shr", "NULL::smallint, NULL::integer", 31),
        ("int4shr", "NULL::integer, NULL::integer", 31),
        ("int8shr", "NULL::bigint, NULL::integer", 63),
    ],
)
def test_a_right_shift_is_arithmetic_by_an_integer_count_on_trino(function, args, width):
    tree = _sent(_call(function, args))
    (shift,) = [
        n for n in tree.find_all(exp.Anonymous) if n.name.lower().startswith("bitwise_right")
    ]
    assert shift.name.lower() == "bitwise_right_shift_arithmetic"
    count = shift.expressions[1]
    assert isinstance(count, exp.Cast) and count.to.is_type(exp.DataType.Type.INT)
    assert str(width) in count.sql("trino")


# --- dates: Trino's date difference is an interval -----------------------------------------------


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("date_mi", "NULL::date, NULL::date"),
        ("date_mii", "NULL::date, NULL::integer"),
        ("date_pli", "NULL::date, NULL::integer"),
    ],
)
def test_a_dates_days_are_counted_by_date_diff_and_date_add_on_trino(function, args):
    tree = _sent(_call(function, args))
    assert _calls(tree) & {"datediff", "date_add", "dateadd"}  # sqlglot's names of Trino's
    assert not [n for n in tree.find_all(exp.Sub) if isinstance(n.this, exp.Cast)
                and n.this.to.is_type(exp.DataType.Type.DATE)]  # fmt: skip


def test_time_mi_time_is_refused_on_trino():
    _refused(_call("time_mi_time", "NULL::time, NULL::time"), "time_mi_time")


def test_make_date_is_a_date_of_its_parts_on_trino():
    calls = _calls(_sent(_call("make_date", "2024::integer, 2::integer, 29::integer")))
    assert "make_date" not in calls and "date_add" in calls


# --- functions Trino has none of ------------------------------------------------------------------


def test_array_append_of_a_null_array_is_an_array_of_the_element_on_trino():
    tree = _sent(_call("array_append", "NULL::integer[], NULL::integer"))
    assert not _calls(tree) & {"array_append", "array_prepend"}
    (array,) = tree.find_all(exp.Array)  # a NULL array is read as an empty one: {NULL}
    assert len(array.expressions) == 1


def test_array_append_is_trinos_concatenation():
    tree = _sent(_call("array_append", "'{2,3}'::integer[], 1::integer"))
    assert not _calls(tree) & {"array_append", "array_prepend"}
    assert tree.find(exp.Case) is not None  # a NULL array is read as an empty one
    (joined,) = [n for n in tree.find_all(exp.ArrayConcat, exp.DPipe)]
    assert isinstance(joined.expression, exp.Array)


def test_array_prepend_puts_the_element_first_on_trino():
    tree = _sent(_call("array_prepend", "1::integer, '{2,3}'::integer[]"))
    assert not _calls(tree) & {"array_append", "array_prepend"}
    (joined,) = [n for n in tree.find_all(exp.ArrayConcat, exp.DPipe)]
    assert isinstance(joined.this, exp.Array)


@pytest.mark.parametrize("type_name", ["integer", "smallint", "bigint"])
def test_numeric_of_an_integer_is_a_decimal_on_trino(type_name):
    tree = _sent(_call("numeric", f"5::{type_name}"))
    assert "numeric" not in _calls(tree)
    assert any(c.to.is_type(exp.DataType.Type.DECIMAL) for c in tree.find_all(exp.Cast))


def test_bool_of_an_integer_is_a_boolean_on_trino():
    tree = _sent(_call("bool", "NULL::integer"))
    assert "bool" not in _calls(tree)
    assert any(c.to.is_type(exp.DataType.Type.BOOLEAN) for c in tree.find_all(exp.Cast))


@pytest.mark.parametrize("function", ["bool", "numeric"])
def test_a_jsonb_scalar_read_as_a_type_is_refused_on_trino(function):
    _refused(_call(function, "NULL::jsonb"), function)


def test_factorial_is_a_product_on_trino():
    tree = _sent(_call("factorial", "5::bigint"))
    assert "factorial" not in _calls(tree) and "reduce" in _calls(tree)


def test_oid_of_a_bigint_is_checked_for_its_range_on_trino():
    sent = transpile_to_trino(_call("oid", "5::bigint"))
    assert "OID out of range" in sent and "OID(" not in sent.upper().replace("VOID(", "")


def test_transaction_timestamp_is_the_statements_now_on_trino():
    assert _calls(_sent(_call("transaction_timestamp", ""))) == {"currenttimestamp"} or (
        "CURRENT_TIMESTAMP" in transpile_to_trino(_call("transaction_timestamp", ""))
    )


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("current_schemas", "NULL::boolean"),
        ("pg_conf_load_time", ""),
        ("pg_my_temp_schema", ""),
        ("pg_postmaster_start_time", ""),
        ("pg_isolation_test_session_is_blocked", "NULL::integer, NULL::integer[]"),
        ("postgresql_fdw_validator", "NULL::text[], NULL::oid"),
        ("generate_subscripts", "NULL::integer[], NULL::integer"),
    ],
)
def test_a_function_trino_has_no_form_of_is_refused_by_name(function, args):
    _refused(_call(function, args), function)


@pytest.mark.parametrize("args", ["(-5)::integer, 5::integer", "(-5000000000)::bigint, 5::bigint"])
def test_random_between_two_bounds_is_both_bounds_inclusive_on_trino(args):
    sent = transpile_to_trino(_call("random", args))
    tree = sqlglot.parse_one(sent, read="trino")
    assert "lower bound must be less than or equal to upper bound" in sent
    assert not [n for n in tree.find_all(exp.Rand) if n.this is not None]


# --- set-returning calls, windows, aggregates ----------------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        "NULL::bigint, NULL::bigint",
        "NULL::bigint, NULL::bigint, NULL::bigint",
        "NULL::integer, NULL::integer",
        "NULL::integer, NULL::integer, NULL::integer",
    ],
)
def test_generate_series_in_a_select_list_is_refused_on_trino(args):
    _refused(_call("generate_series", args), "generate_series")


def test_generate_series_in_from_is_still_trinos_sequence():
    sent = transpile_to_trino("SELECT * FROM generate_series(1, 3)")
    assert sent == "SELECT * FROM UNNEST(SEQUENCE(1, 3))"


@pytest.mark.parametrize("function", ["lag", "lead"])
@pytest.mark.parametrize(
    "args", ["NULL::integer", "NULL::integer, NULL::integer", "NULL::integer, 1, NULL::integer"]
)
def test_lag_and_lead_with_no_order_are_refused_on_trino(function, args):
    _refused(_call(function, args, " OVER ()"), function)


def test_lag_with_an_order_is_still_trinos_own():
    assert "lag" in _calls(_sent("SELECT lag(x) OVER (ORDER BY y) FROM t"))


@pytest.mark.parametrize("args", ["NULL::text, NULL::text", "NULL::bytea, NULL::bytea"])
def test_string_agg_of_a_delimiter_trino_takes_no_value_of_is_refused(args):
    _refused(_call("string_agg", args), "string_agg")


def test_string_agg_of_a_constant_delimiter_is_still_trinos_listagg():
    assert "listagg" in transpile_to_trino("SELECT string_agg(x, ',') FROM t").lower()


@pytest.mark.parametrize("args", ["NULL::text[]", "NULL::text[], NULL::text[]"])
def test_json_object_of_arrays_is_refused_on_trino(args):
    _refused(_call("json_object", args), "json_object")


def test_each_row_is_in_the_grid_and_the_orders_moved_out_of_the_old_table():
    from provisa.transpiler.pg_forms_types import FORMS_BY_NAME
    from provisa.transpiler.polyfill import GRID

    in_the_grid = {f.name for f in GRID.features}
    rows = {
        *(name for name, _ in _COLLATED),
        *_NETWORKS,
        *_ORDERS,
        "array_larger",
        "array_smaller",
        "btarraycmp",
        *(f"record_{o}" for o in ("lt", "le", "gt", "ge")),
        *(f"record_image_{o}" for o in ("lt", "le", "gt", "ge")),
        "int2_sum",
        "int8shl",
        "int8shr",
        "date_mi",
        "make_date",
        "factorial",
        "lag",
        "generate_series",
        "max",
        "trunc",
    }
    assert rows <= in_the_grid
    # The orders of arrays and records moved whole: no old table holds them.
    moved = {*_ORDERS, "array_larger", "array_smaller"}
    moved |= {f"record_{o}" for o in ("lt", "le", "gt", "ge")}
    moved |= {f"record_image_{o}" for o in ("lt", "le", "gt", "ge")}
    assert not moved & set(FORMS_BY_NAME)


# --- confirming run 43324155455 ------------------------------------------------------------------

_COUNTS = [
    "(-7)::integer",
    "(-1)::integer",
    "0::integer",
    "63::integer",
    "64::integer",
    "70::integer",
]


@pytest.mark.parametrize("count", _COUNTS)
def test_int8shl_is_trinos_own_wrapping_shift_by_the_counts_low_six_bits(count):
    """C's int64 << of the count modulo 64 wraps in two's complement, as Java's << does; a power of
    two read as a double loses digits past 2^53 (int8shl(-7, -7) answered ...090, not ...104)."""
    tree = _sent(_call("int8shl", f"(-7)::bigint, {count}"))
    (shift,) = [n for n in tree.find_all(exp.Anonymous) if n.name.lower() == "bitwise_left_shift"]
    masked = shift.expressions[1]
    assert isinstance(masked, exp.Cast) and masked.to.is_type(exp.DataType.Type.INT)
    assert isinstance(masked.this, exp.BitwiseAnd) and masked.this.expression.name == "63"
    assert tree.find(exp.Pow) is None and "power" not in _calls(tree)


@pytest.mark.parametrize("count", _COUNTS)
@pytest.mark.parametrize("function", ["int2shl", "int4shl"])
def test_a_narrow_left_shift_reads_the_counts_low_five_bits_exactly(function, count):
    """int2shl and int4shl shift the promoted int by the count modulo 32: a power of two of at most
    31 bits, which a double holds exactly."""
    value = "(-7)::smallint" if function == "int2shl" else "(-7)::integer"
    tree = _sent(_call(function, f"{value}, {count}"))
    exponents = [n.expression for n in tree.find_all(exp.Pow)]
    assert exponents and all(
        isinstance(e.unnest(), exp.BitwiseAnd) and e.unnest().expression.name == "31"
        for e in exponents
    )


@pytest.mark.parametrize("count", _COUNTS)
@pytest.mark.parametrize(
    ("function", "value", "width"),
    [
        ("int2shr", "(-7)::smallint", 31),
        ("int4shr", "(-7)::integer", 31),
        ("int8shr", "(-7)::bigint", 63),
    ],
)
def test_a_right_shift_reads_the_counts_low_bits(function, value, width, count):
    tree = _sent(_call(function, f"{value}, {count}"))
    (shift,) = [
        n
        for n in tree.find_all(exp.Anonymous)
        if n.name.lower() == "bitwise_right_shift_arithmetic"
    ]
    masked = shift.expressions[1]
    assert isinstance(masked.this, exp.BitwiseAnd) and masked.this.expression.name == str(width)


@pytest.mark.parametrize(
    ("function", "args"), [("bool", "'true'::jsonb"), ("numeric", "'3'::jsonb")]
)
def test_a_jsonb_literal_read_as_a_type_is_refused_on_trino(function, args):
    """The literal reaches the grid as JSON_PARSE of its text: still a jsonb value."""
    _refused(_call(function, args), function)


def _expanded(sql: str) -> str:
    """The statement as pgwire sends it: each server function in its form (metadata_functions)."""
    from provisa.transpiler.metadata_facts import CatalogFacts
    from provisa.transpiler.metadata_functions import expand

    facts = CatalogFacts("analyst", privileges=((16385, "SELECT"),))
    return expand(sqlglot.parse_one(sql, read="postgres"), facts).sql(dialect="postgres")


@pytest.mark.parametrize(
    ("function", "args"),
    [
        ("pg_isolation_test_session_is_blocked", "NULL::integer, NULL::integer[]"),
        ("pg_isolation_test_session_is_blocked", "1::integer, '{1,NULL,3}'::integer[]"),
        ("postgresql_fdw_validator", "NULL::text[], NULL::oid"),
        ("postgresql_fdw_validator", "'{a,\"b c\",NULL}'::text[], 1::oid"),
    ],
)
def test_a_server_forms_array_is_its_text_as_array_out_writes_it_on_trino(function, args):
    """The form reads each argument as text: an array's text is array_out's, not Trino's cast."""
    tree = sqlglot.parse_one(transpile_to_trino(_expanded(_call(function, args))), read="trino")
    unread = [
        c
        for c in tree.find_all(exp.Cast)
        if c.to.is_type(exp.DataType.Type.VARCHAR)
        and isinstance(c.this, exp.Cast)
        and c.this.to.is_type(exp.DataType.Type.ARRAY)
    ]
    assert unread == []
    assert "array_to_string" in _calls(tree)  # sqlglot's name of Trino's array_join


def test_an_array_of_text_is_quoted_as_array_out_quotes_it_on_trino():
    sent = transpile_to_trino("SELECT CAST('{a,\"b c\"}'::text[] AS text) AS v")
    assert "REGEXP_LIKE" in sent.upper() and "'NULL'" in sent


@pytest.mark.parametrize("element", ["date", "numeric", "boolean"])
def test_an_array_of_another_type_as_text_is_refused_on_trino(element):
    _refused(f"SELECT CAST(NULL::{element}[] AS text) AS v", "array_out")


def test_pg_xact_status_as_pgwire_sends_it_is_refused_on_trino():
    with pytest.raises(RowRefused) as refused:
        transpile_to_trino(_expanded(_call("pg_xact_status", "NULL::xid8")))
    assert refused.value.sqlstate == "0A000"
    assert str(refused.value) == "xid8in() has no form on trino"
