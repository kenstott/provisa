# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8b2d61-9e37-4a05-b1c8-7d2e6a90f3c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino's wrong answers at edge values (the function support measure, run 44670380355), each the
statement the measure sent, as Provisa now writes it for Trino: a form that answers as
postgres:17.11 does, PostgreSQL's own error where it raised, or a refusal by name where Trino holds
no exact answer (REQ-2236, REQ-2171)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.pgwire.catalog_server_answers import macros
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.transpile import transpile


def _trino(sql: str) -> str:
    return transpile(f"SELECT pg_catalog.{sql} AS v", "trino")


def _refused(sql: str) -> RowRefused:
    with pytest.raises(RowRefused) as refused:
        _trino(sql)
    return refused.value


def _tree(sql: str) -> exp.Expr:
    return sqlglot.parse_one(_trino(sql), read="trino")


# --- floats: NaN after every other value ----------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "\"float8lt\"(0.5::double precision, 'NaN'::double precision)",
        "\"float4ge\"('NaN'::real, 0.5::real)",
        "\"float48le\"(0.5::real, 'NaN'::double precision)",
        "\"float84gt\"('NaN'::double precision, 0.5::real)",
    ],
)
def test_a_float_comparison_orders_nan_last(sql):
    written = _trino(sql)
    assert "IS_NAN" in written and "IS NULL" in written


@pytest.mark.parametrize("name", ["float8larger", "float4smaller"])
def test_the_larger_or_smaller_of_nan_reads_nan_last(name):
    kind = "double precision" if name.startswith("float8") else "real"
    written = _trino(f"\"{name}\"('NaN'::{kind}, 0.5::{kind})")
    assert "IS_NAN" in written


@pytest.mark.parametrize(
    ("sql", "state"),
    [
        ("\"acos\"('-Infinity'::double precision)", "22003"),
        ("\"sin\"('-Infinity'::double precision)", "22003"),
        ("\"cot\"('-Infinity'::double precision)", "22003"),
        ("\"sqrt\"('-Infinity'::double precision)", "2201F"),
        ("\"ln\"('-Infinity'::double precision)", "2201E"),
        ("\"log10\"('-Infinity'::double precision)", "2201E"),
        ("\"pow\"('-Infinity'::double precision, 0.5::double precision)", "2201F"),
        ("\"exp\"('1.7976931348623157e+308'::double precision)", "22003"),
        ("\"degrees\"('1.7976931348623157e+308'::double precision)", "22003"),
    ],
)
def test_a_float_outside_its_domain_raises_postgresqls_error(sql, state):
    assert f"FAIL('postgresql-error {state}:" in _trino(sql)


@pytest.mark.parametrize("name", ["acosh", "asinh"])
def test_a_large_hyperbolic_inverse_is_ln_2x(name):
    """x * x overflows past 1e154: past 1e8 the form is ln(x) + ln(2), libm's answer."""
    written = _trino(f"\"{name}\"('1.7976931348623157e+308'::double precision)")
    assert "1e8" in written and "LN(2.0)" in written


def test_sign_of_nan_is_zero():
    assert "IS_NAN" in _trino("\"sign\"('NaN'::double precision)")


def test_power_reads_c_pows_special_cases_before_javas():
    written = _trino("\"power\"(1::double precision, 'NaN'::double precision)")
    assert "NAN()" in written and "IS_INFINITE" in written


@pytest.mark.parametrize("kind", ["double precision", "real"])
def test_in_range_of_a_nan_base(kind):
    written = _trino(f"\"in_range\"(0.5::{kind}, 'NaN'::{kind}, 0.5::double precision, true, true)")
    assert "IS_NAN" in written and "22013" in written


@pytest.mark.parametrize(
    "function", ["sum", "avg", "var_pop", "var_samp", "variance", "stddev", "stddev_pop"]
)
def test_a_double_aggregate_raises_its_sums_overflow(function):
    sql = (
        f'SELECT pg_catalog."{function}"(c1) AS v FROM (VALUES '
        "('1.7976931348623157e+308'::double precision), "
        "('1.7976931348623157e+308'::double precision)) AS t(c1)"
    )
    written = transpile(sql, "trino")
    assert "COUNT_IF(NOT IS_FINITE(c1)) = 0" in written
    assert "22003: value out of range: overflow" in written


@pytest.mark.parametrize("function", ["regr_slope", "regr_intercept"])
def test_a_regression_of_nan_is_nan(function):
    sql = (
        f'SELECT pg_catalog."{function}"(c1, c2) AS v FROM (VALUES '
        "(0.5::double precision, 'NaN'::double precision)) AS t(c1, c2)"
    )
    assert "NAN()" in transpile(sql, "trino")


# --- literals read as PostgreSQL's input reads them ----------------------------------------------


@pytest.mark.parametrize(
    ("sql", "state", "message"),
    [
        (
            "\"oideq\"('4294967296'::oid, 12345::oid)",
            "22003",
            'value "4294967296" is out of range for type oid',
        ),
        (
            "\"pg_column_compression\"('2147483648'::integer)",
            "22003",
            'value "2147483648" is out of range for type integer',
        ),
        ("\"isfinite\"('5874898-01-01'::date)", "22008", 'date out of range: "5874898-01-01"'),
        (
            "\"isfinite\"('294277-01-01 00:00:00'::timestamp without time zone)",
            "22008",
            'timestamp out of range: "294277-01-01 00:00:00"',
        ),
        (
            "\"date_part\"('year'::text, '5874897-12-31'::date)",
            "22008",
            "date out of range for timestamp",
        ),
        ("\"chr\"('0'::integer)", None, None),
    ],
)
def test_a_literal_past_postgresqls_range_raises_its_error(sql, state, message):
    if state is None:
        assert "54000: null character not permitted" in _trino(sql)
        return
    refused = _refused(sql)
    assert (refused.sqlstate, str(refused)) == (state, message)


@pytest.mark.parametrize(
    "sql",
    [
        "\"abs\"('123456789012345678901234567890.123456789012345678901234567890'::numeric)",
        "\"timestamp_lt\"('294276-12-31 23:59:59.999999'::timestamp without time zone, "
        "'2024-06-15 12:30:45'::timestamp without time zone)",
        "\"int8_sum\"('-0.000000000000000000001'::numeric, 3::bigint)",
        "\"numeric_mod\"('-0.000000000000000000001'::numeric, 2.5::numeric)",
    ],
)
def test_a_value_trino_holds_no_exact_form_of_is_refused_by_name(sql):
    refused = _refused(sql)
    assert refused.sqlstate == "0A000"
    assert "on trino" in str(refused)


def test_a_numeric_literal_is_of_its_digits_alone_on_trino():
    written = _trino("\"numeric_add\"('-0.000000000000000000001'::numeric, 2.5::numeric)")
    assert "DECIMAL(21, 21)" in written and "DECIMAL(2, 1)" in written


def test_a_bigint_step_of_a_sum_is_a_decimal_of_19_digits():
    assert "DECIMAL(19, 0)" in _trino('"int8_sum"(2.5::numeric, 3::bigint)')


# --- times to the microsecond -----------------------------------------------------------------------


@pytest.mark.parametrize("name", ["time_lt", "time_cmp", "time_larger"])
def test_times_compare_to_the_microsecond(name):
    written = _trino(
        f"\"{name}\"('23:59:59.999999'::time without time zone, '12:30:45'::time without time zone)"
    )
    assert "CAST('23:59:59.999999' AS TIME(6))" in written
    assert "AS TIME)" not in written


@pytest.mark.parametrize(
    "sql",
    [
        "\"datetime_pl\"('2024-06-15'::date, '23:59:59.999999'::time without time zone)",
        "\"timedate_pl\"('23:59:59.999999'::time without time zone, '2024-06-15'::date)",
        "\"timestamp\"('2024-06-15'::date, '23:59:59.999999'::time without time zone)",
    ],
)
def test_a_date_plus_a_time_is_a_timestamp_of_microseconds(sql):
    written = _trino(sql)
    assert "AS TIMESTAMP(6))" in written and "CAST('23:59:59.999999' AS TIME(6))" in written


def test_extract_of_a_date_reads_the_date_not_a_timestamp():
    tree = _tree("\"extract\"('year'::text, '5874897-12-31'::date)")
    assert not any(c.to.is_type(exp.DataType.Type.TIMESTAMP) for c in tree.find_all(exp.Cast))


# --- texts ------------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["length", "char_length", "character_length", "text", "name"])
def test_a_character_values_text_drops_its_padding(name):
    assert "TRIM(TRAILING ' '" in _trino(f"\"{name}\"(''::character)")


@pytest.mark.parametrize("name", ["text", "bpchar"])
def test_a_one_byte_char_is_held_as_its_byte(name):
    assert '"char"' not in _trino(f'"{name}"(\'\'::"char")')


def test_position_by_name_is_strpos_in_its_order():
    assert _trino("\"position\"(''::text, 'abc'::text)") == "SELECT STRPOS('', 'abc') AS v"


def test_replace_of_an_empty_text_leaves_the_string():
    written = _trino("\"replace\"('abc'::text, ''::text, 'abc'::text)")
    assert "WHEN '' = '' THEN 'abc'" in written


def test_a_negative_substring_length_raises_22011():
    written = _trino("\"substr\"('abc'::text, 3::integer, '-2147483648'::integer)")
    assert "22011" in written


# --- arrays -----------------------------------------------------------------------------------------


def test_cardinality_of_two_dimensions_counts_every_element():
    assert "CARDINALITY(FLATTEN(" in _trino("\"cardinality\"('{{1,2},{3,4}}'::integer[])")


def test_unnest_of_two_dimensions_answers_each_element():
    assert "UNNEST(FLATTEN(" in _trino("\"unnest\"('{{1,2},{3,4}}'::integer[])")


def test_array_to_string_of_a_null_text_leaves_null_elements_out():
    written = _trino("\"array_to_string\"('{1,2,3}'::integer[], 'abc'::text, NULL::text)")
    assert "IS NULL THEN ARRAY_JOIN(" in written


# --- money -----------------------------------------------------------------------------------------


def test_money_times_a_float_rounds_through_a_bigint():
    written = _trino("\"cash_mul_flt8\"('-92233720368547758.08'::money, 0.5::double precision)")
    assert "AS BIGINT) AS DECIMAL(19, 0))" in written
    assert "AS DECIMAL), 0)" not in written


# --- postgresql_fdw_validator (the catalog's) -------------------------------------------------------


@pytest.mark.parametrize(
    ("options", "catalog", "answer"),
    [
        ("[]", 12345, True),
        ("['host=x']", 1417, True),
        ("['user=a']", 1418, True),
        ("['', 'é']", 12345, 'invalid option ""'),
        ("['user=a', 'host=b']", 1418, 'invalid option "host"'),
    ],
)
def test_postgresql_fdw_validator_checks_each_options_name(options, catalog, answer):
    con = duckdb.connect()
    for macro in macros():
        if "postgresql_fdw_validator" in macro:
            con.execute(macro)
    statement = f"SELECT postgresql_fdw_validator({options}::VARCHAR[], {catalog})"
    if answer is True:
        assert con.execute(statement).fetchone() == (True,)
        return
    with pytest.raises(duckdb.Error, match=f"42601: {answer}"):
        con.execute(statement)
