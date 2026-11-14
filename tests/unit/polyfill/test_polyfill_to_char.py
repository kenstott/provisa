# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6c1e94-7d3a-4b58-9e21-c4a8d0f3b617
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""to_char(timestamp, text) written from PostgreSQL's own tokens (REQ-2167, REQ-2236): the template
read as formatting.c reads it, each keyword one engine expression that yields PostgreSQL's text,
on ClickHouse (run on the embedded engine), DuckDB (run) and Trino (its SQL). Every expected
answer is PostgreSQL's (17 and 18 write the same), padding, case, FM and TH included."""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.core.pg_error import PostgresError, from_engine
from provisa.core.session_time_zone import StatementZone, bind_zone, reset_zone
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import instants
from provisa.transpiler.to_char import Token, plain_strftime, tokens
from provisa.transpiler.transpile import transpile, transpile_to_duckdb


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _on_clickhouse(runtime: ClickHouseFederationRuntime, sql: str) -> list[tuple]:
    return list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())


def _on_duckdb(sql: str) -> list[tuple]:
    with duckdb.connect() as con:
        return con.execute(transpile_to_duckdb(sql)).fetchall()


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


# --- The template, as formatting.c reads it ----------------------------------------------------


def _read(template: str) -> list[str]:
    return [t.written if t.keyword else t.text for t in tokens(template)]


@pytest.mark.parametrize(
    ("template", "read"),
    [
        ("%Y", ["%", "Y"]),  # a percent sign, then the year's last digit
        ("SSSS", ["SSSS"]),  # the seconds past midnight, not SS twice
        ("DDTHH24", ["DDTH", "H", "2", "4"]),  # DD with its TH suffix, then the text H24
        ("DDDD", ["DDD", "D"]),  # the day of the year, then of the week
        ("FMFMDD", ["F", "M", "DD"]),  # one prefix: the second FM is text
        ("FMTMMonth", ["T", "MM", "o", "n", "t", "h"]),
        ("DDTHSP", ["DDTH", "S", "P"]),  # one suffix
        ("Yyyy", ["Y", "yyy"]),
        ('"YYYY"YYYY', ["Y", "Y", "Y", "Y", "YYYY"]),
        ('"a\\"b"', ["a", '"', "b"]),  # a backslash in quotes quotes the next character
        ("\\MM", ["\\", "MM"]),  # outside quotes a backslash is itself ...
        ('\\"x', ['"', "x"]),  # ... unless it precedes a double quote
        ("FMx", ["x"]),  # a prefix before no keyword is dropped
        ("of", ["of"]),
    ],
)
def test_a_template_is_read_as_formatting_c_reads_it(template, read):
    assert _read(template) == read


def test_a_keyword_carries_its_modifiers():
    assert tokens("FMDDth") == (Token("DD", keyword=True, fill=True, suffix="th"),)
    assert tokens("TMMon") == (Token("Mon", keyword=True, translated=True),)


@pytest.mark.parametrize(
    ("template", "strftime"),
    [("YYYY-MM-DD", "%Y-%m-%d"), ("hh24:mi:ss", "%H:%M:%S"), ("%Y", None), ("SSSS", None),
     ("FMDD", None), ("DDth", None), ('"YYYY"', None)],
)  # fmt: skip
def test_the_plain_strftime_of_a_template_is_read_from_its_tokens(template, strftime):
    assert plain_strftime(template) == strftime


# --- Each keyword, on each engine --------------------------------------------------------------

#: A Wednesday at 08:03:04.000001; the Sunday of ISO week 53 of 2004 a microsecond before
#: midnight; a day of elevens (TH after 11); a Thursday afternoon at 13:02:03.123456.
_STAMPS = (
    "2024-06-05 08:03:04.000001",
    "2005-01-02 23:59:59.999999",
    "2011-11-11 11:11:11.011",
    "2022-09-22 13:02:03.123456",
)
_VALUES = ", ".join(f"({i}, {_quoted(s)}::timestamp)" for i, s in enumerate(_STAMPS))

#: Each date/time keyword of PostgreSQL's Table 9.26, plain, FM, TH and th, and PostgreSQL's
#: text of it at each of _STAMPS.
_TOKENS = [
    ("HH", ("08", "11", "11", "01")),
    ("FMHH", ("8", "11", "11", "1")),
    ("HHTH", ("08TH", "11TH", "11TH", "01ST")),
    ("HHth", ("08th", "11th", "11th", "01st")),
    ("HH12", ("08", "11", "11", "01")),
    ("FMHH12", ("8", "11", "11", "1")),
    ("HH12TH", ("08TH", "11TH", "11TH", "01ST")),
    ("HH12th", ("08th", "11th", "11th", "01st")),
    ("HH24", ("08", "23", "11", "13")),
    ("FMHH24", ("8", "23", "11", "13")),
    ("HH24TH", ("08TH", "23RD", "11TH", "13TH")),
    ("HH24th", ("08th", "23rd", "11th", "13th")),
    ("MI", ("03", "59", "11", "02")),
    ("FMMI", ("3", "59", "11", "2")),
    ("MITH", ("03RD", "59TH", "11TH", "02ND")),
    ("MIth", ("03rd", "59th", "11th", "02nd")),
    ("SS", ("04", "59", "11", "03")),
    ("FMSS", ("4", "59", "11", "3")),
    ("SSTH", ("04TH", "59TH", "11TH", "03RD")),
    ("SSth", ("04th", "59th", "11th", "03rd")),
    ("MS", ("000", "999", "011", "123")),
    ("FMMS", ("000", "999", "011", "123")),
    ("MSTH", ("000TH", "999TH", "011TH", "123RD")),
    ("MSth", ("000th", "999th", "011th", "123rd")),
    ("US", ("000001", "999999", "011000", "123456")),
    ("FMUS", ("000001", "999999", "011000", "123456")),
    ("USTH", ("000001ST", "999999TH", "011000TH", "123456TH")),
    ("USth", ("000001st", "999999th", "011000th", "123456th")),
    ("FF1", ("0", "9", "0", "1")),
    ("FMFF1", ("0", "9", "0", "1")),
    ("FF1TH", ("0TH", "9TH", "0TH", "1ST")),
    ("FF1th", ("0th", "9th", "0th", "1st")),
    ("FF2", ("00", "99", "01", "12")),
    ("FMFF2", ("00", "99", "01", "12")),
    ("FF2TH", ("00TH", "99TH", "01ST", "12TH")),
    ("FF2th", ("00th", "99th", "01st", "12th")),
    ("FF3", ("000", "999", "011", "123")),
    ("FMFF3", ("000", "999", "011", "123")),
    ("FF3TH", ("000TH", "999TH", "011TH", "123RD")),
    ("FF3th", ("000th", "999th", "011th", "123rd")),
    ("FF4", ("0000", "9999", "0110", "1234")),
    ("FMFF4", ("0000", "9999", "0110", "1234")),
    ("FF4TH", ("0000TH", "9999TH", "0110TH", "1234TH")),
    ("FF4th", ("0000th", "9999th", "0110th", "1234th")),
    ("FF5", ("00000", "99999", "01100", "12345")),
    ("FMFF5", ("00000", "99999", "01100", "12345")),
    ("FF5TH", ("00000TH", "99999TH", "01100TH", "12345TH")),
    ("FF5th", ("00000th", "99999th", "01100th", "12345th")),
    ("FF6", ("000001", "999999", "011000", "123456")),
    ("FMFF6", ("000001", "999999", "011000", "123456")),
    ("FF6TH", ("000001ST", "999999TH", "011000TH", "123456TH")),
    ("FF6th", ("000001st", "999999th", "011000th", "123456th")),
    ("SSSS", ("28984", "86399", "40271", "46923")),
    ("FMSSSS", ("28984", "86399", "40271", "46923")),
    ("SSSSTH", ("28984TH", "86399TH", "40271ST", "46923RD")),
    ("SSSSth", ("28984th", "86399th", "40271st", "46923rd")),
    ("SSSSS", ("28984", "86399", "40271", "46923")),
    ("FMSSSSS", ("28984", "86399", "40271", "46923")),
    ("SSSSSTH", ("28984TH", "86399TH", "40271ST", "46923RD")),
    ("SSSSSth", ("28984th", "86399th", "40271st", "46923rd")),
    ("AM", ("AM", "PM", "AM", "PM")),
    ("FMAM", ("AM", "PM", "AM", "PM")),
    ("AMTH", ("AM", "PM", "AM", "PM")),
    ("AMth", ("AM", "PM", "AM", "PM")),
    ("am", ("am", "pm", "am", "pm")),
    ("FMam", ("am", "pm", "am", "pm")),
    ("amTH", ("am", "pm", "am", "pm")),
    ("amth", ("am", "pm", "am", "pm")),
    ("PM", ("AM", "PM", "AM", "PM")),
    ("FMPM", ("AM", "PM", "AM", "PM")),
    ("PMTH", ("AM", "PM", "AM", "PM")),
    ("PMth", ("AM", "PM", "AM", "PM")),
    ("pm", ("am", "pm", "am", "pm")),
    ("FMpm", ("am", "pm", "am", "pm")),
    ("pmTH", ("am", "pm", "am", "pm")),
    ("pmth", ("am", "pm", "am", "pm")),
    ("A.M.", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("FMA.M.", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("A.M.TH", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("A.M.th", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("a.m.", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("FMa.m.", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("a.m.TH", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("a.m.th", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("P.M.", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("FMP.M.", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("P.M.TH", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("P.M.th", ("A.M.", "P.M.", "A.M.", "P.M.")),
    ("p.m.", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("FMp.m.", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("p.m.TH", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("p.m.th", ("a.m.", "p.m.", "a.m.", "p.m.")),
    ("Y,YYY", ("2,024", "2,005", "2,011", "2,022")),
    ("FMY,YYY", ("2,024", "2,005", "2,011", "2,022")),
    ("Y,YYYTH", ("2,024TH", "2,005TH", "2,011TH", "2,022ND")),
    ("Y,YYYth", ("2,024th", "2,005th", "2,011th", "2,022nd")),
    ("YYYY", ("2024", "2005", "2011", "2022")),
    ("FMYYYY", ("2024", "2005", "2011", "2022")),
    ("YYYYTH", ("2024TH", "2005TH", "2011TH", "2022ND")),
    ("YYYYth", ("2024th", "2005th", "2011th", "2022nd")),
    ("YYY", ("024", "005", "011", "022")),
    ("FMYYY", ("24", "5", "11", "22")),
    ("YYYTH", ("024TH", "005TH", "011TH", "022ND")),
    ("YYYth", ("024th", "005th", "011th", "022nd")),
    ("YY", ("24", "05", "11", "22")),
    ("FMYY", ("24", "5", "11", "22")),
    ("YYTH", ("24TH", "05TH", "11TH", "22ND")),
    ("YYth", ("24th", "05th", "11th", "22nd")),
    ("Y", ("4", "5", "1", "2")),
    ("FMY", ("4", "5", "1", "2")),
    ("YTH", ("4TH", "5TH", "1ST", "2ND")),
    ("Yth", ("4th", "5th", "1st", "2nd")),
    ("IYYY", ("2024", "2004", "2011", "2022")),
    ("FMIYYY", ("2024", "2004", "2011", "2022")),
    ("IYYYTH", ("2024TH", "2004TH", "2011TH", "2022ND")),
    ("IYYYth", ("2024th", "2004th", "2011th", "2022nd")),
    ("IYY", ("024", "004", "011", "022")),
    ("FMIYY", ("24", "4", "11", "22")),
    ("IYYTH", ("024TH", "004TH", "011TH", "022ND")),
    ("IYYth", ("024th", "004th", "011th", "022nd")),
    ("IY", ("24", "04", "11", "22")),
    ("FMIY", ("24", "4", "11", "22")),
    ("IYTH", ("24TH", "04TH", "11TH", "22ND")),
    ("IYth", ("24th", "04th", "11th", "22nd")),
    ("I", ("4", "4", "1", "2")),
    ("FMI", ("4", "4", "1", "2")),
    ("ITH", ("4TH", "4TH", "1ST", "2ND")),
    ("Ith", ("4th", "4th", "1st", "2nd")),
    ("BC", ("AD", "AD", "AD", "AD")),
    ("FMBC", ("AD", "AD", "AD", "AD")),
    ("BCTH", ("AD", "AD", "AD", "AD")),
    ("BCth", ("AD", "AD", "AD", "AD")),
    ("bc", ("ad", "ad", "ad", "ad")),
    ("FMbc", ("ad", "ad", "ad", "ad")),
    ("bcTH", ("ad", "ad", "ad", "ad")),
    ("bcth", ("ad", "ad", "ad", "ad")),
    ("AD", ("AD", "AD", "AD", "AD")),
    ("FMAD", ("AD", "AD", "AD", "AD")),
    ("ADTH", ("AD", "AD", "AD", "AD")),
    ("ADth", ("AD", "AD", "AD", "AD")),
    ("ad", ("ad", "ad", "ad", "ad")),
    ("FMad", ("ad", "ad", "ad", "ad")),
    ("adTH", ("ad", "ad", "ad", "ad")),
    ("adth", ("ad", "ad", "ad", "ad")),
    ("B.C.", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("FMB.C.", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("B.C.TH", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("B.C.th", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("b.c.", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("FMb.c.", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("b.c.TH", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("b.c.th", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("A.D.", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("FMA.D.", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("A.D.TH", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("A.D.th", ("A.D.", "A.D.", "A.D.", "A.D.")),
    ("a.d.", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("FMa.d.", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("a.d.TH", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("a.d.th", ("a.d.", "a.d.", "a.d.", "a.d.")),
    ("MONTH", ("JUNE     ", "JANUARY  ", "NOVEMBER ", "SEPTEMBER")),
    ("FMMONTH", ("JUNE", "JANUARY", "NOVEMBER", "SEPTEMBER")),
    ("MONTHTH", ("JUNE     ", "JANUARY  ", "NOVEMBER ", "SEPTEMBER")),
    ("MONTHth", ("JUNE     ", "JANUARY  ", "NOVEMBER ", "SEPTEMBER")),
    ("Month", ("June     ", "January  ", "November ", "September")),
    ("FMMonth", ("June", "January", "November", "September")),
    ("MonthTH", ("June     ", "January  ", "November ", "September")),
    ("Monthth", ("June     ", "January  ", "November ", "September")),
    ("month", ("june     ", "january  ", "november ", "september")),
    ("FMmonth", ("june", "january", "november", "september")),
    ("monthTH", ("june     ", "january  ", "november ", "september")),
    ("monthth", ("june     ", "january  ", "november ", "september")),
    ("MON", ("JUN", "JAN", "NOV", "SEP")),
    ("FMMON", ("JUN", "JAN", "NOV", "SEP")),
    ("MONTH", ("JUNE     ", "JANUARY  ", "NOVEMBER ", "SEPTEMBER")),
    ("MONth", ("JUN", "JAN", "NOV", "SEP")),
    ("Mon", ("Jun", "Jan", "Nov", "Sep")),
    ("FMMon", ("Jun", "Jan", "Nov", "Sep")),
    ("MonTH", ("Jun", "Jan", "Nov", "Sep")),
    ("Month", ("June     ", "January  ", "November ", "September")),
    ("mon", ("jun", "jan", "nov", "sep")),
    ("FMmon", ("jun", "jan", "nov", "sep")),
    ("monTH", ("jun", "jan", "nov", "sep")),
    ("month", ("june     ", "january  ", "november ", "september")),
    ("MM", ("06", "01", "11", "09")),
    ("FMMM", ("6", "1", "11", "9")),
    ("MMTH", ("06TH", "01ST", "11TH", "09TH")),
    ("MMth", ("06th", "01st", "11th", "09th")),
    ("DAY", ("WEDNESDAY", "SUNDAY   ", "FRIDAY   ", "THURSDAY ")),
    ("FMDAY", ("WEDNESDAY", "SUNDAY", "FRIDAY", "THURSDAY")),
    ("DAYTH", ("WEDNESDAY", "SUNDAY   ", "FRIDAY   ", "THURSDAY ")),
    ("DAYth", ("WEDNESDAY", "SUNDAY   ", "FRIDAY   ", "THURSDAY ")),
    ("Day", ("Wednesday", "Sunday   ", "Friday   ", "Thursday ")),
    ("FMDay", ("Wednesday", "Sunday", "Friday", "Thursday")),
    ("DayTH", ("Wednesday", "Sunday   ", "Friday   ", "Thursday ")),
    ("Dayth", ("Wednesday", "Sunday   ", "Friday   ", "Thursday ")),
    ("day", ("wednesday", "sunday   ", "friday   ", "thursday ")),
    ("FMday", ("wednesday", "sunday", "friday", "thursday")),
    ("dayTH", ("wednesday", "sunday   ", "friday   ", "thursday ")),
    ("dayth", ("wednesday", "sunday   ", "friday   ", "thursday ")),
    ("DY", ("WED", "SUN", "FRI", "THU")),
    ("FMDY", ("WED", "SUN", "FRI", "THU")),
    ("DYTH", ("WED", "SUN", "FRI", "THU")),
    ("DYth", ("WED", "SUN", "FRI", "THU")),
    ("Dy", ("Wed", "Sun", "Fri", "Thu")),
    ("FMDy", ("Wed", "Sun", "Fri", "Thu")),
    ("DyTH", ("Wed", "Sun", "Fri", "Thu")),
    ("Dyth", ("Wed", "Sun", "Fri", "Thu")),
    ("dy", ("wed", "sun", "fri", "thu")),
    ("FMdy", ("wed", "sun", "fri", "thu")),
    ("dyTH", ("wed", "sun", "fri", "thu")),
    ("dyth", ("wed", "sun", "fri", "thu")),
    ("DDD", ("157", "002", "315", "265")),
    ("FMDDD", ("157", "2", "315", "265")),
    ("DDDTH", ("157TH", "002ND", "315TH", "265TH")),
    ("DDDth", ("157th", "002nd", "315th", "265th")),
    ("IDDD", ("157", "371", "313", "263")),
    ("FMIDDD", ("157", "371", "313", "263")),
    ("IDDDTH", ("157TH", "371ST", "313TH", "263RD")),
    ("IDDDth", ("157th", "371st", "313th", "263rd")),
    ("DD", ("05", "02", "11", "22")),
    ("FMDD", ("5", "2", "11", "22")),
    ("DDTH", ("05TH", "02ND", "11TH", "22ND")),
    ("DDth", ("05th", "02nd", "11th", "22nd")),
    ("D", ("4", "1", "6", "5")),
    ("FMD", ("4", "1", "6", "5")),
    ("DTH", ("4TH", "1ST", "6TH", "5TH")),
    ("Dth", ("4th", "1st", "6th", "5th")),
    ("ID", ("3", "7", "5", "4")),
    ("FMID", ("3", "7", "5", "4")),
    ("IDTH", ("3RD", "7TH", "5TH", "4TH")),
    ("IDth", ("3rd", "7th", "5th", "4th")),
    ("W", ("1", "1", "2", "4")),
    ("FMW", ("1", "1", "2", "4")),
    ("WTH", ("1ST", "1ST", "2ND", "4TH")),
    ("Wth", ("1st", "1st", "2nd", "4th")),
    ("WW", ("23", "01", "45", "38")),
    ("FMWW", ("23", "1", "45", "38")),
    ("WWTH", ("23RD", "01ST", "45TH", "38TH")),
    ("WWth", ("23rd", "01st", "45th", "38th")),
    ("IW", ("23", "53", "45", "38")),
    ("FMIW", ("23", "53", "45", "38")),
    ("IWTH", ("23RD", "53RD", "45TH", "38TH")),
    ("IWth", ("23rd", "53rd", "45th", "38th")),
    ("CC", ("21", "21", "21", "21")),
    ("FMCC", ("21", "21", "21", "21")),
    ("CCTH", ("21ST", "21ST", "21ST", "21ST")),
    ("CCth", ("21st", "21st", "21st", "21st")),
    ("J", ("2460467", "2453373", "2455877", "2459845")),
    ("FMJ", ("2460467", "2453373", "2455877", "2459845")),
    ("JTH", ("2460467TH", "2453373RD", "2455877TH", "2459845TH")),
    ("Jth", ("2460467th", "2453373rd", "2455877th", "2459845th")),
    ("Q", ("2", "1", "4", "3")),
    ("FMQ", ("2", "1", "4", "3")),
    ("QTH", ("2ND", "1ST", "4TH", "3RD")),
    ("Qth", ("2nd", "1st", "4th", "3rd")),
    ("RM", ("VI  ", "I   ", "XI  ", "IX  ")),
    ("FMRM", ("VI", "I", "XI", "IX")),
    ("RMTH", ("VI  ", "I   ", "XI  ", "IX  ")),
    ("RMth", ("VI  ", "I   ", "XI  ", "IX  ")),
    ("rm", ("vi  ", "i   ", "xi  ", "ix  ")),
    ("FMrm", ("vi", "i", "xi", "ix")),
    ("rmTH", ("vi  ", "i   ", "xi  ", "ix  ")),
    ("rmth", ("vi  ", "i   ", "xi  ", "ix  ")),
    ("TZ", ("", "", "", "")),
    ("FMTZ", ("", "", "", "")),
    ("TZTH", ("", "", "", "")),
    ("TZth", ("", "", "", "")),
    ("tz", ("", "", "", "")),
    ("FMtz", ("", "", "", "")),
    ("tzTH", ("", "", "", "")),
    ("tzth", ("", "", "", "")),
    ("TZH", ("+00", "+00", "+00", "+00")),
    ("FMTZH", ("+00", "+00", "+00", "+00")),
    ("TZHTH", ("+00", "+00", "+00", "+00")),
    ("TZHth", ("+00", "+00", "+00", "+00")),
    ("TZM", ("00", "00", "00", "00")),
    ("FMTZM", ("00", "00", "00", "00")),
    ("TZMTH", ("00", "00", "00", "00")),
    ("TZMth", ("00", "00", "00", "00")),
    ("OF", ("+00", "+00", "+00", "+00")),
    ("FMOF", ("+0", "+0", "+0", "+0")),
    ("OFTH", ("+00", "+00", "+00", "+00")),
    ("OFth", ("+00", "+00", "+00", "+00")),
    ("of", ("+00", "+00", "+00", "+00")),
    ("FMof", ("+0", "+0", "+0", "+0")),
    ("ofTH", ("+00", "+00", "+00", "+00")),
    ("ofth", ("+00", "+00", "+00", "+00")),
    ("FX", ("", "", "", "")),
    ("FMFX", ("", "", "", "")),
    ("FXTH", ("", "", "", "")),
    ("FXth", ("", "", "", "")),
]


#: The keywords no engine writes exactly: a name in lc_time's language.
_REFUSED = ("TMMonth", "TMMONTH", "TMmonth", "TMMon", "TMDay", "TMDy", "TMdy")


def _answers(rows: list[tuple]) -> tuple:
    return tuple(text for _, text in sorted(rows))


@pytest.mark.parametrize(("template", "answers"), _TOKENS)
def test_each_keyword_is_postgresql_s_text_on_clickhouse(runtime, template, answers):
    sql = f"SELECT i, to_char(v, {_quoted(template)}) AS c FROM (VALUES {_VALUES}) AS t(i, v)"
    assert _answers(_on_clickhouse(runtime, sql)) == answers


@pytest.mark.parametrize(("template", "answers"), _TOKENS)
def test_each_keyword_is_postgresql_s_text_on_duckdb(template, answers):
    sql = f"SELECT i, to_char(v, {_quoted(template)}) AS c FROM (VALUES {_VALUES}) AS t(i, v)"
    assert _answers(_on_duckdb(sql)) == answers


@pytest.mark.parametrize(("template", "answers"), _TOKENS)
def test_each_keyword_is_written_in_trino_s_functions(template, answers):
    sql = f"SELECT to_char({_quoted(_STAMPS[0])}::timestamp, {_quoted(template)}) AS c"
    written = transpile(sql, "trino")
    sqlglot.parse_one(written, read="trino")
    assert "DATE_FORMAT" not in written.upper() and "FORMAT_DATETIME" not in written.upper()


@pytest.mark.parametrize("template", _REFUSED)
@pytest.mark.parametrize("dialect", ["clickhouse", "trino"])
def test_a_name_in_lc_time_s_language_is_refused_naming_its_keyword(template, dialect):
    with pytest.raises(FunctionRefused) as refused:
        transpile(f"SELECT to_char('2024-06-05'::timestamp, '{template}') AS v", dialect)
    assert str(refused.value) == (
        f"to_char() of the template element {template!r} has no form on {dialect}"
    )
    assert getattr(refused.value, "sqlstate") == "0A000"


@pytest.mark.parametrize("template", _REFUSED)
def test_a_name_in_lc_time_s_language_is_refused_naming_its_keyword_on_duckdb(template):
    with pytest.raises(NotImplementedError, match=template):
        transpile_to_duckdb(f"SELECT to_char('2024-06-05'::timestamp, '{template}')")


# --- The templates strftime misread (J1309's refusals), and other templates --------------------

_STAMP = "'2024-06-15 12:30:45'::timestamp"
_TEMPLATES = [
    ("%Y", "%4"),  # a percent sign and the year's last digit
    ("%Y-%m-%d", "%4-%m-%7"),  # D: Saturday is the week's seventh day
    ("SSSS", "45045"),
    ("DDTHH24", "15THH24"),
    ("DDDD", "1677"),
    ("FMDD", "15"),
    ('"YYYY"', "YYYY"),
    ("HH24:MI:SS", "12:30:45"),
    ("FM999.00", "999.00"),  # a timestamp's template: none of it is a keyword
    ("YYYY-MM-DD", "2024-06-15"),
    ("\\MM", "\\06"),
    ("FMFMDD", "FM15"),
    ("FMxDD", "x15"),
    ("FMTMMonth", "T06onth"),
    ("Day, DD  HH12:MI:SS", "Saturday , 15  12:30:45"),
    ("DDSP", "15"),
    ("it''s", "4t's"),  # i: the ISO year's last digit
]


@pytest.mark.parametrize(("template", "answer"), _TEMPLATES)
def test_a_template_is_postgresql_s_text_on_clickhouse(runtime, template, answer):
    assert _on_clickhouse(runtime, f"SELECT to_char({_STAMP}, '{template}') AS v") == [(answer,)]


@pytest.mark.parametrize(("template", "answer"), _TEMPLATES)
def test_a_template_is_postgresql_s_text_on_duckdb(template, answer):
    assert _on_duckdb(f"SELECT to_char({_STAMP}, '{template}')") == [(answer,)]


#: Trino's SQL of two of them: the year's last digit after a percent sign that is only text, and
#: the seconds past midnight.
_TRINO = [
    ("%Y", "SELECT ('%' || CAST(((CASE WHEN YEAR(CAST('2024-06-15 12:30:45' AS TIMESTAMP(6))) <= 0 "
     "THEN 1 - YEAR(CAST('2024-06-15 12:30:45' AS TIMESTAMP(6))) ELSE YEAR(CAST('2024-06-15 "
     "12:30:45' AS TIMESTAMP(6))) END) % 10) AS VARCHAR)) AS v"),
    ("SSSS", "SELECT (CAST((HOUR(CAST('2024-06-15 12:30:45' AS TIMESTAMP(6))) * 3600 + "
     "MINUTE(CAST('2024-06-15 12:30:45' AS TIMESTAMP(6))) * 60 + SECOND(CAST('2024-06-15 "
     "12:30:45' AS TIMESTAMP(6)))) AS VARCHAR)) AS v"),
]  # fmt: skip


@pytest.mark.parametrize(("template", "written"), _TRINO)
def test_a_template_strftime_misread_is_written_from_its_tokens_on_trino(template, written):
    assert transpile(f"SELECT to_char({_STAMP}, '{template}') AS v", "trino") == written


@pytest.mark.parametrize("dialect", ["clickhouse", "trino"])
def test_a_template_that_is_no_literal_stays_refused_by_name(dialect):
    sql = f"SELECT to_char({_STAMP}, x) AS v FROM (VALUES ('YYYY')) AS t(x)"
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, dialect)
    assert str(refused.value) == f"to_char() has no form on {dialect}"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT to_char(NULL::timestamp, 'YYYY') AS v",
        f"SELECT to_char({_STAMP}, NULL) AS v",
        "SELECT to_char(NULL::timestamp, 'xyz') AS v",  # strict, though no keyword reads it
        f"SELECT to_char({_STAMP}, '') AS v",  # formatting.c answers NULL of an empty template
    ],
)
def test_to_char_of_a_null_or_an_empty_template_is_null(runtime, sql):
    assert _on_clickhouse(runtime, sql) == [(None,)]
    assert _on_duckdb(sql) == [(None,)]


def test_a_literal_outside_what_clickhouse_holds_raises(runtime):
    with pytest.raises(Exception) as raised:
        _on_clickhouse(runtime, "SELECT to_char('2300-01-01 00:00:00'::timestamp, 'YYYY') AS v")
    error = from_engine(raised.value)
    assert isinstance(error, PostgresError)
    assert (error.sqlstate, str(error)) == ("0A000", instants.out_of_bound("to_char"))


@pytest.mark.parametrize(
    ("value", "answer"),
    [("'1900-01-01 00:00:00'", "1900-01-01 001 Mon 0000"),
     ("'2299-12-31 23:59:59.999999'", "2299-12-31 365 Sun 9999")],
)  # fmt: skip
def test_the_ends_of_what_clickhouse_holds_are_postgresql_s_text(runtime, value, answer):
    sql = f"SELECT to_char({value}::timestamp, 'YYYY-MM-DD DDD Dy FF4') AS v"
    assert _on_clickhouse(runtime, sql) == [(answer,)]


# --- An instant on ClickHouse --------------------------------------------------------------------

_INSTANTS = ", ".join(
    f"('{v}'::timestamptz)"
    for v in ("2024-06-15 12:30:45+00", "1999-12-31 23:59:59+02", "2000-02-29 00:00:00-05")
)


def test_an_instant_is_its_utc_wall_time_on_a_session_on_utc(runtime):
    template = "'YYYY-MM-DD HH24:MI:SS TZ tz OF TZH:TZM'"
    sql = f"SELECT to_char(v, {template}) AS c FROM (VALUES {_INSTANTS}) AS t(v)"
    assert sorted(_on_clickhouse(runtime, sql)) == [
        ("1999-12-31 21:59:59 UTC utc +00 +00:00",),
        ("2000-02-29 05:00:00 UTC utc +00 +00:00",),
        ("2024-06-15 12:30:45 UTC utc +00 +00:00",),
    ]


def test_an_instant_on_a_session_off_utc_is_refused_by_name():
    token = bind_zone(StatementZone("America/New_York", name="America/New_York"))
    try:
        with pytest.raises(FunctionRefused) as refused:
            transpile("SELECT to_char(c, 'YYYY') AS v FROM (VALUES "
                      "('2024-06-15 12:30:45+00'::timestamptz)) AS t(c)", "clickhouse")  # fmt: skip
    finally:
        reset_zone(token)
    assert str(refused.value) == "to_char() has no form on clickhouse"


def test_a_timestamp_s_zone_keywords_are_postgresql_s_constants(runtime):
    sql = f"SELECT to_char({_STAMP}, '[TZ][tz][OF][FMOF][TZH][TZM]') AS v"
    assert _on_clickhouse(runtime, sql) == [("[][][+00][+0][+00][00]",)]
    assert _on_duckdb(sql.replace(" AS v", "")) == [("[][][+00][+0][+00][00]",)]
