# Copyright (c) 2026 Kenneth Stott
# Canary: 68d280b7-411c-4c19-a9f4-6a6aeba51f5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of XML (REQ-2167, REQ-2171): ``ROWS``, which provisa.transpiler.rows
collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.core.pg_error import raising
from provisa.transpiler.rows.base import _EVERY, _TRINO, Macro, Pg, _macro, _pg, _udf, strict

#: An XML value's escaped text, as an element's content or an attribute's value reads it.
_XML_TEXT = (
    "provisa_xml_value(CAST(value AS VARCHAR), typeof(value), CASE WHEN typeof(value) LIKE '%[]' "
    "THEN CAST(to_json(value) AS VARCHAR) END, {content})"
)
_XML_SPACES = "CAST(to_json(spaces) AS VARCHAR)"

#: xmlEncodeSpecialChars, which any dialect's REPLACE can write.
_XML_ESCAPED = (
    "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(value, '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), "
    "'\"', '&quot;'), CHR(13), '&#13;')"
)
_COMMENT_REFUSED = (
    "strpos(value, '--') > 0 OR substr(value, length(value)) = '-' THEN fail('invalid XML comment')"
)

#: XML: DuckDB's forms, over the UDFs of provisa.federation.duckdb_pg_xml, and what another dialect
#: can do itself. A dialect with no form is refused by provisa.transpiler.pg_xml.for_dialect before
#: any row is read.
_XML: tuple[Macro, ...] = (
    # A value that needs no reading: what query_to_xml wrote (provisa.compiler.xml_mapping).
    _pg("provisa_xml", "value", "CAST(value AS TEXT)"),
    _macro("provisa_xml_text", "value", _XML_TEXT.format(content="true")),
    _macro("provisa_xml_attribute", "value", _XML_TEXT.format(content="false")),
    _udf("xml_is_well_formed", "value", "provisa_xml_well_formed(value, false)"),
    _udf("xml_is_well_formed_content", "value", "provisa_xml_well_formed(value, false)"),
    _macro(
        "xml_is_well_formed_document",
        "value",
        "provisa_xml_well_formed(value, true)",
        own={
            frozenset(
                {"snowflake"}
            ): "CASE WHEN value IS NOT NULL THEN CHECK_XML(value) IS NULL END"
        },
    ),
    _udf("xml_send", "value", "provisa_xml_send(value)"),
    _macro(
        "xmlcomment",
        "value",
        "provisa_xmlcomment(value)",
        {_TRINO: f"CASE WHEN {_COMMENT_REFUSED} ELSE '<!--' || value || '-->' END"},
    ),
    _macro("xmltext", "value", "provisa_xmltext(value)", {_EVERY: Pg(_XML_ESCAPED)}),
    _udf("xmlconcat2", "a, b", "provisa_xmlconcat2(a, b)"),
    _udf("xmlexists", "path, value", "provisa_xpath_exists(path, value, '[]')"),
    _udf("xpath", "path, value", "provisa_xpath(path, value, '[]')"),
    _macro("xpath", "path, value, spaces", f"provisa_xpath(path, value, {_XML_SPACES})"),
    _udf("xpath_exists", "path, value", "provisa_xpath_exists(path, value, '[]')"),
    _macro(
        "xpath_exists", "path, value, spaces", f"provisa_xpath_exists(path, value, {_XML_SPACES})"
    ),
    _macro(
        "xmlvalidate",
        "value, dtd",
        strict("value, dtd", "BOOLEAN", raising("0A000", "'xmlvalidate is not implemented'")),
    ),
)

ROWS: tuple[Macro, ...] = (*_XML,)
