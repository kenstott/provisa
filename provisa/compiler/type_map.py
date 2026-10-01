# Copyright (c) 2026 Kenneth Stott
# Canary: 497dc85c-db96-44c2-ad8f-48c5ae2ac44b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""the engine type → GraphQL scalar mapping (REQ-010).

Nullability preserved from INFORMATION_SCHEMA. Custom scalars for DateTime, Date, Interval, JSON,
BigInt.
"""

# Requirements: REQ-010, REQ-306

from typing import cast

from graphql import (
    GraphQLBoolean as _GraphQLBoolean,
    GraphQLFloat as _GraphQLFloat,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt as _GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLScalarType,
    GraphQLString as _GraphQLString,
)

# graphql-core 3.2.x: __new__ returns GraphQLNamedType instead of Self;
# re-bind scalars with explicit GraphQLScalarType annotation so Pyright narrows correctly.
GraphQLString: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLString)
GraphQLInt: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLInt)
GraphQLFloat: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLFloat)
GraphQLBoolean: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLBoolean)

# --- Custom scalars ---


def _iso_temporal(kind: str, value: object) -> str:  # object-ok: an engine's date/time value
    """Date and time values are ALWAYS ISO 8601 strings in a GraphQL response — never a number. A
    driver's date/datetime renders with ``isoformat()`` (``2026-01-02T03:04:05``, not ``str()``'s
    space-separated form); engine text is already ISO. A number (an epoch offset in unknown units)
    or anything else is a wrong type for this column and raises (a GraphQL field error)."""
    from datetime import date

    if isinstance(value, date):  # datetime is a date subclass
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise TypeError(f"{kind} cannot represent {type(value).__name__} value {value!r}")


DateTime: GraphQLScalarType = cast(
    GraphQLScalarType,
    GraphQLScalarType(
        "DateTime",
        description="ISO 8601 datetime string",
        serialize=lambda v: _iso_temporal("DateTime", v),
        parse_value=str,
    ),
)

Date: GraphQLScalarType = cast(
    GraphQLScalarType,
    GraphQLScalarType(
        "Date",
        description="ISO 8601 date string",
        serialize=lambda v: _iso_temporal("Date", v),
        parse_value=str,
    ),
)

JSONScalar: GraphQLScalarType = cast(
    GraphQLScalarType,
    GraphQLScalarType(
        "JSON",
        description="Arbitrary JSON value",
        serialize=lambda v: v,
        parse_value=lambda v: v,
    ),
)


def _serialize_interval(value: object) -> str:  # object-ok: an engine's interval value, typed below
    """A driver hands an interval back as a ``timedelta``; an engine that renders it as text (a JSON
    result, a text-format column) has already produced its canonical ISO 8601 form. Anything else is
    a wrong type for this column and raises (graphql-core reports it as a field error)."""
    from datetime import timedelta

    from provisa.core.ir_types import iso8601_duration

    if isinstance(value, timedelta):
        return iso8601_duration(value)
    if isinstance(value, str):
        return value
    raise TypeError(f"Interval cannot represent {type(value).__name__} value {value!r}")


Interval: GraphQLScalarType = cast(
    GraphQLScalarType,
    GraphQLScalarType(
        "Interval",
        description="Duration as an ISO 8601 duration string (e.g. P3DT4.000005S)",
        serialize=_serialize_interval,
        parse_value=str,
    ),
)

BigInt: GraphQLScalarType = cast(
    GraphQLScalarType,
    GraphQLScalarType(
        "BigInt",
        description="64-bit integer as string",
        serialize=str,
        parse_value=int,
    ),
)

# --- Type mapping ---

_TYPE_MAP: dict[str, GraphQLScalarType] = cast(
    dict[str, GraphQLScalarType],
    {
        # String types
        "varchar": GraphQLString,
        "text": GraphQLString,  # views cast array/jsonb/json columns to text (see app.py)
        "char": GraphQLString,
        "bpchar": GraphQLString,  # postgres blank-padded char
        "name": GraphQLString,  # postgres internal identifier type
        "varbinary": GraphQLString,
        "bytea": GraphQLString,  # postgres binary (REQ-686 encrypted-at-rest columns)
        "blob": GraphQLString,  # sqlite / mysql binary storage class
        "bytes": GraphQLString,  # BigQuery's own binary column type name
        # REQ-1753: HANA's own SQL_TYPE_NAMEs, from SYS.TABLE_COLUMNS' DATA_TYPE_NAME (not an
        # ANSI/information_schema name) — hit live registering a saphana source's own NVARCHAR
        # column, the same class of gap array/list handling already covers for other engines.
        "nvarchar": GraphQLString,  # HANA unicode varchar — every HANA text column defaults to it
        "nclob": GraphQLString,  # HANA unicode CLOB, HANA's unbounded-text equivalent of "text"
        # Oracle's own ALL_TAB_COLUMNS.DATA_TYPE names (introspect.py's oracle native_columns
        # branch) — Oracle has no information_schema, so these never went through the ANSI
        # varchar/text names above. Hit live registering oracle's WIDGETS.NAME (VARCHAR2): the
        # Register Table form's column preview raised, blocking the row from ever appearing.
        # Not adding Oracle's CLOB/RAW/LONG here too: LONG already maps to BigInt above for
        # Databricks' BIGINT alias, and this is a single flat name->type map with no per-engine
        # namespace — a collision would silently break Databricks. Add only names confirmed live.
        "varchar2": GraphQLString,
        "nvarchar2": GraphQLString,
        "uuid": GraphQLString,
        "string": GraphQLString,  # OpenAPI JSON-Schema "string" (provisa.openapi.register._OPENAPI_TYPE_MAP)
        # Integer types
        "tinyint": GraphQLInt,
        "smallint": GraphQLInt,
        "int2": GraphQLInt,  # postgres smallint alias
        "integer": GraphQLInt,
        "int": GraphQLInt,
        "int4": GraphQLInt,  # postgres integer alias
        # Large integer
        "bigint": BigInt,
        "int8": BigInt,  # postgres bigint alias
        "int64": BigInt,  # BigQuery's own INFORMATION_SCHEMA.COLUMNS data_type for integers
        "long": BigInt,  # Databricks SQL's own name for BIGINT (DESCRIBE TABLE / INFORMATION_SCHEMA)
        # Floating point
        "real": GraphQLFloat,
        "float4": GraphQLFloat,  # postgres real alias
        "float": GraphQLFloat,  # canonical IR floating type (REQ-846)
        "double": GraphQLFloat,
        "float8": GraphQLFloat,  # postgres double precision alias
        "float64": GraphQLFloat,  # BigQuery's own floating-point type name
        "decimal": GraphQLFloat,
        "numeric": GraphQLFloat,
        "bignumeric": GraphQLFloat,  # BigQuery's extended-precision numeric type
        "number": GraphQLFloat,  # OpenAPI JSON-Schema "number" (provisa.openapi.register._OPENAPI_TYPE_MAP)
        # Boolean
        "boolean": GraphQLBoolean,
        "bool": GraphQLBoolean,  # postgres boolean alias
        # Date/time
        "date": Date,
        "time": GraphQLString,
        "time with time zone": GraphQLString,
        "timetz": GraphQLString,  # postgres time with time zone alias
        "interval": Interval,
        "timestamp": DateTime,
        "timestamp with time zone": DateTime,
        "timestamptz": DateTime,  # postgres timestamp with time zone alias
        "datetime": DateTime,  # sqlite / mysql timestamp type name
        # JSON
        "json": JSONScalar,
        "jsonb": JSONScalar,
    },
)


def column_type_to_graphql(column_type: str) -> GraphQLScalarType | GraphQLList:  # REQ-010, REQ-306
    """Map the engine column type to a GraphQL scalar.

    Handles parameterized types like varchar(255), decimal(10,2), array(varchar).
    Raises ValueError for unmapped types.
    """
    normalized = column_type.lower().strip()

    # Check exact match first
    if normalized in _TYPE_MAP:
        return _TYPE_MAP[normalized]

    # Handle parameterized types: varchar(255) → varchar, decimal(10,2) → decimal
    base = normalized.split("(")[0].strip()
    if base in _TYPE_MAP:
        return _TYPE_MAP[base]

    # Handle array types: array(varchar) → List of mapped type
    if normalized.startswith("array(") and normalized.endswith(")"):
        inner = normalized[6:-1]
        inner_type = column_type_to_graphql(inner)
        return GraphQLList(GraphQLNonNull(inner_type))

    raise ValueError(f"Unmapped the engine type: {column_type!r}")


# --- Filter input types (shared across all schemas) ---


def _filter_fields(scalar: GraphQLScalarType) -> dict[str, GraphQLInputField]:
    """Base comparison fields for a scalar type."""
    return {
        "eq": GraphQLInputField(scalar),
        "neq": GraphQLInputField(scalar),
        "in": GraphQLInputField(GraphQLList(GraphQLNonNull(scalar))),
        "is_null": GraphQLInputField(GraphQLBoolean),
    }


def _ordered_filter_fields(scalar: GraphQLScalarType) -> dict[str, GraphQLInputField]:
    """Comparison fields for ordered types (adds gt/gte/lt/lte)."""
    fields = _filter_fields(scalar)
    fields.update(
        {
            "gt": GraphQLInputField(scalar),
            "gte": GraphQLInputField(scalar),
            "lt": GraphQLInputField(scalar),
            "lte": GraphQLInputField(scalar),
        }
    )
    return fields


StringFilter = GraphQLInputObjectType(
    "StringFilter",
    lambda: {
        **_ordered_filter_fields(GraphQLString),
        "like": GraphQLInputField(GraphQLString),
    },
)

IntFilter = GraphQLInputObjectType("IntFilter", lambda: _ordered_filter_fields(GraphQLInt))

BigIntFilter = GraphQLInputObjectType("BigIntFilter", lambda: _ordered_filter_fields(BigInt))

FloatFilter = GraphQLInputObjectType("FloatFilter", lambda: _ordered_filter_fields(GraphQLFloat))

BooleanFilter = GraphQLInputObjectType(
    "BooleanFilter",
    lambda: {
        "eq": GraphQLInputField(GraphQLBoolean),
        "is_null": GraphQLInputField(GraphQLBoolean),
    },
)

DateFilter = GraphQLInputObjectType("DateFilter", lambda: _ordered_filter_fields(Date))

DateTimeFilter = GraphQLInputObjectType("DateTimeFilter", lambda: _ordered_filter_fields(DateTime))

IntervalFilter = GraphQLInputObjectType("IntervalFilter", lambda: _ordered_filter_fields(Interval))

JSONFilter = GraphQLInputObjectType(
    "JSONFilter",
    lambda: {
        "is_null": GraphQLInputField(GraphQLBoolean),
    },
)

# Map GraphQL scalar → filter input type
FILTER_TYPE_MAP: dict[GraphQLScalarType, GraphQLInputObjectType] = cast(
    dict[GraphQLScalarType, GraphQLInputObjectType],
    {
        GraphQLString: StringFilter,
        GraphQLInt: IntFilter,
        BigInt: BigIntFilter,
        GraphQLFloat: FloatFilter,
        GraphQLBoolean: BooleanFilter,
        Date: DateFilter,
        DateTime: DateTimeFilter,
        Interval: IntervalFilter,
        JSONScalar: JSONFilter,
    },
)


# Postgres -> physical engine type map for the OTel ops schema DDL (REQ-016). Neutral home so both
# the engine ops writer and generic boot import it without reaching into an engine-impl module.
OPS_PG_TO_PHYSICAL: dict[str, str] = {
    "text": "VARCHAR",
    "bigint": "BIGINT",
    "integer": "INTEGER",
    "float8": "DOUBLE",
    "date": "DATE",
    "boolean": "BOOLEAN",
    # REQ-1435: microseconds is the resolution the compactor writes (jobs._PA_TO_PHYSICAL) and the
    # one every store in the lane keeps, so the ops DDL and the registered column agree on it.
    "timestamp": "TIMESTAMP(6)",
}
