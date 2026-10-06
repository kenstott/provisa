# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b9d21-58c4-4f0a-b6e2-9a1c4d7f8e53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The FIXED, SHIPPED profile results schema (REQ-1934).

A profile run of one member table writes one row set into each of the result relations below, all
keyed by ``run_id`` with ``run_time`` as the watermark, so every run is kept as history and nothing
is overwritten. The relations are physical tables in the org's control-plane schema, written
directly by the run (the ingest route, REQ-1771), so a result table registered on the profiler
source reads them in place with no second copy.

Each relation is named ``<profiled table>_<its id>_profile_<kind>``. A table registered on a profiler source
exposes one of them; its column types are the shipped ones (``provisa.profiler.registration``).
"""

from __future__ import annotations

import re

import sqlalchemy as sa

# The watermark column — the run instant. Every relation carries it, so each registered result table
# is an append-only history whose latest run is its highest run_time.
PROFILE_WATERMARK_COLUMN = "run_time"

_RUN_KEY: tuple[tuple[str, str, str], ...] = (
    ("run_id", "varchar", "Identifier of the profile run that produced this row."),
    (
        PROFILE_WATERMARK_COLUMN,
        "timestamp",
        "When the run started. The watermark: runs accumulate.",
    ),
)

# (name, IR data_type, description) per result kind, in display order, after the run key.
_KINDS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "runs": (
        ("region", "varchar", "Region attribute the run read with; empty where none applies."),
        ("profiled_table", "varchar", "The governed table profiled, by its registered name."),
        ("row_count", "bigint", "Rows in the table as the org admin reads it."),
        ("sampled", "boolean", "Whether the run profiled a sample rather than the whole table."),
        (
            "sample_method",
            "varchar",
            "whole | block | key_range | random — how the rows were read.",
        ),
        ("target_fraction", "double", "Fraction of rows the run asked for; 1 for the whole table."),
        ("sample_fraction", "double", "Fraction of rows profiled: profiled_rows / row_count."),
        (
            "sample_attempts",
            "varchar",
            "Each sample read, as JSON [{percent, rows}]; a block sample that came back under half "
            "its target is read again at four times the percentage.",
        ),
        ("profiled_rows", "bigint", "Rows the profile statement read."),
        (
            "previous_run_id",
            "varchar",
            "The previous successful run this run is compared with; empty for the first.",
        ),
        ("duplicate_rows", "bigint", "Rows repeating an earlier row in every profiled column."),
        ("duplicate_share", "double", "duplicate_rows / profiled_rows."),
        (
            "key_duplicates",
            "bigint",
            "Key values held by more than one row, over every declared primary or unique key; "
            "empty where the table declares none.",
        ),
        ("duration_ms", "bigint", "How long the run took."),
        ("status", "varchar", "succeeded | failed."),
        ("error", "varchar", "Why the run failed; empty when it succeeded."),
    ),
    "columns": (
        ("column_name", "varchar", "The profiled column."),
        ("physical_column", "varchar", "The profiled column's registered name."),
        ("data_type", "varchar", "The column's registered data type."),
        ("family", "varchar", "numeric | temporal | text | boolean | other."),
        ("row_count", "bigint", "Rows profiled."),
        ("null_count", "bigint", "Rows where the column is null."),
        ("null_share", "double", "null_count / row_count."),
        ("distinct_count", "bigint", "Distinct non-null values."),
        ("distinct_ratio", "double", "distinct_count / row_count."),
        ("min_value", "varchar", "Smallest value, rendered as text."),
        ("max_value", "varchar", "Largest value, rendered as text."),
        ("mean", "double", "Mean (numeric; epoch seconds for temporal)."),
        ("stddev", "double", "Population standard deviation."),
        ("variance", "double", "Population variance."),
        ("skewness", "double", "Population skewness."),
        ("kurtosis", "double", "Population excess kurtosis."),
        ("log_mean", "double", "Mean of ln(x), when every value is positive."),
        ("log_variance", "double", "Variance of ln(x), when every value is positive."),
        ("integer_only", "boolean", "Whether every value is a whole number."),
        ("zero_share", "double", "Share of non-null values equal to zero."),
        ("length_min", "bigint", "Shortest text length."),
        ("length_max", "bigint", "Longest text length."),
    ),
    "quantiles": (
        ("column_name", "varchar", "The profiled column."),
        ("measure", "varchar", "value | length — what the sketch is of."),
        ("q", "double", "Quantile, 0 to 1 in steps of 0.01."),
        ("value", "double", "Value at q (epoch seconds for temporal)."),
    ),
    "histogram": (
        ("column_name", "varchar", "The profiled column."),
        ("bucket", "integer", "Bucket number, from 1."),
        ("lo", "double", "Bucket lower bound."),
        ("hi", "double", "Bucket upper bound."),
        ("row_count", "double", "Rows in the bucket, estimated from the quantile sketch."),
    ),
    "top_values": (
        ("column_name", "varchar", "The profiled column."),
        (
            "kind",
            "varchar",
            "top | frequency — most frequent values, or every value of a low-cardinality column.",
        ),
        ("rank", "integer", "Rank by count, from 1."),
        ("value", "varchar", "The value, rendered as text; empty for null."),
        ("row_count", "bigint", "Rows holding the value."),
    ),
    "fits": (
        ("column_name", "varchar", "The profiled column."),
        ("family", "varchar", "Distribution family fitted."),
        ("param", "varchar", "Parameter name."),
        ("value", "double", "Parameter value."),
    ),
    "fit_quality": (
        ("column_name", "varchar", "The profiled column."),
        ("family", "varchar", "Distribution family fitted."),
        ("ks_stat", "double", "Kolmogorov-Smirnov distance against the quantile sketch."),
        ("rank", "integer", "Rank by ks_stat, best first."),
    ),
    "fanout": (
        ("relationship", "varchar", "The relationship from this table to its children."),
        ("q", "double", "Quantile, 0 to 1 in steps of 0.01."),
        ("value", "double", "Children per parent at q."),
    ),
    "fanout_runs": (
        ("relationship", "varchar", "The relationship from this table to its children."),
        ("child_table", "varchar", "The child table, as domain.table."),
        ("parents", "bigint", "Parents profiled."),
        ("mean", "double", "Mean children per parent."),
        ("max", "bigint", "Most children of one parent."),
        ("childless_share", "double", "Share of parents with no children."),
    ),
    "shapes": (
        ("column_name", "varchar", "The profiled column."),
        ("rank", "integer", "Rank by count, from 1."),
        ("shape", "varchar", "Value with A upper, a lower, 9 digit; other characters kept."),
        ("row_count", "bigint", "Rows with the shape."),
    ),
    "plausible_type": (
        ("column_name", "varchar", "The profiled column."),
        ("plausible_type", "varchar", "What the column plausibly holds."),
        ("confidence", "double", "Confidence, 0 to 1."),
        ("evidence", "varchar", "Why."),
    ),
    "duplicates": (
        (
            "subject",
            "varchar",
            "row | key -- whole rows (every profiled column), or a declared key.",
        ),
        ("key_name", "varchar", "The primary or unique key; empty for whole rows."),
        (
            "involved_columns",
            "varchar",
            "The key's columns, as a JSON array; empty for whole rows.",
        ),
        ("repeated_values", "bigint", "Distinct rows, or key values, held by more than one row."),
        ("extra_rows", "bigint", "Rows that repeat an earlier row, or key value."),
        ("extra_share", "double", "extra_rows / rows profiled."),
    ),
    "repeats": (
        ("rank", "integer", "Rank by repeat count, from 1."),
        ("row_count", "bigint", "How many rows hold one of the most repeated rows."),
    ),
    "drift": (
        ("previous_run_id", "varchar", "The table's previous successful run; empty for its first."),
        (
            "scope",
            "varchar",
            "run | table | key | column | category | relationship -- what the measure is of.",
        ),
        ("column_name", "varchar", "The profiled column the measure is of; empty for others."),
        (
            "involved_columns",
            "varchar",
            "Every profiled column the measure describes, as a JSON array.",
        ),
        (
            "value_bearing",
            "boolean",
            "Whether the measure holds values of the columns it describes.",
        ),
        ("measure", "varchar", "The measure."),
        ("subject", "varchar", "The category, key or relationship the measure is of; or empty."),
        ("current", "double", "The measure in this run."),
        ("previous", "double", "The measure in the previous run."),
        ("change", "double", "current - previous."),
        (
            "ks_previous",
            "double",
            "Kolmogorov-Smirnov statistic of the distribution against the previous run's.",
        ),
        (
            "psi_previous",
            "double",
            "Population stability index of the distribution against the previous run's.",
        ),
        ("detail", "varchar", "A change in kind: a column added or removed, a type changed."),
    ),
}

RESULT_KINDS: tuple[str, ...] = tuple(_KINDS)

_KIND_ALTERNATION = "|".join(sorted(_KINDS, key=len, reverse=True))
# ``<profiled table>_<its id>_profile_<kind>``: the profiled table's id makes the name unique however
# many profiled tables share a name.
_RESULT_NAME = re.compile(rf"^(?P<table>.+)_(?P<id>\d+)_profile_(?P<kind>{_KIND_ALTERNATION})$")

_SA_TYPES = {
    "varchar": sa.Text,
    "timestamp": lambda: sa.DateTime(timezone=True),
    "bigint": sa.BigInteger,
    "integer": sa.Integer,
    "double": sa.Float,
    "boolean": sa.Boolean,
}


def kind_fields(kind: str) -> tuple[tuple[str, str, str], ...]:
    """Every field of ``kind``'s relation, run key first."""
    if kind not in _KINDS:
        raise ValueError(f"unknown profile result kind {kind!r}; expected one of {RESULT_KINDS}")
    return _RUN_KEY + _KINDS[kind]


def field_names(kind: str) -> tuple[str, ...]:
    return tuple(name for name, _, _ in kind_fields(kind))


def result_table_name(profiled_table: str, table_id: int, kind: str) -> str:
    kind_fields(kind)
    return f"{profiled_table}_{table_id}_profile_{kind}"


def parse_result_table_name(name: str) -> tuple[str, int, str]:
    """``(profiled table, its id, kind)`` of a result relation name, or ValueError naming it."""
    m = _RESULT_NAME.match(name)
    if m is None:
        raise ValueError(
            f"table {name!r} is not a profile result table: its name must be "
            f"<profiled table>_<its id>_profile_<kind>, kind one of {list(RESULT_KINDS)}"
        )
    return m["table"], int(m["id"]), m["kind"]


def result_sa_table(profiled_table: str, table_id: int, kind: str) -> sa.Table:
    """The relation ``kind`` of ``profiled_table`` as a SQLAlchemy table, schema-less: the writer's
    connection is scoped to the org's control-plane schema, as an ingest table's is."""
    return sa.Table(
        result_table_name(profiled_table, table_id, kind),
        sa.MetaData(),
        *[sa.Column(name, _SA_TYPES[data_type]()) for name, data_type, _ in kind_fields(kind)],
    )
