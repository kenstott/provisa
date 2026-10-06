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
        (
            "freshness_seconds",
            "double",
            "Run time minus the latest value of the table's declared watermark column, in seconds; "
            "empty where the table declares no temporal watermark.",
        ),
        (
            "window_runs",
            "integer",
            "Previous successful runs at this run's point in the season, up to the profiler's "
            "drift window.",
        ),
        (
            "dependence_method",
            "varchar",
            "How the dependence statements read the rows: the run's method and fraction, a block "
            "or row-filter sample being an independent draw; empty where none ran.",
        ),
        ("dependence_fraction", "double", "Fraction of rows the dependence statements asked for."),
        ("dependence_rows", "bigint", "Rows the pairs statement read."),
        ("network_rows", "bigint", "Rows the triples statement read."),
        (
            "dependence_attempts",
            "varchar",
            "Each read of the dependence statements, as JSON [{statement, percent, rows}]; a block "
            "sample under half its target is read again at four times the percentage.",
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
    "correlations": (
        ("column_name", "varchar", "The column; for a correlation ratio, the category."),
        ("other_column", "varchar", "The other column; for a correlation ratio, the number."),
        (
            "involved_columns",
            "varchar",
            "Both columns, as a JSON array; a parent table's column as <relationship>.<column>.",
        ),
        (
            "measure",
            "varchar",
            "spearman (rank correlation, a category entering as the middle of its share's slice "
            "of 0 to 1) | correlation_ratio (the share of the number's variance the category "
            "explains).",
        ),
        ("value", "double", "The measure."),
        ("rows", "bigint", "Rows holding both columns."),
    ),
    "dependencies": (
        ("column_name", "varchar", "The column that depends."),
        ("rank", "integer", "Rank by mutual information, from 1."),
        ("parent_1", "varchar", "A column it depends on; <relationship>.<column> for a parent's."),
        ("parent_2", "varchar", "The second column of a two-column set; empty for one."),
        ("involved_columns", "varchar", "The column and its parents, as a JSON array."),
        ("mutual_information", "double", "Mutual information of the column and the set, in nats."),
        ("uncertainty", "double", "mutual_information / the column's entropy: 0 to 1."),
        ("score", "double", "mutual_information less the BIC penalty of the set's states."),
        ("in_network", "boolean", "Whether this set is the column's parents in the network."),
    ),
    "joint_counts": (
        ("column_name", "varchar", "The column that depends."),
        ("parent_1", "varchar", "Its first parent in the network."),
        ("parent_2", "varchar", "Its second parent; empty for one."),
        ("involved_columns", "varchar", "The column and its parents, as a JSON array."),
        ("target_value", "varchar", "The column's category; empty for a number or null."),
        ("target_bucket", "integer", "The number's decile of rank, 0 to 9; empty for a category."),
        ("parent_1_value", "varchar", "The first parent's category."),
        ("parent_1_bucket", "integer", "The first parent's decile of rank."),
        ("parent_2_value", "varchar", "The second parent's category."),
        ("parent_2_bucket", "integer", "The second parent's decile of rank."),
        ("row_count", "bigint", "Rows holding the combination."),
    ),
    "constraints": (
        (
            "constraint",
            "varchar",
            "not_null | unique | value_set | range | ordering -- what the profile proposes.",
        ),
        ("column_name", "varchar", "The column the constraint is on."),
        (
            "other_column",
            "varchar",
            "For an ordering, the column never below or before column_name; else empty.",
        ),
        ("involved_columns", "varchar", "The columns it names, as a JSON array."),
        (
            "value_bearing",
            "boolean",
            "Whether its definition holds values of the column (a value set, a range).",
        ),
        ("definition", "varchar", "Its parameters, as JSON: values, min and max, family."),
        ("evidence", "varchar", "What the run found that supports it."),
        ("share", "double", "Share of the rows it speaks of that it held for."),
        ("sampled", "boolean", "Whether the run, so the proposal, rests on a sample."),
        (
            "status",
            "varchar",
            "proposed | accepted | dismissed -- the operator's decision at the run's time.",
        ),
    ),
    "constraint_checks": (
        ("constraint_id", "varchar", "The accepted constraint checked."),
        ("constraint", "varchar", "not_null | unique | value_set | range | ordering."),
        ("column_name", "varchar", "The column the constraint is on."),
        ("other_column", "varchar", "For an ordering, the other column; else empty."),
        ("involved_columns", "varchar", "The columns it names, as a JSON array."),
        ("value_bearing", "boolean", "Whether its definition holds values of the column."),
        ("definition", "varchar", "Its parameters as accepted, as JSON."),
        ("rows_checked", "bigint", "Rows the run checked it over: the rows profiled."),
        ("violations", "bigint", "Rows breaking it; for unique, rows repeating a value."),
        ("pass_share", "double", "1 - violations / rows_checked."),
        ("detail", "varchar", "Why it was not checked; empty when it was."),
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
            "run | table | key | column | category | relationship | correlation | dependency -- "
            "what the measure is of.",
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
        (
            "window_runs",
            "integer",
            "Previous successful runs at this run's point in the season, up to the window; every "
            "window measure below is empty until the window is full.",
        ),
        ("baseline", "double", "The window's median of the measure."),
        ("spread", "double", "The window's median absolute deviation (MAD) from its baseline."),
        ("distance", "double", "(current - baseline) / spread: how far the run lies, in MADs."),
        ("slope", "double", "Least-squares slope of the measure over the window and run, per day."),
        (
            "slope_spread",
            "double",
            "The slope's change across the window's time span, in MADs.",
        ),
        (
            "ks",
            "double",
            "Kolmogorov-Smirnov statistic of the distribution against the window's pooled one.",
        ),
        (
            "psi",
            "double",
            "Population stability index of the distribution against the window's pooled one.",
        ),
        (
            "drifting",
            "boolean",
            "Whether the distance, slope, KS or PSI passes the profiler's threshold.",
        ),
        ("drift_reason", "varchar", "Which thresholds were passed: distance, slope, ks, psi."),
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
