# Copyright (c) 2026 Kenneth Stott
# Canary: 1d7f3a92-c4e8-4b06-97a1-5e2c8d0f6b37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What OpenLineage events report, joined to the lineage graph (REQ-2229).

Airflow, Spark, dbt and Flink each emit OpenLineage run events: a job, the datasets it read and
wrote, and on an output the columnLineage facet saying which input column each column came from.
Each job joins the graph as a box between its datasets. A dataset that names a registered table is
that table's own node; any other is a box outside the model. Datasets are named here at read time,
so a table registered after the event still becomes its node.

A dataset names a table only through the table's source: same kind of source, same host and port,
then the same schema and table (and database, when the name carries one). A dataset with the same
table name on another server is a different dataset.
"""

# Requirements: REQ-2229

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

from provisa.lineage.graph import Edge, LineageGraph, Node

#: An edge's origin when an OpenLineage event reported it (provisa.lineage.graph.DEFINITION else).
OPENLINEAGE = "openlineage"
JOB = "job"
EXTERNAL = "external"
#: The node kinds only a report makes.
REPORTED_KINDS = frozenset({JOB, EXTERNAL})
#: A job's box and a box outside the model are relations under these prefixes.
JOB_PREFIX = "openlineage-job:"
OUTSIDE_PREFIX = "outside:"
#: The row that stands for a whole dataset, or a job's work on one, with no column lineage.
TABLE_LEVEL = "*"

_INPUT = "input"
_OUTPUT = "output"

#: The source types an OpenLineage namespace scheme can name, where they differ from the scheme.
_SCHEME_SOURCE_TYPES: dict[str, frozenset[str]] = {
    "postgres": frozenset({"postgresql"}),
    "mysql": frozenset({"mysql", "mariadb"}),
    "mssql": frozenset({"sqlserver"}),
    "awsathena": frozenset({"athena"}),
}


class InvalidRunEvent(ValueError):
    """The body is not an OpenLineage run event."""


@dataclass(frozen=True)
class ColumnInput:
    namespace: str
    name: str
    field: str
    transform: str  # the facet's "type:subtype"; never its free-text description


@dataclass(frozen=True)
class ReportedDataset:
    namespace: str
    name: str
    column_lineage: dict[str, tuple[ColumnInput, ...]]  # output column -> what it came from


@dataclass(frozen=True)
class RunEvent:
    job_namespace: str
    job_name: str
    run_id: str
    event_time: datetime
    inputs: tuple[ReportedDataset, ...]
    outputs: tuple[ReportedDataset, ...]


@dataclass(frozen=True)
class ReportedJob:
    namespace: str
    name: str
    inputs: tuple[ReportedDataset, ...]
    outputs: tuple[ReportedDataset, ...]


@dataclass(frozen=True)
class RegisteredTable:
    relation: str
    source_type: str
    host: str
    port: int
    database: str
    schema: str
    table: str
    columns: dict[str, str]  # physical column, lower-cased -> the name its node goes by


def _text(parent: dict, key: str, where: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value:
        raise InvalidRunEvent(f"{where}.{key} must be a non-empty string")
    return value


def _transform(field: dict) -> str:
    """The first transformation's ``type:subtype``. The facet's description is free text that can
    name other columns, so it is not kept."""
    transformations = field.get("transformations") or [{}]
    if not isinstance(transformations, list) or not isinstance(transformations[0], dict):
        raise InvalidRunEvent("inputFields[].transformations must be a list of objects")
    first = transformations[0]
    return ":".join(v for v in (first.get("type"), first.get("subtype")) if isinstance(v, str))


def _column_lineage(facets: dict) -> dict[str, tuple[ColumnInput, ...]]:
    facet = facets.get("columnLineage")
    if facet is None:
        return {}
    fields = facet.get("fields") if isinstance(facet, dict) else None
    if not isinstance(fields, dict):
        raise InvalidRunEvent("columnLineage.fields must be an object")
    out: dict[str, tuple[ColumnInput, ...]] = {}
    for column, spec in fields.items():
        inputs = spec.get("inputFields") if isinstance(spec, dict) else None
        if not isinstance(inputs, list) or not all(isinstance(i, dict) for i in inputs):
            raise InvalidRunEvent(f"columnLineage.fields.{column}.inputFields must be a list")
        out[column] = tuple(
            ColumnInput(
                _text(i, "namespace", "inputField"),
                _text(i, "name", "inputField"),
                _text(i, "field", "inputField"),
                _transform(i),
            )
            for i in inputs
        )
    return out


def _datasets(body: dict, key: str) -> tuple[ReportedDataset, ...]:
    listed = body.get(key)
    if listed is None:
        return ()
    if not isinstance(listed, list) or not all(isinstance(d, dict) for d in listed):
        raise InvalidRunEvent(f"{key} must be a list of datasets")
    out = []
    for dataset in listed:
        facets = dataset.get("facets") or {}
        if not isinstance(facets, dict):
            raise InvalidRunEvent(f"{key}[].facets must be an object")
        out.append(
            ReportedDataset(
                _text(dataset, "namespace", key),
                _text(dataset, "name", key),
                _column_lineage(facets),
            )
        )
    return tuple(out)


def parse_run_event(body: Any) -> RunEvent:
    """The run event in ``body``, or :class:`InvalidRunEvent`. A dataset or job event carries no
    run and is refused."""
    if not isinstance(body, dict):
        raise InvalidRunEvent("a run event is a JSON object")
    run, job = body.get("run"), body.get("job")
    if not isinstance(run, dict) or not isinstance(job, dict):
        raise InvalidRunEvent("a run event has a run and a job")
    stamp = _text(body, "eventTime", "event")
    try:
        event_time = datetime.fromisoformat(stamp)
    except ValueError as exc:
        raise InvalidRunEvent(f"event.eventTime is not a timestamp: {stamp!r}") from exc
    if event_time.tzinfo is None:
        raise InvalidRunEvent("event.eventTime must carry a UTC offset")
    return RunEvent(
        job_namespace=_text(job, "namespace", "job"),
        job_name=_text(job, "name", "job"),
        run_id=_text(run, "runId", "run"),
        event_time=event_time,
        inputs=_datasets(body, "inputs"),
        outputs=_datasets(body, "outputs"),
    )


def dataset_rows(event: RunEvent) -> list[dict]:
    """One row per dataset the event names: what the store keeps of it."""
    return [
        {
            "direction": direction,
            "dataset_namespace": d.namespace,
            "dataset_name": d.name,
            "column_lineage": {
                column: [
                    {
                        "namespace": i.namespace,
                        "name": i.name,
                        "field": i.field,
                        "transform": i.transform,
                    }
                    for i in inputs
                ]
                for column, inputs in d.column_lineage.items()
            },
        }
        for direction, datasets in ((_INPUT, event.inputs), (_OUTPUT, event.outputs))
        for d in datasets
    ]


def reported_jobs(rows: list[dict]) -> list[ReportedJob]:
    """The stored rows (:func:`dataset_rows`, with their job), one job each."""
    by_job: dict[tuple[str, str], dict[str, dict[tuple[str, str], ReportedDataset]]] = {}
    for row in rows:
        job = by_job.setdefault((row["job_namespace"], row["job_name"]), {_INPUT: {}, _OUTPUT: {}})
        key = (row["dataset_namespace"], row["dataset_name"])
        job[row["direction"]][key] = ReportedDataset(
            key[0],
            key[1],
            {
                column: tuple(ColumnInput(**i) for i in inputs)
                for column, inputs in row["column_lineage"].items()
            },
        )
    return [
        ReportedJob(ns, name, tuple(sides[_INPUT].values()), tuple(sides[_OUTPUT].values()))
        for (ns, name), sides in by_job.items()
    ]


def _names_source(namespace: str, table: RegisteredTable) -> bool:
    if "://" not in namespace:  # e.g. bigquery: no server, the dataset name carries the project
        return _source_type_matches(namespace.lower(), table)
    parts = urlsplit(namespace)
    try:
        port = parts.port
    except ValueError:
        return False
    if not _source_type_matches(parts.scheme.lower(), table):
        return False
    host = (parts.hostname or "").lower()
    source_host = table.host.lower()
    # A Snowflake namespace is the account, the source's host the account's whole address.
    if table.source_type == "snowflake":
        if source_host != host and not source_host.startswith(f"{host}."):
            return False
    elif source_host != host:
        return False
    return port is None or not table.port or port == table.port


def _source_type_matches(scheme: str, table: RegisteredTable) -> bool:
    return table.source_type in _SCHEME_SOURCE_TYPES.get(scheme, frozenset({scheme}))


def _names_table(name: str, table: RegisteredTable) -> bool:
    parts = name.lower().split(".")
    if len(parts) < 2 or parts[-2:] != [table.schema.lower(), table.table.lower()]:
        return False
    return len(parts) == 2 or not table.database or parts[-3] == table.database.lower()


def resolve_dataset(
    tables: list[RegisteredTable], namespace: str, name: str
) -> list[RegisteredTable]:
    """The registered tables a dataset names: none for one outside the model, several for a table
    registered in several domains."""
    return [t for t in tables if _names_source(namespace, t) and _names_table(name, t)]


@dataclass(frozen=True)
class _End:
    """One graph end of a dataset: a registered table (``columns`` set), or a box outside."""

    relation: str
    columns: dict[str, str] | None
    label: str | None


def _ends(tables: list[RegisteredTable], namespace: str, name: str) -> list[_End]:
    found = resolve_dataset(tables, namespace, name)
    if found:
        return [_End(t.relation, t.columns, None) for t in found]
    label = f"{namespace}/{name}"
    return [_End(f"{OUTSIDE_PREFIX}{label}", None, label)]


def _column_node(graph: LineageGraph, end: _End, field: str | None) -> str | None:
    """The node of ``field`` on ``end`` (of the whole dataset for None); None for a column of a
    registered table nobody registered, which no one reads through the model."""
    if end.columns is None:
        column = field or TABLE_LEVEL
        node = Node(
            id=f"{end.relation}.{column}",
            column=column,
            relation=end.relation,
            kind=EXTERNAL,
            label=end.label,
        )
    else:
        sql = end.columns.get((field or "").lower())
        if sql is None:
            return None
        node = Node(id=f"{end.relation}.{sql}", column=sql, relation=end.relation, kind="source")
    graph.add_node(node)
    return node.id


def _every_node(graph: LineageGraph, end: _End) -> list[str]:
    fields: list[str | None] = [None] if end.columns is None else list(end.columns)
    return [n for n in (_column_node(graph, end, f) for f in fields) if n is not None]


class _JobBox:
    def __init__(self, graph: LineageGraph, job: ReportedJob) -> None:
        self._graph = graph
        self._relation = f"{JOB_PREFIX}{job.namespace}/{job.name}"
        self._label = job.name
        self._linked: set[tuple[str, str]] = set()

    def row(self, key: str, column: str) -> str:
        node_id = f"{self._relation}.{key}"
        self._graph.add_node(
            Node(id=node_id, column=column, relation=self._relation, kind=JOB, label=self._label)
        )
        return node_id

    def hub(self) -> str:
        return self.row(TABLE_LEVEL, TABLE_LEVEL)

    def link(self, source: str, target: str, transform: str = "") -> None:
        if (source, target) not in self._linked:
            self._linked.add((source, target))
            self._graph.add_edge(Edge(source, target, transform, (), origin=OPENLINEAGE))


def _link_columns(
    graph: LineageGraph,
    box: _JobBox,
    tables: list[RegisteredTable],
    output: ReportedDataset,
    end: _End,
) -> set[tuple[str, str]]:
    """Draw the facet's column-to-column lines into ``end``; the input datasets they used."""
    used: set[tuple[str, str]] = set()
    for column, inputs in output.column_lineage.items():
        target = _column_node(graph, end, column)
        if target is None:
            continue
        written = graph.nodes[target].column
        step = box.row(f"{end.relation}.{written}", written)
        drawn = False
        for source in inputs:
            for in_end in _ends(tables, source.namespace, source.name):
                node = _column_node(graph, in_end, source.field)
                if node is not None:
                    box.link(node, step, source.transform)
                    used.add((source.namespace, source.name))
                    drawn = True
        if drawn:
            box.link(step, target)
        else:
            graph.nodes.pop(step)
    return used


def _add_job(graph: LineageGraph, job: ReportedJob, tables: list[RegisteredTable]) -> None:
    box = _JobBox(graph, job)
    inputs = [(d, _ends(tables, d.namespace, d.name)) for d in job.inputs]
    if not job.outputs:
        for ends in (ends for _, ends in inputs):
            for end in ends:
                for node in _every_node(graph, end):
                    box.link(node, box.hub())
        return
    for output in job.outputs:
        for end in _ends(tables, output.namespace, output.name):
            used = _link_columns(graph, box, tables, output, end)
            whole = [ends for d, ends in inputs if (d.namespace, d.name) not in used]
            for ends in whole:
                for in_end in ends:
                    for node in _every_node(graph, in_end):
                        box.link(node, box.hub())
            if whole or not used:
                for node in _every_node(graph, end):
                    box.link(box.hub(), node)


def add_reported_lineage(
    graph: LineageGraph, jobs: list[ReportedJob], tables: list[RegisteredTable]
) -> None:
    """Join each reported job to ``graph``, in place. A facet's columns draw column to column; an
    input it does not name, and a dataset with no facet, draw whole through the job's ``*`` row."""
    for job in jobs:
        _add_job(graph, job, tables)


def reported_seeds(graph: LineageGraph, seeds: set[str], *, unscoped: bool) -> set[str]:
    """The reported nodes a lineage read's ``seeds`` show. Lineage walks upstream, and a job's
    outside output is downstream of what it reads, so it would never be reached: with no lens,
    every reported node seeds; with one, those the seeds report into."""
    if unscoped:
        return {n.id for n in graph.nodes.values() if n.kind in REPORTED_KINDS}
    down: dict[str, list[str]] = {}
    for edge in graph.edges:
        if edge.origin == OPENLINEAGE:
            down.setdefault(edge.source, []).append(edge.target)
    added: set[str] = set()
    stack = list(seeds)
    while stack:
        for target in down.get(stack.pop(), []):
            if target not in added and graph.nodes[target].kind in REPORTED_KINDS:
                added.add(target)
                stack.append(target)
    return added
