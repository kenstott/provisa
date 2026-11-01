# Copyright (c) 2026 Kenneth Stott
# Canary: a3c9e175-08d4-4f6b-b2e1-5d7f90c4a826
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The W3C SPARQL suite's datasets, declared in the job's fixture model (REQ-2167, "setup that
creates objects is adapted, not excluded").

Provisa has no triple store and takes no data through a query surface, so a test's data file is
setup: it is replaced by the equivalent objects, declared in the model, and the test's own query
then runs against them. Each distinct set of data files is one dataset, in a domain and a role of
its own, so an open pattern (``?s ?p ?o``) reads that dataset and nothing else. The job user holds
every dataset's role and names one per request.

A dataset becomes, in schema ``conformance``:

* a node table per class (``rdf:type``), and one for the untyped subjects, keyed by ``id``: a
  subject is a row, and so is an IRI or blank node only named as an object;
* a column per predicate with literal objects, typed so its xsd datatype is the literal's. A
  predicate given literals of several datatypes splits its subjects into a table per datatype, the
  columns sharing one name, and an approved Property term (REQ-2163) is its one IRI; a class held
  by several tables likewise has a Class term;
* a junction table and a relationship per predicate with IRI or blank node objects, between each
  pair of node tables it joins.

Subjects, predicates and classes then have the model's IRIs (provisa.gql.iri, vocabulary), so
:func:`Fixture.term_map` maps the query's constant IRIs onto them and the answer's back; a blank
node's row maps back to a blank node. A table adds two things the dataset does not state, both read
by an open pattern: its key column as a predicate, and, for an untyped subject, its table class as
``rdf:type``. The runner keeps a variable predicate from them
(sparql_w3c_results.guard_open_patterns).

A dataset the model cannot state faithfully is never approximated: :attr:`Fixture.refusal` names
what stops it, and its cases fail with that reason. Which are those:

* a language-tagged literal, a datatype no column type answers, an ill-typed lexical form;
* two literal values of one predicate on one subject, or one predicate of both literals and
  resources;
* a subject of several classes.

Usage (in the job's .venv, before the instance starts):
  sparql_w3c_fixtures.py seed --suite <sparql/sparql11> --pg <dsn> --tables <tables.yaml>
      --relationships <relationships.yaml> --access <access.yaml>
"""

# Requirements: REQ-2167

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from rdflib import RDF, Dataset, Graph, Literal, URIRef, XSD  # noqa: E402

import sparql_w3c_manifest as manifest  # noqa: E402

SCHEMA = "conformance"
#: The deployment's public address in the job (PROVISA_PUBLIC_URL), the base of every model IRI.
BASE = "http://provisa.conformance.test"
#: The dataset of a case that reads none: syntax tests, and tests over an empty default graph.
EMPTY = "d000"

_TIMEZONED = re.compile(r"(Z|[+-]\d\d:\d\d)$")
_INT64 = 2**63


def _sql_type(lit: Literal) -> tuple[str | None, str | None]:
    """(the SQL type holding ``lit`` so it reads back as itself, why none does)."""
    if lit.language:
        return None, f"the language-tagged literal {lit.n3()}"
    if lit.ill_typed:
        return None, f"the ill-typed literal {lit.n3()}"
    dt = lit.datatype
    if dt is None or dt == XSD.string:
        return "text", None
    if dt == XSD.integer:
        return ("bigint", None) if abs(int(lit)) < _INT64 else (None, f"the integer {lit.n3()}")
    if dt == XSD.decimal:
        return "numeric", None
    if dt == XSD.double:
        return "double precision", None
    if dt == XSD.boolean:
        return "boolean", None
    if dt == XSD.date:
        return "date", None
    if dt == XSD.dateTime:
        return ("timestamptz" if _TIMEZONED.search(str(lit)) else "timestamp"), None
    return None, f"the datatype <{dt}>"


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    predicate: str


@dataclass
class Table:
    name: str
    columns: list[Column] = field(default_factory=list)
    rows: dict[str, dict[str, Any]] = field(default_factory=dict)  # id -> column -> value
    class_term: str | None = None  # the Class term of a class several tables hold


@dataclass
class Edge:
    rel_type: str
    predicate: str
    source: str
    target: str
    via: str
    rows: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class Fixture:
    id: str
    tables: list[Table] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    #: source term -> model IRI: subjects, predicates and classes.
    subjects: dict[Any, str] = field(default_factory=dict)
    predicates: dict[str, str] = field(default_factory=dict)
    classes: dict[str, str] = field(default_factory=dict)
    #: A predicate or class spanning tables: the Property or Class term that is its one IRI.
    terms: dict[str, str] = field(default_factory=dict)
    class_terms: dict[str, str] = field(default_factory=dict)
    refusal: str | None = None

    @property
    def domain(self) -> str:
        return f"sparql_{self.id}"

    role = domain

    def term_map(self) -> dict[str, str]:
        """Every source IRI to its model IRI, for a query's constants. A predicate's or class's
        mapping stands where an IRI is used in two ways: a query's token has no position here."""
        iris = {str(k): v for k, v in self.subjects.items() if isinstance(k, URIRef)}
        return {**iris, **self.classes, **self.predicates}

    def key_columns(self) -> tuple[str, ...]:
        """Each table's key column as a predicate: the fixture's, not the dataset's."""
        vocab = f"{BASE}/vocab/{_seg(self.domain)}"
        return tuple(f"{vocab}/{_seg(t.name)}#id" for t in self.tables)

    def untyped_classes(self) -> tuple[str, ...]:
        """The table class of each table of untyped subjects, an ``rdf:type`` only the fixture
        states."""
        vocab = f"{BASE}/vocab/{_seg(self.domain)}"
        classed = set(self.classes.values())
        names = (f"{vocab}/{_seg(t.name)}" for t in self.tables)
        return tuple(n for n in names if n not in classed)

    def scaffolding(self) -> frozenset[str]:
        """The IRIs of what the fixture adds to the dataset, which no answer of it holds."""
        return frozenset((*self.key_columns(), *self.untyped_classes()))

    def reverse_map(self) -> dict[str, Any]:
        """Every model IRI to the source term it stands for: an answer's terms, read back."""
        out: dict[str, Any] = {v: k for k, v in self.subjects.items()}
        out.update({v: URIRef(k) for k, v in {**self.classes, **self.predicates}.items()})
        return out


def _seg(value: str) -> str:
    return quote(value, safe="")


def _partition_keys(
    triples: list[tuple], classes: dict[Any, list[Any]], fixture: Fixture
) -> dict[Any, tuple] | None:
    """Each subject's table key: its class and the datatype of each predicate given literals of
    several, so those subjects' columns can differ in type. None, with the refusal set, when no
    table holds a subject: a literal this model has no column for, or two values on one subject."""
    types: dict[str, set[str]] = defaultdict(set)
    held: dict[tuple[Any, str], str] = {}
    for s, p, o in triples:
        if not isinstance(o, Literal) or p == RDF.type:
            continue
        sql_type, why = _sql_type(o)
        if sql_type is None:
            fixture.refusal = f"The dataset holds {why}, which no column holds"
            return None
        if (s, str(p)) in held:
            fixture.refusal = f"The dataset gives {s.n3()} several values of <{p}>"
            return None
        held[(s, str(p))] = sql_type
        types[str(p)].add(sql_type)
    mixed = {p for p, kinds in types.items() if len(kinds) > 1}
    subjects = {s for s, _, _ in triples} | {o for _, p, o in triples if not isinstance(o, Literal)}
    keys: dict[Any, tuple] = {}
    for term in subjects:
        own = classes.get(term)
        declared = tuple(sorted((p, t) for (s, p), t in held.items() if s == term and p in mixed))
        keys[term] = (str(own[0]) if own else "", declared)
    return keys


def adapt(ident: str, graph: Graph) -> Fixture:
    """``graph`` as fixture tables, or with its refusal."""
    fixture = Fixture(ident)
    domain = fixture.domain
    classes: dict[Any, list[Any]] = defaultdict(list)
    triples = sorted(graph, key=lambda t: (str(t[0]), str(t[1]), str(t[2])))
    typed = {(s, o) for s, p, o in triples if p == RDF.type and isinstance(o, URIRef)}
    objects_only = {o for s, o in typed} - {s for s, _, _ in triples}
    triples = [t for t in triples if not (t[1] == RDF.type and (t[0], t[2]) in typed)]
    for s, o in sorted(typed, key=str):
        classes[s].append(o)
    for s, held in classes.items():
        if len(held) > 1:
            fixture.refusal = f"The dataset gives {s.n3()} several classes, and a row has one table"
            return fixture
    keys = _partition_keys(
        [*triples, *((s, RDF.type, o) for s, o in sorted(typed, key=str))], classes, fixture
    )
    if keys is None:
        return fixture
    keys = {t: k for t, k in keys.items() if t not in objects_only}
    tables: dict[tuple, Table] = {}
    for key in sorted({("", ()), *keys.values()}, key=str):
        tables[key] = Table(f"{ident}_t{len(tables)}")

    ids: dict[Any, tuple[Table, str]] = {}
    for term in sorted(keys, key=str):
        table = tables[keys[term]]
        key = f"n{len(ids)}"
        ids[term] = (table, key)
        table.rows[key] = {}
        fixture.subjects[term] = f"{BASE}/id/{_seg(domain)}/{_seg(table.name)}/{key}"

    columns: dict[str, str] = {}  # predicate -> its column's name, the same in every table
    homes: dict[str, set[str]] = defaultdict(set)  # predicate -> the tables holding its column
    edges: dict[tuple[str, str, str], Edge] = {}
    rel_types: dict[str, str] = {}
    for s, p, o in triples:
        pred = str(p)
        table, key = ids[s]
        if not isinstance(o, Literal):
            rel_type = rel_types.setdefault(pred, f"{ident.upper()}_P{len(rel_types)}")
            target_table, target = ids[o]
            edge = edges.get((pred, table.name, target_table.name))
            if edge is None:
                via = f"{ident}_e{len(edges)}"
                edge = Edge(rel_type, pred, table.name, target_table.name, via)
                edges[(pred, table.name, target_table.name)] = edge
            edge.rows.append((key, target))
            continue
        sql_type, _why = _sql_type(o)
        assert sql_type is not None  # _partition_keys refused any other
        name = columns.setdefault(pred, f"{ident}_p{len(columns)}")
        if name not in {c.name for c in table.columns}:
            table.columns.append(Column(name, sql_type, pred))
        homes[pred].add(table.name)
        table.rows[key][name] = str(o) if sql_type == "text" else o.toPython()
    both = set(rel_types) & set(columns)
    if both:
        fixture.refusal = f"The dataset gives <{sorted(both)[0]}> both literals and resources"
        return fixture
    fixture.tables = list(tables.values())
    fixture.edges = list(edges.values())
    for pred, name in columns.items():
        if len(homes[pred]) == 1:
            home = next(iter(homes[pred]))
            fixture.predicates[pred] = f"{BASE}/vocab/{_seg(domain)}/{_seg(home)}#{_seg(name)}"
        else:
            fixture.terms[pred] = _term_name(name)
            fixture.predicates[pred] = f"{BASE}/vocab/term/{_seg(fixture.terms[pred])}"
    for pred, rel_type in rel_types.items():
        fixture.predicates[pred] = f"{BASE}/vocab/rel/{_seg(rel_type)}"
    for cls in sorted({c for held in classes.values() for c in held}, key=str):
        owners = [t for k, t in tables.items() if k[0] == str(cls)]
        if len(owners) == 1:
            table_iri = f"{BASE}/vocab/{_seg(domain)}/{_seg(owners[0].name)}"
            fixture.classes[str(cls)] = table_iri
        else:
            name = f"{ident} c{len(fixture.class_terms)}"
            fixture.class_terms[str(cls)] = name
            for t in owners:
                t.class_term = name
            fixture.classes[str(cls)] = f"{BASE}/vocab/term/{_seg(name)}"
    return fixture


def _term_name(column: str) -> str:
    """The Property term a column of this name derives (provisa.core.glossary.normalize_term)."""
    return column.replace("_", " ")


def data_uri(path: Path, root: Path) -> str:
    """The base a data file's relative IRIs resolve against, the same for its query's."""
    return f"http://conformance.test/{path.resolve().relative_to(root.resolve()).as_posix()}"


def read_data(files: tuple[Path, ...], root: Path) -> Graph:
    """The default graph of ``files``: a TriG file's named graphs are no default graph's, and a
    query reading them through GRAPH is refused before they matter."""
    g = Graph()
    for f in files:
        if f.suffix == ".trig":
            ds = Dataset()
            ds.parse(f, format="trig", publicID=data_uri(f, root))
            for triple in ds.default_context:
                g.add(triple)
            continue
        fmt = {".rdf": "xml", ".nt": "nt"}.get(f.suffix, "turtle")
        g.parse(f, format=fmt, publicID=data_uri(f, root))
    return g


def datasets(root: Path, cases: list[manifest.Case]) -> dict[tuple[Path, ...], str]:
    """Each distinct set of data files and its dataset's id, the no-data set being EMPTY."""
    keys = sorted({c.data for c in cases if c.kind == "query" and c.data})
    return {(): EMPTY, **{k: f"d{n + 1:03d}" for n, k in enumerate(keys)}}


def fixtures(root: Path, cases: list[manifest.Case]) -> dict[tuple[Path, ...], Fixture]:
    return {k: adapt(i, read_data(k, root)) for k, i in datasets(root, cases).items()}


def ddl(f: Fixture) -> list[str]:
    out = []
    for t in f.tables:
        cols = "".join(f", {c.name} {c.sql_type}" for c in t.columns)
        out.append(f"CREATE TABLE {SCHEMA}.{t.name} (id text PRIMARY KEY{cols})")
    for e in f.edges:
        out.append(
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{e.via} "
            "(id serial PRIMARY KEY, from_id text NOT NULL, to_id text NOT NULL)"
        )
    return out


def _table_entry(
    name: str,
    role: str,
    domain: str,
    columns: list[tuple[str, str, bool]],
    class_term: str | None = None,
) -> dict:
    from pgwire_catalog_types import registered_typing

    return {
        **({"class_term": class_term} if class_term else {}),
        "source_id": "conformance-pg",
        "schema": SCHEMA,
        "table": name,
        "alias": name,
        "domain_id": domain,
        "columns": [
            {
                "name": c,
                **registered_typing(sql_type),
                **({"is_primary_key": True} if pk else {}),
                "visible_to": [role],
            }
            for c, sql_type, pk in columns
        ],
    }


def table_entries(f: Fixture) -> list[dict]:
    out = []
    for t in f.tables:
        cols = [("id", "text", True), *((c.name, c.sql_type, False) for c in t.columns)]
        out.append(_table_entry(t.name, f.role, f.domain, cols, t.class_term))
    for e in f.edges:
        cols = [("id", "integer", True), ("from_id", "text", False), ("to_id", "text", False)]
        out.append(_table_entry(e.via, f.role, f.domain, cols))
    return out


def relationship_entries(f: Fixture) -> list[dict]:
    return [
        {
            "id": e.via,
            "alias": e.rel_type,
            "cardinality": "one-to-many",
            "source_table_id": e.source,
            "source_column": "id",
            "target_table_id": e.target,
            "target_column": "id",
            "via_table": e.via,
            "via_source_column": "from_id",
            "via_target_column": "to_id",
            "via_label_source": "fixed",
        }
        for e in f.edges
    ]


def access(served: list[Fixture]) -> dict:
    """Each dataset's domain and role. The job user holds them all (``default_assignments``) and
    names one per request (REQ-273)."""
    return {
        "domains": [{"id": f.domain, "description": f"W3C SPARQL dataset {f.id}"} for f in served],
        "roles": [
            {
                "id": f.role,
                "description": f"Reads W3C SPARQL dataset {f.id} alone",
                "capabilities": ["query_development", "full_results"],
                "domain_access": [f.domain],
            }
            for f in served
        ],
        "role_mapping": [],
        "default_assignments": [{"domain_id": "*", "role_id": f.role} for f in served],
        "glossary_terms": [
            {
                "name": name,
                "definition": f"W3C SPARQL dataset {f.id}: {iri}",
                "domains": [f.domain],
                "approve": True,
            }
            for f in served
            for iri, name in {**f.terms, **f.class_terms}.items()
        ],
    }


def seed(root: Path, pg_dsn: str, tables: Path, relationships: Path, access_out: Path) -> None:
    import psycopg
    import yaml

    cases = manifest.read(root)
    served = [f for f in fixtures(root, cases).values() if f.refusal is None]
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        for f in served:
            for statement in ddl(f):
                conn.execute(statement)
            for t in f.tables:
                names = ["id", *(c.name for c in t.columns)]
                insert = (
                    f"INSERT INTO {SCHEMA}.{t.name} ({', '.join(names)}) "
                    f"VALUES ({', '.join(['%s'] * len(names))})"
                )
                for key, row in t.rows.items():
                    conn.execute(insert, [key, *(row.get(c.name) for c in t.columns)])
            for e in f.edges:
                for src, dst in e.rows:
                    conn.execute(
                        f"INSERT INTO {SCHEMA}.{e.via} (from_id, to_id) VALUES (%s, %s)",
                        [src, dst],
                    )
    tables.write_text(
        yaml.safe_dump([t for f in served for t in table_entries(f)], sort_keys=False)
    )
    relationships.write_text(
        yaml.safe_dump([r for f in served for r in relationship_entries(f)], sort_keys=False)
    )
    access_out.write_text(yaml.safe_dump(access(served), sort_keys=False))
    print(f"seeded {len(served)} datasets in {SCHEMA}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--suite", required=True, type=Path)
    s.add_argument("--pg", required=True)
    s.add_argument("--tables", required=True, type=Path)
    s.add_argument("--relationships", required=True, type=Path)
    s.add_argument("--access", required=True, type=Path)
    args = parser.parse_args(argv)
    seed(args.suite, args.pg, args.tables, args.relationships, args.access)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
