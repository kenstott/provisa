# Copyright (c) 2026 Kenneth Stott
# Canary: 2d84f6b0-7e19-4c35-a1d2-c9e07b3f5146
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_catalog and information_schema as views in the meta domain (REQ-2190): meta is a superset of
the PostgreSQL catalog, and a client's relation is the view of its name, addressed by
:func:`catalog_view_name`. Nothing here is stored or kept in step: each view reads the model as it
stands, so there is nothing to populate.

Every view has its relation's PostgreSQL 17 columns in PostgreSQL 17's order
(catalog_pg17.RELATIONS), whatever the server's version, because each writes its own columns; only
the reference relations read the server's catalog, pg_collation alone by a column that differs
between versions. Two column types cannot be a view's or a value's as PostgreSQL has them, and are
held as text: anyarray, a pseudo-type, and pg_node_tree, which takes no input. An information_schema
relation has no oid to scope its rows by, so it carries one more column after PostgreSQL's,
:data:`SCOPE_COLUMN`.

Three things these views hold the shape of but not the rows of, because no view can: reltuples
(a live statistic of a source), the extension surfaces (a process's environment), and which of a
role's columns are served (provisa.transpiler.metadata_facts)."""

# Requirements: REQ-2190

from __future__ import annotations

from collections.abc import Mapping

from provisa.compiler.naming import SYSTEM_SCHEMA_GQL_PREFIX
from provisa.compiler.sql_literals import sql_literal
from provisa.pgwire.catalog_data import _PG_SETTINGS_ROWS, _PG_SYSTEM_ROLES
from provisa.pgwire.catalog_pg17 import RELATION_CLASS, RELATIONS, STATIC_ROWS, TYPES
from provisa.transpiler.metadata_functions import (
    listed_type_sql,
    pg_type_name_sql,
    pg_type_oid_sql,
    pg_typmod_sql,
)
from provisa.transpiler.metadata_names import (
    bare_field_sql,
    domain_sql_name_sql,
    sql_name_sql,
    table_convention_sql,
    without_domain_prefix_sql,
)
from provisa.transpiler.metadata_oids import (
    FIRST_FUNCTION_OID,
    INFORMATION_SCHEMA_OID,
    OWNER_OID,
    PG_CATALOG_OID,
    PUBLIC_OID,
    key_oid_sql,
    metric_oid_sql,
    role_oid_sql,
    schema_oid_sql,
    table_oid_sql,
)
from provisa.transpiler.metadata_settings import setting
from provisa.transpiler.metadata_views import (
    CATALOG_VIEW_NAMES,
    PG_CLASS_OID,
    SCOPE_COLUMN,
    SCOPED_BY_COLUMN,
    catalog_view_name,
)

_PG = "pg_catalog"
_IS = "information_schema"
#: PostgreSQL's oids of the catalogs pg_description names a row's kind by.
_PG_NAMESPACE = 2615
_BTREE = 403
_MASKED_PASSWORD = "'********'"
_SERVED_NAME = setting("database")
_METRICS_SCHEMA = "metrics"
#: What a type no pg_type row describes takes (catalog_data._PG_OID_ATTR_META's default).
_TYPES_HELD_AS_TEXT = frozenset({"anyarray", "pg_node_tree", "pg_statistic[]"})


def _column_type(pg_type: str) -> str:
    return "text" if pg_type in _TYPES_HELD_AS_TEXT else pg_type


def _select(relation: tuple[str, str], values: Mapping[str, str], source: str) -> str:
    """One arm of ``relation``'s view: each of its columns, in order, the value ``values`` gives
    it or NULL, of its type."""
    columns = RELATIONS[relation]
    unknown = set(values) - {c for c, _, _ in columns} - {SCOPE_COLUMN}
    assert not unknown, (relation, unknown)
    written = [
        f'CAST({values.get(column, "NULL")} AS {_column_type(pg_type)}) AS "{column}"'
        for column, pg_type, _ in columns
    ]
    if SCOPE_COLUMN in values:
        written.append(f"CAST({values[SCOPE_COLUMN]} AS INTEGER) AS {SCOPE_COLUMN}")
    return f"SELECT {', '.join(written)} {source}"


def _view(relation: tuple[str, str], *arms: str) -> str:
    return f"CREATE OR REPLACE VIEW {catalog_view_name(*relation)} AS\n" + "\nUNION ALL\n".join(
        arms
    )


def _empty(relation: tuple[str, str]) -> str:
    return _view(relation, _select(relation, {}, "WHERE FALSE"))


def _literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return sql_literal(str(value), "postgres")


def _rows(rows, columns: str) -> str:
    """``rows`` as a derived table of ``columns``."""
    values = ", ".join("(" + ", ".join(_literal(v) for v in row) + ")" for row in rows)
    return f"(VALUES {values}) AS _v({columns})"


# --- what every view reads ----------------------------------------------------------------------

_SYSTEM_DOMAINS = ", ".join(f"'{domain}'" for domain in SYSTEM_SCHEMA_GQL_PREFIX)
_CATALOG_NAMES = ", ".join(
    f"'{name}'" for name in (*CATALOG_VIEW_NAMES, "table_details", "column_details", "table_keys")
)
_IS_ITSELF = f"rt.domain_id = 'meta' AND rt.table_name IN ({_CATALOG_NAMES})"


def _catalog_tables() -> str:
    """Every table a catalog lists, by the name a statement queries it by, and every metric:
    drafts are in no catalog, and these views are in none (security.rights.SQL_CATALOG_TABLES).
    The naming rules apply in order, each everywhere in the name, as naming._apply_naming_rules."""
    convention = (
        "COALESCE(t.gql_naming_convention, t.source_convention, " + setting("gql_convention") + ")"
    )
    field = table_convention_sql("t.named", convention)
    bare = bare_field_sql(f"({field})", f"t.domain_id IN ({_SYSTEM_DOMAINS})")
    schema = domain_sql_name_sql("t.domain_id")
    return f"""
        CREATE OR REPLACE VIEW catalog_tables AS
        WITH RECURSIVE ordered_rules AS (
            SELECT ROW_NUMBER() OVER (ORDER BY id) AS step, pattern, replacement
            FROM naming_rules
        ),
        listed AS (
            SELECT rt.id, rt.domain_id, rt.table_name, rt.alias, rt.gql_naming_convention,
                   s.gql_naming_convention AS source_convention,
                   {without_domain_prefix_sql("rt.table_name", "rt.domain_id")} AS base
            FROM registered_tables rt
            JOIN sources s ON s.id = rt.source_id
            WHERE NOT rt.draft AND NOT ({_IS_ITSELF})
        ),
        renamed (id, named, step) AS (
            SELECT id, base, CAST(0 AS BIGINT) FROM listed
            UNION ALL
            SELECT r.id, REGEXP_REPLACE(r.named, o.pattern, o.replacement, 'g'), o.step
            FROM renamed r
            JOIN ordered_rules o ON o.step = r.step + 1
        ),
        final AS (
            SELECT DISTINCT ON (l.id) l.id, l.domain_id, l.alias, l.gql_naming_convention,
                   l.source_convention, r.named
            FROM listed l
            JOIN renamed r ON r.id = l.id
            ORDER BY l.id, r.step DESC
        ),
        named AS (
            SELECT t.id AS table_id, t.domain_id, {schema} AS schema_name,
                   CASE WHEN t.alias IS NOT NULL AND t.alias <> ''
                        THEN {sql_name_sql("t.alias")}
                        ELSE {sql_name_sql(f"({bare})")} END AS table_name,
                   rt.description
            FROM final t
            JOIN registered_tables rt ON rt.id = t.id
        )
        SELECT table_id, {table_oid_sql("table_id")} AS relation_oid, domain_id, schema_name,
               {schema_oid_sql("schema_name")} AS schema_oid, table_name
        FROM named
        UNION ALL
        SELECT NULL, {metric_oid_sql("name")}, NULL, '{_METRICS_SCHEMA}',
               {schema_oid_sql(f"'{_METRICS_SCHEMA}'")}, name
        FROM metrics
    """


_VIRTUAL_TYPE = "'varchar'"


def _catalog_columns() -> str:
    """Every column a catalog lists: a table's registered columns in order, then the columns every
    table is served with (sql_where.SERVED_WITH_EVERY_TABLE), then each metric's one value."""
    listed = listed_type_sql("c.data_type", "c.native_type", "d.subtype")
    return f"""
        CREATE OR REPLACE VIEW catalog_columns AS
        WITH registered AS (
            SELECT t.relation_oid, t.table_id, t.schema_name, t.table_name, c.column_name,
                   CASE WHEN c.alias IS NOT NULL AND c.alias <> '' THEN c.alias
                        ELSE {sql_name_sql("c.column_name")} END AS sql_name,
                   d.ordinal_position AS attnum, {listed} AS listed_type,
                   c.not_null, d.comment
            FROM catalog_tables t
            JOIN table_columns c ON c.table_id = t.table_id
            JOIN column_details d ON d.id = c.id
        ),
        last_column AS (
            SELECT table_id, MAX(attnum) AS attnum FROM registered GROUP BY table_id
        ),
        virtual AS (
            SELECT t.relation_oid, t.table_id, t.schema_name, t.table_name, v.column_name,
                   v.column_name AS sql_name,
                   CAST(COALESCE(l.attnum, 0) + v.place AS INTEGER) AS attnum,
                   {_VIRTUAL_TYPE} AS listed_type, FALSE AS not_null,
                   CAST(NULL AS TEXT) AS comment
            FROM catalog_tables t
            JOIN registered_tables rt ON rt.id = t.table_id
            CROSS JOIN LATERAL (
                VALUES ('_name_', 1), ('_domain_', 2),
                       ('table_query_name', CASE WHEN rt.domain_id = 'meta'
                                                  AND rt.table_name = 'registered_tables'
                                                 THEN 3 END)
            ) AS v(column_name, place)
            LEFT JOIN last_column l ON l.table_id = t.table_id
            WHERE v.place IS NOT NULL
        ),
        metric_values AS (
            SELECT t.relation_oid, t.table_id, t.schema_name, t.table_name,
                   'value' AS column_name, 'value' AS sql_name, 1 AS attnum,
                   COALESCE(m.datatype, 'double') AS listed_type, FALSE AS not_null,
                   CAST(NULL AS TEXT) AS comment
            FROM catalog_tables t
            JOIN metrics m ON m.name = t.table_name
            WHERE t.table_id IS NULL
        ),
        every_column AS (
            SELECT * FROM registered
            UNION ALL SELECT * FROM virtual
            UNION ALL SELECT * FROM metric_values
        )
        SELECT e.*, {pg_type_oid_sql("e.listed_type")} AS type_oid,
               {pg_type_name_sql("e.listed_type")} AS type_name,
               {pg_typmod_sql("e.listed_type")} AS typmod
        FROM every_column e
    """


def _catalog_keys() -> str:
    """Each key once, with what a catalog shows of it: its name and its columns' attnums in
    order."""
    return f"""
        CREATE OR REPLACE VIEW catalog_keys AS
        SELECT n.key_id, {key_oid_sql("n.key_id")} AS key_oid, MIN(n.kind) AS kind,
               n.table_id, {table_oid_sql("n.table_id")} AS relation_oid,
               MIN(n.schema_name) AS schema_name, {schema_oid_sql("MIN(n.schema_name)")}
                   AS schema_oid,
               MIN(n.table_name) AS table_name, n.referenced_table_id,
               {table_oid_sql("n.referenced_table_id")} AS referenced_oid,
               CASE MIN(n.kind)
                    WHEN 'p' THEN 'pk_' || MIN(n.table_name)
                    WHEN 'u' THEN MIN(n.key_name)
                    ELSE 'fk_' || MIN(n.table_name) || '__' || MIN(n.column_sql_name) END
                   AS key_name,
               ARRAY_AGG(c.attnum ORDER BY n.position) AS columns,
               ARRAY_AGG(rc.attnum ORDER BY n.position)
                   FILTER (WHERE rc.attnum IS NOT NULL) AS referenced_columns,
               ARRAY_AGG(n.column_sql_name ORDER BY n.position) AS column_names
        FROM table_keys n
        JOIN catalog_columns c
          ON c.table_id = n.table_id AND c.column_name = n.column_name
        LEFT JOIN catalog_columns rc
          ON rc.table_id = n.referenced_table_id AND rc.column_name = n.referenced_column
        GROUP BY n.key_id, n.table_id, n.referenced_table_id
    """


# --- relations with rows of the model ----------------------------------------------------------


def _pg_namespace() -> str:
    relation = (_PG, "pg_namespace")
    fixed = (
        ("pg_catalog", PG_CATALOG_OID),
        ("information_schema", INFORMATION_SCHEMA_OID),
        ("public", PUBLIC_OID),
    )
    domains = domain_sql_name_sql("d.id")
    return _view(
        relation,
        _select(
            relation,
            {"oid": "n.oid", "nspname": "n.name", "nspowner": str(OWNER_OID)},
            "FROM (SELECT DISTINCT o, n FROM ("
            f"SELECT {schema_oid_sql('s.name')} AS o, s.name AS n FROM (SELECT {domains} AS name "
            "FROM domains d WHERE d.id <> '') s "
            f"UNION ALL SELECT oid, name FROM {_rows(fixed, 'name, oid')} "
            f"UNION ALL SELECT DISTINCT schema_oid, schema_name FROM catalog_tables) a) "
            "AS n(oid, name)",
        ),
    )


def _class_row(values: Mapping[str, str]) -> dict[str, str]:
    """A pg_class row's values, over what every relation here has in common."""
    constants = {
        "reloftype": "0",
        "relowner": str(OWNER_OID),
        "relam": "0",
        "reltablespace": "0",
        "relpages": "0",
        "reltuples": "0",
        "relallvisible": "0",
        "reltoastrelid": "0",
        "relhasindex": "FALSE",
        "relisshared": "FALSE",
        "relpersistence": "'p'",
        "relchecks": "0",
        "relhasrules": "FALSE",
        "relhastriggers": "FALSE",
        "relhassubclass": "FALSE",
        "relrowsecurity": "FALSE",
        "relforcerowsecurity": "FALSE",
        "relispopulated": "TRUE",
        "relreplident": "'d'",
        "relispartition": "FALSE",
        "relrewrite": "0",
        "relfrozenxid": "'0'",
        "relminmxid": "'0'",
    }
    return {**constants, **values}


_ROW_TYPE_OFFSET = 100000


def _system_relations() -> list[tuple]:
    return [
        (
            oid,
            name,
            PG_CATALOG_OID if schema == _PG else INFORMATION_SCHEMA_OID,
            kind,
            len(RELATIONS[(schema, name)]),
        )
        for (schema, name), (oid, kind) in sorted(RELATION_CLASS.items())
    ]


def _pg_class() -> str:
    relation = (_PG, "pg_class")
    tables = _select(
        relation,
        _class_row(
            {
                "oid": "t.relation_oid",
                "relname": "t.table_name",
                "relnamespace": "t.schema_oid",
                "reltype": f"CAST(t.relation_oid AS BIGINT) + {_ROW_TYPE_OFFSET}",
                "relfilenode": "t.relation_oid",
                "relkind": "'r'",
                "relnatts": "(SELECT COUNT(*) FROM catalog_columns c "
                "WHERE c.relation_oid = t.relation_oid)",
            }
        ),
        "FROM catalog_tables t",
    )
    indexes = _select(
        relation,
        _class_row(
            {
                "oid": "k.key_oid",
                "relname": "k.key_name",
                "relnamespace": "k.schema_oid",
                "reltype": "0",
                "relam": str(_BTREE),
                "relfilenode": "k.key_oid",
                "relkind": "'i'",
                "relnatts": "CARDINALITY(k.columns)",
                "relreplident": "'n'",
            }
        ),
        "FROM catalog_keys k WHERE k.kind IN ('p', 'u')",
    )
    system = _select(
        relation,
        _class_row(
            {
                "oid": "s.oid",
                "relname": "s.name",
                "relnamespace": "s.namespace",
                "reltype": f"s.oid + {_ROW_TYPE_OFFSET}",
                "relfilenode": "s.oid",
                "relkind": "s.kind",
                "relnatts": "s.natts",
            }
        ),
        "FROM "
        + _rows(_system_relations(), "oid, name, namespace, kind, natts").replace(
            "AS _v(", "AS s("
        ),
    )
    return _view(relation, tables, indexes, system)


def _attribute_row(values: Mapping[str, str]) -> dict[str, str]:
    constants = {
        "attcacheoff": "-1",
        "attndims": "0",
        "attcompression": "''",
        "atthasmissing": "FALSE",
        "attidentity": "''",
        "attgenerated": "''",
        "attisdropped": "FALSE",
        "attislocal": "TRUE",
        "attinhcount": "0",
        "attcollation": "0",
    }
    return {**constants, **values}


def _system_attributes() -> list[tuple]:
    rows = []
    for (schema, name), (oid, _kind) in sorted(RELATION_CLASS.items()):
        for attnum, (column, _type, type_oid) in enumerate(RELATIONS[(schema, name)], 1):
            entry = TYPES[type_oid]
            rows.append((oid, column, type_oid, entry[2], attnum, entry[7], entry[8], entry[9]))
    return rows


def _pg_attribute() -> str:
    relation = (_PG, "pg_attribute")
    model = _select(
        relation,
        _attribute_row(
            {
                "attrelid": "c.relation_oid",
                "attname": "c.sql_name",
                "atttypid": "c.type_oid",
                "attlen": "COALESCE(y.typlen, -1)",
                "attnum": "c.attnum",
                "atttypmod": "c.typmod",
                "attbyval": "COALESCE(y.typbyval, FALSE)",
                "attalign": "COALESCE(y.typalign, 'i')",
                "attstorage": "COALESCE(y.typstorage, 'x')",
                "attnotnull": "c.not_null",
                "atthasdef": "EXISTS (SELECT 1 FROM column_defaults d "
                "WHERE d.table_id = c.table_id AND d.column_name = c.column_name)",
            }
        ),
        "FROM catalog_columns c LEFT JOIN pg_catalog.pg_type y ON y.oid = c.type_oid",
    )
    system = _select(
        relation,
        _attribute_row(
            {
                "attrelid": "s.relation",
                "attname": "s.name",
                "atttypid": "s.type_oid",
                "attlen": "s.length",
                "attnum": "s.attnum",
                "atttypmod": "-1",
                "attbyval": "s.byval",
                "attalign": "s.align",
                "attstorage": "s.storage",
                "attnotnull": "FALSE",
                "atthasdef": "FALSE",
            }
        ),
        "FROM "
        + _rows(
            _system_attributes(), "relation, name, type_oid, length, attnum, byval, align, storage"
        ).replace("AS _v(", "AS s("),
    )
    return _view(relation, model, system)


#: A role's column defaults: what its INSERT grant presets, as PostgreSQL stores a default
#: (catalog_privileges._default_expression), on a table the role may write.
def _column_defaults() -> str:
    numeric = "('smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision')"
    value = "p.preset ->> 'value'"
    quoted = f"QUOTE_LITERAL({value})"
    return f"""
        CREATE OR REPLACE VIEW column_defaults AS
        SELECT g.table_id, p.preset ->> 'column' AS column_name,
               CASE p.preset ->> 'source'
                    WHEN 'now' THEN 'now()'
                    WHEN 'session' THEN
                        'current_setting(' || QUOTE_LITERAL(p.preset ->> 'name') || ')'
                    ELSE CASE WHEN c.type_name IN {numeric} AND {value} ~ '^-?[0-9]+(\\.[0-9]+)?$'
                              THEN {value}
                              WHEN c.type_name = 'boolean'
                               AND LOWER({value}) IN ('true', 'false')
                              THEN LOWER({value})
                              ELSE {quoted} || '::' || c.type_name END
               END AS expression,
               c.relation_oid, c.attnum, c.sql_name,
               ROW_NUMBER() OVER (ORDER BY g.table_id, c.attnum) AS place
        FROM table_write_grants g
        CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(CAST(g.presets AS JSONB)) AS p(preset)
        JOIN catalog_columns c
          ON c.table_id = g.table_id AND c.column_name = p.preset ->> 'column'
        WHERE g.operation = 'insert' AND g.role_id = {setting("role")}
          AND {setting("can_write")} = 'true'
    """


def _pg_attrdef() -> str:
    relation = (_PG, "pg_attrdef")
    return _view(
        relation,
        _select(
            relation,
            {
                "oid": "100000 + d.place",
                "adrelid": "d.relation_oid",
                "adnum": "d.attnum",
                "adbin": "d.expression",
            },
            "FROM column_defaults d",
        ),
    )


def _pg_description() -> str:
    relation = (_PG, "pg_description")
    tables = _select(
        relation,
        {
            "objoid": "t.relation_oid",
            "classoid": str(PG_CLASS_OID),
            "objsubid": "0",
            "description": "d.comment",
        },
        "FROM catalog_tables t JOIN table_details d ON d.id = t.table_id "
        "WHERE d.comment IS NOT NULL",
    )
    columns = _select(
        relation,
        {
            "objoid": "c.relation_oid",
            "classoid": str(PG_CLASS_OID),
            "objsubid": "c.attnum",
            "description": "c.comment",
        },
        "FROM catalog_columns c WHERE c.comment IS NOT NULL",
    )
    schemas = _select(
        relation,
        {
            "objoid": schema_oid_sql(domain_sql_name_sql("d.id")),
            "classoid": str(_PG_NAMESPACE),
            "objsubid": "0",
            "description": "d.description",
        },
        "FROM domains d WHERE d.id <> '' AND d.description <> ''",
    )
    metrics = _select(
        relation,
        {
            "objoid": metric_oid_sql("m.name"),
            "classoid": str(PG_CLASS_OID),
            "objsubid": "0",
            "description": "COALESCE(m.description, m.ai_context)",
        },
        "FROM metrics m WHERE COALESCE(m.description, m.ai_context) IS NOT NULL",
    )
    return _view(relation, tables, columns, schemas, metrics)


def _pg_constraint() -> str:
    relation = (_PG, "pg_constraint")
    return _view(
        relation,
        _select(
            relation,
            {
                "oid": "k.key_oid",
                "conname": "k.key_name",
                "connamespace": "k.schema_oid",
                "contype": "k.kind",
                "condeferrable": "FALSE",
                "condeferred": "FALSE",
                "convalidated": "TRUE",
                "conrelid": "k.relation_oid",
                "contypid": "0",
                "conindid": "CASE WHEN k.kind = 'f' THEN 0 ELSE k.key_oid END",
                "conparentid": "0",
                "confrelid": "CASE WHEN k.kind = 'f' THEN k.referenced_oid ELSE 0 END",
                "confupdtype": "CASE WHEN k.kind = 'f' THEN 'a' ELSE ' ' END",
                "confdeltype": "CASE WHEN k.kind = 'f' THEN 'a' ELSE ' ' END",
                "confmatchtype": "CASE WHEN k.kind = 'f' THEN 's' ELSE ' ' END",
                "conislocal": "TRUE",
                "coninhcount": "0",
                "connoinherit": "TRUE",
                "conkey": "k.columns",
                "confkey": "k.referenced_columns",
            },
            "FROM catalog_keys k",
        ),
    )


def _pg_index() -> str:
    relation = (_PG, "pg_index")
    zeros = "ARRAY_TO_STRING(ARRAY_FILL(0, ARRAY[CARDINALITY(k.columns)]), ' ')"
    return _view(
        relation,
        _select(
            relation,
            {
                "indexrelid": "k.key_oid",
                "indrelid": "k.relation_oid",
                "indnatts": "CARDINALITY(k.columns)",
                "indnkeyatts": "CARDINALITY(k.columns)",
                "indisunique": "TRUE",
                "indnullsnotdistinct": "FALSE",
                "indisprimary": "k.kind = 'p'",
                "indisexclusion": "FALSE",
                "indimmediate": "TRUE",
                "indisclustered": "FALSE",
                "indisvalid": "TRUE",
                "indcheckxmin": "FALSE",
                "indisready": "TRUE",
                "indislive": "TRUE",
                "indisreplident": "FALSE",
                "indkey": "ARRAY_TO_STRING(k.columns, ' ')",
                "indcollation": zeros,
                "indclass": zeros,
                "indoption": zeros,
            },
            "FROM catalog_keys k WHERE k.kind IN ('p', 'u')",
        ),
    )


def _function_type(returns: str) -> str:
    """function_catalog._fn_sql_type, over a tracked function's declared type (SQL)."""
    from provisa.pgwire.function_catalog import _FN_TYPE_TO_SQL

    cases = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in _FN_TYPE_TO_SQL.items())
    return f"CASE LOWER(COALESCE({returns}, '')) {cases} ELSE 'character varying' END"


_FUNCTION_OID = f"{FIRST_FUNCTION_OID} + f.id"


def _pg_proc() -> str:
    relation = (_PG, "pg_proc")
    return _view(
        relation,
        _select(
            relation,
            {
                "oid": _FUNCTION_OID,
                "proname": "f.name",
                "pronamespace": str(PUBLIC_OID),
                "proowner": str(OWNER_OID),
                "prokind": "'f'",
                "prosecdef": "FALSE",
                "proleakproof": "FALSE",
                "proisstrict": "FALSE",
                "proretset": "f.return_schema IS NOT NULL",
                "provolatile": "'v'",
                "proparallel": "'u'",
                "pronargs": "JSONB_ARRAY_LENGTH(CAST(f.arguments AS JSONB))",
                "pronargdefaults": "0",
                "prorettype": "0",
                "prosrc": "f.name",
            },
            "FROM tracked_functions f",
        ),
    )


def _is_routines() -> str:
    relation = (_IS, "routines")
    specific = f"f.name || '_' || CAST({_FUNCTION_OID} AS TEXT)"
    return _view(
        relation,
        _select(
            relation,
            {
                "specific_catalog": _SERVED_NAME,
                "specific_schema": "'public'",
                "specific_name": specific,
                "routine_catalog": _SERVED_NAME,
                "routine_schema": "'public'",
                "routine_name": "f.name",
                "routine_type": "'FUNCTION'",
                "data_type": "CASE WHEN f.return_schema IS NOT NULL THEN 'record' "
                f"ELSE {_function_type('f.returns')} END",
                "routine_body": "'EXTERNAL'",
                "external_language": "'EXTERNAL'",
                "is_deterministic": "'NO'",
                "sql_data_access": "'MODIFIES'",
                "security_type": "'INVOKER'",
            },
            "FROM tracked_functions f",
        ),
    )


def _is_parameters() -> str:
    relation = (_IS, "parameters")
    return _view(
        relation,
        _select(
            relation,
            {
                "specific_catalog": _SERVED_NAME,
                "specific_schema": "'public'",
                "specific_name": f"f.name || '_' || CAST({_FUNCTION_OID} AS TEXT)",
                "ordinal_position": "a.position",
                "parameter_mode": "'IN'",
                "is_result": "'NO'",
                "parameter_name": "a.argument ->> 'name'",
                "data_type": _function_type("a.argument ->> 'type'"),
            },
            "FROM tracked_functions f CROSS JOIN LATERAL "
            "JSONB_ARRAY_ELEMENTS(CAST(f.arguments AS JSONB)) WITH ORDINALITY AS a(argument, "
            "position)",
        ),
    )


_ROLE_COLUMNS = (
    "oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, "
    "rolconnlimit, rolbypassrls"
)


def _roles() -> str:
    """Every role of the model, the asking role, the role that owns every table, and PostgreSQL's
    own: what catalog_populate._populate_pg_roles_and_database lists, numbered by name."""
    system = _rows(
        _PG_SYSTEM_ROLES,
        _ROLE_COLUMNS,
    )
    model = (
        "FALSE AS rolsuper, TRUE AS rolinherit, FALSE AS rolcreaterole, FALSE AS rolcreatedb, "
        "TRUE AS rolcanlogin, FALSE AS rolreplication, -1 AS rolconnlimit, FALSE AS rolbypassrls"
    )
    return f"""
        CREATE OR REPLACE VIEW catalog_roles AS
        SELECT {role_oid_sql("r.id")} AS oid, r.id AS rolname, {model}
        FROM roles r
        UNION ALL
        SELECT {role_oid_sql(setting("role"))}, {setting("role")}, {model}
        WHERE {setting("role")} IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM roles r WHERE r.id = {setting("role")})
        UNION ALL
        SELECT {OWNER_OID}, 'provisa', {model}
        UNION ALL
        SELECT {_ROLE_COLUMNS} FROM {system}
    """


def _pg_roles() -> str:
    relation = (_PG, "pg_roles")
    names = (
        "rolname rolsuper rolinherit rolcreaterole rolcreatedb rolcanlogin rolreplication "
        "rolconnlimit rolbypassrls oid"
    ).split()
    return _view(
        relation,
        _select(
            relation,
            {**{n: f"r.{n}" for n in names}, "rolpassword": _MASKED_PASSWORD},
            "FROM catalog_roles r",
        ),
    )


def _pg_authid() -> str:
    relation = (_PG, "pg_authid")
    names = (
        "oid rolname rolsuper rolinherit rolcreaterole rolcreatedb rolcanlogin rolreplication "
        "rolbypassrls rolconnlimit"
    ).split()
    return _view(relation, _select(relation, {n: f"r.{n}" for n in names}, "FROM catalog_roles r"))


def _login_roles(name: str, password: str) -> str:
    relation = (_PG, name)
    return _view(
        relation,
        _select(
            relation,
            {
                "usename": "r.rolname",
                "usesysid": "r.oid",
                "usecreatedb": "r.rolcreatedb",
                "usesuper": "r.rolsuper",
                "userepl": "r.rolreplication",
                "usebypassrls": "r.rolbypassrls",
                "passwd": password,
            },
            "FROM catalog_roles r WHERE r.rolcanlogin",
        ),
    )


def _pg_group() -> str:
    relation = (_PG, "pg_group")
    return _view(
        relation,
        _select(
            relation,
            {"groname": "r.rolname", "grosysid": "r.oid", "grolist": "ARRAY[]::oid[]"},
            "FROM catalog_roles r WHERE NOT r.rolcanlogin",
        ),
    )


def _pg_auth_members() -> str:
    relation = (_PG, "pg_auth_members")
    return _view(
        relation,
        _select(
            relation,
            {
                "oid": role_oid_sql("(p.role_id || ':' || p.parent_role_id)"),
                "roleid": role_oid_sql("p.parent_role_id"),
                "member": role_oid_sql("p.role_id"),
                "grantor": str(OWNER_OID),
                "admin_option": "FALSE",
                "inherit_option": "TRUE",
                "set_option": "TRUE",
            },
            "FROM roles_parents p",
        ),
    )


def _pg_database() -> str:
    relation = (_PG, "pg_database")
    return _view(
        relation,
        _select(
            relation,
            {
                "oid": "16384",
                "datname": _SERVED_NAME,
                "datdba": str(OWNER_OID),
                "encoding": "6",
                "datlocprovider": "'c'",
                "datistemplate": "FALSE",
                "datallowconn": "TRUE",
                "dathasloginevt": "FALSE",
                "datconnlimit": "-1",
                "datfrozenxid": "'726'",
                "datminmxid": "'1'",
                "dattablespace": "1663",
                "datcollate": "'en_US.UTF-8'",
                "datctype": "'en_US.UTF-8'",
            },
            "",
        ),
    )


def _pg_settings() -> str:
    relation = (_PG, "pg_settings")
    columns = [c for c, _, _ in RELATIONS[relation]]
    return _view(
        relation,
        _select(
            relation,
            {c: f"s.{c}" for c in columns if c != "enumvals"},
            "FROM "
            + _rows(
                [
                    tuple(None if c == "enumvals" else v for c, v in zip(columns, row))
                    for row in _PG_SETTINGS_ROWS
                ],
                ", ".join(columns),
            ).replace("AS _v(", "AS s("),
        ),
    )


def _pg_tables() -> str:
    relation = (_PG, "pg_tables")
    return _view(
        relation,
        _select(
            relation,
            {
                "schemaname": "t.schema_name",
                "tablename": "t.table_name",
                "tableowner": "'provisa'",
                "hasindexes": "FALSE",
                "hasrules": "FALSE",
                "hastriggers": "FALSE",
                "rowsecurity": "FALSE",
                SCOPE_COLUMN: "t.table_id",
            },
            "FROM catalog_tables t",
        ),
    )


def _pg_indexes() -> str:
    relation = (_PG, "pg_indexes")
    return _view(
        relation,
        _select(
            relation,
            {
                "schemaname": "k.schema_name",
                "tablename": "k.table_name",
                "indexname": "k.key_name",
                "indexdef": "'CREATE UNIQUE INDEX ' || k.key_name || ' ON ' || k.schema_name || "
                "'.' || k.table_name || ' USING btree (' || "
                "ARRAY_TO_STRING(k.column_names, ', ') || ')'",
                SCOPE_COLUMN: "k.table_id",
            },
            "FROM catalog_keys k WHERE k.kind IN ('p', 'u')",
        ),
    )


def _pg_stat_activity() -> str:
    """The connection asking, the one row a catalog lists (catalog._list_session). A statement that
    names no connection lists none."""
    relation = (_PG, "pg_stat_activity")
    return _view(
        relation,
        _select(
            relation,
            {
                "datid": "16384",
                "datname": _SERVED_NAME,
                "pid": f"CAST({setting('pid')} AS INTEGER)",
                "usesysid": role_oid_sql(setting("role")),
                "usename": setting("role"),
                "application_name": f"COALESCE({setting('application_name')}, '')",
                "backend_start": f"CAST({setting('backend_start')} AS TIMESTAMPTZ)",
                "xact_start": "NOW()",
                "query_start": "NOW()",
                "state_change": "NOW()",
                "state": "'active'",
                "query": setting("query"),
                "backend_type": "'client backend'",
            },
            f"WHERE {setting('pid')} IS NOT NULL",
        ),
    )


def _is_tables() -> str:
    relation = (_IS, "tables")
    return _view(
        relation,
        _select(
            relation,
            {
                "table_catalog": _SERVED_NAME,
                "table_schema": "t.schema_name",
                "table_name": "t.table_name",
                "table_type": "'BASE TABLE'",
                "is_insertable_into": "'YES'",
                "is_typed": "'NO'",
                SCOPE_COLUMN: "t.table_id",
            },
            "FROM catalog_tables t",
        ),
    )


def _is_columns() -> str:
    relation = (_IS, "columns")
    return _view(
        relation,
        _select(
            relation,
            {
                "table_catalog": _SERVED_NAME,
                "table_schema": "c.schema_name",
                "table_name": "c.table_name",
                "column_name": "c.sql_name",
                "ordinal_position": "c.attnum",
                "column_default": "(SELECT d.expression FROM column_defaults d "
                "WHERE d.table_id = c.table_id AND d.column_name = c.column_name LIMIT 1)",
                "is_nullable": "CASE WHEN c.not_null THEN 'NO' ELSE 'YES' END",
                "data_type": "c.type_name",
                "udt_name": "c.type_name",
                "dtd_identifier": "CAST(c.attnum AS TEXT)",
                "is_self_referencing": "'NO'",
                "is_identity": "'NO'",
                "is_generated": "'NEVER'",
                "is_updatable": "'YES'",
                SCOPE_COLUMN: "c.table_id",
            },
            "FROM catalog_columns c",
        ),
    )


_CONSTRAINT_TYPE = (
    "CASE k.kind WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' ELSE 'FOREIGN KEY' END"
)


def _is_table_constraints() -> str:
    relation = (_IS, "table_constraints")
    return _view(
        relation,
        _select(
            relation,
            {
                "constraint_catalog": _SERVED_NAME,
                "constraint_schema": "k.schema_name",
                "constraint_name": "k.key_name",
                "table_catalog": _SERVED_NAME,
                "table_schema": "k.schema_name",
                "table_name": "k.table_name",
                "constraint_type": _CONSTRAINT_TYPE,
                "is_deferrable": "'NO'",
                "initially_deferred": "'NO'",
                "enforced": "'YES'",
                "nulls_distinct": "'YES'",
                SCOPE_COLUMN: "k.table_id",
            },
            "FROM catalog_keys k",
        ),
    )


def _is_key_column_usage() -> str:
    relation = (_IS, "key_column_usage")
    return _view(
        relation,
        _select(
            relation,
            {
                "constraint_catalog": _SERVED_NAME,
                "constraint_schema": "k.schema_name",
                "constraint_name": "k.key_name",
                "table_catalog": _SERVED_NAME,
                "table_schema": "k.schema_name",
                "table_name": "k.table_name",
                "column_name": "u.name",
                "ordinal_position": "u.position",
                "position_in_unique_constraint": "CASE WHEN k.kind = 'p' THEN u.position END",
                SCOPE_COLUMN: "k.table_id",
            },
            "FROM catalog_keys k CROSS JOIN LATERAL UNNEST(k.column_names) WITH ORDINALITY "
            "AS u(name, position)",
        ),
    )


def _is_referential_constraints() -> str:
    relation = (_IS, "referential_constraints")
    return _view(
        relation,
        _select(
            relation,
            {
                "constraint_catalog": _SERVED_NAME,
                "constraint_schema": "k.schema_name",
                "constraint_name": "k.key_name",
                "unique_constraint_catalog": _SERVED_NAME,
                "unique_constraint_schema": "r.schema_name",
                "unique_constraint_name": "r.key_name",
                "match_option": "'NONE'",
                "update_rule": "'NO ACTION'",
                "delete_rule": "'NO ACTION'",
                SCOPE_COLUMN: "k.table_id",
            },
            "FROM catalog_keys k JOIN catalog_keys r "
            "ON r.table_id = k.referenced_table_id AND r.kind IN ('p', 'u') "
            "AND r.columns @> k.referenced_columns AND r.columns <@ k.referenced_columns "
            "WHERE k.kind = 'f'",
        ),
    )


def _is_constraint_table_usage() -> str:
    relation = (_IS, "constraint_table_usage")
    return _view(
        relation,
        _select(
            relation,
            {
                "table_catalog": _SERVED_NAME,
                "table_schema": "t.schema_name",
                "table_name": "t.table_name",
                "constraint_catalog": _SERVED_NAME,
                "constraint_schema": "k.schema_name",
                "constraint_name": "k.key_name",
                SCOPE_COLUMN: "k.table_id",
            },
            "FROM catalog_keys k JOIN catalog_tables t ON t.table_id = k.referenced_table_id",
        ),
    )


def _is_constraint_column_usage() -> str:
    relation = (_IS, "constraint_column_usage")
    return _view(
        relation,
        _select(
            relation,
            {
                "table_catalog": _SERVED_NAME,
                "table_schema": "c.schema_name",
                "table_name": "c.table_name",
                "column_name": "c.sql_name",
                "constraint_catalog": _SERVED_NAME,
                "constraint_schema": "k.schema_name",
                "constraint_name": "k.key_name",
                SCOPE_COLUMN: "k.table_id",
            },
            "FROM catalog_keys k JOIN catalog_columns c "
            "ON c.table_id = k.referenced_table_id AND c.attnum = ANY("
            "CASE WHEN k.kind = 'f' THEN k.referenced_columns ELSE k.columns END)",
        ),
    )


def _is_check_constraints() -> str:
    relation = (_IS, "check_constraints")
    return _view(
        relation,
        _select(
            relation,
            {
                "constraint_catalog": _SERVED_NAME,
                "constraint_schema": "c.schema_name",
                "constraint_name": f"CAST({schema_oid_sql('c.schema_name')} AS TEXT) || '_' || "
                "CAST(c.relation_oid AS TEXT) || '_' || CAST(c.attnum AS TEXT) || '_not_null'",
                "check_clause": "c.sql_name || ' IS NOT NULL'",
                SCOPE_COLUMN: "c.table_id",
            },
            "FROM catalog_columns c WHERE c.not_null",
        ),
    )


def _is_character_sets() -> str:
    relation = (_IS, "character_sets")
    return _view(
        relation,
        _select(
            relation,
            {
                "character_set_name": "'UTF8'",
                "character_repertoire": "'UCS'",
                "form_of_use": "'UTF8'",
            },
            "",
        ),
    )


def _is_catalog_name() -> str:
    relation = (_IS, "information_schema_catalog_name")
    return _view(relation, _select(relation, {"catalog_name": _SERVED_NAME}, ""))


# --- reference data -----------------------------------------------------------------------------

#: A reference relation, and the condition keeping PostgreSQL's own rows of the server's: a
#: server's relation also lists what is created on it, here the control plane's objects.
_REFERENCE: dict[str, str] = {
    "pg_aggregate": "r.aggfnoid::oid < 16384",
    "pg_am": "r.oid < 16384",
    "pg_amop": "r.oid < 16384",
    "pg_amproc": "r.oid < 16384",
    "pg_cast": "r.oid < 16384",
    "pg_collation": "r.oid < 16384",
    "pg_conversion": "r.oid < 16384",
    "pg_language": "r.oid < 16384",
    "pg_opclass": "r.oid < 16384",
    "pg_operator": "r.oid < 16384",
    "pg_opfamily": "r.oid < 16384",
    "pg_range": "r.rngtypid < 16384",
    "pg_tablespace": "r.oid < 16384",
    "pg_timezone_abbrevs": "TRUE",
    "pg_timezone_names": "TRUE",
    "pg_ts_config": "r.oid < 16384",
    "pg_ts_config_map": "r.mapcfg < 16384",
    "pg_ts_dict": "r.oid < 16384",
    "pg_ts_parser": "r.oid < 16384",
    "pg_ts_template": "r.oid < 16384",
}
#: A column of PostgreSQL 17 a server of another version names otherwise, read by name.
_ACROSS_VERSIONS = {
    ("pg_collation", "colllocale"): "COALESCE(to_jsonb(r) ->> 'colllocale', "
    "to_jsonb(r) ->> 'colliculocale')",
}


def _reference(name: str) -> str:
    relation = (_PG, name)
    values = {
        column: _ACROSS_VERSIONS.get((name, column), f'r."{column}"')
        for column, _, _ in RELATIONS[relation]
    }
    return _view(
        relation,
        _select(relation, values, f"FROM pg_catalog.{name} r WHERE {_REFERENCE[name]}"),
    )


def _pg_type() -> str:
    """PostgreSQL's types, and the two PostgreSQL has only with PostGIS (ir_types.SUBTYPES)."""
    relation = (_PG, "pg_type")
    real = {column: f'r."{column}"' for column, _, _ in RELATIONS[relation]}
    own = _select(
        relation,
        {
            **{
                c: f"{v}"
                for c, v in {
                    "typowner": "10",
                    "typlen": "-1",
                    "typbyval": "FALSE",
                    "typtype": "'b'",
                    "typcategory": "'U'",
                    "typispreferred": "FALSE",
                    "typisdefined": "TRUE",
                    "typdelim": "','",
                    "typrelid": "0",
                    "typsubscript": "0",
                    "typelem": "0",
                    "typarray": "0",
                    "typinput": "0",
                    "typoutput": "0",
                    "typreceive": "0",
                    "typsend": "0",
                    "typmodin": "0",
                    "typmodout": "0",
                    "typanalyze": "0",
                    "typalign": "'d'",
                    "typstorage": "'m'",
                    "typnotnull": "FALSE",
                    "typbasetype": "0",
                    "typtypmod": "-1",
                    "typndims": "0",
                    "typcollation": "0",
                }.items()
            },
            "oid": "o.oid",
            "typname": "o.name",
            "typnamespace": str(PUBLIC_OID),
        },
        "FROM (VALUES (8310, 'geometry'), (8311, 'geography')) AS o(oid, name)",
    )
    return _view(
        relation,
        _select(relation, real, "FROM pg_catalog.pg_type r WHERE r.oid < 16384"),
        own,
    )


def _conformance(name: str) -> str:
    """The rows PostgreSQL fixes for an information_schema table on SQL conformance."""
    relation = (_IS, name)
    columns = [c for c, _, _ in RELATIONS[relation]]
    return _view(
        relation,
        _select(
            relation,
            {c: f's."{c}"' for c in columns},
            "FROM "
            + _rows(STATIC_ROWS[name], ", ".join(f'"{c}"' for c in columns)).replace(
                "AS _v(", "AS s("
            ),
        ),
    )


# --- every relation -----------------------------------------------------------------------------

_MODEL_VIEWS = {
    (_PG, "pg_namespace"): _pg_namespace,
    (_PG, "pg_class"): _pg_class,
    (_PG, "pg_attribute"): _pg_attribute,
    (_PG, "pg_attrdef"): _pg_attrdef,
    (_PG, "pg_description"): _pg_description,
    (_PG, "pg_constraint"): _pg_constraint,
    (_PG, "pg_index"): _pg_index,
    (_PG, "pg_proc"): _pg_proc,
    (_PG, "pg_roles"): _pg_roles,
    (_PG, "pg_authid"): _pg_authid,
    (_PG, "pg_user"): lambda: _login_roles("pg_user", _MASKED_PASSWORD),
    (_PG, "pg_shadow"): lambda: _login_roles("pg_shadow", "NULL"),
    (_PG, "pg_group"): _pg_group,
    (_PG, "pg_auth_members"): _pg_auth_members,
    (_PG, "pg_database"): _pg_database,
    (_PG, "pg_settings"): _pg_settings,
    (_PG, "pg_tables"): _pg_tables,
    (_PG, "pg_indexes"): _pg_indexes,
    (_PG, "pg_stat_activity"): _pg_stat_activity,
    (_PG, "pg_type"): _pg_type,
    (_IS, "tables"): _is_tables,
    (_IS, "columns"): _is_columns,
    (_IS, "table_constraints"): _is_table_constraints,
    (_IS, "key_column_usage"): _is_key_column_usage,
    (_IS, "referential_constraints"): _is_referential_constraints,
    (_IS, "constraint_table_usage"): _is_constraint_table_usage,
    (_IS, "constraint_column_usage"): _is_constraint_column_usage,
    (_IS, "check_constraints"): _is_check_constraints,
    (_IS, "routines"): _is_routines,
    (_IS, "parameters"): _is_parameters,
    (_IS, "character_sets"): _is_character_sets,
    (_IS, "information_schema_catalog_name"): _is_catalog_name,
}


def _ddl(relation: tuple[str, str]) -> str:
    schema, name = relation
    if relation in _MODEL_VIEWS:
        return _MODEL_VIEWS[relation]()
    if schema == _PG and name in _REFERENCE:
        return _reference(name)
    if schema == _IS and name in STATIC_ROWS:
        return _conformance(name)
    return _empty(relation)


#: The views the relations read, none of them registered, so no statement names one: those that
#: read the views the meta domain lists (column_details, table_keys) after them.
CATALOG_BASE_VIEWS: dict[str, str] = {
    "catalog_tables": _catalog_tables(),
}
CATALOG_COLUMN_VIEWS: dict[str, str] = {
    "catalog_columns": _catalog_columns(),
}
CATALOG_KEY_VIEWS: dict[str, str] = {
    "catalog_keys": _catalog_keys(),
    "column_defaults": _column_defaults(),
    "catalog_roles": _roles(),
}

#: Each relation's DDL by its registered name: no relation reads another.
CATALOG_VIEWS: dict[str, str] = {
    catalog_view_name(*relation): _ddl(relation) for relation in sorted(RELATIONS)
}
#: is_schemata lists what pg_namespace does; a view reads no other catalog view by name, since a
#: name pg_catalog also has would resolve there first.
CATALOG_VIEWS["is_schemata"] = _view(
    (_IS, "schemata"),
    _select(
        (_IS, "schemata"),
        {
            "catalog_name": _SERVED_NAME,
            "schema_name": "n.name",
            "schema_owner": "'provisa'",
        },
        "FROM (SELECT schema_name AS name FROM catalog_tables UNION "
        "SELECT name FROM (VALUES ('pg_catalog'), ('information_schema'), ('public')) AS f(name)) "
        "AS n",
    ),
)

assert SCOPED_BY_COLUMN <= set(CATALOG_VIEWS)


__all__ = [
    "CATALOG_BASE_VIEWS",
    "CATALOG_COLUMN_VIEWS",
    "CATALOG_KEY_VIEWS",
    "CATALOG_VIEWS",
]
