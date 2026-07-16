# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL catalog DuckDB population.

Builds the in-memory DuckDB catalog snapshot that answers intercepted
information_schema / pg_catalog queries: the CatalogIndex (single source of
truth for OID/attnum/name mappings) plus every _populate_* table builder.
"""

# Requirements: REQ-127, REQ-128, REQ-363

from __future__ import annotations

import logging
import time

from provisa.pgwire.function_catalog import populate_functions
from provisa.pgwire.catalog_columns_data import _SYSTEM_TABLE_COLUMNS
from provisa.pgwire.catalog_data import (
    _TABLE_MAP,
    _PG_OID_ATTR_META,
    _PG_SETTINGS_ROWS,
    _PG_SYSTEM_ROLES,
    pg_type_rows,
)
from provisa.pgwire.ext_surfaces import extension_rows
from provisa.pgwire.system_tables import _populate_empty_system_tables
from provisa.pgwire.catalog_constraints import (
    _populate_pg_constraint,
    _populate_pg_index,
    _populate_is_constraints,
)

log = logging.getLogger(__name__)


# Stable OID assignments for system objects surfaced in pg_class/pg_attribute.
# 8001+ for pg_catalog tables, 9001+ for information_schema views.
_PG_CAT_TABLE_NAMES: list[str] = sorted(k[1] for k in _TABLE_MAP if k[0] == "pg_catalog")
_PG_CAT_TABLE_OIDS: dict[str, int] = {n: 8001 + i for i, n in enumerate(_PG_CAT_TABLE_NAMES)}

_IS_VIEW_NAMES: list[str] = [
    "schemata",
    "tables",
    "columns",
    "views",
    "key_column_usage",
    "table_constraints",
    "referential_constraints",
    "role_table_grants",
    "role_column_grants",
    "triggers",
    "sequences",
    "routines",
    "parameters",
    "enabled_roles",
    "applicable_roles",
]
_IS_VIEW_OIDS: dict[str, int] = {n: 9001 + i for i, n in enumerate(_IS_VIEW_NAMES)}

# Column definitions sourced from the live PostgreSQL 16 instance (information_schema.columns).
# Keys: information_schema view name OR pg_catalog table name.
# Values: ordered list of (column_name, pg_data_type).


def _physical_to_pg_name(column_type: str) -> str:
    t = column_type.lower().split("(")[0].strip()
    return {
        "varchar": "character varying",
        "char": "character",
        "integer": "integer",
        "int": "integer",
        "bigint": "bigint",
        "smallint": "smallint",
        "boolean": "boolean",
        "double": "double precision",
        "real": "real",
        "date": "date",
        "time": "time without time zone",
        "timestamp": "timestamp without time zone",
        "timestamp with time zone": "timestamp with time zone",
        "decimal": "numeric",
        "json": "jsonb",
        "row": "jsonb",
        "array": "ARRAY",
        "varbinary": "bytea",
        "uuid": "uuid",
    }.get(t, "text")


def _physical_to_pg_oid(column_type: str) -> int:
    t = column_type.lower().split("(")[0].strip()
    return {
        "varchar": 1043,
        "char": 18,
        "integer": 23,
        "int": 23,
        "bigint": 20,
        "smallint": 21,
        "boolean": 16,
        "double": 701,
        "real": 700,
        "date": 1082,
        "time": 1083,
        "timestamp": 1114,
        "timestamp with time zone": 1184,
        "decimal": 1700,
        "json": 3802,
        "jsonb": 3802,
        "row": 3802,
        "array": 2277,
        "varbinary": 17,
        "uuid": 2950,
    }.get(t, 25)


class CatalogIndex:  # REQ-532
    """Single source of truth for all OID/attnum/name mappings used by catalog populate functions."""

    __slots__ = (
        "tables",
        "all_cols",
        "table_id_to_oid",
        "toid_to_table",
        "col_attnum",
        "attnum_to_col",
        "ns_map",
    )

    def __init__(self) -> None:
        self.tables: list[tuple] = []
        self.all_cols: list[tuple] = []
        self.table_id_to_oid: dict[int, int] = {}
        self.toid_to_table: dict[int, tuple] = {}
        self.col_attnum: dict[tuple[int, str], int] = {}
        self.attnum_to_col: dict[tuple[int, int], str] = {}
        self.ns_map: dict[str, int] = {"pg_catalog": 11, "information_schema": 12, "public": 2200}


def _build_catalog_index(ctx, col_types: dict) -> CatalogIndex:  # REQ-128, REQ-363
    """Build the CatalogIndex once. All populate functions read from it — nothing recomputes."""
    from provisa.compiler.naming import domain_to_sql_name, apply_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    idx = CatalogIndex()
    if not ctx:
        return idx

    # Assign stable namespace OIDs — sort extra schemas so OIDs don't depend on iteration order.
    extra_schemas: set[str] = set()
    for tm in ctx.tables.values():
        raw = tm.domain_id or tm.schema_name or "public"
        sch = domain_to_sql_name(raw)
        if sch not in idx.ns_map:
            extra_schemas.add(sch)
    _ns_extra = 2201
    for sch in sorted(extra_schemas):
        idx.ns_map[sch] = _ns_extra
        _ns_extra += 1

    seen_table_ids: set[int] = set()

    for tm in ctx.tables.values():
        if tm.table_id in seen_table_ids:
            continue
        seen_table_ids.add(tm.table_id)
        cat = "provisa"
        raw_schema = tm.domain_id or tm.schema_name or "public"
        sch = domain_to_sql_name(raw_schema)
        tname = semantic_table_name(tm)
        toid = 16384 + tm.table_id

        idx.tables.append((cat, sch, tname, tm.table_id, toid))
        idx.table_id_to_oid[tm.table_id] = toid
        idx.toid_to_table[toid] = (cat, sch, tname)

        _p2s_raw = getattr(ctx, "physical_to_sql", None)
        _p2s: dict = _p2s_raw if isinstance(_p2s_raw, dict) else {}
        real_cols = col_types.get(tm.table_id, [])
        for i, col in enumerate(real_cols, 1):
            phys = col.column_name
            # Skip columns not visible to this role (absent from physical_to_sql).
            if _p2s and (tm.table_id, phys) not in _p2s:
                continue
            exposed = _p2s.get((tm.table_id, phys)) or apply_sql_name(phys)
            idx.all_cols.append((toid, exposed, col.data_type, col.is_nullable, i))
            idx.col_attnum[(toid, exposed)] = i
            idx.attnum_to_col[(toid, i)] = exposed

        virtual = getattr(ctx, "virtual_columns", {}).get(tm.table_id, {})
        for j, vcol in enumerate(virtual, len(real_cols) + 1):
            idx.all_cols.append((toid, vcol, "varchar", True, j))
            idx.col_attnum[(toid, vcol)] = j
            idx.attnum_to_col[(toid, j)] = vcol

    return idx


def _populate_is_schemata(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_schemata (
        catalog_name VARCHAR, schema_name VARCHAR, schema_owner VARCHAR,
        default_character_set_catalog VARCHAR, default_character_set_schema VARCHAR,
        default_character_set_name VARCHAR, sql_path VARCHAR)""")
    seen_schemas: set[tuple] = {(c, s) for c, s, *_ in idx.tables}
    # Always include schemas from the namespace map (public, information_schema, pg_catalog).
    for ns in idx.ns_map:
        seen_schemas.add(("provisa", ns))
    if seen_schemas:
        db.executemany(
            "INSERT INTO _is_schemata VALUES (?,?,'provisa',NULL,NULL,NULL,NULL)",
            list(seen_schemas),
        )


def _populate_is_tables(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_tables (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, table_type VARCHAR,
        self_referencing_column_name VARCHAR, reference_generation VARCHAR,
        user_defined_type_catalog VARCHAR, user_defined_type_schema VARCHAR,
        user_defined_type_name VARCHAR, is_insertable_into VARCHAR,
        is_typed VARCHAR, commit_action VARCHAR)""")
    if idx.tables:
        db.executemany(
            "INSERT INTO _is_tables VALUES (?,?,?,'BASE TABLE',NULL,NULL,NULL,NULL,NULL,'YES','NO',NULL)",
            [(row[0], row[1], row[2]) for row in idx.tables],
        )


def _populate_is_columns(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_columns (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        column_name VARCHAR, ordinal_position INTEGER, column_default VARCHAR,
        is_nullable VARCHAR, data_type VARCHAR,
        character_maximum_length INTEGER, character_octet_length INTEGER,
        numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER,
        datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER,
        character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR,
        collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR,
        domain_catalog VARCHAR, domain_schema VARCHAR, domain_name VARCHAR,
        udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR,
        scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR,
        maximum_cardinality INTEGER, dtd_identifier VARCHAR,
        is_self_referencing VARCHAR, is_identity VARCHAR, identity_generation VARCHAR,
        identity_start VARCHAR, identity_increment VARCHAR, identity_maximum VARCHAR,
        identity_minimum VARCHAR, identity_cycle VARCHAR, is_generated VARCHAR,
        generation_expression VARCHAR, is_updatable VARCHAR)""")
    col_rows = []
    for toid, col_name, col_type, is_nullable, ordinal in idx.all_cols:
        c, s, t = idx.toid_to_table.get(toid, ("provisa", "public", ""))
        pg_type = _physical_to_pg_name(col_type)
        null_str = "YES" if is_nullable else "NO"
        col_rows.append(
            (
                c,
                s,
                t,
                col_name,
                ordinal,
                None,
                null_str,
                pg_type,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                pg_type,
                None,
                None,
                None,
                None,
                str(ordinal),
                "NO",
                "NO",
                None,
                None,
                None,
                None,
                None,
                "NO",
                "NEVER",
                None,
                "YES",
            )
        )
    if col_rows:
        db.executemany(f"INSERT INTO _is_columns VALUES ({','.join(['?'] * 44)})", col_rows)


def _populate_pg_namespace(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _pg_namespace (
        oid INTEGER, nspname VARCHAR, nspowner INTEGER, nspacl VARCHAR)""")
    ns_rows = [(oid, name, 10, None) for name, oid in idx.ns_map.items()]
    db.executemany("INSERT INTO _pg_namespace VALUES (?,?,?,?)", ns_rows)


def _populate_pg_class(db, idx: CatalogIndex, row_counts: dict[int, float] | None = None) -> None:
    db.execute("""CREATE TABLE _pg_class (
        oid INTEGER, relname VARCHAR, relnamespace INTEGER, reltype INTEGER,
        reloftype INTEGER, relowner INTEGER, relam INTEGER, relfilenode INTEGER,
        reltablespace INTEGER, relpages INTEGER, reltuples REAL, relallvisible INTEGER,
        reltoastrelid INTEGER, relhasindex BOOLEAN, relisshared BOOLEAN,
        relpersistence VARCHAR, relkind VARCHAR, relnatts INTEGER, relchecks SMALLINT,
        relhasrules BOOLEAN, relhastriggers BOOLEAN, relhassubclass BOOLEAN,
        relrowsecurity BOOLEAN, relforcerowsecurity BOOLEAN, relispopulated BOOLEAN,
        relreplident VARCHAR, relispartition BOOLEAN, relrewrite INTEGER,
        relfrozenxid INTEGER, relminmxid INTEGER, relacl VARCHAR,
        reloptions VARCHAR, relpartbound VARCHAR)""")
    natts_by_toid: dict[int, int] = {}
    for col in idx.all_cols:
        natts_by_toid[col[0]] = natts_by_toid.get(col[0], 0) + 1
    pg_class_rows = []
    for _, s, t, _, toid in idx.tables:
        ns_oid = idx.ns_map.get(s, 2200)
        natts = natts_by_toid.get(toid, 0)
        reltuples = float(row_counts.get(toid, 0.0)) if row_counts else 0.0
        pg_class_rows.append(
            (
                toid,
                t,
                ns_oid,
                toid + 100000,
                0,
                10,
                0,
                toid,
                0,
                0,
                reltuples,
                0,
                0,
                False,
                False,
                "p",
                "r",
                natts,
                0,
                False,
                False,
                False,
                False,
                False,
                True,
                "d",
                False,
                0,
                0,
                0,
                None,
                None,
                None,
            )
        )
    for vname, oid in _IS_VIEW_OIDS.items():
        natts = len(_SYSTEM_TABLE_COLUMNS.get(vname, []))
        pg_class_rows.append(
            (
                oid,
                vname,
                12,
                oid + 100000,
                0,
                10,
                0,
                oid,
                0,
                0,
                0.0,
                0,
                0,
                False,
                False,
                "p",
                "v",
                natts,
                0,
                False,
                False,
                False,
                False,
                False,
                True,
                "d",
                False,
                0,
                0,
                0,
                None,
                None,
                None,
            )
        )
    for tname, oid in _PG_CAT_TABLE_OIDS.items():
        natts = len(_SYSTEM_TABLE_COLUMNS.get(tname, []))
        pg_class_rows.append(
            (
                oid,
                tname,
                11,
                oid + 100000,
                0,
                10,
                0,
                oid,
                0,
                0,
                0.0,
                0,
                0,
                False,
                False,
                "p",
                "r",
                natts,
                0,
                False,
                False,
                False,
                False,
                False,
                True,
                "d",
                False,
                0,
                0,
                0,
                None,
                None,
                None,
            )
        )
    if pg_class_rows:
        db.executemany(f"INSERT INTO _pg_class VALUES ({','.join(['?'] * 33)})", pg_class_rows)


def _populate_pg_description(
    db, idx: CatalogIndex, raw_tables: list, raw_domains: list | None = None
) -> None:
    from provisa.compiler.naming import domain_to_sql_name

    tid_desc: dict[int, str] = {}
    tid_col_desc: dict[int, dict[str, str]] = {}
    for rt in raw_tables:
        _tid = rt["id"] if isinstance(rt, dict) else getattr(rt, "id", None)
        _tdesc = rt.get("description") if isinstance(rt, dict) else getattr(rt, "description", None)
        _cols = rt["columns"] if isinstance(rt, dict) else getattr(rt, "columns", [])
        if _tid is None:
            continue
        if _tdesc:
            tid_desc[_tid] = _tdesc
        cdesc: dict[str, str] = {}
        for col in _cols:
            _cname = col["column_name"] if isinstance(col, dict) else getattr(col, "name", "")
            _cdesc = (
                col.get("description")
                if isinstance(col, dict)
                else getattr(col, "description", None)
            )
            if _cdesc:
                cdesc[_cname] = _cdesc
        if cdesc:
            tid_col_desc[_tid] = cdesc

    desc_rows: list[tuple] = []

    # Namespace (schema/domain) descriptions
    for dom in raw_domains or []:
        _did = dom["id"] if isinstance(dom, dict) else getattr(dom, "id", None)
        _ddesc = (
            dom.get("description") if isinstance(dom, dict) else getattr(dom, "description", None)
        )
        if not _did or not _ddesc:
            continue
        ns_oid = idx.ns_map.get(domain_to_sql_name(_did))
        if ns_oid is not None:
            desc_rows.append((ns_oid, "pg_namespace", 0, _ddesc))

    for _, _, _, table_id, toid in idx.tables:
        tdesc = tid_desc.get(table_id)
        if tdesc:
            desc_rows.append((toid, "pg_class", 0, tdesc))
        for cname, cdesc_val in (tid_col_desc.get(table_id) or {}).items():
            attnum = idx.col_attnum.get((toid, cname))
            if attnum is not None:
                desc_rows.append((toid, "pg_class", attnum, cdesc_val))

    if desc_rows:
        db.executemany("INSERT INTO _pg_description VALUES (?,?,?,?)", desc_rows)


def _populate_pg_attribute(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _pg_attribute (
        attrelid INTEGER, attname VARCHAR, atttypid INTEGER, attstattarget INTEGER,
        attlen SMALLINT, attnum SMALLINT, attndims INTEGER, attcacheoff INTEGER,
        atttypmod INTEGER, attbyval BOOLEAN, attalign VARCHAR, attstorage VARCHAR,
        attnotnull BOOLEAN, atthasdef BOOLEAN, atthasmissing BOOLEAN,
        attidentity VARCHAR, attgenerated VARCHAR, attisdropped BOOLEAN,
        attislocal BOOLEAN, attinhcount INTEGER, attcollation INTEGER,
        attacl VARCHAR, attoptions VARCHAR, attfdwoptions VARCHAR)""")
    attr_rows = []
    for toid, col_name, col_type, is_nullable, ordinal in idx.all_cols:
        pg_oid = _physical_to_pg_oid(col_type)
        attlen, attbyval, attalign, attstorage = _PG_OID_ATTR_META.get(
            pg_oid, (-1, False, "i", "x")
        )
        attr_rows.append(
            (
                toid,
                col_name,
                pg_oid,
                -1,
                attlen,
                ordinal,
                0,
                -1,
                -1,
                attbyval,
                attalign,
                attstorage,
                not is_nullable,
                False,
                False,
                "",
                "",
                False,
                True,
                0,
                0,
                None,
                None,
                None,
            )
        )
    if attr_rows:
        db.executemany(f"INSERT INTO _pg_attribute VALUES ({','.join(['?'] * 24)})", attr_rows)


def _populate_system_attributes(db) -> None:
    """Add pg_attribute rows for pg_catalog and information_schema system objects."""
    _type_to_oid = {
        "varchar": 1043,
        "name": 25,
        "text": 25,
        "oid": 23,
        "integer": 23,
        "smallint": 21,
        "bigint": 20,
        "boolean": 16,
        "real": 700,
        "double": 701,
        "double precision": 701,
        "xid": 23,
        "array": 25,
        "char": 18,
        "timestamp with time zone": 1184,
        "timestamp": 1114,
    }
    attr_rows = []
    for is_name, oid in _IS_VIEW_OIDS.items():
        for attnum, (col_name, col_type) in enumerate(_SYSTEM_TABLE_COLUMNS.get(is_name, []), 1):
            pg_oid = _type_to_oid.get(col_type.lower().split("(")[0].strip(), 25)
            attlen, attbyval, attalign, attstorage = _PG_OID_ATTR_META.get(
                pg_oid, (-1, False, "i", "x")
            )
            attr_rows.append(
                (
                    oid,
                    col_name,
                    pg_oid,
                    -1,
                    attlen,
                    attnum,
                    0,
                    -1,
                    -1,
                    attbyval,
                    attalign,
                    attstorage,
                    False,
                    False,
                    False,
                    "",
                    "",
                    False,
                    True,
                    0,
                    0,
                    None,
                    None,
                    None,
                )
            )
    for pg_name, oid in _PG_CAT_TABLE_OIDS.items():
        for attnum, (col_name, col_type) in enumerate(_SYSTEM_TABLE_COLUMNS.get(pg_name, []), 1):
            pg_oid = _type_to_oid.get(col_type.lower().split("(")[0].strip(), 25)
            attlen, attbyval, attalign, attstorage = _PG_OID_ATTR_META.get(
                pg_oid, (-1, False, "i", "x")
            )
            attr_rows.append(
                (
                    oid,
                    col_name,
                    pg_oid,
                    -1,
                    attlen,
                    attnum,
                    0,
                    -1,
                    -1,
                    attbyval,
                    attalign,
                    attstorage,
                    False,
                    False,
                    False,
                    "",
                    "",
                    False,
                    True,
                    0,
                    0,
                    None,
                    None,
                    None,
                )
            )
    if attr_rows:
        db.executemany(f"INSERT INTO _pg_attribute VALUES ({','.join(['?'] * 24)})", attr_rows)


def _populate_pg_type(db) -> None:
    db.execute("""CREATE TABLE _pg_type (
        oid INTEGER, typname VARCHAR, typnamespace INTEGER, typowner INTEGER,
        typlen SMALLINT, typbyval BOOLEAN, typtype VARCHAR, typcategory VARCHAR,
        typispreferred BOOLEAN, typisdefined BOOLEAN, typdelim VARCHAR,
        typrelid INTEGER, typelem INTEGER, typarray INTEGER,
        typinput VARCHAR, typoutput VARCHAR, typreceive VARCHAR, typsend VARCHAR,
        typmodin VARCHAR, typmodout VARCHAR, typanalyze VARCHAR,
        typalign VARCHAR, typstorage VARCHAR, typnotnull BOOLEAN,
        typbasetype INTEGER, typtypmod INTEGER, typndims INTEGER, typcollation INTEGER,
        typdefaultbin VARCHAR, typdefault VARCHAR, typacl VARCHAR)""")
    db.executemany(
        f"INSERT INTO _pg_type VALUES ({','.join(['?'] * 31)})",
        [
            (
                oid,
                name,
                ns,
                10,
                ln,
                byval,
                tt,
                cat,
                False,
                True,
                ",",
                0,
                0,
                0,
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                align,
                storage,
                nn,
                base,
                -1,
                0,
                0,
                None,
                None,
                None,
            )
            for oid, name, ns, ln, tt, cat, nn, base, byval, align, storage in pg_type_rows()
        ],
    )


def _populate_pg_extension(db) -> None:
    """Advertise every enabled extension surface in _pg_extension (REQ-892)."""
    rows = extension_rows()
    if rows:
        db.executemany("INSERT INTO _pg_extension VALUES (?,?,?,?,?,?,?,?)", rows)


def _populate_pg_roles_and_database(db, role_id: str, state=None) -> None:
    db.execute("""CREATE TABLE _pg_roles (
        oid INTEGER, rolname VARCHAR, rolsuper BOOLEAN, rolinherit BOOLEAN,
        rolcreaterole BOOLEAN, rolcreatedb BOOLEAN, rolcanlogin BOOLEAN,
        rolreplication BOOLEAN, rolconnlimit INTEGER, rolpassword VARCHAR,
        rolvaliduntil VARCHAR, rolbypassrls BOOLEAN, rolconfig VARCHAR)""")

    rows: list[tuple] = []
    seen_names: set[str] = set()
    # Provisa roles from state (all defined roles, not just the connected one)
    _roles_attr = getattr(state, "roles", None)
    provisa_roles = list(_roles_attr.values()) if isinstance(_roles_attr, dict) else []
    for i, role in enumerate(provisa_roles):
        rname = role["id"] if isinstance(role, dict) else getattr(role, "id", None)
        if not rname or rname in seen_names:
            continue
        seen_names.add(rname)
        roid = 10 + i
        rows.append(
            (roid, rname, False, True, False, False, True, False, -1, None, None, False, None)
        )
    # Ensure the connected role is present even if state.roles is empty
    if role_id not in seen_names:
        rows.append(
            (10, role_id, False, True, False, False, True, False, -1, None, None, False, None)
        )
        seen_names.add(role_id)
    # Standard PG system roles
    for oid, name, sup, inh, crrole, crdb, login, repl, conn, byp in _PG_SYSTEM_ROLES:
        if name not in seen_names:
            rows.append(
                (oid, name, sup, inh, crrole, crdb, login, repl, conn, None, None, byp, None)
            )
            seen_names.add(name)

    db.executemany("INSERT INTO _pg_roles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    db.execute("""CREATE TABLE _pg_user AS
        SELECT oid AS usesysid, rolname AS usename,
               rolcreatedb AS usecreatedb, rolsuper AS usesuper,
               rolreplication AS userepl, rolbypassrls AS usebypassrls,
               '********' AS passwd, rolvaliduntil AS valuntil,
               rolconfig AS useconfig
        FROM _pg_roles WHERE rolcanlogin""")
    db.execute("""CREATE TABLE _pg_database (
        oid INTEGER, datname VARCHAR, datdba INTEGER, encoding INTEGER,
        datlocprovider VARCHAR, datistemplate BOOLEAN, datallowconn BOOLEAN,
        datconnlimit INTEGER, datfrozenxid INTEGER, datminmxid INTEGER,
        dattablespace INTEGER, datcollate VARCHAR, datctype VARCHAR, datacl VARCHAR)""")
    db.execute(
        "INSERT INTO _pg_database VALUES (16384,'provisa',10,6,'c',FALSE,TRUE,-1,726,1,1663,'en_US.UTF-8','en_US.UTF-8',NULL)"
    )


def _populate_pg_settings(db) -> None:
    db.execute("""CREATE TABLE _pg_settings (
        name VARCHAR, setting VARCHAR, unit VARCHAR, category VARCHAR,
        short_desc VARCHAR, extra_desc VARCHAR, context VARCHAR,
        vartype VARCHAR, source VARCHAR, min_val VARCHAR, max_val VARCHAR,
        enumvals VARCHAR, boot_val VARCHAR, reset_val VARCHAR,
        sourcefile VARCHAR, sourceline INTEGER, pending_restart BOOLEAN)""")
    db.executemany(
        f"INSERT INTO _pg_settings VALUES ({','.join(['?'] * 17)})",
        _PG_SETTINGS_ROWS,
    )


def _populate_pg_tables_and_am(db, idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _pg_tables (
        schemaname VARCHAR, tablename VARCHAR, tableowner VARCHAR,
        tablespace VARCHAR, hasindexes BOOLEAN, hasrules BOOLEAN,
        hastriggers BOOLEAN, rowsecurity BOOLEAN)""")
    if idx.tables:
        db.executemany(
            "INSERT INTO _pg_tables VALUES (?,?,'provisa',NULL,FALSE,FALSE,FALSE,FALSE)",
            [(row[1], row[2]) for row in idx.tables],
        )
    db.execute("""CREATE TABLE _pg_am (
        oid INTEGER, amname VARCHAR, amhandler VARCHAR, amtype VARCHAR)""")
    db.executemany(
        "INSERT INTO _pg_am VALUES (?,?,?,?)",
        [
            (2, "heap", "heap_tableam_handler", "t"),
            (403, "btree", "bthandler", "i"),
            (405, "hash", "hashhandler", "i"),
            (783, "gist", "gisthandler", "i"),
            (2742, "gin", "ginhandler", "i"),
            (4000, "spgist", "spghandler", "i"),
            (3580, "brin", "brinhandler", "i"),
        ],
    )


_row_count_cache: dict[str, tuple[float, dict[int, float]]] = {}
_ROW_COUNT_TTL = 300.0


def _fetch_row_counts(ctx, idx: CatalogIndex, engine_conn) -> dict[int, float]:
    """Fetch row count estimates via SHOW STATS FOR. Returns {toid: row_count}."""
    if ctx is None or engine_conn is None:
        return {}
    table_id_to_meta: dict[int, tuple[str, str, str]] = {
        tm.table_id: (tm.catalog_name, tm.schema_name, tm.table_name) for tm in ctx.tables.values()
    }
    result: dict[int, float] = {}
    for _, _, _, table_id, toid in idx.tables:
        ref = table_id_to_meta.get(table_id)
        if not ref:
            continue
        cat, sch, tname = ref
        try:
            cur = engine_conn.cursor()
            cur.execute(f'SHOW STATS FOR "{cat}"."{sch}"."{tname}"')
            for row in cur.fetchall():
                if row[0] is None and row[4] is not None:
                    result[toid] = float(row[4])
                    break
        except Exception:  # complexity-gate: allow-ble=1 reason=row-count estimates are best-effort; any engine error (missing table, stats unavailable) leaves the toid absent and reltuples defaults to 0
            pass
    return result


def _build_catalog_db(role_id: str, state):  # REQ-127, REQ-128, REQ-363
    import duckdb

    db = duckdb.connect(":memory:")
    db.execute("CREATE MACRO pg_backend_pid() AS 0")
    db.execute("CREATE MACRO age(x) AS 0")
    db.execute("CREATE MACRO quote_ident(x) AS '\"' || replace(x, '\"', '\"\"') || '\"'")
    db.execute("""CREATE MACRO pg_available_extensions() AS TABLE
        SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS VARCHAR) AS default_version,
               CAST(NULL AS VARCHAR) AS installed_version, CAST(NULL AS VARCHAR) AS comment
        LIMIT 0""")
    db.execute("""CREATE MACRO pg_available_extension_versions() AS TABLE
        SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS VARCHAR) AS version,
               FALSE AS installed, FALSE AS superuser, FALSE AS trusted,
               FALSE AS relocatable, CAST(NULL AS VARCHAR) AS schema,
               CAST(NULL AS VARCHAR[]) AS requires, CAST(NULL AS VARCHAR) AS comment
        LIMIT 0""")
    ctx = state.contexts.get(role_id)
    col_types: dict = state.schema_build_cache.get("column_types", {})
    idx = _build_catalog_index(ctx, col_types)

    now = time.monotonic()
    cached = _row_count_cache.get(role_id)
    if cached and now - cached[0] < _ROW_COUNT_TTL:
        row_counts = cached[1]
    else:
        row_counts = _fetch_row_counts(ctx, idx, getattr(state, "engine_conn", None))
        _row_count_cache[role_id] = (now, row_counts)

    _populate_is_schemata(db, idx)
    _populate_is_tables(db, idx)
    _populate_is_columns(db, idx)
    _populate_pg_namespace(db, idx)
    _populate_pg_class(db, idx, row_counts)
    _populate_pg_attribute(db, idx)
    _populate_system_attributes(db)
    _populate_pg_type(db)
    _populate_empty_system_tables(db)
    _populate_pg_extension(db)  # REQ-892: advertise enabled extension surfaces
    populate_functions(db, state, role_id)  # REQ-872
    raw_tables = state.schema_build_cache.get("tables", []) if state else []
    raw_domains = state.schema_build_cache.get("domains", []) if state else []
    _populate_pg_description(db, idx, raw_tables, raw_domains)
    constraint_rows = _populate_pg_constraint(db, ctx, idx)
    _populate_pg_index(db, constraint_rows)
    _populate_pg_roles_and_database(db, role_id, state)
    _populate_pg_settings(db)
    _populate_pg_tables_and_am(db, idx)
    _populate_is_constraints(db, constraint_rows, idx)

    return db
