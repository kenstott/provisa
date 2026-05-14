# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL catalog proxy.

Intercepts information_schema and pg_catalog queries and answers them
from CompilationContext without a Trino round-trip. Uses DuckDB in-memory
as the query engine so clients can send arbitrary JOINs and WHERE clauses.
"""

from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

_SET_RE = re.compile(r"^\s*SET\b", re.IGNORECASE)
_SHOW_RE = re.compile(r"^\s*SHOW\b", re.IGNORECASE)
_TXN_RE = re.compile(
    r"^\s*(BEGIN|START\s+TRANSACTION|START|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_SCALAR_FN_RE = re.compile(
    r"^\s*SELECT\s+(current_user|session_user|current_database\(\)|current_schema\(\)|version\(\)|pg_backend_pid\(\))\s*$",
    re.IGNORECASE,
)

_INTERCEPT_SCHEMAS = frozenset({"information_schema", "pg_catalog"})

_TABLE_MAP: dict[tuple[str, str], str] = {
    ("information_schema", "schemata"): "_is_schemata",
    ("information_schema", "tables"): "_is_tables",
    ("information_schema", "columns"): "_is_columns",
    ("information_schema", "views"): "_is_views",
    ("pg_catalog", "pg_namespace"): "_pg_namespace",
    ("pg_catalog", "pg_class"): "_pg_class",
    ("pg_catalog", "pg_attribute"): "_pg_attribute",
    ("pg_catalog", "pg_type"): "_pg_type",
    ("pg_catalog", "pg_attrdef"): "_pg_attrdef",
    ("pg_catalog", "pg_description"): "_pg_description",
    ("pg_catalog", "pg_index"): "_pg_index",
    ("pg_catalog", "pg_constraint"): "_pg_constraint",
    ("pg_catalog", "pg_proc"): "_pg_proc",
    ("pg_catalog", "pg_roles"): "_pg_roles",
    ("pg_catalog", "pg_auth_members"): "_pg_auth_members",
    ("pg_catalog", "pg_database"): "_pg_database",
    ("pg_catalog", "pg_settings"): "_pg_settings",
    ("pg_catalog", "pg_tables"): "_pg_tables",
    ("pg_catalog", "pg_stat_user_tables"): "_pg_stat_user_tables",
    ("pg_catalog", "pg_statio_user_tables"): "_pg_stat_user_tables",
    ("pg_catalog", "pg_am"): "_pg_am",
}

_PG_TYPE_ROWS = [
    # (oid, typname, typnamespace, typlen, typtype, typcategory, typnotnull, typbasetype)
    (16, "bool", 11, 1, "b", "B", False, 0),
    (17, "bytea", 11, -1, "b", "U", False, 0),
    (20, "int8", 11, 8, "b", "N", False, 0),
    (21, "int2", 11, 2, "b", "N", False, 0),
    (23, "int4", 11, 4, "b", "N", False, 0),
    (25, "text", 11, -1, "b", "S", False, 0),
    (114, "json", 11, -1, "b", "U", False, 0),
    (700, "float4", 11, 4, "b", "N", False, 0),
    (701, "float8", 11, 8, "b", "N", False, 0),
    (1043, "varchar", 11, -1, "b", "S", False, 0),
    (1082, "date", 11, 4, "b", "D", False, 0),
    (1083, "time", 11, 8, "b", "D", False, 0),
    (1114, "timestamp", 11, 8, "b", "D", False, 0),
    (1184, "timestamptz", 11, 8, "b", "D", False, 0),
    (1700, "numeric", 11, -1, "b", "N", False, 0),
    (3802, "jsonb", 11, -1, "b", "U", False, 0),
    (2950, "uuid", 11, 16, "b", "U", False, 0),
]

_KNOWN_SETTINGS = {
    "server_version": "14.0.provisa",
    "server_version_num": "140000",
    "server_encoding": "UTF8",
    "client_encoding": "UTF8",
    "datestyle": "ISO, MDY",
    "timezone": "UTC",
    "integer_datetimes": "on",
    "standard_conforming_strings": "on",
    "intervalstyle": "postgres",
}


def _trino_to_pg_name(trino_type: str) -> str:
    t = trino_type.lower().split("(")[0].strip()
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


def _trino_to_pg_oid(trino_type: str) -> int:
    t = trino_type.lower().split("(")[0].strip()
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


_SCALAR_NAMES = frozenset(
    {
        "current_user",
        "session_user",
        "current_database",
        "current_schema",
        "version",
        "pg_backend_pid",
    }
)


def classify(sql: str) -> str:
    """Return 'INTERCEPT' or 'PASS_THROUGH'."""
    stripped = sql.strip()
    if _SET_RE.match(stripped) or _SHOW_RE.match(stripped) or _TXN_RE.match(stripped):
        return "INTERCEPT"
    if _SCALAR_FN_RE.match(stripped):
        return "INTERCEPT"
    try:
        import sqlglot.expressions as exp
        import sqlglot

        tree = sqlglot.parse_one(stripped, read="postgres")
        for tbl in tree.find_all(exp.Table):
            db = tbl.db.lower() if tbl.db else ""
            if db in _INTERCEPT_SCHEMAS:
                return "INTERCEPT"
        for func in tree.find_all(exp.Anonymous):
            fn = func.name.lower()
            if "current_setting" in fn:
                return "INTERCEPT"
            if fn in _SCALAR_NAMES:
                return "INTERCEPT"
        for col in tree.find_all(exp.Column):
            if col.name.lower() in _SCALAR_NAMES:
                return "INTERCEPT"
        for node in tree.walk():
            if type(node).__name__ in ("CurrentUser", "CurrentDatabase", "CurrentSchema"):
                return "INTERCEPT"
    except Exception:
        pass
    return "PASS_THROUGH"


def _build_catalog_db(role_id: str, state):
    import duckdb

    db = duckdb.connect(":memory:")
    ctx = state.contexts.get(role_id)
    col_types: dict = state.schema_build_cache.get("column_types", {})

    _TABLE_OID_BASE = 16384
    _oid = _TABLE_OID_BASE

    tables: list[tuple] = []
    all_cols: list[tuple] = []

    if ctx:
        for _field, tm in ctx.tables.items():
            cat = tm.catalog_name or "provisa"
            sch = tm.schema_name or "public"
            tname = tm.table_name
            toid = _oid
            _oid += 1
            tables.append((cat, sch, tname, tm.table_id, toid))
            for i, col in enumerate(col_types.get(tm.table_id, []), 1):
                all_cols.append((toid, col.column_name, col.data_type, col.is_nullable, i))

    _NS: dict[str, int] = {"pg_catalog": 11, "information_schema": 12, "public": 2200}
    toid_map: dict[int, tuple] = {oid: (c, s, t) for c, s, t, tid, oid in tables}

    # information_schema.schemata
    db.execute("""CREATE TABLE _is_schemata (
        catalog_name VARCHAR, schema_name VARCHAR, schema_owner VARCHAR,
        default_character_set_catalog VARCHAR, default_character_set_schema VARCHAR,
        default_character_set_name VARCHAR, sql_path VARCHAR)""")
    seen_schemas: set[tuple] = {(c, s) for c, s, *_ in tables}
    if seen_schemas:
        db.executemany(
            "INSERT INTO _is_schemata VALUES (?,?,'provisa',NULL,NULL,NULL,NULL)",
            list(seen_schemas),
        )

    # information_schema.tables
    db.execute("""CREATE TABLE _is_tables (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, table_type VARCHAR,
        self_referencing_column_name VARCHAR, reference_generation VARCHAR,
        user_defined_type_catalog VARCHAR, user_defined_type_schema VARCHAR,
        user_defined_type_name VARCHAR, is_insertable_into VARCHAR,
        is_typed VARCHAR, commit_action VARCHAR)""")
    if tables:
        db.executemany(
            "INSERT INTO _is_tables VALUES (?,?,?,'BASE TABLE',NULL,NULL,NULL,NULL,NULL,'YES','NO',NULL)",
            [(c, s, t) for c, s, t, tid, oid in tables],
        )

    # information_schema.columns
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
    for toid, col_name, col_type, is_nullable, ordinal in all_cols:
        c, s, t = toid_map.get(toid, ("provisa", "public", ""))
        pg_type = _trino_to_pg_name(col_type)
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

    # information_schema.views (empty)
    db.execute("""CREATE TABLE _is_views (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        view_definition VARCHAR, check_option VARCHAR, is_updatable VARCHAR,
        is_insertable_into VARCHAR, is_trigger_updatable VARCHAR,
        is_trigger_deletable VARCHAR, is_trigger_insertable_into VARCHAR)""")

    # pg_namespace
    db.execute("""CREATE TABLE _pg_namespace (
        oid INTEGER, nspname VARCHAR, nspowner INTEGER, nspacl VARCHAR)""")
    ns_rows = [
        (11, "pg_catalog", 10, None),
        (12, "information_schema", 10, None),
        (2200, "public", 10, None),
    ]
    extra_ns_oid = 2201
    seen_ns: set[str] = {"pg_catalog", "information_schema", "public"}
    for c, s, *_ in tables:
        if s not in seen_ns:
            ns_rows.append((extra_ns_oid, s, 10, None))
            _NS[s] = extra_ns_oid
            extra_ns_oid += 1
            seen_ns.add(s)
    db.executemany("INSERT INTO _pg_namespace VALUES (?,?,?,?)", ns_rows)

    # pg_class
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
    pg_class_rows = []
    for c, s, t, tid, toid in tables:
        ns_oid = _NS.get(s, 2200)
        natts = sum(1 for col in all_cols if col[0] == toid)
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

    # pg_attribute
    db.execute("""CREATE TABLE _pg_attribute (
        attrelid INTEGER, attname VARCHAR, atttypid INTEGER, attstattarget INTEGER,
        attlen SMALLINT, attnum SMALLINT, attndims INTEGER, attcacheoff INTEGER,
        atttypmod INTEGER, attbyval BOOLEAN, attalign VARCHAR, attstorage VARCHAR,
        attnotnull BOOLEAN, atthasdef BOOLEAN, atthasmissing BOOLEAN,
        attidentity VARCHAR, attgenerated VARCHAR, attisdropped BOOLEAN,
        attislocal BOOLEAN, attinhcount INTEGER, attcollation INTEGER,
        attacl VARCHAR, attoptions VARCHAR, attfdwoptions VARCHAR)""")
    attr_rows = []
    for toid, col_name, col_type, is_nullable, ordinal in all_cols:
        pg_oid = _trino_to_pg_oid(col_type)
        attr_rows.append(
            (
                toid,
                col_name,
                pg_oid,
                -1,
                -1,
                ordinal,
                0,
                -1,
                -1,
                False,
                "i",
                "x",
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

    # pg_type
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
                False,
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
                "i",
                "x",
                nn,
                base,
                -1,
                0,
                0,
                None,
                None,
                None,
            )
            for oid, name, ns, ln, tt, cat, nn, base in _PG_TYPE_ROWS
        ],
    )

    # pg_attrdef (empty)
    db.execute(
        "CREATE TABLE _pg_attrdef (oid INTEGER, adrelid INTEGER, adnum SMALLINT, adbin VARCHAR)"
    )

    # pg_description (empty)
    db.execute(
        "CREATE TABLE _pg_description (objoid INTEGER, classoid INTEGER, objsubid INTEGER, description VARCHAR)"
    )

    # pg_index (empty)
    db.execute("""CREATE TABLE _pg_index (
        indexrelid INTEGER, indrelid INTEGER, indnatts SMALLINT, indnkeyatts SMALLINT,
        indisunique BOOLEAN, indisprimary BOOLEAN, indisexclusion BOOLEAN,
        indimmediate BOOLEAN, indisclustered BOOLEAN, indisvalid BOOLEAN,
        indcheckxmin BOOLEAN, indisready BOOLEAN, indislive BOOLEAN,
        indisreplident BOOLEAN, indkey VARCHAR, indcollation VARCHAR,
        indclass VARCHAR, indoption VARCHAR, indexprs VARCHAR, indpred VARCHAR)""")

    # pg_constraint (empty)
    db.execute("""CREATE TABLE _pg_constraint (
        oid INTEGER, conname VARCHAR, connamespace INTEGER, contype VARCHAR,
        condeferrable BOOLEAN, condeferred BOOLEAN, convalidated BOOLEAN,
        conrelid INTEGER, contypid INTEGER, conindid INTEGER, conparentid INTEGER,
        confrelid INTEGER, confupdtype VARCHAR, confdeltype VARCHAR, confmatchtype VARCHAR,
        conislocal BOOLEAN, coninhcount INTEGER, connoinherit BOOLEAN,
        conkeys VARCHAR, confkey VARCHAR, conpfeqop VARCHAR, conppeqop VARCHAR,
        conffeqop VARCHAR, conexclop VARCHAR, conbin VARCHAR)""")

    # pg_proc (empty)
    db.execute("""CREATE TABLE _pg_proc (
        oid INTEGER, proname VARCHAR, pronamespace INTEGER, proowner INTEGER,
        prolang INTEGER, procost REAL, prorows REAL, provariadic INTEGER,
        prosupport VARCHAR, prokind VARCHAR, prosecdef BOOLEAN, proleakproof BOOLEAN,
        proisstrict BOOLEAN, proretset BOOLEAN, provolatile VARCHAR, proparallel VARCHAR,
        pronargs SMALLINT, pronargdefaults SMALLINT, prorettype INTEGER,
        proargtypes VARCHAR, proallargtypes VARCHAR, proargmodes VARCHAR,
        proargnames VARCHAR, proargdefaults VARCHAR, protrftypes VARCHAR,
        prosrc VARCHAR, probin VARCHAR, prosqlbody VARCHAR,
        proconfig VARCHAR, proacl VARCHAR)""")

    # pg_roles
    db.execute("""CREATE TABLE _pg_roles (
        oid INTEGER, rolname VARCHAR, rolsuper BOOLEAN, rolinherit BOOLEAN,
        rolcreaterole BOOLEAN, rolcreatedb BOOLEAN, rolcanlogin BOOLEAN,
        rolreplication BOOLEAN, rolconnlimit INTEGER, rolpassword VARCHAR,
        rolvaliduntil VARCHAR, rolbypassrls BOOLEAN, rolconfig VARCHAR)""")
    db.execute(
        "INSERT INTO _pg_roles VALUES (10,?,FALSE,TRUE,FALSE,FALSE,TRUE,FALSE,-1,NULL,NULL,FALSE,NULL)",
        [role_id],
    )

    # pg_auth_members (empty)
    db.execute(
        "CREATE TABLE _pg_auth_members (roleid INTEGER, member INTEGER, grantor INTEGER, admin_option BOOLEAN)"
    )

    # pg_database
    db.execute("""CREATE TABLE _pg_database (
        oid INTEGER, datname VARCHAR, datdba INTEGER, encoding INTEGER,
        datlocprovider VARCHAR, datistemplate BOOLEAN, datallowconn BOOLEAN,
        datconnlimit INTEGER, datfrozenxid INTEGER, datminmxid INTEGER,
        dattablespace INTEGER, datcollate VARCHAR, datctype VARCHAR, datacl VARCHAR)""")
    db.execute(
        "INSERT INTO _pg_database VALUES (16384,'provisa',10,6,'c',FALSE,TRUE,-1,726,1,1663,'en_US.UTF-8','en_US.UTF-8',NULL)"
    )

    # pg_settings
    db.execute("""CREATE TABLE _pg_settings (
        name VARCHAR, setting VARCHAR, unit VARCHAR, category VARCHAR,
        short_desc VARCHAR, extra_desc VARCHAR, context VARCHAR,
        vartype VARCHAR, source VARCHAR, min_val VARCHAR, max_val VARCHAR,
        enumvals VARCHAR, boot_val VARCHAR, reset_val VARCHAR,
        sourcefile VARCHAR, sourceline INTEGER, pending_restart BOOLEAN)""")
    db.executemany(
        f"INSERT INTO _pg_settings VALUES ({','.join(['?'] * 17)})",
        [
            (
                "server_version",
                "14.0.provisa",
                None,
                "Preset Options",
                "Shows the server version.",
                None,
                "internal",
                "string",
                "default",
                None,
                None,
                None,
                "14.0.provisa",
                "14.0.provisa",
                None,
                None,
                False,
            ),
            (
                "server_version_num",
                "140000",
                None,
                "Preset Options",
                "Shows the server version as an integer.",
                None,
                "internal",
                "integer",
                "default",
                None,
                None,
                None,
                "140000",
                "140000",
                None,
                None,
                False,
            ),
            (
                "server_encoding",
                "UTF8",
                None,
                "Preset Options",
                "Sets the server character set encoding.",
                None,
                "internal",
                "string",
                "default",
                None,
                None,
                None,
                "UTF8",
                "UTF8",
                None,
                None,
                False,
            ),
            (
                "client_encoding",
                "UTF8",
                None,
                "Client Connection Defaults",
                "Sets the client character set encoding.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                "SQL_ASCII",
                "UTF8",
                None,
                None,
                False,
            ),
            (
                "DateStyle",
                "ISO, MDY",
                None,
                "Client Connection Defaults",
                "Sets the display format for date and time values.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                "ISO, MDY",
                "ISO, MDY",
                None,
                None,
                False,
            ),
            (
                "TimeZone",
                "UTC",
                None,
                "Client Connection Defaults",
                "Sets the time zone for displaying and interpreting time stamps.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                "GMT",
                "UTC",
                None,
                None,
                False,
            ),
            (
                "max_connections",
                "100",
                None,
                "Connections and Authentication",
                "Sets the maximum number of concurrent connections.",
                None,
                "postmaster",
                "integer",
                "default",
                "1",
                "262143",
                None,
                "100",
                "100",
                None,
                None,
                False,
            ),
            (
                "standard_conforming_strings",
                "on",
                None,
                "Version and Platform Compatibility",
                "Causes strings to treat backslashes literally.",
                None,
                "user",
                "bool",
                "default",
                None,
                None,
                None,
                "on",
                "on",
                None,
                None,
                False,
            ),
            (
                "integer_datetimes",
                "on",
                None,
                "Preset Options",
                "Datetimes are integer based.",
                None,
                "internal",
                "bool",
                "default",
                None,
                None,
                None,
                "on",
                "on",
                None,
                None,
                False,
            ),
            (
                "IntervalStyle",
                "postgres",
                None,
                "Client Connection Defaults",
                "Sets the display format for interval values.",
                None,
                "user",
                "string",
                "default",
                None,
                None,
                None,
                "postgres",
                "postgres",
                None,
                None,
                False,
            ),
        ],
    )

    # pg_tables
    db.execute("""CREATE TABLE _pg_tables (
        schemaname VARCHAR, tablename VARCHAR, tableowner VARCHAR,
        tablespace VARCHAR, hasindexes BOOLEAN, hasrules BOOLEAN,
        hastriggers BOOLEAN, rowsecurity BOOLEAN)""")
    if tables:
        db.executemany(
            "INSERT INTO _pg_tables VALUES (?,?,'provisa',NULL,FALSE,FALSE,FALSE,FALSE)",
            [(s, t) for c, s, t, tid, oid in tables],
        )

    # pg_am (access methods)
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

    # pg_stat_user_tables (empty stub)
    db.execute("""CREATE TABLE _pg_stat_user_tables (
        relid INTEGER, schemaname VARCHAR, relname VARCHAR,
        seq_scan BIGINT, seq_tup_read BIGINT, idx_scan BIGINT, idx_tup_fetch BIGINT,
        n_tup_ins BIGINT, n_tup_upd BIGINT, n_tup_del BIGINT, n_tup_hot_upd BIGINT,
        n_live_tup BIGINT, n_dead_tup BIGINT, n_mod_since_analyze BIGINT,
        n_ins_since_vacuum BIGINT, last_vacuum VARCHAR, last_autovacuum VARCHAR,
        last_analyze VARCHAR, last_autoanalyze VARCHAR, vacuum_count BIGINT,
        autovacuum_count BIGINT, analyze_count BIGINT, autoanalyze_count BIGINT)""")

    return db


def _rewrite_for_duckdb(sql: str, role_id: str = "") -> str:
    """Rewrite catalog table refs for DuckDB and transpile from postgres dialect."""
    import sqlglot
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql

    def _transform(node):
        if isinstance(node, exp.Table):
            db = node.db.lower() if node.db else ""
            name = node.name.lower() if node.name else ""
            mapped = _TABLE_MAP.get((db, name))
            if mapped:
                new_tbl = exp.Table(this=exp.Identifier(this=mapped, quoted=False))
                if node.alias:
                    new_tbl.set("alias", node.args.get("alias"))
                return new_tbl
        if isinstance(node, exp.Anonymous):
            fn = node.name.lower()
            if "pg_get_expr" in fn or "pg_get_constraintdef" in fn or "pg_get_indexdef" in fn:
                return exp.null()
            if fn in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
            if fn in ("current_database",):
                return exp.Literal.string("provisa")
            if fn == "version":
                return exp.Literal.string("PostgreSQL 14.0 on Provisa")
        if type(node).__name__ == "CurrentUser":
            return exp.Literal.string(role_id)
        if isinstance(node, exp.Column):
            if node.name.lower() in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
        return node

    try:
        rewritten = tree.transform(_transform)
        return rewritten.sql(dialect="duckdb")
    except Exception:
        return sql


def _handle_show(sql: str):
    """Answer SHOW commands without DuckDB."""
    from provisa.executor.trino import QueryResult

    normalized = sql.strip().rstrip(";")
    if re.match(r"^\s*SHOW\s+TRANSACTION\s+ISOLATION\s+LEVEL\s*$", normalized, re.IGNORECASE):
        return QueryResult(rows=[("read committed",)], column_names=["transaction_isolation"])
    parts = normalized.split()
    if len(parts) < 2:
        return QueryResult(rows=[], column_names=[])
    setting = parts[1].lower()
    if setting == "all":
        rows = [(k, v) for k, v in _KNOWN_SETTINGS.items()]
        return QueryResult(rows=rows, column_names=["name", "setting"])
    value = _KNOWN_SETTINGS.get(setting, "")
    return QueryResult(rows=[(value,)], column_names=[setting])


def _handle_scalar(sql: str, role_id: str):
    from provisa.executor.trino import QueryResult

    s = sql.strip().lower()
    if "current_user" in s or "session_user" in s:
        return QueryResult(rows=[(role_id,)], column_names=["current_user"])
    if "current_database" in s:
        return QueryResult(rows=[("provisa",)], column_names=["current_database"])
    if "version()" in s:
        return QueryResult(rows=[("PostgreSQL 14.0 on Provisa",)], column_names=["version"])
    if "current_schema()" in s:
        return QueryResult(rows=[("public",)], column_names=["current_schema"])
    if "pg_backend_pid()" in s:
        return QueryResult(rows=[(0,)], column_names=["pg_backend_pid"])
    return None


def _handle_current_setting(sql: str):
    """Answer SELECT current_setting(...) without DuckDB."""
    from provisa.executor.trino import QueryResult

    m = re.search(r"current_setting\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", sql, re.IGNORECASE)
    if not m:
        return None
    key = m.group(1).lower()
    value = _KNOWN_SETTINGS.get(key, "")
    return QueryResult(rows=[(value,)], column_names=["current_setting"])


def answer(sql: str, role_id: str, state):
    """Return a synthetic QueryResult for intercepted catalog/SET/SHOW queries."""
    from provisa.executor.trino import QueryResult

    stripped = sql.strip().rstrip(";")

    if _TXN_RE.match(stripped) or _SET_RE.match(stripped):
        return QueryResult(rows=[], column_names=[])

    if _SHOW_RE.match(stripped):
        return _handle_show(stripped)

    if _SCALAR_FN_RE.match(stripped):
        result = _handle_scalar(stripped, role_id)
        if result is not None:
            return result

    if "current_setting" in stripped.lower():
        result = _handle_current_setting(stripped)
        if result is not None:
            return result

    db = _build_catalog_db(role_id, state)
    try:
        rewritten = _rewrite_for_duckdb(stripped, role_id)
        log.debug("[CATALOG] rewritten: %s", rewritten[:200])
        cur = db.execute(rewritten)
        rows = [tuple(r) for r in cur.fetchall()]
        col_names = [desc[0] for desc in (cur.description or [])]
        return QueryResult(rows=rows, column_names=col_names)
    except Exception as exc:
        log.warning("[CATALOG] DuckDB error sql=%r: %s", stripped[:100], exc)
        return QueryResult(rows=[], column_names=[])
    finally:
        db.close()
