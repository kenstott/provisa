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
import time

log = logging.getLogger(__name__)

_SET_RE = re.compile(r"^\s*SET\b", re.IGNORECASE)
_SHOW_RE = re.compile(r"^\s*SHOW\b", re.IGNORECASE)
_TXN_RE = re.compile(
    r"^\s*(BEGIN|START\s+TRANSACTION|START|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_SCALAR_FN_RE = re.compile(
    r"^\s*SELECT\s+(?:pg_catalog\.)?(current_user|session_user|current_database\(\)|current_schema\(\)|version\(\)|pg_backend_pid\(\))\s*$",
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
    ("pg_catalog", "pg_tablespace"): "_pg_tablespace",
    ("pg_catalog", "pg_conversion"): "_pg_conversion",
    ("pg_catalog", "pg_shdescription"): "_pg_shdescription",
    ("pg_catalog", "pg_extension"): "_pg_extension",
    ("pg_catalog", "pg_enum"): "_pg_enum",
    ("pg_catalog", "pg_stat_activity"): "_pg_stat_activity",
    ("pg_catalog", "pg_trigger"): "_pg_trigger",
    ("pg_catalog", "pg_inherits"): "_pg_inherits",
    ("pg_catalog", "pg_rewrite"): "_pg_rewrite",
    ("pg_catalog", "pg_depend"): "_pg_depend",
    ("pg_catalog", "pg_shdepend"): "_pg_shdepend",
    ("pg_catalog", "pg_aggregate"): "_pg_aggregate",
    ("pg_catalog", "pg_language"): "_pg_language",
    ("pg_catalog", "pg_operator"): "_pg_operator",
    ("pg_catalog", "pg_cast"): "_pg_cast",
    ("pg_catalog", "pg_collation"): "_pg_collation",
    ("pg_catalog", "pg_range"): "_pg_range",
    ("pg_catalog", "pg_foreign_table"): "_pg_foreign_table",
    ("pg_catalog", "pg_foreign_server"): "_pg_foreign_server",
    ("pg_catalog", "pg_user_mapping"): "_pg_user_mapping",
    ("pg_catalog", "pg_foreign_data_wrapper"): "_pg_foreign_data_wrapper",
    ("pg_catalog", "pg_sequence"): "_pg_sequence",
    ("pg_catalog", "pg_policy"): "_pg_policy",
    ("pg_catalog", "pg_partitioned_table"): "_pg_partitioned_table",
    ("pg_catalog", "pg_publication"): "_pg_publication",
    ("pg_catalog", "pg_subscription"): "_pg_subscription",
    ("pg_catalog", "pg_event_trigger"): "_pg_event_trigger",
    ("pg_catalog", "pg_stat_user_indexes"): "_pg_stat_user_indexes",
    ("pg_catalog", "pg_locks"): "_pg_locks",
    ("information_schema", "key_column_usage"): "_is_key_column_usage",
    ("information_schema", "table_constraints"): "_is_table_constraints",
    ("information_schema", "referential_constraints"): "_is_referential_constraints",
    ("information_schema", "role_table_grants"): "_is_role_table_grants",
    ("information_schema", "role_column_grants"): "_is_role_column_grants",
    ("information_schema", "triggers"): "_is_triggers",
    ("information_schema", "sequences"): "_is_sequences",
    ("information_schema", "routines"): "_is_routines",
    ("information_schema", "parameters"): "_is_parameters",
    ("information_schema", "enabled_roles"): "_is_enabled_roles",
    ("information_schema", "applicable_roles"): "_is_applicable_roles",
}

_CATALOG_TABLE_NAMES = frozenset(t for _, t in _TABLE_MAP)

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
    "search_path": '"$user", public',
    "extra_float_digits": "0",
    "application_name": "",
    "is_superuser": "on",
    "session_authorization": "admin",
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
            tname = tbl.name.lower() if tbl.name else ""
            if db in _INTERCEPT_SCHEMAS:
                return "INTERCEPT"
            if not db and tname in _CATALOG_TABLE_NAMES:
                return "INTERCEPT"
        for func in tree.find_all(exp.Anonymous):
            fn = func.name.lower()
            if "current_setting" in fn:
                return "INTERCEPT"
            if fn in _SCALAR_NAMES:
                return "INTERCEPT"
            if any(x in fn for x in ("obj_description", "col_description", "shobj_description", "pg_get_expr", "pg_stat_get")):
                return "INTERCEPT"
        for col in tree.find_all(exp.Column):
            if col.name.lower() in _SCALAR_NAMES:
                return "INTERCEPT"
        for node in tree.walk():
            if type(node).__name__ in ("CurrentUser", "CurrentDatabase", "CurrentSchema"):
                return "INTERCEPT"
    except Exception:
        lower = stripped.lower()
        for name in _CATALOG_TABLE_NAMES:
            if re.search(r"\b" + re.escape(name) + r"\b", lower):
                return "INTERCEPT"
        for schema in _INTERCEPT_SCHEMAS:
            if schema in lower:
                return "INTERCEPT"
    return "PASS_THROUGH"


class CatalogIndex:
    """Single source of truth for all OID/attnum/name mappings used by catalog populate functions."""

    __slots__ = (
        "tables", "all_cols",
        "table_id_to_oid", "toid_to_table",
        "col_attnum", "attnum_to_col",
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


def _build_catalog_index(ctx, col_types: dict) -> CatalogIndex:
    """Build the CatalogIndex once. All populate functions read from it — nothing recomputes."""
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_gen import semantic_table_name

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

        real_cols = col_types.get(tm.table_id, [])
        for i, col in enumerate(real_cols, 1):
            idx.all_cols.append((toid, col.column_name, col.data_type, col.is_nullable, i))
            idx.col_attnum[(toid, col.column_name)] = i
            idx.attnum_to_col[(toid, i)] = col.column_name

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
        pg_type = _trino_to_pg_name(col_type)
        null_str = "YES" if is_nullable else "NO"
        col_rows.append(
            (
                c, s, t, col_name, ordinal, None, null_str, pg_type,
                None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None,
                None, None, pg_type, None, None, None, None, str(ordinal),
                "NO", "NO", None, None, None, None, None, "NO", "NEVER", None, "YES",
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
    for _c, s, t, _, toid in idx.tables:
        ns_oid = idx.ns_map.get(s, 2200)
        natts = natts_by_toid.get(toid, 0)
        reltuples = float(row_counts.get(toid, 0.0)) if row_counts else 0.0
        pg_class_rows.append(
            (
                toid, t, ns_oid, toid + 100000, 0, 10, 0, toid, 0, 0, reltuples, 0, 0,
                False, False, "p", "r", natts, 0, False, False, False, False, False,
                True, "d", False, 0, 0, 0, None, None, None,
            )
        )
    if pg_class_rows:
        db.executemany(f"INSERT INTO _pg_class VALUES ({','.join(['?'] * 33)})", pg_class_rows)


def _populate_pg_description(db, idx: CatalogIndex, raw_tables: list) -> None:
    tid_desc: dict[int, str] = {}
    tid_col_desc: dict[int, dict[str, str]] = {}
    for rt in raw_tables:
        _tid = rt["id"] if isinstance(rt, dict) else getattr(rt, "id", None)
        _tdesc = rt["description"] if isinstance(rt, dict) else getattr(rt, "description", None)
        _cols = rt["columns"] if isinstance(rt, dict) else getattr(rt, "columns", [])
        if _tid is None:
            continue
        if _tdesc:
            tid_desc[_tid] = _tdesc
        cdesc: dict[str, str] = {}
        for col in _cols:
            _cname = col["column_name"] if isinstance(col, dict) else getattr(col, "name", "")
            _cdesc = col["description"] if isinstance(col, dict) else getattr(col, "description", None)
            if _cdesc:
                cdesc[_cname] = _cdesc
        if cdesc:
            tid_col_desc[_tid] = cdesc

    if not tid_desc and not tid_col_desc:
        return

    desc_rows: list[tuple] = []
    for _cat, _sch, _tname, table_id, toid in idx.tables:
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
        pg_oid = _trino_to_pg_oid(col_type)
        attr_rows.append(
            (
                toid, col_name, pg_oid, -1, -1, ordinal, 0, -1, -1,
                False, "i", "x", not is_nullable, False, False, "", "",
                False, True, 0, 0, None, None, None,
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
                oid, name, ns, 10, ln, False, tt, cat, False, True, ",",
                0, 0, 0, "-", "-", "-", "-", "-", "-", "-", "i", "x",
                nn, base, -1, 0, 0, None, None, None,
            )
            for oid, name, ns, ln, tt, cat, nn, base in _PG_TYPE_ROWS
        ],
    )


def _populate_empty_system_tables(db) -> None:
    db.execute(
        "CREATE TABLE _pg_attrdef (oid INTEGER, adrelid INTEGER, adnum SMALLINT, adbin VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_description (objoid INTEGER, classoid VARCHAR, objsubid INTEGER, description VARCHAR)"
    )
    db.execute("""CREATE TABLE _pg_index (
        indexrelid INTEGER, indrelid INTEGER, indnatts SMALLINT, indnkeyatts SMALLINT,
        indisunique BOOLEAN, indisprimary BOOLEAN, indisexclusion BOOLEAN,
        indimmediate BOOLEAN, indisclustered BOOLEAN, indisvalid BOOLEAN,
        indcheckxmin BOOLEAN, indisready BOOLEAN, indislive BOOLEAN,
        indisreplident BOOLEAN, indkey VARCHAR, indcollation VARCHAR,
        indclass VARCHAR, indoption VARCHAR, indexprs VARCHAR, indpred VARCHAR)""")
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
    db.execute(
        "CREATE TABLE _pg_auth_members (roleid INTEGER, member INTEGER, grantor INTEGER, admin_option BOOLEAN)"
    )
    db.execute("CREATE TABLE _pg_tablespace (oid INTEGER, spcname VARCHAR, spcowner INTEGER, spcacl VARCHAR, spcoptions VARCHAR)")
    db.execute("CREATE TABLE _pg_conversion (oid INTEGER, conname VARCHAR, connamespace INTEGER, conowner INTEGER, conforencoding INTEGER, contoencoding INTEGER, conproc INTEGER, condefault BOOLEAN)")
    db.execute("CREATE TABLE _pg_shdescription (objoid INTEGER, classoid INTEGER, description VARCHAR)")
    db.execute("""CREATE TABLE _pg_extension (
        oid INTEGER, extname VARCHAR, extowner INTEGER, extnamespace INTEGER,
        extrelocatable BOOLEAN, extversion VARCHAR, extconfig INTEGER[], extcondition VARCHAR)""")
    db.execute("""CREATE TABLE _pg_enum (
        oid INTEGER, enumtypid INTEGER, enumsortorder REAL, enumlabel VARCHAR)""")
    db.execute("""CREATE TABLE _pg_stat_activity (
        datid INTEGER, datname VARCHAR, pid INTEGER, usesysid INTEGER,
        usename VARCHAR, application_name VARCHAR, client_addr VARCHAR,
        client_hostname VARCHAR, client_port INTEGER, backend_start VARCHAR,
        xact_start VARCHAR, query_start VARCHAR, state_change VARCHAR,
        wait_event_type VARCHAR, wait_event VARCHAR, state VARCHAR,
        backend_xid INTEGER, backend_xmin INTEGER, query VARCHAR,
        backend_type VARCHAR)""")
    db.execute("""CREATE TABLE _pg_stat_user_tables (
        relid INTEGER, schemaname VARCHAR, relname VARCHAR,
        seq_scan BIGINT, seq_tup_read BIGINT, idx_scan BIGINT, idx_tup_fetch BIGINT,
        n_tup_ins BIGINT, n_tup_upd BIGINT, n_tup_del BIGINT, n_tup_hot_upd BIGINT,
        n_live_tup BIGINT, n_dead_tup BIGINT, n_mod_since_analyze BIGINT,
        n_ins_since_vacuum BIGINT, last_vacuum VARCHAR, last_autovacuum VARCHAR,
        last_analyze VARCHAR, last_autoanalyze VARCHAR, vacuum_count BIGINT,
        autovacuum_count BIGINT, analyze_count BIGINT, autoanalyze_count BIGINT)""")
    db.execute("""CREATE TABLE _is_views (
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        view_definition VARCHAR, check_option VARCHAR, is_updatable VARCHAR,
        is_insertable_into VARCHAR, is_trigger_updatable VARCHAR,
        is_trigger_deletable VARCHAR, is_trigger_insertable_into VARCHAR)""")
    db.execute("""CREATE TABLE _is_referential_constraints (
        constraint_catalog VARCHAR, constraint_schema VARCHAR, constraint_name VARCHAR,
        unique_constraint_catalog VARCHAR, unique_constraint_schema VARCHAR,
        unique_constraint_name VARCHAR, match_option VARCHAR,
        update_rule VARCHAR, delete_rule VARCHAR)""")
    db.execute("CREATE TABLE _pg_trigger (oid INTEGER, tgrelid INTEGER, tgparentid INTEGER, tgname VARCHAR, tgfoid INTEGER, tgtype SMALLINT, tgenabled VARCHAR, tgisinternal BOOLEAN, tgconstrrelid INTEGER, tgconstrindid INTEGER, tgconstraint INTEGER, tgdeferrable BOOLEAN, tginitdeferred BOOLEAN, tgnargs SMALLINT, tgattr VARCHAR, tgargs VARCHAR, tgqual VARCHAR, tgoldtable VARCHAR, tgnewtable VARCHAR)")
    db.execute("CREATE TABLE _pg_inherits (inhrelid INTEGER, inhparent INTEGER, inhseqno INTEGER, inhdetachpending BOOLEAN)")
    db.execute("CREATE TABLE _pg_rewrite (oid INTEGER, rulename VARCHAR, ev_class INTEGER, ev_type VARCHAR, ev_enabled VARCHAR, is_instead BOOLEAN, ev_qual VARCHAR, ev_action VARCHAR)")
    db.execute("CREATE TABLE _pg_depend (classid INTEGER, objid INTEGER, objsubid INTEGER, refclassid INTEGER, refobjid INTEGER, refobjsubid INTEGER, deptype VARCHAR)")
    db.execute("CREATE TABLE _pg_shdepend (dbid INTEGER, classid INTEGER, objid INTEGER, objsubid INTEGER, refclassid INTEGER, refobjid INTEGER, deptype VARCHAR)")
    db.execute("CREATE TABLE _pg_aggregate (aggfnoid INTEGER, aggkind VARCHAR, aggnumdirectargs SMALLINT, aggtransfn INTEGER, aggfinalfn INTEGER, aggcombinefn INTEGER, aggserialfn INTEGER, aggdeserialfn INTEGER, aggmtransfn INTEGER, aggminvtransfn INTEGER, aggmfinalfn INTEGER, aggfinalextra BOOLEAN, aggmfinalextra BOOLEAN, aggfinalmodify VARCHAR, aggmfinalmodify VARCHAR, aggsortop INTEGER, aggtranstype INTEGER, aggtransspace INTEGER, aggmtranstype INTEGER, aggmtransspace INTEGER, agginitval VARCHAR, aggminitval VARCHAR)")
    db.execute("CREATE TABLE _pg_language (oid INTEGER, lanname VARCHAR, lanowner INTEGER, lanispl BOOLEAN, lanpltrusted BOOLEAN, lanplcallfoid INTEGER, laninline INTEGER, lanvalidator INTEGER, lanacl VARCHAR)")
    db.execute("CREATE TABLE _pg_operator (oid INTEGER, oprname VARCHAR, oprnamespace INTEGER, oprowner INTEGER, oprkind VARCHAR, oprcanmerge BOOLEAN, oprcanhash BOOLEAN, oprleft INTEGER, oprright INTEGER, oprresult INTEGER, oprcom INTEGER, oprnegate INTEGER, oprcode INTEGER, oprrest INTEGER, oprjoin INTEGER)")
    db.execute("CREATE TABLE _pg_cast (oid INTEGER, castsource INTEGER, casttarget INTEGER, castfunc INTEGER, castcontext VARCHAR, castmethod VARCHAR)")
    db.execute("CREATE TABLE _pg_collation (oid INTEGER, collname VARCHAR, collnamespace INTEGER, collowner INTEGER, collprovider VARCHAR, collisdeterministic BOOLEAN, collencoding INTEGER, collcollate VARCHAR, collctype VARCHAR, collversion VARCHAR)")
    db.execute("CREATE TABLE _pg_range (rngtypid INTEGER, rngsubtype INTEGER, rngmultitypid INTEGER, rngcollation INTEGER, rngsubopc INTEGER, rngcanonical INTEGER, rngsubdiff INTEGER)")
    db.execute("CREATE TABLE _pg_foreign_table (ftrelid INTEGER, ftserver INTEGER, ftoptions VARCHAR)")
    db.execute("CREATE TABLE _pg_foreign_server (oid INTEGER, srvname VARCHAR, srvowner INTEGER, srvfdw INTEGER, srvtype VARCHAR, srvversion VARCHAR, srvacl VARCHAR, srvoptions VARCHAR)")
    db.execute("CREATE TABLE _pg_user_mapping (oid INTEGER, umuser INTEGER, umserver INTEGER, umoptions VARCHAR)")
    db.execute("CREATE TABLE _pg_foreign_data_wrapper (oid INTEGER, fdwname VARCHAR, fdwowner INTEGER, fdwhandler INTEGER, fdwvalidator INTEGER, fdwacl VARCHAR, fdwoptions VARCHAR)")
    db.execute("CREATE TABLE _pg_sequence (seqrelid INTEGER, seqtypid INTEGER, seqstart BIGINT, seqincrement BIGINT, seqmax BIGINT, seqmin BIGINT, seqcache BIGINT, seqcycle BOOLEAN)")
    db.execute("CREATE TABLE _pg_policy (oid INTEGER, polname VARCHAR, polrelid INTEGER, polcmd VARCHAR, polpermissive BOOLEAN, polroles VARCHAR, polqual VARCHAR, polwithcheck VARCHAR)")
    db.execute("CREATE TABLE _pg_partitioned_table (partrelid INTEGER, partstrat VARCHAR, partnatts SMALLINT, partdefid INTEGER, partattrs VARCHAR, partclass VARCHAR, partcollation VARCHAR, partexprs VARCHAR)")
    db.execute("CREATE TABLE _pg_publication (oid INTEGER, pubname VARCHAR, pubowner INTEGER, puballtables BOOLEAN, pubinsert BOOLEAN, pubupdate BOOLEAN, pubdelete BOOLEAN, pubtruncate BOOLEAN, pubviaroot BOOLEAN)")
    db.execute("CREATE TABLE _pg_subscription (oid INTEGER, subdbid INTEGER, subskiplsn VARCHAR, subname VARCHAR, subowner INTEGER, subenabled BOOLEAN, subbinary BOOLEAN, substream VARCHAR, subtwophasestate VARCHAR, subdisableonerr BOOLEAN, subpasswordrequired BOOLEAN, subrunasowner BOOLEAN, subconninfo VARCHAR, subslotname VARCHAR, subsynccommit VARCHAR, subpublications VARCHAR, suborigin VARCHAR)")
    db.execute("CREATE TABLE _pg_event_trigger (oid INTEGER, evtname VARCHAR, evtevent VARCHAR, evtowner INTEGER, evtfoid INTEGER, evtenabled VARCHAR, evttags VARCHAR)")
    db.execute("CREATE TABLE _pg_stat_user_indexes (relid INTEGER, indexrelid INTEGER, schemaname VARCHAR, relname VARCHAR, indexrelname VARCHAR, idx_scan BIGINT, idx_tup_read BIGINT, idx_tup_fetch BIGINT)")
    db.execute("CREATE TABLE _pg_locks (locktype VARCHAR, database INTEGER, relation INTEGER, page INTEGER, tuple SMALLINT, virtualxid VARCHAR, transactionid INTEGER, classid INTEGER, objid INTEGER, objsubid SMALLINT, virtualtransaction VARCHAR, pid INTEGER, mode VARCHAR, granted BOOLEAN, fastpath BOOLEAN, waitstart VARCHAR)")
    db.execute("CREATE TABLE _is_role_table_grants (grantor VARCHAR, grantee VARCHAR, table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, privilege_type VARCHAR, is_grantable VARCHAR, with_hierarchy VARCHAR)")
    db.execute("CREATE TABLE _is_role_column_grants (grantor VARCHAR, grantee VARCHAR, table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, privilege_type VARCHAR, is_grantable VARCHAR)")
    db.execute("CREATE TABLE _is_triggers (trigger_catalog VARCHAR, trigger_schema VARCHAR, trigger_name VARCHAR, event_manipulation VARCHAR, event_object_catalog VARCHAR, event_object_schema VARCHAR, event_object_table VARCHAR, action_order INTEGER, action_condition VARCHAR, action_statement VARCHAR, action_orientation VARCHAR, action_timing VARCHAR, action_reference_old_table VARCHAR, action_reference_new_table VARCHAR, action_reference_old_row VARCHAR, action_reference_new_row VARCHAR, created VARCHAR)")
    db.execute("CREATE TABLE _is_sequences (sequence_catalog VARCHAR, sequence_schema VARCHAR, sequence_name VARCHAR, data_type VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, start_value VARCHAR, minimum_value VARCHAR, maximum_value VARCHAR, increment VARCHAR, cycle_option VARCHAR)")
    db.execute("CREATE TABLE _is_routines (specific_catalog VARCHAR, specific_schema VARCHAR, specific_name VARCHAR, routine_catalog VARCHAR, routine_schema VARCHAR, routine_name VARCHAR, routine_type VARCHAR, module_catalog VARCHAR, module_schema VARCHAR, module_name VARCHAR, udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR, data_type VARCHAR, character_maximum_length INTEGER, character_octet_length INTEGER, character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR, collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER, type_udt_catalog VARCHAR, type_udt_schema VARCHAR, type_udt_name VARCHAR, scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR, maximum_cardinality INTEGER, dtd_identifier VARCHAR, routine_body VARCHAR, routine_definition VARCHAR, external_name VARCHAR, external_language VARCHAR, parameter_style VARCHAR, is_deterministic VARCHAR, sql_data_access VARCHAR, is_null_call VARCHAR, sql_path VARCHAR, schema_level_routine VARCHAR, max_dynamic_result_sets INTEGER, is_user_defined_cast VARCHAR, is_implicitly_invocable VARCHAR, security_type VARCHAR, to_sql_specific_catalog VARCHAR, to_sql_specific_schema VARCHAR, to_sql_specific_name VARCHAR, as_locator VARCHAR, created VARCHAR, last_altered VARCHAR, new_savepoint_level VARCHAR, is_udt_dependent VARCHAR, result_cast_from_data_type VARCHAR, result_cast_as_locator VARCHAR, result_cast_char_max_length INTEGER, result_cast_char_octet_length INTEGER, result_cast_char_set_catalog VARCHAR, result_cast_char_set_schema VARCHAR, result_cast_char_set_name VARCHAR, result_cast_collation_catalog VARCHAR, result_cast_collation_schema VARCHAR, result_cast_collation_name VARCHAR, result_cast_numeric_precision INTEGER, result_cast_numeric_precision_radix INTEGER, result_cast_numeric_scale INTEGER, result_cast_datetime_precision INTEGER, result_cast_interval_type VARCHAR, result_cast_interval_precision INTEGER, result_cast_type_udt_catalog VARCHAR, result_cast_type_udt_schema VARCHAR, result_cast_type_udt_name VARCHAR, result_cast_scope_catalog VARCHAR, result_cast_scope_schema VARCHAR, result_cast_scope_name VARCHAR, result_cast_maximum_cardinality INTEGER, result_cast_dtd_identifier VARCHAR)")
    db.execute("CREATE TABLE _is_parameters (specific_catalog VARCHAR, specific_schema VARCHAR, specific_name VARCHAR, ordinal_position INTEGER, parameter_mode VARCHAR, is_result VARCHAR, as_locator VARCHAR, parameter_name VARCHAR, data_type VARCHAR, character_maximum_length INTEGER, character_octet_length INTEGER, character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR, collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER, udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR, scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR, maximum_cardinality INTEGER, dtd_identifier VARCHAR, parameter_default VARCHAR)")
    db.execute("CREATE TABLE _is_enabled_roles (role_name VARCHAR)")
    db.execute("CREATE TABLE _is_applicable_roles (grantee VARCHAR, role_name VARCHAR, is_grantable VARCHAR)")


def _populate_pg_roles_and_database(db, role_id: str) -> None:
    db.execute("""CREATE TABLE _pg_roles (
        oid INTEGER, rolname VARCHAR, rolsuper BOOLEAN, rolinherit BOOLEAN,
        rolcreaterole BOOLEAN, rolcreatedb BOOLEAN, rolcanlogin BOOLEAN,
        rolreplication BOOLEAN, rolconnlimit INTEGER, rolpassword VARCHAR,
        rolvaliduntil VARCHAR, rolbypassrls BOOLEAN, rolconfig VARCHAR)""")
    db.execute(
        "INSERT INTO _pg_roles VALUES (10,?,FALSE,TRUE,FALSE,FALSE,TRUE,FALSE,-1,NULL,NULL,FALSE,NULL)",
        [role_id],
    )
    db.execute("""CREATE TABLE _pg_database (
        oid INTEGER, datname VARCHAR, datdba INTEGER, encoding INTEGER,
        datlocprovider VARCHAR, datistemplate BOOLEAN, datallowconn BOOLEAN,
        datconnlimit INTEGER, datfrozenxid INTEGER, datminmxid INTEGER,
        dattablespace INTEGER, datcollate VARCHAR, datctype VARCHAR, datacl VARCHAR)""")
    db.execute(
        "INSERT INTO _pg_database VALUES (16384,'provisa',10,6,'c',FALSE,TRUE,-1,726,1,1663,'en_US.UTF-8','en_US.UTF-8',NULL)"
    )


_PG_SETTINGS_ROWS: list[tuple] = [
    ("server_version", "14.0.provisa", None, "Preset Options", "Shows the server version.", None, "internal", "string", "default", None, None, None, "14.0.provisa", "14.0.provisa", None, None, False),
    ("server_version_num", "140000", None, "Preset Options", "Shows the server version as an integer.", None, "internal", "integer", "default", None, None, None, "140000", "140000", None, None, False),
    ("server_encoding", "UTF8", None, "Preset Options", "Sets the server character set encoding.", None, "internal", "string", "default", None, None, None, "UTF8", "UTF8", None, None, False),
    ("client_encoding", "UTF8", None, "Client Connection Defaults", "Sets the client character set encoding.", None, "user", "string", "default", None, None, None, "SQL_ASCII", "UTF8", None, None, False),
    ("DateStyle", "ISO, MDY", None, "Client Connection Defaults", "Sets the display format for date and time values.", None, "user", "string", "default", None, None, None, "ISO, MDY", "ISO, MDY", None, None, False),
    ("TimeZone", "UTC", None, "Client Connection Defaults", "Sets the time zone for displaying and interpreting time stamps.", None, "user", "string", "default", None, None, None, "GMT", "UTC", None, None, False),
    ("max_connections", "100", None, "Connections and Authentication", "Sets the maximum number of concurrent connections.", None, "postmaster", "integer", "default", "1", "262143", None, "100", "100", None, None, False),
    ("standard_conforming_strings", "on", None, "Version and Platform Compatibility", "Causes strings to treat backslashes literally.", None, "user", "bool", "default", None, None, None, "on", "on", None, None, False),
    ("integer_datetimes", "on", None, "Preset Options", "Datetimes are integer based.", None, "internal", "bool", "default", None, None, None, "on", "on", None, None, False),
    ("IntervalStyle", "postgres", None, "Client Connection Defaults", "Sets the display format for interval values.", None, "user", "string", "default", None, None, None, "postgres", "postgres", None, None, False),
]


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


def _build_pk_constraint_rows(
    ctx,
    idx: CatalogIndex,
    con_oid_start: int,
) -> tuple[list[tuple], int]:
    from provisa.compiler.sql_gen import semantic_table_name
    rows: list[tuple] = []
    con_oid = con_oid_start
    seen_table_ids: set[int] = set()
    for _, tm in ctx.tables.items():
        if tm.table_id in seen_table_ids:
            continue
        toid_pk = idx.table_id_to_oid.get(tm.table_id)
        if toid_pk is None:
            continue
        pk_cols = ctx.pk_columns.get(tm.table_id, [])
        if not pk_cols:
            continue
        seen_table_ids.add(tm.table_id)
        ns_oid_pk = idx.ns_map.get(idx.toid_to_table[toid_pk][1], 2200)
        conkey = [idx.col_attnum.get((toid_pk, c), 0) for c in pk_cols]
        rows.append(
            (
                con_oid, f"pk_{semantic_table_name(tm)}", ns_oid_pk, "p",
                False, False, True, toid_pk, 0, 0, 0, 0,
                None, None, None, True, 0, True, conkey,
                None, None, None, None, None, None,
            )
        )
        con_oid += 1
    return rows, con_oid


def _build_fk_constraint_rows(
    ctx,
    idx: CatalogIndex,
    con_oid_start: int,
) -> tuple[list[tuple], int]:
    from provisa.compiler.sql_gen import semantic_table_name
    rows: list[tuple] = []
    con_oid = con_oid_start
    seen_joins: set[tuple] = set()
    used_names: set[str] = set()
    for (src_type, join_field), jm in ctx.joins.items():
        if not jm.target_column:
            continue
        src_tm = next((tm for tm in ctx.tables.values() if tm.type_name == src_type), None)
        if src_tm is None:
            continue
        dedup_key = (src_tm.table_id, jm.source_column, jm.target.table_id, jm.target_column)
        if dedup_key in seen_joins:
            continue
        seen_joins.add(dedup_key)
        src_toid = idx.table_id_to_oid.get(src_tm.table_id)
        tgt_toid = idx.table_id_to_oid.get(jm.target.table_id)
        if src_toid is None or tgt_toid is None:
            continue
        ns_oid_fk = idx.ns_map.get(idx.toid_to_table[src_toid][1], 2200)
        is_synthetic = (
            jm.source_constant is not None
            or jm.source_expr is not None
            or jm.source_column.startswith("__")
        )
        col_label = join_field if is_synthetic else jm.source_column
        src_sem_name = semantic_table_name(src_tm)
        base_name = f"fk_{src_sem_name}__{col_label}"
        tgt_sem_name = semantic_table_name(jm.target)
        con_name = base_name if base_name not in used_names else f"{base_name}__{tgt_sem_name}"
        used_names.add(con_name)
        attnum_col = jm.source_column
        if attnum_col.startswith("__"):
            attnum_col = "_name_"
        src_attnum = idx.col_attnum.get((src_toid, attnum_col), 0)
        tgt_attnum = idx.col_attnum.get((tgt_toid, jm.target_column), 0)
        if src_attnum == 0:
            continue
        rows.append(
            (
                con_oid, con_name, ns_oid_fk, "f",
                False, False, True, src_toid, 0, 0, 0, tgt_toid,
                "a", "a", "s", True, 0, True, [src_attnum], [tgt_attnum],
                None, None, None, None, None,
            )
        )
        con_oid += 1
    return rows, con_oid


def _populate_pg_constraint(db, ctx, idx: CatalogIndex) -> list[tuple]:
    db.execute("""CREATE TABLE _pg_constraint (
        oid INTEGER, conname VARCHAR, connamespace INTEGER, contype VARCHAR,
        condeferrable BOOLEAN, condeferred BOOLEAN, convalidated BOOLEAN,
        conrelid INTEGER, contypid INTEGER, conindid INTEGER, conparentid INTEGER,
        confrelid INTEGER, confupdtype VARCHAR, confdeltype VARCHAR, confmatchtype VARCHAR,
        conislocal BOOLEAN, coninhcount INTEGER, connoinherit BOOLEAN,
        conkey INTEGER[], confkey INTEGER[], conpfeqop INTEGER[], conppeqop INTEGER[],
        conffeqop INTEGER[], conexclop INTEGER[], conbin VARCHAR)""")
    constraint_rows: list[tuple] = []
    if ctx:
        pk_rows, next_oid = _build_pk_constraint_rows(ctx, idx, 20000)
        constraint_rows.extend(pk_rows)
        fk_rows, _ = _build_fk_constraint_rows(ctx, idx, next_oid)
        constraint_rows.extend(fk_rows)
        import os as _os
        _pglog = _os.path.expanduser("~/pgwire_debug.log")
        with open(_pglog, "a") as _f:
            fk_summary = [(r[1], r[7], r[11]) for r in fk_rows]  # (conname, conrelid, confrelid)
            _f.write(f"[CATALOG] built pk={len(pk_rows)} fk={len(fk_rows)} fk_rows={fk_summary}\n")
            _f.write(f"[CATALOG] joins_keys={list(ctx.joins.keys())[:10]}\n")
    if constraint_rows:
        db.executemany(
            f"INSERT INTO _pg_constraint VALUES ({','.join(['?'] * 25)})",
            constraint_rows,
        )
    return constraint_rows


def _populate_is_constraints(db, constraint_rows: list[tuple], idx: CatalogIndex) -> None:
    db.execute("""CREATE TABLE _is_table_constraints (
        constraint_catalog VARCHAR, constraint_schema VARCHAR, constraint_name VARCHAR,
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        constraint_type VARCHAR, is_deferrable VARCHAR, initially_deferred VARCHAR,
        enforced VARCHAR, nulls_distinct VARCHAR)""")
    db.execute("""CREATE TABLE _is_key_column_usage (
        constraint_catalog VARCHAR, constraint_schema VARCHAR, constraint_name VARCHAR,
        table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR,
        column_name VARCHAR, ordinal_position INTEGER, position_in_unique_constraint INTEGER)""")
    if not constraint_rows:
        return
    oid_to_ns: dict[int, str] = {v: k for k, v in idx.ns_map.items()}
    is_tc_rows: list[tuple] = []
    is_kcu_rows: list[tuple] = []
    for con_row in constraint_rows:
        conname_v: str = con_row[1]
        conns_oid_v: int = con_row[2]
        contype_v: str = con_row[3]
        conrelid_v: int = con_row[7]
        c_v, c_sch_v, c_tname_v = idx.toid_to_table.get(conrelid_v, ("provisa", "public", ""))
        con_schema_v = oid_to_ns.get(conns_oid_v, "public")
        ctype_str = "PRIMARY KEY" if contype_v == "p" else "FOREIGN KEY"
        is_tc_rows.append(
            ("provisa", con_schema_v, conname_v, c_v, c_sch_v, c_tname_v, ctype_str, "NO", "NO", "YES", "YES")
        )
        conkeys_raw = con_row[18]
        conkeys_list: list[int] = list(conkeys_raw) if conkeys_raw else []
        for pos, attnum_v in enumerate(conkeys_list, 1):
            col_name_v = idx.attnum_to_col.get((conrelid_v, int(attnum_v)), "")
            if col_name_v:
                is_kcu_rows.append(
                    (
                        "provisa", con_schema_v, conname_v, c_v, c_sch_v, c_tname_v,
                        col_name_v, pos, pos if contype_v == "p" else None,
                    )
                )
    if is_tc_rows:
        db.executemany(
            f"INSERT INTO _is_table_constraints VALUES ({','.join(['?'] * 11)})",
            is_tc_rows,
        )
    if is_kcu_rows:
        db.executemany(
            f"INSERT INTO _is_key_column_usage VALUES ({','.join(['?'] * 9)})",
            is_kcu_rows,
        )


_row_count_cache: dict[str, tuple[float, dict[int, float]]] = {}
_ROW_COUNT_TTL = 300.0


def _fetch_row_counts(ctx, idx: CatalogIndex, trino_conn) -> dict[int, float]:
    """Fetch row count estimates via SHOW STATS FOR. Returns {toid: row_count}."""
    if ctx is None or trino_conn is None:
        return {}
    table_id_to_meta: dict[int, tuple[str, str, str]] = {
        tm.table_id: (tm.catalog_name, tm.schema_name, tm.table_name)
        for tm in ctx.tables.values()
    }
    result: dict[int, float] = {}
    for _cat, _sch, _tname, table_id, toid in idx.tables:
        ref = table_id_to_meta.get(table_id)
        if not ref:
            continue
        cat, sch, tname = ref
        try:
            cur = trino_conn.cursor()
            cur.execute(f'SHOW STATS FOR "{cat}"."{sch}"."{tname}"')
            for row in cur.fetchall():
                if row[0] is None and row[4] is not None:
                    result[toid] = float(row[4])
                    break
        except Exception:
            pass
    return result


def _build_catalog_db(role_id: str, state):
    import duckdb
    import os as _os2
    _pglog2 = _os2.path.expanduser("~/pgwire_debug.log")
    with open(_pglog2, "a") as _f2:
        _ctx_keys = list((state.contexts or {}).keys())[:10]
        _f2.write(f"[CATALOG] build_catalog_db role_id={role_id!r} ctx_keys={_ctx_keys}\n")

    db = duckdb.connect(":memory:")
    ctx = state.contexts.get(role_id)
    col_types: dict = state.schema_build_cache.get("column_types", {})

    idx = _build_catalog_index(ctx, col_types)

    now = time.monotonic()
    cached = _row_count_cache.get(role_id)
    if cached and now - cached[0] < _ROW_COUNT_TTL:
        row_counts = cached[1]
    else:
        row_counts = _fetch_row_counts(ctx, idx, getattr(state, "trino_conn", None))
        _row_count_cache[role_id] = (now, row_counts)

    _populate_is_schemata(db, idx)
    _populate_is_tables(db, idx)
    _populate_is_columns(db, idx)
    _populate_pg_namespace(db, idx)
    _populate_pg_class(db, idx, row_counts)
    _populate_pg_attribute(db, idx)
    _populate_pg_type(db)
    _populate_empty_system_tables(db)
    raw_tables = state.schema_build_cache.get("tables", []) if state else []
    _populate_pg_description(db, idx, raw_tables)
    constraint_rows = _populate_pg_constraint(db, ctx, idx)
    _populate_pg_roles_and_database(db, role_id)
    _populate_pg_settings(db)
    _populate_pg_tables_and_am(db, idx)
    _populate_is_constraints(db, constraint_rows, idx)

    return db


def _rewrite_for_duckdb(sql: str, role_id: str = "") -> str:
    """Rewrite catalog table refs for DuckDB and transpile from postgres dialect."""
    import sqlglot
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if isinstance(node, exp.Table):
            db = node.db.lower() if node.db else ""
            name = node.name.lower() if node.name else ""
            mapped = _TABLE_MAP.get((db, name)) or (
                _TABLE_MAP.get(("pg_catalog", name)) if not db and name in _CATALOG_TABLE_NAMES else None
            )
            if mapped:
                new_tbl = exp.Table(this=exp.Identifier(this=mapped, quoted=False))
                if node.alias:
                    new_tbl.set("alias", node.args.get("alias"))
                return new_tbl
        if isinstance(node, exp.Anonymous):
            fn = node.name.lower()
            if "pg_get_userbyid" in fn or "pg_get_role_name" in fn:
                return exp.Literal.string("provisa")
            if fn.startswith("pg_get_") or "pg_tablespace_location" in fn:
                return exp.null()
            if "pg_encoding_to_char" in fn:
                return exp.Literal.string("UTF8")
            if "format_type" in fn:
                return exp.Literal.string("text")
            if "obj_description" in fn or "shobj_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0] if args else exp.null()
                subq = (
                    exp.select(exp.column("description"))
                    .from_("_pg_description")
                    .where(exp.EQ(this=exp.column("objoid"), expression=oid_expr))
                    .where(exp.EQ(this=exp.column("objsubid"), expression=exp.Literal.number(0)))
                )
                return exp.Subquery(this=subq)
            if "col_description" in fn:
                args = node.args.get("expressions", [])
                oid_expr = args[0] if args else exp.null()
                attnum_expr = args[1] if len(args) > 1 else exp.null()
                subq = (
                    exp.select(exp.column("description"))
                    .from_("_pg_description")
                    .where(exp.EQ(this=exp.column("objoid"), expression=oid_expr))
                    .where(exp.EQ(this=exp.column("objsubid"), expression=attnum_expr))
                )
                return exp.Subquery(this=subq)
            if any(p in fn for p in ("pg_get_constraintdef", "pg_get_expr", "pg_get_indexdef", "pg_get_partkeydef", "pg_get_partition", "pg_get_serial_sequence", "pg_get_userbyid", "pg_get_ruledef", "pg_get_triggerdef", "pg_get_viewdef")):
                return exp.null()
            if "pg_postmaster_start_time" in fn or "pg_conf_load_time" in fn:
                return exp.null()
            if "pg_is_other_temp_schema" in fn:
                return exp.false()
            if "pg_function_is_visible" in fn or "pg_opclass_is_visible" in fn or "pg_type_is_visible" in fn or "pg_ts_config_is_visible" in fn or "pg_ts_dict_is_visible" in fn or "pg_ts_parser_is_visible" in fn or "pg_ts_template_is_visible" in fn or "pg_operator_is_visible" in fn:
                return exp.true()
            if "pg_relation_size" in fn or "pg_total_relation_size" in fn or "pg_indexes_size" in fn or "pg_stat_get" in fn:
                return exp.Literal.number(0)
            if "pg_table_is_visible" in fn or "pg_has_role" in fn:
                return exp.true()
            if fn in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
            if fn in ("current_database",):
                return exp.Literal.string("provisa")
            if fn == "version":
                return exp.Literal.string("PostgreSQL 14.0 on Provisa")
        if type(node).__name__ == "CurrentUser":
            return exp.Literal.string(role_id)
        if isinstance(node, exp.Dot):
            # Strip schema qualifier from schema-qualified expressions: pg_catalog.TRUE → TRUE
            left = node.this
            if isinstance(left, exp.Identifier) and left.name.lower() in _INTERCEPT_SCHEMAS:
                # Re-apply transform to inner node so schema-qualified function calls
                # like pg_catalog.pg_encoding_to_char(...) are fully handled
                return _transform(node.expression)
        if isinstance(node, exp.Cast):
            dtype = node.args.get("to")
            dtype_str = str(dtype).lower() if dtype else ""
            if dtype_str in ("regclass", "regtype", "regproc", "regprocedure", "regoper", "regoperator", "regconfig", "regdictionary", "regrole", "regnamespace"):
                return node.this
            if dtype_str in ("oid", "xid", "tid", "cid"):
                return exp.Literal.number(0)
        if isinstance(node, exp.Column):
            if node.name.lower() in ("current_user", "session_user"):
                return exp.Literal.string(role_id)
            # Rewrite schema-qualified column refs: pg_catalog.pg_class.col → _pg_class.col
            db_node = node.args.get("db") or node.args.get("catalog")
            db = db_node.name.lower() if db_node and hasattr(db_node, "name") else ""
            tbl = node.args.get("table")
            tname = tbl.name.lower() if tbl and hasattr(tbl, "name") else ""
            if db in _INTERCEPT_SCHEMAS and tname:
                mapped = _TABLE_MAP.get((db, tname)) or tname
                return exp.column(node.name, table=mapped)
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

    rewritten = stripped
    db = None
    try:
        db = _build_catalog_db(role_id, state)
        rewritten = _rewrite_for_duckdb(stripped, role_id)
        import re as _re
        rewritten = _re.sub(r'\$\d+', 'NULL', rewritten)
        cur = db.execute(rewritten)
        rows = [tuple(r) for r in cur.fetchall()]
        col_names = [desc[0] for desc in (cur.description or [])]
        _debug_lower = stripped.lower()
        if "pg_constraint" in _debug_lower or "conrelid" in _debug_lower or "confrelid" in _debug_lower:
            import os as _os
            _pglog = _os.path.expanduser("~/pgwire_debug.log")
            with open(_pglog, "a") as _f:
                _f.write(f"[CATALOG] constraint sql={stripped[:300]!r} rows={len(rows)} first={rows[:2]!r}\n")
        return QueryResult(rows=rows, column_names=col_names)
    except Exception as exc:
        import logging as _logging
        import os as _os
        _pglog = _os.path.expanduser("~/pgwire_debug.log")
        with open(_pglog, "a") as _f:
            _f.write(f"[CATALOG] ERROR sql={stripped[:300]!r} rewritten={rewritten[:300]!r} exc={exc!r}\n")
        _logging.getLogger("uvicorn.error").warning("[CATALOG] DuckDB error sql=%r rewritten=%r: %s", stripped[:200], rewritten[:200], exc)
        return QueryResult(rows=[], column_names=[])
    finally:
        if db is not None:
            db.close()
