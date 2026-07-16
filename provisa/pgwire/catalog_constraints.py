# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL catalog constraint population.

Builds pg_constraint (PK/FK) rows from the compilation context plus the
information_schema table_constraints / key_column_usage projections.
"""

# Requirements: REQ-128, REQ-363

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.pgwire.catalog_populate import CatalogIndex


def _build_pk_constraint_rows(
    ctx,
    idx: CatalogIndex,
    con_oid_start: int,
) -> tuple[list[tuple], int]:
    from provisa.compiler.sql_rewrite import semantic_table_name

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
                con_oid,
                f"pk_{semantic_table_name(tm)}",
                ns_oid_pk,
                "p",
                False,
                False,
                True,
                toid_pk,
                0,
                0,
                0,
                0,
                None,
                None,
                None,
                True,
                0,
                True,
                conkey,
                None,
                None,
                None,
                None,
                None,
                None,
            )
        )
        con_oid += 1
    return rows, con_oid


def _build_unique_constraint_rows(  # REQ-1093
    ctx,
    idx: CatalogIndex,
    con_oid_start: int,
) -> tuple[list[tuple], int]:
    """One pg_constraint row (contype 'u') per declared UNIQUE constraint.

    conkey is the ordered attnum list; there is no referenced table (confrelid=0).
    PK columns are already emitted as contype 'p'; these are the non-PK unique keys.
    """
    rows: list[tuple] = []
    con_oid = con_oid_start
    seen_table_ids: set[int] = set()
    used_names: set[str] = set()
    for _, tm in ctx.tables.items():
        if tm.table_id in seen_table_ids:
            continue
        toid = idx.table_id_to_oid.get(tm.table_id)
        if toid is None:
            continue
        uniques = ctx.unique_constraints.get(tm.table_id, [])
        if not uniques:
            continue
        seen_table_ids.add(tm.table_id)
        ns_oid = idx.ns_map.get(idx.toid_to_table[toid][1], 2200)
        for uc_name, uc_cols in uniques:
            conkey = [idx.col_attnum.get((toid, c), 0) for c in uc_cols]
            if any(a == 0 for a in conkey):
                continue  # a column is not in this projection — skip the whole constraint
            con_name = uc_name if uc_name not in used_names else f"{uc_name}_{con_oid}"
            used_names.add(con_name)
            rows.append(
                (
                    con_oid,
                    con_name,
                    ns_oid,
                    "u",
                    False,
                    False,
                    True,
                    toid,
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    True,
                    0,
                    True,
                    conkey,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            )
            con_oid += 1
    return rows, con_oid


def _build_fk_constraint_rows(
    ctx,
    idx: CatalogIndex,
    con_oid_start: int,
) -> tuple[list[tuple], int]:
    from provisa.compiler.sql_rewrite import semantic_table_name

    rows: list[tuple] = []
    con_oid = con_oid_start
    seen_joins: set[tuple] = set()
    used_names: set[str] = set()
    for (src_type, join_field), jm in ctx.joins.items():
        if not jm.target_column:
            continue
        if jm.cardinality != "many-to-one":
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
        from provisa.compiler.naming import apply_sql_name

        src_col_sql = apply_sql_name(jm.source_column)
        tgt_col_sql = apply_sql_name(jm.target_column)
        col_label = join_field if is_synthetic else src_col_sql
        src_sem_name = semantic_table_name(src_tm)
        base_name = f"fk_{src_sem_name}__{col_label}"
        tgt_sem_name = semantic_table_name(jm.target)
        con_name = base_name if base_name not in used_names else f"{base_name}__{tgt_sem_name}"
        used_names.add(con_name)
        attnum_col = src_col_sql
        if jm.source_column.startswith("__"):
            attnum_col = "_name_"
        src_attnum = idx.col_attnum.get((src_toid, attnum_col), 0)
        tgt_attnum = idx.col_attnum.get((tgt_toid, tgt_col_sql), 0)
        if src_attnum == 0:
            continue
        rows.append(
            (
                con_oid,
                con_name,
                ns_oid_fk,
                "f",
                False,
                False,
                True,
                src_toid,
                0,
                0,
                0,
                tgt_toid,
                "a",
                "a",
                "s",
                True,
                0,
                True,
                [src_attnum],
                [tgt_attnum],
                None,
                None,
                None,
                None,
                None,
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
        uq_rows, next_oid = _build_unique_constraint_rows(ctx, idx, next_oid)  # REQ-1093
        constraint_rows.extend(uq_rows)
        fk_rows, _ = _build_fk_constraint_rows(ctx, idx, next_oid)
        constraint_rows.extend(fk_rows)
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
        ctype_str = {"p": "PRIMARY KEY", "u": "UNIQUE", "f": "FOREIGN KEY"}[contype_v]  # REQ-1093
        is_tc_rows.append(
            (
                "provisa",
                con_schema_v,
                conname_v,
                c_v,
                c_sch_v,
                c_tname_v,
                ctype_str,
                "NO",
                "NO",
                "YES",
                "YES",
            )
        )
        conkeys_raw = con_row[18]
        conkeys_list: list[int] = list(conkeys_raw) if conkeys_raw else []
        for pos, attnum_v in enumerate(conkeys_list, 1):
            col_name_v = idx.attnum_to_col.get((conrelid_v, int(attnum_v)), "")
            if col_name_v:
                is_kcu_rows.append(
                    (
                        "provisa",
                        con_schema_v,
                        conname_v,
                        c_v,
                        c_sch_v,
                        c_tname_v,
                        col_name_v,
                        pos,
                        pos if contype_v == "p" else None,
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
    is_rc_rows = _build_referential_rows(constraint_rows, oid_to_ns)
    if is_rc_rows:
        db.executemany(
            f"INSERT INTO _is_referential_constraints VALUES ({','.join(['?'] * 9)})",
            is_rc_rows,
        )


def _build_referential_rows(
    constraint_rows: list[tuple],
    oid_to_ns: dict[int, str],
) -> list[tuple]:
    """One information_schema.referential_constraints row per FK constraint.

    Joins each FK (confrelid) to the referenced table's primary-key constraint.
    Fails loud if a referenced PK constraint cannot be resolved.
    """
    pk_by_relid: dict[int, tuple[str, str]] = {
        con_row[7]: (con_row[1], oid_to_ns.get(con_row[2], "public"))
        for con_row in constraint_rows
        if con_row[3] == "p"
    }
    rc_rows: list[tuple] = []
    for con_row in constraint_rows:
        if con_row[3] != "f":
            continue
        fk_name: str = con_row[1]
        fk_schema: str = oid_to_ns.get(con_row[2], "public")
        confrelid: int = con_row[11]
        referenced = pk_by_relid.get(confrelid)
        if referenced is None:
            raise ValueError(
                f"referential_constraints: FK {fk_name!r} references table oid "
                f"{confrelid} with no resolvable primary-key constraint"
            )
        uniq_name, uniq_schema = referenced
        rc_rows.append(
            (
                "provisa",
                fk_schema,
                fk_name,
                "provisa",
                uniq_schema,
                uniq_name,
                "NONE",
                "NO ACTION",
                "NO ACTION",
            )
        )
    return rc_rows
