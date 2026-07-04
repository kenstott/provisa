# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7b2c90-6a13-4d85-9f02-1c8a0d4f7b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Empty pg_catalog / information_schema system tables for the pgwire catalog DB.

Creates the DuckDB-backed schemas of the system tables Provisa emulates but does not
populate per-role (pg_proc, pg_stat_*, pg_extension, information_schema.routines, ...).
Split out of catalog.py to keep that module within its size budget.
"""

from __future__ import annotations


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
        indisreplident BOOLEAN, indkey INTEGER[], indcollation INTEGER[],
        indclass INTEGER[], indoption SMALLINT[], indexprs VARCHAR, indpred VARCHAR)""")
    db.execute("""CREATE TABLE _pg_proc (
        oid INTEGER, proname VARCHAR, pronamespace INTEGER, proowner INTEGER,
        prolang INTEGER, procost REAL, prorows REAL, provariadic INTEGER,
        prosupport VARCHAR, prokind VARCHAR, prosecdef BOOLEAN, proleakproof BOOLEAN,
        proisstrict BOOLEAN, proretset BOOLEAN, provolatile VARCHAR, proparallel VARCHAR,
        pronargs SMALLINT, pronargdefaults SMALLINT, prorettype INTEGER,
        proargtypes INTEGER[], proallargtypes INTEGER[], proargmodes VARCHAR[],
        proargnames VARCHAR, proargdefaults VARCHAR, protrftypes VARCHAR,
        prosrc VARCHAR, probin VARCHAR, prosqlbody VARCHAR,
        proconfig VARCHAR, proacl VARCHAR)""")
    db.execute(
        "CREATE TABLE _pg_auth_members (roleid INTEGER, member INTEGER, grantor INTEGER, admin_option BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_tablespace (oid INTEGER, spcname VARCHAR, spcowner INTEGER, spcacl VARCHAR, spcoptions VARCHAR)"
    )
    db.execute("INSERT INTO _pg_tablespace VALUES (1663, 'pg_default', 10, NULL, NULL)")
    db.execute("INSERT INTO _pg_tablespace VALUES (1664, 'pg_global', 10, NULL, NULL)")
    db.execute(
        "CREATE TABLE _pg_conversion (oid INTEGER, conname VARCHAR, connamespace INTEGER, conowner INTEGER, conforencoding INTEGER, contoencoding INTEGER, conproc INTEGER, condefault BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_shdescription (objoid INTEGER, classoid INTEGER, description VARCHAR)"
    )
    db.execute("""CREATE TABLE _pg_extension (
        oid INTEGER, extname VARCHAR, extowner INTEGER, extnamespace INTEGER,
        extrelocatable BOOLEAN, extversion VARCHAR, extconfig VARCHAR[], extcondition VARCHAR[])""")
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
    db.execute(
        "CREATE TABLE _pg_trigger (oid INTEGER, tgrelid INTEGER, tgparentid INTEGER, tgname VARCHAR, tgfoid INTEGER, tgtype SMALLINT, tgenabled VARCHAR, tgisinternal BOOLEAN, tgconstrrelid INTEGER, tgconstrindid INTEGER, tgconstraint INTEGER, tgdeferrable BOOLEAN, tginitdeferred BOOLEAN, tgnargs SMALLINT, tgattr VARCHAR, tgargs VARCHAR, tgqual VARCHAR, tgoldtable VARCHAR, tgnewtable VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_inherits (inhrelid INTEGER, inhparent INTEGER, inhseqno INTEGER, inhdetachpending BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_rewrite (oid INTEGER, rulename VARCHAR, ev_class INTEGER, ev_type VARCHAR, ev_enabled VARCHAR, is_instead BOOLEAN, ev_qual VARCHAR, ev_action VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_depend (classid INTEGER, objid INTEGER, objsubid INTEGER, refclassid INTEGER, refobjid INTEGER, refobjsubid INTEGER, deptype VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_shdepend (dbid INTEGER, classid INTEGER, objid INTEGER, objsubid INTEGER, refclassid INTEGER, refobjid INTEGER, deptype VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_aggregate (aggfnoid INTEGER, aggkind VARCHAR, aggnumdirectargs SMALLINT, aggtransfn INTEGER, aggfinalfn INTEGER, aggcombinefn INTEGER, aggserialfn INTEGER, aggdeserialfn INTEGER, aggmtransfn INTEGER, aggminvtransfn INTEGER, aggmfinalfn INTEGER, aggfinalextra BOOLEAN, aggmfinalextra BOOLEAN, aggfinalmodify VARCHAR, aggmfinalmodify VARCHAR, aggsortop INTEGER, aggtranstype INTEGER, aggtransspace INTEGER, aggmtranstype INTEGER, aggmtransspace INTEGER, agginitval VARCHAR, aggminitval VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_language (oid INTEGER, lanname VARCHAR, lanowner INTEGER, lanispl BOOLEAN, lanpltrusted BOOLEAN, lanplcallfoid INTEGER, laninline INTEGER, lanvalidator INTEGER, lanacl VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_operator (oid INTEGER, oprname VARCHAR, oprnamespace INTEGER, oprowner INTEGER, oprkind VARCHAR, oprcanmerge BOOLEAN, oprcanhash BOOLEAN, oprleft INTEGER, oprright INTEGER, oprresult INTEGER, oprcom INTEGER, oprnegate INTEGER, oprcode INTEGER, oprrest INTEGER, oprjoin INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_cast (oid INTEGER, castsource INTEGER, casttarget INTEGER, castfunc INTEGER, castcontext VARCHAR, castmethod VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_opfamily (oid INTEGER, opfmethod INTEGER, opfname VARCHAR, opfnamespace INTEGER, opfowner INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_opclass (oid INTEGER, opcmethod INTEGER, opcname VARCHAR, opcnamespace INTEGER, opcowner INTEGER, opcfamily INTEGER, opcintype INTEGER, opcdefault BOOLEAN, opckeytype INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_amop (oid INTEGER, amopfamily INTEGER, amoplefttype INTEGER, amoprighttype INTEGER, amopstrategy SMALLINT, amoppurpose VARCHAR, amopopr INTEGER, amopmethod INTEGER, amopsortfamily INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_amproc (oid INTEGER, amprocfamily INTEGER, amproclefttype INTEGER, amprocrighttype INTEGER, amprocnum SMALLINT, amproc INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_collation (oid INTEGER, collname VARCHAR, collnamespace INTEGER, collowner INTEGER, collprovider VARCHAR, collisdeterministic BOOLEAN, collencoding INTEGER, collcollate VARCHAR, collctype VARCHAR, collversion VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_range (rngtypid INTEGER, rngsubtype INTEGER, rngmultitypid INTEGER, rngcollation INTEGER, rngsubopc INTEGER, rngcanonical INTEGER, rngsubdiff INTEGER)"
    )
    db.execute(
        "CREATE TABLE _pg_foreign_table (ftrelid INTEGER, ftserver INTEGER, ftoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_foreign_server (oid INTEGER, srvname VARCHAR, srvowner INTEGER, srvfdw INTEGER, srvtype VARCHAR, srvversion VARCHAR, srvacl VARCHAR, srvoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_user_mapping (oid INTEGER, umuser INTEGER, umserver INTEGER, umoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_user_mappings (umid INTEGER, srvid INTEGER, srvname VARCHAR, umuser INTEGER, usename VARCHAR, umoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_foreign_data_wrapper (oid INTEGER, fdwname VARCHAR, fdwowner INTEGER, fdwhandler INTEGER, fdwvalidator INTEGER, fdwacl VARCHAR, fdwoptions VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_sequence (seqrelid INTEGER, seqtypid INTEGER, seqstart BIGINT, seqincrement BIGINT, seqmax BIGINT, seqmin BIGINT, seqcache BIGINT, seqcycle BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_policy (oid INTEGER, polname VARCHAR, polrelid INTEGER, polcmd VARCHAR, polpermissive BOOLEAN, polroles VARCHAR, polqual VARCHAR, polwithcheck VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_partitioned_table (partrelid INTEGER, partstrat VARCHAR, partnatts SMALLINT, partdefid INTEGER, partattrs VARCHAR, partclass VARCHAR, partcollation VARCHAR, partexprs VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_publication (oid INTEGER, pubname VARCHAR, pubowner INTEGER, puballtables BOOLEAN, pubinsert BOOLEAN, pubupdate BOOLEAN, pubdelete BOOLEAN, pubtruncate BOOLEAN, pubviaroot BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_subscription (oid INTEGER, subdbid INTEGER, subskiplsn VARCHAR, subname VARCHAR, subowner INTEGER, subenabled BOOLEAN, subbinary BOOLEAN, substream VARCHAR, subtwophasestate VARCHAR, subdisableonerr BOOLEAN, subpasswordrequired BOOLEAN, subrunasowner BOOLEAN, subconninfo VARCHAR, subslotname VARCHAR, subsynccommit VARCHAR, subpublications VARCHAR, suborigin VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_event_trigger (oid INTEGER, evtname VARCHAR, evtevent VARCHAR, evtowner INTEGER, evtfoid INTEGER, evtenabled VARCHAR, evttags VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_stat_user_indexes (relid INTEGER, indexrelid INTEGER, schemaname VARCHAR, relname VARCHAR, indexrelname VARCHAR, idx_scan BIGINT, idx_tup_read BIGINT, idx_tup_fetch BIGINT)"
    )
    db.execute(
        "CREATE TABLE _pg_locks (locktype VARCHAR, database INTEGER, relation INTEGER, page INTEGER, tuple SMALLINT, virtualxid VARCHAR, transactionid INTEGER, classid INTEGER, objid INTEGER, objsubid SMALLINT, virtualtransaction VARCHAR, pid INTEGER, mode VARCHAR, granted BOOLEAN, fastpath BOOLEAN, waitstart VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _pg_stat_ssl (pid INTEGER, ssl BOOLEAN, version VARCHAR, cipher VARCHAR, bits INTEGER, client_dn VARCHAR, client_serial VARCHAR, issuer_dn VARCHAR)"
    )
    db.execute("INSERT INTO _pg_stat_ssl VALUES (0, false, NULL, NULL, NULL, NULL, NULL, NULL)")
    db.execute(
        "CREATE TABLE _pg_timezone_names (name VARCHAR, abbrev VARCHAR, utc_offset VARCHAR, is_dst BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _pg_timezone_abbrevs (abbrev VARCHAR, utc_offset VARCHAR, is_dst BOOLEAN)"
    )
    db.execute(
        "CREATE TABLE _is_role_table_grants (grantor VARCHAR, grantee VARCHAR, table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, privilege_type VARCHAR, is_grantable VARCHAR, with_hierarchy VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_role_column_grants (grantor VARCHAR, grantee VARCHAR, table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, privilege_type VARCHAR, is_grantable VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_triggers (trigger_catalog VARCHAR, trigger_schema VARCHAR, trigger_name VARCHAR, event_manipulation VARCHAR, event_object_catalog VARCHAR, event_object_schema VARCHAR, event_object_table VARCHAR, action_order INTEGER, action_condition VARCHAR, action_statement VARCHAR, action_orientation VARCHAR, action_timing VARCHAR, action_reference_old_table VARCHAR, action_reference_new_table VARCHAR, action_reference_old_row VARCHAR, action_reference_new_row VARCHAR, created VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_sequences (sequence_catalog VARCHAR, sequence_schema VARCHAR, sequence_name VARCHAR, data_type VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, start_value VARCHAR, minimum_value VARCHAR, maximum_value VARCHAR, increment VARCHAR, cycle_option VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_routines (specific_catalog VARCHAR, specific_schema VARCHAR, specific_name VARCHAR, routine_catalog VARCHAR, routine_schema VARCHAR, routine_name VARCHAR, routine_type VARCHAR, module_catalog VARCHAR, module_schema VARCHAR, module_name VARCHAR, udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR, data_type VARCHAR, character_maximum_length INTEGER, character_octet_length INTEGER, character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR, collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER, type_udt_catalog VARCHAR, type_udt_schema VARCHAR, type_udt_name VARCHAR, scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR, maximum_cardinality INTEGER, dtd_identifier VARCHAR, routine_body VARCHAR, routine_definition VARCHAR, external_name VARCHAR, external_language VARCHAR, parameter_style VARCHAR, is_deterministic VARCHAR, sql_data_access VARCHAR, is_null_call VARCHAR, sql_path VARCHAR, schema_level_routine VARCHAR, max_dynamic_result_sets INTEGER, is_user_defined_cast VARCHAR, is_implicitly_invocable VARCHAR, security_type VARCHAR, to_sql_specific_catalog VARCHAR, to_sql_specific_schema VARCHAR, to_sql_specific_name VARCHAR, as_locator VARCHAR, created VARCHAR, last_altered VARCHAR, new_savepoint_level VARCHAR, is_udt_dependent VARCHAR, result_cast_from_data_type VARCHAR, result_cast_as_locator VARCHAR, result_cast_char_max_length INTEGER, result_cast_char_octet_length INTEGER, result_cast_char_set_catalog VARCHAR, result_cast_char_set_schema VARCHAR, result_cast_char_set_name VARCHAR, result_cast_collation_catalog VARCHAR, result_cast_collation_schema VARCHAR, result_cast_collation_name VARCHAR, result_cast_numeric_precision INTEGER, result_cast_numeric_precision_radix INTEGER, result_cast_numeric_scale INTEGER, result_cast_datetime_precision INTEGER, result_cast_interval_type VARCHAR, result_cast_interval_precision INTEGER, result_cast_type_udt_catalog VARCHAR, result_cast_type_udt_schema VARCHAR, result_cast_type_udt_name VARCHAR, result_cast_scope_catalog VARCHAR, result_cast_scope_schema VARCHAR, result_cast_scope_name VARCHAR, result_cast_maximum_cardinality INTEGER, result_cast_dtd_identifier VARCHAR)"
    )
    db.execute(
        "CREATE TABLE _is_parameters (specific_catalog VARCHAR, specific_schema VARCHAR, specific_name VARCHAR, ordinal_position INTEGER, parameter_mode VARCHAR, is_result VARCHAR, as_locator VARCHAR, parameter_name VARCHAR, data_type VARCHAR, character_maximum_length INTEGER, character_octet_length INTEGER, character_set_catalog VARCHAR, character_set_schema VARCHAR, character_set_name VARCHAR, collation_catalog VARCHAR, collation_schema VARCHAR, collation_name VARCHAR, numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, interval_type VARCHAR, interval_precision INTEGER, udt_catalog VARCHAR, udt_schema VARCHAR, udt_name VARCHAR, scope_catalog VARCHAR, scope_schema VARCHAR, scope_name VARCHAR, maximum_cardinality INTEGER, dtd_identifier VARCHAR, parameter_default VARCHAR)"
    )
    db.execute("CREATE TABLE _is_enabled_roles (role_name VARCHAR)")
    db.execute(
        "CREATE TABLE _is_applicable_roles (grantee VARCHAR, role_name VARCHAR, is_grantable VARCHAR)"
    )
