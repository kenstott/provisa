# pgwire Conformance

**Suite:** pgspec v1.5.11, against PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2).

**Pass rate:** 100.0%: 346 passed, 9 skipped by design, 0 failed, of 355 cases. Catalog functions are reported by engine.

<!-- conformance-results:begin -->
## Skipped by design

| Skipped by design | Count | Decision |
| --- | --- | --- |
| A source's array column is JSON | 3 | Semantic SQL has no array type: a source's native array column is registered as JSON, the type every catalog and every wire lists for it, so its value is sent as a JSON array rather than a PostgreSQL array. |
| The session's role and database | 3 | current_user and session_user name the role the session acts as, and current_database() the served database (the organization the client named, or provisa): not a PostgreSQL user or database. |
| The server's version | 1 | The server reports version 14.0.provisa, so a tool that gates features on the version behaves as with PostgreSQL 14: server_version_num is 140000. |
| An unsized source numeric is the numeric(38,s) registration samples | 1 | A registered numeric(p,s) column is described with its typmod (PGWIRE SENDS NUMERIC(P,S)). An unsized source numeric registers as numeric(38,s), s the largest scale among its values at registration, so an engine that cannot hold an unbounded numeric reads it exactly: it is described as that numeric(38,s), where PostgreSQL describes an unsized numeric. |
| The catalog's bootstrap owner is provisa | 1 | The pgwire catalog's bootstrap role (oid 10) is named provisa, not postgres: pg_get_userbyid(10) and every owner column answer provisa (THE CATALOG'S BOOTSTRAP OWNER IS PROVISA). |
| Internal functions | 558 | PostgreSQL itself cannot call these from SQL: each takes an internal argument, or runs only from a trigger, an event trigger, binary upgrade or initdb, or is the input or output function of a pseudo-type. They are parts of the server rather than functions a client calls. |
| Index functions | 44 | Indexes belong to the source database, and Provisa does not handle them: these are the support and maintenance functions of index access methods. Index metadata stays served. |

<details><summary>A source's array column is JSON (3)</summary>

| Case | Reason |
| --- | --- |
| type conversion::SELECT c_int4_array FROM conformance.types_fixture | type oid 3802 (jsonb), PostgreSQL 1007 (_int4); text '[1, 2, 3]', PostgreSQL '{1,2,3}'; binary 015b312c20322c20335d, PostgreSQL 0000000100000000000000170000000300000001000000040000000100000004000000020000000400000003 |
| type conversion::SELECT c_text_array FROM conformance.types_fixture | type oid 3802 (jsonb), PostgreSQL 1009 (_text); text '["a", "b", "c"]', PostgreSQL '{a,b,c}'; binary 015b2261222c202262222c202263225d, PostgreSQL 0000000100000000000000190000000300000001000000016100000001620000000163 |
| type conversion::SELECT c_float8_array FROM conformance.types_fixture | type oid 3802 (jsonb), PostgreSQL 1022 (_float8); text '[1.5, 2.5]', PostgreSQL '{1.5,2.5}'; binary 015b312e352c20322e355d, PostgreSQL 0000000100000000000002bd0000000200000001000000083ff8000000000000000000084004000000000000 |

</details>

<details><summary>The session's role and database (3)</summary>

| Case | Reason |
| --- | --- |
| type conversion::SELECT current_database() AS v | text 'provisa', PostgreSQL 'conformance'; binary 70726f76697361, PostgreSQL 636f6e666f726d616e6365 |
| type conversion::SELECT current_user AS v | text 'org_admin', PostgreSQL 'postgres'; binary 6f72675f61646d696e, PostgreSQL 706f737467726573 |
| type conversion::SELECT session_user AS v | text 'org_admin', PostgreSQL 'postgres'; binary 6f72675f61646d696e, PostgreSQL 706f737467726573 |

</details>

<details><summary>The server's version (1)</summary>

| Case | Reason |
| --- | --- |
| type conversion::SELECT pg_catalog.current_setting('server_version_num') AS v | text '140000', PostgreSQL '170011'; binary 313430303030, PostgreSQL 313730303131 |

</details>

<details><summary>An unsized source numeric is the numeric(38,s) registration samples (1)</summary>

| Case | Reason |
| --- | --- |
| type conversion::SELECT c_numeric FROM conformance.types_fixture | typmod 2490376, PostgreSQL -1 |

</details>

<details><summary>The catalog's bootstrap owner is provisa (1)</summary>

| Case | Reason |
| --- | --- |
| type conversion::SELECT pg_catalog.pg_get_userbyid(10) AS v | text 'provisa', PostgreSQL 'postgres'; binary 70726f76697361, PostgreSQL 706f737467726573 |

</details>

<details><summary>Internal functions (558)</summary>

| Case | Reason |
| --- | --- |
| catalog function::RI_FKey_cascade_del() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_cascade_del" was not called by trigger manager |
| catalog function::RI_FKey_cascade_upd() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_cascade_upd" was not called by trigger manager |
| catalog function::RI_FKey_check_ins() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_check_ins" was not called by trigger manager |
| catalog function::RI_FKey_check_upd() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_check_upd" was not called by trigger manager |
| catalog function::RI_FKey_noaction_del() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_noaction_del" was not called by trigger manager |
| catalog function::RI_FKey_noaction_upd() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_noaction_upd" was not called by trigger manager |
| catalog function::RI_FKey_restrict_del() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_restrict_del" was not called by trigger manager |
| catalog function::RI_FKey_restrict_upd() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_restrict_upd" was not called by trigger manager |
| catalog function::RI_FKey_setdefault_del() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_setdefault_del" was not called by trigger manager |
| catalog function::RI_FKey_setdefault_upd() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_setdefault_upd" was not called by trigger manager |
| catalog function::RI_FKey_setnull_del() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_setnull_del" was not called by trigger manager |
| catalog function::RI_FKey_setnull_upd() | no call reaches it: PostgreSQL refuses the call: ERROR: function "RI_FKey_setnull_upd" was not called by trigger manager |
| catalog function::anyarray_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::anycompatiblearray_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::areajoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::areasel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_array_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_array_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_array_finalfn(internal, anyarray) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_array_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_array_transfn(internal, anyarray) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_finalfn(internal, anynonarray) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_agg_transfn(internal, anynonarray) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_subscript_handler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_typanalyze(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::array_unnest_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::arraycontjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::arraycontsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bernoulli(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::big5_to_euc_tw(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::big5_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::big5_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::binary_upgrade_add_sub_rel_state(text, oid, "char", pg_lsn) | no call reaches it: PostgreSQL refuses the call: ERROR: function can only be called when server is in binary upgrade mode |
| catalog function::binary_upgrade_create_empty_extension(text, text, boolean, text, oid[], text[], text[]) | no call reaches it: PostgreSQL refuses the call: ERROR: function can only be called when server is in binary upgrade mode |
| catalog function::binary_upgrade_replorigin_advance(text, pg_lsn) | no call reaches it: PostgreSQL refuses the call: ERROR: function can only be called when server is in binary upgrade mode |
| catalog function::bit_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bool_accum(internal, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bool_accum_inv(internal, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bool_alltrue(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bool_anytrue(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::boolrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::box_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bpchar_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bpcharrecv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_bloom_add_value(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_bloom_consistent(internal, internal, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_bloom_opcinfo(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_bloom_options(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_bloom_summary_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_bloom_union(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_inclusion_add_value(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_inclusion_consistent(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_inclusion_opcinfo(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_inclusion_union(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_add_value(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_consistent(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_add_value(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_consistent(internal, internal, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_date(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_float4(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_float8(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_inet(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_int2(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_int4(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_int8(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_interval(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_macaddr(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_macaddr8(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_numeric(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_pg_lsn(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_tid(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_time(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_timestamp(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_timetz(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_distance_uuid(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_opcinfo(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_options(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_summary_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_multi_union(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_opcinfo(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brin_minmax_union(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::brinhandler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btbpchar_pattern_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btfloat4sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btfloat8sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bthandler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btint2sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btint4sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btint8sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btnamesortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::btoidsortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bttext_pattern_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bttextsortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bytea_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bytea_string_agg_finalfn(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bytea_string_agg_transfn(internal, bytea, bytea) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::bytearecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::cash_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::charrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::cidr_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::cidrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::circle_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::contjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::contsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::cstring_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::cume_dist_final(internal, VARIADIC "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::date_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::date_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dense_rank_final(internal, VARIADIC "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dispell_init(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dispell_lexize(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::domain_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dsimple_init(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dsimple_lexize(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dsnowball_init(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dsnowball_lexize(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dsynonym_init(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::dsynonym_lexize(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::elem_contained_by_range_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::enum_recv(internal, oid) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::eqjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::eqsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_cn_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_cn_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_jis_2004_to_shift_jis_2004(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_jis_2004_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_jp_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_jp_to_sjis(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_jp_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_kr_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_kr_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_tw_to_big5(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_tw_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::euc_tw_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::event_trigger_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type event_trigger |
| catalog function::event_trigger_out(event_trigger) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type event_trigger LINE 1: SELECT pg_catalog."event_trigger_out"(NULL::event_trigger) A... ^ |
| catalog function::fdw_handler_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type fdw_handler |
| catalog function::fdw_handler_out(fdw_handler) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type fdw_handler LINE 1: SELECT pg_catalog."fdw_handler_out"(NULL::fdw_handler) AS v ^ |
| catalog function::float4recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::float8recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gb18030_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gbk_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::generate_series_int4_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::generate_series_int8_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_cmp_prefix(text, text, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_consistent_jsonb(internal, smallint, jsonb, integer, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_consistent_jsonb_path(internal, smallint, jsonb, integer, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_jsonb(jsonb, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_jsonb_path(jsonb, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_jsonb_query(jsonb, internal, smallint, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_jsonb_query_path(jsonb, internal, smallint, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_tsquery(tsquery, internal, smallint, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_tsquery(tsquery, internal, smallint, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_tsquery(tsvector, internal, smallint, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_tsvector(tsvector, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_extract_tsvector(tsvector, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_triconsistent_jsonb(internal, smallint, jsonb, integer, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_triconsistent_jsonb_path(internal, smallint, jsonb, integer, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_tsquery_consistent(internal, smallint, tsquery, integer, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_tsquery_consistent(internal, smallint, tsquery, integer, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_tsquery_consistent(internal, smallint, tsvector, integer, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gin_tsquery_triconsistent(internal, smallint, tsvector, integer, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ginarrayconsistent(internal, smallint, anyarray, integer, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ginarrayextract(anyarray, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ginarrayextract(anyarray, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ginarraytriconsistent(internal, smallint, anyarray, integer, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ginhandler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ginqueryarrayextract(anyarray, internal, smallint, internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_box_consistent(internal, box, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_box_distance(internal, box, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_box_penalty(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_box_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_box_same(box, box, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_box_union(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_circle_compress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_circle_consistent(internal, circle, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_circle_distance(internal, circle, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_point_compress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_point_consistent(internal, point, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_point_distance(internal, point, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_point_fetch(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_point_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_poly_compress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_poly_consistent(internal, polygon, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gist_poly_distance(internal, polygon, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gisthandler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsquery_compress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsquery_consistent(internal, internal, integer, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsquery_consistent(internal, tsquery, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsquery_penalty(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsquery_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsquery_same(bigint, bigint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsquery_union(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_compress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_consistent(internal, gtsvector, integer, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_consistent(internal, tsvector, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_decompress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_options(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_penalty(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_same(gtsvector, gtsvector, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::gtsvector_union(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::hashhandler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::hashvarlena(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::hashvarlenaextended(internal, bigint) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::heap_tableam_handler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iclikejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iclikesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::icnlikejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::icnlikesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::icregexeqjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::icregexeqsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::icregexnejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::icregexnesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::index_am_handler_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type index_am_handler |
| catalog function::index_am_handler_out(index_am_handler) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type index_am_handler LINE 1: SELECT pg_catalog."index_am_handler_out"(NULL::index_am_hand... ^ |
| catalog function::inet_gist_compress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_gist_consistent(internal, inet, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_gist_fetch(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_gist_penalty(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_gist_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_gist_same(inet, inet, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_gist_union(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_spg_choose(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_spg_config(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_spg_inner_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_spg_leaf_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::inet_spg_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int2_accum(internal, smallint) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int2_accum_inv(internal, smallint) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int2recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int2vectorrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int4_accum(internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int4_accum_inv(internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int4recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8_accum(internal, bigint) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8_accum_inv(internal, bigint) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8_avg_accum(internal, bigint) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8_avg_accum_inv(internal, bigint) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8_avg_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8_avg_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8_avg_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8inc_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::int8recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::internal_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: functions returning type "internal" cannot be called explicitly LINE 1: SELECT pg_catalog."internal_in"(NULL::cstring) AS v ^ |
| catalog function::internal_out(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_avg(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_avg_accum(internal, interval) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_avg_accum_inv(internal, interval) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_avg_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_avg_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_avg_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_sum(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::interval_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iso8859_1_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iso8859_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iso_to_koi8r(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iso_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iso_to_win1251(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::iso_to_win866(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::johab_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_agg_finalfn(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_agg_strict_transfn(internal, anyelement) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_agg_transfn(internal, anyelement) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_object_agg_finalfn(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_object_agg_strict_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_object_agg_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_object_agg_unique_strict_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_object_agg_unique_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::json_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_agg_finalfn(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_agg_strict_transfn(internal, anyelement) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_agg_transfn(internal, anyelement) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_object_agg_finalfn(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_object_agg_strict_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_object_agg_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_object_agg_unique_strict_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_object_agg_unique_transfn(internal, "any", "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonb_subscript_handler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::jsonpath_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::koi8r_to_iso(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::koi8r_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::koi8r_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::koi8r_to_win1251(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::koi8r_to_win866(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::koi8u_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::language_handler_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type language_handler |
| catalog function::language_handler_out(language_handler) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type language_handler LINE 1: SELECT pg_catalog."language_handler_out"(NULL::language_hand... ^ |
| catalog function::latin1_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::latin2_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::latin2_to_win1250(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::latin3_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::latin4_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::likejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::likesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::line_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::lseg_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::macaddr8_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::macaddr_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::macaddr_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::matchingjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::matchingsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_big5(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_euc_cn(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_euc_jp(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_euc_kr(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_euc_tw(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_iso(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_koi8r(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_latin1(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_latin2(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_latin3(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_latin4(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_sjis(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_win1250(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_win1251(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mic_to_win866(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::mode_final(internal, anyelement) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::multirange_agg_finalfn(internal, anymultirange) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::multirange_agg_transfn(internal, anymultirange) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::multirange_gist_compress(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::multirange_gist_consistent(internal, anymultirange, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::multirange_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::multirange_typanalyze(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::multirangesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::namerecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::neqjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::neqsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::network_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::network_subset_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::networkjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::networksel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::nlikejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::nlikesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_accum(internal, numeric) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_accum_inv(internal, numeric) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_avg(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_avg_accum(internal, numeric) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_avg_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_avg_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_avg_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_avg(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_stddev_pop(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_stddev_samp(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_sum(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_var_pop(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_poly_var_samp(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_stddev_pop(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_stddev_samp(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_sum(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_var_pop(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::numeric_var_samp(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::oidrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::oidvectorrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ordered_set_transition(internal, "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::ordered_set_transition_multi(internal, VARIADIC "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::path_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::percent_rank_final(internal, VARIADIC "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::percentile_cont_float8_final(internal, double precision) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::percentile_cont_float8_multi_final(internal, double precision[]) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::percentile_cont_interval_final(internal, double precision) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::percentile_cont_interval_multi_final(internal, double precision[]) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::percentile_disc_final(internal, double precision, anyelement) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::percentile_disc_multi_final(internal, double precision[], anyelement) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_ddl_command_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_dependencies_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_event_trigger_ddl_commands(OUT classid oid, OUT objid oid, OUT objsubid integer, OUT command_tag text, OUT object_type text, OUT schema_name text, OUT object_identity text, OUT in_extension boolean, OUT command pg_ddl_command) | no call reaches it: PostgreSQL refuses the call: ERROR: pg_event_trigger_ddl_commands() can only be called in an event trigger function |
| catalog function::pg_event_trigger_dropped_objects(OUT classid oid, OUT objid oid, OUT objsubid integer, OUT original boolean, OUT normal boolean, OUT is_temporary boolean, OUT object_type text, OUT schema_name text, OUT object_name text, OUT object_identity text, OUT address_names text[], OUT address_args text[]) | no call reaches it: PostgreSQL refuses the call: ERROR: pg_event_trigger_dropped_objects() can only be called in a sql_drop event trigger function |
| catalog function::pg_event_trigger_table_rewrite_oid(OUT oid oid) | no call reaches it: PostgreSQL refuses the call: ERROR: pg_event_trigger_table_rewrite_oid() can only be called in a table_rewrite event trigger function |
| catalog function::pg_event_trigger_table_rewrite_reason() | no call reaches it: PostgreSQL refuses the call: ERROR: pg_event_trigger_table_rewrite_reason() can only be called in a table_rewrite event trigger function |
| catalog function::pg_lsn_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_mcv_list_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_ndistinct_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_node_tree_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_snapshot_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::pg_stop_making_pinned_objects() | no call reaches it: PostgreSQL refuses the call: ERROR: cannot advance OID counter anymore |
| catalog function::plpgsql_call_handler() | no call reaches it: PostgreSQL refuses the call: ERROR: PL/pgSQL functions cannot return type language_handler CONTEXT: compilation of PL/pgSQL function "plpgsql_call_handler" near line 1 |
| catalog function::plpgsql_inline_handler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::point_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::poly_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::positionjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::positionsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::prefixjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::prefixsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::prsd_end(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::prsd_headline(internal, internal, tsquery) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::prsd_lextype(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::prsd_nexttoken(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::prsd_start(internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_agg_finalfn(internal, anyrange) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_agg_transfn(internal, anyrange) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_contains_elem_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_gist_consistent(internal, anyrange, smallint, oid, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_gist_penalty(internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_gist_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_gist_same(anyrange, anyrange, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_gist_union(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::range_typanalyze(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::rangesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::rank_final(internal, VARIADIC "any") | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::raw_array_subscript_handler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::record_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regclassrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regcollationrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regconfigrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regdictionaryrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regexeqjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regexeqsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regexnejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regexnesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regnamespacerecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regoperatorrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regoperrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regprocedurerecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regprocrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regrolerecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::regtyperecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalargejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalargesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalargtjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalargtsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalarlejoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalarlesel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalarltjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::scalarltsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::shell_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of a shell type |
| catalog function::shift_jis_2004_to_euc_jis_2004(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::shift_jis_2004_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::sjis_to_euc_jp(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::sjis_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::sjis_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_bbox_quad_config(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_box_quad_choose(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_box_quad_config(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_box_quad_inner_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_box_quad_leaf_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_box_quad_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_kd_choose(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_kd_config(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_kd_inner_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_kd_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_quad_choose(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_quad_config(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_quad_inner_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_quad_leaf_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_quad_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_range_quad_choose(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_range_quad_config(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_range_quad_inner_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_range_quad_leaf_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_range_quad_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_text_choose(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_text_config(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_text_inner_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_text_leaf_consistent(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spg_text_picksplit(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::spghandler(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::string_agg_combine(internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::string_agg_deserialize(bytea, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::string_agg_finalfn(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::string_agg_serialize(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::string_agg_transfn(internal, text, text) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::suppress_redundant_updates_trigger() | no call reaches it: PostgreSQL refuses the call: ERROR: suppress_redundant_updates_trigger: must be called as trigger |
| catalog function::system(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::table_am_handler_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type table_am_handler |
| catalog function::table_am_handler_out(table_am_handler) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type table_am_handler LINE 1: SELECT pg_catalog."table_am_handler_out"(NULL::table_am_hand... ^ |
| catalog function::text_starts_with_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::texticlike_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::texticregexeq_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::textlike_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::textrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::textregexeq_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::thesaurus_init(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::thesaurus_lexize(internal, internal, internal, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::tidrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::time_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::time_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::timestamp_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::timestamp_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::timestamp_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::timestamptz_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::timetz_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::trigger_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type trigger |
| catalog function::trigger_out(trigger) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type trigger LINE 1: SELECT pg_catalog."trigger_out"(NULL::trigger) AS v ^ |
| catalog function::ts_typanalyze(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::tsm_handler_in(cstring) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type tsm_handler |
| catalog function::tsm_handler_out(tsm_handler) | no call reaches it: PostgreSQL refuses the call: ERROR: cannot accept a value of type tsm_handler LINE 1: SELECT pg_catalog."tsm_handler_out"(NULL::tsm_handler) AS v ^ |
| catalog function::tsmatchjoinsel(internal, oid, internal, smallint, internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::tsmatchsel(internal, oid, internal, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::tsqueryrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::tsvector_update_trigger() | no call reaches it: PostgreSQL refuses the call: ERROR: tsvector_update_trigger: not fired by trigger manager |
| catalog function::tsvector_update_trigger_column() | no call reaches it: PostgreSQL refuses the call: ERROR: tsvector_update_trigger: not fired by trigger manager |
| catalog function::tsvectorrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::txid_snapshot_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::uhc_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::unique_key_recheck() | no call reaches it: PostgreSQL refuses the call: ERROR: function "unique_key_recheck" was not called by trigger manager |
| catalog function::unknownrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_big5(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_euc_cn(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_euc_jis_2004(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_euc_jp(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_euc_kr(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_euc_tw(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_gb18030(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_gbk(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_iso8859(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_iso8859_1(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_johab(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_koi8r(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_koi8u(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_shift_jis_2004(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_sjis(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_uhc(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::utf8_to_win(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::uuid_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::uuid_sortsupport(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::varbit_recv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::varbit_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::varchar_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::varcharrecv(internal, oid, integer) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::void_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win1250_to_latin2(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win1250_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win1251_to_iso(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win1251_to_koi8r(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win1251_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win1251_to_win866(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win866_to_iso(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win866_to_koi8r(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win866_to_mic(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win866_to_win1251(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::win_to_utf8(integer, integer, cstring, internal, integer, boolean) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::window_cume_dist_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::window_dense_rank_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::window_ntile_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::window_percent_rank_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::window_rank_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::window_row_number_support(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::xid8recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::xidrecv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |
| catalog function::xml_recv(internal) | no call reaches it: takes an internal argument, which no SQL expression can supply |

</details>

<details><summary>Index functions (44)</summary>

| Case | Reason |
| --- | --- |
| catalog function::brin_bloom_summary_in(cstring) | not served: ERROR: Catalog Error: Scalar Function with name brin_bloom_summary_in does not exist! Did you mean "bin"? |
| catalog function::brin_bloom_summary_out(pg_brin_bloom_summary) | not served: ERROR: Catalog Error: Scalar Function with name brin_bloom_summary_out does not exist! Did you mean "bin"? |
| catalog function::brin_bloom_summary_send(pg_brin_bloom_summary) | not served: ERROR: Catalog Error: Scalar Function with name brin_bloom_summary_send does not exist! Did you mean "bin"? |
| catalog function::brin_desummarize_range(regclass, bigint) | not served: ERROR: brin_desummarize_range() has no form over the meta domain |
| catalog function::brin_minmax_multi_summary_in(cstring) | not served: ERROR: Catalog Error: Scalar Function with name brin_minmax_multi_summary_in does not exist! Did you mean "bin"? |
| catalog function::brin_minmax_multi_summary_out(pg_brin_minmax_multi_summary) | not served: ERROR: Catalog Error: Scalar Function with name brin_minmax_multi_summary_out does not exist! Did you mean "bin"? |
| catalog function::brin_minmax_multi_summary_send(pg_brin_minmax_multi_summary) | not served: ERROR: Catalog Error: Scalar Function with name brin_minmax_multi_summary_send does not exist! Did you mean "bin"? |
| catalog function::brin_summarize_new_values(regclass) | not served: ERROR: brin_summarize_new_values() has no form over the meta domain |
| catalog function::brin_summarize_range(regclass, bigint) | not served: ERROR: brin_summarize_range() has no form over the meta domain |
| catalog function::btarraycmp(anyarray, anyarray) | not served: ERROR: Catalog Error: Scalar Function with name btarraycmp does not exist! Did you mean "bar"? |
| catalog function::btboolcmp(boolean, boolean) | not served: ERROR: Catalog Error: Scalar Function with name btboolcmp does not exist! Did you mean "bool_or"? |
| catalog function::btbpchar_pattern_cmp(character, character) | not served: ERROR: Catalog Error: Scalar Function with name btbpchar_pattern_cmp does not exist! Did you mean "bar"? |
| catalog function::btcharcmp("char", "char") | not served: ERROR: Catalog Error: Scalar Function with name btcharcmp does not exist! Did you mean "bar"? |
| catalog function::btequalimage(oid) | not served: ERROR: Catalog Error: Scalar Function with name btequalimage does not exist! Did you mean "quantile"? |
| catalog function::btfloat48cmp(real, double precision) | not served: ERROR: Catalog Error: Scalar Function with name btfloat48cmp does not exist! Did you mean "current_localtime"? |
| catalog function::btfloat4cmp(real, real) | not served: ERROR: Catalog Error: Scalar Function with name btfloat4cmp does not exist! Did you mean "current_localtime"? |
| catalog function::btfloat84cmp(double precision, real) | not served: ERROR: Catalog Error: Scalar Function with name btfloat84cmp does not exist! Did you mean "current_localtime"? |
| catalog function::btfloat8cmp(double precision, double precision) | not served: ERROR: Catalog Error: Scalar Function with name btfloat8cmp does not exist! Did you mean "current_localtime"? |
| catalog function::btint24cmp(smallint, integer) | not served: ERROR: Catalog Error: Scalar Function with name btint24cmp does not exist! Did you mean "bin"? |
| catalog function::btint28cmp(smallint, bigint) | not served: ERROR: Catalog Error: Scalar Function with name btint28cmp does not exist! Did you mean "bin"? |
| catalog function::btint2cmp(smallint, smallint) | not served: ERROR: Catalog Error: Scalar Function with name btint2cmp does not exist! Did you mean "bin"? |
| catalog function::btint42cmp(integer, smallint) | not served: ERROR: Catalog Error: Scalar Function with name btint42cmp does not exist! Did you mean "bin"? |
| catalog function::btint48cmp(integer, bigint) | not served: ERROR: Catalog Error: Scalar Function with name btint48cmp does not exist! Did you mean "bin"? |
| catalog function::btint4cmp(integer, integer) | not served: ERROR: Catalog Error: Scalar Function with name btint4cmp does not exist! Did you mean "bin"? |
| catalog function::btint82cmp(bigint, smallint) | not served: ERROR: Catalog Error: Scalar Function with name btint82cmp does not exist! Did you mean "bin"? |
| catalog function::btint84cmp(bigint, integer) | not served: ERROR: Catalog Error: Scalar Function with name btint84cmp does not exist! Did you mean "bin"? |
| catalog function::btint8cmp(bigint, bigint) | not served: ERROR: Catalog Error: Scalar Function with name btint8cmp does not exist! Did you mean "bin"? |
| catalog function::btnamecmp(name, name) | not served: ERROR: Catalog Error: Scalar Function with name btnamecmp does not exist! Did you mean "html_escape"? |
| catalog function::btnametextcmp(name, text) | not served: ERROR: Catalog Error: Scalar Function with name btnametextcmp does not exist! Did you mean "to_timestamp"? |
| catalog function::btoidcmp(oid, oid) | not served: ERROR: Catalog Error: Scalar Function with name btoidcmp does not exist! Did you mean "trim"? |
| catalog function::btoidvectorcmp(oidvector, oidvector) | not served: ERROR: Catalog Error: Scalar Function with name btoidvectorcmp does not exist! Did you mean "bit_or"? |
| catalog function::btrecordcmp(record, record) | not served: ERROR: Catalog Error: Scalar Function with name btrecordcmp does not exist! Did you mean "cbrt"? |
| catalog function::btrecordimagecmp(record, record) | not served: ERROR: Catalog Error: Scalar Function with name btrecordimagecmp does not exist! Did you mean "trim"? |
| catalog function::bttext_pattern_cmp(text, text) | not served: ERROR: Catalog Error: Scalar Function with name bttext_pattern_cmp does not exist! Did you mean "exp"? |
| catalog function::bttextcmp(text, text) | not served: ERROR: Catalog Error: Scalar Function with name bttextcmp does not exist! Did you mean "st_astext"? |
| catalog function::bttextnamecmp(text, name) | not served: ERROR: Catalog Error: Scalar Function with name bttextnamecmp does not exist! Did you mean "to_timestamp"? |
| catalog function::bttidcmp(tid, tid) | not served: ERROR: Catalog Error: Scalar Function with name bttidcmp does not exist! Did you mean "trim"? |
| catalog function::btvarstrequalimage(oid) | not served: ERROR: Catalog Error: Scalar Function with name btvarstrequalimage does not exist! Did you mean "getvariable"? |
| catalog function::gin_clean_pending_list(regclass) | not served: ERROR: gin_clean_pending_list() has no form over the meta domain |
| catalog function::gin_cmp_tslexeme(text, text) | not served: ERROR: Catalog Error: Scalar Function with name gin_cmp_tslexeme does not exist! Did you mean "pg_sleep"? |
| catalog function::gin_compare_jsonb(text, text) | not served: ERROR: Catalog Error: Scalar Function with name gin_compare_jsonb does not exist! Did you mean "provisa_jsonb_insert"? |
| catalog function::gtsvectorin(cstring) | not served: ERROR: Catalog Error: Scalar Function with name gtsvectorin does not exist! Did you mean "bitstring"? |
| catalog function::gtsvectorout(gtsvector) | not served: ERROR: Catalog Error: Scalar Function with name gtsvectorout does not exist! Did you mean "second"? |
| catalog function::spg_poly_quad_compress(polygon) | not served: ERROR: Catalog Error: Scalar Function with name spg_poly_quad_compress does not exist! Did you mean "pg_conf_load_time"? |

</details>


## Failed cases

None.
<!-- conformance-results:end -->

## Deliberate differences

### Writes

`INSERT`, `UPDATE`, and `DELETE` are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name. `BEGIN`/`COMMIT`/`ROLLBACK` are a no-op by design.

### Vocabulary fixed by the model

`CREATE TABLE`, `ALTER TABLE`, and DDL statements are not supported. The model is the source of schema.

Postgres extensions are not loadable via `CREATE EXTENSION`. Where an extension function is commonly needed, Provisa offers it as a command.

### Add-ins

Extension functions (`pg_trgm`, `pg_vector`, PostGIS, etc.) are not available unless exposed as commands. Extension functions refused by name appear in the refused by name column of the function results table above.

### geometry and geography in binary format

A `geometry` or `geography` column is reported with its PostGIS type name and is sent in text format only, as WKT. PostGIS's binary format (EWKB) is not sent; a client that asks for the column in binary format gets an error naming it.

### Types whose text does not give their binary form

A column of `aclitem`, `gtsvector`, `tsquery`, `regprocedure`, `regoper`, `regoperator`, `regrole`, `regconfig`, `regdictionary`, `regcollation`, `pg_ndistinct`, `pg_dependencies`, `pg_mcv_list`, `pg_brin_bloom_summary` or `pg_brin_minmax_multi_summary` is held as PostgreSQL's canonical text and sent in text format only, because its binary form holds what only PostgreSQL can derive. A client that asks for one in binary gets an error naming the type.

### Catalog functions

Provisa intercepts the scalar catalog functions tools call to introspect schemas and format type names. Each one is answered from the role's governed model view:

| Function | Behavior |
|---|---|
| `format_type(typid, typmod)` | Returns the type name tools display in schema panels. Subtypes report by their subtype name (`inet`, `macaddr`, `money`), not the base type. |
| `col_description(relid, colnum)` | Returns the column's description from the model. Columns with no description return `NULL`. |
| `pg_get_constraintdef(oid)` | Returns a text rendering of the constraint (`PRIMARY KEY (col)`, `FOREIGN KEY (col) REFERENCES other(col)`, or `UNIQUE (col)`) derived from the model's declared keys and joins. |
| `pg_get_expr(expr, relid)` | Returns the default expression of a column read from `pg_attrdef.adbin`, such as `now()`, `current_setting('app.user')` or `'new'::character varying`. A default is the value your role's insert grant presets on the column. A column with no preset, and any other expression column, returns `NULL`. |
| `has_table_privilege([user,] table, privilege)` | Answers for your role. `SELECT` is held on every table you are served. `INSERT`, `UPDATE` and `DELETE` are held where your role has the `write` right and a write grant for that operation on a table whose source takes it. A list such as `'INSERT, UPDATE'` is true when any item is held. `REFERENCES`, `TRIGGER`, `TRUNCATE` and any `WITH GRANT OPTION` are false. The table is an oid or a name, and the user must be your own role. A table that does not exist, or another user, returns `NULL`. |
| `current_schema()` | Returns `'public'`. |
| `obj_description(oid, catalog)` | Returns the description of the named catalog object (table or schema) from the model, or `NULL`. |

Each function is answered in the catalog intercept layer alongside `pg_catalog` and `information_schema` tables; none reaches a backend engine. A call with arguments Provisa cannot resolve returns `NULL`, matching PostgreSQL's own behavior for missing objects.

Every overload of every `pg_catalog` function is called the way PostgreSQL requires it, with the `VARIADIC` form, `WITHIN GROUP`, `OVER`, or a real argument where PostgreSQL refuses a `NULL`. Each overload then has a result on each engine, because what an engine can answer is that engine's own. A function refused by name on an engine is that engine's result, and a wrong answer on any engine is a defect.

Two groups are skipped by design and listed under the pass rate. Internal functions cannot be called by PostgreSQL itself: each takes an `internal` argument, or PostgreSQL refuses the call whatever the server's state. Index functions are the access method support and index maintenance functions, such as `brin_summarize_range`. Indexes belong to the source database, so these are not served. Index metadata is unaffected, and `pg_index`, `pg_get_indexdef` and the like stay served.

<!-- function-results:begin -->
### Function results by engine

Measured against PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2). Each overload has a result on each engine. Each result is that engine's own, and the rows do not add up to a product result.

| Engine | Exact | Equivalent | Refused by name | Differs: wrong answer | Differs: engine error |
| --- | ---: | ---: | ---: | ---: | ---: |
| Postgres | 2647 | 14 | 10 | 55 | 22 |
| DuckDB | 1778 | 13 | 120 | 94 | 743 |
| Trino | 955 | 4 | 335 | 49 | 1405 |
| ClickHouse | 717 | 1 | 493 | 155 | 1382 |

| Result | Meaning |
| --- | --- |
| Exact | The engine answered every call as PostgreSQL did. |
| Equivalent | The answer passes under a numbered rule below, linked from its row. |
| Refused by name | Provisa refused the call and said what it refuses. |
| Differs: wrong answer | The answer is wrong. This is a defect. |
| Differs: engine error | The engine returned an error and no named refusal. This is a defect. |

An exact result marked *form only* could not be compared by value, so its form was checked. One marked *call only* was called with `NULL`, which proves its result type and nothing about its answer. `n/a` means PostgreSQL itself refuses every call.

### Equivalence rules

<a id="rule-json-spacing"></a>**1. Whitespace in JSON.** JSON compares as parsed JSON, so spacing between its tokens is no difference. This covers a json or jsonb answer and a JSON value inside a record, such as the value `json_each` returns on DuckDB. Overloads covered: Postgres 1, DuckDB 1, Trino 0, ClickHouse 0.

<a id="rule-enum-label-text"></a>**2. Enum label sent as text.** Provisa holds an enum as text. The label is the same, and the column reports `text` where PostgreSQL reports the enum's own type oid. Overloads covered: Postgres 10, DuckDB 0, Trino 0, ClickHouse 0.

<a id="rule-row-order"></a>**3. Rows compare as a set.** These calls have no `ORDER BY`, so nothing fixes the order of their rows. The engine returned the same rows in another order. Overloads covered: Postgres 0, DuckDB 0, Trino 0, ClickHouse 0.

<a id="rule-number-value"></a>**4. Numbers equal by value.** `2.50` and `2.5` are the same number. A number compares by value, so its trailing zeros and scale are no difference. Overloads covered: Postgres 0, DuckDB 8, Trino 2, ClickHouse 1.

<a id="rule-model-answer"></a>**5. Answers describe the model.** `database_to_xmlschema` and its kin describe the database they run on. Provisa answers for its model and PostgreSQL for its own catalog, so the contents differ. The result type matches and both answers are XML. Overloads covered: Postgres 3, DuckDB 1, Trino 1, ClickHouse 0.

<a id="rule-float-tolerance"></a>**6. Floats within one part in 10^14.** Math libraries differ in the last digits of a float, so `real` and `double precision` answers compare within one part in 10^14. Overloads covered: Postgres 0, DuckDB 3, Trino 1, ClickHouse 0.

<a id="rule-json-key-order"></a>**7. JSON key order.** A JSON object has no key order. The same keys and values in another order are the same answer. Overloads covered: Postgres 0, DuckDB 0, Trino 0, ClickHouse 0.

<a id="rule-array-order"></a>**8. Array built without ORDER BY.** Only an `ORDER BY` in the statement fixes the order of an array an aggregate builds. Without one, its elements compare in any order. Overloads covered: Postgres 0, DuckDB 0, Trino 0, ClickHouse 0.

### No engine serves

55 functions have no overload that any engine answers exactly or equivalently, nor do 71 overloads.

| Family | Functions | Overloads |
| --- | ---: | ---: |
| Aggregate and window | 1 | 8 |
| Array | 1 | 3 |
| Date | 1 | 3 |
| Index support | 4 | 4 |
| JSON | 6 | 6 |
| Numeric | 2 | 3 |
| Other | 3 | 3 |
| Range | 1 | 5 |
| String | 1 | 1 |
| System | 4 | 4 |
| Type operators | 31 | 31 |

### Results by family

Each cell reads exact / equivalent / refused by name / differs.

| Family | Postgres | DuckDB | Trino | ClickHouse |
| --- | ---: | ---: | ---: | ---: |
| Aggregate and window | 77 / 2 / 0 / 8 | 47 / 0 / 0 / 40 | 1 / 0 / 0 / 86 | 0 / 0 / 1 / 86 |
| Aggregate support | 15 / 0 / 0 / 0 | 0 / 0 / 0 / 15 | 0 / 0 / 0 / 15 | 0 / 0 / 0 / 15 |
| Array | 33 / 0 / 0 / 3 | 19 / 0 / 0 / 17 | 0 / 0 / 3 / 33 | 0 / 0 / 3 / 33 |
| Binary upgrade | 16 / 0 / 0 / 0 | 14 / 0 / 0 / 2 | 2 / 0 / 0 / 14 | 0 / 0 / 14 / 2 |
| Date | 113 / 0 / 0 / 3 | 100 / 0 / 3 / 13 | 7 / 0 / 28 / 81 | 5 / 0 / 29 / 82 |
| Encoding conversion | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Enum | 7 / 6 / 0 / 0 | 0 / 0 / 0 / 13 | 0 / 0 / 0 / 13 | 0 / 0 / 0 / 13 |
| Geometric | 207 / 0 / 0 / 0 | 37 / 0 / 0 / 170 | 32 / 0 / 0 / 175 | 0 / 0 / 37 / 170 |
| Hash | 44 / 0 / 0 / 0 | 0 / 0 / 0 / 44 | 0 / 0 / 0 / 44 | 0 / 0 / 0 / 44 |
| Index support | 41 / 0 / 4 / 0 | 1 / 0 / 4 / 40 | 0 / 0 / 4 / 41 | 0 / 0 / 5 / 40 |
| JSON | 95 / 1 / 2 / 4 | 24 / 1 / 73 / 4 | 9 / 0 / 81 / 12 | 8 / 0 / 87 / 7 |
| Large object | 20 / 0 / 0 / 0 | 20 / 0 / 0 / 0 | 9 / 0 / 0 / 11 | 0 / 0 / 20 / 0 |
| Network | 33 / 0 / 0 / 0 | 11 / 0 / 19 / 3 | 4 / 0 / 5 / 24 | 0 / 0 / 2 / 31 |
| Numeric | 289 / 0 / 0 / 3 | 254 / 11 / 3 / 24 | 69 / 1 / 101 / 121 | 70 / 0 / 101 / 121 |
| Other | 84 / 0 / 2 / 1 | 69 / 0 / 2 / 16 | 20 / 0 / 7 / 60 | 17 / 0 / 14 / 56 |
| Planner support | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Range | 91 / 0 / 0 / 20 | 95 / 0 / 0 / 16 | 81 / 0 / 0 / 30 | 29 / 0 / 0 / 82 |
| String | 199 / 0 / 0 / 2 | 160 / 0 / 6 / 35 | 36 / 0 / 36 / 129 | 19 / 0 / 58 / 124 |
| System | 476 / 0 / 2 / 2 | 467 / 0 / 2 / 11 | 369 / 0 / 23 / 88 | 322 / 0 / 73 / 85 |
| Text search | 87 / 0 / 0 / 0 | 4 / 0 / 0 / 83 | 2 / 0 / 1 / 84 | 0 / 0 / 3 / 84 |
| Trigger | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Type operators | 699 / 2 / 0 / 31 | 435 / 0 / 8 / 289 | 301 / 2 / 38 / 391 | 247 / 1 / 36 / 448 |
| XML | 21 / 3 / 0 / 0 | 21 / 1 / 0 / 2 | 13 / 1 / 8 / 2 | 0 / 0 / 10 / 14 |

### Wrong answers

<details><summary>Postgres (55)</summary>

| Overload | Difference |
| --- | --- |
| `any_in(cstring)` | result type oids [25], PostgreSQL [2276] |
| `anycompatible_in(cstring)` | result type oids [25], PostgreSQL [5077] |
| `anycompatible_out(anycompatible)` | result type oids [25], PostgreSQL [2275] |
| `anycompatiblearray_in(cstring)` | result type oids [25], PostgreSQL [5078] |
| `anycompatiblemultirange_in(cstring, oid, integer)` | result type oids [25], PostgreSQL [4538] |
| `anycompatiblemultirange_out(anycompatiblemultirange)` | result type oids [25], PostgreSQL [2275] |
| `anycompatiblenonarray_in(cstring)` | result type oids [25], PostgreSQL [5079] |
| `anycompatiblenonarray_out(anycompatiblenonarray)` | result type oids [25], PostgreSQL [2275] |
| `anycompatiblerange_in(cstring, oid, integer)` | result type oids [25], PostgreSQL [5080] |
| `anycompatiblerange_out(anycompatiblerange)` | result type oids [25], PostgreSQL [2275] |
| `anyelement_in(cstring)` | result type oids [25], PostgreSQL [2283] |
| `anyenum_out(anyenum)` | result type oids [25], PostgreSQL [2275] |
| `anymultirange_in(cstring, oid, integer)` | result type oids [25], PostgreSQL [4537] |
| `anynonarray_in(cstring)` | result type oids [25], PostgreSQL [2776] |
| `anynonarray_out(anynonarray)` | result type oids [25], PostgreSQL [2275] |
| `anyrange_in(cstring, oid, integer)` | result type oids [25], PostgreSQL [3831] |
| `array_agg(anyarray)` | '{[1]}', PostgreSQL '{{1}}' |
| `array_fill(anyelement, integer[])` | '{"[[3, 3, 3], [3, 3, 3]]"}', PostgreSQL '{{{3,3,3},{3,3,3}}}' |
| `daterange(date, date)` | '(None, None)', PostgreSQL '(,)' |
| `daterange(date, date, text)` | '(None, None)', PostgreSQL '(,)' |
| `daterange_canonical(daterange)` | '[2024-01-01, 2024-02-01)', PostgreSQL '[2024-01-01,2024-02-01)' |
| `domain_in(cstring, oid, integer)` | result type oids [25], PostgreSQL [2276] |
| `enum_out(anyenum)` | result type oids [25], PostgreSQL [2275] |
| `int4range(integer, integer)` | '(None, None)', PostgreSQL '(,)' |
| `int4range(integer, integer, text)` | '(None, None)', PostgreSQL '(,)' |
| `int4range_canonical(int4range)` | '[1, 5)', PostgreSQL '[1,5)' |
| `int8range(bigint, bigint)` | '(None, None)', PostgreSQL '(,)' |
| `int8range(bigint, bigint, text)` | '(None, None)', PostgreSQL '(,)' |
| `int8range_canonical(int8range)` | '[1, 5)', PostgreSQL '[1,5)' |
| `json_populate_record(base anyelement, from_json json, use_json_as_text boolean)` | result type oids [25], PostgreSQL [10047] |
| `json_populate_recordset(base anyelement, from_json json, use_json_as_text boolean)` | result type oids [25], PostgreSQL [10047] |
| `jsonb_populate_record(anyelement, jsonb)` | result type oids [25], PostgreSQL [10047] |
| `jsonb_populate_recordset(anyelement, jsonb)` | result type oids [25], PostgreSQL [10047] |
| `max(character)` | result type oids [25], PostgreSQL [1042] |
| `min(character)` | result type oids [25], PostgreSQL [1042] |
| `multirange_in(cstring, oid, integer)` | result type oids [25], PostgreSQL [4537] |
| `numrange(numeric, numeric)` | '(None, None)', PostgreSQL '(,)' |
| `numrange(numeric, numeric, text)` | '(None, None)', PostgreSQL '(,)' |
| `pg_ddl_command_in(cstring)` | result type oids [25], PostgreSQL [32] |
| `pg_stat_get_bgwriter_stat_reset_time()` | None, PostgreSQL '2026-11-01 15:21:49.03569+00' |
| `pg_stat_get_checkpointer_stat_reset_time()` | None, PostgreSQL '2026-11-01 15:21:49.03569+00' |
| `range_in(cstring, oid, integer)` | result type oids [25], PostgreSQL [3831] |
| `range_intersect(anyrange, anyrange)` | '[1, 5)', PostgreSQL '[1,5)' |
| `range_merge(anymultirange)` | '[1, 9)', PostgreSQL '[1,9)' |
| `range_merge(anyrange, anyrange)` | '[1, 5)', PostgreSQL '[1,5)' |
| `range_union(anyrange, anyrange)` | '[1, 5)', PostgreSQL '[1,5)' |
| `set_config(text, text, boolean)` | None, PostgreSQL 'conformance' |
| `system_user()` | None, PostgreSQL 'scram-sha-256:postgres' |
| `timeofday()` | 'Sun Nov 01 15:26:55.207108 2026 UTC' is not the form of PostgreSQL's 'Mon Nov 02 00:26:55.205112 2026 JST' |
| `tsrange(timestamp without time zone, timestamp without time zone)` | '(None, None)', PostgreSQL '(,)' |
| `tsrange(timestamp without time zone, timestamp without time zone, text)` | '(None, None)', PostgreSQL '(,)' |
| `tstzrange(timestamp with time zone, timestamp with time zone)` | '(None, None)', PostgreSQL '(,)' |
| `tstzrange(timestamp with time zone, timestamp with time zone, text)` | '(None, None)', PostgreSQL '(,)' |
| `unnest(anymultirange)` | '[1, 5)', PostgreSQL '[1,5)' |
| `xpath(text, xml)` | '{"{","}"}', PostgreSQL '{}' |

</details>

<details><summary>DuckDB (94)</summary>

| Overload | Difference |
| --- | --- |
| `array_agg(anyarray)` | result type oids [114], PostgreSQL [1007] |
| `array_cat(anycompatiblearray, anycompatiblearray)` | '{}', PostgreSQL None |
| `array_position(anycompatiblearray, anycompatible, integer)` | '2', PostgreSQL '3' |
| `array_remove(anycompatiblearray, anycompatible)` | '{-7}', PostgreSQL '{-7,NULL}' |
| `avg(bigint)` | result type oids [701], PostgreSQL [1700] |
| `avg(integer)` | result type oids [701], PostgreSQL [1700] |
| `avg(interval)` | '-3 mons +1 day 00:41:01.333334', PostgreSQL '-3 mons +1 day 00:41:01.333333' |
| `avg(numeric)` | result type oids [701], PostgreSQL [1700] |
| `avg(smallint)` | result type oids [701], PostgreSQL [1700] |
| `bit(bigint, integer)` | '11', PostgreSQL '011' |
| `bit(integer, integer)` | '11', PostgreSQL '011' |
| `bpchar(character, integer, boolean)` | 'ab', PostgreSQL 'a' |
| `bpchar_larger(character, character)` | 'abc', PostgreSQL 'a' |
| `bpchar_smaller(character, character)` | 'abc', PostgreSQL 'a' |
| `bpchariclike(character, text)` | 't', PostgreSQL 'f' |
| `bpcharicnlike(character, text)` | 'f', PostgreSQL 't' |
| `bpcharicregexeq(character, text)` | 't', PostgreSQL 'f' |
| `bpcharicregexne(character, text)` | 'f', PostgreSQL 't' |
| `bpcharlike(character, text)` | 't', PostgreSQL 'f' |
| `bpcharnlike(character, text)` | 'f', PostgreSQL 't' |
| `bpcharregexeq(character, text)` | 't', PostgreSQL 'f' |
| `bpcharregexne(character, text)` | 'f', PostgreSQL 't' |
| `cash_out(money)` | result type oids [23], PostgreSQL [2275] |
| `char_length(character)` | '3', PostgreSQL '1' |
| `character_length(character)` | '3', PostgreSQL '1' |
| `current_query()` | 'SELECT CURRENT_QUERY() AS v' is not the form of PostgreSQL's 'SELECT pg_catalog."current_query"() AS v' |
| `exp(numeric)` | '0.0008047330101246132', PostgreSQL '0.0008047330101246133' |
| `icu_unicode_version()` | None, PostgreSQL '16.0' |
| `length(character)` | '3', PostgreSQL '1' |
| `ln(numeric)` | '6.907755278982137', PostgreSQL '6.9077552789821371' |
| `max(character)` | result type oids [25], PostgreSQL [1042] |
| `max(money)` | result type oids [1700], PostgreSQL [790] |
| `max(oid)` | result type oids [20], PostgreSQL [26] |
| `max(pg_lsn)` | result type oids [25], PostgreSQL [3220] |
| `max(text)` | 'abc', PostgreSQL 'Hello World' |
| `min(character)` | result type oids [25], PostgreSQL [1042] |
| `min(money)` | result type oids [1700], PostgreSQL [790] |
| `min(numeric)` | '-7', PostgreSQL '-7.125' |
| `min(oid)` | result type oids [20], PostgreSQL [26] |
| `min(pg_lsn)` | result type oids [25], PostgreSQL [3220] |
| `mul_d_interval(double precision, interval)` | '-4860:00:00', PostgreSQL '-6 mons -22 days -12:00:00' |
| `name(character)` | 'abc', PostgreSQL 'a' |
| `netmask(inet)` | '255.255.255.0/24', PostgreSQL '255.255.255.0' |
| `ntile(integer)` | result type oids [20], PostgreSQL [23] |
| `octet_length(character)` | '3', PostgreSQL '1' |
| `pg_encoding_max_length(integer)` | '2', PostgreSQL None |
| `pg_size_pretty(numeric)` | '3 bytes', PostgreSQL '2.5 bytes' |
| `pg_stat_get_bgwriter_stat_reset_time()` | None, PostgreSQL '2026-11-01 15:21:47.787194+00' |
| `pg_stat_get_checkpointer_stat_reset_time()` | None, PostgreSQL '2026-11-01 15:21:47.787194+00' |
| `pow(numeric, numeric)` | '9.882117688026186', PostgreSQL '9.8821176880261854' |
| `power(numeric, numeric)` | '9.882117688026186', PostgreSQL '9.8821176880261854' |
| `regexp_replace(text, text, text, integer)` | 'aabcc', PostgreSQL 'abc' |
| `regexp_replace(text, text, text, integer, integer)` | 'aabcc', PostgreSQL 'abc' |
| `round(double precision)` | '1', PostgreSQL '0' |
| `set_config(text, text, boolean)` | None, PostgreSQL 'conformance' |
| `sqrt(numeric)` | '1.5811388300841898', PostgreSQL '1.581138830084190' |
| `stddev(bigint)` | result type oids [701], PostgreSQL [1700] |
| `stddev(integer)` | result type oids [701], PostgreSQL [1700] |
| `stddev(numeric)` | result type oids [701], PostgreSQL [1700] |
| `stddev(smallint)` | result type oids [701], PostgreSQL [1700] |
| `stddev_pop(bigint)` | result type oids [701], PostgreSQL [1700] |
| `stddev_pop(integer)` | result type oids [701], PostgreSQL [1700] |
| `stddev_pop(numeric)` | result type oids [701], PostgreSQL [1700] |
| `stddev_pop(smallint)` | result type oids [701], PostgreSQL [1700] |
| `stddev_samp(bigint)` | result type oids [701], PostgreSQL [1700] |
| `stddev_samp(integer)` | result type oids [701], PostgreSQL [1700] |
| `stddev_samp(numeric)` | result type oids [701], PostgreSQL [1700] |
| `stddev_samp(smallint)` | result type oids [701], PostgreSQL [1700] |
| `substr(text, integer)` | 'o World', PostgreSQL 'Hello World' |
| `sum(bigint)` | result type oids [20], PostgreSQL [1700] |
| `sum(money)` | result type oids [1700], PostgreSQL [790] |
| `sum(numeric)` | '996', PostgreSQL '995.375' |
| `sum(real)` | result type oids [701], PostgreSQL [700] |
| `system_user()` | None, PostgreSQL 'scram-sha-256:postgres' |
| `text(character)` | 'abc', PostgreSQL 'a' |
| `text(inet)` | '10.1.2.3', PostgreSQL '10.1.2.3/32' |
| `timeofday()` | 'Sun Nov 01 15:28:52.574000 2026 UTC' is not the form of PostgreSQL's 'Mon Nov 02 00:28:52.571921 2026 JST' |
| `to_hex(bigint)` | 'FFFFFFFFFFFFFFF9', PostgreSQL 'fffffffffffffff9' |
| `to_hex(integer)` | 'FFFFFFFFFFFFFFF9', PostgreSQL 'fffffff9' |
| `unnest(anymultirange)` | result type oids [23], PostgreSQL [3904] |
| `uuid_extract_version(uuid)` | '0', PostgreSQL None |
| `var_pop(bigint)` | result type oids [701], PostgreSQL [1700] |
| `var_pop(integer)` | result type oids [701], PostgreSQL [1700] |
| `var_pop(numeric)` | result type oids [701], PostgreSQL [1700] |
| `var_pop(smallint)` | result type oids [701], PostgreSQL [1700] |
| `var_samp(bigint)` | result type oids [701], PostgreSQL [1700] |
| `var_samp(integer)` | result type oids [701], PostgreSQL [1700] |
| `var_samp(numeric)` | result type oids [701], PostgreSQL [1700] |
| `var_samp(smallint)` | result type oids [701], PostgreSQL [1700] |
| `varchar(character varying, integer, boolean)` | 'ab', PostgreSQL 'abc' |
| `variance(bigint)` | result type oids [701], PostgreSQL [1700] |
| `variance(integer)` | result type oids [701], PostgreSQL [1700] |
| `variance(numeric)` | result type oids [701], PostgreSQL [1700] |
| `variance(smallint)` | result type oids [701], PostgreSQL [1700] |

</details>

<details><summary>Trino (49)</summary>

| Overload | Difference |
| --- | --- |
| `array_larger(anyarray, anyarray)` | result type oids [25], PostgreSQL [1007] |
| `array_smaller(anyarray, anyarray)` | result type oids [25], PostgreSQL [1007] |
| `box_center(box)` | '(null,null)', PostgreSQL None |
| `cash_div_int2(money, smallint)` | '$23.81', PostgreSQL '$23.80' |
| `cash_div_int4(money, integer)` | '$23.81', PostgreSQL '$23.80' |
| `cash_send(money)` | result type oids [25], PostgreSQL [17] |
| `center(box)` | '(null,null)', PostgreSQL None |
| `center(circle)` | '(null,null)', PostgreSQL None |
| `circle_center(circle)` | '(null,null)', PostgreSQL None |
| `diagonal(box)` | '[(null,null),(null,null)]', PostgreSQL None |
| `icu_unicode_version()` | None, PostgreSQL '16.0' |
| `isempty(anyrange)` | result type oids [25], PostgreSQL [16] |
| `loread(integer, integer)` | result type oids [25], PostgreSQL [17] |
| `lower_inc(anyrange)` | result type oids [25], PostgreSQL [16] |
| `lower_inf(anyrange)` | result type oids [25], PostgreSQL [16] |
| `money(numeric)` | '$3.00', PostgreSQL '$2.50' |
| `numeric_abs(numeric)` | '3', PostgreSQL '2.5' |
| `numeric_add(numeric, numeric)` | '6', PostgreSQL '5.0' |
| `numeric_inc(numeric)` | '4', PostgreSQL '3.5' |
| `numeric_larger(numeric, numeric)` | '3', PostgreSQL '2.5' |
| `numeric_mul(numeric, numeric)` | '9', PostgreSQL '6.25' |
| `numeric_smaller(numeric, numeric)` | '3', PostgreSQL '2.5' |
| `numeric_uminus(numeric)` | '-3', PostgreSQL '-2.5' |
| `numeric_uplus(numeric)` | '3', PostgreSQL '2.5' |
| `numrange(numeric, numeric, text)` | '[-7,-7]', PostgreSQL '[-7.125,-7.125]' |
| `pg_blocking_pids(integer)` | result type oids [25], PostgreSQL [1007] |
| `pg_dependencies_send(pg_dependencies)` | result type oids [25], PostgreSQL [17] |
| `pg_mcv_list_send(pg_mcv_list)` | result type oids [25], PostgreSQL [17] |
| `pg_ndistinct_send(pg_ndistinct)` | result type oids [25], PostgreSQL [17] |
| `pg_node_tree_send(pg_node_tree)` | result type oids [25], PostgreSQL [17] |
| `pg_read_binary_file(text)` | result type oids [25], PostgreSQL [17] |
| `pg_read_binary_file(text, bigint, bigint)` | result type oids [25], PostgreSQL [17] |
| `pg_read_binary_file(text, bigint, bigint, boolean)` | result type oids [25], PostgreSQL [17] |
| `pg_read_binary_file(text, boolean)` | result type oids [25], PostgreSQL [17] |
| `pg_safe_snapshot_blocking_pids(integer)` | result type oids [25], PostgreSQL [1007] |
| `pg_stat_get_bgwriter_stat_reset_time()` | None, PostgreSQL '2026-11-01 15:21:47.282129+00' |
| `pg_stat_get_checkpointer_stat_reset_time()` | None, PostgreSQL '2026-11-01 15:21:47.282129+00' |
| `range_adjacent(anyrange, anyrange)` | result type oids [25], PostgreSQL [16] |
| `range_cmp(anyrange, anyrange)` | result type oids [25], PostgreSQL [23] |
| `scale(numeric)` | '0', PostgreSQL '1' |
| `set_config(text, text, boolean)` | None, PostgreSQL 'conformance' |
| `sha256(bytea)` | result type oids [25], PostgreSQL [17] |
| `sha512(bytea)` | result type oids [25], PostgreSQL [17] |
| `system_user()` | None, PostgreSQL 'scram-sha-256:postgres' |
| `timestamp_mi(timestamp without time zone, timestamp without time zone)` | result type oids [25], PostgreSQL [1186] |
| `timestamptz_mi(timestamp with time zone, timestamp with time zone)` | result type oids [25], PostgreSQL [1186] |
| `upper_inc(anymultirange)` | result type oids [25], PostgreSQL [16] |
| `upper_inc(anyrange)` | result type oids [25], PostgreSQL [16] |
| `upper_inf(anyrange)` | result type oids [25], PostgreSQL [16] |

</details>

<details><summary>ClickHouse (155)</summary>

| Overload | Difference |
| --- | --- |
| `bpcharicregexeq(character, text)` | 't', PostgreSQL 'f' |
| `bpcharicregexne(character, text)` | 'f', PostgreSQL 't' |
| `bpcharregexeq(character, text)` | 't', PostgreSQL 'f' |
| `bpcharregexne(character, text)` | 'f', PostgreSQL 't' |
| `cash_div_int2(money, smallint)` | '$23.81', PostgreSQL '$23.80' |
| `cash_div_int4(money, integer)` | '$23.81', PostgreSQL '$23.80' |
| `cidr(inet)` | None, PostgreSQL '::1/128' |
| `cursor_to_xml(cursor refcursor, count integer, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `cursor_to_xmlschema(cursor refcursor, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `database_to_xml(nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `database_to_xmlschema(nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `elem_contained_by_multirange(anyelement, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `elem_contained_by_range(anyelement, anyrange)` | result type oids [17], PostgreSQL [16] |
| `hostmask(inet)` | None, PostgreSQL '::' |
| `icu_unicode_version()` | None, PostgreSQL '16.0' |
| `inetnot(inet)` | None, PostgreSQL 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe' |
| `int2_sum(bigint, smallint)` | '0', PostgreSQL '6' |
| `int4_sum(bigint, integer)` | '0', PostgreSQL '6' |
| `isempty(anymultirange)` | result type oids [17], PostgreSQL [16] |
| `isempty(anyrange)` | result type oids [17], PostgreSQL [16] |
| `json_array_element(from_json json, element_index integer)` | result type oids [25], PostgreSQL [114] |
| `json_object_field(from_json json, field_name text)` | result type oids [25], PostgreSQL [114] |
| `jsonb_array_element(from_json jsonb, element_index integer)` | result type oids [25], PostgreSQL [3802] |
| `jsonb_object_field(from_json jsonb, field_name text)` | result type oids [25], PostgreSQL [3802] |
| `lower_inc(anymultirange)` | result type oids [17], PostgreSQL [16] |
| `lower_inc(anyrange)` | result type oids [17], PostgreSQL [16] |
| `lower_inf(anymultirange)` | result type oids [17], PostgreSQL [16] |
| `lower_inf(anyrange)` | result type oids [17], PostgreSQL [16] |
| `masklen(inet)` | None, PostgreSQL '128' |
| `money(numeric)` | '$2.00', PostgreSQL '$2.50' |
| `multirange_adjacent_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_adjacent_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `multirange_after_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_after_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `multirange_before_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_before_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `multirange_contained_by_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_contained_by_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `multirange_contains_elem(anymultirange, anyelement)` | result type oids [17], PostgreSQL [16] |
| `multirange_contains_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_contains_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `multirange_eq(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_ge(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_gt(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_le(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_lt(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_ne(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_overlaps_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_overlaps_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `multirange_overleft_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_overleft_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `multirange_overright_multirange(anymultirange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `multirange_overright_range(anymultirange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `numeric_abs(numeric)` | '2', PostgreSQL '2.5' |
| `numeric_add(numeric, numeric)` | '4', PostgreSQL '5.0' |
| `numeric_inc(numeric)` | '3', PostgreSQL '3.5' |
| `numeric_larger(numeric, numeric)` | '2', PostgreSQL '2.5' |
| `numeric_mul(numeric, numeric)` | '4', PostgreSQL '6.25' |
| `numeric_pl_pg_lsn(numeric, pg_lsn)` | '00/03F5', PostgreSQL '0/16B374B' |
| `numeric_smaller(numeric, numeric)` | '2', PostgreSQL '2.5' |
| `numeric_uminus(numeric)` | '-2', PostgreSQL '-2.5' |
| `numeric_uplus(numeric)` | '2', PostgreSQL '2.5' |
| `numrange(numeric, numeric, text)` | '[-7,-7]', PostgreSQL '[-7.125,-7.125]' |
| `pg_dependencies_send(pg_dependencies)` | result type oids [25], PostgreSQL [17] |
| `pg_last_xact_replay_timestamp()` | result type oids [20], PostgreSQL [1184] |
| `pg_lsn_larger(pg_lsn, pg_lsn)` | '00/016B3748', PostgreSQL '0/16B3748' |
| `pg_lsn_mii(pg_lsn, numeric)` | '00/03F1', PostgreSQL '0/16B3746' |
| `pg_lsn_pli(pg_lsn, numeric)` | '00/03F5', PostgreSQL '0/16B374B' |
| `pg_lsn_smaller(pg_lsn, pg_lsn)` | '00/016B3748', PostgreSQL '0/16B3748' |
| `pg_mcv_list_send(pg_mcv_list)` | result type oids [25], PostgreSQL [17] |
| `pg_ndistinct_send(pg_ndistinct)` | result type oids [25], PostgreSQL [17] |
| `pg_node_tree_send(pg_node_tree)` | result type oids [25], PostgreSQL [17] |
| `pg_stat_get_backend_activity_start(integer)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_backend_start(integer)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_backend_xact_start(integer)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_bgwriter_stat_reset_time()` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_checkpointer_stat_reset_time()` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_db_checksum_last_failure(oid)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_db_stat_reset_time(oid)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_last_analyze_time(oid)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_last_autoanalyze_time(oid)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_last_autovacuum_time(oid)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_last_vacuum_time(oid)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_lastscan(oid)` | result type oids [20], PostgreSQL [1184] |
| `pg_stat_get_snapshot_timestamp()` | result type oids [20], PostgreSQL [1184] |
| `query_to_xml(query text, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `query_to_xml_and_xmlschema(query text, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `query_to_xmlschema(query text, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `range_adjacent(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_adjacent_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_after(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_after_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_before(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_before_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_cmp(anyrange, anyrange)` | result type oids [17], PostgreSQL [23] |
| `range_contained_by(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_contained_by_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_contains(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_contains_elem(anyrange, anyelement)` | result type oids [17], PostgreSQL [16] |
| `range_contains_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_eq(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_ge(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_gt(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_le(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_lt(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_ne(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_overlaps(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_overlaps_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_overleft(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_overleft_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_overright(anyrange, anyrange)` | result type oids [17], PostgreSQL [16] |
| `range_overright_multirange(anyrange, anymultirange)` | result type oids [17], PostgreSQL [16] |
| `range_union(anyrange, anyrange)` | result type oids [17], PostgreSQL [3904] |
| `record_eq(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_ge(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_gt(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_image_eq(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_image_ge(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_image_gt(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_image_le(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_image_lt(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_image_ne(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_le(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_lt(record, record)` | result type oids [17], PostgreSQL [16] |
| `record_ne(record, record)` | result type oids [17], PostgreSQL [16] |
| `schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `set_config(text, text, boolean)` | None, PostgreSQL 'conformance' |
| `statement_timestamp()` | result type oids [20], PostgreSQL [1184] |
| `system_user()` | None, PostgreSQL 'scram-sha-256:postgres' |
| `table_to_xml(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `table_to_xml_and_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `table_to_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | result type oids [17], PostgreSQL [142] |
| `time_cmp(time without time zone, time without time zone)` | None, PostgreSQL '0' |
| `time_eq(time without time zone, time without time zone)` | None, PostgreSQL 't' |
| `time_ge(time without time zone, time without time zone)` | None, PostgreSQL 't' |
| `time_gt(time without time zone, time without time zone)` | None, PostgreSQL 'f' |
| `time_larger(time without time zone, time without time zone)` | result type oids [17], PostgreSQL [1083] |
| `time_le(time without time zone, time without time zone)` | None, PostgreSQL 't' |
| `time_lt(time without time zone, time without time zone)` | None, PostgreSQL 'f' |
| `time_mi_time(time without time zone, time without time zone)` | result type oids [20], PostgreSQL [1186] |
| `time_ne(time without time zone, time without time zone)` | None, PostgreSQL 'f' |
| `time_smaller(time without time zone, time without time zone)` | result type oids [17], PostgreSQL [1083] |
| `timestamp_larger(timestamp without time zone, timestamp without time zone)` | result type oids [20], PostgreSQL [1114] |
| `timestamp_mi(timestamp without time zone, timestamp without time zone)` | result type oids [20], PostgreSQL [1186] |
| `timestamp_smaller(timestamp without time zone, timestamp without time zone)` | result type oids [20], PostgreSQL [1114] |
| `timestamptz_larger(timestamp with time zone, timestamp with time zone)` | result type oids [20], PostgreSQL [1184] |
| `timestamptz_mi(timestamp with time zone, timestamp with time zone)` | result type oids [20], PostgreSQL [1186] |
| `timestamptz_smaller(timestamp with time zone, timestamp with time zone)` | result type oids [20], PostgreSQL [1184] |
| `upper_inc(anymultirange)` | result type oids [17], PostgreSQL [16] |
| `upper_inc(anyrange)` | result type oids [17], PostgreSQL [16] |
| `upper_inf(anymultirange)` | result type oids [17], PostgreSQL [16] |
| `upper_inf(anyrange)` | result type oids [17], PostgreSQL [16] |

</details>

### Every overload

<details><summary>Aggregate and window (87)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `any_value(anyelement)` | exact | exact | differs (error) | differs (error) |
| `avg(bigint)` | exact | differs | differs (error) | differs (error) |
| `avg(integer)` | exact | differs | differs (error) | differs (error) |
| `avg(smallint)` | exact | differs | differs (error) | differs (error) |
| `bit_and(bigint)` | exact | exact | differs (error) | differs (error) |
| `bit_and(bit)` | exact | exact | differs (error) | differs (error) |
| `bit_and(integer)` | exact | exact | differs (error) | differs (error) |
| `bit_and(smallint)` | exact | exact | differs (error) | differs (error) |
| `bit_or(bigint)` | exact | exact | differs (error) | differs (error) |
| `bit_or(bit)` | exact | exact | differs (error) | differs (error) |
| `bit_or(integer)` | exact | exact | differs (error) | differs (error) |
| `bit_or(smallint)` | exact | exact | differs (error) | differs (error) |
| `bit_xor(bigint)` | exact | exact | differs (error) | differs (error) |
| `bit_xor(bit)` | exact | exact | differs (error) | differs (error) |
| `bit_xor(integer)` | exact | exact | differs (error) | differs (error) |
| `bit_xor(smallint)` | exact | exact | differs (error) | differs (error) |
| `bool_and(boolean)` | exact | exact | differs (error) | differs (error) |
| `bool_or(boolean)` | exact | exact | differs (error) | differs (error) |
| `count()` | exact | exact | differs (error) | differs (error) |
| `count("any")` | exact | exact | differs (error) | differs (error) |
| `cume_dist()` | exact | exact | differs (error) | differs (error) |
| `cume_dist(VARIADIC "any" ORDER BY VARIADIC "any")` | differs (error) | differs (error) | differs (error) | differs (error) |
| `dense_rank()` | exact | exact | differs (error) | differs (error) |
| `dense_rank(VARIADIC "any" ORDER BY VARIADIC "any")` | differs (error) | differs (error) | differs (error) | differs (error) |
| `every(boolean)` | exact | exact | exact | refused |
| `first_value(anyelement)` | exact | exact | differs (error) | differs (error) |
| `lag(anycompatible, integer, anycompatible)` | exact | exact | differs (error) | differs (error) |
| `lag(anyelement)` | exact | exact | differs (error) | differs (error) |
| `lag(anyelement, integer)` | exact | exact | differs (error) | differs (error) |
| `last_value(anyelement)` | exact | exact | differs (error) | differs (error) |
| `lead(anycompatible, integer, anycompatible)` | exact | exact | differs (error) | differs (error) |
| `lead(anyelement)` | exact | exact | differs (error) | differs (error) |
| `lead(anyelement, integer)` | exact | exact | differs (error) | differs (error) |
| `max(anyarray)` | exact | exact | differs (error) | differs (error) |
| `max(anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `max(bigint)` | exact | exact | differs (error) | differs (error) |
| `max(character)` | differs | differs | differs (error) | differs (error) |
| `max(integer)` | exact | exact | differs (error) | differs (error) |
| `max(oid)` | exact | differs | differs (error) | differs (error) |
| `max(pg_lsn)` | exact | differs | differs (error) | differs (error) |
| `max(smallint)` | exact | exact | differs (error) | differs (error) |
| `max(text)` | exact | differs | differs (error) | differs (error) |
| `max(tid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `max(xid8)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `min(anyarray)` | exact | exact | differs (error) | differs (error) |
| `min(anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `min(bigint)` | exact | exact | differs (error) | differs (error) |
| `min(character)` | differs | differs | differs (error) | differs (error) |
| `min(integer)` | exact | exact | differs (error) | differs (error) |
| `min(oid)` | exact | differs | differs (error) | differs (error) |
| `min(pg_lsn)` | exact | differs | differs (error) | differs (error) |
| `min(smallint)` | exact | exact | differs (error) | differs (error) |
| `min(text)` | exact | exact | differs (error) | differs (error) |
| `min(tid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `min(xid8)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `mode(ORDER BY anyelement)` | differs (error) | differs (error) | differs (error) | differs (error) |
| `nth_value(anyelement, integer)` | exact | exact | differs (error) | differs (error) |
| `ntile(integer)` | exact | differs | differs (error) | differs (error) |
| `percent_rank()` | exact | exact | differs (error) | differs (error) |
| `percent_rank(VARIADIC "any" ORDER BY VARIADIC "any")` | differs (error) | differs (error) | differs (error) | differs (error) |
| `percentile_disc(double precision[] ORDER BY anyelement)` | differs (error) | differs (error) | differs (error) | differs (error) |
| `rank()` | exact | exact | differs (error) | differs (error) |
| `rank(VARIADIC "any" ORDER BY VARIADIC "any")` | differs (error) | differs (error) | differs (error) | differs (error) |
| `row_number()` | exact | exact | differs (error) | differs (error) |
| `stddev(bigint)` | exact | differs | differs (error) | differs (error) |
| `stddev(integer)` | exact | differs | differs (error) | differs (error) |
| `stddev(smallint)` | exact | differs | differs (error) | differs (error) |
| `stddev_pop(bigint)` | exact | differs | differs (error) | differs (error) |
| `stddev_pop(integer)` | exact | differs | differs (error) | differs (error) |
| `stddev_pop(smallint)` | exact | differs | differs (error) | differs (error) |
| `stddev_samp(bigint)` | exact | differs | differs (error) | differs (error) |
| `stddev_samp(integer)` | exact | differs | differs (error) | differs (error) |
| `stddev_samp(smallint)` | exact | differs | differs (error) | differs (error) |
| `string_agg(value bytea, delimiter bytea)` | exact | differs (error) | differs (error) | differs (error) |
| `string_agg(value text, delimiter text)` | exact | differs (error) | differs (error) | differs (error) |
| `sum(bigint)` | exact | differs | differs (error) | differs (error) |
| `sum(integer)` | exact | exact | differs (error) | differs (error) |
| `sum(smallint)` | exact | exact | differs (error) | differs (error) |
| `var_pop(bigint)` | exact | differs | differs (error) | differs (error) |
| `var_pop(integer)` | exact | differs | differs (error) | differs (error) |
| `var_pop(smallint)` | exact | differs | differs (error) | differs (error) |
| `var_samp(bigint)` | exact | differs | differs (error) | differs (error) |
| `var_samp(integer)` | exact | differs | differs (error) | differs (error) |
| `var_samp(smallint)` | exact | differs | differs (error) | differs (error) |
| `variance(bigint)` | exact | differs | differs (error) | differs (error) |
| `variance(integer)` | exact | differs | differs (error) | differs (error) |
| `variance(smallint)` | exact | differs | differs (error) | differs (error) |

</details>

<details><summary>Aggregate support (15)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `any_value_transfn(anyelement, anyelement)` | exact | differs (error) | differs (error) | differs (error) |
| `booland_statefunc(boolean, boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `boolor_statefunc(boolean, boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `float4_accum(double precision[], real)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_accum(double precision[], double precision)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_combine(double precision[], double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_accum(double precision[], double precision, double precision)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_combine(double precision[], double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int2_avg_accum(bigint[], smallint)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int2_avg_accum_inv(bigint[], smallint)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int4_avg_accum(bigint[], integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int4_avg_accum_inv(bigint[], integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int4_avg_combine(bigint[], bigint[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `multirange_intersect_agg_transfn(anymultirange, anymultirange)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `range_intersect_agg_transfn(anyrange, anyrange)` | exact (call only) | differs (error) | differs (error) | differs (error) |

</details>

<details><summary>Array (36)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `array_agg(anyarray)` | differs | differs | differs (error) | differs (error) |
| `array_agg(anynonarray)` | exact | exact | differs (error) | differs (error) |
| `array_append(anycompatiblearray, anycompatible)` | exact | exact | differs (error) | differs (error) |
| `array_cat(anycompatiblearray, anycompatiblearray)` | exact | differs | differs (error) | differs (error) |
| `array_dims(anyarray)` | exact | differs (error) | differs (error) | differs (error) |
| `array_eq(anyarray, anyarray)` | exact | exact | differs (error) | differs (error) |
| `array_fill(anyelement, integer[])` | differs | differs (error) | differs (error) | differs (error) |
| `array_fill(anyelement, integer[], integer[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `array_ge(anyarray, anyarray)` | exact | exact | differs (error) | differs (error) |
| `array_gt(anyarray, anyarray)` | exact | exact | differs (error) | differs (error) |
| `array_larger(anyarray, anyarray)` | exact | exact | differs | differs (error) |
| `array_le(anyarray, anyarray)` | exact | exact | differs (error) | differs (error) |
| `array_length(anyarray, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `array_lower(anyarray, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `array_lt(anyarray, anyarray)` | exact | exact | differs (error) | differs (error) |
| `array_ndims(anyarray)` | exact | differs (error) | differs (error) | differs (error) |
| `array_ne(anyarray, anyarray)` | exact | exact | differs (error) | differs (error) |
| `array_position(anycompatiblearray, anycompatible)` | exact | exact | differs (error) | differs (error) |
| `array_position(anycompatiblearray, anycompatible, integer)` | exact | differs | differs (error) | differs (error) |
| `array_positions(anycompatiblearray, anycompatible)` | exact | differs (error) | differs (error) | differs (error) |
| `array_prepend(anycompatible, anycompatiblearray)` | exact | exact | differs (error) | differs (error) |
| `array_remove(anycompatiblearray, anycompatible)` | exact | differs | differs (error) | differs (error) |
| `array_replace(anycompatiblearray, anycompatible, anycompatible)` | exact | differs (error) | differs (error) | differs (error) |
| `array_sample(anyarray, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `array_shuffle(anyarray)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `array_smaller(anyarray, anyarray)` | exact | exact | differs | differs (error) |
| `array_to_string(anyarray, text)` | exact | exact | differs (error) | differs (error) |
| `array_to_string(anyarray, text, text)` | exact | exact | differs (error) | differs (error) |
| `array_upper(anyarray, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `arraycontained(anyarray, anyarray)` | exact | exact | refused | refused |
| `arraycontains(anyarray, anyarray)` | exact | exact | refused | refused |
| `arrayoverlap(anyarray, anyarray)` | exact | exact | refused | refused |
| `cardinality(anyarray)` | exact | exact | differs (error) | differs (error) |
| `generate_subscripts(anyarray, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `generate_subscripts(anyarray, integer, boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `unnest(anyarray)` | exact | exact | differs (error) | differs (error) |

</details>

<details><summary>Binary upgrade (16)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `binary_upgrade_logical_slot_has_caught_up(name)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `binary_upgrade_set_missing_value(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_array_pg_type_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_heap_pg_class_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_heap_relfilenode(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_index_pg_class_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_index_relfilenode(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_multirange_array_pg_type_oid(oid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `binary_upgrade_set_next_multirange_pg_type_oid(oid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `binary_upgrade_set_next_pg_authid_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_pg_enum_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_pg_tablespace_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_pg_type_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_toast_pg_class_oid(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_next_toast_relfilenode(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `binary_upgrade_set_record_init_privs(boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |

</details>

<details><summary>Date (116)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `age(timestamp with time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `age(timestamp with time zone, timestamp with time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `age(timestamp without time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `age(timestamp without time zone, timestamp without time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `avg(interval)` | exact | differs | differs (error) | differs (error) |
| `clock_timestamp()` | exact (form only) | exact (form only) | refused | refused |
| `date(timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `date(timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `date_add(timestamp with time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `date_add(timestamp with time zone, interval, text)` | exact | differs (error) | differs (error) | differs (error) |
| `date_part(text, date)` | exact | exact | differs (error) | differs (error) |
| `date_part(text, interval)` | exact | exact | differs (error) | differs (error) |
| `date_part(text, time with time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `date_part(text, time without time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `date_part(text, timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `date_part(text, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `date_trunc(text, interval)` | exact | exact | differs (error) | differs (error) |
| `date_trunc(text, timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `date_trunc(text, timestamp with time zone, text)` | exact | differs (error) | differs (error) | differs (error) |
| `date_trunc(text, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `daterange_subdiff(date, date)` | exact | exact | exact | exact |
| `datetime_pl(date, time without time zone)` | exact | exact | refused | refused |
| `datetimetz_pl(date, time with time zone)` | exact | exact | refused | refused |
| `extract(text, date)` | exact | exact | differs (error) | differs (error) |
| `extract(text, interval)` | exact | exact | differs (error) | differs (error) |
| `extract(text, time with time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `extract(text, time without time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `extract(text, timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `extract(text, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `generate_series(timestamp with time zone, timestamp with time zone, interval)` | exact | differs (error) | differs (error) | differs (error) |
| `generate_series(timestamp with time zone, timestamp with time zone, interval, text)` | exact | differs (error) | differs (error) | differs (error) |
| `generate_series(timestamp without time zone, timestamp without time zone, interval)` | exact | differs (error) | differs (error) | differs (error) |
| `in_range(date, date, interval, boolean, boolean)` | exact | exact | differs (error) | differs (error) |
| `in_range(interval, interval, interval, boolean, boolean)` | exact | exact | differs (error) | differs (error) |
| `in_range(time with time zone, time with time zone, interval, boolean, boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `in_range(time without time zone, time without time zone, interval, boolean, boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `in_range(timestamp with time zone, timestamp with time zone, interval, boolean, boolean)` | exact | exact | differs (error) | differs (error) |
| `in_range(timestamp without time zone, timestamp without time zone, interval, boolean, boolean)` | exact | exact | differs (error) | differs (error) |
| `integer_pl_date(integer, date)` | exact | exact | refused | refused |
| `interval(interval, integer)` | exact | exact | refused | refused |
| `interval(time without time zone)` | exact | exact | refused | refused |
| `isfinite(date)` | exact | exact | differs (error) | differs (error) |
| `isfinite(interval)` | exact | exact | differs (error) | differs (error) |
| `isfinite(timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `isfinite(timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `justify_days(interval)` | exact | exact | refused | refused |
| `justify_hours(interval)` | exact | exact | refused | refused |
| `justify_interval(interval)` | exact | exact | refused | refused |
| `make_interval(years integer, months integer, weeks integer, days integer, hours integer, mins integer, secs double precision)` | exact | exact | differs (error) | differs (error) |
| `max(date)` | exact | exact | differs (error) | differs (error) |
| `max(interval)` | exact | exact | differs (error) | differs (error) |
| `max(time with time zone)` | exact | exact | differs (error) | differs (error) |
| `max(time without time zone)` | exact | exact | differs (error) | differs (error) |
| `max(timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `max(timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `min(date)` | exact | exact | differs (error) | differs (error) |
| `min(interval)` | exact | exact | differs (error) | differs (error) |
| `min(time with time zone)` | exact | exact | differs (error) | differs (error) |
| `min(time without time zone)` | exact | exact | differs (error) | differs (error) |
| `min(timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `min(timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `mul_d_interval(double precision, interval)` | exact | differs | refused | refused |
| `overlaps(time with time zone, time with time zone, time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `overlaps(time without time zone, interval, time without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `overlaps(time without time zone, interval, time without time zone, time without time zone)` | exact | exact | differs (error) | differs (error) |
| `overlaps(time without time zone, time without time zone, time without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `overlaps(time without time zone, time without time zone, time without time zone, time without time zone)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp with time zone, interval, timestamp with time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp with time zone, interval, timestamp with time zone, timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp without time zone, interval, timestamp without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp without time zone, interval, timestamp without time zone, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `percentile_cont(double precision ORDER BY interval)` | differs (error) | differs (error) | differs (error) | differs (error) |
| `percentile_cont(double precision[] ORDER BY interval)` | differs (error) | differs (error) | differs (error) | differs (error) |
| `pg_replication_origin_xact_setup(pg_lsn, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_sleep_for(interval)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_sleep_until(timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `statement_timestamp()` | exact (form only) | exact (form only) | exact (form only) | differs |
| `sum(interval)` | exact | differs (error) | differs (error) | differs (error) |
| `time(interval)` | exact | exact | refused | refused |
| `time(time with time zone)` | exact | exact | refused | refused |
| `time(time without time zone, integer)` | exact | exact | refused | refused |
| `time(timestamp with time zone)` | exact | exact | refused | refused |
| `time(timestamp without time zone)` | exact | exact | refused | refused |
| `timedate_pl(time without time zone, date)` | exact | exact | refused | refused |
| `timeofday()` | differs | differs | refused | refused |
| `timestamp(date)` | exact | exact | differs (error) | differs (error) |
| `timestamp(date, time without time zone)` | exact | exact | refused | refused |
| `timestamp(timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `timestamp(timestamp without time zone, integer)` | exact | exact | refused | refused |
| `timestamptz(date)` | exact | exact | refused | refused |
| `timestamptz(date, time with time zone)` | exact | exact | refused | refused |
| `timestamptz(date, time without time zone)` | exact | exact | refused | refused |
| `timestamptz(timestamp with time zone, integer)` | exact | exact | refused | refused |
| `timestamptz(timestamp without time zone)` | exact | exact | refused | refused |
| `timetz(time with time zone, integer)` | exact | exact | refused | refused |
| `timetz(time without time zone)` | exact | exact | refused | refused |
| `timetz(timestamp with time zone)` | exact | exact | refused | refused |
| `timetzdate_pl(time with time zone, date)` | exact | exact | refused | refused |
| `timezone(interval, time with time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `timezone(interval, timestamp with time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `timezone(interval, timestamp without time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `timezone(text, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timezone(text, timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `timezone(text, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `timezone(time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timezone(timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `timezone(timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `to_char(interval, text)` | exact | refused | differs (error) | differs (error) |
| `to_char(timestamp with time zone, text)` | exact | refused | differs (error) | differs (error) |
| `to_char(timestamp without time zone, text)` | exact | refused | differs (error) | differs (error) |
| `tsrange_subdiff(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `tstzrange_subdiff(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |

</details>

<details><summary>Enum (13)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `enum_cmp(anyenum, anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `enum_eq(anyenum, anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `enum_first(anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `enum_ge(anyenum, anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `enum_gt(anyenum, anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `enum_larger(anyenum, anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `enum_last(anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `enum_le(anyenum, anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `enum_lt(anyenum, anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `enum_ne(anyenum, anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `enum_range(anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `enum_range(anyenum, anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `enum_smaller(anyenum, anyenum)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |

</details>

<details><summary>Geometric (207)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `area(box)` | exact | exact | exact | refused |
| `area(circle)` | exact | exact | exact | refused |
| `area(path)` | exact | exact | exact | refused |
| `bound_box(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box(circle)` | exact | differs (error) | differs (error) | differs (error) |
| `box(point)` | exact | differs (error) | differs (error) | differs (error) |
| `box(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `box(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `box_above(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_above_eq(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_add(box, point)` | exact | differs (error) | differs (error) | differs (error) |
| `box_below(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_below_eq(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_center(box)` | exact | exact | differs | refused |
| `box_contain(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_contain_pt(box, point)` | exact | differs (error) | differs (error) | differs (error) |
| `box_contained(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_distance(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_div(box, point)` | exact | differs (error) | differs (error) | differs (error) |
| `box_eq(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_ge(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_gt(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_intersect(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_le(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_left(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_lt(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_mul(box, point)` | exact | differs (error) | differs (error) | differs (error) |
| `box_overabove(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_overbelow(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_overlap(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_overleft(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_overright(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_right(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_same(box, box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_sub(box, point)` | exact | differs (error) | differs (error) | differs (error) |
| `center(box)` | exact | exact | differs | refused |
| `center(circle)` | exact | exact | differs | refused |
| `circle(box)` | exact | differs (error) | differs (error) | differs (error) |
| `circle(point, double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `circle(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_above(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_add_pt(circle, point)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_below(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_center(circle)` | exact | exact | differs | refused |
| `circle_contain(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_contain_pt(circle, point)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_contained(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_distance(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_div_pt(circle, point)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_eq(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_ge(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_gt(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_le(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_left(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_lt(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_mul_pt(circle, point)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_ne(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_overabove(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_overbelow(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_overlap(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_overleft(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_overright(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_right(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_same(circle, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_sub_pt(circle, point)` | exact | differs (error) | differs (error) | differs (error) |
| `close_ls(line, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `close_lseg(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `close_pb(point, box)` | exact | differs (error) | differs (error) | differs (error) |
| `close_pl(point, line)` | exact | differs (error) | differs (error) | differs (error) |
| `close_ps(point, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `close_sb(lseg, box)` | exact | differs (error) | differs (error) | differs (error) |
| `diagonal(box)` | exact | exact | differs | refused |
| `diameter(circle)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_bp(box, point)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_bs(box, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_cpoint(circle, point)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_cpoly(circle, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_lp(line, point)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_ls(line, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_pathp(path, point)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_pb(point, box)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_pc(point, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_pl(point, line)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_polyc(polygon, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_polyp(polygon, point)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_ppath(point, path)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_ppoly(point, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_ps(point, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_sb(lseg, box)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_sl(lseg, line)` | exact | differs (error) | differs (error) | differs (error) |
| `dist_sp(lseg, point)` | exact | differs (error) | differs (error) | differs (error) |
| `height(box)` | exact | exact | exact | refused |
| `inter_lb(line, box)` | exact | differs (error) | differs (error) | differs (error) |
| `inter_sb(lseg, box)` | exact | differs (error) | differs (error) | differs (error) |
| `inter_sl(lseg, line)` | exact | differs (error) | differs (error) | differs (error) |
| `isclosed(path)` | exact | exact | exact | refused |
| `ishorizontal(line)` | exact | exact | exact | refused |
| `ishorizontal(lseg)` | exact | exact | exact | refused |
| `ishorizontal(point, point)` | exact | exact | exact | refused |
| `isopen(path)` | exact | exact | exact | refused |
| `isparallel(line, line)` | exact | exact | exact | refused |
| `isparallel(lseg, lseg)` | exact | exact | exact | refused |
| `isperp(line, line)` | exact | exact | exact | refused |
| `isperp(lseg, lseg)` | exact | exact | exact | refused |
| `isvertical(line)` | exact | exact | exact | refused |
| `isvertical(lseg)` | exact | exact | exact | refused |
| `isvertical(point, point)` | exact | exact | exact | refused |
| `length(lseg)` | exact | exact | exact | refused |
| `length(path)` | exact | exact | exact | refused |
| `line(point, point)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `line_distance(line, line)` | exact | differs (error) | differs (error) | differs (error) |
| `line_eq(line, line)` | exact | differs (error) | differs (error) | differs (error) |
| `line_horizontal(line)` | exact | exact | exact | refused |
| `line_interpt(line, line)` | exact | differs (error) | differs (error) | differs (error) |
| `line_intersect(line, line)` | exact | differs (error) | differs (error) | differs (error) |
| `line_parallel(line, line)` | exact | exact | exact | refused |
| `line_perp(line, line)` | exact | exact | exact | refused |
| `line_vertical(line)` | exact | exact | exact | refused |
| `lseg(box)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_center(lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_distance(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_eq(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_ge(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_gt(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_horizontal(lseg)` | exact | exact | exact | refused |
| `lseg_interpt(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_intersect(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_le(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_length(lseg)` | exact | exact | exact | refused |
| `lseg_lt(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_ne(lseg, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_parallel(lseg, lseg)` | exact | exact | exact | refused |
| `lseg_perp(lseg, lseg)` | exact | exact | exact | refused |
| `lseg_vertical(lseg)` | exact | exact | exact | refused |
| `npoints(path)` | exact | differs (error) | differs (error) | differs (error) |
| `npoints(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `on_pb(point, box)` | exact | differs (error) | differs (error) | differs (error) |
| `on_pl(point, line)` | exact | differs (error) | differs (error) | differs (error) |
| `on_ppath(point, path)` | exact | differs (error) | differs (error) | differs (error) |
| `on_ps(point, lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `on_sb(lseg, box)` | exact | differs (error) | differs (error) | differs (error) |
| `on_sl(lseg, line)` | exact | differs (error) | differs (error) | differs (error) |
| `path(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `path_add(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_add_pt(path, point)` | exact | differs (error) | differs (error) | differs (error) |
| `path_contain_pt(path, point)` | exact | differs (error) | differs (error) | differs (error) |
| `path_distance(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_div_pt(path, point)` | exact | differs (error) | differs (error) | differs (error) |
| `path_inter(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_length(path)` | exact | exact | exact | refused |
| `path_mul_pt(path, point)` | exact | differs (error) | differs (error) | differs (error) |
| `path_n_eq(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_n_ge(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_n_gt(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_n_le(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_n_lt(path, path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_npoints(path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_sub_pt(path, point)` | exact | differs (error) | differs (error) | differs (error) |
| `pclose(path)` | exact | differs (error) | differs (error) | differs (error) |
| `point(box)` | exact | differs (error) | differs (error) | differs (error) |
| `point(circle)` | exact | differs (error) | differs (error) | differs (error) |
| `point(double precision, double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `point(lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `point(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `point_above(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_add(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_below(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_distance(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_div(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_eq(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_horiz(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_left(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_mul(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_ne(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_right(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_sub(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_vert(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_above(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_below(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_center(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_contain(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_contain_pt(polygon, point)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_contained(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_distance(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_left(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_npoints(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_overabove(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_overbelow(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_overlap(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_overleft(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_overright(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_right(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_same(polygon, polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `polygon(box)` | exact | differs (error) | differs (error) | differs (error) |
| `polygon(circle)` | exact | differs (error) | differs (error) | differs (error) |
| `polygon(integer, circle)` | exact | differs (error) | differs (error) | differs (error) |
| `polygon(path)` | exact | differs (error) | differs (error) | differs (error) |
| `popen(path)` | exact | differs (error) | differs (error) | differs (error) |
| `pt_contained_circle(point, circle)` | exact | exact | exact | refused |
| `pt_contained_poly(point, polygon)` | exact | exact | exact | refused |
| `radius(circle)` | exact | exact | exact | refused |
| `slope(point, point)` | exact | differs (error) | differs (error) | differs (error) |
| `width(box)` | exact | exact | exact | refused |
| `width_bucket(anycompatible, anycompatiblearray)` | exact | differs (error) | differs (error) | differs (error) |
| `width_bucket(double precision, double precision, double precision, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `width_bucket(numeric, numeric, numeric, integer)` | exact | differs (error) | differs (error) | differs (error) |

</details>

<details><summary>Hash (44)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `hash_aclitem(aclitem)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hash_aclitem_extended(aclitem, bigint)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hash_array(anyarray)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_array_extended(anyarray, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_multirange(anymultirange)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_multirange_extended(anymultirange, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_numeric(numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_numeric_extended(numeric, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_range(anyrange)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_range_extended(anyrange, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hash_record(record)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hash_record_extended(record, bigint)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hashbpchar(character)` | exact | differs (error) | differs (error) | differs (error) |
| `hashbpcharextended(character, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashchar("char")` | exact | differs (error) | differs (error) | differs (error) |
| `hashcharextended("char", bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashenum(anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hashenumextended(anyenum, bigint)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hashfloat4(real)` | exact | differs (error) | differs (error) | differs (error) |
| `hashfloat4extended(real, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashfloat8(double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `hashfloat8extended(double precision, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashinet(inet)` | exact | differs (error) | differs (error) | differs (error) |
| `hashinetextended(inet, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashint2(smallint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashint2extended(smallint, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashint4(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `hashint4extended(integer, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashint8(bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashint8extended(bigint, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashmacaddr(macaddr)` | exact | differs (error) | differs (error) | differs (error) |
| `hashmacaddr8(macaddr8)` | exact | differs (error) | differs (error) | differs (error) |
| `hashmacaddr8extended(macaddr8, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashmacaddrextended(macaddr, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashname(name)` | exact | differs (error) | differs (error) | differs (error) |
| `hashnameextended(name, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashoid(oid)` | exact | differs (error) | differs (error) | differs (error) |
| `hashoidextended(oid, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashoidvector(oidvector)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hashoidvectorextended(oidvector, bigint)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hashtext(text)` | exact | differs (error) | differs (error) | differs (error) |
| `hashtextextended(text, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `hashtid(tid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `hashtidextended(tid, bigint)` | exact (call only) | differs (error) | differs (error) | differs (error) |

</details>

<details><summary>Index support (45)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `amvalidate(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `brin_bloom_summary_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `brin_bloom_summary_out(pg_brin_bloom_summary)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `brin_bloom_summary_send(pg_brin_bloom_summary)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `brin_desummarize_range(regclass, bigint)` | refused | refused | refused | refused |
| `brin_minmax_multi_summary_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `brin_minmax_multi_summary_out(pg_brin_minmax_multi_summary)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `brin_minmax_multi_summary_send(pg_brin_minmax_multi_summary)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `brin_summarize_new_values(regclass)` | refused | refused | refused | refused |
| `brin_summarize_range(regclass, bigint)` | refused | refused | refused | refused |
| `btarraycmp(anyarray, anyarray)` | exact | differs (error) | differs (error) | differs (error) |
| `btboolcmp(boolean, boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `btbpchar_pattern_cmp(character, character)` | exact | differs (error) | differs (error) | differs (error) |
| `btcharcmp("char", "char")` | exact | differs (error) | differs (error) | differs (error) |
| `btequalimage(oid)` | exact | differs (error) | differs (error) | differs (error) |
| `btfloat48cmp(real, double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `btfloat4cmp(real, real)` | exact | differs (error) | differs (error) | differs (error) |
| `btfloat84cmp(double precision, real)` | exact | differs (error) | differs (error) | differs (error) |
| `btfloat8cmp(double precision, double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `btint24cmp(smallint, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `btint28cmp(smallint, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `btint2cmp(smallint, smallint)` | exact | differs (error) | differs (error) | differs (error) |
| `btint42cmp(integer, smallint)` | exact | differs (error) | differs (error) | differs (error) |
| `btint48cmp(integer, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `btint4cmp(integer, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `btint82cmp(bigint, smallint)` | exact | differs (error) | differs (error) | differs (error) |
| `btint84cmp(bigint, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `btint8cmp(bigint, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `btnamecmp(name, name)` | exact | differs (error) | differs (error) | differs (error) |
| `btnametextcmp(name, text)` | exact | differs (error) | differs (error) | differs (error) |
| `btoidcmp(oid, oid)` | exact | differs (error) | differs (error) | differs (error) |
| `btoidvectorcmp(oidvector, oidvector)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `btrecordcmp(record, record)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `btrecordimagecmp(record, record)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `bttext_pattern_cmp(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `bttextcmp(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `bttextnamecmp(text, name)` | exact | differs (error) | differs (error) | differs (error) |
| `bttidcmp(tid, tid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `btvarstrequalimage(oid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `gin_clean_pending_list(regclass)` | refused | refused | refused | refused |
| `gin_cmp_tslexeme(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `gin_compare_jsonb(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `gtsvectorin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `gtsvectorout(gtsvector)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `spg_poly_quad_compress(polygon)` | exact | differs (error) | differs (error) | differs (error) |

</details>

<details><summary>JSON (102)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `array_to_json(anyarray)` | exact | exact | refused | refused |
| `array_to_json(anyarray, boolean)` | exact | refused | refused | refused |
| `json_agg(anyelement)` | exact | differs (error) | differs (error) | differs (error) |
| `json_agg_strict(anyelement)` | exact | refused | refused | refused |
| `json_array_element(from_json json, element_index integer)` | exact | exact | differs (error) | differs |
| `json_array_element_text(from_json json, element_index integer)` | exact | exact | differs (error) | exact |
| `json_array_elements(from_json json, OUT value json)` | exact | refused | refused | refused |
| `json_array_elements_text(from_json json, OUT value text)` | exact | refused | refused | refused |
| `json_array_length(json)` | exact | exact | exact | exact |
| `json_build_array()` | exact | refused | refused | refused |
| `json_build_array(VARIADIC "any")` | exact | refused | refused | refused |
| `json_build_object()` | exact | exact | differs (error) | refused |
| `json_build_object(VARIADIC "any")` | exact (call only) | exact (call only) | differs (error) | refused |
| `json_each(from_json json, OUT key text, OUT value json)` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `json_each_text(from_json json, OUT key text, OUT value text)` | exact | refused | refused | refused |
| `json_extract_path(from_json json, VARIADIC path_elems text[])` | exact | refused | refused | refused |
| `json_extract_path_text(from_json json, VARIADIC path_elems text[])` | exact | refused | refused | refused |
| `json_object(text[])` | exact | exact | differs (error) | differs (error) |
| `json_object(text[], text[])` | equivalent [1](#rule-json-spacing) | exact | differs (error) | differs (error) |
| `json_object_agg(key "any", value "any")` | exact | differs (error) | refused | refused |
| `json_object_agg_strict(key "any", value "any")` | exact | refused | refused | refused |
| `json_object_agg_unique(key "any", value "any")` | exact | refused | refused | refused |
| `json_object_agg_unique_strict(key "any", value "any")` | exact | refused | refused | refused |
| `json_object_field(from_json json, field_name text)` | exact | exact | exact | differs |
| `json_object_field_text(from_json json, field_name text)` | exact | exact | exact | exact |
| `json_object_keys(json)` | exact | refused | refused | refused |
| `json_populate_record(base anyelement, from_json json, use_json_as_text boolean)` | differs | refused | refused | refused |
| `json_populate_recordset(base anyelement, from_json json, use_json_as_text boolean)` | differs | refused | refused | refused |
| `json_strip_nulls(json)` | exact | refused | refused | refused |
| `json_to_record(json)` | exact (call only) | refused | refused | refused |
| `json_to_recordset(json)` | refused | refused | refused | refused |
| `json_typeof(json)` | exact | exact | exact | exact |
| `jsonb_agg(anyelement)` | exact | differs (error) | refused | refused |
| `jsonb_agg_strict(anyelement)` | exact | refused | refused | refused |
| `jsonb_array_element(from_json jsonb, element_index integer)` | exact | exact | differs (error) | differs |
| `jsonb_array_element_text(from_json jsonb, element_index integer)` | exact | exact | differs (error) | exact |
| `jsonb_array_elements(from_json jsonb, OUT value jsonb)` | exact | refused | refused | refused |
| `jsonb_array_elements_text(from_json jsonb, OUT value text)` | exact | refused | refused | refused |
| `jsonb_array_length(jsonb)` | exact | exact | exact | exact |
| `jsonb_build_array()` | exact | exact | refused | refused |
| `jsonb_build_array(VARIADIC "any")` | exact | exact | refused | refused |
| `jsonb_build_object()` | exact | exact | differs (error) | refused |
| `jsonb_build_object(VARIADIC "any")` | exact (call only) | exact (call only) | differs (error) | refused |
| `jsonb_cmp(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_concat(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_contained(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_contains(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_delete(from_json jsonb, VARIADIC path_elems text[])` | exact | refused | refused | refused |
| `jsonb_delete(jsonb, integer)` | exact | refused | refused | refused |
| `jsonb_delete(jsonb, text)` | exact | refused | refused | refused |
| `jsonb_delete_path(jsonb, text[])` | exact | refused | refused | refused |
| `jsonb_each(from_json jsonb, OUT key text, OUT value jsonb)` | exact | refused | refused | refused |
| `jsonb_each_text(from_json jsonb, OUT key text, OUT value text)` | exact | refused | refused | refused |
| `jsonb_eq(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_exists(jsonb, text)` | exact | refused | refused | refused |
| `jsonb_exists_all(jsonb, text[])` | exact | refused | refused | refused |
| `jsonb_exists_any(jsonb, text[])` | exact | refused | refused | refused |
| `jsonb_extract_path(from_json jsonb, VARIADIC path_elems text[])` | exact | refused | refused | refused |
| `jsonb_extract_path_text(from_json jsonb, VARIADIC path_elems text[])` | exact | refused | refused | refused |
| `jsonb_ge(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_gt(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_hash(jsonb)` | exact | refused | refused | refused |
| `jsonb_hash_extended(jsonb, bigint)` | exact | refused | refused | refused |
| `jsonb_insert(jsonb_in jsonb, path text[], replacement jsonb, insert_after boolean)` | exact | refused | refused | refused |
| `jsonb_le(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_lt(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_ne(jsonb, jsonb)` | exact | refused | refused | refused |
| `jsonb_object(text[])` | exact | refused | refused | refused |
| `jsonb_object(text[], text[])` | exact | refused | refused | refused |
| `jsonb_object_agg(key "any", value "any")` | exact | differs (error) | refused | refused |
| `jsonb_object_agg_strict(key "any", value "any")` | exact | refused | refused | refused |
| `jsonb_object_agg_unique(key "any", value "any")` | exact | refused | refused | refused |
| `jsonb_object_agg_unique_strict(key "any", value "any")` | exact | refused | refused | refused |
| `jsonb_object_field(from_json jsonb, field_name text)` | exact | exact | exact | differs |
| `jsonb_object_field_text(from_json jsonb, field_name text)` | exact | exact | exact | exact |
| `jsonb_object_keys(jsonb)` | exact | refused | refused | refused |
| `jsonb_path_exists(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_exists_opr(jsonb, jsonpath)` | exact | refused | refused | refused |
| `jsonb_path_exists_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_match(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_match_opr(jsonb, jsonpath)` | exact | refused | refused | refused |
| `jsonb_path_match_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_query(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_query_array(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_query_array_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_query_first(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_query_first_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_path_query_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | refused | refused | refused |
| `jsonb_populate_record(anyelement, jsonb)` | differs | refused | refused | refused |
| `jsonb_populate_record_valid(anyelement, jsonb)` | exact (call only) | refused | refused | refused |
| `jsonb_populate_recordset(anyelement, jsonb)` | differs | refused | refused | refused |
| `jsonb_pretty(jsonb)` | exact | refused | refused | refused |
| `jsonb_set(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean)` | exact | refused | refused | refused |
| `jsonb_set_lax(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean, null_value_treatment text)` | exact | refused | refused | refused |
| `jsonb_strip_nulls(jsonb)` | exact | refused | refused | refused |
| `jsonb_to_record(jsonb)` | exact (call only) | refused | refused | refused |
| `jsonb_to_recordset(jsonb)` | refused | refused | refused | refused |
| `jsonb_typeof(jsonb)` | exact | exact | exact | exact |
| `row_to_json(record)` | exact (call only) | exact (call only) | refused | refused |
| `row_to_json(record, boolean)` | exact (call only) | refused | refused | refused |
| `to_json(anyelement)` | exact | exact | differs (error) | refused |
| `to_jsonb(anyelement)` | exact | exact | exact | refused |

</details>

<details><summary>Large object (20)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `lo_close(integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_creat(integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_create(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_export(oid, text)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_from_bytea(oid, bytea)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_get(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_get(oid, bigint, integer)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_import(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_import(text, oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_lseek(integer, integer, integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_lseek64(integer, bigint, integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_open(oid, integer)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_put(oid, bigint, bytea)` | exact (call only) | exact (call only) | differs (error) | refused |
| `lo_tell(integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_tell64(integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_truncate(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_truncate64(integer, bigint)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `lo_unlink(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `loread(integer, integer)` | exact (call only) | exact (call only) | differs | refused |
| `lowrite(integer, bytea)` | exact (call only) | exact (call only) | differs (error) | refused |

</details>

<details><summary>Network (33)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `abbrev(cidr)` | exact | exact | exact | refused |
| `abbrev(inet)` | exact | exact | exact | refused |
| `broadcast(inet)` | exact | differs (error) | differs (error) | differs (error) |
| `cidr(inet)` | exact | refused | refused | differs |
| `family(inet)` | exact | exact | differs (error) | differs (error) |
| `host(inet)` | exact | exact | differs (error) | differs (error) |
| `hostmask(inet)` | exact | refused | refused | differs |
| `macaddr(macaddr8)` | exact (call only) | exact (call only) | exact (call only) | differs (error) |
| `macaddr8(macaddr)` | exact | exact | exact | differs (error) |
| `masklen(inet)` | exact | refused | refused | differs |
| `max(inet)` | exact | exact | differs (error) | differs (error) |
| `min(inet)` | exact | exact | differs (error) | differs (error) |
| `netmask(inet)` | exact | differs | differs (error) | differs (error) |
| `network(inet)` | exact | exact | differs (error) | differs (error) |
| `network_cmp(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_eq(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_ge(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_gt(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_larger(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_le(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_lt(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_ne(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_overlap(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_smaller(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_sub(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_subeq(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_sup(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `network_supeq(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `set_masklen(cidr, integer)` | exact | refused | refused | differs (error) |
| `set_masklen(inet, integer)` | exact | refused | refused | differs (error) |
| `text(inet)` | exact | differs | differs (error) | differs (error) |
| `trunc(macaddr)` | exact | exact | differs (error) | differs (error) |
| `trunc(macaddr8)` | exact | exact | differs (error) | differs (error) |

</details>

<details><summary>Numeric (292)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `abs(double precision)` | exact | exact | differs (error) | differs (error) |
| `abs(numeric)` | exact | exact | differs (error) | differs (error) |
| `abs(real)` | exact | exact | differs (error) | differs (error) |
| `acos(double precision)` | exact | exact | differs (error) | differs (error) |
| `acosd(double precision)` | exact | exact | refused | refused |
| `acosh(double precision)` | exact | equivalent [6](#rule-float-tolerance) | differs (error) | differs (error) |
| `asin(double precision)` | exact | exact | differs (error) | differs (error) |
| `asind(double precision)` | exact | exact | refused | refused |
| `asinh(double precision)` | exact | exact | differs (error) | differs (error) |
| `atan(double precision)` | exact | exact | differs (error) | differs (error) |
| `atan2(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `atan2d(double precision, double precision)` | exact | exact | refused | refused |
| `atand(double precision)` | exact | exact | refused | refused |
| `atanh(double precision)` | exact | equivalent [6](#rule-float-tolerance) | differs (error) | differs (error) |
| `avg(double precision)` | exact | exact | differs (error) | differs (error) |
| `avg(numeric)` | exact | differs | differs (error) | differs (error) |
| `avg(real)` | exact | exact | differs (error) | differs (error) |
| `cash_cmp(money, money)` | exact | exact | exact | exact |
| `cash_div_cash(money, money)` | exact | exact | exact | exact |
| `cash_div_flt4(money, real)` | exact | exact | exact | exact |
| `cash_div_flt8(money, double precision)` | exact | exact | exact | exact |
| `cash_div_int2(money, smallint)` | exact | exact | differs | differs |
| `cash_div_int4(money, integer)` | exact | exact | differs | differs |
| `cash_div_int8(money, bigint)` | exact | exact | exact | exact |
| `cash_eq(money, money)` | exact | exact | exact | exact |
| `cash_ge(money, money)` | exact | exact | exact | exact |
| `cash_gt(money, money)` | exact | exact | exact | exact |
| `cash_le(money, money)` | exact | exact | exact | exact |
| `cash_lt(money, money)` | exact | exact | exact | exact |
| `cash_mi(money, money)` | exact | exact | exact | exact |
| `cash_mul_flt4(money, real)` | exact | exact | exact | exact |
| `cash_mul_flt8(money, double precision)` | exact | exact | exact | exact |
| `cash_mul_int2(money, smallint)` | exact | exact | exact | exact |
| `cash_mul_int4(money, integer)` | exact | exact | exact | exact |
| `cash_mul_int8(money, bigint)` | exact | exact | exact | exact |
| `cash_ne(money, money)` | exact | exact | exact | exact |
| `cash_pl(money, money)` | exact | exact | exact | exact |
| `cash_words(money)` | exact | exact | refused | refused |
| `cashlarger(money, money)` | exact | exact | exact | exact |
| `cashsmaller(money, money)` | exact | exact | exact | exact |
| `cbrt(double precision)` | exact | exact | differs (error) | differs (error) |
| `ceil(double precision)` | exact | exact | differs (error) | differs (error) |
| `ceil(numeric)` | exact | exact | differs (error) | differs (error) |
| `ceiling(double precision)` | exact | exact | differs (error) | differs (error) |
| `ceiling(numeric)` | exact | exact | differs (error) | differs (error) |
| `corr(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `cos(double precision)` | exact | exact | differs (error) | differs (error) |
| `cosd(double precision)` | exact | exact | refused | refused |
| `cosh(double precision)` | exact | exact | differs (error) | differs (error) |
| `cot(double precision)` | exact | exact | differs (error) | differs (error) |
| `cotd(double precision)` | exact | exact | refused | refused |
| `covar_pop(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `covar_samp(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `dcbrt(double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | differs (error) |
| `degrees(double precision)` | exact | equivalent [6](#rule-float-tolerance) | differs (error) | differs (error) |
| `dexp(double precision)` | exact | exact | refused | refused |
| `div(numeric, numeric)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `dlog1(double precision)` | exact | exact | refused | refused |
| `dlog10(double precision)` | exact | exact | refused | refused |
| `dpow(double precision, double precision)` | exact | exact | refused | refused |
| `dround(double precision)` | exact | exact | exact | refused |
| `dsqrt(double precision)` | exact | exact | refused | refused |
| `dtrunc(double precision)` | exact | exact | exact | exact |
| `erf(double precision)` | exact | exact | refused | exact |
| `erfc(double precision)` | exact | exact | refused | exact |
| `exp(double precision)` | exact | exact | differs (error) | differs (error) |
| `exp(numeric)` | exact | differs | differs (error) | differs (error) |
| `float4(bigint)` | exact | exact | refused | refused |
| `float4(double precision)` | exact | exact | refused | refused |
| `float4(integer)` | exact | exact | refused | refused |
| `float4(jsonb)` | exact | exact | refused | refused |
| `float4(numeric)` | exact | exact | refused | refused |
| `float4(smallint)` | exact | exact | refused | refused |
| `float48div(real, double precision)` | exact | exact | refused | refused |
| `float48eq(real, double precision)` | exact | exact | refused | refused |
| `float48ge(real, double precision)` | exact | exact | refused | refused |
| `float48gt(real, double precision)` | exact | exact | refused | refused |
| `float48le(real, double precision)` | exact | exact | refused | refused |
| `float48lt(real, double precision)` | exact | exact | refused | refused |
| `float48mi(real, double precision)` | exact | exact | refused | refused |
| `float48mul(real, double precision)` | exact | exact | refused | refused |
| `float48ne(real, double precision)` | exact | exact | refused | refused |
| `float48pl(real, double precision)` | exact | exact | refused | refused |
| `float8(bigint)` | exact | exact | refused | refused |
| `float8(integer)` | exact | exact | refused | refused |
| `float8(jsonb)` | exact | exact | refused | refused |
| `float8(numeric)` | exact | exact | refused | refused |
| `float8(real)` | exact | exact | refused | refused |
| `float8(smallint)` | exact | exact | refused | refused |
| `float84div(double precision, real)` | exact | exact | refused | refused |
| `float84eq(double precision, real)` | exact | exact | refused | refused |
| `float84ge(double precision, real)` | exact | exact | refused | refused |
| `float84gt(double precision, real)` | exact | exact | refused | refused |
| `float84le(double precision, real)` | exact | exact | refused | refused |
| `float84lt(double precision, real)` | exact | exact | refused | refused |
| `float84mi(double precision, real)` | exact | exact | refused | refused |
| `float84mul(double precision, real)` | exact | exact | refused | refused |
| `float84ne(double precision, real)` | exact | exact | refused | refused |
| `float84pl(double precision, real)` | exact | exact | refused | refused |
| `floor(double precision)` | exact | exact | differs (error) | differs (error) |
| `floor(numeric)` | exact | exact | differs (error) | differs (error) |
| `flt4_mul_cash(real, money)` | exact | exact | exact | exact |
| `flt8_mul_cash(double precision, money)` | exact | exact | exact | exact |
| `gcd(numeric, numeric)` | exact | exact | differs (error) | differs (error) |
| `generate_series(numeric, numeric)` | exact | exact | differs (error) | differs (error) |
| `generate_series(numeric, numeric, numeric)` | exact | exact | differs (error) | differs (error) |
| `in_range(double precision, double precision, double precision, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(numeric, numeric, numeric, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(real, real, double precision, boolean, boolean)` | exact | exact | exact | exact |
| `int2(bigint)` | exact | exact | refused | refused |
| `int2(double precision)` | exact | exact | refused | refused |
| `int2(integer)` | exact | exact | refused | refused |
| `int2(jsonb)` | exact | exact | refused | refused |
| `int2(numeric)` | exact | exact | refused | refused |
| `int2(real)` | exact | exact | refused | refused |
| `int24div(smallint, integer)` | exact | exact | refused | refused |
| `int24eq(smallint, integer)` | exact | exact | exact | exact |
| `int24ge(smallint, integer)` | exact | exact | exact | exact |
| `int24gt(smallint, integer)` | exact | exact | exact | exact |
| `int24le(smallint, integer)` | exact | exact | exact | exact |
| `int24lt(smallint, integer)` | exact | exact | exact | exact |
| `int24mi(smallint, integer)` | exact | exact | refused | refused |
| `int24mul(smallint, integer)` | exact | exact | refused | refused |
| `int24ne(smallint, integer)` | exact | exact | exact | exact |
| `int24pl(smallint, integer)` | exact | exact | refused | refused |
| `int28div(smallint, bigint)` | exact | exact | refused | refused |
| `int28eq(smallint, bigint)` | exact | exact | exact | exact |
| `int28ge(smallint, bigint)` | exact | exact | exact | exact |
| `int28gt(smallint, bigint)` | exact | exact | exact | exact |
| `int28le(smallint, bigint)` | exact | exact | exact | exact |
| `int28lt(smallint, bigint)` | exact | exact | exact | exact |
| `int28mi(smallint, bigint)` | exact | exact | refused | refused |
| `int28mul(smallint, bigint)` | exact | exact | refused | refused |
| `int28ne(smallint, bigint)` | exact | exact | exact | exact |
| `int28pl(smallint, bigint)` | exact | exact | refused | refused |
| `int2_mul_cash(smallint, money)` | exact | exact | exact | exact |
| `int2int4_sum(bigint[])` | exact (call only) | exact (call only) | refused | refused |
| `int4("char")` | exact | exact | refused | refused |
| `int4(bigint)` | exact | exact | refused | refused |
| `int4(bit)` | exact | exact | refused | refused |
| `int4(boolean)` | exact | exact | refused | refused |
| `int4(double precision)` | exact | exact | refused | refused |
| `int4(jsonb)` | exact | exact | refused | refused |
| `int4(numeric)` | exact | exact | refused | refused |
| `int4(real)` | exact | exact | refused | refused |
| `int4(smallint)` | exact | exact | refused | refused |
| `int42div(integer, smallint)` | exact | exact | refused | refused |
| `int42eq(integer, smallint)` | exact | exact | exact | exact |
| `int42ge(integer, smallint)` | exact | exact | exact | exact |
| `int42gt(integer, smallint)` | exact | exact | exact | exact |
| `int42le(integer, smallint)` | exact | exact | exact | exact |
| `int42lt(integer, smallint)` | exact | exact | exact | exact |
| `int42mi(integer, smallint)` | exact | exact | refused | refused |
| `int42mul(integer, smallint)` | exact | exact | refused | refused |
| `int42ne(integer, smallint)` | exact | exact | exact | exact |
| `int42pl(integer, smallint)` | exact | exact | refused | refused |
| `int48div(integer, bigint)` | exact | exact | refused | refused |
| `int48eq(integer, bigint)` | exact | exact | exact | exact |
| `int48ge(integer, bigint)` | exact | exact | exact | exact |
| `int48gt(integer, bigint)` | exact | exact | exact | exact |
| `int48le(integer, bigint)` | exact | exact | exact | exact |
| `int48lt(integer, bigint)` | exact | exact | exact | exact |
| `int48mi(integer, bigint)` | exact | exact | refused | refused |
| `int48mul(integer, bigint)` | exact | exact | refused | refused |
| `int48ne(integer, bigint)` | exact | exact | exact | exact |
| `int48pl(integer, bigint)` | exact | exact | refused | refused |
| `int4_mul_cash(integer, money)` | exact | exact | exact | exact |
| `int8(bit)` | exact | exact | refused | refused |
| `int8(double precision)` | exact | exact | refused | refused |
| `int8(integer)` | exact | exact | refused | refused |
| `int8(jsonb)` | exact | exact | refused | refused |
| `int8(numeric)` | exact | exact | refused | refused |
| `int8(oid)` | exact | exact | refused | refused |
| `int8(real)` | exact | exact | refused | refused |
| `int8(smallint)` | exact | exact | refused | refused |
| `int82div(bigint, smallint)` | exact | exact | refused | refused |
| `int82eq(bigint, smallint)` | exact | exact | exact | exact |
| `int82ge(bigint, smallint)` | exact | exact | exact | exact |
| `int82gt(bigint, smallint)` | exact | exact | exact | exact |
| `int82le(bigint, smallint)` | exact | exact | exact | exact |
| `int82lt(bigint, smallint)` | exact | exact | exact | exact |
| `int82mi(bigint, smallint)` | exact | exact | refused | refused |
| `int82mul(bigint, smallint)` | exact | exact | refused | refused |
| `int82ne(bigint, smallint)` | exact | exact | exact | exact |
| `int82pl(bigint, smallint)` | exact | exact | refused | refused |
| `int84div(bigint, integer)` | exact | exact | refused | refused |
| `int84eq(bigint, integer)` | exact | exact | exact | exact |
| `int84ge(bigint, integer)` | exact | exact | exact | exact |
| `int84gt(bigint, integer)` | exact | exact | exact | exact |
| `int84le(bigint, integer)` | exact | exact | exact | exact |
| `int84lt(bigint, integer)` | exact | exact | exact | exact |
| `int84mi(bigint, integer)` | exact | exact | refused | refused |
| `int84mul(bigint, integer)` | exact | exact | refused | refused |
| `int84ne(bigint, integer)` | exact | exact | exact | exact |
| `int84pl(bigint, integer)` | exact | exact | refused | refused |
| `int8_mul_cash(bigint, money)` | exact | exact | exact | exact |
| `int8dec(bigint)` | exact | exact | refused | refused |
| `lcm(numeric, numeric)` | exact | exact | differs (error) | differs (error) |
| `ln(double precision)` | exact | exact | differs (error) | differs (error) |
| `ln(numeric)` | exact | differs | differs (error) | differs (error) |
| `log(double precision)` | exact | exact | differs (error) | differs (error) |
| `log(numeric)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `log(numeric, numeric)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `log10(double precision)` | exact | exact | differs (error) | differs (error) |
| `log10(numeric)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `make_time(hour integer, min integer, sec double precision)` | exact | exact | differs (error) | differs (error) |
| `make_timestamp(year integer, month integer, mday integer, hour integer, min integer, sec double precision)` | exact | exact | differs (error) | differs (error) |
| `make_timestamptz(year integer, month integer, mday integer, hour integer, min integer, sec double precision)` | exact | exact | differs (error) | differs (error) |
| `make_timestamptz(year integer, month integer, mday integer, hour integer, min integer, sec double precision, timezone text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `max(double precision)` | exact | exact | differs (error) | differs (error) |
| `max(money)` | exact | differs | differs (error) | differs (error) |
| `max(numeric)` | exact | exact | differs (error) | differs (error) |
| `max(real)` | exact | exact | differs (error) | differs (error) |
| `min(double precision)` | exact | exact | differs (error) | differs (error) |
| `min(money)` | exact | differs | differs (error) | differs (error) |
| `min(numeric)` | exact | differs | differs (error) | differs (error) |
| `min(real)` | exact | exact | differs (error) | differs (error) |
| `min_scale(numeric)` | exact | exact | refused | refused |
| `mod(numeric, numeric)` | exact | exact | differs (error) | differs (error) |
| `money(numeric)` | exact | exact | differs | differs |
| `numeric(double precision)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `numeric(money)` | exact | exact | exact | exact |
| `numeric(numeric, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric(real)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `numrange_subdiff(numeric, numeric)` | exact | exact | exact | exact |
| `percentile_cont(double precision ORDER BY double precision)` | differs (error) | differs (error) | differs (error) | differs (error) |
| `percentile_cont(double precision[] ORDER BY double precision)` | differs (error) | differs (error) | differs (error) | differs (error) |
| `percentile_disc(double precision ORDER BY anyelement)` | differs (error) | differs (error) | differs (error) | differs (error) |
| `pg_lsn(numeric)` | exact | exact | refused | refused |
| `pg_size_pretty(numeric)` | exact | differs | differs (error) | differs (error) |
| `pg_sleep(double precision)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pow(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `pow(numeric, numeric)` | exact | differs | differs (error) | differs (error) |
| `power(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `power(numeric, numeric)` | exact | differs | differs (error) | differs (error) |
| `radians(double precision)` | exact | exact | differs (error) | differs (error) |
| `random(min numeric, max numeric)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `random_normal(mean double precision, stddev double precision)` | exact (call only) | exact (call only) | refused | refused |
| `regr_avgx(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_avgy(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_count(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_intercept(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_r2(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_slope(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_sxx(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_sxy(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `regr_syy(double precision, double precision)` | exact | exact | differs (error) | differs (error) |
| `round(double precision)` | exact | differs | differs (error) | differs (error) |
| `round(numeric)` | exact | exact | differs (error) | differs (error) |
| `round(numeric, integer)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `scale(numeric)` | exact | exact | differs | refused |
| `setseed(double precision)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `sign(double precision)` | exact | exact | differs (error) | differs (error) |
| `sign(numeric)` | exact | exact | differs (error) | differs (error) |
| `sin(double precision)` | exact | exact | differs (error) | differs (error) |
| `sind(double precision)` | exact | exact | refused | refused |
| `sinh(double precision)` | exact | exact | differs (error) | differs (error) |
| `sqrt(double precision)` | exact | exact | differs (error) | differs (error) |
| `sqrt(numeric)` | exact | differs | differs (error) | differs (error) |
| `stddev(double precision)` | exact | exact | differs (error) | differs (error) |
| `stddev(numeric)` | exact | differs | differs (error) | differs (error) |
| `stddev(real)` | exact | exact | differs (error) | differs (error) |
| `stddev_pop(double precision)` | exact | exact | differs (error) | differs (error) |
| `stddev_pop(numeric)` | exact | differs | differs (error) | differs (error) |
| `stddev_pop(real)` | exact | exact | differs (error) | differs (error) |
| `stddev_samp(double precision)` | exact | exact | differs (error) | differs (error) |
| `stddev_samp(numeric)` | exact | differs | differs (error) | differs (error) |
| `stddev_samp(real)` | exact | exact | differs (error) | differs (error) |
| `sum(double precision)` | exact | exact | differs (error) | differs (error) |
| `sum(money)` | exact | differs | differs (error) | differs (error) |
| `sum(numeric)` | exact | differs | differs (error) | differs (error) |
| `sum(real)` | exact | differs | differs (error) | differs (error) |
| `tan(double precision)` | exact | exact | differs (error) | differs (error) |
| `tand(double precision)` | exact | exact | refused | refused |
| `tanh(double precision)` | exact | exact | differs (error) | differs (error) |
| `to_char(double precision, text)` | exact | refused | differs (error) | differs (error) |
| `to_char(numeric, text)` | exact | refused | differs (error) | differs (error) |
| `to_char(real, text)` | exact | refused | differs (error) | differs (error) |
| `to_timestamp(double precision)` | exact | exact | differs (error) | differs (error) |
| `trim_scale(numeric)` | exact | exact | refused | refused |
| `trunc(double precision)` | exact | exact | differs (error) | differs (error) |
| `trunc(numeric)` | exact | exact | differs (error) | differs (error) |
| `trunc(numeric, integer)` | exact | equivalent [4](#rule-number-value) | differs (error) | differs (error) |
| `var_pop(double precision)` | exact | exact | differs (error) | differs (error) |
| `var_pop(numeric)` | exact | differs | differs (error) | differs (error) |
| `var_pop(real)` | exact | exact | differs (error) | differs (error) |
| `var_samp(double precision)` | exact | exact | differs (error) | differs (error) |
| `var_samp(numeric)` | exact | differs | differs (error) | differs (error) |
| `var_samp(real)` | exact | exact | differs (error) | differs (error) |
| `variance(double precision)` | exact | exact | differs (error) | differs (error) |
| `variance(numeric)` | exact | differs | differs (error) | differs (error) |
| `variance(real)` | exact | exact | differs (error) | differs (error) |

</details>

<details><summary>Other (87)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `abs(bigint)` | exact | exact | differs (error) | differs (error) |
| `abs(integer)` | exact | exact | differs (error) | differs (error) |
| `abs(smallint)` | exact | exact | differs (error) | differs (error) |
| `age(xid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `bit(bigint, integer)` | exact | differs | differs (error) | differs (error) |
| `bit(integer, integer)` | exact | differs | differs (error) | differs (error) |
| `bool(integer)` | exact | exact | differs (error) | differs (error) |
| `bool(jsonb)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `bpchar("char")` | exact | exact | exact | refused |
| `char(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `chr(integer)` | exact | exact | differs (error) | differs (error) |
| `concat(VARIADIC "any")` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `currval(regclass)` | refused | refused | refused | refused |
| `factorial(bigint)` | exact | exact | differs (error) | differs (error) |
| `fmgr_c_validator(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `fmgr_internal_validator(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `fmgr_sql_validator(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `gcd(bigint, bigint)` | exact | exact | differs (error) | differs (error) |
| `gcd(integer, integer)` | exact | exact | differs (error) | differs (error) |
| `gen_random_uuid()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `generate_series(bigint, bigint)` | exact | exact | differs (error) | differs (error) |
| `generate_series(bigint, bigint, bigint)` | exact | exact | differs (error) | differs (error) |
| `generate_series(integer, integer)` | exact | exact | differs (error) | differs (error) |
| `generate_series(integer, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `getdatabaseencoding()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `getpgusername()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `icu_unicode_version()` | exact | differs | differs | differs |
| `in_range(bigint, bigint, bigint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(integer, integer, bigint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(integer, integer, integer, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(integer, integer, smallint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(smallint, smallint, bigint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(smallint, smallint, integer, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(smallint, smallint, smallint, boolean, boolean)` | exact | exact | exact | exact |
| `int4range_subdiff(integer, integer)` | exact | exact | exact | exact |
| `int8dec_any(bigint, "any")` | exact | differs (error) | differs (error) | differs (error) |
| `int8inc_any(bigint, "any")` | exact | differs (error) | differs (error) | differs (error) |
| `int8range_subdiff(bigint, bigint)` | exact | exact | exact | exact |
| `lastval()` | exact (form only) | differs (error) | differs (error) | differs (error) |
| `lcm(bigint, bigint)` | exact | exact | differs (error) | differs (error) |
| `lcm(integer, integer)` | exact | exact | differs (error) | differs (error) |
| `make_date(year integer, month integer, day integer)` | exact | exact | differs (error) | differs (error) |
| `mod(bigint, bigint)` | exact | exact | differs (error) | differs (error) |
| `mod(integer, integer)` | exact | exact | differs (error) | differs (error) |
| `mod(smallint, smallint)` | exact | exact | differs (error) | differs (error) |
| `money(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `money(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `mxid_age(xid)` | exact (call only) | exact (call only) | refused | refused |
| `nextval(regclass)` | refused | refused | refused | refused |
| `now()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `num_nonnulls(VARIADIC "any")` | exact | differs (error) | differs (error) | differs (error) |
| `num_nulls(VARIADIC "any")` | exact | differs (error) | differs (error) | differs (error) |
| `numeric(bigint)` | exact | exact | differs (error) | differs (error) |
| `numeric(integer)` | exact | exact | differs (error) | differs (error) |
| `numeric(jsonb)` | exact | exact | differs (error) | differs (error) |
| `numeric(smallint)` | exact | exact | differs (error) | differs (error) |
| `oid(bigint)` | exact | exact | differs (error) | differs (error) |
| `pi()` | exact | exact | differs (error) | differs (error) |
| `plpgsql_validator(oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `postgresql_fdw_validator(text[], oid)` | exact | exact | differs (error) | differs (error) |
| `quote_literal(anyelement)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `quote_nullable(anyelement)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `random()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `random(min bigint, max bigint)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `random(min integer, max integer)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `row_security_active(oid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `satisfies_hash_partition(oid, integer, integer, VARIADIC "any")` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `session_user()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `setval(regclass, bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `setval(regclass, bigint, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `system_user()` | differs | differs | differs | differs |
| `text("char")` | exact | exact | differs (error) | differs (error) |
| `text(boolean)` | exact | exact | differs (error) | differs (error) |
| `text(xml)` | exact | exact | differs (error) | differs (error) |
| `to_bin(bigint)` | exact | exact | refused | refused |
| `to_bin(integer)` | exact | exact | refused | refused |
| `to_hex(bigint)` | exact | differs | differs (error) | differs (error) |
| `to_hex(integer)` | exact | differs | differs (error) | differs (error) |
| `to_oct(bigint)` | exact | exact | refused | refused |
| `to_oct(integer)` | exact | exact | refused | refused |
| `transaction_timestamp()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `trim_array(anyarray, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_token_type(parser_oid oid, OUT tokid integer, OUT alias text, OUT description text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `unicode_version()` | exact | exact | exact | exact |
| `uuid_extract_timestamp(uuid)` | exact | differs (error) | differs (error) | differs (error) |
| `uuid_extract_version(uuid)` | exact | differs | differs (error) | differs (error) |
| `xid(xid8)` | exact (call only) | exact (call only) | differs (error) | differs (error) |

</details>

<details><summary>Range (111)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `datemultirange()` | exact | exact | exact | exact |
| `datemultirange(VARIADIC daterange[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `datemultirange(daterange)` | exact | exact | exact | exact |
| `daterange(date, date)` | differs | exact | exact | exact |
| `daterange(date, date, text)` | differs | exact | exact | exact |
| `daterange_canonical(daterange)` | differs | exact | exact | exact |
| `elem_contained_by_multirange(anyelement, anymultirange)` | exact | exact | exact | differs |
| `elem_contained_by_range(anyelement, anyrange)` | exact | exact | exact | differs |
| `int4multirange()` | exact | exact | exact | exact |
| `int4multirange(VARIADIC int4range[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int4multirange(int4range)` | exact | exact | exact | exact |
| `int4range(integer, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `int4range(integer, integer, text)` | differs | exact | exact | exact |
| `int4range_canonical(int4range)` | differs | exact | exact | exact |
| `int8multirange()` | exact | exact | exact | exact |
| `int8multirange(VARIADIC int8range[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int8multirange(int8range)` | exact | exact | exact | exact |
| `int8range(bigint, bigint)` | differs | differs (error) | differs (error) | differs (error) |
| `int8range(bigint, bigint, text)` | differs | exact | exact | exact |
| `int8range_canonical(int8range)` | differs | exact | exact | exact |
| `isempty(anymultirange)` | exact | exact | exact | differs |
| `isempty(anyrange)` | exact | exact | differs | differs |
| `lower(anymultirange)` | exact | exact | differs (error) | differs (error) |
| `lower(anyrange)` | exact | exact | differs (error) | differs (error) |
| `lower_inc(anymultirange)` | exact | exact | exact | differs |
| `lower_inc(anyrange)` | exact | exact | differs | differs |
| `lower_inf(anymultirange)` | exact | exact | exact | differs |
| `lower_inf(anyrange)` | exact | exact | differs | differs |
| `multirange(anyrange)` | exact | exact | exact | exact |
| `multirange_adjacent_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_adjacent_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_after_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_after_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_before_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_before_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_cmp(anymultirange, anymultirange)` | exact | differs (error) | differs (error) | differs (error) |
| `multirange_contained_by_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_contained_by_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_contains_elem(anymultirange, anyelement)` | exact | exact | exact | differs |
| `multirange_contains_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_contains_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_eq(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_ge(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_gt(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_intersect(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_le(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_lt(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_minus(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_ne(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_overlaps_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_overlaps_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_overleft_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_overleft_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_overright_multirange(anymultirange, anymultirange)` | exact | exact | exact | differs |
| `multirange_overright_range(anymultirange, anyrange)` | exact | exact | exact | differs |
| `multirange_union(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `nummultirange()` | exact | exact | exact | exact |
| `nummultirange(VARIADIC numrange[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `nummultirange(numrange)` | exact | exact | exact | exact |
| `numrange(numeric, numeric)` | differs | differs (error) | differs (error) | differs (error) |
| `numrange(numeric, numeric, text)` | differs | exact | differs | differs |
| `range_adjacent(anyrange, anyrange)` | exact | exact | differs | differs |
| `range_adjacent_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_after(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_after_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_agg(anymultirange)` | exact | differs (error) | differs (error) | differs (error) |
| `range_agg(anyrange)` | exact | differs (error) | differs (error) | differs (error) |
| `range_before(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_before_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_cmp(anyrange, anyrange)` | exact | exact | differs | differs |
| `range_contained_by(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_contained_by_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_contains(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_contains_elem(anyrange, anyelement)` | exact | exact | exact | differs |
| `range_contains_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_eq(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_ge(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_gt(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_intersect(anyrange, anyrange)` | differs | exact | exact | exact |
| `range_intersect_agg(anymultirange)` | exact | differs (error) | differs (error) | differs (error) |
| `range_intersect_agg(anyrange)` | exact | differs (error) | differs (error) | differs (error) |
| `range_le(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_lt(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_merge(anymultirange)` | differs | exact | exact | exact |
| `range_merge(anyrange, anyrange)` | differs | exact | exact | exact |
| `range_minus(anyrange, anyrange)` | exact | exact | differs (error) | exact |
| `range_ne(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_overlaps(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_overlaps_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_overleft(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_overleft_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_overright(anyrange, anyrange)` | exact | exact | exact | differs |
| `range_overright_multirange(anyrange, anymultirange)` | exact | exact | exact | differs |
| `range_union(anyrange, anyrange)` | differs | differs (error) | differs (error) | differs |
| `tsmultirange()` | exact | exact | exact | exact |
| `tsmultirange(VARIADIC tsrange[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tsmultirange(tsrange)` | exact | exact | exact | exact |
| `tsrange(timestamp without time zone, timestamp without time zone)` | differs | exact | exact | exact |
| `tsrange(timestamp without time zone, timestamp without time zone, text)` | differs | exact | exact | differs (error) |
| `tstzmultirange()` | exact | exact | exact | exact |
| `tstzmultirange(VARIADIC tstzrange[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tstzmultirange(tstzrange)` | exact | exact | exact | exact |
| `tstzrange(timestamp with time zone, timestamp with time zone)` | differs | exact | exact | exact |
| `tstzrange(timestamp with time zone, timestamp with time zone, text)` | differs | exact | exact | differs (error) |
| `unnest(anymultirange)` | differs | differs | differs (error) | differs (error) |
| `upper(anymultirange)` | exact | exact | differs (error) | differs (error) |
| `upper(anyrange)` | exact | exact | differs (error) | differs (error) |
| `upper_inc(anymultirange)` | exact | exact | differs | differs |
| `upper_inc(anyrange)` | exact | exact | differs | differs |
| `upper_inf(anymultirange)` | exact | exact | exact | differs |
| `upper_inf(anyrange)` | exact | exact | differs | differs |

</details>

<details><summary>String (201)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `anytextcat(anynonarray, text)` | exact | differs (error) | differs (error) | differs (error) |
| `ascii(text)` | exact | exact | differs (error) | differs (error) |
| `bit(bit, integer, boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `bit_count(bit)` | exact | exact | differs (error) | differs (error) |
| `bit_count(bytea)` | exact | exact | differs (error) | differs (error) |
| `bit_length(bit)` | exact | exact | differs (error) | differs (error) |
| `bit_length(bytea)` | exact | differs (error) | differs (error) | differs (error) |
| `bit_length(text)` | exact | exact | differs (error) | differs (error) |
| `bitcat(bit varying, bit varying)` | exact | exact | refused | refused |
| `bitshiftleft(bit, integer)` | exact | exact | refused | refused |
| `bitshiftright(bit, integer)` | exact | exact | refused | refused |
| `bpchar(character, integer, boolean)` | exact | differs | differs (error) | differs (error) |
| `bpchar(name)` | exact | exact | exact | refused |
| `bpchariclike(character, text)` | exact | differs | exact | refused |
| `bpcharicnlike(character, text)` | exact | differs | exact | refused |
| `bpcharicregexeq(character, text)` | exact | differs | differs (error) | differs |
| `bpcharicregexne(character, text)` | exact | differs | differs (error) | differs |
| `bpcharlike(character, text)` | exact | differs | exact | refused |
| `bpcharnlike(character, text)` | exact | differs | exact | refused |
| `bpcharregexeq(character, text)` | exact | differs | differs (error) | differs |
| `bpcharregexne(character, text)` | exact | differs | differs (error) | differs |
| `btrim(bytea, bytea)` | exact | exact | differs (error) | differs (error) |
| `btrim(text)` | exact | exact | differs (error) | differs (error) |
| `btrim(text, text)` | exact | exact | differs (error) | differs (error) |
| `byteacat(bytea, bytea)` | exact | exact | refused | refused |
| `bytealike(bytea, bytea)` | exact | exact | refused | refused |
| `byteanlike(bytea, bytea)` | exact | exact | refused | refused |
| `char(text)` | exact | exact | differs (error) | differs (error) |
| `char_length(character)` | exact | differs | differs (error) | differs (error) |
| `char_length(text)` | exact | exact | differs (error) | differs (error) |
| `character_length(character)` | exact | differs | differs (error) | differs (error) |
| `character_length(text)` | exact | exact | differs (error) | differs (error) |
| `concat_ws(text, VARIADIC "any")` | exact | exact | differs (error) | differs (error) |
| `convert(bytea, name, name)` | exact | exact | differs (error) | differs (error) |
| `convert_from(bytea, name)` | exact | exact | refused | refused |
| `convert_to(text, name)` | exact | exact | refused | refused |
| `currtid2(text, tid)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `decode(text, text)` | exact | refused | differs (error) | differs (error) |
| `encode(bytea, text)` | exact | refused | differs (error) | differs (error) |
| `format(text)` | exact | exact | differs (error) | differs (error) |
| `format(text, VARIADIC "any")` | exact | exact | differs (error) | differs (error) |
| `get_bit(bit, integer)` | exact | exact | differs (error) | differs (error) |
| `get_bit(bytea, bigint)` | exact | exact | differs (error) | differs (error) |
| `get_byte(bytea, integer)` | exact | exact | refused | refused |
| `initcap(text)` | exact | exact | differs (error) | differs (error) |
| `is_normalized(text, text)` | exact (call only) | exact (call only) | refused | refused |
| `left(text, integer)` | exact | exact | differs (error) | differs (error) |
| `length(bit)` | exact | exact | differs (error) | differs (error) |
| `length(bytea)` | exact | exact | differs (error) | differs (error) |
| `length(bytea, name)` | exact | differs (error) | differs (error) | differs (error) |
| `length(character)` | exact | differs | differs (error) | differs (error) |
| `length(text)` | exact | exact | differs (error) | differs (error) |
| `like(bytea, bytea)` | exact | exact | differs (error) | refused |
| `like(name, text)` | exact | exact | exact | refused |
| `like(text, text)` | exact | exact | exact | refused |
| `like_escape(bytea, bytea)` | exact | exact | differs (error) | differs (error) |
| `like_escape(text, text)` | exact | exact | differs (error) | differs (error) |
| `lower(text)` | exact | exact | differs (error) | differs (error) |
| `lpad(text, integer)` | exact | exact | differs (error) | differs (error) |
| `lpad(text, integer, text)` | exact | exact | differs (error) | differs (error) |
| `ltrim(bytea, bytea)` | exact | exact | differs (error) | differs (error) |
| `ltrim(text)` | exact | exact | differs (error) | differs (error) |
| `ltrim(text, text)` | exact | exact | differs (error) | differs (error) |
| `makeaclitem(oid, oid, text, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `md5(bytea)` | exact | exact | exact | exact |
| `md5(text)` | exact | exact | exact | exact |
| `name(character)` | exact | differs | differs (error) | differs (error) |
| `name(character varying)` | exact | exact | differs (error) | differs (error) |
| `name(text)` | exact | exact | differs (error) | differs (error) |
| `nameconcatoid(name, oid)` | exact | exact | differs (error) | refused |
| `nameiclike(name, text)` | exact | exact | exact | refused |
| `nameicnlike(name, text)` | exact | exact | exact | refused |
| `nameicregexeq(name, text)` | exact | exact | exact | exact |
| `nameicregexne(name, text)` | exact | exact | exact | exact |
| `namelike(name, text)` | exact | exact | exact | refused |
| `namenlike(name, text)` | exact | exact | exact | refused |
| `nameregexeq(name, text)` | exact | exact | exact | exact |
| `nameregexne(name, text)` | exact | exact | exact | exact |
| `normalize(text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `notlike(bytea, bytea)` | exact | exact | differs (error) | refused |
| `notlike(name, text)` | exact | exact | exact | refused |
| `notlike(text, text)` | exact | exact | exact | refused |
| `octet_length(bit)` | exact | exact | differs (error) | differs (error) |
| `octet_length(bytea)` | exact | exact | differs (error) | differs (error) |
| `octet_length(character)` | exact | differs | differs (error) | differs (error) |
| `octet_length(text)` | exact | exact | differs (error) | differs (error) |
| `overlay(bit, bit, integer)` | exact | exact | differs (error) | differs (error) |
| `overlay(bit, bit, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `overlay(bytea, bytea, integer)` | exact | exact | differs (error) | differs (error) |
| `overlay(bytea, bytea, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `overlay(text, text, integer)` | exact | exact | differs (error) | differs (error) |
| `overlay(text, text, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `parse_ident(str text, strict boolean)` | exact | exact | refused | refused |
| `position(bit, bit)` | exact | exact | differs (error) | differs (error) |
| `position(bytea, bytea)` | exact | differs (error) | differs (error) | differs (error) |
| `position(text, text)` | exact | exact | differs (error) | differs (error) |
| `quote_ident(text)` | exact | exact | refused | refused |
| `quote_literal(text)` | exact | exact | exact | refused |
| `quote_nullable(text)` | exact | exact | exact | refused |
| `regclass(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `regexp_count(text, text)` | exact | exact | differs (error) | differs (error) |
| `regexp_count(text, text, integer)` | exact | exact | differs (error) | differs (error) |
| `regexp_count(text, text, integer, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_instr(text, text)` | exact | exact | differs (error) | differs (error) |
| `regexp_instr(text, text, integer)` | exact | exact | differs (error) | differs (error) |
| `regexp_instr(text, text, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `regexp_instr(text, text, integer, integer, integer)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_instr(text, text, integer, integer, integer, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_instr(text, text, integer, integer, integer, text, integer)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_like(text, text)` | exact | exact | differs (error) | differs (error) |
| `regexp_like(text, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_match(text, text)` | exact | exact | refused | refused |
| `regexp_match(text, text, text)` | exact (call only) | exact (call only) | refused | refused |
| `regexp_matches(text, text)` | exact | refused | differs (error) | differs (error) |
| `regexp_matches(text, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_replace(text, text, text)` | exact | exact | differs (error) | differs (error) |
| `regexp_replace(text, text, text, integer)` | exact | differs | differs (error) | differs (error) |
| `regexp_replace(text, text, text, integer, integer)` | exact | differs | differs (error) | differs (error) |
| `regexp_replace(text, text, text, integer, integer, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_replace(text, text, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_split_to_array(text, text)` | exact | exact | differs (error) | differs (error) |
| `regexp_split_to_array(text, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_split_to_table(text, text)` | exact | exact | differs (error) | differs (error) |
| `regexp_split_to_table(text, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `regexp_substr(text, text)` | exact | exact | exact | refused |
| `regexp_substr(text, text, integer)` | exact | exact | refused | refused |
| `regexp_substr(text, text, integer, integer)` | exact | exact | refused | refused |
| `regexp_substr(text, text, integer, integer, text)` | exact (call only) | exact (call only) | refused | refused |
| `regexp_substr(text, text, integer, integer, text, integer)` | exact (call only) | exact (call only) | refused | refused |
| `repeat(text, integer)` | exact | exact | differs (error) | differs (error) |
| `replace(text, text, text)` | exact | exact | differs (error) | differs (error) |
| `reverse(text)` | exact | exact | differs (error) | differs (error) |
| `right(text, integer)` | exact | exact | differs (error) | differs (error) |
| `row_security_active(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `rpad(text, integer)` | exact | exact | differs (error) | differs (error) |
| `rpad(text, integer, text)` | exact | exact | differs (error) | differs (error) |
| `rtrim(bytea, bytea)` | exact | exact | differs (error) | differs (error) |
| `rtrim(text)` | exact | exact | differs (error) | differs (error) |
| `rtrim(text, text)` | exact | exact | differs (error) | differs (error) |
| `set_bit(bit, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `set_bit(bytea, bigint, integer)` | exact | exact | differs (error) | differs (error) |
| `set_byte(bytea, integer, integer)` | exact | exact | refused | refused |
| `set_config(text, text, boolean)` | differs | differs | differs | differs |
| `sha224(bytea)` | exact | differs (error) | refused | exact |
| `sha256(bytea)` | exact | exact | differs | exact |
| `sha384(bytea)` | exact | differs (error) | refused | exact |
| `sha512(bytea)` | exact | differs (error) | differs | exact |
| `similar_escape(text, text)` | exact | exact | refused | refused |
| `similar_to_escape(text)` | exact | exact | refused | refused |
| `similar_to_escape(text, text)` | exact | exact | refused | refused |
| `split_part(text, text, integer)` | exact | exact | differs (error) | differs (error) |
| `starts_with(text, text)` | exact | exact | differs (error) | differs (error) |
| `string_to_array(text, text)` | exact | exact | differs (error) | differs (error) |
| `string_to_array(text, text, text)` | exact | exact | differs (error) | differs (error) |
| `string_to_table(text, text)` | exact | exact | refused | refused |
| `string_to_table(text, text, text)` | exact | exact | refused | refused |
| `strpos(text, text)` | exact | exact | differs (error) | differs (error) |
| `substr(bytea, integer)` | exact | exact | differs (error) | differs (error) |
| `substr(bytea, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `substr(text, integer)` | exact | differs | differs (error) | differs (error) |
| `substr(text, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `substring(bit, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `substring(bit, integer, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `substring(bytea, integer)` | exact | exact | differs (error) | differs (error) |
| `substring(bytea, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `substring(text, integer)` | exact | exact | differs (error) | differs (error) |
| `substring(text, integer, integer)` | exact | exact | differs (error) | differs (error) |
| `substring(text, text)` | exact | refused | differs (error) | differs (error) |
| `substring(text, text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `text(character)` | exact | differs | differs (error) | differs (error) |
| `text(name)` | exact | exact | differs (error) | differs (error) |
| `textanycat(text, anynonarray)` | exact | differs (error) | differs (error) | differs (error) |
| `textcat(text, text)` | exact | exact | exact | exact |
| `texticlike(text, text)` | exact | exact | exact | refused |
| `texticnlike(text, text)` | exact | exact | exact | refused |
| `texticregexeq(text, text)` | exact | exact | exact | exact |
| `texticregexne(text, text)` | exact | exact | exact | exact |
| `textlike(text, text)` | exact | exact | exact | refused |
| `textnlike(text, text)` | exact | exact | exact | refused |
| `textregexeq(text, text)` | exact | exact | exact | exact |
| `textregexne(text, text)` | exact | exact | exact | exact |
| `to_ascii(text)` | exact (call only) | exact (call only) | refused | refused |
| `to_ascii(text, integer)` | exact (call only) | exact (call only) | refused | refused |
| `to_ascii(text, name)` | exact (call only) | exact (call only) | refused | refused |
| `to_char(bigint, text)` | exact | refused | differs (error) | differs (error) |
| `to_char(integer, text)` | exact | refused | differs (error) | differs (error) |
| `to_date(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `to_number(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `to_timestamp(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `translate(text, text, text)` | exact | exact | differs (error) | differs (error) |
| `ts_token_type(parser_name text, OUT tokid integer, OUT alias text, OUT description text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `unicode_assigned(text)` | exact | exact | refused | refused |
| `unistr(text)` | exact | exact | refused | refused |
| `upper(text)` | exact | exact | differs (error) | differs (error) |
| `varbit(bit varying, integer, boolean)` | exact | exact | refused | refused |
| `varchar(character varying, integer, boolean)` | exact | differs | differs (error) | differs (error) |
| `varchar(name)` | exact | exact | differs (error) | differs (error) |
| `xpath(text, xml)` | differs | exact | refused | refused |
| `xpath(text, xml, text[])` | exact (call only) | exact (call only) | refused | refused |
| `xpath_exists(text, xml)` | exact | exact | refused | refused |
| `xpath_exists(text, xml, text[])` | exact (call only) | exact (call only) | refused | refused |

</details>

<details><summary>System (480)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `aclcontains(aclitem[], aclitem)` | exact (call only) | exact (call only) | refused | refused |
| `acldefault("char", oid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `aclexplode(acl aclitem[], OUT grantor oid, OUT grantee oid, OUT privilege_type text, OUT is_grantable boolean)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `aclinsert(aclitem[], aclitem)` | exact (call only) | exact (call only) | differs (error) | refused |
| `aclitemeq(aclitem, aclitem)` | exact (call only) | exact (call only) | refused | refused |
| `aclremove(aclitem[], aclitem)` | exact (call only) | exact (call only) | differs (error) | refused |
| `col_description(oid, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `current_database()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `current_query()` | exact (form only) | differs | differs (error) | differs (error) |
| `current_schema()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `current_schemas(boolean)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `current_setting(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `current_setting(text, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `current_user()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `format_type(oid, integer)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_any_column_privilege(name, oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_any_column_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_any_column_privilege(oid, oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_any_column_privilege(oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_any_column_privilege(oid, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_any_column_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(name, oid, smallint, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(name, oid, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(name, text, smallint, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(name, text, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(oid, oid, smallint, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(oid, oid, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(oid, smallint, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(oid, text, smallint, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(oid, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(oid, text, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(text, smallint, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_column_privilege(text, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_database_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_database_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_database_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_database_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_database_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_database_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_foreign_data_wrapper_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_foreign_data_wrapper_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_foreign_data_wrapper_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_foreign_data_wrapper_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_foreign_data_wrapper_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_foreign_data_wrapper_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_function_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_function_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_function_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_function_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_function_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_function_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_language_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_language_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_language_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_language_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_language_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_language_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_parameter_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `has_parameter_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | refused |
| `has_parameter_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `has_schema_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_schema_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_schema_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_schema_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_schema_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_schema_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_sequence_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_sequence_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_sequence_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_sequence_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_sequence_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_sequence_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_server_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_server_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_server_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_server_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_server_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_server_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_table_privilege(name, oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_table_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_table_privilege(oid, oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_table_privilege(oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_table_privilege(oid, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_table_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_tablespace_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_tablespace_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_tablespace_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_tablespace_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_tablespace_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_tablespace_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_type_privilege(name, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_type_privilege(name, text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `has_type_privilege(oid, oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_type_privilege(oid, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_type_privilege(oid, text, text)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `has_type_privilege(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `inet_client_addr()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `inet_client_port()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `inet_server_addr()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `inet_server_port()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `obj_description(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `obj_description(oid, name)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_lock(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_lock(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_lock_shared(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_lock_shared(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_unlock(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_unlock(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_unlock_all()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_unlock_shared(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_unlock_shared(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_xact_lock(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_xact_lock(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_xact_lock_shared(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_advisory_xact_lock_shared(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_available_extension_versions(OUT name name, OUT version text, OUT superuser boolean, OUT trusted boolean, OUT relocatable boolean, OUT schema name, OUT requires name[], OUT comment text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_available_extensions(OUT name name, OUT default_version text, OUT comment text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_available_wal_summaries(OUT tli bigint, OUT start_lsn pg_lsn, OUT end_lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_backend_pid()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_backup_start(label text, fast boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_backup_stop(wait_for_archive boolean, OUT lsn pg_lsn, OUT labelfile text, OUT spcmapfile text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_basetype(regtype)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_blocking_pids(integer)` | exact (call only) | exact (call only) | differs | differs (error) |
| `pg_cancel_backend(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_char_to_encoding(name)` | exact (call only) | exact (call only) | refused | refused |
| `pg_client_encoding()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_collation_actual_version(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_collation_for("any")` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_collation_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_column_compression("any")` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_column_is_updatable(regclass, smallint, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_column_size("any")` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_column_toast_chunk_id("any")` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_conf_load_time()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_config(OUT name text, OUT setting text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_control_checkpoint(OUT checkpoint_lsn pg_lsn, OUT redo_lsn pg_lsn, OUT redo_wal_file text, OUT timeline_id integer, OUT prev_timeline_id integer, OUT full_page_writes boolean, OUT next_xid text, OUT next_oid oid, OUT next_multixact_id xid, OUT next_multi_offset xid, OUT oldest_xid xid, OUT oldest_xid_dbid oid, OUT oldest_active_xid xid, OUT oldest_multi_xid xid, OUT oldest_multi_dbid oid, OUT oldest_commit_ts_xid xid, OUT newest_commit_ts_xid xid, OUT checkpoint_time timestamp with time zone)` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_control_recovery(OUT min_recovery_end_lsn pg_lsn, OUT min_recovery_end_timeline integer, OUT backup_start_lsn pg_lsn, OUT backup_end_lsn pg_lsn, OUT end_of_backup_record_required boolean)` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_control_system(OUT pg_control_version integer, OUT catalog_version_no integer, OUT system_identifier bigint, OUT pg_control_last_modified timestamp with time zone)` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_conversion_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name, OUT slot_name name, OUT lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name, temporary boolean, OUT slot_name name, OUT lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name, temporary boolean, plugin name, OUT slot_name name, OUT lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_copy_physical_replication_slot(src_slot_name name, dst_slot_name name, OUT slot_name name, OUT lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_copy_physical_replication_slot(src_slot_name name, dst_slot_name name, temporary boolean, OUT slot_name name, OUT lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_create_logical_replication_slot(slot_name name, plugin name, temporary boolean, twophase boolean, failover boolean, OUT slot_name name, OUT lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_create_physical_replication_slot(slot_name name, immediately_reserve boolean, temporary boolean, OUT slot_name name, OUT lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_create_restore_point(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_current_logfile()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_current_logfile(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_current_snapshot()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_current_wal_flush_lsn()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_current_wal_insert_lsn()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_current_wal_lsn()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_current_xact_id()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_current_xact_id_if_assigned()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_cursor(OUT name text, OUT statement text, OUT is_holdable boolean, OUT is_binary boolean, OUT is_scrollable boolean, OUT creation_time timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_database_collation_actual_version(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_database_size(name)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_database_size(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_describe_object(oid, oid, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_drop_replication_slot(name)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_encoding_max_length(integer)` | exact | differs | refused | refused |
| `pg_encoding_to_char(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_export_snapshot()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_extension_config_dump(regclass, text)` | exact (call only) | exact (call only) | differs (error) | refused |
| `pg_extension_update_paths(name name, OUT source text, OUT target text, OUT path text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_filenode_relation(oid, oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_function_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_backend_memory_contexts(OUT name text, OUT ident text, OUT parent text, OUT level integer, OUT total_bytes bigint, OUT total_nblocks bigint, OUT free_bytes bigint, OUT free_chunks bigint, OUT used_bytes bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_catalog_foreign_keys(OUT fktable regclass, OUT fkcols text[], OUT pktable regclass, OUT pkcols text[], OUT is_array boolean, OUT is_opt boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_constraintdef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_constraintdef(oid, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_expr(pg_node_tree, oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_expr(pg_node_tree, oid, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_function_arg_default(oid, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_function_arguments(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_function_identity_arguments(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_function_result(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_function_sqlbody(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_functiondef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_indexdef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_indexdef(oid, integer, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_keywords(OUT word text, OUT catcode "char", OUT barelabel boolean, OUT catdesc text, OUT baredesc text)` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_get_multixact_members(multixid xid, OUT xid xid, OUT mode text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_object_address(type text, object_names text[], object_args text[], OUT classid oid, OUT objid oid, OUT objsubid integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_partition_constraintdef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_partkeydef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_publication_tables(VARIADIC pubname text[], OUT pubid oid, OUT relid oid, OUT attrs int2vector, OUT qual pg_node_tree)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_get_replica_identity_index(regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_replication_slots(OUT slot_name name, OUT plugin name, OUT slot_type text, OUT datoid oid, OUT temporary boolean, OUT active boolean, OUT active_pid integer, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn pg_lsn, OUT confirmed_flush_lsn pg_lsn, OUT wal_status text, OUT safe_wal_size bigint, OUT two_phase boolean, OUT inactive_since timestamp with time zone, OUT conflicting boolean, OUT invalidation_reason text, OUT failover boolean, OUT synced boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_ruledef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_ruledef(oid, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_serial_sequence(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_shmem_allocations(OUT name text, OUT off bigint, OUT size bigint, OUT allocated_size bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_statisticsobjdef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_statisticsobjdef_columns(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_statisticsobjdef_expressions(oid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_get_triggerdef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_triggerdef(oid, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_userbyid(oid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_get_viewdef(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_viewdef(oid, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_viewdef(oid, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_viewdef(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_viewdef(text, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_wait_events(OUT type text, OUT name text, OUT description text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_wal_resource_managers(OUT rm_id integer, OUT rm_name text, OUT rm_builtin boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_get_wal_summarizer_state(OUT summarized_tli bigint, OUT summarized_lsn pg_lsn, OUT pending_lsn pg_lsn, OUT summarizer_pid integer)` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_has_role(name, name, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_has_role(name, oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_has_role(name, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_has_role(oid, name, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_has_role(oid, oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_has_role(oid, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_hba_file_rules(OUT rule_number integer, OUT file_name text, OUT line_number integer, OUT type text, OUT database text[], OUT user_name text[], OUT address text, OUT netmask text, OUT auth_method text, OUT options text[], OUT error text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ident_file_mappings(OUT map_number integer, OUT file_name text, OUT line_number integer, OUT map_name text, OUT sys_name text, OUT pg_username text, OUT error text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_identify_object(classid oid, objid oid, objsubid integer, OUT type text, OUT schema text, OUT name text, OUT identity text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_identify_object_as_address(classid oid, objid oid, objsubid integer, OUT type text, OUT object_names text[], OUT object_args text[])` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_import_system_collations(regnamespace)` | exact (call only) | exact (call only) | differs (error) | refused |
| `pg_index_column_has_property(regclass, integer, text)` | refused | refused | refused | refused |
| `pg_index_has_property(regclass, text)` | refused | refused | refused | refused |
| `pg_indexam_has_property(oid, text)` | exact (call only) | exact (call only) | refused | refused |
| `pg_indexam_progress_phasename(oid, bigint)` | exact | exact | differs (error) | differs (error) |
| `pg_indexes_size(regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_input_error_info(value text, type_name text, OUT message text, OUT detail text, OUT hint text, OUT sql_error_code text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_input_is_valid(text, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_is_in_recovery()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_is_other_temp_schema(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_isolation_test_session_is_blocked(integer, integer[])` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_jit_available()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_last_wal_receive_lsn()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_last_wal_replay_lsn()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_last_xact_replay_timestamp()` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_listening_channels()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_lock_status(OUT locktype text, OUT database oid, OUT relation oid, OUT page integer, OUT tuple smallint, OUT virtualxid text, OUT transactionid xid, OUT classid oid, OUT objid oid, OUT objsubid smallint, OUT virtualtransaction text, OUT pid integer, OUT mode text, OUT granted boolean, OUT fastpath boolean, OUT waitstart timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_log_backend_memory_contexts(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_log_standby_snapshot()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_logical_emit_message(transactional boolean, prefix text, message bytea, flush boolean)` | exact (call only) | exact (call only) | differs (error) | exact (call only) |
| `pg_logical_emit_message(transactional boolean, prefix text, message text, flush boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ls_archive_statusdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ls_dir(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_ls_dir(text, boolean, boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_ls_logicalmapdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ls_logicalsnapdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ls_replslotdir(slot_name text, OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_ls_tmpdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ls_tmpdir(tablespace oid, OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | exact (call only) | exact (call only) | differs (error) | refused |
| `pg_ls_waldir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_mcv_list_items(mcv_list pg_mcv_list, OUT index integer, OUT "values" text[], OUT nulls boolean[], OUT frequency double precision, OUT base_frequency double precision)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_my_temp_schema()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_nextoid(regclass, name, regclass)` | exact (call only) | exact (call only) | differs (error) | refused |
| `pg_notification_queue_usage()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_notify(text, text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_opclass_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_operator_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_opfamily_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_options_to_table(options_array text[], OUT option_name text, OUT option_value text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_partition_ancestors(partitionid regclass, OUT relid regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_partition_root(regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_partition_tree(rootrelid regclass, OUT relid regclass, OUT parentrelid regclass, OUT isleaf boolean, OUT level integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_postmaster_start_time()` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `pg_prepared_statement(OUT name text, OUT statement text, OUT prepare_time timestamp with time zone, OUT parameter_types regtype[], OUT result_types regtype[], OUT from_sql boolean, OUT generic_plans bigint, OUT custom_plans bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_prepared_xact(OUT transaction xid, OUT gid text, OUT prepared timestamp with time zone, OUT ownerid oid, OUT dbid oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_promote(wait boolean, wait_seconds integer)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_read_binary_file(text)` | exact (call only) | exact (call only) | differs | refused |
| `pg_read_binary_file(text, bigint, bigint)` | exact (call only) | exact (call only) | differs | refused |
| `pg_read_binary_file(text, bigint, bigint, boolean)` | exact (call only) | exact (call only) | differs | refused |
| `pg_read_binary_file(text, boolean)` | exact (call only) | exact (call only) | differs | refused |
| `pg_read_file(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_read_file(text, bigint, bigint)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_read_file(text, bigint, bigint, boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_read_file(text, boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_relation_filenode(regclass)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_relation_filepath(regclass)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_relation_is_publishable(regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_relation_is_updatable(regclass, boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_relation_size(regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_relation_size(regclass, text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_reload_conf()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_replication_origin_advance(text, pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_replication_origin_create(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_replication_origin_drop(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_replication_origin_oid(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_replication_origin_progress(text, boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_replication_origin_session_is_setup()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_replication_origin_session_progress(boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_replication_origin_session_setup(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_replication_origin_xact_reset()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_replication_slot_advance(slot_name name, upto_lsn pg_lsn, OUT slot_name name, OUT end_lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_rotate_logfile()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_safe_snapshot_blocking_pids(integer)` | exact (call only) | exact (call only) | differs | differs (error) |
| `pg_sequence_last_value(regclass)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_sequence_parameters(sequence_oid oid, OUT start_value bigint, OUT minimum_value bigint, OUT maximum_value bigint, OUT increment bigint, OUT cycle_option boolean, OUT cache_size bigint, OUT data_type oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `pg_settings_get_flags(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_show_all_file_settings(OUT sourcefile text, OUT sourceline integer, OUT seqno integer, OUT name text, OUT setting text, OUT applied boolean, OUT error text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_show_all_settings(OUT name text, OUT setting text, OUT unit text, OUT category text, OUT short_desc text, OUT extra_desc text, OUT context text, OUT vartype text, OUT source text, OUT min_val text, OUT max_val text, OUT enumvals text[], OUT boot_val text, OUT reset_val text, OUT sourcefile text, OUT sourceline integer, OUT pending_restart boolean)` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn pg_lsn, OUT local_lsn pg_lsn)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_size_bytes(text)` | exact | exact | refused | refused |
| `pg_size_pretty(bigint)` | exact | exact | differs (error) | differs (error) |
| `pg_snapshot_xip(pg_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `pg_snapshot_xmax(pg_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `pg_snapshot_xmin(pg_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `pg_split_walfile_name(file_name text, OUT segment_number numeric, OUT timeline_id bigint)` | exact (call only) | exact (call only) | refused | refused |
| `pg_stat_clear_snapshot()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_file(filename text, OUT size bigint, OUT access timestamp with time zone, OUT modification timestamp with time zone, OUT change timestamp with time zone, OUT creation timestamp with time zone, OUT isdir boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_stat_file(filename text, missing_ok boolean, OUT size bigint, OUT access timestamp with time zone, OUT modification timestamp with time zone, OUT change timestamp with time zone, OUT creation timestamp with time zone, OUT isdir boolean)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_stat_force_next_flush()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_activity(pid integer, OUT datid oid, OUT pid integer, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT wait_event_type text, OUT wait_event text, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT backend_xid xid, OUT backend_xmin xid, OUT backend_type text, OUT ssl boolean, OUT sslversion text, OUT sslcipher text, OUT sslbits integer, OUT ssl_client_dn text, OUT ssl_client_serial numeric, OUT ssl_issuer_dn text, OUT gss_auth boolean, OUT gss_princ text, OUT gss_enc boolean, OUT gss_delegation boolean, OUT leader_pid integer, OUT query_id bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_analyze_count(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_archiver(OUT archived_count bigint, OUT last_archived_wal text, OUT last_archived_time timestamp with time zone, OUT failed_count bigint, OUT last_failed_wal text, OUT last_failed_time timestamp with time zone, OUT stats_reset timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_autoanalyze_count(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_autovacuum_count(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_backend_activity(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_backend_activity_start(integer)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_backend_client_addr(integer)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_stat_get_backend_client_port(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_backend_dbid(integer)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_stat_get_backend_idset()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_backend_pid(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_backend_start(integer)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_backend_subxact(bid integer, OUT subxact_count integer, OUT subxact_overflowed boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_backend_userid(integer)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_stat_get_backend_wait_event(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_backend_wait_event_type(integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_backend_xact_start(integer)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_bgwriter_buf_written_clean()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_bgwriter_maxwritten_clean()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_bgwriter_stat_reset_time()` | differs | differs | differs | differs |
| `pg_stat_get_blocks_fetched(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_blocks_hit(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_buf_alloc()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_buffers_written()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_num_requested()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_num_timed()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_restartpoints_performed()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_restartpoints_requested()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_restartpoints_timed()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_stat_reset_time()` | differs | differs | differs | differs |
| `pg_stat_get_checkpointer_sync_time()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_checkpointer_write_time()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_stat_get_db_active_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_blk_read_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_blk_write_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_blocks_fetched(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_blocks_hit(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_checksum_failures(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_checksum_last_failure(oid)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_db_conflict_all(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_conflict_bufferpin(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_conflict_lock(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_conflict_logicalslot(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_conflict_snapshot(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_conflict_startup_deadlock(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_conflict_tablespace(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_deadlocks(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_idle_in_transaction_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_numbackends(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_session_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_sessions(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_sessions_abandoned(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_sessions_fatal(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_sessions_killed(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_stat_reset_time(oid)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_db_temp_bytes(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_temp_files(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_tuples_deleted(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_tuples_fetched(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_tuples_inserted(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_tuples_returned(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_tuples_updated(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_xact_commit(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_db_xact_rollback(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_dead_tuples(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_function_calls(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_function_self_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_function_total_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_ins_since_vacuum(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_io(OUT backend_type text, OUT object text, OUT context text, OUT reads bigint, OUT read_time double precision, OUT writes bigint, OUT write_time double precision, OUT writebacks bigint, OUT writeback_time double precision, OUT extends bigint, OUT extend_time double precision, OUT op_bytes bigint, OUT hits bigint, OUT evictions bigint, OUT reuses bigint, OUT fsyncs bigint, OUT fsync_time double precision, OUT stats_reset timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_last_analyze_time(oid)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_last_autoanalyze_time(oid)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_last_autovacuum_time(oid)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_last_vacuum_time(oid)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_lastscan(oid)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_live_tuples(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_mod_since_analyze(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_numscans(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_progress_info(cmdtype text, OUT pid integer, OUT datid oid, OUT relid oid, OUT param1 bigint, OUT param2 bigint, OUT param3 bigint, OUT param4 bigint, OUT param5 bigint, OUT param6 bigint, OUT param7 bigint, OUT param8 bigint, OUT param9 bigint, OUT param10 bigint, OUT param11 bigint, OUT param12 bigint, OUT param13 bigint, OUT param14 bigint, OUT param15 bigint, OUT param16 bigint, OUT param17 bigint, OUT param18 bigint, OUT param19 bigint, OUT param20 bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_recovery_prefetch(OUT stats_reset timestamp with time zone, OUT prefetch bigint, OUT hit bigint, OUT skip_init bigint, OUT skip_new bigint, OUT skip_fpw bigint, OUT skip_rep bigint, OUT wal_distance integer, OUT block_distance integer, OUT io_depth integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_replication_slot(slot_name text, OUT slot_name text, OUT spill_txns bigint, OUT spill_count bigint, OUT spill_bytes bigint, OUT stream_txns bigint, OUT stream_count bigint, OUT stream_bytes bigint, OUT total_txns bigint, OUT total_bytes bigint, OUT stats_reset timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_slru(OUT name text, OUT blks_zeroed bigint, OUT blks_hit bigint, OUT blks_read bigint, OUT blks_written bigint, OUT blks_exists bigint, OUT flushes bigint, OUT truncates bigint, OUT stats_reset timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_snapshot_timestamp()` | exact (call only) | exact (call only) | exact (call only) | differs |
| `pg_stat_get_subscription(subid oid, OUT subid oid, OUT relid oid, OUT pid integer, OUT leader_pid integer, OUT received_lsn pg_lsn, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn pg_lsn, OUT latest_end_time timestamp with time zone, OUT worker_type text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_subscription_stats(subid oid, OUT subid oid, OUT apply_error_count bigint, OUT sync_error_count bigint, OUT stats_reset timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_tuples_deleted(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_tuples_fetched(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_tuples_hot_updated(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_tuples_inserted(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_tuples_newpage_updated(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_tuples_returned(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_tuples_updated(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_vacuum_count(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_wal(OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT wal_buffers_full bigint, OUT wal_write bigint, OUT wal_sync bigint, OUT wal_write_time double precision, OUT wal_sync_time double precision, OUT stats_reset timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_wal_receiver(OUT pid integer, OUT status text, OUT receive_start_lsn pg_lsn, OUT receive_start_tli integer, OUT written_lsn pg_lsn, OUT flushed_lsn pg_lsn, OUT received_tli integer, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn pg_lsn, OUT latest_end_time timestamp with time zone, OUT slot_name text, OUT sender_host text, OUT sender_port integer, OUT conninfo text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_wal_senders(OUT pid integer, OUT state text, OUT sent_lsn pg_lsn, OUT write_lsn pg_lsn, OUT flush_lsn pg_lsn, OUT replay_lsn pg_lsn, OUT write_lag interval, OUT flush_lag interval, OUT replay_lag interval, OUT sync_priority integer, OUT sync_state text, OUT reply_time timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_blocks_fetched(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_blocks_hit(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_function_calls(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_function_self_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_function_total_time(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_numscans(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_tuples_deleted(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_tuples_fetched(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_tuples_hot_updated(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_tuples_inserted(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_tuples_newpage_updated(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_tuples_returned(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_get_xact_tuples_updated(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_have_stats(text, oid, oid)` | exact (call only) | exact (call only) | differs (error) | refused |
| `pg_stat_reset()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_reset_replication_slot(text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_stat_reset_shared(target text)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_stat_reset_single_function_counters(oid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_stat_reset_single_table_counters(oid)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `pg_stat_reset_slru(target text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_stat_reset_subscription_stats(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_statistics_obj_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_switch_wal()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_table_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_table_size(regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_tablespace_databases(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_tablespace_location(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_tablespace_size(name)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_tablespace_size(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_terminate_backend(pid integer, timeout bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_timezone_abbrevs(OUT abbrev text, OUT utc_offset interval, OUT is_dst boolean)` | exact (form only) | exact (form only) | refused | refused |
| `pg_timezone_names(OUT name text, OUT abbrev text, OUT utc_offset interval, OUT is_dst boolean)` | exact (form only) | exact (form only) | refused | refused |
| `pg_total_relation_size(regclass)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_trigger_depth()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `pg_try_advisory_lock(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_try_advisory_lock(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_try_advisory_lock_shared(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_try_advisory_lock_shared(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_try_advisory_xact_lock(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_try_advisory_xact_lock(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_try_advisory_xact_lock_shared(bigint)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_try_advisory_xact_lock_shared(integer, integer)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ts_config_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ts_dict_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ts_parser_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_ts_template_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_type_is_visible(oid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_typeof("any")` | exact (call only) | exact (call only) | refused | refused |
| `pg_visible_in_snapshot(xid8, pg_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `pg_wal_lsn_diff(pg_lsn, pg_lsn)` | exact | exact | refused | refused |
| `pg_wal_summary_contents(tli bigint, start_lsn pg_lsn, end_lsn pg_lsn, OUT relfilenode oid, OUT reltablespace oid, OUT reldatabase oid, OUT relforknumber smallint, OUT relblocknumber bigint, OUT is_limit_block boolean)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `pg_walfile_name(pg_lsn)` | exact | exact | refused | refused |
| `pg_walfile_name_offset(lsn pg_lsn, OUT file_name text, OUT file_offset integer)` | exact | exact | refused | refused |
| `pg_xact_commit_timestamp(xid)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_xact_commit_timestamp_origin(xid xid, OUT "timestamp" timestamp with time zone, OUT roident oid)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `pg_xact_status(xid8)` | exact (call only) | exact (call only) | differs (error) | refused |
| `shobj_description(oid, name)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regclass(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regcollation(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regnamespace(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regoper(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regoperator(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regproc(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regprocedure(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regrole(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regtype(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `to_regtypemod(text)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `txid_current()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `txid_current_if_assigned()` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `txid_current_snapshot()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |
| `txid_snapshot_xip(txid_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `txid_snapshot_xmax(txid_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `txid_snapshot_xmin(txid_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `txid_status(bigint)` | exact (call only) | exact (call only) | exact (call only) | refused |
| `txid_visible_in_snapshot(bigint, txid_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `version()` | exact (form only) | exact (form only) | exact (form only) | exact (form only) |

</details>

<details><summary>Text search (87)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `array_to_tsvector(text[])` | exact | differs (error) | differs (error) | differs (error) |
| `get_current_ts_config()` | exact | differs (error) | differs (error) | differs (error) |
| `json_to_tsvector(json, jsonb)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `json_to_tsvector(regconfig, json, jsonb)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `jsonb_to_tsvector(jsonb, jsonb)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `jsonb_to_tsvector(regconfig, jsonb, jsonb)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `length(tsvector)` | exact | exact | exact | refused |
| `numnode(tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `pg_control_init(OUT max_data_alignment integer, OUT database_block_size integer, OUT blocks_per_segment integer, OUT wal_block_size integer, OUT bytes_per_wal_segment integer, OUT max_identifier_length integer, OUT max_index_columns integer, OUT max_toast_chunk_size integer, OUT large_object_chunk_size integer, OUT float8_pass_by_value boolean, OUT data_page_checksum_version integer)` | exact (form only) | exact (form only) | differs (error) | differs (error) |
| `phraseto_tsquery(regconfig, text)` | exact | differs (error) | differs (error) | differs (error) |
| `phraseto_tsquery(text)` | exact | differs (error) | differs (error) | differs (error) |
| `plainto_tsquery(regconfig, text)` | exact | differs (error) | differs (error) | differs (error) |
| `plainto_tsquery(text)` | exact | differs (error) | differs (error) | differs (error) |
| `querytree(tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `setweight(tsvector, "char")` | exact | differs (error) | differs (error) | differs (error) |
| `setweight(tsvector, "char", text[])` | exact | differs (error) | differs (error) | differs (error) |
| `strip(tsvector)` | exact | exact | exact | refused |
| `to_tsquery(regconfig, text)` | exact | differs (error) | differs (error) | differs (error) |
| `to_tsquery(text)` | exact | differs (error) | differs (error) | differs (error) |
| `to_tsvector(json)` | exact | differs (error) | differs (error) | differs (error) |
| `to_tsvector(jsonb)` | exact | differs (error) | differs (error) | differs (error) |
| `to_tsvector(regconfig, json)` | exact | differs (error) | differs (error) | differs (error) |
| `to_tsvector(regconfig, jsonb)` | exact | differs (error) | differs (error) | differs (error) |
| `to_tsvector(regconfig, text)` | exact | differs (error) | differs (error) | differs (error) |
| `to_tsvector(text)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_debug(config regconfig, document text, OUT alias text, OUT description text, OUT token text, OUT dictionaries regdictionary[], OUT dictionary regdictionary, OUT lexemes text[])` | exact | differs (error) | differs (error) | differs (error) |
| `ts_debug(document text, OUT alias text, OUT description text, OUT token text, OUT dictionaries regdictionary[], OUT dictionary regdictionary, OUT lexemes text[])` | exact | differs (error) | differs (error) | differs (error) |
| `ts_delete(tsvector, text)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_delete(tsvector, text[])` | exact | differs (error) | differs (error) | differs (error) |
| `ts_filter(tsvector, "char"[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_headline(json, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(json, tsquery, text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_headline(jsonb, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(jsonb, tsquery, text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_headline(regconfig, json, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(regconfig, json, tsquery, text)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(regconfig, jsonb, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(regconfig, jsonb, tsquery, text)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(regconfig, text, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(regconfig, text, tsquery, text)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(text, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_headline(text, tsquery, text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_lexize(regdictionary, text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_match_qv(tsquery, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_match_tq(text, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_match_tt(text, text)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_match_vq(tsvector, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_parse(parser_name text, txt text, OUT tokid integer, OUT token text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_parse(parser_oid oid, txt text, OUT tokid integer, OUT token text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_rank(real[], tsvector, tsquery)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_rank(real[], tsvector, tsquery, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_rank(tsvector, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_rank(tsvector, tsquery, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_rank_cd(real[], tsvector, tsquery)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_rank_cd(real[], tsvector, tsquery, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_rank_cd(tsvector, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_rank_cd(tsvector, tsquery, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_rewrite(tsquery, text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_rewrite(tsquery, tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `ts_stat(query text, OUT word text, OUT ndoc integer, OUT nentry integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `ts_stat(query text, weights text, OUT word text, OUT ndoc integer, OUT nentry integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tsq_mcontained(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsq_mcontains(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_and(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_cmp(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_eq(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_ge(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_gt(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_le(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_lt(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_ne(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_not(tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_or(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_phrase(tsquery, tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquery_phrase(tsquery, tsquery, integer)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_cmp(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_concat(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_eq(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_ge(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_gt(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_le(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_lt(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_ne(tsvector, tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvector_to_array(tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `unnest(tsvector tsvector, OUT lexeme text, OUT positions smallint[], OUT weights text[])` | exact | exact | refused | refused |
| `websearch_to_tsquery(regconfig, text)` | exact | differs (error) | differs (error) | differs (error) |
| `websearch_to_tsquery(text)` | exact | differs (error) | differs (error) | differs (error) |

</details>

<details><summary>Type operators (732)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `aclitemin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `aclitemout(aclitem)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `any_in(cstring)` | differs | differs (error) | differs (error) | differs (error) |
| `any_out("any")` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anyarray_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anyarray_out(anyarray)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anyarray_send(anyarray)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anycompatible_in(cstring)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatible_out(anycompatible)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatiblearray_in(cstring)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatiblearray_out(anycompatiblearray)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anycompatiblearray_send(anycompatiblearray)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anycompatiblemultirange_in(cstring, oid, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatiblemultirange_out(anycompatiblemultirange)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatiblenonarray_in(cstring)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatiblenonarray_out(anycompatiblenonarray)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatiblerange_in(cstring, oid, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `anycompatiblerange_out(anycompatiblerange)` | differs | differs (error) | differs (error) | differs (error) |
| `anyelement_in(cstring)` | differs | differs (error) | differs (error) | differs (error) |
| `anyelement_out(anyelement)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anyenum_in(cstring)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `anyenum_out(anyenum)` | differs | differs (error) | differs (error) | differs (error) |
| `anymultirange_in(cstring, oid, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `anymultirange_out(anymultirange)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `anynonarray_in(cstring)` | differs | differs (error) | differs (error) | differs (error) |
| `anynonarray_out(anynonarray)` | differs | differs (error) | differs (error) | differs (error) |
| `anyrange_in(cstring, oid, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `anyrange_out(anyrange)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `array_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `array_out(anyarray)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `array_send(anyarray)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `bit_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `bit_out(bit)` | exact | differs (error) | differs (error) | differs (error) |
| `bit_send(bit)` | exact | differs (error) | differs (error) | differs (error) |
| `bitand(bit, bit)` | exact | exact | differs (error) | differs (error) |
| `bitcmp(bit, bit)` | exact | exact | exact | exact |
| `biteq(bit, bit)` | exact | exact | exact | exact |
| `bitge(bit, bit)` | exact | exact | exact | exact |
| `bitgt(bit, bit)` | exact | exact | exact | exact |
| `bitle(bit, bit)` | exact | exact | exact | exact |
| `bitlt(bit, bit)` | exact | exact | exact | exact |
| `bitne(bit, bit)` | exact | exact | exact | exact |
| `bitnot(bit)` | exact | exact | differs (error) | differs (error) |
| `bitor(bit, bit)` | exact | exact | differs (error) | differs (error) |
| `bittypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `bittypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `bitxor(bit, bit)` | exact | exact | differs (error) | differs (error) |
| `booleq(boolean, boolean)` | exact | exact | exact | exact |
| `boolge(boolean, boolean)` | exact | exact | exact | exact |
| `boolgt(boolean, boolean)` | exact | exact | exact | exact |
| `boolin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `boolle(boolean, boolean)` | exact | exact | exact | exact |
| `boollt(boolean, boolean)` | exact | exact | exact | exact |
| `boolne(boolean, boolean)` | exact | exact | exact | exact |
| `boolout(boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `boolsend(boolean)` | exact | differs (error) | differs (error) | differs (error) |
| `box_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `box_out(box)` | exact | differs (error) | differs (error) | differs (error) |
| `box_send(box)` | exact | differs (error) | differs (error) | differs (error) |
| `bpchar_larger(character, character)` | exact | differs | differs (error) | differs (error) |
| `bpchar_pattern_ge(character, character)` | exact | exact | exact | exact |
| `bpchar_pattern_gt(character, character)` | exact | exact | exact | exact |
| `bpchar_pattern_le(character, character)` | exact | exact | exact | exact |
| `bpchar_pattern_lt(character, character)` | exact | exact | exact | exact |
| `bpchar_smaller(character, character)` | exact | differs | differs (error) | differs (error) |
| `bpcharcmp(character, character)` | exact | exact | differs (error) | differs (error) |
| `bpchareq(character, character)` | exact | exact | exact | exact |
| `bpcharge(character, character)` | exact | exact | differs (error) | differs (error) |
| `bpchargt(character, character)` | exact | exact | differs (error) | differs (error) |
| `bpcharin(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `bpcharle(character, character)` | exact | exact | differs (error) | differs (error) |
| `bpcharlt(character, character)` | exact | exact | differs (error) | differs (error) |
| `bpcharne(character, character)` | exact | exact | exact | exact |
| `bpcharout(character)` | exact | differs (error) | differs (error) | differs (error) |
| `bpcharsend(character)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `bpchartypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `bpchartypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `byteacmp(bytea, bytea)` | exact | exact | exact | exact |
| `byteaeq(bytea, bytea)` | exact | exact | exact | exact |
| `byteage(bytea, bytea)` | exact | exact | exact | exact |
| `byteagt(bytea, bytea)` | exact | exact | exact | exact |
| `byteain(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `byteale(bytea, bytea)` | exact | exact | exact | exact |
| `bytealt(bytea, bytea)` | exact | exact | exact | exact |
| `byteane(bytea, bytea)` | exact | exact | exact | exact |
| `byteaout(bytea)` | exact | differs (error) | differs (error) | differs (error) |
| `byteasend(bytea)` | exact | differs (error) | differs (error) | differs (error) |
| `cash_in(cstring)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `cash_out(money)` | exact (call only) | differs | exact (call only) | refused |
| `cash_send(money)` | exact | exact | differs | refused |
| `chareq("char", "char")` | exact | exact | exact | exact |
| `charge("char", "char")` | exact | exact | exact | exact |
| `chargt("char", "char")` | exact | exact | exact | exact |
| `charin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `charle("char", "char")` | exact | exact | exact | exact |
| `charlt("char", "char")` | exact | exact | exact | exact |
| `charne("char", "char")` | exact | exact | exact | exact |
| `charout("char")` | exact | differs (error) | differs (error) | differs (error) |
| `charsend("char")` | exact | differs (error) | differs (error) | differs (error) |
| `cideq(cid, cid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `cidin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `cidout(cid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `cidr_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `cidr_out(cidr)` | exact | differs (error) | differs (error) | differs (error) |
| `cidr_send(cidr)` | exact | differs (error) | differs (error) | differs (error) |
| `cidsend(cid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `circle_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `circle_out(circle)` | exact | differs (error) | differs (error) | differs (error) |
| `circle_send(circle)` | exact | differs (error) | differs (error) | differs (error) |
| `cstring_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `cstring_out(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `cstring_send(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `date_bin(interval, timestamp with time zone, timestamp with time zone)` | exact | exact | differs (error) | differs (error) |
| `date_bin(interval, timestamp without time zone, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `date_cmp(date, date)` | exact | exact | exact | exact |
| `date_cmp_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_cmp_timestamptz(date, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `date_eq(date, date)` | exact | exact | exact | exact |
| `date_eq_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_eq_timestamptz(date, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `date_ge(date, date)` | exact | exact | exact | exact |
| `date_ge_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_ge_timestamptz(date, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `date_gt(date, date)` | exact | exact | exact | exact |
| `date_gt_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_gt_timestamptz(date, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `date_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `date_larger(date, date)` | exact | exact | exact | exact |
| `date_le(date, date)` | exact | exact | exact | exact |
| `date_le_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_le_timestamptz(date, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `date_lt(date, date)` | exact | exact | exact | exact |
| `date_lt_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_lt_timestamptz(date, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `date_mi(date, date)` | exact | exact | differs (error) | exact |
| `date_mi_interval(date, interval)` | exact | exact | differs (error) | differs (error) |
| `date_mii(date, integer)` | exact | exact | differs (error) | exact |
| `date_ne(date, date)` | exact | exact | exact | exact |
| `date_ne_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_ne_timestamptz(date, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `date_out(date)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `date_pl_interval(date, interval)` | exact | exact | differs (error) | differs (error) |
| `date_pli(date, integer)` | exact | exact | differs (error) | exact |
| `date_send(date)` | exact | differs (error) | differs (error) | differs (error) |
| `date_smaller(date, date)` | exact | exact | exact | exact |
| `date_subtract(timestamp with time zone, interval)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `date_subtract(timestamp with time zone, interval, text)` | exact | differs (error) | differs (error) | differs (error) |
| `domain_in(cstring, oid, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `enum_in(cstring, oid)` | equivalent [2](#rule-enum-label-text) | differs (error) | differs (error) | differs (error) |
| `enum_out(anyenum)` | differs | differs (error) | differs (error) | differs (error) |
| `enum_send(anyenum)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float4abs(real)` | exact | exact | exact | exact |
| `float4div(real, real)` | exact | exact | exact | exact |
| `float4eq(real, real)` | exact | exact | exact | exact |
| `float4ge(real, real)` | exact | exact | exact | exact |
| `float4gt(real, real)` | exact | exact | exact | exact |
| `float4in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float4larger(real, real)` | exact | exact | exact | exact |
| `float4le(real, real)` | exact | exact | exact | exact |
| `float4lt(real, real)` | exact | exact | exact | exact |
| `float4mi(real, real)` | exact | exact | exact | exact |
| `float4mul(real, real)` | exact | exact | exact | exact |
| `float4ne(real, real)` | exact | exact | exact | exact |
| `float4out(real)` | exact | differs (error) | differs (error) | differs (error) |
| `float4pl(real, real)` | exact | exact | exact | exact |
| `float4send(real)` | exact | differs (error) | differs (error) | differs (error) |
| `float4smaller(real, real)` | exact | exact | exact | exact |
| `float4um(real)` | exact | exact | exact | exact |
| `float4up(real)` | exact | exact | exact | exact |
| `float8_avg(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_corr(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_covar_pop(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_covar_samp(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_avgx(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_avgy(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_intercept(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_r2(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_slope(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_sxx(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_sxy(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_regr_syy(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_stddev_pop(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_stddev_samp(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_var_pop(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8_var_samp(double precision[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8abs(double precision)` | exact | exact | exact | exact |
| `float8div(double precision, double precision)` | exact | exact | exact | exact |
| `float8eq(double precision, double precision)` | exact | exact | exact | exact |
| `float8ge(double precision, double precision)` | exact | exact | exact | exact |
| `float8gt(double precision, double precision)` | exact | exact | exact | exact |
| `float8in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `float8larger(double precision, double precision)` | exact | exact | exact | exact |
| `float8le(double precision, double precision)` | exact | exact | exact | exact |
| `float8lt(double precision, double precision)` | exact | exact | exact | exact |
| `float8mi(double precision, double precision)` | exact | exact | exact | exact |
| `float8mul(double precision, double precision)` | exact | exact | exact | exact |
| `float8ne(double precision, double precision)` | exact | exact | exact | exact |
| `float8out(double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `float8pl(double precision, double precision)` | exact | exact | exact | exact |
| `float8send(double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `float8smaller(double precision, double precision)` | exact | exact | exact | exact |
| `float8um(double precision)` | exact | exact | exact | exact |
| `float8up(double precision)` | exact | exact | exact | exact |
| `inet_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `inet_merge(inet, inet)` | exact | differs (error) | differs (error) | differs (error) |
| `inet_out(inet)` | exact | differs (error) | differs (error) | differs (error) |
| `inet_same_family(inet, inet)` | exact | exact | exact | exact |
| `inet_send(inet)` | exact | differs (error) | differs (error) | differs (error) |
| `inetand(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `inetmi(inet, inet)` | exact | refused | refused | differs (error) |
| `inetmi_int8(inet, bigint)` | exact | exact | exact | differs (error) |
| `inetnot(inet)` | exact | refused | refused | differs |
| `inetor(inet, inet)` | exact | refused | differs (error) | differs (error) |
| `inetpl(inet, bigint)` | exact | refused | refused | differs (error) |
| `int2_sum(bigint, smallint)` | exact | exact | differs (error) | differs |
| `int2abs(smallint)` | exact | exact | exact | exact |
| `int2and(smallint, smallint)` | exact | exact | exact | differs (error) |
| `int2div(smallint, smallint)` | exact | exact | exact | exact |
| `int2eq(smallint, smallint)` | exact | exact | exact | exact |
| `int2ge(smallint, smallint)` | exact | exact | exact | exact |
| `int2gt(smallint, smallint)` | exact | exact | exact | exact |
| `int2in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int2larger(smallint, smallint)` | exact | exact | exact | exact |
| `int2le(smallint, smallint)` | exact | exact | exact | exact |
| `int2lt(smallint, smallint)` | exact | exact | exact | exact |
| `int2mi(smallint, smallint)` | exact | exact | exact | exact |
| `int2mod(smallint, smallint)` | exact | exact | exact | exact |
| `int2mul(smallint, smallint)` | exact | exact | exact | exact |
| `int2ne(smallint, smallint)` | exact | exact | exact | exact |
| `int2not(smallint)` | exact | exact | exact | differs (error) |
| `int2or(smallint, smallint)` | exact | exact | exact | differs (error) |
| `int2out(smallint)` | exact | differs (error) | differs (error) | differs (error) |
| `int2pl(smallint, smallint)` | exact | exact | exact | exact |
| `int2send(smallint)` | exact | differs (error) | differs (error) | differs (error) |
| `int2shl(smallint, integer)` | exact | exact | exact | differs (error) |
| `int2shr(smallint, integer)` | exact | exact | differs (error) | differs (error) |
| `int2smaller(smallint, smallint)` | exact | exact | exact | exact |
| `int2um(smallint)` | exact | exact | exact | exact |
| `int2up(smallint)` | exact | exact | exact | exact |
| `int2vectorin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int2vectorout(int2vector)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int2vectorsend(int2vector)` | exact (call only) | exact (call only) | refused | refused |
| `int2xor(smallint, smallint)` | exact | exact | exact | differs (error) |
| `int4_sum(bigint, integer)` | exact | exact | differs (error) | differs |
| `int4abs(integer)` | exact | exact | exact | exact |
| `int4and(integer, integer)` | exact | exact | exact | differs (error) |
| `int4div(integer, integer)` | exact | exact | exact | exact |
| `int4eq(integer, integer)` | exact | exact | exact | exact |
| `int4ge(integer, integer)` | exact | exact | exact | exact |
| `int4gt(integer, integer)` | exact | exact | exact | exact |
| `int4in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int4inc(integer)` | exact | exact | exact | exact |
| `int4larger(integer, integer)` | exact | exact | exact | exact |
| `int4le(integer, integer)` | exact | exact | exact | exact |
| `int4lt(integer, integer)` | exact | exact | exact | exact |
| `int4mi(integer, integer)` | exact | exact | exact | exact |
| `int4mod(integer, integer)` | exact | exact | exact | exact |
| `int4mul(integer, integer)` | exact | exact | exact | exact |
| `int4ne(integer, integer)` | exact | exact | exact | exact |
| `int4not(integer)` | exact | exact | exact | differs (error) |
| `int4or(integer, integer)` | exact | exact | exact | differs (error) |
| `int4out(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `int4pl(integer, integer)` | exact | exact | exact | exact |
| `int4send(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `int4shl(integer, integer)` | exact | exact | exact | differs (error) |
| `int4shr(integer, integer)` | exact | exact | differs (error) | differs (error) |
| `int4smaller(integer, integer)` | exact | exact | exact | exact |
| `int4um(integer)` | exact | exact | exact | exact |
| `int4up(integer)` | exact | exact | exact | exact |
| `int4xor(integer, integer)` | exact | exact | exact | differs (error) |
| `int8_avg(bigint[])` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int8_sum(numeric, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `int8abs(bigint)` | exact | exact | exact | exact |
| `int8and(bigint, bigint)` | exact | exact | exact | differs (error) |
| `int8div(bigint, bigint)` | exact | exact | exact | exact |
| `int8eq(bigint, bigint)` | exact | exact | exact | exact |
| `int8ge(bigint, bigint)` | exact | exact | exact | exact |
| `int8gt(bigint, bigint)` | exact | exact | exact | exact |
| `int8in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `int8inc(bigint)` | exact | exact | exact | exact |
| `int8inc_float8_float8(bigint, double precision, double precision)` | exact | differs (error) | differs (error) | differs (error) |
| `int8larger(bigint, bigint)` | exact | exact | exact | exact |
| `int8le(bigint, bigint)` | exact | exact | exact | exact |
| `int8lt(bigint, bigint)` | exact | exact | exact | exact |
| `int8mi(bigint, bigint)` | exact | exact | exact | exact |
| `int8mod(bigint, bigint)` | exact | exact | exact | exact |
| `int8mul(bigint, bigint)` | exact | exact | exact | exact |
| `int8ne(bigint, bigint)` | exact | exact | exact | exact |
| `int8not(bigint)` | exact | exact | exact | differs (error) |
| `int8or(bigint, bigint)` | exact | exact | exact | differs (error) |
| `int8out(bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `int8pl(bigint, bigint)` | exact | exact | exact | exact |
| `int8pl_inet(bigint, inet)` | exact | refused | refused | differs (error) |
| `int8send(bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `int8shl(bigint, integer)` | exact | exact | differs (error) | differs (error) |
| `int8shr(bigint, integer)` | exact | exact | differs (error) | differs (error) |
| `int8smaller(bigint, bigint)` | exact | exact | exact | exact |
| `int8um(bigint)` | exact | exact | exact | exact |
| `int8up(bigint)` | exact | exact | exact | exact |
| `int8xor(bigint, bigint)` | exact | exact | exact | differs (error) |
| `interval_cmp(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_div(interval, double precision)` | exact | exact | differs (error) | differs (error) |
| `interval_eq(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_ge(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_gt(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_hash(interval)` | exact | differs (error) | differs (error) | differs (error) |
| `interval_hash_extended(interval, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `interval_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `interval_larger(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_le(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_lt(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_mi(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_mul(interval, double precision)` | exact | exact | differs (error) | differs (error) |
| `interval_ne(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_out(interval)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `interval_pl(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_pl_date(interval, date)` | exact | exact | differs (error) | differs (error) |
| `interval_pl_time(interval, time without time zone)` | exact | exact | differs (error) | differs (error) |
| `interval_pl_timestamp(interval, timestamp without time zone)` | exact | exact | differs (error) | differs (error) |
| `interval_pl_timestamptz(interval, timestamp with time zone)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `interval_pl_timetz(interval, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `interval_send(interval)` | exact | differs (error) | differs (error) | differs (error) |
| `interval_smaller(interval, interval)` | exact | exact | differs (error) | differs (error) |
| `interval_um(interval)` | exact | exact | differs (error) | differs (error) |
| `intervaltypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `intervaltypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `json_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `json_out(json)` | exact | differs (error) | differs (error) | differs (error) |
| `json_send(json)` | exact | refused | refused | refused |
| `jsonb_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `jsonb_out(jsonb)` | exact | differs (error) | differs (error) | differs (error) |
| `jsonb_send(jsonb)` | exact | refused | refused | refused |
| `jsonpath_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `jsonpath_out(jsonpath)` | exact | differs (error) | differs (error) | differs (error) |
| `jsonpath_send(jsonpath)` | exact | differs (error) | differs (error) | differs (error) |
| `line_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `line_out(line)` | exact | differs (error) | differs (error) | differs (error) |
| `line_send(line)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `lseg_out(lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `lseg_send(lseg)` | exact | differs (error) | differs (error) | differs (error) |
| `macaddr8_and(macaddr8, macaddr8)` | exact | exact | differs (error) | differs (error) |
| `macaddr8_cmp(macaddr8, macaddr8)` | exact | exact | exact | differs (error) |
| `macaddr8_eq(macaddr8, macaddr8)` | exact | exact | exact | differs (error) |
| `macaddr8_ge(macaddr8, macaddr8)` | exact | exact | exact | differs (error) |
| `macaddr8_gt(macaddr8, macaddr8)` | exact | exact | exact | differs (error) |
| `macaddr8_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `macaddr8_le(macaddr8, macaddr8)` | exact | exact | exact | differs (error) |
| `macaddr8_lt(macaddr8, macaddr8)` | exact | exact | exact | differs (error) |
| `macaddr8_ne(macaddr8, macaddr8)` | exact | exact | exact | differs (error) |
| `macaddr8_not(macaddr8)` | exact | exact | differs (error) | differs (error) |
| `macaddr8_or(macaddr8, macaddr8)` | exact | exact | differs (error) | differs (error) |
| `macaddr8_out(macaddr8)` | exact | differs (error) | differs (error) | differs (error) |
| `macaddr8_send(macaddr8)` | exact | differs (error) | differs (error) | differs (error) |
| `macaddr8_set7bit(macaddr8)` | exact | exact | differs (error) | differs (error) |
| `macaddr_and(macaddr, macaddr)` | exact | exact | differs (error) | differs (error) |
| `macaddr_cmp(macaddr, macaddr)` | exact | exact | exact | differs (error) |
| `macaddr_eq(macaddr, macaddr)` | exact | exact | exact | differs (error) |
| `macaddr_ge(macaddr, macaddr)` | exact | exact | exact | differs (error) |
| `macaddr_gt(macaddr, macaddr)` | exact | exact | exact | differs (error) |
| `macaddr_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `macaddr_le(macaddr, macaddr)` | exact | exact | exact | differs (error) |
| `macaddr_lt(macaddr, macaddr)` | exact | exact | exact | differs (error) |
| `macaddr_ne(macaddr, macaddr)` | exact | exact | exact | differs (error) |
| `macaddr_not(macaddr)` | exact | exact | differs (error) | differs (error) |
| `macaddr_or(macaddr, macaddr)` | exact | exact | differs (error) | differs (error) |
| `macaddr_out(macaddr)` | exact | differs (error) | differs (error) | differs (error) |
| `macaddr_send(macaddr)` | exact | differs (error) | differs (error) | differs (error) |
| `multirange_in(cstring, oid, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `multirange_out(anymultirange)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `multirange_send(anymultirange)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `nameeq(name, name)` | exact | exact | exact | exact |
| `nameeqtext(name, text)` | exact | exact | exact | exact |
| `namege(name, name)` | exact | exact | exact | exact |
| `namegetext(name, text)` | exact | exact | exact | exact |
| `namegt(name, name)` | exact | exact | exact | exact |
| `namegttext(name, text)` | exact | exact | exact | exact |
| `namein(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `namele(name, name)` | exact | exact | exact | exact |
| `nameletext(name, text)` | exact | exact | exact | exact |
| `namelt(name, name)` | exact | exact | exact | exact |
| `namelttext(name, text)` | exact | exact | exact | exact |
| `namene(name, name)` | exact | exact | exact | exact |
| `namenetext(name, text)` | exact | exact | exact | exact |
| `nameout(name)` | exact | differs (error) | differs (error) | differs (error) |
| `namesend(name)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `numeric_abs(numeric)` | exact | exact | differs | differs |
| `numeric_add(numeric, numeric)` | exact | exact | differs | differs |
| `numeric_cmp(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_div(numeric, numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_div_trunc(numeric, numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_eq(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_exp(numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_ge(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_gt(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `numeric_inc(numeric)` | exact | exact | differs | differs |
| `numeric_larger(numeric, numeric)` | exact | exact | differs | differs |
| `numeric_le(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_ln(numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_log(numeric, numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_lt(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_mod(numeric, numeric)` | exact | exact | equivalent [4](#rule-number-value) | differs (error) |
| `numeric_mul(numeric, numeric)` | exact | exact | differs | differs |
| `numeric_ne(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_out(numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_pl_pg_lsn(numeric, pg_lsn)` | exact | exact | differs (error) | differs |
| `numeric_power(numeric, numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_send(numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_smaller(numeric, numeric)` | exact | exact | differs | differs |
| `numeric_sqrt(numeric)` | exact | differs (error) | differs (error) | differs (error) |
| `numeric_sub(numeric, numeric)` | exact | exact | equivalent [4](#rule-number-value) | equivalent [4](#rule-number-value) |
| `numeric_uminus(numeric)` | exact | exact | differs | differs |
| `numeric_uplus(numeric)` | exact | exact | differs | differs |
| `numerictypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `numerictypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `oideq(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidge(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidgt(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `oidlarger(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidle(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidlt(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidne(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidout(oid)` | exact | differs (error) | differs (error) | differs (error) |
| `oidsend(oid)` | exact | differs (error) | differs (error) | differs (error) |
| `oidsmaller(oid, oid)` | exact | exact | differs (error) | differs (error) |
| `oidvectoreq(oidvector, oidvector)` | exact (call only) | exact (call only) | refused | refused |
| `oidvectorge(oidvector, oidvector)` | exact (call only) | exact (call only) | refused | refused |
| `oidvectorgt(oidvector, oidvector)` | exact (call only) | exact (call only) | refused | refused |
| `oidvectorin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `oidvectorle(oidvector, oidvector)` | exact (call only) | exact (call only) | refused | refused |
| `oidvectorlt(oidvector, oidvector)` | exact (call only) | exact (call only) | refused | refused |
| `oidvectorne(oidvector, oidvector)` | exact (call only) | exact (call only) | refused | refused |
| `oidvectorout(oidvector)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `oidvectorsend(oidvector)` | exact (call only) | exact (call only) | refused | refused |
| `oidvectortypes(oidvector)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `path_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `path_out(path)` | exact | differs (error) | differs (error) | differs (error) |
| `path_send(path)` | exact | differs (error) | differs (error) | differs (error) |
| `pg_ddl_command_in(cstring)` | differs | differs (error) | differs (error) | differs (error) |
| `pg_ddl_command_out(pg_ddl_command)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_ddl_command_send(pg_ddl_command)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_dependencies_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_dependencies_out(pg_dependencies)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_dependencies_send(pg_dependencies)` | exact (call only) | exact (call only) | differs | differs |
| `pg_lsn_cmp(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_eq(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_ge(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_gt(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_hash(pg_lsn)` | exact | differs (error) | differs (error) | differs (error) |
| `pg_lsn_hash_extended(pg_lsn, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `pg_lsn_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_lsn_larger(pg_lsn, pg_lsn)` | exact | exact | differs (error) | differs |
| `pg_lsn_le(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_lt(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_mi(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_mii(pg_lsn, numeric)` | exact | exact | differs (error) | differs |
| `pg_lsn_ne(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_out(pg_lsn)` | exact | differs (error) | differs (error) | differs (error) |
| `pg_lsn_pli(pg_lsn, numeric)` | exact | exact | differs (error) | differs |
| `pg_lsn_send(pg_lsn)` | exact | differs (error) | differs (error) | differs (error) |
| `pg_lsn_smaller(pg_lsn, pg_lsn)` | exact | exact | differs (error) | differs |
| `pg_mcv_list_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_mcv_list_out(pg_mcv_list)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_mcv_list_send(pg_mcv_list)` | exact (call only) | exact (call only) | differs | differs |
| `pg_ndistinct_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_ndistinct_out(pg_ndistinct)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_ndistinct_send(pg_ndistinct)` | exact (call only) | exact (call only) | differs | differs |
| `pg_node_tree_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_node_tree_out(pg_node_tree)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_node_tree_send(pg_node_tree)` | exact (call only) | exact (call only) | differs | differs |
| `pg_snapshot_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_snapshot_out(pg_snapshot)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `pg_snapshot_send(pg_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `point_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `point_out(point)` | exact | differs (error) | differs (error) | differs (error) |
| `point_send(point)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `poly_out(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `poly_send(polygon)` | exact | differs (error) | differs (error) | differs (error) |
| `range_in(cstring, oid, integer)` | differs | differs (error) | differs (error) | differs (error) |
| `range_out(anyrange)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `range_send(anyrange)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `record_eq(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_ge(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_gt(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_image_eq(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_image_ge(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_image_gt(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_image_le(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_image_lt(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_image_ne(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `record_le(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_lt(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_ne(record, record)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `record_out(record)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `record_send(record)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regclassin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regclassout(regclass)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regclasssend(regclass)` | exact (call only) | exact (call only) | refused | refused |
| `regcollationin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regcollationout(regcollation)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regcollationsend(regcollation)` | exact (call only) | exact (call only) | refused | refused |
| `regconfigin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regconfigout(regconfig)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regconfigsend(regconfig)` | exact | differs (error) | refused | refused |
| `regdictionaryin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regdictionaryout(regdictionary)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regdictionarysend(regdictionary)` | exact (call only) | exact (call only) | refused | refused |
| `regnamespacein(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regnamespaceout(regnamespace)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regnamespacesend(regnamespace)` | exact (call only) | exact (call only) | refused | refused |
| `regoperatorin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regoperatorout(regoperator)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regoperatorsend(regoperator)` | exact (call only) | exact (call only) | refused | refused |
| `regoperin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regoperout(regoper)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regopersend(regoper)` | exact (call only) | exact (call only) | refused | refused |
| `regprocedurein(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regprocedureout(regprocedure)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regproceduresend(regprocedure)` | exact (call only) | exact (call only) | refused | refused |
| `regprocin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regprocout(regproc)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regprocsend(regproc)` | exact (call only) | exact (call only) | refused | refused |
| `regrolein(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regroleout(regrole)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regrolesend(regrole)` | exact (call only) | exact (call only) | refused | refused |
| `regtypein(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regtypeout(regtype)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `regtypesend(regtype)` | exact (call only) | exact (call only) | refused | refused |
| `shell_out(void)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `text_ge(text, text)` | exact | exact | differs (error) | differs (error) |
| `text_gt(text, text)` | exact | exact | differs (error) | differs (error) |
| `text_larger(text, text)` | exact | exact | differs (error) | differs (error) |
| `text_le(text, text)` | exact | exact | differs (error) | differs (error) |
| `text_lt(text, text)` | exact | exact | differs (error) | differs (error) |
| `text_pattern_ge(text, text)` | exact | exact | exact | exact |
| `text_pattern_gt(text, text)` | exact | exact | exact | exact |
| `text_pattern_le(text, text)` | exact | exact | exact | exact |
| `text_pattern_lt(text, text)` | exact | exact | exact | exact |
| `text_smaller(text, text)` | exact | exact | differs (error) | differs (error) |
| `texteq(text, text)` | exact | exact | exact | exact |
| `texteqname(text, name)` | exact | exact | exact | exact |
| `textgename(text, name)` | exact | exact | exact | exact |
| `textgtname(text, name)` | exact | exact | exact | exact |
| `textin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `textlen(text)` | exact | exact | exact | exact |
| `textlename(text, name)` | exact | exact | exact | exact |
| `textltname(text, name)` | exact | exact | exact | exact |
| `textne(text, text)` | exact | exact | exact | exact |
| `textnename(text, name)` | exact | exact | exact | exact |
| `textout(text)` | exact | differs (error) | differs (error) | differs (error) |
| `textsend(text)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tideq(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `tidge(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `tidgt(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `tidin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tidlarger(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `tidle(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `tidlt(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `tidne(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `tidout(tid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tidsend(tid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tidsmaller(tid, tid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `time_cmp(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_eq(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_ge(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_gt(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_hash(time without time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `time_hash_extended(time without time zone, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `time_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `time_larger(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_le(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_lt(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_mi_interval(time without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `time_mi_time(time without time zone, time without time zone)` | exact | exact | differs (error) | differs |
| `time_ne(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `time_out(time without time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `time_pl_interval(time without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `time_send(time without time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `time_smaller(time without time zone, time without time zone)` | exact | exact | exact | differs |
| `timestamp_cmp(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_cmp_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_cmp_timestamptz(timestamp without time zone, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamp_eq(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_eq_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_eq_timestamptz(timestamp without time zone, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamp_ge(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_ge_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_ge_timestamptz(timestamp without time zone, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamp_gt(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_gt_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_gt_timestamptz(timestamp without time zone, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamp_hash(timestamp without time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `timestamp_hash_extended(timestamp without time zone, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `timestamp_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `timestamp_larger(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | differs |
| `timestamp_le(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_le_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_le_timestamptz(timestamp without time zone, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamp_lt(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_lt_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_lt_timestamptz(timestamp without time zone, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamp_mi(timestamp without time zone, timestamp without time zone)` | exact | exact | differs | differs |
| `timestamp_mi_interval(timestamp without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `timestamp_ne(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_ne_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_ne_timestamptz(timestamp without time zone, timestamp with time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamp_out(timestamp without time zone)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `timestamp_pl_interval(timestamp without time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `timestamp_send(timestamp without time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `timestamp_smaller(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | differs |
| `timestamptypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `timestamptypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `timestamptz_cmp(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_cmp_date(timestamp with time zone, date)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_cmp_timestamp(timestamp with time zone, timestamp without time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_eq(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_eq_date(timestamp with time zone, date)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_eq_timestamp(timestamp with time zone, timestamp without time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_ge(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_ge_date(timestamp with time zone, date)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_ge_timestamp(timestamp with time zone, timestamp without time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_gt(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_gt_date(timestamp with time zone, date)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_gt_timestamp(timestamp with time zone, timestamp without time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `timestamptz_larger(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | differs |
| `timestamptz_le(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_le_date(timestamp with time zone, date)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_le_timestamp(timestamp with time zone, timestamp without time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_lt(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_lt_date(timestamp with time zone, date)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_lt_timestamp(timestamp with time zone, timestamp without time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_mi(timestamp with time zone, timestamp with time zone)` | exact | exact | differs | differs |
| `timestamptz_mi_interval(timestamp with time zone, interval)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `timestamptz_ne(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_ne_date(timestamp with time zone, date)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_ne_timestamp(timestamp with time zone, timestamp without time zone)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `timestamptz_out(timestamp with time zone)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `timestamptz_pl_interval(timestamp with time zone, interval)` | exact (call only) | exact (call only) | differs (error) | differs (error) |
| `timestamptz_send(timestamp with time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `timestamptz_smaller(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | differs |
| `timestamptztypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `timestamptztypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `timetypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `timetypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `timetz_cmp(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_eq(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_ge(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_gt(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_hash(time with time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `timetz_hash_extended(time with time zone, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `timetz_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `timetz_larger(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_le(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_lt(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_mi_interval(time with time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `timetz_ne(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetz_out(time with time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `timetz_pl_interval(time with time zone, interval)` | exact | exact | differs (error) | differs (error) |
| `timetz_send(time with time zone)` | exact | differs (error) | differs (error) | differs (error) |
| `timetz_smaller(time with time zone, time with time zone)` | exact | exact | differs (error) | differs (error) |
| `timetztypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `timetztypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `tsqueryin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tsqueryout(tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsquerysend(tsquery)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvectorin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `tsvectorout(tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `tsvectorsend(tsvector)` | exact | differs (error) | differs (error) | differs (error) |
| `txid_snapshot_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `txid_snapshot_out(txid_snapshot)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `txid_snapshot_send(txid_snapshot)` | exact (call only) | exact (call only) | refused | refused |
| `unknownout(unknown)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `unknownsend(unknown)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `uuid_cmp(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_eq(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_ge(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_gt(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_hash(uuid)` | exact | differs (error) | differs (error) | differs (error) |
| `uuid_hash_extended(uuid, bigint)` | exact | differs (error) | differs (error) | differs (error) |
| `uuid_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `uuid_le(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_lt(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_ne(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_out(uuid)` | exact | differs (error) | differs (error) | differs (error) |
| `uuid_send(uuid)` | exact | differs (error) | differs (error) | differs (error) |
| `varbit_in(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `varbit_out(bit varying)` | exact | differs (error) | differs (error) | differs (error) |
| `varbit_send(bit varying)` | exact | differs (error) | differs (error) | differs (error) |
| `varbitcmp(bit varying, bit varying)` | exact | exact | exact | exact |
| `varbiteq(bit varying, bit varying)` | exact | exact | exact | exact |
| `varbitge(bit varying, bit varying)` | exact | exact | exact | exact |
| `varbitgt(bit varying, bit varying)` | exact | exact | exact | exact |
| `varbitle(bit varying, bit varying)` | exact | exact | exact | exact |
| `varbitlt(bit varying, bit varying)` | exact | exact | exact | exact |
| `varbitne(bit varying, bit varying)` | exact | exact | exact | exact |
| `varbittypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `varbittypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `varcharin(cstring, oid, integer)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `varcharout(character varying)` | exact | differs (error) | differs (error) | differs (error) |
| `varcharsend(character varying)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `varchartypmodin(cstring[])` | differs (error) | differs (error) | differs (error) | differs (error) |
| `varchartypmodout(integer)` | exact | differs (error) | differs (error) | differs (error) |
| `void_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `void_out(void)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `void_send(void)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `xid8_larger(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8_smaller(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8cmp(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8eq(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8ge(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8gt(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `xid8le(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8lt(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8ne(xid8, xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xid8out(xid8)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `xid8send(xid8)` | exact (call only) | exact (call only) | refused | refused |
| `xideq(xid, xid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `xideqint4(xid, integer)` | exact (call only) | exact (call only) | exact (call only) | differs (error) |
| `xidin(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `xidneq(xid, xid)` | exact (call only) | exact (call only) | exact (call only) | exact (call only) |
| `xidneqint4(xid, integer)` | exact (call only) | exact (call only) | exact (call only) | differs (error) |
| `xidout(xid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `xidsend(xid)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `xml_in(cstring)` | exact (call only) | differs (error) | differs (error) | differs (error) |
| `xml_out(xml)` | exact | differs (error) | differs (error) | differs (error) |
| `xml_send(xml)` | exact (call only) | exact (call only) | refused | refused |

</details>

<details><summary>XML (24)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `cursor_to_xml(cursor refcursor, count integer, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `cursor_to_xmlschema(cursor refcursor, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `database_to_xml(nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | differs (error) | differs (error) | differs |
| `database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | differs (error) | differs (error) | differs |
| `database_to_xmlschema(nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | differs |
| `query_to_xml(query text, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `query_to_xml_and_xmlschema(query text, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `query_to_xmlschema(query text, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `table_to_xml(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `table_to_xml_and_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `table_to_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | exact (call only) | exact (call only) | exact (call only) | differs |
| `xml(text)` | exact | exact | refused | refused |
| `xml_is_well_formed(text)` | exact | exact | refused | refused |
| `xml_is_well_formed_content(text)` | exact | exact | refused | refused |
| `xml_is_well_formed_document(text)` | exact | exact | refused | refused |
| `xmlagg(xml)` | exact | exact | refused | refused |
| `xmlcomment(text)` | exact | exact | exact | refused |
| `xmlconcat2(xml, xml)` | exact | exact | refused | refused |
| `xmlexists(text, xml)` | exact | exact | refused | refused |
| `xmltext(text)` | exact | exact | exact | refused |
| `xmlvalidate(xml, text)` | exact (call only) | exact (call only) | refused | refused |

</details>
<!-- function-results:end -->

### Text search

On DuckDB, Trino and ClickHouse these functions are served for constants only. A column argument is refused by name:

- `to_tsvector`
- `json_to_tsvector`
- `jsonb_to_tsvector`
- `to_tsquery`
- `plainto_tsquery`
- `phraseto_tsquery`
- `websearch_to_tsquery`
- `ts_headline`
- `ts_lexize`
- `ts_match_tt`
- `ts_match_tq`
- `ts_parse`
- `ts_debug`
- `ts_stat`
- `ts_rewrite`

Every `tsquery` argument, of any function, is held to the same rule. A `tsquery` constant is a literal such as `'a & b'::tsquery`, so `ts_rank(v, to_tsquery('english', 'a'))` is refused as well.

Column forms of these functions are not built. Functions that read a `tsvector`, such as `ts_delete`, `setweight` and `ts_rank`, take a column for the vector.

The `@@` operator has no form on these engines. It is not refused by name: the statement reaches the engine and fails there. `ts_match_vq` is the spelling that works.

### Advisory locks

`pg_advisory_lock` and its shared, transaction-level and `try` variants work as they do in PostgreSQL among the sessions of one Provisa process. The lock table lives in that process's memory. Two Provisa processes, such as two replicas behind a load balancer, do not see each other's locks, so a lock taken on one does not make a session on the other wait. Clients that use advisory locks to serialize work across processes must connect to one Provisa process.

### Server version

`server_version_num` reports `140000` and `version()` reports PostgreSQL 14. Clients choose their catalog queries by this number, and the catalog queries Provisa answers are the ones PostgreSQL 14 clients send.

## Known anomalies

### Numeric precision on DuckDB

A PostgreSQL `numeric` column declared without a precision is registered as `numeric(38, s)`, where `s` is the largest scale found among its values (10 for a table with no rows). The DuckDB engine reads it exactly up to 38 significant digits. A value with more digits is rounded to 38.

Workaround: declare the column with its precision and scale at the source, or read the table through the PostgreSQL engine.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| psql | — | — | Verified by replay |
| DBeaver | — | — | Covered by driver (Postgres JDBC) |
| Tableau | — | — | Covered by driver (Postgres JDBC/ODBC) |
| DataGrip | — | — | Not yet verified |
