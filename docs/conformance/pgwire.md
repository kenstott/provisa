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


## Failed cases

None.
<!-- conformance-results:end -->

## Deliberate differences

### Polyfills

On an engine that is not PostgreSQL, Provisa gives PostgreSQL's behaviour by a polyfill: a read polyfill for how a feature's values and operations are computed, a write polyfill for how its values reach the engine. Each polyfill states its limitations, and a feature that cannot be polyfilled is refused by name with a SQLSTATE and message. The [polyfill pages](polyfill/index.md) list them per engine. A difference the pages state is deliberate.

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

Every overload of every `pg_catalog` function is called the way PostgreSQL requires it, with the `VARIADIC` form, `WITHIN GROUP`, `OVER`, or a real argument where PostgreSQL refuses a `NULL`. Each overload then has a result on each engine, because what an engine can answer is that engine's own: its native answer, or the answer of a polyfill. A function refused by name on an engine is that engine's result, and a wrong answer on any engine is a defect.

One group is skipped by design and listed under the pass rate. Internal functions cannot be called by PostgreSQL itself: each takes an `internal` argument, or PostgreSQL refuses the call whatever the server's state. Index access method support and index maintenance functions a client can call, such as `brin_summarize_range`, are ordinary functions with a result on each engine.

<!-- function-results:begin -->
### Function results by engine

Measured against PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2). Each overload has a result on each engine. Each result is that engine's own, and the rows do not add up to a product result.

| Engine | Passed | Skipped by design | Failed | Not tested |
| --- | ---: | ---: | ---: | ---: |
| Postgres | 2478 | 841 | 0 | 0 |
| DuckDB | 2307 | 912 | 100 | 0 |
| Trino | 1767 | 926 | 626 | 0 |
| ClickHouse | 1817 | 916 | 586 | 0 |

In detail: passed is exact and equivalent; skipped by design is the decisions' groups and what PostgreSQL itself cannot call; not tested is what the check did not compare or did not reach; failed is the rest.

| Engine | Exact | Equivalent | By a decision | PostgreSQL calls none | Not checked | Refused by name | Differs: wrong answer | Differs: engine error | PostgreSQL refuses here | Not reached |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Postgres | 2470 | 8 | 276 | 565 | 0 | 0 | 0 | 0 | 0 | 0 |
| DuckDB | 2285 | 22 | 347 | 565 | 0 | 100 | 0 | 0 | 0 | 0 |
| Trino | 1746 | 21 | 361 | 565 | 0 | 626 | 0 | 0 | 0 | 0 |
| ClickHouse | 1797 | 20 | 351 | 565 | 0 | 586 | 0 | 0 | 0 | 0 |

| Result | Meaning |
| --- | --- |
| Exact | The engine answered every call as PostgreSQL did, and an answer, a type and form, or a side effect was compared. |
| Equivalent | The answer passes under a numbered rule below, linked from its row. |
| Not checked | The engine answered with PostgreSQL's result type, but nothing was compared. A call that ran proves nothing, so this is no pass. |
| Skipped by design | A recorded decision skips it: a side-effect function off the PostgreSQL engine, one whose side effect no client can see, or a refusal by name a decision makes. |
| Refused by name | Provisa refused the call and said what it refuses. |
| Differs: wrong answer | The answer is wrong. This is a defect. |
| Differs: engine error | The engine returned an error and no named refusal. This is a defect. |

A result marked *type and form only* could not be compared by value, so its result type and, for a string, its canonical form were compared with PostgreSQL's own output. One marked *side effect* returns void or acts: what it did, seen through a client, was compared with what PostgreSQL did. `n/a` means PostgreSQL itself refuses every call.

Skipped by design, by group and engine:

| Group | Postgres | DuckDB | Trino | ClickHouse |
| --- | ---: | ---: | ---: | ---: |
| A system catalog is written by no role | 8 | 8 | 8 | 8 |
| LISTEN, NOTIFY and pg_notify are refused | 1 | 1 | 1 | 1 |
| Large objects as a read-only transaction | 4 | 0 | 0 | 0 |
| No function reads the server's files | 12 | 12 | 12 | 12 |
| Not testable | 82 | 0 | 0 | 0 |
| One-argument to_ascii needs a single-byte database | 1 | 1 | 1 | 1 |
| PostgreSQL's own server state | 52 | 52 | 52 | 52 |
| Provisa holds no storage | 12 | 12 | 11 | 11 |
| Sequence functions are not supported | 9 | 9 | 9 | 9 |
| Server administration functions | 26 | 26 | 26 | 26 |
| Side-effect functions | 0 | 112 | 112 | 112 |
| Six-argument regexp_replace is not supported | 1 | 1 | 1 | 1 |
| The catalog is the model's | 31 | 31 | 28 | 29 |
| The server's installed extensions | 1 | 1 | 1 | 1 |
| The server's version | 2 | 2 | 2 | 2 |
| The session's own cursors | 1 | 1 | 1 | 1 |
| Types no SQL value can have | 33 | 33 | 33 | 33 |
| Types that exist only inside PostgreSQL | 0 | 45 | 63 | 52 |

Every overload PostgreSQL answers had something compared.

### Literals and columns

Of each engine's passes, these also pass reading the same value from a column of a registered table. An overload that passes on literals only keeps its result above. No column call means PostgreSQL answered none, or the overload takes no value.

| Engine | Literals and columns | Literals only | No column call |
| --- | ---: | ---: | ---: |
| Postgres | 2139 | 164 | 175 |
| DuckDB | 1958 | 202 | 147 |
| Trino | 1460 | 175 | 132 |
| ClickHouse | 1518 | 171 | 128 |

<details><summary>Postgres (164)</summary>

| Overload | Column call |
| --- | --- |
| `any_value(anyelement)` | result type oids [2283], PostgreSQL [23] |
| `any_value_transfn(anyelement, anyelement)` | result type oids [2283], PostgreSQL [23] |
| `anyarray_out(anyarray)` | ERROR: function pg_catalog.anyarray_out(jsonb) does not exist |
| `anyarray_send(anyarray)` | ERROR: function pg_catalog.anyarray_send(jsonb) does not exist |
| `anycompatiblearray_out(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_out(jsonb) does not exist |
| `anycompatiblearray_send(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_send(jsonb) does not exist |
| `array_append(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_append(jsonb, integer) does not exist |
| `array_cat(anycompatiblearray, anycompatiblearray)` | ERROR: function pg_catalog.array_cat(jsonb, jsonb) does not exist |
| `array_dims(anyarray)` | ERROR: function pg_catalog.array_dims(jsonb) does not exist |
| `array_eq(anyarray, anyarray)` | ERROR: function pg_catalog.array_eq(jsonb, jsonb) does not exist |
| `array_fill(anyelement, integer[])` | ERROR: function pg_catalog.array_fill(integer, jsonb) does not exist |
| `array_fill(anyelement, integer[], integer[])` | ERROR: function pg_catalog.array_fill(integer, jsonb, jsonb) does not exist |
| `array_ge(anyarray, anyarray)` | ERROR: function pg_catalog.array_ge(jsonb, jsonb) does not exist |
| `array_gt(anyarray, anyarray)` | ERROR: function pg_catalog.array_gt(jsonb, jsonb) does not exist |
| `array_larger(anyarray, anyarray)` | ERROR: function pg_catalog.array_larger(jsonb, jsonb) does not exist |
| `array_le(anyarray, anyarray)` | ERROR: function pg_catalog.array_le(jsonb, jsonb) does not exist |
| `array_length(anyarray, integer)` | ERROR: function pg_catalog.array_length(jsonb, integer) does not exist |
| `array_lower(anyarray, integer)` | ERROR: function pg_catalog.array_lower(jsonb, integer) does not exist |
| `array_lt(anyarray, anyarray)` | ERROR: function pg_catalog.array_lt(jsonb, jsonb) does not exist |
| `array_ndims(anyarray)` | ERROR: function pg_catalog.array_ndims(jsonb) does not exist |
| `array_ne(anyarray, anyarray)` | ERROR: function pg_catalog.array_ne(jsonb, jsonb) does not exist |
| `array_out(anyarray)` | ERROR: function pg_catalog.array_out(jsonb) does not exist |
| `array_position(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_position(jsonb, integer) does not exist |
| `array_position(anycompatiblearray, anycompatible, integer)` | ERROR: function pg_catalog.array_position(jsonb, integer, integer) does not exist |
| `array_positions(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_positions(jsonb, integer) does not exist |
| `array_prepend(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.array_prepend(integer, jsonb) does not exist |
| `array_remove(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_remove(jsonb, integer) does not exist |
| `array_replace(anycompatiblearray, anycompatible, anycompatible)` | ERROR: function pg_catalog.array_replace(jsonb, integer, integer) does not exist |
| `array_send(anyarray)` | ERROR: function pg_catalog.array_send(jsonb) does not exist |
| `array_smaller(anyarray, anyarray)` | ERROR: function pg_catalog.array_smaller(jsonb, jsonb) does not exist |
| `array_to_json(anyarray)` | ERROR: function pg_catalog.array_to_json(jsonb) does not exist |
| `array_to_json(anyarray, boolean)` | ERROR: function pg_catalog.array_to_json(jsonb, boolean) does not exist |
| `array_to_string(anyarray, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text) does not exist |
| `array_to_string(anyarray, text, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text, text) does not exist |
| `array_to_tsvector(text[])` | ERROR: function pg_catalog.array_to_tsvector(jsonb) does not exist |
| `array_upper(anyarray, integer)` | ERROR: function pg_catalog.array_upper(jsonb, integer) does not exist |
| `arraycontained(anyarray, anyarray)` | ERROR: function pg_catalog.arraycontained(jsonb, jsonb) does not exist |
| `arraycontains(anyarray, anyarray)` | ERROR: function pg_catalog.arraycontains(jsonb, jsonb) does not exist |
| `arrayoverlap(anyarray, anyarray)` | ERROR: function pg_catalog.arrayoverlap(jsonb, jsonb) does not exist |
| `btarraycmp(anyarray, anyarray)` | ERROR: function pg_catalog.btarraycmp(jsonb, jsonb) does not exist |
| `cardinality(anyarray)` | ERROR: function pg_catalog.cardinality(jsonb) does not exist |
| `database_to_xml(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml: an argument that is not a constant expression is not supported |
| `database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `database_to_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xmlschema: an argument that is not a constant expression is not supported |
| `generate_subscripts(anyarray, integer)` | ERROR: function pg_catalog.generate_subscripts(jsonb, integer) does not exist |
| `generate_subscripts(anyarray, integer, boolean)` | ERROR: function pg_catalog.generate_subscripts(jsonb, integer, boolean) does not exist |
| `has_any_column_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `hash_array(anyarray)` | ERROR: function pg_catalog.hash_array(jsonb) does not exist |
| `hash_array_extended(anyarray, bigint)` | ERROR: function pg_catalog.hash_array_extended(jsonb, bigint) does not exist |
| `json_object(text[])` | ERROR: function pg_catalog.json_object(jsonb) does not exist |
| `json_object(text[], text[])` | ERROR: function pg_catalog.json_object(jsonb, jsonb) does not exist |
| `jsonb_delete(from_json jsonb, VARIADIC path_elems text[])` | ERROR: function pg_catalog.jsonb_delete(jsonb, jsonb) does not exist |
| `jsonb_delete_path(jsonb, text[])` | ERROR: function pg_catalog.jsonb_delete_path(jsonb, jsonb) does not exist |
| `jsonb_exists_all(jsonb, text[])` | ERROR: function pg_catalog.jsonb_exists_all(jsonb, jsonb) does not exist |
| `jsonb_exists_any(jsonb, text[])` | ERROR: function pg_catalog.jsonb_exists_any(jsonb, jsonb) does not exist |
| `jsonb_insert(jsonb_in jsonb, path text[], replacement jsonb, insert_after boolean)` | ERROR: function pg_catalog.jsonb_insert(jsonb, jsonb, jsonb, boolean) does not exist |
| `jsonb_object(text[])` | ERROR: function pg_catalog.jsonb_object(jsonb) does not exist |
| `jsonb_object(text[], text[])` | ERROR: function pg_catalog.jsonb_object(jsonb, jsonb) does not exist |
| `jsonb_set(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean)` | ERROR: function pg_catalog.jsonb_set(jsonb, jsonb, jsonb, boolean) does not exist |
| `jsonb_set_lax(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean, null_value_treatment text)` | ERROR: function pg_catalog.jsonb_set_lax(jsonb, jsonb, jsonb, boolean, text) does not exist |
| `makeaclitem(oid, oid, text, boolean)` | ERROR: meta.catalog_pg_roles is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `max(anyarray)` | ERROR: function pg_catalog.max(jsonb) does not exist |
| `max(character)` | result type oids [25], PostgreSQL [1042] |
| `min(anyarray)` | ERROR: function pg_catalog.min(jsonb) does not exist |
| `min(character)` | result type oids [25], PostgreSQL [1042] |
| `pg_column_is_updatable(regclass, smallint, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_describe_object(oid, oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_function_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_constraintdef(oid, boolean)` | ERROR: meta.catalog_pg_constraint is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_function_arg_default(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_identity_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_result(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_sqlbody(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_functiondef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_indexdef(oid, integer, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_publication_tables(VARIADIC pubname text[], OUT pubid oid, OUT relid oid, OUT attrs int2vector, OUT qual pg_node_tree)` | ERROR: pg_get_publication_tables() has no form on postgres |
| `pg_get_replica_identity_index(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_userbyid(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_viewdef(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object(classid oid, objid oid, objsubid integer, OUT type text, OUT schema text, OUT name text, OUT identity text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object_as_address(classid oid, objid oid, objsubid integer, OUT type text, OUT object_names text[], OUT object_args text[])` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_partition_root(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_relation_is_updatable(regclass, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_settings_get_flags(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_type_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `postgresql_fdw_validator(text[], oid)` | ERROR: function pg_catalog.postgresql_fdw_validator(jsonb, oid) does not exist |
| `regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regclassout(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regnamespaceout(regnamespace)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regroleout(regrole)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml: an argument that is not a constant expression is not supported |
| `schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xmlschema: an argument that is not a constant expression is not supported |
| `setweight(tsvector, "char", text[])` | ERROR: function pg_catalog.setweight(tsvector, "char", jsonb) does not exist |
| `to_regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regcollation(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regnamespace(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoper(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoperator(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regproc(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regprocedure(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regrole(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtype(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtypemod(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `trim_array(anyarray, integer)` | ERROR: function pg_catalog.trim_array(jsonb, integer) does not exist |
| `ts_delete(tsvector, text[])` | ERROR: function pg_catalog.ts_delete(tsvector, jsonb) does not exist |
| `unnest(anyarray)` | ERROR: function pg_catalog.unnest(jsonb) does not exist |
| `unnest(tsvector tsvector, OUT lexeme text, OUT positions smallint[], OUT weights text[])` | "('cat', '{1}', '{D}')", PostgreSQL '(cat,{1},{D})' |
| `width_bucket(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.width_bucket(integer, jsonb) does not exist |

</details>

<details><summary>DuckDB (202)</summary>

| Overload | Column call |
| --- | --- |
| `any_value(anyelement)` | result type oids [2283], PostgreSQL [23] |
| `any_value_transfn(anyelement, anyelement)` | result type oids [2283], PostgreSQL [23] |
| `anyarray_out(anyarray)` | ERROR: function pg_catalog.anyarray_out(jsonb) does not exist |
| `anyarray_send(anyarray)` | ERROR: function pg_catalog.anyarray_send(jsonb) does not exist |
| `anycompatiblearray_out(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_out(jsonb) does not exist |
| `anycompatiblearray_send(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_send(jsonb) does not exist |
| `array_append(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_append(jsonb, integer) does not exist |
| `array_cat(anycompatiblearray, anycompatiblearray)` | ERROR: function pg_catalog.array_cat(jsonb, jsonb) does not exist |
| `array_dims(anyarray)` | ERROR: function pg_catalog.array_dims(jsonb) does not exist |
| `array_eq(anyarray, anyarray)` | ERROR: function pg_catalog.array_eq(jsonb, jsonb) does not exist |
| `array_ge(anyarray, anyarray)` | ERROR: function pg_catalog.array_ge(jsonb, jsonb) does not exist |
| `array_gt(anyarray, anyarray)` | ERROR: function pg_catalog.array_gt(jsonb, jsonb) does not exist |
| `array_larger(anyarray, anyarray)` | ERROR: function pg_catalog.array_larger(jsonb, jsonb) does not exist |
| `array_le(anyarray, anyarray)` | ERROR: function pg_catalog.array_le(jsonb, jsonb) does not exist |
| `array_length(anyarray, integer)` | ERROR: function pg_catalog.array_length(jsonb, integer) does not exist |
| `array_lower(anyarray, integer)` | ERROR: function pg_catalog.array_lower(jsonb, integer) does not exist |
| `array_lt(anyarray, anyarray)` | ERROR: function pg_catalog.array_lt(jsonb, jsonb) does not exist |
| `array_ndims(anyarray)` | ERROR: function pg_catalog.array_ndims(jsonb) does not exist |
| `array_ne(anyarray, anyarray)` | ERROR: function pg_catalog.array_ne(jsonb, jsonb) does not exist |
| `array_out(anyarray)` | ERROR: function pg_catalog.array_out(jsonb) does not exist |
| `array_position(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_position(jsonb, integer) does not exist |
| `array_position(anycompatiblearray, anycompatible, integer)` | ERROR: function pg_catalog.array_position(jsonb, integer, integer) does not exist |
| `array_positions(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_positions(jsonb, integer) does not exist |
| `array_prepend(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.array_prepend(integer, jsonb) does not exist |
| `array_remove(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_remove(jsonb, integer) does not exist |
| `array_replace(anycompatiblearray, anycompatible, anycompatible)` | ERROR: function pg_catalog.array_replace(jsonb, integer, integer) does not exist |
| `array_send(anyarray)` | ERROR: function pg_catalog.array_send(jsonb) does not exist |
| `array_smaller(anyarray, anyarray)` | ERROR: function pg_catalog.array_smaller(jsonb, jsonb) does not exist |
| `array_to_json(anyarray)` | ERROR: function pg_catalog.array_to_json(jsonb) does not exist |
| `array_to_json(anyarray, boolean)` | ERROR: function pg_catalog.array_to_json(jsonb, boolean) does not exist |
| `array_to_string(anyarray, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text) does not exist |
| `array_to_string(anyarray, text, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text, text) does not exist |
| `array_to_tsvector(text[])` | ERROR: function pg_catalog.array_to_tsvector(jsonb) does not exist |
| `array_upper(anyarray, integer)` | ERROR: function pg_catalog.array_upper(jsonb, integer) does not exist |
| `arraycontained(anyarray, anyarray)` | ERROR: function pg_catalog.arraycontained(jsonb, jsonb) does not exist |
| `arraycontains(anyarray, anyarray)` | ERROR: function pg_catalog.arraycontains(jsonb, jsonb) does not exist |
| `arrayoverlap(anyarray, anyarray)` | ERROR: function pg_catalog.arrayoverlap(jsonb, jsonb) does not exist |
| `bit_in(cstring, oid, integer)` | ERROR: bit_in does not read this value |
| `bittypmodin(cstring[])` | ERROR: bittypmodin does not read this value |
| `bpchartypmodin(cstring[])` | ERROR: bpchartypmodin does not read this value |
| `btarraycmp(anyarray, anyarray)` | ERROR: function pg_catalog.btarraycmp(jsonb, jsonb) does not exist |
| `cardinality(anyarray)` | ERROR: function pg_catalog.cardinality(jsonb) does not exist |
| `database_to_xml(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml: an argument that is not a constant expression is not supported |
| `database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `database_to_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xmlschema: an argument that is not a constant expression is not supported |
| `datemultirange(VARIADIC daterange[])` | ERROR: Catalog Error: Type with name DATERANGE does not exist! Did you mean "JSON"? |
| `enum_first(anyenum)` | result type oids [3500], PostgreSQL [16428] |
| `enum_last(anyenum)` | result type oids [3500], PostgreSQL [16428] |
| `generate_subscripts(anyarray, integer)` | ERROR: function pg_catalog.generate_subscripts(jsonb, integer) does not exist |
| `generate_subscripts(anyarray, integer, boolean)` | ERROR: function pg_catalog.generate_subscripts(jsonb, integer, boolean) does not exist |
| `has_any_column_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `hash_array(anyarray)` | ERROR: function pg_catalog.hash_array(jsonb) does not exist |
| `hash_array_extended(anyarray, bigint)` | ERROR: function pg_catalog.hash_array_extended(jsonb, bigint) does not exist |
| `int2vectorsend(int2vector)` | ERROR: Binder Error: Invalid LIST argument during lambda function binding! |
| `int4multirange(VARIADIC int4range[])` | ERROR: Catalog Error: Type with name INT4RANGE does not exist! Did you mean "INET"? |
| `int8multirange(VARIADIC int8range[])` | ERROR: Catalog Error: Type with name INT8RANGE does not exist! Did you mean "INET"? |
| `interval_in(cstring, oid, integer)` | ERROR: interval_in does not read this value |
| `intervaltypmodin(cstring[])` | ERROR: intervaltypmodin does not read this value |
| `json_object(text[])` | ERROR: function pg_catalog.json_object(jsonb) does not exist |
| `json_object(text[], text[])` | ERROR: function pg_catalog.json_object(jsonb, jsonb) does not exist |
| `jsonb_delete(from_json jsonb, VARIADIC path_elems text[])` | ERROR: function pg_catalog.jsonb_delete(jsonb, jsonb) does not exist |
| `jsonb_delete_path(jsonb, text[])` | ERROR: function pg_catalog.jsonb_delete_path(jsonb, jsonb) does not exist |
| `jsonb_exists_all(jsonb, text[])` | ERROR: function pg_catalog.jsonb_exists_all(jsonb, jsonb) does not exist |
| `jsonb_exists_any(jsonb, text[])` | ERROR: function pg_catalog.jsonb_exists_any(jsonb, jsonb) does not exist |
| `jsonb_in(cstring)` | ERROR: jsonb_in does not read this value |
| `jsonb_insert(jsonb_in jsonb, path text[], replacement jsonb, insert_after boolean)` | ERROR: function pg_catalog.jsonb_insert(jsonb, jsonb, jsonb, boolean) does not exist |
| `jsonb_object(text[])` | ERROR: function pg_catalog.jsonb_object(jsonb) does not exist |
| `jsonb_object(text[], text[])` | ERROR: function pg_catalog.jsonb_object(jsonb, jsonb) does not exist |
| `jsonb_set(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean)` | ERROR: function pg_catalog.jsonb_set(jsonb, jsonb, jsonb, boolean) does not exist |
| `jsonb_set_lax(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean, null_value_treatment text)` | ERROR: function pg_catalog.jsonb_set_lax(jsonb, jsonb, jsonb, boolean, text) does not exist |
| `makeaclitem(oid, oid, text, boolean)` | ERROR: meta.catalog_pg_roles is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `max(anyarray)` | ERROR: function pg_catalog.max(jsonb) does not exist |
| `max(character)` | result type oids [25], PostgreSQL [1042] |
| `min(anyarray)` | ERROR: function pg_catalog.min(jsonb) does not exist |
| `min(character)` | result type oids [25], PostgreSQL [1042] |
| `numeric_in(cstring, oid, integer)` | ERROR: numeric_in does not read this value |
| `numerictypmodin(cstring[])` | ERROR: numerictypmodin does not read this value |
| `nummultirange(VARIADIC numrange[])` | ERROR: Catalog Error: Type with name NUMRANGE does not exist! Did you mean "INET"? |
| `pg_column_is_updatable(regclass, smallint, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_describe_object(oid, oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_function_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_constraintdef(oid, boolean)` | ERROR: meta.catalog_pg_constraint is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_function_arg_default(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_identity_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_result(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_sqlbody(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_functiondef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_indexdef(oid, integer, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_publication_tables(VARIADIC pubname text[], OUT pubid oid, OUT relid oid, OUT attrs int2vector, OUT qual pg_node_tree)` | ERROR: pg_get_publication_tables() has no form on postgres |
| `pg_get_replica_identity_index(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_userbyid(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_viewdef(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object(classid oid, objid oid, objsubid integer, OUT type text, OUT schema text, OUT name text, OUT identity text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object_as_address(classid oid, objid oid, objsubid integer, OUT type text, OUT object_names text[], OUT object_args text[])` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_partition_root(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_relation_is_updatable(regclass, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_settings_get_flags(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_type_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `postgresql_fdw_validator(text[], oid)` | ERROR: function pg_catalog.postgresql_fdw_validator(jsonb, oid) does not exist |
| `record_eq(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_ge(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_gt(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_image_eq(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_image_ge(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_image_gt(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_image_le(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_image_lt(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_image_ne(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_le(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_lt(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `record_ne(record, record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regclassout(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regnamespaceout(regnamespace)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regroleout(regrole)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `row_to_json(record)` | ERROR: Catalog Error: Type with name record does not exist! Did you mean "dec"? |
| `schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml: an argument that is not a constant expression is not supported |
| `schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xmlschema: an argument that is not a constant expression is not supported |
| `setweight(tsvector, "char", text[])` | ERROR: function pg_catalog.setweight(tsvector, "char", jsonb) does not exist |
| `timestamptypmodin(cstring[])` | ERROR: timestamptypmodin does not read this value |
| `timestamptztypmodin(cstring[])` | ERROR: timestamptztypmodin does not read this value |
| `timetypmodin(cstring[])` | ERROR: timetypmodin does not read this value |
| `timetztypmodin(cstring[])` | ERROR: timetztypmodin does not read this value |
| `to_regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regcollation(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regnamespace(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoper(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoperator(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regproc(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regprocedure(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regrole(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtype(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtypemod(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `trim_array(anyarray, integer)` | ERROR: function pg_catalog.trim_array(jsonb, integer) does not exist |
| `ts_delete(tsvector, text[])` | ERROR: function pg_catalog.ts_delete(tsvector, jsonb) does not exist |
| `tsmultirange(VARIADIC tsrange[])` | ERROR: Catalog Error: Type with name TSRANGE does not exist! Did you mean "JSON"? |
| `tstzmultirange(VARIADIC tstzrange[])` | ERROR: Catalog Error: Type with name TSTZRANGE does not exist! Did you mean "JSON"? |
| `tsvectorin(cstring)` | ERROR: tsvectorin does not read this value |
| `unnest(anyarray)` | ERROR: function pg_catalog.unnest(jsonb) does not exist |
| `unnest(tsvector tsvector, OUT lexeme text, OUT positions smallint[], OUT weights text[])` | "('cat', '{1}', '{D}')", PostgreSQL '(cat,{1},{D})' |
| `varbit_in(cstring, oid, integer)` | ERROR: varbit_in does not read this value |
| `varbittypmodin(cstring[])` | ERROR: varbittypmodin does not read this value |
| `varchartypmodin(cstring[])` | ERROR: varchartypmodin does not read this value |
| `void_out(void)` | ERROR: Catalog Error: Type with name void does not exist! Did you mean "oid"? |
| `void_send(void)` | ERROR: Catalog Error: Type with name void does not exist! Did you mean "oid"? |
| `width_bucket(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.width_bucket(integer, jsonb) does not exist |

</details>

<details><summary>Trino (175)</summary>

| Overload | Column call |
| --- | --- |
| `any_value(anyelement)` | result type oids [2283], PostgreSQL [23] |
| `any_value_transfn(anyelement, anyelement)` | result type oids [2283], PostgreSQL [23] |
| `anyarray_out(anyarray)` | ERROR: function pg_catalog.anyarray_out(jsonb) does not exist |
| `anyarray_send(anyarray)` | ERROR: function pg_catalog.anyarray_send(jsonb) does not exist |
| `anycompatiblearray_out(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_out(jsonb) does not exist |
| `anycompatiblearray_send(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_send(jsonb) does not exist |
| `array_append(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_append(jsonb, integer) does not exist |
| `array_cat(anycompatiblearray, anycompatiblearray)` | ERROR: function pg_catalog.array_cat(jsonb, jsonb) does not exist |
| `array_dims(anyarray)` | ERROR: function pg_catalog.array_dims(jsonb) does not exist |
| `array_eq(anyarray, anyarray)` | ERROR: function pg_catalog.array_eq(jsonb, jsonb) does not exist |
| `array_ge(anyarray, anyarray)` | ERROR: function pg_catalog.array_ge(jsonb, jsonb) does not exist |
| `array_gt(anyarray, anyarray)` | ERROR: function pg_catalog.array_gt(jsonb, jsonb) does not exist |
| `array_larger(anyarray, anyarray)` | ERROR: function pg_catalog.array_larger(jsonb, jsonb) does not exist |
| `array_le(anyarray, anyarray)` | ERROR: function pg_catalog.array_le(jsonb, jsonb) does not exist |
| `array_length(anyarray, integer)` | ERROR: function pg_catalog.array_length(jsonb, integer) does not exist |
| `array_lower(anyarray, integer)` | ERROR: function pg_catalog.array_lower(jsonb, integer) does not exist |
| `array_lt(anyarray, anyarray)` | ERROR: function pg_catalog.array_lt(jsonb, jsonb) does not exist |
| `array_ndims(anyarray)` | ERROR: function pg_catalog.array_ndims(jsonb) does not exist |
| `array_ne(anyarray, anyarray)` | ERROR: function pg_catalog.array_ne(jsonb, jsonb) does not exist |
| `array_out(anyarray)` | ERROR: function pg_catalog.array_out(jsonb) does not exist |
| `array_position(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_position(jsonb, integer) does not exist |
| `array_positions(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_positions(jsonb, integer) does not exist |
| `array_prepend(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.array_prepend(integer, jsonb) does not exist |
| `array_replace(anycompatiblearray, anycompatible, anycompatible)` | ERROR: function pg_catalog.array_replace(jsonb, integer, integer) does not exist |
| `array_send(anyarray)` | ERROR: function pg_catalog.array_send(jsonb) does not exist |
| `array_smaller(anyarray, anyarray)` | ERROR: function pg_catalog.array_smaller(jsonb, jsonb) does not exist |
| `array_to_string(anyarray, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text) does not exist |
| `array_to_string(anyarray, text, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text, text) does not exist |
| `array_to_tsvector(text[])` | ERROR: function pg_catalog.array_to_tsvector(jsonb) does not exist |
| `array_upper(anyarray, integer)` | ERROR: function pg_catalog.array_upper(jsonb, integer) does not exist |
| `arraycontains(anyarray, anyarray)` | ERROR: function pg_catalog.arraycontains(jsonb, jsonb) does not exist |
| `bit_in(cstring, oid, integer)` | ERROR: bit_in does not read this value |
| `bittypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:210: Cannot cast varchar(4) to array(varchar)", query_id=20261114_005634_01180_ii6mf) |
| `bpchartypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:213: Cannot cast varchar(4) to array(varchar)", query_id=20261114_005744_02027_ii6mf) |
| `btarraycmp(anyarray, anyarray)` | ERROR: function pg_catalog.btarraycmp(jsonb, jsonb) does not exist |
| `cardinality(anyarray)` | ERROR: function pg_catalog.cardinality(jsonb) does not exist |
| `database_to_xml(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml: an argument that is not a constant expression is not supported |
| `database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `database_to_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xmlschema: an argument that is not a constant expression is not supported |
| `datemultirange(VARIADIC daterange[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:16087: Unknown type: ARRAY(DATERANGE)", query_id=20261114_010009_04481_ii6mf) |
| `enum_first(anyenum)` | ERROR: conformance.probe_enum() has no form on trino |
| `enum_larger(anyenum, anyenum)` | ERROR: conformance.probe_enum() has no form on trino |
| `enum_last(anyenum)` | ERROR: conformance.probe_enum() has no form on trino |
| `enum_range(anyenum)` | ERROR: conformance.probe_enum() has no form on trino |
| `enum_range(anyenum, anyenum)` | ERROR: conformance.probe_enum() has no form on trino |
| `enum_smaller(anyenum, anyenum)` | ERROR: conformance.probe_enum() has no form on trino |
| `has_any_column_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `hash_array(anyarray)` | ERROR: function pg_catalog.hash_array(jsonb) does not exist |
| `hash_array_extended(anyarray, bigint)` | ERROR: function pg_catalog.hash_array_extended(jsonb, bigint) does not exist |
| `int4multirange(VARIADIC int4range[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:22551: Unknown type: ARRAY(INT4RANGE)", query_id=20261114_011048_08590_ii6mf) |
| `int8multirange(VARIADIC int8range[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:22505: Unknown type: ARRAY(INT8RANGE)", query_id=20261114_011120_09347_ii6mf) |
| `intervaltypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:215: Cannot cast varchar(9) to array(varchar)", query_id=20261114_011142_09664_ii6mf) |
| `makeaclitem(oid, oid, text, boolean)` | ERROR: meta.catalog_pg_roles is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `max(anyarray)` | ERROR: function pg_catalog.max(jsonb) does not exist |
| `min(anyarray)` | ERROR: function pg_catalog.min(jsonb) does not exist |
| `numeric_in(cstring, oid, integer)` | ERROR: numeric_in does not read this value |
| `numerictypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:214: Cannot cast varchar(6) to array(varchar)", query_id=20261114_011441_12724_ii6mf) |
| `nummultirange(VARIADIC numrange[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:19896: Unknown type: ARRAY(NUMRANGE)", query_id=20261114_011444_12766_ii6mf) |
| `pg_collation_for("any")` | ERROR: pg_collation_for() has no form on trino |
| `pg_column_is_updatable(regclass, smallint, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_describe_object(oid, oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_function_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_constraintdef(oid, boolean)` | ERROR: meta.catalog_pg_constraint is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_function_arg_default(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_identity_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_result(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_sqlbody(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_functiondef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_indexdef(oid, integer, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_publication_tables(VARIADIC pubname text[], OUT pubid oid, OUT relid oid, OUT attrs int2vector, OUT qual pg_node_tree)` | ERROR: pg_get_publication_tables() has no form on postgres |
| `pg_get_replica_identity_index(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_userbyid(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_viewdef(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object(classid oid, objid oid, objsubid integer, OUT type text, OUT schema text, OUT name text, OUT identity text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object_as_address(classid oid, objid oid, objsubid integer, OUT type text, OUT object_names text[], OUT object_args text[])` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_partition_root(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_relation_is_updatable(regclass, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_settings_get_flags(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_type_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `postgresql_fdw_validator(text[], oid)` | ERROR: function pg_catalog.postgresql_fdw_validator(jsonb, oid) does not exist |
| `record_eq(record, record)` | ERROR: Expected TYPE after CAST. Line 1, Col: 222. CT FROM ROW(1, CAST('a' AS VARCHAR)) END AS v FROM (VALUES (1, CAST(ROW(1, CAST('a' AS VARCHAR)) AS [4mrecord[0m))) AS t(id, c1) WHERE id = 1 |
| `record_image_eq(record, record)` | ERROR: Expected TYPE after CAST. Line 1, Col: 222. CT FROM ROW(1, CAST('a' AS VARCHAR)) END AS v FROM (VALUES (1, CAST(ROW(1, CAST('a' AS VARCHAR)) AS [4mrecord[0m))) AS t(id, c1) WHERE id = 1 |
| `record_image_ne(record, record)` | ERROR: Expected TYPE after CAST. Line 1, Col: 218. CT FROM ROW(1, CAST('a' AS VARCHAR)) END AS v FROM (VALUES (1, CAST(ROW(1, CAST('a' AS VARCHAR)) AS [4mrecord[0m))) AS t(id, c1) WHERE id = 1 |
| `record_ne(record, record)` | ERROR: Expected TYPE after CAST. Line 1, Col: 218. CT FROM ROW(1, CAST('a' AS VARCHAR)) END AS v FROM (VALUES (1, CAST(ROW(1, CAST('a' AS VARCHAR)) AS [4mrecord[0m))) AS t(id, c1) WHERE id = 1 |
| `regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regclassout(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regnamespaceout(regnamespace)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regroleout(regrole)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml: an argument that is not a constant expression is not supported |
| `schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xmlschema: an argument that is not a constant expression is not supported |
| `setweight(tsvector, "char", text[])` | ERROR: function pg_catalog.setweight(tsvector, "char", jsonb) does not exist |
| `tidout(tid)` | ERROR: tidout() has no form on trino |
| `tidsend(tid)` | ERROR: tidsend() has no form on trino |
| `timestamptypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:216: Cannot cast varchar(4) to array(varchar)", query_id=20261114_012600_18799_ii6mf) |
| `timestamptztypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:218: Cannot cast varchar(4) to array(varchar)", query_id=20261114_012615_19166_ii6mf) |
| `timetypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:211: Cannot cast varchar(4) to array(varchar)", query_id=20261114_012616_19193_ii6mf) |
| `timetztypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:213: Cannot cast varchar(4) to array(varchar)", query_id=20261114_012638_19285_ii6mf) |
| `to_regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regcollation(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regnamespace(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoper(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoperator(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regproc(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regprocedure(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regrole(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtype(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtypemod(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `trim_array(anyarray, integer)` | ERROR: function pg_catalog.trim_array(jsonb, integer) does not exist |
| `ts_delete(tsvector, text[])` | ERROR: function pg_catalog.ts_delete(tsvector, jsonb) does not exist |
| `tsmultirange(VARIADIC tsrange[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:20464: Unknown type: ARRAY(TSRANGE)", query_id=20261114_012753_19953_ii6mf) |
| `tstzmultirange(VARIADIC tstzrange[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:18992: Unknown type: ARRAY(TSTZRANGE)", query_id=20261114_012805_20225_ii6mf) |
| `tsvectorin(cstring)` | ERROR: tsvectorin does not read this value |
| `unnest(anyarray)` | ERROR: function pg_catalog.unnest(jsonb) does not exist |
| `varbit_in(cstring, oid, integer)` | ERROR: varbit_in does not read this value |
| `varbittypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:213: Cannot cast varchar(4) to array(varchar)", query_id=20261114_013141_20822_ii6mf) |
| `varchartypmodin(cstring[])` | ERROR: FederationError(type=USER_ERROR, name=TYPE_MISMATCH, message="line 1:214: Cannot cast varchar(4) to array(varchar)", query_id=20261114_013144_20888_ii6mf) |
| `void_out(void)` | ERROR: Expected TYPE after CAST. Line 1, Col: 104. SELECT CASE WHEN c1 IS NULL THEN CAST(NULL AS VARCHAR) ELSE '' END AS v FROM (VALUES (1, CAST('' AS [4mvoid[0m))) AS t(id, c1) WHERE id = 1 |
| `void_send(void)` | ERROR: Expected TYPE after CAST. Line 1, Col: 116. WHEN c1 IS NULL THEN CAST(NULL AS VARBINARY) ELSE FROM_HEX('') END AS v FROM (VALUES (1, CAST('' AS [4mvoid[0m))) AS t(id, c1) WHERE id = 1 |
| `width_bucket(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.width_bucket(integer, jsonb) does not exist |

</details>

<details><summary>ClickHouse (171)</summary>

| Overload | Column call |
| --- | --- |
| `any_value(anyelement)` | result type oids [2283], PostgreSQL [23] |
| `any_value_transfn(anyelement, anyelement)` | result type oids [2283], PostgreSQL [23] |
| `anyarray_out(anyarray)` | ERROR: function pg_catalog.anyarray_out(jsonb) does not exist |
| `anyarray_send(anyarray)` | ERROR: function pg_catalog.anyarray_send(jsonb) does not exist |
| `anycompatiblearray_out(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_out(jsonb) does not exist |
| `anycompatiblearray_send(anycompatiblearray)` | ERROR: function pg_catalog.anycompatiblearray_send(jsonb) does not exist |
| `array_append(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_append(jsonb, integer) does not exist |
| `array_dims(anyarray)` | ERROR: function pg_catalog.array_dims(jsonb) does not exist |
| `array_eq(anyarray, anyarray)` | ERROR: function pg_catalog.array_eq(jsonb, jsonb) does not exist |
| `array_ge(anyarray, anyarray)` | ERROR: function pg_catalog.array_ge(jsonb, jsonb) does not exist |
| `array_gt(anyarray, anyarray)` | ERROR: function pg_catalog.array_gt(jsonb, jsonb) does not exist |
| `array_larger(anyarray, anyarray)` | ERROR: function pg_catalog.array_larger(jsonb, jsonb) does not exist |
| `array_le(anyarray, anyarray)` | ERROR: function pg_catalog.array_le(jsonb, jsonb) does not exist |
| `array_length(anyarray, integer)` | ERROR: function pg_catalog.array_length(jsonb, integer) does not exist |
| `array_lower(anyarray, integer)` | ERROR: function pg_catalog.array_lower(jsonb, integer) does not exist |
| `array_lt(anyarray, anyarray)` | ERROR: function pg_catalog.array_lt(jsonb, jsonb) does not exist |
| `array_ndims(anyarray)` | ERROR: function pg_catalog.array_ndims(jsonb) does not exist |
| `array_ne(anyarray, anyarray)` | ERROR: function pg_catalog.array_ne(jsonb, jsonb) does not exist |
| `array_out(anyarray)` | ERROR: function pg_catalog.array_out(jsonb) does not exist |
| `array_position(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_position(jsonb, integer) does not exist |
| `array_positions(anycompatiblearray, anycompatible)` | ERROR: function pg_catalog.array_positions(jsonb, integer) does not exist |
| `array_prepend(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.array_prepend(integer, jsonb) does not exist |
| `array_replace(anycompatiblearray, anycompatible, anycompatible)` | ERROR: function pg_catalog.array_replace(jsonb, integer, integer) does not exist |
| `array_send(anyarray)` | ERROR: function pg_catalog.array_send(jsonb) does not exist |
| `array_smaller(anyarray, anyarray)` | ERROR: function pg_catalog.array_smaller(jsonb, jsonb) does not exist |
| `array_to_string(anyarray, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text) does not exist |
| `array_to_string(anyarray, text, text)` | ERROR: function pg_catalog.array_to_string(jsonb, text, text) does not exist |
| `array_to_tsvector(text[])` | ERROR: function pg_catalog.array_to_tsvector(jsonb) does not exist |
| `array_upper(anyarray, integer)` | ERROR: function pg_catalog.array_upper(jsonb, integer) does not exist |
| `bit_in(cstring, oid, integer)` | ERROR: bit_in does not read this value |
| `bittypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `bool(jsonb)` | ERROR: Code: 46. DB::Exception: Function with name `bool` does not exist. In scope SELECT bool(c1) AS v FROM (SELECT 1 AS id, 'true' AS c1) AS t WHERE id = 1. (UNKNOWN_FUNCTION) |
| `bpchartypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `btarraycmp(anyarray, anyarray)` | ERROR: function pg_catalog.btarraycmp(jsonb, jsonb) does not exist |
| `cardinality(anyarray)` | ERROR: function pg_catalog.cardinality(jsonb) does not exist |
| `database_to_xml(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml: an argument that is not a constant expression is not supported |
| `database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `database_to_xmlschema(nulls boolean, tableforest boolean, targetns text)` | ERROR: database_to_xmlschema: an argument that is not a constant expression is not supported |
| `datemultirange(VARIADIC daterange[])` | ERROR: Code: 50. DB::Exception: Unknown data type family: DATERANGE: In scope SELECT 1 AS id, CAST(['[2024-01-01,2024-02-01)', '[2024-03-01,2024-04-01)'], 'Array(Nullable(DATERANGE))') AS c1. (UNKNOWN |
| `enum_first(anyenum)` | ERROR: conformance.probe_enum() has no form on clickhouse |
| `enum_larger(anyenum, anyenum)` | ERROR: conformance.probe_enum() has no form on clickhouse |
| `enum_last(anyenum)` | ERROR: conformance.probe_enum() has no form on clickhouse |
| `enum_range(anyenum)` | ERROR: conformance.probe_enum() has no form on clickhouse |
| `enum_range(anyenum, anyenum)` | ERROR: conformance.probe_enum() has no form on clickhouse |
| `enum_smaller(anyenum, anyenum)` | ERROR: conformance.probe_enum() has no form on clickhouse |
| `has_any_column_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_any_column_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(name, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, smallint, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_column_privilege(oid, text, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_database_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_foreign_data_wrapper_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_function_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_language_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_parameter_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_schema_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_sequence_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_server_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_table_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_tablespace_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(name, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `has_type_privilege(oid, text, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `hash_array(anyarray)` | ERROR: function pg_catalog.hash_array(jsonb) does not exist |
| `hash_array_extended(anyarray, bigint)` | ERROR: function pg_catalog.hash_array_extended(jsonb, bigint) does not exist |
| `int4multirange(VARIADIC int4range[])` | ERROR: Code: 50. DB::Exception: Unknown data type family: INT4RANGE: In scope SELECT 1 AS id, CAST(['[1,5)', '[7,9)'], 'Array(Nullable(INT4RANGE))') AS c1. (UNKNOWN_TYPE) |
| `int8multirange(VARIADIC int8range[])` | ERROR: Code: 50. DB::Exception: Unknown data type family: INT8RANGE: In scope SELECT 1 AS id, CAST(['[1,5)', '[7,9)'], 'Array(Nullable(INT8RANGE))') AS c1. (UNKNOWN_TYPE) |
| `intervaltypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '3': In scope SELECT 1 AS id, CAST('[32767,3]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRIN |
| `makeaclitem(oid, oid, text, boolean)` | ERROR: meta.catalog_pg_roles is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `max(anyarray)` | ERROR: function pg_catalog.max(jsonb) does not exist |
| `min(anyarray)` | ERROR: function pg_catalog.min(jsonb) does not exist |
| `numeric_in(cstring, oid, integer)` | ERROR: numeric_in does not read this value |
| `numerictypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[12,2]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `nummultirange(VARIADIC numrange[])` | ERROR: Code: 50. DB::Exception: Unknown data type family: NUMRANGE: In scope SELECT 1 AS id, CAST(['[1.5,2.5)'], 'Array(Nullable(NUMRANGE))') AS c1. (UNKNOWN_TYPE) |
| `pg_collation_for("any")` | ERROR: pg_collation_for() has no form on clickhouse |
| `pg_column_is_updatable(regclass, smallint, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_describe_object(oid, oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_function_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_constraintdef(oid, boolean)` | ERROR: meta.catalog_pg_constraint is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_function_arg_default(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_identity_arguments(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_result(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_function_sqlbody(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_functiondef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_indexdef(oid, integer, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_publication_tables(VARIADIC pubname text[], OUT pubid oid, OUT relid oid, OUT attrs int2vector, OUT qual pg_node_tree)` | ERROR: pg_get_publication_tables() has no form on postgres |
| `pg_get_replica_identity_index(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_userbyid(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(oid, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_get_viewdef(oid, integer)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_get_viewdef(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(name, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, name, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_has_role(oid, oid, text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object(classid oid, objid oid, objsubid integer, OUT type text, OUT schema text, OUT name text, OUT identity text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_identify_object_as_address(classid oid, objid oid, objsubid integer, OUT type text, OUT object_names text[], OUT object_args text[])` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_partition_root(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_relation_is_updatable(regclass, boolean)` | ERROR: meta.catalog_pg_class is a catalog view of the meta domain and cannot be read in one statement with a table of a data source: read them in two statements |
| `pg_settings_get_flags(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `pg_type_is_visible(oid)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `postgresql_fdw_validator(text[], oid)` | ERROR: function pg_catalog.postgresql_fdw_validator(jsonb, oid) does not exist |
| `regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regclassout(regclass)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regnamespaceout(regnamespace)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `regroleout(regrole)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml: an argument that is not a constant expression is not supported |
| `schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xml_and_xmlschema: an argument that is not a constant expression is not supported |
| `schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | ERROR: schema_to_xmlschema: an argument that is not a constant expression is not supported |
| `setweight(tsvector, "char", text[])` | ERROR: function pg_catalog.setweight(tsvector, "char", jsonb) does not exist |
| `tidout(tid)` | ERROR: tidout() has no form on clickhouse |
| `tidsend(tid)` | ERROR: tidsend() has no form on clickhouse |
| `timestamptypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `timestamptztypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `timetypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `timetztypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `to_regclass(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regcollation(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regnamespace(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoper(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regoperator(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regproc(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regprocedure(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regrole(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtype(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `to_regtypemod(text)` | ERROR: [V003] Column 'c1' is not visible to this role |
| `trim_array(anyarray, integer)` | ERROR: function pg_catalog.trim_array(jsonb, integer) does not exist |
| `ts_delete(tsvector, text[])` | ERROR: function pg_catalog.ts_delete(tsvector, jsonb) does not exist |
| `tsmultirange(VARIADIC tsrange[])` | ERROR: Code: 50. DB::Exception: Unknown data type family: TSRANGE: In scope SELECT 1 AS id, CAST(['[2024-01-01 00:00,2024-02-01 00:00)'], 'Array(Nullable(TSRANGE))') AS c1. (UNKNOWN_TYPE) |
| `tstzmultirange(VARIADIC tstzrange[])` | ERROR: Code: 50. DB::Exception: Unknown data type family: TSTZRANGE: In scope SELECT 1 AS id, CAST(['[2024-01-01 00:00+00,2024-02-01 00:00+00)'], 'Array(Nullable(TSTZRANGE))') AS c1. (UNKNOWN_TYPE) |
| `tsvectorin(cstring)` | ERROR: tsvectorin does not read this value |
| `unnest(anyarray)` | ERROR: function pg_catalog.unnest(jsonb) does not exist |
| `uuid_in(cstring)` | ERROR: Code: 70. DB::Exception: Cannot convert NULL to a non-nullable type: In scope SELECT multiIf(c1 IS NULL, CAST(NULL, 'UUID'), multiIf(match(materialize(c1), '^(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0 |
| `varbit_in(cstring, oid, integer)` | ERROR: varbit_in does not read this value |
| `varbittypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `varchartypmodin(cstring[])` | ERROR: Code: 26. DB::Exception: Cannot parse quoted string: expected opening quote ''', got '1': In scope SELECT 1 AS id, CAST('[10]', 'Array(Nullable(String))') AS c1. (CANNOT_PARSE_QUOTED_STRING) |
| `void_out(void)` | ERROR: Code: 50. DB::Exception: Unknown data type family: void. Maybe you meant: ['UUID']: In scope SELECT 1 AS id, CAST('', 'Nullable(void)') AS c1. (UNKNOWN_TYPE) |
| `void_send(void)` | ERROR: Code: 50. DB::Exception: Unknown data type family: void. Maybe you meant: ['UUID']: In scope SELECT 1 AS id, CAST('', 'Nullable(void)') AS c1. (UNKNOWN_TYPE) |
| `width_bucket(anycompatible, anycompatiblearray)` | ERROR: function pg_catalog.width_bucket(integer, jsonb) does not exist |

</details>

### Equivalence rules

<a id="rule-json-spacing"></a>**1. Whitespace in JSON.** JSON compares as parsed JSON, so spacing between its tokens is no difference. This covers a json or jsonb answer and a JSON value inside a record, such as the value `json_each` returns on DuckDB. Overloads covered: Postgres 0, DuckDB 8, Trino 1, ClickHouse 0.

<a id="rule-enum-label-text"></a>**2. Enum label sent as text.** Provisa holds an enum as text. The label is the same, and the column reports `text` where PostgreSQL reports the enum's own type oid. Overloads covered: Postgres 2, DuckDB 2, Trino 0, ClickHouse 0.

<a id="rule-row-order"></a>**3. Rows compare as a set.** These calls have no `ORDER BY`, so nothing fixes the order of their rows. The engine returned the same rows in another order. Overloads covered: Postgres 0, DuckDB 1, Trino 3, ClickHouse 4.

<a id="rule-number-value"></a>**4. Numbers equal by value.** `2.50` and `2.5` are the same number. A number compares by value, so its trailing zeros and scale are no difference. Overloads covered: Postgres 0, DuckDB 2, Trino 1, ClickHouse 1.

<a id="rule-model-answer"></a>**5. Answers describe the model.** `database_to_xmlschema` and its kin describe the database they run on. Provisa answers for its model and PostgreSQL for its own catalog, so the contents differ. The result type matches and both answers are XML. Overloads covered: Postgres 6, DuckDB 6, Trino 6, ClickHouse 6.

<a id="rule-float-tolerance"></a>**6. Floats within one part in 10^14.** Math libraries differ in the last digits of a float, so `real` and `double precision` answers compare within one part in 10^14. Overloads covered: Postgres 0, DuckDB 3, Trino 10, ClickHouse 9.

<a id="rule-json-key-order"></a>**7. JSON key order.** A JSON object has no key order. The same keys and values in another order are the same answer. Overloads covered: Postgres 0, DuckDB 0, Trino 0, ClickHouse 0.

<a id="rule-array-order"></a>**8. Array built without ORDER BY.** Only an `ORDER BY` in the statement fixes the order of an array an aggregate builds. Without one, its elements compare in any order. Overloads covered: Postgres 0, DuckDB 0, Trino 0, ClickHouse 0.

<a id="rule-clock-field"></a>**9. A clock's field compared by its form.** A field read from the clock, such as `pg_cursor`'s `creation_time`, differs on every call, so it compares by its type's form; the record's other fields compare by value. Overloads covered: Postgres 0, DuckDB 0, Trino 0, ClickHouse 0.

<a id="rule-tzdata-packaging"></a>**10. Time zones a distribution packages differently.** `pg_timezone_names` lists the pinned upstream IANA tzdata, as upstream PostgreSQL ships it, and compares by the names both lists hold. A name that differs only through a distribution's tzdata packaging is skipped by design (time zones follow upstream tzdata): Debian drops the backward links, a link Provisa lists whose zone both list, and adds `localtime` and `posixrules`. Overloads covered: Postgres 0, DuckDB 1, Trino 0, ClickHouse 0.

### No engine serves

236 functions have no overload that any engine answers exactly or equivalently, nor do 276 overloads.

| Family | Functions | Overloads |
| --- | ---: | ---: |
| Binary upgrade | 16 | 16 |
| Date | 1 | 1 |
| Index support | 12 | 12 |
| Large object | 18 | 20 |
| Other | 8 | 9 |
| String | 1 | 3 |
| System | 142 | 177 |
| Type operators | 35 | 35 |
| XML | 3 | 3 |

### Results by family

Each cell reads exact / equivalent / not checked / skipped by design / refused by name / differs.

| Family | Postgres | DuckDB | Trino | ClickHouse |
| --- | ---: | ---: | ---: | ---: |
| Aggregate and window | 87 / 0 / 0 / 0 / 0 / 0 | 82 / 2 / 0 / 0 / 3 / 0 | 44 / 0 / 0 / 0 / 43 / 0 | 70 / 2 / 0 / 0 / 15 / 0 |
| Aggregate support | 12 / 0 / 0 / 0 / 0 / 0 | 12 / 0 / 0 / 0 / 0 / 0 | 12 / 0 / 0 / 0 / 0 / 0 | 12 / 0 / 0 / 0 / 0 / 0 |
| Array | 36 / 0 / 0 / 0 / 0 / 0 | 34 / 0 / 0 / 0 / 2 / 0 | 28 / 0 / 0 / 0 / 8 / 0 | 24 / 0 / 0 / 0 / 12 / 0 |
| Binary upgrade | 0 / 0 / 0 / 16 / 0 / 0 | 0 / 0 / 0 / 16 / 0 / 0 | 0 / 0 / 0 / 16 / 0 / 0 | 0 / 0 / 0 / 16 / 0 / 0 |
| Date | 115 / 0 / 0 / 1 / 0 / 0 | 106 / 0 / 0 / 3 / 7 / 0 | 36 / 0 / 0 / 3 / 77 / 0 | 31 / 0 / 0 / 3 / 82 / 0 |
| Encoding conversion | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| Enum | 11 / 2 / 0 / 0 / 0 / 0 | 13 / 0 / 0 / 0 / 0 / 0 | 6 / 0 / 0 / 0 / 7 / 0 | 6 / 0 / 0 / 0 / 7 / 0 |
| Geometric | 207 / 0 / 0 / 0 / 0 / 0 | 207 / 0 / 0 / 0 / 0 / 0 | 207 / 0 / 0 / 0 / 0 / 0 | 205 / 0 / 0 / 0 / 2 / 0 |
| Hash | 44 / 0 / 0 / 0 / 0 / 0 | 38 / 0 / 0 / 4 / 2 / 0 | 38 / 0 / 0 / 4 / 2 / 0 | 38 / 0 / 0 / 4 / 2 / 0 |
| Index support | 32 / 0 / 0 / 12 / 0 / 0 | 30 / 0 / 0 / 14 / 0 / 0 | 30 / 0 / 0 / 14 / 0 / 0 | 30 / 0 / 0 / 14 / 0 / 0 |
| JSON | 102 / 0 / 0 / 0 / 0 / 0 | 89 / 8 / 0 / 0 / 5 / 0 | 17 / 1 / 0 / 0 / 84 / 0 | 12 / 0 / 0 / 0 / 90 / 0 |
| Large object | 0 / 0 / 0 / 20 / 0 / 0 | 0 / 0 / 0 / 20 / 0 / 0 | 0 / 0 / 0 / 20 / 0 / 0 | 0 / 0 / 0 / 20 / 0 / 0 |
| Network | 33 / 0 / 0 / 0 / 0 / 0 | 14 / 0 / 0 / 0 / 19 / 0 | 7 / 0 / 0 / 0 / 26 / 0 | 5 / 0 / 0 / 0 / 28 / 0 |
| Numeric | 292 / 0 / 0 / 0 / 0 / 0 | 281 / 4 / 0 / 2 / 5 / 0 | 230 / 10 / 0 / 2 / 50 / 0 | 219 / 9 / 0 / 2 / 62 / 0 |
| Other | 78 / 0 / 0 / 9 / 0 / 0 | 78 / 0 / 0 / 9 / 0 / 0 | 52 / 0 / 0 / 10 / 25 / 0 | 69 / 0 / 0 / 9 / 9 / 0 |
| Planner support | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| Range | 111 / 0 / 0 / 0 / 0 / 0 | 111 / 0 / 0 / 0 / 0 / 0 | 106 / 0 / 0 / 0 / 5 / 0 | 106 / 0 / 0 / 0 / 5 / 0 |
| String | 198 / 0 / 0 / 3 / 0 / 0 | 182 / 0 / 0 / 4 / 15 / 0 | 93 / 0 / 0 / 3 / 105 / 0 | 112 / 0 / 0 / 3 / 86 / 0 |
| System | 314 / 0 / 0 / 177 / 0 / 0 | 291 / 1 / 0 / 199 / 0 / 0 | 265 / 0 / 0 / 200 / 26 / 0 | 273 / 0 / 0 / 201 / 17 / 0 |
| Text search | 87 / 0 / 0 / 0 / 0 / 0 | 75 / 0 / 0 / 0 / 12 / 0 | 71 / 3 / 0 / 0 / 13 / 0 | 70 / 2 / 0 / 0 / 15 / 0 |
| Trigger | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| Type operators | 697 / 0 / 0 / 35 / 0 / 0 | 628 / 1 / 0 / 73 / 30 / 0 | 497 / 1 / 0 / 86 / 148 / 0 | 509 / 1 / 0 / 76 / 146 / 0 |
| XML | 14 / 6 / 0 / 3 / 0 / 0 | 14 / 6 / 0 / 3 / 0 / 0 | 7 / 6 / 0 / 3 / 7 / 0 | 6 / 6 / 0 / 3 / 8 / 0 |

### Wrong answers

None.

### Every overload

<details><summary>Aggregate and window (87)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `any_value(anyelement)` | exact | exact | exact | exact |
| `avg(bigint)` | exact | exact | refused | exact |
| `avg(integer)` | exact | exact | refused | exact |
| `avg(smallint)` | exact | exact | refused | exact |
| `bit_and(bigint)` | exact | exact | exact | exact |
| `bit_and(bit)` | exact | exact | refused | refused |
| `bit_and(integer)` | exact | exact | exact | exact |
| `bit_and(smallint)` | exact | exact | exact | exact |
| `bit_or(bigint)` | exact | exact | exact | exact |
| `bit_or(bit)` | exact | exact | refused | refused |
| `bit_or(integer)` | exact | exact | exact | exact |
| `bit_or(smallint)` | exact | exact | exact | exact |
| `bit_xor(bigint)` | exact | exact | exact | exact |
| `bit_xor(bit)` | exact | exact | refused | refused |
| `bit_xor(integer)` | exact | exact | exact | exact |
| `bit_xor(smallint)` | exact | exact | exact | exact |
| `bool_and(boolean)` | exact | exact | exact | exact |
| `bool_or(boolean)` | exact | exact | exact | exact |
| `count()` | exact | exact | exact | exact |
| `count("any")` | exact | exact | exact | exact |
| `cume_dist()` | exact | exact | exact | exact |
| `cume_dist(VARIADIC "any" ORDER BY VARIADIC "any")` | exact | exact | exact | exact |
| `dense_rank()` | exact | exact | exact | exact |
| `dense_rank(VARIADIC "any" ORDER BY VARIADIC "any")` | exact | exact | exact | exact |
| `every(boolean)` | exact | exact | exact | exact |
| `first_value(anyelement)` | exact | exact | exact | exact |
| `lag(anycompatible, integer, anycompatible)` | exact | exact | refused | exact |
| `lag(anyelement)` | exact | exact | refused | exact |
| `lag(anyelement, integer)` | exact | exact | refused | exact |
| `last_value(anyelement)` | exact | exact | exact | equivalent [3](#rule-row-order) |
| `lead(anycompatible, integer, anycompatible)` | exact | exact | refused | exact |
| `lead(anyelement)` | exact | exact | refused | exact |
| `lead(anyelement, integer)` | exact | exact | refused | exact |
| `max(anyarray)` | exact | exact | exact | exact |
| `max(anyenum)` | exact | equivalent [2](#rule-enum-label-text) | refused | refused |
| `max(bigint)` | exact | exact | exact | exact |
| `max(character)` | exact | exact | refused | refused |
| `max(integer)` | exact | exact | exact | exact |
| `max(oid)` | exact | exact | exact | exact |
| `max(pg_lsn)` | exact | exact | exact | exact |
| `max(smallint)` | exact | exact | exact | exact |
| `max(text)` | exact | refused | refused | refused |
| `max(tid)` | exact | exact | refused | refused |
| `max(xid8)` | exact | exact | refused | exact |
| `min(anyarray)` | exact | exact | exact | exact |
| `min(anyenum)` | exact | equivalent [2](#rule-enum-label-text) | refused | refused |
| `min(bigint)` | exact | exact | exact | exact |
| `min(character)` | exact | exact | refused | refused |
| `min(integer)` | exact | exact | exact | exact |
| `min(oid)` | exact | exact | exact | exact |
| `min(pg_lsn)` | exact | exact | exact | exact |
| `min(smallint)` | exact | exact | exact | exact |
| `min(text)` | exact | refused | refused | refused |
| `min(tid)` | exact | exact | refused | refused |
| `min(xid8)` | exact | exact | refused | exact |
| `mode(ORDER BY anyelement)` | exact | exact | refused | refused |
| `nth_value(anyelement, integer)` | exact | exact | exact | equivalent [3](#rule-row-order) |
| `ntile(integer)` | exact | exact | exact | refused |
| `percent_rank()` | exact | exact | exact | exact |
| `percent_rank(VARIADIC "any" ORDER BY VARIADIC "any")` | exact | exact | exact | exact |
| `percentile_disc(double precision[] ORDER BY anyelement)` | exact | exact | exact | exact |
| `rank()` | exact | exact | exact | exact |
| `rank(VARIADIC "any" ORDER BY VARIADIC "any")` | exact | exact | exact | exact |
| `row_number()` | exact | exact | exact | exact |
| `stddev(bigint)` | exact | exact | refused | exact |
| `stddev(integer)` | exact | exact | refused | exact |
| `stddev(smallint)` | exact | exact | refused | exact |
| `stddev_pop(bigint)` | exact | exact | refused | exact |
| `stddev_pop(integer)` | exact | exact | refused | exact |
| `stddev_pop(smallint)` | exact | exact | refused | exact |
| `stddev_samp(bigint)` | exact | exact | refused | exact |
| `stddev_samp(integer)` | exact | exact | refused | exact |
| `stddev_samp(smallint)` | exact | exact | refused | exact |
| `string_agg(value bytea, delimiter bytea)` | exact | exact | refused | refused |
| `string_agg(value text, delimiter text)` | exact | refused | refused | refused |
| `sum(bigint)` | exact | exact | exact | exact |
| `sum(integer)` | exact | exact | exact | exact |
| `sum(smallint)` | exact | exact | exact | exact |
| `var_pop(bigint)` | exact | exact | refused | exact |
| `var_pop(integer)` | exact | exact | refused | exact |
| `var_pop(smallint)` | exact | exact | refused | exact |
| `var_samp(bigint)` | exact | exact | refused | exact |
| `var_samp(integer)` | exact | exact | refused | exact |
| `var_samp(smallint)` | exact | exact | refused | exact |
| `variance(bigint)` | exact | exact | refused | exact |
| `variance(integer)` | exact | exact | refused | exact |
| `variance(smallint)` | exact | exact | refused | exact |

</details>

<details><summary>Aggregate support (12)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `any_value_transfn(anyelement, anyelement)` | exact | exact | exact | exact |
| `booland_statefunc(boolean, boolean)` | exact | exact | exact | exact |
| `boolor_statefunc(boolean, boolean)` | exact | exact | exact | exact |
| `float4_accum(double precision[], real)` | exact | exact | exact | exact |
| `float8_accum(double precision[], double precision)` | exact | exact | exact | exact |
| `float8_combine(double precision[], double precision[])` | exact | exact | exact | exact |
| `float8_regr_accum(double precision[], double precision, double precision)` | exact | exact | exact | exact |
| `float8_regr_combine(double precision[], double precision[])` | exact | exact | exact | exact |
| `int2_avg_accum(bigint[], smallint)` | exact | exact | exact | exact |
| `int2_avg_accum_inv(bigint[], smallint)` | exact | exact | exact | exact |
| `int4_avg_accum(bigint[], integer)` | exact | exact | exact | exact |
| `int4_avg_accum_inv(bigint[], integer)` | exact | exact | exact | exact |

</details>

<details><summary>Array (36)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `array_agg(anyarray)` | exact | exact | exact | refused |
| `array_agg(anynonarray)` | exact | exact | exact | refused |
| `array_append(anycompatiblearray, anycompatible)` | exact | exact | exact | exact |
| `array_cat(anycompatiblearray, anycompatiblearray)` | exact | exact | exact | refused |
| `array_dims(anyarray)` | exact | exact | exact | exact |
| `array_eq(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_fill(anyelement, integer[])` | exact | refused | refused | refused |
| `array_fill(anyelement, integer[], integer[])` | exact | refused | refused | refused |
| `array_ge(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_gt(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_larger(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_le(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_length(anyarray, integer)` | exact | exact | exact | exact |
| `array_lower(anyarray, integer)` | exact | exact | exact | exact |
| `array_lt(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_ndims(anyarray)` | exact | exact | exact | exact |
| `array_ne(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_position(anycompatiblearray, anycompatible)` | exact | exact | exact | exact |
| `array_position(anycompatiblearray, anycompatible, integer)` | exact | exact | refused | refused |
| `array_positions(anycompatiblearray, anycompatible)` | exact | exact | exact | exact |
| `array_prepend(anycompatible, anycompatiblearray)` | exact | exact | exact | exact |
| `array_remove(anycompatiblearray, anycompatible)` | exact | exact | refused | refused |
| `array_replace(anycompatiblearray, anycompatible, anycompatible)` | exact | exact | exact | exact |
| `array_sample(anyarray, integer)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `array_shuffle(anyarray)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `array_smaller(anyarray, anyarray)` | exact | exact | exact | exact |
| `array_to_string(anyarray, text)` | exact | exact | exact | exact |
| `array_to_string(anyarray, text, text)` | exact | exact | exact | exact |
| `array_upper(anyarray, integer)` | exact | exact | exact | exact |
| `arraycontained(anyarray, anyarray)` | exact | exact | refused | refused |
| `arraycontains(anyarray, anyarray)` | exact | exact | exact | refused |
| `arrayoverlap(anyarray, anyarray)` | exact | exact | refused | refused |
| `cardinality(anyarray)` | exact | exact | exact | exact |
| `generate_subscripts(anyarray, integer)` | exact | exact | refused | refused |
| `generate_subscripts(anyarray, integer, boolean)` | exact | exact | refused | refused |
| `unnest(anyarray)` | exact | exact | exact | exact |

</details>

<details><summary>Binary upgrade (16)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `binary_upgrade_logical_slot_has_caught_up(name)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_missing_value(oid, text, text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_array_pg_type_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_heap_pg_class_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_heap_relfilenode(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_index_pg_class_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_index_relfilenode(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_multirange_array_pg_type_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_multirange_pg_type_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_pg_authid_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_pg_enum_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_pg_tablespace_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_pg_type_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_toast_pg_class_oid(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_next_toast_relfilenode(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `binary_upgrade_set_record_init_privs(boolean)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |

</details>

<details><summary>Date (116)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `age(timestamp with time zone)` | exact | exact | refused | refused |
| `age(timestamp with time zone, timestamp with time zone)` | exact | exact | refused | refused |
| `age(timestamp without time zone)` | exact | exact | refused | refused |
| `age(timestamp without time zone, timestamp without time zone)` | exact | exact | refused | refused |
| `avg(interval)` | exact | refused | refused | refused |
| `clock_timestamp()` | exact (type and form only) | exact (type and form only) | refused | refused |
| `date(timestamp with time zone)` | exact | exact | exact | exact |
| `date(timestamp without time zone)` | exact | exact | exact | exact |
| `date_add(timestamp with time zone, interval)` | exact | exact | refused | refused |
| `date_add(timestamp with time zone, interval, text)` | exact | exact | refused | refused |
| `date_part(text, date)` | exact | exact | exact | exact |
| `date_part(text, interval)` | exact | exact | refused | refused |
| `date_part(text, time with time zone)` | exact | exact | refused | refused |
| `date_part(text, time without time zone)` | exact | exact | refused | refused |
| `date_part(text, timestamp with time zone)` | exact | exact | exact | exact |
| `date_part(text, timestamp without time zone)` | exact | exact | exact | exact |
| `date_trunc(text, interval)` | exact | exact | refused | refused |
| `date_trunc(text, timestamp with time zone)` | exact | exact | exact | exact |
| `date_trunc(text, timestamp with time zone, text)` | exact | exact | refused | refused |
| `date_trunc(text, timestamp without time zone)` | exact | exact | exact | exact |
| `daterange_subdiff(date, date)` | exact | exact | exact | exact |
| `datetime_pl(date, time without time zone)` | exact | exact | exact | refused |
| `datetimetz_pl(date, time with time zone)` | exact | exact | refused | refused |
| `extract(text, date)` | exact | exact | exact | exact |
| `extract(text, interval)` | exact | exact | refused | refused |
| `extract(text, time with time zone)` | exact | exact | refused | refused |
| `extract(text, time without time zone)` | exact | exact | refused | refused |
| `extract(text, timestamp with time zone)` | exact | exact | exact | exact |
| `extract(text, timestamp without time zone)` | exact | exact | exact | exact |
| `generate_series(timestamp with time zone, timestamp with time zone, interval)` | exact | refused | refused | refused |
| `generate_series(timestamp with time zone, timestamp with time zone, interval, text)` | exact | refused | refused | refused |
| `generate_series(timestamp without time zone, timestamp without time zone, interval)` | exact | refused | refused | refused |
| `in_range(date, date, interval, boolean, boolean)` | exact | exact | refused | refused |
| `in_range(interval, interval, interval, boolean, boolean)` | exact | exact | refused | refused |
| `in_range(time with time zone, time with time zone, interval, boolean, boolean)` | exact | exact | refused | refused |
| `in_range(time without time zone, time without time zone, interval, boolean, boolean)` | exact | exact | refused | refused |
| `in_range(timestamp with time zone, timestamp with time zone, interval, boolean, boolean)` | exact | exact | refused | refused |
| `in_range(timestamp without time zone, timestamp without time zone, interval, boolean, boolean)` | exact | exact | refused | refused |
| `integer_pl_date(integer, date)` | exact | exact | refused | refused |
| `interval(interval, integer)` | exact | exact | refused | refused |
| `interval(time without time zone)` | exact | exact | refused | refused |
| `isfinite(date)` | exact | exact | exact | exact |
| `isfinite(interval)` | exact | exact | refused | refused |
| `isfinite(timestamp with time zone)` | exact | exact | exact | exact |
| `isfinite(timestamp without time zone)` | exact | exact | exact | exact |
| `justify_days(interval)` | exact | exact | refused | refused |
| `justify_hours(interval)` | exact | exact | refused | refused |
| `justify_interval(interval)` | exact | exact | refused | refused |
| `make_interval(years integer, months integer, weeks integer, days integer, hours integer, mins integer, secs double precision)` | exact | exact | refused | refused |
| `max(date)` | exact | exact | exact | exact |
| `max(interval)` | exact | exact | refused | refused |
| `max(time with time zone)` | exact | exact | refused | refused |
| `max(time without time zone)` | exact | exact | refused | refused |
| `max(timestamp with time zone)` | exact | exact | exact | exact |
| `max(timestamp without time zone)` | exact | exact | exact | exact |
| `min(date)` | exact | exact | exact | exact |
| `min(interval)` | exact | exact | refused | refused |
| `min(time with time zone)` | exact | exact | refused | refused |
| `min(time without time zone)` | exact | exact | refused | refused |
| `min(timestamp with time zone)` | exact | exact | exact | exact |
| `min(timestamp without time zone)` | exact | exact | exact | exact |
| `mul_d_interval(double precision, interval)` | exact | exact | refused | refused |
| `overlaps(time with time zone, time with time zone, time with time zone, time with time zone)` | exact | exact | refused | refused |
| `overlaps(time without time zone, interval, time without time zone, interval)` | exact | exact | refused | refused |
| `overlaps(time without time zone, interval, time without time zone, time without time zone)` | exact | exact | refused | refused |
| `overlaps(time without time zone, time without time zone, time without time zone, interval)` | exact | exact | refused | refused |
| `overlaps(time without time zone, time without time zone, time without time zone, time without time zone)` | exact | exact | exact | refused |
| `overlaps(timestamp with time zone, interval, timestamp with time zone, interval)` | exact | exact | refused | refused |
| `overlaps(timestamp with time zone, interval, timestamp with time zone, timestamp with time zone)` | exact | exact | refused | refused |
| `overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, interval)` | exact | exact | refused | refused |
| `overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `overlaps(timestamp without time zone, interval, timestamp without time zone, interval)` | exact | exact | refused | refused |
| `overlaps(timestamp without time zone, interval, timestamp without time zone, timestamp without time zone)` | exact | exact | refused | refused |
| `overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, interval)` | exact | exact | refused | refused |
| `overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `percentile_cont(double precision ORDER BY interval)` | exact | exact | refused | refused |
| `percentile_cont(double precision[] ORDER BY interval)` | exact | exact | refused | refused |
| `pg_replication_origin_xact_setup(pg_lsn, timestamp with time zone)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_sleep_for(interval)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_sleep_until(timestamp with time zone)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `statement_timestamp()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `sum(interval)` | exact | exact | refused | refused |
| `time(interval)` | exact | exact | refused | refused |
| `time(time with time zone)` | exact | exact | refused | refused |
| `time(time without time zone, integer)` | exact | exact | refused | refused |
| `time(timestamp with time zone)` | exact | exact | exact | refused |
| `time(timestamp without time zone)` | exact | exact | exact | refused |
| `timedate_pl(time without time zone, date)` | exact | exact | exact | refused |
| `timeofday()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `timestamp(date)` | exact | exact | exact | exact |
| `timestamp(date, time without time zone)` | exact | exact | exact | refused |
| `timestamp(timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp(timestamp without time zone, integer)` | exact | exact | refused | refused |
| `timestamptz(date)` | exact | exact | exact | refused |
| `timestamptz(date, time with time zone)` | exact | exact | refused | refused |
| `timestamptz(date, time without time zone)` | exact | exact | refused | refused |
| `timestamptz(timestamp with time zone, integer)` | exact | exact | refused | exact |
| `timestamptz(timestamp without time zone)` | exact | exact | exact | refused |
| `timetz(time with time zone, integer)` | exact | exact | refused | refused |
| `timetz(time without time zone)` | exact | exact | refused | refused |
| `timetz(timestamp with time zone)` | exact | exact | refused | refused |
| `timetzdate_pl(time with time zone, date)` | exact | exact | refused | refused |
| `timezone(interval, time with time zone)` | exact | exact | refused | refused |
| `timezone(interval, timestamp with time zone)` | exact | exact | refused | refused |
| `timezone(interval, timestamp without time zone)` | exact | exact | refused | refused |
| `timezone(text, time with time zone)` | exact | exact | refused | refused |
| `timezone(text, timestamp with time zone)` | exact | exact | refused | exact |
| `timezone(text, timestamp without time zone)` | exact | exact | refused | refused |
| `timezone(time with time zone)` | exact | exact | refused | refused |
| `timezone(timestamp with time zone)` | exact | exact | refused | exact |
| `timezone(timestamp without time zone)` | exact | exact | refused | refused |
| `to_char(interval, text)` | exact | refused | refused | refused |
| `to_char(timestamp with time zone, text)` | exact | refused | refused | refused |
| `to_char(timestamp without time zone, text)` | exact | refused | refused | refused |
| `tsrange_subdiff(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `tstzrange_subdiff(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |

</details>

<details><summary>Enum (13)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `enum_cmp(anyenum, anyenum)` | exact | exact | refused | refused |
| `enum_eq(anyenum, anyenum)` | exact | exact | refused | refused |
| `enum_first(anyenum)` | exact | exact | exact | exact |
| `enum_ge(anyenum, anyenum)` | exact | exact | refused | refused |
| `enum_gt(anyenum, anyenum)` | exact | exact | refused | refused |
| `enum_larger(anyenum, anyenum)` | exact | exact | exact | exact |
| `enum_last(anyenum)` | exact | exact | exact | exact |
| `enum_le(anyenum, anyenum)` | exact | exact | refused | refused |
| `enum_lt(anyenum, anyenum)` | exact | exact | refused | refused |
| `enum_ne(anyenum, anyenum)` | exact | exact | refused | refused |
| `enum_range(anyenum)` | equivalent [2](#rule-enum-label-text) | exact | exact | exact |
| `enum_range(anyenum, anyenum)` | equivalent [2](#rule-enum-label-text) | exact | exact | exact |
| `enum_smaller(anyenum, anyenum)` | exact | exact | exact | exact |

</details>

<details><summary>Geometric (207)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `area(box)` | exact | exact | exact | exact |
| `area(circle)` | exact | exact | exact | exact |
| `area(path)` | exact | exact | exact | exact |
| `bound_box(box, box)` | exact | exact | exact | exact |
| `box(circle)` | exact | exact | exact | exact |
| `box(point)` | exact | exact | exact | exact |
| `box(point, point)` | exact | exact | exact | exact |
| `box(polygon)` | exact | exact | exact | exact |
| `box_above(box, box)` | exact | exact | exact | exact |
| `box_above_eq(box, box)` | exact | exact | exact | exact |
| `box_add(box, point)` | exact | exact | exact | exact |
| `box_below(box, box)` | exact | exact | exact | exact |
| `box_below_eq(box, box)` | exact | exact | exact | exact |
| `box_center(box)` | exact | exact | exact | exact |
| `box_contain(box, box)` | exact | exact | exact | exact |
| `box_contain_pt(box, point)` | exact | exact | exact | exact |
| `box_contained(box, box)` | exact | exact | exact | exact |
| `box_distance(box, box)` | exact | exact | exact | exact |
| `box_div(box, point)` | exact | exact | exact | exact |
| `box_eq(box, box)` | exact | exact | exact | exact |
| `box_ge(box, box)` | exact | exact | exact | exact |
| `box_gt(box, box)` | exact | exact | exact | exact |
| `box_intersect(box, box)` | exact | exact | exact | exact |
| `box_le(box, box)` | exact | exact | exact | exact |
| `box_left(box, box)` | exact | exact | exact | exact |
| `box_lt(box, box)` | exact | exact | exact | exact |
| `box_mul(box, point)` | exact | exact | exact | exact |
| `box_overabove(box, box)` | exact | exact | exact | exact |
| `box_overbelow(box, box)` | exact | exact | exact | exact |
| `box_overlap(box, box)` | exact | exact | exact | exact |
| `box_overleft(box, box)` | exact | exact | exact | exact |
| `box_overright(box, box)` | exact | exact | exact | exact |
| `box_right(box, box)` | exact | exact | exact | exact |
| `box_same(box, box)` | exact | exact | exact | exact |
| `box_sub(box, point)` | exact | exact | exact | exact |
| `center(box)` | exact | exact | exact | exact |
| `center(circle)` | exact | exact | exact | exact |
| `circle(box)` | exact | exact | exact | exact |
| `circle(point, double precision)` | exact | exact | exact | exact |
| `circle(polygon)` | exact | exact | exact | exact |
| `circle_above(circle, circle)` | exact | exact | exact | exact |
| `circle_add_pt(circle, point)` | exact | exact | exact | exact |
| `circle_below(circle, circle)` | exact | exact | exact | exact |
| `circle_center(circle)` | exact | exact | exact | exact |
| `circle_contain(circle, circle)` | exact | exact | exact | exact |
| `circle_contain_pt(circle, point)` | exact | exact | exact | exact |
| `circle_contained(circle, circle)` | exact | exact | exact | exact |
| `circle_distance(circle, circle)` | exact | exact | exact | exact |
| `circle_div_pt(circle, point)` | exact | exact | exact | exact |
| `circle_eq(circle, circle)` | exact | exact | exact | exact |
| `circle_ge(circle, circle)` | exact | exact | exact | exact |
| `circle_gt(circle, circle)` | exact | exact | exact | exact |
| `circle_le(circle, circle)` | exact | exact | exact | exact |
| `circle_left(circle, circle)` | exact | exact | exact | exact |
| `circle_lt(circle, circle)` | exact | exact | exact | exact |
| `circle_mul_pt(circle, point)` | exact | exact | exact | exact |
| `circle_ne(circle, circle)` | exact | exact | exact | exact |
| `circle_overabove(circle, circle)` | exact | exact | exact | exact |
| `circle_overbelow(circle, circle)` | exact | exact | exact | exact |
| `circle_overlap(circle, circle)` | exact | exact | exact | exact |
| `circle_overleft(circle, circle)` | exact | exact | exact | exact |
| `circle_overright(circle, circle)` | exact | exact | exact | exact |
| `circle_right(circle, circle)` | exact | exact | exact | exact |
| `circle_same(circle, circle)` | exact | exact | exact | exact |
| `circle_sub_pt(circle, point)` | exact | exact | exact | exact |
| `close_ls(line, lseg)` | exact | exact | exact | exact |
| `close_lseg(lseg, lseg)` | exact | exact | exact | exact |
| `close_pb(point, box)` | exact | exact | exact | exact |
| `close_pl(point, line)` | exact | exact | exact | exact |
| `close_ps(point, lseg)` | exact | exact | exact | exact |
| `close_sb(lseg, box)` | exact | exact | exact | exact |
| `diagonal(box)` | exact | exact | exact | exact |
| `diameter(circle)` | exact | exact | exact | exact |
| `dist_bp(box, point)` | exact | exact | exact | exact |
| `dist_bs(box, lseg)` | exact | exact | exact | exact |
| `dist_cpoint(circle, point)` | exact | exact | exact | exact |
| `dist_cpoly(circle, polygon)` | exact | exact | exact | exact |
| `dist_lp(line, point)` | exact | exact | exact | exact |
| `dist_ls(line, lseg)` | exact | exact | exact | exact |
| `dist_pathp(path, point)` | exact | exact | exact | exact |
| `dist_pb(point, box)` | exact | exact | exact | exact |
| `dist_pc(point, circle)` | exact | exact | exact | exact |
| `dist_pl(point, line)` | exact | exact | exact | exact |
| `dist_polyc(polygon, circle)` | exact | exact | exact | exact |
| `dist_polyp(polygon, point)` | exact | exact | exact | exact |
| `dist_ppath(point, path)` | exact | exact | exact | exact |
| `dist_ppoly(point, polygon)` | exact | exact | exact | exact |
| `dist_ps(point, lseg)` | exact | exact | exact | exact |
| `dist_sb(lseg, box)` | exact | exact | exact | exact |
| `dist_sl(lseg, line)` | exact | exact | exact | exact |
| `dist_sp(lseg, point)` | exact | exact | exact | exact |
| `height(box)` | exact | exact | exact | exact |
| `inter_lb(line, box)` | exact | exact | exact | exact |
| `inter_sb(lseg, box)` | exact | exact | exact | exact |
| `inter_sl(lseg, line)` | exact | exact | exact | exact |
| `isclosed(path)` | exact | exact | exact | exact |
| `ishorizontal(line)` | exact | exact | exact | exact |
| `ishorizontal(lseg)` | exact | exact | exact | exact |
| `ishorizontal(point, point)` | exact | exact | exact | exact |
| `isopen(path)` | exact | exact | exact | exact |
| `isparallel(line, line)` | exact | exact | exact | exact |
| `isparallel(lseg, lseg)` | exact | exact | exact | exact |
| `isperp(line, line)` | exact | exact | exact | exact |
| `isperp(lseg, lseg)` | exact | exact | exact | exact |
| `isvertical(line)` | exact | exact | exact | exact |
| `isvertical(lseg)` | exact | exact | exact | exact |
| `isvertical(point, point)` | exact | exact | exact | exact |
| `length(lseg)` | exact | exact | exact | refused |
| `length(path)` | exact | exact | exact | refused |
| `line(point, point)` | exact | exact | exact | exact |
| `line_distance(line, line)` | exact | exact | exact | exact |
| `line_eq(line, line)` | exact | exact | exact | exact |
| `line_horizontal(line)` | exact | exact | exact | exact |
| `line_interpt(line, line)` | exact | exact | exact | exact |
| `line_intersect(line, line)` | exact | exact | exact | exact |
| `line_parallel(line, line)` | exact | exact | exact | exact |
| `line_perp(line, line)` | exact | exact | exact | exact |
| `line_vertical(line)` | exact | exact | exact | exact |
| `lseg(box)` | exact | exact | exact | exact |
| `lseg(point, point)` | exact | exact | exact | exact |
| `lseg_center(lseg)` | exact | exact | exact | exact |
| `lseg_distance(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_eq(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_ge(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_gt(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_horizontal(lseg)` | exact | exact | exact | exact |
| `lseg_interpt(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_intersect(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_le(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_length(lseg)` | exact | exact | exact | exact |
| `lseg_lt(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_ne(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_parallel(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_perp(lseg, lseg)` | exact | exact | exact | exact |
| `lseg_vertical(lseg)` | exact | exact | exact | exact |
| `npoints(path)` | exact | exact | exact | exact |
| `npoints(polygon)` | exact | exact | exact | exact |
| `on_pb(point, box)` | exact | exact | exact | exact |
| `on_pl(point, line)` | exact | exact | exact | exact |
| `on_ppath(point, path)` | exact | exact | exact | exact |
| `on_ps(point, lseg)` | exact | exact | exact | exact |
| `on_sb(lseg, box)` | exact | exact | exact | exact |
| `on_sl(lseg, line)` | exact | exact | exact | exact |
| `path(polygon)` | exact | exact | exact | exact |
| `path_add(path, path)` | exact | exact | exact | exact |
| `path_add_pt(path, point)` | exact | exact | exact | exact |
| `path_contain_pt(path, point)` | exact | exact | exact | exact |
| `path_distance(path, path)` | exact | exact | exact | exact |
| `path_div_pt(path, point)` | exact | exact | exact | exact |
| `path_inter(path, path)` | exact | exact | exact | exact |
| `path_length(path)` | exact | exact | exact | exact |
| `path_mul_pt(path, point)` | exact | exact | exact | exact |
| `path_n_eq(path, path)` | exact | exact | exact | exact |
| `path_n_ge(path, path)` | exact | exact | exact | exact |
| `path_n_gt(path, path)` | exact | exact | exact | exact |
| `path_n_le(path, path)` | exact | exact | exact | exact |
| `path_n_lt(path, path)` | exact | exact | exact | exact |
| `path_npoints(path)` | exact | exact | exact | exact |
| `path_sub_pt(path, point)` | exact | exact | exact | exact |
| `pclose(path)` | exact | exact | exact | exact |
| `point(box)` | exact | exact | exact | exact |
| `point(circle)` | exact | exact | exact | exact |
| `point(double precision, double precision)` | exact | exact | exact | exact |
| `point(lseg)` | exact | exact | exact | exact |
| `point(polygon)` | exact | exact | exact | exact |
| `point_above(point, point)` | exact | exact | exact | exact |
| `point_add(point, point)` | exact | exact | exact | exact |
| `point_below(point, point)` | exact | exact | exact | exact |
| `point_distance(point, point)` | exact | exact | exact | exact |
| `point_div(point, point)` | exact | exact | exact | exact |
| `point_eq(point, point)` | exact | exact | exact | exact |
| `point_horiz(point, point)` | exact | exact | exact | exact |
| `point_left(point, point)` | exact | exact | exact | exact |
| `point_mul(point, point)` | exact | exact | exact | exact |
| `point_ne(point, point)` | exact | exact | exact | exact |
| `point_right(point, point)` | exact | exact | exact | exact |
| `point_sub(point, point)` | exact | exact | exact | exact |
| `point_vert(point, point)` | exact | exact | exact | exact |
| `poly_above(polygon, polygon)` | exact | exact | exact | exact |
| `poly_below(polygon, polygon)` | exact | exact | exact | exact |
| `poly_center(polygon)` | exact | exact | exact | exact |
| `poly_contain(polygon, polygon)` | exact | exact | exact | exact |
| `poly_contain_pt(polygon, point)` | exact | exact | exact | exact |
| `poly_contained(polygon, polygon)` | exact | exact | exact | exact |
| `poly_distance(polygon, polygon)` | exact | exact | exact | exact |
| `poly_left(polygon, polygon)` | exact | exact | exact | exact |
| `poly_npoints(polygon)` | exact | exact | exact | exact |
| `poly_overabove(polygon, polygon)` | exact | exact | exact | exact |
| `poly_overbelow(polygon, polygon)` | exact | exact | exact | exact |
| `poly_overlap(polygon, polygon)` | exact | exact | exact | exact |
| `poly_overleft(polygon, polygon)` | exact | exact | exact | exact |
| `poly_overright(polygon, polygon)` | exact | exact | exact | exact |
| `poly_right(polygon, polygon)` | exact | exact | exact | exact |
| `poly_same(polygon, polygon)` | exact | exact | exact | exact |
| `polygon(box)` | exact | exact | exact | exact |
| `polygon(circle)` | exact | exact | exact | exact |
| `polygon(integer, circle)` | exact | exact | exact | exact |
| `polygon(path)` | exact | exact | exact | exact |
| `popen(path)` | exact | exact | exact | exact |
| `pt_contained_circle(point, circle)` | exact | exact | exact | exact |
| `pt_contained_poly(point, polygon)` | exact | exact | exact | exact |
| `radius(circle)` | exact | exact | exact | exact |
| `slope(point, point)` | exact | exact | exact | exact |
| `width(box)` | exact | exact | exact | exact |
| `width_bucket(anycompatible, anycompatiblearray)` | exact | exact | exact | exact |
| `width_bucket(double precision, double precision, double precision, integer)` | exact | exact | exact | exact |
| `width_bucket(numeric, numeric, numeric, integer)` | exact | exact | exact | exact |

</details>

<details><summary>Hash (44)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `hash_aclitem(aclitem)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `hash_aclitem_extended(aclitem, bigint)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `hash_array(anyarray)` | exact | exact | exact | exact |
| `hash_array_extended(anyarray, bigint)` | exact | exact | exact | exact |
| `hash_multirange(anymultirange)` | exact | exact | exact | exact |
| `hash_multirange_extended(anymultirange, bigint)` | exact | exact | exact | exact |
| `hash_numeric(numeric)` | exact | exact | exact | exact |
| `hash_numeric_extended(numeric, bigint)` | exact | exact | exact | exact |
| `hash_range(anyrange)` | exact | exact | exact | exact |
| `hash_range_extended(anyrange, bigint)` | exact | exact | exact | exact |
| `hash_record(record)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `hash_record_extended(record, bigint)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `hashbpchar(character)` | exact | exact | exact | exact |
| `hashbpcharextended(character, bigint)` | exact | exact | exact | exact |
| `hashchar("char")` | exact | exact | exact | exact |
| `hashcharextended("char", bigint)` | exact | exact | exact | exact |
| `hashenum(anyenum)` | exact | refused | refused | refused |
| `hashenumextended(anyenum, bigint)` | exact | refused | refused | refused |
| `hashfloat4(real)` | exact | exact | exact | exact |
| `hashfloat4extended(real, bigint)` | exact | exact | exact | exact |
| `hashfloat8(double precision)` | exact | exact | exact | exact |
| `hashfloat8extended(double precision, bigint)` | exact | exact | exact | exact |
| `hashinet(inet)` | exact | exact | exact | exact |
| `hashinetextended(inet, bigint)` | exact | exact | exact | exact |
| `hashint2(smallint)` | exact | exact | exact | exact |
| `hashint2extended(smallint, bigint)` | exact | exact | exact | exact |
| `hashint4(integer)` | exact | exact | exact | exact |
| `hashint4extended(integer, bigint)` | exact | exact | exact | exact |
| `hashint8(bigint)` | exact | exact | exact | exact |
| `hashint8extended(bigint, bigint)` | exact | exact | exact | exact |
| `hashmacaddr(macaddr)` | exact | exact | exact | exact |
| `hashmacaddr8(macaddr8)` | exact | exact | exact | exact |
| `hashmacaddr8extended(macaddr8, bigint)` | exact | exact | exact | exact |
| `hashmacaddrextended(macaddr, bigint)` | exact | exact | exact | exact |
| `hashname(name)` | exact | exact | exact | exact |
| `hashnameextended(name, bigint)` | exact | exact | exact | exact |
| `hashoid(oid)` | exact | exact | exact | exact |
| `hashoidextended(oid, bigint)` | exact | exact | exact | exact |
| `hashoidvector(oidvector)` | exact | exact | exact | exact |
| `hashoidvectorextended(oidvector, bigint)` | exact | exact | exact | exact |
| `hashtext(text)` | exact | exact | exact | exact |
| `hashtextextended(text, bigint)` | exact | exact | exact | exact |
| `hashtid(tid)` | exact | exact | exact | exact |
| `hashtidextended(tid, bigint)` | exact | exact | exact | exact |

</details>

<details><summary>Index support (44)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `amvalidate(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `brin_bloom_summary_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `brin_bloom_summary_out(pg_brin_bloom_summary)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `brin_bloom_summary_send(pg_brin_bloom_summary)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `brin_desummarize_range(regclass, bigint)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `brin_minmax_multi_summary_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `brin_minmax_multi_summary_out(pg_brin_minmax_multi_summary)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `brin_minmax_multi_summary_send(pg_brin_minmax_multi_summary)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `brin_summarize_new_values(regclass)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `brin_summarize_range(regclass, bigint)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `btarraycmp(anyarray, anyarray)` | exact | exact | exact | exact |
| `btboolcmp(boolean, boolean)` | exact | exact | exact | exact |
| `btbpchar_pattern_cmp(character, character)` | exact | exact | exact | exact |
| `btcharcmp("char", "char")` | exact | exact | exact | exact |
| `btequalimage(oid)` | exact | exact | exact | exact |
| `btfloat48cmp(real, double precision)` | exact | exact | exact | exact |
| `btfloat4cmp(real, real)` | exact | exact | exact | exact |
| `btfloat84cmp(double precision, real)` | exact | exact | exact | exact |
| `btfloat8cmp(double precision, double precision)` | exact | exact | exact | exact |
| `btint24cmp(smallint, integer)` | exact | exact | exact | exact |
| `btint28cmp(smallint, bigint)` | exact | exact | exact | exact |
| `btint2cmp(smallint, smallint)` | exact | exact | exact | exact |
| `btint42cmp(integer, smallint)` | exact | exact | exact | exact |
| `btint48cmp(integer, bigint)` | exact | exact | exact | exact |
| `btint4cmp(integer, integer)` | exact | exact | exact | exact |
| `btint82cmp(bigint, smallint)` | exact | exact | exact | exact |
| `btint84cmp(bigint, integer)` | exact | exact | exact | exact |
| `btint8cmp(bigint, bigint)` | exact | exact | exact | exact |
| `btnamecmp(name, name)` | exact | exact | exact | exact |
| `btnametextcmp(name, text)` | exact | exact | exact | exact |
| `btoidcmp(oid, oid)` | exact | exact | exact | exact |
| `btoidvectorcmp(oidvector, oidvector)` | exact | exact | exact | exact |
| `btrecordcmp(record, record)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `btrecordimagecmp(record, record)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `bttext_pattern_cmp(text, text)` | exact | exact | exact | exact |
| `bttextcmp(text, text)` | exact | exact | exact | exact |
| `bttextnamecmp(text, name)` | exact | exact | exact | exact |
| `bttidcmp(tid, tid)` | exact | exact | exact | exact |
| `gin_clean_pending_list(regclass)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `gin_cmp_tslexeme(text, text)` | exact | exact | exact | exact |
| `gin_compare_jsonb(text, text)` | exact | exact | exact | exact |
| `gtsvectorin(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `gtsvectorout(gtsvector)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `spg_poly_quad_compress(polygon)` | exact | exact | exact | exact |

</details>

<details><summary>JSON (102)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `array_to_json(anyarray)` | exact | exact | refused | refused |
| `array_to_json(anyarray, boolean)` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `json_agg(anyelement)` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `json_agg_strict(anyelement)` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `json_array_element(from_json json, element_index integer)` | exact | exact | exact | exact |
| `json_array_element_text(from_json json, element_index integer)` | exact | exact | exact | exact |
| `json_array_elements(from_json json, OUT value json)` | exact | exact | refused | refused |
| `json_array_elements_text(from_json json, OUT value text)` | exact | exact | refused | refused |
| `json_array_length(json)` | exact | exact | exact | exact |
| `json_build_array()` | exact | exact | refused | refused |
| `json_build_array(VARIADIC "any")` | exact | exact | refused | refused |
| `json_build_object()` | exact | exact | exact | refused |
| `json_build_object(VARIADIC "any")` | exact | equivalent [1](#rule-json-spacing) | equivalent [1](#rule-json-spacing) | refused |
| `json_each(from_json json, OUT key text, OUT value json)` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `json_each_text(from_json json, OUT key text, OUT value text)` | exact | exact | refused | refused |
| `json_extract_path(from_json json, VARIADIC path_elems text[])` | exact | exact | refused | refused |
| `json_extract_path_text(from_json json, VARIADIC path_elems text[])` | exact | exact | refused | refused |
| `json_object(text[])` | exact | exact | refused | refused |
| `json_object(text[], text[])` | exact | exact | refused | refused |
| `json_object_agg(key "any", value "any")` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `json_object_agg_strict(key "any", value "any")` | exact | exact | refused | refused |
| `json_object_agg_unique(key "any", value "any")` | exact | exact | refused | refused |
| `json_object_agg_unique_strict(key "any", value "any")` | exact | exact | refused | refused |
| `json_object_field(from_json json, field_name text)` | exact | exact | exact | exact |
| `json_object_field_text(from_json json, field_name text)` | exact | exact | exact | exact |
| `json_object_keys(json)` | exact | exact | refused | refused |
| `json_populate_record(base anyelement, from_json json, use_json_as_text boolean)` | exact | refused | refused | refused |
| `json_populate_recordset(base anyelement, from_json json, use_json_as_text boolean)` | exact | refused | refused | refused |
| `json_strip_nulls(json)` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `json_to_record(json)` | exact | exact | refused | refused |
| `json_to_recordset(json)` | exact | exact | refused | refused |
| `json_typeof(json)` | exact | exact | exact | exact |
| `jsonb_agg(anyelement)` | exact | exact | refused | refused |
| `jsonb_agg_strict(anyelement)` | exact | exact | refused | refused |
| `jsonb_array_element(from_json jsonb, element_index integer)` | exact | exact | exact | exact |
| `jsonb_array_element_text(from_json jsonb, element_index integer)` | exact | exact | exact | exact |
| `jsonb_array_elements(from_json jsonb, OUT value jsonb)` | exact | exact | refused | refused |
| `jsonb_array_elements_text(from_json jsonb, OUT value text)` | exact | exact | refused | refused |
| `jsonb_array_length(jsonb)` | exact | exact | exact | exact |
| `jsonb_build_array()` | exact | exact | refused | refused |
| `jsonb_build_array(VARIADIC "any")` | exact | exact | refused | refused |
| `jsonb_build_object()` | exact | exact | exact | refused |
| `jsonb_build_object(VARIADIC "any")` | exact | exact | exact | refused |
| `jsonb_cmp(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_concat(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_contained(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_contains(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_delete(from_json jsonb, VARIADIC path_elems text[])` | exact | exact | refused | refused |
| `jsonb_delete(jsonb, integer)` | exact | exact | refused | refused |
| `jsonb_delete(jsonb, text)` | exact | exact | refused | refused |
| `jsonb_delete_path(jsonb, text[])` | exact | exact | refused | refused |
| `jsonb_each(from_json jsonb, OUT key text, OUT value jsonb)` | exact | exact | refused | refused |
| `jsonb_each_text(from_json jsonb, OUT key text, OUT value text)` | exact | exact | refused | refused |
| `jsonb_eq(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_exists(jsonb, text)` | exact | exact | refused | refused |
| `jsonb_exists_all(jsonb, text[])` | exact | exact | refused | refused |
| `jsonb_exists_any(jsonb, text[])` | exact | exact | refused | refused |
| `jsonb_extract_path(from_json jsonb, VARIADIC path_elems text[])` | exact | exact | refused | refused |
| `jsonb_extract_path_text(from_json jsonb, VARIADIC path_elems text[])` | exact | exact | refused | refused |
| `jsonb_ge(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_gt(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_hash(jsonb)` | exact | exact | refused | refused |
| `jsonb_hash_extended(jsonb, bigint)` | exact | exact | refused | refused |
| `jsonb_insert(jsonb_in jsonb, path text[], replacement jsonb, insert_after boolean)` | exact | exact | refused | refused |
| `jsonb_le(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_lt(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_ne(jsonb, jsonb)` | exact | exact | refused | refused |
| `jsonb_object(text[])` | exact | exact | refused | refused |
| `jsonb_object(text[], text[])` | exact | exact | refused | refused |
| `jsonb_object_agg(key "any", value "any")` | exact | exact | refused | refused |
| `jsonb_object_agg_strict(key "any", value "any")` | exact | exact | refused | refused |
| `jsonb_object_agg_unique(key "any", value "any")` | exact | exact | refused | refused |
| `jsonb_object_agg_unique_strict(key "any", value "any")` | exact | exact | refused | refused |
| `jsonb_object_field(from_json jsonb, field_name text)` | exact | exact | exact | exact |
| `jsonb_object_field_text(from_json jsonb, field_name text)` | exact | exact | exact | exact |
| `jsonb_object_keys(jsonb)` | exact | exact | refused | refused |
| `jsonb_path_exists(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_exists_opr(jsonb, jsonpath)` | exact | exact | refused | refused |
| `jsonb_path_exists_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_match(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_match_opr(jsonb, jsonpath)` | exact | exact | refused | refused |
| `jsonb_path_match_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_query(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_query_array(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_query_array_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_query_first(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_query_first_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_path_query_tz(target jsonb, path jsonpath, vars jsonb, silent boolean)` | exact | exact | refused | refused |
| `jsonb_populate_record(anyelement, jsonb)` | exact | refused | refused | refused |
| `jsonb_populate_record_valid(anyelement, jsonb)` | exact | refused | refused | refused |
| `jsonb_populate_recordset(anyelement, jsonb)` | exact | refused | refused | refused |
| `jsonb_pretty(jsonb)` | exact | exact | refused | refused |
| `jsonb_set(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean)` | exact | exact | refused | refused |
| `jsonb_set_lax(jsonb_in jsonb, path text[], replacement jsonb, create_if_missing boolean, null_value_treatment text)` | exact | exact | refused | refused |
| `jsonb_strip_nulls(jsonb)` | exact | exact | refused | refused |
| `jsonb_to_record(jsonb)` | exact | exact | refused | refused |
| `jsonb_to_recordset(jsonb)` | exact | exact | refused | refused |
| `jsonb_typeof(jsonb)` | exact | exact | exact | exact |
| `row_to_json(record)` | exact | exact | refused | refused |
| `row_to_json(record, boolean)` | exact | equivalent [1](#rule-json-spacing) | refused | refused |
| `to_json(anyelement)` | exact | exact | exact | refused |
| `to_jsonb(anyelement)` | exact | exact | exact | refused |

</details>

<details><summary>Large object (20)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `lo_close(integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_creat(integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_create(oid)` | skipped by design (Large objects as a read-only transaction) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_export(oid, text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_from_bytea(oid, bytea)` | skipped by design (Large objects as a read-only transaction) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_get(oid)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `lo_get(oid, bigint, integer)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `lo_import(text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_import(text, oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_lseek(integer, integer, integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_lseek64(integer, bigint, integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_open(oid, integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_put(oid, bigint, bytea)` | skipped by design (Large objects as a read-only transaction) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_tell(integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_tell64(integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_truncate(integer, integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_truncate64(integer, bigint)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lo_unlink(oid)` | skipped by design (Large objects as a read-only transaction) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `loread(integer, integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `lowrite(integer, bytea)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |

</details>

<details><summary>Network (33)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `abbrev(cidr)` | exact | exact | exact | refused |
| `abbrev(inet)` | exact | exact | exact | refused |
| `broadcast(inet)` | exact | exact | refused | refused |
| `cidr(inet)` | exact | refused | refused | refused |
| `family(inet)` | exact | exact | refused | refused |
| `host(inet)` | exact | exact | refused | refused |
| `hostmask(inet)` | exact | refused | refused | refused |
| `macaddr(macaddr8)` | exact | exact | exact | exact |
| `macaddr8(macaddr)` | exact | exact | exact | exact |
| `masklen(inet)` | exact | refused | refused | refused |
| `max(inet)` | exact | exact | refused | refused |
| `min(inet)` | exact | exact | refused | refused |
| `netmask(inet)` | exact | exact | refused | refused |
| `network(inet)` | exact | exact | refused | refused |
| `network_cmp(inet, inet)` | exact | refused | refused | refused |
| `network_eq(inet, inet)` | exact | refused | refused | refused |
| `network_ge(inet, inet)` | exact | refused | refused | refused |
| `network_gt(inet, inet)` | exact | refused | refused | refused |
| `network_larger(inet, inet)` | exact | refused | refused | refused |
| `network_le(inet, inet)` | exact | refused | refused | refused |
| `network_lt(inet, inet)` | exact | refused | refused | refused |
| `network_ne(inet, inet)` | exact | refused | refused | refused |
| `network_overlap(inet, inet)` | exact | refused | refused | refused |
| `network_smaller(inet, inet)` | exact | refused | refused | refused |
| `network_sub(inet, inet)` | exact | refused | refused | refused |
| `network_subeq(inet, inet)` | exact | refused | refused | refused |
| `network_sup(inet, inet)` | exact | refused | refused | refused |
| `network_supeq(inet, inet)` | exact | refused | refused | refused |
| `set_masklen(cidr, integer)` | exact | refused | refused | refused |
| `set_masklen(inet, integer)` | exact | refused | refused | refused |
| `text(inet)` | exact | exact | exact | exact |
| `trunc(macaddr)` | exact | exact | exact | exact |
| `trunc(macaddr8)` | exact | exact | exact | exact |

</details>

<details><summary>Numeric (292)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `abs(double precision)` | exact | exact | exact | exact |
| `abs(numeric)` | exact | exact | exact | exact |
| `abs(real)` | exact | exact | exact | exact |
| `acos(double precision)` | exact | exact | exact | equivalent [6](#rule-float-tolerance) |
| `acosd(double precision)` | exact | exact | exact | exact |
| `acosh(double precision)` | exact | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) |
| `asin(double precision)` | exact | exact | exact | equivalent [6](#rule-float-tolerance) |
| `asind(double precision)` | exact | exact | exact | exact |
| `asinh(double precision)` | exact | exact | exact | exact |
| `atan(double precision)` | exact | exact | exact | exact |
| `atan2(double precision, double precision)` | exact | exact | exact | exact |
| `atan2d(double precision, double precision)` | exact | exact | exact | exact |
| `atand(double precision)` | exact | exact | exact | exact |
| `atanh(double precision)` | exact | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) |
| `avg(double precision)` | exact | exact | exact | exact |
| `avg(numeric)` | exact | exact | refused | refused |
| `avg(real)` | exact | exact | exact | exact |
| `cash_cmp(money, money)` | exact | exact | exact | exact |
| `cash_div_cash(money, money)` | exact | exact | exact | exact |
| `cash_div_flt4(money, real)` | exact | exact | exact | exact |
| `cash_div_flt8(money, double precision)` | exact | exact | exact | exact |
| `cash_div_int2(money, smallint)` | exact | exact | exact | exact |
| `cash_div_int4(money, integer)` | exact | exact | exact | exact |
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
| `cbrt(double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) |
| `ceil(double precision)` | exact | exact | exact | exact |
| `ceil(numeric)` | exact | exact | exact | exact |
| `ceiling(double precision)` | exact | exact | exact | exact |
| `ceiling(numeric)` | exact | exact | exact | exact |
| `corr(double precision, double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | exact |
| `cos(double precision)` | exact | exact | exact | exact |
| `cosd(double precision)` | exact | exact | exact | exact |
| `cosh(double precision)` | exact | exact | exact | exact |
| `cot(double precision)` | exact | exact | exact | equivalent [6](#rule-float-tolerance) |
| `cotd(double precision)` | exact | exact | exact | exact |
| `covar_pop(double precision, double precision)` | exact | exact | exact | exact |
| `covar_samp(double precision, double precision)` | exact | exact | exact | exact |
| `dcbrt(double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) |
| `degrees(double precision)` | exact | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) | equivalent [6](#rule-float-tolerance) |
| `dexp(double precision)` | exact | exact | exact | refused |
| `div(numeric, numeric)` | exact | exact | refused | refused |
| `dlog1(double precision)` | exact | exact | exact | refused |
| `dlog10(double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | exact |
| `dpow(double precision, double precision)` | exact | exact | exact | exact |
| `dround(double precision)` | exact | exact | exact | exact |
| `dsqrt(double precision)` | exact | exact | exact | exact |
| `dtrunc(double precision)` | exact | exact | exact | exact |
| `erf(double precision)` | exact | exact | exact | exact |
| `erfc(double precision)` | exact | exact | exact | exact |
| `exp(double precision)` | exact | exact | exact | refused |
| `exp(numeric)` | exact | exact | refused | refused |
| `float4(bigint)` | exact | exact | exact | exact |
| `float4(double precision)` | exact | exact | exact | exact |
| `float4(integer)` | exact | exact | exact | exact |
| `float4(jsonb)` | exact | exact | refused | refused |
| `float4(numeric)` | exact | exact | exact | exact |
| `float4(smallint)` | exact | exact | exact | exact |
| `float48div(real, double precision)` | exact | exact | exact | exact |
| `float48eq(real, double precision)` | exact | exact | exact | exact |
| `float48ge(real, double precision)` | exact | exact | exact | exact |
| `float48gt(real, double precision)` | exact | exact | exact | exact |
| `float48le(real, double precision)` | exact | exact | exact | exact |
| `float48lt(real, double precision)` | exact | exact | exact | exact |
| `float48mi(real, double precision)` | exact | exact | exact | exact |
| `float48mul(real, double precision)` | exact | exact | exact | exact |
| `float48ne(real, double precision)` | exact | exact | exact | exact |
| `float48pl(real, double precision)` | exact | exact | exact | exact |
| `float8(bigint)` | exact | exact | exact | exact |
| `float8(integer)` | exact | exact | exact | exact |
| `float8(jsonb)` | exact | exact | refused | refused |
| `float8(numeric)` | exact | exact | exact | exact |
| `float8(real)` | exact | exact | exact | exact |
| `float8(smallint)` | exact | exact | exact | exact |
| `float84div(double precision, real)` | exact | exact | exact | exact |
| `float84eq(double precision, real)` | exact | exact | exact | exact |
| `float84ge(double precision, real)` | exact | exact | exact | exact |
| `float84gt(double precision, real)` | exact | exact | exact | exact |
| `float84le(double precision, real)` | exact | exact | exact | exact |
| `float84lt(double precision, real)` | exact | exact | exact | exact |
| `float84mi(double precision, real)` | exact | exact | exact | exact |
| `float84mul(double precision, real)` | exact | exact | exact | exact |
| `float84ne(double precision, real)` | exact | exact | exact | exact |
| `float84pl(double precision, real)` | exact | exact | exact | exact |
| `floor(double precision)` | exact | exact | exact | exact |
| `floor(numeric)` | exact | exact | exact | exact |
| `flt4_mul_cash(real, money)` | exact | exact | exact | exact |
| `flt8_mul_cash(double precision, money)` | exact | exact | exact | exact |
| `gcd(numeric, numeric)` | exact | exact | refused | refused |
| `generate_series(numeric, numeric)` | exact | exact | refused | refused |
| `generate_series(numeric, numeric, numeric)` | exact | exact | refused | refused |
| `in_range(double precision, double precision, double precision, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(numeric, numeric, numeric, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(real, real, double precision, boolean, boolean)` | exact | exact | exact | exact |
| `int2(bigint)` | exact | exact | exact | exact |
| `int2(double precision)` | exact | exact | exact | exact |
| `int2(integer)` | exact | exact | exact | exact |
| `int2(jsonb)` | exact | exact | refused | refused |
| `int2(numeric)` | exact | exact | exact | exact |
| `int2(real)` | exact | exact | exact | exact |
| `int24div(smallint, integer)` | exact | exact | exact | exact |
| `int24eq(smallint, integer)` | exact | exact | exact | exact |
| `int24ge(smallint, integer)` | exact | exact | exact | exact |
| `int24gt(smallint, integer)` | exact | exact | exact | exact |
| `int24le(smallint, integer)` | exact | exact | exact | exact |
| `int24lt(smallint, integer)` | exact | exact | exact | exact |
| `int24mi(smallint, integer)` | exact | exact | exact | exact |
| `int24mul(smallint, integer)` | exact | exact | exact | exact |
| `int24ne(smallint, integer)` | exact | exact | exact | exact |
| `int24pl(smallint, integer)` | exact | exact | exact | exact |
| `int28div(smallint, bigint)` | exact | exact | exact | exact |
| `int28eq(smallint, bigint)` | exact | exact | exact | exact |
| `int28ge(smallint, bigint)` | exact | exact | exact | exact |
| `int28gt(smallint, bigint)` | exact | exact | exact | exact |
| `int28le(smallint, bigint)` | exact | exact | exact | exact |
| `int28lt(smallint, bigint)` | exact | exact | exact | exact |
| `int28mi(smallint, bigint)` | exact | exact | exact | exact |
| `int28mul(smallint, bigint)` | exact | exact | exact | exact |
| `int28ne(smallint, bigint)` | exact | exact | exact | exact |
| `int28pl(smallint, bigint)` | exact | exact | exact | exact |
| `int2_mul_cash(smallint, money)` | exact | exact | exact | exact |
| `int2int4_sum(bigint[])` | exact | exact | refused | refused |
| `int4("char")` | exact | exact | refused | refused |
| `int4(bigint)` | exact | exact | exact | exact |
| `int4(bit)` | exact | exact | refused | refused |
| `int4(boolean)` | exact | exact | exact | exact |
| `int4(double precision)` | exact | exact | exact | exact |
| `int4(jsonb)` | exact | exact | refused | refused |
| `int4(numeric)` | exact | exact | exact | exact |
| `int4(real)` | exact | exact | exact | exact |
| `int4(smallint)` | exact | exact | exact | exact |
| `int42div(integer, smallint)` | exact | exact | exact | exact |
| `int42eq(integer, smallint)` | exact | exact | exact | exact |
| `int42ge(integer, smallint)` | exact | exact | exact | exact |
| `int42gt(integer, smallint)` | exact | exact | exact | exact |
| `int42le(integer, smallint)` | exact | exact | exact | exact |
| `int42lt(integer, smallint)` | exact | exact | exact | exact |
| `int42mi(integer, smallint)` | exact | exact | exact | exact |
| `int42mul(integer, smallint)` | exact | exact | exact | exact |
| `int42ne(integer, smallint)` | exact | exact | exact | exact |
| `int42pl(integer, smallint)` | exact | exact | exact | exact |
| `int48div(integer, bigint)` | exact | exact | exact | exact |
| `int48eq(integer, bigint)` | exact | exact | exact | exact |
| `int48ge(integer, bigint)` | exact | exact | exact | exact |
| `int48gt(integer, bigint)` | exact | exact | exact | exact |
| `int48le(integer, bigint)` | exact | exact | exact | exact |
| `int48lt(integer, bigint)` | exact | exact | exact | exact |
| `int48mi(integer, bigint)` | exact | exact | exact | exact |
| `int48mul(integer, bigint)` | exact | exact | exact | exact |
| `int48ne(integer, bigint)` | exact | exact | exact | exact |
| `int48pl(integer, bigint)` | exact | exact | exact | exact |
| `int4_mul_cash(integer, money)` | exact | exact | exact | exact |
| `int8(bit)` | exact | exact | refused | refused |
| `int8(double precision)` | exact | exact | exact | exact |
| `int8(integer)` | exact | exact | exact | exact |
| `int8(jsonb)` | exact | exact | refused | refused |
| `int8(numeric)` | exact | exact | exact | exact |
| `int8(oid)` | exact | exact | refused | refused |
| `int8(real)` | exact | exact | exact | exact |
| `int8(smallint)` | exact | exact | exact | exact |
| `int82div(bigint, smallint)` | exact | exact | exact | exact |
| `int82eq(bigint, smallint)` | exact | exact | exact | exact |
| `int82ge(bigint, smallint)` | exact | exact | exact | exact |
| `int82gt(bigint, smallint)` | exact | exact | exact | exact |
| `int82le(bigint, smallint)` | exact | exact | exact | exact |
| `int82lt(bigint, smallint)` | exact | exact | exact | exact |
| `int82mi(bigint, smallint)` | exact | exact | exact | exact |
| `int82mul(bigint, smallint)` | exact | exact | exact | exact |
| `int82ne(bigint, smallint)` | exact | exact | exact | exact |
| `int82pl(bigint, smallint)` | exact | exact | exact | exact |
| `int84div(bigint, integer)` | exact | exact | exact | exact |
| `int84eq(bigint, integer)` | exact | exact | exact | exact |
| `int84ge(bigint, integer)` | exact | exact | exact | exact |
| `int84gt(bigint, integer)` | exact | exact | exact | exact |
| `int84le(bigint, integer)` | exact | exact | exact | exact |
| `int84lt(bigint, integer)` | exact | exact | exact | exact |
| `int84mi(bigint, integer)` | exact | exact | exact | exact |
| `int84mul(bigint, integer)` | exact | exact | exact | exact |
| `int84ne(bigint, integer)` | exact | exact | exact | exact |
| `int84pl(bigint, integer)` | exact | exact | exact | exact |
| `int8_mul_cash(bigint, money)` | exact | exact | exact | exact |
| `int8dec(bigint)` | exact | exact | exact | exact |
| `lcm(numeric, numeric)` | exact | exact | refused | refused |
| `ln(double precision)` | exact | exact | exact | refused |
| `ln(numeric)` | exact | exact | refused | refused |
| `log(double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | exact |
| `log(numeric)` | exact | exact | refused | refused |
| `log(numeric, numeric)` | exact | exact | refused | refused |
| `log10(double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | exact |
| `log10(numeric)` | exact | exact | refused | refused |
| `make_time(hour integer, min integer, sec double precision)` | exact | exact | refused | refused |
| `make_timestamp(year integer, month integer, mday integer, hour integer, min integer, sec double precision)` | exact | exact | refused | refused |
| `make_timestamptz(year integer, month integer, mday integer, hour integer, min integer, sec double precision)` | exact | exact | refused | refused |
| `make_timestamptz(year integer, month integer, mday integer, hour integer, min integer, sec double precision, timezone text)` | exact | exact | refused | refused |
| `max(double precision)` | exact | exact | exact | exact |
| `max(money)` | exact | exact | refused | refused |
| `max(numeric)` | exact | equivalent [4](#rule-number-value) | refused | refused |
| `max(real)` | exact | exact | exact | exact |
| `min(double precision)` | exact | exact | exact | exact |
| `min(money)` | exact | exact | refused | refused |
| `min(numeric)` | exact | exact | refused | refused |
| `min(real)` | exact | exact | exact | exact |
| `min_scale(numeric)` | exact | exact | exact | exact |
| `mod(numeric, numeric)` | exact | exact | exact | exact |
| `money(numeric)` | exact | exact | exact | exact |
| `numeric(double precision)` | exact | exact | refused | refused |
| `numeric(money)` | exact | exact | exact | exact |
| `numeric(numeric, integer)` | exact | exact | refused | refused |
| `numeric(real)` | exact | exact | refused | refused |
| `numrange_subdiff(numeric, numeric)` | exact | exact | exact | exact |
| `percentile_cont(double precision ORDER BY double precision)` | exact | exact | exact | exact |
| `percentile_cont(double precision[] ORDER BY double precision)` | exact | exact | exact | exact |
| `percentile_disc(double precision ORDER BY anyelement)` | exact | exact | exact | exact |
| `pg_lsn(numeric)` | exact | exact | refused | refused |
| `pg_size_pretty(numeric)` | exact | exact | refused | refused |
| `pg_sleep(double precision)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pow(double precision, double precision)` | exact | exact | exact | exact |
| `pow(numeric, numeric)` | exact | exact | refused | refused |
| `power(double precision, double precision)` | exact | exact | exact | exact |
| `power(numeric, numeric)` | exact | exact | refused | refused |
| `radians(double precision)` | exact | exact | exact | exact |
| `random(min numeric, max numeric)` | exact (type and form only) | exact (type and form only) | refused | refused |
| `random_normal(mean double precision, stddev double precision)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `regr_avgx(double precision, double precision)` | exact | exact | exact | exact |
| `regr_avgy(double precision, double precision)` | exact | exact | exact | exact |
| `regr_count(double precision, double precision)` | exact | exact | exact | exact |
| `regr_intercept(double precision, double precision)` | exact | exact | exact | exact |
| `regr_r2(double precision, double precision)` | exact | exact | equivalent [6](#rule-float-tolerance) | exact |
| `regr_slope(double precision, double precision)` | exact | exact | exact | exact |
| `regr_sxx(double precision, double precision)` | exact | exact | exact | exact |
| `regr_sxy(double precision, double precision)` | exact | exact | exact | exact |
| `regr_syy(double precision, double precision)` | exact | exact | exact | exact |
| `round(double precision)` | exact | exact | exact | exact |
| `round(numeric)` | exact | exact | exact | exact |
| `round(numeric, integer)` | exact | refused | refused | refused |
| `scale(numeric)` | exact | exact | exact | exact |
| `setseed(double precision)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `sign(double precision)` | exact | exact | exact | exact |
| `sign(numeric)` | exact | exact | exact | exact |
| `sin(double precision)` | exact | exact | exact | exact |
| `sind(double precision)` | exact | exact | exact | exact |
| `sinh(double precision)` | exact | exact | exact | exact |
| `sqrt(double precision)` | exact | exact | exact | exact |
| `sqrt(numeric)` | exact | exact | refused | refused |
| `stddev(double precision)` | exact | exact | exact | exact |
| `stddev(numeric)` | exact | exact | refused | refused |
| `stddev(real)` | exact | exact | exact | refused |
| `stddev_pop(double precision)` | exact | exact | exact | exact |
| `stddev_pop(numeric)` | exact | exact | refused | refused |
| `stddev_pop(real)` | exact | exact | exact | refused |
| `stddev_samp(double precision)` | exact | exact | exact | exact |
| `stddev_samp(numeric)` | exact | exact | refused | refused |
| `stddev_samp(real)` | exact | exact | exact | refused |
| `sum(double precision)` | exact | exact | exact | exact |
| `sum(money)` | exact | exact | exact | exact |
| `sum(numeric)` | exact | exact | exact | exact |
| `sum(real)` | exact | exact | exact | exact |
| `tan(double precision)` | exact | exact | exact | equivalent [6](#rule-float-tolerance) |
| `tand(double precision)` | exact | exact | exact | exact |
| `tanh(double precision)` | exact | exact | exact | refused |
| `to_char(double precision, text)` | exact | refused | refused | refused |
| `to_char(numeric, text)` | exact | refused | refused | refused |
| `to_char(real, text)` | exact | refused | refused | refused |
| `to_timestamp(double precision)` | exact | exact | exact | refused |
| `trim_scale(numeric)` | exact | exact | exact | exact |
| `trunc(double precision)` | exact | exact | exact | exact |
| `trunc(numeric)` | exact | exact | exact | exact |
| `trunc(numeric, integer)` | exact | refused | refused | refused |
| `var_pop(double precision)` | exact | exact | exact | exact |
| `var_pop(numeric)` | exact | exact | refused | refused |
| `var_pop(real)` | exact | exact | exact | refused |
| `var_samp(double precision)` | exact | exact | exact | exact |
| `var_samp(numeric)` | exact | exact | refused | refused |
| `var_samp(real)` | exact | exact | exact | refused |
| `variance(double precision)` | exact | exact | exact | exact |
| `variance(numeric)` | exact | exact | refused | refused |
| `variance(real)` | exact | exact | exact | refused |

</details>

<details><summary>Other (87)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `abs(bigint)` | exact | exact | exact | exact |
| `abs(integer)` | exact | exact | exact | exact |
| `abs(smallint)` | exact | exact | exact | exact |
| `age(xid)` | exact (type and form only) | exact (type and form only) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `bit(bigint, integer)` | exact | exact | refused | refused |
| `bit(integer, integer)` | exact | exact | refused | refused |
| `bool(integer)` | exact | exact | exact | exact |
| `bool(jsonb)` | exact | exact | refused | exact |
| `bpchar("char")` | exact | exact | exact | exact |
| `char(integer)` | exact | exact | refused | refused |
| `chr(integer)` | exact | exact | exact | exact |
| `concat(VARIADIC "any")` | exact | exact | exact | exact |
| `currval(regclass)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `factorial(bigint)` | exact | exact | exact | exact |
| `fmgr_c_validator(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `fmgr_internal_validator(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `fmgr_sql_validator(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `gcd(bigint, bigint)` | exact | exact | refused | exact |
| `gcd(integer, integer)` | exact | exact | refused | exact |
| `gen_random_uuid()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `generate_series(bigint, bigint)` | exact | exact | refused | exact |
| `generate_series(bigint, bigint, bigint)` | exact | exact | refused | exact |
| `generate_series(integer, integer)` | exact | exact | refused | exact |
| `generate_series(integer, integer, integer)` | exact | exact | refused | exact |
| `getdatabaseencoding()` | exact | exact | exact | exact |
| `getpgusername()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `icu_unicode_version()` | exact | exact | exact | exact |
| `in_range(bigint, bigint, bigint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(integer, integer, bigint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(integer, integer, integer, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(integer, integer, smallint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(smallint, smallint, bigint, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(smallint, smallint, integer, boolean, boolean)` | exact | exact | exact | exact |
| `in_range(smallint, smallint, smallint, boolean, boolean)` | exact | exact | exact | exact |
| `int4range_subdiff(integer, integer)` | exact | exact | exact | exact |
| `int8dec_any(bigint, "any")` | exact | exact | refused | refused |
| `int8inc_any(bigint, "any")` | exact | exact | refused | refused |
| `int8range_subdiff(bigint, bigint)` | exact | exact | exact | exact |
| `lastval()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `lcm(bigint, bigint)` | exact | exact | refused | exact |
| `lcm(integer, integer)` | exact | exact | refused | exact |
| `make_date(year integer, month integer, day integer)` | exact | exact | exact | exact |
| `mod(bigint, bigint)` | exact | exact | exact | exact |
| `mod(integer, integer)` | exact | exact | exact | exact |
| `mod(smallint, smallint)` | exact | exact | exact | exact |
| `money(bigint)` | exact | exact | exact | exact |
| `money(integer)` | exact | exact | exact | exact |
| `mxid_age(xid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `nextval(regclass)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `now()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `num_nonnulls(VARIADIC "any")` | exact | exact | exact | exact |
| `num_nulls(VARIADIC "any")` | exact | exact | exact | exact |
| `numeric(bigint)` | exact | exact | exact | exact |
| `numeric(integer)` | exact | exact | exact | exact |
| `numeric(jsonb)` | exact | exact | refused | exact |
| `numeric(smallint)` | exact | exact | exact | exact |
| `oid(bigint)` | exact | exact | exact | exact |
| `pi()` | exact | exact | exact | exact |
| `plpgsql_validator(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `postgresql_fdw_validator(text[], oid)` | exact | exact | exact | exact |
| `quote_literal(anyelement)` | exact | exact | exact | exact |
| `quote_nullable(anyelement)` | exact | exact | exact | exact |
| `random()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `random(min bigint, max bigint)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `random(min integer, max integer)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `row_security_active(oid)` | exact | exact | exact | exact |
| `satisfies_hash_partition(oid, integer, integer, VARIADIC "any")` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | refused | refused |
| `session_user()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `setval(regclass, bigint)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `setval(regclass, bigint, boolean)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `system_user()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `text("char")` | exact | exact | exact | exact |
| `text(boolean)` | exact | exact | exact | exact |
| `text(xml)` | exact | exact | refused | refused |
| `to_bin(bigint)` | exact | exact | refused | exact |
| `to_bin(integer)` | exact | exact | refused | exact |
| `to_hex(bigint)` | exact | exact | refused | exact |
| `to_hex(integer)` | exact | exact | refused | exact |
| `to_oct(bigint)` | exact | exact | refused | exact |
| `to_oct(integer)` | exact | exact | refused | exact |
| `transaction_timestamp()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `trim_array(anyarray, integer)` | exact | exact | exact | exact |
| `ts_token_type(parser_oid oid, OUT tokid integer, OUT alias text, OUT description text)` | exact | exact | exact | exact |
| `unicode_version()` | exact | exact | exact | exact |
| `uuid_extract_timestamp(uuid)` | exact | exact | refused | refused |
| `uuid_extract_version(uuid)` | exact | exact | refused | refused |
| `xid(xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |

</details>

<details><summary>Range (111)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `datemultirange()` | exact | exact | exact | exact |
| `datemultirange(VARIADIC daterange[])` | exact | exact | exact | exact |
| `datemultirange(daterange)` | exact | exact | exact | exact |
| `daterange(date, date)` | exact | exact | exact | exact |
| `daterange(date, date, text)` | exact | exact | exact | exact |
| `daterange_canonical(daterange)` | exact | exact | exact | exact |
| `elem_contained_by_multirange(anyelement, anymultirange)` | exact | exact | exact | exact |
| `elem_contained_by_range(anyelement, anyrange)` | exact | exact | exact | exact |
| `int4multirange()` | exact | exact | exact | exact |
| `int4multirange(VARIADIC int4range[])` | exact | exact | exact | exact |
| `int4multirange(int4range)` | exact | exact | exact | exact |
| `int4range(integer, integer)` | exact | exact | exact | exact |
| `int4range(integer, integer, text)` | exact | exact | exact | exact |
| `int4range_canonical(int4range)` | exact | exact | exact | exact |
| `int8multirange()` | exact | exact | exact | exact |
| `int8multirange(VARIADIC int8range[])` | exact | exact | exact | exact |
| `int8multirange(int8range)` | exact | exact | exact | exact |
| `int8range(bigint, bigint)` | exact | exact | exact | exact |
| `int8range(bigint, bigint, text)` | exact | exact | exact | exact |
| `int8range_canonical(int8range)` | exact | exact | exact | exact |
| `isempty(anymultirange)` | exact | exact | exact | exact |
| `isempty(anyrange)` | exact | exact | exact | exact |
| `lower(anymultirange)` | exact | exact | refused | refused |
| `lower(anyrange)` | exact | exact | refused | refused |
| `lower_inc(anymultirange)` | exact | exact | exact | exact |
| `lower_inc(anyrange)` | exact | exact | exact | exact |
| `lower_inf(anymultirange)` | exact | exact | exact | exact |
| `lower_inf(anyrange)` | exact | exact | exact | exact |
| `multirange(anyrange)` | exact | exact | exact | exact |
| `multirange_adjacent_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_adjacent_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_after_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_after_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_before_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_before_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_cmp(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_contained_by_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_contained_by_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_contains_elem(anymultirange, anyelement)` | exact | exact | exact | exact |
| `multirange_contains_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_contains_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_eq(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_ge(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_gt(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_intersect(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_le(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_lt(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_minus(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_ne(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_overlaps_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_overlaps_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_overleft_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_overleft_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_overright_multirange(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `multirange_overright_range(anymultirange, anyrange)` | exact | exact | exact | exact |
| `multirange_union(anymultirange, anymultirange)` | exact | exact | exact | exact |
| `nummultirange()` | exact | exact | exact | exact |
| `nummultirange(VARIADIC numrange[])` | exact | exact | exact | exact |
| `nummultirange(numrange)` | exact | exact | exact | exact |
| `numrange(numeric, numeric)` | exact | exact | exact | exact |
| `numrange(numeric, numeric, text)` | exact | exact | exact | exact |
| `range_adjacent(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_adjacent_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_after(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_after_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_agg(anymultirange)` | exact | exact | exact | exact |
| `range_agg(anyrange)` | exact | exact | exact | exact |
| `range_before(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_before_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_cmp(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_contained_by(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_contained_by_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_contains(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_contains_elem(anyrange, anyelement)` | exact | exact | exact | exact |
| `range_contains_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_eq(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_ge(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_gt(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_intersect(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_intersect_agg(anymultirange)` | exact | exact | exact | exact |
| `range_intersect_agg(anyrange)` | exact | exact | exact | exact |
| `range_le(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_lt(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_merge(anymultirange)` | exact | exact | exact | exact |
| `range_merge(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_minus(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_ne(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_overlaps(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_overlaps_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_overleft(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_overleft_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_overright(anyrange, anyrange)` | exact | exact | exact | exact |
| `range_overright_multirange(anyrange, anymultirange)` | exact | exact | exact | exact |
| `range_union(anyrange, anyrange)` | exact | exact | exact | exact |
| `tsmultirange()` | exact | exact | exact | exact |
| `tsmultirange(VARIADIC tsrange[])` | exact | exact | exact | exact |
| `tsmultirange(tsrange)` | exact | exact | exact | exact |
| `tsrange(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `tsrange(timestamp without time zone, timestamp without time zone, text)` | exact | exact | exact | exact |
| `tstzmultirange()` | exact | exact | exact | exact |
| `tstzmultirange(VARIADIC tstzrange[])` | exact | exact | exact | exact |
| `tstzmultirange(tstzrange)` | exact | exact | exact | exact |
| `tstzrange(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `tstzrange(timestamp with time zone, timestamp with time zone, text)` | exact | exact | exact | exact |
| `unnest(anymultirange)` | exact | exact | refused | refused |
| `upper(anymultirange)` | exact | exact | refused | refused |
| `upper(anyrange)` | exact | exact | refused | refused |
| `upper_inc(anymultirange)` | exact | exact | exact | exact |
| `upper_inc(anyrange)` | exact | exact | exact | exact |
| `upper_inf(anymultirange)` | exact | exact | exact | exact |
| `upper_inf(anyrange)` | exact | exact | exact | exact |

</details>

<details><summary>String (201)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `anytextcat(anynonarray, text)` | exact | exact | exact | exact |
| `ascii(text)` | exact | exact | exact | exact |
| `bit(bit, integer, boolean)` | exact | exact | refused | refused |
| `bit_count(bit)` | exact | exact | refused | refused |
| `bit_count(bytea)` | exact | exact | refused | exact |
| `bit_length(bit)` | exact | exact | refused | refused |
| `bit_length(bytea)` | exact | exact | refused | exact |
| `bit_length(text)` | exact | exact | exact | exact |
| `bitcat(bit varying, bit varying)` | exact | exact | refused | refused |
| `bitshiftleft(bit, integer)` | exact | exact | refused | refused |
| `bitshiftright(bit, integer)` | exact | exact | refused | refused |
| `bpchar(character, integer, boolean)` | exact | exact | refused | refused |
| `bpchar(name)` | exact | exact | exact | exact |
| `bpchariclike(character, text)` | exact | exact | exact | exact |
| `bpcharicnlike(character, text)` | exact | exact | exact | exact |
| `bpcharicregexeq(character, text)` | exact | exact | exact | exact |
| `bpcharicregexne(character, text)` | exact | exact | exact | exact |
| `bpcharlike(character, text)` | exact | exact | exact | exact |
| `bpcharnlike(character, text)` | exact | exact | exact | exact |
| `bpcharregexeq(character, text)` | exact | exact | exact | exact |
| `bpcharregexne(character, text)` | exact | exact | exact | exact |
| `btrim(bytea, bytea)` | exact | exact | refused | exact |
| `btrim(text)` | exact | exact | exact | exact |
| `btrim(text, text)` | exact | exact | exact | refused |
| `byteacat(bytea, bytea)` | exact | exact | refused | exact |
| `bytealike(bytea, bytea)` | exact | exact | refused | exact |
| `byteanlike(bytea, bytea)` | exact | exact | refused | exact |
| `char(text)` | exact | exact | refused | refused |
| `char_length(character)` | exact | exact | exact | exact |
| `char_length(text)` | exact | exact | exact | exact |
| `character_length(character)` | exact | exact | exact | exact |
| `character_length(text)` | exact | exact | exact | exact |
| `concat_ws(text, VARIADIC "any")` | exact | exact | exact | refused |
| `convert(bytea, name, name)` | exact | exact | refused | refused |
| `convert_from(bytea, name)` | exact | exact | refused | refused |
| `convert_to(text, name)` | exact | exact | refused | refused |
| `currtid2(text, tid)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | refused | refused |
| `decode(text, text)` | exact | refused | refused | refused |
| `encode(bytea, text)` | exact | refused | refused | exact |
| `format(text)` | exact | exact | refused | refused |
| `format(text, VARIADIC "any")` | exact | exact | refused | refused |
| `get_bit(bit, integer)` | exact | exact | refused | refused |
| `get_bit(bytea, bigint)` | exact | exact | refused | exact |
| `get_byte(bytea, integer)` | exact | exact | refused | exact |
| `initcap(text)` | exact | exact | exact | exact |
| `is_normalized(text, text)` | exact | exact | refused | refused |
| `left(text, integer)` | exact | exact | exact | exact |
| `length(bit)` | exact | exact | refused | refused |
| `length(bytea)` | exact | exact | exact | exact |
| `length(bytea, name)` | exact | exact | refused | refused |
| `length(character)` | exact | exact | exact | exact |
| `length(text)` | exact | exact | exact | exact |
| `like(bytea, bytea)` | exact | exact | refused | exact |
| `like(name, text)` | exact | exact | exact | exact |
| `like(text, text)` | exact | exact | exact | exact |
| `like_escape(bytea, bytea)` | exact | exact | refused | exact |
| `like_escape(text, text)` | exact | exact | refused | refused |
| `lower(text)` | exact | exact | exact | exact |
| `lpad(text, integer)` | exact | exact | exact | exact |
| `lpad(text, integer, text)` | exact | exact | exact | exact |
| `ltrim(bytea, bytea)` | exact | exact | refused | exact |
| `ltrim(text)` | exact | exact | exact | exact |
| `ltrim(text, text)` | exact | exact | exact | refused |
| `makeaclitem(oid, oid, text, boolean)` | exact | exact | exact | exact |
| `md5(bytea)` | exact | exact | exact | exact |
| `md5(text)` | exact | exact | exact | exact |
| `name(character)` | exact | exact | exact | exact |
| `name(character varying)` | exact | exact | exact | exact |
| `name(text)` | exact | exact | exact | exact |
| `nameconcatoid(name, oid)` | exact | exact | refused | refused |
| `nameiclike(name, text)` | exact | exact | exact | exact |
| `nameicnlike(name, text)` | exact | exact | exact | exact |
| `nameicregexeq(name, text)` | exact | exact | exact | exact |
| `nameicregexne(name, text)` | exact | exact | exact | exact |
| `namelike(name, text)` | exact | exact | exact | exact |
| `namenlike(name, text)` | exact | exact | exact | exact |
| `nameregexeq(name, text)` | exact | exact | exact | exact |
| `nameregexne(name, text)` | exact | exact | exact | exact |
| `normalize(text, text)` | exact | exact | refused | refused |
| `notlike(bytea, bytea)` | exact | exact | refused | exact |
| `notlike(name, text)` | exact | exact | exact | exact |
| `notlike(text, text)` | exact | exact | exact | exact |
| `octet_length(bit)` | exact | exact | refused | refused |
| `octet_length(bytea)` | exact | exact | refused | exact |
| `octet_length(character)` | exact | exact | exact | exact |
| `octet_length(text)` | exact | exact | exact | exact |
| `overlay(bit, bit, integer)` | exact | exact | refused | refused |
| `overlay(bit, bit, integer, integer)` | exact | exact | refused | refused |
| `overlay(bytea, bytea, integer)` | exact | exact | refused | exact |
| `overlay(bytea, bytea, integer, integer)` | exact | exact | refused | exact |
| `overlay(text, text, integer)` | exact | exact | refused | refused |
| `overlay(text, text, integer, integer)` | exact | exact | refused | refused |
| `parse_ident(str text, strict boolean)` | exact | exact | refused | refused |
| `position(bit, bit)` | exact | exact | refused | refused |
| `position(bytea, bytea)` | exact | exact | refused | exact |
| `position(text, text)` | exact | exact | exact | exact |
| `quote_ident(text)` | exact | exact | exact | exact |
| `quote_literal(text)` | exact | exact | exact | exact |
| `quote_nullable(text)` | exact | exact | exact | exact |
| `regclass(text)` | exact | exact | exact | exact |
| `regexp_count(text, text)` | exact | exact | exact | exact |
| `regexp_count(text, text, integer)` | exact | exact | refused | refused |
| `regexp_count(text, text, integer, text)` | exact | exact | refused | refused |
| `regexp_instr(text, text)` | exact | exact | refused | refused |
| `regexp_instr(text, text, integer)` | exact | exact | refused | refused |
| `regexp_instr(text, text, integer, integer)` | exact | exact | refused | refused |
| `regexp_instr(text, text, integer, integer, integer)` | exact | exact | refused | refused |
| `regexp_instr(text, text, integer, integer, integer, text)` | exact | exact | refused | refused |
| `regexp_instr(text, text, integer, integer, integer, text, integer)` | exact | exact | refused | refused |
| `regexp_like(text, text)` | exact | exact | exact | exact |
| `regexp_like(text, text, text)` | exact | exact | refused | refused |
| `regexp_match(text, text)` | exact | exact | refused | refused |
| `regexp_match(text, text, text)` | exact | exact | refused | refused |
| `regexp_matches(text, text)` | exact | refused | refused | refused |
| `regexp_matches(text, text, text)` | exact | refused | refused | refused |
| `regexp_replace(text, text, text)` | exact | exact | exact | exact |
| `regexp_replace(text, text, text, integer)` | exact | refused | refused | refused |
| `regexp_replace(text, text, text, integer, integer)` | exact | refused | exact | exact |
| `regexp_replace(text, text, text, integer, integer, text)` | skipped by design (Six-argument regexp_replace is not supported) | skipped by design (Six-argument regexp_replace is not supported) | skipped by design (Six-argument regexp_replace is not supported) | skipped by design (Six-argument regexp_replace is not supported) |
| `regexp_replace(text, text, text, text)` | exact | exact | exact | exact |
| `regexp_split_to_array(text, text)` | exact | exact | refused | refused |
| `regexp_split_to_array(text, text, text)` | exact | exact | refused | refused |
| `regexp_split_to_table(text, text)` | exact | exact | refused | refused |
| `regexp_split_to_table(text, text, text)` | exact | exact | refused | refused |
| `regexp_substr(text, text)` | exact | exact | exact | refused |
| `regexp_substr(text, text, integer)` | exact | exact | refused | refused |
| `regexp_substr(text, text, integer, integer)` | exact | exact | refused | refused |
| `regexp_substr(text, text, integer, integer, text)` | exact | exact | refused | refused |
| `regexp_substr(text, text, integer, integer, text, integer)` | exact | exact | refused | refused |
| `repeat(text, integer)` | exact | exact | exact | exact |
| `replace(text, text, text)` | exact | exact | exact | refused |
| `reverse(text)` | exact | exact | exact | exact |
| `right(text, integer)` | exact | exact | exact | exact |
| `row_security_active(text)` | exact | exact | exact | exact |
| `rpad(text, integer)` | exact | exact | exact | exact |
| `rpad(text, integer, text)` | exact | exact | exact | exact |
| `rtrim(bytea, bytea)` | exact | exact | refused | exact |
| `rtrim(text)` | exact | exact | exact | exact |
| `rtrim(text, text)` | exact | exact | exact | refused |
| `set_bit(bit, integer, integer)` | exact | exact | refused | refused |
| `set_bit(bytea, bigint, integer)` | exact | exact | refused | exact |
| `set_byte(bytea, integer, integer)` | exact | exact | refused | exact |
| `set_config(text, text, boolean)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `sha224(bytea)` | exact | refused | refused | exact |
| `sha256(bytea)` | exact | exact | exact | exact |
| `sha384(bytea)` | exact | refused | refused | exact |
| `sha512(bytea)` | exact | refused | exact | exact |
| `similar_escape(text, text)` | exact | exact | refused | refused |
| `similar_to_escape(text)` | exact | exact | refused | refused |
| `similar_to_escape(text, text)` | exact | exact | refused | refused |
| `split_part(text, text, integer)` | exact | exact | exact | exact |
| `starts_with(text, text)` | exact | exact | exact | exact |
| `string_to_array(text, text)` | exact | exact | refused | refused |
| `string_to_array(text, text, text)` | exact | exact | refused | refused |
| `string_to_table(text, text)` | exact | exact | refused | refused |
| `string_to_table(text, text, text)` | exact | exact | refused | refused |
| `strpos(text, text)` | exact | exact | exact | exact |
| `substr(bytea, integer)` | exact | exact | refused | exact |
| `substr(bytea, integer, integer)` | exact | exact | refused | exact |
| `substr(text, integer)` | exact | exact | exact | exact |
| `substr(text, integer, integer)` | exact | exact | exact | exact |
| `substring(bit, integer)` | exact | refused | refused | refused |
| `substring(bit, integer, integer)` | exact | refused | refused | refused |
| `substring(bytea, integer)` | exact | exact | refused | exact |
| `substring(bytea, integer, integer)` | exact | exact | refused | exact |
| `substring(text, integer)` | exact | exact | exact | exact |
| `substring(text, integer, integer)` | exact | exact | exact | exact |
| `substring(text, text)` | exact | refused | refused | refused |
| `substring(text, text, text)` | exact | exact | refused | refused |
| `text(character)` | exact | exact | exact | exact |
| `text(name)` | exact | exact | exact | exact |
| `textanycat(text, anynonarray)` | exact | exact | exact | exact |
| `textcat(text, text)` | exact | exact | exact | exact |
| `texticlike(text, text)` | exact | exact | exact | exact |
| `texticnlike(text, text)` | exact | exact | exact | exact |
| `texticregexeq(text, text)` | exact | exact | exact | exact |
| `texticregexne(text, text)` | exact | exact | exact | exact |
| `textlike(text, text)` | exact | exact | exact | exact |
| `textnlike(text, text)` | exact | exact | exact | exact |
| `textregexeq(text, text)` | exact | exact | exact | exact |
| `textregexne(text, text)` | exact | exact | exact | exact |
| `to_ascii(text)` | skipped by design (One-argument to_ascii needs a single-byte database) | skipped by design (One-argument to_ascii needs a single-byte database) | skipped by design (One-argument to_ascii needs a single-byte database) | skipped by design (One-argument to_ascii needs a single-byte database) |
| `to_ascii(text, integer)` | exact | exact | refused | refused |
| `to_ascii(text, name)` | exact | exact | refused | refused |
| `to_char(bigint, text)` | exact | refused | refused | refused |
| `to_char(integer, text)` | exact | refused | refused | refused |
| `to_date(text, text)` | exact | exact | refused | refused |
| `to_number(text, text)` | exact | refused | refused | refused |
| `to_timestamp(text, text)` | exact | exact | refused | refused |
| `translate(text, text, text)` | exact | exact | exact | refused |
| `ts_token_type(parser_name text, OUT tokid integer, OUT alias text, OUT description text)` | exact | exact | exact | exact |
| `unicode_assigned(text)` | exact | exact | refused | refused |
| `unistr(text)` | exact | exact | refused | refused |
| `upper(text)` | exact | exact | exact | exact |
| `varbit(bit varying, integer, boolean)` | exact | exact | refused | refused |
| `varchar(character varying, integer, boolean)` | exact | exact | refused | refused |
| `varchar(name)` | exact | exact | refused | refused |
| `xpath(text, xml)` | exact | exact | refused | refused |
| `xpath(text, xml, text[])` | exact | exact | refused | refused |
| `xpath_exists(text, xml)` | exact | exact | refused | refused |
| `xpath_exists(text, xml, text[])` | exact | exact | refused | refused |

</details>

<details><summary>System (491)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `aclcontains(aclitem[], aclitem)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `acldefault("char", oid)` | exact | exact | exact | exact |
| `aclexplode(acl aclitem[], OUT grantor oid, OUT grantee oid, OUT privilege_type text, OUT is_grantable boolean)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `aclitemeq(aclitem, aclitem)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `col_description(oid, integer)` | exact | exact | exact | exact |
| `current_database()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `current_query()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `current_schema()` | exact | exact | exact | exact |
| `current_schemas(boolean)` | exact | exact | refused | refused |
| `current_setting(text)` | exact | exact | exact | exact |
| `current_setting(text, boolean)` | exact | exact | exact | exact |
| `current_user()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `format_type(oid, integer)` | exact | exact | exact | exact |
| `has_any_column_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_any_column_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_any_column_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_any_column_privilege(oid, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_any_column_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_any_column_privilege(text, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_column_privilege(name, oid, smallint, text)` | exact | exact | exact | exact |
| `has_column_privilege(name, oid, text, text)` | exact | exact | exact | exact |
| `has_column_privilege(name, text, smallint, text)` | exact | exact | exact | exact |
| `has_column_privilege(name, text, text, text)` | exact | exact | exact | exact |
| `has_column_privilege(oid, oid, smallint, text)` | exact | exact | exact | exact |
| `has_column_privilege(oid, oid, text, text)` | exact | exact | exact | exact |
| `has_column_privilege(oid, smallint, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_column_privilege(oid, text, smallint, text)` | exact | exact | exact | exact |
| `has_column_privilege(oid, text, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_column_privilege(oid, text, text, text)` | exact | exact | exact | exact |
| `has_column_privilege(text, smallint, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_column_privilege(text, text, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_database_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_database_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_database_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_database_privilege(oid, text)` | exact | exact | exact | exact |
| `has_database_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_database_privilege(text, text)` | exact | exact | exact | exact |
| `has_foreign_data_wrapper_privilege(name, oid, text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `has_foreign_data_wrapper_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_foreign_data_wrapper_privilege(oid, oid, text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `has_foreign_data_wrapper_privilege(oid, text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `has_foreign_data_wrapper_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_foreign_data_wrapper_privilege(text, text)` | exact | exact | exact | exact |
| `has_function_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_function_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_function_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_function_privilege(oid, text)` | exact | exact | exact | exact |
| `has_function_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_function_privilege(text, text)` | exact | exact | exact | exact |
| `has_language_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_language_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_language_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_language_privilege(oid, text)` | exact | exact | exact | exact |
| `has_language_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_language_privilege(text, text)` | exact | exact | exact | exact |
| `has_parameter_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_parameter_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_parameter_privilege(text, text)` | exact | exact | exact | exact |
| `has_schema_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_schema_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_schema_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_schema_privilege(oid, text)` | exact | exact | exact | exact |
| `has_schema_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_schema_privilege(text, text)` | exact | exact | exact | exact |
| `has_sequence_privilege(name, oid, text)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `has_sequence_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_sequence_privilege(oid, oid, text)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `has_sequence_privilege(oid, text)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `has_sequence_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_sequence_privilege(text, text)` | exact | exact | exact | exact |
| `has_server_privilege(name, oid, text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `has_server_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_server_privilege(oid, oid, text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `has_server_privilege(oid, text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `has_server_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_server_privilege(text, text)` | exact | exact | exact | exact |
| `has_table_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_table_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_table_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_table_privilege(oid, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_table_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_table_privilege(text, text)` | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) | skipped by design (A system catalog is written by no role) |
| `has_tablespace_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_tablespace_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_tablespace_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_tablespace_privilege(oid, text)` | exact | exact | exact | exact |
| `has_tablespace_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_tablespace_privilege(text, text)` | exact | exact | exact | exact |
| `has_type_privilege(name, oid, text)` | exact | exact | exact | exact |
| `has_type_privilege(name, text, text)` | exact | exact | exact | exact |
| `has_type_privilege(oid, oid, text)` | exact | exact | exact | exact |
| `has_type_privilege(oid, text)` | exact | exact | exact | exact |
| `has_type_privilege(oid, text, text)` | exact | exact | exact | exact |
| `has_type_privilege(text, text)` | exact | exact | exact | exact |
| `inet_client_addr()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `inet_client_port()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `inet_server_addr()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `inet_server_port()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `obj_description(oid)` | exact | exact | exact | exact |
| `obj_description(oid, name)` | exact | exact | exact | exact |
| `pg_advisory_lock(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_lock(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_lock_shared(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_lock_shared(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_unlock(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_unlock(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_unlock_all()` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_unlock_shared(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_unlock_shared(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_xact_lock(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_xact_lock(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_xact_lock_shared(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_advisory_xact_lock_shared(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_available_extension_versions(OUT name name, OUT version text, OUT superuser boolean, OUT trusted boolean, OUT relocatable boolean, OUT schema name, OUT requires name[], OUT comment text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_available_extensions(OUT name name, OUT default_version text, OUT comment text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_available_wal_summaries(OUT tli bigint, OUT start_lsn pg_lsn, OUT end_lsn pg_lsn)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_backend_pid()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_backup_start(label text, fast boolean)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_backup_stop(wait_for_archive boolean, OUT lsn pg_lsn, OUT labelfile text, OUT spcmapfile text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_basetype(regtype)` | exact | exact | exact | exact |
| `pg_blocking_pids(integer)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_cancel_backend(integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_char_to_encoding(name)` | exact | exact | exact | exact |
| `pg_client_encoding()` | exact | exact | exact | exact |
| `pg_collation_actual_version(oid)` | skipped by design (The server's version) | skipped by design (The server's version) | skipped by design (The server's version) | skipped by design (The server's version) |
| `pg_collation_for("any")` | exact | exact | exact | exact |
| `pg_collation_is_visible(oid)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_column_compression("any")` | exact | exact | exact | exact |
| `pg_column_is_updatable(regclass, smallint, boolean)` | exact | exact | exact | exact |
| `pg_column_size("any")` | exact | exact | exact | exact |
| `pg_column_toast_chunk_id("any")` | exact | exact | exact | exact |
| `pg_conf_load_time()` | exact (type and form only) | exact (type and form only) | refused | refused |
| `pg_config(OUT name text, OUT setting text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_control_checkpoint(OUT checkpoint_lsn pg_lsn, OUT redo_lsn pg_lsn, OUT redo_wal_file text, OUT timeline_id integer, OUT prev_timeline_id integer, OUT full_page_writes boolean, OUT next_xid text, OUT next_oid oid, OUT next_multixact_id xid, OUT next_multi_offset xid, OUT oldest_xid xid, OUT oldest_xid_dbid oid, OUT oldest_active_xid xid, OUT oldest_multi_xid xid, OUT oldest_multi_dbid oid, OUT oldest_commit_ts_xid xid, OUT newest_commit_ts_xid xid, OUT checkpoint_time timestamp with time zone)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_control_recovery(OUT min_recovery_end_lsn pg_lsn, OUT min_recovery_end_timeline integer, OUT backup_start_lsn pg_lsn, OUT backup_end_lsn pg_lsn, OUT end_of_backup_record_required boolean)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_control_system(OUT pg_control_version integer, OUT catalog_version_no integer, OUT system_identifier bigint, OUT pg_control_last_modified timestamp with time zone)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_conversion_is_visible(oid)` | exact | exact | exact | exact |
| `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name, OUT slot_name name, OUT lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name, temporary boolean, OUT slot_name name, OUT lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name, temporary boolean, plugin name, OUT slot_name name, OUT lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_copy_physical_replication_slot(src_slot_name name, dst_slot_name name, OUT slot_name name, OUT lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_copy_physical_replication_slot(src_slot_name name, dst_slot_name name, temporary boolean, OUT slot_name name, OUT lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_create_logical_replication_slot(slot_name name, plugin name, temporary boolean, twophase boolean, failover boolean, OUT slot_name name, OUT lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_create_physical_replication_slot(slot_name name, immediately_reserve boolean, temporary boolean, OUT slot_name name, OUT lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_create_restore_point(text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_current_logfile()` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_current_logfile(text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_current_snapshot()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_current_wal_flush_lsn()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_current_wal_insert_lsn()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_current_wal_lsn()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_current_xact_id()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_current_xact_id_if_assigned()` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_cursor(OUT name text, OUT statement text, OUT is_holdable boolean, OUT is_binary boolean, OUT is_scrollable boolean, OUT creation_time timestamp with time zone)` | skipped by design (The session's own cursors) | skipped by design (The session's own cursors) | skipped by design (The session's own cursors) | skipped by design (The session's own cursors) |
| `pg_database_collation_actual_version(oid)` | skipped by design (The server's version) | skipped by design (The server's version) | skipped by design (The server's version) | skipped by design (The server's version) |
| `pg_database_size(name)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_database_size(oid)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_describe_object(oid, oid, integer)` | exact | exact | exact | exact |
| `pg_drop_replication_slot(name)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_encoding_max_length(integer)` | exact | exact | refused | refused |
| `pg_encoding_to_char(integer)` | exact | exact | exact | exact |
| `pg_export_snapshot()` | exact (type and form only) | exact (type and form only) | refused | exact (type and form only) |
| `pg_extension_config_dump(regclass, text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_extension_update_paths(name name, OUT source text, OUT target text, OUT path text)` | skipped by design (The server's installed extensions) | skipped by design (The server's installed extensions) | skipped by design (The server's installed extensions) | skipped by design (The server's installed extensions) |
| `pg_filenode_relation(oid, oid)` | exact | exact | exact | exact |
| `pg_function_is_visible(oid)` | exact | exact | exact | exact |
| `pg_get_backend_memory_contexts(OUT name text, OUT ident text, OUT parent text, OUT level integer, OUT total_bytes bigint, OUT total_nblocks bigint, OUT free_bytes bigint, OUT free_chunks bigint, OUT used_bytes bigint)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_get_catalog_foreign_keys(OUT fktable regclass, OUT fkcols text[], OUT pktable regclass, OUT pkcols text[], OUT is_array boolean, OUT is_opt boolean)` | exact | exact | exact | exact |
| `pg_get_constraintdef(oid)` | exact | exact | exact | exact |
| `pg_get_constraintdef(oid, boolean)` | exact | exact | exact | exact |
| `pg_get_expr(pg_node_tree, oid)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_get_expr(pg_node_tree, oid, boolean)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_get_function_arg_default(oid, integer)` | exact | exact | exact | exact |
| `pg_get_function_arguments(oid)` | exact | exact | exact | exact |
| `pg_get_function_identity_arguments(oid)` | exact | exact | exact | exact |
| `pg_get_function_result(oid)` | exact | exact | exact | exact |
| `pg_get_function_sqlbody(oid)` | exact | exact | exact | exact |
| `pg_get_functiondef(oid)` | exact | exact | exact | exact |
| `pg_get_indexdef(oid)` | exact | exact | exact | exact |
| `pg_get_indexdef(oid, integer, boolean)` | exact | exact | exact | exact |
| `pg_get_keywords(OUT word text, OUT catcode "char", OUT barelabel boolean, OUT catdesc text, OUT baredesc text)` | exact | exact | exact | exact |
| `pg_get_multixact_members(multixid xid, OUT xid xid, OUT mode text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_get_object_address(type text, object_names text[], object_args text[], OUT classid oid, OUT objid oid, OUT objsubid integer)` | exact | exact | exact | exact |
| `pg_get_partition_constraintdef(oid)` | exact | exact | exact | exact |
| `pg_get_partkeydef(oid)` | exact | exact | exact | exact |
| `pg_get_publication_tables(VARIADIC pubname text[], OUT pubid oid, OUT relid oid, OUT attrs int2vector, OUT qual pg_node_tree)` | exact | exact | exact | exact |
| `pg_get_replica_identity_index(regclass)` | exact | exact | exact | exact |
| `pg_get_replication_slots(OUT slot_name name, OUT plugin name, OUT slot_type text, OUT datoid oid, OUT temporary boolean, OUT active boolean, OUT active_pid integer, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn pg_lsn, OUT confirmed_flush_lsn pg_lsn, OUT wal_status text, OUT safe_wal_size bigint, OUT two_phase boolean, OUT inactive_since timestamp with time zone, OUT conflicting boolean, OUT invalidation_reason text, OUT failover boolean, OUT synced boolean)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_get_ruledef(oid)` | exact | exact | exact | exact |
| `pg_get_ruledef(oid, boolean)` | exact | exact | exact | exact |
| `pg_get_serial_sequence(text, text)` | exact | exact | exact | exact |
| `pg_get_shmem_allocations(OUT name text, OUT off bigint, OUT size bigint, OUT allocated_size bigint)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_get_statisticsobjdef(oid)` | exact | exact | exact | exact |
| `pg_get_statisticsobjdef_columns(oid)` | exact | exact | exact | exact |
| `pg_get_statisticsobjdef_expressions(oid)` | exact | exact | exact | exact |
| `pg_get_triggerdef(oid)` | exact | exact | exact | exact |
| `pg_get_triggerdef(oid, boolean)` | exact | exact | exact | exact |
| `pg_get_userbyid(oid)` | exact | exact | exact | exact |
| `pg_get_viewdef(oid)` | exact | exact | exact | exact |
| `pg_get_viewdef(oid, boolean)` | exact | exact | exact | exact |
| `pg_get_viewdef(oid, integer)` | exact | exact | exact | exact |
| `pg_get_viewdef(text)` | exact | exact | exact | exact |
| `pg_get_viewdef(text, boolean)` | exact | exact | exact | exact |
| `pg_get_wait_events(OUT type text, OUT name text, OUT description text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_get_wal_replay_pause_state()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_get_wal_resource_managers(OUT rm_id integer, OUT rm_name text, OUT rm_builtin boolean)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_get_wal_summarizer_state(OUT summarized_tli bigint, OUT summarized_lsn pg_lsn, OUT pending_lsn pg_lsn, OUT summarizer_pid integer)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_has_role(name, name, text)` | exact | exact | exact | exact |
| `pg_has_role(name, oid, text)` | exact | exact | exact | exact |
| `pg_has_role(name, text)` | exact | exact | exact | exact |
| `pg_has_role(oid, name, text)` | exact | exact | exact | exact |
| `pg_has_role(oid, oid, text)` | exact | exact | exact | exact |
| `pg_has_role(oid, text)` | exact | exact | exact | exact |
| `pg_hba_file_rules(OUT rule_number integer, OUT file_name text, OUT line_number integer, OUT type text, OUT database text[], OUT user_name text[], OUT address text, OUT netmask text, OUT auth_method text, OUT options text[], OUT error text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_ident_file_mappings(OUT map_number integer, OUT file_name text, OUT line_number integer, OUT map_name text, OUT sys_name text, OUT pg_username text, OUT error text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_identify_object(classid oid, objid oid, objsubid integer, OUT type text, OUT schema text, OUT name text, OUT identity text)` | exact | exact | exact | exact |
| `pg_identify_object_as_address(classid oid, objid oid, objsubid integer, OUT type text, OUT object_names text[], OUT object_args text[])` | exact | exact | exact | exact |
| `pg_import_system_collations(regnamespace)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_index_column_has_property(regclass, integer, text)` | exact | exact | exact | exact |
| `pg_index_has_property(regclass, text)` | exact | exact | exact | exact |
| `pg_indexam_has_property(oid, text)` | exact | exact | refused | refused |
| `pg_indexam_progress_phasename(oid, bigint)` | exact | exact | exact | exact |
| `pg_indexes_size(regclass)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_input_error_info(value text, type_name text, OUT message text, OUT detail text, OUT hint text, OUT sql_error_code text)` | exact | exact | exact | exact |
| `pg_input_is_valid(text, text)` | exact | exact | exact | exact |
| `pg_is_in_recovery()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_is_other_temp_schema(oid)` | exact | exact | exact | exact |
| `pg_is_wal_replay_paused()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_isolation_test_session_is_blocked(integer, integer[])` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_jit_available()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_last_committed_xact(OUT xid xid, OUT "timestamp" timestamp with time zone, OUT roident oid)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_last_wal_receive_lsn()` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_last_wal_replay_lsn()` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_last_xact_replay_timestamp()` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_listening_channels()` | exact | exact | exact | exact |
| `pg_lock_status(OUT locktype text, OUT database oid, OUT relation oid, OUT page integer, OUT tuple smallint, OUT virtualxid text, OUT transactionid xid, OUT classid oid, OUT objid oid, OUT objsubid smallint, OUT virtualtransaction text, OUT pid integer, OUT mode text, OUT granted boolean, OUT fastpath boolean, OUT waitstart timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_log_backend_memory_contexts(integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_log_standby_snapshot()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_logical_emit_message(transactional boolean, prefix text, message bytea, flush boolean)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_logical_emit_message(transactional boolean, prefix text, message text, flush boolean)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_logical_slot_get_binary_changes(slot_name name, upto_lsn pg_lsn, upto_nchanges integer, VARIADIC options text[], OUT lsn pg_lsn, OUT xid xid, OUT data bytea)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_logical_slot_get_changes(slot_name name, upto_lsn pg_lsn, upto_nchanges integer, VARIADIC options text[], OUT lsn pg_lsn, OUT xid xid, OUT data text)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_logical_slot_peek_binary_changes(slot_name name, upto_lsn pg_lsn, upto_nchanges integer, VARIADIC options text[], OUT lsn pg_lsn, OUT xid xid, OUT data bytea)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_logical_slot_peek_changes(slot_name name, upto_lsn pg_lsn, upto_nchanges integer, VARIADIC options text[], OUT lsn pg_lsn, OUT xid xid, OUT data text)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_ls_archive_statusdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_ls_dir(text)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_ls_dir(text, boolean, boolean)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_ls_logdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_ls_logicalmapdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_ls_logicalsnapdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_ls_replslotdir(slot_name text, OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_ls_tmpdir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_ls_tmpdir(tablespace oid, OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_ls_waldir(OUT name text, OUT size bigint, OUT modification timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_mcv_list_items(mcv_list pg_mcv_list, OUT index integer, OUT "values" text[], OUT nulls boolean[], OUT frequency double precision, OUT base_frequency double precision)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_my_temp_schema()` | exact | exact | refused | refused |
| `pg_nextoid(regclass, name, regclass)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_notification_queue_usage()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_notify(text, text)` | skipped by design (LISTEN, NOTIFY and pg_notify are refused) | skipped by design (LISTEN, NOTIFY and pg_notify are refused) | skipped by design (LISTEN, NOTIFY and pg_notify are refused) | skipped by design (LISTEN, NOTIFY and pg_notify are refused) |
| `pg_opclass_is_visible(oid)` | exact | exact | exact | exact |
| `pg_operator_is_visible(oid)` | exact | exact | exact | exact |
| `pg_opfamily_is_visible(oid)` | exact | exact | exact | exact |
| `pg_options_to_table(options_array text[], OUT option_name text, OUT option_value text)` | exact | exact | refused | refused |
| `pg_partition_ancestors(partitionid regclass, OUT relid regclass)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_partition_root(regclass)` | exact | exact | exact | exact |
| `pg_partition_tree(rootrelid regclass, OUT relid regclass, OUT parentrelid regclass, OUT isleaf boolean, OUT level integer)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_postmaster_start_time()` | exact (type and form only) | exact (type and form only) | refused | refused |
| `pg_prepared_statement(OUT name text, OUT statement text, OUT prepare_time timestamp with time zone, OUT parameter_types regtype[], OUT result_types regtype[], OUT from_sql boolean, OUT generic_plans bigint, OUT custom_plans bigint)` | exact | exact | exact | exact |
| `pg_prepared_xact(OUT transaction xid, OUT gid text, OUT prepared timestamp with time zone, OUT ownerid oid, OUT dbid oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_promote(wait boolean, wait_seconds integer)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_read_binary_file(text)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_read_binary_file(text, bigint, bigint)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_read_binary_file(text, bigint, bigint, boolean)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_read_binary_file(text, boolean)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_read_file(text)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_read_file(text, bigint, bigint)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_read_file(text, bigint, bigint, boolean)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_read_file(text, boolean)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_relation_filenode(regclass)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_relation_filepath(regclass)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_relation_is_publishable(regclass)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_relation_is_updatable(regclass, boolean)` | exact | exact | exact | exact |
| `pg_relation_size(regclass)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_relation_size(regclass, text)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_reload_conf()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_advance(text, pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_create(text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_drop(text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_oid(text)` | exact | exact | exact | exact |
| `pg_replication_origin_progress(text, boolean)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_session_is_setup()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_session_progress(boolean)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_session_reset()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_replication_origin_session_setup(text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_origin_xact_reset()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_replication_slot_advance(slot_name name, upto_lsn pg_lsn, OUT slot_name name, OUT end_lsn pg_lsn)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_rotate_logfile()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_safe_snapshot_blocking_pids(integer)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_sequence_last_value(regclass)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `pg_sequence_parameters(sequence_oid oid, OUT start_value bigint, OUT minimum_value bigint, OUT maximum_value bigint, OUT increment bigint, OUT cycle_option boolean, OUT cache_size bigint, OUT data_type oid)` | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) | skipped by design (Sequence functions are not supported) |
| `pg_settings_get_flags(text)` | exact | exact | exact | exact |
| `pg_show_all_file_settings(OUT sourcefile text, OUT sourceline integer, OUT seqno integer, OUT name text, OUT setting text, OUT applied boolean, OUT error text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_show_all_settings(OUT name text, OUT setting text, OUT unit text, OUT category text, OUT short_desc text, OUT extra_desc text, OUT context text, OUT vartype text, OUT source text, OUT min_val text, OUT max_val text, OUT enumvals text[], OUT boot_val text, OUT reset_val text, OUT sourcefile text, OUT sourceline integer, OUT pending_restart boolean)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn pg_lsn, OUT local_lsn pg_lsn)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_size_bytes(text)` | exact | exact | refused | refused |
| `pg_size_pretty(bigint)` | exact | exact | refused | refused |
| `pg_snapshot_xip(pg_snapshot)` | exact | exact | refused | exact |
| `pg_snapshot_xmax(pg_snapshot)` | exact | exact | refused | exact |
| `pg_snapshot_xmin(pg_snapshot)` | exact | exact | refused | exact |
| `pg_split_walfile_name(file_name text, OUT segment_number numeric, OUT timeline_id bigint)` | exact | exact | refused | refused |
| `pg_stat_clear_snapshot()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_file(filename text, OUT size bigint, OUT access timestamp with time zone, OUT modification timestamp with time zone, OUT change timestamp with time zone, OUT creation timestamp with time zone, OUT isdir boolean)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_stat_file(filename text, missing_ok boolean, OUT size bigint, OUT access timestamp with time zone, OUT modification timestamp with time zone, OUT change timestamp with time zone, OUT creation timestamp with time zone, OUT isdir boolean)` | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) | skipped by design (No function reads the server's files) |
| `pg_stat_force_next_flush()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_get_activity(pid integer, OUT datid oid, OUT pid integer, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT wait_event_type text, OUT wait_event text, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT backend_xid xid, OUT backend_xmin xid, OUT backend_type text, OUT ssl boolean, OUT sslversion text, OUT sslcipher text, OUT sslbits integer, OUT ssl_client_dn text, OUT ssl_client_serial numeric, OUT ssl_issuer_dn text, OUT gss_auth boolean, OUT gss_princ text, OUT gss_enc boolean, OUT gss_delegation boolean, OUT leader_pid integer, OUT query_id bigint)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_analyze_count(oid)` | exact | exact | exact | exact |
| `pg_stat_get_archiver(OUT archived_count bigint, OUT last_archived_wal text, OUT last_archived_time timestamp with time zone, OUT failed_count bigint, OUT last_failed_wal text, OUT last_failed_time timestamp with time zone, OUT stats_reset timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_autoanalyze_count(oid)` | exact | exact | exact | exact |
| `pg_stat_get_autovacuum_count(oid)` | exact | exact | exact | exact |
| `pg_stat_get_backend_activity(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_activity_start(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_client_addr(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_client_port(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_dbid(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_idset()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_pid(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_start(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_subxact(bid integer, OUT subxact_count integer, OUT subxact_overflowed boolean)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_userid(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_wait_event(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_wait_event_type(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_backend_xact_start(integer)` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_stat_get_bgwriter_buf_written_clean()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_bgwriter_maxwritten_clean()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_bgwriter_stat_reset_time()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_blocks_fetched(oid)` | exact | exact | exact | exact |
| `pg_stat_get_blocks_hit(oid)` | exact | exact | exact | exact |
| `pg_stat_get_buf_alloc()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_buffers_written()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_num_requested()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_num_timed()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_restartpoints_performed()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_restartpoints_requested()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_restartpoints_timed()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_stat_reset_time()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_sync_time()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_checkpointer_write_time()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_active_time(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_blk_read_time(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_blk_write_time(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_blocks_fetched(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_blocks_hit(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_checksum_failures(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_db_checksum_last_failure(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_db_conflict_all(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_conflict_bufferpin(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_conflict_lock(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_conflict_logicalslot(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_conflict_snapshot(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_conflict_startup_deadlock(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_conflict_tablespace(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_deadlocks(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_idle_in_transaction_time(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_numbackends(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_session_time(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_sessions(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_sessions_abandoned(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_sessions_fatal(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_sessions_killed(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_stat_reset_time(oid)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_stat_get_db_temp_bytes(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_temp_files(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_tuples_deleted(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_tuples_fetched(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_tuples_inserted(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_tuples_returned(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_tuples_updated(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_xact_commit(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_db_xact_rollback(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_dead_tuples(oid)` | exact | exact | exact | exact |
| `pg_stat_get_function_calls(oid)` | exact | exact | exact | exact |
| `pg_stat_get_function_self_time(oid)` | exact | exact | exact | exact |
| `pg_stat_get_function_total_time(oid)` | exact | exact | exact | exact |
| `pg_stat_get_ins_since_vacuum(oid)` | exact | exact | exact | exact |
| `pg_stat_get_io(OUT backend_type text, OUT object text, OUT context text, OUT reads bigint, OUT read_time double precision, OUT writes bigint, OUT write_time double precision, OUT writebacks bigint, OUT writeback_time double precision, OUT extends bigint, OUT extend_time double precision, OUT op_bytes bigint, OUT hits bigint, OUT evictions bigint, OUT reuses bigint, OUT fsyncs bigint, OUT fsync_time double precision, OUT stats_reset timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_last_analyze_time(oid)` | exact | exact | exact | exact |
| `pg_stat_get_last_autoanalyze_time(oid)` | exact | exact | exact | exact |
| `pg_stat_get_last_autovacuum_time(oid)` | exact | exact | exact | exact |
| `pg_stat_get_last_vacuum_time(oid)` | exact | exact | exact | exact |
| `pg_stat_get_lastscan(oid)` | exact | exact | exact | exact |
| `pg_stat_get_live_tuples(oid)` | exact | exact | exact | exact |
| `pg_stat_get_mod_since_analyze(oid)` | exact | exact | exact | exact |
| `pg_stat_get_numscans(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_get_progress_info(cmdtype text, OUT pid integer, OUT datid oid, OUT relid oid, OUT param1 bigint, OUT param2 bigint, OUT param3 bigint, OUT param4 bigint, OUT param5 bigint, OUT param6 bigint, OUT param7 bigint, OUT param8 bigint, OUT param9 bigint, OUT param10 bigint, OUT param11 bigint, OUT param12 bigint, OUT param13 bigint, OUT param14 bigint, OUT param15 bigint, OUT param16 bigint, OUT param17 bigint, OUT param18 bigint, OUT param19 bigint, OUT param20 bigint)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_recovery_prefetch(OUT stats_reset timestamp with time zone, OUT prefetch bigint, OUT hit bigint, OUT skip_init bigint, OUT skip_new bigint, OUT skip_fpw bigint, OUT skip_rep bigint, OUT wal_distance integer, OUT block_distance integer, OUT io_depth integer)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_replication_slot(slot_name text, OUT slot_name text, OUT spill_txns bigint, OUT spill_count bigint, OUT spill_bytes bigint, OUT stream_txns bigint, OUT stream_count bigint, OUT stream_bytes bigint, OUT total_txns bigint, OUT total_bytes bigint, OUT stats_reset timestamp with time zone)` | exact | exact | exact | exact |
| `pg_stat_get_slru(OUT name text, OUT blks_zeroed bigint, OUT blks_hit bigint, OUT blks_read bigint, OUT blks_written bigint, OUT blks_exists bigint, OUT flushes bigint, OUT truncates bigint, OUT stats_reset timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_snapshot_timestamp()` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_subscription(subid oid, OUT subid oid, OUT relid oid, OUT pid integer, OUT leader_pid integer, OUT received_lsn pg_lsn, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn pg_lsn, OUT latest_end_time timestamp with time zone, OUT worker_type text)` | exact | exact | exact | exact |
| `pg_stat_get_subscription_stats(subid oid, OUT subid oid, OUT apply_error_count bigint, OUT sync_error_count bigint, OUT stats_reset timestamp with time zone)` | exact | exact | exact | exact |
| `pg_stat_get_tuples_deleted(oid)` | exact | exact | exact | exact |
| `pg_stat_get_tuples_fetched(oid)` | exact | exact | exact | exact |
| `pg_stat_get_tuples_hot_updated(oid)` | exact | exact | exact | exact |
| `pg_stat_get_tuples_inserted(oid)` | exact | exact | exact | exact |
| `pg_stat_get_tuples_newpage_updated(oid)` | exact | exact | exact | exact |
| `pg_stat_get_tuples_returned(oid)` | exact | exact | exact | exact |
| `pg_stat_get_tuples_updated(oid)` | exact | exact | exact | exact |
| `pg_stat_get_vacuum_count(oid)` | exact | exact | exact | exact |
| `pg_stat_get_wal(OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT wal_buffers_full bigint, OUT wal_write bigint, OUT wal_sync bigint, OUT wal_write_time double precision, OUT wal_sync_time double precision, OUT stats_reset timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_wal_receiver(OUT pid integer, OUT status text, OUT receive_start_lsn pg_lsn, OUT receive_start_tli integer, OUT written_lsn pg_lsn, OUT flushed_lsn pg_lsn, OUT received_tli integer, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn pg_lsn, OUT latest_end_time timestamp with time zone, OUT slot_name text, OUT sender_host text, OUT sender_port integer, OUT conninfo text)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_wal_senders(OUT pid integer, OUT state text, OUT sent_lsn pg_lsn, OUT write_lsn pg_lsn, OUT flush_lsn pg_lsn, OUT replay_lsn pg_lsn, OUT write_lag interval, OUT flush_lag interval, OUT replay_lag interval, OUT sync_priority integer, OUT sync_state text, OUT reply_time timestamp with time zone)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_blocks_fetched(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_blocks_hit(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_function_calls(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_function_self_time(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_function_total_time(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_numscans(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_tuples_deleted(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_tuples_fetched(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_tuples_hot_updated(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_tuples_inserted(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_tuples_newpage_updated(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_tuples_returned(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_get_xact_tuples_updated(oid)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_stat_have_stats(text, oid, oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_stat_reset()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_reset_replication_slot(text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_reset_shared(target text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_reset_single_function_counters(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_reset_single_table_counters(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_reset_slru(target text)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_stat_reset_subscription_stats(oid)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_statistics_obj_is_visible(oid)` | exact | exact | exact | exact |
| `pg_stop_making_pinned_objects()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_switch_wal()` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_sync_replication_slots()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_table_is_visible(oid)` | exact | exact | exact | exact |
| `pg_table_size(regclass)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_tablespace_databases(oid)` | exact | exact | exact | exact |
| `pg_tablespace_location(oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_tablespace_size(name)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_tablespace_size(oid)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_terminate_backend(pid integer, timeout bigint)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_timezone_abbrevs(OUT abbrev text, OUT utc_offset interval, OUT is_dst boolean)` | exact | exact | refused | refused |
| `pg_timezone_names(OUT name text, OUT abbrev text, OUT utc_offset interval, OUT is_dst boolean)` | exact | equivalent [3](#rule-row-order) [10](#rule-tzdata-packaging) | refused | refused |
| `pg_total_relation_size(regclass)` | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) | skipped by design (Provisa holds no storage) |
| `pg_trigger_depth()` | exact | exact | exact | exact |
| `pg_try_advisory_lock(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_try_advisory_lock(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_try_advisory_lock_shared(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_try_advisory_lock_shared(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_try_advisory_xact_lock(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_try_advisory_xact_lock(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_try_advisory_xact_lock_shared(bigint)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_try_advisory_xact_lock_shared(integer, integer)` | exact (side effect) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `pg_ts_config_is_visible(oid)` | exact | exact | exact | exact |
| `pg_ts_dict_is_visible(oid)` | exact | exact | exact | exact |
| `pg_ts_parser_is_visible(oid)` | exact | exact | exact | exact |
| `pg_ts_template_is_visible(oid)` | exact | exact | exact | exact |
| `pg_type_is_visible(oid)` | exact | exact | exact | exact |
| `pg_typeof("any")` | exact | exact | refused | refused |
| `pg_visible_in_snapshot(xid8, pg_snapshot)` | exact | exact | refused | exact |
| `pg_wal_lsn_diff(pg_lsn, pg_lsn)` | exact | exact | refused | refused |
| `pg_wal_replay_pause()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_wal_replay_resume()` | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) | skipped by design (Server administration functions) |
| `pg_wal_summary_contents(tli bigint, start_lsn pg_lsn, end_lsn pg_lsn, OUT relfilenode oid, OUT reltablespace oid, OUT reldatabase oid, OUT relforknumber smallint, OUT relblocknumber bigint, OUT is_limit_block boolean)` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `pg_walfile_name(pg_lsn)` | exact | exact | refused | refused |
| `pg_walfile_name_offset(lsn pg_lsn, OUT file_name text, OUT file_offset integer)` | exact | exact | refused | refused |
| `pg_xact_commit_timestamp(xid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | refused |
| `pg_xact_commit_timestamp_origin(xid xid, OUT "timestamp" timestamp with time zone, OUT roident oid)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `pg_xact_status(xid8)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | refused | skipped by design (The catalog is the model's) |
| `shobj_description(oid, name)` | exact | exact | exact | exact |
| `to_regclass(text)` | exact | exact | exact | exact |
| `to_regcollation(text)` | exact | exact | exact | exact |
| `to_regnamespace(text)` | exact | exact | exact | exact |
| `to_regoper(text)` | exact | exact | exact | exact |
| `to_regoperator(text)` | exact | exact | exact | exact |
| `to_regproc(text)` | exact | exact | exact | exact |
| `to_regprocedure(text)` | exact | exact | exact | exact |
| `to_regrole(text)` | exact | exact | exact | exact |
| `to_regtype(text)` | exact | exact | exact | exact |
| `to_regtypemod(text)` | exact | exact | exact | exact |
| `txid_current()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `txid_current_if_assigned()` | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) | skipped by design (PostgreSQL's own server state) |
| `txid_current_snapshot()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `txid_snapshot_xip(txid_snapshot)` | exact | exact | refused | exact |
| `txid_snapshot_xmax(txid_snapshot)` | exact | exact | refused | exact |
| `txid_snapshot_xmin(txid_snapshot)` | exact | exact | refused | exact |
| `txid_status(bigint)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `txid_visible_in_snapshot(bigint, txid_snapshot)` | exact | exact | refused | exact |
| `version()` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |

</details>

<details><summary>Text search (87)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `array_to_tsvector(text[])` | exact | exact | exact | exact |
| `get_current_ts_config()` | exact | exact | exact | exact |
| `json_to_tsvector(json, jsonb)` | exact | exact | exact | exact |
| `json_to_tsvector(regconfig, json, jsonb)` | exact | exact | exact | exact |
| `jsonb_to_tsvector(jsonb, jsonb)` | exact | exact | exact | exact |
| `jsonb_to_tsvector(regconfig, jsonb, jsonb)` | exact | exact | exact | exact |
| `length(tsvector)` | exact | exact | exact | refused |
| `numnode(tsquery)` | exact | exact | exact | exact |
| `pg_control_init(OUT max_data_alignment integer, OUT database_block_size integer, OUT blocks_per_segment integer, OUT wal_block_size integer, OUT bytes_per_wal_segment integer, OUT max_identifier_length integer, OUT max_index_columns integer, OUT max_toast_chunk_size integer, OUT large_object_chunk_size integer, OUT float8_pass_by_value boolean, OUT data_page_checksum_version integer)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `phraseto_tsquery(regconfig, text)` | exact | exact | exact | exact |
| `phraseto_tsquery(text)` | exact | exact | exact | exact |
| `plainto_tsquery(regconfig, text)` | exact | exact | exact | exact |
| `plainto_tsquery(text)` | exact | exact | exact | exact |
| `querytree(tsquery)` | exact | exact | exact | exact |
| `setweight(tsvector, "char")` | exact | exact | exact | exact |
| `setweight(tsvector, "char", text[])` | exact | exact | exact | exact |
| `strip(tsvector)` | exact | exact | exact | refused |
| `to_tsquery(regconfig, text)` | exact | exact | exact | exact |
| `to_tsquery(text)` | exact | exact | exact | exact |
| `to_tsvector(json)` | exact | exact | exact | exact |
| `to_tsvector(jsonb)` | exact | exact | exact | exact |
| `to_tsvector(regconfig, json)` | exact | exact | exact | exact |
| `to_tsvector(regconfig, jsonb)` | exact | exact | exact | exact |
| `to_tsvector(regconfig, text)` | exact | exact | exact | exact |
| `to_tsvector(text)` | exact | exact | exact | exact |
| `ts_debug(config regconfig, document text, OUT alias text, OUT description text, OUT token text, OUT dictionaries regdictionary[], OUT dictionary regdictionary, OUT lexemes text[])` | exact | exact | exact | exact |
| `ts_debug(document text, OUT alias text, OUT description text, OUT token text, OUT dictionaries regdictionary[], OUT dictionary regdictionary, OUT lexemes text[])` | exact | exact | equivalent [3](#rule-row-order) | exact |
| `ts_delete(tsvector, text)` | exact | exact | exact | exact |
| `ts_delete(tsvector, text[])` | exact | exact | exact | exact |
| `ts_filter(tsvector, "char"[])` | exact | refused | refused | refused |
| `ts_headline(json, tsquery)` | exact | exact | exact | exact |
| `ts_headline(json, tsquery, text)` | exact | exact | exact | exact |
| `ts_headline(jsonb, tsquery)` | exact | exact | exact | exact |
| `ts_headline(jsonb, tsquery, text)` | exact | exact | exact | exact |
| `ts_headline(regconfig, json, tsquery)` | exact | exact | exact | exact |
| `ts_headline(regconfig, json, tsquery, text)` | exact | exact | exact | exact |
| `ts_headline(regconfig, jsonb, tsquery)` | exact | exact | exact | exact |
| `ts_headline(regconfig, jsonb, tsquery, text)` | exact | exact | exact | exact |
| `ts_headline(regconfig, text, tsquery)` | exact | exact | exact | exact |
| `ts_headline(regconfig, text, tsquery, text)` | exact | refused | refused | refused |
| `ts_headline(text, tsquery)` | exact | exact | exact | exact |
| `ts_headline(text, tsquery, text)` | exact | refused | refused | refused |
| `ts_lexize(regdictionary, text)` | exact | exact | exact | exact |
| `ts_match_qv(tsquery, tsvector)` | exact | exact | exact | exact |
| `ts_match_tq(text, tsquery)` | exact | exact | exact | exact |
| `ts_match_tt(text, text)` | exact | exact | exact | exact |
| `ts_match_vq(tsvector, tsquery)` | exact | exact | exact | exact |
| `ts_parse(parser_name text, txt text, OUT tokid integer, OUT token text)` | exact | exact | equivalent [3](#rule-row-order) | equivalent [3](#rule-row-order) |
| `ts_parse(parser_oid oid, txt text, OUT tokid integer, OUT token text)` | exact | exact | equivalent [3](#rule-row-order) | equivalent [3](#rule-row-order) |
| `ts_rank(real[], tsvector, tsquery)` | exact | refused | refused | refused |
| `ts_rank(real[], tsvector, tsquery, integer)` | exact | refused | refused | refused |
| `ts_rank(tsvector, tsquery)` | exact | exact | exact | exact |
| `ts_rank(tsvector, tsquery, integer)` | exact | refused | refused | refused |
| `ts_rank_cd(real[], tsvector, tsquery)` | exact | refused | refused | refused |
| `ts_rank_cd(real[], tsvector, tsquery, integer)` | exact | refused | refused | refused |
| `ts_rank_cd(tsvector, tsquery)` | exact | refused | refused | refused |
| `ts_rank_cd(tsvector, tsquery, integer)` | exact | refused | refused | refused |
| `ts_rewrite(tsquery, text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `ts_rewrite(tsquery, tsquery, tsquery)` | exact | exact | exact | exact |
| `ts_stat(query text, OUT word text, OUT ndoc integer, OUT nentry integer)` | exact (type and form only) | refused | refused | refused |
| `ts_stat(query text, weights text, OUT word text, OUT ndoc integer, OUT nentry integer)` | exact (type and form only) | refused | refused | refused |
| `tsq_mcontained(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsq_mcontains(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_and(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_cmp(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_eq(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_ge(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_gt(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_le(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_lt(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_ne(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_not(tsquery)` | exact | exact | exact | exact |
| `tsquery_or(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_phrase(tsquery, tsquery)` | exact | exact | exact | exact |
| `tsquery_phrase(tsquery, tsquery, integer)` | exact | exact | exact | exact |
| `tsvector_cmp(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_concat(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_eq(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_ge(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_gt(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_le(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_lt(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_ne(tsvector, tsvector)` | exact | exact | exact | exact |
| `tsvector_to_array(tsvector)` | exact | exact | exact | exact |
| `unnest(tsvector tsvector, OUT lexeme text, OUT positions smallint[], OUT weights text[])` | exact | exact | refused | refused |
| `websearch_to_tsquery(regconfig, text)` | exact | exact | exact | exact |
| `websearch_to_tsquery(text)` | exact | exact | exact | exact |

</details>

<details><summary>Type operators (732)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `aclitemin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `aclitemout(aclitem)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `any_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `any_out("any")` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anyarray_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anyarray_out(anyarray)` | exact | exact | exact | exact |
| `anyarray_send(anyarray)` | exact | exact | exact | exact |
| `anycompatible_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anycompatible_out(anycompatible)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anycompatiblearray_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anycompatiblearray_out(anycompatiblearray)` | exact | exact | exact | exact |
| `anycompatiblearray_send(anycompatiblearray)` | exact | exact | exact | exact |
| `anycompatiblemultirange_in(cstring, oid, integer)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anycompatiblemultirange_out(anycompatiblemultirange)` | exact | exact | exact | exact |
| `anycompatiblenonarray_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anycompatiblenonarray_out(anycompatiblenonarray)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anycompatiblerange_in(cstring, oid, integer)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anycompatiblerange_out(anycompatiblerange)` | exact | exact | exact | exact |
| `anyelement_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anyelement_out(anyelement)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anyenum_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anyenum_out(anyenum)` | exact | exact | refused | refused |
| `anymultirange_in(cstring, oid, integer)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anymultirange_out(anymultirange)` | exact | exact | exact | exact |
| `anynonarray_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anynonarray_out(anynonarray)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anyrange_in(cstring, oid, integer)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `anyrange_out(anyrange)` | exact | exact | exact | exact |
| `array_in(cstring, oid, integer)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `array_out(anyarray)` | exact | exact | exact | exact |
| `array_send(anyarray)` | exact | exact | exact | exact |
| `bit_in(cstring, oid, integer)` | exact | exact | exact | exact |
| `bit_out(bit)` | exact | exact | exact | exact |
| `bit_send(bit)` | exact | exact | exact | exact |
| `bitand(bit, bit)` | exact | exact | refused | refused |
| `bitcmp(bit, bit)` | exact | exact | refused | refused |
| `biteq(bit, bit)` | exact | exact | refused | refused |
| `bitge(bit, bit)` | exact | exact | refused | refused |
| `bitgt(bit, bit)` | exact | exact | refused | refused |
| `bitle(bit, bit)` | exact | exact | refused | refused |
| `bitlt(bit, bit)` | exact | exact | refused | refused |
| `bitne(bit, bit)` | exact | exact | refused | refused |
| `bitnot(bit)` | exact | exact | refused | refused |
| `bitor(bit, bit)` | exact | exact | refused | refused |
| `bittypmodin(cstring[])` | exact | exact | exact | exact |
| `bittypmodout(integer)` | exact | exact | exact | exact |
| `bitxor(bit, bit)` | exact | exact | refused | refused |
| `booleq(boolean, boolean)` | exact | exact | exact | exact |
| `boolge(boolean, boolean)` | exact | exact | exact | exact |
| `boolgt(boolean, boolean)` | exact | exact | exact | exact |
| `boolin(cstring)` | exact | exact | exact | exact |
| `boolle(boolean, boolean)` | exact | exact | exact | exact |
| `boollt(boolean, boolean)` | exact | exact | exact | exact |
| `boolne(boolean, boolean)` | exact | exact | exact | exact |
| `boolout(boolean)` | exact | exact | exact | exact |
| `boolsend(boolean)` | exact | exact | exact | exact |
| `box_in(cstring)` | exact | exact | exact | exact |
| `box_out(box)` | exact | exact | exact | exact |
| `box_send(box)` | exact | exact | exact | exact |
| `bpchar_larger(character, character)` | exact | exact | refused | refused |
| `bpchar_pattern_ge(character, character)` | exact | exact | exact | exact |
| `bpchar_pattern_gt(character, character)` | exact | exact | exact | exact |
| `bpchar_pattern_le(character, character)` | exact | exact | exact | exact |
| `bpchar_pattern_lt(character, character)` | exact | exact | exact | exact |
| `bpchar_smaller(character, character)` | exact | exact | refused | refused |
| `bpcharcmp(character, character)` | exact | exact | refused | refused |
| `bpchareq(character, character)` | exact | exact | exact | exact |
| `bpcharge(character, character)` | exact | exact | refused | refused |
| `bpchargt(character, character)` | exact | exact | refused | refused |
| `bpcharin(cstring, oid, integer)` | exact | exact | exact | exact |
| `bpcharle(character, character)` | exact | exact | refused | refused |
| `bpcharlt(character, character)` | exact | exact | refused | refused |
| `bpcharne(character, character)` | exact | exact | exact | exact |
| `bpcharout(character)` | exact | exact | exact | exact |
| `bpcharsend(character)` | exact | exact | exact | exact |
| `bpchartypmodin(cstring[])` | exact | exact | exact | exact |
| `bpchartypmodout(integer)` | exact | exact | exact | exact |
| `byteacmp(bytea, bytea)` | exact | exact | exact | exact |
| `byteaeq(bytea, bytea)` | exact | exact | exact | exact |
| `byteage(bytea, bytea)` | exact | exact | exact | exact |
| `byteagt(bytea, bytea)` | exact | exact | exact | exact |
| `byteain(cstring)` | exact | exact | exact | exact |
| `byteale(bytea, bytea)` | exact | exact | exact | exact |
| `bytealt(bytea, bytea)` | exact | exact | exact | exact |
| `byteane(bytea, bytea)` | exact | exact | exact | exact |
| `byteaout(bytea)` | exact | exact | exact | exact |
| `byteasend(bytea)` | exact | exact | exact | exact |
| `cash_in(cstring)` | exact | exact | exact | exact |
| `cash_out(money)` | exact | exact | exact | refused |
| `cash_send(money)` | exact | exact | exact | refused |
| `chareq("char", "char")` | exact | exact | exact | exact |
| `charge("char", "char")` | exact | exact | exact | exact |
| `chargt("char", "char")` | exact | exact | exact | exact |
| `charin(cstring)` | exact | exact | exact | exact |
| `charle("char", "char")` | exact | exact | exact | exact |
| `charlt("char", "char")` | exact | exact | exact | exact |
| `charne("char", "char")` | exact | exact | exact | exact |
| `charout("char")` | exact | exact | exact | exact |
| `charsend("char")` | exact | exact | exact | exact |
| `cideq(cid, cid)` | exact | exact | exact | exact |
| `cidin(cstring)` | exact | exact | exact | exact |
| `cidout(cid)` | exact | exact | exact | exact |
| `cidr_in(cstring)` | exact | refused | refused | refused |
| `cidr_out(cidr)` | exact | refused | refused | refused |
| `cidr_send(cidr)` | exact | exact | exact | exact |
| `cidsend(cid)` | exact | exact | exact | exact |
| `circle_in(cstring)` | exact | exact | exact | exact |
| `circle_out(circle)` | exact | exact | exact | exact |
| `circle_send(circle)` | exact | exact | exact | exact |
| `cstring_in(cstring)` | exact | exact | exact | exact |
| `cstring_out(cstring)` | exact | exact | exact | exact |
| `cstring_send(cstring)` | exact | exact | exact | exact |
| `date_bin(interval, timestamp with time zone, timestamp with time zone)` | exact | exact | refused | refused |
| `date_bin(interval, timestamp without time zone, timestamp without time zone)` | exact | exact | refused | refused |
| `date_cmp(date, date)` | exact | exact | exact | exact |
| `date_cmp_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_cmp_timestamptz(date, timestamp with time zone)` | exact | exact | exact | exact |
| `date_eq(date, date)` | exact | exact | exact | exact |
| `date_eq_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_eq_timestamptz(date, timestamp with time zone)` | exact | exact | exact | exact |
| `date_ge(date, date)` | exact | exact | exact | exact |
| `date_ge_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_ge_timestamptz(date, timestamp with time zone)` | exact | exact | exact | exact |
| `date_gt(date, date)` | exact | exact | exact | exact |
| `date_gt_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_gt_timestamptz(date, timestamp with time zone)` | exact | exact | exact | exact |
| `date_in(cstring)` | exact | exact | exact | exact |
| `date_larger(date, date)` | exact | exact | exact | exact |
| `date_le(date, date)` | exact | exact | exact | exact |
| `date_le_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_le_timestamptz(date, timestamp with time zone)` | exact | exact | exact | exact |
| `date_lt(date, date)` | exact | exact | exact | exact |
| `date_lt_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_lt_timestamptz(date, timestamp with time zone)` | exact | exact | exact | exact |
| `date_mi(date, date)` | exact | exact | exact | exact |
| `date_mi_interval(date, interval)` | exact | exact | refused | refused |
| `date_mii(date, integer)` | exact | exact | exact | exact |
| `date_ne(date, date)` | exact | exact | exact | exact |
| `date_ne_timestamp(date, timestamp without time zone)` | exact | exact | exact | exact |
| `date_ne_timestamptz(date, timestamp with time zone)` | exact | exact | exact | exact |
| `date_out(date)` | exact | exact | exact | exact |
| `date_pl_interval(date, interval)` | exact | exact | refused | refused |
| `date_pli(date, integer)` | exact | exact | exact | exact |
| `date_send(date)` | exact | exact | exact | exact |
| `date_smaller(date, date)` | exact | exact | exact | exact |
| `date_subtract(timestamp with time zone, interval)` | exact | exact | refused | refused |
| `date_subtract(timestamp with time zone, interval, text)` | exact | exact | refused | refused |
| `domain_in(cstring, oid, integer)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `enum_in(cstring, oid)` | exact | exact | exact | exact |
| `enum_out(anyenum)` | exact | exact | refused | refused |
| `enum_send(anyenum)` | exact | exact | refused | refused |
| `float4abs(real)` | exact | exact | exact | exact |
| `float4div(real, real)` | exact | exact | exact | exact |
| `float4eq(real, real)` | exact | exact | exact | exact |
| `float4ge(real, real)` | exact | exact | exact | exact |
| `float4gt(real, real)` | exact | exact | exact | exact |
| `float4in(cstring)` | exact | exact | exact | exact |
| `float4larger(real, real)` | exact | exact | exact | exact |
| `float4le(real, real)` | exact | exact | exact | exact |
| `float4lt(real, real)` | exact | exact | exact | exact |
| `float4mi(real, real)` | exact | exact | exact | exact |
| `float4mul(real, real)` | exact | exact | exact | exact |
| `float4ne(real, real)` | exact | exact | exact | exact |
| `float4out(real)` | exact | exact | exact | exact |
| `float4pl(real, real)` | exact | exact | exact | exact |
| `float4send(real)` | exact | exact | exact | exact |
| `float4smaller(real, real)` | exact | exact | exact | exact |
| `float4um(real)` | exact | exact | exact | exact |
| `float4up(real)` | exact | exact | exact | exact |
| `float8_avg(double precision[])` | exact | exact | exact | exact |
| `float8_corr(double precision[])` | exact | exact | exact | exact |
| `float8_covar_pop(double precision[])` | exact | exact | exact | exact |
| `float8_covar_samp(double precision[])` | exact | exact | exact | exact |
| `float8_regr_avgx(double precision[])` | exact | exact | exact | exact |
| `float8_regr_avgy(double precision[])` | exact | exact | exact | exact |
| `float8_regr_intercept(double precision[])` | exact | exact | exact | exact |
| `float8_regr_r2(double precision[])` | exact | exact | exact | exact |
| `float8_regr_slope(double precision[])` | exact | exact | exact | exact |
| `float8_regr_sxx(double precision[])` | exact | exact | exact | exact |
| `float8_regr_sxy(double precision[])` | exact | exact | exact | exact |
| `float8_regr_syy(double precision[])` | exact | exact | exact | exact |
| `float8_stddev_pop(double precision[])` | exact | exact | exact | exact |
| `float8_stddev_samp(double precision[])` | exact | exact | exact | exact |
| `float8_var_pop(double precision[])` | exact | exact | exact | exact |
| `float8_var_samp(double precision[])` | exact | exact | exact | exact |
| `float8abs(double precision)` | exact | exact | exact | exact |
| `float8div(double precision, double precision)` | exact | exact | exact | exact |
| `float8eq(double precision, double precision)` | exact | exact | exact | exact |
| `float8ge(double precision, double precision)` | exact | exact | exact | exact |
| `float8gt(double precision, double precision)` | exact | exact | exact | exact |
| `float8in(cstring)` | exact | exact | exact | exact |
| `float8larger(double precision, double precision)` | exact | exact | exact | exact |
| `float8le(double precision, double precision)` | exact | exact | exact | exact |
| `float8lt(double precision, double precision)` | exact | exact | exact | exact |
| `float8mi(double precision, double precision)` | exact | exact | exact | exact |
| `float8mul(double precision, double precision)` | exact | exact | exact | exact |
| `float8ne(double precision, double precision)` | exact | exact | exact | exact |
| `float8out(double precision)` | exact | exact | exact | exact |
| `float8pl(double precision, double precision)` | exact | exact | exact | exact |
| `float8send(double precision)` | exact | exact | exact | exact |
| `float8smaller(double precision, double precision)` | exact | exact | exact | exact |
| `float8um(double precision)` | exact | exact | exact | exact |
| `float8up(double precision)` | exact | exact | exact | exact |
| `inet_in(cstring)` | exact | refused | refused | refused |
| `inet_merge(inet, inet)` | exact | refused | refused | refused |
| `inet_out(inet)` | exact | refused | refused | refused |
| `inet_same_family(inet, inet)` | exact | exact | exact | exact |
| `inet_send(inet)` | exact | exact | exact | exact |
| `inetand(inet, inet)` | exact | refused | refused | refused |
| `inetmi(inet, inet)` | exact | refused | refused | refused |
| `inetmi_int8(inet, bigint)` | exact | exact | exact | exact |
| `inetnot(inet)` | exact | refused | refused | refused |
| `inetor(inet, inet)` | exact | refused | refused | refused |
| `inetpl(inet, bigint)` | exact | refused | refused | refused |
| `int2_sum(bigint, smallint)` | exact | exact | exact | exact |
| `int2abs(smallint)` | exact | exact | exact | exact |
| `int2and(smallint, smallint)` | exact | exact | exact | exact |
| `int2div(smallint, smallint)` | exact | exact | exact | exact |
| `int2eq(smallint, smallint)` | exact | exact | exact | exact |
| `int2ge(smallint, smallint)` | exact | exact | exact | exact |
| `int2gt(smallint, smallint)` | exact | exact | exact | exact |
| `int2in(cstring)` | exact | exact | exact | exact |
| `int2larger(smallint, smallint)` | exact | exact | exact | exact |
| `int2le(smallint, smallint)` | exact | exact | exact | exact |
| `int2lt(smallint, smallint)` | exact | exact | exact | exact |
| `int2mi(smallint, smallint)` | exact | exact | exact | exact |
| `int2mod(smallint, smallint)` | exact | exact | exact | exact |
| `int2mul(smallint, smallint)` | exact | exact | exact | exact |
| `int2ne(smallint, smallint)` | exact | exact | exact | exact |
| `int2not(smallint)` | exact | exact | exact | exact |
| `int2or(smallint, smallint)` | exact | exact | exact | exact |
| `int2out(smallint)` | exact | exact | exact | exact |
| `int2pl(smallint, smallint)` | exact | exact | exact | exact |
| `int2send(smallint)` | exact | exact | exact | exact |
| `int2shl(smallint, integer)` | exact | exact | exact | exact |
| `int2shr(smallint, integer)` | exact | exact | exact | exact |
| `int2smaller(smallint, smallint)` | exact | exact | exact | exact |
| `int2um(smallint)` | exact | exact | exact | exact |
| `int2up(smallint)` | exact | exact | exact | exact |
| `int2vectorin(cstring)` | exact | exact | exact | exact |
| `int2vectorout(int2vector)` | exact | exact | exact | exact |
| `int2vectorsend(int2vector)` | exact | exact | refused | refused |
| `int2xor(smallint, smallint)` | exact | exact | exact | exact |
| `int4_sum(bigint, integer)` | exact | exact | exact | exact |
| `int4abs(integer)` | exact | exact | exact | exact |
| `int4and(integer, integer)` | exact | exact | exact | exact |
| `int4div(integer, integer)` | exact | exact | exact | exact |
| `int4eq(integer, integer)` | exact | exact | exact | exact |
| `int4ge(integer, integer)` | exact | exact | exact | exact |
| `int4gt(integer, integer)` | exact | exact | exact | exact |
| `int4in(cstring)` | exact | exact | exact | exact |
| `int4inc(integer)` | exact | exact | exact | exact |
| `int4larger(integer, integer)` | exact | exact | exact | exact |
| `int4le(integer, integer)` | exact | exact | exact | exact |
| `int4lt(integer, integer)` | exact | exact | exact | exact |
| `int4mi(integer, integer)` | exact | exact | exact | exact |
| `int4mod(integer, integer)` | exact | exact | exact | exact |
| `int4mul(integer, integer)` | exact | exact | exact | exact |
| `int4ne(integer, integer)` | exact | exact | exact | exact |
| `int4not(integer)` | exact | exact | exact | exact |
| `int4or(integer, integer)` | exact | exact | exact | exact |
| `int4out(integer)` | exact | exact | exact | exact |
| `int4pl(integer, integer)` | exact | exact | exact | exact |
| `int4send(integer)` | exact | exact | exact | exact |
| `int4shl(integer, integer)` | exact | exact | exact | exact |
| `int4shr(integer, integer)` | exact | exact | exact | exact |
| `int4smaller(integer, integer)` | exact | exact | exact | exact |
| `int4um(integer)` | exact | exact | exact | exact |
| `int4up(integer)` | exact | exact | exact | exact |
| `int4xor(integer, integer)` | exact | exact | exact | exact |
| `int8_avg(bigint[])` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `int8_sum(numeric, bigint)` | exact | equivalent [4](#rule-number-value) | equivalent [4](#rule-number-value) | equivalent [4](#rule-number-value) |
| `int8abs(bigint)` | exact | exact | exact | exact |
| `int8and(bigint, bigint)` | exact | exact | exact | exact |
| `int8div(bigint, bigint)` | exact | exact | exact | exact |
| `int8eq(bigint, bigint)` | exact | exact | exact | exact |
| `int8ge(bigint, bigint)` | exact | exact | exact | exact |
| `int8gt(bigint, bigint)` | exact | exact | exact | exact |
| `int8in(cstring)` | exact | exact | exact | exact |
| `int8inc(bigint)` | exact | exact | exact | exact |
| `int8inc_float8_float8(bigint, double precision, double precision)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `int8larger(bigint, bigint)` | exact | exact | exact | exact |
| `int8le(bigint, bigint)` | exact | exact | exact | exact |
| `int8lt(bigint, bigint)` | exact | exact | exact | exact |
| `int8mi(bigint, bigint)` | exact | exact | exact | exact |
| `int8mod(bigint, bigint)` | exact | exact | exact | exact |
| `int8mul(bigint, bigint)` | exact | exact | exact | exact |
| `int8ne(bigint, bigint)` | exact | exact | exact | exact |
| `int8not(bigint)` | exact | exact | exact | exact |
| `int8or(bigint, bigint)` | exact | exact | exact | exact |
| `int8out(bigint)` | exact | exact | exact | exact |
| `int8pl(bigint, bigint)` | exact | exact | exact | exact |
| `int8pl_inet(bigint, inet)` | exact | refused | refused | refused |
| `int8send(bigint)` | exact | exact | exact | exact |
| `int8shl(bigint, integer)` | exact | exact | exact | exact |
| `int8shr(bigint, integer)` | exact | exact | exact | exact |
| `int8smaller(bigint, bigint)` | exact | exact | exact | exact |
| `int8um(bigint)` | exact | exact | exact | exact |
| `int8up(bigint)` | exact | exact | exact | exact |
| `int8xor(bigint, bigint)` | exact | exact | exact | exact |
| `interval_cmp(interval, interval)` | exact | exact | refused | refused |
| `interval_div(interval, double precision)` | exact | exact | refused | refused |
| `interval_eq(interval, interval)` | exact | exact | refused | refused |
| `interval_ge(interval, interval)` | exact | exact | refused | refused |
| `interval_gt(interval, interval)` | exact | exact | refused | refused |
| `interval_hash(interval)` | exact | refused | refused | refused |
| `interval_hash_extended(interval, bigint)` | exact | refused | refused | refused |
| `interval_in(cstring, oid, integer)` | exact | exact | refused | refused |
| `interval_larger(interval, interval)` | exact | exact | refused | refused |
| `interval_le(interval, interval)` | exact | exact | refused | refused |
| `interval_lt(interval, interval)` | exact | exact | refused | refused |
| `interval_mi(interval, interval)` | exact | exact | refused | refused |
| `interval_mul(interval, double precision)` | exact | exact | refused | refused |
| `interval_ne(interval, interval)` | exact | exact | refused | refused |
| `interval_out(interval)` | exact | exact | exact | exact |
| `interval_pl(interval, interval)` | exact | exact | refused | refused |
| `interval_pl_date(interval, date)` | exact | exact | refused | refused |
| `interval_pl_time(interval, time without time zone)` | exact | exact | refused | refused |
| `interval_pl_timestamp(interval, timestamp without time zone)` | exact | exact | refused | refused |
| `interval_pl_timestamptz(interval, timestamp with time zone)` | exact | exact | refused | refused |
| `interval_pl_timetz(interval, time with time zone)` | exact | exact | refused | refused |
| `interval_send(interval)` | exact | exact | exact | exact |
| `interval_smaller(interval, interval)` | exact | exact | refused | refused |
| `interval_um(interval)` | exact | exact | refused | refused |
| `intervaltypmodin(cstring[])` | exact | exact | exact | exact |
| `intervaltypmodout(integer)` | exact | exact | exact | exact |
| `json_in(cstring)` | exact | exact | exact | exact |
| `json_out(json)` | exact | exact | exact | exact |
| `json_send(json)` | exact | exact | exact | exact |
| `jsonb_in(cstring)` | exact | exact | refused | refused |
| `jsonb_out(jsonb)` | exact | exact | exact | exact |
| `jsonb_send(jsonb)` | exact | exact | exact | exact |
| `jsonpath_in(cstring)` | exact | refused | refused | refused |
| `jsonpath_out(jsonpath)` | exact | refused | refused | refused |
| `jsonpath_send(jsonpath)` | exact | refused | refused | refused |
| `line_in(cstring)` | exact | exact | exact | exact |
| `line_out(line)` | exact | exact | exact | exact |
| `line_send(line)` | exact | exact | exact | exact |
| `lseg_in(cstring)` | exact | exact | exact | exact |
| `lseg_out(lseg)` | exact | exact | exact | exact |
| `lseg_send(lseg)` | exact | exact | exact | exact |
| `macaddr8_and(macaddr8, macaddr8)` | exact | exact | refused | exact |
| `macaddr8_cmp(macaddr8, macaddr8)` | exact | exact | exact | exact |
| `macaddr8_eq(macaddr8, macaddr8)` | exact | exact | exact | exact |
| `macaddr8_ge(macaddr8, macaddr8)` | exact | exact | exact | exact |
| `macaddr8_gt(macaddr8, macaddr8)` | exact | exact | exact | exact |
| `macaddr8_in(cstring)` | exact | exact | exact | exact |
| `macaddr8_le(macaddr8, macaddr8)` | exact | exact | exact | exact |
| `macaddr8_lt(macaddr8, macaddr8)` | exact | exact | exact | exact |
| `macaddr8_ne(macaddr8, macaddr8)` | exact | exact | exact | exact |
| `macaddr8_not(macaddr8)` | exact | exact | refused | exact |
| `macaddr8_or(macaddr8, macaddr8)` | exact | exact | refused | exact |
| `macaddr8_out(macaddr8)` | exact | exact | exact | exact |
| `macaddr8_send(macaddr8)` | exact | exact | exact | exact |
| `macaddr8_set7bit(macaddr8)` | exact | exact | refused | exact |
| `macaddr_and(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_cmp(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_eq(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_ge(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_gt(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_in(cstring)` | exact | exact | exact | exact |
| `macaddr_le(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_lt(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_ne(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_not(macaddr)` | exact | exact | exact | exact |
| `macaddr_or(macaddr, macaddr)` | exact | exact | exact | exact |
| `macaddr_out(macaddr)` | exact | exact | exact | exact |
| `macaddr_send(macaddr)` | exact | exact | exact | exact |
| `multirange_in(cstring, oid, integer)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `multirange_out(anymultirange)` | exact | exact | exact | exact |
| `multirange_send(anymultirange)` | exact | exact | exact | exact |
| `nameeq(name, name)` | exact | exact | exact | exact |
| `nameeqtext(name, text)` | exact | exact | exact | exact |
| `namege(name, name)` | exact | exact | exact | exact |
| `namegetext(name, text)` | exact | exact | exact | exact |
| `namegt(name, name)` | exact | exact | exact | exact |
| `namegttext(name, text)` | exact | exact | exact | exact |
| `namein(cstring)` | exact | exact | exact | exact |
| `namele(name, name)` | exact | exact | exact | exact |
| `nameletext(name, text)` | exact | exact | exact | exact |
| `namelt(name, name)` | exact | exact | exact | exact |
| `namelttext(name, text)` | exact | exact | exact | exact |
| `namene(name, name)` | exact | exact | exact | exact |
| `namenetext(name, text)` | exact | exact | exact | exact |
| `nameout(name)` | exact | exact | exact | exact |
| `namesend(name)` | exact | exact | exact | exact |
| `numeric_abs(numeric)` | exact | exact | exact | exact |
| `numeric_add(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_cmp(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_div(numeric, numeric)` | exact | refused | refused | refused |
| `numeric_div_trunc(numeric, numeric)` | exact | refused | refused | refused |
| `numeric_eq(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_exp(numeric)` | exact | refused | refused | refused |
| `numeric_ge(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_gt(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_in(cstring, oid, integer)` | exact | exact | exact | exact |
| `numeric_inc(numeric)` | exact | exact | exact | exact |
| `numeric_larger(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_le(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_ln(numeric)` | exact | refused | refused | refused |
| `numeric_log(numeric, numeric)` | exact | refused | refused | refused |
| `numeric_lt(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_mod(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_mul(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_ne(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_out(numeric)` | exact | refused | refused | refused |
| `numeric_pl_pg_lsn(numeric, pg_lsn)` | exact | exact | refused | refused |
| `numeric_power(numeric, numeric)` | exact | refused | refused | refused |
| `numeric_send(numeric)` | exact | refused | refused | refused |
| `numeric_smaller(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_sqrt(numeric)` | exact | refused | refused | refused |
| `numeric_sub(numeric, numeric)` | exact | exact | exact | exact |
| `numeric_uminus(numeric)` | exact | exact | exact | exact |
| `numeric_uplus(numeric)` | exact | exact | exact | exact |
| `numerictypmodin(cstring[])` | exact | exact | exact | exact |
| `numerictypmodout(integer)` | exact | exact | exact | exact |
| `oideq(oid, oid)` | exact | exact | exact | exact |
| `oidge(oid, oid)` | exact | exact | exact | exact |
| `oidgt(oid, oid)` | exact | exact | exact | exact |
| `oidin(cstring)` | exact | exact | exact | exact |
| `oidlarger(oid, oid)` | exact | exact | exact | exact |
| `oidle(oid, oid)` | exact | exact | exact | exact |
| `oidlt(oid, oid)` | exact | exact | exact | exact |
| `oidne(oid, oid)` | exact | exact | exact | exact |
| `oidout(oid)` | exact | exact | exact | exact |
| `oidsend(oid)` | exact | exact | exact | exact |
| `oidsmaller(oid, oid)` | exact | exact | exact | exact |
| `oidvectoreq(oidvector, oidvector)` | exact | exact | refused | refused |
| `oidvectorge(oidvector, oidvector)` | exact | exact | refused | refused |
| `oidvectorgt(oidvector, oidvector)` | exact | exact | refused | refused |
| `oidvectorin(cstring)` | exact | exact | exact | exact |
| `oidvectorle(oidvector, oidvector)` | exact | exact | refused | refused |
| `oidvectorlt(oidvector, oidvector)` | exact | exact | refused | refused |
| `oidvectorne(oidvector, oidvector)` | exact | exact | refused | refused |
| `oidvectorout(oidvector)` | exact | exact | exact | exact |
| `oidvectorsend(oidvector)` | exact | exact | refused | refused |
| `oidvectortypes(oidvector)` | exact | exact | refused | refused |
| `path_in(cstring)` | exact | exact | exact | exact |
| `path_out(path)` | exact | exact | exact | exact |
| `path_send(path)` | exact | exact | exact | exact |
| `pg_ddl_command_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `pg_ddl_command_out(pg_ddl_command)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `pg_ddl_command_send(pg_ddl_command)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `pg_dependencies_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `pg_dependencies_out(pg_dependencies)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_dependencies_send(pg_dependencies)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_lsn_cmp(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_eq(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_ge(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_gt(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_hash(pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_hash_extended(pg_lsn, bigint)` | exact | exact | exact | exact |
| `pg_lsn_in(cstring)` | exact | exact | exact | exact |
| `pg_lsn_larger(pg_lsn, pg_lsn)` | exact | exact | refused | exact |
| `pg_lsn_le(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_lt(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_mi(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_mii(pg_lsn, numeric)` | exact | exact | refused | refused |
| `pg_lsn_ne(pg_lsn, pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_out(pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_pli(pg_lsn, numeric)` | exact | exact | refused | refused |
| `pg_lsn_send(pg_lsn)` | exact | exact | exact | exact |
| `pg_lsn_smaller(pg_lsn, pg_lsn)` | exact | exact | refused | exact |
| `pg_mcv_list_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `pg_mcv_list_out(pg_mcv_list)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_mcv_list_send(pg_mcv_list)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_ndistinct_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `pg_ndistinct_out(pg_ndistinct)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_ndistinct_send(pg_ndistinct)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_node_tree_in(cstring)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `pg_node_tree_out(pg_node_tree)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_node_tree_send(pg_node_tree)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `pg_snapshot_in(cstring)` | exact | exact | exact | exact |
| `pg_snapshot_out(pg_snapshot)` | exact | exact | exact | exact |
| `pg_snapshot_send(pg_snapshot)` | exact | exact | refused | exact |
| `point_in(cstring)` | exact | exact | exact | exact |
| `point_out(point)` | exact | exact | exact | exact |
| `point_send(point)` | exact | exact | exact | exact |
| `poly_in(cstring)` | exact | exact | exact | exact |
| `poly_out(polygon)` | exact | exact | exact | exact |
| `poly_send(polygon)` | exact | exact | exact | exact |
| `range_in(cstring, oid, integer)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `range_out(anyrange)` | exact | exact | exact | exact |
| `range_send(anyrange)` | exact | exact | exact | exact |
| `record_eq(record, record)` | exact | exact | exact | refused |
| `record_ge(record, record)` | exact | exact | refused | refused |
| `record_gt(record, record)` | exact | exact | refused | refused |
| `record_image_eq(record, record)` | exact | exact | exact | refused |
| `record_image_ge(record, record)` | exact | exact | refused | refused |
| `record_image_gt(record, record)` | exact | exact | refused | refused |
| `record_image_le(record, record)` | exact | exact | refused | refused |
| `record_image_lt(record, record)` | exact | exact | refused | refused |
| `record_image_ne(record, record)` | exact | exact | exact | refused |
| `record_in(cstring, oid, integer)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `record_le(record, record)` | exact | exact | refused | refused |
| `record_lt(record, record)` | exact | exact | refused | refused |
| `record_ne(record, record)` | exact | exact | exact | refused |
| `record_out(record)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `record_send(record)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regclassin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regclassout(regclass)` | exact | exact | exact | exact |
| `regclasssend(regclass)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regcollationin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regcollationout(regcollation)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regcollationsend(regcollation)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regconfigin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regconfigout(regconfig)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regconfigsend(regconfig)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regdictionaryin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regdictionaryout(regdictionary)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regdictionarysend(regdictionary)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regnamespacein(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regnamespaceout(regnamespace)` | exact | exact | exact | exact |
| `regnamespacesend(regnamespace)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regoperatorin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regoperatorout(regoperator)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regoperatorsend(regoperator)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regoperin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regoperout(regoper)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regopersend(regoper)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regprocedurein(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regprocedureout(regprocedure)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regproceduresend(regprocedure)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regprocin(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regprocout(regproc)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regprocsend(regproc)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regrolein(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regroleout(regrole)` | exact | exact | exact | exact |
| `regrolesend(regrole)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regtypein(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `regtypeout(regtype)` | exact | exact | exact | exact |
| `regtypesend(regtype)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `shell_out(void)` | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) | skipped by design (Types no SQL value can have) |
| `text_ge(text, text)` | exact | exact | refused | refused |
| `text_gt(text, text)` | exact | exact | refused | refused |
| `text_larger(text, text)` | exact | exact | refused | refused |
| `text_le(text, text)` | exact | exact | refused | refused |
| `text_lt(text, text)` | exact | exact | refused | refused |
| `text_pattern_ge(text, text)` | exact | exact | exact | exact |
| `text_pattern_gt(text, text)` | exact | exact | exact | exact |
| `text_pattern_le(text, text)` | exact | exact | exact | exact |
| `text_pattern_lt(text, text)` | exact | exact | exact | exact |
| `text_smaller(text, text)` | exact | exact | refused | refused |
| `texteq(text, text)` | exact | exact | exact | exact |
| `texteqname(text, name)` | exact | exact | exact | exact |
| `textgename(text, name)` | exact | exact | exact | exact |
| `textgtname(text, name)` | exact | exact | exact | exact |
| `textin(cstring)` | exact | exact | exact | exact |
| `textlen(text)` | exact | exact | exact | exact |
| `textlename(text, name)` | exact | exact | exact | exact |
| `textltname(text, name)` | exact | exact | exact | exact |
| `textne(text, text)` | exact | exact | exact | exact |
| `textnename(text, name)` | exact | exact | exact | exact |
| `textout(text)` | exact | exact | exact | exact |
| `textsend(text)` | exact | exact | exact | exact |
| `tideq(tid, tid)` | exact | exact | refused | refused |
| `tidge(tid, tid)` | exact | exact | refused | refused |
| `tidgt(tid, tid)` | exact | exact | refused | refused |
| `tidin(cstring)` | exact | exact | exact | exact |
| `tidlarger(tid, tid)` | exact | exact | refused | refused |
| `tidle(tid, tid)` | exact | exact | refused | refused |
| `tidlt(tid, tid)` | exact | exact | refused | refused |
| `tidne(tid, tid)` | exact | exact | refused | refused |
| `tidout(tid)` | exact | exact | exact | exact |
| `tidsend(tid)` | exact | exact | exact | exact |
| `tidsmaller(tid, tid)` | exact | exact | refused | refused |
| `time_cmp(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_eq(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_ge(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_gt(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_hash(time without time zone)` | exact | exact | exact | exact |
| `time_hash_extended(time without time zone, bigint)` | exact | exact | exact | exact |
| `time_in(cstring, oid, integer)` | exact | exact | exact | exact |
| `time_larger(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_le(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_lt(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_mi_interval(time without time zone, interval)` | exact | exact | refused | refused |
| `time_mi_time(time without time zone, time without time zone)` | exact | exact | refused | refused |
| `time_ne(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `time_out(time without time zone)` | exact | exact | exact | exact |
| `time_pl_interval(time without time zone, interval)` | exact | exact | refused | refused |
| `time_send(time without time zone)` | exact | exact | exact | exact |
| `time_smaller(time without time zone, time without time zone)` | exact | exact | exact | exact |
| `timestamp_cmp(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_cmp_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_cmp_timestamptz(timestamp without time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp_eq(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_eq_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_eq_timestamptz(timestamp without time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp_ge(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_ge_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_ge_timestamptz(timestamp without time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp_gt(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_gt_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_gt_timestamptz(timestamp without time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp_hash(timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_hash_extended(timestamp without time zone, bigint)` | exact | exact | exact | exact |
| `timestamp_in(cstring, oid, integer)` | exact | exact | exact | exact |
| `timestamp_larger(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_le(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_le_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_le_timestamptz(timestamp without time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp_lt(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_lt_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_lt_timestamptz(timestamp without time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp_mi(timestamp without time zone, timestamp without time zone)` | exact | exact | refused | refused |
| `timestamp_mi_interval(timestamp without time zone, interval)` | exact | exact | refused | refused |
| `timestamp_ne(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_ne_date(timestamp without time zone, date)` | exact | exact | exact | exact |
| `timestamp_ne_timestamptz(timestamp without time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamp_out(timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_pl_interval(timestamp without time zone, interval)` | exact | exact | refused | refused |
| `timestamp_send(timestamp without time zone)` | exact | exact | exact | exact |
| `timestamp_smaller(timestamp without time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptypmodin(cstring[])` | exact | exact | exact | exact |
| `timestamptypmodout(integer)` | exact | exact | exact | exact |
| `timestamptz_cmp(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_cmp_date(timestamp with time zone, date)` | exact | exact | exact | exact |
| `timestamptz_cmp_timestamp(timestamp with time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptz_eq(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_eq_date(timestamp with time zone, date)` | exact | exact | exact | exact |
| `timestamptz_eq_timestamp(timestamp with time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptz_ge(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_ge_date(timestamp with time zone, date)` | exact | exact | exact | exact |
| `timestamptz_ge_timestamp(timestamp with time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptz_gt(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_gt_date(timestamp with time zone, date)` | exact | exact | exact | exact |
| `timestamptz_gt_timestamp(timestamp with time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptz_in(cstring, oid, integer)` | exact | exact | exact | exact |
| `timestamptz_larger(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_le(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_le_date(timestamp with time zone, date)` | exact | exact | exact | exact |
| `timestamptz_le_timestamp(timestamp with time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptz_lt(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_lt_date(timestamp with time zone, date)` | exact | exact | exact | exact |
| `timestamptz_lt_timestamp(timestamp with time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptz_mi(timestamp with time zone, timestamp with time zone)` | exact | exact | refused | refused |
| `timestamptz_mi_interval(timestamp with time zone, interval)` | exact | exact | refused | refused |
| `timestamptz_ne(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_ne_date(timestamp with time zone, date)` | exact | exact | exact | exact |
| `timestamptz_ne_timestamp(timestamp with time zone, timestamp without time zone)` | exact | exact | exact | exact |
| `timestamptz_out(timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_pl_interval(timestamp with time zone, interval)` | exact | exact | refused | refused |
| `timestamptz_send(timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptz_smaller(timestamp with time zone, timestamp with time zone)` | exact | exact | exact | exact |
| `timestamptztypmodin(cstring[])` | exact | exact | exact | exact |
| `timestamptztypmodout(integer)` | exact | exact | exact | exact |
| `timetypmodin(cstring[])` | exact | exact | exact | exact |
| `timetypmodout(integer)` | exact | exact | exact | exact |
| `timetz_cmp(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_eq(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_ge(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_gt(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_hash(time with time zone)` | exact | exact | exact | exact |
| `timetz_hash_extended(time with time zone, bigint)` | exact | exact | exact | exact |
| `timetz_in(cstring, oid, integer)` | exact | exact | exact | exact |
| `timetz_larger(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_le(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_lt(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_mi_interval(time with time zone, interval)` | exact | exact | refused | refused |
| `timetz_ne(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetz_out(time with time zone)` | exact | exact | exact | exact |
| `timetz_pl_interval(time with time zone, interval)` | exact | exact | refused | refused |
| `timetz_send(time with time zone)` | exact | exact | exact | exact |
| `timetz_smaller(time with time zone, time with time zone)` | exact | exact | refused | refused |
| `timetztypmodin(cstring[])` | exact | exact | exact | exact |
| `timetztypmodout(integer)` | exact | exact | exact | exact |
| `tsqueryin(cstring)` | exact | refused | refused | refused |
| `tsqueryout(tsquery)` | exact | refused | refused | refused |
| `tsquerysend(tsquery)` | exact | refused | refused | refused |
| `tsvectorin(cstring)` | exact | exact | exact | exact |
| `tsvectorout(tsvector)` | exact | exact | exact | exact |
| `tsvectorsend(tsvector)` | exact | exact | exact | exact |
| `txid_snapshot_in(cstring)` | exact | exact | exact | exact |
| `txid_snapshot_out(txid_snapshot)` | exact | exact | exact | exact |
| `txid_snapshot_send(txid_snapshot)` | exact | exact | refused | exact |
| `unknownout(unknown)` | exact | exact | exact | exact |
| `unknownsend(unknown)` | exact | exact | exact | exact |
| `uuid_cmp(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_eq(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_ge(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_gt(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_hash(uuid)` | exact | exact | exact | exact |
| `uuid_hash_extended(uuid, bigint)` | exact | exact | exact | exact |
| `uuid_in(cstring)` | exact | exact | exact | exact |
| `uuid_le(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_lt(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_ne(uuid, uuid)` | exact | exact | exact | exact |
| `uuid_out(uuid)` | exact | exact | exact | exact |
| `uuid_send(uuid)` | exact | exact | exact | exact |
| `varbit_in(cstring, oid, integer)` | exact | exact | exact | exact |
| `varbit_out(bit varying)` | exact | exact | exact | exact |
| `varbit_send(bit varying)` | exact | exact | exact | exact |
| `varbitcmp(bit varying, bit varying)` | exact | exact | refused | refused |
| `varbiteq(bit varying, bit varying)` | exact | exact | refused | refused |
| `varbitge(bit varying, bit varying)` | exact | exact | refused | refused |
| `varbitgt(bit varying, bit varying)` | exact | exact | refused | refused |
| `varbitle(bit varying, bit varying)` | exact | exact | refused | refused |
| `varbitlt(bit varying, bit varying)` | exact | exact | refused | refused |
| `varbitne(bit varying, bit varying)` | exact | exact | refused | refused |
| `varbittypmodin(cstring[])` | exact | exact | exact | exact |
| `varbittypmodout(integer)` | exact | exact | exact | exact |
| `varcharin(cstring, oid, integer)` | exact | exact | exact | exact |
| `varcharout(character varying)` | exact | exact | exact | exact |
| `varcharsend(character varying)` | exact | exact | exact | exact |
| `varchartypmodin(cstring[])` | exact | exact | exact | exact |
| `varchartypmodout(integer)` | exact | exact | exact | exact |
| `void_in(cstring)` | skipped by design (Not testable) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) | skipped by design (Side-effect functions) |
| `void_out(void)` | exact | exact | exact | exact |
| `void_send(void)` | exact | exact | exact | exact |
| `xid8_larger(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8_smaller(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8cmp(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8eq(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8ge(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8gt(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8in(cstring)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `xid8le(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8lt(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8ne(xid8, xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xid8out(xid8)` | exact | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) | skipped by design (Types that exist only inside PostgreSQL) |
| `xid8send(xid8)` | exact | exact | skipped by design (Types that exist only inside PostgreSQL) | exact |
| `xideq(xid, xid)` | exact | exact | exact | exact |
| `xideqint4(xid, integer)` | exact | exact | exact | exact |
| `xidin(cstring)` | exact | exact | exact | exact |
| `xidneq(xid, xid)` | exact | exact | exact | exact |
| `xidneqint4(xid, integer)` | exact | exact | exact | exact |
| `xidout(xid)` | exact | exact | exact | exact |
| `xidsend(xid)` | exact | exact | exact | exact |
| `xml_in(cstring)` | exact | refused | refused | refused |
| `xml_out(xml)` | exact | refused | refused | refused |
| `xml_send(xml)` | exact | exact | refused | refused |

</details>

<details><summary>XML (23)</summary>

| Overload | Postgres | DuckDB | Trino | ClickHouse |
| --- | --- | --- | --- | --- |
| `cursor_to_xml(cursor refcursor, count integer, nulls boolean, tableforest boolean, targetns text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `cursor_to_xmlschema(cursor refcursor, nulls boolean, tableforest boolean, targetns text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `database_to_xml(nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) |
| `database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) |
| `database_to_xmlschema(nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) |
| `query_to_xml(query text, nulls boolean, tableforest boolean, targetns text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `query_to_xml_and_xmlschema(query text, nulls boolean, tableforest boolean, targetns text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `query_to_xmlschema(query text, nulls boolean, tableforest boolean, targetns text)` | exact (type and form only) | exact (type and form only) | exact (type and form only) | exact (type and form only) |
| `schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) |
| `schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) |
| `schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text)` | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) | equivalent [5](#rule-model-answer) |
| `table_to_xml(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `table_to_xml_and_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `table_to_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text)` | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) | skipped by design (The catalog is the model's) |
| `xml(text)` | exact | exact | refused | refused |
| `xml_is_well_formed(text)` | exact | exact | refused | refused |
| `xml_is_well_formed_content(text)` | exact | exact | refused | refused |
| `xml_is_well_formed_document(text)` | exact | exact | refused | refused |
| `xmlagg(xml)` | exact | exact | refused | refused |
| `xmlcomment(text)` | exact | exact | exact | refused |
| `xmlconcat2(xml, xml)` | exact | exact | refused | refused |
| `xmlexists(text, xml)` | exact | exact | refused | refused |
| `xmltext(text)` | exact | exact | exact | exact |

</details>
<!-- function-results:end -->

### Text search

On DuckDB, Trino and ClickHouse the read polyfill for these functions serves constants only. A column argument is refused by name:

- `json_to_tsvector`
- `jsonb_to_tsvector`
- `to_tsvector` of `json` or `jsonb`
- `to_tsquery`
- `websearch_to_tsquery`
- `ts_headline`
- `ts_parse`
- `ts_debug`
- `ts_stat`
- `ts_rewrite`

The read polyfill reads a text column row by row for these: `to_tsvector`, `plainto_tsquery` and `phraseto_tsquery` of `text`, the document of `ts_match_tt` and `ts_match_tq`, and the word of `ts_lexize`. The engine reads the text as PostgreSQL's default parser does, with the `english` or `simple` configuration (the `english_stem` or `simple` dictionary). A text the parser reads in a way these forms do not cover is refused by name, row by row. Such a text holds a dot, an at sign, a slash, an ampersand, a hyphen or an angle bracket outside an email address, URL, host, file path, hyphenated word, number or tag. A configuration or dictionary must be a constant. A column is read as text only where the statement says it is text: a cast such as `body::text`, or a `||` concatenation. Without that, `to_tsvector(body)` is refused, because the `json` overload would answer differently.

Every `tsquery` argument, of any function, must be a constant: a literal such as `'a & b'::tsquery`, or `to_tsquery`, `plainto_tsquery`, `phraseto_tsquery` or `websearch_to_tsquery` of constants. So `ts_rank(to_tsvector(body::text), to_tsquery('english', 'a'))` is served, and a `tsquery` column is refused.

Functions that read a `tsvector`, such as `ts_delete`, `setweight` and `ts_rank`, take a column for the vector.

The `@@` operator is served where the statement says its operands' types: a `tsvector` and a `tsquery` (`ts_match_vq`, `ts_match_qv`), text and a `tsquery` (`ts_match_tq`), or two texts (`ts_match_tt`). An unknown literal takes the type of the other operand, as PostgreSQL resolves it. An operand whose type the statement does not say, such as a bare column, is refused by name.

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
