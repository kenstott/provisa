# Conformance

Provisa checks every protocol and query language it serves against a public test suite. Each suite runs in CI against a test instance after every green build, and each page here reports the results: the suite, its version, the current pass rate, and every deliberate difference with its reason and workaround.

## How conformance is measured

After every green build, CI provisions a fresh test instance and runs the conformance suite against it. Pass rates come directly from that output and are never typed by hand. Each page reflects what the suite last reported.

Provisa fixes failures or lists them. No failure is left unnamed.

## Function results by engine

Each `pg_catalog` function overload has a result on each engine: exact, equivalent under a numbered rule, refused by name, or differs. An engine that refuses a function by name has given that engine's result. A wrong answer on any engine is a defect.

A literal alone does not show that a function works on a column, because a form can fold a literal at transpile time and still refuse a column of the same type. So each overload with a sample value is also called with that value read from a column of a registered table. An overload that passes on literals but not on the column is listed as literals only. The list does not change its result.

<!-- function-results:begin -->
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

Counts are `pg_catalog` overloads against PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2), one row per engine. Each result is that engine's own, and the rows do not add up to a product result. The footnotes for each equivalent result are on the [pgwire page](pgwire.md#function-results-by-engine).

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
<!-- function-results:end -->

## Why differences exist

Deliberate differences group by cause, which also indicates the workaround.

**Writes.** Provisa supports writes that map onto the model through governed table writes. A write needing structure the model lacks is refused with a message naming what is missing. Transaction statements (`BEGIN`, `COMMIT`, `ROLLBACK`) are a no-op by design; the governed pipeline does not expose transaction boundaries to callers.

**Vocabulary fixed by the model.** A new class, property, or relationship type belongs in the model as a table, column, or ontology term. The data store holds instances; vocabulary goes in the model.

**Add-ins.** APOC procedures, GDS algorithms, custom SPARQL functions, and Postgres extensions appear in conformance suites. Where a procedure is commonly needed, Provisa offers it as a command. For others, run it in your own store by querying Provisa live (via `SERVICE` in SPARQL or `apoc.bolt.load` in Neo4j) or by loading the governed graph into it.

## Polyfills

Most engines Provisa serves are not PostgreSQL, so a pgwire client's PostgreSQL behaviour is supplied by a polyfill. A feature's read polyfill computes its values and operations on the engine. Its write polyfill carries values into the engine in literals, parameters, predicates and writes. A polyfill states its limitations, and a feature that cannot be polyfilled is refused by name with a SQLSTATE.

The [polyfill pages](polyfill/index.md) show the grid, one page per engine, generated from the code the translator runs. The conformance runs check it: a feature the grid marks native or polyfilled that answers otherwise than PostgreSQL fails the run, and so does a refusal or difference the grid does not state.

## Tool verification

Each tool entry gives the tool version and the date it was verified, plus how it was verified.

**Covered by its driver.** The open-source driver the tool sits on runs in CI with its own test suite where one exists. Tableau and DBeaver are verified this way through Postgres JDBC/ODBC. Neo4j Browser and Bloom are verified through the Neo4j JavaScript driver. Protégé and Stardog Studio are verified through SPARQL over HTTP.

**Verified by replay.** The real tool is run once against a test instance and its protocol conversation is captured. CI replays that conversation on every subsequent build, so a change breaking what the tool relies on fails the replay. A licence is needed only at capture time.

**Verified manually.** The tool has been run against a test instance and confirmed to work.

## Surfaces

| Surface | Suite | Page |
| --- | --- | --- |
| SPARQL 1.1 | W3C SPARQL 1.1 test suite | [sparql.md](sparql.md) |
| RDF 1.1 syntax | W3C rdf-tests | [rdf-syntax.md](rdf-syntax.md) |
| Cypher | openCypher TCK | [cypher.md](cypher.md) |
| Bolt | Neo4j driver test kit | [bolt.md](bolt.md) |
| GraphQL | graphql-http compliance audit | [graphql.md](graphql.md) |
| pgwire | pgspec v1.5.11 against PostgreSQL 17.11 | [pgwire.md](pgwire.md) |
| pgwire drivers | pgjdbc and psycopg, each its own test suite | [drivers.md](drivers.md) |
| Arrow Flight SQL | ADBC driver validation suite | [flight-sql.md](flight-sql.md) |
| REST / OpenAPI | No public suite runs today | [rest.md](rest.md) |
| JSON:API | No public suite runs today | [jsonapi.md](jsonapi.md) |
| gRPC | No public suite runs today | [grpc.md](grpc.md) |
| MCP | No public suite runs today | [mcp.md](mcp.md) |
