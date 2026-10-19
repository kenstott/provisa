# Features

<!-- Generated from docs/arch/features.yaml and docs/arch/requirements.yaml by scripts/gen_features_doc.py. Do not hand-edit. -->

What Provisa does, feature by feature. The Requirements block under each feature lists the detailed requirements behind each paragraph.

## Model your data

### Source registration

You add a source by naming its type and giving the connection details. Provisa checks the connection before it saves anything, and the source is live without a restart. The same registration path serves the Sources page, the chat assistant and approved requests, so a source behaves the same whichever way it arrived. For a file-based source you can upload the file on the form instead of typing a path.
{ #source-registration-connect }

Connecting a source exposes nothing. A table can be queried only after someone with the right registers it, and an unregistered table is invisible: it does not appear in the schema browser and cannot be named in a query. Registering a table puts it straight into the generated schema, and the schema reflects what the business chose to publish rather than the raw layout of the database. Some sources need extra care at this step. A graph source has no table list to browse, so you supply the query, and a table that takes parameters declares them so callers know what to pass.
{ #source-registration-explicit-tables }

Register Table lists the schemas and tables the source actually offers, using the source's own names for them. Internal namespaces such as engine catalogs or cache schemas never show up. Columns arrive with a data type, including the precision and scale of numbers, and a column is never saved without one. Where the source has a native catalog that tells more, Provisa asks it; otherwise it asks the engine.
{ #source-registration-introspection }

??? note "Requirements"

    - [You add a source by naming its type and giving the connection details.](#source-registration-connect): [REQ-012](arch/requirements.md#REQ-012), [REQ-250](arch/requirements.md#REQ-250), [REQ-952](arch/requirements.md#REQ-952), [REQ-1354](arch/requirements.md#REQ-1354), [REQ-1685](arch/requirements.md#REQ-1685), [REQ-1729](arch/requirements.md#REQ-1729), [REQ-1736](arch/requirements.md#REQ-1736), [REQ-1740](arch/requirements.md#REQ-1740), [REQ-1819](arch/requirements.md#REQ-1819), [REQ-1938](arch/requirements.md#REQ-1938), [REQ-1997](arch/requirements.md#REQ-1997)
    - [Connecting a source exposes nothing.](#source-registration-explicit-tables): [REQ-013](arch/requirements.md#REQ-013), [REQ-014](arch/requirements.md#REQ-014), [REQ-016](arch/requirements.md#REQ-016), [REQ-021](arch/requirements.md#REQ-021), [REQ-464](arch/requirements.md#REQ-464), [REQ-1670](arch/requirements.md#REQ-1670), [REQ-1673](arch/requirements.md#REQ-1673), [REQ-1831](arch/requirements.md#REQ-1831), [REQ-1986](arch/requirements.md#REQ-1986)
    - [Register Table lists the schemas and tables the source actually offers, using the sourc...](#source-registration-introspection): [REQ-252](arch/requirements.md#REQ-252), [REQ-482](arch/requirements.md#REQ-482), [REQ-635](arch/requirements.md#REQ-635), [REQ-636](arch/requirements.md#REQ-636), [REQ-637](arch/requirements.md#REQ-637), [REQ-638](arch/requirements.md#REQ-638), [REQ-639](arch/requirements.md#REQ-639), [REQ-923](arch/requirements.md#REQ-923), [REQ-944](arch/requirements.md#REQ-944), [REQ-1426](arch/requirements.md#REQ-1426), [REQ-1745](arch/requirements.md#REQ-1745), [REQ-1747](arch/requirements.md#REQ-1747), [REQ-1749](arch/requirements.md#REQ-1749), [REQ-1750](arch/requirements.md#REQ-1750), [REQ-1762](arch/requirements.md#REQ-1762), [REQ-1774](arch/requirements.md#REQ-1774), [REQ-1824](arch/requirements.md#REQ-1824), [REQ-1829](arch/requirements.md#REQ-1829), [REQ-1830](arch/requirements.md#REQ-1830), [REQ-1872](arch/requirements.md#REQ-1872), [REQ-1961](arch/requirements.md#REQ-1961), [REQ-2050](arch/requirements.md#REQ-2050)

### Connector catalog

Provisa reads the common relational databases and cloud warehouses: PostgreSQL, MySQL and their wire-compatible relatives, SQL Server, Oracle, Snowflake, BigQuery, Databricks, Athena and others, plus Trino and HiveServer2 endpoints. You register one with host, credentials and database, and the same registration works whichever engine runs underneath. Operators can also declare their own connectors in configuration when a source is not in the list.
{ #connector-catalog-relational-and-warehouses }

Files are sources too. CSV, Parquet, SQLite and DuckDB files, Iceberg and Delta Lake tables, and directories of files matched by a glob all register as tables, with column names mapped to GraphQL-friendly fields. Iceberg and Delta tables can be read as of an earlier point in time. A files source can also crawl web pages, landing HTML tables and linked data files as ordinary tables, which is how Wikipedia becomes a source.
{ #connector-catalog-files-and-lake }

Non-relational stores appear as tables you can join to everything else. MongoDB and Cosmos DB, DynamoDB, Elasticsearch, Redis, Cassandra, Druid, Pinot, Prometheus and Splunk are covered, along with the vector stores Qdrant and S3 Vectors and the graph and triple stores Neo4j and SPARQL endpoints. Each has a mapping from its native shape to columns, so a document or a metric series reads like any other row.
{ #connector-catalog-nosql-search-and-graph }

A remote GraphQL, gRPC or OpenAPI service can be registered as a source. Provisa introspects the schema or spec, turns each readable operation into a virtual table, and turns write operations into commands you register one at a time. The service's authentication, depth limits and relationships to local tables are configured at registration, and a refresh re-reads the spec on demand. Everything the service returns passes through the same row, column and masking rules as local data.
{ #connector-catalog-remote-schemas }

Business applications register as named source types: Salesforce, ServiceNow, SharePoint, Google Workspace, Microsoft 365, Google Sheets and cloud inventory, and branded APIs such as Stripe, Shopify, HubSpot, Jira, Notion, Slack and Airtable built on the generic API sources. Each asks only for what that application needs, such as a login URL and credential, and exposes its objects as tables. Operations to exclude, and write operations to expose, are chosen one at a time.
{ #connector-catalog-saas-and-enterprise }

Event and reference data has its own source types. Kafka topics register with schemas inferred from the topic or a schema registry, log files become tables, and AskAmerica exposes U.S. government open data by subject. Kaggle datasets can be searched and registered, and data-quality checkers and profilers report their results as tables. Node and Python sources let a script supply rows when no connector exists.
{ #connector-catalog-streams-logs-and-public-data }

??? note "Requirements"

    - [Provisa reads the common relational databases and cloud warehouses: PostgreSQL, MySQL a...](#connector-catalog-relational-and-warehouses): [REQ-501](arch/requirements.md#REQ-501), [REQ-502](arch/requirements.md#REQ-502), [REQ-503](arch/requirements.md#REQ-503), [REQ-504](arch/requirements.md#REQ-504), [REQ-950](arch/requirements.md#REQ-950), [REQ-994](arch/requirements.md#REQ-994), [REQ-1177](arch/requirements.md#REQ-1177), [REQ-1731](arch/requirements.md#REQ-1731), [REQ-1737](arch/requirements.md#REQ-1737), [REQ-1753](arch/requirements.md#REQ-1753), [REQ-1755](arch/requirements.md#REQ-1755), [REQ-1757](arch/requirements.md#REQ-1757), [REQ-1881](arch/requirements.md#REQ-1881), [REQ-1981](arch/requirements.md#REQ-1981), [REQ-1998](arch/requirements.md#REQ-1998)
    - [Files are sources too.](#connector-catalog-files-and-lake): [REQ-372](arch/requirements.md#REQ-372), [REQ-499](arch/requirements.md#REQ-499), [REQ-505](arch/requirements.md#REQ-505), [REQ-553](arch/requirements.md#REQ-553), [REQ-736](arch/requirements.md#REQ-736), [REQ-788](arch/requirements.md#REQ-788), [REQ-789](arch/requirements.md#REQ-789), [REQ-790](arch/requirements.md#REQ-790), [REQ-791](arch/requirements.md#REQ-791), [REQ-1650](arch/requirements.md#REQ-1650), [REQ-1726](arch/requirements.md#REQ-1726), [REQ-1738](arch/requirements.md#REQ-1738), [REQ-1748](arch/requirements.md#REQ-1748), [REQ-1780](arch/requirements.md#REQ-1780), [REQ-1781](arch/requirements.md#REQ-1781), [REQ-1784](arch/requirements.md#REQ-1784), [REQ-1785](arch/requirements.md#REQ-1785), [REQ-1786](arch/requirements.md#REQ-1786), [REQ-1787](arch/requirements.md#REQ-1787), [REQ-1788](arch/requirements.md#REQ-1788), [REQ-1960](arch/requirements.md#REQ-1960)
    - [Non-relational stores appear as tables you can join to everything else.](#connector-catalog-nosql-search-and-graph): [REQ-017](arch/requirements.md#REQ-017), [REQ-251](arch/requirements.md#REQ-251), [REQ-295](arch/requirements.md#REQ-295), [REQ-296](arch/requirements.md#REQ-296), [REQ-297](arch/requirements.md#REQ-297), [REQ-298](arch/requirements.md#REQ-298), [REQ-299](arch/requirements.md#REQ-299), [REQ-493](arch/requirements.md#REQ-493), [REQ-494](arch/requirements.md#REQ-494), [REQ-495](arch/requirements.md#REQ-495), [REQ-496](arch/requirements.md#REQ-496), [REQ-497](arch/requirements.md#REQ-497), [REQ-498](arch/requirements.md#REQ-498), [REQ-500](arch/requirements.md#REQ-500), [REQ-512](arch/requirements.md#REQ-512), [REQ-721](arch/requirements.md#REQ-721), [REQ-722](arch/requirements.md#REQ-722), [REQ-723](arch/requirements.md#REQ-723), [REQ-724](arch/requirements.md#REQ-724), [REQ-725](arch/requirements.md#REQ-725), [REQ-735](arch/requirements.md#REQ-735), [REQ-738](arch/requirements.md#REQ-738), [REQ-1668](arch/requirements.md#REQ-1668), [REQ-1672](arch/requirements.md#REQ-1672), [REQ-1675](arch/requirements.md#REQ-1675), [REQ-1683](arch/requirements.md#REQ-1683), [REQ-1689](arch/requirements.md#REQ-1689), [REQ-1980](arch/requirements.md#REQ-1980), [REQ-1982](arch/requirements.md#REQ-1982), [REQ-1983](arch/requirements.md#REQ-1983), [REQ-1985](arch/requirements.md#REQ-1985), [REQ-2017](arch/requirements.md#REQ-2017), [REQ-2018](arch/requirements.md#REQ-2018), [REQ-2019](arch/requirements.md#REQ-2019), [REQ-2020](arch/requirements.md#REQ-2020), [REQ-2021](arch/requirements.md#REQ-2021), [REQ-2022](arch/requirements.md#REQ-2022), [REQ-2023](arch/requirements.md#REQ-2023), [REQ-2024](arch/requirements.md#REQ-2024), [REQ-2025](arch/requirements.md#REQ-2025), [REQ-2026](arch/requirements.md#REQ-2026), [REQ-2027](arch/requirements.md#REQ-2027), [REQ-2028](arch/requirements.md#REQ-2028), [REQ-2029](arch/requirements.md#REQ-2029)
    - [A remote GraphQL, gRPC or OpenAPI service can be registered as a source.](#connector-catalog-remote-schemas): [REQ-307](arch/requirements.md#REQ-307), [REQ-308](arch/requirements.md#REQ-308), [REQ-309](arch/requirements.md#REQ-309), [REQ-310](arch/requirements.md#REQ-310), [REQ-311](arch/requirements.md#REQ-311), [REQ-312](arch/requirements.md#REQ-312), [REQ-313](arch/requirements.md#REQ-313), [REQ-314](arch/requirements.md#REQ-314), [REQ-315](arch/requirements.md#REQ-315), [REQ-316](arch/requirements.md#REQ-316), [REQ-317](arch/requirements.md#REQ-317), [REQ-318](arch/requirements.md#REQ-318), [REQ-319](arch/requirements.md#REQ-319), [REQ-320](arch/requirements.md#REQ-320), [REQ-321](arch/requirements.md#REQ-321), [REQ-322](arch/requirements.md#REQ-322), [REQ-323](arch/requirements.md#REQ-323), [REQ-324](arch/requirements.md#REQ-324), [REQ-325](arch/requirements.md#REQ-325), [REQ-326](arch/requirements.md#REQ-326), [REQ-327](arch/requirements.md#REQ-327), [REQ-328](arch/requirements.md#REQ-328), [REQ-329](arch/requirements.md#REQ-329), [REQ-405](arch/requirements.md#REQ-405), [REQ-406](arch/requirements.md#REQ-406), [REQ-407](arch/requirements.md#REQ-407), [REQ-453](arch/requirements.md#REQ-453), [REQ-454](arch/requirements.md#REQ-454), [REQ-597](arch/requirements.md#REQ-597), [REQ-598](arch/requirements.md#REQ-598), [REQ-599](arch/requirements.md#REQ-599), [REQ-600](arch/requirements.md#REQ-600), [REQ-601](arch/requirements.md#REQ-601), [REQ-602](arch/requirements.md#REQ-602), [REQ-739](arch/requirements.md#REQ-739), [REQ-945](arch/requirements.md#REQ-945), [REQ-1727](arch/requirements.md#REQ-1727), [REQ-1728](arch/requirements.md#REQ-1728), [REQ-1875](arch/requirements.md#REQ-1875)
    - [Business applications register as named source types: Salesforce, ServiceNow, SharePoin...](#connector-catalog-saas-and-enterprise): [REQ-491](arch/requirements.md#REQ-491), [REQ-511](arch/requirements.md#REQ-511), [REQ-726](arch/requirements.md#REQ-726), [REQ-727](arch/requirements.md#REQ-727), [REQ-728](arch/requirements.md#REQ-728), [REQ-729](arch/requirements.md#REQ-729), [REQ-730](arch/requirements.md#REQ-730), [REQ-731](arch/requirements.md#REQ-731), [REQ-732](arch/requirements.md#REQ-732), [REQ-1445](arch/requirements.md#REQ-1445), [REQ-1690](arch/requirements.md#REQ-1690), [REQ-1693](arch/requirements.md#REQ-1693), [REQ-1694](arch/requirements.md#REQ-1694), [REQ-1923](arch/requirements.md#REQ-1923), [REQ-1924](arch/requirements.md#REQ-1924), [REQ-1946](arch/requirements.md#REQ-1946), [REQ-1947](arch/requirements.md#REQ-1947), [REQ-1954](arch/requirements.md#REQ-1954), [REQ-1957](arch/requirements.md#REQ-1957), [REQ-1994](arch/requirements.md#REQ-1994), [REQ-2069](arch/requirements.md#REQ-2069)
    - [Event and reference data has its own source types.](#connector-catalog-streams-logs-and-public-data): [REQ-452](arch/requirements.md#REQ-452), [REQ-492](arch/requirements.md#REQ-492), [REQ-540](arch/requirements.md#REQ-540), [REQ-541](arch/requirements.md#REQ-541), [REQ-1443](arch/requirements.md#REQ-1443), [REQ-1663](arch/requirements.md#REQ-1663), [REQ-1664](arch/requirements.md#REQ-1664), [REQ-1739](arch/requirements.md#REQ-1739), [REQ-1766](arch/requirements.md#REQ-1766), [REQ-1767](arch/requirements.md#REQ-1767), [REQ-1769](arch/requirements.md#REQ-1769), [REQ-1773](arch/requirements.md#REQ-1773), [REQ-1777](arch/requirements.md#REQ-1777), [REQ-1782](arch/requirements.md#REQ-1782), [REQ-1783](arch/requirements.md#REQ-1783), [REQ-1934](arch/requirements.md#REQ-1934), [REQ-1964](arch/requirements.md#REQ-1964), [REQ-1984](arch/requirements.md#REQ-1984), [REQ-1993](arch/requirements.md#REQ-1993), [REQ-1996](arch/requirements.md#REQ-1996)

### Semantic model

Provisa decides every client-facing name in one place. You can alias a table or column, add a description that shows up in the schema, and prefix names with their domain. A naming convention can match Hasura's if you are migrating. Every protocol uses the same names, comparisons go through one normalizer, and nothing is matched by guesswork. A table's SQL address is unique within its domain.
{ #semantic-model-naming }

Registered tables and views are queryable under the user's own rights, with no separate governance mode. You pick which columns to publish, mark primary keys, including composite ones, and declare unique constraints. Columns can pull values out of JSON with a path, and nested fields can be promoted to real columns. Path columns are computed and read-only.
{ #semantic-model-tables-and-columns }

Relationships describe how tables join, and they turn into nested fields in GraphQL and joins elsewhere. Provisa proposes candidates from foreign keys and ranks them by how much evidence backs each one, and a steward confirms or rejects them. Cross-source relationships are declared by hand with a cardinality. Saving a relationship marks the key columns on both sides, and a relationship alias becomes the type name in Cypher.
{ #semantic-model-relationships }

Domains group tables by business area. Each domain has a steward, can nest under a parent, and decides which other domains may read its tables. Views, relationships and grants that cross a domain boundary are approved by the domain that owns the data, and cross-domain access is enforced in GraphQL and in SQL alike. A deployment that does not want domains can turn them off.
{ #semantic-model-domains }

The model is stored once in a model store: sources, tables, columns, relationships, roles, rules and views. Every save goes through a check that shows what the change would affect before it lands. You can also review the whole model as a workbook, not only as JSON.
{ #semantic-model-governed-changes }

The entity relationship diagram opens as a dialog from the Tables and Relationships pages and draws every registered table and its relationships. Tables sit inside a box for their domain, and a domain can be collapsed to hide its tables. A toggle picks how much of each table to show: all columns, only key columns, or none. Hovering a domain, table or column shows its description. The diagram follows the domain filter on the page, draws a relationship backed by a junction table through that table's own node, and exports as SVG, PNG or JSON.
{ #semantic-model-erd }

??? note "Requirements"

    - [Provisa decides every client-facing name in one place.](#semantic-model-naming): [REQ-154](arch/requirements.md#REQ-154), [REQ-155](arch/requirements.md#REQ-155), [REQ-156](arch/requirements.md#REQ-156), [REQ-157](arch/requirements.md#REQ-157), [REQ-194](arch/requirements.md#REQ-194), [REQ-195](arch/requirements.md#REQ-195), [REQ-253](arch/requirements.md#REQ-253), [REQ-388](arch/requirements.md#REQ-388), [REQ-389](arch/requirements.md#REQ-389), [REQ-411](arch/requirements.md#REQ-411), [REQ-412](arch/requirements.md#REQ-412), [REQ-416](arch/requirements.md#REQ-416), [REQ-465](arch/requirements.md#REQ-465), [REQ-466](arch/requirements.md#REQ-466), [REQ-467](arch/requirements.md#REQ-467), [REQ-470](arch/requirements.md#REQ-470), [REQ-471](arch/requirements.md#REQ-471), [REQ-542](arch/requirements.md#REQ-542), [REQ-604](arch/requirements.md#REQ-604), [REQ-640](arch/requirements.md#REQ-640), [REQ-641](arch/requirements.md#REQ-641), [REQ-1484](arch/requirements.md#REQ-1484), [REQ-1933](arch/requirements.md#REQ-1933)
    - [Registered tables and views are queryable under the user's own rights, with no separate...](#semantic-model-tables-and-columns): [REQ-015](arch/requirements.md#REQ-015), [REQ-119](arch/requirements.md#REQ-119), [REQ-151](arch/requirements.md#REQ-151), [REQ-152](arch/requirements.md#REQ-152), [REQ-153](arch/requirements.md#REQ-153), [REQ-221](arch/requirements.md#REQ-221), [REQ-393](arch/requirements.md#REQ-393), [REQ-394](arch/requirements.md#REQ-394), [REQ-395](arch/requirements.md#REQ-395), [REQ-921](arch/requirements.md#REQ-921), [REQ-1093](arch/requirements.md#REQ-1093), [REQ-1361](arch/requirements.md#REQ-1361)
    - [Relationships describe how tables join, and they turn into nested fields in GraphQL and...](#semantic-model-relationships): [REQ-018](arch/requirements.md#REQ-018), [REQ-019](arch/requirements.md#REQ-019), [REQ-020](arch/requirements.md#REQ-020), [REQ-166](arch/requirements.md#REQ-166), [REQ-167](arch/requirements.md#REQ-167), [REQ-399](arch/requirements.md#REQ-399), [REQ-400](arch/requirements.md#REQ-400), [REQ-413](arch/requirements.md#REQ-413), [REQ-612](arch/requirements.md#REQ-612), [REQ-1587](arch/requirements.md#REQ-1587)
    - [Domains group tables by business area.](#semantic-model-domains): [REQ-367](arch/requirements.md#REQ-367), [REQ-432](arch/requirements.md#REQ-432), [REQ-433](arch/requirements.md#REQ-433), [REQ-441](arch/requirements.md#REQ-441), [REQ-443](arch/requirements.md#REQ-443), [REQ-444](arch/requirements.md#REQ-444), [REQ-472](arch/requirements.md#REQ-472), [REQ-473](arch/requirements.md#REQ-473), [REQ-474](arch/requirements.md#REQ-474), [REQ-475](arch/requirements.md#REQ-475), [REQ-476](arch/requirements.md#REQ-476), [REQ-477](arch/requirements.md#REQ-477), [REQ-609](arch/requirements.md#REQ-609), [REQ-610](arch/requirements.md#REQ-610), [REQ-749](arch/requirements.md#REQ-749), [REQ-1363](arch/requirements.md#REQ-1363), [REQ-1917](arch/requirements.md#REQ-1917), [REQ-1978](arch/requirements.md#REQ-1978)
    - [The model is stored once in a model store: sources, tables, columns, relationships, rol...](#semantic-model-governed-changes): [REQ-1592](arch/requirements.md#REQ-1592), [REQ-1918](arch/requirements.md#REQ-1918), [REQ-1919](arch/requirements.md#REQ-1919), [REQ-1969](arch/requirements.md#REQ-1969)
    - [The entity relationship diagram opens as a dialog from the Tables and Relationships pag...](#semantic-model-erd): [REQ-518](arch/requirements.md#REQ-518), [REQ-519](arch/requirements.md#REQ-519), [REQ-520](arch/requirements.md#REQ-520), [REQ-521](arch/requirements.md#REQ-521), [REQ-522](arch/requirements.md#REQ-522), [REQ-523](arch/requirements.md#REQ-523), [REQ-524](arch/requirements.md#REQ-524), [REQ-1588](arch/requirements.md#REQ-1588)

### Views

A view is a computed dataset written in semantic SQL and registered in the model. It gets the same governance as a table: row security, masking, column visibility and role-based schema. Analysts use views to pull cross-domain data into their own domain for reports, and creating one needs approval unless the author already holds the rights to everything underneath.
{ #views-governed-datasets }

Mark a view as materialized and Provisa keeps a periodically refreshed copy. Queries that match the view's pattern are rewritten to read the copy without anyone changing the query.
{ #views-materialization }

A view can be built from metrics, choosing the metrics, dimensions and filters to project. Views, metric views and other definition-backed relations have no physical source of their own, and they are registered without a placeholder one.
{ #views-from-metrics }

??? note "Requirements"

    - [A view is a computed dataset written in semantic SQL and registered in the model.](#views-governed-datasets): [REQ-133](arch/requirements.md#REQ-133), [REQ-134](arch/requirements.md#REQ-134), [REQ-136](arch/requirements.md#REQ-136), [REQ-366](arch/requirements.md#REQ-366), [REQ-418](arch/requirements.md#REQ-418), [REQ-445](arch/requirements.md#REQ-445)
    - [Mark a view as materialized and Provisa keeps a periodically refreshed copy.](#views-materialization): [REQ-135](arch/requirements.md#REQ-135), [REQ-849](arch/requirements.md#REQ-849)
    - [A view can be built from metrics, choosing the metrics, dimensions and filters to project.](#views-from-metrics): [REQ-1318](arch/requirements.md#REQ-1318), [REQ-1328](arch/requirements.md#REQ-1328), [REQ-1329](arch/requirements.md#REQ-1329)

### Metrics and aggregates

Every table can offer an aggregate query and a group-by query in the style Hasura users know. A modeler turns them on per table, and roles are gated on whether they may aggregate at all. Gated tables get these on the gRPC, JSON:API and OpenAPI surfaces too. Expensive aggregates can be served from a materialized copy when one matches. A group-by is ordered by its keys and its aggregates, so a limit returns the top groups, such as the ten slowest pages by average time.
{ #metrics-and-aggregates-aggregates }

A metric is a named, governed aggregate with its own definition. You write it once and every query surface can use it with that surface's native syntax. Fact and entity roles you assign to tables carry through, and the Explore page builds SQL from metrics, facts and dimensions, limited to the domains you have checked. Metrics are managed on their own page with inline editing.
{ #metrics-and-aggregates-metric-objects }

??? note "Requirements"

    - [Every table can offer an aggregate query and a group-by query in the style Hasura users...](#metrics-and-aggregates-aggregates): [REQ-196](arch/requirements.md#REQ-196), [REQ-197](arch/requirements.md#REQ-197), [REQ-198](arch/requirements.md#REQ-198), [REQ-199](arch/requirements.md#REQ-199), [REQ-479](arch/requirements.md#REQ-479), [REQ-653](arch/requirements.md#REQ-653), [REQ-654](arch/requirements.md#REQ-654), [REQ-655](arch/requirements.md#REQ-655), [REQ-733](arch/requirements.md#REQ-733), [REQ-734](arch/requirements.md#REQ-734), [REQ-1359](arch/requirements.md#REQ-1359), [REQ-1405](arch/requirements.md#REQ-1405), [REQ-2007](arch/requirements.md#REQ-2007)
    - [A metric is a named, governed aggregate with its own definition.](#metrics-and-aggregates-metric-objects): [REQ-1317](arch/requirements.md#REQ-1317), [REQ-1319](arch/requirements.md#REQ-1319), [REQ-1320](arch/requirements.md#REQ-1320), [REQ-1322](arch/requirements.md#REQ-1322), [REQ-1323](arch/requirements.md#REQ-1323), [REQ-1324](arch/requirements.md#REQ-1324), [REQ-1723](arch/requirements.md#REQ-1723)

### Glossary, tags and ontology

The business glossary maps plain-language terms to the physical fields behind them. Terms can be derived from column names or written by hand, scoped to one or more domains, related to other terms, and owned by a curator. A term with relationships cannot be deleted. Reading the glossary and administering it are separate rights.
{ #glossary-tags-ontology-terms }

Tags come from one org-wide registry. A tag can apply to sources, tables, columns or relationships, can take a parameter such as the entity a column holds, and is assigned by people rather than automatically. A few system tags have fixed meaning, and the deprecated tag works on any model object. Tags travel with metadata exports and show up in catalog comments.
{ #glossary-tags-ontology-tags }

The glossary and the tag graph are data. Terms, their physical references and relationships, and every tag assignment can be queried like any other table in the meta domain, so a report or an agent can ask what a column means or what is tagged sensitive.
{ #glossary-tags-ontology-queryable-metadata }

??? note "Requirements"

    - [The business glossary maps plain-language terms to the physical fields behind them.](#glossary-tags-ontology-terms): [REQ-1387](arch/requirements.md#REQ-1387), [REQ-1581](arch/requirements.md#REQ-1581), [REQ-1582](arch/requirements.md#REQ-1582), [REQ-1590](arch/requirements.md#REQ-1590), [REQ-1591](arch/requirements.md#REQ-1591), [REQ-1593](arch/requirements.md#REQ-1593), [REQ-1641](arch/requirements.md#REQ-1641), [REQ-1642](arch/requirements.md#REQ-1642), [REQ-1643](arch/requirements.md#REQ-1643), [REQ-1644](arch/requirements.md#REQ-1644), [REQ-1844](arch/requirements.md#REQ-1844)
    - [Tags come from one org-wide registry.](#glossary-tags-ontology-tags): [REQ-1373](arch/requirements.md#REQ-1373), [REQ-1374](arch/requirements.md#REQ-1374), [REQ-1375](arch/requirements.md#REQ-1375), [REQ-1376](arch/requirements.md#REQ-1376), [REQ-1377](arch/requirements.md#REQ-1377), [REQ-1378](arch/requirements.md#REQ-1378), [REQ-1379](arch/requirements.md#REQ-1379), [REQ-1413](arch/requirements.md#REQ-1413), [REQ-1415](arch/requirements.md#REQ-1415), [REQ-1467](arch/requirements.md#REQ-1467), [REQ-1943](arch/requirements.md#REQ-1943), [REQ-1979](arch/requirements.md#REQ-1979)
    - [The glossary and the tag graph are data.](#glossary-tags-ontology-queryable-metadata): [REQ-1414](arch/requirements.md#REQ-1414), [REQ-1584](arch/requirements.md#REQ-1584), [REQ-1585](arch/requirements.md#REQ-1585)

### Catalog and discovery

People find data in layers that get more governed as they go: the source's own schema, the registered model, the semantic names and the data products. Catalog and schema metadata also say which tables support aggregates and group-by.
{ #catalog-and-discovery-discovery }

A data product bundles tables from one domain into something a consumer can browse. Its page lists related tables, the profiles of its tables and a lineage view drawn in lanes by distance from the product's tables. Marking a table as a data product limits what metadata exports publish.
{ #catalog-and-discovery-data-products }

Provisa publishes the model to external catalogs such as OpenMetadata and Atlas. The payload carries tables, columns, domains, stewards, lineage and the governance facts Provisa enforces, with stable provisa:// addresses. Pushes happen on change or on demand from the CLI. A catalog's human enrichment is preserved on update, and columns can be held back from export.
{ #catalog-and-discovery-metadata-export }

Data products can be listed in the catalogs of the warehouse they run on, including Snowflake Horizon and BigQuery Dataplex. Descriptions, data dictionaries, governance tags and the link back to the product's page in Provisa are generated from the model.
{ #catalog-and-discovery-warehouse-listings }

The semantic model can be exported to the standards other tools read: Apache Ossie, the dbt semantic manifest, the Cube model and a dbt sources file.
{ #catalog-and-discovery-model-interchange }

??? note "Requirements"

    - [People find data in layers that get more governed as they go: the source's own schema,...](#catalog-and-discovery-discovery): [REQ-510](arch/requirements.md#REQ-510), [REQ-611](arch/requirements.md#REQ-611), [REQ-1360](arch/requirements.md#REQ-1360)
    - [A data product bundles tables from one domain into something a consumer can browse.](#catalog-and-discovery-data-products): [REQ-1372](arch/requirements.md#REQ-1372), [REQ-1380](arch/requirements.md#REQ-1380), [REQ-1634](arch/requirements.md#REQ-1634), [REQ-1639](arch/requirements.md#REQ-1639), [REQ-1640](arch/requirements.md#REQ-1640), [REQ-1659](arch/requirements.md#REQ-1659), [REQ-1666](arch/requirements.md#REQ-1666), [REQ-1667](arch/requirements.md#REQ-1667), [REQ-1953](arch/requirements.md#REQ-1953)
    - [Provisa publishes the model to external catalogs such as OpenMetadata and Atlas.](#catalog-and-discovery-metadata-export): [REQ-1068](arch/requirements.md#REQ-1068), [REQ-1069](arch/requirements.md#REQ-1069), [REQ-1070](arch/requirements.md#REQ-1070), [REQ-1071](arch/requirements.md#REQ-1071), [REQ-1072](arch/requirements.md#REQ-1072), [REQ-1073](arch/requirements.md#REQ-1073), [REQ-1074](arch/requirements.md#REQ-1074), [REQ-1368](arch/requirements.md#REQ-1368), [REQ-1369](arch/requirements.md#REQ-1369), [REQ-1371](arch/requirements.md#REQ-1371), [REQ-1385](arch/requirements.md#REQ-1385), [REQ-1388](arch/requirements.md#REQ-1388), [REQ-1389](arch/requirements.md#REQ-1389), [REQ-1583](arch/requirements.md#REQ-1583), [REQ-1649](arch/requirements.md#REQ-1649)
    - [Data products can be listed in the catalogs of the warehouse they run on, including Sno...](#catalog-and-discovery-warehouse-listings): [REQ-1635](arch/requirements.md#REQ-1635), [REQ-1636](arch/requirements.md#REQ-1636), [REQ-1645](arch/requirements.md#REQ-1645), [REQ-1646](arch/requirements.md#REQ-1646), [REQ-1647](arch/requirements.md#REQ-1647), [REQ-1648](arch/requirements.md#REQ-1648), [REQ-1656](arch/requirements.md#REQ-1656)
    - [The semantic model can be exported to the standards other tools read: Apache Ossie, the...](#catalog-and-discovery-model-interchange): [REQ-1316](arch/requirements.md#REQ-1316), [REQ-1364](arch/requirements.md#REQ-1364), [REQ-1966](arch/requirements.md#REQ-1966), [REQ-1967](arch/requirements.md#REQ-1967)

### Environments

An organization can run its governed model in more than one environment, such as dev, test and prod. Each environment is a schema of its own. Copying one environment into another carries the model and nothing else; connection settings are set per environment, and write authority is never carried over. Copying into an existing environment is a merge. A credential belongs to exactly one environment, and the browser always says which one it is reading.
{ #environments-isolation }

Changes are saved as immutable builds, so promotion and rollback are exact. You can compare two environments, merge all of one or part of it, and step back and forward along an environment's history. A merge lands as one commit, needs a comment, says when it overwrites someone else's work, and waits for approval if the target is protected. Every operation is also available from the command line.
{ #environments-promotion-and-history }

Each environment has a git repository that mirrors it, one file per entity, so the model lives in version control and can be applied back from files. Provisa pushes to the git host you already use and pulls from it, and your own CI runs the checks. A non-prod environment can also be exported for a developer's desktop.
{ #environments-git-and-ci }

A non-prod environment can own the data it needs. It may hold synthetic datasets generated to match the real shape, have copies of its file-based sources, and keep its own derived stores, all removed when it is. Each environment has one data mode, chosen when you create it. Temporary environments expire and cannot use the administrator's mutation bypass.
{ #environments-test-data }

Authority is scoped to an environment, and the usual way to get it is to create your own. Two rights govern managing environments, and neither comes with an analyst role by default. An invitation can say which environment its redeemer works in, and a membership can be confined to one, which the switcher then shows.
{ #environments-access }

??? note "Requirements"

    - [An organization can run its governed model in more than one environment, such as dev, t...](#environments-isolation): [REQ-1487](arch/requirements.md#REQ-1487), [REQ-1488](arch/requirements.md#REQ-1488), [REQ-1489](arch/requirements.md#REQ-1489), [REQ-1490](arch/requirements.md#REQ-1490), [REQ-1491](arch/requirements.md#REQ-1491), [REQ-1492](arch/requirements.md#REQ-1492), [REQ-1503](arch/requirements.md#REQ-1503), [REQ-1508](arch/requirements.md#REQ-1508), [REQ-1523](arch/requirements.md#REQ-1523), [REQ-1529](arch/requirements.md#REQ-1529), [REQ-1532](arch/requirements.md#REQ-1532), [REQ-1534](arch/requirements.md#REQ-1534), [REQ-1538](arch/requirements.md#REQ-1538), [REQ-1554](arch/requirements.md#REQ-1554)
    - [Changes are saved as immutable builds, so promotion and rollback are exact.](#environments-promotion-and-history): [REQ-1496](arch/requirements.md#REQ-1496), [REQ-1498](arch/requirements.md#REQ-1498), [REQ-1504](arch/requirements.md#REQ-1504), [REQ-1505](arch/requirements.md#REQ-1505), [REQ-1506](arch/requirements.md#REQ-1506), [REQ-1542](arch/requirements.md#REQ-1542), [REQ-1543](arch/requirements.md#REQ-1543), [REQ-1544](arch/requirements.md#REQ-1544), [REQ-1545](arch/requirements.md#REQ-1545), [REQ-1546](arch/requirements.md#REQ-1546), [REQ-1547](arch/requirements.md#REQ-1547), [REQ-1548](arch/requirements.md#REQ-1548), [REQ-1549](arch/requirements.md#REQ-1549), [REQ-1550](arch/requirements.md#REQ-1550), [REQ-1551](arch/requirements.md#REQ-1551), [REQ-1552](arch/requirements.md#REQ-1552), [REQ-1553](arch/requirements.md#REQ-1553), [REQ-1555](arch/requirements.md#REQ-1555), [REQ-1556](arch/requirements.md#REQ-1556), [REQ-1561](arch/requirements.md#REQ-1561)
    - [Each environment has a git repository that mirrors it, one file per entity, so the mode...](#environments-git-and-ci): [REQ-1499](arch/requirements.md#REQ-1499), [REQ-1500](arch/requirements.md#REQ-1500), [REQ-1501](arch/requirements.md#REQ-1501), [REQ-1502](arch/requirements.md#REQ-1502), [REQ-1524](arch/requirements.md#REQ-1524), [REQ-1525](arch/requirements.md#REQ-1525), [REQ-1526](arch/requirements.md#REQ-1526), [REQ-1527](arch/requirements.md#REQ-1527), [REQ-1533](arch/requirements.md#REQ-1533), [REQ-1537](arch/requirements.md#REQ-1537), [REQ-1541](arch/requirements.md#REQ-1541)
    - [A non-prod environment can own the data it needs.](#environments-test-data): [REQ-1493](arch/requirements.md#REQ-1493), [REQ-1495](arch/requirements.md#REQ-1495), [REQ-1620](arch/requirements.md#REQ-1620), [REQ-1621](arch/requirements.md#REQ-1621), [REQ-1622](arch/requirements.md#REQ-1622), [REQ-1623](arch/requirements.md#REQ-1623), [REQ-1624](arch/requirements.md#REQ-1624), [REQ-1939](arch/requirements.md#REQ-1939), [REQ-1942](arch/requirements.md#REQ-1942)
    - [Authority is scoped to an environment, and the usual way to get it is to create your own.](#environments-access): [REQ-1528](arch/requirements.md#REQ-1528), [REQ-1539](arch/requirements.md#REQ-1539), [REQ-1573](arch/requirements.md#REQ-1573), [REQ-1595](arch/requirements.md#REQ-1595), [REQ-1596](arch/requirements.md#REQ-1596), [REQ-1606](arch/requirements.md#REQ-1606), [REQ-1617](arch/requirements.md#REQ-1617)

### Regions and data residency

A deployment can run clusters in several regions off one shared model. The model is declared once; each cluster has its own nodes, engine, replica storage, cache and record, and serves clients that connect to its address. What a reader sees follows the model's rules, wherever they connect, and each audit record names the region whose data answered.
{ #regions-and-residency-clusters }

Residency is a short list of rules. Row and column rules written on user attributes and on the region of the node answering decide what each reader gets and what crosses between regions. A table can name a home region, and only that region builds and refreshes its replica. Readers elsewhere read that replica in place, with their own rules applied in the home region, so only what they may see leaves it. When the home region cannot serve, the read is refused rather than answered from the source live somewhere else. Setting or moving a table's home region needs the data-residency right for the regions involved.
{ #regions-and-residency-residency-rules }

??? note "Requirements"

    - [A deployment can run clusters in several regions off one shared model.](#regions-and-residency-clusters): [REQ-1922](arch/requirements.md#REQ-1922)
    - [Residency is a short list of rules.](#regions-and-residency-residency-rules): [REQ-1921](arch/requirements.md#REQ-1921)

### Commands and functions

Stored procedures and functions in a source can be tracked and exposed as GraphQL mutations or queries. A function returns a registered table or declares its own result shape in JSON Schema. Result rows resolve nested relationships and run directly against the source's connection. Discovered routines are offered for registration, never added automatically.
{ #commands-and-functions-tracked-functions }

An HTTP endpoint can be registered as a command and called like a mutation or query. Its arguments and return type are declared inline, and it is exposed only after a steward approves it.
{ #commands-and-functions-webhooks }

A command can also be implemented by a Provisa SQL statement with typed arguments, including MongoDB aggregation pipelines. Commands can be called inline within a larger SQL statement, take bind parameters and column references as arguments, and appear in every protocol that lists registered commands.
{ #commands-and-functions-sql-commands }

The Commands page lists everything registered, grouped by type, with a form to add one and a Test button that runs it with sample arguments and shows the result with the governance applied.
{ #commands-and-functions-command-admin }

Scheduled triggers run a registered webhook or function on a cron or interval schedule.
{ #commands-and-functions-scheduled-triggers }

??? note "Requirements"

    - [Stored procedures and functions in a source can be tracked and exposed as GraphQL mutat...](#commands-and-functions-tracked-functions): [REQ-205](arch/requirements.md#REQ-205), [REQ-206](arch/requirements.md#REQ-206), [REQ-207](arch/requirements.md#REQ-207), [REQ-208](arch/requirements.md#REQ-208), [REQ-304](arch/requirements.md#REQ-304), [REQ-305](arch/requirements.md#REQ-305), [REQ-306](arch/requirements.md#REQ-306), [REQ-360](arch/requirements.md#REQ-360), [REQ-361](arch/requirements.md#REQ-361), [REQ-362](arch/requirements.md#REQ-362), [REQ-872](arch/requirements.md#REQ-872), [REQ-885](arch/requirements.md#REQ-885), [REQ-887](arch/requirements.md#REQ-887)
    - [An HTTP endpoint can be registered as a command and called like a mutation or query.](#commands-and-functions-webhooks): [REQ-209](arch/requirements.md#REQ-209), [REQ-210](arch/requirements.md#REQ-210), [REQ-211](arch/requirements.md#REQ-211), [REQ-480](arch/requirements.md#REQ-480), [REQ-1172](arch/requirements.md#REQ-1172)
    - [A command can also be implemented by a Provisa SQL statement with typed arguments, incl...](#commands-and-functions-sql-commands): [REQ-998](arch/requirements.md#REQ-998), [REQ-999](arch/requirements.md#REQ-999), [REQ-1000](arch/requirements.md#REQ-1000), [REQ-1001](arch/requirements.md#REQ-1001), [REQ-1156](arch/requirements.md#REQ-1156), [REQ-1159](arch/requirements.md#REQ-1159), [REQ-1173](arch/requirements.md#REQ-1173), [REQ-1928](arch/requirements.md#REQ-1928), [REQ-1988](arch/requirements.md#REQ-1988), [REQ-2001](arch/requirements.md#REQ-2001)
    - [The Commands page lists everything registered, grouped by type, with a form to add one...](#commands-and-functions-command-admin): [REQ-242](arch/requirements.md#REQ-242), [REQ-243](arch/requirements.md#REQ-243), [REQ-244](arch/requirements.md#REQ-244), [REQ-245](arch/requirements.md#REQ-245)
    - [Scheduled triggers run a registered webhook or function on a cron or interval schedule.](#commands-and-functions-scheduled-triggers): [REQ-216](arch/requirements.md#REQ-216), [REQ-1003](arch/requirements.md#REQ-1003), [REQ-1004](arch/requirements.md#REQ-1004)

### Writes

Mutations write directly to the one source that owns the table; the engine is never involved. A user needs write rights to the table, and mutation inputs show only the columns the role may write. Row security applies to updates and deletes, and only registered tables get mutations. Transactions across sources are not supported, and non-relational sources are read-only. Upserts, column presets and batched mutations are available.
{ #writes-governed-mutations }

Writes take the best available path for the source: a native driver, then the engine, then a fallback. Every write on every surface passes through one admission step.
{ #writes-write-routing }

Mutations are classified as writes by contract, not by what the caller says. A global write capability and per-mutation grants decide who may run them, and only an admin can reclassify one as read-safe. At registration, Provisa suggests which table a mutation belongs to.
{ #writes-classification }

??? note "Requirements"

    - [Mutations write directly to the one source that owns the table; the engine is never inv...](#writes-governed-mutations): [REQ-031](arch/requirements.md#REQ-031), [REQ-032](arch/requirements.md#REQ-032), [REQ-033](arch/requirements.md#REQ-033), [REQ-034](arch/requirements.md#REQ-034), [REQ-035](arch/requirements.md#REQ-035), [REQ-036](arch/requirements.md#REQ-036), [REQ-037](arch/requirements.md#REQ-037), [REQ-212](arch/requirements.md#REQ-212), [REQ-214](arch/requirements.md#REQ-214), [REQ-217](arch/requirements.md#REQ-217)
    - [Writes take the best available path for the source: a native driver, then the engine, t...](#writes-write-routing): [REQ-910](arch/requirements.md#REQ-910), [REQ-1925](arch/requirements.md#REQ-1925), [REQ-2074](arch/requirements.md#REQ-2074)
    - [Mutations are classified as writes by contract, not by what the caller says.](#writes-classification): [REQ-867](arch/requirements.md#REQ-867), [REQ-868](arch/requirements.md#REQ-868), [REQ-869](arch/requirements.md#REQ-869), [REQ-870](arch/requirements.md#REQ-870), [REQ-871](arch/requirements.md#REQ-871)

## Govern access

### Roles and capabilities

What a person can do is spelled out as capabilities, and each capability is assigned on its own. Registering sources, defining relationships, writing security rules and administering the organization are separate rights, so an admin builds a role from the pieces it needs. Every gate in the server and the UI tests a capability, never a role name, and every admin handler sits behind one. A role grant can carry an expiry, and a role can remove grants that have gone unused for a set number of days; each removal is recorded, and where mail is configured the holder and the admins are told before a grant ends.
{ #roles-and-capabilities-capabilities }

A role can inherit from one parent, and the child gets everything the parent holds. A request can act as several of the caller's roles at once: their rights are combined, each row is read under the rules of the roles that admitted it, and the audit names every role that acted. Roles can also come from a user's attributes. A declared attribute such as region gives a user the role region__eu, and a grant can require it alongside another role, so "an analyst in the EU sees this column unmasked" is an ordinary grant. Every role carries a description of what it is for, so an access review can read why it exists.
{ #roles-and-capabilities-inheritance }

Domains are permission boundaries. A role reads freely inside the domains it is granted and needs an explicit grant for any other. Even the built-in operations domain follows this rule, and a seeded data steward role owns the governance of a domain.
{ #roles-and-capabilities-domain-access }

Platform administration and organization administration are separate roles. The platform admin runs the control plane and holds no data capabilities. An org admin runs one organization, and nobody can change their own role. The deployment can never be left without a platform admin, and any recovery that crosses into an org is explicit and recorded in that org's audit trail.
{ #roles-and-capabilities-platform-admins }

The sandbox role lets a visitor see how an admin surface looks without being able to change anything. It shows read-only views of normally restricted pages and data, and it can display a control the role does not hold as an inert, visibly disabled one. It is meant for demos and evaluation.
{ #roles-and-capabilities-sandbox-role }

A join is allowed only where a relationship has been registered and approved. When the guard refuses a join, it names the valid paths between the two tables, computed in one place so the refusal, the API and agents all agree. A role with the ignore-relationships capability can join tables that have no registered relationship.
{ #roles-and-capabilities-join-guard }

??? note "Requirements"

    - [What a person can do is spelled out as capabilities, and each capability is assigned on...](#roles-and-capabilities-capabilities): [REQ-002](arch/requirements.md#REQ-002), [REQ-003](arch/requirements.md#REQ-003), [REQ-042](arch/requirements.md#REQ-042), [REQ-059](arch/requirements.md#REQ-059), [REQ-455](arch/requirements.md#REQ-455), [REQ-746](arch/requirements.md#REQ-746), [REQ-1337](arch/requirements.md#REQ-1337), [REQ-1349](arch/requirements.md#REQ-1349), [REQ-1638](arch/requirements.md#REQ-1638), [REQ-1927](arch/requirements.md#REQ-1927), [REQ-1952](arch/requirements.md#REQ-1952), [REQ-2057](arch/requirements.md#REQ-2057), [REQ-2060](arch/requirements.md#REQ-2060), [REQ-2077](arch/requirements.md#REQ-2077), [REQ-2078](arch/requirements.md#REQ-2078), [REQ-2079](arch/requirements.md#REQ-2079), [REQ-2080](arch/requirements.md#REQ-2080)
    - [A role can inherit from one parent, and the child gets everything the parent holds.](#roles-and-capabilities-inheritance): [REQ-215](arch/requirements.md#REQ-215), [REQ-1677](arch/requirements.md#REQ-1677), [REQ-1807](arch/requirements.md#REQ-1807)
    - [Domains are permission boundaries.](#roles-and-capabilities-domain-access): [REQ-437](arch/requirements.md#REQ-437), [REQ-440](arch/requirements.md#REQ-440), [REQ-1133](arch/requirements.md#REQ-1133), [REQ-1530](arch/requirements.md#REQ-1530), [REQ-1531](arch/requirements.md#REQ-1531), [REQ-1944](arch/requirements.md#REQ-1944)
    - [Platform administration and organization administration are separate roles.](#roles-and-capabilities-platform-admins): [REQ-517](arch/requirements.md#REQ-517), [REQ-1299](arch/requirements.md#REQ-1299), [REQ-1303](arch/requirements.md#REQ-1303), [REQ-1308](arch/requirements.md#REQ-1308), [REQ-1327](arch/requirements.md#REQ-1327), [REQ-1340](arch/requirements.md#REQ-1340), [REQ-1352](arch/requirements.md#REQ-1352)
    - [The sandbox role lets a visitor see how an admin surface looks without being able to ch...](#roles-and-capabilities-sandbox-role): [REQ-1297](arch/requirements.md#REQ-1297), [REQ-1597](arch/requirements.md#REQ-1597), [REQ-1602](arch/requirements.md#REQ-1602), [REQ-1608](arch/requirements.md#REQ-1608), [REQ-1609](arch/requirements.md#REQ-1609), [REQ-1610](arch/requirements.md#REQ-1610), [REQ-1611](arch/requirements.md#REQ-1611), [REQ-1612](arch/requirements.md#REQ-1612), [REQ-1613](arch/requirements.md#REQ-1613), [REQ-1618](arch/requirements.md#REQ-1618)
    - [A join is allowed only where a relationship has been registered and approved.](#roles-and-capabilities-join-guard): [REQ-463](arch/requirements.md#REQ-463), [REQ-603](arch/requirements.md#REQ-603), [REQ-747](arch/requirements.md#REQ-747), [REQ-1955](arch/requirements.md#REQ-1955), [REQ-1956](arch/requirements.md#REQ-1956)

### Row-level security, column visibility and masking

Access is enforced twice. Schema visibility hides tables and columns a role may not see, so they never appear in the schema, the query builder or the admin API's listings. Then the SQL layer filters rows and strips columns from every query before it runs, on every surface. No code path returns rows without a role bound to them. A request acts only as roles its caller holds: naming one it does not hold is refused, recorded as a high-severity security event and, for a token-based client, suspends that token until an administrator lifts it. An AI agent over MCP is held to the roles it was given.
{ #row-security-and-masking-two-layers }

Row filters are written as SQL predicates and attached to a table, a whole domain or a tag. They are checked when saved, so a bad rule fails at once instead of at the first query. Predicates can use attributes of the signed-in user, taken from the identity provider or set locally, constants the role carries, and the region of the node answering; those values take the type of the column they are compared with. A role constant can be a reference to an environment variable or a secret, resolved when a query uses it.
{ #row-security-and-masking-row-rules }

A column can be masked per role. The options are a regular-expression replace on text, a constant, a truncated date, or a consistent fake value, and the same real value always shows the same fake. The raw value never leaves the database. Masks respect column types, only touch the selected output, and leave filters and joins working on the real value. A masked column cannot be used to filter results. A column carrying a sensitive tag, such as the built-in pii tag, has its masks, fakes and grants changed only by a holder of the sensitive-data right, in every environment including production.
{ #row-security-and-masking-masking }

The metadata about the model is governed like any other data. Core facts about tables and columns are open, while the governance columns, such as who may see or unmask a column, need a dedicated capability. Admins see every column.
{ #row-security-and-masking-metadata-visibility }

??? note "Requirements"

    - [Access is enforced twice.](#row-security-and-masking-two-layers): [REQ-001](arch/requirements.md#REQ-001), [REQ-038](arch/requirements.md#REQ-038), [REQ-039](arch/requirements.md#REQ-039), [REQ-040](arch/requirements.md#REQ-040), [REQ-263](arch/requirements.md#REQ-263), [REQ-438](arch/requirements.md#REQ-438), [REQ-439](arch/requirements.md#REQ-439), [REQ-1131](arch/requirements.md#REQ-1131), [REQ-1758](arch/requirements.md#REQ-1758), [REQ-1760](arch/requirements.md#REQ-1760), [REQ-1778](arch/requirements.md#REQ-1778), [REQ-1958](arch/requirements.md#REQ-1958)
    - [Row filters are written as SQL predicates and attached to a table, a whole domain or a...](#row-security-and-masking-row-rules): [REQ-041](arch/requirements.md#REQ-041), [REQ-402](arch/requirements.md#REQ-402), [REQ-403](arch/requirements.md#REQ-403), [REQ-404](arch/requirements.md#REQ-404), [REQ-1381](arch/requirements.md#REQ-1381), [REQ-1382](arch/requirements.md#REQ-1382), [REQ-1383](arch/requirements.md#REQ-1383), [REQ-1384](arch/requirements.md#REQ-1384), [REQ-1614](arch/requirements.md#REQ-1614), [REQ-1676](arch/requirements.md#REQ-1676), [REQ-1679](arch/requirements.md#REQ-1679), [REQ-1682](arch/requirements.md#REQ-1682), [REQ-1686](arch/requirements.md#REQ-1686), [REQ-2086](arch/requirements.md#REQ-2086)
    - [A column can be masked per role.](#row-security-and-masking-masking): [REQ-087](arch/requirements.md#REQ-087), [REQ-088](arch/requirements.md#REQ-088), [REQ-089](arch/requirements.md#REQ-089), [REQ-090](arch/requirements.md#REQ-090), [REQ-091](arch/requirements.md#REQ-091), [REQ-249](arch/requirements.md#REQ-249), [REQ-531](arch/requirements.md#REQ-531), [REQ-740](arch/requirements.md#REQ-740), [REQ-741](arch/requirements.md#REQ-741), [REQ-742](arch/requirements.md#REQ-742), [REQ-743](arch/requirements.md#REQ-743), [REQ-744](arch/requirements.md#REQ-744), [REQ-745](arch/requirements.md#REQ-745), [REQ-971](arch/requirements.md#REQ-971), [REQ-1494](arch/requirements.md#REQ-1494), [REQ-2058](arch/requirements.md#REQ-2058), [REQ-2059](arch/requirements.md#REQ-2059), [REQ-2081](arch/requirements.md#REQ-2081)
    - [The metadata about the model is governed like any other data.](#row-security-and-masking-metadata-visibility): [REQ-1132](arch/requirements.md#REQ-1132), [REQ-1134](arch/requirements.md#REQ-1134)

### Approvals and query limits

For rules too complex to write as row filters, Provisa calls out to an approval hook before a query runs. The hook can be switched on for a table, a source or the whole deployment, and it speaks over a webhook, gRPC or a Unix socket. A circuit breaker protects queries when the hook is unhealthy, and the gRPC channel is reused across calls.
{ #approvals-and-limits-approval-hook }

Each role has its own request-rate limit, kept in Redis so any number of stateless containers share one count, along with caps on concurrent subscriptions and Flight streams. A stream over its cap waits for a free slot. Natural-language queries have a separate limit because they cost LLM calls.
{ #approvals-and-limits-rate-limits }

Roles carry ceilings on how long a query may run and how many rows it may return. Roles without the right to full results get a sample, and when a read is cut short the response says so. A role, or the whole organization, can also cap how much one statement may ask for: its score counts the relations, joins, columns and nested queries, with a read from a remote API counting ten. A statement over the cap is refused before it runs, on every interface. Each organization tier also has hard ceilings on query cost, and a query over the ceiling is refused with a message naming the tier it would need.
{ #approvals-and-limits-query-limits }

Creating a relationship, view, table, source or webhook goes through an approval. The approver must hold the matching right, must not be the requester, and for a relationship must have reach over one of the two domains it touches. When a registration change affects an entry, that entry is flagged for review.
{ #approvals-and-limits-creation-approvals }

??? note "Requirements"

    - [For rules too complex to write as row filters, Provisa calls out to an approval hook be...](#approvals-and-limits-approval-hook): [REQ-203](arch/requirements.md#REQ-203), [REQ-204](arch/requirements.md#REQ-204), [REQ-246](arch/requirements.md#REQ-246), [REQ-247](arch/requirements.md#REQ-247), [REQ-555](arch/requirements.md#REQ-555), [REQ-556](arch/requirements.md#REQ-556)
    - [Each role has its own request-rate limit, kept in Redis so any number of stateless cont...](#approvals-and-limits-rate-limits): [REQ-369](arch/requirements.md#REQ-369), [REQ-370](arch/requirements.md#REQ-370), [REQ-371](arch/requirements.md#REQ-371), [REQ-1905](arch/requirements.md#REQ-1905)
    - [Roles carry ceilings on how long a query may run and how many rows it may return.](#approvals-and-limits-query-limits): [REQ-005](arch/requirements.md#REQ-005), [REQ-554](arch/requirements.md#REQ-554), [REQ-1044](arch/requirements.md#REQ-1044), [REQ-1174](arch/requirements.md#REQ-1174), [REQ-1949](arch/requirements.md#REQ-1949)
    - [Creating a relationship, view, table, source or webhook goes through an approval.](#approvals-and-limits-creation-approvals): [REQ-022](arch/requirements.md#REQ-022), [REQ-023](arch/requirements.md#REQ-023), [REQ-024](arch/requirements.md#REQ-024), [REQ-025](arch/requirements.md#REQ-025), [REQ-026](arch/requirements.md#REQ-026), [REQ-434](arch/requirements.md#REQ-434), [REQ-1948](arch/requirements.md#REQ-1948), [REQ-1950](arch/requirements.md#REQ-1950), [REQ-1995](arch/requirements.md#REQ-1995)

### Lineage and provenance

Provisa traces any column back to its sources. From one SQL statement, including commands composed inline, it builds the column-level lineage, and it merges all of them into a single graph across the estate. The graph is drawn with one box per relation and its columns as rows, arrives collapsed when it is large, and is read through a role picked on the lineage form. Every refresh of a materialized view also records its own lineage: the source columns and transform behind each output column, the versions of the definition and of the inputs it read, and the trace of the refresh. Those records are queryable in the ops domain as mv_refresh_lineage. On Trino they are kept in the telemetry catalog's Iceberg tables, and on DuckDB Provisa lands its own traces as Parquet files.
{ #lineage-and-provenance-column-lineage }

A table can record row-level changes into an append-only delta ledger, and a materialized view can be declared bitemporal: each refresh appends a version and never rewrites one, on any store. A plain read returns the current state. A request can read as of a time, measured on the source's own change time where the view declares one and on Provisa's load time where it does not. It can also read as recorded at a time, which reads the view as Provisa had recorded it then, so a correction recorded later does not change a replay of the past. The audit record keeps both times, so a report can be reproduced exactly.
{ #lineage-and-provenance-point-in-time }

Because every change is an event and every derived dataset is a view over those events, the platform describes itself. Quality scorecards, history and operational facts are views over streams the system already emits, not separate subsystems.
{ #lineage-and-provenance-self-describing }

??? note "Requirements"

    - [Provisa traces any column back to its sources.](#lineage-and-provenance-column-lineage): [REQ-862](arch/requirements.md#REQ-862), [REQ-1160](arch/requirements.md#REQ-1160), [REQ-1161](arch/requirements.md#REQ-1161), [REQ-1625](arch/requirements.md#REQ-1625), [REQ-1626](arch/requirements.md#REQ-1626), [REQ-1627](arch/requirements.md#REQ-1627), [REQ-1628](arch/requirements.md#REQ-1628), [REQ-1665](arch/requirements.md#REQ-1665)
    - [A table can record row-level changes into an append-only delta ledger, and a materializ...](#lineage-and-provenance-point-in-time): [REQ-877](arch/requirements.md#REQ-877), [REQ-878](arch/requirements.md#REQ-878), [REQ-1162](arch/requirements.md#REQ-1162), [REQ-1163](arch/requirements.md#REQ-1163), [REQ-1366](arch/requirements.md#REQ-1366), [REQ-2064](arch/requirements.md#REQ-2064)
    - [Because every change is an event and every derived dataset is a view over those events,...](#lineage-and-provenance-self-describing): [REQ-967](arch/requirements.md#REQ-967)

### Audit

Every query is recorded in its organization's audit log, written at the single governed pipeline so no surface can skip it. The record names the user and every role that acted, and holds a hash of the query, the tables touched, the source, status, duration, route and row count, and what was enforced on the statement. It also records the version of the model that answered, the sources read and the age of their data, the region that answered, and the as-of and as-recorded times of a time-travel read. The query text is kept encrypted under the organization's key, or the deployment's where the organization has set none, and is decrypted only on authorized admin reads.
{ #audit-query-log }

The audit log is append-only, and the database itself blocks updates and deletes. Each organization's log is a table in its own schema, in the record store it names for each region it selects. Indexes by tenant and time and by user and time make compliance reports fast to run. Nobody, including an admin, can edit the history. Before anything Provisa records is written, whether a span, a log line, an audit row or an error message to a client, it passes the organization's redaction rules, and every secret Provisa holds or the model references is redacted without a rule naming it.
{ #audit-append-only }

??? note "Requirements"

    - [Every query is recorded in its organization's audit log, written at the single governed...](#audit-query-log): [REQ-596](arch/requirements.md#REQ-596)
    - [The audit log is append-only, and the database itself blocks updates and deletes.](#audit-append-only): [REQ-613](arch/requirements.md#REQ-613)

### Public data publishing

An organization can publish data to people who are not signed in. A request with no login is routed to its organization by subdomain, and it can see only what has been published.
{ #public-data-publishing-anonymous-access }

Publishing is a governance grant. Every organization has a built-in public role that starts with no visibility at all, and publishing an asset means granting that role visibility on it. The same row and column rules apply as for any other role.
{ #public-data-publishing-public-role }

Anything marked public is served to every role in the organization, whatever domain the role is in. A public table, relationship or metric does not need a separate grant to each domain.
{ #public-data-publishing-all-domains }

Public endpoints face the internet with no login, so they get the tightest rate and query-cost limits. High-volume public publishing needs a premium public tier.
{ #public-data-publishing-traffic-caps }

??? note "Requirements"

    - [An organization can publish data to people who are not signed in.](#public-data-publishing-anonymous-access): [REQ-1058](arch/requirements.md#REQ-1058)
    - [Publishing is a governance grant.](#public-data-publishing-public-role): [REQ-1059](arch/requirements.md#REQ-1059)
    - [Anything marked public is served to every role in the organization, whatever domain the...](#public-data-publishing-all-domains): [REQ-1959](arch/requirements.md#REQ-1959)
    - [Public endpoints face the internet with no login, so they get the tightest rate and que...](#public-data-publishing-traffic-caps): [REQ-1060](arch/requirements.md#REQ-1060)

### Encryption and secrets

Sensitive payloads are protected with envelope encryption. Each payload gets its own data key, wrapped by an organization master key. This covers stored API keys and security rules, hot-table row data in Redis and the text of audit entries. Where no OS keychain is available, the master key falls back to a local file so the admin console can still generate a key.
{ #encryption-and-secrets-envelope }

The key service is pluggable. AWS KMS, HashiCorp Vault Transit and Azure Key Vault ship as providers, and an enterprise can register its own in-house KMS or HSM without touching core code. Providers are configured from the admin console, which lists each one with its settings.
{ #encryption-and-secrets-providers }

Bulk results written to S3 or Iceberg can be encrypted with a key only the client holds, and the JDBC driver, the Python client and GraphQL clients decrypt on their side, so the server never sees the clear text. The key can be a customer-managed key in the customer's own cloud account. A high-security mode turns off the wire-protocol listener.
{ #encryption-and-secrets-client-side }

An organization sets and changes its own encryption key and can never read it back. The deployment's own secrets follow the same rule: set, change, never read.
{ #encryption-and-secrets-org-keys }

Credentials in config are references, not values. A reference such as an environment variable or a secret name is resolved at the moment of use. An org admin creates a secret by name and value, the value never comes back out, and a secret can belong to one developer. A vendor API key can be held as a reference to the secrets service.
{ #encryption-and-secrets-secrets }

??? note "Requirements"

    - [Sensitive payloads are protected with envelope encryption.](#encryption-and-secrets-envelope): [REQ-458](arch/requirements.md#REQ-458), [REQ-684](arch/requirements.md#REQ-684), [REQ-685](arch/requirements.md#REQ-685), [REQ-686](arch/requirements.md#REQ-686), [REQ-688](arch/requirements.md#REQ-688), [REQ-689](arch/requirements.md#REQ-689), [REQ-1802](arch/requirements.md#REQ-1802)
    - [The key service is pluggable.](#encryption-and-secrets-providers): [REQ-593](arch/requirements.md#REQ-593), [REQ-918](arch/requirements.md#REQ-918), [REQ-1086](arch/requirements.md#REQ-1086), [REQ-1087](arch/requirements.md#REQ-1087), [REQ-1088](arch/requirements.md#REQ-1088), [REQ-1089](arch/requirements.md#REQ-1089), [REQ-1090](arch/requirements.md#REQ-1090), [REQ-1091](arch/requirements.md#REQ-1091)
    - [Bulk results written to S3 or Iceberg can be encrypted with a key only the client holds...](#encryption-and-secrets-client-side): [REQ-687](arch/requirements.md#REQ-687), [REQ-690](arch/requirements.md#REQ-690), [REQ-691](arch/requirements.md#REQ-691), [REQ-692](arch/requirements.md#REQ-692), [REQ-693](arch/requirements.md#REQ-693), [REQ-694](arch/requirements.md#REQ-694)
    - [An organization sets and changes its own encryption key and can never read it back.](#encryption-and-secrets-org-keys): [REQ-1574](arch/requirements.md#REQ-1574), [REQ-1575](arch/requirements.md#REQ-1575), [REQ-1801](arch/requirements.md#REQ-1801)
    - [Credentials in config are references, not values.](#encryption-and-secrets-secrets): [REQ-557](arch/requirements.md#REQ-557), [REQ-1557](arch/requirements.md#REQ-1557), [REQ-1558](arch/requirements.md#REQ-1558), [REQ-1560](arch/requirements.md#REQ-1560), [REQ-1580](arch/requirements.md#REQ-1580), [REQ-1695](arch/requirements.md#REQ-1695)

## Query it any way

### GraphQL

Each role gets a GraphQL schema generated from the model, showing only what that role may see. Filtering, ordering and pagination follow the Hasura convention, and ordering can reach into related objects. Nested relationships take the same arguments as a root query, relationship fields use the configured naming convention, and date and time types are typed precisely. GraphQL is the canonical API from which gRPC, JSON:API and OpenAPI are derived.
{ #graphql-schema }

Operations can use variables with defaults, and limit and offset travel as bound parameters, never pasted into the SQL.
{ #graphql-variables }

Provisa supports Apollo automatic persisted queries. A client can send the hash of a query it has run before and skip sending the text. The cache lives in Redis with a configurable lifetime. A hash still passes the same rights and governance checks as the original query.
{ #graphql-persisted-queries }

??? note "Requirements"

    - [Each role gets a GraphQL schema generated from the model, showing only what that role m...](#graphql-schema): [REQ-007](arch/requirements.md#REQ-007), [REQ-008](arch/requirements.md#REQ-008), [REQ-009](arch/requirements.md#REQ-009), [REQ-010](arch/requirements.md#REQ-010), [REQ-011](arch/requirements.md#REQ-011), [REQ-043](arch/requirements.md#REQ-043), [REQ-200](arch/requirements.md#REQ-200), [REQ-201](arch/requirements.md#REQ-201), [REQ-202](arch/requirements.md#REQ-202), [REQ-213](arch/requirements.md#REQ-213), [REQ-218](arch/requirements.md#REQ-218), [REQ-259](arch/requirements.md#REQ-259), [REQ-449](arch/requirements.md#REQ-449), [REQ-450](arch/requirements.md#REQ-450), [REQ-451](arch/requirements.md#REQ-451), [REQ-478](arch/requirements.md#REQ-478), [REQ-537](arch/requirements.md#REQ-537), [REQ-605](arch/requirements.md#REQ-605), [REQ-1403](arch/requirements.md#REQ-1403), [REQ-1908](arch/requirements.md#REQ-1908), [REQ-1929](arch/requirements.md#REQ-1929), [REQ-1987](arch/requirements.md#REQ-1987)
    - [Operations can use variables with defaults, and limit and offset travel as bound parame...](#graphql-variables): [REQ-300](arch/requirements.md#REQ-300), [REQ-301](arch/requirements.md#REQ-301)
    - [Provisa supports Apollo automatic persisted queries.](#graphql-persisted-queries): [REQ-288](arch/requirements.md#REQ-288), [REQ-289](arch/requirements.md#REQ-289), [REQ-290](arch/requirements.md#REQ-290), [REQ-291](arch/requirements.md#REQ-291), [REQ-488](arch/requirements.md#REQ-488)

### SQL over HTTP

Send PostgreSQL-compatible SQL to a REST endpoint and get governed results back. The SQL runs against the model's names, not the physical tables, and goes through the same governance as every other surface. The role comes from the credentials, never from the request body. A test endpoint offers the same guards for trying queries.
{ #sql-over-http-endpoint }

A compile-only route returns the SQL Provisa would run, with row filters and masks applied, along with the routing decision and parameters, without executing anything. It helps when checking what a role would actually be allowed to see.
{ #sql-over-http-compile }

Semantic SQL is the one dialect used throughout. It has no array, struct or map type, and nested data comes back as JSON on every engine. Objects are defined in the model, not by SQL, so CREATE, DROP, ALTER and TRUNCATE are refused on every surface.
{ #sql-over-http-semantic-sql }

??? note "Requirements"

    - [Send PostgreSQL-compatible SQL to a REST endpoint and get governed results back.](#sql-over-http-endpoint): [REQ-004](arch/requirements.md#REQ-004), [REQ-266](arch/requirements.md#REQ-266), [REQ-267](arch/requirements.md#REQ-267)
    - [A compile-only route returns the SQL Provisa would run, with row filters and masks appl...](#sql-over-http-compile): [REQ-161](arch/requirements.md#REQ-161), [REQ-487](arch/requirements.md#REQ-487)
    - [Semantic SQL is the one dialect used throughout.](#sql-over-http-semantic-sql): [REQ-1926](arch/requirements.md#REQ-1926), [REQ-1965](arch/requirements.md#REQ-1965)

### Cypher and graph analytics

Send Cypher to an HTTP endpoint and get rows back, with named parameters. Provisa derives labels and relationship types from the model, so there is no graph to load. Cypher can join across sources in one query. Domain boundaries are enforced when the query is parsed, and a graph-schema endpoint lists the labels and relationship types a role can use.
{ #cypher-and-graph-analytics-endpoint }

A large part of Cypher is supported, including optional matches, subqueries, comprehensions, map projections, CASE, UNWIND and label alternation. Each construct is translated into semantic SQL, so the same governance applies as for SQL. Relationships exist as joins between tables, which means they carry no stored properties.
{ #cypher-and-graph-analytics-translation }

Path queries cover shortest paths and variable-length patterns, which compile to recursive queries with a maximum number of hops. When several schema paths are equally short, all of them come back. A path variable returns its nodes and edges, and length(p) gives the hop count.
{ #cypher-and-graph-analytics-paths }

Returning a node, a relationship or a path produces a graph object with stable ids and properties, so a graph tool can draw it directly. Returning a whole domain gives every node it holds. Scalar columns stay plain values.
{ #cypher-and-graph-analytics-graph-results }

CREATE, SET and DELETE write to tables through the same write pipeline as SQL, so access rules, row filters and write permissions all apply. A label must already exist in the model, and only sources that accept writes can be written to. Relationships cannot be written, and DETACH DELETE is refused. The result reports the rows affected.
{ #cypher-and-graph-analytics-writes }

Graph analytics run over a query result in the Graph Explorer. A configurable size limit keeps them in bounds, and the community detection algorithm that cost the most is allowed only on small graphs.
{ #cypher-and-graph-analytics-analytics }

??? note "Requirements"

    - [Send Cypher to an HTTP endpoint and get rows back, with named parameters.](#cypher-and-graph-analytics-endpoint): [REQ-345](arch/requirements.md#REQ-345), [REQ-353](arch/requirements.md#REQ-353), [REQ-392](arch/requirements.md#REQ-392), [REQ-442](arch/requirements.md#REQ-442), [REQ-481](arch/requirements.md#REQ-481), [REQ-778](arch/requirements.md#REQ-778), [REQ-792](arch/requirements.md#REQ-792), [REQ-793](arch/requirements.md#REQ-793), [REQ-796](arch/requirements.md#REQ-796)
    - [A large part of Cypher is supported, including optional matches, subqueries, comprehens...](#cypher-and-graph-analytics-translation): [REQ-347](arch/requirements.md#REQ-347), [REQ-351](arch/requirements.md#REQ-351), [REQ-352](arch/requirements.md#REQ-352), [REQ-390](arch/requirements.md#REQ-390), [REQ-391](arch/requirements.md#REQ-391), [REQ-409](arch/requirements.md#REQ-409), [REQ-571](arch/requirements.md#REQ-571), [REQ-572](arch/requirements.md#REQ-572), [REQ-573](arch/requirements.md#REQ-573), [REQ-574](arch/requirements.md#REQ-574), [REQ-575](arch/requirements.md#REQ-575), [REQ-578](arch/requirements.md#REQ-578), [REQ-748](arch/requirements.md#REQ-748), [REQ-752](arch/requirements.md#REQ-752), [REQ-754](arch/requirements.md#REQ-754), [REQ-755](arch/requirements.md#REQ-755), [REQ-756](arch/requirements.md#REQ-756), [REQ-757](arch/requirements.md#REQ-757), [REQ-758](arch/requirements.md#REQ-758), [REQ-759](arch/requirements.md#REQ-759), [REQ-760](arch/requirements.md#REQ-760), [REQ-761](arch/requirements.md#REQ-761), [REQ-762](arch/requirements.md#REQ-762), [REQ-763](arch/requirements.md#REQ-763), [REQ-764](arch/requirements.md#REQ-764), [REQ-765](arch/requirements.md#REQ-765), [REQ-776](arch/requirements.md#REQ-776), [REQ-777](arch/requirements.md#REQ-777), [REQ-1404](arch/requirements.md#REQ-1404), [REQ-1586](arch/requirements.md#REQ-1586)
    - [Path queries cover shortest paths and variable-length patterns, which compile to recurs...](#cypher-and-graph-analytics-paths): [REQ-348](arch/requirements.md#REQ-348), [REQ-576](arch/requirements.md#REQ-576), [REQ-577](arch/requirements.md#REQ-577), [REQ-751](arch/requirements.md#REQ-751), [REQ-766](arch/requirements.md#REQ-766), [REQ-767](arch/requirements.md#REQ-767), [REQ-775](arch/requirements.md#REQ-775)
    - [Returning a node, a relationship or a path produces a graph object with stable ids and...](#cypher-and-graph-analytics-graph-results): [REQ-349](arch/requirements.md#REQ-349), [REQ-350](arch/requirements.md#REQ-350), [REQ-750](arch/requirements.md#REQ-750), [REQ-753](arch/requirements.md#REQ-753), [REQ-768](arch/requirements.md#REQ-768), [REQ-769](arch/requirements.md#REQ-769), [REQ-770](arch/requirements.md#REQ-770), [REQ-771](arch/requirements.md#REQ-771), [REQ-772](arch/requirements.md#REQ-772), [REQ-773](arch/requirements.md#REQ-773), [REQ-774](arch/requirements.md#REQ-774), [REQ-1630](arch/requirements.md#REQ-1630)
    - [CREATE, SET and DELETE write to tables through the same write pipeline as SQL, so acces...](#cypher-and-graph-analytics-writes): [REQ-346](arch/requirements.md#REQ-346), [REQ-661](arch/requirements.md#REQ-661), [REQ-662](arch/requirements.md#REQ-662), [REQ-663](arch/requirements.md#REQ-663), [REQ-664](arch/requirements.md#REQ-664), [REQ-665](arch/requirements.md#REQ-665), [REQ-666](arch/requirements.md#REQ-666), [REQ-667](arch/requirements.md#REQ-667), [REQ-668](arch/requirements.md#REQ-668), [REQ-669](arch/requirements.md#REQ-669), [REQ-670](arch/requirements.md#REQ-670), [REQ-671](arch/requirements.md#REQ-671), [REQ-798](arch/requirements.md#REQ-798), [REQ-818](arch/requirements.md#REQ-818)
    - [Graph analytics run over a query result in the Graph Explorer.](#cypher-and-graph-analytics-analytics): [REQ-650](arch/requirements.md#REQ-650), [REQ-651](arch/requirements.md#REQ-651)

### Natural-language query

Ask a question in plain language and Provisa starts an asynchronous job. It generates a SQL, a GraphQL and a Cypher answer in parallel, runs all of them, and returns the results. Seeing the same answer in three languages makes it easy to check and reuse.
{ #natural-language-query-jobs }

The model is shown only the schema the asking role may see. A generated query runs with that role's rights like any other query, and it can be saved.
{ #natural-language-query-role-scope }

In strict mode, one chain turns the question into GraphQL, then SQL, then Cypher. Both generation paths produce a best-effort Cypher form beside the SQL, and grouped results can include related columns by dot-path.
{ #natural-language-query-strict-mode }

??? note "Requirements"

    - [Ask a question in plain language and Provisa starts an asynchronous job.](#natural-language-query-jobs): [REQ-354](arch/requirements.md#REQ-354), [REQ-355](arch/requirements.md#REQ-355), [REQ-357](arch/requirements.md#REQ-357), [REQ-358](arch/requirements.md#REQ-358)
    - [The model is shown only the schema the asking role may see.](#natural-language-query-role-scope): [REQ-356](arch/requirements.md#REQ-356), [REQ-359](arch/requirements.md#REQ-359)
    - [In strict mode, one chain turns the question into GraphQL, then SQL, then Cypher.](#natural-language-query-strict-mode): [REQ-1400](arch/requirements.md#REQ-1400), [REQ-1406](arch/requirements.md#REQ-1406), [REQ-1408](arch/requirements.md#REQ-1408)

### REST, JSON:API and OpenAPI

Every registered table gets a plain REST endpoint and a JSON:API endpoint with no setup. Query-string parameters map to filters, ordering and paging. JSON:API uses the physical column and relationship names throughout, and its total count is computed by pushing a COUNT down to the source.
{ #rest-and-jsonapi-endpoints }

An OpenAPI specification is generated from the model, with typed parameters for each table. Registered commands appear as POST paths, and an operation can be marked as a query or a mutation. Tools that read OpenAPI can use the API directly.
{ #rest-and-jsonapi-openapi }

A grouped or aggregated result can carry the rows behind each group. A parameter on REST and JSON:API selects which columns of those rows to include, with dot-paths that reach through any number of relationships.
{ #rest-and-jsonapi-group-by-nodes }

A semantic URI for a piece of data can be resolved through REST, and by an MCP tool, to the governed value, optionally as of an earlier time.
{ #rest-and-jsonapi-resolve }

??? note "Requirements"

    - [Every registered table gets a plain REST endpoint and a JSON:API endpoint with no setup.](#rest-and-jsonapi-endpoints): [REQ-222](arch/requirements.md#REQ-222), [REQ-256](arch/requirements.md#REQ-256), [REQ-257](arch/requirements.md#REQ-257), [REQ-1197](arch/requirements.md#REQ-1197), [REQ-1417](arch/requirements.md#REQ-1417)
    - [An OpenAPI specification is generated from the model, with typed parameters for each ta...](#rest-and-jsonapi-openapi): [REQ-408](arch/requirements.md#REQ-408), [REQ-804](arch/requirements.md#REQ-804), [REQ-805](arch/requirements.md#REQ-805), [REQ-1155](arch/requirements.md#REQ-1155)
    - [A grouped or aggregated result can carry the rows behind each group.](#rest-and-jsonapi-group-by-nodes): [REQ-1401](arch/requirements.md#REQ-1401), [REQ-1402](arch/requirements.md#REQ-1402), [REQ-1722](arch/requirements.md#REQ-1722)
    - [A semantic URI for a piece of data can be resolved through REST, and by an MCP tool, to...](#rest-and-jsonapi-resolve): [REQ-1365](arch/requirements.md#REQ-1365)

### gRPC with a per-role proto

Provisa generates a gRPC service from the model, and the proto each role downloads shows only that role's tables and columns. The role is selected by a metadata key on every call. Column types map to protobuf scalar types, and field numbers stay stable.
{ #grpc-per-role-proto }

Clients that cannot speak gRPC can use an HTTP endpoint that accepts JSON shaped like the gRPC request, turns it into a governed query and returns JSON.
{ #grpc-http-proxy }

Queries stream their rows lazily, a batch at a time, so a large scan does not have to fit in memory. Point lookups take a direct route to the source. The streaming path has been tuned against Flight SQL for large reads.
{ #grpc-streaming }

??? note "Requirements"

    - [Provisa generates a gRPC service from the model, and the proto each role downloads show...](#grpc-per-role-proto): [REQ-525](arch/requirements.md#REQ-525), [REQ-526](arch/requirements.md#REQ-526), [REQ-538](arch/requirements.md#REQ-538), [REQ-617](arch/requirements.md#REQ-617), [REQ-1210](arch/requirements.md#REQ-1210), [REQ-1883](arch/requirements.md#REQ-1883), [REQ-1903](arch/requirements.md#REQ-1903), [REQ-1904](arch/requirements.md#REQ-1904)
    - [Clients that cannot speak gRPC can use an HTTP endpoint that accepts JSON shaped like t...](#grpc-http-proxy): [REQ-803](arch/requirements.md#REQ-803)
    - [Queries stream their rows lazily, a batch at a time, so a large scan does not have to f...](#grpc-streaming): [REQ-1215](arch/requirements.md#REQ-1215), [REQ-1884](arch/requirements.md#REQ-1884), [REQ-1891](arch/requirements.md#REQ-1891), [REQ-1899](arch/requirements.md#REQ-1899)

### PostgreSQL wire protocol

Provisa can speak the PostgreSQL wire protocol, so BI tools and anything with a Postgres driver connect without special support. The listener is off until a port is set. It authenticates through the same auth provider as every other surface, supports TLS, and reports a Postgres-compatible version string. It accepts SQL only, never GraphQL or Cypher.
{ #postgres-wire-protocol-server }

Tools such as DBeaver browse through the usual catalog queries, and Provisa answers these from the model, showing only what the connected role may see. Primary keys, unique constraints and composite foreign keys are reconstructed, so tools can draw the schema and suggest joins.
{ #postgres-wire-protocol-catalog }

The simple and extended query protocols both work, including multiple statements, positional parameters, binary parameters, cursors and COPY. Session commands such as BEGIN and SET are accepted without effect. Writes to existing tables go through the single write admission, and definitions are not accepted over the wire.
{ #postgres-wire-protocol-queries }

Provisa ships client libraries for the places where a standard driver is not enough. The JDBC driver lists the tables and views a role can see and moves results as Arrow over Flight. A Python client offers DB-API, pandas and SQLAlchemy. All of them authenticate with a username and password, and the server decides the role, not the client.
{ #postgres-wire-protocol-client-drivers }

??? note "Requirements"

    - [Provisa can speak the PostgreSQL wire protocol, so BI tools and anything with a Postgre...](#postgres-wire-protocol-server): [REQ-527](arch/requirements.md#REQ-527), [REQ-529](arch/requirements.md#REQ-529), [REQ-530](arch/requirements.md#REQ-530), [REQ-579](arch/requirements.md#REQ-579), [REQ-614](arch/requirements.md#REQ-614), [REQ-883](arch/requirements.md#REQ-883), [REQ-890](arch/requirements.md#REQ-890), [REQ-892](arch/requirements.md#REQ-892)
    - [Tools such as DBeaver browse through the usual catalog queries, and Provisa answers the...](#postgres-wire-protocol-catalog): [REQ-532](arch/requirements.md#REQ-532), [REQ-588](arch/requirements.md#REQ-588), [REQ-891](arch/requirements.md#REQ-891), [REQ-1094](arch/requirements.md#REQ-1094), [REQ-1095](arch/requirements.md#REQ-1095)
    - [The simple and extended query protocols both work, including multiple statements, posit...](#postgres-wire-protocol-queries): [REQ-580](arch/requirements.md#REQ-580), [REQ-581](arch/requirements.md#REQ-581), [REQ-582](arch/requirements.md#REQ-582), [REQ-583](arch/requirements.md#REQ-583), [REQ-584](arch/requirements.md#REQ-584), [REQ-585](arch/requirements.md#REQ-585), [REQ-586](arch/requirements.md#REQ-586), [REQ-587](arch/requirements.md#REQ-587), [REQ-589](arch/requirements.md#REQ-589), [REQ-590](arch/requirements.md#REQ-590), [REQ-615](arch/requirements.md#REQ-615), [REQ-616](arch/requirements.md#REQ-616), [REQ-1862](arch/requirements.md#REQ-1862), [REQ-1863](arch/requirements.md#REQ-1863)
    - [Provisa ships client libraries for the places where a standard driver is not enough.](#postgres-wire-protocol-client-drivers): [REQ-126](arch/requirements.md#REQ-126), [REQ-127](arch/requirements.md#REQ-127), [REQ-128](arch/requirements.md#REQ-128), [REQ-129](arch/requirements.md#REQ-129), [REQ-130](arch/requirements.md#REQ-130), [REQ-131](arch/requirements.md#REQ-131), [REQ-132](arch/requirements.md#REQ-132), [REQ-268](arch/requirements.md#REQ-268), [REQ-269](arch/requirements.md#REQ-269), [REQ-270](arch/requirements.md#REQ-270), [REQ-272](arch/requirements.md#REQ-272), [REQ-273](arch/requirements.md#REQ-273), [REQ-274](arch/requirements.md#REQ-274), [REQ-293](arch/requirements.md#REQ-293), [REQ-363](arch/requirements.md#REQ-363), [REQ-486](arch/requirements.md#REQ-486), [REQ-489](arch/requirements.md#REQ-489), [REQ-490](arch/requirements.md#REQ-490), [REQ-607](arch/requirements.md#REQ-607), [REQ-2070](arch/requirements.md#REQ-2070)

### Arrow Flight

Provisa runs an Arrow Flight server for high-throughput consumers. Results stream batch by batch, so the full result never sits in memory, and every governance rule applies. Flight SQL is also supported, and engines that cannot produce Arrow natively are adapted by a generic row-to-Arrow converter.
{ #arrow-flight-server }

When Trino is the engine, the Flight route goes through a Flight SQL proxy. If the proxy is unavailable the request fails, and Provisa does not quietly fall back to another route.
{ #arrow-flight-trino-route }

ADBC clients connect to the Flight server, on a default port that can be changed. This gives Python and other ADBC users columnar results without converting from rows.
{ #arrow-flight-adbc }

Provisa also serves the DuckDB Airport protocol, so DuckDB can attach to it as if it were a catalog. Each role sees only its own registered columns, and writes are held in a transaction until commit.
{ #arrow-flight-airport }

??? note "Requirements"

    - [Provisa runs an Arrow Flight server for high-throughput consumers.](#arrow-flight-server): [REQ-045](arch/requirements.md#REQ-045), [REQ-051](arch/requirements.md#REQ-051), [REQ-143](arch/requirements.md#REQ-143), [REQ-145](arch/requirements.md#REQ-145), [REQ-447](arch/requirements.md#REQ-447), [REQ-1216](arch/requirements.md#REQ-1216), [REQ-1221](arch/requirements.md#REQ-1221), [REQ-1518](arch/requirements.md#REQ-1518)
    - [When Trino is the engine, the Flight route goes through a Flight SQL proxy.](#arrow-flight-trino-route): [REQ-144](arch/requirements.md#REQ-144), [REQ-146](arch/requirements.md#REQ-146), [REQ-485](arch/requirements.md#REQ-485)
    - [ADBC clients connect to the Flight server, on a default port that can be changed.](#arrow-flight-adbc): [REQ-271](arch/requirements.md#REQ-271), [REQ-608](arch/requirements.md#REQ-608), [REQ-711](arch/requirements.md#REQ-711)
    - [Provisa also serves the DuckDB Airport protocol, so DuckDB can attach to it as if it we...](#arrow-flight-airport): [REQ-1120](arch/requirements.md#REQ-1120), [REQ-1121](arch/requirements.md#REQ-1121), [REQ-1122](arch/requirements.md#REQ-1122), [REQ-1123](arch/requirements.md#REQ-1123), [REQ-1124](arch/requirements.md#REQ-1124), [REQ-1125](arch/requirements.md#REQ-1125)

### Neo4j compatibility

Provisa answers on the Bolt protocol, so Neo4j Browser, Bloom and the Neo4j drivers can connect and run Cypher. The user is the principal and each of their roles appears as a database, and the active role's domain access is always respected. Relationships get stable integer ids.
{ #neo4j-compatibility-bolt }

A graph visible in Provisa can be exported to a real Neo4j instance. Nodes and relationships are merged by a stable id, in batches, and the call reports how many were imported and which statements failed. Connection and auth failures come back as distinct errors.
{ #neo4j-compatibility-export }

??? note "Requirements"

    - [Provisa answers on the Bolt protocol, so Neo4j Browser, Bloom and the Neo4j drivers can...](#neo4j-compatibility-bolt): [REQ-802](arch/requirements.md#REQ-802), [REQ-806](arch/requirements.md#REQ-806), [REQ-807](arch/requirements.md#REQ-807), [REQ-808](arch/requirements.md#REQ-808), [REQ-1885](arch/requirements.md#REQ-1885)
    - [A graph visible in Provisa can be exported to a real Neo4j instance.](#neo4j-compatibility-export): [REQ-713](arch/requirements.md#REQ-713), [REQ-714](arch/requirements.md#REQ-714), [REQ-715](arch/requirements.md#REQ-715), [REQ-716](arch/requirements.md#REQ-716), [REQ-717](arch/requirements.md#REQ-717), [REQ-718](arch/requirements.md#REQ-718), [REQ-719](arch/requirements.md#REQ-719), [REQ-720](arch/requirements.md#REQ-720), [REQ-795](arch/requirements.md#REQ-795), [REQ-797](arch/requirements.md#REQ-797)

### MCP server for AI agents

Provisa exposes a Model Context Protocol server, so agents such as Claude Desktop can discover the catalog and run governed queries. An agent can also propose a new source or table for a human to approve, but never creates one itself. An optional tool exposes an external decision-evaluation service when its key is configured, and agents can run Cypher under the caller's role.
{ #mcp-server-tools }

The desktop build turns on a local MCP server, bound to loopback. The Explore page gives a copy-ready Claude Desktop configuration with a connect URL that is aware of proxies, and the bundled bridge needs no Node install. The local connector speaks plain HTTP, with optional TLS for public deployments.
{ #mcp-server-connect }

Remote bearer tokens map to Provisa roles. Every tool call is scoped to the capabilities of the identity behind it, so an agent can never do more than that person could.
{ #mcp-server-identity }

??? note "Requirements"

    - [Provisa exposes a Model Context Protocol server, so agents such as Claude Desktop can d...](#mcp-server-tools): [REQ-1008](arch/requirements.md#REQ-1008), [REQ-1789](arch/requirements.md#REQ-1789), [REQ-1792](arch/requirements.md#REQ-1792), [REQ-1990](arch/requirements.md#REQ-1990)
    - [The desktop build turns on a local MCP server, bound to loopback.](#mcp-server-connect): [REQ-1101](arch/requirements.md#REQ-1101), [REQ-1102](arch/requirements.md#REQ-1102), [REQ-1103](arch/requirements.md#REQ-1103), [REQ-1104](arch/requirements.md#REQ-1104), [REQ-1106](arch/requirements.md#REQ-1106)
    - [Remote bearer tokens map to Provisa roles.](#mcp-server-identity): [REQ-1105](arch/requirements.md#REQ-1105), [REQ-1857](arch/requirements.md#REQ-1857), [REQ-2075](arch/requirements.md#REQ-2075), [REQ-2076](arch/requirements.md#REQ-2076), [REQ-2082](arch/requirements.md#REQ-2082)

### Vector and semantic search

Embedding models are declared in a registry with their provider and dimensions. OpenAI-compatible APIs, Ollama and local HuggingFace models are supported. The embed function in SQL returns the embedding of a text from any registered model, and API keys can be secret references.
{ #vector-search-models }

A column can be declared as an embedding. A virtual embedding column can be generated from a query and refreshed on a schedule, re-embedding only the rows that changed. Once generated, its model is locked so vectors from different models are never mixed.
{ #vector-search-columns }

Use cosine_similarity in SQL, and in Cypher, which compiles to it. A search can pass a text string, which Provisa embeds at query time. When a source has its own vector support, Provisa detects it and uses it.
{ #vector-search-queries }

Embedding columns obey the same governance as other columns: row filters, domain rules and sensitivity tags. A vector cannot be partially masked, so masks do not apply to it. Where a replica serves the table, it has a vector index built with each replica build and swapped in with it, so the index is as fresh as the replica.
{ #vector-search-governance-and-replicas }

??? note "Requirements"

    - [Embedding models are declared in a registry with their provider and dimensions.](#vector-search-models): [REQ-419](arch/requirements.md#REQ-419), [REQ-420](arch/requirements.md#REQ-420), [REQ-1827](arch/requirements.md#REQ-1827), [REQ-2002](arch/requirements.md#REQ-2002)
    - [A column can be declared as an embedding.](#vector-search-columns): [REQ-421](arch/requirements.md#REQ-421), [REQ-427](arch/requirements.md#REQ-427), [REQ-428](arch/requirements.md#REQ-428), [REQ-429](arch/requirements.md#REQ-429)
    - [Use cosine_similarity in SQL, and in Cypher, which compiles to it.](#vector-search-queries): [REQ-422](arch/requirements.md#REQ-422), [REQ-423](arch/requirements.md#REQ-423), [REQ-430](arch/requirements.md#REQ-430), [REQ-431](arch/requirements.md#REQ-431), [REQ-2000](arch/requirements.md#REQ-2000)
    - [Embedding columns obey the same governance as other columns: row filters, domain rules...](#vector-search-governance-and-replicas): [REQ-424](arch/requirements.md#REQ-424), [REQ-425](arch/requirements.md#REQ-425), [REQ-426](arch/requirements.md#REQ-426), [REQ-652](arch/requirements.md#REQ-652)

## Run it fast and fresh

### Federation and routing

Every query takes one of a few routes, and Provisa picks it for you. A query that touches a single relational source goes straight to that database over its native driver. A query that joins across sources goes to the federation engine. A source the engine cannot reach directly is read through a replica. When the default is wrong for a particular table or view, a steward can override the route. Mutations to a database always go straight to that database, never through the engine.
{ #federation-and-routing-routes }

The engine that runs cross-source joins is a choice, not a fixture. Trino, DuckDB, PostgreSQL, ClickHouse, the cloud warehouses and a generic SQLAlchemy fallback all sit behind one contract, and you pick one in the admin console. Each engine declares what it can do, and the planner reads those declarations when it decides what to push down and what to pull back. A source an engine reaches through a connector is queried live. Anything else lands in the engine's own store first. The same model answers the same question whichever engine you choose, because governance runs the same way on all of them.
{ #federation-and-routing-engines }

Provisa compiles every request to one canonical PostgreSQL-style statement, then translates it into the dialect of wherever it will run: the source database, or the engine. Dialect is decided by the source type recorded when the table was registered, so you never write dialect-specific SQL. Translation belongs to the compiler, not to the protocol you connected with, so pgwire, Flight, GraphQL and the rest produce the same SQL for the same question. Joins across sources get their column types coerced to match automatically.
{ #federation-and-routing-transpilation }

Compilation happens in two stages. The first turns your request into plain SQL against the semantic model. The second rewrites that SQL under the caller's role, applying row filters, masking and column visibility, whatever shape the SQL takes: subqueries, CTEs, joins, star expansion. Because the second stage only needs SQL and a role, every client path gets the same governance.
{ #federation-and-routing-governed-pipeline }

??? note "Requirements"

    - [Every query takes one of a few routes, and Provisa picks it for you.](#federation-and-routing-routes): [REQ-027](arch/requirements.md#REQ-027), [REQ-028](arch/requirements.md#REQ-028), [REQ-030](arch/requirements.md#REQ-030), [REQ-229](arch/requirements.md#REQ-229), [REQ-550](arch/requirements.md#REQ-550), [REQ-863](arch/requirements.md#REQ-863), [REQ-875](arch/requirements.md#REQ-875), [REQ-909](arch/requirements.md#REQ-909), [REQ-997](arch/requirements.md#REQ-997), [REQ-1214](arch/requirements.md#REQ-1214), [REQ-1519](arch/requirements.md#REQ-1519), [REQ-1880](arch/requirements.md#REQ-1880)
    - [The engine that runs cross-source joins is a choice, not a fixture.](#federation-and-routing-engines): [REQ-825](arch/requirements.md#REQ-825), [REQ-827](arch/requirements.md#REQ-827), [REQ-840](arch/requirements.md#REQ-840), [REQ-841](arch/requirements.md#REQ-841), [REQ-842](arch/requirements.md#REQ-842), [REQ-843](arch/requirements.md#REQ-843), [REQ-888](arch/requirements.md#REQ-888), [REQ-893](arch/requirements.md#REQ-893), [REQ-895](arch/requirements.md#REQ-895), [REQ-896](arch/requirements.md#REQ-896), [REQ-897](arch/requirements.md#REQ-897), [REQ-899](arch/requirements.md#REQ-899), [REQ-900](arch/requirements.md#REQ-900), [REQ-903](arch/requirements.md#REQ-903), [REQ-904](arch/requirements.md#REQ-904), [REQ-906](arch/requirements.md#REQ-906), [REQ-907](arch/requirements.md#REQ-907), [REQ-908](arch/requirements.md#REQ-908), [REQ-911](arch/requirements.md#REQ-911), [REQ-912](arch/requirements.md#REQ-912), [REQ-916](arch/requirements.md#REQ-916), [REQ-933](arch/requirements.md#REQ-933), [REQ-946](arch/requirements.md#REQ-946), [REQ-947](arch/requirements.md#REQ-947), [REQ-948](arch/requirements.md#REQ-948), [REQ-951](arch/requirements.md#REQ-951), [REQ-986](arch/requirements.md#REQ-986), [REQ-987](arch/requirements.md#REQ-987), [REQ-988](arch/requirements.md#REQ-988), [REQ-995](arch/requirements.md#REQ-995), [REQ-1178](arch/requirements.md#REQ-1178), [REQ-1179](arch/requirements.md#REQ-1179), [REQ-1180](arch/requirements.md#REQ-1180), [REQ-1730](arch/requirements.md#REQ-1730), [REQ-1867](arch/requirements.md#REQ-1867), [REQ-1868](arch/requirements.md#REQ-1868), [REQ-1870](arch/requirements.md#REQ-1870), [REQ-1871](arch/requirements.md#REQ-1871), [REQ-1874](arch/requirements.md#REQ-1874), [REQ-1999](arch/requirements.md#REQ-1999)
    - [Provisa compiles every request to one canonical PostgreSQL-style statement, then transl...](#federation-and-routing-transpilation): [REQ-066](arch/requirements.md#REQ-066), [REQ-067](arch/requirements.md#REQ-067), [REQ-068](arch/requirements.md#REQ-068), [REQ-265](arch/requirements.md#REQ-265), [REQ-468](arch/requirements.md#REQ-468), [REQ-552](arch/requirements.md#REQ-552), [REQ-809](arch/requirements.md#REQ-809), [REQ-902](arch/requirements.md#REQ-902), [REQ-1002](arch/requirements.md#REQ-1002), [REQ-1411](arch/requirements.md#REQ-1411)
    - [Compilation happens in two stages.](#federation-and-routing-governed-pipeline): [REQ-262](arch/requirements.md#REQ-262), [REQ-264](arch/requirements.md#REQ-264), [REQ-1176](arch/requirements.md#REQ-1176)

### Replicas and freshness

Small lookup tables that many queries join against can be held hot in Redis and injected into queries as an inline set of rows, so the join never leaves memory. A table becomes hot automatically when it is the target of a many-to-one relationship or its row count is under a threshold, and you can opt any table out or force it in. Size limits keep a hot table from growing without bound, and governance still applies to the cached rows.
{ #replicas-and-freshness-hot-tables }

A replica is a copy of a source table that Provisa keeps in a store the engine can read quickly. Sources the engine can attach live are read live. Sources it cannot reach are replicated, and the same applies to a table you ask to be replicated for speed. The choice follows from what the engine can reach, not from a setting you tune. Replicas land at boot as empty tables and fill when first needed. A table can also opt in to replicating only the rows a query asks for by primary key. If a replica cannot be built, the read is refused with a message naming the table and the cause, rather than answered from nothing.
{ #replicas-and-freshness-warm-placement }

A landed replica carries the shape of the model it came from. Column names, types, primary keys, descriptions and steward tags travel with it, and the landing step reconciles an existing table instead of blindly recreating it. Incoming rows are matched to target columns by name, and bulk-load paths are used wherever the target store has them. Once a replica lands it is indexed on its primary key, enforced as a key, and on the columns its declared relationships join on, so joins in a relational store read an index. The indexes are built after the landing, so the replica is readable at once. An index that fails is recorded by name and tried again at the next landing, and rows that repeat a declared key are reported as such.
{ #replicas-and-freshness-landed-shape }

Freshness is judged in one place for every replica and view. You choose per table whether staleness is bounded by a time-to-live, by a probe that checks whether the upstream changed, or by both. A probe can be a query, a version token, or a marker file that a producer writes when new data is ready. A Kafka or WebSocket source's tables apply its changes as they arrive, a table captured by Debezium applies each change event from its topic after its first build, and a PostgreSQL, MongoDB or Cosmos DB table that opts into its database's own change stream is rebuilt on each burst of changes. For databases that cannot absorb heavy reads, a steward can mark a source load-sensitive, so queries never read it and only its scheduled refreshes, on a cadence or a change probe and, where it names one, only in its off-peak window, do. The table's detail page shows the resulting policy in plain words.
{ #replicas-and-freshness-freshness-gate }

A materialized view stores the result of a view so queries that match it read the stored rows. Matching is transparent: you query the same schema and Provisa rewrites to the stored result, including when only part of a join pattern matches. A write to a source table marks affected views stale, and stale views are not used. Row security applies after the rewrite, so a stored result never widens what a role can see. Views are hidden from the schema by default and a steward can expose one. A view can be published only when the relationships it depends on are approved, and in a fleet the refresh of any one view is coordinated so two instances do not rebuild it at once.
{ #replicas-and-freshness-materialized-views }

Some sources, such as files, SharePoint and Splunk, are reached through a bundled connector that Provisa fetches on demand and runs for you, then replicates into the engine's store. You can also copy or move a registered table to another source with an ordinary CREATE TABLE AS statement. The copy reads through the governed pipeline as the organization's administrator, so row rules and masks apply as they would to any read.
{ #replicas-and-freshness-moving-data }

??? note "Requirements"

    - [Small lookup tables that many queries join against can be held hot in Redis and injecte...](#replicas-and-freshness-hot-tables): [REQ-230](arch/requirements.md#REQ-230), [REQ-231](arch/requirements.md#REQ-231), [REQ-232](arch/requirements.md#REQ-232), [REQ-233](arch/requirements.md#REQ-233), [REQ-236](arch/requirements.md#REQ-236), [REQ-237](arch/requirements.md#REQ-237), [REQ-241](arch/requirements.md#REQ-241)
    - [A replica is a copy of a source table that Provisa keeps in a store the engine can read...](#replicas-and-freshness-warm-placement): [REQ-238](arch/requirements.md#REQ-238), [REQ-239](arch/requirements.md#REQ-239), [REQ-240](arch/requirements.md#REQ-240), [REQ-844](arch/requirements.md#REQ-844), [REQ-845](arch/requirements.md#REQ-845), [REQ-846](arch/requirements.md#REQ-846), [REQ-847](arch/requirements.md#REQ-847), [REQ-848](arch/requirements.md#REQ-848), [REQ-905](arch/requirements.md#REQ-905), [REQ-930](arch/requirements.md#REQ-930), [REQ-953](arch/requirements.md#REQ-953), [REQ-1632](arch/requirements.md#REQ-1632), [REQ-1633](arch/requirements.md#REQ-1633), [REQ-1637](arch/requirements.md#REQ-1637), [REQ-1653](arch/requirements.md#REQ-1653), [REQ-1674](arch/requirements.md#REQ-1674), [REQ-1865](arch/requirements.md#REQ-1865), [REQ-1912](arch/requirements.md#REQ-1912), [REQ-1915](arch/requirements.md#REQ-1915), [REQ-1962](arch/requirements.md#REQ-1962), [REQ-2061](arch/requirements.md#REQ-2061), [REQ-2062](arch/requirements.md#REQ-2062)
    - [A landed replica carries the shape of the model it came from.](#replicas-and-freshness-landed-shape): [REQ-934](arch/requirements.md#REQ-934), [REQ-935](arch/requirements.md#REQ-935), [REQ-980](arch/requirements.md#REQ-980), [REQ-990](arch/requirements.md#REQ-990), [REQ-1607](arch/requirements.md#REQ-1607), [REQ-1651](arch/requirements.md#REQ-1651), [REQ-1652](arch/requirements.md#REQ-1652), [REQ-1654](arch/requirements.md#REQ-1654), [REQ-1655](arch/requirements.md#REQ-1655), [REQ-1657](arch/requirements.md#REQ-1657), [REQ-1658](arch/requirements.md#REQ-1658), [REQ-1660](arch/requirements.md#REQ-1660), [REQ-2015](arch/requirements.md#REQ-2015)
    - [Freshness is judged in one place for every replica and view.](#replicas-and-freshness-freshness-gate): [REQ-483](arch/requirements.md#REQ-483), [REQ-826](arch/requirements.md#REQ-826), [REQ-855](arch/requirements.md#REQ-855), [REQ-856](arch/requirements.md#REQ-856), [REQ-857](arch/requirements.md#REQ-857), [REQ-858](arch/requirements.md#REQ-858), [REQ-859](arch/requirements.md#REQ-859), [REQ-860](arch/requirements.md#REQ-860), [REQ-861](arch/requirements.md#REQ-861), [REQ-874](arch/requirements.md#REQ-874), [REQ-881](arch/requirements.md#REQ-881), [REQ-929](arch/requirements.md#REQ-929), [REQ-932](arch/requirements.md#REQ-932), [REQ-1141](arch/requirements.md#REQ-1141), [REQ-1143](arch/requirements.md#REQ-1143), [REQ-1148](arch/requirements.md#REQ-1148), [REQ-1149](arch/requirements.md#REQ-1149), [REQ-1170](arch/requirements.md#REQ-1170), [REQ-1661](arch/requirements.md#REQ-1661), [REQ-2003](arch/requirements.md#REQ-2003), [REQ-2031](arch/requirements.md#REQ-2031), [REQ-2055](arch/requirements.md#REQ-2055), [REQ-2066](arch/requirements.md#REQ-2066)
    - [A materialized view stores the result of a view so queries that match it read the store...](#replicas-and-freshness-materialized-views): [REQ-081](arch/requirements.md#REQ-081), [REQ-084](arch/requirements.md#REQ-084), [REQ-085](arch/requirements.md#REQ-085), [REQ-086](arch/requirements.md#REQ-086), [REQ-158](arch/requirements.md#REQ-158), [REQ-159](arch/requirements.md#REQ-159), [REQ-160](arch/requirements.md#REQ-160), [REQ-234](arch/requirements.md#REQ-234), [REQ-235](arch/requirements.md#REQ-235), [REQ-543](arch/requirements.md#REQ-543), [REQ-810](arch/requirements.md#REQ-810), [REQ-880](arch/requirements.md#REQ-880), [REQ-882](arch/requirements.md#REQ-882), [REQ-984](arch/requirements.md#REQ-984), [REQ-1140](arch/requirements.md#REQ-1140), [REQ-1157](arch/requirements.md#REQ-1157), [REQ-1164](arch/requirements.md#REQ-1164)
    - [Some sources, such as files, SharePoint and Splunk, are reached through a bundled conne...](#replicas-and-freshness-moving-data): [REQ-954](arch/requirements.md#REQ-954), [REQ-955](arch/requirements.md#REQ-955), [REQ-956](arch/requirements.md#REQ-956), [REQ-996](arch/requirements.md#REQ-996), [REQ-1941](arch/requirements.md#REQ-1941)

### Response caching

Provisa can serve a repeated request from a stored result, but only when the request asks for it. A GraphQL query opts in with a directive, a Cypher query with a comment line, and a gRPC call with request metadata. Without it, the request neither reads nor writes the cache. Every response says whether it was a hit or a miss. A hit is a real route in the planner, not a side check, so a cached answer skips execution entirely. The store is Redis when configured and an embedded fallback otherwise.
{ #response-caching-store-and-opt-in }

How long a result lives is set at the most specific level that has an opinion: the table, then the source, then the deployment. A table can also set a different tolerance per role, so a trader sees live data while an analyst accepts a few minutes of staleness. Any mutation to a table drops the cached results that read it.
{ #response-caching-ttl-and-staleness }

A cache hit can never show one person another person's data. The key is built from the governed, normalized form of the query together with the caller's role and the row-security values that apply, so two callers share an entry only when they would see identical rows. In multi-organization deployments the key is also partitioned by organization.
{ #response-caching-isolation }

??? note "Requirements"

    - [Provisa can serve a repeated request from a stored result, but only when the request as...](#response-caching-store-and-opt-in): [REQ-077](arch/requirements.md#REQ-077), [REQ-536](arch/requirements.md#REQ-536), [REQ-865](arch/requirements.md#REQ-865), [REQ-985](arch/requirements.md#REQ-985), [REQ-1896](arch/requirements.md#REQ-1896), [REQ-1897](arch/requirements.md#REQ-1897)
    - [How long a result lives is set at the most specific level that has an opinion: the tabl...](#response-caching-ttl-and-staleness): [REQ-080](arch/requirements.md#REQ-080), [REQ-544](arch/requirements.md#REQ-544), [REQ-1907](arch/requirements.md#REQ-1907)
    - [A cache hit can never show one person another person's data.](#response-caching-isolation): [REQ-595](arch/requirements.md#REQ-595), [REQ-712](arch/requirements.md#REQ-712), [REQ-864](arch/requirements.md#REQ-864), [REQ-866](arch/requirements.md#REQ-866)

### Large results

Results come back in the shape the consumer needs. JSON keeps the nested structure of the query, and newline-delimited JSON streams one object per line. Tabular exports come as a single flattened table or as normalized tables with their foreign keys, in Parquet or CSV. Normalizing is done on the query itself, not by reshaping a serialized response afterward.
{ #large-results-formats }

When a result is too big to send inline, Provisa can write it to object storage and return a presigned link that expires. The caller sets the format and the size threshold with request headers, and the operator sets the defaults. Parquet and ORC are written by the engine straight to the store, so the data never passes through Provisa. Other formats are produced by Provisa and uploaded. The decision is automatic at the point where the query runs, not something each protocol has to implement. Files are readable only by the user who asked for them and are cleaned up after the link expires. Without a cloud bucket, results can be served from local disk with the same scoping and expiry.
{ #large-results-redirect }

Protocols that stream, such as pgwire, Flight SQL and gRPC, receive rows as the engine produces them instead of waiting for the full set. Direct reads use a server-side cursor or an Arrow reader, and each engine's runtime streams in batches. A million-row scan therefore starts returning almost at once and holds only a batch in memory.
{ #large-results-streaming }

??? note "Requirements"

    - [Results come back in the shape the consumer needs.](#large-results-formats): [REQ-047](arch/requirements.md#REQ-047), [REQ-048](arch/requirements.md#REQ-048), [REQ-049](arch/requirements.md#REQ-049), [REQ-050](arch/requirements.md#REQ-050), [REQ-484](arch/requirements.md#REQ-484)
    - [When a result is too big to send inline, Provisa can write it to object storage and ret...](#large-results-redirect): [REQ-006](arch/requirements.md#REQ-006), [REQ-029](arch/requirements.md#REQ-029), [REQ-044](arch/requirements.md#REQ-044), [REQ-137](arch/requirements.md#REQ-137), [REQ-138](arch/requirements.md#REQ-138), [REQ-139](arch/requirements.md#REQ-139), [REQ-140](arch/requirements.md#REQ-140), [REQ-141](arch/requirements.md#REQ-141), [REQ-142](arch/requirements.md#REQ-142), [REQ-171](arch/requirements.md#REQ-171), [REQ-534](arch/requirements.md#REQ-534), [REQ-1191](arch/requirements.md#REQ-1191), [REQ-1192](arch/requirements.md#REQ-1192), [REQ-1193](arch/requirements.md#REQ-1193), [REQ-1194](arch/requirements.md#REQ-1194), [REQ-1195](arch/requirements.md#REQ-1195), [REQ-1196](arch/requirements.md#REQ-1196), [REQ-1203](arch/requirements.md#REQ-1203), [REQ-1204](arch/requirements.md#REQ-1204), [REQ-1224](arch/requirements.md#REQ-1224)
    - [Protocols that stream, such as pgwire, Flight SQL and gRPC, receive rows as the engine...](#large-results-streaming): [REQ-1185](arch/requirements.md#REQ-1185), [REQ-1186](arch/requirements.md#REQ-1186), [REQ-1187](arch/requirements.md#REQ-1187), [REQ-1188](arch/requirements.md#REQ-1188), [REQ-1189](arch/requirements.md#REQ-1189), [REQ-1190](arch/requirements.md#REQ-1190), [REQ-1217](arch/requirements.md#REQ-1217), [REQ-1218](arch/requirements.md#REQ-1218), [REQ-1222](arch/requirements.md#REQ-1222), [REQ-1223](arch/requirements.md#REQ-1223), [REQ-1225](arch/requirements.md#REQ-1225), [REQ-1231](arch/requirements.md#REQ-1231), [REQ-1893](arch/requirements.md#REQ-1893), [REQ-1898](arch/requirements.md#REQ-1898)

### Performance and scale

Federated joins run well when the engine has statistics, so Provisa analyzes registered tables when you add a source and again on demand, and after each replica load. When a query still needs help, you can attach engine session properties to a table, a view or a source, or put a hint in the query text to broadcast a table or pick a join strategy. A constant filter on one side of a join is also carried to the other side, so each source scans less.
{ #performance-and-scale-optimizer-hints }

Compiling and governing a query is cached so repeated shapes skip the work. Raw SQL surfaces key the cache on the statement with its literals blanked, and governance outcomes are cached per organization for a short time. The registry of sources is cached too. All of these caches are keyed to the current model, so a change to the model is never answered from a stale entry.
{ #performance-and-scale-compile-caches }

One slow query does not block the others. Asynchronous queries run on their own connections, and each source can cap its concurrent live reads so an operational database is not overwhelmed. For PostgreSQL sources, connection pooling through PgBouncer is available as an opt-in.
{ #performance-and-scale-concurrency }

On Trino, work that exceeds memory is retried rather than failed, using fault-tolerant execution. Resource groups give each tenant its own CPU and memory ceiling and concurrency limit, and keep interactive queries apart from batch refresh work so one cannot starve the other.
{ #performance-and-scale-engine-resources }

??? note "Requirements"

    - [Federated joins run well when the engine has statistics, so Provisa analyzes registered...](#performance-and-scale-optimizer-hints): [REQ-275](arch/requirements.md#REQ-275), [REQ-276](arch/requirements.md#REQ-276), [REQ-277](arch/requirements.md#REQ-277), [REQ-278](arch/requirements.md#REQ-278), [REQ-279](arch/requirements.md#REQ-279), [REQ-280](arch/requirements.md#REQ-280), [REQ-281](arch/requirements.md#REQ-281), [REQ-673](arch/requirements.md#REQ-673), [REQ-811](arch/requirements.md#REQ-811), [REQ-1688](arch/requirements.md#REQ-1688), [REQ-1876](arch/requirements.md#REQ-1876), [REQ-1902](arch/requirements.md#REQ-1902)
    - [Compiling and governing a query is cached so repeated shapes skip the work.](#performance-and-scale-compile-caches): [REQ-1338](arch/requirements.md#REQ-1338), [REQ-1866](arch/requirements.md#REQ-1866), [REQ-1877](arch/requirements.md#REQ-1877), [REQ-1886](arch/requirements.md#REQ-1886), [REQ-1892](arch/requirements.md#REQ-1892)
    - [One slow query does not block the others.](#performance-and-scale-concurrency): [REQ-052](arch/requirements.md#REQ-052), [REQ-053](arch/requirements.md#REQ-053), [REQ-054](arch/requirements.md#REQ-054), [REQ-1882](arch/requirements.md#REQ-1882), [REQ-1887](arch/requirements.md#REQ-1887), [REQ-1888](arch/requirements.md#REQ-1888), [REQ-1895](arch/requirements.md#REQ-1895), [REQ-1906](arch/requirements.md#REQ-1906), [REQ-1909](arch/requirements.md#REQ-1909)
    - [On Trino, work that exceeds memory is retried rather than failed, using fault-tolerant...](#performance-and-scale-engine-resources): [REQ-817](arch/requirements.md#REQ-817), [REQ-1040](arch/requirements.md#REQ-1040), [REQ-1042](arch/requirements.md#REQ-1042)

### High availability and multi-worker serving

Recovery works in two tiers. Reads that hit a transient error are retried with backoff for a bounded time. A watcher restarts failed components within a few minutes. Writes are never retried automatically, so a retry cannot double-apply a change. Machine and cluster failures stay with the operator's own platform. On the hosted platform, recovery times are published and measured in minutes, for the control plane, the admin database and the engine alike. A failover ends queries that were running at the time.
{ #high-availability-recovery }

One instance can run several worker processes that share the same ports, so every protocol uses all the cores of a machine. Each worker holds its own connections and event loop, and the embedded DuckDB store is arranged so workers never fight over its file. A change to the model or to governance made in any worker reaches every worker and every instance before the next request is served. A deployment can also split roles. Query nodes serve requests and nothing else, while coordinators run replica builds, view refreshes and scheduled jobs.
{ #high-availability-multi-worker }

Provisa is stateless in front of its control-plane database, so you can run any number of instances behind a load balancer. Secondary nodes read their configuration from the shared database, and only the primary writes it, with concurrent config loads serialized so they cannot collide. Trino scales out by adding stateless workers behind a single coordinator, and coordination of view refreshes is shared across instances.
{ #high-availability-fleet }

??? note "Requirements"

    - [Recovery works in two tiers.](#high-availability-recovery): [REQ-703](arch/requirements.md#REQ-703), [REQ-1459](arch/requirements.md#REQ-1459)
    - [One instance can run several worker processes that share the same ports, so every proto...](#high-availability-multi-worker): [REQ-1900](arch/requirements.md#REQ-1900), [REQ-1901](arch/requirements.md#REQ-1901), [REQ-1914](arch/requirements.md#REQ-1914), [REQ-1916](arch/requirements.md#REQ-1916)
    - [Provisa is stateless in front of its control-plane database, so you can run any number...](#high-availability-fleet): [REQ-057](arch/requirements.md#REQ-057), [REQ-879](arch/requirements.md#REQ-879), [REQ-1041](arch/requirements.md#REQ-1041)

## Live data

### Subscriptions and live queries

A client can subscribe to a table over server-sent events and receive each insert, update and delete as a JSON event. For PostgreSQL sources, Provisa installs triggers at startup and listens for notifications, so changes arrive without polling. Mutations made through Provisa notify as well. A subscription can select columns from related tables, and a trigger that cannot be installed is reported rather than silently skipped.
{ #subscriptions-and-live-queries-postgres-notify }

How changes are detected depends on the source. Databases with change capture use it: PostgreSQL through notifications, other relational databases through Debezium. Sources without it are polled by a single live-query engine that runs through the federation engine. You can set the delivery mode per table in the admin console, and the choice follows from the source type and whether the table has a watermark. Push-fed tables repeat their events. Tables with a watermark append new rows as they appear. Tables without one are compared as a whole, using a hash that ignores row order, and only real changes are sent.
{ #subscriptions-and-live-queries-delivery-modes }

A watermark column is a monotonically increasing value, such as an update timestamp, that tells Provisa which rows are new. Subscriptions need one. If the source has none, you can build one in a view, never at the source. Subscriptions deliver inserts and updates. Hard deletes are not delivered.
{ #subscriptions-and-live-queries-watermarks }

The same live query can feed a Kafka topic instead of a browser. A request header redirects a subscription's events to a Kafka topic, and a table's live output publishes its new rows there, governed as the role it names. The Kafka output and the event stream are two outputs of one mechanism.
{ #subscriptions-and-live-queries-sinks }

??? note "Requirements"

    - [A client can subscribe to a table over server-sent events and receive each insert, upda...](#subscriptions-and-live-queries-postgres-notify): [REQ-219](arch/requirements.md#REQ-219), [REQ-258](arch/requirements.md#REQ-258), [REQ-565](arch/requirements.md#REQ-565), [REQ-566](arch/requirements.md#REQ-566), [REQ-567](arch/requirements.md#REQ-567), [REQ-568](arch/requirements.md#REQ-568), [REQ-569](arch/requirements.md#REQ-569), [REQ-1515](arch/requirements.md#REQ-1515), [REQ-2054](arch/requirements.md#REQ-2054)
    - [How changes are detected depends on the source.](#subscriptions-and-live-queries-delivery-modes): [REQ-260](arch/requirements.md#REQ-260), [REQ-261](arch/requirements.md#REQ-261), [REQ-282](arch/requirements.md#REQ-282), [REQ-283](arch/requirements.md#REQ-283), [REQ-285](arch/requirements.md#REQ-285), [REQ-287](arch/requirements.md#REQ-287), [REQ-813](arch/requirements.md#REQ-813), [REQ-814](arch/requirements.md#REQ-814), [REQ-819](arch/requirements.md#REQ-819), [REQ-820](arch/requirements.md#REQ-820), [REQ-821](arch/requirements.md#REQ-821), [REQ-822](arch/requirements.md#REQ-822), [REQ-823](arch/requirements.md#REQ-823), [REQ-824](arch/requirements.md#REQ-824), [REQ-991](arch/requirements.md#REQ-991), [REQ-992](arch/requirements.md#REQ-992), [REQ-993](arch/requirements.md#REQ-993), [REQ-1770](arch/requirements.md#REQ-1770)
    - [A watermark column is a monotonically increasing value, such as an update timestamp, th...](#subscriptions-and-live-queries-watermarks): [REQ-926](arch/requirements.md#REQ-926), [REQ-927](arch/requirements.md#REQ-927), [REQ-928](arch/requirements.md#REQ-928), [REQ-931](arch/requirements.md#REQ-931)
    - [The same live query can feed a Kafka topic instead of a browser.](#subscriptions-and-live-queries-sinks): [REQ-286](arch/requirements.md#REQ-286), [REQ-570](arch/requirements.md#REQ-570), [REQ-812](arch/requirements.md#REQ-812)

### Change events and streams

A Kafka cluster registers as a source, and each topic becomes a table you can query. Schemas come from a Schema Registry when there is one, or you declare them by hand. A default time window keeps an unbounded topic from being scanned end to end, and a discriminator filter lets one topic with several message types appear as several tables.
{ #change-events-and-streams-kafka-topics }

Provisa can announce that a dataset changed. Each mutation emits a small event naming the table, with no row detail, to a Kafka topic you configure, and an external ETL job can send the same signal with a trivial mutation. A table can also publish its new rows to Kafka as JSON, one message per row, through a live output governed as a role; it is opt-in and can be added or removed at any time.
{ #change-events-and-streams-sinks-and-events }

Not every feed lives in a database. An ingest source is an HTTP endpoint that accepts JSON from collectors and webhooks. The steward declares the columns and where to find each in the payload. A WebSocket source connects out, reconnects on its own, and turns messages into rows. An RSS or Atom source polls a feed. All three become governed tables that can be queried and subscribed to like any other.
{ #change-events-and-streams-push-sources }

Change data capture lets a replica follow its source row by row. Debezium topics, including Avro-encoded ones, are read as rows; MongoDB change streams give near-real-time freshness; Kafka and WebSocket sources land through the same path. A source table has at most one watermark column, chosen from its existing columns, and it must only move forward. Content hashing suppresses no-op updates so downstream consumers are not woken for nothing.
{ #change-events-and-streams-cdc-and-watermarks }

A derived table is a view written in SQL that Provisa keeps current as its inputs change. Lineage comes from the SQL, so you do not wire up a pipeline. When an input changes, the dependent tables recompute, optionally only from the changed rows, and a failed run can be replayed. An optional processor step can validate, enrich or halt on bad data before it lands. Work is claimed so that exactly one worker handles each unit, and a crashed worker's claim is picked up by another. Preserved snapshots seal a table's state at a point in time.
{ #change-events-and-streams-derived-tables }

A derived table can be tied to a calendar instead of a fixed interval. Calendars are named and versioned, declared in the admin console, and support recurrence rules like those in a calendar app. A table that follows a calendar cuts a window at each boundary, such as end of day or end of quarter, and the window can be recomputed later by its address.
{ #change-events-and-streams-calendars }

??? note "Requirements"

    - [A Kafka cluster registers as a source, and each topic becomes a table you can query.](#change-events-and-streams-kafka-topics): [REQ-114](arch/requirements.md#REQ-114), [REQ-116](arch/requirements.md#REQ-116), [REQ-147](arch/requirements.md#REQ-147), [REQ-148](arch/requirements.md#REQ-148), [REQ-149](arch/requirements.md#REQ-149), [REQ-150](arch/requirements.md#REQ-150)
    - [Provisa can announce that a dataset changed.](#change-events-and-streams-sinks-and-events): [REQ-115](arch/requirements.md#REQ-115), [REQ-172](arch/requirements.md#REQ-172), [REQ-173](arch/requirements.md#REQ-173), [REQ-174](arch/requirements.md#REQ-174), [REQ-175](arch/requirements.md#REQ-175), [REQ-176](arch/requirements.md#REQ-176), [REQ-177](arch/requirements.md#REQ-177), [REQ-178](arch/requirements.md#REQ-178), [REQ-180](arch/requirements.md#REQ-180), [REQ-181](arch/requirements.md#REQ-181), [REQ-220](arch/requirements.md#REQ-220)
    - [Not every feed lives in a database.](#change-events-and-streams-push-sources): [REQ-331](arch/requirements.md#REQ-331), [REQ-332](arch/requirements.md#REQ-332), [REQ-333](arch/requirements.md#REQ-333), [REQ-334](arch/requirements.md#REQ-334), [REQ-335](arch/requirements.md#REQ-335), [REQ-336](arch/requirements.md#REQ-336), [REQ-337](arch/requirements.md#REQ-337), [REQ-338](arch/requirements.md#REQ-338), [REQ-339](arch/requirements.md#REQ-339), [REQ-340](arch/requirements.md#REQ-340), [REQ-341](arch/requirements.md#REQ-341), [REQ-342](arch/requirements.md#REQ-342), [REQ-343](arch/requirements.md#REQ-343), [REQ-344](arch/requirements.md#REQ-344), [REQ-1764](arch/requirements.md#REQ-1764), [REQ-1765](arch/requirements.md#REQ-1765), [REQ-1771](arch/requirements.md#REQ-1771)
    - [Change data capture lets a replica follow its source row by row.](#change-events-and-streams-cdc-and-watermarks): [REQ-922](arch/requirements.md#REQ-922), [REQ-924](arch/requirements.md#REQ-924), [REQ-925](arch/requirements.md#REQ-925), [REQ-981](arch/requirements.md#REQ-981), [REQ-982](arch/requirements.md#REQ-982), [REQ-1733](arch/requirements.md#REQ-1733), [REQ-1734](arch/requirements.md#REQ-1734), [REQ-1861](arch/requirements.md#REQ-1861), [REQ-1951](arch/requirements.md#REQ-1951)
    - [A derived table is a view written in SQL that Provisa keeps current as its inputs change.](#change-events-and-streams-derived-tables): [REQ-936](arch/requirements.md#REQ-936), [REQ-937](arch/requirements.md#REQ-937), [REQ-938](arch/requirements.md#REQ-938), [REQ-939](arch/requirements.md#REQ-939), [REQ-940](arch/requirements.md#REQ-940), [REQ-941](arch/requirements.md#REQ-941), [REQ-942](arch/requirements.md#REQ-942), [REQ-943](arch/requirements.md#REQ-943), [REQ-957](arch/requirements.md#REQ-957), [REQ-958](arch/requirements.md#REQ-958), [REQ-959](arch/requirements.md#REQ-959), [REQ-960](arch/requirements.md#REQ-960), [REQ-961](arch/requirements.md#REQ-961), [REQ-963](arch/requirements.md#REQ-963), [REQ-964](arch/requirements.md#REQ-964), [REQ-965](arch/requirements.md#REQ-965), [REQ-966](arch/requirements.md#REQ-966), [REQ-968](arch/requirements.md#REQ-968), [REQ-969](arch/requirements.md#REQ-969), [REQ-970](arch/requirements.md#REQ-970), [REQ-983](arch/requirements.md#REQ-983), [REQ-1165](arch/requirements.md#REQ-1165), [REQ-1662](arch/requirements.md#REQ-1662)
    - [A derived table can be tied to a calendar instead of a fixed interval.](#change-events-and-streams-calendars): [REQ-962](arch/requirements.md#REQ-962), [REQ-1166](arch/requirements.md#REQ-1166), [REQ-1167](arch/requirements.md#REQ-1167), [REQ-1168](arch/requirements.md#REQ-1168), [REQ-1169](arch/requirements.md#REQ-1169), [REQ-1171](arch/requirements.md#REQ-1171)

## Workspace

### Admin console

The console is where you run Provisa: register sources and tables, define relationships, set security, review creation requests and read reports. What a person sees depends on the roles they hold. Destructive actions ask for confirmation and say what will happen, and the test tools show the effect of row filters, hidden columns and schema scope as the role would see them. When the deployment stops answering, the whole application says so in one place, and a query waiting for a stopped engine to start tells you that. A user without the right to create something can file a request that an administrator approves or rejects with a reason.
{ #admin-console-surface }

Platform settings are edited in the console and persisted to the control plane. You can download and upload the configuration, or turn on live export that shows exactly what has changed since startup. AI model endpoints, including any provider reachable through an OpenAI-compatible or custom API, are assigned here for chat and embedding.
{ #admin-console-settings }

Everything the console does goes through an admin GraphQL API at its own endpoint, with an interactive GraphiQL page. Scripts and CI pipelines can call it directly.
{ #admin-console-api }

The interface follows one design language. Forms share a layout in which a field is as wide as what it holds. Each item opens in a detail panel with its name and actions pinned at the top and its sections in tabs. Creating an object asks only for what it needs, and editing happens on the page rather than in a modal. Light and dark themes are supported, and every tooltip looks the same.
{ #admin-console-design }

??? note "Requirements"

    - [The console is where you run Provisa: register sources and tables, define relationships...](#admin-console-surface): [REQ-058](arch/requirements.md#REQ-058), [REQ-060](arch/requirements.md#REQ-060), [REQ-061](arch/requirements.md#REQ-061), [REQ-062](arch/requirements.md#REQ-062), [REQ-063](arch/requirements.md#REQ-063), [REQ-401](arch/requirements.md#REQ-401), [REQ-1085](arch/requirements.md#REQ-1085), [REQ-1351](arch/requirements.md#REQ-1351), [REQ-1390](arch/requirements.md#REQ-1390), [REQ-1433](arch/requirements.md#REQ-1433), [REQ-1466](arch/requirements.md#REQ-1466), [REQ-1514](arch/requirements.md#REQ-1514), [REQ-1516](arch/requirements.md#REQ-1516), [REQ-1814](arch/requirements.md#REQ-1814), [REQ-1840](arch/requirements.md#REQ-1840), [REQ-1940](arch/requirements.md#REQ-1940)
    - [Platform settings are edited in the console and persisted to the control plane.](#admin-console-settings): [REQ-164](arch/requirements.md#REQ-164), [REQ-165](arch/requirements.md#REQ-165), [REQ-1080](arch/requirements.md#REQ-1080), [REQ-1096](arch/requirements.md#REQ-1096), [REQ-1790](arch/requirements.md#REQ-1790)
    - [Everything the console does goes through an admin GraphQL API at its own endpoint, with...](#admin-console-api): [REQ-533](arch/requirements.md#REQ-533), [REQ-620](arch/requirements.md#REQ-620), [REQ-622](arch/requirements.md#REQ-622)
    - [The interface follows one design language.](#admin-console-design): [REQ-1009](arch/requirements.md#REQ-1009), [REQ-1010](arch/requirements.md#REQ-1010), [REQ-1011](arch/requirements.md#REQ-1011), [REQ-1520](arch/requirements.md#REQ-1520), [REQ-1559](arch/requirements.md#REQ-1559), [REQ-1968](arch/requirements.md#REQ-1968), [REQ-1970](arch/requirements.md#REQ-1970), [REQ-1971](arch/requirements.md#REQ-1971), [REQ-1972](arch/requirements.md#REQ-1972), [REQ-1973](arch/requirements.md#REQ-1973), [REQ-1974](arch/requirements.md#REQ-1974), [REQ-1975](arch/requirements.md#REQ-1975), [REQ-1977](arch/requirements.md#REQ-1977)

### Explorers

The Graph Explorer draws a Cypher result as a graph. Statistics for what is on the canvas and each node's degree are computed in the browser, with no extra query. You can color and group nodes by any property, collapse a group into a single node, and size nodes by a number. These are view changes only, and the underlying data is untouched.
{ #explorers-graph }

Queries worth keeping can be saved as favorites in the Graph Explorer. A favorite has a label you can rename, loads into the editor on click, and can be run or deleted in place.
{ #explorers-favorites }

Each way to query Provisa has a page in the browser. The gRPC explorer runs calls against the per-role service, the JSON:API explorer composes requests, and the OpenAPI page serves Swagger UI against a spec generated for your role. GraphiQL includes a view of the compiled, governed SQL. The explorers sit outside the form and detail-panel conventions of the rest of the console.
{ #explorers-query-pages }

Results from the SQL explorer, reports, and table or view previews share one grid. It sorts, filters per column, and groups by several levels. Previews are paged on the server, so the governed viewer reads only the page you are looking at. Timestamps are shown the way a person reads them, the page size is yours to choose, and a filter that matches nothing leaves the grid in place with a clear-filters button. Export to Excel or CSV covers the whole relation under your current filter and sort, and the workbook has a second sheet recording how it was produced.
{ #explorers-results-grid }

Explore's Python and Notebook tabs run Python in your browser over the data your role is served. The Python tab runs a script, each run starting fresh, that reads a statement into a pandas DataFrame as you, with your current role, org and environment, and shows what it prints, its tables and its matplotlib figures. It holds several scripts as tabs, each with its own code and last output, kept in your browser across reloads. The Notebook tab is a Jupyter notebook (cells, and %%sql, %%graphql and %%cypher magics) that reads as you the same way. Both stay as you left them while you visit other pages, open to 95% of the window, and Polly can write code into either, run it and read what it returned. Python and its packages (pandas, pyarrow, scikit-learn, matplotlib) are served by Provisa itself, so it works with no internet access. They come with every install; an install missing them says so.
{ #explorers-data-science }

??? note "Requirements"

    - [The Graph Explorer draws a Cypher result as a graph.](#explorers-graph): [REQ-396](arch/requirements.md#REQ-396), [REQ-397](arch/requirements.md#REQ-397), [REQ-398](arch/requirements.md#REQ-398), [REQ-410](arch/requirements.md#REQ-410), [REQ-446](arch/requirements.md#REQ-446), [REQ-642](arch/requirements.md#REQ-642), [REQ-643](arch/requirements.md#REQ-643), [REQ-644](arch/requirements.md#REQ-644), [REQ-645](arch/requirements.md#REQ-645), [REQ-646](arch/requirements.md#REQ-646), [REQ-647](arch/requirements.md#REQ-647), [REQ-648](arch/requirements.md#REQ-648), [REQ-649](arch/requirements.md#REQ-649), [REQ-784](arch/requirements.md#REQ-784), [REQ-785](arch/requirements.md#REQ-785), [REQ-786](arch/requirements.md#REQ-786), [REQ-794](arch/requirements.md#REQ-794), [REQ-1631](arch/requirements.md#REQ-1631), [REQ-1724](arch/requirements.md#REQ-1724)
    - [Queries worth keeping can be saved as favorites in the Graph Explorer.](#explorers-favorites): [REQ-779](arch/requirements.md#REQ-779), [REQ-780](arch/requirements.md#REQ-780), [REQ-781](arch/requirements.md#REQ-781), [REQ-782](arch/requirements.md#REQ-782), [REQ-783](arch/requirements.md#REQ-783)
    - [Each way to query Provisa has a page in the browser.](#explorers-query-pages): [REQ-163](arch/requirements.md#REQ-163), [REQ-248](arch/requirements.md#REQ-248), [REQ-799](arch/requirements.md#REQ-799), [REQ-800](arch/requirements.md#REQ-800), [REQ-801](arch/requirements.md#REQ-801), [REQ-1521](arch/requirements.md#REQ-1521), [REQ-1976](arch/requirements.md#REQ-1976)
    - [Results from the SQL explorer, reports, and table or view previews share one grid.](#explorers-results-grid): [REQ-1391](arch/requirements.md#REQ-1391), [REQ-1392](arch/requirements.md#REQ-1392), [REQ-1436](arch/requirements.md#REQ-1436), [REQ-1437](arch/requirements.md#REQ-1437), [REQ-1438](arch/requirements.md#REQ-1438), [REQ-1440](arch/requirements.md#REQ-1440), [REQ-1441](arch/requirements.md#REQ-1441), [REQ-1442](arch/requirements.md#REQ-1442), [REQ-1444](arch/requirements.md#REQ-1444), [REQ-1937](arch/requirements.md#REQ-1937)
    - [Explore's Python and Notebook tabs run Python in your browser over the data your role i...](#explorers-data-science): [REQ-2071](arch/requirements.md#REQ-2071)

### Chat assistant

Polly, the chat assistant, lives in a panel docked beside whatever page you are on, and she knows which page that is without being told. The panel suggests questions when it opens empty, accepts voice input through the browser, and checks that a model is configured before it opens. Messages can be copied, the conversation can be cleared, and while she works the panel shows each tool call as a badge with a spinner.
{ #chat-assistant-panel }

When Polly needs a decision from you, she shows a choice widget in the panel: yes or no, one of several, or several of many, with a free-text option for anything else. Questions and answers are recorded in the transcript. Some tools pause the model mid-turn so the browser can act, for example to open a page or refresh a view.
{ #chat-assistant-choices }

The assistant runs on the model your organization configures, whether that is Anthropic, Ollama, OpenRouter, or any endpoint that speaks an OpenAI-compatible API. Tool calling works across vendors. With Anthropic, answers stream token by token and hosted web search and fetch are available.
{ #chat-assistant-models }

Polly can do the work as well as describe it. She can search for data sources, register a source or table, create data products, metrics and glossary terms, and build a query and open it in the right explorer with the query filled in. Everything she does runs with the signed-in person's own roles and capabilities, so she can never do more than you could. Admin pages refresh on their own after she changes something.
{ #chat-assistant-actions }

??? note "Requirements"

    - [Polly, the chat assistant, lives in a panel docked beside whatever page you are on, and...](#chat-assistant-panel): [REQ-1092](arch/requirements.md#REQ-1092), [REQ-1800](arch/requirements.md#REQ-1800), [REQ-1803](arch/requirements.md#REQ-1803), [REQ-1804](arch/requirements.md#REQ-1804), [REQ-1805](arch/requirements.md#REQ-1805), [REQ-1806](arch/requirements.md#REQ-1806), [REQ-1811](arch/requirements.md#REQ-1811), [REQ-1813](arch/requirements.md#REQ-1813), [REQ-1815](arch/requirements.md#REQ-1815), [REQ-1821](arch/requirements.md#REQ-1821), [REQ-1822](arch/requirements.md#REQ-1822), [REQ-1823](arch/requirements.md#REQ-1823), [REQ-1828](arch/requirements.md#REQ-1828), [REQ-1836](arch/requirements.md#REQ-1836), [REQ-1843](arch/requirements.md#REQ-1843)
    - [When Polly needs a decision from you, she shows a choice widget in the panel: yes or no...](#chat-assistant-choices): [REQ-1795](arch/requirements.md#REQ-1795), [REQ-1810](arch/requirements.md#REQ-1810), [REQ-1816](arch/requirements.md#REQ-1816), [REQ-1817](arch/requirements.md#REQ-1817), [REQ-1818](arch/requirements.md#REQ-1818)
    - [The assistant runs on the model your organization configures, whether that is Anthropic...](#chat-assistant-models): [REQ-1794](arch/requirements.md#REQ-1794), [REQ-1796](arch/requirements.md#REQ-1796), [REQ-1797](arch/requirements.md#REQ-1797), [REQ-1808](arch/requirements.md#REQ-1808), [REQ-1809](arch/requirements.md#REQ-1809), [REQ-1825](arch/requirements.md#REQ-1825), [REQ-1839](arch/requirements.md#REQ-1839), [REQ-1841](arch/requirements.md#REQ-1841), [REQ-2012](arch/requirements.md#REQ-2012)
    - [Polly can do the work as well as describe it.](#chat-assistant-actions): [REQ-1798](arch/requirements.md#REQ-1798), [REQ-1799](arch/requirements.md#REQ-1799), [REQ-1812](arch/requirements.md#REQ-1812), [REQ-1820](arch/requirements.md#REQ-1820), [REQ-1826](arch/requirements.md#REQ-1826), [REQ-1832](arch/requirements.md#REQ-1832), [REQ-1833](arch/requirements.md#REQ-1833), [REQ-1834](arch/requirements.md#REQ-1834), [REQ-1835](arch/requirements.md#REQ-1835), [REQ-1838](arch/requirements.md#REQ-1838), [REQ-1842](arch/requirements.md#REQ-1842), [REQ-1845](arch/requirements.md#REQ-1845), [REQ-1846](arch/requirements.md#REQ-1846), [REQ-1847](arch/requirements.md#REQ-1847), [REQ-1848](arch/requirements.md#REQ-1848), [REQ-1849](arch/requirements.md#REQ-1849), [REQ-1850](arch/requirements.md#REQ-1850), [REQ-1851](arch/requirements.md#REQ-1851), [REQ-1852](arch/requirements.md#REQ-1852), [REQ-1853](arch/requirements.md#REQ-1853), [REQ-1854](arch/requirements.md#REQ-1854), [REQ-1855](arch/requirements.md#REQ-1855), [REQ-1856](arch/requirements.md#REQ-1856)

### Product tour and named demos

A new user is offered a short core tour: welcome, Polly, connecting a source, and running a query. Every step has a Deep Dives button that jumps to a menu of topic tours. Each step waits until the page it points at is loaded and ready, and the tour is never offered over a sign-in or registration form.
{ #product-tour-and-demos-guided-tour }

You can start Provisa already loaded with a demo. Running the launcher with a demo name picks one of the bundled scenarios, and plain demo mode loads the default. In demo mode a complete configuration is loaded instead of the first-run wizard, and tier restrictions are lifted so every feature can be shown. Each demo, instance and organization keeps what you left in the browser, such as the domain filter, saved query tabs and open panels, so switching demos carries none of it across and coming back restores it. A named demo can also tell its own story, a guided track over its own sources, queries and roles, offered before the product tour, with the Deep Dives after it.
{ #product-tour-and-demos-named-demos }

On the hosted service, a prospect can try Provisa with no signup in an ephemeral sandbox cloned from a prepared template. The sandbox is deleted on logout or after a period of inactivity, has caps on time, rows and cost, and can only use the bundled sample sources.
{ #product-tour-and-demos-sandbox }

??? note "Requirements"

    - [A new user is offered a short core tour: welcome, Polly, connecting a source, and runni...](#product-tour-and-demos-guided-tour): [REQ-1362](arch/requirements.md#REQ-1362), [REQ-1446](arch/requirements.md#REQ-1446), [REQ-1570](arch/requirements.md#REQ-1570), [REQ-1945](arch/requirements.md#REQ-1945), [REQ-2013](arch/requirements.md#REQ-2013)
    - [You can start Provisa already loaded with a demo.](#product-tour-and-demos-named-demos): [REQ-414](arch/requirements.md#REQ-414), [REQ-1220](arch/requirements.md#REQ-1220), [REQ-1858](arch/requirements.md#REQ-1858), [REQ-2008](arch/requirements.md#REQ-2008), [REQ-2009](arch/requirements.md#REQ-2009), [REQ-2010](arch/requirements.md#REQ-2010), [REQ-2016](arch/requirements.md#REQ-2016), [REQ-2030](arch/requirements.md#REQ-2030), [REQ-2032](arch/requirements.md#REQ-2032), [REQ-2033](arch/requirements.md#REQ-2033), [REQ-2034](arch/requirements.md#REQ-2034), [REQ-2035](arch/requirements.md#REQ-2035), [REQ-2036](arch/requirements.md#REQ-2036), [REQ-2037](arch/requirements.md#REQ-2037), [REQ-2038](arch/requirements.md#REQ-2038), [REQ-2039](arch/requirements.md#REQ-2039), [REQ-2040](arch/requirements.md#REQ-2040), [REQ-2041](arch/requirements.md#REQ-2041), [REQ-2042](arch/requirements.md#REQ-2042), [REQ-2043](arch/requirements.md#REQ-2043), [REQ-2044](arch/requirements.md#REQ-2044), [REQ-2045](arch/requirements.md#REQ-2045), [REQ-2046](arch/requirements.md#REQ-2046), [REQ-2047](arch/requirements.md#REQ-2047), [REQ-2048](arch/requirements.md#REQ-2048), [REQ-2051](arch/requirements.md#REQ-2051), [REQ-2052](arch/requirements.md#REQ-2052), [REQ-2053](arch/requirements.md#REQ-2053), [REQ-2056](arch/requirements.md#REQ-2056), [REQ-2063](arch/requirements.md#REQ-2063), [REQ-2067](arch/requirements.md#REQ-2067), [REQ-2083](arch/requirements.md#REQ-2083), [REQ-2084](arch/requirements.md#REQ-2084), [REQ-2085](arch/requirements.md#REQ-2085)
    - [On the hosted service, a prospect can try Provisa with no signup in an ephemeral sandbo...](#product-tour-and-demos-sandbox): [REQ-1033](arch/requirements.md#REQ-1033), [REQ-1034](arch/requirements.md#REQ-1034), [REQ-1036](arch/requirements.md#REQ-1036), [REQ-1037](arch/requirements.md#REQ-1037), [REQ-1039](arch/requirements.md#REQ-1039), [REQ-2006](arch/requirements.md#REQ-2006), [REQ-2049](arch/requirements.md#REQ-2049), [REQ-2065](arch/requirements.md#REQ-2065), [REQ-2068](arch/requirements.md#REQ-2068)

### Internationalization and accessibility

The interface is translated into many languages and picks yours from the browser's language setting. No user-facing string is hard-coded. Translations are produced in a working session by the agent and kept in catalogs, with English as the source. Server errors carry stable codes and parameters, so the interface can show them in your language, and the guided tour is translated too.
{ #i18n-and-accessibility-locales }

Languages written right to left, such as Hebrew, get a mirrored layout, not just translated text.
{ #i18n-and-accessibility-rtl }

The interface targets WCAG Level AA, including focus management and keyboard operation. Automated axe-core checks run in the browser tests, so a regression fails the build.
{ #i18n-and-accessibility-accessibility }

??? note "Requirements"

    - [The interface is translated into many languages and picks yours from the browser's lang...](#i18n-and-accessibility-locales): [REQ-1012](arch/requirements.md#REQ-1012), [REQ-1341](arch/requirements.md#REQ-1341), [REQ-1342](arch/requirements.md#REQ-1342), [REQ-1350](arch/requirements.md#REQ-1350), [REQ-1356](arch/requirements.md#REQ-1356), [REQ-1358](arch/requirements.md#REQ-1358)
    - [Languages written right to left, such as Hebrew, get a mirrored layout, not just transl...](#i18n-and-accessibility-rtl): [REQ-1357](arch/requirements.md#REQ-1357)
    - [The interface targets WCAG Level AA, including focus management and keyboard operation.](#i18n-and-accessibility-accessibility): [REQ-1013](arch/requirements.md#REQ-1013), [REQ-1014](arch/requirements.md#REQ-1014)

## Identity and organizations

### Sign-in and identity

Provisa signs people in through the identity provider a deployment already has: Firebase, Keycloak, any OIDC provider such as Okta or Azure AD, a simple username-and-password option for testing, or no provider at all in development. The provider is chosen in the admin UI, or by a setting when a cloud deployment is provisioned. One built image can run unsecured, with basic auth or behind a full identity provider, and the login page offers every provider the server advertises at once, so Google, GitHub and email can sit side by side.
{ #sign-in-and-identity-providers }

Every request carries a credential, and every credential belongs to exactly one organization. Browsers use a session bearer token. Programs that speak pgwire, Bolt, Arrow Flight or gRPC use personal access tokens meant to last. Roles come either straight from token claims or from assignments Provisa holds itself. Failed attempts are counted and locked out in one place for every surface, and pgwire can prove a local password with SCRAM so it never crosses the wire. The health and setup-status endpoints stay open so load balancers and installers can reach them.
{ #sign-in-and-identity-credentials }

A new deployment has no administrator until someone claims the slot. Claiming platform administration is an explicit act, never a side effect of signing in, and the person who does lands in a populated deployment with a root org to work in. A seeded break-glass account with a username and password signs in under any provider, so a broken identity provider never locks the operator out. A backup administrator is added by inviting them, not by a second claim.
{ #sign-in-and-identity-bootstrap }

Signing in and out behaves the same way everywhere. Interactive sign-in happens only on the control-plane host, and an org's own address borrows that session instead of running a login of its own. Signing out ends the session where it lives. The sign-in card disables its controls while a sign-in runs, the login and registration pages render without the app shell, and the guided tour is never offered over a sign-in form. A stored token is dropped only when the provider says it is dead, and an identity check that has not finished never shows a 'not authorized' screen. A first Firebase login creates the user's profile without creating a password.
{ #sign-in-and-identity-sessions }

??? note "Requirements"

    - [Provisa signs people in through the identity provider a deployment already has: Firebas...](#sign-in-and-identity-providers): [REQ-120](arch/requirements.md#REQ-120), [REQ-121](arch/requirements.md#REQ-121), [REQ-122](arch/requirements.md#REQ-122), [REQ-123](arch/requirements.md#REQ-123), [REQ-124](arch/requirements.md#REQ-124), [REQ-535](arch/requirements.md#REQ-535), [REQ-919](arch/requirements.md#REQ-919), [REQ-1182](arch/requirements.md#REQ-1182), [REQ-1262](arch/requirements.md#REQ-1262), [REQ-1267](arch/requirements.md#REQ-1267), [REQ-1325](arch/requirements.md#REQ-1325), [REQ-1963](arch/requirements.md#REQ-1963)
    - [Every request carries a credential, and every credential belongs to exactly one organiz...](#sign-in-and-identity-credentials): [REQ-539](arch/requirements.md#REQ-539), [REQ-551](arch/requirements.md#REQ-551), [REQ-594](arch/requirements.md#REQ-594), [REQ-606](arch/requirements.md#REQ-606), [REQ-1232](arch/requirements.md#REQ-1232), [REQ-1263](arch/requirements.md#REQ-1263), [REQ-1295](arch/requirements.md#REQ-1295), [REQ-1393](arch/requirements.md#REQ-1393), [REQ-1394](arch/requirements.md#REQ-1394), [REQ-1434](arch/requirements.md#REQ-1434), [REQ-1936](arch/requirements.md#REQ-1936)
    - [A new deployment has no administrator until someone claims the slot.](#sign-in-and-identity-bootstrap): [REQ-125](arch/requirements.md#REQ-125), [REQ-1264](arch/requirements.md#REQ-1264), [REQ-1288](arch/requirements.md#REQ-1288), [REQ-1290](arch/requirements.md#REQ-1290), [REQ-1291](arch/requirements.md#REQ-1291), [REQ-1292](arch/requirements.md#REQ-1292), [REQ-1294](arch/requirements.md#REQ-1294), [REQ-1296](arch/requirements.md#REQ-1296), [REQ-1472](arch/requirements.md#REQ-1472)
    - [Signing in and out behaves the same way everywhere.](#sign-in-and-identity-sessions): [REQ-1246](arch/requirements.md#REQ-1246), [REQ-1247](arch/requirements.md#REQ-1247), [REQ-1326](arch/requirements.md#REQ-1326), [REQ-1348](arch/requirements.md#REQ-1348), [REQ-1430](arch/requirements.md#REQ-1430), [REQ-1571](arch/requirements.md#REQ-1571), [REQ-1603](arch/requirements.md#REQ-1603), [REQ-1721](arch/requirements.md#REQ-1721)

### Organizations

Each organization is an isolated tenant. Its model, cache entries, audit log and materialized data live in its own schema, under its own database role, with its own cache-key and object-store prefix, so no query from another org can reach them. Where a backend has no schemas, each org gets a database of its own. An org on the external-engine lane can run an engine of its own kind, and its sources are provisioned onto that engine even while it sleeps. Creating an org builds all of this in the background and tells the creator when it is ready.
{ #organizations-tenancy }

An org is addressed by its subdomain, and the subdomain only names the org; it never authorizes. The credential must belong to a member of that org, and a header naming another org is checked against membership. Wire protocols cross-check the org named in the TLS server name against the credential and refuse a mismatch. A request acts in the org it names, with no implied org, and a platform administrator can act across orgs without being able to read an org's data unless they are a member there.
{ #organizations-addressing }

People get into an org by invitation, by asking, or by an email rule. A signed-in person with no org accepts a pending invitation or creates an org of their own, and where an org takes requests, an admin approves or denies them. An org can set an email-address rule and an auto-join role, with the breadth of that rule acknowledged by the org that writes it. An invitation is itself the admission decision and can be sent as a message, limited to roles the inviter may confer, and given its own lifetime and number of uses.
{ #organizations-membership }

Org admins run their team from a Team page: create, copy and revoke invitation links and choose the role each invitee receives, and remove members. An org can never be left without an admin by an ordinary act, members can leave on their own, and an org id is checked when it is created and never changes. Deleting an org drops its tenant data and scrubs personal information while keeping the billing record and a platform audit entry that the deletion happened.
{ #organizations-administration }

A public Try it Out link gives each visitor a sandbox. Startup builds the sandbox org itself, and a single invitation admits many people, each into an environment named after them. The environment lasts for a span of use rather than a fixed deadline, so an hour of work does not end it, and reopening the link finds the environment the visitor already holds. Any org admin can mint such an invitation from their own Team page, deployment administrators may use the sandbox, and a visitor's account is deleted when they leave.
{ #organizations-sandbox }

An org can make the workspace its own. An admin sets a display name and logo for the surfaces its people meet, including the glossary and Explore pages of a data portal. On a multi-tenant deployment the org also picks the AI model it uses, with its own API key per vendor and a model picker read live from the vendor. Files that Provisa generates for an org, such as conversion caches and crawl downloads, are kept under the org.
{ #organizations-customization }

??? note "Requirements"

    - [Each organization is an isolated tenant.](#organizations-tenancy): [REQ-456](arch/requirements.md#REQ-456), [REQ-461](arch/requirements.md#REQ-461), [REQ-462](arch/requirements.md#REQ-462), [REQ-513](arch/requirements.md#REQ-513), [REQ-591](arch/requirements.md#REQ-591), [REQ-592](arch/requirements.md#REQ-592), [REQ-695](arch/requirements.md#REQ-695), [REQ-696](arch/requirements.md#REQ-696), [REQ-697](arch/requirements.md#REQ-697), [REQ-698](arch/requirements.md#REQ-698), [REQ-699](arch/requirements.md#REQ-699), [REQ-700](arch/requirements.md#REQ-700), [REQ-701](arch/requirements.md#REQ-701), [REQ-702](arch/requirements.md#REQ-702), [REQ-833](arch/requirements.md#REQ-833), [REQ-838](arch/requirements.md#REQ-838), [REQ-839](arch/requirements.md#REQ-839), [REQ-851](arch/requirements.md#REQ-851), [REQ-852](arch/requirements.md#REQ-852), [REQ-1043](arch/requirements.md#REQ-1043), [REQ-1243](arch/requirements.md#REQ-1243), [REQ-1244](arch/requirements.md#REQ-1244), [REQ-1268](arch/requirements.md#REQ-1268), [REQ-1269](arch/requirements.md#REQ-1269), [REQ-1270](arch/requirements.md#REQ-1270), [REQ-1272](arch/requirements.md#REQ-1272), [REQ-1273](arch/requirements.md#REQ-1273), [REQ-1286](arch/requirements.md#REQ-1286), [REQ-1293](arch/requirements.md#REQ-1293), [REQ-1301](arch/requirements.md#REQ-1301), [REQ-1315](arch/requirements.md#REQ-1315), [REQ-1339](arch/requirements.md#REQ-1339), [REQ-1343](arch/requirements.md#REQ-1343), [REQ-1344](arch/requirements.md#REQ-1344), [REQ-1355](arch/requirements.md#REQ-1355), [REQ-1412](arch/requirements.md#REQ-1412), [REQ-1418](arch/requirements.md#REQ-1418), [REQ-1421](arch/requirements.md#REQ-1421), [REQ-1427](arch/requirements.md#REQ-1427), [REQ-1540](arch/requirements.md#REQ-1540), [REQ-1930](arch/requirements.md#REQ-1930)
    - [An org is addressed by its subdomain, and the subdomain only names the org; it never au...](#organizations-addressing): [REQ-515](arch/requirements.md#REQ-515), [REQ-1054](arch/requirements.md#REQ-1054), [REQ-1055](arch/requirements.md#REQ-1055), [REQ-1056](arch/requirements.md#REQ-1056), [REQ-1061](arch/requirements.md#REQ-1061), [REQ-1233](arch/requirements.md#REQ-1233), [REQ-1234](arch/requirements.md#REQ-1234), [REQ-1235](arch/requirements.md#REQ-1235), [REQ-1236](arch/requirements.md#REQ-1236), [REQ-1237](arch/requirements.md#REQ-1237), [REQ-1238](arch/requirements.md#REQ-1238), [REQ-1241](arch/requirements.md#REQ-1241), [REQ-1242](arch/requirements.md#REQ-1242), [REQ-1276](arch/requirements.md#REQ-1276), [REQ-1605](arch/requirements.md#REQ-1605), [REQ-1935](arch/requirements.md#REQ-1935)
    - [People get into an org by invitation, by asking, or by an email rule.](#organizations-membership): [REQ-514](arch/requirements.md#REQ-514), [REQ-516](arch/requirements.md#REQ-516), [REQ-1017](arch/requirements.md#REQ-1017), [REQ-1248](arch/requirements.md#REQ-1248), [REQ-1249](arch/requirements.md#REQ-1249), [REQ-1250](arch/requirements.md#REQ-1250), [REQ-1251](arch/requirements.md#REQ-1251), [REQ-1252](arch/requirements.md#REQ-1252), [REQ-1255](arch/requirements.md#REQ-1255), [REQ-1261](arch/requirements.md#REQ-1261), [REQ-1271](arch/requirements.md#REQ-1271), [REQ-1275](arch/requirements.md#REQ-1275), [REQ-1277](arch/requirements.md#REQ-1277), [REQ-1282](arch/requirements.md#REQ-1282), [REQ-1284](arch/requirements.md#REQ-1284), [REQ-1285](arch/requirements.md#REQ-1285), [REQ-1287](arch/requirements.md#REQ-1287), [REQ-1289](arch/requirements.md#REQ-1289), [REQ-1310](arch/requirements.md#REQ-1310), [REQ-1313](arch/requirements.md#REQ-1313), [REQ-1314](arch/requirements.md#REQ-1314), [REQ-1477](arch/requirements.md#REQ-1477), [REQ-1478](arch/requirements.md#REQ-1478), [REQ-1497](arch/requirements.md#REQ-1497), [REQ-1567](arch/requirements.md#REQ-1567), [REQ-1568](arch/requirements.md#REQ-1568), [REQ-1569](arch/requirements.md#REQ-1569), [REQ-1572](arch/requirements.md#REQ-1572), [REQ-1604](arch/requirements.md#REQ-1604), [REQ-1616](arch/requirements.md#REQ-1616), [REQ-1696](arch/requirements.md#REQ-1696)
    - [Org admins run their team from a Team page: create, copy and revoke invitation links an...](#organizations-administration): [REQ-1019](arch/requirements.md#REQ-1019), [REQ-1274](arch/requirements.md#REQ-1274), [REQ-1283](arch/requirements.md#REQ-1283), [REQ-1298](arch/requirements.md#REQ-1298), [REQ-1300](arch/requirements.md#REQ-1300), [REQ-1302](arch/requirements.md#REQ-1302), [REQ-1304](arch/requirements.md#REQ-1304), [REQ-1305](arch/requirements.md#REQ-1305), [REQ-1306](arch/requirements.md#REQ-1306), [REQ-1307](arch/requirements.md#REQ-1307), [REQ-1309](arch/requirements.md#REQ-1309), [REQ-1311](arch/requirements.md#REQ-1311), [REQ-1312](arch/requirements.md#REQ-1312)
    - [A public Try it Out link gives each visitor a sandbox.](#organizations-sandbox): [REQ-1594](arch/requirements.md#REQ-1594), [REQ-1598](arch/requirements.md#REQ-1598), [REQ-1599](arch/requirements.md#REQ-1599), [REQ-1600](arch/requirements.md#REQ-1600), [REQ-1601](arch/requirements.md#REQ-1601), [REQ-1615](arch/requirements.md#REQ-1615), [REQ-1931](arch/requirements.md#REQ-1931)
    - [An org can make the workspace its own.](#organizations-customization): [REQ-1062](arch/requirements.md#REQ-1062), [REQ-1395](arch/requirements.md#REQ-1395), [REQ-1398](arch/requirements.md#REQ-1398), [REQ-1409](arch/requirements.md#REQ-1409), [REQ-1486](arch/requirements.md#REQ-1486), [REQ-1992](arch/requirements.md#REQ-1992)

### Hasura migration

Point the converter at a Hasura v2 metadata export or a DDN supergraph project and get Provisa YAML back. Tracked tables, relationships with their cardinality, source kinds, column naming and aggregate expressions are carried across, DDN field names are resolved to physical columns, and Hasura filter expressions become SQL. The result either validates as Provisa config or does not come out at all.
{ #hasura-migration-converters }

Permissions come across in Provisa's own vocabulary. Select permissions become per-role column visibility and row-level security rules, insert and update permissions become writable columns, and any delete permission raises the role to write. JWT settings become an OAuth provider with role mapping, and the admin secret becomes the superuser. Roles are collected only from permission entries, so a role nobody granted anything is not invented.
{ #hasura-migration-permissions }

The same conversion runs from the admin console as Import Hasura Config, so a migration no longer needs shell access to the deployment. The preview lists each converted SQL source with the connection the conversion guessed, ready to edit, and then types every column from the live source. Remote schemas land as GraphQL remote sources with a table per query root field, and a relationship declared only by a foreign-key column is resolved from the export itself.
{ #hasura-migration-import }

What cannot be mapped is reported, not hidden. Event triggers, cron triggers, boolean expression types and webhook-backed actions produce warnings rather than failed conversions, and stored-procedure actions become tracked functions. Connection credentials are placeholders that read from environment variables, to be filled in before the config is used. A model that points at an unknown object type is skipped with a warning.
{ #hasura-migration-gaps }

??? note "Requirements"

    - [Point the converter at a Hasura v2 metadata export or a DDN supergraph project and get...](#hasura-migration-converters): [REQ-182](arch/requirements.md#REQ-182), [REQ-183](arch/requirements.md#REQ-183), [REQ-184](arch/requirements.md#REQ-184), [REQ-188](arch/requirements.md#REQ-188), [REQ-189](arch/requirements.md#REQ-189), [REQ-191](arch/requirements.md#REQ-191), [REQ-193](arch/requirements.md#REQ-193), [REQ-415](arch/requirements.md#REQ-415), [REQ-417](arch/requirements.md#REQ-417), [REQ-623](arch/requirements.md#REQ-623), [REQ-627](arch/requirements.md#REQ-627), [REQ-629](arch/requirements.md#REQ-629)
    - [Permissions come across in Provisa's own vocabulary.](#hasura-migration-permissions): [REQ-185](arch/requirements.md#REQ-185), [REQ-186](arch/requirements.md#REQ-186), [REQ-187](arch/requirements.md#REQ-187), [REQ-190](arch/requirements.md#REQ-190), [REQ-624](arch/requirements.md#REQ-624), [REQ-626](arch/requirements.md#REQ-626), [REQ-1684](arch/requirements.md#REQ-1684)
    - [The same conversion runs from the admin console as Import Hasura Config, so a migration...](#hasura-migration-import): [REQ-1483](arch/requirements.md#REQ-1483), [REQ-1680](arch/requirements.md#REQ-1680), [REQ-1681](arch/requirements.md#REQ-1681), [REQ-1687](arch/requirements.md#REQ-1687), [REQ-1691](arch/requirements.md#REQ-1691)
    - [What cannot be mapped is reported, not hidden.](#hasura-migration-gaps): [REQ-192](arch/requirements.md#REQ-192), [REQ-621](arch/requirements.md#REQ-621), [REQ-625](arch/requirements.md#REQ-625), [REQ-628](arch/requirements.md#REQ-628), [REQ-920](arch/requirements.md#REQ-920)

## Install and operate

### Installation

The desktop installer is a single download that extracts into a hidden Provisa directory and is driven by start, stop, status and open commands. By default it runs everything natively with no Docker: DuckDB as the federation engine, an embedded PostgreSQL control plane and an in-memory cache. Trino on Docker or an external engine are opt-in choices in the setup wizard, and Docker or a VM is provisioned only when one of those, or the demo, needs it. The demo is optional and off by default; when it is installed the launcher opens the guided tour.
{ #installation-desktop }

On first launch the native installers show what is happening instead of running hidden. The Windows launcher puts up a startup window with breadcrumbs for each stage, waits for the demo mock servers to answer before starting the API, kills every process holding the runtime before an upgrade, and leaves opening the browser to the startup window when it is in charge. Afterwards it tells the user which optional pieces, such as the federation engine, still need Docker.
{ #installation-windows-startup }

Provisa also installs from PyPI as a wheel with the built UI inside it, and a provisa command launches the whole stack. The run command waits for the API to report ready before it prints success. The PostgreSQL extensions and foreign data wrappers the embedded engines rely on ship in a separate wheel with pinned per-platform binaries, and a script stages them into an operator's own PostgreSQL. The Python version is pinned, so an airgapped mirror knows which wheels to hold.
{ #installation-python-package }

On Linux the AppImage bundles the core and observability images, and a non-interactive first launch installs a systemd unit so Provisa starts on boot. The compose files are staged out of the AppImage into a writable location, and identity-provider settings are saved with the unit. In a multi-node install the secondary nodes run only the API and an engine worker and read their configuration from the primary.
{ #installation-linux }

Provisa ships in separate packages, a core, an observability add-on and a demo, installed in that order. Heavy add-ons are found locally first, then on mounted volumes and downloads, and the whole distribution works airgapped with no network calls at install time or first launch. A release is cut by one workflow and is never published with a missing installer, and the core image is built with the UI already inside.
{ #installation-packaging }

??? note "Requirements"

    - [The desktop installer is a single download that extracts into a hidden Provisa director...](#installation-desktop): [REQ-223](arch/requirements.md#REQ-223), [REQ-224](arch/requirements.md#REQ-224), [REQ-225](arch/requirements.md#REQ-225), [REQ-226](arch/requirements.md#REQ-226), [REQ-227](arch/requirements.md#REQ-227), [REQ-228](arch/requirements.md#REQ-228), [REQ-558](arch/requirements.md#REQ-558), [REQ-559](arch/requirements.md#REQ-559), [REQ-633](arch/requirements.md#REQ-633), [REQ-634](arch/requirements.md#REQ-634), [REQ-972](arch/requirements.md#REQ-972), [REQ-973](arch/requirements.md#REQ-973), [REQ-974](arch/requirements.md#REQ-974), [REQ-975](arch/requirements.md#REQ-975), [REQ-976](arch/requirements.md#REQ-976), [REQ-978](arch/requirements.md#REQ-978), [REQ-979](arch/requirements.md#REQ-979), [REQ-989](arch/requirements.md#REQ-989), [REQ-1219](arch/requirements.md#REQ-1219), [REQ-1535](arch/requirements.md#REQ-1535), [REQ-2014](arch/requirements.md#REQ-2014)
    - [On first launch the native installers show what is happening instead of running hidden.](#installation-windows-startup): [REQ-1005](arch/requirements.md#REQ-1005), [REQ-1100](arch/requirements.md#REQ-1100), [REQ-1144](arch/requirements.md#REQ-1144), [REQ-1145](arch/requirements.md#REQ-1145), [REQ-1146](arch/requirements.md#REQ-1146), [REQ-1147](arch/requirements.md#REQ-1147)
    - [Provisa also installs from PyPI as a wheel with the built UI inside it, and a provisa c...](#installation-python-package): [REQ-898](arch/requirements.md#REQ-898), [REQ-901](arch/requirements.md#REQ-901), [REQ-1126](arch/requirements.md#REQ-1126), [REQ-1127](arch/requirements.md#REQ-1127), [REQ-1128](arch/requirements.md#REQ-1128), [REQ-1129](arch/requirements.md#REQ-1129), [REQ-1130](arch/requirements.md#REQ-1130), [REQ-1150](arch/requirements.md#REQ-1150), [REQ-1158](arch/requirements.md#REQ-1158), [REQ-1873](arch/requirements.md#REQ-1873)
    - [On Linux the AppImage bundles the core and observability images, and a non-interactive...](#installation-linux): [REQ-560](arch/requirements.md#REQ-560), [REQ-561](arch/requirements.md#REQ-561), [REQ-562](arch/requirements.md#REQ-562), [REQ-563](arch/requirements.md#REQ-563), [REQ-632](arch/requirements.md#REQ-632), [REQ-1183](arch/requirements.md#REQ-1183), [REQ-1184](arch/requirements.md#REQ-1184), [REQ-1211](arch/requirements.md#REQ-1211)
    - [Provisa ships in separate packages, a core, an observability add-on and a demo, install...](#installation-packaging): [REQ-294](arch/requirements.md#REQ-294), [REQ-630](arch/requirements.md#REQ-630), [REQ-631](arch/requirements.md#REQ-631), [REQ-854](arch/requirements.md#REQ-854), [REQ-977](arch/requirements.md#REQ-977), [REQ-1175](arch/requirements.md#REQ-1175), [REQ-1212](arch/requirements.md#REQ-1212), [REQ-1989](arch/requirements.md#REQ-1989)

### Deployment

Docker Compose is the quick way to run Provisa for development or a small team: one command brings up the server, the engine and the connectors, and separate compose projects hold the demo upstreams and the telemetry stack so they start and stop on their own. For production on Kubernetes there is a Helm chart that serves the UI, exposes every protocol port, scales engine workers and validates its values. Authentication is chosen at install time and does not require Firebase.
{ #deployment-compose-helm }

On a cloud VM, Terraform modules for GCP, AWS and Azure provision the stack, stage the release artifacts and run it under systemd. The AWS path builds a multi-node cluster behind load balancers. On GCP each wire protocol gets its own load-balanced address, and the interview that drives the modules lets you switch pgwire, Bolt, MCP and gRPC on or off. The UI is published on the standard HTTPS port, and enterprise modules are single-tenant. A first deployment can run on one modest VM with the database on a separate disk so the VM can be resized without losing data.
{ #deployment-cloud-vm }

A cluster deployment serves every endpoint over TLS, web UI and API as well as pgwire, Bolt, Flight and the rest. If the operator supplies no certificates Provisa generates self-signed ones, so HTTPS works out of the box. The Helm chart takes an existing certificate secret or a cert-manager issuer, and a wildcard certificate from ACME DNS-01 covers the per-org subdomains.
{ #deployment-tls }

Clustered deployments run one primary and any number of secondaries. Only the primary may write the shared configuration, and secondaries load a byte-identical copy and serve the application tier. Every engine in a fleet runs the same container image, and control-plane state sits in a database that all instances share, not in memory.
{ #deployment-clustered }

Provisa keeps its model, its operating state and platform state in a pluggable control-plane store chosen by a SQLAlchemy URI, with an embedded single-user store on the desktop and PostgreSQL elsewhere. Portable types and a capabilities flag keep dialect-specific features out of the shared code. The embedded DuckDB engine is for desktop, edge and development, not for multi-user production; its extensions ship offline, and the embedded store runs in WAL mode so the engine can read while configuration is written.
{ #deployment-state-and-engine }

??? note "Requirements"

    - [Docker Compose is the quick way to run Provisa for development or a small team: one com...](#deployment-compose-helm): [REQ-055](arch/requirements.md#REQ-055), [REQ-056](arch/requirements.md#REQ-056), [REQ-169](arch/requirements.md#REQ-169), [REQ-170](arch/requirements.md#REQ-170), [REQ-507](arch/requirements.md#REQ-507), [REQ-508](arch/requirements.md#REQ-508), [REQ-509](arch/requirements.md#REQ-509), [REQ-619](arch/requirements.md#REQ-619), [REQ-815](arch/requirements.md#REQ-815), [REQ-816](arch/requirements.md#REQ-816), [REQ-831](arch/requirements.md#REQ-831), [REQ-836](arch/requirements.md#REQ-836), [REQ-1006](arch/requirements.md#REQ-1006), [REQ-1007](arch/requirements.md#REQ-1007), [REQ-1256](arch/requirements.md#REQ-1256), [REQ-1258](arch/requirements.md#REQ-1258), [REQ-1265](arch/requirements.md#REQ-1265), [REQ-1266](arch/requirements.md#REQ-1266), [REQ-1407](arch/requirements.md#REQ-1407), [REQ-1468](arch/requirements.md#REQ-1468), [REQ-1837](arch/requirements.md#REQ-1837), [REQ-2072](arch/requirements.md#REQ-2072), [REQ-2073](arch/requirements.md#REQ-2073)
    - [On a cloud VM, Terraform modules for GCP, AWS and Azure provision the stack, stage the...](#deployment-cloud-vm): [REQ-564](arch/requirements.md#REQ-564), [REQ-1026](arch/requirements.md#REQ-1026), [REQ-1027](arch/requirements.md#REQ-1027), [REQ-1028](arch/requirements.md#REQ-1028), [REQ-1029](arch/requirements.md#REQ-1029), [REQ-1031](arch/requirements.md#REQ-1031), [REQ-1181](arch/requirements.md#REQ-1181), [REQ-1198](arch/requirements.md#REQ-1198), [REQ-1199](arch/requirements.md#REQ-1199), [REQ-1202](arch/requirements.md#REQ-1202), [REQ-1205](arch/requirements.md#REQ-1205), [REQ-1206](arch/requirements.md#REQ-1206), [REQ-1207](arch/requirements.md#REQ-1207), [REQ-1208](arch/requirements.md#REQ-1208), [REQ-1209](arch/requirements.md#REQ-1209), [REQ-1213](arch/requirements.md#REQ-1213), [REQ-1253](arch/requirements.md#REQ-1253), [REQ-1254](arch/requirements.md#REQ-1254), [REQ-1278](arch/requirements.md#REQ-1278)
    - [A cluster deployment serves every endpoint over TLS, web UI and API as well as pgwire,...](#deployment-tls): [REQ-1226](arch/requirements.md#REQ-1226), [REQ-1228](arch/requirements.md#REQ-1228), [REQ-1239](arch/requirements.md#REQ-1239), [REQ-1257](arch/requirements.md#REQ-1257), [REQ-1259](arch/requirements.md#REQ-1259)
    - [Clustered deployments run one primary and any number of secondaries.](#deployment-clustered): [REQ-506](arch/requirements.md#REQ-506), [REQ-1229](arch/requirements.md#REQ-1229), [REQ-1230](arch/requirements.md#REQ-1230), [REQ-1245](arch/requirements.md#REQ-1245), [REQ-1457](arch/requirements.md#REQ-1457)
    - [Provisa keeps its model, its operating state and platform state in a pluggable control-...](#deployment-state-and-engine): [REQ-828](arch/requirements.md#REQ-828), [REQ-829](arch/requirements.md#REQ-829), [REQ-830](arch/requirements.md#REQ-830), [REQ-832](arch/requirements.md#REQ-832), [REQ-834](arch/requirements.md#REQ-834), [REQ-835](arch/requirements.md#REQ-835), [REQ-837](arch/requirements.md#REQ-837), [REQ-850](arch/requirements.md#REQ-850), [REQ-853](arch/requirements.md#REQ-853), [REQ-889](arch/requirements.md#REQ-889), [REQ-894](arch/requirements.md#REQ-894), [REQ-1098](arch/requirements.md#REQ-1098), [REQ-1099](arch/requirements.md#REQ-1099), [REQ-1142](arch/requirements.md#REQ-1142), [REQ-1200](arch/requirements.md#REQ-1200), [REQ-1201](arch/requirements.md#REQ-1201), [REQ-1536](arch/requirements.md#REQ-1536), [REQ-1920](arch/requirements.md#REQ-1920), [REQ-1932](arch/requirements.md#REQ-1932)

### Operator settings and configuration

Operators configure a deployment from one config file, its fragments and environment variables. A config file can include other files, resolved relative to the including file, so a large estate stays in readable pieces. The config path comes from an explicit environment variable with no default, so an unset one is an error rather than a guess. Every setting an operator can change has a place in the admin settings pages and the settings API, not only in a file.
{ #operator-settings-config }

Each organization tier has a storage quota for what it keeps in the relational materialization store. An operation that would push an org past its ceiling is rejected with a clear error instead of being cut short. Usage is metered per org on demand, and an org may register its own materialization store in its own cloud account.
{ #operator-settings-storage }

Settings for the systems Provisa talks to are adjustable without code changes. Databricks TLS trust can be set for connections behind an intercepting proxy, the login timeout for SQL Server warehouses such as Synapse and Fabric is configurable, and the mail transport for invitations is set from the platform settings page rather than a file on the node. Invitations name the org in the sender and route replies to the inviter.
{ #operator-settings-integrations }

??? note "Requirements"

    - [Operators configure a deployment from one config file, its fragments and environment va...](#operator-settings-config): [REQ-528](arch/requirements.md#REQ-528), [REQ-618](arch/requirements.md#REQ-618), [REQ-1079](arch/requirements.md#REQ-1079), [REQ-1083](arch/requirements.md#REQ-1083), [REQ-1084](arch/requirements.md#REQ-1084), [REQ-1669](arch/requirements.md#REQ-1669), [REQ-1913](arch/requirements.md#REQ-1913)
    - [Each organization tier has a storage quota for what it keeps in the relational material...](#operator-settings-storage): [REQ-1046](arch/requirements.md#REQ-1046), [REQ-1047](arch/requirements.md#REQ-1047), [REQ-1048](arch/requirements.md#REQ-1048), [REQ-1049](arch/requirements.md#REQ-1049), [REQ-1081](arch/requirements.md#REQ-1081)
    - [Settings for the systems Provisa talks to are adjustable without code changes.](#operator-settings-integrations): [REQ-1076](arch/requirements.md#REQ-1076), [REQ-1077](arch/requirements.md#REQ-1077), [REQ-1576](arch/requirements.md#REQ-1576), [REQ-1577](arch/requirements.md#REQ-1577)

### Observability

Provisa emits OpenTelemetry traces, metrics and logs over OTLP from its API, compiler, executor and engine calls. Tracing is on by default in a lean form, with every request traced and detail added in a debug mode, and individual subsystems can have their traces switched on. Each stage of the query pipeline records the SQL it saw, so a governed query can be followed from request to engine. An opt-in stack of collector, Prometheus and Grafana is available for demonstration.
{ #observability-otel }

A raw SQL statement explains itself the way a GraphQL query does: the plan shows the semantic sources it scanned and how it was routed, and carries the row and cost estimates the target engine already computed, whichever engine that is.
{ #observability-query-plans }

Telemetry has two independent exits: the operator's own collector, and an optional Provisa support endpoint. The support path is off until an endpoint is set. Each path has its own filter, filtering works on copies so original spans are untouched, and SQL literals are redacted by default so query data is not shared.
{ #observability-support-telemetry }

Telemetry lands in an object store that every participant finds through the same environment variables. A scheduled job expires old snapshots and compacts logs, metrics and traces, each signal on its own so none starves another, and timestamps are stored as timestamps rather than epoch integers.
{ #observability-signal-store }

Operations reporting is delivered as views in the built-in ops domain: the query audit log and other internal logs are exposed as queryable tables, and the Reports tab in the admin console shows them with a resizable list.
{ #observability-operational-views }

??? note "Requirements"

    - [Provisa emits OpenTelemetry traces, metrics and logs over OTLP from its API, compiler,...](#observability-otel): [REQ-302](arch/requirements.md#REQ-302), [REQ-303](arch/requirements.md#REQ-303), [REQ-330](arch/requirements.md#REQ-330), [REQ-549](arch/requirements.md#REQ-549), [REQ-886](arch/requirements.md#REQ-886), [REQ-914](arch/requirements.md#REQ-914), [REQ-1082](arch/requirements.md#REQ-1082), [REQ-1425](arch/requirements.md#REQ-1425), [REQ-1432](arch/requirements.md#REQ-1432), [REQ-1910](arch/requirements.md#REQ-1910)
    - [A raw SQL statement explains itself the way a GraphQL query does: the plan shows the se...](#observability-query-plans): [REQ-1517](arch/requirements.md#REQ-1517), [REQ-1522](arch/requirements.md#REQ-1522)
    - [Telemetry has two independent exits: the operator's own collector, and an optional Prov...](#observability-support-telemetry): [REQ-545](arch/requirements.md#REQ-545), [REQ-546](arch/requirements.md#REQ-546), [REQ-547](arch/requirements.md#REQ-547), [REQ-548](arch/requirements.md#REQ-548)
    - [Telemetry lands in an object store that every participant finds through the same enviro...](#observability-signal-store): [REQ-1422](arch/requirements.md#REQ-1422), [REQ-1423](arch/requirements.md#REQ-1423), [REQ-1428](arch/requirements.md#REQ-1428), [REQ-1435](arch/requirements.md#REQ-1435), [REQ-1761](arch/requirements.md#REQ-1761)
    - [Operations reporting is delivered as views in the built-in ops domain: the query audit...](#observability-operational-views): [REQ-884](arch/requirements.md#REQ-884), [REQ-1386](arch/requirements.md#REQ-1386), [REQ-1410](arch/requirements.md#REQ-1410), [REQ-1424](arch/requirements.md#REQ-1424), [REQ-1431](arch/requirements.md#REQ-1431), [REQ-1439](arch/requirements.md#REQ-1439)

### Sizing guide

The sizing tool measures your own Provisa deployment under a workload you describe in one setup file, and writes a sizing and tuning guide from what it measured: how many physical cores and worker processes the workload needs, and what each setting buys. Every figure in the guide comes from a measurement of your deployment, so after a change to the deployment or the workload you rerun the tool and regenerate the guide. The guide and the steps to run the tool are in the documentation.
{ #sizing-guide-your-workload }

??? note "Requirements"

    - [The sizing tool measures your own Provisa deployment under a workload you describe in o...](#sizing-guide-your-workload): [REQ-2005](arch/requirements.md#REQ-2005)

### Licensing and editions

Provisa runs free for an evaluation period. The first-use date is signed and stored in several places outside the install directory, and elapsed time is measured against a high-water mark so setting the clock back does not extend the trial. Once the trial ends without a license, a notice appears on every access surface: a banner in the UI and the native notice channel of each protocol. An Unregistered marker stays in the footer whenever the deployment is not licensed.
{ #licensing-and-editions-trial }

A license is a signed file that proves its own validity offline and is tied to the machine it was issued for. Apply it with a command, upload it in Settings, or drop it in the Provisa directory. Demo mode skips entitlement checks so every feature can be shown.
{ #licensing-and-editions-license-file }

Anyone can get a free license by registering on a web form and confirming by email; the signed file is sent after the confirmation link is clicked. Some source types are refused on the hosted plane where their license forbids offering them as a managed service.
{ #licensing-and-editions-registration }

??? note "Requirements"

    - [Provisa runs free for an evaluation period.](#licensing-and-editions-trial): [REQ-1135](arch/requirements.md#REQ-1135), [REQ-1136](arch/requirements.md#REQ-1136), [REQ-1137](arch/requirements.md#REQ-1137), [REQ-1791](arch/requirements.md#REQ-1791)
    - [A license is a signed file that proves its own validity offline and is tied to the mach...](#licensing-and-editions-license-file): [REQ-1138](arch/requirements.md#REQ-1138), [REQ-1139](arch/requirements.md#REQ-1139), [REQ-1370](arch/requirements.md#REQ-1370), [REQ-1507](arch/requirements.md#REQ-1507)
    - [Anyone can get a free license by registering on a web form and confirming by email; the...](#licensing-and-editions-registration): [REQ-1725](arch/requirements.md#REQ-1725), [REQ-1793](arch/requirements.md#REQ-1793)
