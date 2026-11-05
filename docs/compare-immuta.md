# Provisa and Immuta

Provisa and Immuta both govern who reads which rows and columns, and both enforce that close to where the data is read. They differ in who does the enforcing. Immuta writes your policies into each data platform's own policy objects, and the platform enforces them. Provisa compiles every rule into the statement it runs, so the rule means the same thing whatever the source.

## At a glance

| Question | Immuta | Provisa |
| --- | --- | --- |
| Where rules are enforced | Inside each platform, through the platform's row access policies, masking policies, row filters, column masks, views or access-control plugin | In the statement Provisa compiles, before it runs |
| Same meaning on every source | Policy types and masking methods differ by platform | One rule, one meaning, on every source you register |
| Sources covered | The data platforms Immuta integrates with | Every registered source, including APIs, SaaS applications, files, document stores and search indexes |
| Software on the source | A system account and Immuta-managed objects on each platform. Open-source Trino needs the Immuta plugin on every node | None. Provisa connects with the credentials you give each source |
| Direct access to the source | Governed, because the platform enforces the policy | Not governed by Provisa. You lock the source down to Provisa's account |
| Beyond access policy | Access requests and approvals, sensitive data discovery, purposes | A semantic layer, regions and residency, test environments with fakes and synthetic data |

## Where rules are enforced

**Immuta.** On Snowflake, "Immuta manages and applies Snowflake governance column and row access policies to Snowflake tables" ([Snowflake integration](https://documentation.immuta.com/SaaS/configuration/integrations/snowflake/reference-guides/snowflake-overview)). Databricks Unity Catalog is similar. "Immuta generates and applies SQL user-defined functions", and "Row-level security is enforced by applying SQL UDFs" ([Unity Catalog integration](https://documentation.immuta.com/SaaS/configuration/integrations/databricks/databricks-unity-catalog/databricks-unity-catalog-integration-reference-guide)). Redshift gets views instead, through what Immuta calls "a policy push integration" that will "apply policies directly on Immuta-created views in Redshift" ([Redshift integration](https://documentation.immuta.com/SaaS/configuration/integrations/redshift/amazon-redshift-view-based-integration/reference-guides/redshift)).

Starburst and Trino don't take policy objects. Immuta's plugin runs inside the cluster, and "The masking and row-level security methods apply the actual policy expressions" ([Trino integration](https://documentation.immuta.com/saas/configuration/integrations/starburst-trino/trino-connection-reference-guide)).

**Provisa.** Provisa adds every rule to the statement before it runs. A row rule is a `WHERE` predicate "added after compilation and before execution", and a mask is pushed into the `SELECT` projection, so a masked role never receives the real value ([Security model](security.md#row-level-security-rls)). A statement that touches one direct-capable source runs whole in that source, with its filters pushed down. A statement that joins sources runs in Provisa's federation engine, which reads each source ([Sources](sources.md)).

## Same meaning on every source

**Immuta.** What a policy can do depends on the platform. Immuta's integrations overview publishes a table of data policy types by platform and says "If a data platform isn't included in the table, that integration does not support any data policies." In that table, format-preserving masking and randomized response are marked for Snowflake only, and reversible masking is not marked for Azure Synapse, Databricks Unity Catalog or Google BigQuery. A footnote adds that "Databricks Unity Catalog ARRAY, MAP, or STRUCT type columns only support masking with NULL" ([integrations overview](https://documentation.immuta.com/latest/configuration/integrations/integrations-overview)).

Platform limits carry through as well. "Immuta also inherits some Snowflake limitations", the Snowflake guide says ([Snowflake integration](https://documentation.immuta.com/SaaS/configuration/integrations/snowflake/reference-guides/snowflake-overview)). Unity Catalog row filters and column masks aren't supported on views, and "Row access policies with more than 1023 columns are unsupported." Immuta calls the latter "an underlying limitation of UDFs in Databricks" ([Unity Catalog integration](https://documentation.immuta.com/SaaS/configuration/integrations/databricks/databricks-unity-catalog/databricks-unity-catalog-integration-reference-guide)).

**Provisa.** A rule is applied by Provisa's compiler, so it does not depend on what the source supports. The same row rule, mask and column grant hold on a PostgreSQL table, a Salesforce object, a CSV file and a MongoDB collection. Masked columns are also refused in `WHERE` and `HAVING` on every source, which stops a caller from recovering a value by filtering ([Security model](security.md#layer-5-predicate-guard)).

## Sources covered

**Immuta.** The integrations overview lists Amazon Redshift, Amazon S3, Azure Synapse Analytics, Databricks Spark, Databricks Unity Catalog, Google BigQuery, Snowflake and Starburst (Trino) ([integrations overview](https://documentation.immuta.com/latest/configuration/integrations/integrations-overview)). The support matrix adds legacy databases, and "The following legacy databases are currently supported for existing customers": Apache Impala, CDH Spark, Netezza, Oracle, PostgreSQL and Teradata ([support matrix](https://documentation.immuta.com/latest/releases/support-matrix)). Support also varies inside a platform. On Azure Synapse, "Immuta only supports Dedicated SQL pools, not Serverless SQL pools" ([support matrix](https://documentation.immuta.com/latest/releases/support-matrix)).

**Provisa.** You can register relational databases and warehouses, Elasticsearch and other analytics stores, MongoDB, Cassandra and Redis, files (CSV, Parquet, Excel and more), SaaS applications such as Salesforce, ServiceNow, Jira, Microsoft 365 and Google Workspace, and APIs described by OpenAPI, GraphQL or gRPC ([Sources](sources.md)). A source with no security model of its own, such as a CSV file or a public API, gets the same row rules and masks as a warehouse table.

## Software on the source

**Immuta.** Each platform gets an Immuta account with platform-wide privileges. Snowflake setup needs "A set of Snowflake credentials to a system account", plus "A new system Snowflake role to orchestrate policies in Snowflake". That account holds privileges such as `APPLY MASKING POLICY` and `APPLY ROW ACCESS POLICY` on the account ([Snowflake integration](https://documentation.immuta.com/SaaS/configuration/integrations/snowflake/reference-guides/snowflake-overview)). For Unity Catalog, "The Immuta service principal must own the catalog Immuta creates during setup" ([Unity Catalog integration](https://documentation.immuta.com/SaaS/configuration/integrations/databricks/databricks-unity-catalog/databricks-unity-catalog-integration-reference-guide)).

For open-source Trino, "you must download the Immuta Trino plugin and add it to your Trino cluster", installing "the plugin archive on all nodes in your cluster" ([register a Trino connection](https://documentation.immuta.com/SaaS/configuration/integrations/starburst-trino/register-a-trino-connection)). Starburst users skip that step, since "The Immuta Trino plugin comes pre-installed on Starburst clusters" ([Trino integration](https://documentation.immuta.com/saas/configuration/integrations/starburst-trino/trino-connection-reference-guide)).

To grant those privileges or install that plugin, you have to administer the platform. A SaaS application or a third-party API that you do not run offers no such place.

**Provisa.** You don't install anything on a source, and Provisa creates no policy objects there. You give Provisa a connection and its credentials, and the account needs only the reads and writes you want Provisa to serve ([Sources](sources.md)).

## Direct access to the source

**Immuta.** Users query the platform itself. A Snowflake user "queries the corresponding table directly in Snowflake" and a Databricks user "queries the corresponding table directly in Databricks". On Redshift, users "query Redshift views directly instead of going through a proxy" ([Snowflake](https://documentation.immuta.com/SaaS/configuration/integrations/snowflake/reference-guides/snowflake-overview), [Unity Catalog](https://documentation.immuta.com/SaaS/configuration/integrations/databricks/databricks-unity-catalog/databricks-unity-catalog-integration-reference-guide), [Redshift](https://documentation.immuta.com/SaaS/configuration/integrations/redshift/amazon-redshift-view-based-integration/reference-guides/redshift)). Because the platform enforces the policy, a tool that connects straight to Snowflake or Databricks is governed without passing through Immuta.

**Provisa.** Clients reach Provisa over protocols they already speak. SQL tools use the PostgreSQL wire protocol, JDBC or Arrow Flight SQL, graph tools use Bolt, and applications use GraphQL, gRPC or REST ([Integrations](integrations.md), [Security model](security.md)). For a tool that already speaks one of these, moving to Provisa is a change of connection details. Your queries stay as they are.

A Provisa rule governs only what passes through Provisa. Someone who connects to the source directly, with their own credentials, reads it under the source's own permissions. Close that path yourself. Give Provisa's account the access it needs, and revoke direct access for everyone else.

## Beyond access policy

**Immuta.**

- **Access requests.** A consumer requests a data product. If no approval is needed, access "is automatically granted once the user acknowledges the data use agreement"; otherwise "the request will enter the approval flow and appear as Pending". "Immuta automatically provisions access through policies in the data platform" ([requesting access](https://documentation.immuta.com/saas/request/access-data-products/how-to-guides/requesting-access-to-a-data-product)). "Data stewards are able to make determinations on access requests", and can approve one "for a specific period of time" ([responding to requests](https://documentation.immuta.com/saas/marketplace/review-access-requests/how-to-guides/view-and-respond-to-access-requests)).
- **Sensitive data discovery.** "Identification runs identifiers to discover data", using "built-in identifiers to discover common categories of data". Identifiers match column names, regular expressions and dictionaries, and a competitive identifier "must match at least 90% of the data sampled". Regex and dictionary matching are listed for Redshift, Databricks, Snowflake and Starburst, and column-name matching for all eight integrations ([data discovery](https://documentation.immuta.com/latest/discover-your-data/data-discovery)).

Immuta also offers purposes. "Purposes allow governors to define exceptions to policies", and "Each purpose has its own acknowledgement statement" ([projects and purposes](https://documentation.immuta.com/latest/governance/author-policies-for-data-access-control/projects-and-purpose-based-access-control/projects-and-purpose-controls/reference-guides/projects)). The SaaS documentation marks this feature deprecated: "Support for projects and purposes has been deprecated" ([projects and purposes, SaaS](https://documentation.immuta.com/saas/govern/secure-your-data/projects-and-purpose-based-access-control/purpose-index/reference-guides/projects)).

Provisa adds a semantic layer over the same sources. An [ontology](ontology.md) gives every column a term, with definitions and relationships, and [data products](data-products.md) publish owned bundles of tables. Both span every source you register.

With regions, a table can name a home region, where its personal data is stored and processed. Other regions read it there under the reader's row and column rules, so only what the reader may see leaves the region ([Regions](regions.md), [Data residency](security.md#data-residency)).

A sensitive column has four levels of protection. In any environment a role can be denied it or shown a mask. A test environment fakes it for every role, and a synthetic environment holds no real rows at all ([Building test environments](test-data.md), [Environments](environments.md)).

## Where Immuta fits better

- Your data already lives in Snowflake, Databricks, Redshift, BigQuery, Synapse, S3 or Starburst, and people connect to those platforms with their own tools. Immuta governs those direct connections because the platform enforces the policy.
- You want consumers to request access and stewards to approve it, with access provisioned on approval. Provisa has no request and approval workflow.
- You want built-in identifiers to tag sensitive data on those platforms by sampling column values.

## Where Provisa fits better

- Your sources include APIs, SaaS applications, files, document stores or search indexes, or databases with no row and column security of their own.
- You need one rule to mean the same thing on every source, whatever each source supports.
- You can't install software or create policy objects on a source, because you don't run it.
- You also want a shared semantic layer, residency by region, or safe test data over the same sources.
