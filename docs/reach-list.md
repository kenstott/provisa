# What Provisa can reach

**279 databases, services, file formats and transports, through 70 connectors.**

A service is listed here when one of the connectors below can connect to it. That connector may be written for the service or may be generic, as OpenAPI and GraphQL are. Provisa does not test every listed service in every release. A connector that reaches several services counts each one, so Microsoft 365 adds four lines. A service that speaks the protocol of a listed database counts once, under that database's connector.

| Group | Items |
| --- | --: |
| [Databases](#databases) | 62 |
| [Warehouses and analytics](#warehouses-and-analytics) | 14 |
| [Lakes](#lakes) | 7 |
| [Streaming](#streaming) | 6 |
| [Files and formats](#files-and-formats) | 71 |
| [Transports](#transports) | 17 |
| [SaaS and services](#saas-and-services) | 102 |
| **Total** | **279** |

## Databases

62 items.

| Through | Reaches | Items |
| --- | --- | --: |
| Relational connectors, one each | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, SAP HANA, Firebird, CockroachDB, YugabyteDB, Greenplum, TiDB, SQLite, DuckDB | 14 |
| PostgreSQL connector, hosted | Amazon RDS for PostgreSQL, Amazon Aurora PostgreSQL, Google Cloud SQL for PostgreSQL, AlloyDB, Azure Database for PostgreSQL, Supabase, Neon, Crunchy Bridge, Aiven for PostgreSQL, DigitalOcean Managed PostgreSQL, Heroku Postgres, Render Postgres | 12 |
| PostgreSQL connector, extended Postgres | TimescaleDB, Citus | 2 |
| MySQL connector | Amazon RDS for MySQL, Amazon Aurora MySQL, Google Cloud SQL for MySQL, Percona Server | 4 |
| MariaDB connector | Amazon RDS for MariaDB | 1 |
| SQL Server connector | Azure SQL Database, Azure SQL Managed Instance, Amazon RDS for SQL Server | 3 |
| Oracle connector | Amazon RDS for Oracle | 1 |
| MongoDB connector | MongoDB, MongoDB Atlas, Amazon DocumentDB, FerretDB | 4 |
| Cosmos DB connector | Azure Cosmos DB for NoSQL | 1 |
| Cassandra connector | Apache Cassandra, ScyllaDB | 2 |
| Redis connector | Redis, Valkey, Dragonfly | 3 |
| Qdrant connector | Qdrant | 1 |
| Elasticsearch connector | Elasticsearch, Elastic Cloud, OpenSearch | 3 |
| Neo4j connector | Neo4j | 1 |
| SPARQL connector | Any SPARQL 1.1 endpoint, Apache Jena Fuseki, Ontotext GraphDB, Stardog, Virtuoso, Blazegraph, Eclipse RDF4J, Amazon Neptune, Wikidata, DBpedia | 10 |

## Warehouses and analytics

14 items, each through its own connector. ClickHouse Cloud goes through the ClickHouse connector with TLS on.

| Through | Reaches | Items |
| --- | --- | --: |
| Warehouse connectors | Snowflake, BigQuery, Databricks SQL warehouses, Amazon Redshift, Amazon Athena, Microsoft Fabric Warehouse, Azure Synapse SQL, MotherDuck, Trino | 9 |
| Analytics connectors | ClickHouse, ClickHouse Cloud, Apache Druid, Apache Pinot, Exasol | 5 |

## Lakes

7 items.

| Through | Reaches | Items |
| --- | --- | --: |
| Delta Lake connector | Delta Lake tables | 1 |
| Iceberg connector | Apache Iceberg tables | 1 |
| Hive connector | Hive Metastore, Hive on Azure Data Lake Storage Gen2 | 2 |
| Hive on S3 connector | Hive tables on S3 | 1 |
| HiveServer2 connector | A HiveServer2 endpoint | 1 |
| Databricks connector, opt-in scan | Unity Catalog Iceberg and UniForm tables | 1 |

## Streaming

6 items.

| Through | Reaches | Items |
| --- | --- | --: |
| Kafka connector | Apache Kafka, Redpanda | 2 |
| WebSocket connector | Any WebSocket feed | 1 |
| RSS connector | RSS 2.0, Atom | 2 |
| Ingest connector | HTTP push from any service | 1 |

## Files and formats

71 items. SQLite and DuckDB database files are counted under databases.

| Through | Reaches | Items |
| --- | --- | --: |
| File crawler, CSV and Parquet connectors, read directly | CSV, TSV, JSON, YAML, Parquet, Arrow | 6 |
| File crawler, converted to tables | Excel XLSX, Excel XLS, Word DOCX, PowerPoint PPTX, HTML, XML, Markdown | 7 |
| File crawler, compression | Gzip, of any file read directly | 1 |
| Logs connector, general | BSD syslog (RFC 3164), syslog (RFC 5424), macOS install log, Common Log Format, Combined Log Format, JSON lines, logfmt, W3C extended, ArcSight CEF, Graylog GELF | 10 |
| Logs connector, AWS | Application Load Balancer, Network Load Balancer, Classic Load Balancer, CloudFront, S3 server access, CloudTrail, VPC Flow Logs, AWS WAF, Route 53 Resolver | 9 |
| Logs connector, Microsoft | IIS, Windows Firewall, Windows DNS debug, Windows DHCP audit, Exchange message tracking, SQL Server ERRORLOG, Azure Storage analytics, Azure Monitor and Activity Log export, Windows Event Log (.evtx) | 9 |
| Logs connector, databases and runtimes | PostgreSQL stderr, PostgreSQL csvlog, PostgreSQL jsonlog, MySQL error, MySQL slow query, MongoDB, Redis, log4j 2, logback, Python logging | 10 |
| Logs connector, web servers and proxies | nginx error, Apache httpd error, HAProxy, Squid, Envoy, Caddy, Traefik | 7 |
| Logs connector, Linux and containers | Kubernetes CRI, systemd journal, auditd, dmesg | 4 |
| Logs connector, security | Zeek tab-separated, Zeek JSON, Suricata EVE JSON, Snort alert | 4 |
| Logs connector, Google | Cloud Logging export, Android logcat, Chrome NetLog | 3 |
| Logs connector, your own | A regular expression whose named groups are the columns | 1 |

## Transports

17 items.

| Through | Reaches | Items |
| --- | --- | --: |
| File crawler | Local folders, mounted network shares, HDFS, HTTP and HTTPS, FTP, SFTP, SharePoint document libraries | 7 |
| File crawler, CSV and Parquet connectors | Amazon S3 | 1 |
| Same, with an endpoint override | MinIO, Cloudflare R2, Wasabi, Backblaze B2, DigitalOcean Spaces, Ceph Object Gateway | 6 |
| CSV and Parquet connectors | Google Cloud Storage, Azure Blob Storage, Azure Data Lake Storage Gen2 | 3 |

## SaaS and services

102 items.

| Through | Reaches | Items |
| --- | --- | --: |
| Enterprise connectors, one each | Salesforce, ServiceNow, Jira Cloud, SharePoint sites and lists, Splunk | 5 |
| Google Workspace connector | Gmail, Google Calendar, Google Tasks, Google Workspace Directory | 4 |
| Google Sheets connector | Google Sheets | 1 |
| Microsoft 365 connector | Outlook mail on Exchange Online, Outlook Calendar, Microsoft To Do, Microsoft Entra ID directory | 4 |
| Cloud inventory connector | Azure, AWS, Google Cloud | 3 |
| Branded sources | GitHub and GitLab, over GraphQL; Stripe, over OpenAPI | 3 |
| Data subscriptions | AskAmerica (29 schemas of U.S. government data), Kaggle datasets, Wikipedia | 3 |
| Prometheus connector | Prometheus, Thanos, VictoriaMetrics | 3 |
| Code connectors | Your own Node.js functions, your own Python functions | 2 |
| Generic connectors | Any REST API with an OpenAPI or Swagger spec, any GraphQL endpoint, any gRPC service with its proto, any Arrow Flight server through Airport | 4 |
| GraphQL connector | Atlassian platform GraphQL, Contentful, Railway, AniList, PokéAPI | 5 |
| OpenAPI connector | The 65 services in the next table | 65 |

### Services with a public OpenAPI spec

Each row links the vendor's published spec. "Tables on offer" is how many read operations Provisa's OpenAPI connector found in that spec on 1 November 2026. You register the ones you want, as with any source.

| Service | Spec | Tables on offer |
| --- | --- | --: |
| Ably | [spec](https://raw.githubusercontent.com/ably/open-specs/main/definitions/platform-v1.yaml) | 10 |
| Adyen Checkout | [spec](https://raw.githubusercontent.com/Adyen/adyen-openapi/main/json/CheckoutService-v71.json) | 3 |
| Algolia Search | [spec](https://raw.githubusercontent.com/algolia/api-clients-automation/main/specs/bundled/search.yml) | 30 |
| Argo CD | [spec](https://raw.githubusercontent.com/argoproj/argo-cd/master/assets/swagger.json) | 56 |
| Asana | [spec](https://raw.githubusercontent.com/Asana/openapi/master/defs/asana_oas.yaml) | 128 |
| Attio | [spec](https://api.attio.com/openapi/api) | 39 |
| Bitbucket | [spec](https://api.bitbucket.org/swagger.json) | 159 |
| Box | [spec](https://raw.githubusercontent.com/box/box-openapi/main/openapi.json) | 125 |
| Brevo | [spec](https://api.brevo.com/v3/swagger_definition_v3.yml) | 115 |
| Cal.com | [spec](https://raw.githubusercontent.com/calcom/cal.com/main/docs/api-reference/v2/openapi.json) | 55 |
| CircleCI | [spec](https://circleci.com/api/v2/openapi.json) | 61 |
| Clerk | [spec](https://raw.githubusercontent.com/clerk/openapi-specs/main/bapi/2025-04-10.yml) | 73 |
| ClickUp | [spec](https://developer.clickup.com/openapi/clickup-api-v2-reference.json) | 51 |
| Cloudflare | [spec](https://raw.githubusercontent.com/cloudflare/api-schemas/main/openapi.json) | 1820 |
| Cohere | [spec](https://raw.githubusercontent.com/cohere-ai/cohere-developer-experience/main/cohere-openapi.yaml) | 15 |
| Confluence Cloud | [spec](https://developer.atlassian.com/cloud/confluence/swagger.v3.json) | 65 |
| Datadog | [spec](https://raw.githubusercontent.com/DataDog/datadog-api-client-python/master/.generator/schemas/v2/openapi.yaml) | 705 |
| DigitalOcean | [spec](https://api-engineering.nyc3.cdn.digitaloceanspaces.com/spec-ci/DigitalOcean-public.v2.yaml) | 440 |
| Docker Engine | [spec](https://docs.docker.com/reference/api/engine/version/v1.47.yaml) | 35 |
| DocuSign eSignature | [spec](https://raw.githubusercontent.com/docusign/OpenAPI-Specifications/master/esignature.rest.swagger-v2.1.json) | 155 |
| Dropbox Sign | [spec](https://raw.githubusercontent.com/hellosign/hellosign-openapi/main/openapi.yaml) | 24 |
| Figma | [spec](https://raw.githubusercontent.com/figma/rest-api-spec/main/openapi/openapi.yaml) | 43 |
| Front | [spec](https://raw.githubusercontent.com/frontapp/front-api-specs/main/core-api/core-api.json) | 124 |
| Gitea | [spec](https://gitea.com/swagger.v1.json) | 247 |
| Grafana | [spec](https://raw.githubusercontent.com/grafana/grafana/main/public/api-merged.json) | 132 |
| Hetzner Cloud | [spec](https://docs.hetzner.cloud/cloud.spec.json) | 83 |
| HubSpot Contacts | [spec](https://raw.githubusercontent.com/HubSpot/HubSpot-public-api-spec-collection/main/PublicApiSpecs/CRM/Contacts/Rollouts/424/2025-09/contacts.json) | 2 |
| Jira Service Management | [spec](https://developer.atlassian.com/cloud/jira/service-desk/swagger.v3.json) | 41 |
| Klaviyo | [spec](https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable.json) | 256 |
| Kubernetes | [spec](https://raw.githubusercontent.com/kubernetes/kubernetes/master/api/openapi-spec/swagger.json) | 564 |
| LaunchDarkly | [spec](https://app.launchdarkly.com/api/v2/openapi.json) | 196 |
| Linode | [spec](https://raw.githubusercontent.com/linode/linode-api-openapi/main/openapi.json) | 279 |
| Lob | [spec](https://raw.githubusercontent.com/lob/lob-openapi/main/dist/lob-api-bundled.yml) | 44 |
| Mailchimp | [spec](https://raw.githubusercontent.com/mailchimp/mailchimp-client-lib-codegen/main/spec/marketing.json) | 149 |
| Mailgun | [spec](https://documentation.mailgun.com/_bundle/docs/mailgun/api-reference/send/mailgun.yaml) | 96 |
| Meilisearch | [spec](https://raw.githubusercontent.com/meilisearch/open-api/main/open-api.json) | 29 |
| Microsoft Graph | [spec](https://raw.githubusercontent.com/microsoftgraph/msgraph-metadata/master/openapi/v1.0/openapi.yaml) | 9123 |
| Netlify | [spec](https://open-api.netlify.com/swagger.json) | 78 |
| Notion | [spec](https://developers.notion.com/openapi.json) | 26 |
| Okta | [spec](https://raw.githubusercontent.com/okta/okta-management-openapi-spec/master/dist/current/management-minimal.yaml) | 315 |
| OpenAI | [spec](https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml) | 159 |
| PagerDuty | [spec](https://raw.githubusercontent.com/PagerDuty/api-schema/main/reference/REST/openapiv3.json) | 216 |
| PayPal Invoicing | [spec](https://raw.githubusercontent.com/paypal/paypal-rest-api-specifications/main/openapi/invoicing_v2.json) | 6 |
| Pinecone | [spec](https://raw.githubusercontent.com/pinecone-io/pinecone-api/main/2025-04/db_control_2025-04.oas.yaml) | 9 |
| Pipedrive | [spec](https://developers.pipedrive.com/docs/api/v1/openapi.yaml) | 113 |
| Plaid | [spec](https://raw.githubusercontent.com/plaid/plaid-openapi/master/2020-09-14.yml) | 5 |
| Polygon.io | [spec](https://api.polygon.io/openapi) | 147 |
| Prefect Cloud | [spec](https://api.prefect.cloud/api/openapi.json) | 149 |
| Ramp | [spec](https://docs.ramp.com/openapi/developer-api.json) | 111 |
| Resend | [spec](https://raw.githubusercontent.com/resend/resend-openapi/main/resend.yaml) | 62 |
| SendGrid Mail | [spec](https://raw.githubusercontent.com/twilio/sendgrid-oai/main/spec/json/tsg_mail_v3.json) | 1 |
| Sentry | [spec](https://raw.githubusercontent.com/getsentry/sentry-api-schema/main/openapi-derefed.json) | 149 |
| Slack | [spec](https://raw.githubusercontent.com/slackapi/slack-api-specs/master/web-api/slack_web_openapi_v2.json) | 80 |
| Snyk | [spec](https://api.snyk.io/rest/openapi/2024-10-15) | 154 |
| Spotify | [spec](https://developer.spotify.com/reference/web-api/open-api-schema.yaml) | 52 |
| Tailscale | [spec](https://api.tailscale.com/api/v2?outputOpenapiSchema=true) | 35 |
| Telnyx | [spec](https://raw.githubusercontent.com/team-telnyx/openapi/master/openapi/spec3.yml) | 698 |
| Trello | [spec](https://developer.atlassian.com/cloud/trello/swagger.v3.json) | 128 |
| Twilio | [spec](https://raw.githubusercontent.com/twilio/twilio-oai/main/spec/json/twilio_api_v2010.json) | 103 |
| Typesense | [spec](https://raw.githubusercontent.com/typesense/typesense-api-spec/master/openapi.yml) | 35 |
| Vercel | [spec](https://openapi.vercel.sh/) | 195 |
| Weaviate | [spec](https://raw.githubusercontent.com/weaviate/weaviate/main/openapi-specs/schema.json) | 41 |
| X | [spec](https://api.x.com/2/openapi.json) | 121 |
| Xero Accounting | [spec](https://raw.githubusercontent.com/XeroAPI/Xero-OpenAPI/master/xero_accounting.yaml) | 100 |
| YNAB | [spec](https://api.ynab.com/papi/open_api_spec.yaml) | 28 |

## The 70 connectors

A connector here is an entry you pick to reach something outside Provisa. There are 70.

| Kind | Connectors | Count |
| --- | --- | --: |
| Relational | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB, SAP HANA, CockroachDB, YugabyteDB, Greenplum, TiDB, Firebird, HiveServer2 | 14 |
| Warehouses | Snowflake, BigQuery, Athena, Databricks, Redshift, Fabric Warehouse, Synapse SQL, MotherDuck | 8 |
| Analytics | ClickHouse, Elasticsearch, Pinot, Druid, Exasol, Trino | 6 |
| Lakes | Delta Lake, Iceberg, Hive, Hive on S3 | 4 |
| NoSQL | MongoDB, Cosmos DB, Qdrant, Cassandra, Redis | 5 |
| Graph | Neo4j, SPARQL | 2 |
| Files | SQLite, CSV, Parquet, File Crawler, Logs | 5 |
| Streaming | Kafka, WebSocket, RSS, Ingest | 4 |
| APIs | OpenAPI, GraphQL, gRPC, Airport | 4 |
| Code | Node.js, Python | 2 |
| Enterprise and observability | SharePoint, Salesforce, ServiceNow, Jira, Cloud Inventory, Splunk, Google Sheets, Google Workspace, Microsoft 365, Prometheus | 10 |
| Branded and subscribed | GitHub, GitLab, Stripe, Wikipedia, Kaggle, AskAmerica | 6 |

The Sources form lists 73 types, and the difference is the checkers. Soda, Great Expectations and the Data Profiler read tables Provisa already governs, so they add no reach.
