# Copyright (c) 2026 Kenneth Stott
# Canary: 05337673-2212-42eb-9aa3-d0766bdd6ebd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every source type the product offers, defined once (REQ-2114, amended 2026-11-24).

One descriptor per entry of the Sources type picker: its name, label and category; the backend
:class:`~provisa.core.models.SourceType` it is stored as and the brand it records; how a source of
it is created (``createSource``, or the REST route its form posts to); and the fields that
creation takes -- each a ``SourceInput`` argument, a key of ``mappingJson`` or
``federationHintsJson``, a key of a REST body, or a brand's setup value -- with which are required
and which are secrets.

Where each part comes from:

- the backend types: :class:`SourceType`; a member is offered by some entry, or is one of the
  Supabase ``wrappers`` stubs nothing reaches yet (:data:`WRAPPER_STUBS`);
- the brands: the GraphQL brands (provisa.graphql_remote.brands), the OpenAPI brands
  (provisa.openapi.brands) -- their labels, setup fields and credential kind -- and the Wikipedia
  and Wikidata brands (provisa.file_source.wikipedia, provisa.sparql.wikidata);
- the code types: provisa.codesource.languages, Spark a built-in one (BUILTIN_CONNECTORS);
- a REST route's body: the route's own request model;
- the picker's order, labels, categories and default ports, the aliases and the connection-field
  families: here, moved from the UI (provisa-ui/src/pages/sources/constants.ts), which reads them
  from provisa-ui/src/generated/sourceTypes.json (scripts/gen_source_types.py).

A secret is never a value in a descriptor's create call: it is a ``${secret:NAME}`` reference,
its value entered by the user on the Sources form or in the organization's secrets.

The per-type mapping and hint builders run in the UI (provisa-ui/src/pages/sources/*.ts) and take
their keys, required and secret flags, choices and defaults from this descriptor (through the
generated file); a UI test fails when a builder writes a key the descriptor does not list.
"""

# Requirements: REQ-2114, REQ-1923, REQ-1984, REQ-2225
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from provisa.codesource import remote_sources
from provisa.codesource.languages import LANGUAGES
from provisa.core.models import BUILTIN_CONNECTORS, SourceType
from provisa.core.paging import PaginationType
from provisa.cosmosdb.fetch import AUTH_ENTRA, AUTH_KEY
from provisa.kafka.security import PROTOCOL_PLAINTEXT, SASL_MECHANISMS, SECURITY_PROTOCOLS
from provisa.profiler.source import DRIFT_SEASONS
from provisa.quickbooks.settings import ENVIRONMENTS as QUICKBOOKS_ENVIRONMENTS
from provisa.quickbooks.settings import SANDBOX
from provisa.sparql.reasoning import REASONERS
from provisa.sparql.source import AUTH_KINDS as SPARQL_AUTH_KINDS

#: Where a field is given: a ``SourceInput`` argument (by its GraphQL name), a key of the
#: ``mappingJson`` or ``federationHintsJson`` object, a key of a REST route's body, or a brand's
#: setup value (the body's ``setup`` object).
FieldAt = Literal["input", "mappingJson", "federationHintsJson", "body", "setup"]

#: How a secret is written in a create call: a reference, never the value.
SECRET_REFERENCE = "${secret:NAME}"


@dataclass(frozen=True)
class Field:
    name: str
    at: FieldAt
    required: bool = False
    secret: bool = False
    #: The choice it belongs to (``auth=key_pair``), when the type offers more than one.
    when: str = ""
    note: str = ""
    #: A value the field always takes (a brand's own name).
    value: Any = None
    #: The value used when none is given.
    default: Any = None
    #: The values it takes, when it takes one of a fixed set.
    choices: tuple[str, ...] = ()


@dataclass(frozen=True)
class Create:
    #: ``admin_mutation`` (createSource), ``admin_request`` (a REST route), or ``form`` (the user
    #: completes it on the Sources form, which ``link`` opens).
    via: Literal["admin_mutation", "admin_request", "form"]
    mutation: str = ""
    method: str = ""
    path: str = ""
    link: str = ""
    note: str = ""


@dataclass(frozen=True)
class Offer:
    """One entry of the Sources type picker."""

    name: str
    label: str
    category: str
    default_port: int
    #: The hosted services that are this type, shown beside its label in the picker.
    managed: str | None = None


@dataclass(frozen=True)
class SourceTypeDescriptor:
    name: str
    label: str
    category: str
    default_port: int
    managed: str | None
    #: The SourceType a source of it is stored as.
    backend_type: str
    #: The brand the source records (``federation_hints.brand``), None for a plain type.
    brand: str | None
    #: The picker type a brand is registered through (``graphql``, ``openapi``, ``files``,
    #: ``sparql``); None for a type that is its own.
    carrier: str | None
    #: A code type's language (runtimes, package kind, named entry, built-in host).
    code: dict[str, Any] | None
    create: Create
    fields: tuple[Field, ...]
    note: str = ""
    mapping_json: tuple[str, ...] = field(default=())
    federation_hints: tuple[str, ...] = field(default=())


# ---------------------------------------------------------------------------------------------
# The picker: order, labels, categories, default ports (moved from the UI's SOURCE_TYPES).
# A brand's label is its registry's; ``None`` here takes it from there.
# ---------------------------------------------------------------------------------------------

_PICKER: tuple[tuple[str, str | None, str, int, str | None], ...] = (
    # Subscriptions
    ("govdata", "AskAmerica (US Government Data)", "Subscriptions", 0, None),
    ("kaggle", "Kaggle", "Subscriptions", 0, None),
    # RDBMS
    (
        "postgresql",
        "PostgreSQL",
        "RDBMS",
        5432,
        "RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon",
    ),
    ("mysql", "MySQL", "RDBMS", 3306, None),
    ("singlestore", "SingleStore", "RDBMS", 3306, None),
    ("mariadb", "MariaDB", "RDBMS", 3306, None),
    ("sqlserver", "SQL Server", "RDBMS", 1433, None),
    ("oracle", "Oracle", "RDBMS", 1521, None),
    ("duckdb", "DuckDB", "RDBMS", 0, None),
    ("exasol", "Exasol", "RDBMS", 8563, None),
    ("saphana", "SAP HANA", "RDBMS", 39015, None),
    ("cockroachdb", "CockroachDB", "RDBMS", 26257, None),
    ("yugabytedb", "YugabyteDB", "RDBMS", 5433, None),
    ("greenplum", "Greenplum", "RDBMS", 5432, None),
    ("tidb", "TiDB", "RDBMS", 4000, None),
    ("firebird", "Firebird", "RDBMS", 3050, None),
    ("hiveserver2", "HiveServer2", "RDBMS", 10000, None),
    ("jdbc", "JDBC", "RDBMS", 0, None),
    # Cloud DW
    ("snowflake", "Snowflake", "Cloud DW", 443, None),
    ("bigquery", "BigQuery", "Cloud DW", 443, None),
    ("athena", "Amazon Athena", "Cloud DW", 443, None),
    ("databricks", "Databricks", "Cloud DW", 443, None),
    ("redshift", "Redshift", "Cloud DW", 5439, None),
    ("fabric", "Microsoft Fabric Warehouse", "Cloud DW", 1433, None),
    ("synapse", "Azure Synapse SQL", "Cloud DW", 1433, None),
    ("motherduck", "MotherDuck", "Cloud DW", 0, None),
    # Analytics / OLAP
    ("clickhouse", "ClickHouse", "Analytics", 8123, None),
    ("elasticsearch", "Elasticsearch", "Analytics", 9200, None),
    ("pinot", "Apache Pinot", "Analytics", 8099, None),
    ("druid", "Apache Druid", "Analytics", 8082, None),
    ("trino", "Trino / Presto", "Analytics", 8080, None),
    # Data Lake
    ("delta_lake", "Delta Lake", "Data Lake", 0, None),
    ("iceberg", "Apache Iceberg", "Data Lake", 0, None),
    ("hive", "Hive Metastore", "Data Lake", 9083, None),
    ("hive_s3", "Hive on S3", "Data Lake", 9083, None),
    # NoSQL
    ("mongodb", "MongoDB", "NoSQL", 27017, None),
    ("cosmosdb", "Azure Cosmos DB", "NoSQL", 0, None),
    ("qdrant", "Qdrant", "NoSQL", 0, None),
    ("cassandra", "Cassandra", "NoSQL", 9042, None),
    ("redis", "Redis", "NoSQL", 6379, None),
    # Code
    ("node", "Node.js", "Code", 0, None),
    ("python", "Python", "Code", 0, None),
    ("go", "Go", "Code", 0, None),
    ("rust", "Rust", "Code", 0, None),
    ("spark", "Spark", "Code", 0, None),
    # Graph
    ("neo4j", "Neo4j", "Graph", 7474, None),
    ("sparql", "SPARQL", "Graph", 443, None),
    # File
    ("sqlite", "SQLite", "File", 0, None),
    ("csv", "CSV File", "File", 0, None),
    ("parquet", "Parquet File", "File", 0, None),
    ("files", "File Crawler (XLSX/DOCX/MD/CSV/Parquet/JSON, etc.)", "File", 0, None),
    ("logs", "Log files", "File", 0, None),
    # Other
    ("google_sheets", "Google Sheets", "Other", 0, None),
    ("prometheus", "Prometheus", "Other", 9090, None),
    ("airport", "Airport (Arrow Flight)", "Other", 50051, None),
    # API
    ("openapi", "REST API (OpenAPI)", "API", 443, None),
    ("dbt", "dbt", "API", 0, None),
    ("graphql", "GraphQL", "API", 443, None),
    ("grpc", "gRPC", "API", 50051, None),
    # Streaming
    ("kafka", "Kafka", "Streaming", 9092, None),
    ("websocket", "WebSocket Feed", "Streaming", 0, None),
    ("rss", "RSS / Atom Feed", "Streaming", 0, None),
    ("ingest", "Ingest (HTTP Push Receiver)", "Streaming", 0, None),
    # Enterprise SaaS
    ("sharepoint", "SharePoint", "Enterprise", 0, None),
    ("salesforce", "Salesforce", "Enterprise", 0, None),
    ("servicenow", "ServiceNow", "Enterprise", 0, None),
    ("jira", "Jira", "Enterprise", 0, None),
    ("quickbooks", "QuickBooks Online", "Enterprise", 0, None),
    ("cloudops", "Cloud Inventory (Azure / AWS / GCP)", "Enterprise", 0, None),
    ("google_workspace", "Google Workspace (Gmail)", "Enterprise", 0, None),
    ("microsoft_365", "Microsoft 365 (Outlook, Exchange)", "Enterprise", 0, None),
    # Brands (REQ-1923, REQ-2265): labels from their registries.
    ("github", None, "Enterprise", 443, None),
    ("gitlab", None, "Enterprise", 443, None),
    ("shopify", None, "Enterprise", 443, None),
    ("stripe", None, "Enterprise", 443, None),
    ("openmetadata", None, "Enterprise", 443, None),
    ("datahub", None, "Enterprise", 443, None),
    ("wikipedia", "Wikipedia", "File", 0, None),
    ("wikidata", "Wikidata", "Graph", 443, None),
    ("splunk", "Splunk", "Enterprise", 8089, None),
    # Data Quality (REQ-1443, REQ-1934). soda is hidden on a hosted plane by the picker.
    ("soda", "Soda (Data Quality)", "Data Quality", 5439, None),
    ("great_expectations", "Great Expectations (Data Quality)", "Data Quality", 5439, None),
    ("data_profiler", "Data Profiler", "Data Quality", 0, None),
)

#: Picker types whose backend type is another name (REQ-947).
TYPE_ALIAS: dict[str, str] = {"graphql": "graphql_remote", "grpc": "grpc_remote"}

#: SourceType members no entry offers: the Supabase ``wrappers`` stubs (REQ-1871/1874), each
#: registrable but reached by no connector, dialect or driver yet (provisa/core/models.py).
WRAPPER_STUBS: frozenset[str] = frozenset(
    {
        "airtable",
        "auth0",
        "aws_cognito",
        "dynamodb",
        "firebase",
        "logflare",
        "s3_vectors",
        "calcom",
        "calendly",
        "clerk",
        "cloudflare_d1",
        "gravatar",
        "hubspot",
        "infura",
        "notion",
        "orb",
        "paddle",
        "slack",
    }
)

# ---------------------------------------------------------------------------------------------
# Connection-field families (moved from the UI's constants.ts).
# ---------------------------------------------------------------------------------------------

#: host / port / database / username / password.
SIMPLE_RDBMS: frozenset[str] = frozenset(
    {
        "postgresql",
        "mysql",
        "singlestore",
        "mariadb",
        "sqlserver",
        "oracle",
        "clickhouse",
        "druid",
        "mongodb",
        "cassandra",
        "redis",
        "firebird",
        "cockroachdb",
        "yugabytedb",
        "greenplum",
        "tidb",
        "trino",
        "exasol",
        "hiveserver2",
        "saphana",
        "soda",
        "great_expectations",
    }
)
#: host and port alone.
HOST_PORT_ONLY: frozenset[str] = frozenset({"airport", "websocket", "rss", "pinot", "kafka"})
#: One file at ``path`` (or ``downloadFrom``), no host or credential of its own.
FILE_SOURCES: frozenset[str] = frozenset({"sqlite", "csv", "parquet"})
#: No connection fields at all: a push receiver, a manifest, a schedule, or code.
NO_CONNECTION_TYPES: frozenset[str] = frozenset(
    {"ingest", "dbt", "data_profiler", "node", "python", "go", "rust", "spark"}
)
DATA_LAKE: frozenset[str] = frozenset({"delta_lake", "iceberg", "hive", "hive_s3"})
#: A file source whose path may name an object store, read with that store's credentials.
CLOUD_FILE_TYPES: frozenset[str] = frozenset({"csv", "parquet", "sqlite", "duckdb"})
#: The file source types whose one file may be downloaded into the organization's folder.
DOWNLOAD_TYPES: frozenset[str] = frozenset({"csv", "parquet"})


def _in(name: str, **kw: Any) -> Field:
    return Field(name, "input", **kw)


def _map(name: str, **kw: Any) -> Field:
    return Field(name, "mappingJson", **kw)


def _hint(name: str, **kw: Any) -> Field:
    return Field(name, "federationHintsJson", **kw)


_HOST = _in("host", required=True)
_PORT = _in("port", required=True)
_USER = _in("username")
_PASSWORD = _in("password", secret=True)

#: The object-store credentials a file source whose path is in that store carries in its hints
#: (the UI's OBJECT_STORE_HINT_KEYS).
OBJECT_STORE_HINTS: dict[str, tuple[Field, ...]] = {
    "S3": (
        _hint("access_key_id", when="path=s3://"),
        _hint("secret_access_key", secret=True, when="path=s3://"),
        _hint("region", when="path=s3://"),
        _hint("endpoint", when="path=s3://", note="an S3-compatible store's endpoint"),
    ),
    "GCS": (
        _hint("gcs_access_id", when="path=gs://"),
        _hint("gcs_secret_key", secret=True, when="path=gs://"),
    ),
    "AZURE": (
        _hint("azure_account_name", when="path=az://|abfss://"),
        _hint("azure_account_key", secret=True, when="path=az://|abfss://"),
    ),
}

#: The Iceberg catalog a source reads its tables through (the UI's ICEBERG_CATALOG_FIELDS).
ICEBERG_CATALOG: dict[str, tuple[tuple[str, bool], ...]] = {
    "GLUE": (),
    "REST": (("iceberg_catalog_uri", True),),
    "JDBC": (
        ("iceberg_catalog_name", True),
        ("iceberg_catalog_warehouse", True),
        ("iceberg_catalog_uri", True),
        ("iceberg_catalog_user", False),
        ("iceberg_catalog_password", False),
    ),
    "SNOWFLAKE": (
        ("iceberg_catalog_uri", True),
        ("iceberg_catalog_user", True),
        ("iceberg_catalog_password", True),
        ("iceberg_catalog_role", True),
    ),
}

#: The cloud accounts a Cloud Inventory source reads (the UI's CLOUDOPS_CLOUDS).
CLOUDOPS_CLOUDS: dict[str, tuple[tuple[str, bool, bool], ...]] = {
    "azure": (
        ("azure_tenant_id", True, False),
        ("azure_client_id", True, False),
        ("azure_client_secret", True, True),
        ("azure_subscription_ids", True, False),
    ),
    "aws": (
        ("aws_access_key_id", True, False),
        ("aws_secret_access_key", True, True),
        ("aws_region", True, False),
        ("aws_account_ids", True, False),
        ("aws_role_arn", False, False),
    ),
    "gcp": (("gcp_credentials_path", True, False), ("gcp_project_ids", True, False)),
}

#: The Salesforce sign-ins and the mapping keys each takes (the UI's SALESFORCE_AUTH_FIELDS).
SALESFORCE_AUTH: dict[str, tuple[str, ...]] = {
    "CLIENT_CREDENTIALS": (),
    "USERNAME_PASSWORD": ("sf_username", "sf_password", "security_token"),
    "ACCESS_TOKEN": ("access_token", "instance_url"),
}
_SALESFORCE_SECRETS = frozenset({"sf_password", "security_token", "access_token"})

#: An Athena source's hints (the UI's ATHENA_HINTS).
ATHENA_HINTS: tuple[str, ...] = ("region", "workgroup", "s3_staging_dir", "catalog", "role_arn")


def _object_store_fields() -> tuple[Field, ...]:
    return tuple(f for fields in OBJECT_STORE_HINTS.values() for f in fields)


def _iceberg_fields() -> tuple[Field, ...]:
    # One field per catalog type that takes the key, required as that type needs it.
    return (
        _hint("iceberg_catalog_type", choices=tuple(ICEBERG_CATALOG)),
        _hint(
            "iceberg_table_id",
            required=True,
            when="iceberg_catalog_type",
            note="the table's identifier in the catalog",
        ),
        *(
            _hint(
                key,
                required=required,
                secret=key.endswith("password"),
                when=f"iceberg_catalog_type={catalog}",
            )
            for catalog, entries in ICEBERG_CATALOG.items()
            for key, required in entries
        ),
    )


def _lake_storage(required_region: bool) -> tuple[Field, ...]:
    return (
        _map(
            "storage",
            note="s3, adls, gcs or hadoop: where the warehouse's files are",
        ),
        _map("access_key_id", when="storage=s3", required=True),
        _map("secret_access_key", when="storage=s3", required=True, secret=True),
        _map("region", when="storage=s3", required=required_region),
        _map("endpoint", when="storage=s3", required=required_region),
        _map("storage_account", when="storage=adls", required=True),
        _map("access_key", when="storage=adls", secret=True),
        _map("sas_token", when="storage=adls", secret=True),
        _map("credentials_json", when="storage=gcs", required=True, note="a key file's path"),
    )


_CODE_NOTE = (
    "mappingJson.code is one of {kind: <package kind>, package, version}, {kind: 'git', "
    "repository, commit, subdirectory}, {kind: 'directory', path} or {kind: 'upload', upload} "
    "(an archive uploaded with admin_request); runtime is the language's; secrets lists the "
    "organization secrets the code reads, by name; settings holds its non-secret settings."
)


def _code_fields(source_type: str) -> tuple[Field, ...]:
    language = LANGUAGES[source_type]
    fields = [
        _map("runtime", required=True, note="one of " + ", ".join(language.runtimes)),
        _map("code", required=True, note=_CODE_NOTE),
    ]
    if language.named_entry:
        fields.append(_map("entry", note="where the code is entered, when it has more than one"))
    fields += [
        _map(
            "secrets",
            note="the organization secrets the code reads: a list of names, or each name with "
            f"its {SECRET_REFERENCE} reference",
        ),
        _map("settings", note="non-secret settings, name to value"),
        _map("workers", default=1),
        _map("lockfile", note="{filename, upload}: an uploaded lockfile"),
        _map("llm", note="{cap_usd}: a cap on what the code's model calls spend"),
        _map(
            "mode",
            choices=remote_sources.MODES,
            default=remote_sources.DEV,
            note="dev runs the code on this server; built runs its image on a remote host",
        ),
        _map("remote", when=f"mode={remote_sources.BUILT}", note="the remote host, kept as given"),
        _map("content_hash", when=f"mode={remote_sources.BUILT}", note="the pinned code hash"),
        _map("connector", note="the connector the code is from, kept as given"),
    ]
    return tuple(fields)


_SPARK_NOTE = (
    "A Spark source joins the Spark host this installation carries (no host, port or pairing "
    "code): admin_request GET /admin/orgs/{org}/builtin-sources/spark returns the host and the "
    "cluster a source that names none runs on."
)

#: What a built-in source type's host requires, as its connector file states it
#: (connectors/spark/connector.yaml ``requires``): name, required, note.
BUILTIN_HOST_REQUIRES: dict[str, tuple[tuple[str, bool, str], ...]] = {
    "spark": (
        ("script", True, "the annotated PySpark script, as text"),
        ("cluster", False, "the Spark cluster; left out, the organization's default"),
    ),
}


#: The SASL mechanisms a HiveServer2 source signs in with. PLAIN is HiveDriver's own default
#: (provisa/executor/drivers/hive.py), which the form saves as no hint.
HIVESERVER2_AUTH_MECHANISMS: tuple[str, ...] = ("PLAIN", "LDAP", "GSSAPI")
HIVESERVER2_DEFAULT_MECHANISM = "PLAIN"

#: The sign-ins TrinoSharepointConnector passes as the catalog's auth-type
#: (provisa/federation/trino_connectors.py); CLIENT_CREDENTIALS is the one it takes when none is
#: saved.
SHAREPOINT_AUTH_TYPES: tuple[str, ...] = ("CLIENT_CREDENTIALS", "USERNAME_PASSWORD", "CERTIFICATE")
SHAREPOINT_DEFAULT_AUTH = "CLIENT_CREDENTIALS"

#: Each type's own fields, beyond its family's.
_OWN: dict[str, tuple[Field, ...]] = {
    "govdata": (
        _in(
            "database",
            required=True,
            note="the subscribed subjects' schemas, comma-separated (search_govdata_subjects)",
        ),
        _in("username", secret=True, note="the AskAmerica API key"),
    ),
    "duckdb": (_in("path", required=True, note="the .duckdb file"),),
    "firebird": (_in("path", required=True, note="the .fdb file path on the server"),),
    "hiveserver2": (
        _hint(
            "auth_mechanism",
            choices=HIVESERVER2_AUTH_MECHANISMS,
            default=HIVESERVER2_DEFAULT_MECHANISM,
            note="left out, the driver's default",
        ),
    ),
    "sqlserver": (
        _hint(
            "trust_server_certificate",
            note="'true' trusts a self-signed or internal-CA certificate",
        ),
    ),
    "exasol": (_hint("tls_fingerprint", note="pins a self-signed server certificate"),),
    "mongodb": (),
    "jdbc": (
        _hint("url", required=True, note="the database's SQLAlchemy or jdbc: URL"),
        _hint("sql_dialect", required=True, note="the SQL dialect the database is written in"),
        _hint("jdbc_driver_jar", when="url=jdbc:", required=True),
        _hint("jdbc_driver_class", when="url=jdbc:", required=True),
        _hint("capabilities.<engine>", note="what each engine may push down to it"),
        _USER,
        _PASSWORD,
    ),
    "snowflake": (
        _in("host", required=True, note="the account URL"),
        _in("database", required=True),
        _hint("warehouse"),
        _hint("schema"),
        _hint("role"),
        _in("username", required=True, when="auth=password"),
        _in("password", required=True, secret=True, when="auth=password"),
    ),
    "bigquery": (
        _in("database", required=True, note="the project id"),
        _hint(
            "credentials_path",
            when="auth=service_account",
            required=True,
            note="the service-account key file's path; leave out for application default",
        ),
    ),
    "athena": (
        _hint("region", required=True),
        _in("database", required=True),
        _hint("workgroup"),
        _hint("catalog"),
        _hint("s3_staging_dir", note="the query result location, s3://bucket/prefix"),
        _in("username", note="the access key id; leave out for the ambient AWS credential"),
        _in("password", secret=True, note="the secret access key"),
        _hint("role_arn"),
    ),
    "databricks": (
        _in("host", required=True, note="the workspace URL"),
        _in("database", required=True, note="the catalog"),
        _hint("http_path", required=True, note="the SQL warehouse's HTTP path"),
        _in("password", required=True, secret=True, note="a personal access token"),
        _hint("reach", note="'scan' reads in place; left out, a replica"),
    ),
    "redshift": (_HOST, _PORT, _in("database", required=True), _USER, _PASSWORD),
    "fabric": (
        _HOST,
        _PORT,
        _in("database", required=True),
        _hint("tenant_id", when="auth=service_principal"),
        _hint("client_id", when="auth=service_principal"),
        _hint("client_secret", when="auth=service_principal", secret=True),
    ),
    "synapse": (
        _HOST,
        _PORT,
        _in("database", required=True),
        _hint("tenant_id", when="auth=service_principal"),
        _hint("client_id", when="auth=service_principal"),
        _hint("client_secret", when="auth=service_principal", secret=True),
    ),
    "motherduck": (
        _in("database", required=True),
        _in("password", required=True, secret=True, note="the MotherDuck token"),
    ),
    "elasticsearch": (_HOST, _PORT, _USER, _PASSWORD),
    "delta_lake": (
        _in("host", note="the metastore URI"),
        _in("path", required=True, note="the warehouse path"),
        *OBJECT_STORE_HINTS["S3"],
    ),
    "iceberg": (
        _in("host", note="the metastore URI"),
        _in("path", required=True, note="the warehouse path"),
        *OBJECT_STORE_HINTS["S3"],
        *_iceberg_fields(),
    ),
    "hive": (
        _in("host", required=True, note="the metastore URI"),
        _in("database", required=True, note="the warehouse path"),
        *_lake_storage(required_region=False),
    ),
    "hive_s3": (
        _in("host", required=True, note="the metastore URI"),
        _in("database", required=True, note="the warehouse path"),
        _map("storage", value="s3"),
        _map("access_key_id", required=True),
        _map("secret_access_key", required=True, secret=True),
        _map("region", required=True),
        _map("endpoint", required=True),
    ),
    "cosmosdb": (
        _in("host", required=True, note="the account endpoint"),
        _in("database", required=True),
        _hint("auth", required=True, choices=(AUTH_KEY, AUTH_ENTRA), default=AUTH_KEY),
        _in("password", required=True, secret=True, when="auth=key", note="the account key"),
        _hint("tenant_id", required=True, when="auth=entra"),
        _hint("client_id", required=True, when="auth=entra"),
    ),
    "qdrant": (
        _in("host", required=True, note="the URL, with its port"),
        _in("password", secret=True, note="the API key"),
        _hint("prefer_grpc", note="'true' or 'false'"),
    ),
    "neo4j": (_HOST, _PORT, _in("database"), _USER, _PASSWORD),
    "sparql": (
        _in("host", required=True, note="the endpoint URL"),
        _hint("default_graph_uri"),
        _map("reasoner", choices=REASONERS, note="with its axioms"),
        _map("axioms", when="reasoner"),
        _map("auth", choices=SPARQL_AUTH_KINDS, note="left out, none"),
        _in("password", secret=True, when="auth=bearer|basic", note="the token or password"),
        _in("username", when="auth=basic"),
    ),
    "files": (
        _in(
            "path",
            required=True,
            note="a directory or glob, or an ftp/sftp/s3/https/sharepoint:// address",
        ),
        _in("username", when="path=ftp|sftp|http|https|sharepoint"),
        _in("password", secret=True, when="path=ftp|sftp|http|https|sharepoint"),
        _map("excel_hints", note="how a workbook's sheets are read as tables"),
    ),
    "logs": (
        _map("glob", required=True),
        _map("format", required=True, note="a server log format (GET /admin/logs/formats)"),
        _map("pattern", when="format=regex", required=True),
        _map("record_start", when="format=regex"),
    ),
    "google_sheets": (
        _map("credentials_json", required=True, note="the service-account key file's path"),
        _in("database", note="the metadata sheet id"),
    ),
    "prometheus": (_in("host", required=True, note="the URL"),),
    "websocket": (
        _in("path"),
        _hint("use_ssl"),
        _hint("subscribe_payload"),
        _hint("event_path"),
        _hint("reconnect_interval"),
    ),
    "rss": (_in("path"), _hint("feed_url"), _hint("poll_interval"), _hint("use_ssl")),
    "kafka": (
        _hint(
            "kafka_security_protocol",
            choices=SECURITY_PROTOCOLS,
            default=PROTOCOL_PLAINTEXT,
        ),
        _hint("kafka_sasl_mechanism", when="SASL", choices=SASL_MECHANISMS),
        _hint("kafka_ssl_ca", when="SSL"),
        _in("username", when="SASL"),
        _in("password", secret=True, when="SASL"),
    ),
    "sharepoint": (
        _in("host", required=True, note="the site URL"),
        _in("database", required=True, note="the tenant id"),
        _in("username", required=True, note="the client id"),
        _map(
            "auth_type",
            required=True,
            choices=SHAREPOINT_AUTH_TYPES,
            default=SHAREPOINT_DEFAULT_AUTH,
        ),
        _in("password", secret=True, note="the client secret"),
        _map("sp_username", when="auth_type=USERNAME_PASSWORD", required=True),
        _map("sp_password", when="auth_type=USERNAME_PASSWORD", required=True, secret=True),
        _map("certificate_path", when="auth_type=CERTIFICATE", required=True),
        _map("certificate_password", when="auth_type=CERTIFICATE", secret=True),
    ),
    "salesforce": (
        _in("host", required=True, note="the login URL"),
        _map(
            "auth_type",
            required=True,
            choices=tuple(SALESFORCE_AUTH),
            default=next(iter(SALESFORCE_AUTH)),
        ),
        _in("username", when="auth_type!=ACCESS_TOKEN", required=True, note="the consumer key"),
        _in(
            "password",
            when="auth_type!=ACCESS_TOKEN",
            required=True,
            secret=True,
            note="the consumer secret",
        ),
        *(
            _map(
                key,
                when=f"auth_type={auth}",
                required=key != "security_token",
                secret=key in _SALESFORCE_SECRETS,
            )
            for auth, keys in SALESFORCE_AUTH.items()
            for key in keys
        ),
        _map("api_version"),
    ),
    "servicenow": (
        _in("host", required=True, note="the instance URL"),
        _in("username", required=True),
        _in("password", required=True, secret=True),
    ),
    "jira": (
        _in("host", required=True, note="the site address"),
        _in("username", required=True, note="the account email"),
        _in("password", required=True, secret=True, note="the API token"),
    ),
    "quickbooks": (
        _in("database", required=True, note="the realm id"),
        _map("environment", required=True, choices=QUICKBOOKS_ENVIRONMENTS, default=SANDBOX),
        _in("username", required=True, note="the client id"),
        _in("password", required=True, secret=True, note="the client secret"),
        _map("refresh_token", secret=True),
    ),
    "cloudops": tuple(
        _map(key, when=f"cloud={cloud}", required=required, secret=secret)
        for cloud, keys in CLOUDOPS_CLOUDS.items()
        for key, required, secret in keys
    )
    + (_map("cache_ttl_minutes"),),
    "google_workspace": (
        _map("resources", required=True, note="any of mail, calendar, tasks"),
        _map("accounts", note="[the one account read]; or mailboxes for the organisation's"),
        _map("mailboxes", note="{everyone: true}, {group} or {list}: the organisation's"),
        _map("sign_in", value="google_account", when="accounts"),
        _map("mail_content", when="resources has mail", required=True, note="full or headers"),
        _map("mail_labels"),
        _map("mail_search"),
        _map("mail_since"),
        _map("mail_include_spam_trash"),
        _map("calendar_send_updates"),
        _map(
            "refresh_token",
            secret=True,
            note="an approval kept from before the server held approvals; carried through, never "
            "entered",
        ),
    ),
    "microsoft_365": (
        _map("resources", required=True, note="any of mail, calendar, tasks"),
        _map("accounts", note="[the one account read]; or mailboxes for the organisation's"),
        _map("mailboxes", note="{everyone: true}, {group} or {list}: the organisation's"),
        _map("mail_since"),
        _map("calendar_check_days", when="resources has calendar", required=True),
        _map("calendar_send_updates"),
        _map(
            "refresh_token",
            secret=True,
            note="an approval kept from before the server held approvals; carried through, never "
            "entered",
        ),
    ),
    "splunk": (
        _in("host", required=True),
        _in("port"),
        _map("use_token", note="true signs in with a token in password"),
        _in("username", when="use_token=false", required=True),
        _in("password", required=True, secret=True, note="the password, or the auth token"),
        _in("database", note="the app"),
        _map("disable_ssl_validation"),
    ),
    "data_profiler": (
        _map("cron", required=True),
        _map("sample_above_cells"),
        _map("low_cardinality_max", default=100),
        _map("drift_window", default=7),
        _map("drift_season", default="none", choices=DRIFT_SEASONS),
        _map("drift_distance", default=3),
        _map("drift_slope", default=3),
        _map("drift_ks", default=0.2),
        _map("drift_psi", default=0.25),
        _map("correlation_max_columns", default=20),
        _map("joint_max_distinct", default=20),
        _map("category_max_columns", default=10),
        _map("classification_sample", default=500),
    ),
    "ingest": (),
}


def _family_fields(name: str) -> tuple[Field, ...]:
    if name in SIMPLE_RDBMS:
        database = () if name == "druid" else (_in("database", required=True),)
        return (_HOST, _PORT, _USER, _PASSWORD, *database)
    if name in HOST_PORT_ONLY:
        return (_HOST, _PORT)
    if name in FILE_SOURCES:
        download = (
            (_in("downloadFrom", note="an https:// or s3:// file copied into the org's folder"),)
            if name in DOWNLOAD_TYPES
            else ()
        )
        return (_in("path", required=name not in DOWNLOAD_TYPES), *download)
    return ()


def _with_object_store(name: str, fields: tuple[Field, ...]) -> tuple[Field, ...]:
    return (*fields, *_object_store_fields()) if name in CLOUD_FILE_TYPES else fields


# ---------------------------------------------------------------------------------------------
# Brands and REST routes.
# ---------------------------------------------------------------------------------------------

#: The picker type each brand is registered through, and its route.
_CREATE_SOURCE = Create(via="admin_mutation", mutation="createSource")
_OPENAPI_ROUTE = "/admin/openapi/register"
_GRAPHQL_ROUTE = "/admin/sources/graphql-remote"
_GRPC_ROUTE = "/admin/grpc-remote/register"
_DBT_ROUTE = "/admin/dbt/register"

#: Body fields that carry a credential.
_SECRET_BODY_FIELDS = frozenset({"token", "client_secret", "auth", "auth_config"})


def _rest(path: str, note: str = "") -> Create:
    return Create(via="admin_request", method="POST", path=path, note=note)


def _body_fields(model: Any, leave_out: frozenset[str] = frozenset()) -> tuple[Field, ...]:
    """A REST route's body, from its request model."""
    out = []
    for name, info in model.model_fields.items():
        if name in leave_out:
            continue
        secret = name in _SECRET_BODY_FIELDS
        note = (
            f"an object {{type, ...}}; every credential in it a {SECRET_REFERENCE} reference"
            if name in ("auth", "auth_config")
            else ""
        )
        out.append(
            Field(
                name,
                "body",
                required=info.is_required(),
                secret=secret,
                note=note,
                default=None if info.is_required() else _plain(info.default),
            )
        )
    return tuple(out)


def _plain(value: Any) -> Any:
    return value if isinstance(value, (str, int, float, bool, type(None))) else None


def _brand_setup(setup_fields: tuple[Any, ...]) -> tuple[Field, ...]:
    return tuple(
        Field(
            f.name,
            "setup",
            required=f.default is None,
            note=f"matches {f.pattern}",
            default=f.default,
        )
        for f in setup_fields
    )


def _graphql_brands() -> dict[str, tuple[str, tuple[Field, ...]]]:
    from provisa.graphql_remote.brands import BRANDS

    return {
        brand.id: (
            brand.label,
            (
                Field("source_id", "body", required=True),
                Field("brand", "body", required=True, value=brand.id),
                Field("namespace", "body", note=f"default {brand.namespace}"),
                Field("description", "body"),
                *_brand_setup(brand.setup_fields),
                Field(
                    "auth",
                    "body",
                    required=True,
                    secret=True,
                    note=f"{{type: 'bearer', token: '{SECRET_REFERENCE}'}}",
                ),
            ),
        )
        for brand in BRANDS.values()
    }


def _openapi_brands() -> dict[str, tuple[str, tuple[Field, ...]]]:
    from provisa.openapi.brands import BRANDS

    out = {}
    for brand in BRANDS.values():
        if brand.auth_type == "oauth2_client_credentials":
            credential: tuple[Field, ...] = (
                Field("client_id", "body", required=True),
                Field("client_secret", "body", required=True, secret=True),
            )
        else:
            credential = (Field("token", "body", required=True, secret=True),)
        out[brand.id] = (
            brand.label,
            (
                Field("source_id", "body", required=True),
                Field("brand", "body", required=True, value=brand.id),
                *_brand_setup(brand.setup_fields),
                *credential,
            ),
        )
    return out


def brand_auth(brand: str) -> Literal["token", "client_credentials", "none"]:
    """The credential a brand asks for (the UI's BRAND_AUTH)."""
    from provisa.openapi.brands import BRANDS as OPENAPI_BRANDS

    if brand in OPENAPI_BRANDS:
        oauth = OPENAPI_BRANDS[brand].auth_type == "oauth2_client_credentials"
        return "client_credentials" if oauth else "token"
    if brand in _graphql_brands():
        return "token"
    if brand in _crawl_brands():
        return "none"
    raise KeyError(f"{brand!r} is not a brand")


def _crawl_brands() -> dict[str, str]:
    """The brands carried by the File Crawler and the SPARQL source: brand -> carrier type."""
    from provisa.file_source.wikipedia import BRAND as WIKIPEDIA
    from provisa.sparql.wikidata import BRAND as WIKIDATA

    return {WIKIPEDIA: "files", WIKIDATA: "sparql"}


_WIKIPEDIA_FIELDS = (
    _hint("brand", required=True, value="wikipedia"),
    _hint(
        "wikipedia",
        required=True,
        note="{pages: [titles], follow_see_also, language, see_also_heading, contact, "
        "max_depth, max_pages, crawl: {...}}",
    ),
    _in("path", note="where the crawl's files land; left out, a directory of its own"),
)
_WIKIDATA_FIELDS = (
    _hint("brand", required=True, value="wikidata"),
    _in("host", value="https://query.wikidata.org/sparql"),
)


def brand_carriers() -> dict[str, str]:
    """Each brand's carrier picker type (the UI's BRAND_CARRIER)."""
    carriers = {b: "graphql" for b in _graphql_brands()}
    carriers.update({b: "openapi" for b in _openapi_brands()})
    carriers.update(_crawl_brands())
    return carriers


# ---------------------------------------------------------------------------------------------
# The descriptor.
# ---------------------------------------------------------------------------------------------


def _offers() -> tuple[Offer, ...]:
    labels = {b: label for b, (label, _) in {**_graphql_brands(), **_openapi_brands()}.items()}
    out = []
    for name, label, category, port, managed in _PICKER:
        if label is None:
            label = labels[name]
        out.append(Offer(name, label, category, port, managed))
    return tuple(out)


def _plain_type(offer: Offer) -> SourceTypeDescriptor:
    from provisa.api.admin.dbt_router import DbtRegisterRequest
    from provisa.api.admin.graphql_remote_router import GraphQLRemoteSourceRequest
    from provisa.api.admin.grpc_remote_router import GrpcRemoteRegisterRequest
    from provisa.api.admin.openapi_router import OpenAPIRegisterRequest

    name = offer.name
    backend = TYPE_ALIAS.get(name, name)
    code = None
    create = _CREATE_SOURCE
    note = ""
    brand_keys = frozenset({"brand", "token", "setup", "client_id", "client_secret"})
    if name == "openapi":
        create, fields = _rest(_OPENAPI_ROUTE), _body_fields(OpenAPIRegisterRequest, brand_keys)
    elif name == "graphql":
        create, fields = (
            _rest(_GRAPHQL_ROUTE),
            _body_fields(GraphQLRemoteSourceRequest, frozenset({"brand", "setup"})),
        )
    elif name == "grpc":
        create, fields = _rest(_GRPC_ROUTE), _body_fields(GrpcRemoteRegisterRequest)
    elif name == "dbt":
        create = _rest(
            _DBT_ROUTE,
            note="a warehouse the manifest names and no source reads is created first with "
            "createSource (POST /admin/dbt/preview lists them)",
        )
        fields = _body_fields(DbtRegisterRequest)
    elif name == "kaggle":
        backend = "files"
        create = Create(
            via="form",
            link="/sources?new=kaggle&kaggle=<owner>/<dataset>",
            note="the Kaggle form stages the dataset and creates a files source holding the "
            "user's Kaggle API token; Polly gives the link and never takes the token",
        )
        fields = ()
    elif name in LANGUAGES:
        language = LANGUAGES[name]
        code = {
            "runtimes": list(language.runtimes),
            "package_kind": language.package_kind,
            "named_entry": language.named_entry,
            "builtin_host": name in BUILTIN_CONNECTORS,
        }
        if name in BUILTIN_CONNECTORS:
            fields = (
                _map("connector_join", required=True, value={"connector": name}),
                *(
                    Field(f"settings.{key}", "mappingJson", required=required, note=note)
                    for key, required, note in BUILTIN_HOST_REQUIRES[name]
                ),
                _map("secrets", note=f"each secret the host requires, as a {SECRET_REFERENCE}"),
            )
            note = _SPARK_NOTE
        else:
            fields = _code_fields(name)
    else:
        if backend not in SourceType.__members__:
            raise ValueError(f"picker type {name!r} is no SourceType and no brand")
        fields = _with_object_store(name, (*_family_fields(name), *_OWN.get(name, ())))
    if name in ("google_workspace", "microsoft_365"):
        note = (
            "the account is connected (signed in) by the user on the Sources form; Polly never "
            "takes a refresh token or a password"
        )
    return _descriptor(offer, backend, None, None, code, create, fields, note)


def _descriptor(
    offer: Offer,
    backend: str,
    brand: str | None,
    carrier: str | None,
    code: dict[str, Any] | None,
    create: Create,
    fields: tuple[Field, ...],
    note: str = "",
) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(
        name=offer.name,
        label=offer.label,
        category=offer.category,
        default_port=offer.default_port,
        managed=offer.managed,
        backend_type=backend,
        brand=brand,
        carrier=carrier,
        code=code,
        create=create,
        fields=fields,
        note=note,
        mapping_json=tuple(dict.fromkeys(f.name for f in fields if f.at == "mappingJson")),
        federation_hints=tuple(
            dict.fromkeys(f.name for f in fields if f.at == "federationHintsJson")
        ),
    )


def _branded(offer: Offer) -> SourceTypeDescriptor:
    name = offer.name
    carrier = brand_carriers()[name]
    backend = TYPE_ALIAS.get(carrier, carrier)
    if carrier == "graphql":
        return _descriptor(
            offer, backend, name, carrier, None, _rest(_GRAPHQL_ROUTE), _graphql_brands()[name][1]
        )
    if carrier == "openapi":
        return _descriptor(
            offer, backend, name, carrier, None, _rest(_OPENAPI_ROUTE), _openapi_brands()[name][1]
        )
    fields = _WIKIPEDIA_FIELDS if carrier == "files" else _WIKIDATA_FIELDS
    return _descriptor(offer, backend, name, carrier, None, _CREATE_SOURCE, fields)


def source_types() -> tuple[SourceTypeDescriptor, ...]:
    """Every source type the Sources picker offers, in its order."""
    carriers = brand_carriers()
    return tuple(
        _branded(offer) if offer.name in carriers else _plain_type(offer) for offer in _offers()
    )


def source_type(name: str) -> SourceTypeDescriptor:
    """One offered type by its picker name, its backend type or its brand; refused by name."""
    every = source_types()
    for d in every:
        if d.name == name:
            return d
    for d in every:
        if d.backend_type == name and d.brand is None:
            return d
    raise KeyError(f"{name!r} is not a source type the product offers")


def as_json(d: SourceTypeDescriptor) -> dict[str, Any]:
    """A descriptor as plain JSON, without empty parts."""
    out = asdict(d)
    out["create"] = {k: v for k, v in out["create"].items() if v}
    out["fields"] = [
        {
            k: v
            for k, v in f.items()
            if (v not in ("", None, False) and v != ()) or k in ("name", "at")
        }
        for f in out["fields"]
    ]
    out["mapping_json"] = list(d.mapping_json)
    out["federation_hints"] = list(d.federation_hints)
    return out


def summary(d: SourceTypeDescriptor) -> dict[str, Any]:
    """One line of the list: name, label, category, backend type and brand."""
    return {
        "name": d.name,
        "label": d.label,
        "category": d.category,
        "backend_type": d.backend_type,
        **({"brand": d.brand} if d.brand else {}),
        "create": d.create.via if d.create.via != "admin_request" else d.create.path,
    }


def families() -> dict[str, list[str]]:
    """The connection-field families the Sources form renders by (the UI's constants.ts)."""
    return {
        "simpleRdbms": sorted(SIMPLE_RDBMS),
        "hostPortOnly": sorted(HOST_PORT_ONLY),
        "fileSources": sorted(FILE_SOURCES),
        "noConnection": sorted(NO_CONNECTION_TYPES),
        "dataLake": sorted(DATA_LAKE),
        "cloudFileTypes": sorted(CLOUD_FILE_TYPES),
    }


# ---------------------------------------------------------------------------------------------
# Registration: how a type's tables are registered (REQ-2114, amended 2026-11-24).
# Every name is the admin GraphQL schema's own (tests/unit/test_polly_register_relate.py holds
# them to it): a mutation, a query that finds what is offered, a TableInput field.
# ---------------------------------------------------------------------------------------------

#: The mutations that register tables: one TableInput, or several in one action, each table's
#: outcome reported on its own (REQ-2234).
REGISTER_MUTATIONS: tuple[str, ...] = ("registerTable", "registerTables")

#: The TableInput fields every registration sets.
REGISTER_BASE_FIELDS: tuple[str, ...] = (
    "sourceId",
    "domainId",
    "schemaName",
    "tableName",
    "columns",
)


@dataclass(frozen=True)
class Registration:
    """One kind of table registration a source type uses."""

    kind: Literal["tables", "connection", "file_glob", "openapi_operation", "code", "cypher"]
    #: The admin queries that find what to register, in the order they are asked.
    find: tuple[str, ...]
    #: The TableInput fields this kind sets beyond :data:`REGISTER_BASE_FIELDS`.
    fields: tuple[str, ...]
    note: str
    mutations: tuple[str, ...] = REGISTER_MUTATIONS


_LISTED = ("availableSchemas", "availableTables", "availableColumnsMetadata")

_TABLES = Registration(
    "tables",
    find=_LISTED,
    fields=(),
    note="availableSchemas(sourceId) names the schemas, availableTables(sourceId, schemaName) "
    "the tables each offers, availableColumnsMetadata(sourceId, schemaName, tableName) a "
    "table's columns; each column is registered with at least name and visibleTo. One table is "
    "registerTable(input: TableInput!); several are registerTables(inputs: [TableInput!]!), "
    "whose answer is each table's own outcome.",
)

_CONNECTION = Registration(
    "connection",
    find=_LISTED,
    fields=("pagination",),
    note="a remote GraphQL table is registered as any listed table; a connection table (its "
    "rows under a Relay connection) may bound its rows with pagination {maxRows}, lower than "
    "the organization's graphql_remote max_rows and never higher; it takes no page parameters.",
)

_FILE_GLOB = Registration(
    "file_glob",
    find=("fileGlobPreview", *_LISTED),
    fields=("fileGlob", "sourceFileColumn"),
    note="a files source registers each listed file as a table, or ONE table over many files: "
    "fileGlobPreview(sourceId, glob, tables) shows the files a glob reads (relative to the "
    "source's path), the columns they share and the files that differ; or, with `tables` (the "
    "listed tables picked), derives the glob for them. Register it with fileGlob set to that "
    "glob and the shared columns; sourceFileColumn names an optional column carrying each "
    "row's file path. fileGlob is refused on any other source type.",
)

_OPENAPI_OPERATION = Registration(
    "openapi_operation",
    find=("availableTables", "availableColumnsMetadata", "sampleOpenapiColumns"),
    fields=("pagination", "arguments"),
    note="schemaName is 'openapi'; availableTables(sourceId, schemaName: \"openapi\") lists the "
    "GET operations that return rows, each a table named by its operation; "
    "availableColumnsMetadata gives the response's columns and the operation's parameters as "
    "columns (nativeFilterType set, nativeFilterRequired where the API requires it; "
    "requiredBySteward makes one required). An operation whose response declares no schema "
    "has its columns suggested by sampleOpenapiColumns(sourceId, tableName, params). "
    "pagination (PagingInput) is how the endpoint pages: type (one of "
    + ", ".join(t.value for t in PaginationType)
    + ", as the API pages) with pageParam, pageSizeParam, pageSize, cursorParam, cursorField and "
    "maxPages, and rowsField naming the property the rows sit under (it may be set with no "
    "type, for an answer wrapped and not paged). arguments fixes parameter values by name for "
    "every read, as a JSON object.",
)

_CODE = Registration(
    "code",
    find=_LISTED,
    fields=("arguments",),
    note="schemaName is 'default'; availableTables lists the code's @table functions and every "
    "item its @discover function generates, each by the name it is served under (@discover "
    "itself is never offered). availableColumnsMetadata gives the row type's columns and each "
    "parameter as a column; arguments fixes parameter values by name for every read.",
)

_CYPHER = Registration(
    "cypher",
    find=("availableSchemas", "neo4jPreview"),
    fields=("queryTemplate",),
    note="schemaName is 'neo4j'; a table is the rows a Cypher statement returns: queryTemplate "
    "is required, its columns those the statement returns (neo4jPreview runs it to show them).",
)


def registration(d: SourceTypeDescriptor) -> tuple[Registration, ...]:
    """The kinds of table registration a source of type ``d`` uses."""
    backend = d.backend_type
    if backend == "openapi":
        return (_OPENAPI_OPERATION,)
    if d.code is not None:
        return (_CODE,)
    if backend == "neo4j":
        return (_CYPHER,)
    if backend == "graphql_remote":
        return (_CONNECTION,)
    if backend == "files":
        return (_TABLES, _FILE_GLOB)
    return (_TABLES,)


def registration_kinds() -> tuple[Registration, ...]:
    """Every kind of registration, once each."""
    return (_TABLES, _CONNECTION, _FILE_GLOB, _OPENAPI_OPERATION, _CODE, _CYPHER)


def registration_json(d: SourceTypeDescriptor) -> dict[str, Any]:
    """How ``d``'s tables are registered, as plain JSON."""
    return {
        "base_fields": list(REGISTER_BASE_FIELDS),
        "kinds": [
            {k: list(v) if isinstance(v, tuple) else v for k, v in asdict(r).items()}
            for r in registration(d)
        ],
    }
