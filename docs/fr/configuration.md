# Référence de configuration

Provisa se configure via un fichier YAML (par défaut : `config/provisa.yaml`). (REQ-528)

## Sources

```yaml
sources:
  - id: sales-pg           # unique identifier
    type: postgresql
    host: postgres
    port: 5432
    database: provisa
    username: provisa
    password: ${env:PG_PASSWORD}  # secret resolution
    pool_min: 1
    pool_max: 5
    use_pgbouncer: false
    pgbouncer_port: 6432
```

Toutes les sources partagent un ensemble de champs commun. [tool-verified: `provisa/core/models.py:129-212`]

| Champ | Défaut | Remarques |
|-------|---------|-------|
| `id` | requis | Alphanumérique, tirets, underscores |
| `type` | requis | Voir le tableau ci-dessous |
| `host` | `""` | Nom d'hôte ou IP |
| `port` | `0` | `0` signifie que chaque connecteur fournit son propre défaut — il n'existe pas de table de ports par défaut centralisée |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Prend en charge la résolution de secret `${env:VAR}` |
| `path` | `null` | Chemin de fichier ou URI pour les sources basées sur fichier |
| `base_url` | `null` | URL de base pour les sources API |
| `pool_min` / `pool_max` | `1` / `5` | Bornes du pool de connexions |
| `cache_enabled` | `true` | Active/désactive le cache pour toutes les tables de cette source |
| `cache_ttl` | `null` | Secondes ; `null` hérite du défaut global |
| `federation_hints` | `{}` | Paramètres étendus par connecteur (dict[str,str]) ; voir la référence de type ci-dessous. REQ-281 |
| `mapping` | `{}` | DSL de correspondance pour redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Restreint cette source à des IDs de domaine spécifiques ; vide = illimité |
| `description` | `""` | |

### Types de sources pris en charge [tool-verified: `provisa/core/models.py:36-101`]

| Type | Style de connexion | Remarques |
|------|-----------------|-------|
| **RDBMS** | | |
| `postgresql` | host/port | Pool Asyncpg ; PgBouncer optionnel via `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (fichier DB) | Extension communautaire firebird de DuckDB (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Réutilise le driver/dialecte PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | Réutilise le driver/dialecte PostgreSQL (REQ-950) |
| `greenplum` | host/port | Réutilise le driver/dialecte PostgreSQL (REQ-950) |
| `tidb` | host/port | Réutilise le driver/dialecte MySQL (REQ-950) |
| **Entrepôt cloud** | | |
| `snowflake` | host/port + `federation_hints` | `account` requis dans les hints |
| `bigquery` | `federation_hints` | `project` requis ; authentification via `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` requis dans les hints |
| `fabric` | variables d'env ou `PROVISA_ENGINE_URL` | T-SQL sur TDS, authentification Azure AD |
| `synapse` | variables d'env ou `PROVISA_ENGINE_URL` | T-SQL sur TDS, authentification Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | Le hint `secure` active TLS ; port par défaut 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | Endpoint REST du contrôleur |
| `druid` | host/port | Endpoint Avatica du broker |
| `exasol` | host/port | |
| **Lac de données** | | |
| `delta_lake` | `path` (URI de table) | `delta_scan` de DuckDB ; accès au stockage objet via `federation_hints` |
| `iceberg` | `path` (URI de table) | `iceberg_scan` de DuckDB ; accès au stockage objet via `federation_hints` |
| `hudi` | `path` (URI de table) | Moteur Hudi de ClickHouse, sans copie (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de stockage dans `mapping["storage"]` : hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + clés S3 `mapping` | Type distinct ; toujours stockage S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Champs de connexion simples ; pas de DSL de correspondance |
| `cassandra` | host/port | Champs de connexion simples ; pas de DSL de correspondance |
| `redis` | host/port + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | enregistrement seulement | La configuration réelle vit dans `kafka_sources[]` ; voir §Kafka ci-dessous |
| `websocket` | host/port/path + `federation_hints` | Flux WebSocket externe |
| `rss` | host/port/path + `federation_hints` | Flux RSS 2.0 / Atom |
| **Graphe/Sémantique** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **Fichier** | | |
| `sqlite` | `path` | Toujours routé via le moteur (pas de pool direct) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (répertoire) | Explorateur par glob ; expose CSV/Parquet/XLSX/JSON comme tables |
| **API/Distant** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port ou `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + `mapping` optionnel | En-têtes, transfert des en-têtes client, délai d'expiration dans `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (emplacement Flight) | Extension airport de DuckDB (REQ-899) |
| `ingest` | récepteur push | Les services externes envoient des événements JSON en POST |
| **SaaS** | | |
| `sharepoint` | `base_url` ou `host` + `mapping` | Authentification via `mapping.auth_type` |
| `splunk` | `host`/`port` ou `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | sujet + `domain_id` | Modèle `GovDataSource` séparé ; voir §GovData ci-dessous |

### Référence des types de source

Les types nécessitant une configuration non évidente ont chacun une courte entrée ci-dessous. Les types RDBMS (postgresql, mysql, etc.) n'utilisent que les champs communs ci-dessus — aucune section supplémentaire n'est nécessaire.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Les sources `govdata` utilisent un modèle de premier niveau distinct, `GovDataSource`, pas le `Source` générique. (REQ-540) L'accès est partitionné par regroupement de sujet.

```yaml
sources:
  - id: federal-data
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    api_key: ${env:GOVDATA_API_KEY}   # optional
    start_year: 2020                   # optional year filter
    end_year: 2024                     # optional year filter
```

Chaque sujet correspond à un ou plusieurs schémas GovData. Configurer une source `govdata` avec un sujet expose automatiquement tous les schémas de ce sujet. (REQ-540)

| Sujet | Schémas |
|---------|---------|
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ`, `econ_reference` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec` |

Les schémas `ref` et `geo` sont toujours inclus comme schémas de liaison — non configurables et non listés ci-dessus. (REQ-541) Utilisez le sujet `ALL` pour accorder l'accès à tous les schémas. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

La ligne `kafka` dans `sources:` sert uniquement à l'enregistrement. Sa méthode `details()` du connecteur retourne `{}` — la configuration réelle vit dans le bloc de premier niveau `kafka_sources[]`, pas dans une ligne `sources:`. Kafka est toujours une VIRTUAL_SOURCE (routée via le moteur ; pas de pool direct). [tool-verified: `provisa/transpiler/router.py:44-63`]

```yaml
kafka_sources:
  - id: event-stream
    bootstrap_servers: kafka:9092
    schema_registry_url: http://schema-registry:8081  # optional
    topics:
      - id: order-created
        topic: orders.events
        default_window: 1h          # auto-injected time bound
        schema_source: manual       # manual, registry, or sample
        value_format: json
        discriminator:              # filter shared topic by message type
          field: event_type
          value: OrderCreated
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: amount
            type: double
          - name: metadata
            type: varchar           # raw JSON for complex nested data
      - id: order-shipped
        topic: orders.events        # same physical topic
        default_window: 1h
        discriminator:
          field: event_type
          value: OrderShipped
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: shipped_at
            type: timestamp
```

**Fenêtre temporelle** — `default_window` borne chaque requête à une période récente, évitant les lectures illimitées sur des topics à fort volume. (REQ-148) Format : `1h`, `30m`, `7d`, `60s`. Par défaut `1h`. Auto-injecté comme `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Les clients peuvent le surcharger avec leur propre filtre `_timestamp` dans l'argument `where` GraphQL.

**Discriminateur** — Plusieurs configurations de topic peuvent pointer vers le même topic Kafka physique avec des valeurs `discriminator` différentes, produisant des types GraphQL distincts. (REQ-149) Le discriminateur est auto-injecté comme clause WHERE.

**Source du schéma**

| Valeur | Comportement |
|-------|----------|
| `registry` | Récupère le schéma depuis Confluent Schema Registry |
| `manual` | Définit les colonnes en ligne dans la configuration (pas besoin de Schema Registry) |
| `sample` | Découvre automatiquement à partir de messages échantillons |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` dans `federation_hints` est requis. `warehouse`, `role`, et `schema` sont optionnels.

```yaml
sources:
  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    database: MY_DB
    federation_hints:
      account: myorg-myaccount     # required
      warehouse: COMPUTE_WH
      role: PROVISA_ROLE
      schema: PUBLIC               # remote schema override
```

#### Databricks [tool-verified: `provisa/executor/drivers/databricks.py:34-52`]

`http_path` dans `federation_hints` est requis. `password` porte le jeton d'accès personnel. `catalog` est optionnel (porté dans le SQL/les hints, pas dans le champ `database`).

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required
      catalog: my_unity_catalog              # optional
```

#### BigQuery [tool-verified: `provisa/federation/connector_duckdb.py:238`]

`project` dans `federation_hints` est requis. L'authentification utilise `GOOGLE_APPLICATION_CREDENTIALS` (chemin vers un fichier de clé de compte de service) ou les Application Default Credentials dans l'environnement du moteur.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Les deux utilisent T-SQL sur TDS avec authentification Azure AD. Authentifiez-vous avec `az login` (développement) ou une identité managée (production) — le moteur lit les identifiants via `DefaultAzureCredential` de `azure-identity`. Les détails de connexion proviennent des variables d'environnement : `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) ou `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), ou via `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` dans `federation_hints` active TLS sur l'interface HTTP. Le port est par défaut `8123` (non chiffré) ou `8443` (quand `secure: "true"`). `schema` dans `federation_hints` surcharge le schéma distant. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

```yaml
sources:
  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"    # uses port 8443; omit to use 8123
      schema: analytics
```

#### Delta Lake / Iceberg [tool-verified: `provisa/federation/connector_duckdb.py:291-327`]

`path` est l'URI de la table (S3, GCS, ADLS, ou local). L'accès au stockage objet nécessite des identifiants `federation_hints`. Pour Cloudflare R2, ajoutez `account_id`.

```yaml
sources:
  - id: events-delta
    type: delta_lake
    path: s3://my-bucket/data/events
    federation_hints:
      access_key_id: ${env:S3_ACCESS_KEY}
      secret_access_key: ${env:S3_SECRET}

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

#### Hive / Hive S3 [tool-verified: `provisa/federation/trino_connectors.py:244-363`]

`host` et `port` pointent vers le metastore Thrift Hive (port par défaut 9083). Pour `hive`, définissez `mapping["storage"]` pour choisir le backend de stockage objet. Les clés requises manquantes échouent bruyamment — pas de repli. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` est un type distinct qui déclare toujours un stockage S3 (REQ-229) ; `mapping.storage` n'est pas nécessaire.

```yaml
sources:
  - id: hive-s3-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: s3
      endpoint: https://s3.us-east-1.amazonaws.com
      access_key_id: ${env:AWS_ACCESS_KEY_ID}
      secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
      region: us-east-1
      path_style: true           # required for MinIO and non-AWS S3-compatible endpoints

  - id: hive-adls-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: adls
      storage_account: mystorageaccount
      access_key: ${env:ADLS_ACCESS_KEY}
      # sas_token: ${env:ADLS_SAS_TOKEN}   # alternative to access_key
```

Valeurs acceptées pour `mapping.storage` : `hadoop` (par défaut), `hdfs`, `local`, `s3`, `azure`, `adls`. Clés de correspondance S3 : `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Clés de correspondance ADLS : `storage_account`, `access_key` ou `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Utilise le DSL `mapping`. `mongodb` et `cassandra` utilisent des champs de connexion simples et n'utilisent PAS le DSL de correspondance.

```yaml
sources:
  - id: my-redis
    type: redis
    host: redis.internal
    port: 6379
    password: ${env:REDIS_PASSWORD}
    mapping:
      tables:
        - name: sessions
          key_pattern: "session:*"
          key_column: key           # default "key"
          value_type: hash          # hash | string | zset | list; default hash
          columns:
            - name: user_id
              data_type: VARCHAR
              field: user_id        # Redis hash field name
            - name: expires_at
              data_type: BIGINT
              field: expires_at
```

#### Elasticsearch [tool-verified: `provisa/core/trino_catalog_files.py:78-104`]

```yaml
sources:
  - id: my-es
    type: elasticsearch
    host: es.internal
    port: 9200
    username: elastic
    password: ${env:ES_PASSWORD}
    mapping:
      tls: true
      tables:
        - name: logs
          index: app-logs-*
          discover: false
          columns:
            - name: timestamp
              data_type: TIMESTAMP
              path: "@timestamp"
            - name: level
              data_type: VARCHAR
              path: level
            - name: message
              data_type: VARCHAR
              path: message
```

#### Prometheus [tool-verified: `provisa/core/trino_catalog_files.py:107-124`]

`mapping.url` surcharge `host:port` quand les deux sont présents.

```yaml
sources:
  - id: my-prometheus
    type: prometheus
    mapping:
      url: http://prometheus.internal:9090
      tables:
        - name: http_requests
          metric: http_requests_total
          labels_as_columns: [method, status, handler]
          value_column: value      # default "value"
          default_range: 1h        # default "1h"
```

#### Google Sheets [tool-verified: `provisa/federation/connector_duckdb.py:273-275`]

`spreadsheet_id` dans `federation_hints` est requis. L'authentification utilise un SECRET DuckDB `gsheet` provisionné au moment de l'attache.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Sources fichier (csv / parquet / sqlite / files)

`path` est requis. `files` explore un répertoire à la recherche de fichiers CSV, Parquet, XLSX et JSON, exposant chacun comme une table. Toutes les sources basées sur fichier sont VIRTUAL (routées via le moteur ; pas de pool direct). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### Sources API / Distantes

**openapi** — définissez `base_url` sur l'URL de base OpenAPI. La découverte de schéma lit la spécification OpenAPI au démarrage.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — définissez `base_url`. Clés `mapping` optionnelles : `headers` (dict d'en-têtes statiques), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

```yaml
sources:
  - id: orders-gql
    type: graphql_remote
    base_url: https://orders.internal/graphql
    mapping:
      headers:
        X-Api-Key: ${env:ORDERS_API_KEY}
      forward_client_headers: true
      timeout_seconds: 30
```

**airport** — `base_url` est l'emplacement du serveur Arrow Flight. Extension airport de DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — utilisez `host`, `port`, `path`, et `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

```yaml
sources:
  - id: market-feed
    type: websocket
    host: feed.example.com
    port: 443
    path: /ws/v1
    federation_hints:
      use_ssl: "true"
      subscribe_payload: '{"action":"subscribe","channels":["ticker"]}'
      event_path: data

  - id: news-rss
    type: rss
    host: feeds.example.com
    port: 443
    path: /rss/latest
    federation_hints:
      use_ssl: "true"
      poll_interval: "300"      # seconds
      # feed_url: https://...  # overrides host/port/path when set
```

**sharepoint** [tool-verified: `provisa/federation/trino_connectors.py:394-423`]

```yaml
sources:
  - id: my-sharepoint
    type: sharepoint
    base_url: https://myorg.sharepoint.com/sites/data
    username: ${env:SP_CLIENT_ID}
    password: ${env:SP_CLIENT_SECRET}
    database: ${env:SP_TENANT_ID}
    mapping:
      auth_type: CLIENT_CREDENTIALS   # default
      # certificate_path: /path/to/cert.pem
      # certificate_password: ${env:CERT_PASSWORD}
```

**splunk** [tool-verified: `provisa/federation/trino_connectors.py:426-457`]

```yaml
sources:
  - id: my-splunk
    type: splunk
    host: splunk.internal
    port: 8089
    password: ${env:SPLUNK_TOKEN}
    database: search           # Splunk app name (optional)
    mapping:
      use_token: true          # default; false = username/password auth
      datamodel_filter: ""     # optional Splunk Data Model filter
      disable_ssl_validation: false
```

## Domaines

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Nommage

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Convention de nommage

L'autorité de nommage est la source de vérité unique pour les noms côté client ; les noms de colonnes physiques du backend ne sont jamais exposés aux clients. (REQ-194) Chaque langage de requête dérive le nom d'une colonne à partir de son `column.alias` s'il est défini, sinon à partir du nom de colonne physique via sa convention configurée. (REQ-194)

La convention GraphQL est l'un de trois préréglages énumérés. (REQ-416) Les anciennes chaînes libres (`none`, `snake_case`, `camelCase`, `PascalCase`) sont dépréciées. (REQ-416)

| Préréglage | Défaut | Noms de type | Noms de champ | Noms de mutation |
|--------|---------|------------|-------------|----------------|
| `apollo_graphql` | oui | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convention GraphQL par défaut est `apollo_graphql`, qui produit des noms de champ et de mutation en camelCase. (REQ-194, REQ-416) La convention SQL est distincte, avec `snake_case` par défaut, appliquée via `apply_sql_name()` ; la convention GraphQL est appliquée via `apply_gql_name()`, et le nom CQL est dérivé du nom GraphQL. (REQ-194)

`domain_prefix: bool` est une option orthogonale qui s'applique quel que soit le préréglage choisi. (REQ-416)

Un `column.alias` explicite est le nom canonique : SQL l'utilise tel quel sans appliquer de convention, GraphQL lui applique sa convention, et CQL le dérive du nom GraphQL. (REQ-194)

Surcharge par source :
```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Surcharge par table :
```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Préfixe de domaine

Quand `domain_prefix: true`, tous les noms de champ et de type GraphQL sont préfixés par l'ID de domaine avec un séparateur double underscore : (REQ-154)

| Table | Domaine | Nom de champ |
|-------|--------|-----------|
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Cela évite les collisions de noms lorsque différents domaines ont des tables portant le même nom, et rend les requêtes auto-documentées.

### Règles de nommage

Règles regex appliquées aux noms de table lors de la génération des noms de champ GraphQL. Appliquées dans l'ordre avant la résolution d'unicité. (REQ-542)

## Tables

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    alias: purchase_orders     # optional: override GraphQL name
    description: "Customer purchase orders"  # optional: GraphQL description
    columns:
      - name: id
        visible_to: [admin, analyst]
        writable_by: []           # read-only (empty = no writes)
      - name: email
        visible_to: [admin, analyst]
        writable_by: [admin]      # only admin can mutate
        unmasked_to: [admin]      # admin sees raw, analyst sees masked
        mask_type: regex
        mask_pattern: "^(.{2}).*(@.*)$"
        mask_replace: "$1***$2"
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        writable_by: [admin]
        unmasked_to: [admin]
        mask_type: constant
        mask_value: "0"
      - name: created_at
        visible_to: [admin, analyst]
        writable_by: []           # nobody can write
        unmasked_to: [admin]
        mask_type: truncate
        mask_precision: month
    column_presets:               # auto-set values on insert/update
      - column: created_by
        source: header            # from request header
        name: X-User-ID
      - column: updated_at
        source: now               # current timestamp
```

### Alias

Les alias de table et de colonne surchargent le nom GraphQL par défaut. (REQ-155) Utile pour :
- Renommer des noms de base de données cryptiques (par ex. `tbl_cust_seg` → `customer_segments`)
- Éviter les abréviations dans la couche API
- Créer un vocabulaire propre et spécifique au domaine

### Descriptions

Les descriptions de table et de colonne sont incluses dans le SDL GraphQL généré. (REQ-156) Elles apparaissent dans l'explorateur de documentation de GraphiQL et dans les requêtes d'introspection. Définissez-les dans la configuration YAML ou via l'interface d'administration.

### Path (extraction JSON calculée)

Les colonnes peuvent extraire des valeurs d'une colonne source JSON/JSONB via un `path` en notation pointée. (REQ-151) Utile pour les données semi-structurées dans les messages Kafka, les documents MongoDB, ou les colonnes JSONB PostgreSQL.

```yaml
columns:
  - name: payload
    type: varchar
    visible_to: []            # hide the raw JSON column
  - name: order_id
    type: integer
    path: payload.order_id    # extracts from payload column
    visible_to: [admin, analyst]
  - name: customer_name
    type: varchar
    path: payload.customer.name
    visible_to: [admin, analyst]
```

Le format du path est `source_column.key1.key2...`. Le compilateur génère `json_extract_scalar(source_column, '$.key1.key2')` dans le SQL. (REQ-151)

**Impact sur le routage :** les colonnes de type path utilisent les opérateurs JSON de PostgreSQL (`->>`), pris en charge nativement par le routage PG direct. (REQ-152) Pour les sources non-PostgreSQL (MySQL, SQL Server, etc.), les requêtes avec des colonnes de type path sont automatiquement routées via le moteur de fédération. (REQ-152) Les mutations ne sont pas affectées puisque les colonnes de type path sont des champs calculés en lecture seule. (REQ-153)

### Types de masquage

| Type | Champs | Description |
|------|--------|-------------|
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (colonnes texte uniquement) |
| `constant` | `value` | Remplacement littéral (NULL, 0, MAX, MIN, personnalisé) |
| `truncate` | `precision` | DATE_TRUNC (colonnes date/timestamp uniquement) |

## Relations

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one   # or: one-to-many

  - id: orders-to-reviews
    source_table_id: orders        # sales-pg source
    target_table_id: product_reviews  # reviews-mongo source
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV for this cross-source join
    refresh_interval: 600          # refresh every 10 minutes
```

### Auto-matérialisation

Définissez `materialize: true` sur une relation pour générer automatiquement une vue matérialisée pour les JOIN inter-sources. (REQ-158) Cela évite des requêtes fédérées coûteuses en précalculant le résultat du JOIN.

- Seules les relations inter-sources génèrent des MV (les JOIN même-source sont déjà rapides) (REQ-159)
- La MV démarre périmée et est peuplée par la boucle de rafraîchissement en arrière-plan (REQ-160)
- Les mutations sur l'une ou l'autre table source marquent la MV comme périmée pour un nouveau rafraîchissement (REQ-543)
- `refresh_interval` vaut 300 secondes (5 minutes) par défaut (REQ-543)

## Rôles

```yaml
roles:
  - id: admin
    capabilities:
      - source_registration
      - table_registration
      - relationship_registration
      - security_config
      - query_development
      - full_results
      - admin
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Les rôles avec `parent_role_id` héritent des capacités et de l'accès aux domaines du rôle parent. (REQ-215) La hiérarchie est aplatie au démarrage. (REQ-215)

### Capacités

| Capacité | Description |
|-----------|-------------|
| `source_registration` | Enregistrer des sources de données |
| `table_registration` | Enregistrer des tables |
| `relationship_registration` | Définir des relations |
| `security_config` | Configurer RLS, masquage |
| `query_development` | Exécuter des requêtes |
| `full_results` | Contourner les limites d'échantillonnage |
| `admin` | Toutes les capacités |

## Règles RLS

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Vues matérialisées

```yaml
materialized_views:
  - id: mv-orders-customers
    source_tables: [orders, customers]
    join_pattern:
      left_table: orders
      left_column: customer_id
      right_table: customers
      right_column: id
      join_type: left
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Vues (jeux de données calculés gouvernés)

Les vues sont des jeux de données calculés définis en SQL avec une gouvernance complète au niveau des colonnes. (REQ-133) Elles constituent le mécanisme gouverné pour ajouter des agrégations, transformations et métriques dérivées à la couche sémantique. (REQ-136)

```yaml
views:
  - id: monthly-revenue
    sql: |
      SELECT DATE_TRUNC('month', created_at) AS month,
             region,
             SUM(amount) AS revenue,
             COUNT(*) AS order_count
      FROM orders
      GROUP BY 1, 2
    description: "Monthly revenue by region"
    domain_id: sales-analytics
    materialize: true
    refresh_interval: 3600
    columns:
      - name: month
        visible_to: [admin, analyst]
      - name: region
        visible_to: [admin, analyst]
      - name: revenue
        visible_to: [admin]
      - name: order_count
        visible_to: [admin, analyst]
```

| Champ | Requis | Description |
|-------|----------|-------------|
| `id` | Oui | Identifiant unique de la vue |
| `sql` | Oui | Instruction SQL SELECT définissant la vue |
| `domain_id` | Oui | Domaine pour la visibilité de schéma |
| `materialize` | Non | `true` = rafraîchissement CTAS périodique, `false` = vue fédérée en direct |
| `refresh_interval` | Non | Secondes entre rafraîchissements (matérialisée uniquement, défaut 300) |
| `description` | Non | Apparaît dans le SDL GraphQL |
| `alias` | Non | Surcharge le nom GraphQL |
| `columns` | Oui | Définitions de colonnes avec visibilité, masquage, descriptions |

### Matérialisée vs en direct

- **`materialize: true`** : Provisa crée une table via CTAS et la rafraîchit selon une planification. (REQ-135) Requêtes plus rapides mais les données peuvent être périmées jusqu'à `refresh_interval` secondes.
- **`materialize: false`** : Provisa crée une vue fédérée. (REQ-135) Les requêtes retournent toujours des données en direct mais peuvent être plus lentes pour des agrégations complexes.

Les vues passent par le même pipeline de gouvernance que les tables — RLS, masquage, échantillonnage, et visibilité par rôle. (REQ-134) Cela garantit qu'aucune nouvelle sémantique ne peut être ajoutée à la plateforme sans supervision du data steward. (REQ-136)

### Vues en lecture seule

Les vues `materialize: true` comme `materialize: false` exposent leur type GraphQL en lecture seule. Aucune mutation d'insertion, upsert, mise à jour ou suppression n'est générée pour les relations adossées à `view_sql`. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Hiérarchie du cache

Ordre de résolution du TTL (le plus spécifique gagne) : **table** > **source** > **défaut global**. (REQ-544) La première valeur non nulle est utilisée.

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300              # global fallback: 5 minutes

sources:
  - id: sales-pg
    cache_enabled: true          # toggle caching for all tables in this source
    cache_ttl: 600               # source override: 10 minutes

tables:
  - source_id: sales-pg
    table: orders
    cache_ttl: 60                # table override: 1 minute (frequently changing)
  - source_id: sales-pg
    table: customers
    # no cache_ttl → inherits source TTL (600s)
```

Définir `cache_enabled: false` sur une source désactive le cache pour toutes les tables de cette source, quel que soit le TTL au niveau table. (REQ-544) Les clés de cache incluent toujours `role_id` + les valeurs de contexte RLS pour le partitionnement de sécurité. (REQ-544)

## Authentification

```yaml
auth:
  provider: simple           # none, firebase, keycloak, oauth, simple
  superuser:
    username: admin
    password: ${env:PROVISA_SUPERUSER_PASSWORD}
  simple:
    allow: true
    jwt_secret: ${env:PROVISA_JWT_SECRET}
    users:
      - username: admin
        password_hash: "$2b$12$..."
        roles: [admin]
  role_mapping:
    - claim: groups
      contains: data-analysts
      provisa_role: analyst
    default_role: analyst
```

### Types de fournisseurs d'authentification

| Fournisseur | Cas d'usage | Validation du jeton |
|----------|----------|-----------------|
| `simple` | Dev/test local. Utilisateurs définis en YAML. | JWT signé avec `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (toutes méthodes). | `verify_id_token()` du SDK `firebase-admin` |
| `keycloak` | OIDC Keycloak. Rôles locataire + client mappés. | Validation JWT basée sur JWKS |
| `oauth` | OIDC générique (Okta, Azure AD, Auth0, PingFederate). | JWKS depuis l'URL de découverte |

Les identifiants superutilisateur (bloc `superuser`) fonctionnent avec n'importe quel fournisseur et se résolvent toujours vers le rôle admin avec toutes les capacités. (REQ-125) Utilisés pour la configuration initiale avant que l'authentification externe ne soit configurée.

### Exemple complet de configuration d'authentification (en commentaire)

```yaml
# auth:
#   provider: firebase
#
#   superuser:
#     username: admin
#     password: ${env:PROVISA_SUPERUSER_PASSWORD}
#
#   firebase:
#     project_id: ${env:FIREBASE_PROJECT_ID}
#     service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT}
#
#   # keycloak:
#   #   server_url: https://keycloak.example.com
#   #   # kc-tenant: set to your Keycloak tenant name (e.g. provisa)
#   #   client_id: provisa-app
#   #   client_secret: ${env:KEYCLOAK_CLIENT_SECRET}
#
#   # oauth:
#   #   discovery_url: https://login.example.com/.well-known/openid-configuration
#   #   client_id: provisa
#   #   client_secret: ${env:OAUTH_CLIENT_SECRET}
#   #   role_claim: groups
#   #   audience: provisa-api
#
#   role_mapping:
#     - claim: custom_claims.role
#       value: admin
#       provisa_role: admin
#     - claim: groups
#       contains: data-analysts
#       provisa_role: analyst
#     default_role: analyst
```

## Mutations Upsert

Pour les tables avec une clé primaire, Provisa génère automatiquement des champs de mutation `upsert_<table>`. (REQ-212) Ils se compilent en un upsert dans le dialecte cible — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` sur PostgreSQL, `ON DUPLICATE KEY UPDATE` sur MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Les colonnes de conflit sont dérivées des métadonnées de clé primaire. (REQ-212) Toutes les règles de visibilité de colonne et de permission d'écriture s'appliquent.

## Distinct On

L'argument `distinct_on` sélectionne la première ligne pour chaque valeur distincte des colonnes spécifiées. (REQ-213) Disponible sur les champs de requête racine.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Se compile en `SELECT DISTINCT ON (region) ...` sur PostgreSQL. (REQ-213) Pour les dialectes non PG, un repli par fonction de fenêtrage est utilisé. (REQ-213)

## Préréglages de colonnes (Column Presets)

Injecte automatiquement des valeurs dans les colonnes lors de l'insertion/mise à jour. (REQ-214) Défini par table dans la configuration.

```yaml
tables:
  - source_id: sales-pg
    table: orders
    column_presets:
      - column: created_by
        source: header           # from request header
        name: X-User-ID
      - column: updated_at
        source: now              # current timestamp
      - column: source_system
        source: literal          # constant value
        value: "provisa"
```

| Source | Comportement |
|--------|----------|
| `header` | Injecte la valeur depuis l'en-tête de requête HTTP nommé |
| `now` | Injecte `NOW()` (horodatage courant) |
| `literal` | Injecte une valeur constante |

Les colonnes préréglées sont injectées durant la compilation de la mutation, avant la génération SQL. (REQ-214) Elles ne sont pas visibles dans le type d'entrée de la mutation. (REQ-214)

## Rôles hérités

Les rôles peuvent hériter des capacités et de l'accès aux domaines d'un rôle parent via `parent_role_id`. (REQ-215) La hiérarchie est aplatie au démarrage. (REQ-215)

```yaml
roles:
  - id: admin
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

L'héritage multi-niveau est pris en charge. (REQ-215) Les capacités et le domain_access explicites du rôle enfant sont fusionnés avec ceux du parent. (REQ-215)

## Déclencheurs planifiés

Déclencheurs basés sur cron qui appellent une URL webhook selon une planification. (REQ-216) Utilise APScheduler. (REQ-216)

```yaml
scheduled_triggers:
  - name: daily-report
    cron: "0 8 * * *"           # 8:00 AM daily
    webhook_url: https://hooks.example.com/daily-report
    enabled: true
  - name: hourly-sync
    cron: "0 * * * *"           # every hour
    webhook_url: https://hooks.example.com/sync
    enabled: false
```

Les tâches planifiées sont gérées via l'interface d'administration (bascule activer/désactiver) ou la mutation admin `toggle_scheduled_task`. (REQ-216)

## Format OrderBy

OrderBy utilise le format `{column: direction}` avec une énumération de direction à 6 valeurs : (REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| Direction | SQL |
|-----------|-----|
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

Le tri par relation est pris en charge via des objets imbriqués : (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observabilité

```yaml
observability:
  endpoint: "http://localhost:4319"   # OTLP collector; env OTEL_EXPORTER_OTLP_ENDPOINT overrides
  service_name: provisa               # env OTEL_SERVICE_NAME overrides
  sample_rate: 1.0                    # 0.0–1.0; TraceIdRatioBased sampler
  log_level: WARNING                  # env OTEL_LOG_LEVEL overrides
  compact_batch_size: 1000
  telemetry_filter:
    redact_sql_literals: false        # strip literal values from db.statement before export
    redact_attributes: []             # attribute keys dropped entirely before export
  # support_endpoint: ""              # env PROVISA_SUPPORT_OTLP_ENDPOINT; off by default
  support_telemetry_filter:
    redact_sql_literals: true         # default on — strip literals before sending to support
    redact_attributes: []             # additional keys dropped before sending to support
```

### Filtres de télémétrie [tool-verified]

Provisa exécute deux chemins d'export OTLP indépendants : votre collecteur interne et l'endpoint de support Provisa optionnel. (REQ-545) Chaque chemin a son propre filtre. Les filtres s'exécutent à l'intérieur d'un `_FilteringExporter` englobant avant que les spans ne quittent le processus — les objets span d'origine ne sont jamais modifiés. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lignes 156–207]

**`telemetry_filter`** — contrôle ce qui atteint votre collecteur interne.

| Clé | Type | Défaut | Description |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `false` | Remplace les littéraux chaîne et numériques dans `db.statement` par `?` |
| `redact_attributes` | list[str] | `[]` | Clés d'attribut entièrement supprimées de chaque span |

**`support_telemetry_filter`** — contrôle ce qui atteint l'endpoint de support Provisa. La rédaction des littéraux SQL est activée par défaut (`true`) sur ce chemin, puisque les données de requête vous appartiennent. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` ligne 240]

| Clé | Type | Défaut | Description |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `true` | Remplace les littéraux chaîne et numériques dans `db.statement` par `?` |
| `redact_attributes` | list[str] | `[]` | Clés d'attribut entièrement supprimées de chaque span |

Exemple de `db.statement` rédigé — avec `redact_sql_literals: true`, cet attribut de span :

```
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

devient :

```
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint de support [tool-verified]

`support_endpoint` (ou variable d'env `PROVISA_SUPPORT_OTLP_ENDPOINT`) transmet la télémétrie au support Provisa à des fins de diagnostic. (REQ-548) Lorsqu'il n'est pas défini, aucune donnée ne quitte votre infrastructure par ce chemin. (REQ-548) Le filtre de support s'applique indépendamment du filtre interne — vous pouvez rédiger les littéraux SQL des deux exports tout en partageant les données de timing et d'erreur des spans avec le support. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lignes 238–288]

### Détection du protocole d'endpoint [tool-verified]

Provisa sélectionne OTLP/HTTP ou OTLP/gRPC à partir du schéma de l'URL d'endpoint. (REQ-549) Les URL commençant par `http://` ou `https://` utilisent OTLP/HTTP, avec `/v1/traces`, `/v1/metrics`, et `/v1/logs` ajoutés automatiquement. (REQ-549) Tout autre schéma utilise OTLP/gRPC avec `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lignes 60–70]

## Moteur de fédération

Configurer un moteur de fédération est optionnel. Le défaut est `duckdb` — zéro configuration, en processus, aucun service externe requis (REQ-989). Choisissez un autre moteur lorsque vous avez besoin d'une échelle MPP ou souhaitez réutiliser un entrepôt existant.

Priorité : variable d'env `PROVISA_ENGINE` → champ de configuration `federation_engine` persisté via l'interface admin → `duckdb`. Les changements prennent effet au redémarrage du service. [tool-verified: `engine.py` `build_engine`]

### Vue d'ensemble des moteurs [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Clé de moteur | Libellé | Dialecte | MPP | Mécanisme de lien externe | Authentification |
|-----------|-------|---------|-----|------------------------|------|
| `trino` | Provisa Federation Engine | SQL Trino | Oui | Catalogues Trino (large ensemble de connecteurs) | Identifiants JDBC |
| `trino-byo` | Trino (apportez le vôtre) | SQL Trino | Oui | Identique à `trino` ; coordinateur non géré | Identifiants JDBC |
| `pg` | PostgreSQL | PostgreSQL | Non | FDW / pg_duckdb | Identifiants PostgreSQL |
| `duckdb` | DuckDB | DuckDB | Non | ATTACH natif d'extension | Aucune (en processus) |
| `clickhouse` | ClickHouse (embarqué) | ClickHouse | Oui | Moteurs de table S3 / IcebergS3 / DeltaLake | chdb (en processus, sans authentification) |
| `clickhouse-server` | ClickHouse (Serveur / Cloud) | ClickHouse | Oui | Moteurs de table S3 / IcebergS3 / DeltaLake | Identifiants ClickHouse |
| `snowflake` | Snowflake | Snowflake | Oui | Stage externe + table externe | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Oui | Tables externes Unity Catalog via REST | `PROVISA_ENGINE_URL` (jeton bearer + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Oui | Tables externes BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Oui | Raccourcis OneLake → OPENROWSET | Azure AD (`az login` ou identité managée) |
| `synapse` | Azure Synapse | T-SQL | Oui | ADLS OPENROWSET / tables externes | Azure AD |
| `sqlalchemy` | SQLAlchemy (toute BD relationnelle) | Par dialecte | Non | Aucun (atterrissage uniquement) | Identifiants par dialecte |

### Référence des moteurs

#### trino / trino-byo

`trino` est le coordinateur Provisa géré ; `trino-byo` se connecte à votre propre cluster Trino. Les deux utilisent SQL Trino et ont la plus large portée de types de source.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Fédère via les extensions postgres_fdw (SQL/MED) et pg_duckdb. Nœud unique ; pas de MPP. Idéal lorsque vos données résident déjà dans PostgreSQL et que vous souhaitez joindre quelques sources distantes.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL`.

#### duckdb

En processus ; aucun service externe. Le moteur par défaut (REQ-989). `PROVISA_DATA_DIR` contrôle où réside le magasin embarqué (`~/.provisa` par défaut).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Le magasin de matérialisation est par défaut `~/.provisa/materialize.duckdb` — le seul moteur avec un magasin par défaut non-PostgreSQL.

#### clickhouse (embarqué) / clickhouse-server

`clickhouse` utilise chdb (en processus). `clickhouse-server` se connecte à une instance ClickHouse externe ou à ClickHouse Cloud. Les deux lisent Delta Lake, Iceberg, et Hudi directement via des moteurs de table ClickHouse natifs.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL`.

#### snowflake

Moteur-en-tant-qu'entrepôt : Snowflake exécute les requêtes ; Provisa pousse les données source à travers des stages externes.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL`.

#### databricks

Les tables externes Unity Catalog relient les sources gérées par Provisa à Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL`.

#### bigquery

Tables externes BigQuery et BigLake. Le projet provient de l'URL ou de `GOOGLE_CLOUD_PROJECT` ; authentification via clé de compte de service.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL`.

#### fabric / synapse

Les deux utilisent T-SQL sur TDS avec authentification Azure AD (`az login` ou identité managée). Omettez `PROVISA_ENGINE_URL` pour lire les détails de connexion depuis les variables d'environnement à la place.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL`.

#### sqlalchemy

Moteur RDBMS générique en atterrissage uniquement (pas de fédération vers des sources externes). À utiliser pour les déploiements mono-entrepôt ou les tests.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

Le magasin de matérialisation est par défaut `TENANT_DATABASE_URL`.

### Magasin de matérialisation

Lorsqu'une source ne peut pas s'attacher en direct (aucun connecteur ATTACH pour le moteur sélectionné), elle atterrit dans le magasin de matérialisation du moteur. Ordre de résolution : `PROVISA_MATERIALIZE_URL` explicite → défaut déclaré du moteur → erreur explicite (pas de repli silencieux). [tool-verified: `engine.py` `materialize_store`]

DuckDB déclare son fichier embarqué (`~/.provisa/materialize.duckdb`) comme défaut. Tous les autres moteurs utilisent par défaut `TENANT_DATABASE_URL` (PostgreSQL). Surchargez n'importe quel moteur avec `PROVISA_MATERIALIZE_URL`.

### Hints de fédération par source

Les paramètres de connexion étendus que les champs standard host/port/user/password ne peuvent pas porter vont dans `federation_hints` sur la source. Voir la référence des types de source ci-dessus pour les clés de hint par type. Un exemple consolidé :

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required for Databricks sources

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount
      warehouse: COMPUTE_WH

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"           # enable TLS on the HTTP interface

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

Pour les sources Google Cloud, définissez `GOOGLE_APPLICATION_CREDENTIALS` sur le chemin de votre fichier de clé de compte de service. Pour Fabric et Synapse, authentifiez-vous avec `az login` (développement) ou une identité managée (production) — le moteur lit les identifiants via `DefaultAzureCredential` de `azure-identity`.

## Variables d'environnement

| Variable | Défaut | Description |
|----------|---------|-------------|
| `PROVISA_CONFIG` | `config/provisa.yaml` | Chemin du fichier de configuration |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI du magasin du plan de contrôle (SQLAlchemy async) ; accepte `sqlite+aiosqlite://…` / `duckdb://…` pour le magasin desktop embarqué (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI du registre plateforme (répertoire des locataires, registre des moteurs) ; requis au démarrage, pas de repli (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` utilise fakeredis embarqué au lieu d'un serveur Redis — pas de Docker (REQ-829) |
| `PG_HOST` | `localhost` | Hôte PostgreSQL |
| `PG_PORT` | `5432` | Port PostgreSQL |
| `PG_DATABASE` | `provisa` | Base de données PostgreSQL |
| `PG_USER` | `provisa` | Utilisateur PostgreSQL |
| `PG_PASSWORD` | `provisa` | Mot de passe PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Clé du moteur de fédération (REQ-989) |
| `PROVISA_ENGINE_URL` | — | URL de connexion pour les moteurs pilotés par URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Surcharge le DSN du magasin de matérialisation (par défaut, celui déclaré par le moteur) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Répertoire de données pour le magasin DuckDB embarqué (REQ-989) |
| `TRINO_HOST` | `localhost` | Hôte du coordinateur Trino |
| `TRINO_PORT` | `8080` | Port HTTP du coordinateur Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Chemin vers le JSON de clé de compte de service GCP (moteur/source BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Projet GCP par défaut (BigQuery ; surchargé par l'URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL du Fabric Warehouse (alternative à `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nom de la base de données Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless Synapse |
| `SYNAPSE_DATABASE` | — | Nom de la base de données Synapse |
| `REDIS_URL` | — | URL de connexion Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Limite d'échantillonnage par défaut |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Plafond de lignes quand une requête ne fournit aucune `LIMIT` explicite |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Budget de nouvelle tentative de lecture de niveau 1, en secondes ; backoff exponentiel avec jitter complet (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Port du proxy Flight SQL Zaychik |
| `FLIGHT_PORT` | `8815` | Port du serveur Arrow Flight de Provisa |
| `GRPC_PORT` | `50051` | Port du serveur gRPC Protobuf de Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Active la redirection par seuil côté serveur |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Seuil de nombre de lignes par défaut |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Format de redirection par défaut |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 pour les résultats redirigés |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL d'endpoint compatible S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Clé d'accès S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Clé secrète S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de l'URL présignée (secondes) |
| `ANTHROPIC_API_KEY` | — | Clé API Claude (découverte) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Surcharge `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Surcharge `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Surcharge `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Surcharge `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Délai de vidage du processeur de span par lot |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Surcharge `observability.support_endpoint` |
