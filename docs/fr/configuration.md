# Référence de configuration

Provisa est configuré via un fichier YAML (par défaut : `config/provisa.yaml`). (REQ-528)

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

| Champ | Par défaut | Notes |
| ------- | --------- | ------- |
| `id` | requis | Alphanumérique, tirets, underscores |
| `type` | requis | Voir le tableau ci-dessous |
| `host` | `""` | Nom d'hôte ou IP |
| `port` | `0` | `0` signifie que chaque connecteur fournit son propre défaut — il n'y a pas de carte de ports par défaut centrale |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Prend en charge la résolution de secret `${env:VAR}` |
| `path` | `null` | Chemin de fichier ou URI pour les sources basées sur fichiers |
| `base_url` | `null` | URL de base pour les sources API |
| `pool_min` / `pool_max` | `1` / `5` | Bornes du pool de connexions |
| `cache_enabled` | `true` | Active/désactive le cache pour toutes les tables de cette source |
| `cache_ttl` | `null` | Secondes ; `null` hérite du défaut global |
| `federation_hints` | `{}` | Paramètres étendus par connecteur (dict[str,str]) ; voir la référence de type ci-dessous. REQ-281 |
| `mapping` | `{}` | DSL de mapping pour redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Restreint cette source à des ID de domaine spécifiques ; vide = sans restriction |
| `description` | `""` | |

### Types de source pris en charge [tool-verified: `provisa/core/models.py:36-101`]

| Type | Style de connexion | Notes |
| ------ | ----------------- | ------- |
| **SGBDR** | | |
| `postgresql` | host/port | Pool asyncpg ; PgBouncer opt-in via `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (fichier BD) | Extension communautaire DuckDB firebird (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Réutilise le pilote/dialecte PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | Réutilise le pilote/dialecte PostgreSQL (REQ-950) |
| `greenplum` | host/port | Réutilise le pilote/dialecte PostgreSQL (REQ-950) |
| `tidb` | host/port | Réutilise le pilote/dialecte MySQL (REQ-950) |
| **Entrepôt de données cloud** | | |
| `snowflake` | host/port + `federation_hints` | `account` requis dans les hints |
| `bigquery` | `federation_hints` | `project` requis ; auth via `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` requis dans les hints |
| `fabric` | variables d'env ou `PROVISA_ENGINE_URL` | T-SQL sur TDS, auth Azure AD |
| `synapse` | variables d'env ou `PROVISA_ENGINE_URL` | T-SQL sur TDS, auth Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | Le hint `secure` active TLS ; port par défaut 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | Endpoint REST du contrôleur |
| `druid` | host/port | Endpoint Avatica du broker |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI de table) | `delta_scan` DuckDB ; accès au stockage objet via `federation_hints` |
| `iceberg` | `path` (URI de table) | `iceberg_scan` DuckDB ; accès au stockage objet via `federation_hints` |
| `hudi` | `path` (URI de table) | Moteur Hudi ClickHouse, sans copie (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de stockage dans `mapping["storage"]` : hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + clés S3 `mapping` | Type distinct ; toujours stockage S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Champs de connexion simples ; pas de DSL de mapping |
| `cassandra` | host/port | Champs de connexion simples ; pas de DSL de mapping |
| `redis` | host/port + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | enregistrement uniquement | La configuration réelle vit dans `kafka_sources[]` ; voir §Kafka ci-dessous |
| `websocket` | host/port/path + `federation_hints` | Flux WebSocket externe |
| `rss` | host/port/path + `federation_hints` | Flux RSS 2.0 / Atom |
| **Graphe/Sémantique** | | |
| `neo4j` | [mapping de bout en bout NON VÉRIFIÉ] | |
| `sparql` | [mapping de bout en bout NON VÉRIFIÉ] | |
| **Fichier** | | |
| `sqlite` | `path` | Passe toujours par le moteur (pas de pool direct) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (répertoire) | Crawler glob ; expose CSV/Parquet/XLSX/JSON comme tables |
| **API/Distant** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port ou `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + `mapping` optionnel | En-têtes, forward-client-headers, timeout dans `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [mapping de bout en bout NON VÉRIFIÉ] | |
| `airport` | `base_url` (emplacement Flight) | Extension airport DuckDB (REQ-899) |
| `ingest` | récepteur push | Les services externes POST des événements JSON |
| **SaaS** | | |
| `sharepoint` | `base_url` ou `host` + `mapping` | Auth via `mapping.auth_type` |
| `splunk` | `host`/`port` ou `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | sujet + `domain_id` | Modèle `GovDataSource` séparé ; voir §GovData ci-dessous |
| **Qualité des données** | | |
| `soda` | host/port pointant vers le pgwire de Provisa | Nécessite l'extra `soda` ; Elastic License 2.0, auto-hébergé uniquement (REQ-1443) |
| `great_expectations` | host/port pointant vers le pgwire de Provisa | Nécessite l'extra `gx` ; Apache 2.0 (REQ-1443) |

### Référence de type de source

Les types nécessitant une configuration non évidente ont chacun une courte entrée ci-dessous. Les types SGBDR (postgresql, mysql, etc.) n'utilisent que les champs communs ci-dessus — aucune section supplémentaire nécessaire.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Les sources `govdata` utilisent un modèle de premier niveau séparé, `GovDataSource`, et non le `Source` générique. (REQ-540) L'accès est partitionné par regroupement de sujet.

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
| --------- | --------- |
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

Les schémas `ref` et `geo` sont toujours inclus comme schémas de liaison — non configurables et non listés ci-dessus. (REQ-541) Utilisez le sujet `ALL` pour accorder l'accès à chaque schéma. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

La ligne `kafka` dans `sources:` est d'enregistrement uniquement. Sa méthode connecteur `details()` retourne `{}` — la configuration réelle vit dans le bloc de premier niveau `kafka_sources[]`, pas dans une ligne `sources:`. Kafka est toujours une VIRTUAL_SOURCE (passe par le moteur ; pas de pool direct). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Fenêtre temporelle** — `default_window` borne chaque requête à une période récente, empêchant les lectures non bornées depuis des topics à volume élevé. (REQ-148) Format : `1h`, `30m`, `7d`, `60s`. Défaut `1h`. Auto-injecté comme `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Les clients peuvent surcharger avec leur propre filtre `_timestamp` dans l'argument GraphQL `where`.

**Discriminateur** — Plusieurs configurations de topic peuvent pointer vers le même topic Kafka physique avec des valeurs `discriminator` différentes, produisant des types GraphQL séparés. (REQ-149) Le discriminateur est auto-injecté comme clause WHERE.

**Source de schéma**

| Valeur | Comportement |
| ------- | ---------- |
| `registry` | Récupère le schéma depuis Confluent Schema Registry |
| `manual` | Définit les colonnes en ligne dans la config (pas besoin de Schema Registry) |
| `sample` | Découverte automatique depuis des messages d'exemple |

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

`http_path` dans `federation_hints` est requis. `password` porte le jeton d'accès personnel. `catalog` est optionnel (porté dans SQL/hints, pas dans le champ `database`).

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

Les deux utilisent T-SQL sur TDS avec authentification Azure AD. Authentifiez-vous avec `az login` (développeur) ou une identité managée (production) — le moteur lit les identifiants via `DefaultAzureCredential` d'`azure-identity`. Les détails de connexion proviennent de variables d'environnement : `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) ou `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), ou via `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` dans `federation_hints` active TLS sur l'interface HTTP. Le port par défaut est `8123` (clair) ou `8443` (quand `secure: "true"`). `schema` dans `federation_hints` surcharge le schéma distant. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`hive_s3` est un type distinct qui déclare toujours un stockage S3 (REQ-229) ; pas besoin de `mapping.storage`.

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

Valeurs acceptées pour `mapping.storage` : `hadoop` (défaut), `hdfs`, `local`, `s3`, `azure`, `adls`. Clés de mapping S3 : `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Clés de mapping ADLS : `storage_account`, `access_key` ou `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Utilise le DSL `mapping`. `mongodb` et `cassandra` utilisent des champs de connexion simples et n'utilisent PAS le DSL de mapping.

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
          key_pattern: "sessions:*"
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

`spreadsheet_id` dans `federation_hints` est requis. L'auth utilise un SECRET DuckDB `gsheet` provisionné au moment de l'attachement.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Sources fichier (csv / parquet / sqlite / files)

`path` est requis. `files` parcourt un répertoire pour les fichiers CSV, Parquet, XLSX, et JSON, exposant chacun comme table. Toutes les sources basées sur fichiers sont VIRTUAL (passent par le moteur ; pas de pool direct). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### Sources API / distantes

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

**airport** — `base_url` est l'emplacement du serveur Arrow Flight. Extension airport DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — utilisent `host`, `port`, `path`, et `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

#### Vérificateurs de qualité des données (soda / great_expectations)

[tool-verified: `provisa/dq/registration.py`, `provisa/events/source_loader.py` `make_dq_loader`]

Une source de vérificateur pointe vers l'endpoint pgwire de Provisa lui-même, de sorte qu'un seul pilote postgres scanne la vue fédérée d'une table adossée à Snowflake ou Iceberg. L'identité du scan est déclarée, jamais héritée — la politique s'applique à cette connexion, et un jeu de lignes filtré ne doit pas produire une vérification silencieusement réussie. Les clés de connexion proviennent de `mapping` : `host`, `port`, `database`, `user`, `password`.

```yaml
sources:
  - id: dq
    type: soda                 # or great_expectations
    domain_id: sales-analytics
    mapping:
      host: localhost
      port: 5439               # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner
      password: ${env:PROVISA_DQ_PASSWORD}
```

Chaque table de résultats porte `dq_contract` — YAML de contrat Soda ou JSON de suite Great Expectations, tel quel. Les colonnes, le filigrane (watermark) et les promotions en sont dérivés ; voir [Vérificateurs de qualité des données](sources.md#verificateurs-de-qualite-des-donnees-req-1443) pour la dérivation complète.

**Sélection au moment de l'installation.** Le vérificateur n'est pas lié en dur — le scan s'exécute dans un interpréteur enfant, et la bibliothèque n'est installée que quand un opérateur la nomme. Chaque chemin d'installeur (`install.sh`, `packaging/linux/first-launch.sh`, et l'assistant macOS via `PROVISA_DQ_CHECKER`) écrit le choix dans `~/.provisa/config.yaml` :

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` lit cette clé et exporte `PROVISA_EXTRAS`, que `docker-compose.app.yml` passe comme argument de build à `ARG PROVISA_EXTRAS` du `Dockerfile` : [tool-verified: `scripts/provisa:69-79`]

| `dq_checker` | `PROVISA_EXTRAS` (palier Docker) | Installation venv native |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Installer le jeu de données de démonstration élève `none` à `gx` et le signale, car la config de démo enregistre une suite Great Expectations sur `pet_store.pets` et son tableau de bord de qualité n'aurait sinon rien à montrer. Nommer `soda` conserve `soda`.

Atteindre la démo via pip plutôt qu'un installeur saute cette étape d'assistant, donc l'extra `demo` porte le même vérificateur : `pip install 'provisa[embedded,demo]'` est ce dont `provisa run --demo` a besoin pour que son scan s'exécute. Sans cela, le scan rapporte `data-quality checker 'great_expectations' is not installed`, en nommant la commande d'installation.

Toute autre valeur arrête le lanceur plutôt que de démarrer sans le vérificateur que l'opérateur a demandé. L'extra `soda` tire `soda-postgres` ; `gx` tire `great-expectations[postgresql]`. Soda Core est sous Elastic License 2.0 — `config/capabilities.yaml` marque l'option `cloud_eligible: false`, et le plan hébergé la refuse.

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

L'autorité de nommage est la source unique de vérité pour les noms exposés au client ; les noms de colonnes physiques du backend ne sont jamais exposés aux clients. (REQ-194) Chaque langage de requête dérive le nom d'une colonne depuis son `column.alias` s'il est défini, sinon depuis le nom de colonne physique via sa convention configurée. (REQ-194)

La convention GraphQL est l'un de trois enums prédéfinis. (REQ-416) Les anciennes chaînes libres (`none`, `snake_case`, `camelCase`, `PascalCase`) sont dépréciées. (REQ-416)

| Préréglage | Par défaut | Noms de type | Noms de champ | Noms de mutation |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | oui | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convention GraphQL par défaut est `apollo_graphql`, qui produit des noms de champ et de mutation en camelCase. (REQ-194, REQ-416) La convention SQL est séparée, avec `snake_case` par défaut, appliquée via `apply_sql_name()` ; la convention GraphQL est appliquée via `apply_gql_name()`, et le nom CQL est dérivé du nom GraphQL. (REQ-194)

`domain_prefix: bool` est une option orthogonale qui s'applique quel que soit le préréglage choisi. (REQ-416)

Un `column.alias` explicite est le nom canonique : SQL l'utilise tel quel sans convention appliquée, GraphQL lui applique sa convention, et CQL est dérivé du nom GraphQL. (REQ-194)

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

Quand `domain_prefix: true`, tous les noms de champ et de type GraphQL sont préfixés avec l'ID de domaine en utilisant un séparateur double underscore : (REQ-154)

| Table | Domaine | Nom de champ |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Cela évite les collisions de noms quand différents domaines ont des tables portant le même nom, et rend les requêtes autodocumentées.

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

- Renommer des noms de base de données cryptiques (ex. `tbl_cust_seg` → `customer_segments`)
- Éviter les abréviations dans la couche API
- Créer un vocabulaire propre et spécifique au domaine

### Descriptions

Les descriptions de table et de colonne sont incluses dans le SDL GraphQL généré. (REQ-156) Elles apparaissent dans l'explorateur de documentation de GraphiQL et dans les requêtes d'introspection. Définissez-les dans la config YAML ou via l'UI d'administration.

### Path (extraction JSON calculée)

Les colonnes peuvent extraire des valeurs depuis une colonne source JSON/JSONB en utilisant un `path` en notation pointée. (REQ-151) Utile pour les données semi-structurées dans les messages Kafka, les documents MongoDB, ou les colonnes JSONB PostgreSQL.

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

Le format du path est `source_column.key1.key2...`. Le compilateur génère `json_extract_scalar(source_column, '$.key1.key2')` en SQL. (REQ-151)

**Impact sur le routage :** les colonnes path utilisent les opérateurs JSON PostgreSQL (`->>`), qui sont pris en charge nativement par le routage direct PG. (REQ-152) Pour les sources non-PostgreSQL (MySQL, SQL Server, etc.), les requêtes avec des colonnes path sont automatiquement routées via le moteur de fédération. (REQ-152) Les mutations ne sont pas affectées puisque les colonnes path sont des champs calculés en lecture seule. (REQ-153)

### Types de masquage

| Type | Champs | Description |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (colonnes de type chaîne uniquement) |
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

- Seules les relations inter-sources génèrent des MV (les JOIN de même source sont déjà rapides) (REQ-159)
- La MV démarre périmée et est peuplée par la boucle de rafraîchissement en arrière-plan (REQ-160)
- Les mutations sur l'une ou l'autre table source marquent la MV comme périmée pour re-rafraîchissement (REQ-543)
- `refresh_interval` vaut par défaut 300 secondes (5 minutes) (REQ-543)

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

Les rôles avec `parent_role_id` héritent des capacités et de l'accès au domaine du parent. (REQ-215) La hiérarchie est aplatie au démarrage. (REQ-215)

### Capacités

| Capacité | Description |
| ----------- | ------------- |
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

Les vues sont des jeux de données calculés définis en SQL avec une gouvernance complète au niveau des colonnes. (REQ-133) Elles sont le mécanisme gouverné pour ajouter des agrégations, transformations, et métriques dérivées à la couche sémantique. (REQ-136)

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
| ------- | ---------- | ------------- |
| `id` | Oui | Identifiant unique de vue |
| `sql` | Oui | Instruction SQL SELECT définissant la vue |
| `domain_id` | Oui | Domaine pour la visibilité du schéma |
| `materialize` | Non | `true` = rafraîchissement CTAS périodique, `false` = vue fédérée en direct |
| `refresh_interval` | Non | Secondes entre rafraîchissements (matérialisée uniquement, défaut 300) |
| `description` | Non | Apparaît dans le SDL GraphQL |
| `alias` | Non | Surcharge le nom GraphQL |
| `columns` | Oui | Définitions de colonne avec visibilité, masquage, descriptions |

### Matérialisée vs en direct

- **`materialize: true`** : Provisa crée une table via CTAS et la rafraîchit selon un planning. (REQ-135) Requêtes plus rapides mais les données peuvent être périmées jusqu'à `refresh_interval` secondes.
- **`materialize: false`** : Provisa crée une vue fédérée. (REQ-135) Les requêtes retournent toujours des données en direct mais peuvent être plus lentes pour des agrégations complexes.

Les vues passent par le même pipeline de gouvernance que les tables — RLS, masquage, échantillonnage, et visibilité basée sur le rôle. (REQ-134) Cela garantit qu'aucune nouvelle sémantique ne peut être ajoutée à la plateforme sans supervision d'un steward. (REQ-136)

### Vues en lecture seule

Les vues `materialize: true` et `materialize: false` exposent toutes deux leur type GraphQL en lecture seule. Aucune mutation d'insertion, upsert, mise à jour, ou suppression n'est générée pour les relations adossées à `view_sql`. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

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

Définir `cache_enabled: false` sur une source désactive le cache pour toutes les tables de cette source, indépendamment du TTL au niveau table. (REQ-544) Les clés de cache incluent toujours `role_id` + les valeurs de contexte RLS pour le partitionnement de sécurité. (REQ-544)

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

### Types de fournisseur d'auth

| Fournisseur | Cas d'usage | Validation du jeton |
| ---------- | ---------- | ----------------- |
| `simple` | Dev/test local. Utilisateurs définis en YAML. | JWT signé avec `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (toutes méthodes). | SDK `firebase-admin` `verify_id_token()` |
| `keycloak` | Keycloak OIDC. Rôles de tenant + client mappés. | Validation JWT basée sur JWKS |
| `oauth` | OIDC générique (Okta, Azure AD, Auth0, PingFederate). | JWKS depuis l'URL de découverte |
| `basic` | Déploiements autonomes. Comptes vivant dans le magasin propre de Provisa. | Mot de passe bcrypt, ou SCRAM-SHA-256 sur pgwire |

Les identifiants superuser (bloc `superuser`) fonctionnent avec n'importe quel fournisseur et résolvent toujours vers le rôle admin avec toutes les capacités. (REQ-125) Utilisés pour la configuration initiale avant que l'auth externe ne soit configurée.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Fait annoncer par pgwire le SASL avec `SCRAM-SHA-256`, de sorte qu'un mot de passe est prouvé plutôt qu'envoyé en clair. (REQ-1394) Ne s'applique qu'au fournisseur `basic` — aucun autre fournisseur ne détient les vérificateurs RFC 5802 dont SCRAM a besoin — et le channel binding n'est pas proposé.

Les vérificateurs ne peuvent pas être dérivés des hashs bcrypt existants. Un vérificateur est écrit chaque fois qu'un mot de passe passe en clair, donc la première connexion SCRAM de chaque utilisateur suit sa prochaine inscription, connexion, changement de mot de passe ou réinitialisation admin. Jusque-là, les connexions de cet utilisateur se replient sur l'échange en clair via TLS ; le fil ne révèle pas qui a migré.

### Limitation des tentatives de connexion (`auth.login_throttle`)

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Activé par défaut avec les valeurs montrées ; le bloc ne fait que les ajuster. (REQ-1393) Le compteur se trouve dans la couche de validation d'identifiant, donc les échecs sur HTTP, pgwire et Bolt s'accumulent contre le même sujet et un verrouillage tient sur chaque surface. C'est par processus : plusieurs workers API permettent chacun jusqu'à `max_attempts`.

### Jetons d'accès personnels

Les PAT n'ont besoin d'aucun bloc de configuration — ils sont toujours acceptés, et le magasin est créé avec le reste du schéma du plan de contrôle. (REQ-1263) Ce qui est configurable, c'est l'expiration qu'un utilisateur peut demander à l'émission : de 1 à 366 jours, ou aucune pour un jeton qui n'expire pas. Voir [Modèle de sécurité](security.md#jetons-dacces-personnels).

### TLS mutuel

La vérification du certificat client est configurée par variable d'environnement plutôt que dans `provisa.yaml`, aux côtés des paramètres de certificat TLS qu'elle étend. (REQ-1228)

| Variable | Par défaut | Signification |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | non défini | Bundle PEM de la ou des AC autorisées à signer les certificats client. La définir active la vérification du certificat client |
| `PROVISA_MTLS_MODE` | `required` une fois une AC définie | `required` ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Exiger que le nom commun du certificat soit égal au nom d'utilisateur avec lequel la connexion s'authentifie |

Chaque option prend une surcharge par protocole sous le même nommage que les paramètres TLS. Un mode défini sans AC, ou un mode qui n'est ni l'une ni l'autre valeur, refuse de démarrer plutôt que de servir des connexions que l'opérateur croit vérifiées.

### Adresser une org via TLS

Rien à configurer. Sur un déploiement multi-org, pgwire et Bolt lisent l'org depuis le nom d'hôte que le client a composé, porté dans le ClientHello TLS, exactement comme HTTP le lit depuis l'en-tête `Host`. (REQ-1234) Un client se connectant à `acme.provisa.dev` demande l'org `acme` ; la requête est refusée sauf si le principal authentifié en est membre. Se connecter par adresse IP ne demande aucune org, ce qui correspond à chaque connexion sur un déploiement mono-org.

### Exemple complet de config auth (commenté)

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

Pour les tables avec une clé primaire, Provisa génère automatiquement des champs de mutation `upsert_<table>`. (REQ-212) Ceux-ci se compilent en un upsert dans le dialecte cible — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` sur PostgreSQL, `ON DUPLICATE KEY UPDATE` sur MySQL. (REQ-212)

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

Se compile en `SELECT DISTINCT ON (region) ...` en PostgreSQL. (REQ-213) Pour les dialectes non-PG, un repli à base de fonction fenêtre est utilisé. (REQ-213)

## Préréglages de colonnes

Injectent automatiquement des valeurs dans les colonnes à l'insertion/mise à jour. (REQ-214) Définis par table dans la config.

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
| -------- | ---------- |
| `header` | Injecte la valeur depuis l'en-tête de requête HTTP nommé |
| `now` | Injecte `NOW()` (timestamp actuel) |
| `literal` | Injecte une valeur constante |

Les colonnes préréglées sont injectées pendant la compilation de la mutation, avant la génération SQL. (REQ-214) Elles ne sont pas visibles dans le type d'entrée de mutation. (REQ-214)

## Rôles hérités

Les rôles peuvent hériter des capacités et de l'accès au domaine d'un rôle parent via `parent_role_id`. (REQ-215) La hiérarchie est aplatie au démarrage. (REQ-215)

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

L'héritage multi-niveaux est pris en charge. (REQ-215) Les capacités et domain_access explicites du rôle enfant sont fusionnés avec ceux du parent. (REQ-215)

## Déclencheurs planifiés

Déclencheurs basés sur cron qui appellent une URL de webhook selon un planning. (REQ-216) Utilise APScheduler. (REQ-216)

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

Les tâches planifiées sont gérées via l'UI d'administration (bascule activer/désactiver) ou la mutation admin `toggle_scheduled_task`. (REQ-216)

## Format OrderBy

OrderBy utilise le format `{column: direction}` avec un enum de direction à 6 valeurs : (REQ-200, REQ-201)

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
| ----------- | ----- |
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

Provisa exécute deux chemins d'export OTLP indépendants : votre collecteur interne et l'endpoint de support Provisa optionnel. (REQ-545) Chaque chemin a son propre filtre. Les filtres s'exécutent à l'intérieur d'un `_FilteringExporter` englobant avant que les spans ne quittent le processus — les objets span originaux ne sont jamais modifiés. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lignes 156–207]

**`telemetry_filter`** — contrôle ce qui atteint votre collecteur interne.

| Clé | Type | Par défaut | Description |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Remplace les littéraux chaîne et numériques dans `db.statement` par `?` |
| `redact_attributes` | list[str] | `[]` | Clés d'attribut supprimées entièrement de chaque span |

**`support_telemetry_filter`** — contrôle ce qui atteint l'endpoint de support Provisa. La rédaction des littéraux SQL vaut `true` par défaut sur ce chemin, car les données de requête vous appartiennent. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` ligne 240]

| Clé | Type | Par défaut | Description |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Remplace les littéraux chaîne et numériques dans `db.statement` par `?` |
| `redact_attributes` | list[str] | `[]` | Clés d'attribut supprimées entièrement de chaque span |

Exemple de `db.statement` rédigé — avec `redact_sql_literals: true`, cet attribut de span :

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

devient :

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint de support [tool-verified]

`support_endpoint` (ou l'env `PROVISA_SUPPORT_OTLP_ENDPOINT`) transmet la télémétrie au support Provisa à des fins de diagnostic. (REQ-548) Quand non défini, aucune donnée ne quitte votre infrastructure par ce chemin. (REQ-548) Le filtre de support s'applique indépendamment du filtre interne — vous pouvez rédiger les littéraux SQL des deux exports tout en partageant quand même le timing des spans et les données d'erreur avec le support. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lignes 238–288]

### Détection du protocole d'endpoint [tool-verified]

Provisa sélectionne OTLP/HTTP ou OTLP/gRPC depuis le schéma d'URL de l'endpoint. (REQ-549) Les URL commençant par `http://` ou `https://` utilisent OTLP/HTTP, avec `/v1/traces`, `/v1/metrics`, et `/v1/logs` ajoutés automatiquement. (REQ-549) Tout autre schéma utilise OTLP/gRPC avec `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lignes 60–70]

## Moteur de fédération

Configurer un moteur de fédération est optionnel. Le défaut est `duckdb` — zéro configuration, en processus, aucun service externe requis (REQ-989). Choisissez un autre moteur quand vous avez besoin d'échelle MPP ou souhaitez réutiliser un entrepôt existant.

Précédence : variable d'env `PROVISA_ENGINE` → champ de config `federation_engine` de l'UI admin persisté → `duckdb`. Les changements prennent effet au redémarrage du service. [tool-verified: `engine.py` `build_engine`]

### Vue d'ensemble des moteurs [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Clé de moteur | Libellé | Dialecte | MPP | Mécanisme de liaison externe | Auth |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Moteur de fédération Provisa | Trino SQL | Oui | Catalogues Trino (large ensemble de connecteurs) | Identifiants JDBC |
| `trino-byo` | Trino | Trino SQL | Oui | Identique à `trino` ; coordinateur non géré | Identifiants JDBC |
| `pg` | PostgreSQL | PostgreSQL | Non | FDW / pg_duckdb | Identifiants PostgreSQL |
| `duckdb` | DuckDB | DuckDB | Non | ATTACH natif d'extension | Aucune (en processus) |
| `clickhouse` | ClickHouse (embarqué) | ClickHouse | Oui | Moteurs de table S3 / IcebergS3 / DeltaLake | chdb (en processus, sans auth) |
| `clickhouse-server` | ClickHouse (Serveur / Cloud) | ClickHouse | Oui | Moteurs de table S3 / IcebergS3 / DeltaLake | Identifiants ClickHouse |
| `snowflake` | Snowflake | Snowflake | Oui | Stage externe + table externe | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Oui | Tables externes Unity Catalog via REST | `PROVISA_ENGINE_URL` (jeton bearer + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Oui | Tables externes BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Oui | Raccourcis OneLake → OPENROWSET | Azure AD (`az login` ou identité managée) |
| `synapse` | Azure Synapse | T-SQL | Oui | ADLS OPENROWSET / tables externes | Azure AD |
| `mysql` | MySQL | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `mariadb` | MariaDB | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `oracle` | Oracle Database | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `mssql` | Microsoft SQL Server | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `db2` | IBM Db2 | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `redshift` | Amazon Redshift | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `greenplum` | Greenplum | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `cockroachdb` | CockroachDB | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `yugabytedb` | YugabyteDB | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `opengauss` | openGauss | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `tidb` | TiDB | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `singlestore` | SingleStore | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `vertica` | Vertica | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `exasol` | Exasol | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `teradata` | Teradata Vantage | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `saphana` | SAP HANA | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `sapase` | SAP ASE (Sybase) | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `sqlanywhere` | SAP SQL Anywhere | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `monetdb` | MonetDB | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `firebird` | Firebird | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |
| `sqlalchemy` | Autre base de données relationnelle (par URL de connexion) | Par dialecte | Non | Aucun (atterrissage seul) | Identifiants par dialecte |

### Référence des moteurs

#### trino / trino-byo

`trino` est le coordinateur Provisa géré ; `trino-byo` se connecte à votre propre cluster Trino. Les deux utilisent Trino SQL et ont la portée de type de source la plus large.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Fédère via les extensions postgres_fdw (SQL/MED) et pg_duckdb. Nœud unique ; pas de MPP. Idéal quand vos données vivent déjà dans PostgreSQL et que vous voulez joindre quelques sources distantes.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL`.

#### duckdb

En processus ; aucun service externe. Le moteur par défaut (REQ-989). `PROVISA_DATA_DIR` contrôle où vit le magasin embarqué (`~/.provisa` par défaut).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Le magasin de matérialisation par défaut est `~/.provisa/materialize.duckdb` — le seul moteur avec un magasin par défaut non-PostgreSQL.

#### clickhouse (embarqué) / clickhouse-server

`clickhouse` utilise chdb (en processus). `clickhouse-server` se connecte à une instance ClickHouse externe ou ClickHouse Cloud. Les deux lisent Delta Lake, Iceberg, et Hudi directement via des moteurs de table ClickHouse natifs.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL`.

#### snowflake

Moteur-comme-entrepôt : Snowflake exécute les requêtes ; Provisa pousse les données source à travers des stages externes.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL`.

#### databricks

Les tables externes Unity Catalog relient les sources gérées par Provisa à Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL`.

#### bigquery

Tables externes BigQuery et BigLake. Le projet provient de l'URL ou de `GOOGLE_CLOUD_PROJECT` ; auth via clé de compte de service.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL`.

#### fabric / synapse

Les deux utilisent T-SQL sur TDS avec auth Azure AD (`az login` ou identité managée). Omettez `PROVISA_ENGINE_URL` pour lire les détails de connexion depuis des variables d'environnement à la place.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL`.

#### Moteurs de base de données relationnelle (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) et `sqlalchemy`

Une clé par base de données relationnelle adressable sur le réseau, toutes sur le même runtime d'atterrissage seul (pas de fédération vers des sources externes) : chaque source atterrit dans le magasin et y est interrogée. La clé sélectionne la base de données ; `PROVISA_ENGINE_URL` porte le DSN que son dialecte accepte. `sqlalchemy` est le fourre-tout pour une base de données sans clé propre. Les magasins embarqués en fichier (SQLite, Access) ne sont pas proposés — le serveur doit être accessible via le réseau.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Le magasin de matérialisation par défaut est `TENANT_DATABASE_URL`.

### Magasin de matérialisation

Quand une source ne peut pas s'attacher en direct (pas de connecteur ATTACH pour le moteur sélectionné), elle atterrit dans le magasin de matérialisation du moteur. Ordre de résolution : `PROVISA_MATERIALIZE_URL` explicite → défaut déclaré du moteur → erreur bloquante (pas de repli silencieux). [tool-verified: `engine.py` `materialize_store`]

DuckDB déclare son fichier embarqué (`~/.provisa/materialize.duckdb`) comme son défaut. Tous les autres moteurs utilisent par défaut `TENANT_DATABASE_URL` (PostgreSQL). Surchargez n'importe quel moteur avec `PROVISA_MATERIALIZE_URL`.

### Hints de fédération par source

Les paramètres de connexion étendus que les champs standard host/port/user/password ne peuvent pas porter vont dans `federation_hints` sur la source. Voir la référence de type de source ci-dessus pour les clés de hint par type. Un exemple consolidé :

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

Pour les sources Google Cloud, définissez `GOOGLE_APPLICATION_CREDENTIALS` sur le chemin de votre fichier de clé de compte de service. Pour Fabric et Synapse, authentifiez-vous avec `az login` (développeur) ou une identité managée (production) — le moteur lit les identifiants via `DefaultAzureCredential` d'`azure-identity`.

## Variables d'environnement

| Variable | Par défaut | Description |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Chemin du fichier de config |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI du magasin du plan de contrôle (SQLAlchemy async) ; accepte `sqlite+aiosqlite://…` / `duckdb://…` pour le magasin desktop embarqué (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI du registre de plateforme (répertoire de tenants, registre de moteurs) ; requis au démarrage, pas de repli (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` utilise fakeredis embarqué au lieu d'un serveur Redis — pas de Docker (REQ-829) |
| `PG_HOST` | `localhost` | Hôte PostgreSQL |
| `PG_PORT` | `5432` | Port PostgreSQL |
| `PG_DATABASE` | `provisa` | Base de données PostgreSQL |
| `PG_USER` | `provisa` | Utilisateur PostgreSQL |
| `PG_PASSWORD` | `provisa` | Mot de passe PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Clé de moteur de fédération (REQ-989, REQ-916) |
| `PROVISA_ENGINE_URL` | — | URL de connexion pour les moteurs pilotés par URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Surcharge le DSN du magasin de matérialisation (par défaut, le défaut déclaré du moteur) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Répertoire de données pour le magasin DuckDB embarqué (REQ-989) |
| `TRINO_HOST` | `localhost` | Hôte du coordinateur Trino |
| `TRINO_PORT` | `8080` | Port HTTP du coordinateur Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Chemin vers le JSON de clé de compte de service GCP (moteur/source BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Projet GCP par défaut (BigQuery ; surchargé par l'URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL de l'entrepôt Fabric (alternative à `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nom de base de données de l'entrepôt Fabric |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless Synapse |
| `SYNAPSE_DATABASE` | — | Nom de base de données Synapse |
| `REDIS_URL` | — | URL de connexion Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Limite d'échantillonnage par défaut |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Plafond de lignes quand une requête ne fournit pas de `LIMIT` explicite |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Budget de retry en lecture de niveau 1, en secondes ; backoff exponentiel avec jitter complet (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Port du proxy Flight SQL Zaychik |
| `FLIGHT_PORT` | `8815` | Port du serveur Arrow Flight de Provisa |
| `GRPC_PORT` | `50051` | Port du serveur gRPC Protobuf de Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Active la redirection à seuil côté serveur |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Seuil de nombre de lignes par défaut |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Format de redirection par défaut |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 pour les résultats redirigés |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL d'endpoint compatible S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Clé d'accès S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Clé secrète S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de l'URL présignée (secondes) |
| `PROVISA_MTLS_CLIENT_CA` | — | Bundle PEM de la ou des AC autorisées à signer les certificats client ; le définir active la vérification du certificat client sur pgwire, Bolt, gRPC et Flight (REQ-1228) |
| `PROVISA_MTLS_MODE` | `required` une fois une AC définie | `required` ou `optional` ; toute autre valeur refuse de démarrer (REQ-1228) |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Exige que le nom commun du certificat soit égal au nom d'utilisateur authentifié (REQ-1228) |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Sites séparés par des virgules autorisés à ouvrir un WebSocket Bolt depuis un navigateur ; non défini refuse toute origine navigateur (REQ-802) |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras pyproject intégrés dans l'image de l'app ; `scripts/provisa` le dérive de `dq_checker` dans `~/.provisa/config.yaml` (REQ-1443) |
| `PROVISA_DQ_CHECKER` | `none` | Installeur uniquement : `none`/`soda`/`gx`, lu par `first-launch.sh` en mode non interactif et écrit dans `config.yaml` comme `dq_checker` (REQ-1443) |
| `ANTHROPIC_API_KEY` | — | Clé API Claude (découverte) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Surcharge `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Surcharge `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Surcharge `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Surcharge `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Délai de flush du batch span processor |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Surcharge `observability.support_endpoint` |
