# Référence de configuration {: #configuration-reference }

Provisa se configure via un fichier YAML (par défaut : `config/provisa.yaml`).

## Inclusions {: #includes }

Répartissez une configuration sur plusieurs fichiers avec `includes:`. Le fichier incluant liste les chemins de fragments sous cette clé ; Provisa les fusionne avant validation, produisant le même résultat que si tout était écrit dans un seul fichier.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

Un fichier enveloppe ne contenant que `includes:` est valide. `load_control_plane` lit à travers les inclusions, donc la section `control_plane:` est prise du fichier inclus qui la définit.

**Règles de fusion**

- Les chemins se résolvent relativement au fichier incluant. Les chemins absolus sont utilisés tels quels.
- Les sections de liste (`sources`, `tables`, `domains`, `relationships`, `roles`, …) s'ajoutent — les entrées du fragment suivent celles du fichier incluant.
- Une clé scalaire ou de mappage que le fichier incluant ne définit pas est prise du fragment.
- Une clé définie par les deux fichiers avec des valeurs différentes est un conflit ; le chargement échoue en nommant la clé.
- Des valeurs identiques dans les deux fichiers ne constituent pas un conflit.
- Les inclusions s'imbriquent. Un fichier qui s'inclut lui-même — directement ou via un autre fragment — est refusé.
- `includes` est consommé au chargement et n'apparaît jamais dans la configuration validée.

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

Toutes les sources partagent un jeu de champs commun.

| Champ | Défaut | Notes |
| ------- | --------- | ------- |
| `id` | requis | Alphanumérique, tirets, tirets bas |
| `type` | requis | Voir le tableau ci-dessous |
| `host` | `""` | Nom d'hôte ou IP |
| `port` | `0` | `0` signifie que chaque connecteur fournit son propre défaut — il n'existe pas de table centrale des ports par défaut |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Accepte les références de credentials `${env:VAR}` et `${secret:NAME}` — voir [Secrets](secrets.md) |
| `path` | `null` | Chemin de fichier ou URI pour les sources fichier |
| `base_url` | `null` | URL de base pour les sources API |
| `pool_min` / `pool_max` | `1` / `5` | Bornes du pool de connexions |
| `cache_enabled` | `true` | Active ou coupe le cache pour toutes les tables de cette source |
| `cache_ttl` | `null` | Secondes ; `null` hérite du défaut global ; `-1` n'expire jamais |
| `federation_hints` | `{}` | Paramètres étendus propres au connecteur (dict[str,str]) ; voir la référence par type ci-dessous. |
| `mapping` | `{}` | DSL de mapping pour redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | Restreint cette source à des identifiants de domaine précis ; vide = sans restriction |
| `description` | `""` | |

### Types de source pris en charge {: #supported-source-types }

| Type | Style de connexion | Notes |
| ------ | ----------------- | ------- |
| **SGBDR** | | |
| `postgresql` | host/port | Pool asyncpg ; PgBouncer optionnel via `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (fichier de base) | Extension communautaire firebird de DuckDB |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Réutilise le pilote et le dialecte PostgreSQL |
| `yugabytedb` | host/port | Réutilise le pilote et le dialecte PostgreSQL |
| `greenplum` | host/port | Réutilise le pilote et le dialecte PostgreSQL |
| `tidb` | host/port | Réutilise le pilote et le dialecte MySQL |
| **Entrepôt cloud** | | |
| `snowflake` | host/port + `federation_hints` | `account` requis dans les hints |
| `bigquery` | `federation_hints` | `project` requis ; authentification via `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` requis dans les hints |
| `fabric` | variables d'environnement ou `PROVISA_ENGINE_URL` | T-SQL sur TDS, authentification Azure AD |
| `synapse` | variables d'environnement ou `PROVISA_ENGINE_URL` | T-SQL sur TDS, authentification Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | Le hint `secure` active TLS ; ports par défaut 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | Endpoint REST du contrôleur |
| `druid` | host/port | Endpoint Avatica du broker |
| `exasol` | host/port | |
| **Data lake** | | |
| `delta_lake` | `path` (URI de table) | `delta_scan` de DuckDB ; accès au stockage objet via `federation_hints` |
| `iceberg` | `path` (URI de table) | `iceberg_scan` de DuckDB ; accès au stockage objet via `federation_hints` |
| `hudi` | `path` (URI de table) | Moteur Hudi de ClickHouse, sans copie |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de stockage dans `mapping["storage"]` : hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + clés S3 dans `mapping` | Type distinct ; stockage toujours S3 |
| **NoSQL** | | |
| `mongodb` | host/port | Champs de connexion simples ; pas de DSL de mapping |
| `cassandra` | host/port | Champs de connexion simples ; pas de DSL de mapping |
| `redis` | host/port + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | enregistrement seul | La vraie configuration vit dans `kafka_sources[]` ; voir §Kafka ci-dessous |
| `websocket` | host/port/path + `federation_hints` | Flux WebSocket externe |
| `rss` | host/port/path + `federation_hints` | Flux RSS 2.0 / Atom |
| **Graphe/Sémantique** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **Fichier** | | |
| `sqlite` | `path` | Passe toujours par le moteur (pas de pool direct) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (répertoire) | Explorateur par glob ; expose CSV/Parquet/XLSX/JSON comme tables |
| **API/Distant** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port ou `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + `mapping` optionnel | En-têtes, forward-client-headers, délai d'expiration dans `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (emplacement Flight) | Extension airport de DuckDB |
| `ingest` | récepteur push | Des services externes envoient des événements JSON en POST |
| **SaaS** | | |
| `sharepoint` | `base_url` ou `host` + `mapping` | Authentification via `mapping.auth_type` |
| `splunk` | `host`/`port` ou `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | sujet + `domain_id` | Modèle `GovDataSource` distinct ; voir §GovData ci-dessous |
| **Qualité des données** | | |
| `soda` | host/port pointant vers le pgwire de Provisa | Nécessite l'extra `soda` ; Elastic License 2.0, auto-hébergement uniquement |
| `great_expectations` | host/port pointant vers le pgwire de Provisa | Nécessite l'extra `gx` ; Apache 2.0 |

### Référence des types de source {: #source-type-reference }

Chaque type qui demande une configuration peu évidente a une courte entrée ci-dessous. Les types SGBDR (postgresql, mysql, etc.) n'utilisent que les champs communs ci-dessus et n'ont pas besoin de section propre.

#### GovData

Les sources `govdata` utilisent un modèle de premier niveau à elles, `GovDataSource`, à la place du `Source` générique. L'accès est réparti par regroupement de sujets.

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

Chaque sujet correspond à un ou plusieurs schémas GovData. Configurer une source `govdata` avec un sujet expose automatiquement tous les schémas de ce sujet.

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

Les schémas `ref` et `geo` sont toujours inclus comme schémas de liaison. Ils ne sont pas configurables et ne figurent pas dans la liste ci-dessus. Utilisez le sujet `ALL` pour accorder l'accès à tous les schémas.

#### Kafka

La ligne `kafka` dans `sources:` ne fait qu'enregistrer la source. Le `details()` de son connecteur renvoie `{}` ; la vraie configuration se trouve dans le bloc de premier niveau `kafka_sources[]`. Kafka est toujours un VIRTUAL_SOURCE : il passe par le moteur et n'a pas de pool direct.

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

**Fenêtre temporelle** — `default_window` borne chaque requête à une période récente, ce qui empêche les lectures non bornées sur les topics à fort volume. Format : `1h`, `30m`, `7d`, `60s`. Vaut `1h` par défaut. Injectée automatiquement sous la forme `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Les clients peuvent la remplacer par leur propre filtre `_timestamp` dans l'argument GraphQL `where`.

**Discriminateur** — Plusieurs configurations de topic peuvent pointer vers le même topic Kafka physique avec des valeurs `discriminator` différentes, produisant des types GraphQL distincts. Le discriminateur est injecté automatiquement comme clause WHERE.

**Source du schéma**

| Valeur | Comportement |
| ------- | ---------- |
| `registry` | Récupère le schéma depuis le Confluent Schema Registry |
| `manual` | Définit les colonnes en ligne dans la configuration (aucun Schema Registry nécessaire) |
| `sample` | Découverte automatique à partir de messages d'exemple |

#### Snowflake

`account` dans `federation_hints` est requis. `warehouse`, `role` et `schema` sont optionnels.

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

#### Databricks

`http_path` dans `federation_hints` est obligatoire. `password` porte le jeton d'accès personnel. `catalog` est facultatif et se place dans le SQL ou les hints ; le champ `database` ne le contient pas.

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

#### BigQuery

`project` dans `federation_hints` est requis. L'authentification utilise `GOOGLE_APPLICATION_CREDENTIALS` (chemin vers un fichier de clé de compte de service) ou les Application Default Credentials de l'environnement du moteur.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

Les deux utilisent T-SQL sur TDS avec authentification Azure AD. Connectez-vous avec `az login` (développement) ou une identité managée (production) ; le moteur lit les identifiants via `DefaultAzureCredential` d'`azure-identity`. Les informations de connexion viennent de variables d'environnement, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) ou `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), ou de `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` dans `federation_hints` active TLS sur l'interface HTTP. Le port vaut par défaut `8123` (en clair) ou `8443` (avec `secure: "true"`). `schema` dans `federation_hints` remplace le schéma distant.

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

#### Delta Lake / Iceberg

`path` est l'URI de la table (S3, GCS, ADLS ou local). L'accès au stockage objet exige des credentials dans `federation_hints`. Pour Cloudflare R2, ajoutez `account_id`.

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

#### Hive / Hive S3

`host` et `port` pointent vers le metastore Thrift de Hive (port par défaut 9083). Pour `hive`, définissez `mapping["storage"]` pour choisir le backend de stockage objet. Une clé obligatoire manquante échoue bruyamment, sans valeur de repli.

`hive_s3` est un type distinct qui déclare toujours un stockage S3 ; aucun `mapping.storage` n'est nécessaire.

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

Valeurs acceptées pour `mapping.storage` : `hadoop` (défaut), `hdfs`, `local`, `s3`, `azure`, `adls`. Clés de mapping S3 : `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Clés de mapping ADLS : `storage_account`, `access_key` ou `sas_token`.

#### Redis

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

#### Elasticsearch

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

#### Prometheus

`mapping.url` prend le pas sur `host:port` quand les deux sont présents.

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

#### Google Sheets

`spreadsheet_id` dans `federation_hints` est requis. L'authentification utilise un SECRET DuckDB `gsheet` provisionné au moment de l'attachement.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Sources fichier (csv / parquet / sqlite / files) {: #file-sources-csv-parquet-sqlite-files }

`path` est requis. `files` parcourt un répertoire à la recherche de fichiers CSV, Parquet, XLSX et JSON, et expose chacun comme une table. Toutes les sources fichier sont VIRTUAL (passent par le moteur ; pas de pool direct).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

Les **jeux de données Kaggle** ne peuvent pas être ajoutés par ce fichier, car ils demandent un jeton valide et le sélecteur de jeux de données du formulaire des sources (Sources → Abonnements → Kaggle). Une source Kaggle exportée en YAML apparaît comme `type: files` avec `kaggle_owner` et `kaggle_ref` dans `federation_hints`. Retélécharger depuis Kaggle passe par la mutation `refreshKaggleSource` ou le rafraîchissement dans l'interface ; modifier le YAML ne le fait pas. Voir [Jeux de données Kaggle](sources.md#kaggle-datasets) dans la référence des types de source.

#### Sources API / distantes {: #api-remote-sources }

**openapi** — définissez `base_url` sur l'URL de base OpenAPI. La découverte de schéma lit la spécification OpenAPI au démarrage.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — définissez `base_url`. Clés `mapping` optionnelles : `headers` (dictionnaire d'en-têtes statiques), `forward_client_headers` (booléen), `timeout_seconds` (entier).

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

**airport** — `base_url` est l'emplacement du serveur Arrow Flight. Extension airport de DuckDB.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — utilisez `host`, `port`, `path` et `federation_hints`.

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

**sharepoint**

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

**splunk**

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

#### Vérificateurs de qualité des données (soda / great_expectations) {: #data-quality-checkers-soda-great_expectations }

Une source de vérification pointe vers l'endpoint pgwire de Provisa lui-même ; un seul pilote postgres scanne donc la vue fédérée d'une table adossée à Snowflake ou à Iceberg. Vous déclarez l'identité du scan ; elle n'est jamais héritée. La politique s'applique à cette connexion, et un ensemble de lignes filtré ne doit pas faire passer un contrôle en silence. Les clés de connexion viennent de `mapping` : `host`, `port`, `database`, `user`, `password`.

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

Chaque table de résultats porte `dq_contract`, le YAML du contrat Soda ou le JSON de la suite Great Expectations mot pour mot. Les colonnes, le filigrane et les promotions en sont dérivés ; voir comment dans [Vérificateurs de qualité des données](sources.md#data-quality-checkers).

**Choix à l'installation.** Le vérificateur n'est pas lié : le scan tourne dans un interpréteur enfant, et la bibliothèque n'est installée que lorsqu'un exploitant la nomme. Chaque chemin d'installation (`install.sh`, `packaging/linux/first-launch.sh` et l'assistant macOS via `PROVISA_DQ_CHECKER`) écrit le choix dans `~/.provisa/config.yaml` :

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` lit cette clé et exporte `PROVISA_EXTRAS`, que `docker-compose.app.yml` transmet comme argument de build à l'`ARG PROVISA_EXTRAS` du `Dockerfile` :

| `dq_checker` | `PROVISA_EXTRAS` (couche Docker) | Installation venv native |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Installer le jeu de données de démonstration fait passer `none` à `gx` et le dit, parce que la configuration de démonstration enregistre une suite Great Expectations sur `pet_store.pets` et que sa fiche de qualité n'aurait sinon rien à montrer. Nommer `soda` conserve `soda`.

Si vous arrivez à la démo par pip plutôt que par un installateur, vous sautez cette étape de l'assistant ; l'extra `demo` embarque donc le même vérificateur : `provisa run --demo` a besoin de `pip install 'provisa[embedded,demo]'` pour que son scan s'exécute. Sans lui, le scan signale `data-quality checker 'great_expectations' is not installed` et donne la commande d'installation.

Toute autre valeur arrête le lanceur, qui ne démarre donc jamais sans le vérificateur demandé par l'exploitant. L'extra `soda` installe `soda-postgres`, et `gx` installe `great-expectations[postgresql]`. Soda Core relève de l'Elastic License 2.0 : `config/capabilities.yaml` marque l'option `cloud_eligible: false`, et le plan hébergé la refuse.

## Domaines {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Nommage {: #naming }

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Convention de nommage {: #naming-convention }

L'autorité de nommage est la source unique de vérité pour les noms exposés aux clients ; les noms physiques des colonnes du backend ne sont jamais exposés aux clients. Chaque langage de requête dérive le nom d'une colonne de son `column.alias` s'il est défini, sinon du nom physique de la colonne via la convention configurée.

La convention GraphQL est l'une de trois énumérations prédéfinies. Les anciennes chaînes libres (`none`, `snake_case`, `camelCase`, `PascalCase`) sont dépréciées.

| Préréglage | Défaut | Noms de type | Noms de champ | Noms de mutation |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | oui | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

La convention GraphQL par défaut est `apollo_graphql`, qui produit des noms de champ et de mutation en camelCase. La convention SQL est distincte, avec `snake_case` par défaut, appliquée via `apply_sql_name()` ; la convention GraphQL est appliquée via `apply_gql_name()`, et le nom CQL est dérivé du nom GraphQL.

`domain_prefix: bool` est une option orthogonale qui s'applique quel que soit le préréglage choisi.

Un `column.alias` explicite est le nom canonique : SQL l'utilise tel quel sans appliquer de convention, GraphQL lui applique sa convention, et CQL dérive du nom GraphQL.

Surcharge par source :

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Surcharge par table :

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Préfixe de domaine {: #domain-prefix }

Avec `domain_prefix: true`, tous les noms de champ et de type GraphQL sont préfixés par l'identifiant de domaine, séparé par un double tiret bas :

| Table | Domaine | Nom de champ |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Cela évite les collisions de noms quand des domaines différents ont des tables portant le même nom, et rend les requêtes auto-documentées.

### Règles de nommage {: #naming-rules }

Règles regex appliquées aux noms de table lors de la génération des noms de champ GraphQL. Appliquées dans l'ordre, avant la résolution d'unicité.

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
      - name: email
        visible_to: [admin, analyst]
        masks:                    # first entry the reader holds applies
          - to: [admin]           # admin sees raw
          - to: ["*"]             # everyone else sees it masked
            mask: {type: regex, pattern: "^(.{2}).*(@.*)$", replace: "$1***$2"}
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: constant, value: 0}
      - name: created_by
        visible_to: [admin, analyst]
      - name: created_at
        visible_to: [admin, analyst]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: truncate, precision: month}
    write_grants:                 # who may write this table, per role and operation
      - role: admin
        operation: insert
        columns: [email, amount]
        check: "TRUE"             # any row may be inserted
        presets:                  # set by the grant; the statement may not supply them
          - {column: created_by, source: session, name: user_id}
          - {column: created_at, source: now}
      - role: admin
        operation: update
        columns: [email, amount]
        rows: "TRUE"              # any row may be updated
        check: "TRUE"
```

Une table n'a aucune autorisation d'écriture par défaut, donc aucun rôle ne peut y écrire. Une autorisation donne à un rôle une opération (`insert`, `update` ou `delete`) sur la table. Une table contient au plus une autorisation par rôle et par opération.

| Champ de l'autorisation | Signification |
| ----------- | ------- |
| `columns` | Les colonnes qu'un `INSERT` peut lister ou qu'un `UPDATE` peut définir. Une autorisation `delete` n'en nomme aucune. |
| `rows` | Les lignes qu'un `UPDATE` ou un `DELETE` peut toucher : un prédicat SQL sur les colonnes de la table. Une autorisation `insert` n'en a pas. |
| `check` | Ce que doit satisfaire chaque ligne qu'écrit un `INSERT` ou que laisse un `UPDATE` : un prédicat SQL. Une autorisation `delete` n'en a pas. |
| `presets` | Colonnes que l'autorisation définit elle-même. L'instruction ne peut pas les fournir, donc une colonne à preset ne figure pas aussi dans `columns`. |

Si vous omettez `rows` ou `check`, l'autorisation prend le filtre de lignes en lecture du rôle sur la table : un rôle écrit les lignes qu'il lit, sauf indication contraire de l'autorisation. `"TRUE"` signifie toutes les lignes. Les anciennes clés `writable_by` de colonne et `column_presets` de table sont refusées par leur nom ; la configuration ne se charge pas tant qu'elles ne sont pas remplacées par `write_grants`.

Toute écriture sur toute surface est admise dans cet ordre : le rôle détient le droit `write` ; le rôle détient une autorisation pour l'opération sur la table ; chaque colonne qu'écrit l'instruction figure dans l'autorisation ; les presets de l'autorisation sont ajoutés à l'instruction ; le prédicat `rows` est combiné par AND dans le `WHERE` d'un `UPDATE` ou d'un `DELETE` ; et `check` est évalué sur les nouvelles lignes avant que quoi que ce soit soit envoyé à la source. Une instruction que `check` ne peut pas trancher est refusée. Les colonnes de l'autorisation décident aussi des champs de mutation que GraphQL propose à un rôle : un champ par opération pour laquelle il détient une autorisation, avec exactement ces colonnes.

Certaines instructions demandent une autorisation plus large. `TRUNCATE` demande une autorisation `delete` sur toutes les lignes. `MERGE` demande une autorisation `insert` et une autorisation `update`, toutes deux sur toutes les lignes. `INSERT ... ON CONFLICT DO UPDATE` demande une autorisation `update` sur toutes les lignes, sans presets. `COPY ... FROM STDIN` demande une autorisation `insert` sans presets.

Les autorisations sont héritées. Un rôle enfant prend, par table et par opération, l'autorisation du rôle le plus proche de sa chaîne qui en détient une, la sienne d'abord. Un méta-rôle détient le repli des autorisations de ses membres : l'union de leurs colonnes, le OR de leurs prédicats `rows` et `check`, et seulement les presets que tous les membres définissent de la même façon.

Dans l'interface d'administration, la section **Écritures** d'une table liste les autorisations et les modifie ensemble.

### Alias {: #aliases }

Les alias de table et de colonne remplacent le nom GraphQL par défaut. Utiles pour :

- Renommer des noms de base cryptiques (par exemple `tbl_cust_seg` → `customer_segments`)
- Éviter les abréviations dans la couche API
- Construire un vocabulaire propre, spécifique au domaine

### Descriptions

Les descriptions de table et de colonne sont incluses dans le SDL GraphQL généré. Elles apparaissent dans l'explorateur de documentation de GraphiQL et dans les requêtes d'introspection. Définissez-les dans le YAML de configuration ou via l'interface d'administration.

### Path (extraction JSON calculée) {: #path-computed-json-extraction }

Les colonnes peuvent extraire des valeurs d'une colonne source JSON/JSONB au moyen d'un `path` en notation pointée. C'est utile pour les données semi-structurées des messages Kafka, des documents MongoDB ou des colonnes JSONB PostgreSQL.

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

Le format du chemin est `source_column.key1.key2...`. Le compilateur génère `json_extract_scalar(source_column, '$.key1.key2')` dans le SQL.

**Effet sur le routage :** les colonnes `path` utilisent les opérateurs JSON de PostgreSQL (`->>`), nativement pris en charge par le routage PG direct. Pour les sources non PostgreSQL (MySQL, SQL Server, etc.), les requêtes comportant des colonnes `path` sont automatiquement routées par le moteur de fédération. Les mutations ne sont pas concernées, les colonnes `path` étant des champs calculés en lecture seule.

### Types de masquage {: #masking-types }

| Type | Champs | Description |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (colonnes chaîne uniquement) |
| `constant` | `value` | Remplacement littéral (NULL, 0, MAX, MIN, valeur personnalisée) |
| `truncate` | `precision` | DATE_TRUNC (colonnes date/timestamp uniquement) |

## Relations {: #relationships }

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

### Matérialisation automatique {: #auto-materialization }

Définissez `materialize: true` sur une relation pour générer automatiquement une vue matérialisée destinée aux JOIN inter-sources. Cela évite des requêtes fédérées coûteuses en pré-calculant le résultat du JOIN.

- Seules les relations inter-sources génèrent des vues matérialisées (les JOIN au sein d'une même source sont déjà rapides)
- Sur une relation adossée à une jonction, la vue matérialisée couvre le parcours à deux sauts — saut source, saut de jonction, discriminant et les colonnes propres de la jonction comme attributs d'arête. La jonction compte comme une patte : une arête est donc inter-sources dès que l'une des trois tables se trouve dans une autre source
- La vue matérialisée démarre périmée et est remplie par la boucle de rafraîchissement en arrière-plan
- Les mutations sur l'une ou l'autre table source marquent la vue matérialisée comme périmée en vue d'un nouveau rafraîchissement
- `refresh_interval` vaut 300 secondes (5 minutes) par défaut

## Rôles {: #roles }

```yaml
roles:
  - id: admin
    description: Administers sources, tables, relationships and security.
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
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Les rôles dotés d'un `parent_role_id` héritent des capacités, de l'accès aux domaines, des octrois de colonnes et d'objets, ainsi que des règles RLS du parent, la règle RLS propre de l'enfant pour une table étant prioritaire. La chaîne est aplatie au démarrage.

### Capacités {: #capabilities }

| Capacité | Description |
| ----------- | ------------- |
| `source_registration` | Enregistrer des sources de données |
| `table_registration` | Enregistrer des tables |
| `relationship_registration` | Définir des relations |
| `security_config` | Configurer le RLS et le masquage |
| `query_development` | Exécuter des requêtes |
| `full_results` | Contourner les limites d'échantillonnage |
| `admin` | Toutes les capacités |

## Règles RLS {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## Vues matérialisées {: #materialized-views }

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
      # Add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Vues (jeux de données calculés et gouvernés) {: #views-governed-computed-datasets }

Les vues sont des jeux de données calculés, définis en SQL, avec une gouvernance complète au niveau des colonnes. Elles constituent le mécanisme gouverné pour ajouter agrégations, transformations et métriques dérivées au modèle.

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
| `id` | Oui | Identifiant unique de la vue |
| `sql` | Oui | Instruction SQL SELECT définissant la vue |
| `domain_id` | Oui | Domaine pour la visibilité du schéma |
| `materialize` | Non | `true` = rafraîchissement CTAS périodique, `false` = vue fédérée en direct |
| `refresh_interval` | Non | Secondes entre deux rafraîchissements (matérialisées uniquement, 300 par défaut) |
| `description` | Non | Apparaît dans le SDL GraphQL |
| `alias` | Non | Remplace le nom GraphQL |
| `columns` | Oui | Définitions de colonnes avec visibilité, masquage et descriptions |

### Matérialisée ou en direct {: #materialized-vs-live }

- **`materialize: true`** : Provisa crée une table par CTAS et la rafraîchit selon une planification. Requêtes plus rapides, mais les données peuvent être périmées de `refresh_interval` secondes au plus.
- **`materialize: false`** : Provisa crée une vue fédérée. Les requêtes renvoient toujours des données à jour, mais peuvent être plus lentes pour des agrégations complexes.

Les vues passent par le même pipeline de gouvernance que les tables : RLS, masquage, échantillonnage et visibilité par rôle. Ainsi, rien ne peut ajouter de sémantique nouvelle à la plateforme sans la supervision d'un intendant.

### Vues avec historique (voyage dans le temps) {: #views-with-history-time-travel }

Une vue matérialisée peut conserver chaque version de ses lignes et être lue à un instant passé. Elle déclare comment elle les conserve et à quelle clé appartient une version :

```yaml
views:
  - id: customer-history
    sql: SELECT customer_id, tier, updated_at FROM crm.customers
    domain_id: sales
    materialize: true
    bitemporal_mode: delta            # snapshot | delta
    bitemporal_key: [customer_id]     # required
    bitemporal_source_time: updated_at  # optional: the source's change time
    columns:
      - {name: customer_id, visible_to: [analyst]}
      - {name: tier, visible_to: [analyst]}
      - {name: updated_at, visible_to: [analyst]}
```
| Champ | Obligatoire | Description |
| ------- | ---------- | ------------- |
| `bitemporal_mode` | Oui | `snapshot` ajoute le résultat complet à chaque rafraîchissement ; `delta` n'ajoute que ce qui a changé |
| `bitemporal_key` | Oui | Les colonnes auxquelles appartient une version |
| `bitemporal_source_time` | Non | La colonne qui indique quand chaque ligne a changé dans la source : l'horloge sur laquelle lit le voyage dans le temps |

**Choisir l'horloge.** Avec `bitemporal_source_time`, les versions sont ordonnées et lues selon l'heure de la source : « au mardi dernier » veut dire mardi dernier dans la source. Sans lui, elles sont ordonnées et lues selon le moment où Provisa a déposé chaque changement, ce qui suit la cadence de rafraîchissement et non l'instant où la ligne a changé dans la source. Les deux choix sont valables. L'heure de chargement convient à une cadence de rafraîchissement fréquente, à des données dont l'heure dans la source importe peu, et aux lectures censées porter sur l'heure de chargement, comme vérifier ce que Provisa détenait et quand.

Une version est en vigueur de son instant jusqu'à celui de la version suivante. Une suppression n'a pas d'heure de changement dans la source et prend effet au moment où Provisa l'a enregistrée.

**Lecture.** Une lecture simple renvoie la version de chaque clé en vigueur maintenant. L'en-tête `X-Provisa-As-Of` lit à un instant, ou sur une plage donnée sous forme d'intervalle ISO 8601 `start/end` :

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```
Un instant renvoie la version de chaque clé en vigueur à ce moment. Une plage renvoie chaque version en vigueur à un moment quelconque de cette plage, une ligne par version.

**Tel qu'enregistré.** Une correction enregistrée plus tard change ce qu'une vue dit du passé : la version en vigueur le 30 septembre est la version corrigée une fois la correction enregistrée. Pour lire la vue telle que Provisa l'avait enregistrée à un instant, avant toute correction ultérieure, ajoutez `X-Provisa-As-Recorded`, un instant ISO 8601 :

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

Toute version enregistrée après ce moment est laissée de côté, et l'as-of choisit alors la version en vigueur à son propre moment. Avec `X-Provisa-As-Recorded` seul, la lecture renvoie la version de chaque clé en vigueur telle qu'enregistrée alors. Il prend un instant, pas une plage ; une plage ou une valeur mal formée est refusée avec HTTP 400 (`data.invalid_as_recorded`).

Les deux instants parviennent aussi à Provisa sans les en-têtes. L'instant as-of est l'argument `as_of` de `resolve` (GraphQL, gRPC, MCP), le paramètre de requête REST `as_of` et l'option `as_of` d'un ticket Arrow Flight, et une plage passée à `resolve` renvoie les `versions` de la ligne à la place de sa `row`. L'instant as-recorded est l'argument `as_recorded` de `resolve`, le paramètre de requête REST `as_recorded` et l'option `as_recorded` d'un ticket Arrow Flight. L'enregistrement d'audit de chaque instruction garde les deux instants avec lesquels elle a lu, `as_of` et `as_recorded`. Droits, masques et règles de ligne s'appliquent comme pour toute lecture. Une valeur as-of mal formée, ou une plage dont la fin n'est pas après le début, est refusée avec HTTP 400 (`data.invalid_as_of`).

Toute vue avec historique renvoie trois colonnes supplémentaires, accordées comme l'est sa colonne d'heure source (sinon sa première colonne de clé) :

| Colonne | Signification |
| ------- | ------- |
| `source_from` | Quand cette version a pris effet : son heure source, ou son heure de chargement à défaut |
| `source_to` | Quand la version suivante a pris effet ; vide tant que celle-ci est en vigueur |
| `recorded_at` | Quand Provisa a enregistré cette version |

**Heure source issue de la capture de données de changement.** Une table atterrie par capture de données de changement porte son heure de validation en `source_committed_at`, pour qu'une vue avec historique la désigne comme heure source :

- Debezium : l'heure de validation de la base de données que porte chaque événement de changement (`source.ts_ms`). Une ligne copiée par une construction complète de la réplique n'a pas d'événement de changement, donc son `source_committed_at` est vide jusqu'au prochain changement de la ligne, sauf si la source est PostgreSQL avec les horodatages de validation activés (point suivant).
- PostgreSQL avec le signal de changement `native` : l'heure de validation que PostgreSQL conserve pour chaque ligne (`pg_xact_commit_timestamp(xmin)`), lue par le pilote propre de la source lors de la construction de la réplique. PostgreSQL ne la conserve qu'avec `track_commit_timestamp = on`. Activez-le pour obtenir l'heure source avec le signal natif ; sans lui, `source_committed_at` est vide. Activer ou désactiver le paramètre reconstruit la réplique.

Les topics Kafka sans enveloppe Debezium ne portent pas d'heure de validation.

### Vues en lecture seule {: #query-only-views }

Les vues, qu'elles soient `materialize: true` ou `materialize: false`, exposent leur type GraphQL en lecture seule. Aucune mutation d'insertion, d'upsert, de mise à jour ou de suppression n'est générée pour les relations adossées à `view_sql`.

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Hiérarchie du cache {: #cache-hierarchy }

Ordre de résolution du TTL (le plus spécifique l'emporte) : **table** > **source** > **défaut global**. La première valeur non nulle est retenue.

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

Définir `cache_enabled: false` sur une source désactive le cache pour toutes les tables de cette source, quel que soit le TTL au niveau table. Les clés de cache incluent toujours `role_id` et les valeurs du contexte RLS, à des fins de partitionnement de sécurité.

### Activation par requête {: #per-request-opt-in }

Le cache de réponses est désactivé pour toute requête qui ne le demande pas. Une requête l'active avec :

| Surface | Activation |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` sur l'opération, éventuellement `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | une ligne de commentaire `-- @provisa cache=true` ou `-- @provisa cache_ttl=60` dans l'instruction |
| Cypher (`/data/cypher`, la Neo4j Query API, Bolt, Arrow Flight) | une ligne de commentaire `// @provisa cache=true` ou `// @provisa cache_ttl=60` dans la requête |
| gRPC (et le proxy gRPC HTTP) | métadonnées d'appel (en-têtes sur le proxy) `x-provisa-cache: true` ou `x-provisa-cache-ttl: 60` |
| REST, JSON:API et, en alternative à l'indication dans le texte : GraphQL sur HTTP, `/data/cypher`, la Neo4j Query API | l'en-tête de requête `Cache-Control: max-age=60` (ou `max-stale=60`), ou `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

Un `ttl` qui n'est pas un entier fait échouer la requête. Une requête qui indique l'activation deux fois (dans son texte et dans un en-tête, ou dans deux en-têtes) avec des valeurs différentes échoue avec `400` ; aucune n'est préférée. `Cache-Control: no-cache` ou `no-store` signifie pas de cache, comme sans en-tête.

Sans l'indication, la requête ne lit ni n'écrit le cache. Avec elle, les paramètres ci-dessus décident toujours : le résultat n'est mis en cache que si chaque source lue a `cache_enabled: true` et que le TTL résolu de chaque table est supérieur à 0. L'entrée vit le `ttl` de la requête si elle en donne un, sinon le plus court TTL résolu des tables lues.

L'indication accepte seulement des données plus anciennes en échange de rapidité. Elle ne rend jamais une lecture plus récente que ne le permettent les paramètres de l'opérateur : la fraîcheur de l'atterrissage et du réplica (`cache_ttl`, `change_signal`), `row_materialize`, les instantanés `load_protected` et les contrôles de fraîcheur s'appliquent à une requête avec indication exactement comme à une requête sans. Voir [Fraîcheur et charge : qui décide](#freshness-and-load-who-decides).

`@noCache` et `-- @provisa no_cache=true` n'existent plus : le cache étant désactivé par défaut, il n'y a rien à contourner.

Une écriture invalide chaque entrée de cache que l'organisation qui écrit détient pour les tables écrites ; les entrées d'une autre organisation pour la même table ne sont pas touchées. Si l'invalidation échoue, la requête échoue au lieu de laisser des entrées périmées.

## Fraîcheur et charge : qui décide {: #freshness-and-load-who-decides }

Trois parties déterminent la fraîcheur d'un résultat et la charge qu'une requête impose aux systèmes en aval.

- **La source amont.** Idéalement, elle gère sa propre contre-pression : limites de connexions, délais d'exécution, réplicas de lecture.
- **L'opérateur.** Protège la plateforme, Provisa elle-même, de la contre-pression. Quand une source ne peut pas se protéger, l'opérateur la protège aussi. Les réglages correspondants se trouvent sur les sources et les tables : `load_protected`, `replicate`, les `federation_hints` de la source, le seuil de redirection des grands résultats, le cluster Kafka où écrivent les sinks et le watermark enregistré de chaque table.
- **L'utilisateur final.** Arbitre entre vitesse et fraîcheur, requête par requête.

Les réglages de l'opérateur sont le plancher. Une requête peut aller au-dessus, vers des données plus anciennes ou moins de charge ; l'activation du cache de réponses en est l'exemple courant. Elle ne peut jamais descendre en dessous. Un indice de requête qui le ferait est refusé avec une erreur nommant le réglage de l'opérateur, jamais appliqué ni ignoré en silence.

| Entrée de la requête | Ce qu'elle peut faire | Refusée quand |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | Choisir le pilote direct pour une source | une table que lit l'instruction est `load_protected` ou `replicate: 0` |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | Fixer une propriété de session du moteur laissée libre par les `federation_hints` de la source | elle modifie une propriété fixée par l'opérateur |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | Rediriger un résultat plus tôt | elle dépasse le seuil de l'opérateur |
| `@sink(broker:)`, broker de `X-Provisa-Sink` | Reprendre le broker de l'opérateur | elle nomme un autre broker, ou aucun `KAFKA_BOOTSTRAP_SERVERS` n'est configuré |
| `@watermark` sur un abonnement | Reprendre le watermark enregistré de la table | elle nomme une autre colonne, ou la table n'en a pas |
| `@cached`, `-- @provisa cache=true` | Servir un résultat plus ancien depuis le cache | jamais ; une entrée de cache n'est jamais plus fraîche que la lecture qui l'a stockée |

Les refus renvoient HTTP 403 avec le code `query.operator_floor`, SQLSTATE `42501` via pgwire et `PERMISSION_DENIED` via Flight et gRPC.

Une table `load_protected` ou `replicate: 0` n'est jamais lue en direct par une requête, quel que soit le transport. Les requêtes lisent sa réplique. Seuls la réplication et ses rafraîchissements lisent la source : un rafraîchissement a lieu quand `cache_ttl` ou un contrôle de fraîcheur l'exige ; sans l'un ni l'autre, la réplique est construite une fois puis n'est rafraîchie que par un flux de changements ou le planificateur. Sur une telle table dotée d'une `off_peak_window` (sur la table ou sa source, lue dans `off_peak_tz`), un rafraîchissement ne s'exécute que tant que la fenêtre est ouverte : celui qui arrive à échéance ou est demandé hors de la fenêtre attend sa prochaine ouverture, et une fenêtre sans `cache_ttl` rafraîchit une fois par ouverture. La première construction n'est jamais retenue, car les lectures de la table l'attendent.

### Le paramètre `replicate` {: #the-replicate-setting }

`replicate` est un entier sur une table. Une table sans valeur prend celle de sa source, et quand aucune n'est définie, le seuil global s'applique. Une réplique est une copie d'une table source conservée dans le magasin du moteur et rafraîchie selon les paramètres de ponctualité de la table ; ce n'est pas un flux continu de changements.

| Valeur | Nom | Signification |
| --- | --- | --- |
| non définie | Par défaut | Le seuil global s'applique |
| `-1` | Jamais | Lue en direct partout où un chemin direct existe. Répliquée uniquement sur un moteur qui ne peut pas lire la source sur place |
| `N` supérieur à 0 | Hot-N | Lue en direct jusqu'à ce que la table dépasse N instructions gouvernées par intervalle, puis servie depuis sa réplique |
| `0` | Toujours | Les lectures proviennent de la réplique |

Seul Toujours est une garantie. Jamais et les valeurs Hot sont au mieux. `load_protected` avec `replicate: -1` est refusé au chargement et à l'enregistrement de la configuration. Une source avec `replicate: 0` n'a d'attachement direct sur aucun moteur ; chacune de ses tables est donc servie depuis sa réplique.

Le plancher est jugé d'après les tables qu'une instruction lit. Une instruction qui ne lit aucune table servie depuis une réplique conserve sa route directe, même si sa source a d'autres tables répliquées.

Une réplique est indexée sur sa clé primaire déclarée et sur les colonnes sur lesquelles ses relations déclarées font la jointure, index construits après l'atterrissage. Où ces index accélèrent les jointures, où conserver les répliques et comment choisir leur fraîcheur : voir [Répliques](replicas.md).

### Réplication hot {: #hot-replication }

Une table dont `replicate` n'est pas défini, ou vaut `N` supérieur à 0, est lue en direct tant qu'elle n'est pas très sollicitée.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| Paramètre | Variable d'environnement | Valeur par défaut |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

Les trois sont des paramètres d'opérateur : l'interface d'administration les affiche sous Hot Tables, et une valeur enregistrée s'applique à la prochaine évaluation.

Comment une table est comptée :

- Une instruction gouvernée compte chaque table qu'elle lit une seule fois, quel que soit le nombre de lectures.
- Une instruction servie depuis le cache de réponses ne compte pas. Elle n'a placé aucune charge sur la source.
- Une instruction refusée par la gouvernance ne compte pas.

Une table est promue quand son compteur atteint son seuil (son propre `N`, sinon celui de sa source, sinon `hot_threshold`) et rétrogradée quand le compteur tombe sous la moitié de ce seuil. La promotion demande la construction d'une réplique ; la table est lue en direct jusqu'à la fin de cette construction, puis depuis la réplique. La rétrogradation ramène aussitôt les lectures vers la source.

Une table n'est pas promue quand elle contient plus de lignes que `hot_max_rows`, quand son signal de changement est `ttl` ou `ttl_probe` et que ni elle ni sa source ne déclare de `cache_ttl`, ou quand le niveau hot de Redis la gère déjà. Le résumé de la table dans l'interface d'administration indique lequel de ces cas s'applique.

Les compteurs résident dans Redis. Avec un Redis partagé (`cache.redis_url`), ils couvrent tous les processus du déploiement. Avec le Redis embarqué, ils sont par processus ; la promotion ne s'exécute donc que lorsqu'un seul worker sert les requêtes ; avec plusieurs workers et sans Redis partagé, rien n'est promu, et le résumé l'indique.

## Authentification {: #authentication }

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

### Types de fournisseur d'authentification {: #auth-provider-types }

| Fournisseur | Cas d'usage | Validation du jeton |
| ---------- | ---------- | ----------------- |
| `simple` | Développement et tests locaux. Utilisateurs définis en YAML. | JWT signé avec `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (toutes méthodes). | `verify_id_token()` du SDK `firebase-admin` |
| `keycloak` | OIDC Keycloak. Rôles de tenant et de client mappés. | Validation JWT fondée sur JWKS |
| `oauth` | OIDC générique (Okta, Azure AD, Auth0, PingFederate). | JWKS depuis l'URL de découverte |
| `basic` | Déploiements autonomes. Les comptes vivent dans le magasin propre à Provisa. | Mot de passe bcrypt, ou SCRAM-SHA-256 sur pgwire |

Les credentials du superutilisateur (bloc `superuser`) fonctionnent avec tout fournisseur et se résolvent toujours vers le rôle admin doté de toutes les capacités. Servent à l'installation initiale, avant la configuration d'une authentification externe.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Fait annoncer SASL avec `SCRAM-SHA-256` par pgwire, de sorte qu'un mot de passe est prouvé plutôt qu'envoyé en clair. Cela ne vaut que pour le fournisseur `basic` — aucun autre fournisseur ne détient les vérificateurs RFC 5802 dont SCRAM a besoin — et le channel binding n'est pas proposé.

Les vérificateurs ne peuvent pas être dérivés des hachages bcrypt existants. Un vérificateur est écrit chaque fois qu'un mot de passe transite en clair, si bien que la première connexion SCRAM d'un utilisateur suit son prochain enregistrement, sa prochaine connexion, son prochain changement de mot de passe ou la prochaine réinitialisation par un administrateur. En attendant, les connexions de cet utilisateur retombent sur l'échange en clair au-dessus de TLS ; le réseau ne révèle pas qui a migré.

### Limitation des tentatives de connexion (`auth.login_throttle`) {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Activée par défaut avec les valeurs indiquées ; le bloc ne fait que les ajuster. Le compteur se situe à la couche de validation des credentials, si bien que les échecs sur HTTP, pgwire et Bolt s'accumulent pour le même sujet et qu'un verrouillage tient sur toutes les surfaces. Il est par processus : plusieurs workers d'API autorisent chacun jusqu'à `max_attempts`.

### Jetons d'accès personnels {: #personal-access-tokens }

Les PAT n'ont besoin d'aucun bloc de configuration — ils sont toujours acceptés, et le magasin est créé avec le reste du schéma du plan de contrôle. Ce qui est configurable, c'est l'expiration qu'un utilisateur peut demander à l'émission : de 1 à 366 jours, ou aucune pour un jeton qui n'expire pas. Voir [Modèle de sécurité](security.md#personal-access-tokens).

### TLS mutuel {: #mutual-tls }

La vérification du certificat client se configure par variable d'environnement plutôt que dans `provisa.yaml`, aux côtés des réglages de certificat TLS qu'elle prolonge.

| Variable | Défaut | Signification |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | non défini | Faisceau PEM de la ou des AC autorisées à signer les certificats client. Le définir active la vérification du certificat client |
| `PROVISA_MTLS_MODE` | `required` dès qu'une AC est définie | `required` ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Exige que le common name du certificat soit égal au nom d'utilisateur sous lequel la connexion s'authentifie |

Chacune accepte une surcharge par protocole, selon la même nomenclature que les réglages TLS. Un mode défini sans AC, ou un mode qui n'est ni l'une ni l'autre valeur, refuse de démarrer plutôt que de servir des connexions que l'opérateur croit vérifiées.

### Adresser une organisation via TLS {: #addressing-an-org-over-tls }

Rien à configurer. Sur un déploiement multi-organisations, pgwire et Bolt lisent l'organisation dans le nom d'hôte composé par le client, porté par le ClientHello TLS, exactement comme HTTP la lit dans l'en-tête `Host`. Un client qui se connecte à `acme.provisa.dev` demande l'organisation `acme` ; la requête est refusée à moins que le principal authentifié n'en soit membre. Se connecter par adresse IP ne demande aucune organisation, ce qui est le cas de toute connexion sur un déploiement mono-organisation.

### Exemple complet de configuration d'authentification (commenté) {: #full-auth-config-example-commented-out }

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

## Mutations upsert {: #upsert-mutations }

Pour les tables dotées d'une clé primaire, Provisa génère automatiquement des champs de mutation `upsert_<table>`. Ceux-ci se compilent en un upsert dans le dialecte cible — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` sur PostgreSQL, `ON DUPLICATE KEY UPDATE` sur MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Les colonnes de conflit sont dérivées des métadonnées de clé primaire. Toutes les règles de visibilité de colonne et de droit d'écriture s'appliquent.

## Distinct On

L'argument `distinct_on` sélectionne la première ligne pour chaque valeur distincte des colonnes indiquées. Disponible sur les champs de requête racine.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Se compile en `SELECT DISTINCT ON (region) ...` sur PostgreSQL. Pour les autres dialectes, un repli par fonction de fenêtrage est utilisé.

## Presets des autorisations d'écriture {: #write-grant-presets }

Une autorisation d'écriture définit elle-même des colonnes à l'insertion et à la mise à jour. Les presets sont déclarés dans la liste `presets` de l'autorisation, par rôle et par opération.

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    columns:
      - {name: amount, visible_to: [analyst]}
      - {name: created_by, visible_to: [analyst]}
      - {name: created_at, visible_to: [analyst]}
      - {name: source_system, visible_to: [analyst]}
    write_grants:
      - role: analyst
        operation: insert
        columns: [amount]
        presets:
          - {column: created_by, source: session, name: user_id}   # from the request's session
          - {column: created_at, source: now}                      # time of the write
          - {column: source_system, source: literal, value: provisa}
```

| Source | Comportement |
| -------- | ---------- |
| `session` | Injecte la valeur de la variable de session nommée (`name`) de la requête |
| `now` | Injecte l'instant de l'écriture |
| `literal` | Injecte une constante (`value`) |

Les presets sont ajoutés à l'instruction pendant la compilation, avant la génération du SQL. L'instruction ne peut pas fournir une colonne à preset, et une colonne à preset ne figure pas dans le type d'entrée de la mutation.

## Rôles hérités {: #inherited-roles }

Les rôles peuvent hériter d'un seul rôle parent via `parent_role_id`. La chaîne est aplatie au démarrage. Un enfant détient l'union des capacités et de l'accès aux domaines de ses ancêtres ; une colonne, une métrique, une fonction ou un webhook accordé à un ancêtre est accordé à l'enfant ; et les règles RLS d'un ancêtre s'appliquent à l'enfant par table, en commençant par le rôle le plus proche, la règle propre de l'enfant pour une table remplaçant celle du parent.

```yaml
roles:
  - id: admin
    description: Administers everything.
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    description: An intern, with the junior analyst's access.
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

L'héritage sur plusieurs niveaux est pris en charge. Les capacités et le `domain_access` explicites du rôle enfant sont fusionnés avec ceux du parent. Un parent doit être un rôle existant, ne peut pas être le rôle lui-même et ne peut pas fermer un cycle ; chacun de ces cas est refusé à l'enregistrement.

## Déclencheurs planifiés {: #scheduled-triggers }

Déclencheurs fondés sur cron qui appellent une URL de webhook selon une planification. Utilise APScheduler.

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

Les tâches planifiées se gèrent via l'interface d'administration (bascule activer/désactiver) ou la mutation d'administration `toggle_scheduled_task`.

Un déclencheur peut se déclencher sur une table enregistrée plutôt que selon une planification : `on: {table, event}` nomme la table sous la forme `source_id.schema.table` et l'événement, `change`, `error` ou `quarantine`. Un déclencheur `command` exécute une commande enregistrée sous son `role`, par la répartition gouvernée propre à la commande, et lui transmet en arguments ce qui l'a déclenché. Un changement transmet `table` et, quand le signal de changement le porte (Kafka, Debezium), le lot chargé sous `changes`. Un événement `error` ou `quarantine` transmet `table`, `event`, `reason` et `error`. Un déclencheur dont la commande déclare un argument qu'il ne transmet pas est refusé à l'enregistrement. Un changement se déclenche une fois par salve que signale son signal de changement, ou dès qu'une écriture faite via Provisa aboutit ; une table servie depuis sa réplique se déclenche quand une construction charge des lignes différentes de la précédente. L'écriture propre d'une commande est un changement, les déclencheurs s'enchaînent donc. Chaque exécution est enregistrée avec son heure, son résultat (`succeeded`, `refused` ou `failed`), son erreur et ce qui l'a déclenchée, et la requête d'administration `scheduledTasks` la liste. Seuls les déclencheurs de prod se déclenchent, et rien n'est installé dans une base de données source.

```yaml
scheduled_triggers:
  - id: bronze_to_silver
    on: {table: bronze_pg.public.bronze, event: change}
    command: promote_to_silver
    role: loader
  - id: alert_on_quarantine
    on: {table: bronze_pg.public.bronze, event: quarantine}
    command: notify_steward      # declares arguments table, reason
    role: loader
```

## Format OrderBy {: #orderby-format }

OrderBy utilise le format `{column: direction}` avec une énumération de direction à six valeurs :

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

Le tri par relation est pris en charge au moyen d'objets imbriqués :

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observabilité {: #observability }

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

### Filtres de télémétrie {: #telemetry-filters }

Provisa fait tourner deux chemins d'export OTLP indépendants : votre collecteur interne et l'endpoint optionnel du support Provisa. Chaque chemin a son propre filtre. Les filtres s'exécutent à l'intérieur d'un `_FilteringExporter` enveloppant, avant que les spans ne quittent le processus — les objets span d'origine ne sont jamais modifiés.

**`telemetry_filter`** — contrôle ce qui parvient à votre collecteur interne.

| Clé | Type | Défaut | Description |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Remplace les littéraux chaîne et numériques de `db.statement` par `?` |
| `redact_attributes` | list[str] | `[]` | Clés d'attribut entièrement retirées de chaque span |

**`support_telemetry_filter`** — contrôle ce qui parvient à l'endpoint du support Provisa. Sur ce chemin, la censure des littéraux SQL est activée par défaut, puisque les données de requête vous appartiennent.

| Clé | Type | Défaut | Description |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Remplace les littéraux chaîne et numériques de `db.statement` par `?` |
| `redact_attributes` | list[str] | `[]` | Clés d'attribut entièrement retirées de chaque span |

Exemple de `db.statement` censuré — avec `redact_sql_literals: true`, cet attribut de span :

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

devient :

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint du support {: #support-endpoint }

`support_endpoint` (ou la variable d'environnement `PROVISA_SUPPORT_OTLP_ENDPOINT`) transmet la télémétrie au support Provisa à des fins de diagnostic. Lorsqu'il n'est pas défini, aucune donnée ne quitte votre infrastructure par ce chemin. Le filtre du support s'applique indépendamment du filtre interne — vous pouvez censurer les littéraux SQL des deux exports tout en partageant avec le support les temps d'exécution des spans et les données d'erreur.

### Détection du protocole d'endpoint {: #endpoint-protocol-detection }

Provisa choisit OTLP/HTTP ou OTLP/gRPC d'après le schéma d'URL de l'endpoint. Les URL commençant par `http://` ou `https://` utilisent OTLP/HTTP, avec ajout automatique de `/v1/traces`, `/v1/metrics` et `/v1/logs`. Tout autre schéma utilise OTLP/gRPC avec `insecure=True`.

## Moteur de fédération {: #federation-engine }

Configurer un moteur de fédération est facultatif. Le défaut est `duckdb` — sans configuration, en processus, aucun service externe requis. Choisissez un autre moteur lorsque vous avez besoin d'une échelle MPP ou souhaitez réutiliser un entrepôt existant.

Précédence : variable d'environnement `PROVISA_ENGINE` → champ de configuration `federation_engine` persisté par l'interface d'administration → `duckdb`. Les changements prennent effet au redémarrage du service.

### Vue d'ensemble des moteurs {: #engine-overview }

| Clé de moteur | Libellé | Dialecte | MPP | Mécanisme de lien externe | Authentification |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | Oui | Catalogues Trino (large jeu de connecteurs) | Credentials JDBC |
| `trino-byo` | Trino | Trino SQL | Oui | Comme `trino` ; coordinateur non managé | Credentials JDBC |
| `pg` | PostgreSQL | PostgreSQL | Non | FDW / pg_duckdb | Credentials PostgreSQL |
| `duckdb` | DuckDB | DuckDB | Non | ATTACH natif par extension | Aucune (en processus) |
| `clickhouse` | ClickHouse (embarqué) | ClickHouse | Oui | Moteurs de table S3 / IcebergS3 / DeltaLake | chdb (en processus, sans authentification) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Oui | Moteurs de table S3 / IcebergS3 / DeltaLake | Credentials ClickHouse |
| `snowflake` | Snowflake | Snowflake | Oui | Stage externe + table externe | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Oui | Tables externes Unity Catalog via REST | `PROVISA_ENGINE_URL` (jeton bearer + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Oui | Tables externes / BigLake de BigQuery | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Oui | Raccourcis OneLake → OPENROWSET | Azure AD (`az login` ou identité managée) |
| `synapse` | Azure Synapse | T-SQL | Oui | OPENROWSET ADLS / tables externes | Azure AD |
| `mysql` | MySQL | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `mariadb` | MariaDB | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `oracle` | Oracle Database | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `mssql` | Microsoft SQL Server | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `db2` | IBM Db2 | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `redshift` | Amazon Redshift | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `greenplum` | Greenplum | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `cockroachdb` | CockroachDB | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `yugabytedb` | YugabyteDB | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `opengauss` | openGauss | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `tidb` | TiDB | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `singlestore` | SingleStore | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `vertica` | Vertica | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `exasol` | Exasol | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `teradata` | Teradata Vantage | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `saphana` | SAP HANA | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `sapase` | SAP ASE (Sybase) | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `sqlanywhere` | SAP SQL Anywhere | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `monetdb` | MonetDB | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `firebird` | Firebird | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |
| `sqlalchemy` | Autre base relationnelle (par URL de connexion) | Selon le dialecte | Non | Aucun (atterrissage seul) | Credentials selon le dialecte |

### Référence des moteurs {: #engine-reference }

#### trino / trino-byo

`trino` est le coordinateur Provisa managé ; `trino-byo` se connecte à votre propre cluster Trino. Les deux utilisent Trino SQL et offrent la plus large portée en types de source.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Fédère via les extensions postgres_fdw (SQL/MED) et pg_duckdb. Mononœud ; pas de MPP. Idéal lorsque vos données vivent déjà dans PostgreSQL et que vous voulez joindre quelques sources distantes.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL`.

#### duckdb

En processus ; aucun service externe. Le moteur par défaut. `PROVISA_DATA_DIR` contrôle l'emplacement du magasin embarqué (`~/.provisa` par défaut).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Le magasin de matérialisation vaut par défaut `~/.provisa/materialize.duckdb` — le seul moteur dont le magasin par défaut n'est pas PostgreSQL.

#### clickhouse (embarqué) / clickhouse-server {: #clickhouse-embedded-clickhouse-server }

`clickhouse` utilise chdb (en processus). `clickhouse-server` se connecte à une instance ClickHouse externe ou à ClickHouse Cloud. Les deux lisent directement Delta Lake, Iceberg et Hudi via les moteurs de table natifs de ClickHouse.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL`.

#### snowflake

Moteur en guise d'entrepôt : Snowflake exécute les requêtes ; Provisa y pousse les données des sources par des stages externes.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL`.

#### databricks

Les tables externes d'Unity Catalog font le pont entre les sources gérées par Provisa et Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL`.

#### bigquery

Tables externes et BigLake de BigQuery. Le projet vient de l'URL ou de `GOOGLE_CLOUD_PROJECT` ; authentification par clé de compte de service.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL`.

#### fabric / synapse

Les deux utilisent T-SQL sur TDS avec authentification Azure AD (`az login` ou identité managée). Omettez `PROVISA_ENGINE_URL` pour lire les détails de connexion depuis les variables d'environnement.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL`.

#### Moteurs de base relationnelle (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) et `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

Une clé par base relationnelle joignable sur le réseau, toutes sur le même runtime d'atterrissage seul (pas de fédération vers des sources externes) : chaque source atterrit dans le magasin et y est interrogée. La clé sélectionne la base ; `PROVISA_ENGINE_URL` porte le DSN attendu par son dialecte. `sqlalchemy` est le fourre-tout pour une base sans clé propre. Les magasins embarqués dans un fichier (SQLite, Access) ne sont pas proposés — le serveur doit être joignable sur le réseau.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Le magasin de matérialisation vaut par défaut `TENANT_DATABASE_URL`.

### Magasin de matérialisation {: #materialization-store }

Lorsqu'une source ne peut pas s'attacher en direct (aucun connecteur ATTACH pour le moteur sélectionné), elle atterrit dans le magasin de matérialisation du moteur. Ordre de résolution : `PROVISA_MATERIALIZE_URL` explicite → défaut déclaré du moteur → erreur franche (aucun repli silencieux).

DuckDB déclare son fichier embarqué (`~/.provisa/materialize.duckdb`) comme défaut. Tous les autres moteurs prennent `TENANT_DATABASE_URL` (PostgreSQL) par défaut. Surchargez n'importe quel moteur avec `PROVISA_MATERIALIZE_URL`.

Le magasin détermine ce que les index d'une réplique apportent aux jointures : un magasin PostgreSQL indexe les répliques et un moteur qui exécute le SQL dedans lit ces index, tandis que DuckDB joint les répliques de son fichier embarqué sur une copie par instruction. Voir [Répliques](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

### Hints de fédération par source {: #per-source-federation-hints }

Les paramètres de connexion étendus que les champs standard host/port/user/password ne peuvent pas porter vont dans `federation_hints` sur la source. Voir la référence des types de source ci-dessus pour les clés de hint propres à chaque type. Un exemple consolidé :

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

Pour les sources Google Cloud, pointez `GOOGLE_APPLICATION_CREDENTIALS` vers le chemin de votre fichier de clé de compte de service. Pour Fabric et Synapse, authentifiez-vous avec `az login` (développement) ou une identité managée (production) — le moteur lit les credentials via le `DefaultAzureCredential` d'`azure-identity`.

## Variables d'environnement {: #environment-variables }

| Variable | Défaut | Description |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Chemin du fichier de configuration |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | URI du magasin du plan de contrôle (SQLAlchemy async) ; accepte `sqlite+pysqlite://…` / `duckdb://…` pour le magasin de bureau embarqué |
| `PLATFORM_DATABASE_URL` | — | URI du registre de plateforme (annuaire des tenants, registre des moteurs) ; requis au démarrage, sans repli |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` utilise fakeredis embarqué au lieu d'un serveur Redis — sans Docker |
| `PG_HOST` | `localhost` | Hôte PostgreSQL |
| `PG_PORT` | `5432` | Port PostgreSQL |
| `PG_DATABASE` | `provisa` | Base PostgreSQL |
| `PG_USER` | `provisa` | Utilisateur PostgreSQL |
| `PG_PASSWORD` | `provisa` | Mot de passe PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Clé du moteur de fédération |
| `PROVISA_ENGINE_URL` | — | URL de connexion pour les moteurs pilotés par URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Remplace le DSN du magasin de matérialisation (défaut : celui déclaré par le moteur) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Répertoire de données du magasin DuckDB embarqué |
| `TRINO_HOST` | `localhost` | Hôte du coordinateur Trino |
| `TRINO_PORT` | `8080` | Port HTTP du coordinateur Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Chemin du JSON de clé de compte de service GCP (moteur/source BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Projet GCP par défaut (BigQuery ; remplacé par l'URL) |
| `FABRIC_SQL_SERVER` | — | Endpoint SQL du Fabric Warehouse (alternative à `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Nom de la base du Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Endpoint SQL serverless Synapse |
| `SYNAPSE_DATABASE` | — | Nom de la base Synapse |
| `REDIS_URL` | — | URL de connexion Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Limite d'échantillonnage par défaut |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Plafond de lignes quand une requête ne fournit pas de `LIMIT` explicite |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Budget de reprise en lecture de niveau 1, en secondes ; backoff exponentiel avec jitter complet |
| `ZAYCHIK_PORT` | `8480` | Port du proxy Flight SQL Zaychik |
| `FLIGHT_PORT` | `8815` | Port du serveur Arrow Flight de Provisa |
| `GRPC_PORT` | `50051` | Port du serveur gRPC Protobuf de Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Active la redirection côté serveur au-delà d'un seuil |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Seuil de nombre de lignes par défaut |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Format de redirection par défaut |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 des résultats redirigés |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL d'endpoint compatible S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Clé d'accès S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Clé secrète S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de l'URL présignée (secondes) |
| `PROVISA_MTLS_CLIENT_CA` | — | Faisceau PEM de la ou des AC autorisées à signer les certificats client ; le définir active la vérification du certificat client sur pgwire, Bolt, gRPC et Flight |
| `PROVISA_MTLS_MODE` | `required` dès qu'une AC est définie | `required` ou `optional` ; toute autre valeur refuse de démarrer |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Exige que le common name du certificat soit égal au nom d'utilisateur qui s'authentifie |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Sites séparés par des virgules autorisés à ouvrir un WebSocket Bolt depuis un navigateur ; non défini, toute origine navigateur est refusée |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras pyproject intégrés à l'image applicative ; `scripts/provisa` les dérive de `dq_checker` dans `~/.provisa/config.yaml` |
| `PROVISA_DQ_CHECKER` | `none` | Réservé à l'installeur : `none`/`soda`/`gx`, lu par `first-launch.sh` en mode non interactif et écrit dans `config.yaml` sous la clé `dq_checker` |
| `ANTHROPIC_API_KEY` | — | Clé d'API Claude (découverte) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Remplace `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Remplace `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Remplace `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Remplace `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Délai de vidage du processeur de spans par lots |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Remplace `observability.support_endpoint` |
