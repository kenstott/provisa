# רפרנס תצורה

Provisa מוגדרת דרך קובץ YAML (ברירת מחדל: `config/provisa.yaml`). (REQ-528)

## מקורות (Sources)

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

כל המקורות חולקים קבוצת שדות משותפת. [tool-verified: `provisa/core/models.py:129-212`]

| שדה | ברירת מחדל | הערות |
| ------- | --------- | ------- |
| `id` | חובה | אלפאנומרי, מקפים, קווים תחתונים |
| `type` | חובה | ראו הטבלה למטה |
| `host` | `""` | שם מארח או IP |
| `port` | `0` | `0` משמעו שכל מחבר מספק ברירת מחדל משלו — אין מפת פורט-ברירת-מחדל מרכזית |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | תומך בפענוח סוד `${env:VAR}` |
| `path` | `null` | נתיב קובץ או URI למקורות מבוססי-קובץ |
| `base_url` | `null` | כתובת URL בסיסית למקורות API |
| `pool_min` / `pool_max` | `1` / `5` | גבולות pool החיבורים |
| `cache_enabled` | `true` | הפעלה/כיבוי מטמון לכל הטבלאות במקור זה |
| `cache_ttl` | `null` | שניות; `null` יורש את ברירת המחדל הגלובלית |
| `federation_hints` | `{}` | פרמטרים מורחבים לפי-מחבר (dict[str,str]); ראו רפרנס סוגים למטה. REQ-281 |
| `mapping` | `{}` | DSL מיפוי עבור redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | הגבלת מקור זה למזהי דומיין מסוימים; ריק = ללא הגבלה |
| `description` | `""` | |

### סוגי מקורות נתמכים [tool-verified: `provisa/core/models.py:36-101`]

| סוג | סגנון חיבור | הערות |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | pool של Asyncpg; PgBouncer מרצון דרך `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (קובץ DB) | הרחבת הקהילה firebird של DuckDB (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | משתמש שוב בדרייבר/דיאלקט PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | משתמש שוב בדרייבר/דיאלקט PostgreSQL (REQ-950) |
| `greenplum` | host/port | משתמש שוב בדרייבר/דיאלקט PostgreSQL (REQ-950) |
| `tidb` | host/port | משתמש שוב בדרייבר/דיאלקט MySQL (REQ-950) |
| **Cloud DW** | | |
| `snowflake` | host/port + `federation_hints` | `account` נדרש ב-hints |
| `bigquery` | `federation_hints` | `project` נדרש; אימות דרך `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` נדרש ב-hints |
| `fabric` | משתני סביבה או `PROVISA_ENGINE_URL` | T-SQL על גבי TDS, אימות Azure AD |
| `synapse` | משתני סביבה או `PROVISA_ENGINE_URL` | T-SQL על גבי TDS, אימות Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | ה-hint‏ `secure` מפעיל TLS; ברירת מחדל לפורט 8123/8443 |
| `elasticsearch` | host/port + `mapping` DSL | |
| `pinot` | host/port | נקודת קצה REST של הבקר |
| `druid` | host/port | נקודת קצה Avatica של ה-broker |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI טבלה) | `delta_scan` של DuckDB; גישה לאחסון אובייקטים דרך `federation_hints` |
| `iceberg` | `path` (URI טבלה) | `iceberg_scan` של DuckDB; גישה לאחסון אובייקטים דרך `federation_hints` |
| `hudi` | `path` (URI טבלה) | מנוע Hudi של ClickHouse, zero-copy (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | backend אחסון ב-`mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + מפתחות S3 ב-`mapping` | סוג נבדל; תמיד אחסון S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | שדות חיבור פשוטים; ללא DSL מיפוי |
| `cassandra` | host/port | שדות חיבור פשוטים; ללא DSL מיפוי |
| `redis` | host/port + `mapping` DSL | |
| **סטרימינג** | | |
| `kafka` | רישום בלבד | התצורה האמיתית נמצאת ב-`kafka_sources[]`; ראו §Kafka למטה |
| `websocket` | host/port/path + `federation_hints` | הזנת WebSocket חיצונית |
| `rss` | host/port/path + `federation_hints` | הזנת RSS 2.0 / Atom |
| **גרף/סמנטי** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **קובץ** | | |
| `sqlite` | `path` | תמיד מנותב דרך המנוע (ללא pool ישיר) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (תיקייה) | סורק glob; חושף CSV/Parquet/XLSX/JSON כטבלאות |
| **API/מרוחק** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port או `mapping.url` + `mapping` DSL | |
| `graphql_remote` | `base_url` + `mapping` אופציונלי | כותרות, forward-client-headers, timeout ב-`mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (מיקום Flight) | הרחבת airport של DuckDB (REQ-899) |
| `ingest` | receiver דחיפה | שירותים חיצוניים שולחים POST של אירועי JSON |
| **SaaS** | | |
| `sharepoint` | `base_url` או `host` + `mapping` | אימות דרך `mapping.auth_type` |
| `splunk` | `host`/`port` או `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | מודל `GovDataSource` נפרד; ראו §GovData למטה |

### רפרנס סוג מקור

לסוגים הדורשים תצורה לא-מובנת-מאליה יש ערך קצר למטה. סוגי RDBMS (postgresql, mysql וכו') משתמשים רק בשדות המשותפים לעיל — אין צורך בסעיף נוסף.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

מקורות `govdata` משתמשים במודל ברמה עליונה נפרד, `GovDataSource`, לא ב-`Source` הגנרי. (REQ-540) הגישה מחולקת לפי קיבוץ subject.

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

כל subject ממופה לסכמה אחת או יותר של GovData. הגדרת מקור `govdata` עם subject חושפת אוטומטית את כל הסכמות עבור אותו subject. (REQ-540)

| Subject | סכמות |
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

הסכמות `ref` ו-`geo` תמיד כלולות כסכמות מקשרות (linker) — אינן ניתנות להגדרה ואינן מופיעות ברשימה לעיל. (REQ-541) השתמשו ב-subject‏ `ALL` כדי להעניק גישה לכל הסכמות. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

השורה `kafka` תחת `sources:` היא רישום בלבד. ה-`details()` של המחבר שלה מחזיר `{}` — התצורה האמיתית נמצאת בבלוק `kafka_sources[]` ברמה העליונה, לא בשורת `sources:`. Kafka תמיד VIRTUAL_SOURCE (מנותב דרך המנוע; ללא pool ישיר). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**חלון זמן (Time Window)** — `default_window` מגביל כל שאילתה לתקופת זמן אחרונה, ומונע קריאות בלתי-מוגבלות מנושאים בנפח גבוה. (REQ-148) פורמט: `1h`, `30m`, `7d`, `60s`. ברירת מחדל `1h`. מוזרק אוטומטית כ-`WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. לקוחות יכולים לדרוס עם פילטר `_timestamp` משלהם בארגומנט `where` של GraphQL.

**Discriminator** — תצורות topic מרובות יכולות להצביע על אותו topic פיזי של Kafka עם ערכי `discriminator` שונים, ולהפיק טיפוסי GraphQL נפרדים. (REQ-149) ה-discriminator מוזרק אוטומטית כסעיף WHERE.

**מקור סכמה (Schema Source)**

| ערך | התנהגות |
| ------- | ---------- |
| `registry` | שליפת סכמה מ-Confluent Schema Registry |
| `manual` | הגדרת עמודות inline בתצורה (ללא צורך ב-Schema Registry) |
| `sample` | גילוי אוטומטי מהודעות דוגמה |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` ב-`federation_hints` נדרש. `warehouse`, `role`, ו-`schema` אופציונליים.

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

`http_path` ב-`federation_hints` נדרש. `password` נושא את ה-personal access token. `catalog` אופציונלי (נישא ב-SQL/hints, לא בשדה `database`).

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

`project` ב-`federation_hints` נדרש. האימות משתמש ב-`GOOGLE_APPLICATION_CREDENTIALS` (נתיב לקובץ מפתח service-account) או Application Default Credentials בסביבת המנוע.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

שניהם משתמשים ב-T-SQL על גבי TDS עם אימות Azure AD. התאמתו עם `az login` (פיתוח) או זהות מנוהלת (ייצור) — המנוע קורא את האישורים דרך `DefaultAzureCredential` של `azure-identity`. פרטי חיבור מגיעים ממשתני סביבה: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE‏` (Fabric) או `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE‏` (Synapse), או דרך `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` ב-`federation_hints` מפעיל TLS על ממשק ה-HTTP. הפורט כברירת מחדל `8123` (רגיל) או `8443` (כאשר `secure: "true"`). `schema` ב-`federation_hints` דורס את הסכמה המרוחקת. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`path` הוא ה-URI של הטבלה (S3, GCS, ADLS או מקומי). גישה לאחסון אובייקטים דורשת אישורי `federation_hints`. עבור Cloudflare R2, הוסיפו `account_id`.

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

`host` ו-`port` מצביעים על ה-Hive Thrift metastore (פורט ברירת מחדל 9083). עבור `hive`, הגדירו `mapping["storage"]` כדי לבחור את ה-backend של אחסון האובייקטים. מפתחות חובה חסרים נכשלים בקול רם — ללא נפילה-חוזרת. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` הוא סוג נבדל שתמיד מצהיר על אחסון S3 (REQ-229); אין צורך ב-`mapping.storage`.

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

ערכים מקובלים עבור `mapping.storage`: `hadoop` (ברירת מחדל), `hdfs`, `local`, `s3`, `azure`, `adls`. מפתחות מיפוי S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. מפתחות מיפוי ADLS: `storage_account`, `access_key` או `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

משתמש ב-DSL של `mapping`. `mongodb` ו-`cassandra` משתמשים בשדות חיבור פשוטים ו-**אינם** משתמשים ב-DSL מיפוי.

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

`mapping.url` דורס `host:port` כששניהם קיימים.

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

`spreadsheet_id` ב-`federation_hints` נדרש. האימות משתמש ב-SECRET מסוג `gsheet` של DuckDB המסופק בזמן ה-attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### מקורות קובץ (csv / parquet / sqlite / files)

`path` נדרש. `files` סורק תיקייה עבור קבצי CSV, Parquet, XLSX, ו-JSON, וחושף כל אחד כטבלה. כל המקורות מבוססי-הקובץ הם VIRTUAL (מנותבים דרך המנוע; ללא pool ישיר). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### מקורות API / מרוחקים

**openapi** — הגדירו `base_url` לכתובת ה-URL הבסיסית של OpenAPI. גילוי הסכמה קורא את מפרט ה-OpenAPI בעת ההפעלה.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — הגדירו `base_url`. מפתחות `mapping` אופציונליים: `headers` (dict של כותרות סטטיות), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport** — `base_url` הוא מיקום שרת ה-Arrow Flight. הרחבת airport של DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — השתמשו ב-`host`, `port`, `path`, ו-`federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

## דומיינים

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## שיוֹם (Naming)

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### מוסכמת שיוֹם (Naming Convention)

רשות השיוֹם היא מקור האמת היחיד לשמות הפונים ללקוח; שמות עמודות ה-backend הפיזיים לעולם אינם נחשפים ללקוחות. (REQ-194) כל שפת שאילתה גוזרת את שם העמודה מ-`column.alias` שלה אם הוגדר, אחרת משם העמודה הפיזי דרך המוסכמה המוגדרת שלו. (REQ-194)

מוסכמת ה-GraphQL היא אחת משלושה enum-ים מוגדרים מראש. (REQ-416) מחרוזות free-form ישנות (`none`, `snake_case`, `camelCase`, `PascalCase`) הוצאו משימוש. (REQ-416)

| מוגדר-מראש (Preset) | ברירת מחדל | שמות טיפוס | שמות שדה | שמות מוטציה |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | כן | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

מוסכמת ברירת המחדל של GraphQL היא `apollo_graphql`, שמפיקה שמות שדה ומוטציה ב-camelCase. (REQ-194, REQ-416) מוסכמת ה-SQL נפרדת, עם ברירת מחדל `snake_case`, מוחלת דרך `apply_sql_name()`; מוסכמת GraphQL מוחלת דרך `apply_gql_name()`, ושם ה-CQL נגזר משם ה-GraphQL. (REQ-194)

`domain_prefix: bool` הוא אפשרות אורתוגונלית שחלה בלי קשר למוגדר-מראש שנבחר. (REQ-416)

`column.alias` מפורש הוא השם הקנוני: SQL משתמש בו כלשונו ללא הפעלת מוסכמה, GraphQL מחיל עליו את המוסכמה שלו, ו-CQL נגזר משם ה-GraphQL. (REQ-194)

דריסה לפי-מקור:

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

דריסה לפי-טבלה:

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### קידומת דומיין (Domain Prefix)

כאשר `domain_prefix: true`, כל שמות שדה וטיפוס GraphQL מקבלים קידומת מזהה הדומיין באמצעות מפריד קו-תחתון כפול: (REQ-154)

| טבלה | דומיין | שם שדה |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

זה מונע התנגשויות שמות כאשר לדומיינים שונים יש טבלאות באותו שם, והופך שאילתות למתועדות-מאליהן.

### כללי שיוֹם

כללי regex שמוחלים על שמות טבלה בעת חילול שמות שדה GraphQL. מוחלים לפי סדר לפני פתרון ייחודיות. (REQ-542)

## טבלאות

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

### כינויים (Aliases)

כינויי טבלה ועמודה דורסים את שם ה-GraphQL ברירת המחדל. (REQ-155) שימושי עבור:

- שינוי שם למסדי נתונים חידתיים (לדוגמה, `tbl_cust_seg` ← `customer_segments`)
- הימנעות מקיצורים בשכבת ה-API
- יצירת אוצר מילים נקי, ספציפי-לדומיין

### תיאורים

תיאורי טבלה ועמודה נכללים ב-SDL של GraphQL המחולל. (REQ-156) הם מופיעים בסייר התיעוד של GraphiQL ובשאילתות introspection. הגדירו אותם בתצורת YAML או דרך ממשק הניהול.

### נתיב (חילוץ JSON מחושב)

עמודות יכולות לחלץ ערכים מעמודת מקור JSON/JSONB באמצעות `path` בסימון נקודה. (REQ-151) שימושי לנתונים חצי-מובנים בהודעות Kafka, מסמכי MongoDB, או עמודות JSONB של PostgreSQL.

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

פורמט הנתיב הוא `source_column.key1.key2...`. המהדר מחולל `json_extract_scalar(source_column, '$.key1.key2')` ב-SQL. (REQ-151)

**השפעת ניתוב:** עמודות נתיב משתמשות באופרטורי JSON של PostgreSQL (`->>`), הנתמכים באופן ילידי על ידי ניתוב PG ישיר. (REQ-152) עבור מקורות שאינם PostgreSQL (MySQL, SQL Server וכו'), שאילתות עם עמודות נתיב מנותבות אוטומטית דרך מנוע הפדרציה. (REQ-152) מוטציות אינן מושפעות מכיוון שעמודות נתיב הן שדות מחושבים לקריאה בלבד. (REQ-153)

### סוגי מיסוך

| סוג | שדות | תיאור |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (עמודות מחרוזת בלבד) |
| `constant` | `value` | החלפה מילולית (NULL, 0, MAX, MIN, מותאם אישית) |
| `truncate` | `precision` | DATE_TRUNC (עמודות תאריך/timestamp בלבד) |

## קשרים

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

### מימוש אוטומטי (Auto-Materialization)

הגדירו `materialize: true` על קשר כדי לחולל אוטומטית Materialized View עבור JOIN-ים חוצי-מקורות. (REQ-158) זה נמנע משאילתות פדרטיביות יקרות על ידי חישוב מראש של תוצאת ה-JOIN.

- רק קשרים חוצי-מקורות מחוללים MV-ים (JOIN-ים באותו מקור כבר מהירים) (REQ-159)
- ה-MV מתחיל מיושן (stale) ומתמלא על ידי לולאת הרענון ברקע (REQ-160)
- מוטציות לאחת מטבלאות המקור מסמנות את ה-MV כמיושן לרענון מחדש (REQ-543)
- `refresh_interval` ברירת המחדל היא 300 שניות (5 דקות) (REQ-543)

## תפקידים (Roles)

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

תפקידים עם `parent_role_id` יורשים יכולות וגישת דומיין מההורה. (REQ-215) ההיררכיה משוטחת (flattened) בעת ההפעלה. (REQ-215)

### יכולות (Capabilities)

| יכולת | תיאור |
| ----------- | ------------- |
| `source_registration` | רישום מקורות נתונים |
| `table_registration` | רישום טבלאות |
| `relationship_registration` | הגדרת קשרים |
| `security_config` | הגדרת RLS, מיסוך |
| `query_development` | ביצוע שאילתות |
| `full_results` | עקיפת מגבלות דגימה |
| `admin` | כל היכולות |

## כללי RLS

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Materialized Views

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

## Views (ערכות נתונים מחושבות מסודרות)

Views הן ערכות נתונים מחושבות המוגדרות ב-SQL עם ממשל מלא ברמת עמודה. (REQ-133) הן המנגנון המסודר להוספת אגרגציות, טרנספורמציות, ומדדים נגזרים לשכבה הסמנטית. (REQ-136)

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

| שדה | חובה | תיאור |
| ------- | ---------- | ------------- |
| `id` | כן | מזהה view ייחודי |
| `sql` | כן | הצהרת SQL SELECT שמגדירה את ה-view |
| `domain_id` | כן | דומיין לנראות סכמה |
| `materialize` | לא | `true` = רענון CTAS תקופתי, `false` = view פדרטיבי חי |
| `refresh_interval` | לא | שניות בין רענונים (ממוריאליז בלבד, ברירת מחדל 300) |
| `description` | לא | מופיע ב-SDL של GraphQL |
| `alias` | לא | דריסת שם GraphQL |
| `columns` | כן | הגדרות עמודה עם נראות, מיסוך, תיאורים |

### ממוריאליז לעומת חי

- **`materialize: true`**: Provisa יוצרת טבלה דרך CTAS ומרעננת אותה בלוח זמנים. (REQ-135) שאילתות מהירות יותר אך הנתונים עלולים להיות מיושנים עד `refresh_interval` שניות.
- **`materialize: false`**: Provisa יוצרת view פדרטיבי. (REQ-135) שאילתות תמיד מחזירות נתונים חיים אך עלולות להיות איטיות יותר עבור אגרגציות מורכבות.

Views עוברות באותו צינור ממשל כמו טבלאות — RLS, מיסוך, דגימה, ונראות מבוססת-תפקיד. (REQ-134) זה מבטיח שלא ניתן להוסיף סמנטיקה חדשה לפלטפורמה בלי פיקוח steward. (REQ-136)

### Views לקריאה בלבד

גם views עם `materialize: true` וגם עם `materialize: false` חושפים את טיפוס ה-GraphQL שלהם כלקריאה-בלבד. לא מחוללות מוטציות insert, upsert, update, או delete עבור יחסים מבוססי-`view_sql`. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## מטמון (Cache)

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### היררכיית מטמון

סדר פתרון TTL (הספציפי ביותר מנצח): **טבלה** > **מקור** > **ברירת מחדל גלובלית**. (REQ-544) הערך הלא-null הראשון משמש.

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

הגדרת `cache_enabled: false` על מקור משביתה מטמון לכל הטבלאות במקור זה, ללא קשר ל-TTL ברמת הטבלה. (REQ-544) מפתחות מטמון תמיד כוללים `role_id` + ערכי הקשר RLS לחלוקה מסודרת מבחינת אבטחה. (REQ-544)

## אימות (Authentication)

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

### סוגי ספק אימות

| ספק | מקרה שימוש | אימות token |
| ---------- | ---------- | ----------------- |
| `simple` | פיתוח/בדיקות מקומיים. משתמשים מוגדרים ב-YAML. | JWT חתום עם `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (כל השיטות). | `verify_id_token()` של SDK‏ `firebase-admin` |
| `keycloak` | Keycloak OIDC. תפקידי דייר + לקוח ממופים. | אימות JWT מבוסס JWKS |
| `oauth` | OIDC גנרי (Okta, Azure AD, Auth0, PingFederate). | JWKS מ-URL גילוי |

אישורי superuser (בלוק `superuser`) עובדים עם כל ספק ותמיד נפתרים לתפקיד admin עם כל היכולות. (REQ-125) משמש להגדרה ראשונית לפני שאימות חיצוני מוגדר.

### דוגמת תצורת אימות מלאה (מוערת)

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

## מוטציות Upsert

עבור טבלאות עם מפתח ראשי, Provisa מחוללת אוטומטית שדות מוטציה `upsert_<table>`. (REQ-212) אלה מתקמפלים ל-upsert בדיאלקט היעד — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` ב-PostgreSQL, `ON DUPLICATE KEY UPDATE` ב-MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

עמודות קונפליקט נגזרות ממטא-נתוני PK. (REQ-212) כל כללי נראות עמודה והרשאת כתיבה חלים.

## Distinct On

הארגומנט `distinct_on` בוחר את השורה הראשונה עבור כל ערך נבדל של העמודות שצוינו. (REQ-213) זמין בשדות שאילתת שורש.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

מתקמפל ל-`SELECT DISTINCT ON (region) ...` ב-PostgreSQL. (REQ-213) עבור דיאלקטים שאינם PG, נעשה שימוש בנפילה-חוזרת מבוססת window-function. (REQ-213)

## פריסטים לעמודות (Column Presets)

הזרקת ערכים אוטומטית לעמודות בהוספה/עדכון. (REQ-214) מוגדר לפי טבלה בתצורה.

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

| מקור | התנהגות |
| -------- | ---------- |
| `header` | מזריק ערך מכותרת בקשת HTTP בשם הנתון |
| `now` | מזריק `NOW()` (חותמת זמן נוכחית) |
| `literal` | מזריק ערך קבוע |

עמודות פריסט מוזרקות במהלך קימפול המוטציה לפני חילול SQL. (REQ-214) הן אינן נראות בטיפוס קלט המוטציה. (REQ-214)

## תפקידים בעלי ירושה

תפקידים יכולים לרשת יכולות וגישת דומיין מתפקיד הורה דרך `parent_role_id`. (REQ-215) ההיררכיה משוטחת בעת ההפעלה. (REQ-215)

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

ירושה מרובת-רמות נתמכת. (REQ-215) היכולות המפורשות ו-domain_access של תפקיד הבן ממוזגים עם אלה של ההורה. (REQ-215)

## טריגרים מתוזמנים

טריגרים מבוססי-cron שקוראים לכתובת webhook לפי לוח זמנים. (REQ-216) משתמש ב-APScheduler. (REQ-216)

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

משימות מתוזמנות מנוהלות דרך ממשק הניהול (מתג הפעלה/כיבוי) או דרך מוטציית הניהול `toggle_scheduled_task`. (REQ-216)

## פורמט OrderBy

OrderBy משתמש בפורמט `{column: direction}` עם enum כיוון בעל 6 ערכים: (REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| כיוון | SQL |
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

מיון לפי קשרים נתמך דרך אובייקטים מקוננים: (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observability

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

### מסנני טלמטריה [tool-verified]

Provisa מריצה שני נתיבי ייצוא OTLP עצמאיים: הקולקטור הפנימי שלכם ונקודת הקצה האופציונלית לתמיכה של Provisa. (REQ-545) לכל נתיב מסנן משלו. המסננים רצים בתוך `_FilteringExporter` עוטף לפני שה-spans עוזבים את התהליך — אובייקטי span המקוריים לעולם אינם משתנים. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — שולט במה שמגיע לקולקטור הפנימי שלכם.

| מפתח | טיפוס | ברירת מחדל | תיאור |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | מחליף מילוליים (literals) מחרוזתיים ומספריים ב-`db.statement` ב-`?` |
| `redact_attributes` | list[str] | `[]` | מפתחות תכונה (attribute) שנשמטים לגמרי מכל span |

**`support_telemetry_filter`** — שולט במה שמגיע לנקודת הקצה לתמיכה של Provisa. עריכת מילוליים SQL כברירת מחדל `true` בנתיב זה, מכיוון שנתוני שאילתה שייכים לכם. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| מפתח | טיפוס | ברירת מחדל | תיאור |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | מחליף מילוליים מחרוזתיים ומספריים ב-`db.statement` ב-`?` |
| `redact_attributes` | list[str] | `[]` | מפתחות תכונה שנשמטים לגמרי מכל span |

דוגמת `db.statement` ערוך — עם `redact_sql_literals: true`, תכונת span זו:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

הופכת ל:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### נקודת קצה לתמיכה [tool-verified]

`support_endpoint` (או משתנה סביבה `PROVISA_SUPPORT_OTLP_ENDPOINT`) מעביר טלמטריה לתמיכה של Provisa לצורכי אבחון. (REQ-548) כאשר לא מוגדר, שום נתון אינו עוזב את התשתית שלכם דרך נתיב זה. (REQ-548) מסנן התמיכה חל באופן עצמאי מהמסנן הפנימי — ניתן לערוך מילוליים SQL משני הייצואים בעודכם משתפים עם התמיכה את נתוני תזמון וטעויות ה-span. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### זיהוי פרוטוקול נקודת קצה [tool-verified]

Provisa בוחרת OTLP/HTTP או OTLP/gRPC לפי סכימת ה-URL של נקודת הקצה. (REQ-549) כתובות URL שמתחילות ב-`http://` או `https://` משתמשות ב-OTLP/HTTP, עם `/v1/traces`, `/v1/metrics`, ו-`/v1/logs` מתווספים אוטומטית. (REQ-549) כל סכימה אחרת משתמשת ב-OTLP/gRPC עם `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## מנוע פדרציה

הגדרת מנוע פדרציה אופציונלית. ברירת המחדל היא `duckdb` — ללא תצורה, בתוך-התהליך, ללא צורך בשירות חיצוני (REQ-989). בחרו מנוע אחר כשאתם זקוקים לקנה מידה MPP או רוצים להשתמש שוב ב-warehouse קיים.

עדיפות: משתנה סביבה `PROVISA_ENGINE` ← שדה תצורה `federation_engine` שמור בממשק הניהול ← `duckdb`. שינויים נכנסים לתוקף בהפעלה מחדש של השירות. [tool-verified: `engine.py` `build_engine`]

### סקירת מנועים [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| מפתח מנוע | תווית | דיאלקט | MPP | מנגנון קישור חיצוני | אימות |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | כן | קטלוגי Trino (קבוצת מחברים רחבה) | אישורי JDBC |
| `trino-byo` | Trino (bring-your-own) | Trino SQL | כן | זהה ל-`trino`; מתאם (coordinator) בלתי-מנוהל | אישורי JDBC |
| `pg` | PostgreSQL | PostgreSQL | לא | FDW / pg_duckdb | אישורי PostgreSQL |
| `duckdb` | DuckDB | DuckDB | לא | ATTACH ילידי-הרחבה | ללא (בתוך-התהליך) |
| `clickhouse` | ClickHouse (embedded) | ClickHouse | כן | מנועי טבלה S3 / IcebergS3 / DeltaLake | chdb (בתוך-התהליך, ללא אימות) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | כן | מנועי טבלה S3 / IcebergS3 / DeltaLake | אישורי ClickHouse |
| `snowflake` | Snowflake | Snowflake | כן | stage חיצוני + טבלה חיצונית | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | כן | טבלאות חיצוניות Unity Catalog דרך REST | `PROVISA_ENGINE_URL` (bearer token + `http_path`) |
| `bigquery` | BigQuery | BigQuery | כן | טבלאות חיצוניות / BigLake של BigQuery | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | כן | OneLake shortcuts ← OPENROWSET | Azure AD (`az login` או זהות מנוהלת) |
| `synapse` | Azure Synapse | T-SQL | כן | ADLS OPENROWSET / טבלאות חיצוניות | Azure AD |
| `sqlalchemy` | SQLAlchemy (כל RDB) | לפי-דיאלקט | לא | אין (land-only) | אישורים לפי-דיאלקט |

### רפרנס מנוע

#### trino / trino-byo

`trino` הוא ה-coordinator המנוהל של Provisa; `trino-byo` מתחבר ל-cluster Trino משלכם. שניהם משתמשים ב-Trino SQL ובעלי הטווח הרחב ביותר לסוגי מקורות.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

מאגר המימוש (materialization store) ברירת מחדל `TENANT_DATABASE_URL‏` (PostgreSQL).

#### pg

מפדרר דרך postgres_fdw (SQL/MED) והרחבות pg_duckdb. צומת יחיד; ללא MPP. הכי טוב כשהנתונים שלכם כבר חיים ב-PostgreSQL ואתם רוצים לצרף כמה מקורות מרוחקים.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

#### duckdb

בתוך-התהליך; ללא שירות חיצוני. המנוע ברירת המחדל (REQ-989). `PROVISA_DATA_DIR` שולט היכן חי המאגר המובנה (`~/.provisa` כברירת מחדל).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

מאגר המימוש ברירת מחדל `~/.provisa/materialize.duckdb` — המנוע היחיד עם מאגר ברירת מחדל שאינו PostgreSQL.

#### clickhouse (embedded) / clickhouse-server

`clickhouse` משתמש ב-chdb (בתוך-התהליך). `clickhouse-server` מתחבר למופע ClickHouse חיצוני או ClickHouse Cloud. שניהם קוראים Delta Lake, Iceberg, ו-Hudi ישירות דרך מנועי טבלה ילידיים של ClickHouse.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

#### snowflake

מנוע-כ-warehouse: Snowflake מריץ את השאילתות; Provisa דוחפת נתוני מקור דרך stages חיצוניים.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

#### databricks

טבלאות חיצוניות של Unity Catalog מגשרות מקורות מנוהלים-Provisa לתוך Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

#### bigquery

טבלאות BigQuery חיצוניות ו-BigLake. הפרויקט מגיע מה-URL או `GOOGLE_CLOUD_PROJECT`; אימות דרך מפתח service-account.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

#### fabric / synapse

שניהם משתמשים ב-T-SQL על גבי TDS עם אימות Azure AD (`az login` או זהות מנוהלת). השמיטו `PROVISA_ENGINE_URL` כדי לקרוא פרטי חיבור ממשתני סביבה במקום.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

#### sqlalchemy

מנוע RDBMS גנרי land-only (ללא פדרציה למקורות חיצוניים). לשימוש לפריסות warehouse-יחיד או בדיקות.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

### מאגר המימוש (Materialization store)

כאשר מקור אינו יכול להתחבר (attach) חי (אין מחבר ATTACH למנוע הנבחר), הוא נוחת (lands) לתוך מאגר המימוש של המנוע. סדר פתרון: `PROVISA_MATERIALIZE_URL` מפורש ← ברירת המחדל המוצהרת של המנוע ← שגיאה קשה (ללא נפילה-חוזרת שקטה). [tool-verified: `engine.py` `materialize_store`]

DuckDB מצהיר על קובצו המובנה (`~/.provisa/materialize.duckdb`) כברירת מחדל שלו. כל שאר המנועים ברירת מחדל `TENANT_DATABASE_URL‏` (PostgreSQL). דרסו כל מנוע עם `PROVISA_MATERIALIZE_URL`.

### רמזי פדרציה לפי-מקור

פרמטרי חיבור מורחבים שהשדות הרגילים host/port/user/password אינם יכולים לשאת נכנסים ל-`federation_hints` על המקור. ראו את רפרנס סוג המקור לעיל למפתחות hint לפי-סוג. דוגמה מאוחדת:

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

עבור מקורות Google Cloud, הגדירו `GOOGLE_APPLICATION_CREDENTIALS` לנתיב קובץ מפתח ה-service-account שלכם. עבור Fabric ו-Synapse, התאמתו עם `az login` (פיתוח) או זהות מנוהלת (ייצור) — המנוע קורא אישורים דרך `DefaultAzureCredential` של `azure-identity`.

## משתני סביבה

| משתנה | ברירת מחדל | תיאור |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | נתיב קובץ תצורה |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI מאגר מישור-הבקרה (SQLAlchemy async); מקבל `sqlite+aiosqlite://…` / `duckdb://…` עבור מאגר שולחן העבודה המובנה (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI מרשם הפלטפורמה (ספריית דיירים, מרשם מנועים); נדרש בעת ההפעלה, ללא נפילה-חוזרת (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` משתמש ב-fakeredis מובנה במקום שרת Redis — ללא Docker (REQ-829) |
| `PG_HOST` | `localhost` | מארח PostgreSQL |
| `PG_PORT` | `5432` | פורט PostgreSQL |
| `PG_DATABASE` | `provisa` | מסד נתונים PostgreSQL |
| `PG_USER` | `provisa` | משתמש PostgreSQL |
| `PG_PASSWORD` | `provisa` | סיסמת PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | מפתח מנוע פדרציה (REQ-989) |
| `PROVISA_ENGINE_URL` | — | URL חיבור למנועים מונעי-URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | דריסת DSN של מאגר המימוש (ברירת מחדל לברירת המחדל המוצהרת של המנוע) |
| `PROVISA_DATA_DIR` | `~/.provisa` | תיקיית נתונים למאגר DuckDB המובנה (REQ-989) |
| `TRINO_HOST` | `localhost` | מארח coordinator של Trino |
| `TRINO_PORT` | `8080` | פורט HTTP של coordinator‏ Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | נתיב ל-JSON מפתח service-account של GCP (מנוע/מקור BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | פרויקט GCP ברירת מחדל (BigQuery; נדרס על ידי URL) |
| `FABRIC_SQL_SERVER` | — | נקודת קצה SQL של Fabric Warehouse (חלופה ל-`PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | שם מסד נתונים Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | נקודת קצה Synapse serverless SQL |
| `SYNAPSE_DATABASE` | — | שם מסד נתונים Synapse |
| `REDIS_URL` | — | URL חיבור Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | מגבלת דגימה ברירת מחדל |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | תקרת שורות כאשר שאילתה אינה מספקת `LIMIT` מפורש |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | תקציב ניסיון-חוזר לקריאה מדרג-1 בשניות; נסיגה אקספוננציאלית עם jitter מלא (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | פורט ה-proxy‏ Zaychik Flight SQL |
| `FLIGHT_PORT` | `8815` | פורט שרת Provisa Arrow Flight |
| `GRPC_PORT` | `50051` | פורט שרת Provisa Protobuf gRPC |
| `PROVISA_REDIRECT_ENABLED` | `false` | הפעלת הפניית סף בצד השרת |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | סף ספירת שורות ברירת מחדל |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | פורמט הפניה ברירת מחדל |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 bucket לתוצאות מופנות |
| `PROVISA_REDIRECT_ENDPOINT` | — | כתובת URL של נקודת קצה תואמת-S3 |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | מפתח גישה S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | מפתח סודי S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL של Presigned URL (שניות) |
| `ANTHROPIC_API_KEY` | — | מפתח API של Claude (גילוי) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | דורס את `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | דורס את `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | דורס את `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | דורס את `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | עיכוב flush של batch span processor |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | דורס את `observability.support_endpoint` |
</content>
