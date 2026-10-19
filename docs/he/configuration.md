# רפרנס תצורה {: #configuration-reference }

Provisa מוגדרת דרך קובץ YAML (ברירת מחדל: `config/provisa.yaml`).

## הכללות (Includes) {: #includes }

פיצול הגדרות על פני מספר קבצים באמצעות `includes:`. הקובץ המכליל נוקב נתיבי מקטע (fragment) תחת מפתח זה; Provisa ממזגת אותם לפני האימות, ומפיקה את אותה תוצאה כמו כתיבת הכל בקובץ אחד.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

קובץ עוטף המכיל רק `includes:` תקף. `load_control_plane` קורא דרך ההכללות, כך שהמקטע `control_plane:` נלקח מאיזה קובץ מוכלל שקובע אותו.

**כללי מיזוג**

- נתיבים נפתרים ביחס לקובץ המכליל. נתיבים מוחלטים משמשים כלשונם.
- מקטעי רשימה (`sources`, `tables`, `domains`, `relationships`, `roles`, …) מצטרפים — רשומות המקטע (fragment) עוקבות אחרי רשומות הקובץ המכליל.
- מפתח סקלרי או מיפוי שהקובץ המכליל אינו קובע נלקח מהמקטע.
- מפתח ששני הקבצים קובעים לערכים שונים הוא קונפליקט; הטעינה נכשלת, ונוקבת את המפתח.
- ערכים זהים בשני הקבצים אינם קונפליקט.
- הכללות מקוננות. קובץ המכליל את עצמו — ישירות או דרך מקטע אחר — נדחה.
- `includes` נצרך בזמן הטעינה ולעולם אינו מופיע בהגדרות המאומתות.

## מקורות (Sources) {: #sources }

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

כל המקורות חולקים קבוצת שדות משותפת.

| שדה | ברירת מחדל | הערות |
| ------- | --------- | ------- |
| `id` | חובה | אלפאנומרי, מקפים, קווים תחתונים |
| `type` | חובה | ראו הטבלה למטה |
| `host` | `""` | שם מארח או IP |
| `port` | `0` | `0` משמעו שכל מחבר מספק ברירת מחדל משלו — אין מפת פורט-ברירת-מחדל מרכזית |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | תומך בהפניות אישורים `${env:VAR}` ו-`${secret:NAME}` — ראו [סודות](secrets.md) |
| `path` | `null` | נתיב קובץ או URI למקורות מבוססי-קובץ |
| `base_url` | `null` | כתובת URL בסיסית למקורות API |
| `pool_min` / `pool_max` | `1` / `5` | גבולות pool החיבורים |
| `cache_enabled` | `true` | הפעלה/כיבוי מטמון לכל הטבלאות במקור זה |
| `cache_ttl` | `null` | שניות; `null` יורש את ברירת המחדל הגלובלית; `-1` אינו פג לעולם |
| `federation_hints` | `{}` | פרמטרים מורחבים לפי-מחבר (dict[str,str]); ראו רפרנס סוגים למטה. |
| `mapping` | `{}` | DSL מיפוי עבור redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | הגבלת מקור זה למזהי דומיין מסוימים; ריק = ללא הגבלה |
| `description` | `""` | |

### סוגי מקורות נתמכים {: #supported-source-types }

| סוג | סגנון חיבור | הערות |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | pool של Asyncpg; PgBouncer מרצון דרך `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (קובץ DB) | הרחבת הקהילה firebird של DuckDB |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | משתמש שוב בדרייבר/דיאלקט PostgreSQL |
| `yugabytedb` | host/port | משתמש שוב בדרייבר/דיאלקט PostgreSQL |
| `greenplum` | host/port | משתמש שוב בדרייבר/דיאלקט PostgreSQL |
| `tidb` | host/port | משתמש שוב בדרייבר/דיאלקט MySQL |
| **Cloud DW** | | |
| `snowflake` | host/port + `federation_hints` | `account` נדרש ב-hints |
| `bigquery` | `federation_hints` | `project` נדרש; אימות דרך `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` נדרש ב-hints |
| `fabric` | משתני סביבה או `PROVISA_ENGINE_URL` | T-SQL על גבי TDS, אימות Azure AD |
| `synapse` | משתני סביבה או `PROVISA_ENGINE_URL` | T-SQL על גבי TDS, אימות Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | ה-hint `secure` מפעיל TLS; ברירת מחדל לפורט 8123/8443 |
| `elasticsearch` | host/port + `mapping` DSL | |
| `pinot` | host/port | נקודת קצה REST של הבקר |
| `druid` | host/port | נקודת קצה Avatica של ה-broker |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI טבלה) | `delta_scan` של DuckDB; גישה לאחסון אובייקטים דרך `federation_hints` |
| `iceberg` | `path` (URI טבלה) | `iceberg_scan` של DuckDB; גישה לאחסון אובייקטים דרך `federation_hints` |
| `hudi` | `path` (URI טבלה) | מנוע Hudi של ClickHouse, zero-copy |
| `hive` | host/port (metastore) + `mapping.storage` | backend אחסון ב-`mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + מפתחות S3 ב-`mapping` | סוג נבדל; תמיד אחסון S3 |
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
| `airport` | `base_url` (מיקום Flight) | הרחבת airport של DuckDB |
| `ingest` | receiver דחיפה | שירותים חיצוניים שולחים POST של אירועי JSON |
| **SaaS** | | |
| `sharepoint` | `base_url` או `host` + `mapping` | אימות דרך `mapping.auth_type` |
| `splunk` | `host`/`port` או `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | מודל `GovDataSource` נפרד; ראו §GovData למטה |
| **Data Quality** | | |
| `soda` | host/port המכוונים ל-pgwire של Provisa | דורש את התוסף `soda`; Elastic License 2.0, אירוח עצמי בלבד |
| `great_expectations` | host/port המכוונים ל-pgwire של Provisa | דורש את התוסף `gx`; Apache 2.0 |

### רפרנס סוג מקור {: #source-type-reference }

לכל סוג שצריך תצורה לא מובנת מאליה יש רשומה קצרה למטה. סוגי RDBMS (postgresql, mysql וכו') משתמשים רק בשדות המשותפים שלמעלה ולא צריכים מקטע משלהם.

#### GovData

מקורות `govdata` משתמשים במודל עליון משלהם, `GovDataSource`, במקום ה-`Source` הכללי. הגישה מחולקת לפי קיבוץ נושאים.

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

כל subject ממופה לסכמה אחת או יותר של GovData. הגדרת מקור `govdata` עם subject חושפת אוטומטית את כל הסכמות עבור אותו subject.

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

הסכמות `ref` ו-`geo` תמיד כלולות כסכמות מקשרות. אי אפשר להגדיר אותן והן לא מופיעות ברשימה שלמעלה. השתמשו בנושא `ALL` כדי להעניק גישה לכל הסכמות.

#### Kafka

השורה `kafka` ב-`sources:` רק רושמת את המקור. ה-`details()` של המחבר שלה מחזיר `{}`; התצורה האמיתית נמצאת בבלוק העליון `kafka_sources[]`. Kafka הוא תמיד VIRTUAL_SOURCE: הוא עובר דרך המנוע ואין לו pool ישיר.

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

**חלון זמן (Time Window)** — `default_window` מגביל כל שאילתה לתקופת זמן אחרונה, ומונע קריאות בלתי-מוגבלות מנושאים בנפח גבוה. פורמט: `1h`, `30m`, `7d`, `60s`. ברירת מחדל `1h`. מוזרק אוטומטית כ-`WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. לקוחות יכולים לדרוס עם פילטר `_timestamp` משלהם בארגומנט `where` של GraphQL.

**Discriminator** — תצורות topic מרובות יכולות להצביע על אותו topic פיזי של Kafka עם ערכי `discriminator` שונים, ולהפיק טיפוסי GraphQL נפרדים. ה-discriminator מוזרק אוטומטית כסעיף WHERE.

**מקור סכמה (Schema Source)**

| ערך | התנהגות |
| ------- | ---------- |
| `registry` | שליפת סכמה מ-Confluent Schema Registry |
| `manual` | הגדרת עמודות inline בתצורה (ללא צורך ב-Schema Registry) |
| `sample` | גילוי אוטומטי מהודעות דוגמה |

#### Snowflake

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

#### Databricks

`http_path` ב-`federation_hints` הוא חובה. `password` נושא את טוקן הגישה האישי. `catalog` אופציונלי ונכנס ל-SQL או לרמזים; השדה `database` לא מחזיק אותו.

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

`project` ב-`federation_hints` נדרש. האימות משתמש ב-`GOOGLE_APPLICATION_CREDENTIALS` (נתיב לקובץ מפתח service-account) או Application Default Credentials בסביבת המנוע.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

שניהם משתמשים ב-T-SQL על גבי TDS עם אימות Azure AD. התחברו עם `az login` (פיתוח) או עם זהות מנוהלת (ייצור); המנוע קורא את פרטי ההתחברות דרך `DefaultAzureCredential` של `azure-identity`. פרטי החיבור מגיעים ממשתני סביבה, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) או `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), או מ-`PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` ב-`federation_hints` מפעיל TLS על ממשק ה-HTTP. הפורט כברירת מחדל `8123` (רגיל) או `8443` (כאשר `secure: "true"`). `schema` ב-`federation_hints` דורס את הסכמה המרוחקת.

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

#### Hive / Hive S3

`host` ו-`port` מצביעים על ה-metastore של Hive מסוג Thrift (פורט ברירת מחדל 9083). עבור `hive`, הגדירו `mapping["storage"]` כדי לבחור את ה-backend של אחסון האובייקטים. מפתח חובה חסר נכשל בקול רם, בלי ערך חלופי.

`hive_s3` הוא סוג נבדל שתמיד מצהיר על אחסון S3; אין צורך ב-`mapping.storage`.

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

#### Redis

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

#### Google Sheets

`spreadsheet_id` ב-`federation_hints` נדרש. האימות משתמש ב-SECRET מסוג `gsheet` של DuckDB המסופק בזמן ה-attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### מקורות קובץ (csv / parquet / sqlite / files) {: #file-sources-csv-parquet-sqlite-files }

`path` נדרש. `files` סורק תיקייה עבור קבצי CSV, Parquet, XLSX, ו-JSON, וחושף כל אחד כטבלה. כל המקורות מבוססי-הקובץ הם VIRTUAL (מנותבים דרך המנוע; ללא pool ישיר).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

אי אפשר להוסיף **ערכות נתונים של Kaggle** דרך הקובץ הזה, כי הן צריכות טוקן תקף ואת בורר ערכות הנתונים שבטופס המקורות (Sources → Subscriptions → Kaggle). מקור Kaggle שיוצא ל-YAML מופיע כ-`type: files` עם `kaggle_owner` ו-`kaggle_ref` ב-`federation_hints`. הורדה חוזרת מ-Kaggle דורשת את המוטציה `refreshKaggleSource` או את הרענון בממשק; עריכת ה-YAML לא עושה את זה. ראו [ערכות נתונים של Kaggle](sources.md#kaggle-datasets) בהפניה לסוגי המקורות.

#### מקורות API / מרוחקים {: #api-remote-sources }

**openapi** — הגדירו `base_url` לכתובת ה-URL הבסיסית של OpenAPI. גילוי הסכמה קורא את מפרט ה-OpenAPI בעת ההפעלה.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — הגדירו `base_url`. מפתחות `mapping` אופציונליים: `headers` (dict של כותרות סטטיות), `forward_client_headers` (bool), `timeout_seconds` (int).

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

**airport** — `base_url` הוא מיקום שרת ה-Arrow Flight. הרחבת airport של DuckDB.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — השתמשו ב-`host`, `port`, `path`, ו-`federation_hints`.

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

#### בודקי איכות נתונים (soda / great_expectations) {: #data-quality-checkers-soda-great_expectations }

מקור בודק מצביע על נקודת הקצה pgwire של Provisa עצמו, כך שדרייבר postgres יחיד סורק את התצוגה המפודררת של טבלה שמאחוריה Snowflake או Iceberg. את זהות הסריקה מצהירים; היא לעולם לא עוברת בירושה. על החיבור הזה חלה המדיניות, וקבוצת שורות מסוננת לא אמורה להעביר בדיקה בשקט. מפתחות החיבור מגיעים מ-`mapping`: `host`, `port`, `database`, `user`, `password`.

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

כל טבלת תוצאות נושאת את `dq_contract`, ה-YAML של חוזה Soda או ה-JSON של suite של Great Expectations, מילה במילה. העמודות, ה-watermark והקידומים נגזרים ממנו; איך בדיוק, ראו ב-[בודקי איכות נתונים](sources.md#data-quality-checkers).

**בחירה בזמן ההתקנה.** הבודק לא מקושר פנימה: הסריקה רצה במפרש-בן, והספרייה מותקנת רק כשמפעיל מציין אותה. כל נתיב התקנה (`install.sh`, `packaging/linux/first-launch.sh`, ואשף ה-macOS דרך `PROVISA_DQ_CHECKER`) כותב את הבחירה ל-`~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` קורא מפתח זה ומייצא `PROVISA_EXTRAS`, אותו `docker-compose.app.yml` מעביר כארגומנט build ל-`ARG PROVISA_EXTRAS` של ה-`Dockerfile`:

| `dq_checker` | `PROVISA_EXTRAS` (שכבת Docker) | התקנת venv מקומית |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

התקנת מערך הדגמה מעלה את `none` ל-`gx` ומודיעה על כך, כיוון שתצורת ההדגמה רושמת סוויטת Great Expectations על `pet_store.pets` וכרטיס האיכות שלה לא היה לו מה להציג אחרת. ציון `soda` בשם משאיר `soda`.

אם מגיעים להדגמה דרך pip ולא דרך תוכנת התקנה, מדלגים על שלב האשף הזה, ולכן התוספת `demo` כוללת את אותו בודק: `provisa run --demo` צריך את `pip install 'provisa[embedded,demo]'` כדי שהסריקה שלו תרוץ. בלעדיו, הסריקה מדווחת `data-quality checker 'great_expectations' is not installed` ומציינת את פקודת ההתקנה.

כל ערך אחר עוצר את המשגר, כך שהוא לעולם לא עולה בלי הבודק שהמפעיל ביקש. התוספת `soda` מושכת את `soda-postgres`, ו-`gx` מושכת את `great-expectations[postgresql]`. Soda Core נמצא תחת Elastic License 2.0: `config/capabilities.yaml` מסמן את האפשרות כ-`cloud_eligible: false`, והמישור המתארח דוחה אותה.

## דומיינים {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## שיוֹם (Naming) {: #naming }

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### מוסכמת שיוֹם (Naming Convention) {: #naming-convention }

רשות השיוֹם היא מקור האמת היחיד לשמות הפונים ללקוח; שמות עמודות ה-backend הפיזיים לעולם אינם נחשפים ללקוחות. כל שפת שאילתה גוזרת את שם העמודה מ-`column.alias` שלה אם הוגדר, אחרת משם העמודה הפיזי דרך המוסכמה המוגדרת שלו.

מוסכמת ה-GraphQL היא אחת משלושה enum-ים מוגדרים מראש. מחרוזות free-form ישנות (`none`, `snake_case`, `camelCase`, `PascalCase`) הוצאו משימוש.

| מוגדר-מראש (Preset) | ברירת מחדל | שמות טיפוס | שמות שדה | שמות מוטציה |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | כן | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

מוסכמת ברירת המחדל של GraphQL היא `apollo_graphql`, שמפיקה שמות שדה ומוטציה ב-camelCase. מוסכמת ה-SQL נפרדת, עם ברירת מחדל `snake_case`, מוחלת דרך `apply_sql_name()`; מוסכמת GraphQL מוחלת דרך `apply_gql_name()`, ושם ה-CQL נגזר משם ה-GraphQL.

`domain_prefix: bool` הוא אפשרות אורתוגונלית שחלה בלי קשר למוגדר-מראש שנבחר.

`column.alias` מפורש הוא השם הקנוני: SQL משתמש בו כלשונו ללא הפעלת מוסכמה, GraphQL מחיל עליו את המוסכמה שלו, ו-CQL נגזר משם ה-GraphQL.

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

### קידומת דומיין (Domain Prefix) {: #domain-prefix }

כאשר `domain_prefix: true`, כל שמות שדה וטיפוס GraphQL מקבלים קידומת מזהה הדומיין באמצעות מפריד קו-תחתון כפול:

| טבלה | דומיין | שם שדה |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

זה מונע התנגשויות שמות כאשר לדומיינים שונים יש טבלאות באותו שם, והופך שאילתות למתועדות-מאליהן.

### כללי שיוֹם {: #naming-rules }

כללי regex שמוחלים על שמות טבלה בעת חילול שמות שדה GraphQL. מוחלים לפי סדר לפני פתרון ייחודיות.

## טבלאות {: #tables }

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

כברירת מחדל אין לטבלה שום הרשאת כתיבה, ולכן אף תפקיד אינו יכול לכתוב אליה. הרשאה נותנת לתפקיד אחד פעולה אחת (`insert`, `update` או `delete`) על הטבלה. טבלה מכילה לכל היותר הרשאה אחת לכל תפקיד ופעולה.

| שדה ההרשאה | משמעות |
| ----------- | ------- |
| `columns` | העמודות ש-`INSERT` רשאי לרשום או ש-`UPDATE` רשאי לקבוע. הרשאת `delete` אינה נוקבת באף אחת. |
| `rows` | השורות ש-`UPDATE` או `DELETE` רשאי לגעת בהן: פרדיקט SQL על עמודות הטבלה. להרשאת `insert` אין כזה. |
| `check` | מה שכל שורה ש-`INSERT` כותב, או ש-`UPDATE` משאיר, חייבת לקיים: פרדיקט SQL. להרשאת `delete` אין כזה. |
| `presets` | עמודות שההרשאה קובעת בעצמה. ההצהרה אינה רשאית לספק אותן, ולכן עמודת preset אינה מופיעה גם ב-`columns`. |

אם משמיטים את `rows` או את `check`, ההרשאה לוקחת את מסנן השורות של התפקיד לקריאה בטבלה: תפקיד כותב את השורות שהוא קורא, אלא אם ההרשאה קובעת אחרת. `"TRUE"` פירושו כל שורה. המפתחות הישנים `writable_by` של עמודה ו-`column_presets` של טבלה נדחים בשמם; ההגדרות לא ייטענו עד שיוחלפו ב-`write_grants`.

כל כתיבה בכל משטח מאושרת בסדר הזה: לתפקיד יש את הזכות `write`; לתפקיד יש הרשאה לפעולה על הטבלה; כל עמודה שההצהרה כותבת מופיעה בהרשאה; ה-presets של ההרשאה מתווספים להצהרה; הפרדיקט `rows` מצורף ב-AND ל-`WHERE` של `UPDATE` או `DELETE`; וה-`check` מוערך על השורות החדשות לפני שנשלח משהו למקור. הצהרה ש-`check` אינו יכול להכריע בה נדחית. עמודות ההרשאה קובעות גם אילו שדות מוטציה GraphQL מציע לתפקיד: שדה אחד לכל פעולה שיש לו הרשאה עבורה, בדיוק עם העמודות האלה.

חלק מההצהרות דורשות הרשאה רחבה יותר. `TRUNCATE` דורש הרשאת `delete` על כל שורה. `MERGE` דורש הרשאת `insert` והרשאת `update`, שתיהן על כל שורה. `INSERT ... ON CONFLICT DO UPDATE` דורש הרשאת `update` על כל שורה ללא presets. `COPY ... FROM STDIN` דורש הרשאת `insert` ללא presets.

הרשאות עוברות בירושה. תפקיד צאצא לוקח, לכל טבלה ופעולה, את ההרשאה של התפקיד הקרוב ביותר בשרשרת שלו שמחזיק בה, כשהשלו קודמת. תפקיד-על (meta role) מחזיק את הקיפול של הרשאות חבריו: איחוד העמודות שלהם, ה-OR של הפרדיקטים `rows` ו-`check` שלהם, ורק ה-presets שכל החברים קובעים באופן זהה.

בממשק הניהול, הסעיף **כתיבות** של טבלה מפרט את ההרשאות ועורך אותן יחד.

### כינויים (Aliases) {: #aliases }

כינויי טבלה ועמודה דורסים את שם ה-GraphQL ברירת המחדל. שימושי עבור:

- שינוי שם למסדי נתונים חידתיים (לדוגמה, `tbl_cust_seg` ← `customer_segments`)
- הימנעות מקיצורים בשכבת ה-API
- יצירת אוצר מילים נקי, ספציפי-לדומיין

### תיאורים {: #descriptions }

תיאורי טבלה ועמודה נכללים ב-SDL של GraphQL המחולל. הם מופיעים בסייר התיעוד של GraphiQL ובשאילתות introspection. הגדירו אותם בתצורת YAML או דרך ממשק הניהול.

### נתיב (חילוץ JSON מחושב) {: #path-computed-json-extraction }

עמודות יכולות לחלץ ערכים מעמודת מקור JSON/JSONB באמצעות `path` בסימון נקודה. שימושי לנתונים חצי-מובנים בהודעות Kafka, מסמכי MongoDB, או עמודות JSONB של PostgreSQL.

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

פורמט הנתיב הוא `source_column.key1.key2...`. המהדר מחולל `json_extract_scalar(source_column, '$.key1.key2')` ב-SQL.

**השפעת ניתוב:** עמודות נתיב משתמשות באופרטורי JSON של PostgreSQL (`->>`), הנתמכים באופן ילידי על ידי ניתוב PG ישיר. עבור מקורות שאינם PostgreSQL (MySQL, SQL Server וכו'), שאילתות עם עמודות נתיב מנותבות אוטומטית דרך מנוע הפדרציה. מוטציות אינן מושפעות מכיוון שעמודות נתיב הן שדות מחושבים לקריאה בלבד.

### סוגי מיסוך {: #masking-types }

| סוג | שדות | תיאור |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (עמודות מחרוזת בלבד) |
| `constant` | `value` | החלפה מילולית (NULL, 0, MAX, MIN, מותאם אישית) |
| `truncate` | `precision` | DATE_TRUNC (עמודות תאריך/timestamp בלבד) |

## קשרים {: #relationships }

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

### מימוש אוטומטי (Auto-Materialization) {: #auto-materialization }

הגדירו `materialize: true` על קשר כדי לחולל אוטומטית Materialized View עבור JOIN-ים חוצי-מקורות. זה נמנע משאילתות פדרטיביות יקרות על ידי חישוב מראש של תוצאת ה-JOIN.

- רק קשרים חוצי-מקורות מחוללים MV-ים (JOIN-ים באותו מקור כבר מהירים)
- בקשר מגובה junction ה-MV מכסה את החצייה בשתי קפיצות — קפיצת המקור, קפיצת ה-junction, המבחין ועמודות ה-junction עצמה כמאפייני קשת. ה-junction נחשבת רגל, ולכן קשת היא חוצת-מקורות כאשר אחת משלוש הטבלאות יושבת במקור אחר
- ה-MV מתחיל מיושן (stale) ומתמלא על ידי לולאת הרענון ברקע
- מוטציות לאחת מטבלאות המקור מסמנות את ה-MV כמיושן לרענון מחדש
- `refresh_interval` ברירת המחדל היא 300 שניות (5 דקות)

## תפקידים (Roles) {: #roles }

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

תפקידים עם `parent_role_id` יורשים מההורה יכולות, גישת דומיין, הענקות של עמודות ואובייקטים וכללי RLS, כשכלל ה-RLS של הבן עצמו לטבלה קודם. השרשרת משוטחת בעת ההפעלה.

### יכולות (Capabilities) {: #capabilities }

| יכולת | תיאור |
| ----------- | ------------- |
| `source_registration` | רישום מקורות נתונים |
| `table_registration` | רישום טבלאות |
| `relationship_registration` | הגדרת קשרים |
| `security_config` | הגדרת RLS, מיסוך |
| `query_development` | ביצוע שאילתות |
| `full_results` | עקיפת מגבלות דגימה |
| `admin` | כל היכולות |

## כללי RLS {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
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
      # Add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Views (ערכות נתונים מחושבות מסודרות) {: #views-governed-computed-datasets }

Views הן ערכות נתונים מחושבות המוגדרות ב-SQL עם ממשל מלא ברמת עמודה. הן המנגנון המסודר להוספת אגרגציות, טרנספורמציות, ומדדים נגזרים למודל.

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

### ממוריאליז לעומת חי {: #materialized-vs-live }

- **`materialize: true`**: Provisa יוצרת טבלה דרך CTAS ומרעננת אותה בלוח זמנים. שאילתות מהירות יותר אך הנתונים עלולים להיות מיושנים עד `refresh_interval` שניות.
- **`materialize: false`**: Provisa יוצרת view פדרטיבי. שאילתות תמיד מחזירות נתונים חיים אך עלולות להיות איטיות יותר עבור אגרגציות מורכבות.

תצוגות עוברות דרך אותו צינור ממשל כמו טבלאות: RLS, מיסוך, דגימה ונראות לפי תפקיד. כך שום דבר לא יכול להוסיף סמנטיקה חדשה לפלטפורמה בלי פיקוח של Steward.

### תצוגות עם היסטוריה (מסע בזמן) {: #views-with-history-time-travel }

תצוגה ממומשת יכולה לשמור כל גרסה של השורות שלה ולהיקרא כפי שהייתה ברגע בעבר. היא מצהירה כיצד היא שומרת אותן ולאיזה מפתח שייכת גרסה:

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
| שדה | חובה | תיאור |
| ------- | ---------- | ------------- |
| `bitemporal_mode` | כן | `snapshot` מוסיף את התוצאה כולה בכל רענון; `delta` מוסיף רק את מה שהשתנה |
| `bitemporal_key` | כן | העמודות שגרסה שייכת אליהן |
| `bitemporal_source_time` | לא | עמודה המחזיקה את הרגע שבו כל שורה השתנתה במקור: השעון שלפיו קורא מסע בזמן |

**בחירת השעון.** עם `bitemporal_source_time`, הגרסאות מסודרות ונקראות לפי הזמן של המקור עצמו: "נכון ליום שלישי שעבר" פירושו יום שלישי שעבר במקור. בלעדיו, הן מסודרות ונקראות לפי הזמן שבו Provisa הנחית כל שינוי, וזה הולך לפי קצב הרענון ולא לפי הרגע שבו השורה השתנתה במקור. שתי האפשרויות לגיטימיות. זמן הטעינה מתאים לקצב רענון תכוף, לנתונים שהזמן שלהם במקור לא חשוב, ולקריאות שאמורות לעסוק בזמן הטעינה, כמו ביקורת על מה ש-Provisa החזיק ומתי.

גרסה תקפה מהרגע שלה עד הרגע של הגרסה הבאה. למחיקה אין זמן שינוי במקור, והיא נכנסת לתוקף כש-Provisa רשמה אותה.

**קריאה.** קריאה רגילה מחזירה את הגרסה של כל מפתח שתקפה עכשיו. הכותרת `X-Provisa-As-Of` קוראת ברגע מסוים או לאורך טווח, הנתון כמרווח ISO 8601 בצורת `start/end`:

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```
רגע מחזיר את הגרסה של כל מפתח שהייתה תקפה אז. טווח מחזיר כל גרסה שהייתה תקפה בנקודה כלשהי בתוכו, שורה לכל גרסה.

**כפי שנרשם.** תיקון שנרשם מאוחר יותר משנה את מה שתצוגה אומרת על העבר: הגרסה שבתוקף ב-30 בספטמבר היא הגרסה המתוקנת מרגע שהתיקון נרשם. כדי לקרוא את התצוגה כפי ש-Provisa רשם אותה בזמן מסוים, לפני כל תיקון מאוחר יותר, הוסיפו `X-Provisa-As-Recorded`, זמן ISO 8601 אחד:

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

כל גרסה שנרשמה אחרי הזמן הזה נשארת בחוץ, וה-as-of בוחר אז את הגרסה שהייתה בתוקף בזמן שלו. עם `X-Provisa-As-Recorded` לבדו, הקריאה מחזירה את הגרסה של כל מפתח שהייתה בתוקף לפי מה שנרשם אז. הוא מקבל נקודת זמן אחת, לא טווח; טווח או ערך פגום נדחים עם HTTP 400 (`data.invalid_as_recorded`).

שני הזמנים מגיעים ל-Provisa גם בלי הכותרות. זמן ה-as-of הוא הארגומנט `as_of` של `resolve` (GraphQL, gRPC, MCP), פרמטר השאילתה `as_of` ב-REST והאפשרות `as_of` של כרטיס Arrow Flight, וטווח שמועבר ל-`resolve` מחזיר את ה-`versions` של השורה במקום את ה-`row` שלה. זמן ה-as-recorded הוא הארגומנט `as_recorded` של `resolve`, פרמטר השאילתה `as_recorded` ב-REST והאפשרות `as_recorded` של כרטיס Arrow Flight. רשומת הביקורת של כל הצהרה שומרת את שני הזמנים שבהם היא קראה, `as_of` ו-`as_recorded`. הרשאות, מסכות וכללי שורה חלים כמו בכל קריאה. ערך as-of פגום, או טווח שסופו אינו אחרי תחילתו, נדחים עם HTTP 400 (`data.invalid_as_of`).

כל תצוגה עם היסטוריה מחזירה שלוש עמודות נוספות, המוענקות כמו עמודת זמן המקור שלה (אחרת עמודת המפתח הראשונה):

| עמודה | משמעות |
| ------- | ------- |
| `source_from` | מתי גרסה זו נכנסה לתוקף: זמן המקור שלה, או זמן הטעינה כשאין כזה |
| `source_to` | מתי נכנסה לתוקף הגרסה הבאה; ריק כל עוד גרסה זו תקפה |
| `recorded_at` | מתי Provisa רשמה גרסה זו |

**זמן מקור מלכידת שינויי נתונים.** טבלה שנטענת בלכידת שינויי נתונים נושאת את זמן ה-commit שלה כ-`source_committed_at`, כדי שתצוגה עם היסטוריה תנקוב בו כזמן המקור שלה:

- Debezium: זמן ה-commit של מסד הנתונים שכל אירוע שינוי נושא (`source.ts_ms`). לשורה שהועתקה בבנייה מלאה של העותק אין אירוע שינוי, ולכן ה-`source_committed_at` שלה ריק עד שהשורה משתנה שוב, אלא אם המקור הוא PostgreSQL עם חותמות זמן של commit מופעלות (הפריט הבא).
- PostgreSQL עם אות שינוי `native`: זמן ה-commit ש-PostgreSQL שומרת לכל שורה (`pg_xact_commit_timestamp(xmin)`), הנקרא על ידי הדרייבר של המקור עצמו בבניית העותק. PostgreSQL שומרת אותו רק כש-`track_commit_timestamp = on`. הפעילו אותו כדי לקבל זמן מקור עם האות הנייטיבי; בלעדיו `source_committed_at` ריק. הפעלה או כיבוי של ההגדרה בונים מחדש את העותק.

נושאי Kafka ללא מעטפת Debezium אינם נושאים זמן commit.

### Views לקריאה בלבד {: #query-only-views }

גם views עם `materialize: true` וגם עם `materialize: false` חושפים את טיפוס ה-GraphQL שלהם כלקריאה-בלבד. לא מחוללות מוטציות insert, upsert, update, או delete עבור יחסים מבוססי-`view_sql`.

## מטמון (Cache) {: #cache }

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### היררכיית מטמון {: #cache-hierarchy }

סדר פתרון TTL (הספציפי ביותר מנצח): **טבלה** > **מקור** > **ברירת מחדל גלובלית**. הערך הלא-null הראשון משמש.

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

הגדרת `cache_enabled: false` על מקור משביתה מטמון לכל הטבלאות במקור זה, ללא קשר ל-TTL ברמת הטבלה. מפתחות מטמון תמיד כוללים `role_id` + ערכי הקשר RLS לחלוקה מסודרת מבחינת אבטחה.

### הצטרפות לפי בקשה {: #per-request-opt-in }

מטמון התגובות כבוי עבור כל בקשה שאינה מבקשת אותו. בקשה מצטרפת באמצעות:

| ממשק | הצטרפות |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` על הפעולה, ואופציונלית `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | שורת הערה `-- @provisa cache=true` או `-- @provisa cache_ttl=60` בהצהרה |
| Cypher (`/data/cypher`, ה-Neo4j Query API, Bolt, Arrow Flight) | שורת הערה `// @provisa cache=true` או `// @provisa cache_ttl=60` בשאילתה |
| gRPC (ופרוקסי ה-gRPC ב-HTTP) | מטא-נתוני הקריאה (כותרות בפרוקסי) `x-provisa-cache: true` או `x-provisa-cache-ttl: 60` |
| REST, JSON:API, וכחלופה לרמז בטקסט: GraphQL מעל HTTP, `/data/cypher`, ה-Neo4j Query API | כותרת הבקשה `Cache-Control: max-age=60` (או `max-stale=60`), או `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

`ttl` שאינו מספר שלם מכשיל את הבקשה. בקשה שמציינת את ההצטרפות פעמיים (בטקסט שלה ובכותרת, או בשתי כותרות) בערכים שונים נכשלת עם `400`; אף ערך אינו מועדף. `Cache-Control: no-cache` או `no-store` פירושו ללא מטמון, כמו ללא כותרת.

ללא הרמז הבקשה אינה קוראת מהמטמון ואינה כותבת אליו. עם הרמז, ההגדרות שלמעלה עדיין קובעות: התוצאה נשמרת במטמון רק אם לכל מקור שהיא קוראת יש `cache_enabled: true` וה-TTL המחושב של כל טבלה גדול מ-0. הרשומה נשמרת למשך ה-`ttl` של הבקשה אם צוין, ואחרת למשך ה-TTL המחושב הקצר ביותר של הטבלאות שנקראו.

הרמז רק מקבל נתונים ישנים יותר בתמורה למהירות. הוא לעולם אינו הופך קריאה לעדכנית יותר ממה שהגדרות המפעיל מתירות: עדכניות הנחיתה וההעתק (`cache_ttl`, `change_signal`), `row_materialize`, תמונות מצב `load_protected` ובדיקות עדכניות חלות על בקשה עם רמז בדיוק כמו על בקשה בלעדיו. ראו [עדכניות ועומס: מי מחליט](#freshness-and-load-who-decides).

`@noCache` ו-`-- @provisa no_cache=true` אינם קיימים עוד: כשהמטמון כבוי כברירת מחדל, אין מה לעקוף.

כתיבה מבטלת כל רשומת מטמון שהארגון הכותב מחזיק עבור הטבלאות שנכתבו; רשומות של ארגון אחר עבור אותה טבלה נשארות ללא שינוי. אם הביטול נכשל, הבקשה נכשלת במקום להשאיר רשומות מיושנות.

## עדכניות ועומס: מי מחליט {: #freshness-and-load-who-decides }

שלושה גורמים משפיעים על מידת העדכניות של תוצאה ועל העומס ששאילתה מטילה על המערכות שמאחוריה.

- **מקור הנתונים.** באופן אידיאלי הוא מנהל בעצמו את הלחץ החוזר שלו: מגבלות חיבורים, זמני קצוב להצהרות, העתקי קריאה.
- **המפעיל.** מגן על הפלטפורמה, Provisa עצמה, מפני לחץ חוזר. כשמקור אינו יכול להגן על עצמו, המפעיל מגן גם עליו. ההגדרות לכך נמצאות על מקורות וטבלאות: `load_protected`, `replicate`, `federation_hints` של המקור, סף ההפניה של תוצאות גדולות, אשכול Kafka שאליו כותבים ה-sinks, וה-watermark הרשום של כל טבלה.
- **משתמש הקצה.** מאזן בין מהירות לעדכניות, בקשה אחר בקשה.

הגדרות המפעיל הן הרצפה. בקשה יכולה לנוע מעליה, לעבר נתונים ישנים יותר או עומס קטן יותר; הצטרפות למטמון התגובות היא הדוגמה הרגילה. מתחתיה היא לעולם אינה יכולה לרדת. רמז בבקשה שהיה עושה זאת נדחה בשגיאה שמציינת את הגדרת המפעיל, ואינו מיושם או מושמט בשקט.

| קלט הבקשה | מה מותר לו | נדחה כאשר |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | לבחור את המנהל הישיר עבור מקור אחד | טבלה שההצהרה קוראת היא `load_protected` או `replicate: 0` |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | לקבוע מאפיין סשן של המנוע ש-`federation_hints` של המקור השאירו פתוח | הוא משנה מאפיין שהמפעיל קבע |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | להפנות תוצאה מוקדם יותר | הערך גבוה מסף המפעיל |
| `@sink(broker:)`, ה-broker ב-`X-Provisa-Sink` | לחזור על ה-broker של המפעיל | צוין broker אחר, או ש-`KAFKA_BOOTSTRAP_SERVERS` אינו מוגדר |
| `@watermark` במינוי | לחזור על ה-watermark הרשום של הטבלה | צוינה עמודה אחרת, או שלטבלה אין כזה |
| `@cached`, `-- @provisa cache=true` | להגיש תוצאה ישנה יותר מהמטמון | לעולם לא; רשומת מטמון אינה עדכנית יותר מהקריאה ששמרה אותה |

דחיות מחזירות HTTP 403 עם הקוד `query.operator_floor`, SQLSTATE `42501` ב-pgwire, ו-`PERMISSION_DENIED` ב-Flight וב-gRPC.

טבלה שהיא `load_protected` או `replicate: 0` לעולם אינה נקראת ישירות על ידי שאילתה, בשום ערוץ. שאילתות קוראות את הרפליקה שלה. רק יצירת הרפליקה והרענונים שלה קוראים את המקור: רענון מתבצע כש-`cache_ttl` או בדיקת עדכניות מחייבים זאת; בלי אף אחד מהם הרפליקה נבנית פעם אחת ולאחר מכן מתרעננת רק דרך הזנת שינויים או המתזמן. בטבלה כזו עם `off_peak_window` (בטבלה או במקור שלה, לפי `off_peak_tz`), ריענון רץ רק כשהחלון פתוח: ריענון שמגיע מועדו או מתבקש מחוצה לו ממתין לפתיחה הבאה של החלון, וחלון ללא `cache_ttl` מרענן פעם אחת בכל פתיחה. הבנייה הראשונה לעולם אינה מעוכבת, כי קריאות הטבלה ממתינות לה.

### הגדרת `replicate` {: #the-replicate-setting }

`replicate` הוא מספר שלם על טבלה. טבלה ללא ערך לוקחת את הערך של המקור שלה, וכששניהם אינם מוגדרים חל הסף הגלובלי. רפליקה היא עותק של טבלת מקור המוחזק במאגר של המנוע ומתרענן לפי הגדרות העדכניות של הטבלה; היא אינה זרם שינויים רציף.

| ערך | שם | משמעות |
| --- | --- | --- |
| לא מוגדר | ברירת מחדל (Default) | חל הסף הגלובלי |
| `-1` | לעולם לא (Never) | נקרא חי בכל מקום שקיים נתיב חי. מוחזק כרפליקה רק במנוע שאינו יכול לקרוא את המקור במקומו |
| `N` גדול מ-0 | Hot-N | נקרא חי עד שהטבלה חוצה N הצהרות שעברו ממשל בכל מרווח, ואז מוגש מהרפליקה שלה |
| `0` | תמיד (Always) | קריאות מגיעות מהרפליקה |

רק "תמיד" הוא ערובה. "לעולם לא" וערכי Hot הם מאמץ מיטבי. `load_protected` עם `replicate: -1` נדחה בטעינת התצורה ובשמירה. למקור עם `replicate: 0` אין חיבור (attach) חי באף מנוע, ולכן כל טבלה שלו מוגשת מהרפליקה שלה.

הרצפה נקבעת לפי הטבלאות שהצהרה קוראת. הצהרה שאינה קוראת אף טבלה המוגשת מרפליקה שומרת על הנתיב הישיר שלה גם כשלמקור שלה יש טבלאות משוכפלות אחרות.

רפליקה מאונדקסת לפי המפתח הראשי המוצהר שלה ולפי העמודות שהקשרים המוצהרים שלה מצטרפים עליהן, והאינדקסים נבנים אחרי שהיא נטענת. היכן האינדקסים האלה מאיצים צירופים, היכן לשמור רפליקות וכיצד לבחור את העדכניות שלהן מפורט ב-[Replicas](replicas.md).

### שכפול חם (Hot replication) {: #hot-replication }

טבלה שה-`replicate` שלה אינו מוגדר, או הוא `N` גדול מ-0, נקראת חי עד שהיא עמוסה.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| הגדרה | משתנה סביבה | ברירת מחדל |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

שלושתן הגדרות מפעיל: ממשק הניהול מציג אותן תחת Hot Tables, וערך שנשמר חל בהערכה הבאה.

כיצד נספרת טבלה:

- הצהרה שעברה ממשל סופרת כל טבלה שהיא קוראת פעם אחת, בלי קשר לכמה פעמים היא קוראת אותה.
- הצהרה שנענתה ממטמון התגובות אינה נספרת. היא לא הטילה עומס על המקור.
- הצהרה שהממשל דחה אינה נספרת.

טבלה מקודמת כשהספירה שלה מגיעה לסף שלה (ה-`N` שלה, אחרת של המקור שלה, אחרת `hot_threshold`) ומורדת כשהספירה יורדת מתחת למחציתו. קידום מבקש בניית רפליקה; הטבלה נקראת חי עד שהבנייה מסתיימת, ואז מהרפליקה. הורדה מחזירה את הקריאות למקור מיד.

טבלה אינה מקודמת כשהיא מכילה יותר שורות מ-`hot_max_rows`, כשאות השינוי שלה הוא `ttl` או `ttl_probe` ולא היא ולא המקור שלה מצהירים על `cache_ttl`, או כשרמת ה-Redis החמה כבר מנהלת אותה. הסיכום של הטבלה בממשק הניהול מציין איזה מהם.

הספירות נשמרות ב-Redis. עם Redis משותף (`cache.redis_url`) הן מכסות כל תהליך בפריסה. עם Redis מוטמע הן לכל תהליך, ולכן קידום פועל רק כשעובד (worker) אחד משרת בקשות; עם כמה עובדים וללא Redis משותף שום דבר אינו מקודם, והסיכום מציין זאת.

## אימות (Authentication) {: #authentication }

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

### סוגי ספק אימות {: #auth-provider-types }

| ספק | מקרה שימוש | אימות token |
| ---------- | ---------- | ----------------- |
| `simple` | פיתוח/בדיקות מקומיים. משתמשים מוגדרים ב-YAML. | JWT חתום עם `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (כל השיטות). | `verify_id_token()` של SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. תפקידי דייר + לקוח ממופים. | אימות JWT מבוסס JWKS |
| `oauth` | OIDC גנרי (Okta, Azure AD, Auth0, PingFederate). | JWKS מ-URL גילוי |
| `basic` | פריסות עצמאיות. החשבונות שוכנים במאגר של Provisa עצמה. | סיסמת bcrypt, או SCRAM-SHA-256 ב-pgwire |

אישורי superuser (בלוק `superuser`) עובדים עם כל ספק ותמיד נפתרים לתפקיד admin עם כל היכולות. משמש להגדרה ראשונית לפני שאימות חיצוני מוגדר.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

גורם ל-pgwire להכריז על SASL עם `SCRAM-SHA-256`, כך שסיסמה מוכחת במקום להישלח כטקסט גלוי. הדבר חל על הספק `basic` בלבד — אף ספק אחר אינו מחזיק את מאמתי RFC 5802 ש-SCRAM זקוק להם — וקשירת ערוץ אינה מוצעת.

לא ניתן לגזור מאמתים מגיבובי bcrypt קיימים. מאמת נכתב בכל פעם שסיסמה עוברת כטקסט גלוי, ולכן החיבור הראשון ב-SCRAM של כל משתמש בא לאחר ההרשמה, ההתחברות, שינוי הסיסמה או איפוס המנהל הבאים שלו. עד אז חיבוריו של אותו משתמש נסוגים לחילופין בטקסט גלוי מעל TLS; הקו אינו חושף מי כבר עבר.

### ויסות התחברות (`auth.login_throttle`) {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

פעיל כברירת מחדל עם הערכים המוצגים; הבלוק רק מכוונן אותם. המונה יושב בשכבת אימות האישורים, ולכן כשלים מעל HTTP, pgwire ו-Bolt מצטברים כנגד אותו נושא, ונעילה תופסת בכל ממשק. הוא לכל תהליך: כמה עובדי API מאפשרים כל אחד עד `max_attempts`.

### אסימוני גישה אישיים {: #personal-access-tokens }

PAT אינם דורשים בלוק תצורה — הם מתקבלים תמיד, והמאגר נוצר יחד עם שאר סכמת מישור הבקרה. מה שניתן להגדרה הוא תוקף התפוגה שמשתמש רשאי לבקש בעת ההנפקה: 1 עד 366 ימים, או ללא תפוגה עבור אסימון שאינו פג. ראו [מודל אבטחה](security.md#personal-access-tokens).

### TLS הדדי {: #mutual-tls }

אימות תעודת לקוח מוגדר באמצעות משתני סביבה ולא ב-`provisa.yaml`, לצד הגדרות תעודת ה-TLS שהוא מרחיב.

| משתנה | ברירת מחדל | משמעות |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | לא מוגדר | חבילת PEM של רשות/רשויות האישורים המורשות לחתום על תעודות לקוח. הגדרתו מפעילה את אימות תעודות הלקוח |
| `PROVISA_MTLS_MODE` | `required` לאחר שהוגדרה רשות אישורים | `required` או `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | דורש שה-common name של התעודה יהיה זהה לשם המשתמש שאיתו החיבור מאמת |

לכל אחד יש דריסה לפי פרוטוקול תחת אותה מוסכמת שמות כמו הגדרות ה-TLS. מצב שהוגדר ללא רשות אישורים, או מצב שאינו אף אחד משני הערכים, מסרב לעלות במקום לשרת חיבורים שהמפעיל מאמין שנבדקו.

### מיעון ארגון מעל TLS {: #addressing-an-org-over-tls }

אין מה להגדיר. בפריסה מרובת ארגונים, pgwire ו-Bolt קוראים את הארגון משם המארח שהלקוח חייג אליו, הנישא ב-ClientHello של TLS, בדיוק כפי ש-HTTP קורא אותו מכותרת `Host`. לקוח המתחבר ל-`acme.provisa.dev` מבקש את הארגון `acme`; הבקשה נדחית אלא אם כן ה-principal המאומת חבר בו. התחברות לפי כתובת IP אינה מבקשת ארגון כלל, וזהו כל חיבור בפריסה חד-ארגונית.

### דוגמת תצורת אימות מלאה (מוערת) {: #full-auth-config-example-commented-out }

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

## מוטציות Upsert {: #upsert-mutations }

עבור טבלאות עם מפתח ראשי, Provisa מחוללת אוטומטית שדות מוטציה `upsert_<table>`. אלה מתקמפלים ל-upsert בדיאלקט היעד — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` ב-PostgreSQL, `ON DUPLICATE KEY UPDATE` ב-MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

עמודות קונפליקט נגזרות ממטא-נתוני PK. כל כללי נראות עמודה והרשאת כתיבה חלים.

## Distinct On

הארגומנט `distinct_on` בוחר את השורה הראשונה עבור כל ערך נבדל של העמודות שצוינו. זמין בשדות שאילתת שורש.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

מתקמפל ל-`SELECT DISTINCT ON (region) ...` ב-PostgreSQL. עבור דיאלקטים שאינם PG, נעשה שימוש בנפילה-חוזרת מבוססת window-function.

## Presets של הרשאות כתיבה {: #write-grant-presets }

הרשאת כתיבה קובעת עמודות בעצמה בהוספה ובעדכון. ה-presets מוצהרים ברשימה `presets` של ההרשאה, לכל תפקיד ופעולה.

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

| מקור | התנהגות |
| -------- | ---------- |
| `session` | מזריק את הערך של משתנה ההפעלה הנקוב (`name`) של הבקשה |
| `now` | מזריק את רגע הכתיבה |
| `literal` | מזריק קבוע (`value`) |

ה-presets מתווספים להצהרה בזמן הקומפילציה, לפני יצירת ה-SQL. ההצהרה אינה רשאית לספק עמודת preset, ועמודת preset אינה מופיעה בטיפוס הקלט של המוטציה.

## תפקידים בעלי ירושה {: #inherited-roles }

תפקידים יכולים לרשת מתפקיד הורה אחד דרך `parent_role_id`. השרשרת משוטחת בעת ההפעלה. בן מחזיק את האיחוד של היכולות וגישת הדומיין של אבותיו; עמודה, מטריקה, פונקציה או webhook שהוענקו לאב קדמון מוענקים לבן; וכללי ה-RLS של אב קדמון חלים על הבן לכל טבלה, התפקיד הקרוב ביותר קודם, כשהכלל של הבן עצמו לטבלה מחליף את זה של ההורה.

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

ירושה מרובת-רמות נתמכת. היכולות המפורשות ו-domain_access של תפקיד הבן ממוזגים עם אלה של ההורה. הורה חייב להיות תפקיד קיים, אינו יכול להיות התפקיד עצמו ואינו יכול לסגור מעגל; כל אחד מאלה נדחה בשמירה.

## טריגרים מתוזמנים {: #scheduled-triggers }

טריגרים מבוססי-cron שקוראים לכתובת webhook לפי לוח זמנים. משתמש ב-APScheduler.

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

משימות מתוזמנות מנוהלות דרך ממשק הניהול (מתג הפעלה/כיבוי) או דרך מוטציית הניהול `toggle_scheduled_task`.

טריגר יכול לפעול על טבלה רשומה במקום על פי תזמון: `on: {table, event}` מציין את הטבלה בצורה `source_id.schema.table` ואת האירוע, `change`, `error` או `quarantine`. טריגר `command` מריץ פקודה רשומה בתפקיד `role` שלו, דרך השיגור המנוהל של הפקודה עצמה, ומעביר לה כארגומנטים את מה שהפעיל אותו. שינוי מעביר `table` ובמקום שבו אות השינוי נושא אותה (Kafka, Debezium) את האצווה שנטענה בשם `changes`. אירוע `error` או `quarantine` מעביר `table`, `event`, `reason` ו-`error`. טריגר שהפקודה שלו מצהירה על ארגומנט שהטריגר אינו מעביר נדחה בשמירה. שינוי מפעיל פעם אחת לכל פרץ שאות השינוי שלו מדווח, או כשכתיבה שבוצעה דרך Provisa נוחתת; טבלה המוגשת מהרפליקה שלה מפעילה כשבנייה טוענת שורות השונות מהקודמת. הכתיבה של פקודה עצמה היא שינוי, ולכן טריגרים משתרשרים. כל הרצה נרשמת עם זמנה, תוצאתה (`succeeded`, `refused` או `failed`), השגיאה שלה ומה שהפעיל אותה, ושאילתת הניהול `scheduledTasks` מציגה אותה. רק הטריגרים של prod פועלים, ושום דבר אינו מותקן במסד נתוני מקור.

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

## פורמט OrderBy {: #orderby-format }

OrderBy משתמש בפורמט `{column: direction}` עם enum כיוון בעל 6 ערכים:

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

מיון לפי קשרים נתמך דרך אובייקטים מקוננים:

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

### מסנני טלמטריה {: #telemetry-filters }

Provisa מריצה שני נתיבי ייצוא OTLP עצמאיים: הקולקטור הפנימי שלכם ונקודת הקצה האופציונלית לתמיכה של Provisa. לכל נתיב מסנן משלו. המסננים רצים בתוך `_FilteringExporter` עוטף לפני שה-spans עוזבים את התהליך — אובייקטי span המקוריים לעולם אינם משתנים.

**`telemetry_filter`** — שולט במה שמגיע לקולקטור הפנימי שלכם.

| מפתח | טיפוס | ברירת מחדל | תיאור |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | מחליף מילוליים (literals) מחרוזתיים ומספריים ב-`db.statement` ב-`?` |
| `redact_attributes` | list[str] | `[]` | מפתחות תכונה (attribute) שנשמטים לגמרי מכל span |

**`support_telemetry_filter`** — שולט במה שמגיע לנקודת הקצה לתמיכה של Provisa. עריכת מילוליים SQL כברירת מחדל `true` בנתיב זה, מכיוון שנתוני שאילתה שייכים לכם.

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

### נקודת קצה לתמיכה {: #support-endpoint }

`support_endpoint` (או משתנה סביבה `PROVISA_SUPPORT_OTLP_ENDPOINT`) מעביר טלמטריה לתמיכה של Provisa לצורכי אבחון. כאשר לא מוגדר, שום נתון אינו עוזב את התשתית שלכם דרך נתיב זה. מסנן התמיכה חל באופן עצמאי מהמסנן הפנימי — ניתן לערוך מילוליים SQL משני הייצואים בעודכם משתפים עם התמיכה את נתוני תזמון וטעויות ה-span.

### זיהוי פרוטוקול נקודת קצה {: #endpoint-protocol-detection }

Provisa בוחרת OTLP/HTTP או OTLP/gRPC לפי סכימת ה-URL של נקודת הקצה. כתובות URL שמתחילות ב-`http://` או `https://` משתמשות ב-OTLP/HTTP, עם `/v1/traces`, `/v1/metrics`, ו-`/v1/logs` מתווספים אוטומטית. כל סכימה אחרת משתמשת ב-OTLP/gRPC עם `insecure=True`.

## מנוע פדרציה {: #federation-engine }

הגדרת מנוע פדרציה אופציונלית. ברירת המחדל היא `duckdb` — ללא תצורה, בתוך-התהליך, ללא צורך בשירות חיצוני. בחרו מנוע אחר כשאתם זקוקים לקנה מידה MPP או רוצים להשתמש שוב ב-warehouse קיים.

עדיפות: משתנה סביבה `PROVISA_ENGINE` ← שדה תצורה `federation_engine` שמור בממשק הניהול ← `duckdb`. שינויים נכנסים לתוקף בהפעלה מחדש של השירות.

### סקירת מנועים {: #engine-overview }

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
| `mysql` | MySQL | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `mariadb` | MariaDB | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `oracle` | Oracle Database | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `mssql` | Microsoft SQL Server | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `db2` | IBM Db2 | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `redshift` | Amazon Redshift | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `greenplum` | Greenplum | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `cockroachdb` | CockroachDB | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `yugabytedb` | YugabyteDB | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `opengauss` | openGauss | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `tidb` | TiDB | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `singlestore` | SingleStore | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `vertica` | Vertica | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `exasol` | Exasol | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `teradata` | Teradata Vantage | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `saphana` | SAP HANA | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `sapase` | SAP ASE (Sybase) | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `sqlanywhere` | SAP SQL Anywhere | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `monetdb` | MonetDB | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `firebird` | Firebird | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |
| `sqlalchemy` | מסד נתונים רלציוני אחר (לפי כתובת חיבור) | לפי-דיאלקט | לא | ללא (land-only) | אישורים לפי-דיאלקט |

### רפרנס מנוע {: #engine-reference }

#### trino / trino-byo

`trino` הוא ה-coordinator המנוהל של Provisa; `trino-byo` מתחבר ל-cluster Trino משלכם. שניהם משתמשים ב-Trino SQL ובעלי הטווח הרחב ביותר לסוגי מקורות.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

מאגר המימוש (materialization store) ברירת מחדל `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

מפדרר דרך postgres_fdw (SQL/MED) והרחבות pg_duckdb. צומת יחיד; ללא MPP. הכי טוב כשהנתונים שלכם כבר חיים ב-PostgreSQL ואתם רוצים לצרף כמה מקורות מרוחקים.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

#### duckdb

בתוך-התהליך; ללא שירות חיצוני. המנוע ברירת המחדל. `PROVISA_DATA_DIR` שולט היכן חי המאגר המובנה (`~/.provisa` כברירת מחדל).

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

#### מנועי מסדי נתונים יחסיים (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) ו-`sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

מפתח אחד לכל מסד נתונים יחסי הניתן לכתובת ברשת, כולם על אותו runtime land-only (ללא פדרציה למקורות חיצוניים): כל מקור נוחת (lands) לתוך המאגר ונשאל שם. המפתח בוחר את מסד הנתונים; `PROVISA_ENGINE_URL` נושא את ה-DSN שהדיאלקט שלו מצפה לו. `sqlalchemy` הוא הכל-כולל למסד נתונים ללא מפתח משלו. מאגרים מוטמעי-קובץ (SQLite, Access) אינם מוצעים — השרת חייב להיות ניתן להשגה ברשת.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

מאגר המימוש ברירת מחדל `TENANT_DATABASE_URL`.

### מאגר המימוש (Materialization store) {: #materialization-store }

כאשר מקור אינו יכול להתחבר (attach) חי (אין מחבר ATTACH למנוע הנבחר), הוא נוחת (lands) לתוך מאגר המימוש של המנוע. סדר פתרון: `PROVISA_MATERIALIZE_URL` מפורש ← ברירת המחדל המוצהרת של המנוע ← שגיאה קשה (ללא נפילה-חוזרת שקטה).

DuckDB מצהיר על קובצו המובנה (`~/.provisa/materialize.duckdb`) כברירת מחדל שלו. כל שאר המנועים ברירת מחדל `TENANT_DATABASE_URL` (PostgreSQL). דרסו כל מנוע עם `PROVISA_MATERIALIZE_URL`.

המאגר קובע מה האינדקסים של רפליקה עושים עבור צירופים: מאגר PostgreSQL מאנדקס רפליקות ומנוע שמריץ SQL בו קורא את האינדקסים האלה, ואילו DuckDB מצרף את הרפליקות של הקובץ המובנה שלו מעל עותק לכל הצהרה. ראו [Replicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

### רמזי פדרציה לפי-מקור {: #per-source-federation-hints }

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

## משתני סביבה {: #environment-variables }

| משתנה | ברירת מחדל | תיאור |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | נתיב קובץ תצורה |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | URI מאגר מישור-הבקרה (SQLAlchemy async); מקבל `sqlite+pysqlite://…` / `duckdb://…` עבור מאגר שולחן העבודה המובנה |
| `PLATFORM_DATABASE_URL` | — | URI מרשם הפלטפורמה (ספריית דיירים, מרשם מנועים); נדרש בעת ההפעלה, ללא נפילה-חוזרת |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` משתמש ב-fakeredis מובנה במקום שרת Redis — ללא Docker |
| `PG_HOST` | `localhost` | מארח PostgreSQL |
| `PG_PORT` | `5432` | פורט PostgreSQL |
| `PG_DATABASE` | `provisa` | מסד נתונים PostgreSQL |
| `PG_USER` | `provisa` | משתמש PostgreSQL |
| `PG_PASSWORD` | `provisa` | סיסמת PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | מפתח מנוע פדרציה |
| `PROVISA_ENGINE_URL` | — | URL חיבור למנועים מונעי-URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | דריסת DSN של מאגר המימוש (ברירת מחדל לברירת המחדל המוצהרת של המנוע) |
| `PROVISA_DATA_DIR` | `~/.provisa` | תיקיית נתונים למאגר DuckDB המובנה |
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
| `PROVISA_RETRY_BUDGET_SECS` | `30` | תקציב ניסיון-חוזר לקריאה מדרג-1 בשניות; נסיגה אקספוננציאלית עם jitter מלא |
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
| `PROVISA_MTLS_CLIENT_CA` | — | חבילת PEM של רשות/רשויות האישורים המורשות לחתום על תעודות לקוח; הגדרתו מפעילה אימות תעודות לקוח ב-pgwire, Bolt, gRPC ו-Flight |
| `PROVISA_MTLS_MODE` | `required` לאחר שהוגדרה רשות אישורים | `required` או `optional`; כל ערך אחר מסרב לעלות |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | דורש שה-common name של התעודה יהיה זהה לשם המשתמש המאמת |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | אתרים מופרדים בפסיקים המורשים לפתוח WebSocket של Bolt מדפדפן; ללא הגדרה נדחה כל מקור דפדפן |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras של pyproject האפויים לתוך תמונת האפליקציה; `scripts/provisa` גוזר אותו מ-`dq_checker` ב-`~/.provisa/config.yaml` |
| `PROVISA_DQ_CHECKER` | `none` | למתקין בלבד: `none`/`soda`/`gx`, נקרא על ידי `first-launch.sh` במצב לא-אינטראקטיבי ונכתב ל-`config.yaml` בתור `dq_checker` |
| `ANTHROPIC_API_KEY` | — | מפתח API של Claude (גילוי) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | דורס את `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | דורס את `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | דורס את `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | דורס את `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | עיכוב flush של batch span processor |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | דורס את `observability.support_endpoint` |
