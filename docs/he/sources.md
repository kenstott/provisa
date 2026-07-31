# סוגי מקורות

## מודל ביצוע

כל שאילתה בסופו של דבר מתבצעת דרך מנוע הפדרציה, המספק פדרציה על פני כל המקורות. מקורות מתחלקים לשלוש קטגוריות בהתבסס על קישוריותם. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| קטגוריה | יש דרייבר ישיר | יש מחבר פדרטיבי | דוגמאות |
| --- | --- | --- | --- |
| **בעל יכולת ישירה** | כן | כן | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **פדרציה בלבד** | לא | כן | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (מגובה-S3) |
| **קריאה ישירה (replica)** | כן | כן | Snowflake, Databricks, ClickHouse — הדרייבר קורא נתונים ומנחית replica; השאילתות רצות מול ה-replica במנוע הפעיל |
| **מימוש → פדרציה** | לא | לא | REST/OpenAPI, GraphQL מרוחק, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (מקלט push), GovData, SharePoint, Splunk |

מקורות **בעלי יכולת ישירה** מבצעים שאילתות מקור-יחיד דרך הדרייבר הילידי שלהם (תת-100 מ"ש), עוקפים את מנוע הפדרציה (REQ-027, REQ-229). הם שומרים על תמיכת מחבר מלאה ומשתתפים בפדרציה כשמצטרפים עם מקורות אחרים (REQ-028).

מקורות **פדרציה בלבד** נשאלים תמיד דרך שכבת הפדרציה. לא קיים דרייבר ישיר (REQ-229).

מקורות **קריאה ישירה (replica)** יש להם DirectDriver הקורא מה-warehouse באופן ילידי (ילידי-Arrow היכן שזמין), מנחית replica למאגר המימוש של המנוע הפעיל, ולאחר מכן שאילתות רצות מול אותו replica. ראו [Warehouses כמקורות בעלי-שם](#warehouses-).

למקורות **מימוש** אין מחבר פדרטיבי. Provisa שולפת את הנתונים שלהם (בעת ההפעלה או בזמן שאילתה) ומטמינה אותם כ-Parquet ב-S3 או ב-PostgreSQL, מה שהופך אותם לנגישים על ידי מנוע הפדרציה עבור שאילתות חוצות-מקורות (REQ-309).

---

## כל המקורות

רפרנס לכל סוג מקור ש-Provisa תומכת בו. "דרייבר ישיר" פירושו ששאילתות מקור-יחיד מתבצעות מול המקור באופן ילידי (תת-100 מ"ש) (REQ-027). "שם מחבר" הוא המחבר הפדרטיבי הנעשה בו שימוש כאשר המקור משתתף ב-JOIN-ים רב-מקוריים (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| סוג מקור | דרייבר ישיר | שם מחבר | דיאלקט | מוטציות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | כן |
| `mysql` | aiomysql | mysql | mysql | כן |
| `mariadb` | aiomysql | mariadb | mysql | כן |
| `singlestore` | — | singlestore | singlestore | פדרטיבי |
| `sqlserver` | aioodbc | sqlserver | tsql | כן |
| `oracle` | oracledb | oracle | oracle | כן |
| `duckdb` | duckdb | memory | duckdb | כן |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | כן |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | כן |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | כן |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | כן |

מסדי נתונים תואמי-חוט עושים שימוש חוזר בדרייבר JDBC של חוט בסיס, בדרייבר האסינכרוני הילידי, ובדיאלקט — CockroachDB, YugabyteDB, ו-Greenplum רוכבים על חוט PostgreSQL; TiDB רוכב על חוט MySQL. הם דורשים רק רשומות רישום, ללא קוד מחבר חדש. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) ו-`airport` (שרת Arrow Flight) הם סוגי מקור רשומים המושגים במקומם דרך הרחבות קהילה של DuckDB כאשר DuckDB הוא המנוע הפעיל — ללא דרייבר ישיר, ללא מחבר פדרטיבי. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### מחסני נתונים בענן (Cloud Data Warehouses)

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| סוג מקור | דרייבר ישיר | שם מחבר | דיאלקט | מוטציות | הערות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | פדרטיבי | קורא דרך snowflake-connector-python; מנחית replica; `account`/`warehouse`/`role` ב-`federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | פדרטיבי | ללא DirectDriver; מושג דרך מנוע הפדרציה או ATTACH‏ מנוע BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | פדרטיבי | קורא דרך databricks-sql-connector (Cloud Fetch, Arrow); מנחית replica; `http_path` נדרש ב-`federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | פדרטיבי | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | פדרטיבי | Microsoft Fabric Warehouse; T-SQL על גבי TDS, אימות Azure AD; מנחית replica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | פדרטיבי | Azure Synapse SQL; T-SQL על גבי TDS, אימות Azure AD; מנחית replica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | פדרטיבי | קואורדינטור Trino/Presto מרוחק נקרא דרך דיאלקט trino של SQLAlchemy; מנחית replica על כל מנוע (REQ-994) |

### אנליטיקה / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| סוג מקור | דרייבר ישיר | שם מחבר | דיאלקט | מוטציות | הערות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | פדרטיבי | קורא דרך clickhouse-connect (HTTP); `secure: "true"` ב-`federation_hints` עבור TLS (REQ-986) |
| `druid` | — | druid | druid | לא | — |
| `exasol` | — | exasol | exasol | לא | — |
| `elasticsearch` | — | elasticsearch | — | לא | מאפייני המחבר מגיעים מ-DSL המיפוי של הסוג [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | לא | מחבר `pinot` של Trino; `pinot.controller-urls` = host:port של בקר ה-Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / פורמטי טבלה פתוחים

סוגי מקור אלה הם פדרציה-בלבד — ללא דרייבר ישיר, ללא דיאלקט. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| סוג מקור | שם מחבר | Time Travel | הערות |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | כן (ארגומנט `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | כן (ארגומנט `as_of`, REQ-372) | — |
| `hive` | hive | לא | — |
| `hive_s3` | hive | לא | Hive מגובה-S3 |

### NoSQL

ל-`mongodb`, `cassandra`, ו-`redis` יש מחברי Trino (`redis` בונה את מאפייניו מ-DSL המיפוי של הסוג). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| סוג מקור | שם מחבר | מוטציות |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | לא |
| `cassandra` | cassandra | לא |
| `redis` | redis | לא |

### סטרימינג

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `kafka` | מחבר Kafka פדרטיבי; סכמה דרך Confluent Schema Registry (Avro, Protobuf, JSON Schema), הגדרה ידנית, או הסקת דגימה (REQ-147, REQ-150) | Sink בלבד (REQ-176) |
| `websocket` | פיד WebSocket חיצוני — חיבור, הרשמה, קבלת אירועים; תוצאות ממומשות (REQ-338) | לא |
| `rss` | פיד RSS 2.0 / Atom — polling, סימון-מים לפי pubDate/updated; תוצאות ממומשות (REQ-342, REQ-343) | לא |

### מקלט Push

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `ingest` | שירותים חיצוניים שולחים POST של אירועי JSON; תוצאות ממומשות (REQ-331, REQ-335) | לא |

### גרף וסמנטי

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher דרך API HTTP, תוצאות ממוטמנות ב-PostgreSQL (REQ-295) | לא |
| `sparql` | SPARQL 1.1 POST, תוצאות ממוטמנות ב-PostgreSQL (REQ-297) | לא |

### מבוסס-קובץ

שני מנגנונים מכסים קבצים. שניהם משתמשים בשדה `path` במקום `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**מקורות קובץ-יחיד** — `sqlite`, `csv`, `parquet` מפנים את `path` לקובץ אחד.

| סוג מקור | תעבורות | מוטציות |
| --- | --- | --- |
| `sqlite` | מקומי | כן |
| `csv` | מקומי | לא |
| `parquet` | מקומי, `s3://` | לא |

buckets פרטיים דורשים אישורים (אזור AWS ומפתחות מהסביבה). עבור CSV דרך `s3://` או `http(s)://`, או כדי לרשום קבצים רבים בבת אחת, השתמשו במקור `files`. [tool-verified: `provisa/file_source/source.py`]

**מקור `files`** — מפנה את `path` ל-glob, סורק אותו רקורסיבית, ורושם את הספרייה כקטלוג פדרטיבי של טבלאות. הוא קורא פורמטים רבים על פני תעבורות רבות; הקבוצות למטה מגיעות ממחבר הקבצים (fork של kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| פורמטים | תעבורות |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, ומסמכים המומרים לטבלאות — HTML, Markdown, DOCX, PPTX | מערכת קבצים מקומית, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST ו-Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observability ואחר

ל-`prometheus` יש מחבר Trino (מאפיינים בנויים מ-DSL המיפוי של הסוג). `google_sheets` הוא סוג מקור רשום ללא מחבר Trino וממומש דרך צינור מטמון ה-API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| סוג מקור | שם מחבר | מוטציות |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (ממומש) | לא |
| `prometheus` | prometheus | לא |

### מחברי SaaS ארגוניים

SharePoint ו-Splunk נרשמים דרך מחברי Apache Calcite (fork של kenstott/calcite). לאף אחד מהם אין דרייבר ישיר — Provisa ממשת את השורות שלהם על ידי הפעלת שרת ה-pgwire המובנה של Calcite של המחבר (`pgwire-sharepoint`, `pgwire-splunk`), מתחברת אליו כנקודת קצה PostgreSQL גנרית, ומנחיתה את השורות למאגר המימוש עבור פדרציה (REQ-954). שני המחברים תמיד מפעילים התאמת שם לא-רגישת-רישיות, בהתאם לסמנטיקה הלא-רגישת-רישיות של כל מוצר בעצמו (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

רשימות SharePoint נמנות כסכמות וחשופות כטבלאות ניתנות-לשאילתה (REQ-726, REQ-731). שתי שיטות אימות: `CLIENT_CREDENTIALS` (ברירת מחדל) ומבוססת-תעודה דרך תעודת PFX (REQ-727). ערכי סוד ב-`mapping` נפתרים דרך מנוע הסודות לפני שהם מגיעים למחבר (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| שדה מקור | מאפיין מחבר | הערות |
| --- | --- | --- |
| `base_url` או `host` | `site-url` | URL אתר SharePoint |
| `username` | `client-id` | מזהה client של אפליקציית Azure |
| `password` | `client-secret` | סוד client של אפליקציית Azure |
| `database` | `tenant-id` | UUID של tenant Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (ברירת מחדל) או `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | נתיב PFX כאשר `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | סיסמת PFX |

כאשר המחבר אינו חושף `information_schema.columns`, רשמו את הטבלה עם הגדרות עמודה מפורשות (המתקבלות מ-Microsoft Graph API) דרך המוטציה `registerTable` (REQ-732).

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  password: ${env:SP_CLIENT_SECRET}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CLIENT_CREDENTIALS
```

#### `splunk`

תוצאות חיפוש Splunk ניתנות-לשאילתה כטבלאות (לדוגמה `internal_server`) (REQ-721). ה-URL של המחבר מגיע מ-`base_url`, או נבנה כ-`https://{host}:{port}` עם פורט ברירת מחדל `8089` (REQ-722). אימות: כאשר `mapping.use_token` הוא `true` (ברירת המחדל), `password` מועבר כטוקן API; כאשר `false`, `username` ו-`password` מועברים כאישורים נפרדים (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| שדה מקור | מאפיין מחבר | הערות |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, אחרת `https://host:port` (ברירת מחדל פורט 8089) |
| `password` | `token` או `password` | טוקן כאשר `use_token: true` |
| `username` | `user` | רק כאשר `use_token: false` |
| `database` | `app` | הגבלה לאפליקציית Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | פילטור למודל נתונים |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | עבור תעודות self-signed (REQ-724) |

```yaml
- id: ops-splunk
  type: splunk
  host: splunk
  port: 8089
  password: ${env:SPLUNK_TOKEN}
  mapping:
    use_token: true
    disable_ssl_validation: true
```

### מקורות API

רישום כל נקודת קצה HTTP כטבלה ניתנת-לשאילתה. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| סוג API | גילוי | הסקת עמודות |
| --------- | ----------- | ----------------- |
| `openapi` | פענוח מפרט OpenAPI (REQ-314, REQ-316) | פרימיטיבים → ילידי, אובייקטים → JSONB |
| `graphql_remote` | Introspection של סכמה (REQ-307, REQ-308) | פרימיטיבים → ילידי, אובייקטים → JSONB |
| `grpc_remote` | Server reflection (REQ-322, REQ-325) | פרימיטיבים → ילידי, אובייקטים → JSONB |

תגובות API נשלפות, ממוטמנות ב-PostgreSQL (TTL ניתן-להגדרה), וחשופות כטיפוסי GraphQL (REQ-309, REQ-318, REQ-327). טבלאות ממוטמנות משתתפות בשאילתות פדרטיביות כמו כל מקור אחר (REQ-313).

**כללי JSONB**: עמודות מורכבות (אובייקטים, מערכים) המאוחסנות כ-JSONB אינן ניתנות-לפילטור (REQ-119). גישה לתת-שדה משתמשת בהוצאת `->>` ב-SQL (REQ-151). קשרים מוצהרים בין טבלאות באמצעות עמודות FK סקלריות — עמודות blob של JSONB אינן יעדי join. השתמשו בקידום JSONB כדי להמיר שדות מקוננים לעמודות סקלריות ילידיות כאשר נדרש לפלטר או להצטרף עליהן (REQ-119).

### GovData

נתונים פתוחים של ממשלת ארה"ב. הגישה מחולקת לפי קיבוץ נושא. [tool-verified: `provisa/core/models.py` lines 543–609]

כל מקור `govdata` בוחר נושא אחד. אותו נושא קובע אילו סכמות GovData חשופות. הסכמות `ref` ו-`geo` תמיד כלולות כסכמות מקשרות — הן אינן רשומות לפי-נושא אך תמיד נוכחות. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| נושא | סכמות חשופות |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |
| `ALL` | כל הסכמות שלעיל |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| שדה | נדרש | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `id` | כן | — | מזהה ייחודי |
| `subject` | כן | — | אחד מערכי הנושא שלעיל |
| `domain_id` | כן | — | הדומיין שאליו שייך מקור זה |
| `description` | לא | `""` | תיאור קריא-לאדם |

---

## מחברים מותאמים אישית (REQ-1177)

מנועי הפדרציה הילידיים — Postgres, DuckDB, ו-ClickHouse — מקבלים קישוריות לסוג מקור חדש כאשר מפעיל מצהיר על מחבר עבורו ב-`config/custom_connectors.yaml`. אין צורך בקוד. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

הרחבת מחברים עצמה קדמה לזה. מנוע Trino ניתן-להרחבה זה מכבר בשכבה שלו — מחבר JDBC גנרי אחד עם פרמטרים לפי-סוג-מקור, גוף `.properties` של קטלוג לכל סוג, ותוספי מחבר Trino מותאמים-אישית משל Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 מביא את אותה הרחבתיות מונעת-תצורה לשני המנועים הילידיים, חסרי-האשכול, שנשאו בעבר קבוצת מחברים קבועה.

התצורה נשלחת ריקה. מחברים מובנים מכסים קישוריות מוכנה-לשימוש; כל דבר בקובץ זה נכתב על ידי המפעיל. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] הגדירו `PROVISA_CUSTOM_CONNECTORS` כדי להפנות לנתיב אחר (שימושי לבדיקות).

### סוגי תיאור (Descriptor kinds)

| מנוע | סוג | מנגנון | מה התיאור מספק |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (תקן ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + תצוגת סורק | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (חושף אוטומטית כל טבלה מרוחקת) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` לפי-טבלה (עמודות מהרישום) | `ch_engine`, `engine_template` (עשוי לשאת `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse מסיק את הסכמה | `ch_engine`, `engine_template` |

**Postgres הוא גנרי.** SQL/MED הוא תקן ISO, כך שכל FDW תואם חולק את אותה צורת DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, `CREATE USER MAPPING` אופציונלי, ולאחר מכן או `IMPORT FOREIGN SCHEMA` (כאשר `supports_import: true`) או `CREATE FOREIGN TABLE` מפורש לכל טבלה (כאשר `false`). תיאור `pg_fdw` מספק רק את השונות לפי-FDW — שם תוסף, מפתחות אפשרויות שרת, מפתחות מיפוי-משתמש, דגל import, אפשרויות טבלה. כל FDW תואם-תקן ניתן אפוא להנעה מתצורה בלבד. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB תומכת בשני מנגנונים.** הרחבה החושפת קטלוג דרך ATTACH משתמשת ב-`duckdb_attach`; הרחבה החושפת פונקציית-טבלה לקריאה משתמשת ב-`duckdb_scan`. הרחבה שאינה מתאימה לאף דפוס אינה נתמכת. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse תומכת בשלושה מנגנונים**, אחד לכל צורת מנוע-אינטגרציה: מנוע DATABASE רלציוני החושף אוטומטית כל טבלה מרוחקת (`clickhouse_database`, לדוגמה Redis/MySQL), מנוע לפי-טבלה שהעמודות שלו מספק הרישום (`clickhouse_table`, לדוגמה גשר JDBC/ODBC — ה-`engine_template` עשוי לשאת placeholder‏ `{table}` שזמן הריצה קושר), ומנוע קובץ/lake/URL שאת הסכמה שלו ClickHouse מסיק (`clickhouse_scan`, לדוגמה HDFS/URL). SQLite (מנוע DATABASE, קובץ, ללא שרת) ו-Hudi (lakehouse, zero-copy) נשלחים out-of-the-box. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

ערך `kind` לא-מוכר נכשל בקול רם בעת ההפעלה — שגיאת הקלדה בתיאור לא צריכה להשאיר סוג מקור בלתי-נגיש בשקט. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### שער בדיקה (Probe gating)

זמינות מאומתת בעת ה-attach מול קטלוג הגילוי הסטנדרטי של כל מנוע:

- **Postgres** — בודק `pg_extension`, ואז `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — מריץ `INSTALL`/`LOAD` ובודק את `duckdb_functions()` עבור ה-`probe_symbol` המוצהר. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — בודק `system.table_engines` עבור ה-`ch_engine` המוצהר; היעדרות מהבנייה נכשלת בקול רם. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

הרחבה מוצהרת שאינה ניתנת-להתקנה נכשלת בקול רם. אין דילוג שקט, אין נפילה-חוזרת. מחבר שהבדיקה שלו נכשלת פשוט אינו פעיל עבור אותה פריסה.

### משתני תבנית

כל ערך `server_options`, ערך `user_mapping`, `attach_template`, ו-`scan_template` יכולים להשתמש ב-placeholders מסוג `{field}`. שדות זמינים: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, בתוספת כל מפתח מ-`federation_hints`. תבניות attach של DuckDB גם מקבלות `{alias}` — כינוי הקטלוג הפנימי ש-Provisa מקצה למסד הנתונים המצורף.

תבנית המפנה לשדה לא-מוכר נכשלת בקול רם בעת ה-attach, וחושפת אי-התאמה תיאור/מקור לפני ש-DDL שבור מגיע למנוע.

### דוגמאות

**Postgres — MongoDB דרך `mongo_fdw` (ללא ייבוא סכמה; עמודות מסופקות לכל טבלה)**

```yaml
# config/custom_connectors.yaml
connectors:
  - engine: postgres
    source_type: mongodb
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping:
      username: "{username}"
      password: "{password}"
    supports_import: false
    table_options:
      database: "{database}"
      collection: "{table_name}"
```

**DuckDB — קבצי Excel דרך `read_xlsx` (פונקציית-טבלה לסריקה)**

```yaml
  - engine: duckdb
    source_type: xlsx
    kind: duckdb_scan
    extension: excel
    install_from_community: false
    probe_symbol: read_xlsx
    scan_template: "read_xlsx('{path}')"
```

[tool-verified: `config/custom_connectors.yaml` commented examples, lines 26–50]

עם כל אחד מהתיאורים במקום, רישום מקור עם ה-`source_type` המוצהר מנותב דרך המחבר המותאם-אישית, בכפוף לבדיקה מוצלחת. אין צורך בשינוי תצורה נוסף.

---

## Warehouses כמקורות בעלי-שם

Snowflake, Databricks, ו-ClickHouse יכולים להירשם כמקורות בעלי-שם באופן בלתי-תלוי באיזה מנוע פדרציה פעיל. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

כאשר רשום, Provisa קוראת את ה-warehouse דרך ה-DirectDriver של המקור ומנחיתה replica למאגר המימוש של המנוע הפעיל. השאילתה אז רצה מול אותו replica. זה שונה מהנתיב המסורתי בעל-יכולת-ישירה (asyncpg, aiomysql) שבו המנוע נעקף לחלוטין — כאן המנוע עדיין מבצע את השאילתה, אך מול replica מקומי במקום על החוט אל ה-warehouse בכל בקשה.

קריאות הן ילידיות-Arrow היכן שה-warehouse תומך בכך: Databricks משתמש ב-Cloud Fetch, Snowflake משתמש ב-`fetch_arrow_table`, ו-ClickHouse משתמש בממשק HTTP העמודתי הילידי.

פרמטרי חיבור מורחבים שהשדות הסטנדרטיים `host`/`port`/`username`/`password` אינם יכולים לשאת נכנסים ל-`federation_hints`:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required — the SQL Warehouse connection detail

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount    # required — Snowflake account identifier
      warehouse: COMPUTE_WH       # optional — virtual warehouse to use
      role: PROVISA_ROLE          # optional — Snowflake role

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    database: analytics
    username: default
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"              # optional — enables TLS on the HTTP interface
```

רישום כמקור בעל-שם הוא בלתי-תלוי בבחירת אותו warehouse כמנוע הפדרציה. מקור Snowflake על מנוע DuckDB מנחית replica ל-DuckDB, לא ל-Snowflake.

נתוני אחסון אובייקטים/lake בענן (קבצי parquet, csv, iceberg, delta_lake ב-S3 / GCS / R2) הם סוג מקור נפרד המצטרף (attach) במקומו כאשר למנוע הפעיל יש מחבר ATTACH עבור אותו סוג. לא מונחת replica — המנוע סורק את אחסון האובייקטים ישירות. אישורים עבור מקורות אלה נכנסים גם הם ל-`federation_hints`:

```yaml
sources:
  - id: r2-events
    type: parquet
    path: s3://my-bucket/events/2026/*.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}     # Cloudflare R2 account (S3-compatible)
```

---

## שדות תצורת מקור

כל המקורות חולקים קבוצת שדות משותפת. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| שדה | נדרש | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `id` | כן | — | מזהה ייחודי; אלפאנומרי עם מקפים/קווים תחתונים |
| `type` | כן | — | סוג מקור (ראו הטבלאות לעיל) |
| `host` | לא | `""` | שם host או IP |
| `port` | לא | `0` | מספר פורט |
| `database` | לא | `""` | שם מסד נתונים |
| `username` | לא | `""` | שם משתמש |
| `password` | לא | `""` | סיסמה; השתמשו ב-`${env:VAR}` לפתירת סוד |
| `path` | לא | `null` | נתיב קובץ או URI ענן עבור מקורות מבוססי-קובץ ואובייקט/lake |
| `base_url` | לא | `null` | URL בסיס עבור מקורות OpenAPI |
| `pool_min` | לא | `1` | גודל מזערי של pool חיבורים (REQ-052) |
| `pool_max` | לא | `5` | גודל מרבי של pool חיבורים (REQ-052) |
| `use_pgbouncer` | לא | `false` | ניתוב חיבורים דרך PgBouncer (REQ-053) |
| `pgbouncer_port` | לא | `6432` | פורט PgBouncer (REQ-053) |
| `cache_enabled` | לא | `true` | הפעלת מיטמון תגובת API |
| `cache_ttl` | לא | `null` | TTL מיטמון בשניות; יורש ברירת מחדל גלובלית כאשר null |
| `cache_catalog` | לא | `null` | קטלוג פדרטיבי עבור מיטמון API; ברירת מחדל לקטלוג של המקור עצמו |
| `cache_schema` | לא | `api_cache` | סכמה בתוך קטלוג המיטמון |
| `naming_convention` | לא | `null` | דריסת מוסכמת שם גלובלית עבור מקור זה (REQ-194) |
| `federation_hints` | לא | `{}` | מאפייני session המועברים למנוע הפדרציה, ופרמטרי חיבור מורחבים עבור מקורות warehouse (REQ-278, REQ-281) |
| `mapping` | לא | `{}` | הגדרות מחבר ספציפיות-לסוג עבור מקורות NoSQL ו-SaaS (לדוגמה `auth_type` של SharePoint, `use_token` של Splunk) (REQ-251) |
| `allowed_domains` | לא | `[]` | הגבלת המקור לדומיינים ספציפיים; ריק = ללא הגבלה |
| `description` | לא | `""` | תיאור קריא-לאדם |

---

## מקורות Kafka

טופיקים של Kafka מוגדרים בנפרד תחת `kafka_sources`, ממופתחים לפי ה-`id` של מקור `kafka` רשום. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| שדה | תיאור |
| ------- | ------------- |
| `id` | חייב להתאים ל-`id` של מקור עם `type: kafka` |
| `topics[].id` | שם לוגי לטופיק זה בתוך Provisa |
| `topics[].topic` | שם טופיק Kafka |
| `topics[].domain_id` | הדומיין שאליו שייך טופיק זה |
| `topics[].description` | תיאור קריא-לאדם |
| `topics[].default_window` | חלון זמן ברירת מחדל עבור שאילתות מחולנות (לדוגמה `1h`) (REQ-148) |
| `topics[].columns` | הגדרות עמודה עבור סכמת הטופיק (REQ-150) |

---

## נראות עמודה

השדה `visible_to` על כל עמודה הוא רשימת מזהי תפקיד שיכולים לראות אותה עמודה. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

עמודות שהושמטו מרשימת ה-`visible_to` של תפקיד אינן מופיעות בסכמת GraphQL של אותו תפקיד ולא ניתן לשאול אותן או להפנות אליהן בפילטרים (REQ-039).

---

## קשרים

קשרים מחברים שתי טבלאות רשומות ומופיעים כשדות מקוננים ב-GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| שדה | נדרש | תיאור |
| ------- | ---------- | ------------- |
| `id` | כן | מזהה ייחודי לקשר זה |
| `source_table_id` | כן | הטבלה המחזיקה את המפתח הזר |
| `target_table_id` | כן | הטבלה שאליה מפנים; ריק עבור קשרים מחושבים |
| `source_column` | כן | עמודה בטבלת המקור |
| `target_column` | כן | עמודה בטבלת היעד; ריק עבור קשרים מחושבים |
| `cardinality` | כן | `many-to-one` או `one-to-many` (REQ-019) |
| `materialize` | לא | יצירה אוטומטית של materialized view עבור joins חוצי-מקורות (REQ-158) |
| `refresh_interval` | לא | מרווח רענון MV בשניות (ברירת מחדל: 300) |
| `target_function_name` | לא | שם פונקציית DB עבור קשרים מחושבים |
| `function_arg` | לא | איזה ארגומנט פונקציה מקבל את ערך עמודת המקור |
| `alias` | לא | סוג קשר קריא-לאדם (לדוגמה `WORKS_FOR`) |
| `graphql_alias` | לא | שם שדה ה-SDL שקשר זה חושף על טיפוס ההורה. כאשר נעדר, השם נגזר מה-`field_name` של טבלת היעד ומ-cardinality הקשר. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | לא | כאשר `true`, מוציא קשר זה מקצוות גרף Cypher |
| `source_json_key` | לא | מוציא מפתח זה מעמודת המקור כאובייקט JSON לפני JOIN |

ערכי Cardinality [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — כל שורת מקור ממופה לשורת יעד אחת (FK ל-PK)
- `one-to-many` — כל שורת מקור ממופה למספר שורות יעד (הפוך מלעיל)

---

## כללי אבטחה ברמת השורה

כללי RLS מזריקים סעיפי `WHERE` בזמן שאילתה, בהיקף לתפקיד ואופציונלית לטבלה או דומיין. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

כאשר קיימים גם כלל ברמת-דומיין וגם כלל ברמת-טבלה עבור אותו תפקיד, הכלל ברמת-הטבלה גובר (REQ-403).

| שדה | נדרש | תיאור |
| ------- | ---------- | ------------- |
| `table_id` | מותנה | טבלה שעליה חל הכלל; בלעדי הדדית עם `domain_id` |
| `domain_id` | מותנה | דומיין שעליו חל הכלל; חל על כל הטבלאות בדומיין (REQ-402) |
| `role_id` | כן | התפקיד שעליו חל כלל זה |
| `filter` | כן | predicate‏ SQL המוזרק ל-`WHERE`; יכול להפנות למשתני session (REQ-041) |

---

## פונקציות ו-Webhooks

### פונקציות DB

מעקב אחר פונקציית מסד נתונים וחשיפתה כשאילתת או מוטציית GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

מקורות מסד נתונים יכולים גם לגלות אוטומטית procedures ופונקציות מאוחסנות מקטלוג הספק (`pg_proc`, `information_schema.routines`, או שוות-ערך ספק), ומסירים את הצורך לרשום כל אחת ידנית. הגילוי קורא `prokind` ו-`provolatile`: פונקציות immutable/stable נרשמות כרלציות עם פרמטרים (ארגומנטי proc הופכים לפרמטרי שאילתה, אותה צורה כמו טבלאות GET‏ OpenAPI), ופרוצדורות volatile נרשמות כמוטציות/פונקציות במעקב. routines מגולים זורמים דרך ממשל שלב-2 באופן זהה לרשומים-ידנית. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| שדה | נדרש | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `name` | כן | — | שם שדה GraphQL |
| `source_id` | כן | — | המקור המכיל את הפונקציה |
| `schema` | לא | `public` | סכמת מסד נתונים |
| `function_name` | כן | — | שם פונקציית מסד הנתונים בפועל |
| `returns` | כן | — | מזהה טבלה רשום שהפונקציה מחזירה (REQ-207) |
| `arguments` | לא | `[]` | רשימת הגדרות ארגומנט `{name, type}` (REQ-211) |
| `visible_to` | לא | `[]` | תפקידים שיכולים לקרוא לפונקציה זו |
| `writable_by` | לא | `[]` | תפקידים שיכולים לקרוא לזו כמוטציה |
| `domain_id` | לא | `""` | הדומיין שאליו שייכת פונקציה זו |
| `description` | לא | `null` | תיאור שדה GraphQL |
| `kind` | לא | `mutation` | `"query"` או `"mutation"` (REQ-205) |

### Webhooks

חשיפת נקודת קצה HTTP חיצונית כשאילתת או מוטציית GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| שדה | נדרש | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `name` | כן | — | שם שדה GraphQL |
| `url` | כן | — | URL נקודת קצה webhook |
| `method` | לא | `POST` | שיטת HTTP |
| `timeout_ms` | לא | `5000` | timeout בקשה במילישניות |
| `returns` | לא | `null` | מזהה טבלה רשום, או null עבור טיפוס inline |
| `inline_return_type` | לא | `[]` | רשימת שדות `{name, type}` עבור צורות החזרה מותאמות אישית (REQ-210) |
| `arguments` | לא | `[]` | רשימת הגדרות ארגומנט `{name, type}` |
| `visible_to` | לא | `[]` | תפקידים שיכולים לקרוא ל-webhook זה |
| `domain_id` | לא | `""` | הדומיין שאליו שייך webhook זה |
| `description` | לא | `null` | תיאור שדה GraphQL |
| `kind` | לא | `mutation` | `"query"` או `"mutation"` |

---

## אימות

האימות מוגדר תחת המפתח `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| ספק | תיאור |
| ---------- | ------------- |
| `none` | ללא אימות; כל הבקשות מטופלות כ-`default_role` |
| `firebase` | Firebase Authentication; דורש `project_id` ו-`service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | OAuth 2.0 גנרי (REQ-123) |
| `simple` | שם משתמש/סיסמה ללא ספק חיצוני (REQ-124) |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` קורא הקצאות תפקיד מ-claims של JWT. `assignments_source: provisa` קורא אותן ממאגר ההקצאה הפנימי של Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## ניתוב ביצוע

**ביצוע ישיר** — שאילתות RDBMS מקור-יחיד מנותבות לדרייבר הילידי עבור latency תת-100 מ"ש (REQ-027). מקורות דורשים גם רשומת `SOURCE_TO_DIALECT` וגם רשומת `SOURCE_TO_CONNECTOR` כדי לתמוך בנתיב זה (REQ-229).

**ביצוע פדרטיבי** — שאילתות רב-מקוריות ומקורות ללא דרייבר ישיר מנותבים דרך מנוע הפדרציה (REQ-028). Provisa כוללת מנוע פדרציה מוטמע; הפנו לאשכול תואם משלכם עבור פריסות בקנה-מידה גדול (REQ-226).

**סטטיסטיקה** — ברישום, Provisa מריצה `ANALYZE` מול כל טבלה מפורסמת כדי להכין את האופטימייזר מבוסס-העלות (ספירות שורות, שבר null, ערכים נבדלים, מינימום/מקסימום). כשלים נרשמים ואינם חוסמים רישום (REQ-275).

---

## מקורות גרף וסמנטיים

### Neo4j

רישום מסד נתוני גרף Neo4j כמקור ניתן-לשאילתה. סטיוארדים כותבים שאילתות Cypher המקרינות ערכים סקלריים; Provisa ממטמנת תוצאות וחושפת אותן כטיפוסי GraphQL (REQ-295).

שאילתות Cypher חייבות להשתמש בגישת מאפיינים בסעיף ה-`RETURN` (‏`RETURN n.id AS id, n.name AS name`) — החזרת אובייקטי node נדחית בזמן הרישום (REQ-296).

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

נקודת הקצה של תצוגה מקדימה (`POST /admin/sources/neo4j/{id}/preview`) מחזירה שורות דוגמה וחוסמת רישום אם ה-Cypher מחזיר אובייקטי node (REQ-296).

### SPARQL

רישום כל triplestore תואם SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog, וכו') כמקור ניתן-לשאילתה (REQ-297).

שאילתות חייבות להיות שאילתות `SELECT`. שמות משתנה בסעיף ה-`SELECT` הופכים לשמות עמודה אוטומטית (REQ-297).

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

שני המחברים משתמשים בצינור מיטמון מקור-API — תוצאות מאוחסנות ב-PostgreSQL עם TTL ניתן-להגדרה, מה שהופך אותן לזמינות עבור JOIN-ים פדרטיביים חוצי-מקורות (REQ-295, REQ-297, REQ-299).

---

## דוגמאות חיבור

### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
  federation_hints:
    account: myorg-myaccount
    warehouse: COMPUTE_WH
```

### Databricks

```yaml
- id: lakehouse-db
  type: databricks
  host: my-workspace.azuredatabricks.net
  password: ${env:DATABRICKS_TOKEN}
  federation_hints:
    http_path: /sql/1.0/warehouses/xxxx
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### שאילתה חוצת-מקורות

```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

חלקים של מקור-יחיד מנותבים ישירות (REQ-027). JOIN-ים חוצי-מקורות מתפדרטים עם כפיית טיפוס אוטומטית (REQ-028, REQ-552).
</content>
