# סוגי מקורות

## מודל הביצוע

כל שאילתה מבוצעת בסופו של דבר דרך מנוע הפדרציה, המספק פדרציה על פני כל המקורות. המקורות מתחלקים לשלוש קטגוריות בהתאם לקישוריות שלהם. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| קטגוריה | יש דרייבר ישיר | יש מחבר מפודרר | דוגמאות |
| --- | --- | --- | --- |
| **תמיכה ישירה** | כן | כן | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **פדרציה בלבד** | לא | כן | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-backed) |
| **קריאה ישירה (עותק משוכפל)** | כן | כן | Snowflake, Databricks, ClickHouse — הדרייבר קורא נתונים ומנחית עותק משוכפל; השאילתות רצות מול העותק המשוכפל במנוע הפעיל |
| **מטריאליזציה → פדרציה** | לא | לא | REST/OpenAPI, remote GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (push receiver), GovData, SharePoint, Splunk |

מקורות מסוג **תמיכה ישירה** מבצעים שאילתות חד-מקוריות דרך הדרייבר הילידי שלהם (מתחת ל-100ms), תוך עקיפת מנוע הפדרציה (REQ-027, REQ-229). הם שומרים על תמיכה מלאה במחבר ומשתתפים בפדרציה כשהם מוצטרפים (joined) עם מקורות אחרים (REQ-028).

מקורות מסוג **פדרציה בלבד** נשאלים תמיד דרך שכבת הפדרציה. לא קיים דרייבר ישיר (REQ-229).

למקורות מסוג **קריאה ישירה (עותק משוכפל)** יש DirectDriver שקורא מהמחסן באופן ילידי (Arrow-native היכן שזמין), מנחית עותק משוכפל למחסן המטריאליזציה של המנוע הפעיל, ולאחר מכן השאילתות רצות מול אותו עותק משוכפל. ראו [Warehouses as Named Sources](#_10).

למקורות **מטריאליזציה** אין מחבר מפודרר. Provisa שולפת את הנתונים שלהם (בעת ההפעלה או בזמן השאילתה) ושומרת אותם במטמון כ-Parquet ב-S3 או ב-PostgreSQL, כך שהם נגישים למנוע הפדרציה לצורך שאילתות חוצות מקורות (REQ-309).

---

## כל המקורות

מסמך עזר לכל סוג מקור שנתמך על ידי Provisa. "דרייבר ישיר" פירושו ששאילתות חד-מקוריות מבוצעות מול המקור באופן ילידי (מתחת ל-100ms) (REQ-027). "שם המחבר" הוא המחבר המפודרר שבו נעשה שימוש כאשר המקור משתתף ב-JOIN רב-מקורי (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| סוג מקור | דרייבר ישיר | שם המחבר | דיאלקט | מוטציות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | כן |
| `mysql` | aiomysql | mysql | mysql | כן |
| `mariadb` | aiomysql | mariadb | mysql | כן |
| `singlestore` | — | singlestore | singlestore | מפודרר |
| `sqlserver` | aioodbc | sqlserver | tsql | כן |
| `oracle` | oracledb | oracle | oracle | כן |
| `duckdb` | duckdb | memory | duckdb | כן |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | כן |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | כן |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | כן |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | כן |

מסדי נתונים תואמי-wire עושים שימוש חוזר ב-JDBC driver, בדרייבר האסינכרוני הילידי ובדיאלקט של ה-wire הבסיסי — CockroachDB, YugabyteDB ו-Greenplum רוכבים על ה-wire של PostgreSQL; TiDB רוכב על ה-wire של MySQL. הם זקוקים רק לרשומות רישום, ללא קוד מחבר חדש. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) ו-`airport` (Arrow Flight server) הם סוגי מקור רשומים המושגים במקום דרך תוספי הקהילה של DuckDB כאשר DuckDB הוא המנוע הפעיל — ללא דרייבר ישיר, ללא מחבר מפודרר. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### מחסני נתונים בענן

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| סוג מקור | דרייבר ישיר | שם המחבר | דיאלקט | מוטציות | הערות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | מפודרר | קורא באמצעות snowflake-connector-python; מנחית עותק משוכפל; `account`/`warehouse`/`role` ב-`federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | מפודרר | אין DirectDriver; מושג דרך מנוע הפדרציה או ATTACH של מנוע BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | מפודרר | קורא באמצעות databricks-sql-connector (Cloud Fetch, Arrow); מנחית עותק משוכפל; `http_path` נדרש ב-`federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | מפודרר | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | מפודרר | Microsoft Fabric Warehouse; T-SQL מעל TDS, אימות Azure AD; מנחית עותק משוכפל (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | מפודרר | Azure Synapse SQL; T-SQL מעל TDS, אימות Azure AD; מנחית עותק משוכפל (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | מפודרר | קריאה של קואורדינטור Trino/Presto מרוחק דרך דיאלקט ה-trino של SQLAlchemy; מנחית עותק משוכפל על כל מנוע (REQ-994) |

### אנליטיקה / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| סוג מקור | דרייבר ישיר | שם המחבר | דיאלקט | מוטציות | הערות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | מפודרר | קורא באמצעות clickhouse-connect (HTTP); `secure: "true"` ב-`federation_hints` עבור TLS (REQ-986) |
| `druid` | — | druid | druid | לא | — |
| `exasol` | — | exasol | exasol | לא | — |
| `elasticsearch` | — | elasticsearch | — | לא | מאפייני המחבר מגיעים מ-DSL המיפוי של הסוג [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | לא | מחבר `pinot` של Trino; `pinot.controller-urls` = host:port של בקר ה-Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / פורמטי טבלה פתוחים

סוגי המקור הבאים הם פדרציה-בלבד — ללא דרייבר ישיר, ללא דיאלקט. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| סוג מקור | שם המחבר | Time Travel | הערות |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | כן (ארגומנט `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | כן (ארגומנט `as_of`, REQ-372) | — |
| `hive` | hive | לא | — |
| `hive_s3` | hive | לא | Hive מגובה-S3 |

### NoSQL

ל-`mongodb`, `cassandra` ו-`redis` יש מחברי Trino (`redis` בונה את המאפיינים שלו מ-DSL המיפוי של הסוג). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| סוג מקור | שם המחבר | מוטציות |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | לא |
| `cassandra` | cassandra | לא |
| `redis` | redis | לא |

### סטרימינג

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `kafka` | מחבר Kafka מפודרר; סכמה דרך Confluent Schema Registry (Avro, Protobuf, JSON Schema), הגדרה ידנית, או הסקה מדגמית (REQ-147, REQ-150) | Sink בלבד (REQ-176) |
| `websocket` | הזנת WebSocket חיצונית — התחברות, הרשמה, קבלת אירועים; התוצאות עוברות מטריאליזציה (REQ-338) | לא |
| `rss` | הזנת RSS 2.0 / Atom — בדיקה תקופתית (poll), סימון מים לפי pubDate/updated; התוצאות עוברות מטריאליזציה (REQ-342, REQ-343) | לא |

### Push Receiver

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `ingest` | שירותים חיצוניים שולחים POST של אירועי JSON; התוצאות עוברות מטריאליזציה (REQ-331, REQ-335) | לא |

### גרף וסמנטיקה

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher דרך HTTP API, התוצאות נשמרות במטמון ב-PostgreSQL (REQ-295) | לא |
| `sparql` | POST של SPARQL 1.1, התוצאות נשמרות במטמון ב-PostgreSQL (REQ-297) | לא |

### מבוססי קובץ

שני מנגנונים מכסים קבצים. שניהם משתמשים בשדה `path` במקום `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**מקורות חד-קובציים** — `sqlite`, `csv`, `parquet` מפנים את `path` לקובץ בודד.

| סוג מקור | תעבורות | מוטציות |
| --- | --- | --- |
| `sqlite` | מקומי | כן |
| `csv` | מקומי | לא |
| `parquet` | מקומי, `s3://` | לא |

דליים (buckets) פרטיים דורשים אישורים (region ומפתחות AWS מהסביבה). עבור CSV מעל `s3://` או `http(s)://`, או כדי לרשום קבצים רבים בבת אחת, השתמשו במקור `files`. [tool-verified: `provisa/file_source/source.py`]

**מקור `files`** — מפנה את `path` ל-glob, סורק אותו רקורסיבית, ורושם את הספרייה כקטלוג מפודרר של טבלאות. הוא קורא פורמטים רבים על פני תעבורות רבות; הקבוצות שלהלן מגיעות ממחבר הקבצים (kenstott/calcite fork). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| פורמטים | תעבורות |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, ומסמכים המומרים לטבלאות — HTML, Markdown, DOCX, PPTX | מערכת קבצים מקומית, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST ו-Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observability ואחרים

ל-`prometheus` יש מחבר Trino (מאפיינים הנבנים מ-DSL המיפוי של הסוג). `google_sheets` הוא סוג מקור רשום ללא מחבר Trino, ועובר מטריאליזציה דרך צינור המטמון של ה-API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| סוג מקור | שם המחבר | מוטציות |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (מטריאליזציה) | לא |
| `prometheus` | prometheus | לא |

### מחברי SaaS ארגוניים

SharePoint ו-Splunk נרשמים דרך מחברי Apache Calcite (kenstott/calcite fork). לאף אחד מהם אין דרייבר ישיר — Provisa מבצעת מטריאליזציה של השורות שלהם על ידי הפעלת שרת ה-pgwire המצורף של המחבר (`pgwire-sharepoint`, `pgwire-splunk`), התחברות אליו כנקודת קצה גנרית של PostgreSQL, והנחתת השורות למחסן המטריאליזציה לצורך פדרציה (REQ-954). שני המחברים מפעילים תמיד התאמת שמות שאינה תלוית רישיות, בהתאמה לסמנטיקה שאינה תלוית-רישיות של כל מוצר (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

רשימות SharePoint נמנות כסכמות וחשופות כטבלאות ניתנות לשאילתה (REQ-726, REQ-731). שתי שיטות אימות: `CLIENT_CREDENTIALS` (ברירת מחדל) ומבוססת-תעודה (certificate) דרך תעודת PFX (REQ-727). ערכי סוד ב-`mapping` נפתרים דרך מנוע הסודות לפני שהם מגיעים למחבר (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| שדה מקור | מאפיין מחבר | הערות |
| --- | --- | --- |
| `base_url` או `host` | `site-url` | כתובת URL של אתר SharePoint |
| `username` | `client-id` | מזהה client של אפליקציית Azure |
| `password` | `client-secret` | סוד client של אפליקציית Azure |
| `database` | `tenant-id` | UUID דייר (tenant) של Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (ברירת מחדל) או `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | נתיב PFX כאשר `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | סיסמת PFX |

כאשר המחבר לא חושף `information_schema.columns`, יש לרשום את הטבלה עם הגדרות עמודה מפורשות (המתקבלות מ-Microsoft Graph API) דרך המוטציה `registerTable` (REQ-732).

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

תוצאות חיפוש Splunk ניתנות לשאילתה כטבלאות (לדוגמה `internal_server`) (REQ-721). כתובת ה-URL של המחבר מגיעה מ-`base_url`, או נבנית כ-`https://{host}:{port}` עם פורט ברירת מחדל `8089` (REQ-722). אימות: כאשר `mapping.use_token` הוא `true` (ברירת המחדל), `password` מועבר כאסימון ה-API; כאשר `false`, `username` ו-`password` מועברים כאישורים נפרדים (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| שדה מקור | מאפיין מחבר | הערות |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, אחרת `https://host:port` (ברירת מחדל לפורט 8089) |
| `password` | `token` או `password` | אסימון כאשר `use_token: true` |
| `username` | `user` | רק כאשר `use_token: false` |
| `database` | `app` | הגבלה לאפליקציית Splunk מסוימת |
| `mapping.datamodel_filter` | `datamodel-filter` | סינון למודל נתונים מסוים |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | עבור תעודות בחתימה עצמית (REQ-724) |

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

רישום כל נקודת קצה HTTP כטבלה ניתנת לשאילתה. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| סוג API | גילוי | הסקת עמודות |
| --------- | ----------- | ----------------- |
| `openapi` | ניתוח מפרט OpenAPI (REQ-314, REQ-316) | פרימיטיבים → ילידי, אובייקטים → JSONB |
| `graphql_remote` | Schema introspection (REQ-307, REQ-308) | פרימיטיבים → ילידי, אובייקטים → JSONB |
| `grpc_remote` | Server reflection (REQ-322, REQ-325) | פרימיטיבים → ילידי, אובייקטים → JSONB |

תגובות API נשלפות, נשמרות במטמון ב-PostgreSQL (TTL הניתן להגדרה), וחשופות כטיפוסי GraphQL (REQ-309, REQ-318, REQ-327). טבלאות שנשמרו במטמון משתתפות בשאילתות מפודררות כמו כל מקור אחר (REQ-313).

**כללי JSONB**: עמודות מורכבות (אובייקטים, מערכים) המאוחסנות כ-JSONB אינן ניתנות לסינון (REQ-119). גישה לתת-שדה משתמשת בחילוץ `->>` ב-SQL (REQ-151). קשרים (relationships) מוצהרים בין טבלאות באמצעות עמודות FK סקלריות — עמודות blob מסוג JSONB אינן יעדי join. השתמשו בקידום JSONB (JSONB promotion) כדי להמיר שדות מקוננים לעמודות סקלריות ילידיות כאשר נדרש לסנן או להצטרף (join) אליהן (REQ-119).

### GovData

נתונים פתוחים של ממשלת ארה"ב. הגישה מחולקת לפי קיבוץ נושאים. [tool-verified: `provisa/core/models.py` lines 543–609]

כל מקור `govdata` בוחר נושא אחד. אותו נושא קובע אילו סכמות GovData חשופות. הסכמות `ref` ו-`geo` נכללות תמיד כסכמות מקשרות (linker schemas) — הן אינן מופיעות ברשימה לפי נושא אך תמיד נוכחות. [tool-verified: `provisa/core/models.py` line 562–563 comment]

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
| `ALL` | כל סכמה שלעיל |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| שדה | חובה | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `id` | כן | — | מזהה ייחודי |
| `subject` | כן | — | אחד מערכי הנושא שלעיל |
| `domain_id` | כן | — | הדומיין שאליו שייך מקור זה |
| `description` | לא | `""` | תיאור קריא לבני אדם |

### בודקי איכות נתונים (REQ-1443)

בודק איכות נתונים הוא סוג מקור, לא תת-מערכת. פלט הסריקה שלו הוא נתונים: תוצאת בדיקה היא תצפית, כך שהיא עוברת דרך נתיב המקור הרגיל ויורשת קדנס (cadence), רעננות (freshness), אירועים, Data Lineage, ממשל, RLS, grid וייצוא מכל מקור אחר. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

שניים נתמכים, והבחירה היא בחירת רישוי לא פחות מבחירת פיצ'ר.

| סוג מקור | דיאלקט חוזה (Contract) | תוסף | רישיון | מישור ענן מאוחסן |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda contract YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | נדחה — ראו בהמשך |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | מותר |

Elastic License 2.0 אוסר לספק את התוכנה לצדדים שלישיים כשירות מאוחסן או מנוהל, והרצת Soda בתוך מישור ה-SaaS בשם דייר (tenant) היא בדיוק זה. `config/capabilities.yaml` נושא את החלוקה כ-`cloud_eligible: false` על האפשרות `soda`, והמישור המאוחסן קורא את הדגל הזה. פריסה מאוחסנת שרוצה Soda מגיעה לנקודת קצה של Soda המסופקת על ידי המפעיל (operator) ומורצת על ידיו. [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa לא מספקת (vendors) ולא מקשרת (links) שום דבר. הסריקה רצה בתוך פרשן ילד (child interpreter) (`python -m provisa.dq.worker`), שהוא המקום היחיד שבו `soda_core` או `great_expectations` מיובאים, כך שבודק source-available לעולם לא מגיע לתהליך השרת, וקריסת בודק הורגת תת-תהליך (subprocess) ולא את לולאת האירועים (event loop). [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**המקור מצביע על נקודת הקצה של pgwire של Provisa עצמה.** זה מה שמאפשר לדרייבר postgres אחד לבדוק טבלה מגובה-Snowflake או מגובה-Iceberg: הבודק סורק את התצוגה המפודררת, לא את המערכת שמתחת. מכיוון שהמדיניות חלה על אותו חיבור, זהות הסריקה מוצהרת ולא נורשת — קבוצת שורות מסוננת אסור לה בשום פנים ואופן לגרום לבדיקה שעוברת בשקט.

```yaml
sources:

  - id: dq
    type: soda
    domain_id: sales-analytics
    description: Soda contract scans over the governed estate
    mapping:
      host: localhost
      port: 5439          # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner    # the scan identity, declared explicitly
      password: ${env:PROVISA_DQ_PASSWORD}
```

**טבלת תוצאות אחת לכל חוזה, והחוזה הוא כל הרישום.** הטבלה נושאת את `dq_contract` — טקסט החוזה מילה במילה — ושום דבר נוסף על צורתה. עמודות, watermark וקידומים כולם נגזרים. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

```yaml
tables:

  - source_id: dq
    schema_name: quality
    table_name: orders_scan
    domain_id: sales-analytics
    change_signal: ttl_probe
    cache_ttl: 3600
    columns:
      - name: scan_id          # declared only to carry visible_to; replaced at parse
        visible_to: [analyst, admin]
    dq_contract: |
      dataset: provisa/sales/orders
      columns:
        - name: customer_id
          checks:
            - missing:
                threshold:
                  metric: percent
                  must_be_less_than: 1
      checks:
        - row_count:
            must_be_greater_than: 0
```

מה שהרישום גוזר מהטקסט הזה:

- **Data Lineage.** החוזה כבר נוקב בשם ערכת הנתונים היעד שלו, כך שהרישום מנתח אותו באותו אופן שבו `extract_inputs` מנתח SQL (REQ-939) ומפענח אותו לטבלה המנוהלת. הגדרה אחת, ללא עותק שני שעלול לסטות. חוזה הנוקב בשם ערכת נתונים לא-מנוהלת נכשל בקול רם בזמן הרישום במקום להנחית שורות שאף אחד לא ביקש.
- **עמודות.** מעטפת התוצאה שייכת לבודק, לא למפעיל — 16 עמודות מסופקות, מ-`scan_id` ועד `diagnostics`. עמודות מוצהרות נקראות רק עבור ה-`visible_to` שלהן, שחייב להיות פה אחד, ולאחר מכן מוחלפות. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Watermark.** `scan_time` הופך ל-watermark, מה שהופך את ההנחתה ל-append (REQ-982). היסטוריית סריקות מצטברת ללא תת-מערכת היסטוריה.
- **קידומים.** `freshness_max_timestamp` ו-`dataset_rows_tested` מקודמים מתוך ה-jsonb `diagnostics` כעמודות מוקלדות (REQ-119). ניתן להוסיף עוד באותו אופן שבו עושים זאת בכל עמודת jsonb אחרת. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

התזמון (Timing) אינו מוסיף שדות חדשים. `change_signal` יחד עם `cache_ttl` נותנים את קדנס הבדיקה (poll cadence); `mv_debounce_quiet` ו-`mv_debounce_max_delay` מקפלים פרץ עליון (upstream burst) לסריקה אחת (REQ-963); דרגת לוח שנה (calendar grain) הופכת אותה לתקופתית (REQ-962); `expected_events` עוצר את הסריקה עד שהקלטים שלה טריים לאורך החלון (REQ-961). לולאת ה-poll היא מתזמן הסריקה.

`outcome` הוא אחד מ-`pass`, `fail`, `warn`, `error`, `skipped`. אף אחד מהם אינו פסק דין — אכיפה, אם רוצים, היא הצהרה נפרדת מאוחר יותר: preflight או MV מעל התוצאות שהונחתו. מכיוון שתצפית שהונחתה אינה נושאת מחויבות דטרמיניזם (REQ-964), בדיקות לא-דטרמיניסטיות קבילות כאן שלעולם לא יכלו לשבת על שער preflight — ציון אנומליה, שינוי בחלון נגרר, רעננות מול עכשיו.

החוזה נכתב בממשק המשתמש, במשטח עריכת הטבלה, בפאנל איכות הנתונים, וטקסט החוזה הגולמי שם הוא תמיד מקור האמת. הרצה יבשה (dry run) מבצעת את החוזה מול הטבלה החיה ומציגה את התוצאות מבלי להנחית אותן — כך תופסים חוזה ששם ערכת הנתונים שלו פוענח למקום בלתי צפוי, ואחרת היה מנחית רק שורות עוברות.

---

## מחברים מותאמים אישית (REQ-1177)

מנועי הפדרציה הילידיים — Postgres, DuckDB ו-ClickHouse — משיגים נגישות לסוג מקור חדש כאשר מפעיל (operator) מצהיר על מחבר עבורו ב-`config/custom_connectors.yaml`. לא נדרש קוד. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

הרחבת מחברים כשלעצמה קדמה לזה. מנוע Trino ניתן להרחבה זה מכבר בשכבה שלו — מחבר JDBC גנרי אחד שמפורמט לפי סוג מקור, גוף `.properties` של קטלוג לכל סוג, ותוספי מחבר Trino מותאמים אישית משל Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 מביא את אותה הרחבה מונעת-קונפיגורציה לשני המנועים הילידיים, ללא-אשכול (no-cluster), שקודם נשאו סט מחברים קבוע.

הקונפיגורציה נשלחת ריקה. מחברים מובנים מכסים נגישות מוכנה-מהקופסה; כל מה שבקובץ הזה נכתב על ידי המפעיל. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] הגדירו `PROVISA_CUSTOM_CONNECTORS` כדי להצביע על נתיב אחר (שימושי לבדיקות).

### סוגי מתאר (Descriptor)

| מנוע | סוג | מנגנון | מה המתאר מספק |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (תקן ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + תצוגת סורק | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (חושף אוטומטית כל טבלה מרוחקת) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` לכל טבלה (עמודות מהרישום) | `ch_engine`, `engine_template` (עשוי לשאת `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse מסיק את הסכמה | `ch_engine`, `engine_template` |

**Postgres הוא גנרי.** SQL/MED הוא תקן ISO, כך שכל FDW תואם חולק את אותה צורת DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, אופציונלית `CREATE USER MAPPING`, ואז או `IMPORT FOREIGN SCHEMA` (כאשר `supports_import: true`) או `CREATE FOREIGN TABLE` מפורש לכל טבלה (כאשר `false`). מתאר `pg_fdw` מספק רק את השונות הספציפית ל-FDW — שם התוסף, מפתחות אפשרויות שרת, מפתחות user-mapping, דגל import, אפשרויות טבלה. כל FDW תואם-תקן ניתן אפוא להפעלה מקונפיגורציה בלבד. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB תומך בשני מנגנונים.** תוסף החושף קטלוג דרך ATTACH משתמש ב-`duckdb_attach`; תוסף החושף פונקציית-טבלה לקריאה משתמש ב-`duckdb_scan`. תוסף שאינו מתאים לאף דפוס אינו נתמך. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse תומך בשלושה מנגנונים**, אחד לכל צורת integration-engine: מנוע DATABASE יחסי החושף אוטומטית כל טבלה מרוחקת (`clickhouse_database`, למשל Redis/MySQL), מנוע לכל-טבלה שהעמודות שלו מסופקות מהרישום (`clickhouse_table`, למשל הגשר JDBC/ODBC — ה-`engine_template` עשוי לשאת placeholder בצורת `{table}` שהריצה (runtime) מקשרת), ומנוע file/lake/URL שהסכמה שלו מוסקת על ידי ClickHouse (`clickhouse_scan`, למשל HDFS/URL). SQLite (מנוע DATABASE, קובץ, ללא שרת) ו-Hudi (lakehouse, zero-copy) נשלחים מוכנים-מהקופסה. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

ערך `kind` לא מוכר נכשל בקול רם בעת ההפעלה — טעות הקלדה במתאר אסור לה להשאיר בשקט סוג מקור בלתי נגיש. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### שערי בדיקה (Probe Gating)

הזמינות מאומתת בזמן ה-attach מול קטלוג הגילוי הסטנדרטי של כל מנוע:

- **Postgres** — בודק את `pg_extension`, ואז את `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — מריץ `INSTALL`/`LOAD` ובודק את `duckdb_functions()` עבור ה-`probe_symbol` המוצהר. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — בודק את `system.table_engines` עבור ה-`ch_engine` המוצהר; היעדרות מה-build נכשלת בקול רם. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

תוסף מוצהר שאינו ניתן להתקנה נכשל בקול רם. ללא דילוג שקט, ללא נפילה חזרה (fallback). מחבר שהבדיקה שלו נכשלת פשוט אינו פעיל עבור אותה פריסה.

### משתני תבנית

כל ערך `server_options`, ערך `user_mapping`, `attach_template`, ו-`scan_template` יכולים להשתמש ב-placeholders מסוג `{field}`. שדות זמינים: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, בתוספת כל מפתח מ-`federation_hints`. תבניות attach של DuckDB מקבלות גם `{alias}` — כינוי הקטלוג הפנימי ש-Provisa מקצה למסד הנתונים המצורף.

תבנית המפנה לשדה לא מוכר נכשלת בקול רם בזמן ה-attach, וחושפת אי-התאמה בין מתאר למקור לפני ש-DDL שבור מגיע למנוע.

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

**DuckDB — קובצי Excel דרך `read_xlsx` (פונקציית-טבלה לסריקה)**

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

עם מתאר כלשהו במקום, רישום מקור עם ה-`source_type` המוצהר מנותב דרך המחבר המותאם אישית, בכפוף לבדיקה מוצלחת. אין צורך בשינוי קונפיגורציה נוסף.

---

## מחסנים כמקורות בעלי שם

Snowflake, Databricks ו-ClickHouse ניתנים לרישום כמקורות בעלי שם באופן בלתי תלוי במנוע הפדרציה הפעיל. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

עם הרישום, Provisa קוראת את המחסן באמצעות ה-DirectDriver של המקור ומנחיתה עותק משוכפל למחסן המטריאליזציה של המנוע הפעיל. השאילתה רצה לאחר מכן מול אותו עותק משוכפל. זה שונה מהנתיב המסורתי בעל התמיכה הישירה (asyncpg, aiomysql) שבו המנוע נעקף לחלוטין — כאן המנוע עדיין מבצע את השאילתה, אך מול עותק משוכפל מקומי ולא דרך ה-wire למחסן בכל בקשה.

הקריאות הן Arrow-native היכן שהמחסן תומך בכך: Databricks משתמש ב-Cloud Fetch, Snowflake משתמש ב-`fetch_arrow_table`, ו-ClickHouse משתמש בממשק ה-HTTP העמודתי הילידי.

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

רישום כמקור בעל שם בלתי תלוי בבחירת אותו מחסן כמנוע הפדרציה. מקור Snowflake על מנוע DuckDB מנחית עותק משוכפל ל-DuckDB, לא ל-Snowflake.

נתוני object/lake בענן (קבצי parquet, csv, iceberg, delta_lake על S3 / GCS / R2) הם סוג מקור נפרד שמתחבר (attach) במקום כאשר למנוע הפעיל יש מחבר ATTACH לאותו סוג. לא מונחת עותק משוכפל — המנוע סורק את אחסון האובייקטים ישירות. אישורים (credentials) עבור מקורות אלה נכנסים אף הם ל-`federation_hints`:

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

## שדות קונפיגורציית מקור

כל המקורות חולקים סט משותף של שדות. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| שדה | חובה | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `id` | כן | — | מזהה ייחודי; אלפאנומרי עם מקפים/קווים תחתונים |
| `type` | כן | — | סוג מקור (ראו הטבלאות לעיל) |
| `host` | לא | `""` | Hostname או IP |
| `port` | לא | `0` | מספר פורט |
| `database` | לא | `""` | שם מסד נתונים |
| `username` | לא | `""` | שם משתמש |
| `password` | לא | `""` | סיסמה; השתמשו ב-`${env:VAR}` לפתרון סוד |
| `path` | לא | `null` | נתיב קובץ או URI ענן עבור מקורות מבוססי-קובץ ו-object/lake |
| `base_url` | לא | `null` | כתובת URL בסיסית עבור מקורות OpenAPI |
| `pool_min` | לא | `1` | גודל מינימלי של pool חיבורים (REQ-052) |
| `pool_max` | לא | `5` | גודל מקסימלי של pool חיבורים (REQ-052) |
| `use_pgbouncer` | לא | `false` | ניתוב חיבורים דרך PgBouncer (REQ-053) |
| `pgbouncer_port` | לא | `6432` | פורט PgBouncer (REQ-053) |
| `cache_enabled` | לא | `true` | הפעלת שמירה במטמון של תגובות API |
| `cache_ttl` | לא | `null` | TTL של המטמון בשניות; יורש ברירת מחדל גלובלית כאשר null |
| `cache_catalog` | לא | `null` | קטלוג מפודרר עבור מטמון ה-API; ברירת מחדל היא הקטלוג של המקור עצמו |
| `cache_schema` | לא | `api_cache` | סכמה בתוך קטלוג המטמון |
| `naming_convention` | לא | `null` | דריסת מוסכמת השמות הגלובלית עבור מקור זה (REQ-194) |
| `federation_hints` | לא | `{}` | מאפייני session המועברים למנוע הפדרציה, ופרמטרי חיבור מורחבים עבור מקורות מחסן (REQ-278, REQ-281) |
| `mapping` | לא | `{}` | הגדרות מחבר ספציפיות-סוג עבור מקורות NoSQL ו-SaaS (למשל `auth_type` של SharePoint, `use_token` של Splunk) (REQ-251) |
| `allowed_domains` | לא | `[]` | הגבלת המקור לדומיינים ספציפיים; ריק = ללא הגבלה |
| `description` | לא | `""` | תיאור קריא לבני אדם |

---

## מקורות Kafka

נושאי (topics) Kafka מוגדרים בנפרד תחת `kafka_sources`, ממופתחים לפי ה-`id` של מקור `kafka` רשום. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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
| `id` | חייב להתאים ל-`id` של מקור מסוג `type: kafka` |
| `topics[].id` | שם לוגי לנושא זה בתוך Provisa |
| `topics[].topic` | שם נושא Kafka |
| `topics[].domain_id` | הדומיין שאליו שייך נושא זה |
| `topics[].description` | תיאור קריא לבני אדם |
| `topics[].default_window` | חלון זמן ברירת מחדל עבור שאילתות מחולנות (windowed) (למשל `1h`) (REQ-148) |
| `topics[].columns` | הגדרות עמודה עבור סכמת הנושא (REQ-150) |

---

## נראות עמודות

השדה `visible_to` בכל עמודה הוא רשימת מזהי תפקיד (role) שיכולים לראות את אותה עמודה. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

עמודות שהושמטו מרשימת ה-`visible_to` של תפקיד לא מופיעות בסכמת ה-GraphQL של אותו תפקיד ולא ניתנות לשאילתה או להפניה במסננים (REQ-039).

---

## קשרים (Relationships)

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

| שדה | חובה | תיאור |
| ------- | ---------- | ------------- |
| `id` | כן | מזהה ייחודי לקשר זה |
| `source_table_id` | כן | הטבלה הנושאת את המפתח הזר |
| `target_table_id` | כן | הטבלה שאליה מפנים; ריק עבור קשרים מחושבים |
| `source_column` | כן | עמודה בטבלת המקור |
| `target_column` | כן | עמודה בטבלת היעד; ריק עבור קשרים מחושבים |
| `cardinality` | כן | `many-to-one` או `one-to-many` (REQ-019) |
| `materialize` | לא | יצירה אוטומטית של Materialized View עבור joins חוצי-מקורות (REQ-158) |
| `refresh_interval` | לא | מרווח רענון MV בשניות (ברירת מחדל: 300) |
| `target_function_name` | לא | שם פונקציית DB עבור קשרים מחושבים |
| `function_arg` | לא | איזה ארגומנט פונקציה מקבל את ערך עמודת המקור |
| `alias` | לא | סוג קשר קריא לבני אדם (למשל `WORKS_FOR`) |
| `graphql_alias` | לא | קובע את שם שדה ה-SDL שקשר זה חושף על הטיפוס ההורה. כאשר נעדר, השם נגזר מ-`field_name` של טבלת היעד ומקרדינליות הקשר. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | לא | כאשר `true`, מוציא קשר זה מקשתות הגרף של Cypher |
| `source_json_key` | לא | חילוץ מפתח זה מעמודת המקור כאובייקט JSON לפני JOIN |

ערכי קרדינליות [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — כל שורת מקור ממופה לשורת יעד אחת (FK ל-PK)
- `one-to-many` — כל שורת מקור ממופה למספר שורות יעד (הפוך מלמעלה)

---

## כללי אבטחה ברמת השורה

כללי RLS מזריקים משפטי `WHERE` בזמן השאילתה, בהיקף של תפקיד ואופציונלית של טבלה או דומיין. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

כאשר קיים גם כלל ברמת-דומיין וגם כלל ברמת-טבלה עבור אותו תפקיד, הכלל ברמת-הטבלה גובר (REQ-403).

| שדה | חובה | תיאור |
| ------- | ---------- | ------------- |
| `table_id` | מותנה | הטבלה שעליה חל הכלל; בלעדי הדדית עם `domain_id` |
| `domain_id` | מותנה | הדומיין שעליו חל הכלל; חל על כל הטבלאות בדומיין (REQ-402) |
| `role_id` | כן | התפקיד שעליו חל כלל זה |
| `filter` | כן | פרדיקט SQL המוזרק לתוך `WHERE`; יכול להפנות למשתני session (REQ-041) |

---

## פונקציות ו-Webhooks

### פונקציות DB

מעקב אחר פונקציית מסד נתונים וחשיפתה כשאילתת GraphQL או מוטציה. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

מקורות מסד נתונים יכולים גם לגלות אוטומטית את הפרוצדורות המאוחסנות והפונקציות שלהם מקטלוג הספק (`pg_proc`, `information_schema.routines`, או שווי-ערך של הספק), ולבטל את הצורך ברישום ידני של כל אחת. הגילוי קורא `prokind` ו-`provolatile`: פונקציות immutable/stable נרשמות כיחסים (relations) מפורמטים (ארגומנטי הפרוצדורה הופכים לפרמטרי שאילתה, אותה צורה כמו טבלאות OpenAPI GET), ופרוצדורות volatile נרשמות כמוטציות/פונקציות במעקב. פרוצדורות שהתגלו זורמות דרך ממשל שלב-2 (Stage-2 governance) באותו אופן כמו כאלה שנרשמו ידנית. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| שדה | חובה | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `name` | כן | — | שם שדה GraphQL |
| `source_id` | כן | — | המקור המכיל את הפונקציה |
| `schema` | לא | `public` | סכמת מסד נתונים |
| `function_name` | כן | — | שם פונקציית מסד הנתונים בפועל |
| `returns` | כן | — | מזהה טבלה רשומה שהפונקציה מחזירה (REQ-207) |
| `arguments` | לא | `[]` | רשימת הגדרות ארגומנט `{name, type}` (REQ-211) |
| `visible_to` | לא | `[]` | תפקידים שיכולים לקרוא לפונקציה זו |
| `writable_by` | לא | `[]` | תפקידים שיכולים לקרוא לזה כמוטציה |
| `domain_id` | לא | `""` | הדומיין שאליו שייכת פונקציה זו |
| `description` | לא | `null` | תיאור שדה GraphQL |
| `kind` | לא | `mutation` | `"query"` או `"mutation"` (REQ-205) |

### Webhooks

חשיפת נקודת קצה HTTP חיצונית כשאילתת GraphQL או מוטציה. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| שדה | חובה | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `name` | כן | — | שם שדה GraphQL |
| `url` | כן | — | כתובת URL של נקודת הקצה של ה-webhook |
| `method` | לא | `POST` | שיטת HTTP |
| `timeout_ms` | לא | `5000` | timeout בקשה במילישניות |
| `returns` | לא | `null` | מזהה טבלה רשומה, או null עבור טיפוס inline |
| `inline_return_type` | לא | `[]` | רשימת שדות `{name, type}` עבור צורות החזרה מותאמות אישית (REQ-210) |
| `arguments` | לא | `[]` | רשימת הגדרות ארגומנט `{name, type}` |
| `visible_to` | לא | `[]` | תפקידים שיכולים לקרוא ל-webhook זה |
| `domain_id` | לא | `""` | הדומיין שאליו שייך webhook זה |
| `description` | לא | `null` | תיאור שדה GraphQL |
| `kind` | לא | `mutation` | `"query"` או `"mutation"` |

---

## אימות (Authentication)

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

`assignments_source: claims` קורא שיוכי תפקיד מתביעות (claims) JWT. `assignments_source: provisa` קורא אותם ממחסן השיוכים של Provisa עצמה. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## ניתוב ביצוע

**ביצוע ישיר** — שאילתות RDBMS חד-מקוריות מנותבות לדרייבר הילידי עבור latency מתחת ל-100ms (REQ-027). מקורות דורשים גם רשומת `SOURCE_TO_DIALECT` וגם רשומת `SOURCE_TO_CONNECTOR` כדי לתמוך בנתיב זה (REQ-229).

**ביצוע מפודרר** — שאילתות רב-מקוריות ומקורות ללא דרייבר ישיר מנותבים דרך מנוע הפדרציה (REQ-028). Provisa כוללת מנוע פדרציה מוטמע (embedded); ניתן להצביע על אשכול (cluster) תואם משלכם עבור פריסות בקנה מידה גדול (REQ-226).

**סטטיסטיקה** — עם הרישום, Provisa מריצה `ANALYZE` מול כל טבלה שפורסמה כדי להכין את האופטימיזטור מבוסס-עלות (מספרי שורות, שבר null, ערכים ייחודיים, מינימום/מקסימום). כשלים נרשמים ביומן ואינם חוסמים את הרישום (REQ-275).

---

## מקורות גרף וסמנטיקה

### Neo4j

רישום מסד נתוני גרף Neo4j כמקור ניתן לשאילתה. Stewards כותבים שאילתות Cypher המקרינות ערכים סקלריים; Provisa שומרת את התוצאות במטמון וחושפת אותן כטיפוסי GraphQL (REQ-295).

שאילתות Cypher חייבות להשתמש בגישה למאפיין (property accessor) במשפט `RETURN` (‏`RETURN n.id AS id, n.name AS name`) — החזרת אובייקטי node נדחית בזמן הרישום (REQ-296).

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

רישום כל triplestore תואם SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog וכדומה) כמקור ניתן לשאילתה (REQ-297).

השאילתות חייבות להיות שאילתות `SELECT`. שמות משתנים במשפט ה-`SELECT` הופכים אוטומטית לשמות עמודות (REQ-297).

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

שני המחברים משתמשים בצינור המטמון של מקור ה-API — התוצאות מאוחסנות ב-PostgreSQL עם TTL הניתן להגדרה, מה שהופך אותן לזמינות עבור joins מפודררים חוצי-מקורות (REQ-295, REQ-297, REQ-299).

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

חלקים חד-מקוריים מנותבים ישירות (REQ-027). joins חוצי-מקורות מבצעים פדרציה עם המרת טיפוסים אוטומטית (REQ-028, REQ-552).
