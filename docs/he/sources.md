# סוגי מקורות {: #source-types }

## מודל הביצוע {: #execution-model }

כל שאילתה רצה בסופו של דבר דרך מנוע הפדרציה, וזה מה שמאפשר לצרף כל מקור לכל מקור אחר. איך מקור מגיע לשם תלוי באופן שבו Provisa מגיע אליו, וזה ממקם כל מקור באחת מחמש קבוצות.

| קטגוריה | יש דרייבר ישיר | יש מחבר מפודרר | דוגמאות |
| --- | --- | --- | --- |
| **תמיכה ישירה** | כן | כן | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **פדרציה בלבד** | לא | כן | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-backed) |
| **קריאה ישירה (עותק משוכפל)** | כן | כן | Snowflake, Databricks, ClickHouse — הדרייבר קורא נתונים ומנחית עותק משוכפל; השאילתות רצות מול העותק המשוכפל במנוע הפעיל |
| **ישיר (ללא מחבר חוצה-מקורות)** | כן | לא | Athena — הצהרות על מקור בודד רצות במלואן ב-Athena; הצטרפויות בין מקורות מנחיתות עותק משוכפל מלא |
| **מטריאליזציה → פדרציה** | לא | לא | REST/OpenAPI, remote GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (push receiver), GovData, SharePoint, Salesforce, ServiceNow, Cloud inventory, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

מקורות מסוג **תמיכה ישירה** עונים על שאילתה שנוגעת רק בהם דרך הדרייבר שלהם, בפחות מ-100ms, בלי לעבור דרך מנוע הפדרציה. הם שומרים על המחבר המלא שלהם, כך שהם עדיין מצטרפים למקורות אחרים דרך המנוע.

למקורות מסוג **פדרציה בלבד** אין דרייבר ישיר. כל שאילתה מגיעה אליהם דרך שכבת הפדרציה.

למקורות מסוג **קריאה ישירה (עותק משוכפל)** יש DirectDriver שקורא מהמחסן באופן ילידי (Arrow-native היכן שאפשר) ומנחית עותק משוכפל במחסן המטריאליזציה של המנוע הפעיל. השאילתות רצות אחר כך מול העותק הזה. ראו [Warehouses as Named Sources](#warehouses-as-named-sources).

למקורות מסוג **ישיר (ללא מחבר חוצה-מקורות)** יש DirectDriver שמריץ הצהרה על מקור בודד במלואה בתוך המקור, וכל מסנן, עמודה והגבלה מועברים אליו. אין להם מחבר Trino, ולכן כשהצהרה מצרפת אחד מהם למקור אחר, הדרייבר קורא את הנתונים ומנחית עותק משוכפל, וההצטרפות רצה מול העותק הזה במנוע הפעיל.

גם למקורות **מטריאליזציה** אין מחבר מפודרר. Provisa שולף את הנתונים שלהם, בעת ההפעלה או בזמן השאילתה, ושומר אותם במטמון כ-Parquet ב-S3 או ב-PostgreSQL. משם מנוע הפדרציה מגיע אליהם לשאילתות חוצות מקורות.

---

## כל המקורות {: #all-sources }

Provisa רושמת **73** סוגי מקור, אלה שטופס המקורות מציע. הטבלאות שלהלן מכסות את כל 73; המספור הוא הספירה.

| # | קבוצה | סוגי מקור |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [מחסני נתונים בענן](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [אנליטיקה / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data Lake / פורמטי טבלה פתוחים](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [מקורות קוד](code-sources.md) | `node`, `python` |
| 41–43 | [סטרימינג](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Push Receiver](#push-receiver) | `ingest` |
| 45–46 | [גרף וסמנטיקה](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [מבוססי קובץ](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observability ואחרים](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [מחברי SaaS ארגוניים](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [מקורות API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [בודקי איכות נתונים](#data-quality-checkers) | `soda`, `great_expectations` |
| 71 | [Data Profiler](#data-profiler) | `data_profiler` |
| 72 | [ערכות נתונים של Kaggle](#kaggle-datasets) | Kaggle (מוכן דרך טופס המקורות; נרשם כמקור מסוג `files` — ראו [ערכות נתונים של Kaggle](#kaggle-datasets)) |
| 73 | [Wikipedia](#wikipedia) | Wikipedia (נרשם כמקור מסוג `files` שסורק את הדפים הנקובים — ראו [Wikipedia](#wikipedia)) |

מסמך עזר לכל סוג מקור שנתמך על ידי Provisa. "דרייבר ישיר" פירושו ששאילתות חד-מקוריות מבוצעות מול המקור באופן ילידי (מתחת ל-100ms). "שם המחבר" הוא המחבר המפודרר שבו נעשה שימוש כאשר המקור משתתף ב-JOIN רב-מקורי.

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
| `firebird` | — | — (DuckDB extension) | — | לא |
| `airport` | — | — (DuckDB extension) | — | לא |
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | לא |
| `hiveserver2` | impyla (HiveServer2 over Thrift) | — | hive | לא |

מסדי נתונים תואמי-פרוטוקול משתמשים מחדש בדרייבר ה-JDBC, בדרייבר האסינכרוני המקורי ובניב של פרוטוקול בסיס. CockroachDB, YugabyteDB ו-Greenplum רוכבים על פרוטוקול PostgreSQL, ו-TiDB על פרוטוקול MySQL. הם צריכים רק רשומות ברישום, בלי קוד מחבר חדש.

`firebird` (Firebird 3/4/5) ו-`airport` (שרת Arrow Flight) הם סוגי מקור רשומים שתוספי הקהילה של DuckDB מגיעים אליהם במקומם כש-DuckDB הוא המנוע הפעיל. אין להם דרייבר ישיר ואין להם מחבר מפודרר.

#### `saphana`

SAP HANA נקרא דרך הדרייבר שלו ומשוכפל אל מחסן המנוע הפעיל, ולכן הוא נגיש בכל מנוע. הטופס מקבל host, פורט (ברירת מחדל 39015), שם משתמש, סיסמה ומסד נתונים; במערכת מרובת-דיירים שם מסד הנתונים מציין את הדייר (tenant). Register Table מציג את הסכמות שהמשתמש יכול לקרוא, ללא סכמות המערכת של HANA.

#### `hiveserver2`

נקודת קצה של HiveServer2 הנקראת ישירות מעל Thrift, עבור פריסת Hive שבה נקודת הקצה היא הדבר היחיד שנגיש. הוא נבדל מ-`hive` ומ-`hive_s3`, שקוראים את קבצי המחסן ואת ה-metastore ולעולם אינם עוברים דרך HiveServer2. השורות נקראות דרך נקודת הקצה ומשוכפלות אל מחסן המנוע הפעיל. הטופס מקבל host, פורט (ברירת מחדל 10000), שם משתמש, סיסמה ומסד נתונים; `federation_hints.auth_mechanism` בוחר את שיטת האימות (ברירת מחדל `PLAIN`, למשל `GSSAPI` עבור Kerberos).

### מחסני נתונים בענן {: #cloud-data-warehouses }

| סוג מקור | דרייבר ישיר | שם המחבר | דיאלקט | מוטציות | הערות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | מפודרר | קורא באמצעות snowflake-connector-python; מנחית עותק משוכפל; `account`/`warehouse`/`role` ב-`federation_hints` |
| `bigquery` | — | bigquery | bigquery | מפודרר | אין DirectDriver; מושג דרך מנוע הפדרציה או ATTACH של מנוע BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | מפודרר | קורא באמצעות databricks-sql-connector (Cloud Fetch, Arrow); מנחית עותק משוכפל; `http_path` נדרש ב-`federation_hints` |
| `redshift` | — | redshift | redshift | מפודרר | — |
| `athena` | AthenaDriver | — | athena | חלקי (INSERT תמיד; UPDATE/DELETE/MERGE רק ב-Iceberg) | שאילתה ללא שרת על S3; הצהרות על מקור בודד מופעלות ב-Athena במלואן; הצטרפויות בין מקורות מנחיתות עותק; `region` נדרש ב-`federation_hints` |
| `fabric` | MssqlWarehouseDriver | — | tsql | מפודרר | Microsoft Fabric Warehouse; T-SQL מעל TDS, אימות Azure AD; מנחית עותק משוכפל |
| `synapse` | MssqlWarehouseDriver | — | tsql | מפודרר | Azure Synapse SQL; T-SQL מעל TDS, אימות Azure AD; מנחית עותק משוכפל |
| `trino` | SQLAlchemyDriver | — | — | מפודרר | קריאה של קואורדינטור Trino/Presto מרוחק דרך דיאלקט ה-trino של SQLAlchemy; מנחית עותק משוכפל על כל מנוע |

#### `athena`

Amazon Athena הוא שירות שאילתות ללא שרת על נתונים ב-S3, שמחויב לפי בייט שנסרק. הצהרה על טבלאות Athena בלבד הולכת ישר ל-Athena, כשמסננים, רשימות עמודות ו-`LIMIT` מועברים אליו, כך ש-Athena סורק ומחייב רק את הבייטים שההצהרה צריכה. הצהרה שמצרפת טבלת Athena למקור אחר מנחיתה קודם עותק משוכפל מלא מ-Athena. זה קורא את כל הטבלה, ו-Athena מחייב על כך; אחר כך ה-join רץ מול העותק. הגדרות Replicate והגנת העומס של המקור קובעות כל כמה זמן מתבצעת הנחתה, והערה זו היא האזהרה היחידה על העלות הזו.

**גילוי.** Register Table קורא ל-API המטא-נתונים של Athena: `ListDatabases`, `ListTableMetadata` ו-`GetTableMetadata`. הקריאות האלה לא מריצות שום שאילתה, ולכן רישום מסדי נתונים וטבלאות וקריאת העמודות של טבלה לא עולים דבר לפי בייט. העמודות של טבלה נשלפות רק כשבוחרים בטבלה הזו, ומפתחות המחיצה באים אחרי העמודות הרגילות. `array<…>`, `map<…>` ו-`struct<…>` הופכים ל-`text`, וטיפוס לא מוכר נדחה בציון שמו.

**שדות המקור.**

| שדה | היכן | נדרש | ברירת מחדל | הערות |
| --- | --- | --- | --- | --- |
| `database` | מקור | כן | — | מסד הנתונים של Glue המשמש כברירת מחדל |
| `username` | מקור | לא | — | מזהה מפתח גישה AWS; יכול להיות `${secret:…}` או `${env:…}` |
| `password` | מקור | לא | — | מפתח גישה סודי AWS; יכול להיות `${secret:…}` או `${env:…}` |
| `region` | `federation_hints` | כן | — | אזור AWS, למשל `us-east-1` |
| `workgroup` | `federation_hints` | לא | `primary` | קבוצת עבודה של Athena |
| `s3_staging_dir` | `federation_hints` | לא | ברירת מחדל של קבוצת העבודה | היכן Athena כותב תוצאות; נדרש כאשר קבוצת העבודה אינה מגדירה מיקום |
| `catalog` | `federation_hints` | לא | `AwsDataCatalog` | קטלוג נתונים של Athena |
| `role_arn` | `federation_hints` | לא | — | ARN של תפקיד IAM המונח לכל קריאה |

כאשר גם `username` וגם `password` ריקים, נעשה שימוש בשרשרת אישורי boto3: תפקיד מכונה, משתני סביבה (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) או פרופיל בעל שם. אחד ללא השני נדחה.

**כתיבות.** כל טבלה רשומה מקבלת `INSERT`. `UPDATE`, `DELETE` ו-`MERGE` עובדים רק על טבלת Iceberg, והפורמט נרשם בזמן הרישום (`registered_tables.table_format`). בטבלת Hive, כתיבה שמבקשת `UPDATE`, `DELETE` או `MERGE` נדחית בציון הטבלה והפעולה. Athena לא מחזיר שורות שנכתבו, ולכן פסוקית `RETURNING` נדחית ו-GraphQL לא מציע שדות מוטציה לטבלאות האלה. נתיבי הכתיבה הם `INSERT`, `UPDATE`, `DELETE` ו-`COPY`. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` ו-`TRUNCATE` נדחים בציון הצורה.

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

עם תפקיד המונח לכל קריאה (שרשרת אישורים לסשן הבסיסי):

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```

### MotherDuck

`motherduck` הוא מסד נתונים של DuckDB המתארח ב-MotherDuck. במנוע DuckDB הוא מצורף במקומו; במנוע Postgres הוא נשאר המחבר דרך ה-foreign data wrapper `pg_duckdb`.

**חיבור.** `database` מציין את מסד הנתונים של MotherDuck, או שיתוף לפי כתובת השיתוף שלו. `password` הוא הטוקן של MotherDuck, ונפתר כמו כל סוד של מקור.

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

מנוע DuckDB מריץ `ATTACH 'md:<database>'`; עבור שיתוף, `database` הוא כתובת השיתוף (`_share/...`), ולכן הצירוף הוא `md:_share/...`.

- **טיפול בטוקן.** המנוע מתחבר עם `SET GLOBAL motherduck_token = ?`, והטוקן מועבר כערך מאוגד. הוא אינו מופיע בטקסט של אף הצהרה, ולכן אף לוג, trace או שגיאה אינם מכילים אותו.
- **אין טוקן.** מקור ללא `password` נדחה בשמו לפני שמשהו מצורף (`a MotherDuck source signs in with its token (the password field); none is set`). בלי הבדיקה, ההרחבה הייתה מתחילה התחברות אינטראקטיבית וממתינה כדקה. גם מקור ללא `database` נדחה.
- **חשבון אחד לכל מנוע.** מנוע אחד מתחבר לחשבון MotherDuck אחד. מקור MotherDuck שני עם טוקן אחר נדחה בשמו: "one DuckDB engine reads one MotherDuck account".
- **כתיבות** עוברות דרך המנוע, כמו במנוע Postgres.

**ביצוע היברידי (כפול)** אינו דורש הגדרה. במנוע DuckDB כל מקור מצורף על חיבור אחד, ולכן הצהרה המחברת טבלה של מקור קובץ DuckDB עם טבלת MotherDuck רצה כהצהרת DuckDB אחת, ו-MotherDuck מחליט היכן רץ כל חלק.

> **תנועת נתונים.** בביצוע כפול, MotherDuck עשוי להעביר שורות מקומיות לצד שלו עבור החלקים שהוא מריץ. לכן הצהרה כזו יכולה לשלוח נתונים מהקובץ המקומי לחשבון MotherDuck של הארגון. אל תחברו קובץ המכיל נתונים שחייבים להישאר בצומת עם מקור MotherDuck, אלא אם שליחתם לחשבון זה מקובלת.

מקור MotherDuck מוגש על ידי מנועי DuckDB ו-Postgres בלבד. לאף מנוע אחר אין מחבר עבורו, ושום דבר אינו מנחית שם את הטבלאות שלו כעותקים.

### אנליטיקה / OLAP {: #analytics-olap }

| סוג מקור | דרייבר ישיר | שם המחבר | דיאלקט | מוטציות | הערות |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | מפודרר | קורא באמצעות clickhouse-connect (HTTP); `secure: "true"` ב-`federation_hints` עבור TLS |
| `druid` | — | druid | druid | לא | — |
| `exasol` | — | exasol | exasol | לא | — |
| `elasticsearch` | HTTP (מנועים ילידיים) | elasticsearch (Trino) | — | לא | ב-Trino המחבר קורא אותו, ומאפייניו מגיעים מ-DSL המיפוי של הסוג; בכל מנוע אחר Provisa קוראת את האינדקס דרך HTTP (אינדקסים ומיפוי עבור Register Table, קריאת scroll עבור הנחתה) ומנחיתה את השורות |
| `pinot` | — | pinot | — | לא | מחבר `pinot` של Trino; `pinot.controller-urls` = host:port של בקר ה-Pinot |

### Data Lake / פורמטי טבלה פתוחים {: #data-lake-open-table-formats }

סוגי המקור הבאים הם פדרציה-בלבד. אין להם דרייבר ישיר ואין להם דיאלקט.

| סוג מקור | שם המחבר | Time Travel | הערות |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | כן (ארגומנט `as_of`) | — |
| `delta_lake` | delta_lake | כן (ארגומנט `as_of`) | — |
| `hive` | hive | לא | — |
| `hive_s3` | hive | לא | Hive מגובה-S3 |

### NoSQL

ל-`mongodb`, `cassandra` ו-`redis` יש מחברי Trino (`redis` בונה את המאפיינים שלו מ-DSL המיפוי של הסוג).

| סוג מקור | שם המחבר | מוטציות |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | לא |
| `cassandra` | cassandra (Trino); קריאת CQL דרך cassandra-driver בכל מנוע אחר | לא | Keyspace הם סכמות; Register Table מציג את הטבלאות של keyspace ומקצה טיפוסים לעמודות לפי מטא-נתוני הסכמה של האשכול (מפתחות מחיצה כמפתחות ראשיים); התוסף `cassandra` מתקין את הדרייבר |
| `redis` | redis (Trino); קריאת redis-py ללא HTTP בכל מנוע אחר | לא | קידומת מפתח `<table>:*` היא טבלה ו-hash הוא שורה; Register Table מציג את הקידומות הקיימות ומקצה טיפוסים לעמודות הקידומת לפי ה-hashes שלה (רשומת `mapping.tables` עוקפת תבנית, עמודת מפתח, טיפוס ערך ועמודות) |

**Redis במנועים המתארחים.** Redis אינו זמין כשהמנוע של הארגון הוא אחד המנועים המוקצים של הפלטפורמה המתארחת. Trino קורא את הטבלאות של קטלוג Redis מקובצי תיאור בתיקייה משל הקטלוג, ולא ניתן לתת למנוע מתארח תיקייה כזו בזמן שהוא פועל. שמירה או החלה של מקור Redis שם נדחות בשמן (HTTP 422, `sources.redis_needs_engine_files`); פריסה באירוח עצמי קוראת את Redis כפי שתואר לעיל.

### Qdrant

`qdrant` נקרא ונכתב באמצעות הלקוח של Qdrant עצמה, `qdrant-client`. Qdrant הוא **חי בלבד**: תכונה של סוג המקור, לא הגדרה. היא עונה באמצעות אינדקס הווקטורים וה-Query API שלה, ושום עותק אינו יכול לענות כך, ולכן הטבלאות שלה אינן משוכפלות לעולם. אין בחירת Replicate למקור Qdrant או לטבלאות שלו; ערך `replicate` או `row_materialize` עליהם נדחה.

**חיבור.** `host` הוא כתובת ה-URL של המופע עם הפורט (`https://<cluster>.cloud.qdrant.io:6333`, או `http://qdrant:6333` באירוח עצמי); `password` הוא מפתח ה-API שלו, הפניה לסוד (secret) הנפתרת כשהמקור נקרא, ריק עבור מופע ללא מפתח. `federation_hints.prefer_grpc: true` קורא דרך פורט ה-gRPC של Qdrant במקום REST. למקור סכמה אחת, `default`.

**טבלאות קולקציה.** קולקציה היא טבלה. Register Table מציע את עמודות הקולקציה: `id` (מזהה הנקודה, המפתח שלה, נתיב `@id`), שדות ה-payload (שדה מאונדקס מטיפוס לפי האינדקס שלו, כל שדה אחר לפי מה שמציעות נקודות הדגימה; שדות מקוננים משוטחים עם הנתיב שלהם), ועמודת `json` אחת לכל וקטור (נתיב `@vector`, או `@vector.<name>` לוקטור בעל שם), המוצעת כשהיא לא נבחרת. עמודת וקטור מציינת את מודל ה-embedding הרשום שהיא מחזיקה embeddings שלו, הנבחר מבין המודלים שהממדים שלהם הם גודל הווקטור (ב-Register Table ובלשונית "סוג ומפתחות" של העורך); בחירת מודל רושמת את הווקטור.

**הצהרות על מקור Qdrant אחד.** הצהרה שקוראת קולקציה אחת בלבד רצה ב-Qdrant כאשר ה-API של Qdrant מבטא אותה במדויק:

- תנאי payload הופכים למסנן של Qdrant (`=`, `<>`, `IN`, טווחים על מספרים ותאריכים, `IS NULL`, `AND`, `OR`, `NOT`); סמנטיקת ה-NULL של SQL נשמרת, כך ש-`lang <> 'en'` מדלג על נקודות ללא `lang`;
- תנאי על `id` בלבד (`=` או `IN`) הוא `retrieve` אחד; כל קריאה אחרת היא `scroll` מסונן;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` הוא `query_points` אחד על וקטור שהמרחק שלו Cosine, כשהמסנן מוחל לפני ה-top-k. טקסט של שאילתה מוטמע על ידי המודל של הווקטור. התשובה נושאת אזהרה שה-top-k משוער, ונבחר דרך האינדקס של Qdrant.

כל השאר (אגרגט, `GROUP BY`, עמודה מחושבת או מוסתרת, סדר שאינו "הדומה ביותר קודם") נענה על ידי המנוע, כמתואר להלן.

**הצהרות שהמנוע עונה עליהן.** כאשר הצהרה מחברת קולקציה למקור אחר, או מחשבת על עמודותיה, חלק הקולקציה נקרא חי מ-Qdrant עבור אותה הצהרה ומועבר למנוע כשורות. החלק הוא עמודות הקולקציה, תנאי ההצהרה על הקולקציה לבדה (כולל כללי השורה שלה) ו, כשהקולקציה נקראת לבדה ב-`SELECT` שלה, סדר הדמיון וה-`LIMIT` של אותו `SELECT`. חלק ש-Qdrant אינה יכולה לבטא במדויק נדחה, תוך ציון הטבלה והסיבה. חלק ללא תנאי, ללא מפתח וללא top-k היה קורא את כל הקולקציה ונדחה אף הוא. תנאי על פרמטר (`$1`) נשאר למנוע.

**כתיבות.** `INSERT`, `UPDATE` ו-`DELETE` על קולקציה רצים ב-Qdrant:

- `INSERT … VALUES` מציין את העמודה `id` וכותב נקודות חדשות. `id` שהקולקציה כבר מחזיקה נדחה בשמו. הבדיקה קוראת את המזהים קודם, ולכן אינה אטומית עם הכתיבה.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` הוא upsert. עמודות שההצהרה אינה קובעת שומרות על ערכיהן.
- `UPDATE` ו-`DELETE` פועלים על הנקודות שה-`WHERE` שלהם בוחר, כמסנן של Qdrant; תנאי ללא צורת מסנן נדחה.
- טקסט שנכתב לעמודת וקטור מוטמע על ידי המודל של העמודה. וקטור שנכתב אליה חייב להיות בממדי המודל.

**טבלאות חיפוש.** חיפוש וקטורי הוא טבלת חיפוש: ה-`query_template` שלה מחזיק בקשת Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` הוא תמיד ארגומנט; `using`, `filter`, `limit`, `score_threshold` ו-`model` אופציונליים. כל ערך ב-`using`, `filter`, `limit` או `score_threshold` שהוא בדיוק `"@name"` הוא אף הוא ארגומנט.
- כל ארגומנט הוא פרמטר של הטבלה (`query_arg`), המומר בקפדנות לטיפוס שהוצהר לו ומוכנס לבקשה כערך JSON, לעולם לא לטקסט שלה. הארגומנט של `query` הוא `array(double)` והוא חובה.
- מי שקורא מעביר ארגומנטים כמו כל פרמטר: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` ב-SQL, ארגומנטים של שדות ב-GraphQL.
- ארגומנט אופציונלי שלא ניתן אינו מסנן דבר: התנאי שלו מושמט מהמסנן. ללא `limit` חיפוש מחזיר את ברירת המחדל של Qdrant, 10 נקודות.
- המסנן מוחל על ידי Qdrant לפני בחירת הנקודות הקרובות ביותר, ולכן הוא מדויק. כל תנאי אחר בהצהרה מצמצם לאחר מכן את הנקודות שהחיפוש החזיר; הוא אינו שולף עוד.
- `score` הוא המדד של הקולקציה עצמה עבור הוקטור שנחפש: בקולקציית Cosine, דמיון קוסינוס. Register Table מציג את הוקטורים של הקולקציה עם הגודל והמרחק שלהם.
- `model` נוקב במודל embedding רשום (`vector_models`, admin ← AI Models). טקסט שניתן עבור השאילתה מוטמע אז על ידי אותו מודל, ווקטור שניתן חייב להיות בממדי המודל הזה. ללא `model` השאילתה חייבת להיות וקטור.

חיפוש נקרא לכל הצהרה תחת הארגומנטים שלו, ושורותיו נשמרות תחת אותם ארגומנטים למשך ה-TTL של הטבלה. טבלת חיפוש היא לקריאה בלבד.

### סטרימינג {: #streaming }

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `kafka` | מחבר Kafka מפודרר; סכמה דרך Confluent Schema Registry (Avro, Protobuf, JSON Schema), הגדרה ידנית, או הסקה מדגמית | לא; הפלט החי של טבלה מפרסם ל-Kafka |
| `websocket` | הזנת WebSocket חיצונית — התחברות, הרשמה, קבלת אירועים; התוצאות עוברות מטריאליזציה | לא |
| `rss` | הזנת RSS 2.0 / Atom — בדיקה תקופתית (poll), סימון מים לפי pubDate/updated; התוצאות עוברות מטריאליזציה | לא |

**Kafka במנועים המתארחים.** במנועים המוקצים של הפלטפורמה המתארחת, מקור Kafka חייב לציין מאגר סכימות (`schema_registry_url`). מקור שרק המודל מצהיר על עמודות ה-topics שלו נדחה בשמו בעת שמירה או החלה (HTTP 422, `sources.kafka_needs_schema_registry`), משום שתיאורי הטבלאות שלו היו דורשים תיקייה שלא ניתן לתת למנוע בזמן ריצה. אישורי SASL מגיעים למנועים האלה בזיכרון (ראו [אבטחה](security.md#the-federation-engine-trino)).

### מקבל דחיפה (Push Receiver) {: #push-receiver }

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `ingest` | שירותים חיצוניים שולחים POST של אירועי JSON; התוצאות עוברות מטריאליזציה | לא |

### גרף וסמנטיקה {: #graph-semantic }

| סוג מקור | מנגנון | מוטציות |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher דרך HTTP API, התוצאות נשמרות במטמון ב-PostgreSQL | לא |
| `sparql` | POST של SPARQL 1.1, התוצאות נשמרות במטמון ב-PostgreSQL | לא |

### מבוססי קובץ {: #file-based }

שלושה מנגנונים מכסים קבצים. מקורות הקובץ הבודד וסורק הקבצים משתמשים בשדה `path` במקום `host`/`port`. השלישי, [Logs](#logs), נוקב בקבציו באמצעות glob ב-`mapping` שלו וקורא רק קבצים שנמצאים בשרת Provisa.

**מקורות של מסד נתונים בקובץ ושל קובץ יחיד.** מקור `sqlite`, `duckdb`, `csv` או `parquet` מפנה את `path` לקובץ אחד. (מקור `duckdb` שומר את המיקום שלו בשדה `database` של הטופס.)

| סוג מקור | מיקומים | מוטציות |
| --- | --- | --- |
| `sqlite` | נתיב מקומי, `https://`, `s3://`, `gs://`, `az://` | כן בקובץ מקומי; אין בקובץ מרוחק |
| `duckdb` | נתיב מקומי, `https://`, `s3://`, `gs://`, `az://` | כן בקובץ מקומי; אין בקובץ מרוחק |
| `csv` | נתיב מקומי, `https://`, `s3://`, `gs://`, `az://` (מנוע DuckDB) | לא |
| `parquet` | נתיב מקומי, `https://`, `s3://`, `gs://`, `az://` (מנוע DuckDB) | לא |

נתיב עם כל סכמה אחרת (למשל `ftp://`) נדחה: "DuckDB reads no file at a `ftp://` location".

**אישורים למיקום מרוחק.** שימו אותם ב-`federation_hints` של המקור. כל אחד מהם מוגבל לדלי (bucket) או למכל (container) של הנתיב עצמו, כך שהאישורים של מקור אחד לעולם אינם משמשים למיקום של מקור אחר. מקור שאינו מספק אישורים נקרא כמיקום ציבורי.

| מיקום | מפתחות `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; אופציונליים `region`, `endpoint` (אחסון תואם S3 כמו R2 או MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (מפתח HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**קובצי SQLite ו-DuckDB מרוחקים הם לקריאה בלבד.** מנוע DuckDB מצרף אותם במקומם עם `READ_ONLY`, וטבלה עליהם אינה מציעה insert, update או delete. במנוע שאינו מצרף SQLite, קובץ SQLite מרוחק נקרא דרך חיבור DuckDB זמני שמצרף אותו לקריאה בלבד, והשורות שלו מונחתות כרגיל.

**העלאת קובץ.** בטופס של מקור `sqlite`, `duckdb`, `csv` או `parquet`, העלו את הקובץ במקום להקליד נתיב. הקובץ נכתב לדיסק בזרימה, לעולם אינו מוחזק בזיכרון, לתיקיית הארגון תחת `files/<source id>/<file name>`, ונתיב המקור מוגדר אליו.

- **החלפה.** העלאה חוזרת תופסת את מקום הקובץ הקודם, יהיה שמו אשר יהיה, והמקור מוקצה מחדש כך שהקריאה הבאה רואה את הקובץ החדש.
- **הסרה.** הסרת המקור מסירה את הקובץ שהועלה. שום דבר אחר אינו מסיר אותו.
- **מי.** רק תפקידים עם היכולת `source_registration` רשאים להעלות.
- **מגבלת גודל.** ההגדרה `sources.max_upload_bytes` (משתנה סביבה `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) מגבילה את הקובץ, 1 GiB (1073741824 בתים) כברירת מחדל, מינימום 1, ונקראת כשהעלאה מתחילה. קובץ גדול יותר נדחה עם HTTP 413 שמציין את המגבלה, ושום חלק ממנו אינו נשמר.
- **נתיב.** כל עוד הנתיב הוא הקובץ שהועלה למקור, שדה הנתיב בטופס הוא לקריאה בלבד.
- **שמות.** מזהה המקור ושם הקובץ רשאים להכיל רק אותיות, ספרות, `.`, `_` ו-`-`.
- **אחסון.** קובץ שהועלה מאוחסן כמו כל קובץ של הארגון: תחת תיקיית הארגון, מוצפן כשתיקיית הארגון מוצפנת, ונראה לכל צומת על mount משותף.

**הורדת קובץ.** מקור `csv` או `parquet` יכול לקבל את הקובץ שלו בהורדה במקום זאת. בחרו **Download a copy** בטופס המקור וציינו קובץ אחד: נתיב בשרת, כתובת `https://` או כתובת `s3://` שנקראת עם האישורים שהטופס שומר עבור האחסון. ‏Provisa מזרים את הקובץ דרך נתיב ההעלאה אל `files/<source id>/<file name>` בתיקיית הארגון, עם אותה מגבלת גודל, אחסון והחלפה כמו בהעלאה, והמקור קורא את העותק הזה.

- **מתי הוא נשלף שוב.** ה-TTL של המטמון של המקור קובע. טבלה שנבנתה מהמקור שולפת את הקובץ מחדש תחילה ברגע שה-TTL שלה חלף מאז שהעותק נעשה. עם TTL של ‎-1 הקובץ נשלף פעם אחת ונשמר.
- **נתיבים בשרת.** נתיב בשרת חייב להיות קובץ תחת תיקייה שמופיעה בהגדרה `sources.download_roots` (משתנה הסביבה `PROVISA_SOURCES_DOWNLOAD_ROOTS`), אחרי שהקישורים הסמליים שלו נפתרו. כברירת מחדל אף תיקייה אינה מופיעה. נתיב מחוץ להן נדחה בשמו ושום דבר אינו מועתק.
- **ארגונים רבים.** בשרת של ארגונים רבים (`multitenancy: true`) נתיב בשרת נדחה בשמו, מה שלא יהיה בהגדרה, ורק `https://` ו-`s3://` מוצעים. כתובת שהמארח שלה נפתר בתוך הרשת של השרת עצמו (פרטית, loopback או link-local) נדחית גם היא בשמה, בכל הפניה.
- **הטבלה שלו, כטיוטה.** כשהמקור נוצר, הטופס מציע להקים את הטבלה שלו מיד, כטיוטה, בדומיין שתבחרו. הטבלה נקראת על שם המקור ומקבלת טיפוסים מהקובץ כפי ש-Register Table היה מציע, והיא נשארת מחוץ לשירות עד ש-steward מוציא אותה מטיוטה. האפשרות מופעלת כברירת מחדל.
- **דרך Polly.** תנו ל-Polly נתיב פתור במלואו לקובץ CSV או Parquet יחיד, והיא תציע את המקור עם ההורדה וטבלת הטיוטה שלו, ותיצור אותו כשתאשרו. תיקייה, glob או כמה קבצים הופכים במקום זאת למקור קבצים.

**כתיבות לקובץ SQLite או DuckDB מקומי, ונעילות.** כתיבה לוקחת את נעילת הקובץ ולעולם אינה ממתינה לה. כשתהליך או צומת אחר מחזיקים את הקובץ, הכתיבה נדחית, עם שם המקור וציון שהקובץ שלו נעול.

- **SQLite.** הכתיבה מגדירה `PRAGMA busy_timeout = 0`, כך שקובץ נעול נכשל מיד.
- **DuckDB.** DuckDB משתף קובץ רק בין פתיחות לקריאה בלבד. לכן הצירוף של המנוע והחיבור של המקור עצמו מחזיקים את הקובץ לקריאה בלבד, וכל צומת קורא אותו בו-זמנית. כתיבה משהה את המחזיקים של התהליך הזה, פותחת את הקובץ לקריאה וכתיבה עבור ההצהרה הזו בלבד, וסוגרת אותו. אם הפתיחה אינה יכולה לקחת את הנעילה, הכתיבה נדחית.

**mounts משותפים.** בתיקיית ארגון מוצפנת שה-mount שלה משותף לכמה צמתים (ההגדרה `org_files.shared_mount`), כתיבה לקובץ SQLite או DuckDB בתוכה נדחית מראש, עם שם המקור. שכבת הצפנת התיקיות (gocryptfs) אינה מעבירה נעילות קבצים לצמתים האחרים, כך ששום דבר לא היה מונע משני צמתים לכתוב בו-זמנית. קריאות עדיין עובדות.

תיקייה המשותפת ללא הצפנה חייבת להיות מעוגנת (mount) עם נעילות שמגיעות לכל צומת: mount של NFS עם נעילה פעילה, לא `nolock` ולא `local_lock`. אף בדיקה אינה בודקת זאת; זו דרישת פריסה.

**סורק קבצים (מקור `files`).** הוא מפנה את `path` לתבנית glob, סורק אותה באופן רקורסיבי ורושם את התיקייה כקטלוג מפודרר של טבלאות. הוא קורא פורמטים רבים דרך ערוצי העברה רבים:

| פורמטים | תעבורות |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, ומסמכים המומרים לטבלאות — HTML, Markdown, DOCX, PPTX | מערכת קבצים מקומית, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST ו-Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

כל מקור `files` יכול גם להצהיר על `crawl` ב-mapping שלו, עם אותן הגדרות המפורטות תחת [Wikipedia](#wikipedia): המקור קורא תחילה דפי אינטרנט ומנחית כל טבלת HTML של נתונים וכל קובץ נתונים מקושר בספרייה שלו כטבלה.

במנוע DuckDB, `files` נקרא באופן מקורי, עם תצוגת סריקה `read_csv_auto` לכל `<table>.csv` תחת התיקייה שנפתרה. במנוע שאין לו מחבר `files` משלו, השורות מונחתות דרך אותו שרת pgwire של Calcite שמגיע עם המחבר (`pgwire-file`) ש-sharepoint ו-splunk משתמשים בו. ראו [מחברי SaaS ארגוניים](#enterprise-saas-connectors) בהמשך.

#### Logs {: #logs }

מקור `logs` הופך קובצי יומן שבשרת Provisa לטבלה אחת. נוקבים בקבצים לפי נתיב מלא או glob, ובוחרים את הפורמט שבו הרשומות שלהם כתובות. כל רשומה הופכת לשורה. רשומה שהפורמט אינו מצליח לקרוא נשמרת, עם הטקסט הגולמי שלה והסיבה, ולעולם אינה נזרקת.

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

ה-mapping מקבל ארבעה מפתחות ודוחה כל מפתח אחר בשמו: `glob` ו-`format` (שניהם חובה), ו-`pattern` ו-`record_start`, שרק הפורמט `regex` מקבל.

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

בטופס המקורות, מקור Logs מופיע בשם "Log files" תחת File. הכפתור **Preview** מציג את הקבצים שה-glob תופס, את העמודות שהפורמט נותן ואת 20 השורות הראשונות מהקובץ החדש ביותר, עוד לפני שמשהו נשמר.

**ה-glob.** הוא חייב להיות נתיב מלא; `~` מורחב, ונתיב יחסי נדחה ("name the files by their full path"). ההתאמה נעשית באמצעות `glob` של Python במצב לא רקורסיבי, ולכן `**` מתנהג כמו `*` ותופס בתוך רמת ספרייה אחת. נלקחים רק קבצים רגילים, לפי סדר שמות. קובץ ריק מדולג עד שיש בו בתים. ה-glob יכול לציין משתנה סביבה, כמו ב-`${env:LOG_DIR}/*.log`. המקור שומר את ההפניה כפי שנכתבה ומפענח אותה בכל קריאה של הקבצים; משתנה שלא הוגדר נדחה בשמו.

**העמודות של כל שורה.** הן באות לפני עמודות הפורמט:

| עמודה | טיפוס | תוכן |
| --- | --- | --- |
| `_file_id` | text | זהות הקובץ: סכום ביקורת של 4096 הבתים הראשונים שלו, לאחר פריסה |
| `_file_name` | text | הנתיב שתחתיו נקראה הרשומה |
| `_line_offset` | bigint | היסט בבתים של השורה הראשונה של הרשומה בטקסט הפרוס |
| `_entry` | bigint | איזו רשומת משנה של הרשומה היא השורה: 0, מלבד רשומת אצווה כגון `Records` בקובץ CloudTrail |
| `_raw` | text | הרשומה כפי שנכתבה |
| `_parse_error` | text | מדוע לא ניתן היה לקרוא את הרשומה, או אחד מערכיה; NULL כשנקראה |
| `_landed_at` | timestamp | מתי השורה הונחתה |

`_file_id`, `_line_offset` ו-`_entry` הם המפתח של השורה, ולכן רשומה שנקראה פעמיים היא שורה אחת.

**פורמטים ותבניות מוכנות.** רשימת הפורמטים מקבצת אותם לפי משפחה; הבחירה האחרונה, `regex`, היא התבנית שלכם.

| משפחה (תווית בטופס) | שמות פורמטים |
| --- | --- |
| תבניות נפוצות | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| מסדי נתונים וסביבות ריצה | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| קונטיינרים ולינוקס | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| ניטור אבטחת רשת | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| שרתי אינטרנט ופרוקסי | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| משלכם | `regex` |

אלה 56 פורמטים מובנים ו-`regex`.

**כיצד פורמט יוצר עמודות.** פורמטים מקבלים את עמודותיהם באחת מארבע דרכים.

- **עמודות קבועות.** רוב הפורמטים מצהירים על עמודותיהם ועל טיפוסיהן. `rfc3164`, למשל, נותן `timestamp`, `facility`, `severity`, `host`, `process`, `pid` ו-`message`; `facility` ו-`severity` מחושבים מהערך `<PRI>`. שורת syslog של BSD אינה נושאת שנה, ולכן השנה היא שנת הכתיבה האחרונה של הקובץ, או השנה שלפניה כשחודש השורה מאוחר מחודש הכתיבה.
- **שדות שהרשומה נותנת להם שם.** `jsonl` ו-`logfmt` לוקחים את עמודותיהם מהשדות שנראו בשורות הראשונות של הקבצים (ההגדרה `logs.preview_lines`, ברירת מחדל 1000). כל עמודה מקבלת את הטיפוס הצר ביותר שכל הערכים שנראו מתאימים לו: boolean, אחר כך integer, float, timestamp ו-json, ואחרת text. שדה שרשומה מאוחרת יותר נושאת ואין לו עמודה נכנס לעמודת json בשם `extra`.
- **שדות שכותרת נותנת להם שם.** `w3c` והתבניות המוכנות שנבנו עליו קוראים את שורת `#Fields:` של כל קובץ; `date` ו-`time` מתחברים לעמודת `timestamp` אחת. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` ו-`zeek_tsv` קוראים שורות כותרת המתחילות ב-`#` באותו אופן, ו-`aws_vpc_flow` קורא שורת כותרת שמתחילה ב-`version`. המקום שבו נקראה שורת כותרת נשאר עם הקובץ, ולכן קובץ שנקרא בהמשך שומר על השדות שלו.
- **`regex`.** כל קבוצה בעלת שם, כגון `(?P<level>…)`, היא עמודה מטיפוס text.

מסך Register Table מציג את העמודות האלה עם הטיפוסים שלהן ומסמן את המפתח. כשהשורות נקראות, כל ערך מומר לטיפוס שיש לעמודתו בטבלה הרשומה, ולכן טיפוס ששונה שם הוא זה שמיושם. ערך שהעמודה אינה יכולה להכיל הופך ל-NULL, וסיבתו נכתבת ב-`_parse_error`.

רשומה ארוכה מ-`logs.max_line_bytes` (ברירת מחדל 65536) שומרת את הבתים הראשונים שלה ב-`_raw` ואינה מנותחת; `_parse_error` נותן את אורכה. קובץ שמתחיל בסימן סדר בתים (BOM) של UTF-8 או UTF-16 נקרא בקידוד הזה, וכל קובץ אחר נקרא כ-UTF-8.

**רשומות המשתרעות על כמה שורות.** פורמט עם תבנית תחילת רשומה יוצר רשומה משורה שמתאימה לה ומכל השורות העוקבות שאינן מתאימות. Stack trace נשאר עם החריגה שלו, ורשומת שאילתה איטית נשארת שלמה. שורות שלפני ההתאמה הראשונה הן רשומה בפני עצמה; הפורמט בדרך כלל דוחה אותה, והשורה נשמרת. הפורמטים המובנים עם תחילת רשומה הם `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` ו-`azure_monitor`. עם `regex`, קבעו `record_start` בעצמכם.

שני פורמטים קוראים את הרשומות שלהם בעצמם ולא לפי שורות. `kubernetes_cri` מחבר לרשומה אחת את שורות ה-`P` החלקיות שסביבת הריצה פיצלה, ו-`windows_evtx` קורא את הקובץ הבינארי `.evtx`.

קובץ שעדיין נכתב נקרא עד השורה השלמה האחרונה שלו. בפורמט רב-שורתי הוא נקרא עד תחילת הרשומה האחרונה שלו, שהשורה הבאה עשויה להמשיך. משעברו 2 שניות בלי כתיבה לקובץ, הרשומה האחרונה נחשבת שלמה.

**רוטציה.** קובץ מזוהה לפי סכום ביקורת של 4096 הבתים הראשונים שלו, לאחר פריסה, ולא לפי שמו. כש-`system.log` עובר רוטציה ל-`system.log.0` ואחר כך נדחס ל-`system.log.0.gz`, כל העותקים הם אותו קובץ. הקריאה נמשכת מהבית שהקריאה הקודמת הגיעה אליו, ולכן אף שורה אינה נקראת פעמיים.

- דחיסה מזוהה לפי הבתים הראשונים של הקובץ, אף פעם לא לפי שמו: gzip ו-bzip2. קובץ דחוס נחשב שלם.
- עותק שעבר רוטציה והתאום הדחוס שלו, שניהם תואמים, נקראים פעם אחת, מהדחוס.
- קובץ שנמצא קצר מההיסט שכבר נקרא נקטע: הוא קובץ חדש, ונקרא מהתחלתו. השורות של הישן נשארות.
- קבצים קצרים מ-4096 בתים שמתחילים אותו דבר מובחנים לפי inode, ולכן נשארים שני קבצים.
- קובץ שה-glob כבר אינו תופס נשכח; השורות שלו נשארות כהיסטוריה.

**רענון והעברה חיה.** טבלת Logs היא עותק משוכפל. שום דבר אינו עוקב אחר הקבצים. במקום זאת, כאשר אות השינוי של המקור הוא `probe` או `ttl_probe`, הבדיקה שלו מחשבת תקציר של הנתיב, ה-inode, הגודל והכתיבה האחרונה של כל קובץ תואם; כשהתקציר שונה, מתבקש רענון. סוגי הבדיקה שמקור Logs מקבל הם `hash` ו-`none`.

רענון קורא רק את החדש. כל קובץ נקרא מהמקום שאליו הגיעה הסריקה הקודמת, והשורות נכתבות לפי המפתח שלהן, 5000 בכל פעם, לפני שהמיקום החדש נשמר. סריקה שנעצרת בין שני השלבים קוראת את הרשומות האלה שוב, וזה משאיר את אותן שורות. כל סריקה מחתימה את שורותיה ב-`_landed_at`, העמודה שההעברה החיה משתמשת בה כסימן מים. שינוי בהגדרת הטבלה הוא בנייה מחדש מלאה שקוראת כל קובץ מתחילתו.

**שורשים מורשים.** הגדרת המפעיל `logs.allowed_roots` מפרטת את הספריות שאליהן glob רשאי להיפתר. ברירת המחדל שלה היא `["/var/log"]`; זו רשימה בכרטיס Security של Admin → Settings, שאפשר לקבוע גם באמצעות `PROVISA_LOGS_ALLOWED_ROOTS`, ושינוי בה מבקש אישור. כל שורש נפתר לאחר הרחבת `~` ו-symlinks.

חלות שתי בדיקות. החלק הקבוע של ה-glob, עד התו הכללי הראשון שלו, חייב להיפתר בתוך שורש; זה נבדק כשהמקור נשמר ובכל פעם שהוא נבדק בתצוגה מקדימה או נקרא. גם כל קובץ תואם חייב להיפתר בתוך שורש לאחר מעקב אחר ה-symlinks שלו, ולכן symlink היוצא מ-`/var/log` נדחה. הדחייה נוקבת בנתיב, במה שהוא נפתר אליו ובשורשים המורשים:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

בשמירה הקוד הוא `schema.logs_path_refused`; בתצוגה מקדימה זו תשובת 403 עם `logs.path_refused`.

**דחיית פריסה מתארחת (SaaS).** פריסה המחזיקה ארגונים רבים אינה קוראת שום קובץ יומן משלה. כל בדיקת נתיב דוחה ראשונה, בשמירה, בתצוגה מקדימה ובקריאה, עם "`<path>`: a server that holds many organizations reads no log file of its own". הקבצים שייכים לשרת, לא לארגון כלשהו, ולכן אין דייר שרשאי לקרוא אותם. הפריסה יודעת אם היא מחזיקה ארגונים רבים ממצב ה-multitenancy שלה.

#### ערכות נתונים של Kaggle {: #kaggle-datasets }

Kaggle הוא אתר להורדת קובצי נתונים. ערכת נתונים של Kaggle שאתם מוסיפים נרשמת כמקור `files`, ושואלים אותה כמו כל מקור `files` אחר.

**הוספת ערכת נתונים.** פתחו את Sources → Subscriptions → Kaggle. הטופס עובד בשני שלבים, כי החיפוש של Kaggle בערכות נתונים דורש טוקן, ולכן הבורר מופיע רק אחרי שהטוקן שלכם עבר בדיקה.

1. **טוקן.** הזינו טוקן API של Kaggle ולחצו "Validate". הבדיקה קוראת ל-`POST https://www.kaggle.com/api/v1/datasets/create/new` עם גוף ריק. `401` פירושו שהטוקן לא תקף, וכל תשובה אחרת פירושה שהוא תקף. Kaggle בודק את הגוף הריק לפני שהוא יוצר משהו, כך ששום דבר לא נשמר.
2. **בורר.** שדה חיפוש שואל את `GET /api/v1/datasets/list` תוך כדי הקלדה, וכל תוצאה מציגה את הכותרת והתיאור של ערכת הנתונים. בחרו אחת ולחצו "Add Dataset".

"Add Dataset" מוריד את החבילה מ-`GET /api/v1/datasets/download/{owner}/{ref}`, פורס את קובצי ה-CSV וה-Parquet שלה אל `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` ויוצר מקור `files` **אחד** שה-`path` שלו הוא תיקיית השורש הזו. השרת לעולם לא שומר את הטוקן.

אחר כך רשמו את הטבלאות שאתם רוצים במסך Register Table הרגיל. כל קובץ מופיע כטבלה משלו, כמו בכל מקור `files` אחר.

**רק CSV ו-Parquet.** חבילה שמכילה קובץ `.sqlite` או `.db` נדחית עם שגיאה שאומרת זאת. רק קובצי CSV ו-Parquet מוכנים.

**רענון.** כש-Kaggle מפרסם גרסה חדשה של ערכת נתונים, קראו למוטציית GraphQL `refreshKaggleSource` עם מזהה המקור וטוקן תקף. היא מכינה את הקבצים מחדש במקומם ומנקה את המטמון של נקודת הקצה של הקבצים, כך שהשאילתה הבאה קולטת כל שינוי סכמה. `kaggle_owner` ו-`kaggle_ref`, שנשמרו ב-`federation_hints` כשהמקור נוצר, אומרים איזו ערכת נתונים להוריד.

**אין YAML משלו.** מקור Kaggle נוצר רק דרך טופס המקורות. כשמייצאים אותו ל-YAML הוא מופיע כ-`type: files` עם `kaggle_owner` ו-`kaggle_ref` ב-`federation_hints`. הורדה חוזרת מ-Kaggle דורשת את הרענון בממשק או את המוטציה `refreshKaggleSource`. בסביבה מבודדת, הפנו את ה-`path` ב-YAML לתיקייה שהכנתם בעצמכם.

#### Wikipedia {: #wikipedia }

Wikipedia סורקת את הדפים שאתם מציינים. כל טבלת נתונים בדף, וכל קובץ CSV, TSV, Excel, JSON או Parquet מקושר, הופכים לטבלה של המקור. כמו Kaggle, הוא נרשם כמקור `files` עם `crawl` ב-mapping שלו. הוספתו אינה רושמת דבר: steward רושם את הטבלאות הרצויות דרך מסך Register Table הרגיל.

**שדות ההגדרה.**

| שדה | ברירת מחדל | משמעות |
| --- | --- | --- |
| Pages to start from | אין (חובה) | אחד בכל שורה: כותרת דף, או כתובת של דף במהדורה שנבחרה |
| Language edition | `en` | קוד המהדורה, למשל `en`, `de` או `ja` |
| Link depth | `1` | כמה קישורים הרחק מדף התחלה לקרוא; `0` קורא רק את דפי ההתחלה |
| Page limit | `25` | מספר הדפים המרבי שסריקה אחת קוראת |
| Contact | ריק | נשלח עם כל בקשה כדי ש-Wikipedia תוכל ליצור קשר, כפי שמדיניותה מבקשת |
| Follow "See also" links | כבוי | כבוי משמיט את הפרק הזה מהערך |
| What this edition calls its "See also" section | ריק | נשאל רק כשהתיבה שלמעלה כבויה ו-Provisa אינה יכולה לנקוב בפרק עבור המהדורה שנבחרה. כותרת הפרק כפי שדף מציג אותה, למשל `Voir aussi` |
| Directory for the crawl's files | ריק | היכן נכתבות הטבלאות וקבצי הנתונים. ריק: `<PROVISA_DATA_DIR>/crawl/<source id>` |

Provisa מכירה את כותרת "See also" של המהדורות האלה: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. עבור כל מהדורה אחרת כש-"See also" כבוי, הטופס שואל איך המהדורה קוראת לפרק הזה; הקלידו את כותרתו כפי שדף מציג אותה (למשל `Voir aussi`). מקור שנשמר בלעדיה נדחה (`wikipedia.see_also_heading_needed`).

**אילו קישורים נעקבים.** קישור נבחר לפי מה שהסימון של הדף עצמו אומר שהוא, לעולם לא לפי שם בכתובת שלו: שמות כאלה שונים בכל מהדורת שפה ומופיעים גם בכותרות רגילות.

- נעקבים: קישורים לערכים אחרים השוכנים בטקסט הערך עצמו, ברשימות ובטבלאות הנתונים. סימן הסימון הוא `a[rel='mw:WikiLink']`.
- לא נעקבים: דפים שטרם נכתבו, הדף עצמו, חיפושי ספרים, דפי קבצים, ויקיות אחרות ואתרים חיצוניים.
- לא נקראים כלל (מוסרים לפני שאוספים קישורים וטבלאות): תיבות ניווט, תיבות מידע, הערות וציטוטים, קטגוריות, תוכן העניינים, תמונות והודעות תחזוקה.
- לא נקרא אלא אם מפעילים את האפשרות: הפרק "See also".

רק טבלאות שעורכים מסמנים כטבלאות נתונים (`table.wikitable`) הופכות לטבלאות, וטבלה עם פחות מ-2 שורות אינה מוצעת.

**הגדרות סריקה מתקדמות.** הטופס שומר אותן מקופלות תחת "Crawl settings" ומציג כל אחת עם ברירת המחדל שלה; השאירו שדה ריק כדי לשמור אותה. במיפוי `files` אותם שמות באים תחת `crawl:`, ושם שמחוץ לרשימה זו נדחה.

| הגדרה | ברירת מחדל ב-Wikipedia | משמעות |
| --- | --- | --- |
| `start_urls` | הדפים שציינתם | הדפים שהסריקה מתחילה מהם |
| `max_depth` | `1` | עומק קישורים |
| `max_pages` | `25` | מגבלת דפים |
| `request_delay` | `1 seconds` | השהיה בין בקשות |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, עם איש הקשר שלכם מצורף | השם שהסריקה נותנת לעצמה |
| `content_selector` | `#mw-content-text .mw-parser-output` | בורר CSS: קישורים וטבלאות נקראים רק בתוכו |
| `remove_selectors` | רשימת החלקים שתוארו לעיל | בוררי CSS לחלקים שמושמטים. ציון שלה מחליף את הרשימה כולה, כולל הכלל של "See also" |
| `link_selector` | `a[rel='mw:WikiLink']` עם החרגות | בורר CSS שקישור חייב להתאים לו כדי להיעקב |
| `link_exclude_patterns` | ריק | ביטויים רגולריים; קישור שכתובתו מתאימה לאחד מהם אינו נעקב |
| `follow_external_links` | `false` | לעקוב אחר קישורים לאתרים אחרים |
| `allowed_domains` | לא מוגדר | הדומיינים שהסריקה רשאית לבקר בהם |
| `table_selector` | `table.wikitable` | בורר CSS שטבלה חייבת להתאים לו |
| `generate_tables_from_html` | לא מוגדר | האם טבלאות HTML הופכות לטבלאות |
| `html_table_min_rows` | `2` | מספר השורות המינימלי בטבלה |
| `html_table_max_rows` | לא מוגדר | מספר השורות המרבי בטבלה |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | סוגי קבצים מקושרים שהופכים לטבלאות |
| `html_cache_ttl` | `1 days` | כמה זמן סריקה נשמרת |
| `max_html_size` | לא מוגדר | הדף הגדול ביותר שנקרא, למשל `10MB` |
| `max_data_file_size` | לא מוגדר | קובץ הנתונים הגדול ביותר שנקרא, למשל `100MB` |

**שמות טבלאות.** טבלה שנסרקה נקראת `<page>__<name>`. השם הוא הכיתוב (caption) של הטבלה, או, כשאין לה כזה, הכותרת האחרונה מעליה; טבלה בלי אף אחד מהם נקראת `table_1`, `table_2` וכן הלאה. שני החלקים מומרים לאותיות קטנות, וכל רצף של תווים אחרים הופך ל-`_`. לדוגמה, טבלה תחת הכותרת "Tallest buildings" בעמוד `List_of_tallest_buildings` הופכת ל-`list_of_tallest_buildings__tallest_buildings`. קובץ נתונים מקושר שומר על השם שלו.

**רענון.** סריקה נשמרת לתקופת המטמון, `1 days` כברירת מחדל (`html_cache_ttl`). דפים נשלפים מחדש רק לאחר שהזמן הזה חלף.

### Observability ואחרים {: #observability-other }

ל-`prometheus` יש מחבר Trino (מאפיינים הנבנים מ-DSL המיפוי של הסוג). `google_sheets` הוא סוג מקור רשום ללא מחבר Trino, ועובר מטריאליזציה דרך צינור המטמון של ה-API.

| סוג מקור | שם המחבר | מוטציות |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (מטריאליזציה) | לא |
| `prometheus` | prometheus | לא | מטריקה היא טבלה ודגימה היא שורה (`timestamp`, `value`, עמודה אחת לכל label); בכל מנוע ללא מחבר חי Provisa קוראת את ה-API של HTTP — שמות מטריקות ו-labels עבור Register Table, `query_range` על טווח הטבלה עבור הנחתה |

### מחברי SaaS ארגוניים {: #enterprise-saas-connectors }

Google Workspace ו-Microsoft 365 נקראים על ידי טוענים משלהם; ראו [Google Workspace ו-Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, Jira, מלאי הענן ו-Splunk נקראים דרך דרייברים שרצים כתהליך משלהם ומדברים בפרוטוקול של PostgreSQL (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-jira`, `pgwire-cloudops`, `pgwire-splunk`), ולכן לאף אחד מהם אין דרייבר ישיר. במנוע DuckDB נקודת הקצה של הדרייבר מחוברת בזמן אמת דרך תוסף postgres: Register Table מציג את טבלאות המחבר מהקטלוג המחובר, שאילתות קוראות את המחבר במקומו, ומסננים והטלות מועברים אליו. כל מנוע אחר מנחית את השורות במחסן המטריאליזציה לטובת הפדרציה. חבילות הדרייברים מורדות לפי מערכת הפעלה וארכיטקטורה מגרסה מקובעת (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64). מפעיל יכול במקום זאת לציין, לכל מחבר, תיקיית חבילה שנבנתה למארח הזה, תחת Admin → Settings → Connector servers (`connectors.<connector>.bundle_dir`); ראו [Admin](admin.md). כל חמשת מחברי Trino (ל-Jira אין) תמיד משווים שמות בלי תלות ברישיות, כמו שכל מוצר עושה בעצמו.

#### `sharepoint`

רשימות SharePoint נמנות כסכמות וחשופות כטבלאות ניתנות לשאילתה. שתי שיטות אימות: `CLIENT_CREDENTIALS` (ברירת מחדל) ומבוססת-תעודה (certificate) דרך תעודת PFX. ערכי סוד ב-`mapping` נפתרים דרך מנוע הסודות לפני שהם מגיעים למחבר.

| שדה מקור | מאפיין מחבר | הערות |
| --- | --- | --- |
| `base_url` או `host` | `site-url` | כתובת URL של אתר SharePoint |
| `username` | `client-id` | מזהה client של אפליקציית Azure |
| `password` | `client-secret` | סוד client של אפליקציית Azure |
| `database` | `tenant-id` | UUID דייר (tenant) של Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (ברירת מחדל) או `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | נתיב PFX כאשר `auth_type: CERTIFICATE` — חייב להיות מוחלט |
| `mapping.certificate_password` | `certificate-password` | סיסמת PFX — המפתח חייב להיות נוכח, מחרוזת ריקה עבור PFX ללא סיסמה |

אימות תעודה (certificate) על המנועים שאינם Trino נושא שני כללים נוספים, שניהם נאכפים כאשר האופרנד `model.json` של שרת ה-Calcite pgwire נבנה. `certificate_path` חייב להיות מוחלט: השרת רץ עם תיקיית החבילה שלו כתיקיית העבודה, כך שנתיב יחסי נפתר בתוך מטמון runtime-deps והקובץ PFX אינו נמצא. `certificate_password` חייב להיות נוכח ב-`mapping` גם כאשר ל-PFX אין סיסמה, ובמקרה זה הוא מחרוזת ריקה. מתאם ה-Calcite דוחה סיסמת null באופן מוחלט, ומפתח נעדר מטופל כשגיאת הגדרות ולא נקרא בשקט כסיסמה ריקה. ערך חסר או יחסי מעלה `MissingConnectorConfig` הנוקב בשדה.

כאשר המחבר לא חושף `information_schema.columns`, יש לרשום את הטבלה עם הגדרות עמודה מפורשות (המתקבלות מ-Microsoft Graph API) דרך המוטציה `registerTable`.

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

אימות תעודה, עם הנתיב המוחלט והסיסמה הנוכחת-תמיד:

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

כל sObject שפרטי ההזדהות יכולים לתשאל הוא טבלה שה-steward רשאי לרשום (`Account`, `Opportunity`, אובייקטים מותאמים `*__c`); דבר אינו נרשם אוטומטית. העמודות והטיפוסים מגיעים מה-describe של ה-sObject. סינונים, הטלות, מיונים והגבלות מועברים ל-Salesforce כ-SOQL.

מקור Salesforce מציין את כתובת ההתחברות My Domain של הארגון וסט שלם אחד של פרטי הזדהות, שנבחר באמצעות `mapping.auth_type`. כתובת ההתחברות היא כתובת `https://…` מלאה (לדוגמה `https://acme.my.salesforce.com`); מארח בלבד או כל פרוטוקול אחר נדחה תוך ציון השדה. סט חלקי נדחה בעת השימוש במקור, תוך ציון הערך החסר. כל סוד יכול להיות הפניה מסוג `${secret:…}` או `${env:…}`. סוד שהוקלד כערך מילולי נשמר בכספת הארגון, והמקור שומר את ההפניה אליו.

| שדה מקור | מאפיין מחבר | הערות |
| --- | --- | --- |
| `base_url` או `host` | `login-url` | כתובת ה-My Domain של הארגון (Setup, ואז My Domain). `login.salesforce.com` אינו פועל בזרימת client credentials |
| `username` | `client-id` | ה-consumer key של האפליקציה המחוברת |
| `password` | `client-secret` | ה-consumer secret של האפליקציה המחוברת |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (ברירת מחדל), `USERNAME_PASSWORD` או `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | `USERNAME_PASSWORD` בלבד, לצד ה-consumer key וה-consumer secret |
| `mapping.security_token` | `security-token` | `USERNAME_PASSWORD` בלבד; אופציונלי |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | `ACCESS_TOKEN` בלבד; ללא consumer key או consumer secret |
| `mapping.api_version` | `api-version` | אופציונלי, לדוגמה `v61.0` |

עבור `CLIENT_CREDENTIALS` האפליקציה המחוברת זקוקה ל-*Enable Client Credentials Flow* ולמשתמש *Run As*; כל קריאה וכתיבה רצה עם ההרשאות של אותו משתמש. ארגונים שנוצרו מאז Summer '23 חוסמים כברירת מחדל את זרימת שם המשתמש והסיסמה.

**קריאות.** ב-Trino המקור נקרא דרך הקטלוג `salesforce`, שרק קורא. בכל מנוע אחר, המנוע מצרף את שרת ה-pgwire של המקור וקורא אותו במקומו.

**כתיבות.** `INSERT`, `UPDATE` ו-`DELETE` על sObject רשום הן מוטציות: נשלטות, מתועדות בביקורת ומטופלות לפי סביבה ככל מוטציה אחרת. בכל מנוע, כולל Trino, כתיבה רצה על שרת ה-pgwire של המקור; ב-Trino שרת זה רץ לכתיבות בלבד. כל פקודה נשלחת ל-Salesforce ומאושרת עם ביצועה, ולכן אי אפשר לבטל אותה. השרת אינו מחזיר את השורות שהוא כותב: פקודה עם סעיף `RETURNING` נדחית, ו-GraphQL אינו מציע שדות מוטציה עבור הטבלאות הללו; `INSERT`, `UPDATE`, `DELETE` ו-`COPY` ב-SQL הם נתיבי הכתיבה. כתיבה היא פקודה פשוטה אחת על טבלה אחת: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` ו-`TRUNCATE` נדחים תוך ציון הצורה. מקור SharePoint נכתב באותה דרך.

**הפעלה.** לפני שהוא מקבל חיבורים, שרת המקור קורא את העמודות של כל sObject, כארבע דקות לארגון עם 1,200 כאלה. המחיר משולם פעם אחת: תוצאות ה-describe נשמרות בספריית המצב של המקור עצמו, תחת ספריית הנתונים של המופע. השרת מופעל כשהמקור נרשם או נטען. ב-Trino הקטלוג `salesforce` שומר תוצאות describe משלו, ספרייה אחת לכל מקור תחת `/data/trino/cache/salesforce-describe` ב-coordinator, כך שהמחיר משולם פעם אחת גם שם כשהנתיב הזה הוא volume שמתקיים מעבר לחיי הקונטיינר: ה-volume בשם `trino_cache` של ערימת ה-compose, או ה-volume של מטמון ה-coordinator בתרשים Helm (`trino.coordinator.cache.persistence`). עד שהשרת מקבל חיבורים, Register Table מציג את המחבר כמתחיל לפעול, וכתיבה מקבלת את אותה הודעה במקום להמתין.

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

זרימת שם משתמש וסיסמה:

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

כל טבלה של המופע זמינה לרישום, ושום דבר לא נרשם אוטומטית. במופע עמוס יש אלפי טבלאות, ולכן Register Table מקבץ אותן לפי נושא. כל נושא הוא שם התצוגה של חבילת ServiceNow, או "(no package)" לטבלה שאינה שייכת לאף חבילה. כל טבלה מוצגת כ-`name — label`. בחירת טבלה קוראת את העמודות שלה מהמופע; רישום הטבלאות לא עושה זאת.

מקור ServiceNow מציין את כתובת ה-`https://…` המלאה של ה-instance יחד עם שם משתמש וסיסמה. הכתובת נלקחת מ-`base_url` או `host`; מארח בלבד או כל פרוטוקול אחר נדחה תוך ציון השדה (`instanceUrl`). שם משתמש או סיסמה חסרים נדחים תוך ציון השדה. כל ערך יכול להיות הפניה `${secret:…}` או `${env:…}`.

| שדה מקור | מאפיין המתאם | הערות |
| --- | --- | --- |
| `base_url` או `host` | `instanceUrl` | כתובת `https://…` המלאה של ה-instance (לדוגמה `https://acme.service-now.com`). מארח בלבד או פרוטוקול אחר נדחה |
| `username` | `username` | שם משתמש לאימות בסיסי |
| `password` | `password` | סיסמה לאימות בסיסי |

**קריאות.** ב-Trino המקור נקרא דרך הקטלוג `servicenow`, שרק קורא. בכל מנוע אחר, המנוע מצרף את שרת ה-pgwire של המקור וקורא אותו במקומו. רק העמודות שנבחרו מתבקשות מה-instance (`sysparm_fields`). פילטר נשלח ל-instance (`sysparm_query`) רק בצורה שהמתאם אימת מול instance חי; כל פילטר אחר, וכל מיון והגבלה, מוחל על השורות שנקראו.

שדה הפניה מופיע כשתי עמודות: `<field>` מכיל את ה-sys_id של הרשומה המוזכרת, ו-`<field>__display` מכיל את ערך התצוגה. שדה ריק הוא NULL. שדה שסוגו אינו ניתן למיפוי על ידי המתאם מייצר שגיאה הנוקבת בטבלה, בשדה ובסוג.

חיפוש טבלאות (שפה טבעית) מתאים לשם, לתווית ולנושא של כל טבלה. לאחר רישום טבלה, העמודות שלה מצטרפות לחיפוש.

**מחזור חיים השרת.** השרת משרת רק את הטבלאות הרשומות ממנו. רישום או הסרה של טבלה מפעיל מחדש את השרת ומספק מחדש את המקור על המנוע; פקודה שמגיעה במהלך ההפעלה מחדש נכשלת.

**כתיבות.** `INSERT`, `UPDATE` ו-`DELETE` על טבלה רשומה הן מוטציות: נשלטות, מתועדות בביקורת ומטופלות לפי סביבה ככל מוטציה אחרת. בכל מנוע, כולל Trino, כתיבה רצה על שרת ה-pgwire של המקור. השרת מחזיר את השורה שה-instance מכיל לאחר כל כתיבה; קלוזת `RETURNING` לפיכך מתקבלת, ו-GraphQL מציע שדות מוטציה עבור הטבלאות הללו. כתיבה היא פקודה אחת על טבלה אחת: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` ו-`TRUNCATE` נדחים תוך ציון הצורה.

**ערכות יבוא (Import sets).** כאשר לטבלה יש מפת טרנספורמציה (transform map) פעילה אליה, Register Table מציגה את ערכות היבוא שלה, והסטיוארד יכול לקשור את הטבלה לאחת מהן (טבלה עם מספר מפות מציינת כל אחת בשמה). הוספות לטבלה קשורה עוברות דרך Import Set API של ה-instance וטרנספורמציית אותה מפה; הוספות לטבלה לא קשורה, וכל עדכון ומחיקה, נכתבים ישירות לטבלה. שורה שהטרנספורמציה דוחה — נדחית תוך ציון השורה; שורות שכבר נכתבו נשארות כתובות. הקישור נבדק בעת שמירת הטבלה: הוא חייב להיות ערכת היבוא של מפת טרנספורמציה פעילה אל הטבלה, במקור ServiceNow.

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

אתר Jira Cloud נקרא ונכתב דרך מתאם Calcite משלו. טבלה של מקור Jira היא **טופס**: פרויקט אחד וסוג issue אחד, והשדות שהעמודות שלה קוראות. Register Table מציגה כל טופס שהחשבון יכול ליצור ממנו issue, מקובצים לפי פרויקט; בחירת טופס קוראת את השדות שלו מ-Jira, ורישום הטפסים אינו עושה זאת. לכל טופס יש את העמודות `key` (המפתח הראשי שלו), `id`, `summary`, `status`, `created` ו-`updated`, ואחריהן עמודה אחת לכל שדה שנבחר, בשם שהמתאם נותן לה: שם השדה ב-snake case (‏`Story Points` הופך ל-`story_points`). שדה ששמו כבר שם של עמודה נקרא לפי מזהה השדה שלו (`customfield_10099`), כך שאפשר לבחור כל שדה. שדה משתמש מכיל את מזהה החשבון; שם התצוגה שלו הוא שדה בפני עצמו (`assignee.displayName`, עמודה `assignee_name`). עמודות ש-Jira עצמה כותבת הן לקריאה בלבד: `key`, `id`, `created`, `updated`, שם התצוגה של שדה משתמש, ושדה בצורה שהמתאם אינו מכיר, שהוא מגיש כטקסט JSON. רישום עמודה כזו מסמן אותה ב-"Read only: Jira sets it" בתיאור שלה כשאין לה תיאור, וכתיבה שקובעת אותה נדחית על ידי השרת תוך ציון העמודה.

טבלת טופס מציינת את הפרויקט ואת סוג ה-issue שלה ב-`arguments` שלה (`project`, `issue_type`), וכל עמודה קוראת את השדה ש-`path` שלה מציין (מזהה שדה או שם שדה), או את השדה בעל שמה. טבלה שנרשמה מה-admin מקבלת את שניהם מ-Jira; תצורה מצהירה עליהם. טבלת Jira שקובעת דבר אחר, או שאחד מהם חסר בה, נדחית תוך ציון שמה.

| שדה מקור | מאפיין המתאם | הערות |
| --- | --- | --- |
| `base_url` או `host` | `baseUrl` | כתובת `https://…` המלאה של האתר (לדוגמה `https://acme.atlassian.net`). מארח בלבד או פרוטוקול אחר נדחה |
| `username` | `email` | כתובת האימייל של חשבון Atlassian |
| `password` | `apiToken` | אסימון API של אותו חשבון, שנוצר ב-id.atlassian.com תחת Security, API tokens |

**קישורים.** טבלת `issue_links` של המקור מחזיקה את הקישורים בין ה-issues של הטפסים שלו: `link_id`, `link_type`, `outward_key` ו-`inward_key`, כל קישור פעם אחת. קשר בין שני טפסים מוצהר דרכה כ-junction, כש-`link_type` הוא המבחין, כך ש-GraphQL ו-Cypher עוברים מ-issue ל-issue המקושר לפי סוג הקישור.

**קריאות.** חיפוש לפי מפתח (`key =`, `key IN`) נשלף לפי מפתח, כך שמפתח שאינו מציין issue כלשהו אינו שורה. השוואת תאריך או מספר נשלחת ל-Jira כ-JQL. כל פילטר אחר, וכל מיון, מוחל על השורות שנקראו: שוויון על טקסט, אפשרות, סטטוס או משתמש אינו נשלח, כיוון ש-JQL דוחה ערך שאינו מציין דבר במקום שבו SQL מחזיר אפס שורות. שדה מרובה ערכים (labels, בחירה מרובה, components, versions) הוא טקסט המכיל מערך JSON, ונכתב באותה צורה.

ב-DuckDB, ב-PostgreSQL וב-ClickHouse המנוע מצרף את שרת ה-pgwire של המקור וקורא אותו במקומו. ב-Trino אין למקור קטלוג משלו, והוא נטען דרך השרת שלו.

**מחזור חיים השרת.** השרת משרת רק את הטפסים הרשומים ממנו, לצד רשימת הטפסים שלו, רשימת השדות שלו והקישורים. רישום, שינוי או הסרה של טופס מפעיל מחדש את השרת ומספק מחדש את המקור על המנוע.

**כתיבות.** `INSERT`, `UPDATE` ו-`DELETE` על טופס הן מוטציות, הנכתבות על שרת ה-pgwire של המקור בכל מנוע. שורות מוכנסות בקבוצות של 50; עדכון או מחיקה נעשים issue אחר issue, לפי מפתח. הגדרת `status` מבצעת את מעבר ה-workflow לסטטוס זה, ונדחית, תוך ציון הסטטוסים שניתן להגיע אליהם, כשאין ל-workflow של ה-issue אף סטטוס כזה. issue בעל subtasks אינו נמחק. השרת מחזיר כל שורה שנכתבה לפי המפתח שלה, ולכן `RETURNING` מתקבל ו-GraphQL מציע שדות מוטציה. ב-`issue_links`, `INSERT` יוצר קישור ו-`DELETE` מסיר קישור; `UPDATE` נדחה.

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

מלאי הענן מציג את המשאבים של Azure,‏ AWS ו-GCP כסט אחד של טבלאות: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries`, ו-`compute_security_groups`, המקשרת משאב חישוב לקבוצות האבטחה שלו. לכל טבלה אותן עמודות בשלושת העננים, והעמודה `cloud_provider` מציינת מאיזה ענן הגיעה כל שורה. המקור קורא בלבד, והטבלאות שלו נרשמות על ידי steward כמו כל טבלה אחרת.

מקור מציין ענן אחד או יותר. כל ענן הוא הכול או כלום: ענן שרק חלק מערכי החובה שלו מולאו נדחה, תוך ציון מה חסר, וכך גם מקור שאינו מציין אף ענן. המקור קורא בדיוק את העננים שהוא מציין. כל הערכים נמצאים ב-`mapping`; כל סוד יכול להיות הפניה מסוג `${secret:…}` או `${env:…}`, סוד שהוקלד כערך מילולי נשמר בכספת הארגון, והמקור שומר את ההפניה אליו.

| ענן | מפתח `mapping` | מאפיין מחבר | הערות |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | חובה |
| Azure | `azure_client_id` | `azure.client-id` | חובה; מזהה ה-client של רישום האפליקציה |
| Azure | `azure_client_secret` | `azure.client-secret` | חובה; סוד |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | חובה; מופרדים בפסיקים |
| AWS | `aws_access_key_id` | `aws.access-key-id` | חובה |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | חובה; סוד |
| AWS | `aws_region` | `aws.region` | חובה, לדוגמה `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | חובה; מופרדים בפסיקים |
| AWS | `aws_role_arn` | `aws.role-arn` | אופציונלי; תפקיד להנחה (assume) לגישה חוצת-חשבונות |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | חובה; נתיב מוחלט לקובץ מפתח JSON של חשבון שירות |
| GCP | `gcp_project_ids` | `gcp.project-ids` | חובה; מופרדים בפסיקים |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | אופציונלי; כמה זמן תוצאה נעשית בה שימוש חוזר לפני שהענן נשאל שוב (ברירת מחדל של המתאם 5) |

קובץ המפתח של GCP נפתח על ידי מי שקורא את המקור. ב-Trino זהו שרת Trino, ולכן הנתיב חייב להתקיים שם. בכל מנוע אחר זהו שרת ה-pgwire של המקור במארח של Provisa, שספריית העבודה שלו היא ספריית המצב שלו, ולכן נתיב יחסי נדחה.

ב-Trino המקור נקרא דרך הקטלוג `cloudops`. בכל מנוע אחר, המנוע מצרף את שרת ה-pgwire של המקור וקורא אותו במקומו, כמו מקור SharePoint.

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
```

#### `splunk`

תוצאות חיפוש Splunk ניתנות לשאילתה כטבלאות (לדוגמה `internal_server`). כתובת ה-URL של המחבר מגיעה מ-`base_url`, או נבנית כ-`https://{host}:{port}` עם פורט ברירת מחדל `8089`. אימות: כאשר `mapping.use_token` הוא `true` (ברירת המחדל), `password` מועבר כאסימון ה-API; כאשר `false`, `username` ו-`password` מועברים כאישורים נפרדים.

| שדה מקור | מאפיין מחבר | הערות |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, אחרת `https://host:port` (ברירת מחדל לפורט 8089) |
| `password` | `token` או `password` | אסימון כאשר `use_token: true` |
| `username` | `user` | רק כאשר `use_token: false` |
| `database` | `app` | הגבלה לאפליקציית Splunk מסוימת |
| `mapping.datamodel_filter` | `datamodel-filter` | סינון למודל נתונים מסוים |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | עבור תעודות בחתימה עצמית |

בנתיב העותק המשוכפל של pgwire (כל מנוע מלבד Trino) אותן ארבע הגדרות אופציונליות הופכות למפתחות האופרנד של Calcite `model.json`: `app`, `token`/`username`+`password`, `datamodelFilter` ו-`disableSslValidation`. שני האחרונים מקבלים את הטיפוסים ש-`SplunkSchemaFactory` ממיר אותם אליהם, מחרוזת ובוליאני.

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

#### Google Workspace ו-Microsoft 365 {: #google-workspace-and-microsoft-365 }

`google_workspace` ו-`microsoft_365` קוראים את הדואר, לוחות השנה והמשימות של החשבונות שמקור נוקב בהם, ומנחיתים אותם כעותקים משוכפלים. הם אינם מחברי Calcite: לכל אחד יש טוען משלו הקורא ל-API של הספק, Gmail, Google Calendar ו-Google Tasks מצד אחד ו-Microsoft Graph מצד שני. הטופס מציג אותם תחת Enterprise בשם "Google Workspace (Gmail)" ו-"Microsoft 365 (Outlook, Exchange)".

**טבלאות.** שני הסוגים מפיקים אותן טבלאות קנוניות: אותם שמות, טיפוסי עמודות וסדר. איחוד של השניים אינו דורש המרה או שינוי שם; עמודה שלספק אחד אין בה ערך היא NULL. הטבלאות רשומות תחת הסכמה `default`, וכל שורה מובילה בחשבון שממנו נקראה. מקור מציע את הטבלאות של המשאבים שהוא קורא:

| משאב | טבלאות | נכתב על ידי הצהרות |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | לא |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

העמודות מפורטות ב-[סכמת הדואר הקנונית](arch/canonical-mail-schema.md). `events` מחזיקה אירועים בודדים, אירועי אב של סדרות עם החזרתיות שלהם כשורות RFC 5545, וחריגים; היא לעולם אינה מחזיקה את המופעים המורחבים של סדרה.

**רישום.** מנהל הארגון מזין תחילה את הלקוח של הארגון עבור הפלטפורמה תחת Admin → Email: מזהה לקוח וסוד, ועבור Microsoft 365 גם מזהה הספרייה (tenant) או הדומיין. מי שמוסיף מקור אינו רואה אותו ואינו מקליד אותו. מקור מקבל אחת משתי צורות:

- **תיבת דואר אחת.** המקור נוקב כתובת אחת ב-`accounts`. הבעלים שלה לוחץ על **Connect** בטופס ומאשר את לקוח הארגון. האישור שמתקבל נשמר במאגר המצב של הארגון, לא בהגדרות המקור. ב-Google Workspace הצורה הזו מצהירה גם `sign_in: google_account`.
- **תיבות הדואר של הארגון.** המקור מצהיר `mailboxes` כאחד בדיוק מ-`everyone: true`, `group: <address or id>` או `list: [addresses]`. הוא אינו מחזיק אישור גישה ואיש אינו מחבר אותו. הארגון קורא בגישה משלו, ורק לאחר שמנהל המערכת הפעיל את "Read the organization's mailboxes". ב-Google הגישה היא מפתח של חשבון שירות עם האצלה ברמת הדומיין (domain-wide delegation), בצירוף כתובת מנהל לקריאות ספרייה. ב-Microsoft זו זרימת client-credentials של רישום האפליקציה.

בצורה השנייה, טבלה אחת מחזיקה את השורות של כל תיבות הדואר. תפקיד שרשאי לקרוא את הטבלה קורא את כל תיבות הדואר, אלא אם כלל שורה על העמודה `account` מגביל אותו.

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**הגדרות.** כל מודול דוחה בשמו מפתח שהוא אינו קורא.

| מפתח `mapping` | טיפוס | משמעות |
| --- | --- | --- |
| `resources` | שניהם | אילו מ-`mail`, `calendar`, `tasks` המקור קורא; לפחות אחד |
| `accounts` | שניהם | כתובת תיבת הדואר היחידה (צורת תיבה אחת) |
| `mailboxes` | שניהם | `everyone`, `group` או `list` (צורת ארגון) |
| `sign_in` | `google_workspace` | `google_account` (צורת תיבה אחת) |
| `mail_since` | שניהם | `YYYY-MM-DD`; דבר שהתקבל קודם אינו נשמר |
| `mail_content` | `google_workspace` | `full`, או `headers` לכותרות ותוויות בלבד (ההיקף `gmail.metadata` של Gmail). חובה כשקוראים דואר |
| `mail_search` | `google_workspace` | חיפוש כפי שמקלידים בתיבת החיפוש של Gmail |
| `mail_labels` | `google_workspace` | שמות תוויות; נקרא רק דואר הנושא את כולן |
| `mail_include_spam_trash` | `google_workspace` | `false` כברירת מחדל |
| `calendar_send_updates` | שניהם | האם כתיבה מודיעה למשתתפי אירוע; כבוי כברירת מחדל |
| `calendar_check_days` | `microsoft_365` | ימים משני צדי היום שבהם בדיקת השינויים מסתכלת; מספר שלם, 1 או יותר, חובה כשקוראים לוח שנה |

Google דוחה חיפוש תחת `gmail.metadata`, ולכן `mail_content: headers` עם `mail_search` או `mail_since` נדחה.

**היקפים (Scopes).** כניסה מבקשת רק את ההיקפים של המשאבים שסומנו:

| משאב | Google Workspace | Microsoft 365 (delegated) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, או `gmail.metadata` לכותרות בלבד | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| כל מקור | `userinfo.email` | `offline_access`, `User.Read` |

בצורת הארגון, Microsoft דורשת את הרשאות האפליקציה `Mail.Read` ו-`User.Read.All`, וכן `Calendars.ReadWrite` ו-`Tasks.ReadWrite.All` ללוחות שנה ולמשימות. חשבון השירות של Google מורשה להיקפים ב-`DELEGATED_SCOPES`, הכוללים גם קריאת משתמשי ספרייה וחברי קבוצות.

**רענון ועדכניות.** סוגי הבדיקה שהמקורות האלה מקבלים הם `hash` ו-`none`, והטוען שלהם מספק את אסימון השינוי:

- **Gmail:** תקציר של מזהה ההיסטוריה הנוכחי של כל תיבת דואר, קריאת `users.getProfile` אחת לכל תיבה.
- **דואר Microsoft:** כל delta link שנשמר, במעקב אחר פריט אחד.
- **לוחות שנה ומשימות של Google:** התג וזמן השינוי האחרון של כל לוח שנה; התג של כל רשימת משימות, והמזהה וזמן העדכון של כל משימה.
- **לוחות שנה ומשימות של Microsoft:** שורות השינוי של כל תיבת דואר, על פני `calendar_check_days` משני צדי היום.

בדיקה שאי אפשר לבצע מבקשת קריאה; היא לעולם אינה מדווחת "ללא שינוי". טבלאות לוח שנה ומשימות נקראות במלואן בכל רענון. העותקים המשוכפלים של הדואר במקור Microsoft 365 או Google Workspace נשמרים במקומם: כל חלק של תיבת דואר נכתב כשהוא מגיע וניתן לשאול אותו מיד. מקור Google Workspace המצמצם לפי `mail_search` נקרא שוב במלואו בכל רענון, משום שרק Google יכולה לומר מה חיפוש תואם.

**כתיבות.** `INSERT`, `UPDATE` ו-`DELETE` על `events`, `event_attendees` ו-`tasks` עוברים לספק דרך נתיב כתיבה אחד לשני הסוגים. `UPDATE` או `DELETE` נוקבים בשורותיהם לפי `provider`, `account` ו-`id`; משתתף נקוב לפי `event_id` ו-`address`. מופע אחד של סדרה נקוב לפי `series_id` ו-`original_start_at`. כתיבה שקובעת עמודה מחוץ לקבוצת העמודות הניתנות לכתיבה של הטבלה נדחית בשמה. Google כותבת עם `sendUpdates=all` כש-`calendar_send_updates` פעיל ועם `sendUpdates=none` כשהוא כבוי. Microsoft Graph אינו יכול לבצע שינוי כזה בשקט, ולכן שינוי במשתתפי פגישה נדחה, תוך ציון ההגדרה.

**החרגות.** הדואר לקריאה בלבד; שום הצהרה אינה משנה תיבת דואר. `calendars`, `task_lists` וכל טבלאות הדואר אינן מקבלות כתיבות. התיקיות Spam ו-Trash של Gmail מושמטות אלא אם `mail_include_spam_trash` פעיל. ה-API של Google Tasks שומר רק את היום שבו משימה אמורה להסתיים, ואין בו תזכורת.

### מקורות API {: #api-sources }

רישום כל נקודת קצה HTTP כטבלה ניתנת לשאילתה.

| סוג API | גילוי | הסקת עמודות |
| --------- | ----------- | ----------------- |
| `openapi` | ניתוח מפרט OpenAPI | פרימיטיבים → ילידי, אובייקטים → JSONB |
| `graphql_remote` | Schema introspection | פרימיטיבים → ילידי, אובייקטים → JSONB |
| `grpc_remote` | Server reflection | פרימיטיבים → ילידי, אובייקטים → JSONB |

תגובות API נשלפות, נשמרות במטמון ב-PostgreSQL (TTL הניתן להגדרה), וחשופות כטיפוסי GraphQL. טבלאות שנשמרו במטמון משתתפות בשאילתות מפודררות כמו כל מקור אחר.

**כללי JSONB**: עמודות מורכבות (אובייקטים, מערכים) המאוחסנות כ-JSONB אינן ניתנות לסינון. גישה לתת-שדה משתמשת בחילוץ `->>` ב-SQL. קשרים (relationships) מוצהרים בין טבלאות באמצעות עמודות FK סקלריות. עמודות blob מסוג JSONB אינן יעדי join. השתמשו בקידום JSONB (JSONB promotion) כדי להמיר שדות מקוננים לעמודות סקלריות ילידיות כאשר נדרש לסנן או להצטרף (join) אליהן.

#### GitHub ו-GitLab {: #github-and-gitlab }

GitHub ו-GitLab הם מקורות ממותגים הנישאים על ידי `graphql_remote`: כל אחד מופיע בטופס המקורות בשמו שלו ונרשם כמקור GraphQL מרוחק השומר את המותג שלו. הטופס מקבל אסימון גישה אישי (personal access token) ותו לא. נקודת הקצה (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) והסכמה נשלחות עם Provisa, ולכן הסכמה אינה נבדקת ב-introspection בעת הרישום. האסימון נבדק כשהמקור מתווסף. הוספת המקור אינה רושמת טבלאות: כל טבלה שהסכמה מציעה זמינה תחת מרחב השמות של המקור (`gh`, `gl`), וה-steward רושם את הרצויות. שדה שהאסימון אינו רשאי לקרוא מושמט מטבלה כשהיא נרשמת.

#### Stripe {: #stripe }

Stripe הוא מקור ממותג הנישא על ידי `openapi`, שנבנה כפי ש-GitHub ו-GitLab נבנים על `graphql_remote`. הוא מופיע בטופס המקורות בשם Stripe, תחת Enterprise. הוא נרשם כמקור OpenAPI ששורתו רושמת את המותג ב-`federation_hints`, ונתיב המפרט שלו הוא `brand:stripe`.

**מה הוא מגיע אליו.** ה-REST API של Stripe, דרך מפרט ה-OpenAPI שהוא מפרסם (`stripe/openapi`, `spec3.json`). המפרט נשלח עם Provisa, מוצמד ודחוס ב-gzip, ואינו נשלף בעת הרישום. זה שנשלח הוא גרסת ה-API `2026-09-30.endive` עם 425 נתיבים. כל קריאה נושאת `Stripe-Version` הקבוע לגרסה הזו, ולכן לכל תשובה יש הצורה שהמפרט מתאר, יהיו ברירת המחדל של החשבון אשר יהיו. שדות ש-Stripe מחזירה והמפרט משמיט מתווספים מ-`stripe.additions.json`.

כמו בכל מקור OpenAPI, כל פעולת `GET` היא טבלה מוצעת וכל פעולה אחרת היא פקודה מוצעת. הוספת המקור אינה רושמת אף אחת מהן: ה-steward רושם את הרצויות.

**אימות.** הטופס מבקש דבר אחד, אסימון גישה (`BRAND_AUTH`: `token`). השתמשו במפתח API סודי או מוגבל של Stripe. האסימון נשלח כ-bearer token, ונבדק פעם אחת עם `GET /v1/balance` לפני שהמקור מתווסף. אסימון שנדחה מכשיל את הרישום עם `openapi.credential_rejected`; אסימון חסר מכשיל אותו עם `openapi.credential_required`.

**רענון ועדכניות.** טבלת Stripe היא עותק משוכפל, כמו כל טבלת OpenAPI. התשובות השמורות שלה בתוקף למשך `cache_ttl` של המקור, שבקשת רישום קובעת ל-300 שניות אלא אם נאמר אחרת. סוגי הבדיקה של OpenAPI חלים. תבנית מוכנה (preset) יכולה לנקוב בפרמטר שרענון משתמש בו כדי לשלוף רק את מה שהשתנה. מותג Stripe אינו נוקב באף אחד, ולכן כל רענון קורא את הטבלאות שלו במלואן.

**החרגות.** שילוב התשלומים הישן של Stripe בשם token ו-Sources מושמט מהמפרט שנשלח. הרשימה נמצאת תחת [החרגות במקור הנבנה ממפרט](#exclusions-on-a-specification-built-source). `exclusions` של המפעיל עצמו מתווספות לאלה של המותג.

`stripe` מופיע גם ב-`SourceType` בין סוגי ה-stub של Supabase `wrappers`. מקור Stripe שנוסף דרך הטופס הוא מקור `openapi`, לא אותו stub.

#### החרגות במקור הנבנה ממפרט {: #exclusions-on-a-specification-built-source }

מפרט OpenAPI שפורסם מתאר כל מה שה-API המרוחק מקבל, כולל מה שהספק שלו הכריז עליו כמיושן ומה שפריסה אסור לה לשלוח לעולם. מקור OpenAPI יכול לשאת החרגות: דברים במפרט שהמקור אינו מציע.

| מוחרג | נקוב לפי | השפעה |
|---|---|---|
| פעולה (operation) | `method` ו-`path` | אינה מוצגת לרישום כטבלה או כפקודה |
| ארגומנט בקשה | `name`, על פעולה אחת (`method` ו-`path`) או, בלעדיהם, על כל פעולה שיש בה אותו | אינו בין ארגומנטי הפעולה, אינו מוצג, לעולם אינו נשלח |

החרגות מוגדרות תחת `exclusions` ב-`mapping` של המקור:

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

הן מוחלות על המפרט בעת טעינתו, כך שרשימת הרישום, הפקודות שנוצרות והבקשה שנשלחת כולם רואים מפרט שבו הדברים המוחרגים אינם קיימים. אפשר להחריג מאפיין גוף (body) ופרמטר של query, header או cookie; אי אפשר להחריג פרמטר נתיב (path), כי הוא חלק מכתובת הפעולה.

החרגה הנוקבת בפעולה או בארגומנט שאינם קיימים במפרט מכשילה את טעינת המפרט של אותו מקור ונוקבת בכל רשומה מיושנת, כך שרשימה שאינה תואמת עוד מפרט חדש יותר מתגלה.

מקור ממותג נשלח עם החרגות משלו כחלק מהאצירה שלו, והחרגות המפעיל מתווספות אליהן. מקור Stripe מחריג את שילוב התשלומים המיושן של אסימונים ומקורות ב-Stripe: יצירת חיוב (charge), ממשקי Tokens ו-Sources, הוספת כרטיס או חשבון בנק ללקוח מתוך אסימון, והארגומנטים `source`, `card`, `bank_account` ו-`default_card` בכתיבות של לקוח ושל תשלום חשבונית. תשלומים נגבים דרך Payment Intents, Payment Methods ו-Setup Intents, שהמקור מציע. קריאה ומחיקה של מקורות תשלום שכבר יש לחשבון נשארות מוצעות.

### GovData

נתונים פתוחים של ממשלת ארה"ב. הגישה מחולקת לפי קיבוץ נושאים.

למקור ניתן נושא אחד או יותר, וכל נושא מביא את סכמות המתאם המפורטות להלן. סכמה יכולה להשתייך ליותר מנושא אחד (`census`). הסכמות `ref` ו-`geo` הן סכמות מקשרות (linker schemas): נתוני ייחוס וגיאוגרפיה שהטבלאות של כל נושא מצטרפות דרכן, ולכן כל מקור משרת אותן יהיו הנושאים שניתנו לו אשר יהיו. הרשימה מוצהרת פעם אחת בשרת ומוצמדת, באמצעות בדיקה, לסכמות שחבילת המתאם המוצמדת משרתת.

| נושא | סכמות חשופות |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| כל מקור | `ref`, `geo` |

`ALL` הוא קיצור של מנוי לכל הנושאים; אינו בחירה בטופס המקורות.

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

מקור AskAmerica שנוצר בממשק הניהול מקבל פרט הזדהות אחד, מפתח ה-API. Provisa מציגה את המפתח ל-API של AskAmerica ומקבלת את פרטי ההזדהות לאחסון, קצרי-חיים ולקריאה בלבד, שבהם הנתונים נקראים; המפתח נדחה שם בשמו אם הוא אינו מוכר או שבוטל.

המקור הוא אז מקור Postgres-wire כמו מקורות SharePoint, Salesforce ומלאי הענן: Provisa מפעילה עבורו את שרת `pgwire-govdata` המצורף, כשהמפתח ופרטי ההזדהות האלה בסביבת השרת, וכל מנוע מגיע לשרת הזה כפי שהוא מגיע לשרת של אח. DuckDB,‏ Postgres ו-ClickHouse מצרפים אותו וקוראים את הטבלאות שלו במקומן, כך שטבלת AskAmerica מצטרפת לטבלאות של מקורות אחרים, יושבת מתחת לתצוגות (views) ומסופקת כמו כל טבלה אחרת. ל-Trino אין מחבר עבורו והוא קורא עותק משוכפל שהונחת דרך השרת: עותק של הטבלה השמור במחסן המטריאליזציה, טרי כמו הרענון האחרון שלו לפי הגדרות המטמון וההעתקה של הטבלה ולא נקרא בזמן השאילתה. טבלה נקראת מסכמת המתאם שהיא נוקבת בה (`sec`, `econ` וכן הלאה).

השרת מרכיב את הסכמות שלו מאחסון אובייקטים לפני שהוא מקבל חיבור, וזה לוקח דקות בהפעלה הראשונה. הוא מופעל כשהמקור נשמר; עד שהוא מוכן, טופס Register Table מדווח שהמקור עדיין מתחיל לפעול.

### היכן חיה סיסמת המקור {: #where-a-sources-password-lives }

סיסמת מקור לעולם אינה נשמרת לצד שאר הגדרות החיבור שלו. שורת ה-`sources` של מישור הבקרה
נושאת עמודת `password_ref` המחזיקה את מה שהוזן עבור הסיסמה: הערך עצמו, או הפניה כגון
`${env:PG_PASSWORD}` או `${secret:SNOWFLAKE_KEY}`. היא נפתרת ברגע שהמקור
נחייג, בתוך הארגון שהבקשה רצה כמותו.

ערך ללא `${…}` בתוכו משמש כפי שהוא. `${env:VAR}` קורא את סביבת התהליך של הפריסה ואינו
זקוק לכריכה כלשהי. `${secret:NAME}` נוקב סוד בבעלות ארגון, כך שהוא נפתר רק בתוך הפעולות
של אותו ארגון: תפרי חשיפת הניהול ומסוף השאילתות שכל משטח מגיע אליו מכוננים את הכריכה
הזו.

השדה מחזיק את מה שהוקלד בו, בכל דרך שבה המקור נרשם:

- **מקובץ הגדרות.** הקובץ הוא הרשומה: Provisa מעתיקה את הערך או את ההפניה אל
  `password_ref` כלשונם.
- **מטופס המקורות.** מה שמוקלד בשדה הסיסמה נשמר כלשונו, ערך או הפניה כאחד. Provisa אינה כותבת דבר
  לכספת הארגון עבורו ואינה ממציאה שם סוד. כדי להרחיק פרט הזדהות משורת מישור הבקרה, צרו את הסוד
  בעצמכם (ראו [סודות](secrets.md)) והקלידו `${secret:NAME}` בשדה.

ערך שהוקלד כערך נשמר במסד הנתונים של מישור הבקרה כפי שהוקלד. הקלדת הסיסמה מחדש
מחליפה את טקסט השדה. מחיקת המקור מסירה את השורה שלו ואינה מסירה דבר מהכספת: סוד
שיצרתם הוא שלכם.

`password_ref` אינה עוברת בין סביבות. ענף או סביבה משוכפלת
מספקים ערכי חיבור משלהם, והכספת שההפניה נוקבת בה שייכת לאיזו
סביבה שסיפקה אותה.

### Data Profiler {: #data-profiler }

Data profiler הוא מקור שמחזיק שם, לוח זמנים וברירות מחדל להרצה, ותו לא. הוא אינו נסרק לעולם. טבלאות של מקורות אחרים מתווספות אליו, וכל הרצה מודדת אותן וכותבת את השורות שלה למישור הבקרה של הארגון, שם הן נקראות כמו הטבלאות של כל מקור אחר. ראו [Test data](test-data.md) ליצירת profiler ולהוספת טבלאות אליו.

### בודקי איכות נתונים {: #data-quality-checkers }

בודק איכות נתונים הוא פשוט עוד סוג מקור. הפלט של הסריקה שלו הוא נתונים: כל תוצאת בדיקה היא תצפית, ולכן היא מונחתת בנתיב הרגיל של המקורות ומקבלת את מה שכל מקור אחר מקבל, מקצב, רעננות, אירועים ו-Data Lineage ועד ממשל, RLS, הטבלה והייצוא.

יש תמיכה בשני בודקים, והבחירה ביניהם היא שאלה של רישיון לא פחות משאלה של יכולות.

| סוג מקור | דיאלקט חוזה (Contract) | תוסף | רישיון | מישור ענן מאוחסן |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda contract YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | נדחה — ראו בהמשך |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | מותר |

Elastic License 2.0 אוסר לספק את התוכנה לצד שלישי כשירות מתארח או מנוהל, והרצת Soda בתוך מישור ה-SaaS בשם דייר הייתה בדיוק זה. `config/capabilities.yaml` מתעד זאת כ-`cloud_eligible: false` על האפשרות `soda`, והמישור המתארח קורא את הדגל הזה. פריסה מתארחת שרוצה Soda מפנה לנקודת קצה של Soda שהמפעיל מריץ בעצמו.

Provisa לא מצרף ולא מקשר שום דבר. הסריקה רצה במפרש-בן (`python -m provisa.dq.worker`), המקום היחיד שבו מייבאים את `soda_core` או את `great_expectations`. לכן בודק בקוד זמין לעולם לא מגיע לתהליך השרת, ואם בודק קורס, הוא מפיל תת-תהליך ומשאיר את לולאת האירועים בשקט.

**המקור מצביע על נקודת הקצה pgwire של Provisa עצמו.** כך דרייבר postgres יחיד יכול לבדוק טבלה שמאחוריה Snowflake או Iceberg: הבודק סורק את התצוגה המפודררת, ולעולם לא את המערכת שמתחתיה. על החיבור הזה חלה המדיניות, ולכן מצהירים במפורש על זהות הסריקה. אילו היא הייתה עוברת בירושה, קבוצת שורות מסוננת הייתה יכולה להעביר בדיקה בלי שאיש ישים לב.

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

**טבלת תוצאות אחת לכל חוזה, והחוזה הוא כל הרישום.** הטבלה נושאת את `dq_contract`, טקסט החוזה מילה במילה, ושום דבר אחר על הצורה שלה. העמודות, ה-watermark והקידומים נגזרים כולם ממנו.

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

הרישום גוזר מהטקסט הזה ארבעה דברים:

- **Data Lineage.** החוזה כבר נוקב בשם ערכת הנתונים היעד שלו, ולכן הרישום מנתח אותו כמו ש-`extract_inputs` מנתח SQL ופותר אותו לטבלה המנוהלת. יש הגדרה אחת ואין עותק שני שעלול לסטות. חוזה שנוקב בערכת נתונים לא מנוהלת נכשל בקול רם ברישום, ולכן לעולם לא מנחית שורות שאיש לא ביקש.
- **עמודות.** מעטפת התוצאה שייכת לבודק: 16 עמודות מסופקות, מ-`scan_id` ועד `diagnostics`. מהעמודות המוצהרות נקרא רק ה-`visible_to`, שחייב להיות זהה בכולן, ואחר כך הן מוחלפות.
- **Watermark.** `scan_time` הופך ל-watermark, מה שהופך את ההנחתה ל-append. היסטוריית הסריקות מצטברת בלי תת-מערכת היסטוריה.
- **קידומים.** `freshness_max_timestamp` ו-`dataset_rows_tested` מקודמים מתוך ה-jsonb `diagnostics` כעמודות עם טיפוס. אפשר להוסיף עוד כמו בכל עמודת jsonb אחרת.

התזמון לא דורש שדות חדשים. `change_signal` יחד עם `cache_ttl` קובעים כל כמה זמן מתבצעת הבדיקה, `mv_debounce_quiet` ו-`mv_debounce_max_delay` מקפלים פרץ של שינויים במעלה הזרם לסריקה אחת, גרעין לוח שנה הופך אותה למחזורית, ו-`expected_events` מעכב את הסריקה עד שהקלטים שלה רעננים לאורך כל החלון. לולאת התשאול היא מתזמן הסריקות.

`outcome` הוא אחד מ-`pass`, `fail`, `warn`, `error` או `skipped`. אף אחד מהם לא אוכף שום דבר בעצמו. אם רוצים אכיפה, מצהירים עליה בנפרד: preflight, או MV מעל התוצאות המונחתות. תצפית מונחתת לא חייבת להיות דטרמיניסטית, ולכן מתאימות כאן בדיקות שלעולם לא היו יכולות לשלוט ב-preflight, כמו ציון חריגות, שינוי לאורך חלון נע או רעננות שנמדדת ביחס לעכשיו.

Materialized View יכולה לאכוף את הבדיקות של הקלטים שלה באמצעות **לעצור כשבדיקות נכשלות**, בדף העריכה שלה לצד בדיקת הקדם שלה. רענון מבוטל אז, והתצוגה שומרת את העותק הקודם שלה, כאשר ההרצה האחרונה של טבלת בדיקה כלשהי הסורקת אחד מהקלטים הפיזיים הקרובים ביותר של התצוגה נכשלה. כל טבלת בדיקה על קלט נספרת, לא משנה לאיזה מוצר נתונים היא משויכת. הביטול מציין כל טבלת בדיקה שנכשלה ואת הקלט שהיא סורקת, ומתפשט במורד הזרם כמו כל ביטול של בדיקת קדם. האפשרות היא קוד בדיקת קדם רגיל הקורא ל-`provisa_services.dq`; **הצג את הקוד** מציג אותו, ואפשר להעתיק אותו לבדיקת הקדם של התצוגה עצמה כדי לשנות אותו. כאשר לתצוגה יש גם בדיקת קדם משלה, הרענון נשמר רק אם שתיהן ממשיכות.

את החוזה כותבים בממשק, בלוח איכות הנתונים של עמוד העריכה של הטבלה, וטקסט החוזה הגולמי שם הוא תמיד מקור האמת. הרצת ניסיון מפעילה את החוזה מול הטבלה החיה ומציגה את התוצאות בלי להנחית אותן. השתמשו בה כדי לתפוס חוזה ששם ערכת הנתונים שלו נפתר למקום לא צפוי, ושאחרת היה מנחית רק שורות שעוברות.

---

## מחברים מותאמים אישית {: #custom-connectors }

מפעיל יכול לתת למנועי הפדרציה המקוריים (Postgres, DuckDB ו-ClickHouse) גישה לסוג מקור חדש על ידי הצהרה על מחבר עבורו ב-`config/custom_connectors.yaml`. לא צריך קוד.

כך המנועים המקוריים, שרצים בלי אשכול, מקבלים את אותה הרחבה מבוססת תצורה שיש למנוע Trino בשכבה שלו. שם, מחבר JDBC גנרי אחד מקבל פרמטרים לכל סוג מקור, כל סוג מקבל גוף `.properties` של קטלוג, ו-Provisa מוסיף תוספי מחבר Trino משלו (Splunk, SharePoint, Calcite).

הקובץ מגיע ריק. המחברים המובנים מכסים את המקורות שעובדים בלי הגדרה נוספת, וכל מה שבקובץ הזה כותב המפעיל. כדי להשתמש בנתיב אחר, למשל בבדיקות, הגדירו את `PROVISA_CUSTOM_CONNECTORS`.

### סוגי מתאר (Descriptor) {: #descriptor-kinds }

| מנוע | סוג | מנגנון | מה המתאר מספק |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (תקן ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + תצוגת סורק | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (חושף אוטומטית כל טבלה מרוחקת) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` לכל טבלה (עמודות מהרישום) | `ch_engine`, `engine_template` (עשוי לשאת `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse מסיק את הסכמה | `ch_engine`, `engine_template` |

**Postgres הוא גנרי.** SQL/MED הוא תקן ISO, כך שכל FDW תואם חולק את אותה צורת DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, אופציונלית `CREATE USER MAPPING`, ואז או `IMPORT FOREIGN SCHEMA` (כאשר `supports_import: true`) או `CREATE FOREIGN TABLE` מפורש לכל טבלה (כאשר `false`). מתאר `pg_fdw` מספק רק את השונות הספציפית ל-FDW: שם התוסף, מפתחות אפשרויות שרת, מפתחות user-mapping, דגל import, אפשרויות טבלה. כל FDW תואם-תקן ניתן אפוא להפעלה מקונפיגורציה בלבד.

**DuckDB תומך בשני מנגנונים.** תוסף החושף קטלוג דרך ATTACH משתמש ב-`duckdb_attach`; תוסף החושף פונקציית-טבלה לקריאה משתמש ב-`duckdb_scan`. תוסף שאינו מתאים לאף דפוס אינו נתמך.

**ClickHouse תומך בשלושה מנגנונים**, אחד לכל צורת integration-engine: מנוע DATABASE יחסי החושף אוטומטית כל טבלה מרוחקת (`clickhouse_database`, למשל Redis/MySQL), מנוע לכל-טבלה שהעמודות שלו מסופקות מהרישום (`clickhouse_table`, למשל הגשר JDBC/ODBC; ה-`engine_template` עשוי לשאת placeholder בצורת `{table}` שהריצה (runtime) מקשרת), ומנוע file/lake/URL שהסכמה שלו מוסקת על ידי ClickHouse (`clickhouse_scan`, למשל HDFS/URL). SQLite (מנוע DATABASE, קובץ, ללא שרת) ו-Hudi (lakehouse, zero-copy) נשלחים מוכנים-מהקופסה.

ערך `kind` לא מוכר נכשל בקול רם בעת ההפעלה, כך ששגיאת הקלדה במתאר לעולם לא משאירה בשקט סוג מקור בלתי נגיש.

### שערי בדיקה (Probe Gating) {: #probe-gating }

הזמינות מאומתת בזמן ה-attach מול קטלוג הגילוי הסטנדרטי של כל מנוע:

- **Postgres** — בודק את `pg_extension`, ואז את `pg_available_extensions`.
- **DuckDB** — מריץ `INSTALL`/`LOAD` ובודק את `duckdb_functions()` עבור ה-`probe_symbol` המוצהר.
- **ClickHouse** — בודק את `system.table_engines` עבור ה-`ch_engine` המוצהר; היעדרות מה-build נכשלת בקול רם.

תוסף מוצהר שאינו ניתן להתקנה נכשל בקול רם. ללא דילוג שקט, ללא נפילה חזרה (fallback). מחבר שהבדיקה שלו נכשלת פשוט אינו פעיל עבור אותה פריסה.

### משתני תבנית {: #template-variables }

כל ערך `server_options`, ערך `user_mapping`, `attach_template`, ו-`scan_template` יכולים להשתמש ב-placeholders מסוג `{field}`. שדות זמינים:

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, ועוד כל מפתח מ-`federation_hints`. תבניות attach של DuckDB מקבלות גם את `{alias}`, הכינוי הפנימי של הקטלוג ש-Provisa נותן למסד הנתונים המחובר.

תבנית המפנה לשדה לא מוכר נכשלת בקול רם בזמן ה-attach, וחושפת אי-התאמה בין מתאר למקור לפני ש-DDL שבור מגיע למנוע.

### דוגמאות {: #examples }

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

עם מתאר כלשהו במקום, רישום מקור עם ה-`source_type` המוצהר מנותב דרך המחבר המותאם אישית, בכפוף לבדיקה מוצלחת. אין צורך בשינוי קונפיגורציה נוסף.

---

## מחסנים כמקורות בעלי שם {: #warehouses-as-named-sources }

Snowflake, Databricks ו-ClickHouse ניתנים לרישום כמקורות בעלי שם באופן בלתי תלוי במנוע הפדרציה הפעיל.

אחרי הרישום, המחסן נקרא דרך ה-DirectDriver של המקור, שמנחית עותק משוכפל במחסן המטריאליזציה של המנוע הפעיל, והשאילתה רצה מול העותק הזה. בנתיב הישיר הרגיל (asyncpg, aiomysql) מדלגים על המנוע לגמרי. כאן המנוע עדיין מריץ את השאילתה, אבל מול עותק מקומי, במקום לעבור ברשת אל המחסן בכל בקשה.

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

נתוני אובייקטים ו-lake בענן (קובצי parquet, csv, iceberg ו-delta_lake ב-S3, GCS או R2) הם סוג מקור נפרד. הם מחוברים במקומם כשלמנוע הפעיל יש מחבר ATTACH לסוג הזה, כך שלא מונחת שום עותק משוכפל והמנוע סורק את אחסון האובייקטים ישירות. גם פרטי ההתחברות של המקורות האלה נכנסים ל-`federation_hints`:

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

## שדות קונפיגורציית מקור {: #source-configuration-fields }

כל המקורות חולקים סט משותף של שדות.

| שדה | חובה | ברירת מחדל | תיאור |
| ------- | ---------- | --------- | ------------- |
| `id` | כן | — | מזהה ייחודי; אלפאנומרי עם מקפים/קווים תחתונים |
| `type` | כן | — | סוג מקור (ראו הטבלאות לעיל) |
| `host` | לא | `""` | Hostname או IP |
| `port` | לא | `0` | מספר פורט |
| `database` | לא | `""` | שם מסד נתונים |
| `username` | לא | `""` | שם משתמש |
| `password` | לא | `""` | סיסמה; השתמשו ב-`${env:VAR}` או ב-`${secret:NAME}` ולא בערך מילולי (ראו להלן) |
| `path` | לא | `null` | נתיב קובץ או URI ענן עבור מקורות מבוססי-קובץ ו-object/lake |
| `base_url` | לא | `null` | כתובת URL בסיסית עבור מקורות OpenAPI |
| `pool_min` | לא | `1` | גודל מינימלי של pool חיבורים |
| `pool_max` | לא | `5` | גודל מקסימלי של pool חיבורים |
| `use_pgbouncer` | לא | `false` | ניתוב חיבורים דרך PgBouncer |
| `pgbouncer_port` | לא | `6432` | פורט PgBouncer |
| `cache_enabled` | לא | `true` | הפעלת שמירה במטמון של תגובות API |
| `cache_ttl` | לא | `null` | TTL של המטמון בשניות; יורש ברירת מחדל גלובלית כאשר null |
| `cache_catalog` | לא | `null` | קטלוג מפודרר עבור מטמון ה-API; ברירת מחדל היא הקטלוג של המקור עצמו |
| `cache_schema` | לא | `api_cache` | סכמה בתוך קטלוג המטמון |
| `naming_convention` | לא | `null` | דריסת מוסכמת השמות הגלובלית עבור מקור זה |
| `federation_hints` | לא | `{}` | מאפייני session המועברים למנוע הפדרציה, ופרמטרי חיבור מורחבים עבור מקורות מחסן |
| `mapping` | לא | `{}` | הגדרות מחבר ספציפיות-סוג עבור מקורות NoSQL ו-SaaS (למשל `auth_type` של SharePoint, `auth_type` של Salesforce, `use_token` של Splunk) |
| `allowed_domains` | לא | `[]` | הגבלת המקור לדומיינים ספציפיים; ריק = ללא הגבלה |
| `description` | לא | `""` | תיאור קריא לבני אדם |

---

## מקורות Kafka {: #kafka-sources }

נושאי (topics) Kafka מוגדרים בנפרד תחת `kafka_sources`, ממופתחים לפי ה-`id` של מקור `kafka` רשום.

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
| `topics[].default_window` | חלון זמן ברירת מחדל עבור שאילתות מחולנות (windowed) (למשל `1h`) |
| `topics[].columns` | הגדרות עמודה עבור סכמת הנושא |

---

## נראות עמודות {: #column-visibility }

השדה `visible_to` בכל עמודה הוא רשימת מזהי תפקיד (role) שיכולים לראות את אותה עמודה.

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

עמודות שהושמטו מרשימת ה-`visible_to` של תפקיד לא מופיעות בסכמת ה-GraphQL של אותו תפקיד ולא ניתנות לשאילתה או להפניה במסננים.

---

## קשרים (Relationships) {: #relationships }

קשרים מחברים שתי טבלאות רשומות ומופיעים כשדות מקוננים ב-GraphQL.

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
| `cardinality` | כן | `many-to-one` או `one-to-many` |
| `materialize` | לא | יצירה אוטומטית של Materialized View עבור joins חוצי-מקורות. בקשת מגובת junction ה-view מכסה את החצייה בשתי קפיצות, לא join ישיר |
| `refresh_interval` | לא | מרווח רענון MV בשניות (ברירת מחדל: 300) |
| `target_function_name` | לא | שם פונקציית DB עבור קשרים מחושבים |
| `function_arg` | לא | איזה ארגומנט פונקציה מקבל את ערך עמודת המקור |
| `alias` | לא | סוג קשר קריא לבני אדם (למשל `WORKS_FOR`) |
| `graphql_alias` | לא | קובע את שם שדה ה-SDL שקשר זה חושף על הטיפוס ההורה. כאשר נעדר, השם נגזר מ-`field_name` של טבלת היעד ומקרדינליות הקשר. |
| `disable_cypher` | לא | כאשר `true`, מוציא קשר זה מקשתות הגרף של Cypher |
| `source_json_key` | לא | חילוץ מפתח זה מעמודת המקור כאובייקט JSON לפני JOIN |
| `via_table` | לא | שם הטבלה הרשומה של ה-junction שהקשת הזו חוצה. הגדרתו הופכת את הקשת למגובת junction; השארתו ריק משאירה אותה קשת מפתח-זר |
| `via_source_column` | לא | עמודת ה-junction המזדווגת עם `source_column`. מופרדת בפסיקים ולפי מיקום עבור מפתח מורכב |
| `via_target_column` | לא | עמודת ה-junction המזדווגת עם `target_column` |
| `via_type_column` | לא | עמודת המבחין, כאשר junction אחת נושאת כמה סוגי קשרים |
| `via_type_value` | לא | ערך המבחין שאליו הקשת הזו מקובעת |
| `via_label_source` | לא | איזו מועמדות מעניקה את שם טיפוס ה-Cypher: `column` (ערך המבחין), `table` (שם טבלת ה-junction) או `fixed` (הכינוי המוצהר). כולן מומרות ל-UPPER_SNAKE_CASE |

### קשרים מגובי junction {: #junction-backed-relationships }

אפשר להצהיר על טבלה מקשרת כקשר Cypher בפני עצמו, במקום כצומת. העמודות שלה הופכות אז לתכונות של הקשר:

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

טבלת הקישור היא טבלה רשומה כמו כל טבלה אחרת, וצריך לרשום אותה לפני שקשר יכול לציין אותה. הצהירו על קשר אחד לכל ערך מבחין. שלוש שורות על `pet_companions` נותנות את `BONDED_PAIR`, `LITTERMATE` ו-`SHARES_ENCLOSURE` כשלושה סוגי Cypher נפרדים, שכל אחד מהם נושא את שאר העמודות של שורת הקישור כמאפייני קשת. תצורת ההדגמה שמגיעה עם המוצר מצהירה בדיוק על זה.

קשת דרך טבלת קישור קיימת רק ב-Cypher. מחולל ה-joins של GraphQL בונה את פסוקית ה-`ON` שלו לזוג עמודות אחד, ואין לו איפה לשים את הקפיצה השנייה, ולכן קשתות כאלה לא נכללות ב-SDL שנוצר וב-`pg_constraint`. טבלת הקישור עצמה נשארת זמינה לשאילתות כשדה שורש משלה. היא מוסרת מצד הצמתים של סכמת הגרף של Cypher, ולכן היא לעולם לא מופיעה כתווית צומת.

`materialize: true` עובד גם על קשת כזו. מה שמתממש הוא המעבר כולו: התצוגה מחזיקה את הקפיצה מהמקור, את הקפיצה דרך טבלת הקישור, את הערך המבחין ואת העמודות של טבלת הקישור לצד אלה של היעד. טבלת הקישור היא רגל שלישית של ה-join, ולכן השאלה אם הקשת חוצה מקורות נבחנת על פני שלוש הטבלאות. טבלת קישור שנמצאת במקור אחר משתי הטבלאות שהיא מקשרת מתממשת גם כששתיהן באותו מקור. כל הצהרה מממשת סוג קשת אחד, ולכן תצוגה שנבנתה עבור `bonded pair` לעולם לא עונה על מעבר `littermate`.

ערכי קרדינליות:

- `many-to-one`: כל שורת מקור ממופה לשורת יעד אחת (FK ל-PK)
- `one-to-many`: כל שורת מקור ממופה לכמה שורות יעד (ההפך)

---

## כללי אבטחה ברמת השורה {: #row-level-security-rules }

כללי RLS מזריקים משפטי `WHERE` בזמן השאילתה, בהיקף של תפקיד ואופציונלית של טבלה או דומיין.

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.

  - domain_id: sales-analytics  # applies to every table in domain
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
    description: Analysts read only their own tenant's rows.
```

כאשר קיים גם כלל ברמת-דומיין וגם כלל ברמת-טבלה עבור אותו תפקיד, הכלל ברמת-הטבלה גובר.

| שדה | חובה | תיאור |
| ------- | ---------- | ------------- |
| `table_id` | מותנה | הטבלה שעליה חל הכלל; בלעדי הדדית עם `domain_id` |
| `domain_id` | מותנה | הדומיין שעליו חל הכלל; חל על כל הטבלאות בדומיין |
| `role_id` | כן | התפקיד שעליו חל כלל זה |
| `filter` | כן | פרדיקט SQL המוזרק לתוך `WHERE`; יכול להפנות למשתני session |
| `description` | כן | לשם מה הכלל קיים, כפי שסקירת גישה קוראת אותו |

---

## פונקציות ו-Webhooks {: #functions-and-webhooks }

### פונקציות DB {: #db-functions }

מעקב אחר פונקציית מסד נתונים וחשיפתה כשאילתת GraphQL או מוטציה.

מקורות מסד נתונים יכולים גם לגלות אוטומטית את הפרוצדורות המאוחסנות והפונקציות שלהם מקטלוג הספק (`pg_proc`, `information_schema.routines`, או שווי-ערך של הספק), ולבטל את הצורך ברישום ידני של כל אחת. הגילוי קורא `prokind` ו-`provolatile`: פונקציות immutable/stable נרשמות כיחסים (relations) מפורמטים (ארגומנטי הפרוצדורה הופכים לפרמטרי שאילתה, אותה צורה כמו טבלאות OpenAPI GET), ופרוצדורות volatile נרשמות כמוטציות/פונקציות במעקב. פרוצדורות שהתגלו זורמות דרך ממשל שלב-2 (Stage-2 governance) באותו אופן כמו כאלה שנרשמו ידנית.

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
| `returns` | כן | — | מזהה טבלה רשומה שהפונקציה מחזירה |
| `arguments` | לא | `[]` | רשימת הגדרות ארגומנט `{name, type}` |
| `visible_to` | לא | `[]` | תפקידים שהפונקציה משויכת אליהם (ריק: כל תפקיד). `mutation` דורשת בנוסף את הזכות `write` |
| `domain_id` | לא | `""` | הדומיין שאליו שייכת פונקציה זו |
| `description` | לא | `null` | תיאור שדה GraphQL |
| `kind` | לא | `mutation` | `"query"` או `"mutation"` |

### Webhooks

חשיפת נקודת קצה HTTP חיצונית כשאילתת GraphQL או מוטציה.

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
| `inline_return_type` | לא | `[]` | רשימת שדות `{name, type}` עבור צורות החזרה מותאמות אישית |
| `arguments` | לא | `[]` | רשימת הגדרות ארגומנט `{name, type}` |
| `visible_to` | לא | `[]` | תפקידים שיכולים לקרוא ל-webhook זה |
| `domain_id` | לא | `""` | הדומיין שאליו שייך webhook זה |
| `description` | לא | `null` | תיאור שדה GraphQL |
| `kind` | לא | `mutation` | `"query"` או `"mutation"` |

---

## אימות (Authentication) {: #authentication }

האימות מוגדר תחת המפתח `auth`.

| ספק | תיאור |
| ---------- | ------------- |
| `none` | ללא אימות; כל הבקשות מטופלות כ-`default_role` |
| `firebase` | Firebase Authentication; דורש `project_id` ו-`service_account_key` |
| `keycloak` | Keycloak OIDC |
| `oauth` | OAuth 2.0 גנרי |
| `simple` | שם משתמש/סיסמה ללא ספק חיצוני |

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

`assignments_source: claims` קורא שיוכי תפקיד מתביעות (claims) JWT. `assignments_source: provisa` קורא אותם ממחסן השיוכים של Provisa עצמה.

---

## ניתוב ביצוע {: #execution-routing }

**ביצוע ישיר.** שאילתה שנוגעת במקור רלציוני אחד בלבד הולכת ישר לדרייבר של אותו מקור, בפחות מ-100ms. סוג מקור הולך בנתיב הזה כש-Provisa מכיר גם את ניב ה-SQL שלו וגם את המחבר שלו.

**ביצוע מפודרר.** שאילתה חוצת מקורות, או שאילתה על מקור בלי דרייבר ישיר, עוברת דרך מנוע הפדרציה. Provisa מגיע עם מנוע מוטמע; בפריסות גדולות, הפנו אותו לאשכול תואם משלכם.

**סטטיסטיקות.** כשטבלה נרשמת, Provisa מריץ `ANALYZE` על כל טבלה מפורסמת כדי שלמייעל מבוסס העלות יהיו ספירות שורות, שיעורי null, ערכים ייחודיים ומינימום/מקסימום. אם זה נכשל, הכישלון נרשם ביומן והרישום ממשיך.

---

## מקורות גרף וסמנטיקה {: #graph-semantic-sources }

### Neo4j

רישום מסד נתוני גרף Neo4j כמקור ניתן לשאילתה. Stewards כותבים שאילתות Cypher המקרינות ערכים סקלריים; Provisa שומרת את התוצאות במטמון וחושפת אותן כטיפוסי GraphQL.

שאילתות Cypher חייבות להשתמש בגישה למאפיינים בפסוקית `RETURN` (`RETURN n.id AS id, n.name AS name`). הרישום דוחה שאילתה שמחזירה אובייקטי צומת.

#### רישום מקובץ הגדרות {: #config-file-registration }

הצהר על מקור `neo4j` והטבלאות שלו ב-YAML. כל טבלה דורשת `query_template` (ה-Cypher שמפיק את שורותיה) ועמודות מוקלדות. המפתח `query_template` אינו תקף תחת כל סוג מקור אחר.

מקור דורש `host`, `port`, ו-`database`. שורות נשלפות על ידי POST של `{"statements": [{"statement": <cypher>}]}` אל `/db/<database>/tx/commit` (ה-API של עסקאות HTTP של Neo4j). תגובה עם רשימת `errors` לא-ריקה מטופלת כשאילתה כושלת, לא כתוצאה ריקה.

כל עמודה דורשת `data_type`. הטוען ממפה טיפוסי הגדרות לטיפוס העמודה של ה-API הנמצא בשימוש בזמן השאילתה:

| `data_type` בהגדרות | טיפוס API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` ו-`decimal(10,2)` מתקבלים, ומשתמשים בטיפוס הבסיס שלפני הסוגריים.

הרישום שומר שורת `api_sources` ושורת `api_endpoints` אחת לכל טבלה, כך שהטבלאות שורדות הפעלה מחדש ללא קריאה חוזרת של הקובץ. נקודות הקצה של REST הניהול תחת `/admin/sources/neo4j` כותבות את אותן שורות.

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### רישום טבלה ב-UI {: #register-table-in-the-ui }

למקור neo4j אין טבלאות לרשימה, כך שטופס Register Table שואל לגבי הטבלה במקום להציע אחת.

1. בחר את מקור ה-neo4j ותחום. בוררי הסכמה והטבלה, תיבת הסימון discover ובורר ה-watermark אינם מופיעים; המקור לעולם אינו נבדק (introspected).
2. הקלד שם טבלה ואת ה-Cypher. ה-Cypher חייב להקרין סקלרים (`RETURN a.name AS name`); הקרנה המחזירה node או רשימה מדווחת כשגיאה.
3. לחץ Preview. הטופס מריץ את ה-Cypher עם `LIMIT 5` דרך שאילתת ה-GraphQL‏ `neo4jPreview` וממלא את רשימת העמודות מהשורות המתקבלות, מוקלדות כ-`text`, `integer`, `double`, `boolean` או `json`. תצוגה מקדימה כושלת משאירה את ה-Cypher בעורך ומציגה את ההודעה.
4. התאם נראות, כינויים או מיסוך כמו לכל טבלה, ואז רשום. הטופס מסרב להגיש עד שתצוגה מקדימה הקלידה את העמודות, והשרת מסרב לטבלת neo4j שאינה נושאת Cypher (`schema.neo4j_query_required`).

ה-Cypher נשמר עם הטבלה כ-`queryTemplate`, מוצג בתצוגת הקריאה של הטבלה, ונשמר בדיוק כפי שרישום מקובץ הגדרות עושה: שורת `api_sources` ושורת `api_endpoints` שההפעלה הבאה מטעינה. עריכת הטבלה שומרת מחדש Cypher ערוך.

#### רישום REST ניהול {: #admin-rest-registration }

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

נקודת הקצה של תצוגה מקדימה (`POST /admin/sources/neo4j/{id}/preview`) מחזירה שורות דוגמה וחוסמת רישום אם ה-Cypher מחזיר אובייקטי node.

### SPARQL

רישום כל triplestore תואם SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog וכדומה) כמקור ניתן לשאילתה.

השאילתות חייבות להיות שאילתות `SELECT`. שמות משתנים במשפט ה-`SELECT` הופכים אוטומטית לשמות עמודות.

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

שני המחברים משתמשים בצינור המטמון של מקור ה-API. התוצאות מאוחסנות ב-PostgreSQL עם TTL הניתן להגדרה, מה שהופך אותן לזמינות עבור joins מפודררים חוצי-מקורות.

---

#### רישום מקובץ הגדרות ו-UI {: #config-file-and-ui-registration }

ה-`host` של מקור `sparql` הוא כתובת ה-URL של נקודת הקצה SPARQL שלו (טופס המקורות שומר אותו באותה דרך). כל טבלה תחתיו נושאת `query_template`, שאילתת SELECT שמשתניה הם העמודות; כל כריכה (binding) היא `text`.

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table עובד באותה דרך כמו עבור Neo4j: בחר את המקור, הקלד שם טבלה ואת ה-SELECT, לחץ Preview (שאילתת ה-`sparqlPreview` מריצה אותה עם `LIMIT 5` וממלאת את רשימת העמודות), ואז רשום. הרישום שומר שורת `api_sources` ושורת `api_endpoints` (POST מקודד-טופס לנתיב נקודת הקצה, מנרמל `sparql_bindings`), אותן שורות שרישום מקובץ הגדרות כותב, והמנוע הילידי מנחית את השורות דרך אותה שרשרת שליפה כמו Neo4j.

## דוגמאות חיבור {: #connection-examples }

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

### שאילתה חוצת-מקורות {: #cross-source-query }

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

חלקים חד-מקוריים מנותבים ישירות. joins חוצי-מקורות מבצעים פדרציה עם המרת טיפוסים אוטומטית.
