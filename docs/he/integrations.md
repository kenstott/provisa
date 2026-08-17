# אינטגרציות

## בחירת נתיב חיבור

| סוג לקוח | נתיב מומלץ | מדוע |
| ------------- | ----------------- | ----- |
| כלי BI (Tableau, Power BI, Looker) | JDBC | הזרמה טורית (columnar) של Arrow Flight על גבי הקו; לכלי BI יש אשף JDBC מובנה והם נהנים ממסירה טורית בעלת תפוקה גבוהה עבור סטי תוצאות גדולים |
| psql, DBeaver, כל כלי תואם PG | pgwire (מנהל התקן PG native) | ברירת המחדל חסרת החיכוך — אין צורך במנהל התקן מותאם; השתמשו במה שכבר יש לכם |
| מחסנית נתונים של Python (pandas, pyarrow) | `provisa-client` או ADBC גולמי | קבוצות (batches) Arrow בזרימה; ללא תקורת serialization של שורות |
| Spark, DuckDB, pipelines בעלי תפוקה גבוהה | Arrow Flight (ADBC) | הזרמה טורית בלתי מוגבלת ישירות לזיכרון Arrow |
| שירות-לשירות (חוזים בעלי טיפוסים) | Protobuf gRPC | proto מיוצר פר-תפקיד; שורות בזרימה; בטיחות טיפוסים |
| אפליקציות ווב, סקריפטים | HTTP (`/data/graphql`, `/data/sql`) | ללא מנהל התקן; HTTP סטנדרטי; בחירה מלאה של שפת שאילתה |
| לקוחות REST (תקן JSON:API) | `GET /data/jsonapi/{table}` | מעטפת JSON:API v1.0; שדות דלילים (sparse fieldsets), pagination, סינון דרך פרמטרי שאילתה; ללא מנהל התקן |

---

## pgwire — מנהל התקן PostgreSQL native

Provisa מיישמת את פרוטוקול ה-wire של PostgreSQL (גרסת פרוטוקול 3.0). כל לקוח שדובר PostgreSQL מתחבר ללא מנהל התקן מותאם.

הפעילו על ידי הגדרת `PROVISA_PGWIRE_PORT` (לדוגמה `5433`) לפני הפעלת Provisa. מושבת כאשר לא מוגדר או `0`.

### מדוע pgwire ולא JDBC?

מנהל ההתקן JDBC משתמש ב-Arrow Flight כתעבורה שלו ודורש פריסה של `provisa-jdbc.jar`. pgwire לא דורש דבר — אם כבר יש לכם `psql`, DBeaver, SQLAlchemy, או מנהל התקן PG JDBC, סיימתם. זהו הנתיב בעל החיכוך הנמוך יותר עבור עומסי עבודה מבוססי SQL בלבד.

JDBC הוא הבחירה הנכונה עבור כלי BI בעלי אשף חיבור JDBC מובנה שנהנים מהזרמה טורית של Arrow Flight עבור סטי תוצאות גדולים. pgwire מקבל SQL חופשי כנגד הסכמה המפורסמת המלאה — אותן שאילתות, עלות הקמה נמוכה יותר.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host: `localhost`, Port: `5433`
3. שם משתמש / סיסמה כפי שהוגדרו ב-Provisa
4. אין צורך בהורדת מנהל התקן נוסף

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

או עם `asyncpg`:

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### אימות (Authentication)

שדה ה-`password` של חבילת ה-startup נושא את האישור (credential), ומה שהאישור *הוא* קובע את השיטה: אסימון גישה אישי, אסימון bearer של OIDC, או סיסמה כנגד הספק המוגדר. תחת ספק ה-`basic` עם `auth.scram: true` הסיסמה מוכחת דרך SCRAM-SHA-256 במקום להישלח. תעודות לקוח (client certificates) נתמכות. במצב אמון (`none`) שם המשתמש ממופה ישירות לתפקיד והסיסמה מתעלמת.

טבלת המשטח × השיטה המלאה נמצאת ב-[Security Model](security.md#_12). MD5 אינו נתמך; הפעילו TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) בעת הרצה על גבי רשת לא מהימנה.

### מגבלות

- SQL בלבד. GraphQL ו-Cypher אינם מתקבלים דרך pgwire.
- לא לקריאה בלבד (read-only). `COPY ... FROM STDIN` מכניס שורות ל-`postgresql`, `mysql`, `sqlite`, ו-`mariadb`, ו-DDL נתמך (ראו למטה).
- DDL (`CREATE`, `ALTER`, `DROP`) נתמך ומועבר לנתיב Trino או הישיר; הטבלה החדשה נרשמת להקשר הקומפילציה וניתנת לשאילתה מיידית. `COPY ... TO STDOUT` (ייצוא) ו-`COPY ... FROM STDIN` (ייבוא) נתמכים בפורמטים `text` ו-`csv`.
- שאילתות `information_schema` ו-`pg_catalog` מיורטות ונענות מ-shim קטלוג DuckDB — כלי גילוי סכמה עובדים כראוי.

---

## מנהל התקן JDBC

מנהל ההתקן JDBC של Provisa משתמש ב-Arrow Flight כתעבורת הבסיס שלו. זהו הנתיב המומלץ עבור כלי BI בעלי אשף חיבור JDBC.

### חיבור

הורידו את [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (תמיד הגרסה האחרונה) והוסיפו אותו לנתיב מנהלי ההתקן של הכלי שלכם.

כתובת JDBC:

```yaml
jdbc:provisa://<host>:8815
```

האימות משתמש בתכונות `user` / `password` הסטנדרטיות של JDBC. Provisa מאמתת את האישורים כנגד ספק האימות המוגדר ומקצה את התפקיד — הלקוח לא בוחר את התפקיד שלו.

### הגדרת כלי BI

**Tableau**

1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. הזינו את שם המשתמש והסיסמה כאשר תתבקשו

**DBeaver** (נתיב JDBC — עבור נתיב pgwire ראו למעלה)

1. Database → New Connection → JDBC
2. Driver: הוסיפו את `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. הזינו את שם המשתמש והסיסמה בלשונית Authentication

**Power BI** — השתמשו בשער ODBC עם הגשר Provisa JDBC-ODBC (כלול במתקין).

---

## לקוחות Arrow Flight

Arrow Flight (פורט 8815) הוא הנתיב המומלץ עבור כלי נתונים התומכים בו. תוצאות זורמות כ-Arrow RecordBatches מבלי להתממש בזיכרון של Provisa.

### Python (`provisa-client`)

הנתיב המומלץ ב-Python — עוטף גם GraphQL וגם Arrow Flight:

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", username="alice", password="secret")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("SELECT id, amount FROM sales.orders")

# Arrow Flight → pandas DataFrame
df = client.flight_df("SELECT id, amount FROM sales.orders")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

ראו [docs/python-client.md](python-client.md) להתייחסות מלאה כולל DB-API 2.0, dialect של SQLAlchemy, ו-ADBC.

### Python (PyArrow גולמי)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight נושא את האישור שלו במטען ה-JSON, כשדה `token` — אסימון bearer של ספק או אסימון גישה אישי. גם ה-handshake וגם כל ticket מקבלים אותו, ושניהם מאמתים אותו באותו אופן, כך שלקוח שהתאמת (authenticated) ב-handshake עדיין מציג את האסימון בכל `do_get`. שדה `role` לצידו *מבקש* תפקיד; השרת גוזר את התפקידים המורשים של הזהות ומחליף לערך המורשה, כך ששדה role ב-ticket לעולם אינו הזהות עצמה. (REQ-1263) ראו [Security Model](security.md#_12).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

### ADBC

```python
import adbc_driver_flightsql.dbapi as adbc

conn = adbc.connect("grpc://localhost:8815", db_kwargs={"username": "alice", "password": "secret"})
cursor = conn.cursor()
cursor.execute("SELECT id, amount FROM sales.orders")
table = cursor.fetch_arrow_table()
```

### DuckDB

```python
import duckdb, pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT * FROM sales.orders"}')
arrow_table = client.do_get(ticket).read_all()

conn = duckdb.connect()
result = conn.execute("SELECT region, sum(amount) FROM arrow_table GROUP BY 1").df()
```

### Spark (PySpark)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC (פורט 50051)

נתיב שירות-לשירות. Provisa מייצרת `.proto` פר-תפקיד באתחול — כל תפקיד רואה רק את הטבלאות והעמודות שיש לו גישה אליהן.

הורידו את ה-proto עבור התפקיד שלכם:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

השתמשו ב-`grpc_server_reflection` לגילוי הסכמה באופן פרוגרמטי.

כל RPC חייב לשאת אישור במפתח metadata `authorization` — אסימון ספק או אסימון גישה אישי. `x-provisa-role` מבקש תפקיד מתוך הקבוצה המורשית של הזהות; זה אינו אישור ולעולם לא היה. תעודות לקוח נתמכות. ראו [Security Model](security.md#_12).

שאילתות בזרימה (streaming) מנפיקות הודעה אחת לכל שורה; mutations הן unary.

---

## הפעלת Commands על פני פרוטוקולים

**command** הוא פונקציה עקובה רשומה או webhook — יישות ניתנת לקריאה (callable) הרשומה בשכבה הסמנטית של Provisa עם `kind` (`query` או `mutation`) ו-`impl_kind` המתאר כיצד היא רצה. כל משטח מנתב הפעלות דרך מבצע מנוהל יחיד (`invoke_tracked_function`) שאוכף `writable_by` וממשל באופן אחיד (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | מה רץ | שדות קישור |
| ------------ | ----------- | --------------- |
| `source_procedure` | פרוצדורה מאוחסנת על מקור רשום (ברירת מחדל) | `sourceId`, `schemaName`, `functionName` |
| `script` | סקריפט בצד השרת | `script` |
| `http` | קריאת HTTP יוצאת | `url`, `method` |
| `grpc` | קריאת gRPC יוצאת לשרת חיצוני | `target`, `method` |
| `python` | יישות Python ניתנת לקריאה המתארחת על ידי Provisa (REQ-885) | `callable` (למשל `demo.py_functions:random_dataset`) |

כאשר command מצהיר `return_schema` (JSON Schema עם `type: array, items: object`), הוא set-returning — כל משטח משליך אותו כסט שורות בעל טיפוס. ה-commands לדוגמה `random_python_set` (impl_kind `python`) ו-`random_grpc_set` (impl_kind `grpc`) ממחישים גם יישות מתארחת ניתנת לקריאה וגם גשר gRPC חיצוני המחזיר שורות בעלות ערך אקראי; שניהם רשומים ב-`config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### מטריצת פרוטוקולים

| משטח | תחביר | דוגמה |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → שדה Query; `kind=mutation` → שדה Mutation; עם קידומת דומיין כאשר `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` או `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — ארגומנטים לפי מיקום ממופים לשמות הארגומנטים המוצהרים | `CALL random_python_set(3, 7)` |
| Provisa gRPC (פורט 50051) | Unary `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

שדה ה-`kind` שולט במיקום GraphQL בלבד — משטחי SQL, Cypher, Bolt, ו-gRPC מקבלים commands מסוג `query` ו-`mutation` באופן זהה.

---

## Apollo Federation

Provisa יכולה לפעול כ-subgraph של Federation v2, וחושפת את הסכמה המפורסמת שלה ל-Apollo Router או Apollo Gateway.

### הגדרה

הפעילו federation ב-`config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa מייצרת דירקטיבות `@key` על עמודות מפתח ראשי ו-`@external`/`@provides` על קשרים חוצי-subgraph באופן אוטומטי.

### רישום עם Apollo Router

ב-`supergraph.yaml` שלכם:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

הריצו `rover supergraph compose --config supergraph.yaml` לייצור סכמת ה-supergraph.

### ישויות (Entities)

Provisa מגיבה לשאילתות `_entities` עבור joins חוצי-subgraph. כל טבלה עם מפתח ראשי ניתנת לפתירה אוטומטית כישות Federation.

---

## ייבוא Hasura v2 / DDN

ראו [docs/import.md](import.md) עבור מעבר מ-Hasura ל-Provisa.

---

## Kafka

ראו [docs/sources.md](sources.md#kafka) עבור קונפיגורציית נושאי (topics) Kafka כטבלאות לקריאה בלבד ופתחי יעד (sinks) לתוצאות שאילתה.

---

## בודקי איכות נתונים (Data Quality Checkers) (REQ-1443)

Soda Core ו-Great Expectations מתחברים ל-Provisa באותו אופן שבו כל לקוח postgres אחר מתחבר — דרך pgwire. זו כל האינטגרציה: הבודק מחזיק מנהל התקן postgres אחד וסורק את התצוגה הפדרטיבית, כך שטבלת Snowflake, טבלת Iceberg, ואוסף Mongo נבדקים כולם על ידי אותו dialect חוזה, ללא בודק פר-מערכת. [tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

הסריקה רצה בפרשן (interpreter) ילד — `python -m provisa.dq.worker` — שהוא המקום היחיד שבו `soda_core` או `great_expectations` מיובאים. שום דבר לא מקושר לתהליך השרת, וקריסת בודק מפילה תת-תהליך ולא את לולאת האירועים. [tool-verified: `provisa/dq/runner.py` `build_command`]

תוצאות הסריקה נוחתות כשורות מקור רגילות, כך שקצב (cadence), טריות (freshness), אירועים, lineage, ממשל, RLS, ה-grid והייצוא — כולם חלים ללא מנגנון שני. כתיבת חוזים, מעטפת התוצאה, והרישום הנגזר מכוסים ב-[docs/sources.md](sources.md#req-1443).

### התקנת בודק

אף אחת מהספריות לא מגיעה כברירת מחדל. המתקין שואל איזו אתם רוצים, והתשובה הופכת ל-`dq_checker: none|soda|gx` ב-`~/.provisa/config.yaml`. בשכבת Docker `scripts/provisa` הופך את זה לארגומנט build `PROVISA_EXTRAS`; בשכבה native `first-launch.sh` מתקין את ה-pyproject extra המתאים לתוך ה-venv. [tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | ספרייה | רישיון | שכבת ענן מתארחת (hosted cloud plane) |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | מסורב (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | מותר |

Elastic License 2.0 אוסר על אספקת התוכנה לצדדים שלישיים כשירות מתארח, וזה בדיוק מה שהרצת Soda בתוך שכבת ה-SaaS מטעם דייר תהיה. פריסה מתארחת שרוצה Soda מפנה לנקודת קצה Soda שהמפעיל מריץ בעצמו. ראו [docs/configuration.md](configuration.md#soda-great_expectations) עבור מפתחות החיבור.

---

## חילופי סמנטיקה עם Apache Ossie (REQ-1316)

Provisa מחליפה מודלים סמנטיים עם Apache Ossie (spec 0.2.0.dev0, בדגירה; לשעבר Open
Semantic Interchange) דרך מתאם גבול (boundary adapter). אוצר המילים הפנימי של Provisa לעולם לא
משנה שם לזה של Ossie — ה-spec מצהיר על שינויים שוברים כסבירים, כך שהצימוד (coupling) מוגבל למתאם.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### ייצוא

משטח הייצוא הקנוני הוא נקודת קצה HTTP חיה. היא גוזרת את מסמך ה-Ossie ממצב חי בכל בקשה — ללא
מטמון, ללא שלב חילול.

```http
GET /admin/ossie
```

התגובה היא מסמך YAML עם `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

דף ה-Metrics גם מציע כפתור **הורדה** וכתובת נקודת קצה הניתנת להעתקה בפאנל Ossie
Interchange, שתיהן מצביעות לאותה נקודת קצה.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### מה מיוצא

המתאם ממפה אובייקטי Provisa לאובייקטי Ossie באופן הבא:

| אובייקט Provisa | אובייקט Ossie | הערות |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; מפתחות ראשיים/ייחודיים מקונפיגורציית עמודה ו-`UniqueConstraint` |
| `Column` | `field` | `expression` = הפניית עמודה (dialect ANSI_SQL); עמודות זמן מקבלות `dimension.is_time: true` |
| `Relationship` | `relationship` | Alias משמש כשם כאשר מוגדר; קשרים מחושבים (function-target) מדולגים |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — ללא אובדן (lossless) בעיצוב |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | round-trip בלבד; כלים אחרים עשויים להתעלם |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

ממשל, RLS, lineage, וסמנטיקת גרף אינם מיוצאים. הם עשויים לנסוע בחריץ ה-`custom_extensions`
האופציונלי של `provisa` לצורך נאמנות round-trip, אך חילופים לעולם אינם תלויים בכלים אחרים
שקוראים זאת. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

טיפוסי עמודה של Provisa שאינם מוכרים עוברים כפי שהם (verbatim); המתאם לעולם לא ממפה בשקט
לטיפוס שגוי. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### מיפוי טיפוסים

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| טיפוס Provisa / מקור | `datatype` ב-Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| כל דבר אחר | עובר כפי שהוא |

### ייבוא

הייבוא מקבל מסמך Ossie (YAML או JSON) ומחזיר הצעות רישום. שום דבר לא נרשם אוטומטית — הגדרות
מיובאות לעולם לא עוקפות את שלב הסקירה.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

השרת מנתח (parses) את המסמך עם `parse_ossie_model`, המאמת מבנה ומחזיר מחלקת נתונים (dataclass)
`OssieImport` המכילה טבלאות, קשרים, ומדדים (metrics) מוצעים כמילונים רגילים. כל בעיה מבנית היא
`400` עם שגיאה בעלת נתיב-שם, למשל
`ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### מסך הסקירה

ב-UI, כפתור ה-**Import** (דף Metrics → פאנל Ossie Interchange) פותח בורר קבצים.
לאחר שהמסמך נשלח ונותח, נפתח מודל סקירה עם כל טבלה, קשר, ומדד מוצעים רשומים כפריט מסומן.
המודלר יכול לבטל סימון של כל דבר כדי להחריג אותו. לחיצה על **Apply** רושמת את הפריטים המסומנים
דרך ה-mutations הקיימים לרישום — טבלאות תחילה, אחר כך קשרים (שמפנים לטבלאות), ואחר כך מדדים.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

תפקיד המודלינג וההיסטוריה השמורים במסמך Ossie המיוצא על ידי Provisa עוברים round-trip נכון
דרך הייבוא. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## מדדים (Metrics) על פני פרוטוקולים (REQ-1319)

ההגדרה של מדד מנוהל — הביטוי, התיאור, ו-`ai_context` שלו — נוסעת עם הערך לתוך כל משטח שאילתה
דרך הרחבת מהדר (compiler) אחת. אין עותקים. המהדר שומר את הסכמה `metrics` עבור גישת SQL; כל
פרוטוקול מוסיף אז ערוץ metadata משלו.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

כתבו לכל מדד ככיחס וירטואלי (virtual relation) בסכמת ה-`metrics`. עמודות הממד שאתם בוחרים
הופכות ל-GROUP BY:

```sql
-- Grand total
SELECT value FROM metrics.net_revenue;

-- By region
SELECT region, value FROM metrics.net_revenue GROUP BY region;

-- By region and month, filtered
SELECT region, month, value
FROM metrics.net_revenue
WHERE net_revenue.status = 'completed'
GROUP BY region, month;
```

המהדר מרחיב את הצורה `metrics.<name>` לצירוף (aggregate) המקובץ האמיתי לפני שהממשל רץ.
תיאורי עמודות מוצגים כרשומות `pg_description`, כך ש-DBeaver ו-`\d+` ב-psql מציגים אותם.
[tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` נדחה — ציינו את העמודות במפורש.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

מדדים מוקרנים בתוך שדה השורש `_aggregate` כבלוק `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

טקסט ההגדרה (`description`, `ai_context`) מופיע בתיעוד ה-introspection של GraphQL, כך שכלים
מודעי-סכמה ו-codegen קולטים אותו אוטומטית.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (סוכני AI)

שני כלים חושפים מדדים ללקוחות MCP:

- **`list_metrics`** — מחזיר את כל המדדים המנוהלים הגלויים לסשן, עם `name`,
  `description`, ו-`ai_context`.
- **`query_metric`** — מקבל שם מדד ורשימת ממדים וקורא לנתיב ה-SQL הסמנטי של המהדר,
  ומחזיר את תוצאת הצירוף.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

סוכנים שקוראים ל-`list_metrics` לפני בניית שאילתה בוחרים מדד מנוהל לפי שם במקום לכתוב SQL
צירוף (aggregation) ידנית. שדה ה-`ai_context` הוא המקום להציב בו את טקסט ההגדרה שמנחה בחירה
נכונה.

### Arrow Flight

מדדים ניתנים לכתובת כתיאורי טיסה (flight descriptors) של metric המחזירים טבלאות Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

השתמשו באותה צורת SQL `metrics.<name>` דרך נתיב ticket הסטנדרטי של Flight SQL.

### Bolt / Cypher (Neo4j Browser)

קריאה למדד באמצעות הפרוצדורה `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

טבלאות Fact ו-Dimension נושאות תוויות צומת `:Fact` ו-`:Dimension` בגרף הפדרטיבי, כך ש-Bloom
מרנדר את צורת הכוכב אוטומטית.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### שאילתות בשפה טבעית

מתאם הסכמה של NL פותר אוצר מילים של מדדים בשאלות בשפה טבעית ישירות למדד ולממדים, ואז מייצר
SQL סמנטי. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

טבלאות Fact מתויגות `[fact]` בפרומפט ה-NL; טבלאות dimension מתויגות `[dimension]`. המתאם
מטה נתיבי join מ-fact ל-dimension בעת פתירת שאלות.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### זרימה (Streaming)

שילוב `view_metrics` עם `materialize` ופתח יעד (sink) של Kafka מפיק פלט מדד מסוג push-on-change
באמצעות מכניקת ההמחשה הקיימת. אין צורך ב-pipeline חדש.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observability (OTel)

הערכות מדד עוקבות (traced) וניתנות לייצוא כמדדי OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
