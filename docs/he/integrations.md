# אינטגרציות

## בחירת נתיב חיבור

| סוג לקוח | נתיב מומלץ | למה |
|-------------|-----------------|-----|
| כלי BI (Tableau, Power BI, Looker) | JDBC | סטרימינג עמודתי Arrow Flight על גבי החוט; לכלי BI יש אשף JDBC מובנה והם נהנים מאספקה עמודתית בעלת תפוקה גבוהה עבור סטים גדולים של תוצאות |
| psql, DBeaver, כל כלי תואם-PG | pgwire (דרייבר PG ילידי) | ברירת מחדל חסרת-חיכוך — אין צורך בדרייבר מותאם אישית; השתמשו במה שיש לכם כבר |
| מחסנית נתונים Python (pandas, pyarrow) | `provisa-client` או ADBC גולמי | batches‏ Arrow בסטרימינג; ללא overhead של סריאליזציית שורות |
| Spark, DuckDB, צינורות בעלי-תפוקה-גבוהה | Arrow Flight (ADBC) | סטרימינג עמודתי בלתי-מוגבל ישירות לזיכרון Arrow |
| שירות-לשירות (חוזים מוקלדים) | Protobuf gRPC | proto מיוצר לכל תפקיד; שורות בסטרימינג; בטיחות טיפוסים |
| אפליקציות web, סקריפטים | HTTP (`/data/graphql`, `/data/sql`) | ללא דרייבר; HTTP סטנדרטי; בחירה מלאה של שפת שאילתה |
| לקוחות REST (תקן JSON:API) | `GET /data/jsonapi/{table}` | מעטפת JSON:API v1.0; קבוצות שדות דלילות, pagination, פילטור דרך פרמטרי שאילתה; ללא דרייבר |

---

## pgwire — דרייבר PostgreSQL ילידי

Provisa מממשת את פרוטוקול החוט של PostgreSQL (גרסת פרוטוקול 3.0). כל לקוח הדובר PostgreSQL מתחבר ללא דרייבר מותאם אישית.

הפעילו על ידי הגדרת `PROVISA_PGWIRE_PORT` (לדוגמה `5433`) לפני הפעלת Provisa. מבוטל כאשר לא מוגדר או `0`.

### למה pgwire במקום JDBC?

דרייבר ה-JDBC משתמש ב-Arrow Flight כתעבורה שלו ודורש פריסת `provisa-jdbc.jar`. pgwire אינו דורש דבר — אם כבר יש לכם `psql`, DBeaver, SQLAlchemy, או דרייבר PG JDBC, סיימתם. זהו הנתיב בעל-החיכוך-הנמוך יותר עבור עומסי עבודה מבוססי-SQL בלבד.

JDBC הוא הבחירה הנכונה עבור כלי BI שיש להם אשף חיבור JDBC מובנה ונהנים מהסטרימינג העמודתי של Arrow Flight עבור סטי תוצאות גדולים. pgwire מקבל SQL חופשי מול הסכמה המפורסמת המלאה — אותן שאילתות, עלות הקמה נמוכה יותר.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. חיבור חדש ← PostgreSQL
2. Host: `localhost`, Port: `5433`
3. שם משתמש / סיסמה כפי שהוגדרו ב-Provisa
4. אין צורך בהורדת דרייבר נוסף

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

### אימות

pgwire משתמשת באימות סיסמה בטקסט-גלוי המגושר לספק האימות המוגדר של Provisa (`none` או `simple`). במצב אמון (`none`), שם המשתמש ממופה ישירות לתפקיד — הסיסמה מתעלמת. MD5 אינו נתמך; הפעילו TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) בעת הרצה על רשת לא-מהימנה.

### מגבלות

- SQL בלבד. GraphQL ו-Cypher אינם מתקבלים דרך pgwire.
- לא read-only. `COPY ... FROM STDIN` מכניס שורות למקורות `postgresql`, `mysql`, `sqlite`, ו-`mariadb`, ו-DDL נתמך (ראו למטה).
- DDL (`CREATE`, `ALTER`, `DROP`) נתמך ומנותב לנתיב Trino או הישיר; הטבלה החדשה נרשמת להקשר הקומפילציה וניתנת לשאילתה מיידית. `COPY ... TO STDOUT` (ייצוא) ו-`COPY ... FROM STDIN` (ייבוא) נתמכים בפורמטים `text` ו-`csv`.
- שאילתות `information_schema` ו-`pg_catalog` מיורטות ונענות משכבת קטלוג DuckDB — כלי גילוי סכמה עובדים כראוי.

---

## דרייבר JDBC

דרייבר ה-JDBC של Provisa משתמש ב-Arrow Flight כתעבורה הבסיסית שלו. זהו הנתיב המומלץ עבור כלי BI עם אשף חיבור JDBC.

### חיבור

הורידו את [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (תמיד ה-release העדכני ביותר) והוסיפו אותו לנתיב הדרייברים של הכלי שלכם.

URL של JDBC:
```
jdbc:provisa://<host>:8815
```

האימות משתמש במאפייני JDBC סטנדרטיים `user` / `password`. Provisa מאמתת את האישורים מול ספק האימות המוגדר ומקצה את התפקיד — הלקוח אינו בוחר תפקיד משלו.

### הגדרת כלי BI

**Tableau**
1. Manage ← Drivers ← Install Provisa JDBC
2. Connect ← Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. הזינו שם משתמש וסיסמה כשמתבקש

**DBeaver** (נתיב JDBC — עבור נתיב pgwire ראו למעלה)
1. Database ← New Connection ← JDBC
2. Driver: הוסיפו `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. הזינו שם משתמש וסיסמה בלשונית Authentication

**Power BI** — השתמשו ב-ODBC gateway עם ה-bridge JDBC-ODBC של Provisa (כלול במתקין).

---

## לקוחות Arrow Flight

Arrow Flight (פורט 8815) הוא הנתיב המומלץ עבור כלי נתונים התומכים בו. תוצאות עוברות בסטרימינג כ-RecordBatches של Arrow ללא מימוש בזיכרון Provisa.

### Python (`provisa-client`)

הנתיב המומלץ עבור Python — עוטף גם GraphQL וגם Arrow Flight:

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

ראו [docs/python-client.md](python-client.md) לרפרנס המלא כולל DB-API 2.0, דיאלקט SQLAlchemy, ו-ADBC.

### Python (PyArrow גולמי)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

הכרטיס אינו נושא תפקיד. השרת מקצה את התפקיד מספק האימות המוגדר. במקומות שבהם בחירת תפקיד מותרת, העבירו אותה במטא-דאטה של קריאת ה-gRPC תחת המפתח `x-provisa-role` (לדוגמה `flight.FlightCallOptions(headers=[(b"x-provisa-role", b"analyst")])`), לא ב-JSON של הכרטיס.

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

נתיב שירות-לשירות. Provisa מייצרת `.proto` לכל תפקיד בעת ההפעלה — כל תפקיד רואה רק את הטבלאות והעמודות שיש לו גישה אליהן.

הורידו את ה-proto עבור התפקיד שלכם:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

השתמשו ב-`grpc_server_reflection` כדי לגלות את הסכמה באופן פרוגרמטי.

התפקיד מועבר דרך מפתח המטא-דאטה `x-provisa-role` בכל RPC. שאילתות סטרימינג פולטות הודעה אחת לכל שורה; מוטציות הן unary.

---

## הפעלת פקודות על פני פרוטוקולים

**פקודה (command)** היא פונקציה במעקב או webhook רשומים — callable רשום בשכבה הסמנטית של Provisa עם `kind` (‏`query` או `mutation`) ו-`impl_kind` המתאר כיצד היא רצה. כל משטח מנתב הפעלות דרך מבצע ממושל יחיד (`invoke_tracked_function`) האוכף `writable_by` וממשל באופן אחיד (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | מה רץ | שדות קישור |
|------------|-----------|---------------|
| `source_procedure` | פרוצדורה מאוחסנת על מקור רשום (ברירת מחדל) | `sourceId`, `schemaName`, `functionName` |
| `script` | סקריפט צד-שרת | `script` |
| `http` | קריאת HTTP יוצאת | `url`, `method` |
| `grpc` | קריאת gRPC יוצאת לשרת חיצוני | `target`, `method` |
| `python` | callable של Python המתארח על ידי Provisa (REQ-885) | `callable` (לדוגמה `demo.py_functions:random_dataset`) |

כאשר פקודה מכריזה על `return_schema` (סכמת JSON עם `type: array, items: object`), היא מחזירת-סט — כל משטח מקרין אותה כסט שורות מוקלד. פקודות ההדגמה `random_python_set` (‏`impl_kind` = `python`) ו-`random_grpc_set` (‏`impl_kind` = `grpc`) ממחישות הן callable מתארח והן bridge‏ gRPC חיצוני המחזירים שורות בעלות ערכים אקראיים; שתיהן רשומות ב-`config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### מטריצת פרוטוקולים

| משטח | תחביר | דוגמה |
|---------|--------|---------|
| GraphQL | `kind=query` ← שדה Query; `kind=mutation` ← שדה Mutation; מקודם-דומיין כאשר `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` או `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / דרייבר) | `CALL fn(args)` — ארגומנטים פוזיציוניים ממופים לשמות ארגומנט מוצהרים | `CALL random_python_set(3, 7)` |
| Provisa gRPC (פורט 50051) | Unary‏ `CallCommand(CommandRequest{name, args_json})` ← `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

השדה `kind` שולט במיקום ב-GraphQL בלבד — משטחי SQL, Cypher, Bolt, ו-gRPC מקבלים פקודות `query` ו-`mutation` באופן זהה.

---

## Apollo Federation

Provisa יכולה לפעול כ-subgraph‏ Federation v2, וחושפת את הסכמה המפורסמת שלה ל-Apollo Router או Apollo Gateway.

### הגדרה

הפעילו federation ב-`config.yaml`:
```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa מייצרת דירקטיבות `@key` על עמודות מפתח-ראשי ו-`@external`/`@provides` על קשרים חוצי-subgraph באופן אוטומטי.

### רישום עם Apollo Router

ב-`supergraph.yaml` שלכם:
```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

הריצו `rover supergraph compose --config supergraph.yaml` כדי לייצר את סכמת ה-supergraph.

### ישויות (Entities)

Provisa עונה לשאילתות `_entities` עבור joins חוצי-subgraph. כל טבלה עם מפתח ראשי ניתנת לפתרון אוטומטית כישות Federation.

---

## ייבוא Hasura v2 / DDN

ראו [docs/import.md](import.md) עבור הגירה מ-Hasura ל-Provisa.

---

## Kafka

ראו [docs/sources.md](sources.md#kafka) עבור תצורת טופיק Kafka כטבלאות read-only ו-sinks לתוצאות שאילתה.

---

## חילופין סמנטיים של Apache Ossie (REQ-1316)

Provisa מחליפה מודלים סמנטיים עם Apache Ossie (מפרט 0.2.0.dev0, incubating; לשעבר Open
Semantic Interchange) דרך מתאם גבול. אוצר המילים הפנימי של Provisa לעולם אינו משנה שם
לזה של Ossie — המפרט מכריז על שינויים שוברים כסבירים, כך שהצימוד מוגבל למתאם.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### ייצוא

משטח הייצוא הקנוני הוא נקודת קצה HTTP חיה. היא גוזרת את מסמך Ossie ממצב חי
בכל בקשה — ללא מטמון, ללא שלב ייצור.

```
GET /admin/ossie
```

התגובה היא מסמך YAML עם `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

עמוד Metrics מציע גם כפתור **Download** ו-URL נקודת קצה הניתן להעתקה בפאנל Ossie
Interchange, שניהם מפנים לאותה נקודת קצה.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### מה מיוצא

המתאם ממפה אובייקטי Provisa לאובייקטי Ossie כדלקמן:

| אובייקט Provisa | אובייקט Ossie | הערות |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; מפתחות ראשיים/ייחודיים מתצורת עמודה ומ-`UniqueConstraint` |
| `Column` | `field` | `expression` = הפניית עמודה (דיאלקט ANSI_SQL); עמודות זמן מקבלות `dimension.is_time: true` |
| `Relationship` | `relationship` | כינוי (alias) נעשה בו שימוש כשם כאשר מוגדר; קשרים מחושבים (יעד-פונקציה) מדולגים |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — ללא-אובדן בעיצוב |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | round-trip בלבד; כלים אחרים עשויים להתעלם |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

ממשל, RLS, ליניאז', וסמנטיקת גרף אינם מיוצאים. הם עשויים לנוע בחריץ
`custom_extensions` האופציונלי של `provisa` עבור נאמנות round-trip, אך החילופין לעולם אינם תלויים בכך שכלים אחרים יקראו אותו. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

טיפוסי עמודה לא-מוכרים של Provisa עוברים כלשונם; המתאם לעולם אינו ממפה בשקט
לטיפוס שגוי. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### מיפוי טיפוסים

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| טיפוס Provisa / מקור | `datatype` של Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| כל דבר אחר | עובר כלשונו |

### ייבוא

הייבוא מקבל מסמך Ossie (YAML או JSON) ומחזיר הצעות רישום. שום דבר אינו
נרשם אוטומטית — הגדרות מיובאות לעולם אינן עוקפות את שלב הסקירה.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

השרת מפענח את המסמך עם `parse_ossie_model`, המאמת מבנה ומחזיר מחלקת נתונים
`OssieImport` המכילה טבלאות, קשרים, ומטריקות מוצעים כ-dicts רגילים.
כל בעיה מבנית היא `400` עם שגיאה בעלת-שם-נתיב, לדוגמה
`ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### מסך הסקירה

בממשק המשתמש, כפתור **Import** (עמוד Metrics ← פאנל Ossie Interchange) פותח בורר קבצים.
לאחר שהמסמך נשלח ומפוענח, נפתח modal סקירה עם כל טבלה, קשר, ומטריקה מוצעים
רשומים כפריט מסומן. המודלר יכול לבטל סימון של כל דבר כדי להחריג אותו.
לחיצה על **Apply** רושמת את הפריטים המסומנים דרך מוטציות הרישום הקיימות — טבלאות
תחילה, ואז קשרים (המפנים לטבלאות), ואז מטריקות.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

תפקיד המידול (modeling role) והיסטוריית המידול המאוחסנים במסמך Ossie מיוצא-Provisa
עוברים round-trip נכון דרך הייבוא. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## מטריקות על פני פרוטוקולים (REQ-1319)

ההגדרה של מטריקה ממושלת — הביטוי, התיאור, וה-`ai_context` שלה — נעה עם
הערך לכל משטח שאילתה דרך הרחבת קומפיילר יחידה. אין עותקים. הקומפיילר
שומר את הסכמה `metrics` עבור גישת SQL; כל פרוטוקול אז מוסיף ערוץ מטא-דאטה משלו.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

פנו לכל מטריקה כרלציה וירטואלית בסכמת `metrics`. עמודות הממד שאתם בוחרים
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

הקומפיילר מרחיב את הצורה `metrics.<name>` לצירוף המקובץ האמיתי לפני שהממשל
רץ. תיאורי עמודות נחשפים כרשומות `pg_description`, כך ש-DBeaver ו-`\d+` של psql
מציגים אותם. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` נדחה — יש לציין את העמודות במפורש.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

מטריקות מוקרנות בתוך שדה השורש `_aggregate` כבלוק `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

טקסט ההגדרה (`description`, `ai_context`) מופיע במסמכי ה-introspection של GraphQL, כך ש
כלים מודעי-סכמה וכלי codegen קולטים אותו אוטומטית.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (סוכני AI)

שני כלים חושפים מטריקות ללקוחות MCP:

- **`list_metrics`** — מחזיר את כל המטריקות הממושלות הנראות ל-session, עם `name`,
  `description`, ו-`ai_context`.
- **`query_metric`** — מקבל שם מטריקה ורשימת ממדים וקורא לנתיב ה-semantic-SQL של הקומפיילר,
  ומחזיר את תוצאת הצירוף.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

סוכנים הקוראים ל-`list_metrics` לפני בניית שאילתה בוחרים מטריקה ממושלת לפי שם
במקום לכתוב SQL צירוף ידנית. השדה `ai_context` הוא המקום להציב את
טקסט ההגדרה המנחה בחירה נכונה.

### Arrow Flight

מטריקות ניתנות-לכתובת כ-descriptors‏ flight של מטריקה המחזירים טבלאות Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

השתמשו באותה צורת SQL‏ `metrics.<name>` דרך נתיב הכרטיס הסטנדרטי של Flight SQL.

### Bolt / Cypher (Neo4j Browser)

קראו למטריקה באמצעות הפרוצדורה `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

טבלאות Fact ו-Dimension נושאות תוויות node‏ `:Fact` ו-`:Dimension` בגרף הפדרטיבי, כך ש
Bloom מרנדר את צורת הכוכב באופן אוטומטי.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### שאילתות בשפה טבעית

מתאם הסכמה של NL פותר אוצר מילים של מטריקות בשאלות בשפה-טבעית ישירות למטריקה
בתוספת ממדים, ואז מייצר SQL סמנטי. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

טבלאות Fact מתויגות `[fact]` בפרומפט ה-NL; טבלאות dimension מתויגות `[dimension]`. המתאם
מטה נתיבי join מ-fact לעבר dimension בעת פתרון שאלות.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

שילוב `view_metrics` עם `materialize` ו-sink‏ Kafka מייצר פלט מטריקה push-on-change
באמצעות מכונת המימוש הקיימת. אין צורך בצינור חדש.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observability (OTel)

הערכות מטריקה מתועדות (traced) וניתנות לייצוא כמטריקות OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
</content>
