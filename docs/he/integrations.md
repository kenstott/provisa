# אינטגרציות {: #integrations }

## בחירת נתיב חיבור {: #choosing-a-connection-path }

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

## pgwire — מנהל התקן PostgreSQL native {: #pgwire-native-postgresql-driver }

Provisa מיישמת את פרוטוקול ה-wire של PostgreSQL (גרסת פרוטוקול 3.0). כל לקוח שדובר PostgreSQL מתחבר ללא מנהל התקן מותאם.

הפעילו על ידי הגדרת `PROVISA_PGWIRE_PORT` (לדוגמה `5433`) לפני הפעלת Provisa. מושבת כאשר לא מוגדר או `0`.

### מדוע pgwire ולא JDBC? {: #why-pgwire-instead-of-jdbc }

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

טבלת המשטח × השיטה המלאה נמצאת ב-[Security Model](security.md#surfaces-and-credentials). MD5 אינו נתמך; הפעילו TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) בעת הרצה על גבי רשת לא מהימנה.

### מגבלות {: #limitations }

- SQL בלבד. GraphQL ו-Cypher אינם מתקבלים דרך pgwire.
- לא לקריאה בלבד. שורות של טבלאות רשומות נכתבות עם `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` ו-`COPY ... FROM STDIN`, כל אחת מתקבלת לפי זכות ה-`write` של התפקיד, הענקות העמודות שלו ומסנן השורות שלו; ראו [כתיבת נתונים](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin).
- ללא הגדרות: `CREATE`, `ALTER` ו-`DROP` נדחים עם SQLSTATE 0A000, למעט טבלה זמנית של הסשן עצמו; ראו [הגדרות](pgwire.md#definitions-create-alter-drop). `COPY ... TO STDOUT` מייצא ב-`text`, `csv` ו-`binary`; `COPY ... FROM STDIN` קורא `text` ו-`csv`.
- שאילתות `information_schema` ו-`pg_catalog` מיורטות ונענות מ-shim קטלוג DuckDB — כלי גילוי סכמה עובדים כראוי.

---

## מנהל התקן JDBC {: #jdbc-driver }

מנהל ההתקן JDBC של Provisa משתמש ב-Arrow Flight כתעבורת הבסיס שלו. זהו הנתיב המומלץ עבור כלי BI בעלי אשף חיבור JDBC.

שאילתות עוברות דרך Arrow Flight כשפורט ה-Flight עונה בפתיחת החיבור, והתוצאות מגיעות אצווה אחר אצווה עם סוגי העמודות שלהן. פורט ה-Flight נחשב לפורט שליד פורט ה-HTTP (8815 ליד 8001); הגדירו את המאפיין `flight_port` כשזה לא המצב. כשפורט ה-Flight אינו נגיש בזמן ההתחברות, החיבור משתמש ב-API של HTTP לשאילתות שלו ורושם זאת ביומן ברמת INFO. כשחיבור כבר פועל דרך Flight, שגיאת שרת — אישורים שנדחו, תפקיד שאין למשתמש, הצהרה שנכשלה — מגיעה לקורא עם ההודעה של השרת; היא אינה מורצת שוב דרך HTTP.

עיון בסכמה (`getTables`, `getColumns`, מפתחות ראשיים וזרים) נקרא דרך אותו פורט Flight עם אישורי הכניסה: הדרייבר מפרט את הקטלוג של התפקיד שבשמו הוא פועל, ולכן טבלה או עמודה שלא הוענקו לתפקיד זה אינן מופיעות. מפתח שמפנה לטבלה או עמודה שלא הוענקו לתפקיד גם הוא אינו מפורט. בחיבור שבו פורט ה-Flight לא היה נגיש בהתחברות, הדרייבר קורא את אותו קטלוג דרך HTTP (`GET /data/catalog`): השרת בונה את שתי הרשימות באותה פונקציה, ולכן הטבלאות, שמות העמודות, הסוגים והמפתחות אינם תלויים בפורט שענה.

### חיבור {: #connection }

הורידו את [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (תמיד הגרסה האחרונה) והוסיפו אותו לנתיב מנהלי ההתקן של הכלי שלכם.

כתובת JDBC:

```yaml
jdbc:provisa://<host>:8815
```

האימות משתמש בתכונות `user` / `password` הסטנדרטיות של JDBC. הדרייבר מחליף אותן בטוקן סשן ושולח אותו עם כל בקשה; כניסה שנדחתה מכשילה את החיבור עם הסטטוס והסיבה של השרת. Provisa גוזרת את התפקיד מהזהות שנכנסה. התכונה האופציונלית `role` (או `?role=` בכתובת) מבקשת לפעול בתפקיד אחד שהמשתמש מחזיק, או בקבוצה של תפקידים כאלה מופרדים בפסיקים; תפקיד שאין למשתמש נדחה. בפריסה ללא ספק אימות אין כניסה, ושם המשתמש מתקבל כתפקיד המבוקש.

### הגדרת כלי BI {: #bi-tool-setup }

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

## לקוחות Arrow Flight {: #arrow-flight-clients }

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

### Python (PyArrow גולמי) {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight נושא את האישור שלו במטען ה-JSON, כשדה `token` — אסימון bearer של ספק או אסימון גישה אישי. גם ה-handshake וגם כל ticket מקבלים אותו, ושניהם מאמתים אותו באותו אופן, כך שלקוח שהתאמת (authenticated) ב-handshake עדיין מציג את האסימון בכל `do_get`. שדה `role` לצידו *מבקש* תפקיד; השרת גוזר את התפקידים המורשים של הזהות ומחליף לערך המורשה, כך ששדה role ב-ticket לעולם אינו הזהות עצמה. ראו [Security Model](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

גם עיון צריך את האישורים. `list_flights`, `get_flight_info` ו-`get_schema` אינם נושאים כרטיס, ולכן הם קוראים את האישורים מכותרת הקריאה `authorization: Bearer <token>` ואת התפקיד המבוקש מ-`x-provisa-role` (תפקיד אחד, או קבוצה מופרדת בפסיקים של תפקידים מוחזקים). כשהאימות פעיל, קריאת עיון ללא אישורים נדחית, והקטלוג שהיא מחזירה הוא של התפקיד המורשה: הטבלאות, העמודות והפקודות שהוענקו לתפקיד זה, ולא יותר. כרטיס קטלוג (כרטיס ללא `query`) נענה באותו אופן מה-`token` וה-`role` שלו. לכן לקוח חייב לשלוח את האישורים שלו לפני שהוא מעיין; `provisa-client` עושה זאת ב-`list_tables()`. בפריסה ללא ספק אימות הקריאות האלה מפרטות את הקטלוג כולו ואינן לוקחות אישורים.

```python
options = flight.FlightCallOptions(headers=[
    (b"authorization", b"Bearer provisa_pat_..."),
    (b"x-provisa-role", b"analyst"),
])
for info in client.list_flights(b"", options):
    print(info.descriptor.path)
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

## Protobuf gRPC (פורט 50051) {: #protobuf-grpc-port-50051 }

נתיב משירות לשירות. Provisa מייצרת `.proto` לכל תפקיד ומייצרת אותו מחדש בכל שינוי במודל או בתפקיד — כל תפקיד רואה רק את הטבלאות והעמודות שיש לו גישה אליהן. בשירות יש RPC לכל טבלה בשורש הסכמה של התפקיד; טבלאות מקושרות נגישות דרך הקשרים שלהן, כמו ב-GraphQL.

הורידו את ה-proto עבור התפקיד שלכם:

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

השתמשו ב-`grpc_server_reflection` כדי לגלות את הסכמה באופן תכנותי. הרפלקציה מתארת את השירות של התפקיד שבשמו הקריאה פועלת, אותו קובץ כמו ההורדה שלמעלה. כשהאימות כבוי, קריאת רפלקציה מציינת את התפקיד שלה כמו כל קריאה אחרת:

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

כל RPC חייב לשאת אישור במפתח metadata `authorization` — אסימון ספק או אסימון גישה אישי. `x-provisa-role` מבקש תפקיד מתוך הקבוצה המורשית של הזהות; זה אינו אישור ולעולם לא היה. תעודות לקוח נתמכות. ראו [Security Model](security.md#surfaces-and-credentials).

שאילתות בזרימה (streaming) מנפיקות הודעה אחת לכל שורה; mutations הן unary.

---

## הפעלת Commands על פני פרוטוקולים {: #invoking-commands-across-protocols }

**command** הוא פונקציה עקובה רשומה או webhook — יישות ניתנת לקריאה (callable) הרשומה במודל של Provisa עם `kind` (`query` או `mutation`) ו-`impl_kind` המתאר כיצד היא רצה. כל משטח מנתב הפעלות דרך מבצע מנוהל יחיד (`invoke_tracked_function`) שאוכף את שיוך ה-command (`visible_to`), את הזכות `write` למוטציה וממשל באופן אחיד.

| `impl_kind` | מה רץ | שדות קישור |
| ------------ | ----------- | --------------- |
| `source_procedure` | פרוצדורה מאוחסנת על מקור רשום (ברירת מחדל) | `sourceId`, `schemaName`, `functionName` |
| `script` | סקריפט בצד השרת | `script` |
| `http` | קריאת HTTP יוצאת | `url`, `method` |
| `grpc` | קריאת gRPC יוצאת לשרת חיצוני | `target`, `method` |
| `python` | יישות Python ניתנת לקריאה המתארחת על ידי Provisa | `callable` (למשל `demo.py_functions:random_dataset`) |

כאשר command מצהיר `return_schema` (JSON Schema עם `type: array, items: object`), הוא set-returning — כל משטח משליך אותו כסט שורות בעל טיפוס. ה-commands לדוגמה `random_python_set` (impl_kind `python`) ו-`random_grpc_set` (impl_kind `grpc`) ממחישים גם יישות מתארחת ניתנת לקריאה וגם גשר gRPC חיצוני המחזיר שורות בעלות ערך אקראי; שניהם רשומים ב-`config/provisa-install.yaml`.

### מטריצת פרוטוקולים {: #protocol-matrix }

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

### הגדרה {: #setup }

הפעילו federation ב-`config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa מייצרת דירקטיבות `@key` על עמודות מפתח ראשי ו-`@external`/`@provides` על קשרים חוצי-subgraph באופן אוטומטי.

### רישום עם Apollo Router {: #register-with-apollo-router }

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

## ייבוא Hasura v2 / DDN {: #hasura-v2-ddn-import }

ראו [docs/import.md](import.md) עבור מעבר מ-Hasura ל-Provisa.

---

## Kafka

ראו [docs/sources.md](sources.md#kafka-sources) עבור קונפיגורציית נושאי (topics) Kafka כטבלאות לקריאה בלבד ופתחי יעד (sinks) לתוצאות שאילתה.

---

## בודקי איכות נתונים (Data Quality Checkers)

Soda Core ו-Great Expectations מתחברים ל-Provisa באותו אופן שבו כל לקוח postgres אחר מתחבר — דרך pgwire. זו כל האינטגרציה: הבודק מחזיק מנהל התקן postgres אחד וסורק את התצוגה הפדרטיבית, כך שטבלת Snowflake, טבלת Iceberg, ואוסף Mongo נבדקים כולם על ידי אותו dialect חוזה, ללא בודק פר-מערכת.

הסריקה רצה בפרשן (interpreter) ילד — `python -m provisa.dq.worker` — שהוא המקום היחיד שבו `soda_core` או `great_expectations` מיובאים. שום דבר לא מקושר לתהליך השרת, וקריסת בודק מפילה תת-תהליך ולא את לולאת האירועים.

תוצאות הסריקה נוחתות כשורות מקור רגילות, כך שקצב (cadence), טריות (freshness), אירועים, lineage, ממשל, RLS, ה-grid והייצוא — כולם חלים ללא מנגנון שני. כתיבת חוזים, מעטפת התוצאה, והרישום הנגזר מכוסים ב-[docs/sources.md](sources.md#data-quality-checkers).

### התקנת בודק {: #installing-a-checker }

אף אחת מהספריות לא מגיעה כברירת מחדל. המתקין שואל איזו אתם רוצים, והתשובה הופכת ל-`dq_checker: none|soda|gx` ב-`~/.provisa/config.yaml`. בשכבת Docker `scripts/provisa` הופך את זה לארגומנט build `PROVISA_EXTRAS`; בשכבה native `first-launch.sh` מתקין את ה-pyproject extra המתאים לתוך ה-venv.

| `dq_checker` | ספרייה | רישיון | שכבת ענן מתארחת (hosted cloud plane) |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | מסורב (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | מותר |

Elastic License 2.0 אוסר על אספקת התוכנה לצדדים שלישיים כשירות מתארח, וזה בדיוק מה שהרצת Soda בתוך שכבת ה-SaaS מטעם דייר תהיה. פריסה מתארחת שרוצה Soda מפנה לנקודת קצה Soda שהמפעיל מריץ בעצמו. ראו [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations) עבור מפתחות החיבור.

---

## חילופי סמנטיקה עם Apache Ossie {: #apache-ossie-semantic-interchange }

Provisa מחליפה מודלים סמנטיים עם Apache Ossie (מפרט 0.2.0.dev0, באינקובציה; לשעבר Open
Semantic Interchange) דרך מתאם גבול. אוצר המילים הפנימי של Provisa לעולם אינו משנה את שמו
לאוצר המילים של Ossie — המפרט מצהיר ששינויים שוברים צפויים, ולכן הצימוד מוגבל למתאם.

טיוטת המפרט שינתה את צורתה בלי לשנות את מחרוזת הגרסה. המתאם עוקב אחר
המפרט כפי שהוא ב-commit `642cb894` של `apache/ossie` (2026-10-10): מודל אחד לכל מסמך,
עם `version`, `name`, `datasets`, `relationships` ו-`metrics` בשורש המסמך ו-
`datatype` מנומר. הייצוא נבדק בבדיקות יחידה מול
`core-spec/ossie-schema.json` של אותו commit.

### ייצוא {: #export }

משטח הייצוא הקנוני הוא נקודת קצה HTTP חיה. היא גוזרת את מסמך ה-Ossie מהמצב החי
בכל בקשה — ללא מטמון וללא שלב יצירה.

```http
GET /admin/ossie
```
התגובה היא מסמך YAML עם `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

עמוד המדדים מציע גם כפתור **הורדה** וכתובת URL של נקודת הקצה הניתנת להעתקה בלוח Ossie
Interchange, ושניהם מובילים לאותה נקודת קצה.

#### מה מיוצא {: #what-is-exported }

המתאם ממפה אובייקטי Provisa לאובייקטי Ossie כך:

| אובייקט Provisa | אובייקט Ossie | הערות |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; מפתחות ראשיים/ייחודיים מקונפיגורציית העמודות ו-`UniqueConstraint` |
| `Column` | `field` | `expression` = הפניה לעמודה (דיאלקט ANSI_SQL); עמודות זמן מקבלות `dimension.is_time: true` |
| `Relationship` | `relationship` | הכינוי משמש כשם כשהוא מוגדר; קשרים מחושבים (עם יעד פונקציה) מדולגים |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (ממופה כמו של עמודה), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | להלוך-ושוב בלבד; כלים אחרים רשאים להתעלם |

קשר על פני כמה עמודות מפרט כל עמודה ב-`from_columns` וב-`to_columns`.
מודל ללא טבלאות נדחה עם `422`: במסמך Ossie יש לפחות dataset אחד.

ממשל, RLS, lineage וסמנטיקת הגרף אינם מיוצאים. הם יכולים לעבור בחריץ האופציונלי
`provisa` ב-custom_extensions לשם נאמנות בהלוך-ושוב, אך החילופים לעולם אינם תלויים בכך שכלים אחרים
קוראים אותו.

סוג עמודה מחוץ לאוצר המילים של Ossie נכתב כ-`Opaque`, עם שם הסוג עצמו בהרחבת
`provisa` של השדה — זה מה שהמפרט קובע לסוג ידוע שאין לו
איבר עבורו. עמודה ללא סוג רשום אינה מציינת `datatype`. המתאם לעולם אינו ממפה לסוג שאין
לעמודה.

#### מיפוי סוגים {: #type-mapping }

| סוג Provisa / מקור | `datatype` של Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `String` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `Integer` |
| `numeric`, `decimal` | `Decimal` |
| `float`, `double`, `real` | `Float` |
| `bool`, `boolean` | `Boolean` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp`, `datetime` | `DateTime` |
| `timestamptz`, `timestamp with time zone` | `DateTimeTz` |
| כל סוג אחר | `Opaque`, עם שם הסוג בהרחבת `provisa` |

`Decimal` של Ossie אינו נושא דיוק וקנה מידה, ולכן `numeric(10,2)` מיוצא כ-`Decimal`
וחוזר מהייבוא כ-`decimal`.

### ייבוא {: #import }

הייבוא מקבל מסמך Ossie (YAML או JSON) ומחזיר הצעות רישום. דבר אינו
נרשם אוטומטית — הגדרות מיובאות לעולם אינן עוקפות את שלב הסקירה.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```
השרת מנתח את המסמך עם `parse_ossie_model`, שבודק את המבנה ומחזיר
dataclass של `OssieImport` עם הטבלאות, הקשרים והמדדים המוצעים כמילונים פשוטים.
כל בעיה מבנית היא `400` עם שגיאה הנקובה לפי נתיב, למשל
`ossie import: missing $.datasets[1].source`.

המייבא קורא רק את הצורה השטוחה של המסמך. שלושה דברים נדחים בשמם:

- מסמך שעוטף את המודל שלו ב-`semantic_model` — הצורה שהייתה לטיוטת המפרט לפני
  ספטמבר 2026. השגיאה אומרת שהמסמך קודם לצורה השטוחה, ואיך להמיר אותו.
- `version` שאינה `0.2.0.dev0`.
- `datatype` שאינו איבר באוצר המילים של Ossie; השגיאה נוקבת בנתיב ומפרטת
  את האיברים.

עמודה או מדד מוצעים מקבלים את הסוג שה-datatype של Ossie שלהם מייצג (`Integer` מציע
`bigint`, `DateTime` מציע `timestamp`). `Opaque` מציע את הסוג שהרחבת `provisa` נוקבת בו
כשהמסמך הגיע מ-Provisa, ואף סוג אחרת. `ai_context` שניתן
כאובייקט נקרא לפי ה-`instructions` שלו.

שדה מוצע כעמודה רק כשהביטוי שלו הוא עמודת מקור אחת. שדה
שהביטוי שלו הוא חישוב אינו מוצע; מסך הסקירה נוקב בו תחת הטבלה שלו
כלא מיובא. שדה שמשנה את שם העמודה שלו מוצע כאותה עמודה, עם שם
השדה ככינוי העמודה. ה-`source` של dataset חייב להיות בן שלושה חלקים,
`source.schema.table`; שאילתה או שם קצר יותר נדחים, והשגיאה אומרת זאת.

#### מ-dbt, Cube, Snowflake ושכבות סמנטיות אחרות {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

פרויקט Apache Ossie מתחזק ממירים בין Ossie לבין פורמטי המודל של כלים אחרים. הריצו את
הממיר של הכלי שלכם, ואז שלחו את הפלט שלו לנקודת הקצה של הייבוא או בחרו את הקובץ
במסך הסקירה שלהלן. Provisa קוראת מסמך Ossie; היא אינה קוראת את הקבצים של כלי אחר.

| מאיפה | פקודת הממיר | מה הוא קורא |
|---|---|---|
| dbt (שכבה סמנטית) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | ה-`semantic_manifest.json` ש-dbt כותב עבור MetricFlow |
| Cube | `ossie-cube import -i model/ -o model.yaml` | תיקיית מודל של Cube או קובצי cube ו-view בודדים |

באותו מאגר יש ממירים ל-Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData ואחרים; לכל אחד README משלו.

שתי מגבלות שכדאי לדעת לפני ההמרה:

- ממיר ה-dbt קורא רק את מניפסט השכבה הסמנטית. לפרויקט dbt ללא מודלים
  סמנטיים אין מה להמיר בו. הוא משמיט מדדי המרה, מדדים פרטיים ו
  ישויות עם מפתח טבעי, ומדפיס אזהרה לכל אחד.

- פלט הממיר עוקב אחר המפרט כפי שהוא ב-commit של הממיר עצמו. Provisa קוראת
  את הצורה השטוחה של commit `642cb894` של `apache/ossie`; ממיר שקודם לספטמבר 2026
  כותב את עטיפת `semantic_model`, ש-Provisa דוחה בשמה.

שני הנתיבים הורצו ב-2026-10-10 עם ממירים שהותקנו מ-commit `642cb894` של `apache/ossie`
(הם אינם מתפרסמים כחבילות; הם מותקנים מתיקיות `converters/dbt` ו-
`converters/cube` של המאגר ודורשים Python 3.11 ומעלה). מודל של
שתי טבלאות, קשר אחד ומדד אחד יוצא מ-Provisa, הומר למניפסט סמנטי של dbt
ולמודל Cube, הומר בחזרה ל-Ossie עם `ossie-dbt msi-to-ossie` ו-
`ossie-cube import`, ונקרא על ידי `parse_ossie_model`:

| נתיב | תוצאה |
|---|---|
| מודל Cube ← Ossie ← Provisa | נקרא. הטבלאות, המפתחות, סוגי העמודות, הקשר והמדד חזרו; שם הקשר היה שם הממיר (`orders_to_customers`). |
| מניפסט סמנטי של dbt ← Ossie ← Provisa | נקרא. הטבלאות, המפתחות הראשיים והמדד חזרו. סוגי העמודות לא (פלט הממיר אינו מציין אותם), והקשר חזר כפי שהממיר הסיק אותו מישויות dbt, שאינו הקשר הרשום. בדקו את הקשרים לפני ההחלה. |

ממיר ה-dbt דוחה מודל שבו לטבלה כלשהי יש אילוץ ייחודיות על פני כמה
עמודות ("MetricFlow entities cannot represent composite keys losslessly").

#### מסך הסקירה {: #the-review-screen }

בממשק, כפתור **ייבוא** (עמוד המדדים ← לוח Ossie Interchange) פותח בורר קבצים.
אחרי שהמסמך נשלח ונותח, נפתחת חלונית סקירה שבה כל טבלה,
קשר ומדד מוצעים מפורטים כפריטים מסומנים. המעצב יכול לבטל את הסימון של כל דבר כדי להחריג אותו.
**החלה** רושמת את הפריטים המסומנים דרך מוטציות הרישום הקיימות — קודם
הטבלאות, אחר כך הקשרים (שמפנים לטבלאות), ואז המדדים.

תפקיד המידול וההיסטוריה שנשמרו במסמך Ossie שיוצא מ-Provisa עוברים הלוך-ושוב נכון
בייבוא.

### קובץ sources של dbt {: #dbt-sources-file }

לפרויקט dbt שאינו משתמש בשכבה הסמנטית, Provisa כותבת `sources.yml` של dbt ישירות.
הוא נגזר מהמודל החי בכל בקשה.

```http
GET /admin/dbt/sources?role=<role id>
```
התגובה היא YAML עם `Content-Disposition: attachment; filename=provisa.sources.yml`.
הקורא צריך `org_settings`, ו-`role` חייב להיות תפקיד שהוא מחזיק; מחזיק
`access_config` רשאי לנקוב בכל תפקיד. לוח החילופים בעמוד המדדים מציע את אותה הורדה
עבור התפקיד הפעיל.

הקובץ מפרט כל טבלה שהתפקיד מקבל, תחת source של dbt לכל מקור וסכמה (שנקרא
לפי מזהה המקור, או `<source>_<schema>` כשלמקור יש טבלאות ביותר מסכמה אחת),
עם תיאור הטבלה, התיאורים של העמודות המוגשות והבדיקות האלה:

| המודל מציין | הקובץ כותב |
|---|---|
| מפתח ראשי מעמודה אחת | `unique` ו-`not_null` על אותה עמודה |
| מפתח ראשי מכמה עמודות | `not_null` על כל עמודה |
| אילוץ ייחודיות מעמודה אחת | `unique` על אותה עמודה |
| קשר בין שתי טבלאות ששתיהן מוגשות לתפקיד | `relationships` על עמודת המקור, אל `source('<source>', '<table>')` |

עמודה שאינה מוגשת לתפקיד חסרה, וכך גם טבלה ללא עמודות מוגשות. למפתח על פני
כמה עמודות אין בדיקת dbt סטנדרטית; הערת הפתיחה של הקובץ נוקבת בכל מפתח כזה, ו
לא נכתבת עבורו בדיקה. המודל אינו רושם אפשרות NULL, ולכן `not_null` מופיע רק בעמודות מפתח.
הקובץ אינו נושא מדדים, אבטחה ברמת השורה, מיסוך, תפקידים או lineage, והערת הפתיחה שלו
אומרת זאת.

קובץ שנוצר ממודל של שתי טבלאות וקשר אחד נותח על ידי dbt-core 1.12.5
עם dbt-duckdb 1.11.0: שני sources וחמש בדיקות, ללא אזהרות. dbt-core 1.9.11 אינו קורא
את בדיקות ה-`relationships` שלו, שמקננות את הארגומנטים שלהן תחת `arguments`; גרסאות ביניים
לא נבדקו.

---

## מדדים (Metrics) על פני פרוטוקולים {: #metrics-across-protocols }

ההגדרה של מדד מנוהל — הביטוי, התיאור, ו-`ai_context` שלו — נוסעת עם הערך לתוך כל משטח שאילתה
דרך הרחבת מהדר (compiler) אחת. אין עותקים. המהדר שומר את הסכמה `metrics` עבור גישת SQL; כל
פרוטוקול מוסיף אז ערוץ metadata משלו.

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

`SELECT *` נדחה — ציינו את העמודות במפורש.

### GraphQL

מדדים מוקרנים בתוך שדה השורש `_aggregate` כבלוק `metrics`.

טקסט ההגדרה (`description`, `ai_context`) מופיע בתיעוד ה-introspection של GraphQL, כך שכלים
מודעי-סכמה ו-codegen קולטים אותו אוטומטית.

### MCP (סוכני AI) {: #mcp-ai-agents }

שני כלים חושפים מדדים ללקוחות MCP:

- **`list_metrics`** — מחזיר את כל המדדים המנוהלים הגלויים לסשן, עם `name`,
  `description`, ו-`ai_context`.
- **`query_metric`** — מקבל שם מדד ורשימת ממדים וקורא לנתיב ה-SQL הסמנטי של המהדר,
  ומחזיר את תוצאת הצירוף.

סוכנים שקוראים ל-`list_metrics` לפני בניית שאילתה בוחרים מדד מנוהל לפי שם במקום לכתוב SQL
צירוף (aggregation) ידנית. שדה ה-`ai_context` הוא המקום להציב בו את טקסט ההגדרה שמנחה בחירה
נכונה.

### Arrow Flight {: #arrow-flight }

מדדים ניתנים לכתובת כתיאורי טיסה (flight descriptors) של metric המחזירים טבלאות Arrow.

השתמשו באותה צורת SQL `metrics.<name>` דרך נתיב ticket הסטנדרטי של Flight SQL.

### Bolt / Cypher (Neo4j Browser)

קריאה למדד באמצעות הפרוצדורה `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

טבלאות Fact ו-Dimension נושאות תוויות צומת `:Fact` ו-`:Dimension` בגרף הפדרטיבי, כך ש-Bloom
מרנדר את צורת הכוכב אוטומטית.

### שאילתות בשפה טבעית {: #natural-language-queries }

מתאם הסכמה של NL פותר אוצר מילים של מדדים בשאלות בשפה טבעית ישירות למדד ולממדים, ואז מייצר
SQL סמנטי.

טבלאות Fact מתויגות `[fact]` בפרומפט ה-NL; טבלאות dimension מתויגות `[dimension]`. המתאם
מטה נתיבי join מ-fact ל-dimension בעת פתירת שאלות.

### זרימה (Streaming)

שילוב `view_metrics` עם `materialize` שומר view של מדדים עדכני כשהקלטים שלו משתנים, באמצעות
מכניקת ההמחשה הקיימת. אין צורך ב-pipeline חדש.

### Observability (OTel)

הערכות מדד עוקבות (traced) וניתנות לייצוא כמדדי OpenTelemetry.
