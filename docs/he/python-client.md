# לקוח Python (`provisa-client`) {: #python-client-provisa-client }

לקוח Python עבור Provisa. מספק ארבע ממשקים:

| ממשק | מקרה שימוש |
| ----------- | ---------- |
| `ProvisaClient` | שאילתות GraphQL, Arrow Flight, פלט DataFrame |
| DB-API 2.0 (`connect`) | ממשק מסד נתונים סטנדרטי של Python (PEP 249) |
| דיאלקט SQLAlchemy | כלי BI, ORM, `read_sql`‏ Pandas |
| ADBC | סטרימינג עמודתי ילידי-Arrow דרך Flight |

## התקנה {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### התחלה מהירה {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` מקבל אישור, ולא שם משתמש וסיסמה: אין בו שלב התחברות משלו. אסימון גישה אישי הוא האישור המתאים כששריפט אמור לרוץ ללא השגחה — הוא מונפק מהפרופיל של המשתמש עצמו, נושא תפוגה, וניתן לביטול בלי לגעת בחשבון. אסימון bearer של הספק פועל באותו אופן. כל אחד מהם נמסר ב-`token`, והלקוח מציג אותו גם בנתיב ה-HTTP וגם בנתיב Arrow Flight.

כדי להמיר סיסמה לאסימון, שלחו POST אל `/auth/login` וקראו את `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

נקודות הכניסה של DB-API ושל ADBC מבצעות עבורכם את ההמרה הזו — ראו להלן.

### שאילתות GraphQL {: #graphql-queries }

```python
# Raw response dict
result = client.query("{ orders { id amount region } }")

# With variables
result = client.query(
    "query Q($region: String!) { orders(region: $region) { id amount } }",
    variables={"region": "west"},
)

# pandas DataFrame (first root field is flattened)
df = client.query_df("{ orders { id amount region } }")
```

### Async

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (עמודתי בעל-תפוקה-גבוהה) {: #arrow-flight-high-throughput-columnar }

השתמשו ב-Flight עבור סטי תוצאות גדולים — נתונים זורמים בסטרימינג כ-batches‏ רשומה של Arrow ללא מימוש בשרת.

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight מתחבר לפורט 8815 כברירת מחדל. עקפו עם `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### סקירת קטלוג {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### רפרנס חיבור {: #connection-reference }

| פרמטר | ברירת מחדל | תיאור |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL בסיס של שרת Provisa |
| `token` | `None` | אישור bearer — אסימון של הספק או אסימון גישה אישי; השמיטו עבור אימות סיסמה |
| `role` | `None` | התפקיד שבשמו לפעול, נשלח כ-`X-Provisa-Role` ומכובד רק אם הוא מוקצה למשתמש; אם הוא מושמט, הבקשה רצה בתפקיד של הזהות עצמה |
| `flight_port` | `8815` | פורט gRPC של Arrow Flight |

### טיפול בשגיאות {: #error-handling }

`query()` מעלה `httpx.HTTPStatusError` על שגיאות HTTP.
`query_df()` מעלה `RuntimeError` אם התגובה מכילה שגיאות GraphQL.

---

## שימוש ב-Provisa ב-Jupyter {: #using-provisa-in-jupyter }

התקינו בקרנל הפועל לפני הייבוא. התוסף `pandas` מכסה פלט כ-DataFrame; הוסיפו `sqlalchemy` אם צריך גם `pandas.read_sql` דרך הדיאלקט.

```python
%pip install "provisa-client[pandas]"
```

### אישורי גישה {: #credentials }

מחברות שומרות את פלט התאים בקובץ `.ipynb`. אסימון שמוקלד בתא יישמר בדיסק לצד הקוד שלכם. אל תעשו זאת.

`ProvisaClient` אינו קורא את הסביבה: העבירו לו את אישורי הגישה במפורש, ממשתני סביבה. (`provisa_client.query()`, להלן, קורא אותם בשבילכם.)

```python
import os
from provisa_client import ProvisaClient

client = ProvisaClient(
    os.environ["PROVISA_URL"],           # choose your own variable name
    token=os.environ["PROVISA_TOKEN"],   # personal access token
    role=os.environ.get("PROVISA_ROLE"), # optional; omit to run as your own role
    org=os.environ.get("PROVISA_ORG"),   # required on a multi-tenant deployment; None otherwise
)
```

הגדירו את המשתנים האלה במעטפת לפני הפעלת Jupyter, או טענו קובץ `.env` בעזרת `python-dotenv`. בכל מקרה, השאירו אותם מחוץ למחברת.

### קריאה אחת: `query()` {: #one-call-query }

`provisa_client.query()` מקבל משפט SQL, GraphQL או Cypher ומחזיר DataFrame של pandas. השרת מזהה באיזו שפה מדובר. דבר אינו נבנה מראש: החיבור מגיע מהסביבה או מקובץ פרופיל.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` פועל בתפקיד הזה לקריאה אחת, ונשלח כ-`X-Provisa-Role`. השרת מכבד אותו רק לתפקיד שיש לכם. הקריאה עוברת דרך Arrow Flight; הגדירו את `transport` ל-`http` כדי לשלוח אותה ל-`/data/query` במקום. `await provisa_client.aquery(...)` היא הצורה הא-סינכרונית.

החיבור נקרא מהמקום הראשון שמציין כל הגדרה:

1. ארגומנטים של `provisa_client.sign_in(...)`, שמפענח את החיבור פעם אחת לכל הקריאות שאחריו;
2. משתני סביבה: `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`;
3. מקטע של `~/.provisa/credentials`: `[default]`, או זה ש-`PROVISA_PROFILE` (או `sign_in(profile=...)`) מציין.

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

`token` הוא אסימון גישה אישי או אסימון הפעלה. תנו במקומו `username` ו-`password`, והלקוח מחליף אותם באסימון הפעלה ב-`POST /auth/login`. זה עובד בפריסה שספק הכניסה שלה מקבל סיסמה (basic, simple, LDAP). שמרו את הסיסמה ב-`PROVISA_PASSWORD` ולא בקובץ. כניסה שנדחתה, URL חסר או פרופיל בשם שאינו בקובץ מעלים שגיאה עם הסיבה. ל-Provisa אין תהליך כניסה שמחברת יכולה להפעיל דרך דפדפן, ולכן בפריסות עם כניסה יחידה השתמשו באסימון גישה אישי.

### פקודות קסם לתאים {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` ו-`%%cypher` עונים לתא באמצעות `query()`. ה-DataFrame הוא תוצאת התא. שם אחרי פקודת הקסם גם קושר אותו במחברת (`eu` למעלה); `--role` פועל בתפקיד הזה עבור התא.

גם JupySQL (`%load_ext sql`) מגדיר `%%sql`. טענו את שניהם ו-`%%sql` שייך להרחבה שנטענה אחרונה. זו של Provisa מקבלת `--role` וקוראת דרך Arrow Flight. גם JupySQL עובד, דרך דיאלקט SQLAlchemy: `%sql provisa+http://user:password@host:port` מתחבר, `%sql`/`%%sql` מריצים משפטים מנוהלים, ו-`%sqlcmd tables -s <domain>` ו-`%sqlcmd columns -t <table> -s <domain>` מציגים את הקטלוג.

### בדפדפן (Pyodide) {: #in-the-browser-pyodide }

תחת Pyodide, `query()` קורא מהשרת שהגיש את הדף, כמשתמש המחובר בדף. דף המארח שומר את המשתמש הזה בהיקף הגלובלי של הקרנל:

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` ריק עבור המקור של הדף עצמו. הלקוח קורא את `provisaAuth` בכל בקשה, כך שאסימון שהוחלף נכנס לתוקף בלי הפעלה מחדש. התוצאות מגיעות כ-Arrow כש-`pyarrow` טעון; בלעדיו הן מגיעות כ-JSON. משפט Cypher תמיד חוזר כ-JSON. `query()` חוסם, מה ש-Web Worker (שבו רץ קרנל של מחברת) יכול לעשות; בתהליכון הראשי של הדף הוא מעלה שגיאה, והקריאה שיש להשתמש בה היא `await provisa_client.aquery(...)`.

### DataFrames מ-GraphQL, SQL ו-Cypher {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` מקבל את שלוש שפות השאילתה ומחזיר DataFrame של pandas.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

תשובות GraphQL נושאות את הנתונים תחת שם של שדה שורש; `query_df()` מחלץ אותם. תשובות SQL ו-Cypher נושאות שמות עמודות מפורשים מהשרת וממופות ישירות.

### Arrow Flight לשליפות גדולות {: #arrow-flight-for-large-pulls }

`flight_df()` מזרים נתונים כאצוות רשומות Arrow במקום לפענח JSON. לטבלאות רחבות או לתוצאות של יותר מכמה מאות אלפי שורות, זה מהיר יותר וצורך פחות זיכרון.

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow שומר על סוגי העמודות – מספרים שלמים נשארים שלמים, חותמות זמן נשארות חותמות זמן. JSON לא. זו לבדה סיבה להעדיף את Flight כשהסכימה חשובה.

Flight מתחבר כברירת מחדל לפורט 8815. העבירו `flight_port=` לבנאי של `ProvisaClient` אם השרת שלכם משתמש בפורט אחר.

### `pandas.read_sql` דרך דיאלקט SQLAlchemy {: #pandasread_sql-through-the-sqlalchemy-dialect }

התקינו את התוסף `sqlalchemy` בקרנל:

```python
%pip install "provisa-client[sqlalchemy]"
```

לאחר מכן בנו engine עם URL מסוג `provisa+http://`. שימו את אישורי הגישה במשתני סביבה, לא במחרוזת ה-URL.

```python
import os
import pandas as pd
from sqlalchemy import create_engine

host = os.environ["PROVISA_HOST"]       # e.g. "localhost:8001"
user = os.environ["PROVISA_USER"]
pwd  = os.environ["PROVISA_PASSWORD"]

engine = create_engine(f"provisa+http://{user}:{pwd}@{host}")

df = pd.read_sql("SELECT id, amount FROM orders", engine)
```

הדיאלקט שולח את שם המשתמש והסיסמה ל-`/auth/login` ומחזיק את האסימון שהתקבל לכל אורך חיי ה-engine – אישורי הגישה אינם נשלחים מחדש בכל שאילתה.

---

## DB-API 2.0

ממשק [PEP 249](https://peps.python.org/pep-0249/) סטנדרטי. עובד עם כל כלי המקבל חיבור DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` שולח את שם המשתמש והסיסמה ב-POST אל `/auth/login` ושומר את ה-`access_token` שחוזר, כך שהחיבור נושא אישור אמיתי ולא רק שם. `role` *מבקש* תפקיד, והשרת נעתר רק אם התפקיד מוקצה לזהות; בהשמטה, החיבור רץ בתפקיד שההתחברות פתרה.

### ביצוע שאילתות {: #executing-queries }

הסמן מקבל GraphQL או SQL — מזוהה אוטומטית.

```python
cur = conn.cursor()

# GraphQL
cur.execute("{ orders { id amount region } }")
rows = cur.fetchall()           # list of tuples
one  = cur.fetchone()           # single tuple or None
many = cur.fetchmany(size=50)   # up to N tuples

# SQL (routed through Stage 2 governance)
cur.execute("SELECT id, amount FROM orders WHERE region = 'west'")
rows = cur.fetchall()
```

### מטא-דאטה עמודות {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### פרמטרים בעלי-שם {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### מנהלי הקשר (Context managers) {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## דיאלקט SQLAlchemy {: #sqlalchemy-dialect }

```bash
pip install "provisa-client[sqlalchemy]"
```

סכמת URL: `provisa+http://` או `provisa+https://`

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### עם pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### פרמטרי URL {: #url-parameters }

| פרמטר | תיאור | ברירת מחדל |
| ----------- | ------------- | --------- |
| `role` | התפקיד המבוקש; מאומת בצד השרת | התפקיד שההתחברות פותרת |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspection סכמה {: #schema-introspection }

הדיאלקט משקף את הקטלוג שמוגש לתפקיד שלכם, נקרא מ-`GET /data/catalog` כמשתמש, התפקיד והארגון של החיבור. כל דומיין הוא סכימה, וכל טבלה נמצאת באחת: `get_schema_names()` מציג את הדומיינים, `get_table_names(schema="shop")` מציג את טבלאות הדומיין לפי שמות ה-SQL שלהן, ו-`get_columns("orders", schema="shop")` נותן את הסוג, האפשרות ל-NULL והתיאור של כל עמודה. בלי סכימה שצוינה, `get_table_names()` אינו מציג אף אחת. טבלה לא מוכרת מעלה `NoSuchTableError`; קטלוג שהשרת מסרב לו מעלה שגיאה.

---

## ADBC

Arrow Database Connectivity מגובה Arrow Flight. מחזיר `pyarrow.Table` ישירות — ללא דה-סריאליזציית JSON.

```bash
pip install "provisa-client[adbc]"
```

```python
from provisa_client.adbc import adbc_connect

conn = adbc_connect(
    "http://localhost:8001",
    user="alice",
    password="secret",
    role="analyst",   # optional; server validates the requested role
    port=8815,        # Arrow Flight port
)
```

`adbc_connect` מתחבר תחילה מעל HTTP ומשבץ את האסימון שהתקבל בכל כרטיס Flight, כך ששרת ה-Flight מאמת את החיבור בדיוק כפי שממשק ה-REST עושה זאת. הארגומנט `role` הוא בקשה, המאומתת בצד השרת מול ההקצאות של הזהות — הוא לעולם אינו הופך לזהות.

### שליפה כטבלת Arrow {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### שליפה כ-tuples {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### מטא-דאטה עמודות {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### מנהל הקשר (Context manager) {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC מתחבר לשרת ה-Flight על פורט 8815 כברירת מחדל. העבירו `port=` כדי להגיע לשרת Flight המקושר לפורט לא-ברירת-מחדל.
