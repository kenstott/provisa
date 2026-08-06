# לקוח Python (`provisa-client`)

לקוח Python עבור Provisa. מספק ארבע ממשקים:

| ממשק | מקרה שימוש |
| ----------- | ---------- |
| `ProvisaClient` | שאילתות GraphQL, Arrow Flight, פלט DataFrame |
| DB-API 2.0 (`connect`) | ממשק מסד נתונים סטנדרטי של Python (PEP 249) (REQ-268) |
| דיאלקט SQLAlchemy | כלי BI, ORM, `read_sql`‏ Pandas (REQ-270) |
| ADBC | סטרימינג עמודתי ילידי-Arrow דרך Flight (REQ-271) |

## התקנה

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### התחלה מהירה

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` מקבל אישור, ולא שם משתמש וסיסמה: אין בו שלב התחברות משלו. אסימון גישה אישי הוא האישור המתאים כששריפט אמור לרוץ ללא השגחה — הוא מונפק מהפרופיל של המשתמש עצמו, נושא תפוגה, וניתן לביטול בלי לגעת בחשבון. (REQ-1263) אסימון bearer של הספק פועל באותו אופן. כל אחד מהם נמסר ב-`token`, והלקוח מציג אותו גם בנתיב ה-HTTP וגם בנתיב Arrow Flight.

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

### שאילתות GraphQL

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

### Arrow Flight (עמודתי בעל-תפוקה-גבוהה)

השתמשו ב-Flight עבור סטי תוצאות גדולים — נתונים זורמים בסטרימינג כ-batches‏ רשומה של Arrow ללא מימוש בשרת. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight מתחבר לפורט 8815 כברירת מחדל. (REQ-143) עקפו עם `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### סקירת קטלוג

```python
tables_df = client.list_tables()
```

### רפרנס חיבור

| פרמטר | ברירת מחדל | תיאור |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL בסיס של שרת Provisa |
| `token` | `None` | אישור bearer — אסימון של הספק או אסימון גישה אישי; השמיטו עבור אימות סיסמה (REQ-606, REQ-1263) |
| `role` | `"admin"` | תפקיד הנשלח עם כל בקשה (REQ-273) |
| `flight_port` | `8815` | פורט gRPC של Arrow Flight (REQ-143) |

### טיפול בשגיאות

`query()` מעלה `httpx.HTTPStatusError` על שגיאות HTTP. (REQ-607)
`query_df()` מעלה `RuntimeError` אם התגובה מכילה שגיאות GraphQL. (REQ-607)

---

## DB-API 2.0

ממשק [PEP 249](https://peps.python.org/pep-0249/) סטנדרטי. (REQ-268) עובד עם כל כלי המקבל חיבור DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` שולח את שם המשתמש והסיסמה ב-POST אל `/auth/login` ושומר את ה-`access_token` שחוזר, כך שהחיבור נושא אישור אמיתי ולא רק שם. `role` *מבקש* תפקיד, והשרת נעתר רק אם התפקיד מוקצה לזהות (REQ-273); בהשמטה, החיבור רץ בתפקיד שההתחברות פתרה.

### ביצוע שאילתות

הסמן מקבל GraphQL או SQL — מזוהה אוטומטית. (REQ-268, REQ-274)

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

### מטא-דאטה עמודות

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### פרמטרים בעלי-שם

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### מנהלי הקשר (Context managers)

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## דיאלקט SQLAlchemy

```bash
pip install "provisa-client[sqlalchemy]"
```

סכמת URL: `provisa+http://` או `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### עם pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### פרמטרי URL

| פרמטר | תיאור | ברירת מחדל |
| ----------- | ------------- | --------- |
| `role` | התפקיד המבוקש; מאומת בצד השרת (REQ-273) | התפקיד שההתחברות פותרת |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspection סכמה

הדיאלקט מממש `get_table_names()`, `get_columns()`, ו-`has_table()` — כלי קטלוג (DBeaver, SQLAlchemy automap) יכולים לבחון את הסכמה. (REQ-363, REQ-270)

---

## ADBC

Arrow Database Connectivity מגובה Arrow Flight. (REQ-271) מחזיר `pyarrow.Table` ישירות — ללא דה-סריאליזציית JSON. (REQ-271)

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
    port=8815,        # Arrow Flight port (REQ-711)
)
```

`adbc_connect` מתחבר תחילה מעל HTTP ומשבץ את האסימון שהתקבל בכל כרטיס Flight, כך ששרת ה-Flight מאמת את החיבור בדיוק כפי שממשק ה-REST עושה זאת. (REQ-1263) הארגומנט `role` הוא בקשה, המאומתת בצד השרת מול ההקצאות של הזהות — הוא לעולם אינו הופך לזהות. (REQ-273)

### שליפה כטבלת Arrow

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### שליפה כ-tuples

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### מטא-דאטה עמודות

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### מנהל הקשר (Context manager)

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC מתחבר לשרת ה-Flight על פורט 8815 כברירת מחדל. (REQ-143) העבירו `port=` כדי להגיע לשרת Flight המקושר לפורט לא-ברירת-מחדל. (REQ-711)
</content>
