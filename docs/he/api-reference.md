# רפרנס API

## סקירה כללית

Provisa חושפת נקודות קצה REST תחת שתי קידומות: `/data` להרצת שאילתות ולביקורת סכמה (introspection), ו-`/admin` לניהול תצורה. (REQ-043) רוב נקודות הקצה של הנתונים דורשות מזהה תפקיד. פעולות תצורת ניהול משתמשות ב-API‏ Strawberry GraphQL בכתובת `/admin/graphql`. (REQ-164)

---

## אימות (Authentication)

כאשר `auth.provider` מוגדר ב-`provisa.yaml`, כל נקודות הקצה מלבד `/health` ו-`/setup/status` דורשות כותרת `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

ללא אימות מוגדר, השרת רץ במצב פיתוח. כל בקשה מטופלת כזהות `anonymous`, הממופה לכל התפקידים המוגדרים עם גישת דומיין כללית (wildcard). (REQ-535)

**התחברות (`POST /auth/login`)** מסופקת על ידי ספק האימות הפעיל כאשר `provider: basic` מוגדר. (REQ-124) פורמט האישורים והתגובה תלויים בספק.

**ביקורת זהות (Identity introspection):**

```
GET /auth/me
```

מחזיר את מזהה המשתמש המאומת, הדוא"ל, שם התצוגה, חברויות בארגונים, והקצאות תפקידים. במצב פיתוח מחזיר `dev_mode: true` עם כל מזהי התפקידים רשומים. [tool-verified: `provisa/api/auth_router.py`]

```
GET /auth/provider-type
```

מחזיר `{"provider": "<name>"}` או `{"provider": null}` כאשר האימות אינו מוגדר. [tool-verified: `provisa/api/auth_router.py`]

---

## נקודות קצה של נתונים

### `POST /data/graphql`

הרצת שאילתת GraphQL או מוטציה. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**גוף הבקשה:**
```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

השדה `role` משמש רק במצב פיתוח (ללא אימות). כאשר האימות פעיל, נעשה שימוש בתפקיד המשתמש המאומת ו-`role` בגוף הבקשה מתעלם.

השדה `extensions` תומך בפרוטוקול Automatic Persisted Query (APQ): (REQ-288)
```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**כותרות:**
- `X-Provisa-Role` — דריסת תפקיד (מצב פיתוח)
- `Accept` — פורמט תגובה (ראו משא ומתן תוכן)
- `Authorization` — `Bearer <token>` כאשר האימות מופעל
- `X-Provisa-Redirect-Format` — סוג MIME לפלט הפניית S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — סף ספירת שורות שמעליו ההפניה מופעלת (REQ-137)
- `X-Provisa-Redirect` — `true` לכפיית הפניה ללא תנאי (REQ-029)

**תגובה (JSON inline):**
```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**תגובה (הפניה):**
```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**תגובה (multi-root עם inline/הפניה מעורבים):**
```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

שאילתות multi-root מריצות כל שדה שורש באופן עצמאי. שדות מתחת לסף ההפניה מוחזרים inline; שדות מעל מופנים. המפתח `redirects` (רבים) ממפה שמות שדות למידע הפניה. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**כותרות מטמון:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (ב-HIT) (REQ-536)

**יכולות נדרשות:** `QUERY_DEVELOPMENT` עבור כל הבקשות כולל introspection. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### משא ומתן תוכן (Content Negotiation)

| כותרת Accept | פורמט |
|---|---|
| `application/json` | JSON (ברירת מחדל) |
| `application/x-ndjson` | JSON מופרד בשורות (Newline-delimited) |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### הפניה (Redirect)

תוצאות מעל סף שורות מוגדר (או כאשר `X-Provisa-Redirect: true`) נכתבות ל-S3 ומוחזר URL חתום מראש (presigned). (REQ-029, REQ-044)

| פורמט הפניה | נכתב על ידי | זיכרון |
|---|---|---|
| `application/vnd.apache.parquet` | CTAS פדרטיבי | ללא — הנתונים לעולם אינם עוברים דרך Provisa |
| `application/x-orc` | CTAS פדרטיבי | ללא — הנתונים לעולם אינם עוברים דרך Provisa |
| `application/json` | Provisa | תלוי-זיכרון |
| `application/x-ndjson` | Provisa | תלוי-זיכרון |
| `text/csv` | Provisa | תלוי-זיכרון |
| `application/vnd.apache.arrow.stream` | Provisa | תלוי-זיכרון |

עבור ייצוא אנליטי גדול, השתמשו בהפניית Parquet או ORC. מנוע הפדרציה כותב ישירות ל-S3 במקביל — אין נתונים העוברים דרך Provisa. (REQ-138)

```
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

הרצת SQL גולמי דרך צינור הממשל של שלב 2 (Stage 2 governance). (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**גוף הבקשה:**
```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin",
  "discovery_mode": false
}
```

הדגל `discovery_mode` מרחיב את בדיקת נראות הטבלה כדי לכלול את כל הטבלאות מכל ההקשרים. לכלי פנימיים בלבד. [tool-verified: `provisa/api/data/endpoint_dev.py:148-152`]

**יכולות נדרשות:** `QUERY_DEVELOPMENT`.

הפרות ממשל ב-`POST /data/sql` מחזירות HTTP 403. (REQ-002, REQ-266)

**תגובה:** אותו פורמט כמו `/data/graphql` (שורות JSON כברירת מחדל, במשא ומתן תוכן דרך `Accept`).

---

### `POST /data/query`

נקודת קצה שאילתה מאוחדת. מקבלת GraphQL, SQL, או Cypher — התחביר מזוהה אוטומטית. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

שאילתות Cypher יכולות גם להישלח לנקודת הקצה הייעודית ל-Cypher בלבד, `POST /query/cypher`. (REQ-345)

**גוף הבקשה:**
```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

מחזיר `{"data": ...}` עבור GraphQL, `{"columns": [...], "rows": [...]}` עבור SQL ו-Cypher.

---

### `GET /data/rest/{domain_id}/{table_name}`

נקודת קצה REST רגילה שמחוללת אוטומטית עבור כל טבלה רשומה. מחרוזת השאילתה ממופה לארגומנטים של GraphQL והבקשה מתקמפלת ומתבצעת דרך אותו צינור (RLS, מיסוך, ניתוב) כמו GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**פרמטרי שאילתה:**
- `limit` — מספר שורות מקסימלי (≥ 1)
- `offset` — דילוג על שורות (≥ 0)
- `fields` — שמות עמודות מופרדים בפסיקים (ברירת מחדל לכל השדות הסקלריים)
- `filter` — מערך JSON של אובייקטי פילטר `{"field", "comparator", "value"}`
- `orderBy` — מערך JSON של אובייקטי מיון `{"field", "direction"}`

התפקיד המאומת נדרש; בקשות לא-מאומתות מחזירות `401`. מפרט OpenAPI עבור routes אלה מוגש בכתובת `GET /data/rest/openapi.json` עם Swagger UI בכתובת `GET /data/rest/docs`.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

נקודת קצה תואמת [JSON:API](https://jsonapi.org) שמחוללת אוטומטית עבור כל טבלה רשומה. אותו RLS, מיסוך, וניתוב כמו GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**כותרת `Accept`:** חייבת לכלול `application/vnd.api+json` (סוג המדיה של JSON:API) אחרת הבקשה מחזירה `406`.

**פרמטרי שאילתה:**
- `fields[<type>]` — שדות דלילים (sparse fieldsets), לדוגמה `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — לדוגמה `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — מופרד בפסיקים, קידומת `-` לסדר יורד, לדוגמה `?sort=-created_at,amount`
- `page[number]` / `page[size]` — עימוד (pagination)

תגובות הן אובייקטי משאב עם `type`/`id`/`attributes`. שגיאות עוקבות אחר צורת אובייקט השגיאה של JSON:API.

---

### `POST /query/nl`

שליחת שאלה בשפה טבעית. השירות מתחיל job אסינכרוני ומחזיר `202 Accepted` עם `job_id` באופן מיידי. דורש ספק LLM מוגדר תחת סעיף התצורה `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**גוף הבקשה:**
```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

מחזיר `{"job_id": "<id>"}`. חריגה ממגבלת קצב ה-NL לפי-תפקיד מחזירה `429` עם כותרת `Retry-After`. (REQ-370)

**אחזור התוצאה:**

- `GET /query/nl/{job_id}` — polling. מחזיר את מסמך ה-job.
- `GET /query/nl/{job_id}/stream` — SSE. אירוע `branch` אחד לכל יעד חילול עם השלמתו, ואז אירוע `done`. (REQ-357, REQ-358)

שלושה לולאות חילול (Cypher, GraphQL, SQL) רצות במקביל, כל אחת מאומתת דרך המהדר ומתוקנת בשגיאה. (REQ-355) ה-prompt מוגבל לסכמה הנראית של התפקיד. (REQ-356) מסמך התוצאה ממפתח כל ענף (branch) לפי יעד: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

ענף שממצה את מגבלת האיטרציה שלו מחזיר `query: null`, `result: null`, ומחרוזת `error`. כל שאילתה מחוללת מבוצעת תחת הרשאות הצרכן עם ממשל שלב 2 מיושם — השירות לעולם אינו עוקף את הממשל. (REQ-359)

---

### `GET /data/sdl`

מחזיר את ה-SDL של GraphQL עבור סכמת תפקיד. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**כותרות:** `X-Role: <role_id>` (חובה)

**פרמטרי שאילתה:**
- `domain` — מזהי דומיין מופרדים בפסיקים. כאשר מוגדר, התגובה מסוננת לדומיין(ים) הנקוב(ים) ולטבלאות הנגישות מהם.

**תגובה:** `text/plain` SDL של GraphQL.

---

### `GET /data/introspection`

מחזיר JSON‏ introspection של GraphQL, מסונן-דומיין אופציונלית. [tool-verified: `provisa/api/data/sdl.py:200`]

**כותרות:** `X-Provisa-Role: <role_id>` (חובה)

**פרמטרי שאילתה:** `domain` — מזהי דומיין מופרדים בפסיקים.

**תגובה:** תוצאת introspection מסוג `application/json`.

---

### `GET /data/graph-schema`

מחזיר את תצוגת הגרף של סכמת התפקיד: תוויות node וסוגי הקשר שלהם, עבור לקוחות Cypher/גרף. כולל `pk_columns` לכל תווית node כך שקוראים יכולים לקבוע עמודות מפתח ראשי. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**תגובה:** `application/json` עם `node_labels` (כל אחת נושאת `pk`/`pk_columns`) ו-`relationship_types`.

---

### `GET /data/domains`

מחזיר מזהי דומיין נגישים לתפקיד המבקש. [tool-verified: `provisa/api/data/sdl.py:116`]

**כותרות:** `X-Role: <role_id>` (חובה)

**תגובה:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

מחזיר את מחרוזת גרסת הסכמה הנוכחית. משלב nonce לפי-הפעלה עם מונה חילול-מחדש. לקוחות משתמשים בזה לביטול תוקף מטמוני סכמה לאחר הפעלות מחדש של השרת. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**תגובה:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

מחזיר את קובץ ה-`.proto` שמחולל אוטומטית עבור תפקיד. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**תגובה:** סכמת protobuf מסוג `text/plain`.

כל טבלה רשומה מפיקה `message` proto. קשרים מפיקים שדות message מקוננים. מיפוי טיפוסים: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

זרם Server-Sent Events עבור התראות שינוי בזמן אמת מטבלה. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

מסירת ההתראות משתמשת בספק ניתן-להחלפה (pluggable) שנבחר לפי סוג מקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY` (דרך asyncpg), מקורות MongoDB משתמשים ב-Change Streams‏ (`collection.watch()`), ומקורות Kafka משתמשים בקבוצות צרכנים (consumer groups). כל ספק מיישם ממשק צפייה (watch) אסינכרוני משותף. סינון RLS ואימות סכמה חלים ללא קשר לספק. (REQ-258) מקורות WebSocket ו-RSS נתמכים גם הם. (REQ-338, REQ-342)

**כותרת — `X-Provisa-Sink`:** הגדירו ליעד Kafka (לדוגמה `kafka://broker:9092/topic`) כדי להפנות אירועי שינוי ל-sink של Kafka במקום לתגובת ה-SSE. השרת משגר צרכן sink ומחזיר `202 Accepted` במקום זרם פתוח. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## נקודות קצה REST של ניהול (Admin)

### תצורה (Config)

#### `GET /admin/config`

הורדת ה-`provisa.yaml` הנוכחי כ-`application/x-yaml` עם כותרת `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

העלאת YAML תצורה מתוקן. השרת כותב גיבוי `.bak`, שומר את הקובץ החדש, וטוען מחדש את כל הסכמות, המקורות, וה-Materialized Views. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**גוף הבקשה:** תוכן YAML גולמי.

**תגובה:**
```json
{"success": true, "message": "Config uploaded and reloaded"}
```

בכשל טעינה מחדש: `{"success": false, "message": "<error>"}`.

---

### הגדרות (Settings)

#### `GET /admin/settings`

מחזיר את הגדרות הפלטפורמה הנוכחיות כ-JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

**תגובה:**
```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

עדכון הגדרות פלטפורמה בזמן ריצה. כל השדות אופציונליים — רק מפתחות שנוכחים בגוף מעודכנים. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**גוף הבקשה (דוגמה חלקית):**
```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

שדות ניתנים-לעדכון לפי סעיף:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — כותב לקובץ התצורה ומפעיל טעינה מחדש של סכמה (REQ-253)
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**תגובה:**
```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Observability

#### `GET /admin/traces/recent`

מחזיר עד N spans אחרונים שהושלמו ממאגר ה-span בזיכרון. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**פרמטרי שאילתה:** `limit` (ברירת מחדל 50, מקסימום 200)

**תגובה:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

טעינה מחדש חמה (hot-reload) של קטלוג בשם נתון במתאם (coordinator) מנוע הפדרציה דרך ה-REST API שלו. מחבר מחדש את החיבור הפנימי של Provisa ומריץ מחדש DDL של OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**פרמטרי שאילתה:** `catalog` (ברירת מחדל `"otel"`)

**תגובה:**
```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

הפעלה מחדש של קונטיינר מנוע הפדרציה (פיתוח צומת-יחיד בלבד). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**פרמטרי שאילתה:** `container` (ברירת מחדל למשתנה סביבה `QUERY_ENGINE_CONTAINER`, ואז `"trino"`)

---

### גילוי (Discovery)

#### `POST /admin/discover/relationships`

הפעלת גילוי קשרים. תמיד מריץ introspection של FK ממנוע הפדרציה. (REQ-018) מריץ הסקת LLM אם `ANTHROPIC_API_KEY` מוגדר. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**גוף הבקשה:**
```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` חייב להיות אחד מ-`"table"`, `"domain"`, `"cross-domain"`. עבור scope‏ `"table"`, `table_id` (מספר שלם) נדרש. עבור scope‏ `"domain"`, `domain_id` נדרש.

**תגובה:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

רשימת מועמדי קשר ממתינים. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

קבלת מועמד ורישומו כקשר. [tool-verified: `provisa/api/admin/discovery.py:103`]

**גוף הבקשה (אופציונלי):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

דחיית מועמד. [tool-verified: `provisa/api/admin/discovery.py:110`]

**גוף הבקשה:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

מחזיר ספירת מועמדים שנדחו. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

מחיקת כל המועמדים שנדחו. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### סריקת מקור (Source Crawl)

#### `POST /admin/sources/crawl`

סריקת מקור נתונים לביקורת סכמתו ורישום טבלאות. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### חיפוש טבלאות מקור

#### `GET /admin/sources/{source_id}/tables/search`

חיפוש טבלאות זמינות (עדיין לא רשומות) במקור לפי שם. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### פרופיל טבלה

#### `POST /admin/tables/{table_id}/profile`

הרצת פרופיל עמודות על טבלה רשומה — קרדינליות, מינימום/מקסימום, שיעורי null. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### תיאורי מקור

#### `POST /admin/source-meta/db-description`

חילול תיאורים בעזרת LLM עבור טבלאות ועמודות של מקור. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### פעולות (פונקציות ו-Webhooks)

כל נקודות הקצה תחת קידומת `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

כל הפעלה — מ-GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP‏ `run_sql`, ו-Provisa gRPC — עוברת דרך מבצע (executor) מבוקר יחיד שאוכף `writable_by` וממשל באופן אחיד. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] ראו [docs/integrations.md](integrations.md#_6) לתחביר הקריאה לפי-פרוטוקול.

#### `GET /admin/actions`

מחזיר את כל הפונקציות ו-webhooks של DB במעקב. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

**תגובה:**
```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "writableBy": [],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

כל אובייקט webhook נושא בוליאני `approved`. webhook מאושר ברגע ש-steward מבצע את בקשת היצירה שלו (REQ-209); webhooks המוצהרים בתצורה מאושרים אוטומטית. webhook לא-מאושר רשום אך לא חשוף בשום surface. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

רישום פונקציה במעקב (command). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**שדות מפתח:**

| שדה | חובה | תיאור |
|---|---|---|
| `name` | כן | שם command ייחודי |
| `kind` | כן | `"query"` ← שדה GraphQL Query; `"mutation"` ← שדה Mutation |
| `implKind` | לא | כיצד ה-command רץ — ראו הטבלה למטה (ברירת מחדל `source_procedure`) |
| `binding` | לא | פרטי חיבור ספציפיים ל-`implKind` (אובייקט JSON) |
| `returnSchema` | לא | JSON Schema‏ `{type:"array", items:{type:"object", properties:{...}}}` — הופך את ה-command למחזיר-סט (set-returning) בכל surface |
| `arguments` | לא | הגדרות ארגומנט `[{name, type}]`; סדר מיקומי (positional) חשוב עבור קוראי SQL ו-Bolt |
| `visibleTo` | לא | מזהי תפקיד שיכולים לקרוא ל-command |
| `writableBy` | לא | מזהי תפקיד עם הרשאה לקרוא לו כמוטציה |
| `domainId` | לא | דומיין למיקום GraphQL ובקרת גישה |

**ערכי `implKind`:**

| `implKind` | מה רץ | שדות `binding` |
|---|---|---|
| `source_procedure` | פרוצדורה מאוחסנת במקור רשום (ברירת מחדל) | `sourceId`, `schemaName`, `functionName` |
| `script` | סקריפט בצד השרת | `script` |
| `http` | קריאת HTTP יוצאת | `url`, `method` |
| `grpc` | קריאת gRPC יוצאת לשרת חיצוני | `target`, `method` |
| `python` | callable של Python המתארח על ידי Provisa (REQ-885) | `callable` (לדוגמה `"demo.py_functions:random_dataset"`) |

ה-commands להדגמה `random_python_set` (`implKind: python`) ו-`random_grpc_set` (`implKind: grpc`) מדגימים commands מחזירי-סט עם `returnSchema` בפועל; שניהם נמצאים ב-`config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

עדכון פונקציה במעקב לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

מחיקת פונקציה במעקב לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

רישום webhook במעקב. (REQ-209) רישום או עדכון webhook מכניס לתור בקשת אישור steward — ה-webhook הופך פעיל בכל ה-surfaces רק לאחר שסטיוארד מאשר אותו. webhooks המוצהרים בתצורה מאושרים אוטומטית. **שדות גוף הבקשה:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

עדכון webhook במעקב לפי שם. כל עריכה מאפסת את האישור למצב ממתין עד לאישור מחדש. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

מחיקת webhook במעקב לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

בדיקת פעולה (פונקציה או webhook) לפי שם. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### תפקידים (Roles)

כל נקודות הקצה תחת קידומת `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| שיטה | נתיב | תיאור |
|---|---|---|
| `GET` | `/admin/roles/` | רשימת כל התפקידים |
| `POST` | `/admin/roles/` | יצירת תפקיד |
| `PUT` | `/admin/roles/{role_id}` | עדכון תפקיד |
| `DELETE` | `/admin/roles/{role_id}` | מחיקת תפקיד |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### משתמשים (Users)

כל נקודות הקצה תחת קידומת `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| שיטה | נתיב | תיאור |
|---|---|---|
| `POST` | `/admin/users/` | יצירת משתמש מקומי |
| `GET` | `/admin/users/` | רשימת משתמשים מקומיים |
| `GET` | `/admin/users/{user_id}` | קבלת משתמש |
| `PUT` | `/admin/users/{user_id}` | עדכון משתמש |
| `PATCH` | `/admin/users/{user_id}/password` | שינוי סיסמה |
| `DELETE` | `/admin/users/{user_id}` | מחיקת משתמש |
| `GET` | `/admin/users/{user_id}/assignments` | רשימת הקצאות תפקיד |
| `POST` | `/admin/users/{user_id}/assignments` | הוספת הקצאת תפקיד |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | הסרת הקצאת תפקיד |

---

### ארגונים (Organizations)

כל נקודות הקצה תחת `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| שיטה | נתיב | תיאור |
|---|---|---|
| `GET` | `/admin/orgs/` | רשימת ארגונים |
| `POST` | `/admin/orgs/` | יצירת ארגון |
| `PUT` | `/admin/orgs/{org_id}` | עדכון ארגון |
| `DELETE` | `/admin/orgs/{org_id}` | מחיקת ארגון |
| `GET` | `/admin/orgs/{org_id}/members` | רשימת חברים |
| `POST` | `/admin/orgs/{org_id}/members` | הוספת חבר |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | הסרת חבר |

---

### הזמנות (Invites)

כל נקודות הקצה תחת `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| שיטה | נתיב | תיאור |
|---|---|---|
| `POST` | `/admin/invites/` | יצירת הזמנה |
| `GET` | `/admin/invites/` | רשימת הזמנות ממתינות |
| `DELETE` | `/admin/invites/{token}` | ביטול הזמנה |

---

### GraphQL של ניהול

#### `POST /admin/graphql`

נקודת קצה Strawberry GraphQL עבור כל פעולות הניהול: CRUD של מקורות וטבלאות, ניהול קשרים, תצורת דומיין, כללי RLS, בקרת מטמון, מוסכמות שיוֹם, ניהול משימות מתוזמנות, וקימפול שאילתות. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**מוטציות מפתח:**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### הגדרה ראשונית (Setup)

#### `GET /setup/status`

מחזיר סטטוס הגדרה ראשונית. תמיד לא-מאומת. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

השלמת הגדרה ראשונית. [tool-verified: `provisa/api/setup_router.py:142`]

---

## בדיקת בריאות (Health Check)

#### `GET /health` או `HEAD /health`

מחזיר `{"status": "ok"}`. תמיד לא-מאומת. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## תגובות שגיאה

| סטטוס | משמעות |
|---|---|
| 400 | שאילתה לא תקינה, שגיאת אימות, או שגיאת ניתוח SQL |
| 401 | טוקן אימות חסר או לא תקין |
| 403 | יכולות לא מספיקות; הפרת ממשל |
| 404 | תפקיד, משאב, או קובץ תצורה לא נמצא |
| 422 | כותרת חובה חסרה (לדוגמה `X-Role`) |
| 503 | מסד נתונים או מקור לא מחובר; תלות לא זמינה |
| 504 | הבקשה פגה (timeout) |

הפרות ממשל ב-`POST /data/sql` מחזירות HTTP 403 עם גוף מובנה: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

כל שאר השגיאות משתמשות ב: `{"detail": "<message>"}`.

---

## נקודת קצה Arrow Flight

פורט `8815`. תעבורה עמודתית (columnar) ילידית של Arrow על גבי gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

שאילתות וגילוי קטלוג זמינים שניהם על אותו חיבור. צינור הממשל המלא (RLS, מיסוך, דגימה) מיושם על כל שאילתה. (REQ-130, REQ-143)

**פורמט Ticket** (JSON):
```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**שימוש (Python):**
```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

כאשר ה-proxy‏ Zaychik Flight SQL זמין (פורט 8480), אצוות רשומות (record batches) זורמות מקצה-לקצה ללא מימוש מלא. (REQ-144) נופל חזרה למימוש דרך שכבת השאילתה הפדרטיבית אם Zaychik אינו זמין. (REQ-146)

---

## נקודת קצה Protobuf gRPC

פורט `50051` (דריסה עם משתנה סביבה `GRPC_PORT` או תצורת `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

העבירו את התפקיד במפתח מטא-נתוני gRPC‏ `x-provisa-role`. אם חסר, השרת מבטל עם `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

הורידו את ה-proto הספציפי-לתפקיד מ-`GET /data/proto/{role_id}`. רק טבלאות ועמודות הנראות לאותו תפקיד מופיעות. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

כל טבלה מפיקה RPC סטרימינג בשם `Query{TypeName}`. RPC-ים בשם `Insert{TypeName}` קיימים לסימטריית סכמה אך מבוטלים עם `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` מופעל עבור גילוי שירות ללא proto מקומפל מראש. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

שרת ה-gRPC מתחיל רק כאשר proto תקין ניתן לקימפול בעת ההפעלה. אם בניית הסכמה נכשלת, שרת ה-gRPC אינו מתחיל. (REQ-529)

---

## דרייבר JDBC

דרייבר ה-JDBC של Provisa (`provisa-jdbc-0.1.0.jar`) חושף את הקטלוג הסמנטי לכלי BI (Tableau, PowerBI, DBeaver). (REQ-126)

**כתובת URL לחיבור:** `jdbc:provisa://host:port` (REQ-131)

דומיינים ממופים לסכמות JDBC. (REQ-127) טבלאות משתמשות בכינויים הרשומים שלהן. עמודות משתמשות בכינויים וחושפות תיאורים כ-`REMARKS`. (REQ-128) שיטות מטא-נתונים תקניות (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) חושפות קשרים סמנטיים כמטא-נתוני PK/FK.

**תמיכת SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

הדרייבר מבקש הפניית Arrow IPC כברירת מחדל. תוצאות זורמות אצווה-אחר-אצווה דרך `ArrowStreamReader`, מוגבלות לאצוות רשומה (record batch) אחת בזיכרון. (REQ-293)

---

## פורמט ארגומנט `orderBy`

הארגומנט `order_by` משתמש באובייקטי `{column: direction}` עם enum כיוון בעל 6 ערכים: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

כיוונים נתמכים: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## מנויים (Subscriptions)

מנויי SSE זמינים בכתובת `GET /data/subscribe/{table}`. (REQ-219, REQ-258) מסירת ההתראות משתמשת בספק ניתן-להחלפה שנבחר לפי סוג מקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY`, מקורות MongoDB משתמשים ב-Change Streams, ומקורות Kafka משתמשים בקבוצות צרכנים. סינון RLS ואימות סכמה חלים ללא קשר לספק. מקורות WebSocket ו-RSS נתמכים גם הם דרך אותה נקודת קצה. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]
</content>
