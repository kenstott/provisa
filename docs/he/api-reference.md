# מדריך API

## סקירה כללית

Provisa חושפת נקודות קצה REST תחת שתי קידומות: `/data` לביצוע שאילתות ולבדיקת Schema (Introspection), ו-`/admin` לניהול תצורה. (REQ-043) רוב נקודות הקצה של הנתונים דורשות מזהה תפקיד (Role). פעולות תצורת ניהול משתמשות ב-API של Strawberry GraphQL בכתובת `/admin/graphql`. (REQ-164)

---

## אימות (Authentication)

כאשר `auth.provider` מוגדר ב-`provisa.yaml`, כל נקודות הקצה מלבד `/health` ו-`/setup/status` דורשות כותרת `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

ללא תצורת אימות, השרת פועל במצב פיתוח (dev mode). כל בקשה מטופלת כזהות `anonymous`, המשויכת לכל התפקידים המוגדרים עם גישת Wildcard לתחומים. (REQ-535)

**התחברות (`POST /auth/login`)** מסופקת על-ידי ספק האימות הפעיל כאשר `provider: basic` מוגדר. (REQ-124) פורמט ההרשאות והתגובה תלויים בספק.

**בדיקת זהות (Introspection):**

```http
GET /auth/me
```

מחזיר את מזהה המשתמש המאומת, אימייל, שם תצוגה, חברויות ארגון, ושיוכי תפקיד. במצב פיתוח מחזיר `dev_mode: true` עם כל מזהי התפקידים רשומים. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

מחזיר `{"provider": "<name>"}` או `{"provider": null}` כאשר האימות אינו מוגדר. [tool-verified: `provisa/api/auth_router.py`]

---

## נקודות קצה של נתונים (Data Endpoints)

### `POST /data/graphql`

ביצוע שאילתת או מוטציית GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**גוף הבקשה:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

השדה `role` משמש רק במצב פיתוח (ללא אימות). כאשר האימות פעיל, נעשה שימוש בתפקיד המשתמש המאומת ו-`role` בגוף הבקשה מתעלמים ממנו.

השדה `extensions` תומך בפרוטוקול Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**כותרות:**

- `X-Provisa-Role` — עקיפת תפקיד (מצב פיתוח)
- `Accept` — פורמט תגובה (ראו משא-ומתן תוכן)
- `Authorization` — `Bearer <token>` כאשר האימות פעיל
- `X-Provisa-Redirect-Format` — סוג MIME לפלט הפניית S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — מספר שורות שמעליו מופעלת הפניה (REQ-137)
- `X-Provisa-Redirect` — `true` לכפיית הפניה ללא תנאי (REQ-029)

**תגובה (JSON מוטבע):**

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

**תגובה (Multi-Root עם מוטבע/הפניה מעורבים):**

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

שאילתות Multi-Root מריצות כל שדה שורש (Root Field) באופן עצמאי. שדות מתחת לסף ההפניה מוחזרים מוטבעים; שדות מעל מוסטים בהפניה. המפתח `redirects` (רבים) ממפה שמות שדות למידע הפניה. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**כותרות מטמון:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (ב-HIT) (REQ-536)

**יכולות נדרשות:** `QUERY_DEVELOPMENT` לכל הבקשות כולל Introspection. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### משא-ומתן תוכן (Content Negotiation)

| כותרת Accept | פורמט |
| --- | --- |
| `application/json` | JSON (ברירת מחדל) |
| `application/x-ndjson` | JSON מופרד-שורות (Newline-delimited) |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### הפניה (Redirect)

תוצאות מעל סף שורות מוגדר (או כאשר `X-Provisa-Redirect: true`) נכתבות ל-S3 ומוחזר כתובת URL חתומה מראש (Presigned). (REQ-029, REQ-044)

| פורמט הפניה | נכתב על-ידי | זיכרון |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS פדרטיבי | ללא — הנתונים לעולם אינם עוברים דרך Provisa |
| `application/x-orc` | CTAS פדרטיבי | ללא — הנתונים לעולם אינם עוברים דרך Provisa |
| `application/json` | Provisa | תלוי-זיכרון |
| `application/x-ndjson` | Provisa | תלוי-זיכרון |
| `text/csv` | Provisa | תלוי-זיכרון |
| `application/vnd.apache.arrow.stream` | Provisa | תלוי-זיכרון |

לייצוא אנליטי גדול, השתמשו בהפניית Parquet או ORC. מנוע הפדרציה כותב ישירות ל-S3 במקביל — ללא מעבר נתונים דרך Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

ביצוע SQL גולמי דרך Pipeline הממשל בשלב 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**גוף הבקשה:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**יכולות נדרשות:** `QUERY_DEVELOPMENT`.

הפרות ממשל ב-`POST /data/sql` מחזירות HTTP 403. (REQ-002, REQ-266)

**תגובה:** אותו פורמט כמו `/data/graphql` (שורות JSON כברירת מחדל, לפי משא-ומתן תוכן דרך `Accept`).

---

### `POST /data/query`

נקודת קצה שאילתה מאוחדת. מקבלת GraphQL, SQL, או Cypher — התחביר מזוהה אוטומטית. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

שאילתות Cypher יכולות להישלח גם לנקודת הקצה הייעודית `POST /query/cypher`. (REQ-345)

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

### `POST /data/sql/explain`

הסבר (Explain) או ניתוח (Analyze) של הצהרת SQL דרך ה-Pipeline המְמוּשְל. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

נקודת הקצה עוטפת את ה-SQL **המְמוּשְל** — ההצהרה שרצה בפועל תחת התפקיד של הקורא, לאחר אבטחה ברמת שורה ומיסוך — בתחביר EXPLAIN של הדיאלקט. מה שהתוכנית (Plan) מציגה הוא הגרסה המורשית של השאילתה, לא הקלט הגולמי.

**גוף הבקשה:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

הגדירו `analyze: true` להרצת EXPLAIN ANALYZE. השאילתה מתבצעת והתוכנית נושאת ספירות שורות ותזמונים אמיתיים. לא כל דיאלקט תומך ב-ANALYZE; ראו הטבלה ב-[תוכניות שאילתה וסטטיסטיקות](engines.md#query-plans-and-statistics).

**תגובה:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` כאשר לדיאלקט אין תמיכת EXPLAIN, או כאשר `analyze: true` מתבקש בדיאלקט שאינו תומך בכך (למשל SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

מחזיר את המצב הנוכחי של Shard המנוע ללא הערתו (Waking). (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

ה-UI סוקר (Polls) נקודת קצה זו כדי להציג באנר עלייה בזמן שהמנוע מתחיל בקור (Cold-Starting). היא לעולם אינה מפעילה הערה — סקירה בטוחה ואינה נחשבת פעילות עבור ה-Idle Reaper.

**תגובה:**

```json
{"state": "ready"}
```

ערכים אפשריים:

| מצב | משמעות |
| --- | --- |
| `always-on` | Desktop, מתארח עצמי (Self-Hosted), או Coordinator עצמאי — ללא ניהול מחזור חיים |
| `ready` | ה-Shard פעיל ומקבל שאילתות |
| `starting` | עלייה בקור בתהליך |
| `stopped` | ה-Shard סולם לאפס (Scaled to Zero) |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

הפעלת הערת מנוע ללא הרצת שאילתה. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

מחזיר `202 Accepted` מיידית. ההערה רצה ברקע. השתמשו בכך אם אתם רוצים שהמנוע יהיה מוכן לפני הגעת השאילתה הראשונה — לדוגמה, ממתזמן (Scheduler) שמריץ שאילתות כמה דקות מאוחר יותר.

**תגובה:** `202 Accepted`, גוף `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

נקודת קצה REST פשוטה, נוצרת אוטומטית עבור כל טבלה רשומה. מחרוזת השאילתה ממופה לארגומנטים של GraphQL והבקשה מהודרת ומתבצעת דרך אותו Pipeline (RLS, מיסוך, ניתוב) כמו GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**פרמטרי שאילתה:**

- `limit` — מספר שורות מרבי (≥ 1)
- `offset` — דילוג שורות (≥ 0)
- `fields` — שמות עמודות מופרדים בפסיק (ברירת מחדל: כל השדות הסקלריים)
- `filter` — מערך JSON של אובייקטי סינון `{"field", "comparator", "value"}`
- `orderBy` — מערך JSON של אובייקטי מיון `{"field", "direction"}`

התפקיד המאומת נדרש; בקשות לא מאומתות מחזירות `401`. מפרט OpenAPI עבור נתיבים אלה מוגש בכתובת `GET /data/rest/openapi.json` עם Swagger UI בכתובת `GET /data/rest/docs`.

#### חוקר OpenAPI / Swagger UI

עמוד חוקר ה-OpenAPI (`/app/openapi`) מטביע את Swagger UI ב-iframe מבודד (Sandboxed). המפרט מוגבל-תפקיד — רק טבלאות ועמודות הנראות לתפקיד הנוכחי מופיעות — ובאופן אופציונלי מסונן לפי תחום דרך בורר התחום. ה-UI עובר בין ערכות נושא בהירה וכהה אוטומטית. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

העמוד טוען את ה-HTML של המפרט דרך `fetch()` ולא דרך `src` ישיר של iframe, כך שהבקשה נושאת את אסימון ה-Bearer של ה-Session ובקשות היחסיות (Relative) של Swagger UI עצמו נפתרות נכון מול אותו מקור (Origin). [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

כאשר מנווטים מקישור NL "פתח ב-OpenAPI", העמוד מרחיב אוטומטית את נקודת הקצה היעד, ממלא פרמטרי שאילתה מכתובת ה-URL שנוצרה על-ידי NL (למשל `aggregate`, `groupBy`), ולוחץ Execute — תוך שימוש בסקירה (Polling) של DOM כדי להבטיח שכל שלב מסתיים לפני שהבא מופעל. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

נקודת קצה תואמת [JSON:API](https://jsonapi.org) הנוצרת אוטומטית עבור כל טבלה רשומה. אותה אבטחה ברמת שורה, מיסוך, וניתוב כמו GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**כותרת `Accept`:** חייבת לכלול `application/vnd.api+json` (סוג המדיה של JSON:API) אחרת הבקשה מחזירה `406`.

**פרמטרי שאילתה:**

- `fields[<type>]` — קבוצות שדות דלילות (Sparse Fieldsets), למשל `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — למשל `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — מופרד בפסיק, קידומת `-` לסדר יורד, למשל `?sort=-created_at,amount`
- `page[number]` / `page[size]` — עימוד (Pagination)
- `aggregate` — פונקציות אגרגציה מופרדות בפסיק להרצה במקום שליפת שורות: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. השתמשו ב-`?aggregate=count,sum` לבקשת תת-קבוצה. תגובות אגרגציה מחזירות `data: null` עם תוצאות ב-`meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — שמות עמודות מופרדים בפסיק; משמש עם `?aggregate=` לקיבוץ תוצאות. רק עמודות ב-Enum `DistinctOnColumn` של הטבלה תקפות; השרת מחזיר `400` עבור כל עמודה שהתפקיד אינו יכול לראות. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` לכלול עמודות סקלריות של טבלת הבסיס (וסקלרים של ממדים מצורפים (Joined) הנקובים ב-`include=`) בתוך מערך `nodes` של כל שורת קבוצה. נדרש כאשר שאילתת קיבוץ NL מבקשת גם פרטי ממד. (REQ-1405)

התגובות הן אובייקטי משאב עם `type`/`id`/`attributes`. שגיאות עוקבות אחר צורת אובייקט השגיאה של JSON:API.

#### חוקר JSON:API

עמוד חוקר ה-JSON:API (`/app/jsonapi`) הוא UI דפדפן מעל נקודות קצה אלה. בחרו טבלה מהרשימה המקובצת-לפי-תחום, ואז הגדירו:

- **שדות** — בחרו אילו עמודות לכלול (Sparse Fieldset); השאירו הכל לא מסומן לבקשת כל עמודה
- **קשרים** — בחרו שמות קשרים נגזרי-FK לצירוף (Sideload) דרך `?include=`
- **סינון** — שדה, אופרטור (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`), וערך
- **מיון** — שדה אחד, עולה או יורד
- **אגרגציה** — בחרו עמודות קיבוץ מהרשימה המאומתת בשרת, ואז סמנו פונקציית אגרגציה אחת או יותר; כאשר עמודות קיבוץ נבחרות, תיבת סימון "כלול Nodes" מוסיפה עמודות סקלריות של טבלת הבסיס לכל שורה
- **גודל עמוד** — משאבים לעמוד, עם ניווט ראשון/קודם/הבא/אחרון

התוצאות מוצגות בתצוגת סיכום מעוצבת (כרטיסי משאב עם עוגני קשר הניתנים ללחיצה) או בלשונית JSON גולמית. כתובת ה-URL של הבקשה החיה מוצגת וניתנת להעתקה. בחירת טבלה וגודל עמוד נשמרים בין Sessions ב-`localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

כאשר מנווטים מקישור NL "פתח ב-JSON:API", החוקר בוחר מראש את הטבלה וזורע את בורר האגרגציה מפרמטרי השאילתה שנוצרו על-ידי NL, ואז מריץ אוטומטית את הבקשה. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

שליחת שאלה בשפה טבעית. השירות מתחיל עבודה אסינכרונית ומחזיר `202 Accepted` עם `job_id` מיידית. דורש ספק LLM מוגדר תחת סעיף התצורה `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**גוף הבקשה:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

מחזיר `{"job_id": "<id>"}`. חריגה ממגבלת קצב NL לתפקיד מחזירה `429` עם כותרת `Retry-After`. (REQ-370)

**אחזור התוצאה:**

- `GET /query/nl/{job_id}` — סקירה (Poll). מחזיר את מסמך העבודה.
- `GET /query/nl/{job_id}/stream` — SSE. אירוע `branch` אחד לכל יעד יצירה (Generation Target) עם השלמתו, ואז אירוע `done`. (REQ-357, REQ-358)

שלוש לולאות יצירה (Cypher, GraphQL, SQL) רצות במקביל, כל אחת מאומתת דרך המהדר ומחודדת בעת שגיאה. (REQ-355) ה-Prompt מוגבל ל-Schema הנראה של התפקיד. (REQ-356) מסמך התוצאה מפתח כל ענף (Branch) לפי יעד: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

ענף שממצה את מגבלת האיטרציות שלו מחזיר `query: null`, `result: null`, ומחרוזת `error`. כל שאילתה שנוצרת מתבצעת תחת זכויות הצרכן עם ממשל שלב 2 מיושם — השירות לעולם אינו עוקף את הממשל. (REQ-359)

#### קיבוץ NL עם פרטי ממד (REQ-1405)

כאשר שאילתת קיבוץ NL גם מקרינה (Projects) עמודות מטבלת ממד מצורפת — לדוגמה, "ספירת פניות לפי משתמש עם שם משתמש ואימייל" — הרץ (Runner) גוזר נתיבי-נקודה (Dot-Paths) לכל שדה (`dim_paths`) מעמודות הממד המוקרנות ב-SELECT. נתיבים אלה ממלאים את הפרמטר `includeNodes=` בכתובות ה-URL שנוצרו של חלוניות JSON:API ו-OpenAPI, כך שחלוניות אלה מבקשות את אותם שדות ממד מצורף שענפי SQL ו-GraphQL פתרו. ללא זאת, `includeNodes=true` היה מחזיר רק את השדות הסקלריים של טבלת האגרגט הבסיסית עצמה. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

בחלונית ה-gRPC, `{Type}GroupByRequest` שנוצר נושא `include_nodes` (Bool) ו-`include` (מחרוזת חוזרת של שמות שדות קשר). `{Type}GroupByRow` המוחזר כולל שדה `nodes` מוקלד עם שורות פרטי הממד. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

מחזיר את ה-GraphQL SDL עבור Schema של תפקיד. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**כותרות:** `X-Role: <role_id>` (נדרש)

**פרמטרי שאילתה:**

- `domain` — מזהי תחום מופרדים בפסיק. כאשר מוגדר, התגובה מסוננת לתחום(ים) הנקוב(ים) והטבלאות הניתנות להגעה מהם.

**תגובה:** `text/plain` GraphQL SDL.

---

### `GET /data/introspection`

מחזיר JSON של Introspection של GraphQL, אופציונלית מסונן לפי תחום. [tool-verified: `provisa/api/data/sdl.py:200`]

**כותרות:** `X-Provisa-Role: <role_id>` (נדרש)

**פרמטרי שאילתה:** `domain` — מזהי תחום מופרדים בפסיק.

**תגובה:** תוצאת Introspection מסוג `application/json`.

---

### `GET /data/graph-schema`

מחזיר את תצוגת הגרף של Schema התפקיד: תוויות Node וסוגי הקשרים שלהן, עבור לקוחות Cypher/גרף. כולל `pk_columns` לכל תווית Node כדי שהקוראים יוכלו לקבוע עמודות מפתח ראשי. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**תגובה:** `application/json` עם `node_labels` (כל אחת נושאת `pk`/`pk_columns`) ו-`relationship_types`.

כל סוג קשר נושא גם `junction_table_name` ו-`properties` (REQ-1586). בקצה המבוסס על טבלת חיבור (Junction), הראשון נוקב בשם הטבלה האסוציאטיבית שהוא חוצה והשני מפרט את עמודות אותה טבלה, קריאות כ-`r.attr` וניתנות לסינון ב-`WHERE`; בקצה המבוסס על FK, השם הוא `null` ורשימת המאפיינים ריקה, וכך לקוח מבחין בין השניים. טבלת החיבור עצמה לעולם אינה תווית Node — היא הקשת (Edge) עצמה, ולכן אין לה כפתור (Pill) בלקוח גרף ואין לה שורה ב-`node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

מחזיר מזהי תחום נגישים לתפקיד המבקש. [tool-verified: `provisa/api/data/sdl.py:116`]

**כותרות:** `X-Role: <role_id>` (נדרש)

**תגובה:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

מחזיר את מחרוזת גרסת ה-Schema הנוכחית. משלב Nonce לכל אתחול עם מונה בנייה מחדש. לקוחות משתמשים בכך לביטול תוקף מטמוני Schema לאחר הפעלה מחדש של השרת. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**תגובה:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

מחזיר את קובץ `.proto` שנוצר אוטומטית עבור תפקיד. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**תגובה:** Schema פרוטובאף מסוג `text/plain`.

כל טבלה רשומה מפיקה `message` פרוטו. קשרים מפיקים שדות הודעה מקוננים. מיפוי סוגים: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

זרם Server-Sent Events עבור התראות שינוי בזמן אמת מטבלה. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

מסירת ההתראות משתמשת בספק ניתן-להתקנה (Pluggable) הנבחר לפי סוג מקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY` (דרך asyncpg), מקורות MongoDB משתמשים ב-Change Streams (`collection.watch()`), ומקורות Kafka משתמשים בקבוצות צרכנים (Consumer Groups). כל ספק מיישם ממשק Watch אסינכרוני משותף. סינון RLS ואימות Schema חלים ללא תלות בספק. (REQ-258) מקורות WebSocket ו-RSS נתמכים גם הם. (REQ-338, REQ-342)

**כותרת — `X-Provisa-Sink`:** הגדירו ליעד Kafka (למשל `kafka://broker:9092/topic`) כדי להפנות אירועי שינוי ל-Sink של Kafka במקום לתגובת SSE. השרת מפעיל צרכן Sink ומחזיר `202 Accepted` במקום זרם פתוח. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## נקודות קצה REST לניהול (Admin)

### תצורה (Config)

#### `GET /admin/config`

הורדת ה-`provisa.yaml` הנוכחי כ-`application/x-yaml` עם כותרת `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

העלאת YAML תצורה מתוקן. השרת כותב גיבוי `.bak`, שומר את הקובץ החדש, וטוען מחדש את כל ה-Schema-ים, המקורות, ו-Materialized Views. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**גוף הבקשה:** תוכן YAML גולמי.

**תגובה:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

בכשל טעינה מחדש: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

הורדת **תצורת ה-Live הנוכחית** — התצורה כפי ש-Provisa הייתה כותבת אותה היום, המשקפת כל טבלה, קשר, תחום, תפקיד, וכלל RLS שנוצרו על-ידי המנהל ונצברו מאז ההפעלה. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

הקובץ בדיסק עשוי לפגר אחר מצב ה-Live אם שינויים נעשו דרך ה-API של המנהל ללא העלאה עוקבת. נקודת קצה זו סוגרת את הפער: הפלט שלה הוא מה ש-`PUT /admin/config` היה צריך לקבל כדי להביא את הקובץ בדיסק להתאים למצב ה-Live.

מחזיר `application/x-yaml` עם `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

מחזיר את שני צדדי ההשוואה (Diff) של התצורה — `original` (קו הבסיס בהפעלה) ו-`current` (מצב Live) — מנורמלים באופן זהה, כך שההשוואה מציגה רק שינויים אמיתיים, לא סידור מחדש או סחיפת הערות. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**תגובה:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

יצירת Patch מסוג Unified-Diff מקו הבסיס לתצורה שנשלחה. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

שלחו את ה-YAML המתוקן כגוף הבקשה. התגובה היא קובץ `text/x-patch` (`provisa.config.patch`) ש-`git apply` או `patch` יכולים לצרוך ישירות — שימושי להזנת שינויי תצורה שמקורם ב-UI דרך Pipeline של CI/CD.

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

עדכון הגדרות פלטפורמה בזמן ריצה. כל השדות אופציונליים — רק מפתחות הנוכחים בגוף מתעדכנים. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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

שדות ניתנים לעדכון לפי סעיף:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — כותב לקובץ תצורה ומפעיל טעינה מחדש של Schema (REQ-253)
- `relationships`: `auto_track_fk` — שולט רק במעקב מפתח זר (FK). קשר המבוסס על טבלת חיבור (Junction) מוצהר בעת רישום הטבלה ולעולם אינו מוסק, כך שהגדרה זו אינה מגיעה אליו. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**תגובה:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### מודלי AI

#### `GET /admin/ai-models`

מחזיר את שיוכי מודל ה-AI של הארגון הפועל, רישום מודלי הווקטור, ומגבלת קצב NL. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

**תגובה:**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

מפתחות API לעולם אינם מוחזרים חזרה — `api_keys_set` מדווח רק האם לכל ספק יש מפתח מוגדר. שינויים נכנסים לתוקף בבקשה הבאה; אין צורך באתחול מחדש. (REQ-1349)

#### `PUT /admin/ai-models`

עדכון שיוכי מודל ה-AI של הארגון, רישום מודלי הווקטור, או מגבלת קצב NL. נכנס לתוקף בבקשה הבאה. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

מחזיר את שמות המודלים שספק מציע כרגע, עבור בורר המודלים. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

הרשימה נקראת חי (Live) מ-API רשימת-המודלים של הספק עצמו, באמצעות המפתח המוגדר של הארגון — או Credential הפריסה כאשר לא הוגדר מפתח ארגוני. מודל שפורסם לאחר בניית גרסה זו ניתן לבחירה באותו יום שבו הספק מציע אותו.

מחזיר `400` כאשר לספק אין API רשימת-מודלים (הזינו את שם המודל ישירות במקרה זה) או כאשר אין מפתח זמין. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### מנוע פדרציה

#### `GET /admin/federation-engine`

מחזיר את בחירת מנוע הפדרציה הנוכחית, תצורת החיבור שלה, ואת מרשם המנועים הבחירים המלא. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

**תגובה:**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

המפתח `current` הוא המנוע הפועל כרגע; `persisted` הוא מה שנכתב לקובץ התצורה ויטען באתחול הבא. הם מתפצלים כאשר התצורה שונתה אך השירות טרם הופעל מחדש.

#### `PUT /admin/federation-engine`

שמירה קבועה של בחירת מנוע פדרציה. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**גוף הבקשה:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

הבחירה נכתבת לתצורת הפלטפורמה. היא נכנסת לתוקף לאחר הפעלה מחדש הבאה של השירות — המנוע נבחר פעם אחת באתחול.

---

### מדיניות תחום (Domain Policy)

#### `POST /admin/domain-policy`

שינוי מדיניות התחום של הארגון הפועל (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

זוהי פעולה הרסנית המוגבלת לארגון הפועל. כל מקור, טבלה, תחום, וקשר רשומים נמחקים ונבנים מחדש תחת המדיניות החדשה. השתמשו בכך כאשר מעבירים ארגון בין מבנה תחומי-שמות (Domain-Namespaced) ל-Flat (או להפך).

**גוף הבקשה:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` מנקה את דריסת (Override) הארגון וחוזר להגדרת רמת הפריסה. `use_domains: false` דורש `default_domain` (שם התחום היחיד שכל הטבלאות נוחתות בו). בניית הקטלוג מחדש היא סינכרונית; התגובה מוחזרת ברגע שה-Schema-ים מוכנים.

---

### תצפיתיות (Observability)

#### `GET /admin/traces/recent`

מחזיר עד N Spans שהושלמו לאחרונה ממאגר ה-Span בזיכרון. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**פרמטרי שאילתה:** `limit` (ברירת מחדל 50, מקסימום 200)

**תגובה:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

טעינה מחדש חמה (Hot-Reload) של קטלוג בעל שם במתאם מנוע הפדרציה דרך ה-API של REST שלו. מחבר מחדש את החיבור הפנימי של Provisa ומריץ מחדש OTel DDL. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**פרמטרי שאילתה:** `catalog` (ברירת מחדל `"otel"`)

**תגובה:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

הפעלה מחדש של מיכל (Container) מנוע הפדרציה (Single-Node dev בלבד). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**פרמטרי שאילתה:** `container` (ברירת מחדל למשתנה סביבה `QUERY_ENGINE_CONTAINER`, ואז `"trino"`)

---

### גילוי (Discovery)

#### `POST /admin/discover/relationships`

הפעלת גילוי קשרים. תמיד מריץ Introspection של FK ממנוע הפדרציה. (REQ-018) מריץ הסקת LLM אם `ANTHROPIC_API_KEY` מוגדר. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**גוף הבקשה:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` חייב להיות אחד מ-`"table"`, `"domain"`, `"cross-domain"`. עבור Scope מסוג `"table"`, `table_id` (מספר שלם) נדרש. עבור Scope מסוג `"domain"`, `domain_id` נדרש.

**תגובה:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

רשימת מועמדי קשר ממתינים. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

אישור מועמד ורישומו כקשר. [tool-verified: `provisa/api/admin/discovery.py:103`]

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

סריקת מקור נתונים כדי לבצע Introspection ל-Schema שלו ולרשום טבלאות. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### חיפוש טבלאות מקור

#### `GET /admin/sources/{source_id}/tables/search`

חיפוש טבלאות זמינות (עדיין לא רשומות) במקור לפי שם. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### פרופיל טבלה

#### `POST /admin/tables/{table_id}/profile`

הרצת פרופיל עמודה על טבלה רשומה — קרדינליות, min/max, שיעורי Null. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### תיאורי מקור

#### `POST /admin/source-meta/db-description`

יצירת תיאורים בסיוע LLM עבור טבלאות ועמודות של מקור. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### אחסון אובייקטים (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

דיווח על טביעת רגל האחסון של הארגון הפועל מול מכסת הפלטפורמה שלו, והאם הארגון רשם אחסון משלו. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

כאשר לארגון רשום DSN משלו, ה-Materializations שלו הולכים לשם ואינם נספרים עוד כנגד המכסה. ה-DSN עצמו לעולם אינו מוחזר.

#### `PUT /admin/org-storage`

רישום (או ניקוי) אחסון Materialization משלו של הארגון. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**גוף הבקשה:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

ה-DSN מאומת מול מנוע הפדרציה לפני קבלתו — DSN לא שמיש נכשל ברישום, לא שעות מאוחר יותר במהלך רענון. הערך מוצפן במנוחה (At Rest) ולעולם אינו מוחזר על-ידי GET.

שלחו `storage_url: null` כדי לנקות את האחסון העצמי של הארגון ולהחזיר את ה-Materializations שלו לאחסון הפלטפורמה (ולמכסה). זמן הריצה (Runtime) של הארגון נבנה מחדש באותה קריאה, כך שהאחסון החדש בתוקף מיידית. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### הצפנת ארגון (REQ-1574)

#### `GET /admin/org-encryption`

מחזיר את מצב המפתח הנוכחי של הארגון: טביעת אצבע (Fingerprint), מזהה, ומקור. לעולם אינו מחזיר חומר מפתח. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

כאשר לארגון לא הוגדר מפתח, מחזיר `{"configured": false}`. כל ארגון מתחיל במצב זה ויורש את מפתח הפריסה.

#### `PUT /admin/org-encryption`

הגדרה או סבב (Rotation) של מפתח ההצפנה במנוחה של הארגון. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**גוף הבקשה:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

השמיטו `key_b64` כדי ש-Provisa תייצר מפתח — הנתיב הבטוח ביותר, שכן המפתח לעולם אינו מופיע בלוח (Clipboard) או ביומן בקשה. אספקת `key_b64` מביאה מפתח משלכם.

סבב מוסיף רשומה פעילה חדשה לטבעת המפתחות (Key Ring) ושומר את הישנה, כך שנתונים שנכתבו תחת המפתח הקודם נשארים קריאים. סבב אינו הצפנה מחדש. אין נקודת קצה למחיקה: פרישת המפתח האחרון הייתה הופכת כל מטען עטוף (Wrapped) לבלתי קריא. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

טבעת ה-Live נקשרת מחדש באותה קריאה, כך שהכתיבה המוצפנת הבאה משתמשת במפתח החדש מיידית.

---

### ייבוא Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

המרת ארכיון פרויקט Hasura v2 או DDN לתצורת Provisa מוצעת ללא כתיבה. [tool-verified: `provisa/api/admin/import_router.py`]

**גוף הבקשה:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` הוא `"auto"` (מזוהה ממבנה הארכיון), `"hasura_v2"`, או `"ddn"`.

**תגובה:**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

דבר אינו נשמר. התצוגה המקדימה אינה נשמרת במטמון בצד השרת; `apply` לוקח את ה-YAML שסיפקתם, כך שמה שמיושם הוא בדיוק מה שנבדק (ואופציונלית נערך).

#### `POST /admin/import/hasura/apply`

טעינת תצורה שנבדקה מראש לארגון הפועל. [tool-verified: `provisa/api/admin/import_router.py`]

**גוף הבקשה:**

```json
{"config_yaml": "<yaml string>"}
```

משתמש באותו נתיב Hot-Reload כמו `PUT /admin/config`. הקטלוג, ה-Schema-ים, וה-Pools של הארגון נבנים מחדש לפני שהתגובה מוחזרת.

---

### חליפין Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

ייצוא המודל המְמוּשְל של הארגון כמסמך YAML של Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

המסמך נגזר ממצב Live בכל בקשה — לעולם אינו נשמר במטמון — כך שהוא לעולם אינו יכול להיות לא-עדכני. טבלאות הופכות לאובייקטי `dataset`, עמודות הופכות לאובייקטי `field`, וקשרים ממופים לאובייקטי `relationship` של Ossie.

מחזיר `text/yaml` עם `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

ניתוח מסמך YAML או JSON של Ossie והחזרת הצעות רישום טבלה וקשר. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**גוף הבקשה:** YAML או JSON גולמי של Ossie. הפורמט מזוהה אוטומטית.

**תגובה:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

דבר אינו נרשם. השתמשו במסך הבדיקה של ה-UI המנהל כדי לאשר או לצמצם הצעות לפני שכל מוטציה מופעלת.

---

### פעולות (Functions ו-Webhooks)

כל נקודות הקצה תחת הקידומת `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

כל הפעלה — מ-GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, ו-Provisa gRPC — עוברת דרך מבצע (Executor) מְמוּשְל יחיד האוכף `writable_by` וממשל באופן אחיד. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] ראו [docs/integrations.md](integrations.md#invoking-commands-across-protocols) לתחביר הקריאה לכל פרוטוקול.

#### `GET /admin/actions`

מחזיר את כל ה-Functions וה-Webhooks של מסד הנתונים במעקב. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

כל אובייקט Webhook נושא Boolean בשם `approved`. Webhook מאושר ברגע ש-Steward מבצע את בקשת היצירה שלו (REQ-209); Webhooks המוצהרים בתצורה מאושרים אוטומטית. Webhook לא מאושר רשום אך אינו חשוף בשום משטח. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

רישום Function במעקב (פקודה). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**שדות מפתח:**

| שדה | נדרש | תיאור |
| --- | --- | --- |
| `name` | כן | שם פקודה ייחודי |
| `kind` | כן | `"query"` → שדה Query של GraphQL; `"mutation"` → שדה Mutation |
| `implKind` | לא | כיצד הפקודה רצה — ראו הטבלה למטה (ברירת מחדל `source_procedure`) |
| `binding` | לא | פרטי חיבור ספציפיים ל-`implKind` (אובייקט JSON) |
| `returnSchema` | לא | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — הופך את הפקודה למחזירה-קבוצה (Set-Returning) בכל משטח |
| `arguments` | לא | הגדרות ארגומנט `[{name, type}]`; סדר מיקומי חשוב עבור קוראי SQL ו-Bolt |
| `visibleTo` | לא | מזהי תפקיד שיכולים לקרוא לפקודה |
| `writableBy` | לא | מזהי תפקיד המורשים להפעיל אותה כ-Mutation |
| `domainId` | לא | תחום עבור מיקום GraphQL ובקרת גישה |

**ערכי `implKind`:**

| `implKind` | מה רץ | שדות `binding` |
| --- | --- | --- |
| `source_procedure` | פרוצדורה מאוחסנת במקור רשום (ברירת מחדל) | `sourceId`, `schemaName`, `functionName` |
| `script` | סקריפט צד-שרת | `script` |
| `http` | קריאת HTTP יוצאת | `url`, `method` |
| `grpc` | קריאת gRPC יוצאת לשרת חיצוני | `target`, `method` |
| `python` | Callable של Python המתארח על-ידי Provisa (REQ-885) | `callable` (למשל `"demo.py_functions:random_dataset"`) |

פקודות ההדגמה `random_python_set` (`implKind: python`) ו-`random_grpc_set` (`implKind: grpc`) מדגימות פקודות מחזירות-קבוצה עם `returnSchema` בפועל; שתיהן ב-`config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

עדכון Function במעקב לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

מחיקת Function במעקב לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

רישום Webhook במעקב. (REQ-209) רישום או עדכון Webhook מוסיף לתור בקשת אישור Steward — ה-Webhook הופך פעיל בכל המשטחים רק לאחר שה-Steward מאשר אותו. Webhooks המוצהרים בתצורה מאושרים אוטומטית. **שדות גוף הבקשה:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

עדכון Webhook במעקב לפי שם. כל עריכה מאפסת את האישור למצב ממתין עד לאישור מחדש. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

מחיקת Webhook במעקב לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

בדיקת פעולה (Function או Webhook) לפי שם. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### תפקידים (Roles)

כל נקודות הקצה תחת הקידומת `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| שיטה | נתיב | תיאור |
| --- | --- | --- |
| `GET` | `/admin/roles/` | רשימת כל התפקידים |
| `POST` | `/admin/roles/` | יצירת תפקיד |
| `PUT` | `/admin/roles/{role_id}` | עדכון תפקיד |
| `DELETE` | `/admin/roles/{role_id}` | מחיקת תפקיד |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### משתמשים (Users)

כל נקודות הקצה תחת הקידומת `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| שיטה | נתיב | תיאור |
| --- | --- | --- |
| `POST` | `/admin/users/` | יצירת משתמש מקומי |
| `GET` | `/admin/users/` | רשימת משתמשים מקומיים |
| `GET` | `/admin/users/{user_id}` | קבלת משתמש |
| `PUT` | `/admin/users/{user_id}` | עדכון משתמש |
| `PATCH` | `/admin/users/{user_id}/password` | שינוי סיסמה |
| `DELETE` | `/admin/users/{user_id}` | מחיקת משתמש |
| `GET` | `/admin/users/{user_id}/assignments` | רשימת שיוכי תפקיד |
| `POST` | `/admin/users/{user_id}/assignments` | הוספת שיוך תפקיד |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | הסרת שיוך תפקיד |

---

### ארגונים (Organizations)

כל נקודות הקצה תחת `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| שיטה | נתיב | תיאור |
| --- | --- | --- |
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
| --- | --- | --- |
| `POST` | `/admin/invites/` | יצירת הזמנה |
| `GET` | `/admin/invites/` | רשימת הזמנות ממתינות |
| `DELETE` | `/admin/invites/{token}` | ביטול הזמנה |

---

### Admin GraphQL

#### `POST /admin/graphql`

נקודת קצה Strawberry GraphQL עבור כל פעולות הניהול: CRUD של מקורות וטבלאות, ניהול קשרים, תצורת תחום, כללי RLS, בקרת מטמון, מוסכמות שמות, ניהול משימות מתוזמנות, והידור שאילתה. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

מחזיר את מצב ההגדרה של ריצה ראשונה. תמיד לא מאומת. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

השלמת הגדרת ריצה ראשונה. [tool-verified: `provisa/api/setup_router.py:142`]

---

## בדיקת תקינות (Health Check)

#### `GET /health` או `HEAD /health`

מחזיר `{"status": "ok"}`. תמיד לא מאומת. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## תגובות שגיאה

| סטטוס | משמעות |
| --- | --- |
| 400 | שאילתה לא תקינה, שגיאת אימות, או שגיאת ניתוח SQL |
| 401 | אסימון אימות חסר או לא תקין |
| 403 | יכולות לא מספיקות; הפרת ממשל |
| 404 | תפקיד, משאב, או קובץ תצורה לא נמצא |
| 422 | כותרת נדרשת חסרה (למשל `X-Role`) |
| 503 | מסד נתונים או מקור לא מחובר; תלות לא זמינה |
| 504 | הבקשה עברה Timeout |

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

כל שגיאה אחרת משתמשת ב-: `{"detail": "<message>"}`.

---

## נקודת קצה Arrow Flight

פורט `8815`. תעבורה עמודתית (Columnar) ילידית של Arrow מעל gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

שאילתות וגילוי קטלוג שניהם זמינים על אותו חיבור. Pipeline הממשל המלא (RLS, מיסוך, Sampling) מיושם על כל שאילתה. (REQ-130, REQ-143)

**פורמט כרטיס (Ticket)** (JSON):

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

כאשר Proxy מסוג Zaychik Flight SQL זמין (פורט 8480), אצוות (Batches) של רשומות זורמות מקצה-לקצה ללא Materialization מלא. (REQ-144) חוזר ל-Materializing דרך שכבת השאילתה הפדרטיבית כאשר Zaychik אינו זמין. (REQ-146)

---

## נקודת קצה Protobuf gRPC

פורט `50051` (עקיפה עם משתנה סביבה `GRPC_PORT` או תצורת `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

העבירו את התפקיד במפתח המטא-דאטה של gRPC `x-provisa-role`. אם חסר, השרת מבטל (Aborts) עם `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

הורידו את ה-Proto הספציפי-תפקיד מ-`GET /data/proto/{role_id}`. רק טבלאות ועמודות הנראות לאותו תפקיד מופיעות. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

כל טבלה מפיקה RPC זרם בשם `Query{TypeName}`. RPCs מסוג `Insert{TypeName}` קיימים לצורך סימטריית Schema אך מבוטלים עם `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` מופעל עבור גילוי שירות ללא Proto מהודר מראש. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

שרת ה-gRPC מתחיל רק כאשר ניתן להדר Proto תקף באתחול. אם בניית ה-Schema נכשלת, שרת ה-gRPC אינו מתחיל. (REQ-529)

#### RPCs לאגרגציה וקיבוץ (REQ-1359, REQ-1361, REQ-1405)

כאשר לטבלה מוגדר `enable_aggregates`, ה-Proto שנוצר כולל שני RPCs נוספים לצד `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — מחזיר סקלרים אגרגטיביים עבור הטבלה (`count`; `sum`, `avg`, `stddev`, `variance` לכל עמודה מספרית; `min`, `max` לכל עמודה בת-השוואה)
- **`Query{TypeName}GroupBy`** — מחזיר שורה אחת לכל מפתח קיבוץ עם תת-שדות אגרגטיביים ובאופן אופציונלי, סקלרים של טבלת הבסיס ושורות ממד מצורף בשדה `nodes`

שניהם עוברים דרך אותו Pipeline מהדר-אגרגציה כמו שדות השורש `{field}_aggregate` ו-`{field}_group_by` של GraphQL — ללא מימוש אגרגציה נפרד. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**שדה `funcs` (REQ-1361).** הודעת הבקשה מקבלת שדה מחרוזת-חוזרת בשם `funcs`. ערכים תקפים הם `count`, `sum`, `avg`, `stddev`, `variance`, `min`, ו-`max`. כאשר `funcs` מושמט, כל פונקציה ש-Schema חושף עבור אותה טבלה מתבקשת. כאשר מוגדר, רק הפונקציות הנקובות מופיעות. אם אף אחת מהפונקציות הנקובות אינה חלה על סוגי העמודות של הטבלה, השאילתה חוזרת ל-`count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**שדות `include_nodes` ו-`include` (REQ-1405).** בקשות `Query{TypeName}GroupBy` יכולות להגדיר `include_nodes: true` כדי לכלול עמודות סקלריות של טבלת הבסיס בשדה `nodes` של כל שורה. השדה החוזר `include` נוקב בשמות שדות קשר Many-to-One שהעמודות הסקלריות שלהם מקוננות גם הן בתוך `nodes`. זה תואם את התנהגות `?includeNodes=` / `?include=` של JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## מנהל התקן JDBC

מנהל התקן ה-JDBC של Provisa (`provisa-jdbc-0.1.0.jar`) חושף את הקטלוג הסמנטי לכלי BI (Tableau, PowerBI, DBeaver). (REQ-126)

**כתובת חיבור (Connection URL):** `jdbc:provisa://host:port` (REQ-131)

תחומים ממופים ל-Schema-ים של JDBC. (REQ-127) טבלאות משתמשות בכינוים (Aliases) הרשומים שלהן. עמודות משתמשות בכינויים וחושפות תיאורים כ-`REMARKS`. (REQ-128) שיטות מטא-דאטה סטנדרטיות (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) חושפות קשרים סמנטיים כמטא-דאטה של PK/FK.

**תמיכת SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

מנהל ההתקן מבקש הפניית Arrow IPC כברירת מחדל. תוצאות זורמות אצווה-אחר-אצווה (Batch-by-Batch) דרך `ArrowStreamReader`, מוגבל לאצווה אחת של רשומות בזיכרון. (REQ-293)

---

## פורמט ארגומנט `orderBy`

הארגומנט `order_by` משתמש באובייקטי `{column: direction}` עם Enum כיוון בעל 6 ערכים: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

כיוונים נתמכים: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscriptions

Subscriptions מסוג SSE זמינים בכתובת `GET /data/subscribe/{table}`. (REQ-219, REQ-258) מסירת ההתראות משתמשת בספק ניתן-להתקנה הנבחר לפי סוג מקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY`, מקורות MongoDB משתמשים ב-Change Streams, ומקורות Kafka משתמשים בקבוצות צרכנים. סינון RLS ואימות Schema חלים ללא תלות בספק. מקורות WebSocket ו-RSS נתמכים גם הם דרך אותה נקודת קצה. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## מילון עסקי (REQ-1387)

המילון העסקי ממפה שמות שדה פיזיים — כפי שהם קיימים במסדי הנתונים המקור — לאוצר מילים אנושי משותף. כל עמודה הרשומה בשכבה הסמנטית מקבלת מונח אוטומטית. לא נדרשת הזנה ידנית לאכלוס המילון; אוצרים (Curators) מוסיפים הגדרות, קשרים, ומומחים מעל מה שהמערכת גוזרת.

### כיצד מונחים נגזרים

כאשר Provisa רושמת או מעדכנת עמודות של טבלה, `normalize_term` (`provisa/core/glossary.py`) רץ על כל שם עמודה ומייצר ביטוי קנוני. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

הנרמול מיישם חמישה כללים ברצף:

1. פיצול על גבולות camelCase ותווי הפרדה (`_`, `-`, `.`, `/`, רווח).
2. המרה לאותיות קטנות (Case-Fold).
3. הרחבת טבלת קיצורים קבועה (למשל `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. הסרת **אסימון Proxy** נגרר (`identifier`, `code`, `index`, או `reference`) — עמודה הנקראת לפי המפתח או הקוד שלה מצביעה על המושג הבסיסי דרך ערך תחליף, כך שהמונח צריך להיות המושג עצמו. האסימון האחרון הנותר לעולם אינו מוסר.
5. הכשרת (Qualify) **ביטוי כללי מדי** בעזרת מושג הטבלה. כאשר הביטוי המנורמל המלא הוא מילת מאפיין חשופה (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, ודומיהם), המונח הופך ל-`<table concept> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. מונח `name` אחד משותף לטבלאות לא קשורות היה ממזג משמעויות נבדלות; ההכשרה מחברת כל עמודה למושג המכיל אותה במקום זאת. מושג הטבלה הוא השם העסקי של הטבלה, מנורמל עם שם עצם יחיד ראשי (`order_lines` → `order line`).

עמודות פסאודו-Native-Filter (בקידומת `_nf_`, או כל עמודה הנושאת `native_filter_type`) הן מכניקת פרמטר-שאילתה, לא שדות עסקיים, ואינן גוזרות מונחים.

מכיוון ש-`id`, `key`, `pk`, ו-`sk` כולם מתרחבים ל-`identifier` לפני בדיקת ה-Proxy, שלושה שמות עמודה שונים פיזית נוחתים על אותו מונח בדיוק:

| שם פיזי | לאחר נרמול |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

השלושה הראשונים מתמזגים למונח אחד. `transaction amount` שומר על שני האסימונים כי `amount` אינו Proxy. עמודת `id` חשופה — ללא אסימונים קודמים — לא ניתנת להסרה; היא מנורמלת ל-`identifier` כך שהמונח אינו ריק. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### מחזור חיים

מונחים **נגזרים מחברות בשכבה הסמנטית**, לא נוצרים לפי דרישה על-ידי משתמשים. מאגר הטבלה (Repository) הוא נתיב הכתיבה היחיד: `sync_table_refs` רץ בתוך כל Upsert של קבוצת עמודות, ו-`sweep_refless_terms` רץ לאחר כל נתיב מחיקה. [tool-verified: `provisa/core/repositories/glossary.py`]

**כאשר עמודה מתווספת:** Provisa מחפשת את המונח המנורמל לפי שם. אם הוא כבר קיים, העמודה מקבלת הפניה (Ref) אליו (ואם המונח היה מוצא משימוש (Deprecated), הוא מוחיה — `deprecated` מוגדר בחזרה ל-`False`). אם לא קיים מונח עדיין, אחד נוצר.

**כאשר עמודה עוזבת** (שינוי Schema או הסרת טבלה): ה-Ref שלה נמחק והמונח **מיושב (Settled)** תחת כלל הסרה-או-הוצאה-משימוש. מונח מושרש (Rooted) ללא Refs נותרים מוסר לחלוטין — יחד עם הקשתות והשיוכי המומחה שלו — אלא אם הסרתו הייתה משאירה מונח מופשט מנותק מכל המונחים המושרשים (ללא נתיב דרך גרף המונחים). במקרה כזה, המונח **מוצא משימוש** (מסומן `deprecated=True`) במקום להימחק, כך שעוגן הגרף של המונח המופשט שורד.

מונחים מופשטים לעולם אינם מוסרים אוטומטית; הם קיימים מחוץ למחזור החיים הפיזי ומוסרים רק במפורש דרך ה-API של המנהל.

**החייאה:** אם השם המנורמל של מונח מוצא-משימוש מופיע שוב (עמודה נרשמת מחדש), המונח מבוטל ו-Refs שלו חוזרים להצטבר.

### נקודות קצה לאצירה (Curation)

כל נקודות הקצה תחת `/admin/glossary`. הן דורשות גישת `org_admin` וארגון מוגדר. כל מוטציה מפעילה פרסום מטא-דאטה. [tool-verified: `provisa/api/admin/glossary_router.py`]

| שיטה | נתיב | תיאור |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | רשימת מונחים. פרמטרי שאילתה: `q` (חיפוש שם/הגדרה), `include_deprecated` (ברירת מחדל `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | קבלת פרטי מונח: הגדרה, Refs פיזיים, קשתות מוקלדות, מומחים |
| `POST` | `/admin/glossary/terms` | יצירת מונח מופשט — אוצר מילים משתמש ללא Refs פיזיים |
| `PATCH` | `/admin/glossary/terms/{term_id}` | שינוי שם, הגדרת הגדרה, או החלפת החרגת ייצוא |
| `DELETE` | `/admin/glossary/terms/{term_id}` | מחיקת מונח ללא Refs פיזיים |
| `POST` | `/admin/glossary/refs/move` | העברת Ref פיזי אחד למונח אחר (איחוד) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | הוספת קשת קשר מוקלדת בין שני מונחים |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | הסרת קשת (פרמטרי שאילתה: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | תיוג משתמש כמומחה או מחבר עבור מונח |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | הסרת ייעוד מומחה/מחבר של משתמש |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | ניסוח הגדרה עבור מונח אחד באמצעות מודל ה-AI של הארגון — מחזיר טקסט בלבד, דבר אינו נשמר עד לשמירה |
| `POST` | `/admin/glossary/definitions/generate` | יצירה ושמירה של הגדרות עבור כל מונח ללא הגדרה — לעולם אינו דורס טקסט שנכתב על-ידי אדם |
| `POST` | `/admin/glossary/relationships/generate` | הצעה ושמירה של קשתות מוקלדות על-פני כל המילון באמצעות מודל ה-AI של הארגון |

**גוף `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**גוף `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

ערכי `rel_type` תקפים: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**גוף `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

ערכי `kind` תקפים: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**גוף `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

העברת Ref מיישבת (Settles) את המונח המפסיד תחת כלל הסרה-או-הוצאה-משימוש. השתמשו בכך כדי לאחד שני מונחים שהנרמול שמר נפרדים — לדוגמה, לאחר שמקור השתמש בקיצור לא-סטנדרטי שנפל מחוץ לטבלת ההרחבה.

מחיקת מונח מושרש (בעל Refs פיזיים) מחזירה `400 glossary.invalid`. הסירו או העבירו את כל ה-Refs תחילה.

**`PATCH /admin/glossary/terms/{term_id}` — שדה `export_excluded`:**

```json
{"export_excluded": true}
```

הגדרת `export_excluded` ל-`true` שומרת את המונח בחוץ מכל תמונות מצב (Snapshots) של ייצוא מטא-דאטה, ללא תלות ב-Refs הפיזיים או במעמד המופשט שלו. הגדרתו בחזרה ל-`false` משיבה את המונח לתמונת המצב בפרסום הבא. נתוני אצירה (הגדרה, קשתות, מומחים) אינם מושפעים. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### אצירה בסיוע AI

מודל ה-AI המוגדר של הארגון יכול לנסח הגדרות ולהציע קשתות קשר על-פני כל המילון בפעולה אחת. שתי הפעולות האגרגטיביות דורשות גישת `org_admin` וארגון מוגדר.

**`POST /admin/glossary/definitions/generate`**

עובר על כל מונח במילון, מדלג על כל מונח שכבר יש לו הגדרה, וקורא למודל ה-AI של הארגון לנסח אחת עבור כל מונח נותר. הטיוטה נשמרת מיידית — בניגוד לנקודת הקצה של טיוטה-לכל-מונח (`POST /admin/glossary/terms/{term_id}/definition/generate`), אין שלב עורך. הגדרות שנכתבו על-ידי אדם לעולם אינן נדרסות: השומר הוא `if summary["definition"]: continue` לפני כל קריאה למודל. התראת פרסום אחת מכסה את כל האצווה. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

תגובה:

```json
{"generated": 12}
```

`generated` היא ספירת המונחים שקיבלו הגדרה חדשה. היא אפס כאשר לכל מונח כבר יש אחת.

**`POST /admin/glossary/relationships/generate`**

שולח את רשימת המונחים המלאה למודל ה-AI של הארגון עם Prompt המפרט את עשרת סוגי הקשת המותרים (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) ומבקש רק הצעות בטוחות. המודל מגיב עם מערך JSON; כל רשומה מאומתת לפני כל כתיבה: שמות מונח לא ידועים, קשתות-עצמיות (Self-Edges), וסוגי קשת מחוץ ל-Enum הסגור נשמטים בשקט. הצעות תקפות מוכנסות (Upserted) באופן אידמפוטנטי — הרצה חוזרת של הפעולה אינה משכפלת קשתות. התראת פרסום אחת מכסה את האצווה. נקודת הקצה מחזירה `{"added": 0}` מיידית כאשר המילון מכיל פחות משני מונחים לא-מוצאים-משימוש. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

תגובה:

```json
{"added": 5}
```

`added` היא ספירת הקשתות שנכתבו. קשת שכבר הייתה קיימת עדיין נספרת — ה-Upsert מצליח, אך נתוני הקשת אינם משתנים.

### כלי MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

מחפש שמות ודיוגי מונחים עם התאמת תת-מחרוזת לא-תלוית-רישיות, עד `limit` תוצאות. כל תוצאה היא פרט המונח המלא: `name`, `definition`, `is_abstract`, `deprecated`, Refs פיזיים (עם `source_id`, `schema_name`, `table_name`, `column_name`), קשתות מוקלדות, ושיוכי מומחה. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

השתמשו ב-`search_terms` לפני כתיבת SQL כדי למצוא כל שדה פיזי המייצג מושג לפי שם. לדוגמה, חיפוש `"order date"` מחזיר את המונח וכל עמודות `order_dt`, `orderDate`, `ORDER_DATE` על-פני כל טבלה רשומה.

### ייצוא מטא-דאטה

גרף מונחי המילון נכלל בכל `MetadataSnapshot` שנבנה על-ידי `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

הייצוא מיישם את אותם מסננים כמו שאר תמונת המצב:

- מונח המסומן `export_excluded` נשמר בחוץ לחלוטין — ללא תלות ב-Refs הפיזיים שלו, במעמד המופשט, או האם קטלוג הארגון מוגדר. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- מונח מושרש מתפרסם רק כאשר לפחות אחד מה-Refs הפיזיים שלו שייך לעמודה שעוברת גם את מסנן **Data Product** (דגל `data_product` של הטבלה חייב להיות `true`) וגם את מסנן העמודה **הטכנית** (עמודות המתויגות `technical` נשמרות בחוץ).
- מונח מושרש שכל ה-Refs שלו נשמרים בחוץ על-ידי מסננים אלה נשמר בחוץ יחד איתם.
- מונחים מופשטים מתפרסמים ללא תנאי — הם אוצר מילים משתמש, לא מחוברים לעמודות פיזיות.
- קשת בין שני מונחים מתפרסמת רק כאשר שני מונחי הקצה מתפרסמים.

כל מתאם ספק מפרסם את גרף המונחים באופן ילידי, לתוך מכל מילון בבעלות Provisa שהוא יוצר באופן אידמפוטנטי — לעולם לא לתוך מילון קטלוג קיים:

| ספק | מכל | מונחים | קשרים | הוצאה משימוש |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (API מילון) | מונחי מילון, הגדרה על `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | סמן `[DEPRECATED]` ב-shortDescription |
| Atlan | מילון Provisa לפי qualifiedName יציב | `longDescription` (לעולם לא ה-`userDescription` הערוך-אנושית) | אותו מיפוי Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | Aspect מסוג `glossaryTermInfo` לכל מונח | KIND_OF → Inherits, PART_OF → Contains (הפוך), RELATED_TO/SYNONYM_OF → מונחים קשורים | Aspect הוצאה משימוש; שינויי שם עוקבים אחר סוקצסיה של URN |
| OpenMetadata | מילון Provisa דרך `/v1/glossaries` | PUT ממופתח-fqn, שינויי שם PATCH-מקושרים-מחדש לפי UUID שמור | KIND_OF → היררכיית הורה ילידית, SYNONYM_OF → `synonyms`, אחרים → `relatedTerms` | `entityStatus` |
| Collibra | תחום מסוג-מילון "Provisa Glossary" | נכסי Business Term דרך Import API | סוגי יחס Business Term ילידיים | סטטוס נכס |

הבעלות היא הקישור, לא השם: המזהה של כל ספק לכל מונח מפורסם נלכד תחת `catalog_bindings` תחת ה-URN של המונח (`provisa://<org>/terms/<name>`), ו-Provisa משנה או מוחקת פריט מילון בצד הספק רק כאשר היא מחזיקה קישור זה (או שהפריט חי במכל בבעלות Provisa שהיא יצרה). פריט מילון ללא קישור Provisa מקורו במערכת החיצונית ולעולם אינו נגע בו; עדכונים בקריאה-מיזוג כך ששדות שנוספו על-ידי Steward על מונחי Provisa עצמם שורדים; דבר אינו נמחק כאשר מונח עוזב את תמונת המצב. שיוכי מונח-לנכס של Steward נשארים בבעלות חיצונית — אף מתאם אינו כותב שיוכי מונח-לנכס (פרסום שיוכים שנכתבו על-ידי Provisa הוא המשך עתידי מפורש). ב-Collibra ספציפית, הבטיחות תחת סמנטיקת REPLACE של Import API נשענת על הכלה: המטען מזכיר רק נכסים בתוך תחום מילון Provisa ומופעי יחס רק בין מונחי Provisa, כך שמילוני Steward והיחסים שלהם לעולם אינם ניתנים להגעה. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]

---

## מוצרי נתונים (REQ-1634)

מוצר נתונים מקבץ טבלאות המתפרסמות יחד לצריכה, בבעלות תחום אחד בלבד. שדות עוקבים אחר אוצר המילים של ODPS (Open Data Product Standard) במקום שבו כבר קיים ל-Provisa מקור האמת. ה-UI המנהל חושף מוצרי נתונים תחת **Admin → Data Products**. [tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/schema_mutation.py:949-1017`, `provisa/api/admin/schema_query.py:352-362`]

### יכולות

| יכולת | מעניק |
| --- | --- |
| `data_product_read` | גישת קריאה לשדה השאילתה `data_products` ולעמוד הניהול של מוצרי נתונים. מוזרע כברירת מחדל ל-`org_admin`, `analyst`, `developer`, ו-`modeler`. |
| `data_product_rw` | מוטציות יצירה ומחיקה. מפעיל את בקרות New / Edit / Delete ב-UI. |

[tool-verified: `provisa/api/admin/schema_mutation.py:959,1001`, `provisa/api/admin/schema_query.py:357`]

### Admin GraphQL

כל פעולות מוצר הנתונים עוברות דרך `POST /admin/graphql`.

**שאילתה:**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

דורש `data_product_read`.

**יצירה או עדכון:**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` מבצע Upsert — קריאה עם `id` קיים מעדכנת את הרשומה. דורש `data_product_rw`.

**מחיקה:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

מחיקת מוצר מנקה את `product_id` מכל טבלת חבר, ומסירה את חברותן. דורש `data_product_rw`. [tool-verified: `provisa/api/admin/schema_mutation.py:995-1017`]

### Schema של שדות

| שדה | סוג | נדרש | הערות |
| --- | --- | --- | --- |
| `id` | `String` | כן | מזהה יציב קריא-מכונה, למשל `customer_360` |
| `domain_id` | `String` | כן | התחום הבעלים. טבלאות חבר חייבות לשתף `domain_id` זה — אי-התאמות נדחות בעת שמירה |
| `name` | `String` | כן | שם תצוגה |
| `owner_role` | `String` | לא | תפקיד האחראי למוצר זה; נבדל מ-Steward התחום |
| `team_role` | `String` | לא | תפקיד שבעליו מתחזקים מוצר זה יום-יום; מתפענח לפרטים |
| `purpose` | `String` | לא | מה מוצר זה מפרסם ומדוע |
| `limitations` | `String` | לא | אילוצים, הסתייגויות או החרגות ידועים |
| `usage` | `String` | לא | כיצד לצרוך מוצר זה |
| `version` | `String` | לא | למשל `1.2.0` |
| `status` | `String` | לא | למשל `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | לא | התחייבויות רמת שירות; פרוזה — מוצר משתרע על פני מספר טבלאות וניתן SLA מובנה לא יכול לנקוב באופן חד-משמעי איזה חבר הוא מתאר |
| `support` | `String` | לא | הנחיית תמיכה בטקסט חופשי |
| `custom_properties` | `JSON` | לא | מטא-דאטה במפתח-ערך שרירותי שאינו מכוסה על-ידי השדות הסטנדרטיים |

שני שדות נוספים קיימים במודל אך אינם חשופים ב-`DataProductType` / `DataProductInput` של Strawberry — הם ספציפיים ל-Snowflake Horizon Catalog (REQ-1635):

| שדה | הערות |
| --- | --- |
| `support_contact` | אימייל או כתובת URL; נדרש עבור קובצי רשימת ארגון של Horizon Catalog |
| `publish` | `true` לפרסום רשימות Horizon מיידית; רשימות חדשות ברירת מחדל DRAFT |

[tool-verified: `provisa/core/models.py:338-341`, `provisa/api/admin/types.py:104-118,538-551`]

### חברות טבלה

טבלה מצטרפת למוצר נתונים על-ידי הגדרת שדה ה-`product_id` שלה בטופס עריכת הטבלה. הבורר מוגבל למוצרים ש-`domain_id` שלהם תואם לתחום של הטבלה עצמה — טבלה בתחום `marketing` לעולם אינה מוצעת מוצר בתחום `sales`. [tool-verified: `provisa/api/admin/actions_router.py:244-260`, `docs/arch/requirements.yaml:54585-54586`]

פקודות באותו תחום יכולות להיות משויכות גם הן כחברות. [tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:commandsLabel`]

### מסנן ייצוא מטא-דאטה

`build_snapshot` מיישם `data_products_only=True` עבור כל פרסום קטלוג. טבלאות ללא `product_id` נשמרות בחוץ מתמונת המצב, יחד עם קשתות הקשר, קשתות ה-Lineage, ותגי הממשל שלהן. מקורות ותחומים מתפרסמים תמיד. מונחי מילון מתפרסמים רק כאשר לפחות אחד מה-Refs הפיזיים שלהם שייך לטבלה מיוצאת (חברת-מוצר). [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

מוצר ללא חברים מיוצאים אינו בונה רשומת תמונת מצב — רשימה ללא חברים הייתה מציגה מצג שווא של המוצר לקטלוג. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

### תמיכת מוצר נתונים לפי יעד קטלוג

`MetadataSnapshot.data_products` מגיע לכל מתאם, אך רק מתאמים שהפלטפורמה שלהם בעלת מושג מוצר-נתונים ילידי מפרסמים אותו כישות מדרגה ראשונה; השאר מפרסמים את טבלאות החבר (שכבר סוננו כאמור) ללא קיבוץ מוצר.

| יעד | ייצוג מוצר נתונים |
| --- | --- |
| Snowflake Horizon | כל מוצר הופך ל-`SHARE` מעל הכתובות הפיזיות של טבלאות החבר שלו, עטוף ב-`CREATE ORGANIZATION LISTING` פנימי — Data Product ילידי של Horizon Catalog. `publish=true` מפעיל את הרשימה מיידית; אחרת היא נוחתת כ-DRAFT. [tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:21-34,389-418`] |
| BigQuery Dataplex | כל מוצר הופך לרשימת Analytics Hub דרך `/v1/dataProducts`. [tool-verified: `provisa/api/metadata_export/bigquery_dataplex.py:100,136,159`] |
| OpenMetadata | כל מוצר הופך לישות `DataProduct` ילידית (`/api/v1/dataProducts`), עם בעלות נגזרת-תחום. [tool-verified: `provisa/api/metadata_export/openmetadata.py:326-344,635`] |
| DataHub | כל מוצר הופך לישות `dataProduct` ילידית (`urn:li:dataProduct:...`) עם Aspects `dataProductProperties`/בעלות משלה. [tool-verified: `provisa/api/metadata_export/datahub.py:133-136,443-483`] |
| Collibra | כל מוצר הופך לנכס מסוג קהילת `Data Product`, מקושר לטבלאות החבר שלו דרך יחס `Data Product groups Table`. [tool-verified: `provisa/api/metadata_export/collibra.py:129-133,371-388`] |
| Atlan | מתפרסם כניחוש typedef מותאם `DataProduct` — ל-Atlan אין שם סוג יציב מתועד למושג זה, כך שהמיפוי הוא במיטב המאמץ. [tool-verified: `provisa/api/metadata_export/atlan.py:60`] |
| Apache Atlas | מתפרסם כ-typedef מותאם `provisa_data_product` עם יחס `provisa_data_product_members` — ל-Atlas אין סוג ישות מוצר-נתונים ילידי. [tool-verified: `provisa/api/metadata_export/atlas.py:134-147,191,256-260`] |
| OpenLineage | לא ישות מדרגה ראשונה — טבלאות חבר נושאות Facet מותאם `provisa_data_product` הנוקב במוצר הבעלים. [tool-verified: `provisa/api/metadata_export/openlineage.py:243,348`] |
