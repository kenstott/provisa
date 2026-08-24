# מדריך API

## סקירה כללית

Provisa חושפת נקודות קצה REST תחת שתי תחיליות: `/data` להרצת שאילתות ולבדיקת סכמה (introspection), ו-`/admin` לניהול תצורה. (REQ-043) רוב נקודות הקצה של הנתונים דורשות מזהה תפקיד. פעולות ניהול תצורה משתמשות ב-Strawberry GraphQL API בכתובת `/admin/graphql`. (REQ-164)

---

## אימות (Authentication)

כאשר `auth.provider` מוגדר בקובץ `provisa.yaml`, כל נקודות הקצה מלבד `/health` ו-`/setup/status` דורשות כותרת `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

ללא הגדרת אימות, השרת פועל במצב פיתוח (dev mode). כל בקשה מטופלת כזהות `anonymous`, אשר ממופה לכל התפקידים המוגדרים עם גישת תחום כללית (wildcard). (REQ-535)

**התחברות (`POST /auth/login`)** מסופקת על ידי ספק האימות הפעיל כאשר מוגדר `provider: basic`. (REQ-124) פורמט האישורים והתגובה תלויים בספק.

**בדיקת זהות (introspection):**

```http
GET /auth/me
```

מחזיר את מזהה המשתמש המאומת, כתובת האימייל, שם התצוגה, חברויות בארגונים ושיוכי תפקידים. במצב פיתוח מחזיר `dev_mode: true` עם רשימת כל מזהי התפקידים. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

מחזיר `{"provider": "<name>"}` או `{"provider": null}` כאשר האימות אינו מוגדר. [tool-verified: `provisa/api/auth_router.py`]

---

## נקודות קצה של נתונים

### `POST /data/graphql`

הרצת שאילתת או מוטציית GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**גוף הבקשה:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

השדה `role` משמש רק במצב פיתוח (ללא אימות). כאשר האימות פעיל, נעשה שימוש בתפקיד המשתמש המאומת, והשדה `role` בגוף הבקשה מתעלם.

השדה `extensions` תומך בפרוטוקול Automatic Persisted Query‏ (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**כותרות:**

- `X-Provisa-Role` — דריסת תפקיד (מצב פיתוח)
- `Accept` — פורמט התגובה (ראו משא ומתן על תוכן)
- `Authorization` — `Bearer <token>` כאשר האימות מופעל
- `X-Provisa-Redirect-Format` — סוג MIME לפלט הפניית S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — מספר השורות שמעליו מופעלת הפניה (REQ-137)
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

**תגובה (שורש מרובה עם שילוב מוטבע/הפניה):**

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

שאילתות עם שורש מרובה מריצות כל שדה שורש באופן עצמאי. שדות מתחת לסף ההפניה מוחזרים מוטבעים; שדות מעליו מופנים. המפתח `redirects` (ברבים) ממפה שמות שדות למידע ההפניה. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**כותרות מטמון:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (במקרה של HIT) (REQ-536)

**יכולות נדרשות:** `QUERY_DEVELOPMENT` עבור כל הבקשות, כולל בדיקת סכמה. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### משא ומתן על תוכן (Content Negotiation)

| כותרת Accept | פורמט |
| --- | --- |
| `application/json` | JSON (ברירת מחדל) |
| `application/x-ndjson` | JSON מופרד בשורות (Newline-delimited) |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### הפניה (Redirect)

תוצאות מעל סף שורות מוגדר (או כאשר `X-Provisa-Redirect: true`) נכתבות ל-S3, ומוחזר קישור חתום מראש (presigned URL). (REQ-029, REQ-044)

| פורמט הפניה | נכתב על ידי | זיכרון |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS פדרטיבי | ללא — הנתונים לעולם לא עוברים דרך Provisa |
| `application/x-orc` | CTAS פדרטיבי | ללא — הנתונים לעולם לא עוברים דרך Provisa |
| `application/json` | Provisa | תלוי זיכרון |
| `application/x-ndjson` | Provisa | תלוי זיכרון |
| `text/csv` | Provisa | תלוי זיכרון |
| `application/vnd.apache.arrow.stream` | Provisa | תלוי זיכרון |

לייצוא אנליטי גדול, השתמשו בהפניית Parquet או ORC. מנוע הפדרציה כותב ישירות ל-S3 במקביל — אין נתונים העוברים דרך Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

הרצת SQL גולמי דרך צינור הממשל (governance pipeline) של שלב 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**גוף הבקשה:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**יכולות נדרשות:** `QUERY_DEVELOPMENT`.

הפרות ממשל ב-`POST /data/sql` מחזירות HTTP 403. (REQ-002, REQ-266)

**תגובה:** אותו פורמט כמו `/data/graphql` (שורות JSON כברירת מחדל, עם משא ומתן על תוכן דרך `Accept`).

---

### `POST /data/query`

נקודת קצה מאוחדת לשאילתות. תומכת ב-GraphQL, SQL או Cypher — התחביר מזוהה אוטומטית. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

שאילתות Cypher ניתנות להגשה גם לנקודת הקצה הייעודית `POST /query/cypher`. (REQ-345)

**גוף הבקשה:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

מחזיר `{"data": ...}` עבור GraphQL, ו-`{"columns": [...], "rows": [...]}` עבור SQL ו-Cypher.

---

### `POST /data/sql/explain`

הסבר או ניתוח של משפט SQL דרך הצינור הממושל. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

נקודת הקצה עוטפת את ה-SQL **הממושל** — המשפט שרץ בפועל תחת התפקיד של הקורא, לאחר RLS ומיסוך — בתחביר ה-EXPLAIN של הדיאלקט. מה שהתוכנית מציגה הוא הגרסה המאושרת של השאילתה, לא הקלט הגולמי.

**גוף הבקשה:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

קבעו `analyze: true` כדי להריץ EXPLAIN ANALYZE. השאילתה מבוצעת והתוכנית נושאת ספירות שורות וזמנים אמיתיים. לא כל דיאלקט תומך ב-ANALYZE; ראו את הטבלה ב-[תוכניות שאילתה וסטטיסטיקות](engines.md#query-plans-and-statistics).

**תגובה:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

‏`400` כאשר לדיאלקט אין תמיכה ב-EXPLAIN, או כאשר מתבקש `analyze: true` בדיאלקט שאינו תומך בכך (למשל SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

מחזיר את המצב הנוכחי של שבר (shard) המנוע מבלי להעיר אותו. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

ממשק המשתמש מתשאל נקודת קצה זו כדי להציג באנר הפעלה בזמן שהמנוע מבצע התנעה קרה. היא לעולם אינה מפעילה העָרה — התשאול בטוח ואינו נחשב פעילות עבור מנגנון הכיבוי בהיעדר פעילות.

**תגובה:**

```json
{"state": "ready"}
```

ערכים אפשריים:

| מצב | משמעות |
| --- | --- |
| `always-on` | Desktop, אירוח עצמי, או coordinator עצמאי — ללא ניהול מחזור חיים |
| `ready` | השבר פעיל ומקבל שאילתות |
| `starting` | התנעה קרה מתבצעת |
| `stopped` | השבר מוקטן לאפס |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

מפעיל העָרה של המנוע ללא הרצת שאילתה. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

מחזיר `202 Accepted` מיד. ההעָרה רצה ברקע. השתמשו בכך אם ברצונכם שהמנוע יהיה מוכן לפני שהשאילתה הראשונה מגיעה — למשל, ממתזמן שמריץ שאילתות כמה דקות מאוחר יותר.

**תגובה:** `202 Accepted`, גוף `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---


### `GET /data/rest/{domain_id}/{table_name}`

נקודת קצה REST רגילה, שנוצרת אוטומטית עבור כל טבלה רשומה. מחרוזת השאילתה ממופה לארגומנטים של GraphQL, והבקשה מקומפלת ומורצת דרך אותו צינור (RLS, מיסוך, ניתוב) כמו GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**פרמטרי שאילתה:**

- `limit` — מספר שורות מקסימלי (≥ 1)
- `offset` — דילוג על שורות (≥ 0)
- `fields` — שמות עמודות מופרדות בפסיק (ברירת מחדל: כל השדות הסקלריים)
- `filter` — מערך JSON של אובייקטי סינון `{"field", "comparator", "value"}`
- `orderBy` — מערך JSON של אובייקטי מיון `{"field", "direction"}`

התפקיד המאומת נדרש; בקשות לא מאומתות מחזירות `401`. מפרט OpenAPI עבור נתיבים אלה מוגש ב-`GET /data/rest/openapi.json`, עם Swagger UI ב-`GET /data/rest/docs`.

#### חוקר OpenAPI / Swagger UI

עמוד חוקר ה-OpenAPI (`/app/openapi`) משבץ את Swagger UI בתוך iframe במצב sandbox. המפרט מוגבל לפי תפקיד — רק טבלאות ועמודות הנראות לתפקיד הנוכחי מוצגות — ובאופן אופציונלי מסונן לפי תחום דרך בורר התחום. הממשק עובר אוטומטית בין ערכת נושא בהירה וכהה. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

העמוד טוען את ה-HTML של המפרט דרך `fetch()` במקום `src` ישיר ל-iframe, כך שהבקשה נושאת את אסימון ה-bearer של ההפעלה (session), ובקשות היחסיות הפנימיות של Swagger UI נפתרות כראוי מול אותו מקור (origin). [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

בעת ניווט מקישור NL מסוג "פתח ב-OpenAPI", העמוד מרחיב אוטומטית את נקודת הקצה הרלוונטית, ממלא את פרמטרי השאילתה מתוך כתובת ה-URL שנוצרה על ידי NL (למשל `aggregate`, `groupBy`), ולוחץ על Execute — תוך שימוש בסקר DOM (polling) כדי להבטיח שכל שלב מסתיים לפני שהשלב הבא מופעל. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

נקודת קצה תואמת [JSON:API](https://jsonapi.org), שנוצרת אוטומטית עבור כל טבלה רשומה. אותו RLS, מיסוך וניתוב כמו GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**כותרת `Accept`:** חייבת לכלול `application/vnd.api+json` (סוג המדיה של JSON:API), אחרת הבקשה מחזירה `406`.

**פרמטרי שאילתה:**

- `fields[<type>]` — קבוצות שדות דלילות (sparse fieldsets), לדוגמה `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — לדוגמה `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — מופרד בפסיק, קידומת `-` לסדר יורד, לדוגמה `?sort=-created_at,amount`
- `page[number]` / `page[size]` — עימוד
- `aggregate` — פונקציות צבירה מופרדות בפסיק, שמופעלות במקום שליפת שורות: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. השתמשו ב-`?aggregate=count,sum` לבקשת תת-קבוצה. תגובות צבירה מחזירות `data: null` עם תוצאות ב-`meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — שמות עמודות מופרדות בפסיק; משמש עם `?aggregate=` לקיבוץ תוצאות. רק עמודות בתוך ה-enum `DistinctOnColumn` של הטבלה תקפות; השרת מחזיר `400` עבור כל עמודה שהתפקיד אינו רשאי לראות. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` לכלול עמודות סקלריות של טבלת הבסיס (וכן שדות ממד סקלריים המצורפים ששמם צוין ב-`include=`) בתוך המערך `nodes` של כל שורת קבוצה. נדרש כאשר שאילתת קיבוץ (group-by) של NL מבקשת גם פרטי ממד. (REQ-1405)

התגובות הן אובייקטי משאב עם `type`/`id`/`attributes`. שגיאות פועלות לפי צורת אובייקט השגיאה של JSON:API.

#### חוקר JSON:API

עמוד חוקר ה-JSON:API (`/app/jsonapi`) הוא ממשק דפדפן מעל נקודות קצה אלו. בחרו טבלה מהרשימה המקובצת לפי תחום, ואז הגדירו:

- **שדות** — בחרו אילו עמודות לכלול (קבוצת שדות דלילה); השאירו הכול לא מסומן כדי לבקש כל עמודה
- **קשרים** — בחרו שמות קשרים הנגזרים ממפתח זר (FK) לצירוף (sideload) דרך `?include=`
- **סינון** — שדה, אופרטור (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) וערך
- **מיון** — שדה אחד, סדר עולה או יורד
- **צבירה** — בחרו עמודות קיבוץ מהרשימה המאומתת בשרת, ואז סמנו פונקציית צבירה אחת או יותר; כאשר נבחרו עמודות קיבוץ, תיבת סימון "Include nodes" מוסיפה עמודות סקלריות של טבלת הבסיס לכל שורה
- **גודל עמוד** — מספר משאבים לעמוד, עם ניווט ראשון/קודם/הבא/אחרון

התוצאות מוצגות בתצוגת סיכום מעוצבת (כרטיסי משאב עם עוגני קשר לחיצים) או בכרטיסייה של JSON גולמי. כתובת ה-URL של הבקשה החיה מוצגת וניתנת להעתקה. בחירת הטבלה וגודל העמוד נשמרים בין הפעלות ב-`localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

בעת ניווט מקישור NL מסוג "פתח ב-JSON:API", החוקר בוחר מראש את הטבלה וממלא את בורר הצבירה מתוך פרמטרי השאילתה שנוצרו על ידי NL, ואז מריץ את הבקשה אוטומטית. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

הגשת שאלה בשפה טבעית (NL). השירות מתחיל עבודה אסינכרונית ומחזיר מיד `202 Accepted` עם `job_id`. דורש ספק LLM מוגדר תחת מקטע התצורה `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**גוף הבקשה:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

מחזיר `{"job_id": "<id>"}`. חריגה ממגבלת קצב ה-NL לפי תפקיד מחזירה `429` עם כותרת `Retry-After`. (REQ-370)

**קבלת התוצאה:**

- `GET /query/nl/{job_id}` — סקר (poll). מחזיר את מסמך העבודה.
- `GET /query/nl/{job_id}/stream` — SSE. אירוע `branch` אחד לכל יעד יצירה בהשלמתו, ולאחר מכן אירוע `done`. (REQ-357, REQ-358)

שלוש לולאות יצירה (Cypher, GraphQL, SQL) רצות במקביל, כל אחת מאומתת דרך המהדר ומעודנת בעת שגיאה. (REQ-355) ה-prompt מוגבל לסכמה הנראית לתפקיד. (REQ-356) מסמך התוצאה ממפתח כל ענף לפי יעד: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

ענף שממצה את מגבלת האיטרציות שלו מחזיר `query: null`, `result: null`, ומחרוזת `error`. כל שאילתה שנוצרת מורצת תחת הרשאות הצרכן, עם אכיפת ממשל שלב 2 — השירות לעולם אינו עוקף את הממשל. (REQ-359)

#### קיבוץ NL עם פרטי ממד (REQ-1405)

כאשר שאילתת קיבוץ (group-by) של NL מקרינה (projects) גם עמודות מטבלת ממד מצורפת — לדוגמה, "count of inquiries by user with user name and email" — המריץ (runner) גוזר נתיבי-נקודה (dot-paths) לפי שדה (`dim_paths`) מתוך עמודות הממד המוקרנות ב-SELECT. נתיבים אלה ממלאים את הפרמטר `includeNodes=` בכתובות ה-URL שנוצרות בלוחות JSON:API ו-OpenAPI, כך שלוחות אלה מבקשים את אותם שדות ממד מצורף שהענפים של SQL ו-GraphQL פתרו. ללא זאת, `includeNodes=true` היה מחזיר רק את השדות הסקלריים של טבלת הצבירה הבסיסית עצמה. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

בלוח gRPC, ה-`{Type}GroupByRequest` שנוצר נושא את `include_nodes` (בוליאני) ואת `include` (מחרוזת חוזרת של שמות שדות קשר). ה-`{Type}GroupByRow` המוחזר כולל שדה `nodes` מוקלד עם שורות פרטי הממד. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

מחזיר את ה-GraphQL SDL עבור סכמה של תפקיד. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**כותרות:** `X-Role: <role_id>` (נדרש)

**פרמטרי שאילתה:**

- `domain` — מזהי תחום מופרדים בפסיק. כאשר מוגדר, התגובה מסוננת לתחום(ים) הנקוב(ים) ולטבלאות הנגישות מהם.

**תגובה:** GraphQL SDL מסוג `text/plain`.

---

### `GET /data/introspection`

מחזיר JSON של בדיקת סכמה (introspection) של GraphQL, אופציונלית מסונן לפי תחום. [tool-verified: `provisa/api/data/sdl.py:200`]

**כותרות:** `X-Provisa-Role: <role_id>` (נדרש)

**פרמטרי שאילתה:** `domain` — מזהי תחום מופרדים בפסיק.

**תגובה:** תוצאת introspection מסוג `application/json`.

---

### `GET /data/graph-schema`

מחזיר את תצוגת הגרף של סכמת התפקיד: תוויות צמתים (node labels) וסוגי הקשרים שלהם, עבור לקוחות Cypher/גרף. כולל `pk_columns` לכל תווית צומת, כדי שהקוראים יוכלו לקבוע את עמודות המפתח הראשי. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**תגובה:** `application/json` עם `node_labels` (כל אחת נושאת `pk`/`pk_columns`) ו-`relationship_types`.

---

### `GET /data/domains`

מחזיר מזהי תחום הנגישים לתפקיד המבקש. [tool-verified: `provisa/api/data/sdl.py:116`]

**כותרות:** `X-Role: <role_id>` (נדרש)

**תגובה:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

מחזיר את מחרוזת גרסת הסכמה הנוכחית. משלב nonce לפי הפעלה עם מונה בנייה מחדש. לקוחות משתמשים בכך לביטול תוקף מטמוני סכמה לאחר הפעלות מחדש של השרת. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**תגובה:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

מחזיר את קובץ ה-`.proto` שנוצר אוטומטית עבור תפקיד. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**תגובה:** סכמת protobuf מסוג `text/plain`.

כל טבלה רשומה מייצרת `message` proto. קשרים מייצרים שדות message מקוננים. מיפוי טיפוסים: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

זרם Server-Sent Events עבור התראות שינוי בזמן אמת מטבלה. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

מסירת ההתראות משתמשת בספק ניתן להחלפה (pluggable), הנבחר לפי סוג המקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY` (דרך asyncpg), מקורות MongoDB משתמשים ב-Change Streams (`collection.watch()`), ומקורות Kafka משתמשים בקבוצות צרכנים (consumer groups). כל ספק מממש ממשק צפייה (watch) אסינכרוני משותף. סינון RLS ואימות סכמה חלים ללא תלות בספק. (REQ-258) נתמכים גם מקורות WebSocket ו-RSS. (REQ-338, REQ-342)

**כותרת — `X-Provisa-Sink`:** הגדירו ליעד Kafka (למשל `kafka://broker:9092/topic`) כדי להפנות אירועי שינוי לתעלת Kafka במקום לתגובת SSE. השרת מפעיל צרכן תעלה (sink consumer) ומחזיר `202 Accepted` במקום זרם פתוח. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## נקודות קצה REST לניהול

### תצורה (Config)

#### `GET /admin/config`

הורדת קובץ ה-`provisa.yaml` הנוכחי כ-`application/x-yaml` עם כותרת `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

העלאת YAML תצורה מעודכן. השרת כותב גיבוי `.bak`, שומר את הקובץ החדש, וטוען מחדש את כל הסכמות, המקורות ותצוגות ה-Materialized View. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**גוף הבקשה:** תוכן YAML גולמי.

**תגובה:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

בעת כשל טעינה מחדש: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

הורדת **התצורה החיה הנוכחית** — התצורה כפי ש-Provisa הייתה כותבת אותה היום, המשקפת כל טבלה, קשר, דומיין, תפקיד וכלל RLS שנוצרו על ידי מנהל והצטברו מאז ההפעלה. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

הקובץ שעל הדיסק עשוי לפגר אחר המצב החי אם בוצעו שינויים דרך ה-Admin API ללא העלאה עוקבת. נקודת קצה זו סוגרת את הפער: הפלט שלה הוא מה ש-`PUT /admin/config` היה צריך לקבל כדי שהקובץ שעל הדיסק יתאים למצב החי.

מחזיר `application/x-yaml` עם `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

מחזיר את שני הצדדים של הפרש התצורה — `original` (בסיס ההפעלה) ו-`current` (המצב החי) — מנורמלים באופן זהה, כך שההשוואה מציגה רק שינויים אמיתיים, ולא שינויי סדר או סחיפת הערות. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**תגובה:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

יצירת טלאי בפורמט unified diff מבסיס ההפעלה אל התצורה שנשלחה. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

שלחו את ה-YAML המעודכן בגוף הבקשה. התגובה היא קובץ `text/x-patch` (`provisa.config.patch`) ש-`git apply` או `patch` יכולים לצרוך ישירות — שימושי לשמירת שינויי תצורה שנעשו בממשק המשתמש דרך צינור CI/CD.

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

עדכון הגדרות פלטפורמה בזמן ריצה. כל השדות אופציונליים — רק מפתחות שקיימים בגוף הבקשה מתעדכנים. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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

שדות ניתנים לעדכון לפי מקטע:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — כותב לקובץ התצורה ומפעיל טעינה מחדש של הסכמה (REQ-253)
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**תגובה:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### מודלי AI

#### `GET /admin/ai-models`

מחזיר את שיוכי מודלי ה-AI של הארגון הפועל, את רישום מודלי הווקטורים ואת מגבלת הקצב של NL. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

מפתחות API לעולם אינם מוחזרים — `api_keys_set` מדווח רק אם לכל ספק מוגדר מפתח. שינויים נכנסים לתוקף בבקשה הבאה; אין צורך בהפעלה מחדש. (REQ-1349)

#### `PUT /admin/ai-models`

עדכון שיוכי מודלי ה-AI של הארגון, רישום מודלי הווקטורים או מגבלת הקצב של NL. נכנס לתוקף בבקשה הבאה. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

מחזיר את שמות המודלים שהספק מגיש כרגע, עבור בורר המודלים. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

הרשימה נקראת בזמן אמת מממשק list-models של הספק עצמו באמצעות המפתח המוגדר של הארגון — או באמצעות אישור הפריסה כאשר לא הוגדר מפתח ארגוני. מודל שיצא לאחר שגרסת בנייה זו שוחררה ניתן לבחירה באותו יום שבו הספק מגיש אותו.

מחזיר `400` כאשר הספק אינו מפרסם ממשק list-models (במקרה כזה הזינו את שם המודל ישירות) או כאשר לא זמין מפתח. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### מנוע הפדרציה

#### `GET /admin/federation-engine`

מחזיר את בחירת מנוע הפדרציה הנוכחית, את תצורת החיבור שלו ואת רישום המנועים הניתנים לבחירה במלואו. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

המפתח `current` הוא המנוע שרץ ברגע זה; `persisted` הוא מה שנכתב לקובץ התצורה וייטען בהפעלה מחדש הבאה. הם מתפצלים כאשר התצורה שונתה אך השירות טרם הופעל מחדש.

#### `PUT /admin/federation-engine`

שמירת בחירת מנוע פדרציה. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**גוף הבקשה:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

הבחירה נכתבת לתצורת הפלטפורמה. היא נכנסת לתוקף לאחר ההפעלה מחדש הבאה של השירות — המנוע נבחר פעם אחת בעת האתחול.

---

### מדיניות דומיין

#### `POST /admin/domain-policy`

שינוי מדיניות הדומיין של הארגון הפועל (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

זוהי פעולה הרסנית המוגבלת לארגון הפועל. כל מקור, טבלה, דומיין וקשר רשומים נמחקים ונבנים מחדש תחת המדיניות החדשה. השתמשו בה בעת מעבר של ארגון ממרחב-שמות מבוסס-דומיינים למבנה שטוח (או להפך).

**גוף הבקשה:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

‏`use_domains: null` מנקה את העקיפה של הארגון וחוזר להגדרה ברמת הפריסה. `use_domains: false` מחייב `default_domain` (שם הדומיין היחיד שכל הטבלאות נוחתות בו). בניית הקטלוג מחדש היא סינכרונית; התגובה חוזרת ברגע שהסכמות מוכנות.

---


### Observability

#### `GET /admin/traces/recent`

מחזיר עד N spans שהושלמו לאחרונה ממאגר ה-span שבזיכרון. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**פרמטרי שאילתה:** `limit` (ברירת מחדל 50, מקסימום 200)

**תגובה:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

טעינה מחדש (hot-reload) של קטלוג בשם נתון במתאם הפדרציה, דרך ה-REST API שלו. מחבר מחדש את החיבור הפנימי של Provisa ומריץ מחדש את ה-DDL של OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**פרמטרי שאילתה:** `catalog` (ברירת מחדל `"otel"`)

**תגובה:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

הפעלה מחדש של מכולת מנוע הפדרציה (רק לפיתוח בעל צומת יחיד). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**פרמטרי שאילתה:** `container` (ברירת מחדל: משתנה הסביבה `QUERY_ENGINE_CONTAINER`, ואז `"trino"`)

---

### גילוי (Discovery)

#### `POST /admin/discover/relationships`

הפעלת גילוי קשרים. תמיד מריץ בדיקת מפתחות זרים (FK introspection) ממנוע הפדרציה. (REQ-018) מריץ הסקת LLM אם `ANTHROPIC_API_KEY` מוגדר. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**גוף הבקשה:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` חייב להיות אחד מ-`"table"`, `"domain"`, `"cross-domain"`. עבור scope `"table"`, נדרש `table_id` (מספר שלם). עבור scope `"domain"`, נדרש `domain_id`.

**תגובה:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

רשימת מועמדי קשרים ממתינים. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

קבלת מועמד ורישומו כקשר. [tool-verified: `provisa/api/admin/discovery.py:103`]

**גוף הבקשה (אופציונלי):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

דחיית מועמד. [tool-verified: `provisa/api/admin/discovery.py:110`]

**גוף הבקשה:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

מחזיר את ספירת המועמדים שנדחו. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

מחיקת כל המועמדים הדחויים. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### סריקת מקור (Source Crawl)

#### `POST /admin/sources/crawl`

סריקת מקור נתונים לבדיקת סכמה (introspection) ורישום טבלאות. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### חיפוש טבלאות מקור (Source Table Search)

#### `GET /admin/sources/{source_id}/tables/search`

חיפוש טבלאות זמינות (שטרם נרשמו) במקור, לפי שם. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### פרופיל טבלה (Table Profiling)

#### `POST /admin/tables/{table_id}/profile`

הרצת פרופיל עמודות על טבלה רשומה — קרדינליות, מינימום/מקסימום, שיעורי ערכי null. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### תיאורי מקור (Source Descriptions)

#### `POST /admin/source-meta/db-description`

יצירת תיאורים בסיוע LLM עבור טבלאות ועמודות של מקור. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### אחסון אובייקטים (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

מדווח על נפח האחסון של הארגון הפועל אל מול המכסה שהפלטפורמה הקצתה לו, ואם הארגון רשם מאגר משלו. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

כאשר הארגון רשם DSN משלו, המטריאליזציות שלו נכתבות לשם ואינן נספרות עוד אל מול המכסה. ה-DSN עצמו לעולם אינו מוחזר.

#### `PUT /admin/org-storage`

רישום (או ניקוי) של מאגר המטריאליזציה של הארגון. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**גוף הבקשה:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

ה-DSN מאומת מול מנוע הפדרציה לפני שהוא מתקבל — DSN שאינו שמיש נכשל ברישום, ולא שעות מאוחר יותר בעת רענון. הערך מוצפן במנוחה ולעולם אינו מוחזר על ידי GET.

שלחו `storage_url: null` כדי לנקות את המאגר של הארגון ולהחזיר את המטריאליזציות שלו אל מאגר הפלטפורמה (ואל המכסה). זמן הריצה של הארגון נבנה מחדש באותה קריאה, כך שהמאגר החדש נכנס לתוקף מיד. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### הצפנת ארגון (REQ-1574)

#### `GET /admin/org-encryption`

מחזיר את מצב המפתח הנוכחי של הארגון: טביעת אצבע, מזהה ומקור. לעולם אינו מחזיר חומר מפתח. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

כאשר הארגון לא הגדיר מפתח, מוחזר `{"configured": false}`. כל ארגון מתחיל במצב זה ויורש את המפתח של הפריסה.

#### `PUT /admin/org-encryption`

הגדרה או החלפה של מפתח ההצפנה במנוחה של הארגון. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**גוף הבקשה:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

השמיטו את `key_b64` כדי ש-Provisa תייצר מפתח — הדרך הבטוחה ביותר, שכן המפתח לעולם אינו מופיע בלוח ההעתקה או ביומן בקשות. אספקת `key_b64` מביאה מפתח משלכם.

החלפה מוסיפה רשומה פעילה חדשה לטבעת המפתחות ושומרת את הישנה, כך שנתונים שנכתבו תחת המפתח הקודם נותרים קריאים. החלפה אינה הצפנה מחדש. אין נקודת קצה למחיקה: הוצאת המפתח האחרון משימוש הייתה הופכת כל מטען עטוף לבלתי קריא. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

הטבעת החיה נקשרת מחדש באותה קריאה, כך שהכתיבה המוצפנת הבאה משתמשת במפתח החדש מיד.

---

### ייבוא Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

המרת ארכיון פרויקט Hasura v2 או DDN לתצורת Provisa מוצעת מבלי לכתוב דבר. [tool-verified: `provisa/api/admin/import_router.py`]

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

‏`flavor` הוא `"auto"` (מזוהה ממבנה הארכיון), `"hasura_v2"`, או `"ddn"`.

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

דבר אינו נשמר. התצוגה המקדימה אינה נשמרת במטמון בצד השרת; `apply` לוקח את ה-YAML שאתם מספקים, כך שמה שמוחל הוא בדיוק מה שנסקר (ואולי נערך).

#### `POST /admin/import/hasura/apply`

טעינת תצורה שנצפתה מראש אל הארגון הפועל. [tool-verified: `provisa/api/admin/import_router.py`]

**גוף הבקשה:**

```json
{"config_yaml": "<yaml string>"}
```

משתמש באותו מסלול טעינה חמה כמו `PUT /admin/config`. הקטלוג, הסכמות והמאגרים של הארגון נבנים מחדש לפני שהתגובה חוזרת.

---

### חילופי Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

ייצוא המודל הממושל של הארגון כמסמך YAML של Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

המסמך נגזר מהמצב החי בכל בקשה — לעולם אינו נשמר במטמון — ולכן אינו יכול להיות מיושן. טבלאות הופכות לאובייקטי `dataset`, עמודות הופכות לאובייקטי `field`, וקשרים ממופים לאובייקטי `relationship` של Ossie.

מחזיר `text/yaml` עם `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

ניתוח מסמך Ossie בפורמט YAML או JSON והחזרת רישומי טבלאות וקשרים מוצעים. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**גוף הבקשה:** Ossie YAML או JSON גולמי. הפורמט מזוהה אוטומטית.

**תגובה:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

דבר אינו נרשם. השתמשו במסך הסקירה בממשק הניהול כדי לקבל או לצמצם הצעות לפני שמופעלת מוטציה כלשהי.

---


### פעולות (פונקציות ו-Webhooks)

כל נקודות הקצה נמצאות תחת התחילית `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

כל קריאה — מ-GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP‏ `run_sql`, ו-Provisa gRPC — עוברת דרך מבצע (executor) ממוגבל אחד ואחיד, שאוכף `writable_by` וממשל באופן עקבי. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] ראו [docs/integrations.md](integrations.md#commands) לתחביר הקריאה לפי פרוטוקול.

#### `GET /admin/actions`

מחזיר את כל פונקציות ה-DB וה-webhooks המנוטרים. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

כל אובייקט webhook נושא בוליאני `approved`. webhook מאושר ברגע ש-steward מבצע (executes) את בקשת היצירה שלו (REQ-209); webhooks המוצהרים בתצורה מאושרים אוטומטית. webhook לא מאושר נרשם אך אינו נחשף באף משטח (surface). [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

רישום פונקציה מנוטרת (פקודה). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**שדות מרכזיים:**

| שדה | נדרש | תיאור |
| --- | --- | --- |
| `name` | כן | שם פקודה ייחודי |
| `kind` | כן | `"query"` → שדה GraphQL Query; `"mutation"` → שדה Mutation |
| `implKind` | לא | כיצד הפקודה רצה — ראו טבלה למטה (ברירת מחדל `source_procedure`) |
| `binding` | לא | פרטי חיבור ספציפיים ל-`implKind` (אובייקט JSON) |
| `returnSchema` | לא | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — הופך את הפקודה למחזירת קבוצה (set-returning) בכל משטח |
| `arguments` | לא | הגדרות ארגומנטים `[{name, type}]`; סדר מיקומי חשוב עבור קוראי SQL ו-Bolt |
| `visibleTo` | לא | מזהי תפקידים שרשאים לקרוא לפקודה |
| `writableBy` | לא | מזהי תפקידים שמורשים להפעילה כמוטציה |
| `domainId` | לא | תחום למיקום ב-GraphQL ולבקרת גישה |

**ערכי `implKind`:**

| `implKind` | מה מורץ | שדות `binding` |
| --- | --- | --- |
| `source_procedure` | פרוצדורה מאוחסנת במקור רשום (ברירת מחדל) | `sourceId`, `schemaName`, `functionName` |
| `script` | סקריפט בצד השרת | `script` |
| `http` | קריאת HTTP יוצאת | `url`, `method` |
| `grpc` | קריאת gRPC יוצאת לשרת חיצוני | `target`, `method` |
| `python` | קריאה (callable) Python המתארחת ב-Provisa (REQ-885) | `callable` (למשל `"demo.py_functions:random_dataset"`) |

פקודות ההדגמה `random_python_set` (`implKind: python`) ו-`random_grpc_set` (`implKind: grpc`) מדגימות בפועל פקודות מחזירות קבוצה עם `returnSchema`; שתיהן ב-`config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

עדכון פונקציה מנוטרת לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

מחיקת פונקציה מנוטרת לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

רישום webhook מנוטר. (REQ-209) רישום או עדכון webhook מכניס בקשת אישור steward לתור — ה-webhook הופך פעיל בכל המשטחים רק לאחר אישור steward. webhooks המוצהרים בתצורה מאושרים אוטומטית. **שדות גוף הבקשה:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

עדכון webhook מנוטר לפי שם. כל עריכה מאפסת את האישור למצב ממתין, עד לאישור מחדש. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

מחיקת webhook מנוטר לפי שם. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

בדיקת פעולה (פונקציה או webhook) לפי שם. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### תפקידים (Roles)

כל נקודות הקצה נמצאות תחת התחילית `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| שיטה (Method) | נתיב | תיאור |
| --- | --- | --- |
| `GET` | `/admin/roles/` | רשימת כל התפקידים |
| `POST` | `/admin/roles/` | יצירת תפקיד |
| `PUT` | `/admin/roles/{role_id}` | עדכון תפקיד |
| `DELETE` | `/admin/roles/{role_id}` | מחיקת תפקיד |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### משתמשים (Users)

כל נקודות הקצה נמצאות תחת התחילית `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| שיטה (Method) | נתיב | תיאור |
| --- | --- | --- |
| `POST` | `/admin/users/` | יצירת משתמש מקומי |
| `GET` | `/admin/users/` | רשימת משתמשים מקומיים |
| `GET` | `/admin/users/{user_id}` | קבלת משתמש |
| `PUT` | `/admin/users/{user_id}` | עדכון משתמש |
| `PATCH` | `/admin/users/{user_id}/password` | שינוי סיסמה |
| `DELETE` | `/admin/users/{user_id}` | מחיקת משתמש |
| `GET` | `/admin/users/{user_id}/assignments` | רשימת שיוכי תפקידים |
| `POST` | `/admin/users/{user_id}/assignments` | הוספת שיוך תפקיד |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | הסרת שיוך תפקיד |

---

### ארגונים (Organizations)

כל נקודות הקצה נמצאות תחת `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| שיטה (Method) | נתיב | תיאור |
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

כל נקודות הקצה נמצאות תחת `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| שיטה (Method) | נתיב | תיאור |
| --- | --- | --- |
| `POST` | `/admin/invites/` | יצירת הזמנה |
| `GET` | `/admin/invites/` | רשימת הזמנות ממתינות |
| `DELETE` | `/admin/invites/{token}` | ביטול הזמנה |

---

### Admin GraphQL

#### `POST /admin/graphql`

נקודת קצה Strawberry GraphQL עבור כל פעולות הניהול: CRUD למקורות וטבלאות, ניהול קשרים, תצורת תחומים, כללי RLS, בקרת מטמון, מוסכמות שיוֹם (naming conventions), ניהול משימות מתוזמנות, וקומפילציית שאילתות. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**מוטציות מרכזיות:**

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

### התקנה (Setup)

#### `GET /setup/status`

מחזיר את סטטוס ההתקנה בהרצה ראשונה. תמיד לא מאומת. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

השלמת ההתקנה בהרצה ראשונה. [tool-verified: `provisa/api/setup_router.py:142`]

---

## בדיקת תקינות (Health Check)

#### `GET /health` או `HEAD /health`

מחזיר `{"status": "ok"}`. תמיד לא מאומת. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## תגובות שגיאה

| סטטוס | משמעות |
| --- | --- |
| 400 | שאילתה לא תקינה, שגיאת אימות, או שגיאת פרסור SQL |
| 401 | אסימון אימות חסר או לא תקין |
| 403 | יכולות לא מספיקות; הפרת ממשל |
| 404 | תפקיד, משאב, או קובץ תצורה לא נמצאו |
| 422 | כותרת נדרשת חסרה (למשל `X-Role`) |
| 503 | מסד נתונים או מקור לא מחובר; תלות לא זמינה |
| 504 | הבקשה עברה time-out |

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

פורט `8815`. תעבורה עמודתית (columnar) מקורית של Arrow דרך gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

שאילתות וגילוי קטלוג זמינים שניהם באותו חיבור. צינור הממשל המלא (RLS, מיסוך, דגימה) חל על כל שאילתה. (REQ-130, REQ-143)

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

כאשר proxy ה-Flight SQL של Zaychik זמין (פורט 8480), אצוות רשומות (record batches) זורמות מקצה לקצה ללא חומרנות (materialization) מלאה. (REQ-144) חוזר לחומרנות דרך שכבת השאילתה הפדרטיבית אם Zaychik אינו זמין. (REQ-146)

---

## נקודת קצה Protobuf gRPC

פורט `50051` (ניתן לדריסה עם משתנה הסביבה `GRPC_PORT` או תצורת `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

העבירו את התפקיד במפתח המטא-דאטה של gRPC‏ `x-provisa-role`. אם חסר, השרת מבטל (aborts) עם `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

הורידו את ה-proto הספציפי לתפקיד מ-`GET /data/proto/{role_id}`. רק טבלאות ועמודות הנראות לאותו תפקיד מופיעות. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

כל טבלה מייצרת RPC זרימה (streaming) בשם `Query{TypeName}`. RPCs מסוג `Insert{TypeName}` קיימים לצורך סימטריית סכמה אך מבוטלים (abort) עם `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` מופעל עבור גילוי שירות (service discovery) ללא proto מקומפל מראש. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

שרת ה-gRPC מתחיל לפעול רק כאשר ניתן לקמפל proto תקף באתחול. אם בניית הסכמה נכשלת, שרת ה-gRPC אינו מתחיל. (REQ-529)

#### RPCs לצבירה וקיבוץ (REQ-1359, REQ-1361, REQ-1405)

כאשר לטבלה מוגדר `enable_aggregates`, ה-proto שנוצר כולל שני RPCs נוספים לצד `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — מחזיר סקלרי צבירה עבור הטבלה (`count`; `sum`, `avg`, `stddev`, `variance` לכל עמודה מספרית; `min`, `max` לכל עמודה הניתנת להשוואה)
- **`Query{TypeName}GroupBy`** — מחזיר שורה אחת לכל מפתח קבוצה, עם תת-שדות צבירה, ואופציונלית סקלרי טבלת בסיס ושורות ממד מצורף בשדה `nodes`

שני ה-RPCs עוברים דרך אותו צינור צבירה של המהדר, כמו שדות השורש `{field}_aggregate` ו-`{field}_group_by` של GraphQL — אין מימוש צבירה נפרד. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**שדה `funcs` (REQ-1361).** הודעת הבקשה מקבלת שדה מחרוזת חוזרת `funcs`. ערכים תקפים הם `count`, `sum`, `avg`, `stddev`, `variance`, `min`, ו-`max`. כאשר `funcs` מושמט, מתבקשת כל פונקציה שהסכמה חושפת עבור אותה טבלה. כאשר מוגדר, רק הפונקציות הנקובות מופיעות. אם אף אחת מהפונקציות הנקובות אינה חלה על טיפוסי העמודות של הטבלה, השאילתה נופלת חזרה ל-`count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**שדות `include_nodes` ו-`include` (REQ-1405).** בקשות `Query{TypeName}GroupBy` יכולות להגדיר `include_nodes: true` כדי לכלול עמודות סקלריות של טבלת הבסיס בשדה `nodes` של כל שורה. שדה המחרוזת החוזרת `include` נוקב שדות קשר מסוג רבים-לאחד (many-to-one), שהעמודות הסקלריות שלהם מקוננות גם הן בתוך `nodes`. זה תואם להתנהגות `?includeNodes=` / `?include=` של JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## מנהל התקן JDBC

מנהל ההתקן JDBC של Provisa (‏`provisa-jdbc-0.1.0.jar`) חושף את הקטלוג הסמנטי לכלי BI (Tableau, PowerBI, DBeaver). (REQ-126)

**כתובת חיבור:** `jdbc:provisa://host:port` (REQ-131)

תחומים ממופים לסכמות JDBC. (REQ-127) טבלאות משתמשות בכינויים הרשומים שלהן. עמודות משתמשות בכינויים וחושפות תיאורים כ-`REMARKS`. (REQ-128) שיטות מטא-דאטה סטנדרטיות (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) חושפות קשרים סמנטיים כמטא-דאטה של PK/FK.

**תמיכת SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

מנהל ההתקן מבקש הפניית Arrow IPC כברירת מחדל. תוצאות זורמות אצווה-אחר-אצווה דרך `ArrowStreamReader`, מוגבלות לאצוות רשומות (record batch) אחת בזיכרון. (REQ-293)

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

מנויי SSE זמינים ב-`GET /data/subscribe/{table}`. (REQ-219, REQ-258) מסירת ההתראות משתמשת בספק ניתן להחלפה, הנבחר לפי סוג המקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY`, מקורות MongoDB משתמשים ב-Change Streams, ומקורות Kafka משתמשים בקבוצות צרכנים. סינון RLS ואימות סכמה חלים ללא תלות בספק. נתמכים גם מקורות WebSocket ו-RSS דרך אותה נקודת קצה. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## מילון עסקי (Business Glossary) (REQ-1387)

המילון העסקי ממפה שמות שדות פיזיים — כפי שהם קיימים במסדי הנתונים המקוריים — לאוצר מילים אנושי משותף. כל עמודה הרשומה בשכבה הסמנטית מקבלת מונח אוטומטית. אין צורך בהזנה ידנית כדי לאכלס את המילון; אוצרי תוכן (curators) מוסיפים הגדרות, קשרים ומומחים מעל מה שהמערכת גוזרת.

### כיצד מונחים נגזרים

כאשר Provisa רושמת או מעדכנת עמודות של טבלה, `normalize_term` (`provisa/core/glossary.py`) רץ על כל שם עמודה ומייצר ביטוי קנוני. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

הנרמול מיישם חמישה כללים ברצף:

1. פיצול לפי גבולות camelCase ותווי הפרדה (‏`_`, `-`, `.`, `/`, רווח לבן).
2. המרת התוצאה לאותיות קטנות (case-fold).
3. הרחבת טבלת קיצורים קבועה (למשל `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. הסרת **אסימון תחליף (proxy token)** בסוף (‏`identifier`, `code`, `index`, או `reference`) — עמודה ששמה מציין את המפתח או הקוד שלה מצביעה על המושג הבסיסי דרך ערך ממלא-מקום, ולכן המונח צריך להיות המושג עצמו. האסימון האחרון הנותר לעולם אינו מוסר.
5. הכשרת **ביטוי כללי מדי** בעזרת מושג הטבלה. כאשר הביטוי המנורמל המלא הוא מילת תכונה חשופה (‏`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` וכדומה), המונח הופך ל-`<table concept> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. מונח `name` משותף אחד לטבלאות לא קשורות היה ממזג משמעויות שונות; ההכשרה מחברת כל עמודה למושג המכיל אותה במקום זאת. מושג הטבלה הוא השם העסקי של הטבלה, מנורמל עם שם עצם ראשי ביחיד (`order_lines` → `order line`).

עמודות-דמה של סינון מקורי (pseudo-columns, בעלות קידומת `_nf_`, או כל עמודה הנושאת `native_filter_type`) הן מנגנון פרמטרי שאילתה, לא שדות עסקיים, ואינן גוזרות מונחים.

מכיוון ש-`id`, `key`, `pk`, ו-`sk` כולם מתרחבים ל-`identifier` לפני בדיקת התחליף, שלושה שמות עמודות שונים פיזית נוחתים בדיוק על אותו מונח:

| שם פיזי | לאחר נרמול |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

שלושת הראשונים מתמזגים למונח אחד. `transaction amount` שומר על שני האסימונים כי `amount` אינו תחליף. עמודת `id` חשופה — ללא אסימונים קודמים — אינה ניתנת להסרה; היא מנורמלת ל-`identifier` כך שהמונח אינו ריק. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### מחזור חיים

מונחים **נגזרים מחברות בשכבה הסמנטית**, ואינם נוצרים לפי דרישה על ידי משתמשים. מאגר הטבלאות (repository) הוא נתיב הכתיבה היחיד: `sync_table_refs` רץ בתוך כל upsert של קבוצת עמודות, ו-`sweep_refless_terms` רץ לאחר כל נתיב מחיקה. [tool-verified: `provisa/core/repositories/glossary.py`]

**כאשר עמודה נוספת:** Provisa מחפשת את המונח המנורמל לפי שם. אם הוא כבר קיים, העמודה מקבלת הפניה (ref) אליו (ואם המונח היה מוצא משימוש — deprecated — הוא מוחזר לחיים: `deprecated` מוגדר בחזרה ל-`False`). אם עדיין אין מונח, אחד נוצר.

**כאשר עמודה נעלמת** (שינוי סכמה או הסרת טבלה): ההפניה שלה נמחקת והמונח **מיושב (settled)** תחת כלל הסר-או-הוצא-משימוש. מונח מושרש (rooted) ללא הפניות נותרות מוסר לחלוטין — יחד עם הקשתות והקצאות המומחים שלו — אלא אם הסרתו תשאיר מונח מופשט מנותק מכל המונחים המושרשים (ללא נתיב דרך גרף המונחים). במקרה זה, המונח **מוצא משימוש** (מסומן `deprecated=True`) במקום להימחק, כדי שעוגן הגרף של המונח המופשט ישרוד.

מונחים מופשטים לעולם אינם מוסרים אוטומטית; הם קיימים מחוץ למחזור החיים הפיזי ונמחקים רק במפורש דרך ה-API הניהולי.

**החייאה:** אם שמו המנורמל של מונח מוצא משימוש מופיע שוב (עמודה נרשמת מחדש), המונח מבוטל מ-deprecated וההפניות שלו ממשיכות להצטבר.

### נקודות קצה לאוצרות (Curation)

כל נקודות הקצה נמצאות תחת `/admin/glossary`. הן דורשות גישת `org_admin` וארגון מוגדר. כל מוטציה מפעילה פרסום מטא-דאטה. [tool-verified: `provisa/api/admin/glossary_router.py`]

| שיטה (Method) | נתיב | תיאור |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | רשימת מונחים. פרמטרי שאילתה: `q` (חיפוש שם/הגדרה), `include_deprecated` (ברירת מחדל `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | קבלת פרטי מונח: הגדרה, הפניות פיזיות, קשתות מוקלדות, מומחים |
| `POST` | `/admin/glossary/terms` | יצירת מונח מופשט — אוצר מילים של המשתמש ללא הפניות פיזיות |
| `PATCH` | `/admin/glossary/terms/{term_id}` | שינוי שם, הגדרת הגדרה, או החלפת מצב אי-הכללה מייצוא |
| `DELETE` | `/admin/glossary/terms/{term_id}` | מחיקת מונח ללא הפניות פיזיות |
| `POST` | `/admin/glossary/refs/move` | העברת הפניה פיזית אחת למונח אחר (איחוד) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | הוספת קשת קשר מוקלדת בין שני מונחים |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | הסרת קשת (פרמטרי שאילתה: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | תיוג משתמש כמומחה או מחבר עבור מונח |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | הסרת ייעוד מומחה/מחבר של משתמש |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | ניסוח טיוטת הגדרה למונח בודד באמצעות מודל ה-AI של הארגון — מחזיר טקסט בלבד, שום דבר לא נשמר עד לשמירה |
| `POST` | `/admin/glossary/definitions/generate` | יצירה ושמירה של הגדרות עבור כל מונח שאין לו הגדרה — לעולם אינו דורס טקסט שנכתב על ידי אדם |
| `POST` | `/admin/glossary/relationships/generate` | הצעה ושמירה של קשתות מוקלדות על פני כל המילון, באמצעות מודל ה-AI של הארגון |

**גוף הבקשה של `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**גוף הבקשה של `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

ערכי `rel_type` תקפים: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**גוף הבקשה של `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

ערכי `kind` תקפים: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**גוף הבקשה של `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

העברת הפניה מיישבת את המונח המפסיד תחת כלל הסר-או-הוצא-משימוש. השתמשו בזה לאיחוד שני מונחים שהנרמול השאיר נפרדים — לדוגמה, לאחר שמקור השתמש בקיצור לא סטנדרטי שנפל מחוץ לטבלת ההרחבה.

מחיקת מונח מושרש (בעל הפניות פיזיות) מחזירה `400 glossary.invalid`. הסירו או העבירו את כל ההפניות תחילה.

**שדה `export_excluded` ב-`PATCH /admin/glossary/terms/{term_id}`:**

```json
{"export_excluded": true}
```

הגדרת `export_excluded` ל-`true` שוללת את המונח מכל תמונות ייצוא המטא-דאטה, ללא תלות בהפניות הפיזיות שלו או במעמדו המופשט. הגדרה חזרה ל-`false` משיבה את המונח לתמונת המצב בפרסום הבא. נתוני אוצרות (הגדרה, קשתות, מומחים) אינם מושפעים. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### אוצרות בסיוע AI

מודל ה-AI המוגדר של הארגון יכול לנסח טיוטות הגדרות ולהציע קשתות קשר על פני כל המילון בפעולה אחת. שתי הפעולות הקבוצתיות דורשות גישת `org_admin` וארגון מוגדר.

**`POST /admin/glossary/definitions/generate`**

עובר על כל מונח במילון, מדלג על כל מונח שכבר יש לו הגדרה, וקורא למודל ה-AI של הארגון לנסח אחת לכל מונח נותר. הטיוטה נשמרת מיד — בניגוד לנקודת הקצה לניסוח טיוטה למונח בודד (‏`POST /admin/glossary/terms/{term_id}/definition/generate`), אין שלב עורך. הגדרות שנכתבו על ידי אדם לעולם אינן נדרסות: המגן הוא `if summary["definition"]: continue` לפני כל קריאה למודל. הודעת פרסום אחת מכסה את כל האצווה. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

תגובה:

```json
{"generated": 12}
```

`generated` הוא ספירת המונחים שקיבלו הגדרה חדשה. הערך אפס כאשר לכל מונח כבר יש הגדרה.

**`POST /admin/glossary/relationships/generate`**

שולח את רשימת המונחים המלאה למודל ה-AI של הארגון עם prompt המפרט את עשרת סוגי הקשתות המותרים (‏`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) ומבקש רק הצעות בעלות ביטחון גבוה. המודל מחזיר מערך JSON; כל רשומה מאומתת לפני כל כתיבה: שמות מונחים לא ידועים, קשתות עצמיות (self-edges), וסוגי קשתות מחוץ ל-enum הסגור נמחקים בשקט. הצעות תקפות נכתבות (upserted) באופן אידמפוטנטי — הרצה חוזרת של הפעולה אינה משכפלת קשתות. הודעת פרסום אחת מכסה את האצווה. נקודת הקצה מחזירה `{"added": 0}` מיד כאשר במילון פחות משני מונחים שאינם מוצאים משימוש. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

תגובה:

```json
{"added": 5}
```

`added` הוא ספירת הקשתות שנכתבו. קשת שכבר הייתה קיימת עדיין נספרת — פעולת ה-upsert מצליחה, אך נתוני הקשת אינם משתנים.

### כלי MCP‏ `search_terms`

```
search_terms(query, role=None, limit=25)
```

מחפש שמות מונחים והגדרות בהתאמת מחרוזת-משנה (substring) שאינה תלוית רישיות, עד `limit` תוצאות. כל תוצאה היא פרטי המונח המלאים: `name`, `definition`, `is_abstract`, `deprecated`, הפניות פיזיות (עם `source_id`, `schema_name`, `table_name`, `column_name`), קשתות מוקלדות, והקצאות מומחים. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

השתמשו ב-`search_terms` לפני כתיבת SQL כדי למצוא כל שדה פיזי המייצג מושג לפי שם. לדוגמה, חיפוש `"order date"` מחזיר את המונח וכל עמודות `order_dt`, `orderDate`, `ORDER_DATE` על פני כל טבלה רשומה.

### ייצוא מטא-דאטה

גרף מונחי המילון נכלל בכל `MetadataSnapshot` שנבנה על ידי `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

הייצוא מיישם את אותם המסננים כמו שאר תמונת המצב:

- מונח המסומן `export_excluded` נשלל לחלוטין — ללא תלות בהפניות הפיזיות שלו, במעמדו המופשט, או בשאלה האם קטלוג הארגון מוגדר. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- מונח מושרש מתפרסם רק כאשר לפחות אחת מהפניותיו הפיזיות שייכת לעמודה שעוברת גם את מסנן **Data Product** (דגל `data_product` של הטבלה חייב להיות `true`) וגם את מסנן העמודות ה**טכניות** (עמודות המתויגות `technical` נשללות).
- מונח מושרש שכל הפניותיו נשללות על ידי מסננים אלה נשלל יחד איתן.
- מונחים מופשטים מתפרסמים ללא תנאי — הם אוצר מילים של המשתמש, לא כבולים לעמודות פיזיות.
- קשת בין שני מונחים מתפרסמת רק כאשר שני מונחי הקצה מתפרסמים.

כל מתאם ספק מפרסם את גרף המונחים באופן טבעי, לתוך מכולת מילון בבעלות Provisa שהוא יוצר באופן אידמפוטנטי — לעולם לא לתוך מילון קטלוג קיים:

| ספק | מכולה | מונחים | קשרים | הוצאה משימוש |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (glossary API) | מונחי מילון, הגדרה על `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | סמן `[DEPRECATED]` ב-shortDescription |
| Atlan | מילון Provisa לפי qualifiedName יציב | `longDescription` (לעולם לא `userDescription` הערוך אנושית) | אותו מיפוי Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | אספקט `glossaryTermInfo` לכל מונח | KIND_OF → Inherits, PART_OF → Contains (הפוך), RELATED_TO/SYNONYM_OF → related terms | אספקט הוצאה משימוש; שינויי שם עוקבים אחר ירושת URN |
| OpenMetadata | מילון Provisa דרך `/v1/glossaries` | PUT לפי fqn, שינויי שם PATCH-rebind לפי UUID מאוחסן | KIND_OF → היררכיית הורה מקורית, SYNONYM_OF → `synonyms`, אחרים → `relatedTerms` | `entityStatus` |
| Collibra | תחום מסוג-מילון "Provisa Glossary" | נכסי Business Term דרך ה-Import API | סוגי יחס Business Term מקוריים | סטטוס נכס |

הבעלות היא הקישור (binding), לא השם: מזהה הספק של כל מונח שפורסם נלכד לתוך `catalog_bindings` תחת ה-URN של המונח (`provisa://<org>/terms/<name>`), ו-Provisa משנה או מוחקת פריט מילון בצד הספק רק כאשר היא מחזיקה קישור זה (או שהפריט חי במכולה בבעלות Provisa שהיא יצרה). פריט מילון ללא קישור Provisa מקורו במערכת החיצונית ולעולם אינו נגע בו; עדכונים מתבצעים בקריאה-מיזוג (read-merge) כך ששדות שהוספו על ידי steward למונחי Provisa עצמם שורדים; שום דבר לא נמחק כאשר מונח עוזב את תמונת המצב. הקצאות מונח-לנכס (term-to-asset) של steward נותרות בבעלות חיצונית — אף מתאם אינו כותב הקצאות מונח-לנכס (פרסום הקצאות שנוצרו על ידי Provisa הוא המשך עתידי מפורש). ב-Collibra באופן ספציפי, הבטיחות תחת סמנטיקת ה-REPLACE של ה-Import API נשענת על הכלה (containment): המטען (payload) מזכיר רק נכסים בתוך תחום מילון Provisa ומופעי קשר רק בין מונחי Provisa, כך שמילוני steward והקשרים שלהם לעולם אינם נגישים. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
