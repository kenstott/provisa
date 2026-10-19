# מדריך API {: #api-reference }

## סקירה כללית {: #overview }

Provisa חושפת נקודות קצה REST תחת שתי קידומות: `/data` לביצוע שאילתות ולבדיקת Schema (Introspection), ו-`/admin` לניהול תצורה. רוב נקודות הקצה של הנתונים דורשות מזהה תפקיד (Role). פעולות תצורת ניהול משתמשות ב-API של Strawberry GraphQL בכתובת `/admin/graphql`.

---

## אימות (Authentication)

כאשר `auth.provider` מוגדר ב-`provisa.yaml`, כל נקודות הקצה מלבד `/health` ו-`/setup/status` דורשות כותרת `Authorization: Bearer <token>`.

ללא תצורת אימות, השרת פועל במצב פיתוח (dev mode). כל בקשה מטופלת כזהות `anonymous`, המשויכת לכל התפקידים המוגדרים עם גישת Wildcard לתחומים.

**התחברות (`POST /auth/login`)** מסופקת על-ידי ספק האימות הפעיל כאשר `provider: basic` מוגדר. פורמט ההרשאות והתגובה תלויים בספק.

**בדיקת זהות (Introspection):**

```http
GET /auth/me
```

מחזיר את מזהה המשתמש המאומת, אימייל, שם תצוגה, חברויות ארגון, ושיוכי תפקיד. במצב פיתוח מחזיר `dev_mode: true` עם כל מזהי התפקידים רשומים.

```http
GET /auth/provider-type
```

מחזיר `{"provider": "<name>"}` או `{"provider": null}` כאשר האימות אינו מוגדר.

---

## נקודות קצה של נתונים (Data Endpoints)

### `POST /data/graphql`

ביצוע שאילתת או מוטציית GraphQL.

**גוף הבקשה:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

בקשה רצה בתפקיד הפועל שלה: התפקיד של המשתמש המאומת, או, במצב פיתוח (ללא אימות), הכותרת `X-Provisa-Role`, ובהיעדרה `org_admin`. השדה `role` בגוף אינו בוחר תפקיד. כשהוא קיים, הוא חייב להתאים לתפקיד הפועל; ערך אחר מחזיר `400` עם הקוד `data.role_mismatch`, שנוקב בשני התפקידים. אותו כלל חל על `POST /data/sql`, `POST /data/query` ו-`POST /query/nl`.

השדה `extensions` תומך בפרוטוקול Automatic Persisted Query (APQ):

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**כותרות:**

- `X-Provisa-Role` — התפקיד שבו לפעול (מצב פיתוח); עם אימות — בקשת תפקיד שמכובדת רק אם התפקיד מוקצה למשתמש
- `Accept` — פורמט תגובה (ראו משא-ומתן תוכן)
- `Authorization` — `Bearer <token>` כאשר האימות פעיל
- `X-Provisa-Redirect-Format` — סוג MIME לפלט הפניית S3
- `X-Provisa-Redirect-Threshold` — מספר שורות שמעליו מופעלת הפניה
- `X-Provisa-Redirect` — `true` לכפיית הפניה ללא תנאי

**מטמון תגובות:** הצטרפות לפי בקשה באמצעות הנחיית הפעולה `@cached` (`query @cached(ttl: 60) { ... }`, `ttl` אופציונלי); אותה הנחיה חלה על GraphQL הנשלח דרך Arrow Flight. Cypher מצטרף בשורה `// @provisa cache=true` או `// @provisa cache_ttl=N`, ו-gRPC במטא-נתוני הקריאה `x-provisa-cache: true` או `x-provisa-cache-ttl: N`. בלעדיה הבקשה אינה קוראת ממטמון התגובות ואינה כותבת אליו. כותרת התגובה `X-Provisa-Cache` מדווחת `HIT` או `MISS`. ראו [מטמון](configuration.md#per-request-opt-in).

<a id="response-cache-request-headers"></a>

#### כותרות בקשה למטמון התגובות {: #response-cache-request-headers }

`/data/graphql`, `/data/cypher`, ה-Neo4j Query API (`/db/{database}/query/v2`), `/data/rest` ו-`/data/jsonapi` מקבלים את ההצטרפות גם ככותרת בקשה. כל אחת מהצורות שלהלן מתנהגת כמו `@cached(ttl: N)`:

| כותרת | שווה ערך ל |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale` (ללא ערך) | `@cached` (ה-TTL של המפעיל) |
| `Cache-Control: no-cache` / `no-store` | ללא מטמון, כמו אי-שליחת דבר |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

בקשה ללא אחת מהכותרות האלה וללא רמז בטקסט שלה אינה נשמרת במטמון, אף ש-cache רגיל של HTTP שומר כברירת מחדל. הגדרות המטמון של המפעיל עדיין חלות כמו ב-`@cached`: כותרת לעולם אינה הופכת קריאה לטרייה יותר ממה שההגדרות מתירות, ולעולם אינה נדחית בשל בקשת `ttl` ארוך יותר. הנחיות `Cache-Control` אחרות (`no-transform`, `min-fresh`, ...) מתעלמים מהן, כפי ש-RFC 9111 דורש ממטמון.

בקשה יכולה לציין את ההצטרפות יותר מפעם אחת: בטקסט שלה ובכותרת, או בשתי כותרות. אם הן מסכימות, הבקשה נשמרת במטמון פעם אחת כפי שנתבקש. אם הן סותרות (למשל `@cached(ttl: 60)` עם `Cache-Control: max-age=5`, או `x-provisa-cache-ttl: 10` עם `Cache-Control: max-age=20`), הבקשה נכשלת עם `400` והקוד `data.invalid_cache_control`, ומציינת כל צורה שנשלחה. ערך `N` שאינו מספר שלם של שניות נכשל באותו אופן. אף צורה אינה מועדפת על האחרת.

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

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

שאילתות Multi-Root מריצות כל שדה שורש (Root Field) באופן עצמאי. שדות מתחת לסף ההפניה מוחזרים מוטבעים; שדות מעל מוסטים בהפניה. המפתח `redirects` (רבים) ממפה שמות שדות למידע הפניה.

**כותרות מטמון:**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (ב-HIT)

**יכולות נדרשות:** `QUERY_DEVELOPMENT` לכל הבקשות כולל Introspection.

---

### משא-ומתן תוכן (Content Negotiation) {: #content-negotiation }

| כותרת Accept | פורמט |
| --- | --- |
| `application/json` | JSON (ברירת מחדל) |
| `application/x-ndjson` | JSON מופרד-שורות (Newline-delimited) |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### הפניה (Redirect)

תוצאות מעל סף שורות מוגדר (או כאשר `X-Provisa-Redirect: true`) נכתבות ל-S3 ומוחזר כתובת URL חתומה מראש (Presigned).

| פורמט הפניה | נכתב על-ידי | זיכרון |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS פדרטיבי | ללא — הנתונים לעולם אינם עוברים דרך Provisa |
| `application/x-orc` | CTAS פדרטיבי | ללא — הנתונים לעולם אינם עוברים דרך Provisa |
| `application/json` | Provisa | תלוי-זיכרון |
| `application/x-ndjson` | Provisa | תלוי-זיכרון |
| `text/csv` | Provisa | תלוי-זיכרון |
| `application/vnd.apache.arrow.stream` | Provisa | תלוי-זיכרון |

לייצוא אנליטי גדול, השתמשו בהפניית Parquet או ORC. מנוע הפדרציה כותב ישירות ל-S3 במקביל — ללא מעבר נתונים דרך Provisa.

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

ביצוע SQL גולמי דרך Pipeline הממשל בשלב 2.

**גוף הבקשה:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params` (אופציונלי) נושא את הערכים של מצייני המקום `$1…$n` של ההצהרה, לפי הסדר, בנפרד מהטקסט שלה:

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

הערכים נקשרים כשההצהרה רצה — על ידי המנוע או הדרייבר של המקור — ולעולם אינם נכתבים לתוך ההצהרה, ולכן ערך אינו יכול לשנות את מה שההצהרה היא. הם שייכים להצהרה אחת בדיוק, שמצייני המקום שלה ממספרים אותם בדיוק: אצוות הצהרות, מציין מקום בלי ערך, או ערך שאף מציין מקום אינו לוקח, נדחים עם `400` והקוד `data.sql_parameters_do_not_fit` לפני שההצהרה מנוהלת או רצה. הצהרה בלי מצייני מקום נשלחת בלי `params`, כמו קודם.

**יכולות נדרשות:** `QUERY_DEVELOPMENT`.

הפרות ממשל ב-`POST /data/sql` מחזירות HTTP 403.

**תגובה:** אותו פורמט כמו `/data/graphql` (שורות JSON כברירת מחדל, לפי משא-ומתן תוכן דרך `Accept`).

---

### `POST /data/query`

נקודת קצה שאילתה מאוחדת. מקבלת GraphQL, SQL, או Cypher — התחביר מזוהה אוטומטית.

שאילתות Cypher יכולות להישלח גם לנקודת הקצה הייעודית `POST /query/cypher`.

**גוף הבקשה:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

מחזיר `{"data": ...}` עבור GraphQL, `{"columns": [...], "rows": [...]}` עבור SQL ו-Cypher.

---

### `POST /data/sql/explain`

הסבר (Explain) או ניתוח (Analyze) של הצהרת SQL דרך ה-Pipeline המְמוּשְל.

נקודת הקצה עוטפת את ה-SQL **המְמוּשְל** — ההצהרה שרצה בפועל תחת התפקיד של הקורא, לאחר אבטחה ברמת שורה ומיסוך — בתחביר EXPLAIN של הדיאלקט. מה שהתוכנית (Plan) מציגה הוא הגרסה המורשית של השאילתה, לא הקלט הגולמי.

**גוף הבקשה:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

הגדירו `analyze: true` להרצת EXPLAIN ANALYZE. השאילתה מתבצעת והתוכנית נושאת ספירות שורות ותזמונים אמיתיים. לא כל דיאלקט תומך ב-ANALYZE; ראו הטבלה ב-[תוכניות שאילתה וסטטיסטיקות](engines.md#query-plans-and-statistics).

**תגובה:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` כאשר לדיאלקט אין תמיכת EXPLAIN, או כאשר `analyze: true` מתבקש בדיאלקט שאינו תומך בכך (למשל SQLite).

---

### `GET /data/engine/state`

מחזיר את המצב הנוכחי של Shard המנוע ללא הערתו (Waking).

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

---

### `POST /data/engine/prewarm`

הפעלת הערת מנוע ללא הרצת שאילתה.

מחזיר `202 Accepted` מיידית. ההערה רצה ברקע. השתמשו בכך אם אתם רוצים שהמנוע יהיה מוכן לפני הגעת השאילתה הראשונה — לדוגמה, ממתזמן (Scheduler) שמריץ שאילתות כמה דקות מאוחר יותר.

**תגובה:** `202 Accepted`, גוף `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

נקודת קצה REST פשוטה, נוצרת אוטומטית עבור כל טבלה רשומה. מחרוזת השאילתה ממופה לארגומנטים של GraphQL והבקשה מהודרת ומתבצעת דרך אותו Pipeline (RLS, מיסוך, ניתוב) כמו GraphQL.

**פרמטרי שאילתה:**

- `limit` — מספר שורות מרבי (≥ 1)
- `offset` — דילוג שורות (≥ 0)
- `fields` — שמות שדות מופרדים בפסיקים בכתיב של סכמת GraphQL, ב-camelCase תחת מוסכמת השמות ברירת המחדל, למשל `?fields=orderId,amount` (ברירת מחדל: כל השדות הסקלריים). שמות השדות ב-`filter` וב-`orderBy` עוקבים אחר אותו כתיב. JSON:API מקבל במקום זאת את שם העמודה הפיזי (`fields[orders]=order_id`).
- `filter` — מערך JSON של אובייקטי סינון `{"field", "comparator", "value"}`. ערך נקרא לפי סוג העמודה שלו, כך ש-`"100"` בעמודה מספרית מושווה כ-100; ערך שסוג העמודה אינו יכול להכיל מחזיר `400`.
- `orderBy` — מערך JSON של אובייקטי מיון `{"field", "direction"}`
- `params` — הפרמטרים של הטבלה כאובייקט JSON אחד לפי שם פרמטר, למשל `?params={"region":"EU"}`. לטבלה שנרשמה מעל API, או לטבלת שאילתה, יש פרמטר לכל קלט שהמקור שלה מקבל. קריאה שחסר בה פרמטר חובה מחזירה `400` ומציינת את שמו, וכך גם שם שאין לטבלה פרמטר בשמו. כל ערך נבדק מול סוג הפרמטר שלו.

ערכי סינון וערכי פרמטרים נשלחים כמשתנים לצד טקסט ה-GraphQL שנקודת הקצה בונה, ולכן ערך שמכיל מירכאות מושווה כפי שנכתב.

התפקיד המאומת נדרש; בקשות לא מאומתות מחזירות `401`. מפרט OpenAPI עבור נתיבים אלה מוגש בכתובת `GET /data/rest/openapi.json` עם Swagger UI בכתובת `GET /data/rest/docs`.

**מטמון תגובות:** הצטרפות באמצעות כותרת בקשה, `Cache-Control: max-age=N` או `x-provisa-cache: true` (ראו [כותרות בקשה למטמון התגובות](#response-cache-request-headers)). כל תגובה נושאת `X-Provisa-Cache: HIT|MISS`, ובפגיעה גם `X-Provisa-Cache-Age`.

#### חוקר OpenAPI / Swagger UI {: #openapi-swagger-ui-explorer }

עמוד חוקר ה-OpenAPI (`/app/openapi`) מטביע את Swagger UI ב-iframe מבודד (Sandboxed). המפרט מוגבל-תפקיד — רק טבלאות ועמודות הנראות לתפקיד הנוכחי מופיעות — ובאופן אופציונלי מסונן לפי תחום דרך בורר התחום. ה-UI עובר בין ערכות נושא בהירה וכהה אוטומטית.

העמוד טוען את ה-HTML של המפרט דרך `fetch()` ולא דרך `src` ישיר של iframe, כך שהבקשה נושאת את אסימון ה-Bearer של ה-Session ובקשות היחסיות (Relative) של Swagger UI עצמו נפתרות נכון מול אותו מקור (Origin).

כאשר מנווטים מקישור NL "פתח ב-OpenAPI", העמוד מרחיב אוטומטית את נקודת הקצה היעד, ממלא פרמטרי שאילתה מכתובת ה-URL שנוצרה על-ידי NL (למשל `aggregate`, `groupBy`), ולוחץ Execute — תוך שימוש בסקירה (Polling) של DOM כדי להבטיח שכל שלב מסתיים לפני שהבא מופעל.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

נקודת קצה תואמת [JSON:API](https://jsonapi.org) הנוצרת אוטומטית עבור כל טבלה רשומה. אותה אבטחה ברמת שורה, מיסוך, וניתוב כמו GraphQL.

**כותרת `Accept`:** חייבת לכלול `application/vnd.api+json` (סוג המדיה של JSON:API) אחרת הבקשה מחזירה `406`.

**פרמטרי שאילתה:**

- `fields[<type>]` — קבוצות שדות דלילות, למשל `?fields[orders]=amount`. השמות הם שמות העמודות הפיזיים, למשל `fields[orders]=order_id`, כמו ב-`filter[...]`, ב-`sort` ובמפתחות התכונות המוחזרות. REST מקבל במקום זאת את כתיב ה-GraphQL (`fields=orderId`).
- `filter[<col>]` / `filter[<col>][<op>]` — למשל `?filter[region]=US`, `?filter[amount][gt]=100`. ערך נקרא לפי סוג העמודה שלו; ערך שהסוג אינו יכול להכיל מחזיר `400`.
- `params` — הפרמטרים של הטבלה כאובייקט JSON אחד לפי שם פרמטר, למשל `?params={"region":"EU"}`, כמו בנקודת הקצה של REST. פרמטר חובה חסר מחזיר `400` ומציין את שמו; שם לא מוכר או ערך מסוג שגוי מחזיר `400` עם `source.parameter` שערכו `params`. קישורי העימוד שומרים על הפרמטר.
- `sort` — מופרד בפסיק, קידומת `-` לסדר יורד, למשל `?sort=-created_at,amount`
- `page[number]` / `page[size]` — עימוד (Pagination)
- `page[total]` — `true` סופר את השורות שתואמות למסנן ומחזיר `meta.total` ו-`links.last`. בלעדיו התגובה נושאת רק `links.self`, `links.first`, `links.prev` ו-`links.next`, ומסד הנתונים אינו מריץ ספירה. כל ערך שאינו `true` או `false` מחזיר `400`. הקישורים בתגובה שומרים על הפרמטר.
- `aggregate` — פונקציות אגרגציה מופרדות בפסיק להרצה במקום שליפת שורות: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. השתמשו ב-`?aggregate=count,sum` לבקשת תת-קבוצה. תגובות אגרגציה מחזירות `data: null` עם תוצאות ב-`meta.aggregate`.
- `groupBy` — שמות עמודות מופרדים בפסיק; משמש עם `?aggregate=` לקיבוץ תוצאות. רק עמודות ב-Enum `DistinctOnColumn` של הטבלה תקפות; השרת מחזיר `400` עבור כל עמודה שהתפקיד אינו יכול לראות.
- `includeNodes` — `true` לכלול עמודות סקלריות של טבלת הבסיס (וסקלרים של ממדים מצורפים (Joined) הנקובים ב-`include=`) בתוך מערך `nodes` של כל שורת קבוצה. נדרש כאשר שאילתת קיבוץ NL מבקשת גם פרטי ממד.

- `include` — שמות קשרים מופרדים בפסיקים, באיות הפיזי, למשל `?include=order_items,shipment`: כל משאב קשור מוחזר פעם אחת ב-`included`, וכל משאב ראשי מקשר את מה שהוא כולל תחת `relationships.<name>.data` (רשימת מזהי משאבים לקשר אחד-לרבים, מזהה אחד או `null` לקשר אחד-לאחד).

התשובות הן אובייקטי משאב עם `type`/`id`/`attributes`. ה-`id` של משאב הוא המפתח הראשי של הטבלה שלו: הערך של עמודת המפתח או, במפתח מורכב, ערכי העמודות שלו בסדר המפתח מחוברים ב-`:` (שורה 2 של הזמנה 7 בטבלה עם המפתח `(order_id, line)` היא `7:2`). `:` או `\` בתוך ערך מוברחים עם `\`. טבלה בלי מפתח ראשי מוצהר משתמשת בעמודת `id` שלה; טבלה בלי אף אחד מהם מחזירה `400`, וכך גם `include` שמגיע לטבלה כזו. השגיאות עוקבות אחר מבנה אובייקט השגיאה של JSON:API.

**מטמון תגובות:** הצטרפות באמצעות כותרת בקשה, `Cache-Control: max-age=N` או `x-provisa-cache: true` (ראו [כותרות בקשה למטמון התגובות](#response-cache-request-headers)). כל תגובה נושאת `X-Provisa-Cache: HIT|MISS`, ובפגיעה גם `X-Provisa-Cache-Age`. שתי צורות סותרות מחזירות `400` כאובייקט שגיאה של JSON:API.

#### חוקר JSON:API {: #jsonapi-explorer }

עמוד חוקר ה-JSON:API (`/app/jsonapi`) הוא UI דפדפן מעל נקודות קצה אלה. בחרו טבלה מהרשימה המקובצת-לפי-תחום, ואז הגדירו:

- **שדות** — בחרו אילו עמודות לכלול (Sparse Fieldset); השאירו הכל לא מסומן לבקשת כל עמודה
- **קשרים** — בחרו שמות קשרים נגזרי-FK לצירוף (Sideload) דרך `?include=`
- **סינון** — שדה, אופרטור (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`), וערך
- **מיון** — שדה אחד, עולה או יורד
- **אגרגציה** — בחרו עמודות קיבוץ מהרשימה המאומתת בשרת, ואז סמנו פונקציית אגרגציה אחת או יותר; כאשר עמודות קיבוץ נבחרות, תיבת סימון "כלול Nodes" מוסיפה עמודות סקלריות של טבלת הבסיס לכל שורה
- **גודל עמוד** — משאבים לעמוד, עם ניווט ראשון/קודם/הבא/אחרון

התוצאות מוצגות בתצוגת סיכום מעוצבת (כרטיסי משאב עם עוגני קשר הניתנים ללחיצה) או בלשונית JSON גולמית. כתובת ה-URL של הבקשה החיה מוצגת וניתנת להעתקה. בחירת טבלה וגודל עמוד נשמרים בין Sessions ב-`localStorage`.

כאשר מנווטים מקישור NL "פתח ב-JSON:API", החוקר בוחר מראש את הטבלה וזורע את בורר האגרגציה מפרמטרי השאילתה שנוצרו על-ידי NL, ואז מריץ אוטומטית את הבקשה.

---

### `POST /query/nl`

שליחת שאלה בשפה טבעית. השירות מתחיל עבודה אסינכרונית ומחזיר `202 Accepted` עם `job_id` מיידית. דורש ספק LLM מוגדר תחת סעיף התצורה `ai_models`.

**גוף הבקשה:**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

מחזיר `{"job_id": "<id>"}`. חריגה ממגבלת קצב NL לתפקיד מחזירה `429` עם כותרת `Retry-After`.

**אחזור התוצאה:**

- `GET /query/nl/{job_id}` — סקירה (Poll). מחזיר את מסמך העבודה.
- `GET /query/nl/{job_id}/stream` — SSE. אירוע `branch` אחד לכל יעד יצירה (Generation Target) עם השלמתו, ואז אירוע `done`.

שלוש לולאות יצירה (Cypher, GraphQL, SQL) רצות במקביל, כל אחת מאומתת דרך המהדר ומחודדת בעת שגיאה. ה-Prompt מוגבל ל-Schema הנראה של התפקיד. מסמך התוצאה מפתח כל ענף (Branch) לפי יעד:

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

ענף שממצה את מגבלת האיטרציות שלו מחזיר `query: null`, `result: null`, ומחרוזת `error`. כל שאילתה שנוצרת מתבצעת תחת זכויות הצרכן עם ממשל שלב 2 מיושם — השירות לעולם אינו עוקף את הממשל.

#### קיבוץ NL עם פרטי ממד {: #nl-group-by-with-dimension-details }

כאשר שאילתת קיבוץ NL גם מקרינה (Projects) עמודות מטבלת ממד מצורפת — לדוגמה, "ספירת פניות לפי משתמש עם שם משתמש ואימייל" — הרץ (Runner) גוזר נתיבי-נקודה (Dot-Paths) לכל שדה (`dim_paths`) מעמודות הממד המוקרנות ב-SELECT. נתיבים אלה ממלאים את הפרמטר `includeNodes=` בכתובות ה-URL שנוצרו של חלוניות JSON:API ו-OpenAPI, כך שחלוניות אלה מבקשות את אותם שדות ממד מצורף שענפי SQL ו-GraphQL פתרו. ללא זאת, `includeNodes=true` היה מחזיר רק את השדות הסקלריים של טבלת האגרגט הבסיסית עצמה.

בחלונית ה-gRPC, `{Type}GroupByRequest` שנוצר נושא `include_nodes` (Bool) ו-`include` (מחרוזת חוזרת של שמות שדות קשר). `{Type}GroupByRow` המוחזר כולל שדה `nodes` מוקלד עם שורות פרטי הממד.

---

### `GET /data/sdl`

מחזיר את ה-GraphQL SDL עבור Schema של תפקיד.

**כותרות:** `X-Provisa-Role: <role_id>` (נדרש)

**פרמטרי שאילתה:**

- `domain` — מזהי תחום מופרדים בפסיק. כאשר מוגדר, התגובה מסוננת לתחום(ים) הנקוב(ים) והטבלאות הניתנות להגעה מהם.

**תגובה:** `text/plain` GraphQL SDL.

---

### `GET /data/introspection`

מחזיר JSON של Introspection של GraphQL, אופציונלית מסונן לפי תחום.

**כותרות:** `X-Provisa-Role: <role_id>` (נדרש)

**פרמטרי שאילתה:** `domain` — מזהי תחום מופרדים בפסיק.

**תגובה:** תוצאת Introspection מסוג `application/json`.

---

### `GET /data/graph-schema`

מחזיר את תצוגת הגרף של Schema התפקיד: תוויות Node וסוגי הקשרים שלהן, עבור לקוחות Cypher/גרף. כולל `pk_columns` לכל תווית Node כדי שהקוראים יוכלו לקבוע עמודות מפתח ראשי.

**תגובה:** `application/json` עם `node_labels` (כל אחת נושאת `pk`/`pk_columns`) ו-`relationship_types`.

כל סוג קשר נושא גם `junction_table_name` ו-`properties`. בקצה המבוסס על טבלת חיבור (Junction), הראשון נוקב בשם הטבלה האסוציאטיבית שהוא חוצה והשני מפרט את עמודות אותה טבלה, קריאות כ-`r.attr` וניתנות לסינון ב-`WHERE`; בקצה המבוסס על FK, השם הוא `null` ורשימת המאפיינים ריקה, וכך לקוח מבחין בין השניים. טבלת החיבור עצמה לעולם אינה תווית Node — היא הקשת (Edge) עצמה, ולכן אין לה כפתור (Pill) בלקוח גרף ואין לה שורה ב-`node_labels`.

---

### `GET /data/domains`

מחזיר מזהי תחום נגישים לתפקיד המבקש.

**כותרות:** `X-Provisa-Role: <role_id>` (נדרש)

**תגובה:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

מחזיר את מחרוזת גרסת ה-Schema הנוכחית. משלב Nonce לכל אתחול עם מונה בנייה מחדש. לקוחות משתמשים בכך לביטול תוקף מטמוני Schema לאחר הפעלה מחדש של השרת.

**תגובה:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

מחזיר את הקטלוג של התפקיד שבו הבקשה רצה: הטבלאות שמוגשות לתפקיד הזה, כפי שרשימת Arrow Flight נותנת אותן.

**תגובה:** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`. `schema` היא סכמת ה-Arrow של הטבלה בקידוד Arrow IPC: כל עמודה תחת השם ש-SQL קורא אותה בו, עם הסוג הרשום, התיאור ומטא-דאטת המפתחות שלה (`primary_key`, `references`) במטא-דאטה של השדה. פקודות ומדדים אינם מפורטים.

התפקיד הוא זה ששכבת האימות קבעה (`X-Provisa-Role` יכול לבקש תפקיד מוחזק או קבוצה מופרדת בפסיקים). פריסה בלי ספק אימות מפרטת את הקטלוג כולו, כמו רשימת ה-Flight. דרייבר ה-JDBC קורא את הנתיב הזה כשפורט ה-Flight אינו נגיש.

---

### `GET /data/proto/{role_id}`

מחזיר את קובץ `.proto` שנוצר אוטומטית עבור תפקיד.

**תגובה:** Schema פרוטובאף מסוג `text/plain`.

`{role_id}` הוא תפקיד שהקורא מחזיק בו, או קבוצה מופרדת בפסיקים של תפקידים מוחזקים (`analyst,org_admin`), המוגשת כמטא-תפקיד של הקבוצה: איחוד ההרשאות של חבריה. תפקיד שהקורא אינו מחזיק בו נדחה עם `403 auth.role_not_assigned`, ומזהה מטא-תפקיד שצוין ישירות נדחה עם `403 auth.meta_role_named`. `?domains=a,b` מצמצם את ה-proto לתחומים אלה; תחום שהתפקיד אינו מגיע אליו נדחה עם `403 data.domain_not_accessible`. תפקיד ללא משטח נתונים מחזיר `404 data.no_proto_for_role`, תפקיד לא מוכר `404 data.no_role`, ומודל שטרם נבנה `503 data.schema_cache_not_ready`.

כל טבלה בסכמת התפקיד מייצרת `message` של proto. קשרים מייצרים שדות הודעה מקוננים. השירות מפרט קריאות RPC רק לטבלאות שבשורש הסכמה של התפקיד, אותן טבלאות שבטיפוס `Query` של GraphQL שלו; לטבלה מקושרת יש הודעה משלה והיא נגישה דרך הקשר שלה.

הוסיפו `?domains=a,b` כדי להגביל את הקובץ לדומיינים האלה. השירות מפרט אז קריאות RPC לטבלאות של הדומיינים שנבחרו עצמם; טבלאות המקושרות אליהן מופיעות כהודעות.

הקובץ עוקב אחר המודל. הוא נבנה מחדש יחד עם הסכמה של התפקיד בכל שינוי במודל או בתפקיד, גם עבור מטא-תפקיד. מיפוי סוגים: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`.

---

### `GET /data/subscribe/{table}`

זרם Server-Sent Events עבור התראות שינוי בזמן אמת מטבלה.

מסירת ההתראות משתמשת בספק ניתן-להתקנה (Pluggable) הנבחר לפי סוג מקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY` (דרך asyncpg), מקורות MongoDB משתמשים ב-Change Streams (`collection.watch()`), ומקורות Kafka משתמשים בקבוצות צרכנים (Consumer Groups). כל ספק מיישם ממשק Watch אסינכרוני משותף. סינון RLS ואימות Schema חלים ללא תלות בספק. מקורות WebSocket ו-RSS נתמכים גם הם.

**כותרת — `X-Provisa-Sink`:** הגדירו ליעד Kafka (למשל `kafka://broker:9092/topic`) כדי להפנות אירועי שינוי ל-Sink של Kafka במקום לתגובת SSE. השרת מפעיל צרכן Sink ומחזיר `202 Accepted` במקום זרם פתוח.

### `POST /data/sources/{source_id}/signal`

מודיע ל-Provisa שמקור השתנה, בלי שורות: המקבל של אות השינוי `signal`. כל טבלה של המקור שאות השינוי שלה הוא `signal` (משלה, או, כשאין לה, של המקור) נחשבת כנכתבה, כמו בכתיבה דרך Provisa: התגובות השמורות שלה במטמון מושלכות, התצוגות הממומשות מעליה מתיישנות, ונבקשת בניית העותק שלה. בנייה של טבלה מוגנת מעומס ממתינה לחלון שמחוץ לשיא שלה. `?table=schema.table` מאותת טבלה אחת.

התפקיד של הקורא צריך זכות כתיבה, והוא חייב להגיע לתחום של כל טבלה שהוא מאותת. התגובה היא `202 Accepted` עם הטבלאות שאותתו:

```json
{"signalled": ["public.orders"]}
```

מקור שאינו קיים, או `table` שאין לו, עונים `404`; מקור בלי טבלאות על אות השינוי `signal` עונה `409`, כי לא היה מה לרענן.

---

## נקודות קצה REST לניהול (Admin) {: #admin-rest-endpoints }

### תצורה (Config)

#### `GET /admin/config`

הורדת ה-`provisa.yaml` הנוכחי כ-`application/x-yaml` עם כותרת `Content-Disposition: attachment`.

#### `PUT /admin/config`

מעלה YAML קונפיגורציה מתוקן. השרת כותב גיבוי `.bak`, שומר את הקובץ החדש, טוען ממנו מחדש את הגדרות הפריסה, ומחיל אותו על מאגר המודל כאכלוס חד-פעמי מפורש: מה שהקובץ מצהיר מתווסף או מתעדכן דרך מאגר המודל, ודבר אינו מוסר. אתחול מאוחר יותר קורא את הגדרות הקובץ ולעולם אינו מחיל שוב את המודל שלו; אחרי ההפעלה הראשונה רק מאגר המודל הוא בעל המודל.

**גוף הבקשה:** תוכן YAML גולמי.

**תגובה:**

```json
{"success": true, "message": "Config uploaded and applied"}
```

בכשל טעינה מחדש: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

מוריד את המודל כפי שהוא עכשיו, כתוב כקובץ קונפיגורציה: הגדרות קובץ הפריסה עם כל חלק של המודל כפי שנקרא ממאגר המודל — מאגרים, אזורים, מקורות, תחומים, תפקידים, מוצרי נתונים, מונחי מילון, טבלאות והעמודות שלהן, קשרים, מדדים, תגים וההקצאות שלהם, מסנני שורות, פקודות, webhooks, טריגרים מתוזמנים וכללי שמות. כשהוא מוחל על מאגר מודל ריק, הקובץ בונה את אותו מודל. מה שהפריסה מוסיפה לכל מאגר (מקורות מובנים והטבלאות שלהם, תחומי מערכת, `org_admin` ו-`platform_admin`) אינו נכלל.

**פרמטר שאילתה:** `sections` (אופציונלי) — רשימה מופרדת בפסיקים של חלקי מודל, למשל `sources,tables`. רק החלקים האלה נכתבים; שם לא ידוע עונה 400 `settings.export_unknown_section`.

נקודת הקצה פתוחה בכל פריסה לקורא עם הזכות להגדרות הפריסה; אזור קובץ הקונפיגורציה בעמוד הניהול מציע אותה כ**ייצוא מודל**.

מחזיר `application/x-yaml` עם `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

מחזיר את שני צדדי ההשוואה (Diff) של התצורה — `original` (קו הבסיס בהפעלה) ו-`current` (מצב Live) — מנורמלים באופן זהה, כך שההשוואה מציגה רק שינויים אמיתיים, לא סידור מחדש או סחיפת הערות.

**תגובה:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

יצירת Patch מסוג Unified-Diff מקו הבסיס לתצורה שנשלחה.

שלחו את ה-YAML המתוקן כגוף הבקשה. התגובה היא קובץ `text/x-patch` (`provisa.config.patch`) ש-`git apply` או `patch` יכולים לצרוך ישירות — שימושי להזנת שינויי תצורה שמקורם ב-UI דרך Pipeline של CI/CD.

---

### הגדרות (Settings)

#### `GET /admin/settings`

מחזיר את הגדרות הפלטפורמה הנוכחיות כ-JSON.

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

עדכון הגדרות פלטפורמה בזמן ריצה. כל השדות אופציונליים — רק מפתחות הנוכחים בגוף מתעדכנים.

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
- `naming`: `domain_prefix`, `convention` — כותב לקובץ תצורה ומפעיל טעינה מחדש של Schema
- `relationships`: `auto_track_fk` — שולט רק במעקב מפתח זר (FK). קשר המבוסס על טבלת חיבור (Junction) מוצהר בעת רישום הטבלה ולעולם אינו מוסק, כך שהגדרה זו אינה מגיעה אליו.
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**תגובה:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### מודלי AI {: #ai-models }

#### `GET /admin/ai-models`

מחזיר את שיוכי מודל ה-AI של הארגון הפועל, רישום מודלי הווקטור, ומגבלת קצב NL.

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

מפתחות API לעולם אינם מוחזרים חזרה — `api_keys_set` מדווח רק האם לכל ספק יש מפתח מוגדר. שינויים נכנסים לתוקף בבקשה הבאה; אין צורך באתחול מחדש.

#### `PUT /admin/ai-models`

עדכון שיוכי מודל ה-AI של הארגון, רישום מודלי הווקטור, או מגבלת קצב NL. נכנס לתוקף בבקשה הבאה.

#### `GET /admin/ai-models/vendors/{vendor}/models`

מחזיר את שמות המודלים שספק מציע כרגע, עבור בורר המודלים.

הרשימה נקראת חי (Live) מ-API רשימת-המודלים של הספק עצמו, באמצעות המפתח המוגדר של הארגון — או Credential הפריסה כאשר לא הוגדר מפתח ארגוני. מודל שפורסם לאחר בניית גרסה זו ניתן לבחירה באותו יום שבו הספק מציע אותו.

מחזיר `400` כאשר לספק אין API רשימת-מודלים (הזינו את שם המודל ישירות במקרה זה) או כאשר אין מפתח זמין.

---

### מנוע פדרציה {: #federation-engine }

#### `GET /admin/federation-engine`

מחזיר את בחירת מנוע הפדרציה הנוכחית, תצורת החיבור שלה, ואת מרשם המנועים הבחירים המלא.

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

שמירה קבועה של בחירת מנוע פדרציה.

**גוף הבקשה:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

הבחירה נכתבת לתצורת הפלטפורמה. היא נכנסת לתוקף לאחר הפעלה מחדש הבאה של השירות — המנוע נבחר פעם אחת באתחול.

---

### מדיניות תחום (Domain Policy)

#### `POST /admin/domain-policy`

שינוי מדיניות התחום של הארגון הפועל (`use_domains` / `default_domain`).

ההחלפה אינה מוחקת דבר. היא נדחית עם `409` והקוד `settings.domain_policy_catalog_exists` כל עוד לארגון הפעיל יש קטלוג: טבלה, מקור או תחום רשומים שהפריסה לא הוסיפה בעצמה. ה-`params` של הדחייה סופרים כל סוג (`tables`, `sources`, `domains`). הסירו קודם את האובייקטים האלה, כל אחד בפעולה שלו, ואז החליפו. כשקובץ הקונפיגורציה מצהיר על חלק מהקטלוג הזה, הקוד הוא `settings.domain_policy_catalog_in_config`: הטעינה הבאה הייתה משחזרת אותו, ולכן המדיניות נקבעת בקובץ הקונפיגורציה.

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

מחזיר עד N Spans שהושלמו לאחרונה ממאגר ה-Span בזיכרון.

**פרמטרי שאילתה:** `limit` (ברירת מחדל 50, מקסימום 200)

**תגובה:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

טעינה מחדש חמה (Hot-Reload) של קטלוג בעל שם במתאם מנוע הפדרציה דרך ה-API של REST שלו. מחבר מחדש את החיבור הפנימי של Provisa ומריץ מחדש OTel DDL.

**פרמטרי שאילתה:** `catalog` (ברירת מחדל `"otel"`)

**תגובה:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

הפעלה מחדש של מיכל (Container) מנוע הפדרציה (Single-Node dev בלבד).

**פרמטרי שאילתה:** `container` (ברירת מחדל למשתנה סביבה `QUERY_ENGINE_CONTAINER`, ואז `"trino"`)

---

### גילוי (Discovery)

#### `POST /admin/discover/relationships`

הפעלת גילוי קשרים. תמיד מריץ Introspection של FK ממנוע הפדרציה. מריץ הסקת LLM אם `ANTHROPIC_API_KEY` מוגדר.

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

רשימת מועמדי קשר ממתינים.

#### `POST /admin/discover/candidates/{candidate_id}/accept`

אישור מועמד ורישומו כקשר.

**גוף הבקשה (אופציונלי):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

דחיית מועמד.

**גוף הבקשה:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

מחזיר ספירת מועמדים שנדחו.

#### `DELETE /admin/discover/candidates/rejected`

מחיקת כל המועמדים שנדחו.

---

### סריקת מקור (Source Crawl)

#### `POST /admin/sources/crawl`

סריקת מקור נתונים כדי לבצע Introspection ל-Schema שלו ולרשום טבלאות.

---

### חיפוש טבלאות מקור {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

חיפוש טבלאות זמינות (עדיין לא רשומות) במקור לפי שם.

---

### פרופיל טבלה {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

הרצת פרופיל עמודה על טבלה רשומה — קרדינליות, min/max, שיעורי Null.

---

### תיאורי מקור {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

יצירת תיאורים בסיוע LLM עבור טבלאות ועמודות של מקור.

---

### אחסון אובייקטים {: #object-storage }

#### `GET /admin/org-storage`

דיווח על טביעת רגל האחסון של הארגון הפועל מול מכסת הפלטפורמה שלו, והאם הארגון רשם אחסון משלו.

כאשר לארגון רשום DSN משלו, ה-Materializations שלו הולכים לשם ואינם נספרים עוד כנגד המכסה. ה-DSN עצמו לעולם אינו מוחזר.

#### `PUT /admin/org-storage`

רישום (או ניקוי) אחסון Materialization משלו של הארגון.

**גוף הבקשה:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

ה-DSN מאומת מול מנוע הפדרציה לפני קבלתו — DSN לא שמיש נכשל ברישום, לא שעות מאוחר יותר במהלך רענון. הערך מוצפן במנוחה (At Rest) ולעולם אינו מוחזר על-ידי GET.

שלחו `storage_url: null` כדי לנקות את האחסון העצמי של הארגון ולהחזיר את ה-Materializations שלו לאחסון הפלטפורמה (ולמכסה). זמן הריצה (Runtime) של הארגון נבנה מחדש באותה קריאה, כך שהאחסון החדש בתוקף מיידית.

---

### הצפנת ארגון {: #org-encryption }

#### `GET /admin/org-encryption`

מחזיר את מצב המפתח הנוכחי של הארגון: טביעת אצבע (Fingerprint), מזהה, ומקור. לעולם אינו מחזיר חומר מפתח.

כאשר לארגון לא הוגדר מפתח, מחזיר `{"configured": false}`. כל ארגון מתחיל במצב זה ויורש את מפתח הפריסה.

#### `PUT /admin/org-encryption`

הגדרה או סבב (Rotation) של מפתח ההצפנה במנוחה של הארגון.

**גוף הבקשה:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

השמיטו `key_b64` כדי ש-Provisa תייצר מפתח — הנתיב הבטוח ביותר, שכן המפתח לעולם אינו מופיע בלוח (Clipboard) או ביומן בקשה. אספקת `key_b64` מביאה מפתח משלכם.

סבב מוסיף רשומה פעילה חדשה לטבעת המפתחות (Key Ring) ושומר את הישנה, כך שנתונים שנכתבו תחת המפתח הקודם נשארים קריאים. סבב אינו הצפנה מחדש. אין נקודת קצה למחיקה: פרישת המפתח האחרון הייתה הופכת כל מטען עטוף (Wrapped) לבלתי קריא.

טבעת ה-Live נקשרת מחדש באותה קריאה, כך שהכתיבה המוצפנת הבאה משתמשת במפתח החדש מיידית.

---

### ייבוא Hasura / DDN {: #hasura-ddn-import }

#### `POST /admin/import/hasura/preview`

המרת ארכיון פרויקט Hasura v2 או DDN לתצורת Provisa מוצעת ללא כתיבה.

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

טעינת תצורה שנבדקה מראש לארגון הפועל.

**גוף הבקשה:**

```json
{"config_yaml": "<yaml string>"}
```

ייבוא הוא אכלוס חד-פעמי מפורש של מאגר המודל של הארגון הפעיל, אותה החלה כמו `PUT /admin/config`: הוא מוסיף ומעדכן דרך מאגר המודל את מה שהקונפיגורציה המיובאת מצהירה, עם הגנות וגרסאות, ואינו מוחק דבר שאינו מוזכר בה. אין שדה `replace`. קטלוגי המנוע של המקורות המוצהרים, המאגרים של הארגון והסכמות שלו נבנים מחדש לפני שהתגובה חוזרת.

**תגובה:** `{"summary": {...}}` — ספירות של מה שהקונפיגורציה המיובאת מצהירה.

---

### חליפין Apache Ossie {: #apache-ossie-interchange }

#### `GET /admin/ossie`

ייצוא המודל המְמוּשְל של הארגון כמסמך YAML של Apache Ossie (incubating).

המסמך נגזר ממצב Live בכל בקשה — לעולם אינו נשמר במטמון — כך שהוא לעולם אינו יכול להיות לא-עדכני. טבלאות הופכות לאובייקטי `dataset`, עמודות הופכות לאובייקטי `field`, וקשרים ממופים לאובייקטי `relationship` של Ossie.

מחזיר `text/yaml` עם `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

#### `GET /admin/dbt/sources`

מוריד את הקטלוג המנוהל כ-`sources.yml` של dbt, כפי שהוא מוגש לתפקיד אחד.

**פרמטר שאילתה:** `role` — תפקיד שהקורא מחזיק, או כל תפקיד למחזיק `access_config`.

מחזיר `text/yaml` עם `Content-Disposition: attachment; filename=provisa.sources.yml`. תפקיד לא ידוע הוא `404` עם הקוד `dbt_sources.unknown_role`. מה הקובץ מכיל, ראו [אינטגרציות](integrations.md#dbt-sources-file).

#### `POST /admin/ossie/import`

ניתוח מסמך YAML או JSON של Ossie והחזרת הצעות רישום טבלה וקשר.

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

### פעולות (Functions ו-Webhooks) {: #actions-functions-and-webhooks }

כל נקודות הקצה תחת הקידומת `/admin/actions`.

כל הפעלה — מ-GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, ו-Provisa gRPC — עוברת דרך מבצע (Executor) מְמוּשְל יחיד האוכף את שיוך ה-command (`visible_to`), את הזכות `write` למוטציה וממשל באופן אחיד. ראו [docs/integrations.md](integrations.md#invoking-commands-across-protocols) לתחביר הקריאה לכל פרוטוקול.

#### `GET /admin/actions`

מחזיר את כל ה-Functions וה-Webhooks של מסד הנתונים במעקב.

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

כל אובייקט Webhook נושא Boolean בשם `approved`. Webhook מאושר ברגע ש-Steward מבצע את בקשת היצירה שלו; Webhooks המוצהרים בתצורה מאושרים אוטומטית. Webhook לא מאושר רשום אך אינו חשוף בשום משטח.

#### `POST /admin/actions/functions`

רישום Function במעקב (פקודה).

**שדות מפתח:**

| שדה | נדרש | תיאור |
| --- | --- | --- |
| `name` | כן | שם פקודה ייחודי |
| `kind` | כן | `"query"` → שדה Query של GraphQL; `"mutation"` → שדה Mutation |
| `implKind` | לא | כיצד הפקודה רצה — ראו הטבלה למטה (ברירת מחדל `source_procedure`) |
| `binding` | לא | פרטי חיבור ספציפיים ל-`implKind` (אובייקט JSON) |
| `returnSchema` | לא | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — הופך את הפקודה למחזירה-קבוצה (Set-Returning) בכל משטח |
| `arguments` | לא | הגדרות ארגומנט `[{name, type}]`; סדר מיקומי חשוב עבור קוראי SQL ו-Bolt |
| `visibleTo` | לא | מזהי תפקידים שה-command משויך אליהם (ריק: כל תפקיד). `mutation` דורשת בנוסף את הזכות `write` |
| `domainId` | לא | תחום עבור מיקום GraphQL ובקרת גישה |

**ערכי `implKind`:**

| `implKind` | מה רץ | שדות `binding` |
| --- | --- | --- |
| `source_procedure` | פרוצדורה מאוחסנת במקור רשום (ברירת מחדל) | `sourceId`, `schemaName`, `functionName` |
| `script` | סקריפט צד-שרת | `script` |
| `http` | קריאת HTTP יוצאת | `url`, `method` |
| `grpc` | קריאת gRPC יוצאת לשרת חיצוני | `target`, `method` |
| `python` | Callable של Python המתארח על-ידי Provisa | `callable` (למשל `"demo.py_functions:random_dataset"`) |

פקודות ההדגמה `random_python_set` (`implKind: python`) ו-`random_grpc_set` (`implKind: grpc`) מדגימות פקודות מחזירות-קבוצה עם `returnSchema` בפועל; שתיהן ב-`config/provisa-install.yaml`.

#### `PUT /admin/actions/functions/{name}`

עדכון Function במעקב לפי שם.

#### `DELETE /admin/actions/functions/{name}`

מחיקת Function במעקב לפי שם.

#### `POST /admin/actions/webhooks`

רישום Webhook במעקב. רישום או עדכון Webhook מוסיף לתור בקשת אישור Steward — ה-Webhook הופך פעיל בכל המשטחים רק לאחר שה-Steward מאשר אותו. Webhooks המוצהרים בתצורה מאושרים אוטומטית. **שדות גוף הבקשה:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`.

#### `PUT /admin/actions/webhooks/{name}`

עדכון Webhook במעקב לפי שם. כל עריכה מאפסת את האישור למצב ממתין עד לאישור מחדש.

#### `DELETE /admin/actions/webhooks/{name}`

מחיקת Webhook במעקב לפי שם.

#### `POST /admin/actions/test`

בדיקת פעולה (Function או Webhook) לפי שם.

---

### תפקידים (Roles)

כל נקודות הקצה תחת הקידומת `/admin/roles`.

| שיטה | נתיב | תיאור |
| --- | --- | --- |
| `GET` | `/admin/roles/` | רשימת כל התפקידים |
| `POST` | `/admin/roles/` | יצירת תפקיד |
| `PUT` | `/admin/roles/{role_id}` | עדכון תפקיד |
| `DELETE` | `/admin/roles/{role_id}` | מחיקת תפקיד |

---

### משתמשים (Users)

כל נקודות הקצה תחת הקידומת `/admin/users`.

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

כל נקודות הקצה תחת `/admin/orgs`.

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

כל נקודות הקצה תחת `/admin/invites`.

| שיטה | נתיב | תיאור |
| --- | --- | --- |
| `POST` | `/admin/invites/` | יצירת הזמנה |
| `GET` | `/admin/invites/` | רשימת הזמנות ממתינות |
| `DELETE` | `/admin/invites/{token}` | ביטול הזמנה |

---

### Admin GraphQL

#### `POST /admin/graphql`

נקודת קצה Strawberry GraphQL עבור כל פעולות הניהול: CRUD של מקורות וטבלאות, ניהול קשרים, תצורת תחום, כללי RLS, בקרת מטמון, מוסכמות שמות, ניהול משימות מתוזמנות, והידור שאילתה.

להפניה המלאה לסכמה — כל שדה Query, שדה Mutation, וטיפוס קלט/פלט — ראו [הפניית Admin GraphQL API](admin-graphql.md).

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

---

### הגדרה ראשונית (Setup)

#### `GET /setup/status`

מחזיר את מצב ההגדרה של ריצה ראשונה. תמיד לא מאומת.

#### `POST /setup/`

השלמת הגדרת ריצה ראשונה.

---

## בדיקת תקינות (Health Check)

#### `GET /health` או `HEAD /health` {: #get-health-or-head-health }

מחזיר `{"status": "ok"}`. תמיד לא מאומת.

---

## תגובות שגיאה {: #error-responses }

| סטטוס | משמעות |
| --- | --- |
| 400 | שאילתה לא תקינה, שגיאת אימות, או שגיאת ניתוח SQL |
| 401 | אסימון אימות חסר או לא תקין |
| 403 | יכולות לא מספיקות; הפרת ממשל |
| 404 | תפקיד, משאב, או קובץ תצורה לא נמצא |
| 422 | כותרת נדרשת חסרה (למשל `X-Provisa-Role`) |
| 503 | מסד נתונים או מקור לא מחובר; תלות לא זמינה |
| 504 | הבקשה עברה Timeout |

הפרות ממשל ב-`POST /data/sql` מחזירות HTTP 403 עם גוף מובנה:

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

## נקודת קצה Arrow Flight {: #arrow-flight-endpoint }

פורט `8815`. תעבורה עמודתית (Columnar) ילידית של Arrow מעל gRPC.

שאילתות וגילוי קטלוג שניהם זמינים על אותו חיבור. Pipeline הממשל המלא (RLS, מיסוך, Sampling) מיושם על כל שאילתה.

גילוי הקטלוג (`list_flights`, `get_flight_info`, `get_schema` וכרטיס בלי `query`) מפרט את מה שהוענק לתפקיד של הקורא. כשהאימות פעיל, שלוש הקריאות שבלי כרטיס לוקחות את האישורים מכותרת הקריאה `authorization: Bearer` ואת התפקיד מ-`x-provisa-role`, ונדחות בלי אישורים; ראו [אינטגרציות](integrations.md).

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

כאשר Proxy מסוג Zaychik Flight SQL זמין (פורט 8480), אצוות (Batches) של רשומות זורמות מקצה-לקצה ללא Materialization מלא. חוזר ל-Materializing דרך שכבת השאילתה הפדרטיבית כאשר Zaychik אינו זמין.

**אזהרות.** מה שתגובה מדווחת על עצמה עובר בזרם כ-`app_metadata` של אצווה בלי שורות: `{"provisa_warnings": [{"code", "params", "message"}]}`. אזהרות שידועות לפני קריאת השורות מגיעות באצווה בלי שורות לפניהן. אזהרה שידועה רק אחרי קריאת השורות מגיעה באצווה שנייה בלי שורות, האחרונה בזרם: `statement.rows_cut` אומרת שהתגובה נקטעה במגבלת השורות ושורות נוספות תואמות. קראו את `app_metadata` בכל אצווה, כולל האחרונה.

---

## נקודת קצה Protobuf gRPC {: #protobuf-grpc-endpoint }

פורט `50051` (עקיפה עם משתנה סביבה `GRPC_PORT` או תצורת `server.grpc_port`).

העבירו את התפקיד במפתח המטא-דאטה של gRPC `x-provisa-role`. אם חסר, השרת מבטל (Aborts) עם `UNAUTHENTICATED`.

הורידו את ה-Proto הספציפי-תפקיד מ-`GET /data/proto/{role_id}`. רק טבלאות ועמודות הנראות לאותו תפקיד מופיעות.

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

כל טבלה בשורש הסכמה של התפקיד מייצרת RPC זורם `Query{TypeName}`. אלה הטבלאות שהטיפוס `Query` של GraphQL של התפקיד מפרט. לטבלה שהתפקיד מגיע אליה רק דרך קשר אין RPC משלה, כשם שאין לה שדה שורש ב-GraphQL. קריאות `Insert{TypeName}` קיימות לשם סימטריית הסכמה אך נכשלות עם `UNIMPLEMENTED`.

**פרמטרים.** לטבלה עם פרמטרים (טבלה שנרשמה מעל API, או טבלת שאילתה) יש הודעה `{TypeName}Params`, שדה אופציונלי לכל פרמטר, בשמו ללא הקידומת `_nf_`, ו-`Query{TypeName}Request` נושא אותה כ-`params`. כל ערך שנקבע שם מגיע למקור כפרמטר, כשהוא קשור בהוראה; שם שאינו פרמטר של הטבלה נדחה עם `INVALID_ARGUMENT`, וקריאה שחסר בה פרמטר חובה נדחית ומציינת את שמו. ה-proxy של gRPC מעל HTTP מקבל את אותם ערכים באובייקט `params` של הגוף (HTTP 400, קוד `data.invalid_parameter`, לשם לא מוכר או לערך מסוג JSON שגוי).

מתודה שאינה חלק מהשירות של תפקיד הקורא משיבה `UNIMPLEMENTED`.

`grpc_reflection.v1alpha` מופעל לגילוי שירותים ללא proto מקומפל מראש. הרפלקציה מתארת לכל קורא את השירות של התפקיד שבשמו הוא פועל: אותו `.proto` שהתפקיד מוריד מ-`GET /data/proto/{role_id}`. היא מפרטת בשמם את שירות Provisa ואת שירות הרפלקציה, ומתארת את הקובץ של שירות Provisa.

קריאת רפלקציה מזוהה כמו כל קריאת gRPC אחרת. כשהאימות פעיל, התפקיד נגזר מאישורי הקורא. כשהאימות כבוי, ציינו את התפקיד ב-`x-provisa-role`; בפריסה שמשרתת יותר מארגון אחד, ציינו גם את הארגון ב-`x-provisa-org`. קריאה בלעדיהם נדחית עם `UNAUTHENTICATED`. כשהאימות כבוי, הרפלקציה נשארת מושבתת אלא אם הוגדר `server.grpc_allow_unsecured_reflection`.

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

שרת ה-gRPC עולה כאשר המודל שנטען בעת ההפעלה מגדיר שירות gRPC. מכאן כל קריאה מוגשת מהמודל כפי שהוא: טבלה שנרשמה או שונתה בזמן שהשרת פועל מוגשת ומתוארת כבר בקריאה הבאה, ללא הפעלה מחדש.

#### RPCs לאגרגציה וקיבוץ {: #aggregate-and-group-by-rpcs }

כאשר לטבלה מוגדר `enable_aggregates`, ה-Proto שנוצר כולל שני RPCs נוספים לצד `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — מחזיר סקלרים אגרגטיביים עבור הטבלה (`count`; `sum`, `avg`, `stddev`, `variance` לכל עמודה מספרית; `min`, `max` לכל עמודה בת-השוואה)
- **`Query{TypeName}GroupBy`** — מחזיר שורה אחת לכל מפתח קיבוץ עם תת-שדות אגרגטיביים ובאופן אופציונלי, סקלרים של טבלת הבסיס ושורות ממד מצורף בשדה `nodes`

שניהם עוברים דרך אותו Pipeline מהדר-אגרגציה כמו שדות השורש `{field}_aggregate` ו-`{field}_group_by` של GraphQL — ללא מימוש אגרגציה נפרד.

**שדה `funcs`.** הודעת הבקשה מקבלת שדה מחרוזת-חוזרת בשם `funcs`. ערכים תקפים הם `count`, `sum`, `avg`, `stddev`, `variance`, `min`, ו-`max`. כאשר `funcs` מושמט, כל פונקציה ש-Schema חושף עבור אותה טבלה מתבקשת. כאשר מוגדר, רק הפונקציות הנקובות מופיעות. אם אף אחת מהפונקציות הנקובות אינה חלה על סוגי העמודות של הטבלה, השאילתה חוזרת ל-`count`.

**שדות `include_nodes` ו-`include`.** בקשות `Query{TypeName}GroupBy` יכולות להגדיר `include_nodes: true` כדי לכלול עמודות סקלריות של טבלת הבסיס בשדה `nodes` של כל שורה. השדה החוזר `include` נוקב בשמות שדות קשר Many-to-One שהעמודות הסקלריות שלהם מקוננות גם הן בתוך `nodes`. זה תואם את התנהגות `?includeNodes=` / `?include=` של JSON:API.

---

## מנהל התקן JDBC {: #jdbc-driver }

מנהל התקן ה-JDBC של Provisa (`provisa-jdbc-<release>.jar`) חושף את הקטלוג הסמנטי לכלי BI (Tableau, PowerBI, DBeaver).

**כתובת חיבור (Connection URL):** `jdbc:provisa://host:port`

תחומים ממופים ל-Schema-ים של JDBC. טבלאות משתמשות בכינוים (Aliases) הרשומים שלהן. עמודות משתמשות בכינויים וחושפות תיאורים כ-`REMARKS`. שיטות מטא-דאטה סטנדרטיות (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) חושפות קשרים סמנטיים כמטא-דאטה של PK/FK.

**תמיכת SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`.

מנהל ההתקן מבקש הפניית Arrow IPC כברירת מחדל. תוצאות זורמות אצווה-אחר-אצווה (Batch-by-Batch) דרך `ArrowStreamReader`, מוגבל לאצווה אחת של רשומות בזיכרון.

---

## פורמט ארגומנט `orderBy` {: #orderby-argument-format }

הארגומנט `order_by` משתמש באובייקטי `{column: direction}` עם Enum כיוון בעל 6 ערכים:

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

כיוונים נתמכים: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`.

---

## Subscriptions

Subscriptions מסוג SSE זמינים בכתובת `GET /data/subscribe/{table}`. מסירת ההתראות משתמשת בספק ניתן-להתקנה הנבחר לפי סוג מקור: מקורות PostgreSQL משתמשים ב-`LISTEN/NOTIFY`, מקורות MongoDB משתמשים ב-Change Streams, ומקורות Kafka משתמשים בקבוצות צרכנים. סינון RLS ואימות Schema חלים ללא תלות בספק. מקורות WebSocket ו-RSS נתמכים גם הם דרך אותה נקודת קצה.

---

## מילון עסקי {: #business-glossary }

המילון העסקי ממפה שמות שדה פיזיים — כפי שהם קיימים במסדי הנתונים המקור — לאוצר מילים אנושי משותף. כל עמודה הרשומה במודל מקבלת מונח אוטומטית. לא נדרשת הזנה ידנית לאכלוס המילון; אוצרים (Curators) מוסיפים הגדרות, קשרים, ומומחים מעל מה שהמערכת גוזרת.

### כיצד מונחים נגזרים {: #how-terms-are-derived }

כאשר Provisa רושמת או מעדכנת עמודות של טבלה, `normalize_term` (`provisa/core/glossary.py`) רץ על כל שם עמודה ומייצר ביטוי קנוני.

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

השלושה הראשונים מתמזגים למונח אחד. `transaction amount` שומר על שני האסימונים כי `amount` אינו Proxy. עמודת `id` חשופה — ללא אסימונים קודמים — לא ניתנת להסרה; היא מנורמלת ל-`identifier` כך שהמונח אינו ריק.

### מחזור חיים {: #lifecycle }

מונחים **נגזרים מחברות במודל**, לא נוצרים לפי דרישה על-ידי משתמשים. מאגר הטבלה (Repository) הוא נתיב הכתיבה היחיד: `sync_table_refs` רץ בתוך כל Upsert של קבוצת עמודות, ו-`sweep_refless_terms` רץ לאחר כל נתיב מחיקה.

**כאשר עמודה מתווספת:** Provisa מחפשת את המונח המנורמל לפי שם. אם הוא כבר קיים, העמודה מקבלת הפניה (Ref) אליו (ואם המונח היה מוצא משימוש (Deprecated), הוא מוחיה — `deprecated` מוגדר בחזרה ל-`False`). אם לא קיים מונח עדיין, אחד נוצר.

**כאשר עמודה עוזבת** (שינוי Schema או הסרת טבלה): ה-Ref שלה נמחק והמונח **מיושב (Settled)** תחת כלל הסרה-או-הוצאה-משימוש. מונח מושרש (Rooted) ללא Refs נותרים מוסר לחלוטין — יחד עם הקשתות והשיוכי המומחה שלו — אלא אם הסרתו הייתה משאירה מונח מופשט מנותק מכל המונחים המושרשים (ללא נתיב דרך גרף המונחים). במקרה כזה, המונח **מוצא משימוש** (מסומן `deprecated=True`) במקום להימחק, כך שעוגן הגרף של המונח המופשט שורד.

מונחים מופשטים לעולם אינם מוסרים אוטומטית; הם קיימים מחוץ למחזור החיים הפיזי ומוסרים רק במפורש דרך ה-API של המנהל.

**החייאה:** אם השם המנורמל של מונח מוצא-משימוש מופיע שוב (עמודה נרשמת מחדש), המונח מבוטל ו-Refs שלו חוזרים להצטבר.

### נקודות קצה לאצירה (Curation) {: #curation-endpoints }

כל נקודות הקצה תחת `/admin/glossary`. הן דורשות גישת `org_admin` וארגון מוגדר. כל מוטציה מפעילה פרסום מטא-דאטה.

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

ערכי `rel_type` תקפים: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`.

**גוף `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

ערכי `kind` תקפים: `expert`, `author`.

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

הגדרת `export_excluded` ל-`true` שומרת את המונח בחוץ מכל תמונות מצב (Snapshots) של ייצוא מטא-דאטה, ללא תלות ב-Refs הפיזיים או במעמד המופשט שלו. הגדרתו בחזרה ל-`false` משיבה את המונח לתמונת המצב בפרסום הבא. נתוני אצירה (הגדרה, קשתות, מומחים) אינם מושפעים.

### אצירה בסיוע AI {: #ai-assisted-curation }

מודל ה-AI המוגדר של הארגון יכול לנסח הגדרות ולהציע קשתות קשר על-פני כל המילון בפעולה אחת. שתי הפעולות האגרגטיביות דורשות גישת `org_admin` וארגון מוגדר.

**`POST /admin/glossary/definitions/generate`**

עובר על כל מונח במילון, מדלג על כל מונח שכבר יש לו הגדרה, וקורא למודל ה-AI של הארגון לנסח אחת עבור כל מונח נותר. הטיוטה נשמרת מיידית — בניגוד לנקודת הקצה של טיוטה-לכל-מונח (`POST /admin/glossary/terms/{term_id}/definition/generate`), אין שלב עורך. הגדרות שנכתבו על-ידי אדם לעולם אינן נדרסות: השומר הוא `if summary["definition"]: continue` לפני כל קריאה למודל. התראת פרסום אחת מכסה את כל האצווה.

תגובה:

```json
{"generated": 12}
```

`generated` היא ספירת המונחים שקיבלו הגדרה חדשה. היא אפס כאשר לכל מונח כבר יש אחת.

**`POST /admin/glossary/relationships/generate`**

שולח את רשימת המונחים המלאה למודל ה-AI של הארגון עם Prompt המפרט את עשרת סוגי הקשת המותרים (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) ומבקש רק הצעות בטוחות. המודל מגיב עם מערך JSON; כל רשומה מאומתת לפני כל כתיבה: שמות מונח לא ידועים, קשתות-עצמיות (Self-Edges), וסוגי קשת מחוץ ל-Enum הסגור נשמטים בשקט. הצעות תקפות מוכנסות (Upserted) באופן אידמפוטנטי — הרצה חוזרת של הפעולה אינה משכפלת קשתות. התראת פרסום אחת מכסה את האצווה. נקודת הקצה מחזירה `{"added": 0}` מיידית כאשר המילון מכיל פחות משני מונחים לא-מוצאים-משימוש.

תגובה:

```json
{"added": 5}
```

`added` היא ספירת הקשתות שנכתבו. קשת שכבר הייתה קיימת עדיין נספרת — ה-Upsert מצליח, אך נתוני הקשת אינם משתנים.

### כלי MCP `search_terms` {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

מחפש שמות ודיוגי מונחים עם התאמת תת-מחרוזת לא-תלוית-רישיות, עד `limit` תוצאות. כל תוצאה היא פרט המונח המלא: `name`, `definition`, `is_abstract`, `deprecated`, Refs פיזיים (עם `source_id`, `schema_name`, `table_name`, `column_name`), קשתות מוקלדות, ושיוכי מומחה.

השתמשו ב-`search_terms` לפני כתיבת SQL כדי למצוא כל שדה פיזי המייצג מושג לפי שם. לדוגמה, חיפוש `"order date"` מחזיר את המונח וכל עמודות `order_dt`, `orderDate`, `ORDER_DATE` על-פני כל טבלה רשומה.

### ייצוא מטא-דאטה {: #metadata-export }

גרף מונחי המילון נכלל בכל `MetadataSnapshot` שנבנה על-ידי `build_snapshot`.

הייצוא מיישם את אותם מסננים כמו שאר תמונת המצב:

- מונח המסומן `export_excluded` נשמר בחוץ לחלוטין — ללא תלות ב-Refs הפיזיים שלו, במעמד המופשט, או האם קטלוג הארגון מוגדר.
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

הבעלות היא הקישור, לא השם: המזהה של כל ספק לכל מונח מפורסם נלכד תחת `catalog_bindings` תחת ה-URN של המונח (`provisa://<org>/terms/<name>`), ו-Provisa משנה או מוחקת פריט מילון בצד הספק רק כאשר היא מחזיקה קישור זה (או שהפריט חי במכל בבעלות Provisa שהיא יצרה). פריט מילון ללא קישור Provisa מקורו במערכת החיצונית ולעולם אינו נגע בו; עדכונים בקריאה-מיזוג כך ששדות שנוספו על-ידי Steward על מונחי Provisa עצמם שורדים; דבר אינו נמחק כאשר מונח עוזב את תמונת המצב. שיוכי מונח-לנכס של Steward נשארים בבעלות חיצונית — אף מתאם אינו כותב שיוכי מונח-לנכס (פרסום שיוכים שנכתבו על-ידי Provisa הוא המשך עתידי מפורש). ב-Collibra ספציפית, הבטיחות תחת סמנטיקת REPLACE של Import API נשענת על הכלה: המטען מזכיר רק נכסים בתוך תחום מילון Provisa ומופעי יחס רק בין מונחי Provisa, כך שמילוני Steward והיחסים שלהם לעולם אינם ניתנים להגעה.

---

## מוצרי נתונים {: #data-products }

מוצר נתונים מקבץ טבלאות המתפרסמות יחד לצריכה, בבעלות תחום אחד בלבד. שדות עוקבים אחר אוצר המילים של ODPS (Open Data Product Standard) במקום שבו כבר קיים ל-Provisa מקור האמת. ה-UI המנהל חושף מוצרי נתונים תחת **Admin → Data Products**.

### יכולות {: #capabilities }

| יכולת | מעניק |
| --- | --- |
| `data_product_read` | גישת קריאה לשדה השאילתה `data_products` ולעמוד הניהול של מוצרי נתונים. מוזרע כברירת מחדל ל-`org_admin`, `analyst`, `developer`, ו-`modeler`. |
| `data_product_rw` | מוטציות יצירה ומחיקה. מפעיל את בקרות New / Edit / Delete ב-UI. |

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

מחיקת מוצר מנקה את `product_id` מכל טבלת חבר, ומסירה את חברותן. דורש `data_product_rw`.

### Schema של שדות {: #field-schema }

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

שני שדות נוספים קיימים במודל אך אינם חשופים ב-`DataProductType` / `DataProductInput` של Strawberry — הם ספציפיים ל-Snowflake Horizon Catalog:

| שדה | הערות |
| --- | --- |
| `support_contact` | אימייל או כתובת URL; נדרש עבור קובצי רשימת ארגון של Horizon Catalog |
| `publish` | `true` לפרסום רשימות Horizon מיידית; רשימות חדשות ברירת מחדל DRAFT |

### חברות טבלה {: #table-membership }

טבלה מצטרפת למוצר נתונים על-ידי הגדרת שדה ה-`product_id` שלה בטופס עריכת הטבלה. הבורר מוגבל למוצרים ש-`domain_id` שלהם תואם לתחום של הטבלה עצמה — טבלה בתחום `marketing` לעולם אינה מוצעת מוצר בתחום `sales`.

פקודות באותו תחום יכולות להיות משויכות גם הן כחברות.

### מסנן ייצוא מטא-דאטה {: #metadata-export-filter }

`build_snapshot` מיישם `data_products_only=True` עבור כל פרסום קטלוג. טבלאות ללא `product_id` נשמרות בחוץ מתמונת המצב, יחד עם קשתות הקשר, קשתות ה-Lineage, ותגי הממשל שלהן. מקורות ותחומים מתפרסמים תמיד. מונחי מילון מתפרסמים רק כאשר לפחות אחד מה-Refs הפיזיים שלהם שייך לטבלה מיוצאת (חברת-מוצר).

מוצר ללא חברים מיוצאים אינו בונה רשומת תמונת מצב — רשימה ללא חברים הייתה מציגה מצג שווא של המוצר לקטלוג.

### תמיכת מוצר נתונים לפי יעד קטלוג {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` מגיע לכל מתאם, אך רק מתאמים שהפלטפורמה שלהם בעלת מושג מוצר-נתונים ילידי מפרסמים אותו כישות מדרגה ראשונה; השאר מפרסמים את טבלאות החבר (שכבר סוננו כאמור) ללא קיבוץ מוצר.

| יעד | ייצוג מוצר נתונים |
| --- | --- |
| Snowflake Horizon | כל מוצר הופך ל-`SHARE` מעל הכתובות הפיזיות של טבלאות החבר שלו, עטוף ב-`CREATE ORGANIZATION LISTING` פנימי — Data Product ילידי של Horizon Catalog. `publish=true` מפעיל את הרשימה מיידית; אחרת היא נוחתת כ-DRAFT. |
| BigQuery Dataplex | כל מוצר הופך לרשימת Analytics Hub דרך `/v1/dataProducts`. |
| OpenMetadata | כל מוצר הופך לישות `DataProduct` ילידית (`/api/v1/dataProducts`), עם בעלות נגזרת-תחום. |
| DataHub | כל מוצר הופך לישות `dataProduct` ילידית (`urn:li:dataProduct:...`) עם Aspects `dataProductProperties`/בעלות משלה. |
| Collibra | כל מוצר הופך לנכס מסוג קהילת `Data Product`, מקושר לטבלאות החבר שלו דרך יחס `Data Product groups Table`. |
| Atlan | מתפרסם כניחוש typedef מותאם `DataProduct` — ל-Atlan אין שם סוג יציב מתועד למושג זה, כך שהמיפוי הוא במיטב המאמץ. |
| Apache Atlas | מתפרסם כ-typedef מותאם `provisa_data_product` עם יחס `provisa_data_product_members` — ל-Atlas אין סוג ישות מוצר-נתונים ילידי. |
| OpenLineage | לא ישות מדרגה ראשונה — טבלאות חבר נושאות Facet מותאם `provisa_data_product` הנוקב במוצר הבעלים. |
