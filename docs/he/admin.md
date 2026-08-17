# Admin API

ה-Admin API הוא נקודת קצה GraphQL מבוססת Strawberry ב-`POST /admin/graphql` (REQ-533). היא דורשת תפקיד superuser או admin (REQ-125, REQ-060) ונפרדת מנקודת הקצה GraphQL של הנתונים (REQ-533).

## אימות (Authentication)

העבירו את פרטי ההזדהות שלכם בכותרת `Authorization` באמצעות ספק האימות הסטנדרטי של Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

גישת ניהול נשלטת על ידי היכולת (capability) `admin` המוקצית לתפקיד (REQ-060, REQ-042).

### אסימוני גישה אישיים (Personal access tokens)

אסימון גישה אישי מתקבל בכל מקום שבו מתקבל bearer token, כולל נקודת קצה זו. הנפקה וביטול הם שירות עצמי — זהו האישור (credential) של בעל האסימון עצמו, ולכן הוא נמצא בפרופיל המשתמש ב-UI הניהולי ולא תחת דף ניהול, לצד עזיבת ארגון ומחיקת החשבון. מנהל אינו טובע אסימונים מטעם מישהו אחר. (REQ-1263)

| נתיב | אפקט |
| ------- | -------- |
| `POST /auth/tokens` | טביעת אסימון עבור הקורא (caller). גוף (Body): `name`, אופציונלי `role_id`, `scopes`, `expires_in_days` (1–366). התגובה היא המקום היחיד שבו ה-secret אי פעם מופיע |
| `GET /auth/tokens` | האסימונים הפעילים של הקורא בארגון זה — קידומת תצוגה, שם, חותמות זמן של מחזור החיים, וה-hash שמזהה אסימון לצורך ביטול. אף פעם לא אישור עובד |
| `DELETE /auth/tokens/{token_hash}` | ביטול אחד מהאסימונים של הקורא. 404 כאשר הוא אינו שלו או שכבר בוטל |

השמטת `role_id` משאירה את האסימון נפתר לכל תפקיד שבעליו מחזיק בו; ציון תפקיד מצמצם את האסימון מתחת לבעליו. ביטול קורה גם באופן משתמע: הסרת חברות ארגון של משתמש מבטלת את האסימונים שלו עבור אותו ארגון. ראו [Security Model](security.md#_13) עבור האישור עצמו.

## יכולות (Capabilities)

### ניהול קונפיגורציה

הורדת הקונפיגורציה הרצה הנוכחית (REQ-164):

```http
GET /admin/config
```

מחזיר את `config.yaml` המלא כקובץ YAML. העלאת קונפיגורציה חדשה (REQ-164):

```http
PUT /admin/config
```

Provisa מאמתת את ה-YAML, טוענת מחדש קטלוגים, ומחוללת מחדש סכמות (REQ-012, REQ-253). אין צורך באתחול מחדש (restart).

### הגדרות זמן ריצה (Runtime Settings)

קריאה וכתיבה של הגדרות פלטפורמה בזמן ריצה מבלי לערוך את קובץ הקונפיגורציה (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

משטח ההגדרות מכסה הפניית תוצאה גדולה (large-result redirect), דגימת ברירת מחדל וגבול שורות, TTL של מטמון תגובה, מוסכמת שמות, מעקב FK אוטומטי לקשרים, DSN של מאגר המחשה, זיכרון מנוע הפדרציה (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), ומשטח הכוונון המלא של pipeline המעקב (tracing) של OpenTelemetry (REQ-1082). מגבלות מעבר (traversal) של Remote-GraphQL והגדרות שכבת warm-tier/read-cache חשופות אף הן (REQ-1081, REQ-1083).

תנוחת אבטחה (security posture) — `security.mode` (`standard` | `high`) — מוחלת באתחול מחדש (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

שיוכי מודלי AI, רישום מודל ה-embedding/vector, ומגבלת קצב NL — מוחלים באתחול מחדש (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

לשונית ההצפנה בניהול גוזרת את רשימת הספקים שלה באופן חי מרישום ההצפנה; ספקים שאינם זמינים מופיעים אך אינם ניתנים לבחירה (REQ-1091).

`GET`/`HEAD /health` ו-`GET /setup/status` תמיד ללא אימות — הם עוקפים את דרישת `Authorization: Bearer` גם כאשר ספק אימות מוגדר (REQ-539).

### עורך קשרים (Relationship Editor)

רשימת קשרים (REQ-166):

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

יצירת קשר (REQ-019):

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### גילוי קשרים מבוסס AI

הפעלת ניתוח FK מבוסס Claude דרך REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

מחזיר מועמדי FK מדורגים לפי רמת ביטחון. אישור מועמד:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### חקירת סכמה (Schema Introspection)

עיון בטבלאות מפורסמות על פני כל המקורות (REQ-008):

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### בדיקת תלות עמודה (REQ-1484)

לפני שמירת עריכת טבלה ששוב שינוי שם ל-alias של SQL של עמודה או מוחקת עמודה, בררו מה עוד
מפנה אליה:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

שינוי שם ל-alias שובר כל artifact שנכתב כנגד השם החשוף — תצוגות, MVs, ביטויי metric,
פרדיקטים של RLS, חוזי DQ. מחיקת עמודה שוברת את אלה בתוספת ה-artifacts ששומרים את ה-`column_name`
הפיזי: קשרים (relationships), קשרי glossary, שיוכי תגיות. `breaksOn` מציין איזה. דף ה-Tables
מריץ זאת בעת שמירה ומציג את התוצאה כדיאלוג מייעץ (advisory). ראו [Lineage](lineage.md) למה
השאילתה מכסה ומה אינה יכולה.

### ניהול תצוגות (View Management)

רישום תצוגה מומחשת (REQ-133, REQ-135):

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

הפעלת רענון ידני (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### רישום מקור גרף (Graph Source Registration)

מקורות Neo4j ו-SPARQL נרשמים דרך נקודות קצה REST (לא ה-GraphQL admin API) (REQ-295, REQ-297):

**Neo4j:**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

לאחר הרישום, טבלאות מופיעות בסכמת ה-GraphQL וניתנות לשאילתה כמו כל מקור אחר (REQ-016).

## GraphiQL

ה-Admin API מגיע עם GraphiQL ב-`GET /admin/graphql` בדפדפן (REQ-622). השתמשו בו כדי לחקור באופן אינטראקטיבי את סכמת הניהול המלאה.

## תצוגות ניהול בדומיין Ops (REQ-1386)

שמונה תצוגות SQL נזרעות (seeded) לתוך דומיין ה-`ops` המובנה בכל התקנה. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] הן חושפות את יומן ביקורת השאילתות (query audit log) כטבלאות מנוהלות — ניתנות לשאילתה דרך SQL (pgwire), GraphQL, ו-Cypher תחת אותה גישת דומיין, RLS, וכללי מיסוך כמו כל טבלה עסקית.

`org_admin` מיועד כ-steward של דומיין ה-ops בזמן הזריעה (seed time), כך שהדומיין לעולם לא מופיע כפער ממשל (governance gap) ב-`stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| תצוגה | על מה היא עונה |
| --- | --- |
| `usage_ranking` | ספירת שאילתות ומשתמשים ייחודיים פר טבלה רשומה; טבלאות עם אפס פגיעות (zero-hit) עולות כמועמדות להוצאה משימוש |
| `deprecated_usage` | כל גישה לטבלה או עמודה הנושאת את התגית `deprecated` — הצרכנים הפעילים החוסמים הסרה בטוחה |
| `pii_access` | כל גישה לטבלה או עמודה הנושאת את התגית `pii`: מי שאל אותה, תחת איזה תפקיד, על פני איזה משטח |
| `policy_denials` | כל ניסיונות הגישה שהממשל דחה (HTTP 401/403) |
| `surface_mix` | ספירת שאילתות יומית ומשתמשים ייחודיים פר משטח פרוטוקול (SQL, GraphQL, Cypher, gRPC, וכו') |
| `query_health` | ספירת שגיאות יומית וממוצע/מקסימום latency פר משטח |
| `stale_metadata` | טבלאות ועמודות חסרות תיאורים; דומיינים חסרי steward |
| `join_hotspots` | זוגות טבלאות שנשאלים יחד לרוב — מועמדים להמחשה או מטמון |

שתי מגבלות חלות כיום. הגרנולריות היא ברמת הטבלה — יומן הביקורת רושם `table_ids`, לא עמודות בודדות שנגישות. טקסט השאילתה מוצפן (REQ-689) ומוחרג מכל תצוגה כאן; הוא נגיש רק דרך נתיב פענוח ניהולי (admin decrypt) מורשה. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

תפקיד זקוק לגישת דומיין `ops` לפני שתצוגות אלה גלויות. הענקת גישה נעשית באותו אופן כמו הענקת גישה לכל דומיין אחר.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

אותן שאילתות רצות כ-GraphQL או Cypher על פני כל תעבורה מנוהלת — pgwire, Arrow Flight, או Bolt. [inferred from governed-surface design]

## מציג דוחות (Reports viewer) (REQ-1390)

מציג הדוחות נמצא ב-`/admin/reports`. תפקידים ללא היכולת `observability` אינם יכולים להגיע אליו.

הפאנל השמאלי מציג כל טבלה רשומה בדומיין ה-`ops`, ממוינת לפי alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] שמונת תצוגות הניהול הזרועות מופיעות שם אוטומטית. לחיצה על כל דוח טוענת אותו במציג הנתונים המנוהל מימין.

**הוספת דוח מותאם אישית.** כפתור "Add report" פותח דיאלוג. ספקו שם, תיאור אופציונלי, ו-statement מסוג SELECT. השמירה רושמת את התצוגה כטבלה נגזרת מנוהלת (governed derived table) בדומיין ה-`ops` — מקוטלגת, מבוקרת גישה, וניתנת לשאילתה על פני כל משטח לצד התצוגות הזרועות. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**מחיקה.** סמל הפח מופיע רק עבור דוחות מותאמים אישית. תצוגות ניהול זרועות לא ניתנות למחיקה מממשק זה. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## תצוגה מקדימה של טבלה (REQ-1392)

הרחיבו כל שורת טבלה בדף Tables. כפתור **Preview** פותח modal ברוחב 90% עם נתוני הטבלה המנוהלים החיים. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

טבלאות המגובות על ידי APIs עם פרמטרי path נדרשים חוסמות תצוגה מקדימה עד שערכים אלה מסופקים. טופס מוטבע אוסף כל פרמטר נדרש לפני הרצת השאילתה הראשונה; פרמטרי שאילתה אופציונליים מופיעים באותו טופס. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## מציג נתונים מנוהל (Governed data viewer) (REQ-1391)

אותו רכיב מציג מפעיל גם את מודל התצוגה המקדימה וגם את מציג הדוחות. ההתנהגות שלו זהה בשני ההקשרים.

**דפדוף בצד השרת (Server-side paging).** כל עמוד הוא `SELECT *` מנוהל משלו עם `LIMIT 101 OFFSET n`. 100 שורות מופיעות בכל עמוד; השורה ה-101 מסמנת האם קיימות עוד. מערך הנתונים המלא לעולם לא נטען לדפדפן. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**מסננים ומיונים נדחפים (Pushed-down filters and sorts).** לכל כותרת עמודה יש שדה קלט מסנן. מונחי מסנן הופכים לפרדיקטים `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; לחיצות מיון מפיקות סעיפי `ORDER BY`. שניהם הולכים למסד הנתונים — מסנן על טבלה בת מיליארד שורות סורק את המקור, לא את עמוד ה-100 השורות שלפניכם. [tool-verified: `nativeParams.ts:53-70`]

**קיבוץ רב-רמתי (Multi-level group-by).** סמל השכבות (Layers) בכל כותרת עמודה מחליף את אותה עמודה לתוך הקיבוץ. עמודות קיבוץ מובילות את ה-`ORDER BY` כך שחברי קבוצה נוחתים באותו עמוד כמו הכותרת שלהם על פני גבולות עמוד. עמודות מפתח ראשי מתווספות כשובר-שוויון (tiebreaker) יציב. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] שורות כותרת קבוצה ניתנות לקיפול; קיפול מסתיר חברים מבלי להנפיק שאילתה חדשה. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**בחירות מתמידות (Persistent choices).** הגדרות מסנן, מיון, וקיבוץ נשמרות ב-`localStorage` תחת `provisa.grid.table:<domain>.<table>` ומשוחזרות בביקור הבא. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**ייצוא (Export).** הורדת העמוד הנוכחי כ-CSV, או העתקתו ללוח כטקסט מופרד-טאבים. הייצוא מכסה רק את העמוד הגלוי. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
