# Admin API

ה-admin API הוא נקודת קצה GraphQL של Strawberry ב-`POST /admin/graphql` (REQ-533). הוא דורש תפקיד superuser או admin (REQ-125, REQ-060) ונפרד מנקודת הקצה של GraphQL לנתונים (REQ-533).

## אימות

העבירו את האישורים שלכם בכותרת ה-`Authorization` באמצעות ספק האימות הסטנדרטי של Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

גישת admin מנוהלת על ידי היכולת (capability) `admin` המוקצית לתפקיד (REQ-060, REQ-042).

### אסימוני גישה אישיים

אסימון גישה אישי מתקבל בכל מקום שבו מתקבל אסימון bearer, ובכלל זה בנקודת קצה זו. ההנפקה והביטול הם בשירות עצמי — זהו האישור הפרטי של מחזיק האסימון, ולכן הוא שוכן בפרופיל המשתמש בממשק הניהול ולא תחת עמוד מנהל, לצד עזיבת ארגון ומחיקת החשבון. מנהל אינו מנפיק אסימונים בשם מישהו אחר. (REQ-1263)

| נתיב | השפעה |
| ------- | -------- |
| `POST /auth/tokens` | מנפיק אסימון עבור הקורא. גוף: `name`, ואופציונלית `role_id`, `scopes`, `expires_in_days` (1–366). התשובה היא המקום היחיד שבו הסוד מופיע אי-פעם |
| `GET /auth/tokens` | האסימונים הפעילים של הקורא בארגון זה — קידומת תצוגה, שם, חותמות זמן של מחזור החיים, וה-hash המזהה אסימון לצורך ביטול. לעולם לא אישור פעיל |
| `DELETE /auth/tokens/{token_hash}` | מבטל אחד מהאסימונים של הקורא. 404 כאשר אינו שלו או שכבר בוטל |

השמטת `role_id` מותירה את האסימון נפתר לתפקיד שבעליו מחזיק; ציון תפקיד מצמצם את האסימון מתחת לבעליו. הביטול מתרחש גם באופן משתמע: הסרת חברותו של משתמש בארגון מבטלת את האסימונים שלו עבור אותו ארגון. לאישור עצמו ראו [מודל האבטחה](security.md#personal-access-tokens).

## יכולות

### ניהול תצורה

הורדת התצורה הרצה הנוכחית (REQ-164):

```http
GET /admin/config
```

מחזיר את מלוא `config.yaml` כקובץ YAML. העלאת תצורה חדשה (REQ-164):

```http
PUT /admin/config
```

Provisa מאמתת את ה-YAML, טוענת מחדש קטלוגים, ומייצרת מחדש סכמות (REQ-012, REQ-253). אין צורך באתחול מחדש.

### הגדרות זמן ריצה

קריאה וכתיבה של הגדרות פלטפורמה בזמן-ריצה ללא עריכת קובץ התצורה (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

משטח ההגדרות מכסה הפניית תוצאות-גדולות, דגימה וגבול שורות ברירת-מחדל, TTL‏ של מטמון תגובה, מוסכמת שם, מעקב-אוטומטי אחר FK בקשרים, DSN‏ של מאגר מימוש, זיכרון מנוע הפדרציה (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), ומשטח כיוונון צינור מעקב ה-OpenTelemetry המלא (REQ-1082). מגבלות traversal של GraphQL מרוחק והגדרות שכבת-חמימה/מטמון-קריאה חשופות אף הן (REQ-1081, REQ-1083).

תנוחת אבטחה — `security.mode` (`standard` | `high`) — מוחלת בעת אתחול מחדש (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

הקצאות מודל AI, רישום מודל embedding/וקטור, ומגבלת קצב NL — מוחלים בעת אתחול מחדש (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

לשונית ההצפנה ב-admin גוזרת את רשימת הספקים שלה באופן חי מרישום ההצפנה; ספקים לא-זמינים מופיעים אך אינם ניתנים לבחירה (REQ-1091).

`GET`/`HEAD /health` ו-`GET /setup/status` הם תמיד ללא-אימות — הם עוקפים את דרישת `Authorization: Bearer` גם כאשר ספק אימות מוגדר (REQ-539).

### עורך קשרים

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

### גילוי קשרים בעזרת AI

הפעלת ניתוח FK מונע-Claude דרך REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

מחזיר מועמדי FK מדורגים לפי רמת ביטחון. קבלת מועמד:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspection סכמה

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

### ניהול תצוגות

רישום materialized view (REQ-133, REQ-135):

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

### רישום מקור גרף

מקורות Neo4j ו-SPARQL נרשמים דרך נקודות קצה REST (לא ה-admin API של GraphQL) (REQ-295, REQ-297):

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

לאחר הרישום, טבלאות מופיעות בסכמת GraphQL וניתנות לשאילתה כמו כל מקור אחר (REQ-016).

## GraphiQL

ה-admin API מגיע עם GraphiQL ב-`GET /admin/graphql` בדפדפן (REQ-622). השתמשו בו כדי לחקור את סכמת ה-admin המלאה באופן אינטראקטיבי.

## תצוגות ניהול של דומיין ops (REQ-1386)

שמונה תצוגות SQL נזרעות לתוך הדומיין המובנה `ops` בכל התקנה. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] הן חושפות את יומן ביקורת השאילתות כטבלאות מנוהלות — ניתנות לתשאול דרך SQL‏ (pgwire), GraphQL ו-Cypher, תחת אותם כללי גישה לדומיין, RLS ומיסוך כמו כל טבלה עסקית.

`org_admin` מוגדר כאחראי (steward) של דומיין ops בעת הזריעה, כך שהדומיין לעולם אינו מופיע כפער ממשל ב-`stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| תצוגה | על מה היא עונה |
| --- | --- |
| `usage_ranking` | מספר שאילתות ומשתמשים ייחודיים לכל טבלה רשומה; טבלאות ללא פניות עולות כמועמדות להוצאה משימוש |
| `deprecated_usage` | כל גישה לטבלה או לעמודה הנושאת את התג `deprecated` — הצרכנים הפעילים החוסמים הסרה בטוחה |
| `pii_access` | כל גישה לטבלה או לעמודה הנושאת את התג `pii`: מי תישאל, תחת איזה תפקיד, דרך איזו ממשק |
| `policy_denials` | כל ניסיונות הגישה שהממשל דחה (HTTP 401/403) |
| `surface_mix` | מספר שאילתות יומי ומשתמשים ייחודיים לכל ממשק פרוטוקול (SQL, GraphQL, Cypher, gRPC וכו') |
| `query_health` | מספר שגיאות יומי והשהיה ממוצעת/מרבית לכל ממשק |
| `stale_metadata` | טבלאות ועמודות ללא תיאורים; דומיינים ללא אחראי |
| `join_hotspots` | זוגות טבלאות המתושאלים יחד בתדירות הגבוהה ביותר — מועמדים למימוש (materialization) או לשמירה במטמון |

שתי מגבלות חלות כיום. הרזולוציה היא ברמת הטבלה — יומן הביקורת רושם `table_ids`, ולא את העמודות הבודדות שאליהן ניגשו. טקסט השאילתה מוצפן (REQ-689) ומוחרג מכל תצוגה כאן; הוא נגיש רק דרך נתיב הפענוח הניהולי המורשה. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

תפקיד זקוק לגישה לדומיין `ops` כדי שתצוגות אלה יהיו גלויות. העניקו אותה באותו אופן שבו אתם מעניקים גישה לכל דומיין אחר.

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

אותן שאילתות רצות כ-GraphQL או כ-Cypher מעל כל תעבורה מנוהלת — pgwire, ‏Arrow Flight או Bolt. [inferred from governed-surface design]

## מציג הדוחות (REQ-1390)

מציג הדוחות נמצא בכתובת `/admin/reports`. תפקידים ללא היכולת `observability` אינם יכולים להגיע אליו.

הפאנל השמאלי מציג כל טבלה רשומה בדומיין `ops`, ממוינת לפי כינוי. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] שמונה תצוגות הניהול הזרועות מופיעות שם אוטומטית. לחצו על דוח כלשהו כדי לטעון אותו במציג הנתונים המנוהל שמימין.

**הוספת דוח מותאם.** הכפתור "הוסף דוח" פותח דיאלוג. ספקו שם, תיאור אופציונלי, והוראת SELECT. השמירה רושמת את התצוגה כטבלה נגזרת מנוהלת בדומיין `ops` — מקוטלגת, מבוקרת-גישה, וניתנת לתשאול דרך כל ממשק לצד התצוגות הזרועות. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**מחיקה.** סמל פח האשפה מופיע רק עבור דוחות מותאמים. תצוגות ניהול זרועות אינן ניתנות למחיקה מממשק זה. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## תצוגה מקדימה של טבלה (REQ-1392)

הרחיבו שורת טבלה כלשהי בעמוד הטבלאות. הכפתור **תצוגה מקדימה** פותח חלון מודאלי ברוחב 90% עם הנתונים המנוהלים החיים של הטבלה. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

טבלאות הנשענות על ממשקי API עם פרמטרי נתיב נדרשים חוסמות את התצוגה המקדימה עד שהערכים הללו יסופקו. טופס מוטבע אוסף כל פרמטר נדרש לפני שהשאילתה הראשונה רצה; פרמטרי שאילתה אופציונליים מופיעים באותו טופס. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## מציג הנתונים המנוהל (REQ-1391)

אותו רכיב מציג מפעיל את חלון התצוגה המקדימה ואת מציג הדוחות. התנהגותו זהה בשני ההקשרים.

**דפדוף בצד השרת.** כל עמוד הוא `SELECT *` מנוהל משל עצמו עם `LIMIT 101 OFFSET n`. מופיעות 100 שורות לעמוד; השורה ה-101 מסמנת אם קיימות נוספות. מערך הנתונים המלא לעולם אינו נטען לדפדפן. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**סינון ומיון הנדחפים למקור.** לכל כותרת עמודה יש שדה סינון. מונחי סינון הופכים לפרדיקטים `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; לחיצות מיון מפיקות פסוקיות `ORDER BY`. שניהם נשלחים למסד הנתונים — סינון על טבלה בת מיליארד שורות סורק את המקור, ולא את 100 השורות שלפניכם. [tool-verified: `nativeParams.ts:53-70`]

**קיבוץ רב-רמות.** סמל השכבות בכל כותרת עמודה מכניס את אותה עמודה לקיבוץ. עמודות הקיבוץ מובילות את ה-`ORDER BY`, כך שחברי קבוצה נוחתים באותו עמוד ככותרת שלהם גם מעבר לגבולות עמודים. עמודות מפתח ראשי מצורפות בסוף כשובר-שוויון יציב. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] שורות כותרת קבוצה ניתנות לכיווץ; כיווץ מסתיר את החברים בלי להוציא שאילתה חדשה. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**בחירות נשמרות.** הגדרות סינון, מיון וקיבוץ נשמרות ב-`localStorage` תחת `provisa.grid.table:<domain>.<table>` ומשוחזרות בביקור הבא. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**ייצוא.** הורידו את העמוד הנוכחי כ-CSV, או העתיקו אותו ללוח כטקסט מופרד בטאבים. הייצוא מכסה את העמוד הנראה בלבד. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
