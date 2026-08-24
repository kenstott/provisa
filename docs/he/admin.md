# Admin API

ה-admin API הוא נקודת קצה של Strawberry GraphQL בכתובת `POST /admin/graphql` ‏(REQ-533). היא מחייבת תפקיד superuser או admin ‏(REQ-125, REQ-060) והיא נפרדת מנקודת הקצה של GraphQL לנתונים (REQ-533).

## אימות

העבירו את אישורי הגישה שלכם בכותרת `Authorization` באמצעות ספק האימות הרגיל של Provisa ‏(REQ-120):

```yaml
Authorization: Bearer <token>
```

גישת admin ממושלת על ידי היכולת `admin` המוקצית לתפקיד (REQ-060, REQ-042).

### אסימוני גישה אישיים

אסימון גישה אישי מתקבל בכל מקום שבו מתקבל אסימון bearer, כולל נקודת קצה זו. הנפקתו וביטולו הם שירות עצמי — זהו אישור הגישה של מחזיק האסימון עצמו, ולכן הוא שוכן בפרופיל המשתמש בממשק הניהול ולא תחת עמוד admin, לצד עזיבת ארגון ומחיקת החשבון. מנהל אינו מטביע אסימונים בשם מישהו אחר. (REQ-1263)

| נתיב | השפעה |
| ------- | -------- |
| `POST /auth/tokens` | הטבעת אסימון עבור הקורא. גוף: `name`, ואופציונלית `role_id`, `scopes`, `expires_in_days` ‏(1–366). התגובה היא המקום היחיד שבו הסוד מופיע אי פעם |
| `GET /auth/tokens` | האסימונים הפעילים של הקורא בארגון זה — קידומת התצוגה, השם, חותמות זמן של מחזור החיים, וה-hash המזהה אסימון לצורך ביטול. לעולם לא אישור גישה עובד |
| `DELETE /auth/tokens/{token_hash}` | ביטול אחד מאסימוני הקורא. ‏404 כשהוא אינו שלו או כבר בוטל |

השמטת `role_id` משאירה את האסימון מתפענח לכל תפקיד שבעליו מחזיק; נקיבה בשם אחד מצמצמת את האסימון מתחת לבעליו. ביטול קורה גם במשתמע: הסרת חברות ארגון של משתמש מבטלת את אסימוניו עבור אותו ארגון. ראו [מודל אבטחה](security.md#_13) עבור אישור הגישה עצמו.

## יכולות

### ניהול תצורה

הורידו את התצורה הרצה הנוכחית (REQ-164):

```http
GET /admin/config
```

מחזיר את `config.yaml` המלא כקובץ YAML. העלו תצורה חדשה (REQ-164):

```http
PUT /admin/config
```

‏Provisa מאמתת את ה-YAML, טוענת מחדש קטלוגים ומייצרת מחדש סכמות (REQ-012, REQ-253). אין צורך באתחול מחדש.

### הגדרות זמן ריצה

קראו וכתבו הגדרות פלטפורמה בזמן ריצה מבלי לערוך את קובץ התצורה (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

משטח ההגדרות מכסה הפניית תוצאות גדולות, דגימה ומגבלת שורות כברירת מחדל, ‏TTL של מטמון תגובות, מוסכמת שמות, מעקב אוטומטי אחר FK בקשרים, ‏DSN של מאגר המטריאליזציה, זיכרון מנוע הפדרציה (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), ואת מלוא משטח הכוונון של צינור המעקב של OpenTelemetry ‏(REQ-1082). גם מגבלות מעבר של GraphQL מרוחק והגדרות שכבה חמה / מטמון קריאה חשופות (REQ-1081, REQ-1083).

תנוחת אבטחה — `security.mode` (`standard` | `high`) — מיושמת באתחול מחדש (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

הקצאות מודלי AI, מרשם מודלי ה-embedding/וקטור, ומגבלת קצב ה-NL — נכנסות לתוקף בבקשה הבאה, ללא צורך באתחול מחדש (REQ-1349): [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

לשונית ההצפנה בממשק הניהול גוזרת את רשימת הספקים שלה חיה ממרשם ההצפנה; ספקים שאינם זמינים מופיעים אך אינם ניתנים לבחירה (REQ-1091).

‏`GET`/`HEAD /health` ו-`GET /setup/status` הם תמיד לא-מאומתים — הם עוקפים את דרישת `Authorization: Bearer` גם כשמוגדר ספק אימות (REQ-539).

### מנוע פדרציה

קראו או שנו באיזה מנוע הפריסה משתמשת (REQ-916):

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

‏`GET` מחזיר את מפתח המנוע הפעיל ואת שדות התצורה שהוא זקוק להם. `PUT` מקבל גוף עם `engine` (המפתח) וכל שדה ספציפי למנוע; הבחירה נשמרת לתצורת הפלטפורמה ונכבלת באתחול השירות הבא. [tool-verified: `provisa/api/admin/settings_router.py:730-829`]

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

הפעילו ניתוח FK מונע-Claude דרך REST ‏(REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

מחזיר מועמדי FK מדורגים לפי ביטחון. קבלת מועמד:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspection של סכמה

עיינו בטבלאות מפורסמות בכל המקורות (REQ-008):

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

### בדיקת תלויות עמודה (REQ-1484)

לפני שמירת עריכת טבלה המשנה את שם הכינוי ב-SQL של עמודה או מוחקת עמודה, שאלו מה עוד
מפנה אליה:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

שינוי שם כינוי שובר כל פריט שנכתב מול השם החשוף — תצוגות, ‏MVs, ביטויי מדדים,
פרדיקטים של RLS, חוזי DQ. מחיקת עמודה שוברת את אלה ובנוסף את הפריטים ששומרים את
‏`column_name` הפיזי: קשרים, כבילות מילון, הקצאות תגיות. `breaksOn` אומר איזה מהם. עמוד הטבלאות
מריץ זאת בשמירה ומציג את התוצאה כדיאלוג יידוע. ראו
[Lineage](lineage.md) לגבי מה השאילתה מכסה ומה אינה יכולה.

### ניהול תצוגות

רישום תצוגה ממוטריאלת (REQ-133, REQ-135):

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

### רישום מקורות גרף

מקורות Neo4j ו-SPARQL נרשמים דרך נקודות קצה REST ‏(לא דרך ה-admin API של GraphQL) ‏(REQ-295, REQ-297):

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

### ייבוא Hasura / DDN ‏(REQ-1483)

המירו פרויקט Hasura v2 או Hasura DDN קיים לתצורת Provisa דרך ממשק הניהול או ה-API, מבלי ששום דבר ינחת עד שתאשרו זאת.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**תצוגה מקדימה** ממירה את הארכיון שהועלה ומחזירה את ה-`config_yaml` המוצע, רשימת אזהרות, וסיכום של מה שנמצא (ספירות של מקורות, דומיינים, טבלאות, עמודות, תפקידים, קשרים ו-RLS). שום דבר אינו נכתב למסד הנתונים של הדייר. גוף הבקשה:

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

‏`flavor` הוא `"auto"` ‏(מזוהה ממבנה הארכיון), `"hasura_v2"`, או `"ddn"`.

**יישום** לוקח את ה-YAML שסקרתם (ואולי ערכתם) וטוען אותו לארגון הפועל — אותו נתיב טעינה-חמה כמו `PUT /admin/config`. גוף הבקשה: `{"config_yaml": "<yaml string>"}`.

התצוגה המקדימה לעולם אינה שומרת את ה-YAML הממיר במטמון בצד השרת; היישום לוקח את ה-YAML שאתם מספקים, ולכן מה שמיושם הוא בדיוק מה שנסקר. [tool-verified: `provisa/api/admin/import_router.py`]

### חילופין עם Apache Ossie ‏(REQ-1316, REQ-1321)

‏Provisa פועלת הדדית עם Apache Ossie ‏(incubating) כגבול ייבוא/ייצוא.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**ייצוא** ‏(`GET /admin/ossie`) גוזר את מסמך ה-YAML של Ossie מן המודל הממושל החי בכל בקשה — הוא לעולם אינו נשמר במטמון, ולכן אינו יכול להתיישן. התגובה היא `text/yaml` עם כותרת `Content-Disposition: attachment`. טבלאות הופכות לאובייקטי `dataset`, עמודות הופכות לאובייקטי `field`, וקשרים ממופים לאובייקטי `relationship` של Ossie. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**ייבוא** ‏(`POST /admin/ossie/import`) מקבל מסמך YAML או JSON של Ossie ‏(הפורמט מזוהה אוטומטית). הוא מנתח את המסמך ומחזיר רישומי טבלאות וקשרים מוצעים כאובייקט JSON — שום דבר אינו נרשם. מסך הסקירה בממשק הניהול מאפשר לכם לקבל או לגזום הצעות לפני שמוטציה כלשהי נורית. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### אחסון אובייקטים (REQ-1046, REQ-1048, REQ-1049)

קראו או הגדירו את אחסון המטריאליזציה של הארגון:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

‏`GET` מדווח כמה מקצבת האחסון של הפלטפורמה הארגון מנצל. `PUT` רושם את ה-DSN של האחסון של הארגון עצמו (מוצפן במנוחה; לעולם אינו מוחזר על ידי GET). לאחר שנקבע, המטריאליזציות של הארגון נוחתות בדלי שלו עצמו ואינן נספרות עוד מול קצבת הפלטפורמה. שליחת `storage_url: null` מנקה זאת ומחזירה את הארגון למאגר הפלטפורמה. [tool-verified: `provisa/api/admin/org_storage_router.py`]

### הצפנת ארגון (REQ-1574)

קבעו או סבבו את מפתח ההצפנה במנוחה של הארגון:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

‏`GET` מחזיר את טביעת האצבע של המפתח, את המזהה שלו ואת מוצאו — לעולם לא חומר מפתח. `PUT` קובע או מסובב את המפתח. ספקו `key_b64` (32 בתים גולמיים, מקודדי base64) כדי להביא מפתח משלכם, או השמיטו אותו כדי ש-Provisa תייצר אחד. אין מחיקה: הוצאת המפתח האחרון מכלל שירות הייתה משאירה כל מטען שהוא עטף בלתי קריא. [tool-verified: `provisa/api/admin/org_encryption_router.py`]

## GraphiQL

ה-admin API מגיע עם GraphiQL בכתובת `GET /admin/graphql` בדפדפן (REQ-622). השתמשו בו כדי לחקור את סכמת ה-admin המלאה באופן אינטראקטיבי.

## תצוגות ניהול בדומיין ops ‏(REQ-1386)

שמונה תצוגות SQL נזרעות לתוך דומיין ה-`ops` המובנה בכל התקנה. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] הן חושפות את יומן ביקורת השאילתות כטבלאות ממושלות — ניתנות לשאילתה דרך SQL ‏(pgwire), ‏GraphQL ו-Cypher תחת אותה גישת דומיין, ‏RLS וכללי מיסוך כמו כל טבלה עסקית.

‏`org_admin` מיועד כאוצר דומיין ה-ops בזמן הזריעה, ולכן הדומיין לעולם אינו מופיע כפער ממשל ב-`stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| תצוגה | על מה היא עונה |
| --- | --- |
| `usage_ranking` | ספירת שאילתות ומשתמשים ייחודיים לכל טבלה רשומה; טבלאות ללא פגיעות עולות כמועמדות להוצאה משימוש |
| `deprecated_usage` | כל גישה לטבלה או עמודה הנושאת את התגית `deprecated` — הצרכנים הפעילים החוסמים הסרה בטוחה |
| `pii_access` | כל גישה לטבלה או עמודה הנושאת את התגית `pii`: מי שאל אותה, תחת איזה תפקיד, על פני איזה ממשק |
| `policy_denials` | כל ניסיונות הגישה שהממשל דחה (HTTP 401/403) |
| `surface_mix` | ספירת שאילתות יומית ומשתמשים ייחודיים לכל ממשק פרוטוקול (SQL, ‏GraphQL, ‏Cypher, ‏gRPC וכו') |
| `query_health` | ספירת שגיאות יומית והשהיה ממוצעת/מרבית לכל ממשק |
| `stale_metadata` | טבלאות ועמודות שחסר להן תיאור; דומיינים שחסר להם אוצר |
| `join_hotspots` | זוגות טבלאות הנשאלים יחד לרוב — מועמדים למטריאליזציה או למטמון |

שתי מגבלות חלות היום. הגרעיניות היא ברמת הטבלה — יומן הביקורת רושם `table_ids`, לא עמודות בודדות שניגשו אליהן. טקסט השאילתה מוצפן (REQ-689) ומוחרג מכל תצוגה כאן; הוא נגיש רק דרך נתיב הפענוח המורשה של ה-admin. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

תפקיד זקוק לגישה לדומיין `ops` לפני שתצוגות אלה נראות. הענקו אותה באותה דרך שבה אתם מעניקים גישה לכל דומיין אחר.

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

אותן שאילתות רצות כ-GraphQL או Cypher על פני כל תעבורה ממושלת — pgwire, ‏Arrow Flight, או Bolt. [inferred from governed-surface design]

## מציג הדוחות (REQ-1390)

מציג הדוחות נמצא ב-`/admin/reports`. תפקידים ללא היכולת `observability` אינם יכולים להגיע אליו.

הפאנל השמאלי מונה כל טבלה רשומה בדומיין `ops`, ממוינת לפי כינוי. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] שמונת תצוגות הניהול הזרועות מופיעות שם אוטומטית. לחצו על דוח כלשהו כדי לטעון אותו במציג הנתונים הממושל מימין.

**הוספת דוח מותאם.** הכפתור "Add report" פותח דיאלוג. ספקו שם, תיאור אופציונלי, ופקודת SELECT. השמירה רושמת את התצוגה כטבלה נגזרת ממושלת בדומיין `ops` — מקוטלגת, מבוקרת-גישה, וניתנת לשאילתה דרך כל ממשק לצד התצוגות הזרועות. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**מחיקה.** סמל הפח מופיע רק עבור דוחות מותאמים. תצוגות ניהול זרועות אינן ניתנות למחיקה מממשק זה. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## תצוגה מקדימה של טבלה (REQ-1392)

הרחיבו שורת טבלה כלשהי בעמוד הטבלאות. הכפתור **Preview** פותח מודאל ברוחב 90% עם הנתונים הממושלים החיים של הטבלה. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

טבלאות המגובות ב-APIs עם פרמטרי נתיב נדרשים חוסמות תצוגה מקדימה עד שערכים אלה מסופקים. טופס מוטבע אוסף כל פרמטר נדרש לפני שהשאילתה הראשונה רצה; פרמטרי שאילתה אופציונליים מופיעים באותו טופס. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## מציג נתונים ממושל (REQ-1391)

אותו רכיב מציג מפעיל את מודאל התצוגה המקדימה ואת מציג הדוחות. התנהגותו זהה בשני ההקשרים.

**דפדוף בצד השרת.** כל עמוד הוא `SELECT *` ממושל משלו עם `LIMIT 101 OFFSET n`. 100 שורות מופיעות בכל עמוד; ה-101 מאותת אם קיימות עוד. מערך הנתונים המלא לעולם אינו נטען לדפדפן. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**מסננים ומיונים מודחפים למטה.** לכל כותרת עמודה יש קלט סינון. מונחי סינון הופכים לפרדיקטים `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; לחיצות מיון מייצרות פסוקיות `ORDER BY`. שניהם הולכים למסד הנתונים — סינון על טבלה בת מיליארד שורות סורק את המקור, לא את עמוד 100 השורות שלפניכם. [tool-verified: `nativeParams.ts:53-70`]

**קיבוץ רב-שכבתי.** סמל ה-Layers בכל כותרת עמודה מחליף את מצב שילובה של אותה עמודה בקיבוץ. עמודות הקיבוץ מובילות את ה-`ORDER BY` כך שחברי קבוצה נוחתים באותו עמוד כמו הכותרת שלהם על פני גבולות עמוד. עמודות מפתח ראשי מצורפות כשובר-שוויון יציב. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] שורות כותרת קבוצה ניתנות לכיווץ; כיווץ מסתיר חברים מבלי להנפיק שאילתה חדשה. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**בחירות מתמידות.** הגדרות סינון, מיון וקיבוץ נשמרות ב-`localStorage` תחת `provisa.grid.table:<domain>.<table>` ומשוחזרות בביקור הבא. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**ייצוא.** הורידו את העמוד הנוכחי כ-CSV, או העתיקו אותו ללוח כטקסט מופרד-טאבים. הייצוא מכסה את העמוד הנראה בלבד. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
