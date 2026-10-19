# Admin API

ה-admin API הוא נקודת קצה של Strawberry GraphQL בכתובת `POST /admin/graphql` ‏. היא מחייבת תפקיד superuser או admin ‏ והיא נפרדת מנקודת הקצה של GraphQL לנתונים.

## אימות {: #authentication }

העבירו את אישורי הגישה שלכם בכותרת `Authorization` באמצעות ספק האימות הרגיל של Provisa ‏:

```yaml
Authorization: Bearer <token>
```

גישת admin ממושלת על ידי היכולת `admin` המוקצית לתפקיד.

### אסימוני גישה אישיים {: #personal-access-tokens }

אסימון גישה אישי מתקבל בכל מקום שבו מתקבל אסימון bearer, כולל נקודת קצה זו. אתם מנפיקים ומבטלים את שלכם, כי אסימון הוא פרטי ההתחברות של מי שמחזיק בו. לכן הוא נמצא בפרופיל שלכם בממשק הניהול, לצד עזיבת ארגון ומחיקת החשבון, ולא תחת דף ניהול. מנהל לעולם לא מנפיק אסימון בשם מישהו אחר.

| נתיב | השפעה |
| ------- | -------- |
| `POST /auth/tokens` | הטבעת אסימון עבור הקורא. גוף: `name`, ואופציונלית `role_id`, `scopes`, `expires_in_days` ‏(1–366). התגובה היא המקום היחיד שבו הסוד מופיע אי פעם |
| `GET /auth/tokens` | האסימונים הפעילים של הקורא בארגון זה — קידומת התצוגה, השם, חותמות זמן של מחזור החיים, וה-hash המזהה אסימון לצורך ביטול. לעולם לא אישור גישה עובד |
| `DELETE /auth/tokens/{token_hash}` | ביטול אחד מאסימוני הקורא. ‏404 כשהוא אינו שלו או כבר בוטל |

השמטת `role_id` משאירה את האסימון מתפענח לכל תפקיד שבעליו מחזיק; נקיבה בשם אחד מצמצמת את האסימון מתחת לבעליו. ביטול קורה גם במשתמע: הסרת חברות ארגון של משתמש מבטלת את אסימוניו עבור אותו ארגון. ראו [מודל אבטחה](security.md#personal-access-tokens) עבור אישור הגישה עצמו.

## יכולות {: #capabilities }

### ניהול תצורה {: #config-management }

הורידו את התצורה הרצה הנוכחית:

```http
GET /admin/config
```

מחזיר את `config.yaml` המלא כקובץ YAML. העלו תצורה חדשה:

```http
PUT /admin/config
```

‏Provisa מאמתת את ה-YAML, טוענת מחדש קטלוגים ומייצרת מחדש סכמות. אין צורך באתחול מחדש.

### הגדרות זמן ריצה {: #runtime-settings }

קראו וכתבו הגדרות פלטפורמה בזמן ריצה מבלי לערוך את קובץ התצורה:

```http
GET  /admin/settings
PUT  /admin/settings
```

משטח ההגדרות מכסה הפניית תוצאות גדולות, דגימה ומגבלת שורות כברירת מחדל, ‏TTL של מטמון תגובות, מוסכמת שמות, מעקב אוטומטי אחר FK בקשרים, ‏DSN של מאגר המטריאליזציה, זיכרון מנוע הפדרציה (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), ואת מלוא משטח הכוונון של צינור המעקב של OpenTelemetry ‏. גם מגבלות מעבר של GraphQL מרוחק והגדרות שכבה חמה / מטמון קריאה חשופות.

כרטיס **שרתי מחברים** מציין, עבור כל מחבר בעל שרת מצורף (File Crawler, SharePoint, Salesforce, ServiceNow, Cloud Inventory, Splunk, AskAmerica), תיקייה המכילה חבילת שרת שנבנתה עבור מארח זה (`connectors.<connector>.bundle_dir`, משתנה סביבה `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`). ריקה — שרת המחבר נשלף מה-release הקבוע של `kenstott/calcite`. מוגדרת — התיקייה משמשת כפי שהיא: ה-launcher שלה חייב להיות ב-`bin/pgwire-<connector>`, ותיקייה ללא מפעיל זה מונעת הפעלת שרת המחבר עם שגיאה המזכירה את ההגדרה — ה-release אינו נשלף במקומו. ההגדרה נקראת בכל פעם שמופעל שרת של מחבר.

מצב האבטחה, `security.mode` (`standard` | `high`), מוחל בהפעלה מחדש:

```http
GET  /admin/security
PUT  /admin/security
```

שיוכי מודלי AI, רישום מודלי ה-embedding והווקטורים ומגבלת הקצב של NL נכנסים לתוקף בבקשה הבאה, בלי הפעלה מחדש:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

לשונית ההצפנה בממשק הניהול גוזרת את רשימת הספקים שלה חיה ממרשם ההצפנה; ספקים שאינם זמינים מופיעים אך אינם ניתנים לבחירה.

`GET`/`HEAD /health` ו-`GET /setup/status` לעולם לא דורשים אימות. הם עוקפים את דרישת `Authorization: Bearer` גם כשמוגדר ספק אימות.

### מנוע פדרציה {: #federation-engine }

קראו או שנו באיזה מנוע הפריסה משתמשת:

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

‏`GET` מחזיר את מפתח המנוע הפעיל ואת שדות התצורה שהוא זקוק להם. `PUT` מקבל גוף עם `engine` (המפתח) וכל שדה ספציפי למנוע; הבחירה נשמרת לתצורת הפלטפורמה ונכבלת באתחול השירות הבא.

### עורך קשרים {: #relationship-editor }

רשימת קשרים:

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

יצירת קשר. מזהה שכבר קיים נדחה; קשר משתנה עם `updateRelationship`, שמקבל את אותו קלט:

```graphql
mutation {
  createRelationship(input: {
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

הצהרה על קשר מגובה junction:

```graphql
mutation {
  createRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

טבלת קישור מוצהרת כקשת; Provisa לעולם לא מגלה אחת כזו. `viaTable` מציין טבלה רשומה. שתי עמודות המפתח שלה נושאות את הקשת, וכל עמודה אחרת הופכת לתכונה של הקשר, שאפשר לסנן כמו כל שדה. `viaTypeColumn` / `viaTypeValue` מפצלים טבלת צומת אחת לכמה סוגי קשתות. לדוגמה, שורות של `pet_companions` שבהן `companion_type` הוא `bonded pair`, `littermate` או `shares enclosure` נותנות שלושה קשרים נפרדים על אותו זוג טבלאות.

`viaLabelSource` קובע מאין מגיע השם החשוף, ו-Cypher מקבל כל צורה באותיות גדולות עם קווים תחתונים. `column` משתמש ב-`viaTypeValue` (`BONDED_PAIR`), `table` בשם של טבלת הצומת עצמה (`PET_COMPANIONS`), ו-`fixed` ב-`alias` המוצהר. טבלת צומת שמוצהרת כך היא קשת. Provisa מוריד אותה מתוויות הצמתים, ולכן היא לעולם לא מופיעה כגלולת צומת בממשק הגרף.

### גילוי קשרים בעזרת AI {: #ai-relationship-discovery }

הפעילו ניתוח FK מונע-Claude דרך REST ‏:

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

### Introspection של סכמה {: #schema-introspection }

עיינו בטבלאות מפורסמות בכל המקורות:

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### בדיקת תלויות עמודה {: #column-dependency-check }

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

שינוי שם של alias שובר כל רכיב שנכתב מול השם החשוף: views, MVs, ביטויי מדדים, פרדיקטים של RLS וחוזי DQ. מחיקת עמודה שוברת גם אותם, ובנוסף את הרכיבים ששומרים את `column_name` הפיזי, כלומר קשרים, קישורי מילון מונחים ושיוכי תגיות. `breaksOn` אומר איזה מהם. דף הטבלאות מריץ בדיקה זו בשמירה ומציג את התוצאה בתיבת דו-שיח מייעצת. ב-[Lineage](lineage.md) מוסבר מה השאילתה מכסה ומה לא.

### ניהול תצוגות {: #view-management }

רישום תצוגה ממוטריאלת:

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

הפעלת רענון ידני:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### רישום מקורות גרף {: #graph-source-registration }

מקורות Neo4j ו-SPARQL נרשמים דרך נקודות קצה REST ‏(לא דרך ה-admin API של GraphQL) ‏:

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

לאחר הרישום, טבלאות מופיעות בסכמת GraphQL וניתנות לשאילתה כמו כל מקור אחר.

### ייבוא Hasura / DDN ‏ {: #hasura-ddn-import }

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

**יישום** טוען את ה-YAML שבדקתם, וערכתם אם רציתם, לארגון הפועל. הוא משתמש באותו נתיב טעינה חמה כמו `PUT /admin/config`. גוף הבקשה: `{"config_yaml": "<yaml string>"}`.

התצוגה המקדימה לעולם אינה שומרת את ה-YAML הממיר במטמון בצד השרת; היישום לוקח את ה-YAML שאתם מספקים, ולכן מה שמיושם הוא בדיוק מה שנסקר.

### חילופין עם Apache Ossie ‏ {: #apache-ossie-interchange }

‏Provisa פועלת הדדית עם Apache Ossie ‏(incubating) כגבול ייבוא/ייצוא.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**ייצוא** (`GET /admin/ossie`) בונה את מסמך ה-YAML של Ossie מהמודל המנוהל החי בכל בקשה. שום דבר לא נשמר במטמון, ולכן המסמך לעולם לא מיושן. התגובה היא `text/yaml` עם כותרת `Content-Disposition: attachment`. המסמך הוא מודל אחד עם `version`, `name`, `datasets`, `relationships` ו-`metrics` בשורש. טבלאות הופכות לאובייקטי `dataset`, עמודות לאובייקטי `field`, וקשרים ממופים לאובייקטי `relationship` של Ossie. מודל בלי טבלה נדחה עם `422`.

**ייבוא** (`POST /admin/ossie/import`) מקבל מסמך Ossie ב-YAML או ב-JSON ומזהה את הפורמט בעצמו. הוא מנתח את המסמך ומחזיר כאובייקט JSON רישומים מוצעים של טבלאות, קשרים ומדדים. שום דבר לא נרשם. הוא קורא רק את צורת המסמך השטוחה, ומסמך שעוטף את המודל שלו ב-`semantic_model` נדחה בשמו. בממשק הניהול, מסך הסקירה מאפשר לכם לקבל או לקצץ הצעות לפני שמשהו משתנה.

### אחסון אובייקטים {: #object-storage }

קראו או הגדירו את אחסון המטריאליזציה של הארגון:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

‏`GET` מדווח כמה מקצבת האחסון של הפלטפורמה הארגון מנצל. `PUT` רושם את ה-DSN של האחסון של הארגון עצמו (מוצפן במנוחה; לעולם אינו מוחזר על ידי GET). לאחר שנקבע, המטריאליזציות של הארגון נוחתות בדלי שלו עצמו ואינן נספרות עוד מול קצבת הפלטפורמה. שליחת `storage_url: null` מנקה זאת ומחזירה את הארגון למאגר הפלטפורמה.

### הצפנת ארגון {: #org-encryption }

קבעו או סבבו את מפתח ההצפנה במנוחה של הארגון:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` מחזיר את טביעת האצבע, המזהה והמקור של המפתח, ולעולם לא חומר מפתח. `PUT` קובע או מחליף את המפתח. ספקו `key_b64` (32 בתים גולמיים, בקידוד base64) כדי להביא מפתח משלכם, או השמיטו אותו ו-Provisa ייצור אחד. אין מחיקה, כי הוצאת המפתח האחרון משימוש הייתה משאירה כל מטען שהוא עטף בלתי קריא.

## GraphiQL

ה-admin API מגיע עם GraphiQL בכתובת `GET /admin/graphql` בדפדפן. השתמשו בו כדי לחקור את סכמת ה-admin המלאה באופן אינטראקטיבי.

## תצוגות ניהול בדומיין ops ‏ {: #ops-domain-management-views }

כל התקנה זורעת שמונה views של SQL בדומיין המובנה `ops`. הם חושפים את יומן הביקורת של השאילתות כטבלאות מנוהלות. אתם שואלים אותם דרך SQL (pgwire), GraphQL ו-Cypher, תחת אותם כללי גישה לדומיין, RLS והסתרה כמו כל טבלה עסקית.

‏`org_admin` מיועד כאוצר דומיין ה-ops בזמן הזריעה, ולכן הדומיין לעולם אינו מופיע כפער ממשל ב-`stale_metadata`.

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

הרזולוציה נעצרת ברמת הטבלה. יומן הביקורת רושם `table_ids`, והעמודות שהשאילתה קראה אינן נרשמות. טקסט השאילתה מוצפן ואינו מופיע באף אחד מה-views כאן; רק נתיב הפענוח המורשה של הניהול יכול לקרוא אותו.

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

מכיוון שה-views הם טבלאות מנוהלות, אפשר לכתוב את אותן שאילתות ב-GraphQL או ב-Cypher.

## מציג הדוחות {: #reports-viewer }

מציג הדוחות נמצא ב-`/admin/reports`. תפקידים ללא היכולת `observability` אינם יכולים להגיע אליו.

הפאנל השמאלי מונה כל טבלה רשומה בדומיין `ops`, ממוינת לפי כינוי. שמונת תצוגות הניהול הזרועות מופיעות שם אוטומטית. לחצו על דוח כלשהו כדי לטעון אותו במציג הנתונים הממושל מימין.

**הוספת דוח מותאם.** הכפתור "Add report" פותח תיבת דו-שיח שבה אתם נותנים שם, תיאור אופציונלי ומשפט SELECT. השמירה רושמת את ה-view כטבלה נגזרת מנוהלת בדומיין `ops`. היא מקוטלגת, עם בקרת גישה, וניתנת לשאילתה מכל ממשק, לצד ה-views שנזרעו.

**מחיקה.** סמל הפח מופיע רק עבור דוחות מותאמים. תצוגות ניהול זרועות אינן ניתנות למחיקה מממשק זה.

## תצוגה מקדימה של טבלה {: #table-preview }

הרחיבו שורת טבלה כלשהי בעמוד הטבלאות. הכפתור **Preview** פותח מודאל ברוחב 90% עם הנתונים הממושלים החיים של הטבלה.

טבלאות המגובות ב-APIs עם פרמטרי נתיב נדרשים חוסמות תצוגה מקדימה עד שערכים אלה מסופקים. טופס מוטבע אוסף כל פרמטר נדרש לפני שהשאילתה הראשונה רצה; פרמטרי שאילתה אופציונליים מופיעים באותו טופס.

## מציג נתונים ממושל {: #governed-data-viewer }

אותו רכיב מציג מפעיל את מודאל התצוגה המקדימה ואת מציג הדוחות. התנהגותו זהה בשני ההקשרים.

**דפדוף בצד השרת.** כל עמוד הוא `SELECT *` ממושל משלו עם `LIMIT 101 OFFSET n`. 100 שורות מופיעות בכל עמוד; ה-101 מאותת אם קיימות עוד. מערך הנתונים המלא לעולם אינו נטען לדפדפן.

**מסננים ומיונים מודחפים למטה.** לכל כותרת עמודה יש שדה סינון. מונח סינון הופך לפרדיקט `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`, ולחיצת מיון יוצרת פסוקית `ORDER BY`. שניהם רצים במסד הנתונים. מסנן על טבלה של מיליארד שורות סורק את המקור, וטווחו לעולם לא מוגבל לעמוד של 100 השורות שלפניכם.

**קיבוץ רב-שכבתי.** סמל ה-Layers בכל כותרת עמודה מחליף את מצב שילובה של אותה עמודה בקיבוץ. עמודות הקיבוץ מובילות את ה-`ORDER BY` כך שחברי קבוצה נוחתים באותו עמוד כמו הכותרת שלהם על פני גבולות עמוד. עמודות מפתח ראשי מצורפות כשובר-שוויון יציב. שורות כותרת קבוצה ניתנות לכיווץ; כיווץ מסתיר חברים מבלי להנפיק שאילתה חדשה.

**בחירות מתמידות.** הגדרות סינון, מיון וקיבוץ נשמרות ב-`localStorage` תחת `provisa.grid.table:<domain>.<table>` ומשוחזרות בביקור הבא.

**ייצוא.** הורידו את העמוד הנוכחי כ-CSV, או העתיקו אותו ללוח כטקסט מופרד-טאבים. הייצוא מכסה את העמוד הנראה בלבד.
