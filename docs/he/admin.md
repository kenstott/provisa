# Admin API

ה-admin API הוא נקודת קצה GraphQL של Strawberry ב-`POST /admin/graphql` (REQ-533). הוא דורש תפקיד superuser או admin (REQ-125, REQ-060) ונפרד מנקודת הקצה של GraphQL לנתונים (REQ-533).

## אימות

העבירו את האישורים שלכם בכותרת ה-`Authorization` באמצעות ספק האימות הסטנדרטי של Provisa (REQ-120):
```
Authorization: Bearer <token>
```

גישת admin מנוהלת על ידי היכולת (capability) `admin` המוקצית לתפקיד (REQ-060, REQ-042).

## יכולות

### ניהול תצורה

הורדת התצורה הרצה הנוכחית (REQ-164):
```
GET /admin/config
```

מחזיר את מלוא `config.yaml` כקובץ YAML. העלאת תצורה חדשה (REQ-164):
```
PUT /admin/config
```

Provisa מאמתת את ה-YAML, טוענת מחדש קטלוגים, ומייצרת מחדש סכמות (REQ-012, REQ-253). אין צורך באתחול מחדש.

### הגדרות זמן ריצה

קריאה וכתיבה של הגדרות פלטפורמה בזמן-ריצה ללא עריכת קובץ התצורה (REQ-165):
```
GET  /admin/settings
PUT  /admin/settings
```

משטח ההגדרות מכסה הפניית תוצאות-גדולות, דגימה וגבול שורות ברירת-מחדל, TTL‏ של מטמון תגובה, מוסכמת שם, מעקב-אוטומטי אחר FK בקשרים, DSN‏ של מאגר מימוש, זיכרון מנוע הפדרציה (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), ומשטח כיוונון צינור מעקב ה-OpenTelemetry המלא (REQ-1082). מגבלות traversal של GraphQL מרוחק והגדרות שכבת-חמימה/מטמון-קריאה חשופות אף הן (REQ-1081, REQ-1083).

תנוחת אבטחה — `security.mode` (`standard` | `high`) — מוחלת בעת אתחול מחדש (REQ-1079):
```
GET  /admin/security
PUT  /admin/security
```

הקצאות מודל AI, רישום מודל embedding/וקטור, ומגבלת קצב NL — מוחלים בעת אתחול מחדש (REQ-1080):
```
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
</content>
