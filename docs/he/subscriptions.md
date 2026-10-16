# מנויי SSE {: #sse-subscriptions }

Provisa תומכת בדחיפה בזמן אמת מעל Server-Sent Events (SSE). לקוחות מקבלים זרם של אירועי שינוי ללא polling. (REQ-258)

## מקורות {: #sources }

מנויים מכוונים ל**טבלה רשומה**:

| מקור | ערכי `strategy` זמינים |
| -------- | ------------------------- |
| טבלה (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| טבלה (RDBMS שאינו PG עם בלוק `cdc` במקור) | `debezium`, `kafka`, `poll` |
| טבלה (תצוגה פדרטיבית / כל מקור אחר) | `poll` בלבד |

### התקנה אוטומטית של טריגר PostgreSQL {: #postgresql-trigger-auto-installation }

Provisa מתקינה אוטומטית טריגרי `AFTER INSERT OR UPDATE OR DELETE` על כל טבלאות PostgreSQL **מאושרות-מראש** בעת ההפעלה. (REQ-565) טריגרים אלה קוראים ל-`pg_notify('provisa_{table}', ...)` כך ש-DML גולמי (לא רק מוטציות Provisa) נקלט על ידי מנויים. (REQ-565)

אם התקנת הטריגר נכשלת (לדוגמה הרשאה לא מספקת — תפקיד מסד הנתונים חייב להיות הבעלים של הטבלה), Provisa נופלת חזרה ל-polling מבוסס watermark עבור אותה טבלה, בתנאי שמוגדר `watermark_column`. (REQ-566) אזהרה נרשמת ביומן. (REQ-566)

### מנויי תצוגה חוצי-מקורות-נתונים {: #cross-datasource-view-subscriptions }

עבור תצוגות המצטרפות למספר מקורות נתונים דרך מנוע הפדרציה, הוסיפו `watermark_column` לרישום הטבלה. (REQ-260, REQ-283) העמודה חייבת להתקיים ב-SQL של התצוגה (אינה חייבת להופיע בסכמת GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

רשמו עם `watermark_column: _watermark`. Provisa מבצעת polling באמצעות `WHERE _watermark > <last_seen>`. (REQ-260)

### מנויי קשר מקוננים {: #nested-relationship-subscriptions }

כאשר שדה המנוי בוחר שדות מטבלאות מצורפות (דרך קשרים רשומים), Provisa עוקבת אחר **כל** הטבלאות הפיזיות המעורבות בו-זמנית. (REQ-567) שינוי בכל טבלה מצורפת מפעיל מחדש את שאילתת המנוי. (REQ-567)

## נקודת קצה {: #endpoint }

הרשמה למנוי על טבלה:

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

החיבור נשאר פתוח ופולט אירוע JSON אחד לכל שינוי: (REQ-258, REQ-568)

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## מצבי אספקה {: #delivery-modes }

האספקה נבחרת על ידי `live.strategy` בתצורת הטבלה: (REQ-813, REQ-814)

| `strategy` | מנגנון | זמין עבור | דורש |
| ------------ | ----------- | --------------- | --------- |
| `native` | PostgreSQL `LISTEN`/`NOTIFY`, MongoDB Change Streams | PG, MongoDB | ללא דבר נוסף |
| `debezium` | טופיק Kafka ממחבר Debezium | טבלאות RDBMS שאינן PG | בלוק `cdc` ברמת המקור (Debezium + Kafka) |
| `kafka` | טופיק delta Kafka שרירותי | כל טבלה מוזנת מ-Kafka | בלוק `cdc` ברמת המקור |
| `poll` | polling מבוסס watermark | כל טבלה עם watermark | `watermark_column` |

### LISTEN/NOTIFY

Provisa מנפיקה `LISTEN <channel>` על חיבור PG מתמשך. (REQ-258) מוטציות Provisa מפעילות `NOTIFY` אוטומטית. (REQ-565) כותבים חיצוניים חייבים לקרוא ל-`NOTIFY <channel>, '<payload>'` לאחר כתיבות. אין צורך בתשתית נוספת.

### Polling

Provisa מבצעת מחדש את שאילתת המקור מעת לעת, בוחרת רק שורות שבהן `watermark_column > last_watermark`. (REQ-260) הבדלים נפלטים כאירועי SSE. Polling אינו יכול לראות מחיקות קשות — שורה שהוסרה אינה משאירה watermark מתקדם. כדי להפוך מחיקה לגלויה, השתמשו במחיקה רכה (לדוגמה הגדרת דגל `deleted_at`) המקדמת את עמודת ה-watermark; המחיקה אז מגיעה כאירוע עדכון הנושא את סמן המחיקה-הרכה. (REQ-260)

תצורת polling של טבלה (ב-`provisa.yaml`):

```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
```

### Debezium CDC

דורש מחבר Debezium פעיל הכותב ל-Kafka. (REQ-261) Provisa צורכת את טופיק ה-Kafka ומעבירה אירועי שינוי ללקוחות SSE מחוברים. (REQ-261)

תעבורת CDC מוגדרת פעם אחת לכל מקור בבלוק `cdc`; טופיקים נגזרים כ-`{topic_prefix}.{schema}.{table}` ואינם חוזרים לכל טבלה. (REQ-824) כל טבלה אז בוחרת `strategy: debezium`:

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      # schema_registry_url: http://schema-registry:8081   # set for Avro; omit for JSON
    tables:
      - id: orders
        live:
          strategy: debezium
```

#### קידודי הודעות {: #message-encodings }

נושא Debezium נקרא באחד משני קידודים, וכל הודעה אומרת באיזה. (REQ-1951)

| קידוד | מה הנושא נושא | מה המקור צריך |
|---|---|---|
| JSON | מעטפת ממיר ה-JSON של Debezium, עם עטיפת `schema`/`payload` או בלעדיה | דבר מעבר לבלוק `cdc` |
| Avro | פורמט התעבורה של schema registry של Confluent: בית אפס, מזהה סכמה בן ארבעה בתים, פריט Avro אחד | `schema_registry_url` ודרך להזדהות מולו |

הודעות Avro מפוענחות בסכמה שהמזהה שלהן נוקב בה, הנשלפת מה-registry פעם אחת ונשמרת. נושא שהסכמה שלו התפתחה נקרא נכון, כי כל הודעה נוקבת בסכמה שבה נכתבה. נושאי Protobuf ו-JSON Schema אינם נקראים.

הודעת Avro לעולם אינה נקראת כ-JSON. כל סיבה שבגללה לא ניתן לקרוא אותה נדחית בשמה:

| מצב | דחייה |
|---|---|
| הודעת Avro, המקור אינו נוקב ב-registry | `subscribe.avro_topic_without_registry`, עם הנושא |
| ה-registry אינו נגיש או אינו עונה בזמן | `subscribe.schema_registry_unreachable` |
| ה-registry דוחה את האישורים של המקור | `subscribe.schema_registry_refused_credentials` |
| מזהה סכמה שאינו קיים ב-registry | `subscribe.schema_id_unknown`, עם המזהה |
| המזהה נוקב בסכמת Protobuf או JSON | `subscribe.schema_not_avro` |

registry שאינו נגיש או שדוחה את האישורים נדחה בהתחלת המינוי, לפני שהזרם נפתח. בקשה ל-registry ממתינה לחיבור עד 5 שניות ולתשובה עד 10 שניות, ופחות אם לבקשה נותר פחות זמן.

#### אימות מול schema registry {: #schema-registry-authentication }

`schema_registry_auth` נוקב בשיטה; השדות שהשיטה צריכה נבדקים בשמירת המקור.

| `schema_registry_auth` | שדות נדרשים | נשלח ל-registry |
|---|---|---|
| `none` (ברירת מחדל) | — | ללא אישורים |
| `basic` | `schema_registry_username`, `schema_registry_password` | אימות HTTP בסיסי |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>` (טוקן סטטי) |
| `mtls` | `schema_registry_client_cert`, `schema_registry_client_key` | תעודת לקוח, בלחיצת היד של TLS |

`schema_registry_ca` אופציונלי בכל שיטה: חבילת CA שיש לבטוח בה כשתעודת ה-registry אינה חתומה על ידי רשות ציבורית.

הסיסמה והטוקן כפופים לחוזה הסודות. ערך שהוזן בטופס המקור נשמר בכספת של הארגון, והמקור שומר הפניית `${secret:NAME}`; הפניה שנכתבה בקובץ קונפיגורציה (`${env:...}`, `${secret:...}`) נשמרת כפי שנכתבה. התעודה, המפתח וחבילת ה-CA הם קבצים שתהליך השרת של Provisa פותח, הנקובים בנתיב מוחלט.

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      schema_registry_url: https://schema-registry:8081
      schema_registry_auth: basic
      schema_registry_username: provisa
      schema_registry_password: ${secret:registry_password}
      schema_registry_ca: /etc/provisa/registry-ca.pem
```
## כשזרם נכשל {: #when-a-stream-ends-on-a-failure }

זרם שנפתח כבר ענה `200`, ולכן כשל לאחר מכן אינו יכול להיות סטטוס HTTP. המנוי מקבל פריים אחרון שאומר מדוע, ואז הזרם נסגר. זרם לעולם אינו נסגר בגלל כשל בלי הפריים הזה.

הפריים נושא את מה שבקשה רגילה הייתה עונה על אותו כשל:

| כשל | גוף |
|---|---|
| דחייה עם קוד (schema registry, אזור, דחיית ממשל) | `detail`, `code`, `params` |
| המועד האחרון של הבקשה | `detail: "Request timed out"` |
| כל דבר בלתי צפוי | `detail: "Internal server error"`, `type` (שם סוג החריגה) |

כל גוף נושא גם `subscription_id`. בכשל בלתי צפוי, ההודעה ו-traceback נמצאים ביומן השרת תחת אותו מזהה, לא בפריים.

מינוי לטבלה (`/data/subscribe/{table}`) שולח את הגוף כנתוני אירוע בשם `stream_error`:

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```
האירוע אינו נקרא `error`, כי `EventSource` מייצר בעצמו אירוע `error` כשהחיבור נקטע, ומאזין לא היה יכול להבחין ביניהם. האזינו לו בשמו:

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```
מינוי GraphQL שומר על צורת התגובה של GraphQL, עם הקוד וה-params ב-`extensions`:

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```
מקרה אחד אינו מסיים מינוי GraphQL: כשקריאה חוזרת לאחר שינוי אחד נכשלת, הפריים הזה נשלח באותה צורה, והזרם ממשיך לשינוי הבא. כשל בקריאה הראשונית, בזרם השינויים עצמו או בהפעלתו הוא הפריים האחרון.

## הפניית Sink ל-Kafka {: #kafka-sink-redirect }

כל מנוי GraphQL ניתן להפניה לטופיק Kafka במקום זרימה חזרה ללקוח. (REQ-812) הוסיפו את הכותרת `X-Provisa-Sink` לבקשת המנוי:

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

השרת משיב `202 Accepted` מיידית ומתחיל משימת רקע ש: (REQ-812)

1. עוקבת אחר שינויי טבלה באמצעות אותה שרשרת פתרון-ספק כמו SSE (LISTEN/NOTIFY ← polling asyncpg ← polling פדרטיבי)
2. מבצעת מחדש את השאילתה השקולה בכל שינוי
3. מפרסמת את התוצאה כהודעת JSON לטופיק ה-Kafka הנקוב

ה-sink רץ למשך חיי תהליך השרת. (REQ-812) הפעלה מחדש של השרת עוצרת אותו (רישום sink מתמשך דרך ה-admin API מתוכנן).

**פורמט URI:** `kafka://[broker:port]/topic`

- אם `broker:port` הושמט, משתנה הסביבה `KAFKA_BOOTSTRAP_SERVERS` נעשה בו שימוש (ברירת מחדל: `localhost:9092`) (REQ-812)
- `topic` נדרש

**דוגמה (curl):**

```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Sink של Kafka כפלט שני ברמת התצורה {: #kafka-sink-as-a-config-level-second-output }

מנוי טבלה מבוסס-poll יכול לפרסם בו-זמנית לטופיק Kafka דרך `provisa.yaml`. (REQ-282, REQ-286) מנוי SSE ו-sink Kafka הם שני פלטים של אותו מנוע Live Query. (REQ-282) כל פלט עוקב אחר ה-watermark שלו באופן עצמאי. (REQ-286)

```yaml
tables:
  - id: active-orders
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
        - type: kafka
          topic: provisa.active-orders
          bootstrap_servers: kafka:9092
          key_column: id
          role: analyst           # required: the rows are published governed as this role
```

פלט Kafka מפרסם את שורות הטבלה כשהן מנוהלות כתפקיד שהוא נוקב בו: כללי השורות, נראות העמודות והמסכות שלו חלים כמו על קריאה של אותו תפקיד. פלט ללא `role` נדחה בטעינת הקונפיגורציה. (REQ-565) [tool-verified: `provisa/core/models.py` `LiveOutputConfig`]

## אבטחה {: #security }

כל מצבי המנוי אוכפים את אותו צינור אבטחה כמו שאילתות רגילות: (REQ-258, REQ-038)

- פילטרים של RLS מוחלים על כל שורה נפלטת (REQ-040)
- עמודות ממוסכות מופיעות ממוסכות באירועים (REQ-040)
- הרשאת תפקיד נבדקת בזמן החיבור (REQ-258)

## דוגמת לקוח {: #client-example }

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```

