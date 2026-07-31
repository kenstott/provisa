# מודל אבטחה

Provisa אוכפת מודל אבטחה רב-שכבתי על פני כל שפת שאילתה (GraphQL, SQL, Cypher) וכל תעבורה (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) הממשל מיושם באופן אחיד — אין נתיב שאילתה העוקף אותו. (REQ-002, REQ-266)

השכבות חלות בסדר. בקשה חייבת לעבור כל שכבה לפני שהשכבה הבאה מוערכת.

## המודל המשוכבב

### שכבה 0 — סינון Introspection

הסכמה והקטלוג המוצגים לתפקיד מכילים רק את הטבלאות ברשימת ה-`domain_access` שלו ואת העמודות העוברות כללי `visible_to` לפי-עמודה. (REQ-039) אובייקטים מחוץ לגישת התפקיד אינם נראים בזמן הגילוי — לא ניתן לשאול אותם, להשלים אוטומטית, או להסיק את קיומם. (REQ-039) זה חל על סכמת GraphQL, קטלוג SQL, ודפדפן הסכמה של עורך השאילתות. (REQ-039, REQ-363)

ראו [נראות סכמה](#_9).

### שכבה 1 — גישה ציבורית

טבלאות בדומיינים ללא הגבלת `domain_access` נראות לכל זהות מאומתת ללא תצורה נוספת. חיכוך אפס עבור נתונים ציבוריים באמת.

### שכבה 2 — גישת דומיין

כל תפקיד נושא רשימת `domain_access` של מזהי דומיין. שאילתה הנוגעת בטבלה מחוץ לדומיינים אלה נדחית לפני הביצוע. (REQ-038, REQ-039) זהו גבול הבעלות הגס — תפקיד HR אינו יכול להגיע לטבלאות פיננסים ללא קשר לאופן כתיבת ה-SQL. (REQ-002)

ראו [מודל הרשאות](#_3).

### שכבה 3 — אבטחה ברמת השורה

לאחר אישור גישת הדומיין, סעיפי `WHERE` לפי-טבלה, לפי-תפקיד מוזרקים לכל `SELECT` בזמן הביצוע. (REQ-041, REQ-263) הסעיפים מוערכים מול הנתונים הגולמיים. מנהל אזורי השואל טבלת הזמנות משותפת רואה רק את שורות האזור שלו אפילו ב-`SELECT *`. (REQ-264)

ראו [אבטחה ברמת השורה (RLS)](#rls).

### שכבה 4 — נראות ומיסוך עמודות

עמודות עם רשימת `visible_to` שאינה כוללת את התפקיד המבקש מוסרות מפלט השאילתה. (REQ-040, REQ-263) לעמודות עם כלל מיסוך יש את ערכיהן מוחלפים — הצנעה ברגקס, החלפה קבועה, או קיצוץ — לפני שהתוצאות עוזבות את השרת. (REQ-263) המיסוך חל בכל שפות השאילתה ופורמטי הפלט. (REQ-263)

ראו [מודל הרשאות עמודה](#_5) ו-[מיסוך ברמת עמודה](#_10).

### שכבה 5 — שומר predicate

עמודות ממוסכות נדחות מסעיפי `WHERE` ו-`HAVING`. (REQ-263) בלי זה, מתקשר יכול להסיק את הערך הלא-ממוסך על ידי חיפוש בינארי שלו בפילטר גם אם הפלט ממוסך. הדחייה נאכפת בזמן פענוח (parse) השאילתה, לפני הביצוע. (REQ-531)

### ממשל קשרים (V002)

תנאי JOIN ב-SQL חייבים להתאים לקשר רשום ומאושר בין טבלאות. (REQ-001) JOIN-ים לא מאושרים נדחים. לכל קשר יש סיבה ותיאור קריאים-לאדם — הנחיה הן למשתמשים והן לסוכנים אוטונומיים אודות הסיבה לקיומו של נתיב מעבר. זוהי מדיניות ממשל, לא גבול אבטחה קשיח: שכבות 2–5 מחזיקות ללא קשר למבנה ה-join, כך שעקיפה מכוונת אינה חושפת נתונים שהתפקיד לא היה יכול להגיע אליהם דרך שתי שאילתות נפרדות. ניסיונות עקיפה נרשמים וניתנים לביקורת.

**מנגנוני עקיפה** — ניתן לעקוף את V002 רק כששני תנאים בלתי-תלויים שניהם אמת:

1. **דגל תפקיד** — `relationship_guard: false` בהגדרת התפקיד (ברירת מחדל: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **הצטרפות לפי-שאילתה** — ה-SQL מכיל את ההערה `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

שניהם חייבים להתקיים. דגל התפקיד לבדו אינו עוקף את V002; ההערה לבדה אינה עוקפת את V002.

**נתיב GraphQL** — V002 מדולג באופן בלתי-מותנה עבור שאילתות GraphQL. קשרים המוגדרים ב-SDL מאושרים מראש בעיצוב; הבדיקה מיותרת ואינה מיושמת. [tool-verified: `provisa/api/data/endpoint.py:468`]

**נתיבי SQL ו-Cypher** — V002 פעיל כברירת מחדל. גם `endpoint_dev.py` וגם `cypher_router.py` מיישמים את בדיקת שני-התנאים לפני קריאה ל-`validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**נתיב pgwire** — אותה בדיקת שני-תנאים כמו SQL. ההערה `--relationship-guard=false` מוסרת מהשאילתה לפני הביצוע; היא אינה מגיעה למסד הנתונים. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

שכבות אלה מצטרפות. לתפקיד עם גישת דומיין, RLS, ועמודות ממוסכות יש את כל חמשת האילוצים פעילים בו-זמנית. הוספת מקור נתונים, עמודה, או קשר חדש אינה דורשת עדכון כל כלל — כל שכבה מוגדרת באופן עצמאי וחלה אוטומטית על כל שאילתה הנוגעת באובייקטים מנוהלים.

---

## מודל הרשאות

יכולות המוקצות באופן עצמאי עם היררכיית תפקידים אופציונלית דרך `parent_role_id`. `admin` מעניק הכול. (REQ-042)

| יכולת | תיאור |
| ----------- | ------------- |
| `source_registration` | רישום מקורות נתונים |
| `table_registration` | רישום טבלאות, עמודות |
| `create_relationship` | הגדרת קשרי FK |
| `access_config` | תצורת RLS, מיסוך |
| `query_development` | ביצוע שאילתות |
| `write` | הפעלת מוטציות רשומות (שער גס; ראו הרשאת מוטציה) |
| `full_results` | עקיפת מגבלות דגימה |
| `ignore_relationships` | עקיפת ממשל קשרים (V002) |
| `admin` | Superuser — מעניק הכול |

### ירושת תפקידים

תפקידים יכולים לרשת יכולות וגישת דומיין מתפקיד הורה דרך `parent_role_id`. (REQ-215) ההיררכיה משוטחת (flattened) בעת ההפעלה — תפקידי בן ממזגים את יכולות ההורה שלהם וגישת הדומיין עם שלהם. (REQ-215)

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## מודל הרשאות עמודה

לכל עמודה יש מודל הרשאות בעל ארבעה שדות השולט בגישת קריאה, כתיבה, ומיסוך לפי-תפקיד. (REQ-042, REQ-249)

### נראות תלת-שכבתית

| שכבה | תנאי | תוצאה |
| ------ | ----------- | -------- |
| **מוסתרת** | תפקיד לא ברשימת `visible_to` | עמודה נעדרת מ-SDL של GraphQL |
| **ממוסכת** | תפקיד ב-`visible_to`, יש כלל מיסוך, תפקיד לא ב-`unmasked_to` | עמודה נראית אך הנתונים ממוסכים ב-SQL |
| **לא-ממוסכת** | תפקיד ב-`visible_to` וגם תפקיד ב-`unmasked_to` (או אין כלל מיסוך) | גישת קריאה מלאה |

### הרשאות כתיבה

| שדה | ריק אומר | מטרה |
| ------- | ------------ | ------------- |
| `visible_to` | כל התפקידים יכולים לקרוא | שולט מי רואה את העמודה (ממוסכת או לא) |
| `unmasked_to` | אף תפקיד לא רואה לא-ממוסך | שולט מי עוקף את המיסוך |
| `writable_by` | אף תפקיד לא יכול לכתוב | שולט מי יכול לשנות (INSERT/UPDATE) |

הרשאת כתיבה נאכפת בצינור המוטציה. תפקיד שאינו ב-`writable_by` מקבל שגיאת 403 בעת ניסיון לכתוב לעמודה מוגבלת. (REQ-033, REQ-034)

### דוגמה

```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

בדוגמה זו:

- `email`: admin רואה `alice@example.com` ויכול לערוך; analyst/viewer רואים `a***@example.com`
- `salary`: admin ו-hr רואים את הערך האמיתי; hr יכול לערוך; כל שאר התפקידים לא רואים את העמודה כלל
- `created_at`: כולם יכולים לקרוא, אף אחד לא יכול לכתוב

## הרשאת מוטציה

מוטציות רשומות (GraphQL מרוחק, OpenAPI, gRPC, Hasura) נשערות על ידי שתי בדיקות בלתי-תלויות. (REQ-867, REQ-868) תפקיד יכול להפעיל מוטציה רק אם הוא מחזיק את יכולת ה-`write` הגלובלית וגם מופיע ברשימת ה-`writable_by` של אותה מוטציה. (REQ-868) `writable_by` ריק הוא דחייה-כברירת-מחדל — אף תפקיד לא יכול להפעיל אותה. (REQ-867)

מוטציות מסווגות ככתיבות לפי חוזה, לא לפי הצהרת המתקשר. (REQ-869) `SELECT` המפנה לפונקציה מסוג-מוטציה מקודם לכתיבה וכפוף לאותה בדיקת שני-שערים, כך שמתקשר אינו יכול להפעיל מוטציה על ידי הסוואתה כקריאה. (REQ-869) סיווג מחדש של מוטציה לבטוחה-לקריאה דורש את יכולת ה-`access_config` ונרשם כהחלטת ממשל; אין הצטרפות לפי-בקשה. (REQ-870)

## נראות סכמה

סכמות GraphQL לפי-תפקיד מסתירות תוכן לא-מורשה: (REQ-039)

- **גישת דומיין**: תפקיד רואה טבלאות רק בדומיינים של ה-`domain_access` שלו (`"*"` = הכול) (REQ-039)
- **נראות עמודה**: עמודות שאינן ב-`visible_to` עבור תפקיד מושמטות מה-SDL (REQ-039)
- טבלאות/עמודות לא-מורשות אינן מופיעות בסכמה (REQ-039)

## אבטחה ברמת השורה (RLS)

הזרקת סעיף WHERE של SQL לפי-טבלה, לפי-תפקיד. מיושם לאחר הקימפול, לפני הביצוע. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

הפילטר משורשר עם AND לסעיף ה-WHERE של השאילתה. עובד גם עבור שאילתות וגם עבור מוטציות (UPDATE/DELETE). (REQ-035, REQ-041)

## מיסוך ברמת עמודה

המיסוך מוגדר פעם אחת לכל עמודה — זו תכונה של העמודה, לא של התפקיד. השדה `unmasked_to` שולט אילו תפקידים עוקפים אותו. (REQ-249)

| סוג מיסוך | טיפוסים נתמכים | ביטוי SQL |
| ----------- | ---------------- | ---------------- |
| `regex` | מחרוזת (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | כל טיפוס | ערך מילולי (NULL, 0, מותאם אישית) |
| `truncate` | תאריך/Timestamp | `DATE_TRUNC(precision, col)` |

המיסוך נדחף לתוך הפרויקציה של ה-SQL SELECT — מסד הנתונים מחזיר נתונים ממוסכים. (REQ-263) נתונים לא-ממוסכים לעולם לא חוצים את החוט עבור תפקידים ממוסכים. (REQ-263) עמודות ממוסכות חסומות גם מסעיפי `WHERE` ו-`HAVING` (שומר predicate של שכבה 5) כדי למנוע הסקת הערך הלא-ממוסך דרך פילטור. (REQ-263, REQ-531)

## דגימה (Sampling)

כל התפקידים רואים תוצאות מדוגמות (ברירת מחדל: 100 שורות) אלא אם יש להם יכולת `full_results`. (REQ-554) נשלט דרך משתנה הסביבה `PROVISA_SAMPLE_SIZE`. (REQ-554)

## רישום ביקורת (Audit Logging)

כל שאילתה הנוגעת בנכס דומיין נרשמת ב-`query_audit_log` שהוא append-only. (REQ-596, REQ-613) כל שורה לוכדת `tenant_id`, `user_id`, `role_id`, hash‏ SHA-256 של טקסט השאילתה, `table_ids`, `source`, `status_code`, `duration_ms`, ו-`logged_at`. (REQ-596) טקסט השאילתה לעולם אינו מאוחסן מילולית — רק ה-hash שלו. (REQ-596)

היומן הוא append-only ברמת מסד הנתונים: כללי PostgreSQL חוסמים `DELETE` ו-`UPDATE`. (REQ-596, REQ-613) שני אינדקסים — `(tenant_id, logged_at)` ו-`(user_id, logged_at)` — תומכים בשאילתות ציות בטווח-זמן לפי-tenant ולפי-משתמש. (REQ-596, REQ-613)

כאשר ההצפנה מופעלת, עמודת ה-hash של טקסט השאילתה מאוחסנת מוצפנת ומפוענחת רק בקריאות admin מורשות. (REQ-689)

## הגבלת קצב (Rate Limiting)

מגבלות קצב לפי-תפקיד מוגדרות ב-`provisa.yaml`: בקשות מרביות לשנייה, מנויי SSE מקבילים מרביים, וזרמי Arrow Flight מקבילים מרביים. (REQ-369) המגבלות נאכפות בשכבת ה-API לפני הקימפול או הביצוע; בקשות מעבר למגבלה נדחות עם HTTP 429 וכותרת `Retry-After`. (REQ-369)

לשירות שאילתת השפה הטבעית (`POST /query/nl`) יש מגבלה בלתי-תלויה דרך `nl.rate_limit` (בקשות לדקה לפי-תפקיד). בקשות מעבר למגבלה נדחות לפני שנעשית קריאת LLM כלשהי. (REQ-370)

מצב הגבלת הקצב חי ב-Redis (`cache.redis_url`) כמונה חלון-נגלל — אין מצב לפי-מופע — כך שהמגבלות מחזיקות על פני כל מופעי Provisa האופקיים. (REQ-371)

## אימות

ספקי אימות ניתנים-להחלפה: (REQ-120)

| ספק | סוג טוקן | מקרה שימוש |
| ---------- | ----------- | ---------- |
| `none` | כותרת X-Provisa-Role | פיתוח |
| `firebase` | Firebase ID token | ייצור |
| `keycloak` | Keycloak JWT | ארגוני |
| `oauth` | OIDC JWT | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | בדיקות |

מיפוי תפקיד: claims של זהות → תפקיד Provisa דרך כללים ניתנים-להגדרה. (REQ-120) השדה `assignments_source` שולט מהיכן מגיעות הקצאות תפקיד: `claims` קורא אותן מ-claims של טוקן JWT (ברירת מחדל), `provisa` קורא ממאגר ההקצאה הפנימי של Provisa. (REQ-551)

superuser המוגדר ב-`provisa.yaml` (שם משתמש בתוספת סיסמה מסוד סביבה) תמיד מקבל את תפקיד ה-admin וכל היכולות ללא קשר לספק המוגדר — נתיב bootstrap להגדרה ראשונית. (REQ-125)

## Hook אישור ABAC

hook מדיניות חיצוני אופציונלי שמופעל לפני ביצוע השאילתה. (REQ-203) כאשר מוגדר, Provisa קוראת למנוע המדיניות שלכם עם זהות המשתמש, התפקידים, הטבלאות, העמודות, והפעולה. התגובה קובעת אם השאילתה ממשיכה. (REQ-203)

### היקף

ה-hook מופעל רק כאשר השאילתה נוגעת בטבלה או מקור בהיקף — אפס תקורה עבור כל השאר. (REQ-204)

| תצורה | אפקט |
| -------- | ------ |
| `auth.approval_hook.scope: all` | כל שאילתה מפעילה את ה-hook |
| `sources[].approval_hook: true` | כל הטבלאות על אותו מקור מפעילות את ה-hook |
| `tables[].approval_hook: true` | אותה טבלה מפעילה את ה-hook |

### פרוטוקולים

שלוש תעבורות נתמכות: (REQ-246)

| סוג | מקרה שימוש | שדה תצורה |
| ------ | ---------- | ------------- |
| `webhook` | כל שירות מדיניות תומך-HTTP (OPA, מותאם אישית) | `url` |
| `unix_socket` | OPA או sidecar מדיניות על אותה מכונה | `socket_path` + `url` |
| `grpc` | שירות מדיניות תפוקה-גבוהה משותף-מיקום | `url` (host:port) |

תעבורת ה-gRPC משתמשת בחוזה `provisa.auth.ApprovalService` המוגדר ב-`provisa/auth/approval.proto`. יישמו שירות זה במנוע המדיניות שלכם: (REQ-246)

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

ערוץ ה-gRPC הוא קבוע — ערוץ אחד לכל מופע Provisa, נעשה בו שימוש חוזר על פני כל הקריאות לנקודת הקצה של ה-hook הזה. (REQ-555)

### בקשה / תגובה

כל שלוש התעבורות נושאות את אותו מטען (payload): (REQ-246)

| שדה | טיפוס | תיאור |
| ------- | ------ | ------------- |
| `user` | string | זהות המשתמש המאומת |
| `roles` | string[] | תפקידי Provisa של המשתמש |
| `tables` | string[] | מזהי טבלה המוזכרים בשאילתה |
| `columns` | string[] | עמודות שנבחרו בשאילתה |
| `operation` | string | `"query"` או `"mutation"` |

תעבורות ה-webhook ו-Unix socket מחליפות JSON. התגובה חייבת לכלול `approved` (bool) ואופציונלית `reason` (string). (REQ-246)

### Timeout ונפילה-חוזרת

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

ב-timeout או שגיאת תעבורה, מדיניות ה-`fallback` חלה. (REQ-247) מפסק מעגל (circuit breaker) (ברירת מחדל: נפתח לאחר 5 כשלים רצופים, נפתח-למחצה לאחר 30 שנ') מונע כשלים מדורגים מנקודת קצה hook איטית. (REQ-556)

### דוגמת תצורה

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```

## סודות

אישורים משתמשים בתחביר `${env:VAR_NAME}`, הנפתר בזמן ריצה. (REQ-557) סיסמאות לעולם אינן מאוחסנות במסד נתוני התצורה. (REQ-557)
</content>
