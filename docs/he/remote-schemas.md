# סכמות מרוחקות (Remote Schemas)

מקור סכמה מרוחקת מחבר API חיצוני — GraphQL, gRPC, או REST (OpenAPI) — לשכבה הסמנטית של Provisa. לאחר הרישום, הפעולות של ה-API החיצוני הופכות לטבלאות ופונקציות מדרגה-ראשונה של Provisa. (REQ-308, REQ-316, REQ-325) כל כלל ממשל, ממשק שאילתה, ושכבת אבטחה חלים אוטומטית. (REQ-310, REQ-319, REQ-328) השירות המרוחק לעולם אינו רואה את כללי הממשל של Provisa. (REQ-310, REQ-319, REQ-328)

---

## שלושה סוגי מקור

### סכמה מרוחקת של GraphQL (REQ-307–313)

**איך לרשום.** שלחו POST ל-`/admin/sources/graphql-remote` עם כתובת ה-URL של הנקודה הקצה, namespace, ואימות אופציונלי. Provisa יורה שאילתת אינטרוספקציה סטנדרטית `__schema` מול הנקודה המרוחקת. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

אפשרויות אימות: `none`, `bearer` (כותרת Authorization), `basic` (Base64 username:password). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**דריסות שדה (Field overrides).** `field_overrides` היא מפת `{fieldName: "query" | "mutation"}` המוחלת לאחר האינטרוספקציה. יש לה עדיפות על פני סיווג מבני. רק שדות מסוג-query ניתנים לסיווג-מחדש כ-mutations; לשדות מסוג-mutation אין נתיב דריסה ב-GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**קשרים בזמן רישום.** `relationships` מצהיר נתיבי join של FK/PK בין טבלאות בזמן הרישום. אלה נשמרים כקשרים מוצהרים-ידנית (ללא דגל `remote_managed`). ברענון, קשרים שאותרו-אוטומטית (אלה עם `remote_managed: True`) רצים מחדש ועשויים להשתנות; קשרים מוצהרים-ידנית אינם נגעים. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**מה מתגלה אוטומטית.** כל שדה בסוג ה-`Query` המרוחק המחזיר OBJECT הופך לטבלה וירטואלית. כל שדה בסוג ה-`Mutation` המרוחק הופך ל-command עוקב (tracked function). (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**שיוך שמות טבלה.** טבלאות נקראות `{namespace}__{field_name}`. עם namespace‏ `petstore` ושדה שאילתה `pets`: שם הטבלה הוא `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**מיפוי טיפוסים (REQ-308).** שדות סקלריים ממופים ישירות לטיפוסי Provisa. שדות OBJECT מתפצלים לשני מקרים תלוי אם הטיפוס היעד ממושל (ראו "טבלאות ממושלות" למטה). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| טיפוס GraphQL | טיפוס Provisa |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (טיפוס inline לא-ממושל, למשל `ContactInfo`) | עמודת blob מסוג `jsonb` |
| OBJECT (טיפוס יעד-ממושל) | מוחרג לחלוטין מ-SDL ומאיסוף |
| כל ENUM | `jsonb` |
| סקלר מותאם-אישית | `text` (ברירת מחדל) |

**טבלאות ממושלות.** טיפוס GQL הוא ממושל כאשר הוא מופיע כשדה שורש `Query` בסכמה המרוחקת. `_collect_queryable_types` אוסף אלה במהלך הרישום, מעדיף שדות ללא-ארגומנט-נדרש כך שניתן לאסוף אותם בכמות כיעדי join. [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

כאשר עמודה מסוג-OBJECT בטבלה ממושלת מצביעה לטיפוס ממושל אחר, עמודה זו כפופה לשלושה כללים בו-זמנית [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **מוחרגת מהאיסוף של GQL** — השדה אינו מבוקש בעת איסוף שורות הטבלה ההורה.
2. **מוחרגת מה-SDL** — השדה אינו מופיע על הטיפוס ההורה בסכמה הנוצרת.
3. **נגישה רק דרך קשר מוצהר** — סטיוארד חייב לרשום JOIN בין שתי הטבלאות הממושלות הממומשות. בלעדיו, השדה פשוט נעדר; אין fallback ל-blob.

טיפוסי OBJECT שאינם ניתנים-להשגה כשדות שורש Query (טיפוסים inline כמו `ContactInfo` או `Address`) עוקבים אחר כללים שונים: הם נאספים כעמודות blob מסוג `jsonb` ומופיעים ב-SDL כשדות אובייקט-מקונן. שדות-משנה נגישים דרך חילוץ `-->>` ב-SQL.

**ארגומנטים נדרשים.** כאשר לשדה שאילתה-שורש יש ארגומנטים לא-null ללא ערך ברירת-מחדל, אלה הופכים לעמודות `native_filter_type: query_param` על הטבלה (מוקדמות ב-`_nf_` בזמן הזרקה). ה-executor מעביר אותן כמשתני GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**קשרים מזוהים אוטומטית.** Provisa סורקת את עמודות ה-OBJECT של כל טבלה. כאשר טיפוס ה-GQL המופנה רשום גם הוא כטבלה באותו מקור, קשר נפלט. קשרי many-to-one גוזרים עמודות מקור ויעד ממוסכמות שמות (`breedName` על טיפוס המקור → `name` על טיפוס היעד `Breed`). שדות one-to-many‏ (LIST) פולטים קשרים עם הפניות עמודה ריקות — ה-FK חי בצד היעד. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutations.** שדות mutation מייצרים commands עוקבים עם טיפוסי ארגומנט ממופים מארגומנטי ה-mutation ו-`return_schema` הנגזר מטיפוס ההחזרה של ה-mutation. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**רענון (Refresh).** שלחו POST ל-`/admin/sources/graphql-remote/{id}/refresh`. מבצע אינטרוספקציה מחדש של הסכמה המרוחקת ומעדכן רישומי טבלה ופונקציה. כללי ממשל קיימים (RLS, מיסוך) נשמרים. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**מגבלות.**

- שדות שאילתה-שורש סקלריים ו-ENUM (טיפוס ההחזרה אינו OBJECT) הופכים ל-commands עוקבים, לא לטבלאות וירטואליות. ה-`return_schema` שלהם הוא עמודת `value` יחידה מהטיפוס הסקלרי הממופה. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- קינון אובייקטים נפתר בזמן הרישום עד `graphql_remote.max_object_depth` (ברירת מחדל: 5). הן בחירת האיסוף המרוחק והן מטא-דאטת שדה-המשנה נבנות עד עומק זה; שדות מעבר לגבול אינם נאספים ואינם זמינים לחילוץ SQL. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- שדות OBJECT מקוננים מסוג-LIST (למשל `breed.awards: [Award]`) נכללים בבחירת האיסוף עד רמות קינון `graphql_remote.max_list_depth` (ברירת מחדל: 2). בתוך גבול זה, הרשימה נאספת כמערך `jsonb` על העמודה ההורה, ובחירת ה-GQL מזריקה `first: N` כאשר N הוא `graphql_remote.max_list_items` (ברירת מחדל: 100) כדי לגבול את גודל המערך. מעבר ל-`max_list_depth`, שדה ה-LIST מוחרג לחלוטין כדי למנוע הרחבת נתונים בלתי-מוגבלת. ב-SQL, המערך נגיש דרך `json_array_elements(column_name)` או חילוץ אינדקס `->>`. אם לטיפוס פריט הרשימה יש query שורש משלו, רשמו אותו כטבלה נפרדת וצרו קשר במקום זאת — נתיב ה-join יעיל יותר ועוקף את ה-blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- עבור שאילתות SQL, עמודות מסוג-OBJECT לא-ממושלות נאספות במלואן מהמרוחק (כל שדות-המשנה עד העומק המוגדר) ונשמרות במטמון כ-`jsonb`. גישה לשדה-משנה ב-SQL מטופלת דרך חילוץ `->>` מול ה-blob; הבקשה המרוחקת אינה מצומצמת רק לשדות ששאילתת ה-SQL בוחרת. כאשר לטיפוס פריט-הרשימה אין query שורש וייצוג ה-blob אינו מספיק, כתבו את השאילתה ב-SDL של GraphQL ישירות — Provisa משחזרת בנאמנות את בחירת שדה ה-GQL, כך שהמרוחק רואה בדיוק את השדות המבוקשים. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- אם השרת המרוחק דוחה שדה מסוג-OBJECT מכיוון שהוא דורש בחירת שדה-משנה (מה שלא אמור לקרות כאשר `gql_selection` זמין), ה-executor מנסה שוב פעם אחת עם שדות אלה מוסרים כך שעמודות סקלריות עדיין מוחזרות. [tool-verified: `provisa/graphql_remote/executor.py:76–80`]

---

### סכמה מרוחקת של gRPC (REQ-322–329)

**איך לרשום.** שלחו POST ל-`/admin/grpc-remote/register` עם כתובת השרת, נתיב או URL לקובץ `.proto`, ותצורת TLS אופציונלית.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa מביאה את ה-proto, מפענחת אותו עם parser טקסט-טהור (ללא תלויות proto חיצוניות בזמן פענוח), מקמפלת stubs של Python דרך `grpc_tools.protoc`, ופותחת `grpc.aio.Channel` מתמיד. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

קבצי proto יכולים להיות גם נתיבים מקומיים. נתיבי import עבור טיפוסים מוכרים-היטב (`google/protobuf/timestamp.proto`) נשמרים בזמן הרישום ונעשה בהם שימוש חוזר ברענון. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**מה מתגלה אוטומטית.** כל שיטת `rpc` ב-proto מסווגת כ-query או mutation באמצעות שלושה אותות בסדר עדיפות: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** במטען הרישום — `{"MethodName": "query"}` או `{"MethodName": "mutation"}` דורס הכל.
2. **`server_streaming: true`** — השרת שולח stream של הודעות; תמיד טבלה וירטואלית (אלא אם הפלט הוא סקלר).
3. **הודעת פלט בעלת שדה מסוג-הודעה חוזר** — למשל `ListOrdersResponse { repeated Order items; }` נחשבת ל-list-wrapper והופכת לטבלה וירטואלית. שדות סקלריים חוזרים (למשל `repeated string tags`) אינם מפעילים זאת — הם תכונות מערך על ישות בודדת, לא מקורות שורה.

שיטות שאינן תואמות אף אחד מהאותות הללו (RPC יוני-קאסטי המחזיר הודעת ישות בודדת, או כל פלט סקלרי) הופכות ל-commands עוקבים.

**שיוך שמות טבלה.** השם ברירת המחדל הוא `{namespace}__{ServiceName}__{MethodName}`. ללא namespace, שמות השירות והשיטה מחוברים ישירות. לכל טבלה רשומה ניתן לתת `alias`; כשמוגדר, ה-alias הוא השם המשמש בכל מקום (שאילתות, SDL, קשרים). השם שנוצר-אוטומטית הוא מפתח הרישום ולעולם אינו משתנה. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**מיפוי טיפוסים (REQ-324).** טיפוסים סקלריים של proto ממופים לטיפוסי SQL כך. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| טיפוס Proto | טיפוס SQL |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| הודעה מקוננת | `jsonb` |
| Enum | `text` |

**קשרים בזמן רישום.** `relationships` פועל זהה למתאם ה-GQL — מצהיר נתיבי join של FK/PK הנשמרים כקשרים מוצהרים-ידנית (ללא דגל `remote_managed`). ברענון, אלה נשמרים ללא שינוי. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**שיטות Query (REQ-325).** שדות הודעת פלט הופכים לעמודות טבלה. שדות הודעת קלט הופכים הן לארגומנטי GraphQL המועברים לקריאה המרוחקת *והן* נרשמים כעמודות מוקדמות-`_nf_` עם `native_filter_type: "grpc_input"` — אותו מנגנון ש-GQL ו-OpenAPI משתמשים בו עבור הזרקת פילטר-ילידי. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**שדות-משנה של הודעה מקוננת.** עבור שיטות query, שדות מסוג-הודעה שאינם-חוזרים בעומק 0 (עמודות פלט ישירות) פותרים את שדות-המשנה שלהם רמה אחת עמוקה ונשמרים כ-`object_fields` על ה-`ColumnDef`. מטא-דאטה זו משמשת לחילוץ שדה-משנה מסוג-`jsonb` ב-SQL ולתיעוד סכמה. שדות מקוננים מעבר לעומק 1 אינם מורחבים באופן רקורסיבי. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

שיטות server-streaming אוספות את כל ההודעות ה-streamed לרשימה לפני החזרת שורות. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**שיטות Mutation (REQ-326).** שדות הודעת קלט הופכים לארגומנטי קלט mutation. סכמת הודעת הפלט הופכת ל-`return_schema`. [tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**ניהול ערוץ (Channel).** `grpc.aio.Channel` אחד לכל מקור רשום נשמר במצב האפליקציה ונעשה בו שימוש חוזר על פני בקשות. הערוץ הישן נסגר לפני שהחדש נפתח ברענון. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**רענון.** שלחו POST ל-`/admin/grpc-remote/refresh/{source_id}`. טוען מחדש את ה-proto מהנתיב השמור, מקמפל-מחדש stubs, ורושם-מחדש טבלאות ופונקציות. לחלופין, שלחו PUT ל-`/admin/grpc-remote/{source_id}/proto` עם `proto_text` חדש כדי לעדכן את ה-proto inline. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`, `provisa/api/admin/grpc_remote_router.py:300–358`]

**מגבלות.**

- חילוץ אובייקט שדה-משנה הוא רמה אחת עמוקה. שדות הודעה מקוננים מעבר לעומק 1 אינם מורחבים באופן רקורסיבי. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**איך לרשום.** קראו ל-`auto_register_openapi_source` עם מזהה מקור, spec מפוענח, ומטא-דאטת חיבור. ה-spec נטען מקובץ מקומי או URL. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/openapi/register.py:249–264`]

**מטען רישום.** נקודת הקצה `/admin/openapi/register` מקבלת שני שדות נוספים לצד `source_id`, `spec_path` וכו':

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**מה מתגלה אוטומטית.** כל פעולת GET ב-spec הופכת לטבלה וירטואלית, אלא אם סכמת התגובה שלה היא טיפוס סקלרי (`string`, `number`, `boolean`, `integer`) — GET-ים המחזירים סקלר הופכים ל-commands עוקבים עם עמודת `value` יחידה במקום זאת. כל פעולה שאינה-GET (POST, PUT, PATCH, DELETE) הופכת ל-command עוקב. (REQ-316, REQ-317)

עדיפות סיווג: `operation_overrides` (מטען) דורס `x-provisa-kind` (הרחבת spec) דורס את היוריסטיקת ה-GET. `operation_overrides` הוא נתיב הדריסה המומלץ; `x-provisa-kind` מיועד למקרים בהם ה-spec עצמו אמור לשאת את הסיווג. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**קשרים בזמן רישום.** `relationships` פועל זהה למתאמים האחרים — נשמר כקשרים מוצהרים-ידנית, נשמר ברענון. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**שיוך שמות טבלה.** טבלאות משתמשות ב-`operationId` של הפעולה. אם `operationId` אינו מוגדר, Provisa בונה slug מ-`{method}_{path}`. alias נגזר על ידי הסרת קטע הפועל המוביל ויחוד השם-העצם (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**מיפוי טיפוסים.** טיפוסי JSON Schema ממופים לטיפוסי Provisa כך. [tool-verified: `provisa/openapi/register.py:59–70`]

| טיפוס JSON Schema | טיפוס Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**פרמטרים כעמודות פילטר-ילידי.** פרמטרי נתיב ו-query שאינם כבר שדות תגובה הופכים לעמודות עם `native_filter_type` מוגדר ל-`path_param` או `query_param`, מוקדמים ב-`_nf_`. כאשר שם פרמטר תואם שם שדה תגובה, מטא-דאטת הפרמטר ממוזגת לרשומת העמודה הקיימת במקום ליצור כפילות. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**פתירת סכמת תגובה.** ה-mapper בודק `responses.200`, לאחר-מכן `responses.2xx`, לאחר-מכן `responses.default`. תגובות מסוג-array נפתחות לסכמת הפריט שלהן. הפניות `$ref` נפתרות רמה אחת עמוקה. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**שדות-משנה של אובייקט.** תכונות תגובה מסוג `type: object` בעלות `properties` משלהן נשמרות כ-`object_fields` על העמודה. שדות-משנה אלה גלויים ב-SDL ומשמשים לחילוץ `jsonb` בשאילתות. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**מטמון תגובה (REQ-318).** תוצאות פעולת GET נשמרות במטמון ב-PostgreSQL על ידי `pg_cache.py`. כל צירוף של פרמטרי בקשה מקבל קבוצת `_params_hash` משלו. שורות עבור hash נתון מוחלפות כאשר ה-TTL פג. נקודות קצה של פרמטר-נתיב (`/pets/{id}`) מדלגות על האיסוף הראשוני בכמות — טבלת המטמון נוצרת ריקה עבור אינטרוספקציית סכמה, ואז מאוכלסת לפי-PK כאשר בקשות מגיעות. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**רענון (REQ-321).** פענחו-מחדש את ה-spec וקראו שוב ל-`auto_register_openapi_source`. כללי ממשל קיימים נשמרים; רישומים מתעדכנים עם upsert‏ ON CONFLICT. [tool-verified: `provisa/openapi/register.py:249–264`]

**מגבלות.**

- חילוץ אובייקט שדה-משנה הוא רמה אחת עמוקה. תכונות מקוננות בתוך `object_fields` אינן מורחבות באופן רקורסיבי. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- פרמטרי header ו-cookie מתעלמים; רק פרמטרי `path` ו-`query` נרשמים. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- פתירת `$ref` ברמת ה-spec היא רמה אחת עמוקה עבור סכמות תכונה; הפניות רכיב מקוננות-עמוק עשויות לא להיפתר. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## ההשפעה של רישום טבלה מרוחקת

טבלה הרשומה מכל מקור סכמה מרוחקת היא טבלת Provisa מדרגה-ראשונה. שום דבר בה אינו מטופל אחרת מטבלה יחסית מחוברת-מקומית בזמן ריצה. (REQ-308, REQ-313)

**ממשקי שאילתה.** הטבלה ניתנת לשאילתה מיידית דרך GraphQL, SQL‏ (pgwire או ישיר), Cypher‏ (GQL), JSON:API, ו-Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) יצירת סכמה מסנתזת `ColumnMetadata` עבור טבלאות מרוחקות מכיוון שאין להן קטלוג — מיפוי טיפוסים מוחל בזמן בניית הסכמה. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**מודל אבטחה.** כל חמש שכבות הממשל חלות:

1. בקרת גישת דומיין — ה-`domain_id` של הטבלה מסייג אילו תפקידים יכולים לראות אותה. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. אבטחה ברמת-שורה (RLS) — פילטרי שורה המוגדרים על הטבלה מוזרקים לכל שאילתה, ללא קשר לממשק. (REQ-040, REQ-041)
3. נראות עמודה — רשימת `visible_to` על כל עמודה שולטת בחשיפת שדה לפי-תפקיד. (REQ-039)
4. מיסוך עמודה — כללי מיסוך חלים בשלב 2 של צינור הממשל. (REQ-040, REQ-263)
5. שומר פרדיקט — עמודות ממוסכות נדחות מסעיפי WHERE ו-HAVING. (REQ-603)

שאילתות אד-הוק מול טבלאות מרוחקות מותרות תחת זכויות המשתמש בלבד — הגישה אחידה מבוססת-זכויות (זכויות טבלה/עמודה + קשרים מאושרים), ללא מצב ממשל לכל-טבלה. (REQ-001, REQ-003)

**ממשל קשרים (V002).** תנאי JOIN מול טבלאות מרוחקות — כאשר נשאלים דרך SQL או Cypher — חייבים להתאים לקשר רשום ומאושר. (REQ-604) בדיקת V002 מדולגת עבור שאילתות GraphQL מכיוון שקשרים מוגדרי-SDL מאושרים-מראש מעצם התכנון. ראו [docs/security.md](security.md#v002).

**עמודות מסוג-OBJECT.** כאשר עמודה ממופה לטיפוס OBJECT‏ inline לא-ממושל של GQL או OpenAPI, טיפוס ה-Provisa שלה הוא `jsonb`. העמודה שומרת את ה-blob המלא של ה-JSON המקונן. כאשר שדות-משנה מוצהרים (`gql_object_fields` או `object_fields`), מפת `gql_object_columns` מאוכלסת בזמן בניית הסכמה. מחולל ה-SQL משתמש במפה זו כדי לפלוט ביטויי חילוץ `->>` עבור שדות-משנה כאשר שאילתה בוחרת אותם. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**ארגומנטים נדרשים כפרמטרי פילטר-ילידי.** שדות שאילתה-שורש עם ארגומנטים לא-null וללא-ברירת-מחדל מזריקים עמודות נוספות לטבלה הרשומה. עמודות אלה נושאות `native_filter_type: query_param`. מתרגם ה-Cypher כותב מחדש `WHERE n.id = $val` ל-`WHERE n._nf_id = $val`, ו-executor‏ ה-GraphQL אוסף אותן כמשתנים להעברה לנקודה הקצה המרוחקת. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## ההשפעה של יצירת קשר מכסה (covering relationship)

כאשר סטיוארד רושם קשר בין שתי טבלאות מרוחקות (או בין טבלה מרוחקת לטבלה מקומית), הקשר הופך לנתיב ה-join המשמש בזמן שאילתה.

**איך ה-join מנצח.** בקימפול שאילתה, Provisa פותרת את נתיב ה-join דרך הקשר הרשום. `source_column` ו-`target_column` על הקשר הופכים לתנאי ה-join ב-SQL הנוצר. ה-join מחליף כל קריאה מרוחקת לכל-טבלה שהייתה אחרת נדרשת עבור הטיפוס המחובר.

**ה-blob הגולמי לעולם אינו נחשף ב-SQL.** עמודת `breed` על `petstore__pets` אינה ניתנת-לבחירה כערך jsonb גולמי בשאילתות SQL. כאשר קשר נרשם בין `petstore__pets` ל-`petstore__breeds`, שאילתות SQL חוצות את ה-join — `SELECT breed.name FROM petstore__pets` נפתר דרך ה-join של FK, לא blob. כאשר לא נרשם קשר אך לעמודה יש שדות-משנה מוצהרים (`gql_object_fields`), הפניות שדה-משנה של SQL נכתבות מחדש לחילוץ `->>` מול ה-blob השמור. נתיב זה זמין רק עבור טיפוסים inline לא-ממושלים — שדות יעד-ממושלים מוחרגים מה-SDL לחלוטין ואין להם blob לחלץ ממנו. ה-blob הגולמי עצמו לעולם אינו נפלט כערך עמודה גולמי. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

ב-SDL של GraphQL, שדה OBJECT‏ inline לא-ממושל מוקלד כטיפוס האובייקט המקונן. האם הוא מוגש על ידי join או על ידי חילוץ blob בזמן הביצוע הוא פרט מימוש — צורת ה-SDL זהה בכל מקרה. כאשר הטיפוס-הבן רשום כטבלה משלו (והופך ממושל), כל חמש שכבות הממשל חלות עליו באופן עצמאי: כללי ה-RLS שלו, נראות עמודה, כללי מיסוך, שומרי פרדיקט, ובקרת גישת דומיין. (REQ-039, REQ-040, REQ-041, REQ-263) חילוץ blob עוקף זאת — נתוני הבן מגיעים מוטמעים-מראש בשורת ההורה ונשלטים רק על ידי כללי הטבלה ההורה. רישום הבן כטבלה ויצירת קשר הוא הנתיב לממשל עדין-פירוט על טיפוס הבן.

**`graphql_alias` על הקשר.** שדה `graphql_alias` נותן שם לשדה ה-SDL שהקשר חושף על הטיפוס ההורה. כשנעדר, השם נגזר מ-`field_name` של טבלת היעד ומעוצמת (cardinality) הקשר דרך `rel_field_name(target.field_name, cardinality)`. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 על נתיב ה-join.** שאילתות SQL ו-Cypher החוצות את הקשר כפופות לממשל קשרים V002. הקשר חייב להיות רשום ומאושר כדי שה-join יורשה. (REQ-604) חצייה דרך שדה קשר SDL של GraphQL תמיד מאושרת-מראש. [tool-verified: `docs/security.md:41–54`]

**דגל remote-managed.** קשרים שאותרו-אוטומטית במהלך רישום מרוחק של GraphQL נשמרים עם `remote_managed: True`. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] זהו סמן מטא-דאטה; הוא אינו משנה התנהגות ממשל.

---

## התנהגות type-def-only

לא כל טיפוס בסכמה מרוחקת צריך להיות טבלה הניתנת-לשאילתה.

כאשר `root_table_ids` מוגדר על `SchemaInput`, טבלאות שמזהיהן נעדרים מאותה קבוצה מוחרגות משדות שאילתת השורש ב-SDL הנוצר. הן נשארות נוכחות כטיפוסי GraphQL וניתנות להשגה דרך שדות קשר על טבלאות שכן יש להן רשומות שורש. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

אותו מנגנון חל על builds סכמה מסוננים-דומיין: טבלאות בדומיינים שהתפקיד אינו יכול לגשת אליהם הן type-def בלבד — הגדרת הטיפוס שלהן קיימת ב-SDL עבור חצייה בקשר, אך אין שדה שאילתת שורש נוצר עבורן. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

טבלת type-def-only:

- אין לה שדה שאילתת שורש — לקוחות אינם יכולים לשאול אותה ישירות לפי שם.
- ניתנת להשגה דרך שדות קשר על טבלאות שכן יש להן רשומות שורש.
- עדיין מופיעה באינטרוספקציית סכמה כטיפוס בעל-שם.
- עדיין חלים עליה כל כללי הממשל כאשר הנתונים ניגשים דרך קשר. (REQ-039, REQ-040)

הסרה מלאה מהסכמה — כולל הגדרת הטיפוס — קורית רק כאשר רישום הטבלה נמחק לחלוטין. סימון טבלה כ-type-def-only (על ידי הסרת המזהה שלה מ-`root_table_ids` או על ידי סינון על גישת דומיין) אינו מסיר את הטיפוס.

עיצוב זה מאפשר לסטיוארדים לחשוף גרפי אובייקט ניתנים-לניווט שבהם חלק מהטיפוסים ניתנים-להשגה רק דרך חצייה, לא דרך שאילתה עצמאית.
