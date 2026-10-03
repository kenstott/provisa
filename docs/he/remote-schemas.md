# סכמות מרוחקות (Remote Schemas)

מקור סכמה מרוחקת מחבר API חיצוני — GraphQL (כולל GitHub), gRPC, או REST (OpenAPI) — לשכבה הסמנטית של Provisa. הוספת מקור אינה רושמת אף טבלה. המקור מציע טבלאות, וסטיוארד רושם כל טבלה רצויה דרך בורר Register Table; רישום זה הוא שלב הקיורציה (curation). (REQ-308, REQ-316, REQ-322) טבלה רשומה היא טבלת Provisa מדרגה-ראשונה. (REQ-308, REQ-316, REQ-325) כל כלל ממשל, ממשק שאילתה, ושכבת אבטחה חלים אוטומטית. (REQ-310, REQ-319, REQ-328) השירות המרוחק לעולם אינו רואה את כללי הממשל של Provisa. (REQ-310, REQ-319, REQ-328)

---

## שלושה סוגי מקור

### סכמה מרוחקת של GraphQL (REQ-307–313)

**איך להוסיף את המקור.** שלחו POST ל-`/admin/sources/graphql-remote` עם כתובת ה-endpoint, namespace, ואימות אופציונלי. Provisa מפעילה שאילתת אינטרוספקציה סטנדרטית `__schema` מול ה-endpoint המרוחק כדי לאשר את ה-endpoint ואת האישורים. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

הוספת המקור אינה רושמת טבלה ואינה רושמת command. כל סוג מקור מרוחק עונה להוספה ולרענון באותם מונים: `tables` (טבלאות שנרשמו או שהובאו להתאמה; 0 בהוספה), `available_tables` (טבלאות מוצעות), `mutations` (תמיד 0) ו-`available_mutations` (commands מוצעים). [tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**רישום טבלאות.** פתחו Tables, אחר כך Register Table, בחרו את המקור ואת הסכמה `graphql`, ובחרו את הטבלאות והעמודות הרצויות. דרך ה-API של GraphQL לניהול: `availableTables(sourceId, schemaName)` מפרט את הטבלאות המוצעות, `availableColumns` מפרט את עמודות הטבלה, ו-`registerTable(input: TableInput)` רושם טבלה עם העמודות שנבחרו. טבלה רשומה כפופה לממשל. (REQ-308) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`] [tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

האופן שבו נקראת טבלה רשומה (שדה שורש, נתיב שורות, ארגומנטים נדרשים, ארגומנטי עימוד) נשמר ב-`sources.mapping["tables"]`, כך שתהליך שהופעל מחדש קורא אותה בלי לשאול את השירות המרוחק על הסכמה שלו. [tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

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

**מה המקור מציע.** כל שדה מסוג `Query` המרוחק שמחזיר אובייקט או רשימת אובייקטים מוצע כטבלה, וכך גם כל חיבור Relay תחת שדה של אובייקט יחיד (ראו להלן). רישום טבלה מוצעת הופך אותה לטבלה. כל שדה מסוג `Mutation` המרוחק הוא פקודה מוצעת, הנספרת ב-`available_mutations`; הוספת המקור אינה רושמת אף אחת. רשמו את הרצויות כפקודות; ראו [פעולת כתיבה של מקור מרוחק](commands.md#a-remote-sources-write-operation-req-1924). (REQ-308, REQ-1924) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**שיוך שמות טבלה.** טבלאות נקראות `{namespace}__{field_name}`. עם namespace `petstore` ושדה שאילתה `pets`: שם הטבלה הוא `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**חיבורי Relay.** ממשקי API רבים מחזירים רשימות כחיבורי Relay: אובייקט עם `nodes` (או `edges { node }`) לצד `pageInfo`. Provisa ממפה חיבור לטבלה של הצמתים שלו וקוראת אותו עמוד אחר עמוד. (REQ-308, REQ-309) [tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- שדה שורש המחזיר חיבור (`securityAdvisories`) הופך לטבלה אחת של הצמתים שלו.
- חיבור על האובייקט היחיד שמחזיר שדה שורש הופך לטבלה משלו. הטבלה לוקחת את הארגומנטים הנדרשים של שדה השורש. עם `repository(owner, name)` וחיבור `issues` על `Repository`, הטבלה היא `repositoryIssues`, שם ה-SQL הוא `gh__repository_issues` תחת ה-namespace `gh`. סננו אותה דרך העמודות `_nf_owner` ו-`_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- חיבור אינו אף פעם עמודה. אחרת שורה הייתה נושאת קריאה שהשירות המרוחק מחשב לכל שורה, עבור כל חיבור שיש לטיפוס שלה.
- חיבור הוא טבלה רק אם השדה שלו מקבל `first` ו-`after`, כך שאפשר לקרוא אותו עמוד אחר עמוד. חיבור שדורש ארגומנט משלו אינו טבלה. וגם לא חיבור של union, או כל חיבור תחת שדה שורש המחזיר רשימה.

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

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

**שדות הדורשים ארגומנט אינם עמודות.** שדה עם ארגומנט נדרש לא ניתן לבחירה חשופה, ולכן הוא נשאר מחוץ לעמודות הטבלה ומחוץ לבחירות מקוננות. [tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**ארגומנטים נדרשים.** כאשר לשדה שאילתה-שורש יש ארגומנטים לא-null ללא ערך ברירת-מחדל, אלה הופכים לעמודות `native_filter_type: query_param` על הטבלה (מוקדמות ב-`_nf_` בזמן הזרקה). ה-executor מעביר אותן כמשתני GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**קשרים מזוהים אוטומטית.** Provisa סורקת את עמודות מסוג-OBJECT של כל טבלה רשומה. כאשר טיפוס ה-GQL המוזכר הוא גם טבלה רשומה באותו מקור, והעמודה שעליה נשען הקשר נמנית עם העמודות הרשומות, הקשר נשמר. טבלה שטרם נרשמה אינה מקבלת קשרים. [tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`] קשרי many-to-one מסיקים עמודות מקור ויעד מכללי שמות (`breedName` בטיפוס המקור ← `name` בטיפוס היעד `Breed`). שדות one-to-many (LIST) מפיקים קשרים עם הפניות עמודה ריקות — המפתח הזר שוכן בצד היעד. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutations.** שדה mutation נרשם כפקודה מסוג `source_operation`, אחד אחד. הארגומנטים שלה מוגדרים כל אחד כ-`json` ומועברים לשירות המרוחק כמשתנים מוגדרי סוג; התשובה היא ה-JSON שהשירות המרוחק מחזיר, ללא `return_schema`. ראו [פעולת כתיבה של מקור מרוחק](commands.md#a-remote-sources-write-operation-req-1924). (REQ-1924) [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**רענון (Refresh).** שלחו POST ל-`/admin/sources/graphql-remote/{id}/refresh`. מבצע אינטרוספקציה מחדש של הסכמה המרוחקת ומביא את הטבלאות שכבר נרשמו להתאמה אליה. הוא אינו מוסיף טבלה או עמודה: טבלה או עמודה שהסכמה הוסיפה נשארת מוצעת, ועמודה שהסכמה איבדה מוסרת. כללי ממשל קיימים (RLS, מיסוך) נשמרים. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**מגבלות.**

- שדות שאילתה-שורש סקלריים ו-ENUM (טיפוס ההחזרה אינו OBJECT) הופכים ל-commands עוקבים, לא לטבלאות וירטואליות. ה-`return_schema` שלהם הוא עמודת `value` יחידה מהטיפוס הסקלרי הממופה. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- קינון אובייקטים נפתר בזמן הרישום עד `graphql_remote.max_object_depth` (ברירת מחדל: 5). הן בחירת השליפה המרוחקת והן מטא-נתוני שדות-המשנה נבנים עד לעומק זה; שדות מעבר לגבול אינם נשלפים ואינם זמינים לחילוץ ב-SQL. טיפוס נכנס פעם אחת לאורך כל נתיב: שדה שהטיפוס שלו כבר נמצא בדרך למטה מושמט, כך שסכמה שהטיפוסים שלה מפנים זה לזה נסרקת פעם אחת לכל טיפוס, ולא פעם אחת לכל רמת עומק. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- שדות OBJECT מקוננים מסוג-LIST (למשל `breed.awards: [Award]`) נכללים בבחירת השליפה עד `graphql_remote.max_list_depth` רמות קינון (ברירת מחדל: 2). בגבול זה הרשימה נשלפת כמערך `jsonb` בעמודת האב. כאשר שדה הרשימה מצהיר על ארגומנט `first` (Relay, PostGraphile, pg_graphql) או על ארגומנט `limit` (Hasura), הבחירה מעבירה אותו כ-`first: N` או `limit: N`, כאשר N הוא `graphql_remote.max_list_items` (ברירת מחדל: 100). שדה רשימה שאינו מצהיר על אף אחד מהם אינו מקבל ארגומנט, מפני ששירות מרוחק דוחה ארגומנט שהשדה אינו מצהיר עליו. מעבר ל-`max_list_depth`, שדה ה-LIST מוחרג לחלוטין כדי למנוע התפשטות נתונים בלתי מוגבלת. ב-SQL, ניגשים למערך דרך `json_array_elements(column_name)` או חילוץ אינדקס עם `->>`. אם לטיפוס הפריט של הרשימה יש שאילתת שורש משלו, רשמו אותו במקום זאת כטבלה נפרדת וצרו קשר — נתיב ה-join יעיל יותר ועוקף את ה-blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- עבור שאילתות SQL, עמודות מסוג-OBJECT לא-ממושלות נאספות במלואן מהמרוחק (כל שדות-המשנה עד העומק המוגדר) ונשמרות במטמון כ-`jsonb`. גישה לשדה-משנה ב-SQL מטופלת דרך חילוץ `->>` מול ה-blob; הבקשה המרוחקת אינה מצומצמת רק לשדות ששאילתת ה-SQL בוחרת. כאשר לטיפוס פריט-הרשימה אין query שורש וייצוג ה-blob אינו מספיק, כתבו את השאילתה ב-SDL של GraphQL ישירות — Provisa משחזרת בנאמנות את בחירת שדה ה-GQL, כך שהמרוחק רואה בדיוק את השדות המבוקשים. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- אם השרת המרוחק דוחה שדה מסוג-OBJECT מכיוון שהוא דורש בחירת שדה-משנה (מה שלא אמור לקרות כאשר `gql_selection` זמין), ה-executor מנסה שוב פעם אחת עם שדות אלה מוסרים כך שעמודות סקלריות עדיין מוחזרות. הדבר חל על טבלאות הנקראות משדה שורש. טבלת חיבור אינה עוברת בנתיב זה. [tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**קריאות מעומדות.** טבלת חיבור נקראת באמצעות סמן (cursor). כל עמוד מבקש `first: N, after: $pageCursor` עם `pageInfo { hasNextPage endCursor }`, והקריאה עוקבת אחר `endCursor` עד שהשירות המרוחק מדווח שאין עמוד הבא. (REQ-309) [tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| הגדרה | ברירת מחדל | השפעה |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | שורות לכל עמוד. [tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | המספר הגדול ביותר של שורות שקריאה אחת של טבלת חיבור לוקחת. קריאה שמגיעה אליו נעצרת ורושמת אזהרה ביומן. [tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

שתי תגובות גורמות ל-executor לנסות שוב:

- **עמוד כבד מדי.** כאשר השירות המרוחק עונה 502 או 504, אותו עמוד מתבקש שוב בחצי מהגודל, עד שורה אחת. [tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **הגבלת קצב עם זמן המתנה.** כאשר השירות המרוחק עונה 403 או 429 עם `Retry-After` של 120 שניות או פחות, ה-executor ממתין פרק זמן זה ושולח את הבקשה שוב, עד שלושה ניסיונות. סירוב ללא `Retry-After`, או כזה שמבקש המתנה ארוכה יותר, מועלה כשגיאה. הדבר חל על כל קריאה, חיבור או לא. [tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

כל שגיאה אחרת בתגובה מכשילה את הקריאה, אלא אם סוג המקור קובע אחרת (ראו GitHub להלן). לחיבור שהאב שלו חזר null אין שורות. [tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub (REQ-1923)

GitHub הוא סוג מקור רגיל. ה-API שלו הוא GraphQL, ולכן הטבלאות שלו מתנהגות כמתואר לעיל, כולל טבלאות חיבור כגון `gh__repository_issues`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**הוספת המקור.**

1. פתחו Sources והוסיפו מקור מסוג **GitHub**.
2. הזינו אסימון גישה (token) של GitHub. אפשר להזין namespace, הקידומת של שמות הטבלאות; ברירת המחדל היא `gh`.
3. שמרו. Provisa בודקת את האסימון מול GitHub. אסימון ש-GitHub דוחה מכשיל את ההוספה עם ההודעה של GitHub.

הוספת המקור אינה רושמת טבלאות. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**רישום טבלאות.** פתחו Tables, אחר כך Register Table. בחרו את מקור GitHub, בחרו את הסכמה `graphql`, ואז בחרו את הטבלאות הרצויות. כל טבלה ש-GitHub מציע מופיעה ברשימה; הרישום הוא בחירתכם מה לחשוף. [tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`] [inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**היקפי הרשאה (scopes) של האסימון.** כאשר אתם רושמים טבלה, Provisa בודקת אותה פעם אחת מול GitHub עם האסימון שלכם.

- שדה שהיקפי האסימון אינם מכסים מושמט מהטבלה. התוצאה מנקבת כל שדה שהושמט: `Left out, because the source's credential may not read them: projectsV2`. [tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- טבלה שהאסימון אינו יכול לקרוא כלל נדחית, עם הסיבה של GitHub: `GitHub does not let this source's credential read gh__repository_issues: ...`. [tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**שורות שהאסימון אינו רשאי לראות.** GitHub עונה `FORBIDDEN` עבור שדה שהאסימון אינו רשאי לראות בשורה מסוימת, כגון משתפי הפעולה של מאגר ללא הרשאת push, ו-`NOT_ORG_OWNED_REPO` עבור שדה שקיים רק במאגרים בבעלות ארגון. שדה זה הוא null באותה שורה, שאר הקריאה נמשכת, ו-Provisa רושמת אזהרה ביומן. שגיאה כלפי הטבלה עצמה מכשילה את הקריאה. [tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**עמודים כבדים.** כאשר GitHub עונה `RESOURCE_LIMITS_EXCEEDED` מפני שחישוב עמוד יקר מדי, העמוד מתבקש שוב בחצי מהגודל. [tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**אובייקטים מקוננים.** טבלאות GitHub משתמשות בעומק קינון משלהן, 0 (`max_object_depth=0` עבור סוג מקור זה), ולא ב-`graphql_remote.max_object_depth`. עמודת אובייקט מקונן נבחרת עם שדות הסקלר שלה בלבד; אובייקטים שבתוכה מוצגים כ-`__typename`. [tool-verified: `brands.py`]

**אחסון האסימון.** האסימון נשמר בכספת הסודות, ושורת המקור שומרת הפניה, כך שהפעלה מחדש קוראת את המקור שוב בלי להזין את האסימון מחדש. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**איך זה עובד (למפעילים).** הסכמה של GitHub נשלחת עם Provisa, ולכן הוספת המקור אינה מבצעת קריאת אינטרוספקציה, וסכמה גדולה אינה עולה דבר ברישום. טבלאות ממופות ממנה אחת אחת עם רישומן. נקודת הקצה refresh מסרבת לסוג מקור זה; סכמת GitHub חדשה מגיעה עם גרסה של Provisa. [tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note] [tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab (REQ-1923)

GitLab הוא סוג מקור רגיל ונוסף ונרשם באותו אופן כמו GitHub: הוסיפו מקור מסוג **GitLab** עם אסימון גישה, ואז רשמו את הטבלאות הרצויות מהסכמה `graphql`. קידומת שמות הטבלאות המוגדרת כברירת מחדל היא `gl`. המקור מגיע אל `gitlab.com`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**בחירת עמודות.** GitLab מתמחר כל שאילתה ודוחה שאילתה יקרה מדי: 200 נקודות למבקש אנונימי, 250 עם אסימון. טבלה רחבה שכל עמודותיה נבחרו חורגת ממחיר זה, ולכן רשמו טבלת GitLab עם העמודות שאתם רוצים. [tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- כאשר אתם רושמים טבלה, Provisa שואלת את GitLab פעם אחת אם יספק את הבחירה בגודל העמוד שבו משתמשות קריאות. אם GitLab עונה שהשאילתה מורכבת מדי או גדולה מדי, הטבלה אינה נרשמת והתוצאה נושאת את ההודעה של GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.` [tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- מחיר עמודה תלוי בסוגה. ערך פשוט עולה כנקודה אחת; עמודת אובייקט מקונן עולה פי כמה וכמה. הסרת עמודות אובייקט מקונן חוסכת הכי הרבה. [tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- גודל העמוד הוא חלק מהמחיר. זהו `graphql_remote.max_list_items`. [tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**בדיקת אסימון.** GitLab עונה לאסימון שאינו מזוהה בתוצאה ריקה, לא בשגיאה. Provisa מתייחסת לכך כאל אסימון שנדחה ואינה מוסיפה את המקור. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### סכמה מרוחקת של gRPC (REQ-322–329)

**איך להוסיף את המקור.** שלחו POST ל-`/admin/grpc-remote/register` עם כתובת השרת, נתיב או URL לקובץ `.proto`, והגדרת TLS אופציונלית. הוספת המקור אינה רושמת אף טבלה.

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

**מה המקור מציע.** כל שיטת `rpc` ב-proto מסווגת כ-query או כ-mutation באמצעות שלושה אותות לפי סדר עדיפות: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** במטען הרישום — `{"MethodName": "query"}` או `{"MethodName": "mutation"}` דורס הכל.
2. **`server_streaming: true`** — השרת שולח stream של הודעות; תמיד טבלה וירטואלית (אלא אם הפלט הוא סקלר).
3. **הודעת פלט בעלת שדה מסוג-הודעה חוזר** — למשל `ListOrdersResponse { repeated Order items; }` נחשבת ל-list-wrapper והופכת לטבלה וירטואלית. שדות סקלריים חוזרים (למשל `repeated string tags`) אינם מפעילים זאת — הם תכונות מערך על ישות בודדת, לא מקורות שורה.

שיטות שאינן תואמות אף אחד מהאותות הללו (RPC יוני-קאסטי המחזיר הודעת ישות בודדת, או כל פלט סקלרי) הופכות ל-commands עוקבים.

**רישום טבלאות.** כל שיטת query מוצעת כטבלה אחת, בשם `{namespace}__{Service}__{Method}`, תחת סכמת הבורר `grpc_remote`. רשמו את הרצויות דרך בורר Register Table (`availableTables`, `availableColumns`, `registerTable`, כמו במקורות GraphQL), ובחרו עמודות תגובה. שדות הבקשה הופכים לעמודות מסנן-מקורי `_nf_*`, ואלה נכללות תמיד. (REQ-322) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

מתודות mutation הן פקודות מוצעות, הנספרות ב-`available_mutations`; הוספת המקור אינה רושמת אף אחת. mutation של gRPC נרשם בעמוד הפקודות על ידי בחירת המקור ואחר כך המתודה, בשם `Service.Method`; סוג הפקודה הוא `source_operation`. ראו [פעולת כתיבה של מקור מרוחק](commands.md#a-remote-sources-write-operation-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

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

**מתודות mutation (REQ-326).** מתודת mutation רשומה היא פקודה שהארגומנטים שלה הם שדות הודעת הקלט, כל אחד מוגדר כ-`json` ומועבר ללא שינוי. התשובה של השירות המרוחק חוזרת כשורות; קריאה שנדחתה היא 422, `functions.remote_refused`. ראו [פעולת כתיבה של מקור מרוחק](commands.md#a-remote-sources-write-operation-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**ניהול ערוץ (Channel).** `grpc.aio.Channel` אחד לכל מקור רשום נשמר במצב האפליקציה ונעשה בו שימוש חוזר על פני בקשות. הערוץ הישן נסגר לפני שהחדש נפתח ברענון. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**רענון.** שלחו POST ל-`/admin/grpc-remote/refresh/{source_id}`. טוען מחדש את ה-proto מהנתיב השמור, מקמפל מחדש את ה-stubs ומביא את הטבלאות שכבר נרשמו להתאמה ל-proto, עם העמודות שבהן נרשמה כל אחת. הוא אינו רושם טבלה חדשה; שיטת query שנוספה ל-proto נשארת מוצעת. לחלופין, שלחו PUT ל-`/admin/grpc-remote/{source_id}/proto` עם `proto_text` חדש כדי לעדכן את ה-proto בתוך הבקשה. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**מגבלות.**

- חילוץ אובייקט שדה-משנה הוא רמה אחת עמוקה. שדות הודעה מקוננים מעבר לעומק 1 אינם מורחבים באופן רקורסיבי. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**איך להוסיף את המקור.** שלחו POST ל-`/admin/openapi/register` עם מזהה מקור ו-spec, הנטען מקובץ מקומי או מ-URL. ה-spec מפוענח ונשמר עם המקור; לא נרשמת אף טבלה ואף command. התגובה מדווחת `tables: 0` ו-`mutations: 0`, כשהמונים המוצעים מופיעים ב-`available_tables` וב-`available_mutations`. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**רישום טבלאות.** רשמו כל פעולת GET רצויה דרך בורר Register Table (`availableTables`, `availableColumns`, `registerTable`), ובחרו עמודות. רשמו כל פעולה שאינה GET בנפרד כפקודה בעמוד הפקודות, המפורטות על ידי `availableFunctions`; ראו [פעולת כתיבה של מקור מרוחק](commands.md#a-remote-sources-write-operation-req-1924). `PUT /admin/openapi/spec/{source_id}` שומר spec שנערך ידנית, אינו רושם דבר, ומחזיר `available_tables` ו-`available_mutations`. (REQ-316) [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

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

**מה המקור מציע.** כל פעולת GET במפרט מוצעת כטבלה, אלא אם סכמת התגובה שלה היא סוג סקלרי (`string`, `number`, `boolean`, `integer`) — פעולות GET שמחזירות סקלר הן במקום זאת פונקציות עם עמודה אחת `value`. כל פעולה שאינה GET (POST, PUT, PATCH, DELETE) מוצעת כפקודה, בשם ה-`operationId` שלה. לאחר הרישום היא מקבלת את פרמטרי הנתיב של הפעולה וארגומנט `body` לגוף הבקשה, כל אחד מוגדר כ-`json`; כל ארגומנט אחר עובר במחרוזת השאילתה. ראו [פעולת כתיבה של מקור מרוחק](commands.md#a-remote-sources-write-operation-req-1924). (REQ-316, REQ-317, REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

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

**רענון (REQ-321).** שלחו POST ל-`/admin/openapi/refresh/{source_id}`. מפענח מחדש את ה-spec דרך `_load_and_register`, שאינו רושם דבר: הוא אינו מוסיף טבלה או עמודה. כללי ממשל קיימים נשמרים. [tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`] טבלה רשומה שומרת על העמודות שלה; היא נקראת דרך הפעולה ב-spec המרוענן. [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

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

**ממשל קשרים (V002).** תנאי JOIN מול טבלאות מרוחקות — כאשר נשאלים דרך SQL או Cypher — חייבים להתאים לקשר רשום ומאושר. (REQ-604) בדיקת V002 מדולגת עבור שאילתות GraphQL מכיוון שקשרים מוגדרי-SDL מאושרים-מראש מעצם התכנון. ראו [docs/security.md](security.md#relationship-governance-v002).

**עמודות מסוג-OBJECT.** כאשר עמודה ממופה לטיפוס OBJECT‏ inline לא-ממושל של GQL או OpenAPI, טיפוס ה-Provisa שלה הוא `jsonb`. העמודה שומרת את ה-blob המלא של ה-JSON המקונן. כאשר שדות-משנה מוצהרים (`gql_object_fields` או `object_fields`), מפת `gql_object_columns` מאוכלסת בזמן בניית הסכמה. מחולל ה-SQL משתמש במפה זו כדי לפלוט ביטויי חילוץ `->>` עבור שדות-משנה כאשר שאילתה בוחרת אותם. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**ארגומנטים נדרשים כפרמטרי פילטר-ילידי.** שדות שאילתה-שורש עם ארגומנטים לא-null וללא-ברירת-מחדל מזריקים עמודות נוספות לטבלה הרשומה. עמודות אלה נושאות `native_filter_type: query_param`. מתרגם ה-Cypher כותב מחדש `WHERE n.id = $val` ל-`WHERE n._nf_id = $val`, ו-executor ה-GraphQL אוסף אותן כמשתנים להעברה לנקודה הקצה המרוחקת. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

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
