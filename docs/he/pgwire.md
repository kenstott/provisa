# שרת ה-pgwire של Provisa

Provisa חושפת נקודת קצה של פרוטוקול חיווט PostgreSQL (pgwire). כל כלי שדובר את פרוטוקול הלקוח של PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — יכול להתחבר ולשאול נתוני Provisa דרך אותו צינור ממשל השולט ב-API של HTTP. (REQ-266)

שאילתות עוברות דרך מחסנית הממשל המלאה: אכיפת RLS, כללי מיסוך, שומרי קשר, בדיקות גישת-דומיין. (REQ-001, REQ-002, REQ-263) ממשק ה-pgwire אינו עוקף. (REQ-002, REQ-266)

---

## פרטי חיבור

השרת מופעל כאשר `PROVISA_PGWIRE_PORT` מוגדר למספר שלם שאינו אפס. הוא מושבת כברירת מחדל. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** הגדירו את `PROVISA_PGWIRE_CERT` ו-`PROVISA_PGWIRE_KEY` לנתיבי תעודת PEM ומפתח. כששניהם קיימים, השרת עוטף חיבורים נכנסים ב-TLS. כשהם נעדרים, TLS כבוי והשרת עונה `N` לבקשות משא-ומתן SSL. (REQ-530) [tool-verified: `server.py:1746-1750`]

**גרסת שרת מדווחת.** לקוחות רואים `14.0.provisa`. כלים המגבילים תכונות לפי מספר הגרסה עשויים להתנהג כאילו הם מחוברים ל-PostgreSQL 14. (REQ-579) [tool-verified: `server.py:208`]

---

## אימות (Authentication)

חבילת ה-startup נושאת שם משתמש ושדה סוד יחיד, וללא סכימה שתאמר מהו אותו סוד. Provisa מכריעה לפי הסוד עצמו, כך שלקוח אינו זקוק לתצורה מעבר ל-`user` ול-`password`:

| הסוד הוא | מזוהה לפי | נפתר אל |
| --------------- | --------------- | ------------- |
| אסימון גישה אישי (PAT) | הקידומת `provisa_pat_` שלו | הבעלים והתפקיד של האסימון (REQ-1263) |
| אסימון bearer של OIDC / של הספק | היות הספק המוגדר ספק אסימונים | הזהות שהאסימון מצהיר עליה (REQ-890) |
| סיסמה | כל דבר אחר | החשבון בספק המוגדר (`basic` או `simple`) |

ההכרעה מתקבלת פעם אחת. אישור שהמאמת שנבחר דוחה אינו נבדק שוב מול מאמת אחר, ולכן דחייה אחת אינה הופכת לניחוש שני.

מצב trust (`provider: none`, או middleware אימות לא-פעיל) הוא היוצא מן הכלל: שם המשתמש משמש ישירות כ-`role_id` והסוד מתעלם. אל תשתמשו בו על חיבור לא-מוצפן.

**SCRAM-SHA-256.** תחת `provider: basic` עם `auth.scram: true` השרת מכריז על SASL (קוד אימות 10) עם `SCRAM-SHA-256`, והסיסמה מוכחת במקום להישלח. (REQ-1394) `SCRAM-SHA-256-PLUS` אינו מוצע. משתמש שה-verifier שלו טרם נכתב — לא ניתן לגזור verifier מגיבובי bcrypt — מקבל חילופי דברים מדומים, כך שהתקשורת אינה חושפת מי כבר עבר; משתמש כזה מתאמת בסיסמת cleartext מעל TLS עד שהזנת הסיסמה הבאה שלו תכתוב verifier. כאשר `auth.scram` כבוי, השרת משתמש בסוג אימות PG 3 (סיסמת cleartext). MD5 אינו נתמך בשני המקרים.

**תעודות לקוח.** הגדירו `PROVISA_MTLS_CLIENT_CA` והשרת יאמת תעודת לקוח במהלך ה-handshake, לפני בחינת כל אישור. (REQ-1228) עם `PROVISA_MTLS_BIND_PRINCIPAL` ה-common name של התעודה חייב להיות זהה ל-`user` שהחיבור מתאמת בשמו לאחר מכן. ראו [תצורה](configuration.md#tls).

**ניסיונות כושלים נספרים.** חמישה כשלונות בחמש דקות נועלים את החשבון לחמש-עשרה דקות, והמונה משותף עם HTTP ועם Bolt — נעילה שהושגה בכל אחת מהממשקים חלה על כולם. (REQ-1393)

**בחירת ארגון.** בפריסה מרובת-ארגונים, התחברו אל `<org>.<הדומיין-שלכם>` ו-pgwire יקרא את הארגון משם המארח שב-ClientHello של TLS, בדיוק כפי ש-HTTP קורא אותו מכותרת `Host`. (REQ-1234) שם המארח מבקש ארגון; הוא אינו מעניק אותו, ו-principal שאינו חבר בו נדחה. התחברות לפי כתובת IP אינה מבקשת ארגון כלל.

---

## מה עובד

### SELECT

כל משפטי ה-SELECT עוברים דרך צינור הממשל (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) הצינור:

1. כותב מחדש SQL סמנטי ל-SQL פיזי (`rewrite_semantic_to_physical`)
2. מחיל ממשל (RLS, מיסוך, גישת דומיין) (REQ-263)
3. מאמת מול סכמה רשומה (REQ-011)
4. מנתב ל-Trino או ל-pool מקור ישיר (REQ-027, REQ-028)

שאילתות פשוטות מרובות-משפטים נתמכות. משפטים מופרדי-נקודה-פסיק מפוצלים ומבוצעים בסדר. (REQ-580) [tool-verified: `server.py:318-381`]

שאילתות פרמטריות (`$1`, `$2`, ...) נתמכות הן במצב שאילתה-פשוטה והן במצב שאילתה-מורחבת (Bind/Execute). פרמטרים מוחלפים כליטרלים לפני הביצוע. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` ו-`SELECT fn(args)` — כאשר `fn` נותן שם לפונקציה עוקבת (tracked) רשומה — מיורטים לפני צינור הממשל ומנותבים דרך ה-executor המנוהל היחיד (`invoke_tracked_function`). התוצאה היא סט שורות מוקלד זהה למה שכל surface אחר מחזיר עבור אותו command. `writable_by` וכללי ממשל נאכפים בתוך ה-executor. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

משפטי DDL מזוהים על ידי ה-regex ב-`server.py` ומנותבים ל-`DdlHandler`. לתפקיד חייבת להיות היכולת `"ddl"`. (REQ-042) בלעדיה, המשפט נדחה עם SQLSTATE 42501. [tool-verified: `ddl_handler.py:82-83`]

צורות ה-DDL המזוהות הן:

```sql
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

שני נתיבי ביצוע קיימים, תלוי ב-`ddl_catalog`: (REQ-582)

**נתיב Trino** — משמש כאשר `ddl_catalog` הוא Iceberg, Hive, או קטלוג Trino לא-רשום אחר (למשל `iceberg`, `hive`, `otel`, `results`). רק `CREATE TABLE` ו-`CREATE VIEW` נתמכים בנתיב זה. ניסיון `ALTER`, `DROP`, או `CREATE INDEX` מעלה שגיאה. שם הטבלה מוסמך במלואו כ-`catalog.schema.table`. [tool-verified: `ddl_handler.py:92-100`]

**נתיב ישיר** — משמש כאשר `ddl_catalog` תואם מזהה מקור רשום. DDL מלא נתמך: CREATE, ALTER, DROP, אינדקסים, רצפים. `CREATE TABLE` ו-`CREATE VIEW` מוסמכים-סכמה כ-`schema.table`. כל שאר ה-DDL (ALTER, DROP, CREATE INDEX) עובר כפי-שהוא לאחר הגדרת הקשר הסכמה. עבור מקורות PostgreSQL ו-SQLite, ההקשר מוגדר עם `SET search_path TO schema`. עבור MySQL ו-MariaDB, ההקשר מוגדר עם `USE schema`. [tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

לאחר DDL בכל אחד מהנתיבים, הטבלה החדשה נרשמת להקשר הקימפול של התפקיד כך שהיא ניתנת לשאילתה מיידית. (REQ-583) [tool-verified: `ddl_handler.py:216-250`]

**פתירת יעד כתיבה.** קטלוג ה-DDL והסכמה מגיעים משדות `ddl_catalog` ו-`ddl_schema` של הדומיין. אם `ddl_catalog` אינו מוגדר, המערכת ברירת-מחדל לקטלוג Iceberg. אם `ddl_schema` אינו מוגדר, ברירת המחדל היא מזהה הדומיין. הדומיין נפתר דרך רשימת `domain_access` של התפקיד. (REQ-584) [tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

הן `COPY ... TO STDOUT` והן `COPY ... FROM STDIN` נתמכים. (REQ-585) [tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT** — מייצא תוצאות שאילתה בפורמט חיווט COPY של PG. שתי צורות עובדות:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

פורמטים נתמכים: `text` (מופרד-tab, ברירת מחדל) ו-`csv`. פורמט בינארי אינו נתמך בפלט COPY. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** — מכניס שורות לטבלת יעד. מוגבל למקורות מסוג `postgresql`, `mysql`, `sqlite`, או `mariadb`. (REQ-586) ניסיון COPY FROM מול מקור Trino-בלבד (למשל Iceberg) מעלה שגיאת הרשאה. [tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

אם לא סופקה רשימת עמודות, העמודות נגזרות מהסכמה הרשומה. [tool-verified: `copy_handler.py:357`]

### עסקאות ופקודות Session

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET, ו-DEALLOCATE מיורטים ומחזירים תגובת הצלחה ריקה. (REQ-587) השרת חסר-מצב (stateless) ביחס לעסקאות — אין תמיכה בבידוד עסקאות או rollback. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## יירוט קטלוג (Catalog Intercept)

שאילתות מול `information_schema` ו-`pg_catalog` נענות מקומית ללא round-trip ל-Trino. (REQ-532) שכבת היירוט בונה מסד נתונים DuckDB בזיכרון לכל בקשה, מאוכלס מהקשר הקימפול של התפקיד. (REQ-532) [tool-verified: `catalog.py:210-213`]

טבלאות מיורטות:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` מאוכלס בנתוני PK ו-FK אמיתיים הנגזרים מ-`pk_columns` ו-`joins` של מודל הדומיין. (REQ-392, REQ-399) כלי BI הבודקים קשרי מפתח-זר (Tableau, DBeaver וכו') יראו את גרף ה-join ש-Provisa מכירה. [tool-verified: `catalog.py:551-632`] joins חד-עמודתיים בין אותו זוג מקור/יעד שעמודות היעד שלהם יחד מהוות את המפתח הראשי המורכב של היעד מכווצים לשורת FK אחת עם מערכי `conkey`/`confkey` מרובי-איברים. (REQ-1094) [tool-verified: `catalog_constraints.py`]

קשר מגובה junction (REQ-1586) אינו מייצר שורת FK. זוהי קשת דרך טבלת שיוך, לא זוג עמודות, ול-`pg_constraint` אין צורה לשתי קפיצות — ולכן מודל הדומיין משמיט אותה מ-`joins`, וטבלת ה-junction מופיעה כטבלה רגילה עם מפתחות זרים משלה לכל אחד מהקצוות. לקוחות SQL מגיעים אליה על ידי join לטבלה הזו; לקוחות Cypher חוצים אותה כקשר יחיד. [tool-verified: `provisa/compiler/schema_gen.py:302-306`]

`pg_index` מאוכלס בשורה אחת לכל אילוץ primary-key ו-UNIQUE (`indrelid` = oid הטבלה, `indkey` = attnums מפתח מסודרים, `indisprimary`/`indisunique` מוגדרים). לקוחות הפותרים עמודות מפתח דרך `pg_index.indkey` ולא `pg_constraint` — DataGrip, לדוגמה — מגלים את העמודות הנכונות דרך ה-join הסטנדרטי `pg_index` → `pg_attribute`. (REQ-1095) [tool-verified: `catalog_constraints.py:340-384`]

הביטויים הסקלריים הבאים גם הם מיורטים: (REQ-588)

- `current_user`, `session_user` → ה-`role_id` המאומת
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → מוחזר מטבלת הגדרות קבועה
- `SHOW <setting>` → מוחזר מאותה טבלת הגדרות

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## קידוד פרמטרים בינארי

פרוטוקול השאילתה-המורחבת (Bind/Execute) תומך בפרמטרים מקודדים-בינארית. (REQ-589) OID-ים של הטיפוסים הבאים מפוענחים מבינארי: [tool-verified: `postgres.py:69-97`]

| OID | טיפוס PG | טיפוס Python |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

כל OID שאינו בטבלה זו מעלה `"Unsupported binary parameter type: <oid>"`. (REQ-589) [tool-verified: `postgres.py:579`]

עמודות תוצאה נשלחות גם הן בבינארי כשהלקוח מבקש זאת, עבור אותו סט טיפוסים בתוספת ARRAY, JSON, INTERVAL, ו-BIGINT. (REQ-589) [tool-verified: `postgres.py:191-244`]

---

## המלצות דרייבר

**דרייברי Python ילידיים (psycopg2, asyncpg).** אלה מנהלים משא-ומתן על פרוטוקול השאילתה-המורחבת כברירת מחדל ומשתמשים בקידוד בינארי עבור רוב הטיפוסים. נאמנות הטיפוסים הגבוהה ביותר כאן — עמודות `NUMERIC` מגיעות כ-`Decimal`, `TIMESTAMP` כ-`datetime`, וכן הלאה. השתמשו באלה עבור ETL מבוסס-Python, סקריפטים, או אינטגרציה ישירה.

**JDBC (דרייבר JDBC של PostgreSQL).** השתמשו בו עבור כלי אקוסיסטם-Java: DBeaver, Tableau, Power BI, Metabase, מפעילי JDBC של Airflow. JDBC ברירת מחדל לפרוטוקול השאילתה-הפשוטה, המונע סיבוכי קידוד בינארי. מחרוזת חיבור:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

חלק מכלי ה-BI מבוססי-JDBC שולחים בהתחברות פרץ שאילתות `information_schema` ו-`pg_catalog` כדי לאכלס את דפדפן הסכמה שלהם. אלה כולן נענות על ידי שכבת יירוט הקטלוג — אין תעבורת Trino נוצרת במהלך בדיקת הסכמה. (REQ-532)

**מתי להעדיף אחד על פני השני.** אם הלקוח הוא Python, השתמשו ב-psycopg2 או asyncpg לטיפול טוב יותר בטיפוסים. אם הלקוח הוא כלי BI או כל אפליקציית JVM, השתמשו ב-JDBC. הימנעו מערבוב ציפיות פרוטוקול בינארי וטקסט באותו חיבור אם אתם רואים הפתעות המרת-טיפוסים — התנהגות מצב-הטקסט של JDBC פשוטה יותר להבנה.

---

## אזהרות ואילוצים

**SQL בלבד; ללא מוטציות DML.** מאזין ה-pgwire מפענח ומבצע SQL בלבד — מחרוזות GraphQL ו-Cypher אינן מתקבלות. (REQ-614) `INSERT`, `UPDATE`, ו-`DELETE` פשוטים אינם מנותבים לנתיב כתיבה. (REQ-615) כתבו נתונים דרך `COPY FROM STDIN` (מקורות בני-כתיבה) או `CREATE TABLE AS`; מוטציות ברמת-שורה עוברות במקום זאת דרך נתיבי הכתיבה של GraphQL, Cypher, או Trino.

**COPY ו-DDL דורשים את היכולת `ddl`.** הן `COPY` (בכל כיוון) והן DDL מסויגים ביכולת `ddl` של התפקיד; תפקידים בלעדיה מקבלים SQLSTATE 42501. (REQ-616)

**אין תמיכת עסקאות אמיתית.** BEGIN/COMMIT/ROLLBACK מתקבלים ומתעלמים בשקט. כל משפט פועל באופן עצמאי. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` מחזיר תמיד `False`]

**timeout של 60 שניות ל-DDL, timeout של 120 שניות לשאילתה.** אלה מקודדים-קשיח ב-threads המטפלים. (REQ-590) DDL ארוך-ריצה מול מקורות מרוחקים (שינויי סכמה על טבלאות גדולות) עשוי לפוג. [tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM הוא מקור-בר-כתיבה-בלבד.** Iceberg, Hive, מקורות Trino-בלבד, וסוגי מקור לקריאה-בלבד אינם מקבלים COPY FROM. השגיאה היא SQLSTATE 42501. (REQ-586) [tool-verified: `copy_handler.py:65`]

**פורמט פלט COPY הוא text או csv.** פורמט COPY בינארי של PG (`FORMAT binary`) אינו ממומש. [inferred: only `text` and `csv` branches exist in `_rows_to_copy_text` / `_rows_to_copy_csv`]

**DDL בנתיב Trino הוא CREATE בלבד.** ALTER, DROP, ו-CREATE INDEX מול קטלוגי Iceberg או Hive אינם נתמכים. השתמשו במקור SQL רשום כ-`ddl_catalog` אם אתם זקוקים ל-DDL מלא. (REQ-582) [tool-verified: `ddl_handler.py:92-100`]

**החלפת פרמטרים היא ליטרלית.** פרמטרים `$1`, `$2`, ... מוחלפים כליטרלי SQL לפני הביצוע, לא נשלחים כפרמטרי bind למנוע ה-upstream. משמעות הדבר שמנוע ה-upstream לעולם לא רואה משפט מוכן (prepared statement). עבור Trino אין לכך השפעה מעשית; עבור מקורות pool-ישיר זה עוקף מטמון prepared-statement. (REQ-581) [tool-verified: `server.py:78-85`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** טבלאות אלה קיימות בשכבת הקטלוג אך הן stubs ריקים. כלי ניטור השואלים אותן יקבלו אפס שורות במקום שגיאות. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` מאוכלס — ראו יירוט קטלוג.)
