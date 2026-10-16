# שרת ה-pgwire של Provisa {: #provisa-pgwire-server }

Provisa חושפת נקודת קצה של פרוטוקול חיווט PostgreSQL (pgwire). כל כלי שדובר את פרוטוקול הלקוח של PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — יכול להתחבר ולשאול נתוני Provisa דרך אותו צינור ממשל השולט ב-API של HTTP. (REQ-266)

שאילתות עוברות דרך מחסנית הממשל המלאה: אכיפת RLS, כללי מיסוך, שומרי קשר, בדיקות גישת-דומיין. (REQ-001, REQ-002, REQ-263) ממשק ה-pgwire אינו עוקף. (REQ-002, REQ-266)

---

## פרטי חיבור {: #connection-details }

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

**תעודות לקוח.** הגדירו `PROVISA_MTLS_CLIENT_CA` והשרת יאמת תעודת לקוח במהלך ה-handshake, לפני בחינת כל אישור. (REQ-1228) עם `PROVISA_MTLS_BIND_PRINCIPAL` ה-common name של התעודה חייב להיות זהה ל-`user` שהחיבור מתאמת בשמו לאחר מכן. ראו [תצורה](configuration.md#mutual-tls).

**ניסיונות כושלים נספרים.** חמישה כשלונות בחמש דקות נועלים את החשבון לחמש-עשרה דקות, והמונה משותף עם HTTP ועם Bolt — נעילה שהושגה בכל אחת מהממשקים חלה על כולם. (REQ-1393)

**בחירת ארגון.** בפריסה מרובת-ארגונים, התחברו אל `<org>.<your-domain>` ו-pgwire יקרא את הארגון משם המארח שב-ClientHello של TLS, בדיוק כפי ש-HTTP קורא אותו מכותרת `Host`. (REQ-1234) שם המארח מבקש ארגון; הוא אינו מעניק אותו, ו-principal שאינו חבר בו נדחה. התחברות לפי כתובת IP אינה מבקשת ארגון כלל.

---

## מה עובד {: #what-works }

### SELECT

כל משפטי ה-SELECT עוברים דרך צינור הממשל (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) הצינור:

1. כותב מחדש SQL סמנטי ל-SQL פיזי (`rewrite_semantic_to_physical`)
2. מחיל ממשל (RLS, מיסוך, גישת דומיין) (REQ-263)
3. מאמת מול סכמה רשומה (REQ-011)
4. מנתב ל-Trino או ל-pool מקור ישיר (REQ-027, REQ-028)

שאילתות פשוטות מרובות-משפטים נתמכות. משפטים מופרדי-נקודה-פסיק מפוצלים ומבוצעים בסדר. (REQ-580) [tool-verified: `server.py:318-381`]

שאילתות פרמטריות (`$1`, `$2`, ...) נתמכות הן במצב שאילתה-פשוטה והן במצב שאילתה-מורחבת (Bind/Execute). פרמטרים מוחלפים כליטרלים לפני הביצוע. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` ו-`SELECT fn(args)` — כאשר `fn` נותן שם לפונקציה עוקבת (tracked) רשומה — מיורטים לפני צינור הממשל ומנותבים דרך ה-executor המנוהל היחיד (`invoke_tracked_function`). התוצאה היא סט שורות מוקלד זהה למה שכל surface אחר מחזיר עבור אותו command. `writable_by` וכללי ממשל נאכפים בתוך ה-executor. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### סמנים (DECLARE / FETCH / MOVE / CLOSE) {: #cursors-declare-fetch-move-close }

Provisa מטפלת בתחביר הטקסטואלי של סמני SQL על פרוטוקול השאילתה הפשוטה. (REQ-1862) לקוחות ששולחים `DECLARE`/`FETCH`/`MOVE`/`CLOSE` כהצהרות SQL רגילות מקבלים הזרמה אמיתית בצד השרת במקום שגיאת פרוטוקול או חוצץ של התוצאה כולה בזיכרון הלקוח.

**זה שונה מהזרמה מבוססת portal.** פרוטוקול השאילתה המורחבת — Bind/Execute עם שדה מגבלת שורות — הוא מנגנון אחר של פרוטוקול התעבורה. asyncpg ורוב הדרייברים המודרניים של PostgreSQL משתמשים בו אוטומטית, ללא SQL של סמנים. אם הדרייבר שלכם כבר משתמש ב-portals, אינכם צריכים את התחביר הזה. `DECLARE CURSOR` מיועד ללקוחות ששולחים SQL של סמנים כלשונו: סמנים בעלי שם/בצד השרת של psycopg2 (`conn.cursor(name="...")`), חלק מכלי ה-BI וחלק מדרייברי JDBC ישנים.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
ה-SELECT הפנימי עובר בצינור המנוהל הרגיל של Provisa — אותה אכיפת RLS, אותה מיסוך ואותן בדיקות גישה לדומיין כמו כל SELECT עצמאי. [tool-verified: `server.py:1367` — `ctx.execute_sql(inner_sql)`, the same call path as a plain SELECT] הצהרה על סמן אינה עוקפת את הממשל.

מילת המפתח `SCROLL` שולטת הן בניווט המותר והן בטביעת הזיכרון של הסמן:

| הצהרה | ניווט לאחור | עלות זיכרון |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (ברירת מחדל) | אסור | מוגבלת לאצוות FETCH אחת |
| `DECLARE c NO SCROLL CURSOR FOR ...` | אסור | מוגבלת לאצוות FETCH אחת |
| `DECLARE c SCROLL CURSOR FOR ...` | מותר | התוצאה כולה בחוצץ בזיכרון Python |

כשלא צוינו `SCROLL` ולא `NO SCROLL`, ברירת המחדל היא `NO SCROLL` — כמו ב-PostgreSQL עצמה. [tool-verified: `server.py:1361-1362`] סמן `NO SCROLL` משליך מהחוצץ הפנימי שלו שורות שנצרכו אחרי כל צעד קדימה, כך שמעבר `FETCH FORWARD` על תוצאה גדולה לעולם אינו מחזיק בזיכרון Python יותר מאצווה אחת בערך בכל פעם. [tool-verified: `server.py:443-447`] סמן `SCROLL` שומר כל שורה שאי פעם משך — זה המחיר של תמיכה ב-`FETCH BACKWARD`, `ABSOLUTE`, `FIRST` ו-`LAST`. לפני שמצהירים על סמן `SCROLL` מעל תוצאה גדולה, תכננו את מלוא גודל התוצאה ב-heap של Python. [tool-verified: `server.py:383-389`]

הצהרה מחדש על שם סמן שכבר פתוח סוגרת קודם את הסמן הקודם. [tool-verified: `server.py:1369-1371`] `WITH HOLD` ו-`WITHOUT HOLD` מתקבלים על ידי התחביר; ראו את משך החיים של הסמן בהמשך.

**FETCH ו-MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```
`FETCH` מחזיר שורות ושולח את תג הפקודה `FETCH <n>`, כאשר `n` הוא המספר שהוחזר בפועל, שעשוי להיות קטן מהמבוקש כשהסמן מגיע לסוף. `MOVE` מזיז את המיקום בלי להחזיר שורות ושולח `MOVE <n>`. [tool-verified: `server.py:1417`, `server.py:1419`]

`FROM` ו-`IN` ניתנים להחלפה לפני שם הסמן. שמות סמנים ללא מירכאות מומרים לאותיות קטנות; שמות במירכאות (`"CaseSensitive"`) שומרים על הרישיות. [tool-verified: `test_cursor.py:74-79`]

ניווט לאחור (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, `RELATIVE` שלילי) על סמן `NO SCROLL` מחזיר SQLSTATE 55000 (`object_not_in_prerequisite_state`), כמו הדחייה של PostgreSQL עצמה. [tool-verified: `server.py:1402-1406`] FETCH או MOVE עם שם של סמן שמעולם לא הוצהר מחזיר SQLSTATE 34000 (`invalid_cursor_name`). [tool-verified: `server.py:1398-1400`]

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` משחרר את זרם התוצאות שמתחת ומסיר את הסמן מהסשן. [tool-verified: `server.py:1421-1436`] ניתוק בלי `CLOSE` בטוח: `ProvisaSession.close()` משחרר אוטומטית בניתוק כל סמן שעדיין פתוח. [tool-verified: `test_cursor.py:335-346`]

**משך החיים של סמן.** לשרת ה-pgwire של Provisa אין מכונת מצבים של טרנזקציות (`ProvisaSession.in_transaction()` מחזיר קבוע `False`). לכן כל סמן מתנהג כאילו הוצהר `WITH HOLD`: הוא חי לאורך כל החיבור, לא עד `COMMIT`. [tool-verified: `server.py:200` — `in_transaction()` always returns `False`] מילות המפתח `WITH HOLD` ו-`WITHOUT HOLD` מתקבלות ומתעלמים מהן בשקט — אין מחזור חיים של סמנים לפי טרנזקציה לאכוף.

### הגדרות (CREATE, ALTER, DROP, TRUNCATE) {: #definitions-create-alter-drop-truncate }

דבר אינו מוגדר דרך pgwire. הצהרה שיוצרת, משנה או מסירה אובייקט (`CREATE TABLE`, `CREATE TABLE … AS SELECT`, `CREATE VIEW`, `ALTER`, `DROP`, אינדקס, רצף או סכמה) נדחית עם SQLSTATE 0A000, לא משנה מה התפקיד מחזיק; היוצא מן הכלל הוא טבלה זמנית של הסשן עצמו (ראו [טבלאות זמניות](#temporary-tables)). טבלה או view הם אובייקט מודל: צרו אותם במודל (עמודי הניהול, API הניהול או הקונפיגורציה), או צרו אותם במקור הנתונים וקבלו אותם למודל. `TRUNCATE` נדחה באותו אופן; `DELETE` היא הדרך המנוהלת להסיר שורות. [tool-verified: `provisa/compiler/definitions.py`]

הדחייה מתבצעת פעם אחת, בצינור שכל משטח SQL עובר בו, כך ש-SQL על HTTP, Flight ו-MCP דוחים את אותן הצהרות עם אותה הודעה.

### טבלאות זמניות {: #temporary-tables }

סשן יכול ליצור, לכתוב, לקרוא ולהסיר טבלאות זמניות משלו:

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```
- **יצירה** — `CREATE TEMP TABLE name (column type, ...)` או `CREATE TEMP TABLE name AS SELECT ...`. השם אינו מקבל סכמה. ה-`SELECT` שממנו היא נוצרת הוא קריאה כמו כל קריאה אחרת: כללי השורות, המסכות, הנתונים המזויפים ומגבלת השורות של התפקיד חלים על השורות שהוא לוקח.
- **סוגי עמודות**: `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean`, `date`, `timestamp`, `text`, `varchar` ו-`DECIMAL(precision, scale)`. מספר עשרוני שומר על הדיוק וקנה המידה שלו. כל סוג אחר נדחה תוך ציון העמודה והסוג: ערך לעולם אינו נשמר כסוג שאינו שלו. עמודה שנוצרה מ-`SELECT` לוקחת את סוג הערכים שלה; עמודה בלי ערך שממנו אפשר לקבוע סוג נדחית, אז החילו `CAST` ב-`SELECT`.
- **מגבלת שורות.** השורות ש-`CREATE ... AS SELECT` או `INSERT ... SELECT` לוקחים נקראות כפי שהתפקיד קורא כל דבר, ולכן מגבלת השורות של התפקיד חלה. טבלה זמנית שמולאה מקריאה שהמגבלה קיצצה נושאת את אותה אזהרת הצהרה כמו כל קריאה מקוצצת (`statement.rows_cut`), עם ציון המגבלה.
- **כתיבה** — `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` ו-`DELETE`, ללא `RETURNING` וללא `FROM`. כתיבה לטבלה זמנית אינה מוטציה: היא אינה מגיעה למקור ולא לנתוני הסביבה, ולכן היא עובדת תחת כל טיפול במוטציות של הסביבה ואינה דורשת זכות כתיבה.
- **קריאה** — לפי שמה, לבד או ב-join עם טבלאות המודל. המנוע מחשב את הקריאה.
- **משך חיים.** טבלה זמנית אינה אובייקט מודל, סשנים אחרים אינם רואים אותה, והיא מסתיימת עם הסשן. ב-pgwire הסשן הוא החיבור. במשטח ללא חיבור משלו (SQL על HTTP, Flight, MCP) הסשן הוא הבקשה, ולכן טבלה זמנית חיה עבור ההצהרות שנשלחו יחד בבקשה זו.
- **אחסון.** השורות נשמרות בסכמה משלו של הסשן במאגר של המנוע ומוסרות כשהסשן מסתיים. `INSERT`, `UPDATE` או `DELETE` כותב מחדש את הטבלה כולה דרך המנוע, כך שטבלה זמנית מתאימה לנתוני עבודה, לא לטבלאות גדולות שנכתבות שורה אחר שורה. תהליך שנעצר בלי לסיים את הסשנים שלו משאיר את הסכמות שלהם במאגר; הן מוסרות כשהסביבה יוצאת משימוש.

אותן הצהרות עובדות בכל משטח SQL: הן מטופלות בצינור המשותף לכולם. [inferred: verified by `tests/integration/test_session_temp_tables.py` over HTTP; the pgwire connection-long session is not yet covered by a test]

### כתיבת נתונים (INSERT, UPDATE, DELETE, MERGE, COPY FROM STDIN) {: #data-writes-insert-update-delete-merge-copy-from-stdin }

שורות של טבלה רשומה קיימת נכתבות דרך pgwire כמו בכל משטח SQL אחר, דרך קבלת כתיבה אחת (`provisa/compiler/write_admission.py`):

1. **הזכות.** לתפקיד יש `write`. (REQ-868)
2. **העמודות.** כל עמודה שההצהרה כותבת נוקבת בתפקיד ב-`writable_by` שלה; עמודה שאינה מצהירה דבר אינה ניתנת לכתיבה על ידי אף אחד. (REQ-663)
3. **השורות שאפשר לגעת בהן.** מסנן השורות של התפקיד, אותו פרדיקט שהקריאות שלו נושאות, מתווסף ל-`WHERE` של `UPDATE` או `DELETE`.
4. **השורות שנשארות.** תפקיד מסונן יכול לכתוב רק שורות שהוא יוכל לקרוא אחר כך: שורות `INSERT` והערכים החדשים של `UPDATE` שקובע עמודה שהמסנן קורא נבדקים מול המסנן לפני שדבר נשלח למקור.

מה שאי אפשר להכריע נדחה בשמו ולא מתקבל מספק (`INSERT … SELECT`, ערך שאינו ליטרל ולא פרמטר קשור, מסנן שקורא עמודה שההצהרה אינה מספקת). [tool-verified: `provisa/compiler/write_admission.py`, `provisa/pgwire/copy_handler.py`]

### COPY

הן `COPY ... TO STDOUT` והן `COPY ... FROM STDIN` נתמכים. (REQ-585)

**COPY TO STDOUT** — מייצא תוצאות שאילתה בפורמט התעבורה של PG COPY. שתי צורות עובדות:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
פורמטים נתמכים: `text` (מופרד בטאבים, ברירת מחדל) ו-`csv`. פורמט בינארי אינו נתמך בפלט COPY. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** הוא כתיבת נתונים לטבלה רשומה קיימת: הוא מתקבל באותה קבלת כתיבה כמו `INSERT` של אותן שורות (ראו [כתיבת נתונים](#data-writes-insert-update-delete-merge-copy-from-stdin)).

### עסקאות ופקודות Session {: #transactions-and-session-commands }

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

## קידוד פרמטרים בינארי {: #binary-parameter-encoding }

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

## המלצות דרייבר {: #driver-recommendations }

**דרייברי Python ילידיים (psycopg2, asyncpg).** אלה מנהלים משא-ומתן על פרוטוקול השאילתה-המורחבת כברירת מחדל ומשתמשים בקידוד בינארי עבור רוב הטיפוסים. נאמנות הטיפוסים הגבוהה ביותר כאן — עמודות `NUMERIC` מגיעות כ-`Decimal`, `TIMESTAMP` כ-`datetime`, וכן הלאה. השתמשו באלה עבור ETL מבוסס-Python, סקריפטים, או אינטגרציה ישירה.

**JDBC (דרייבר JDBC של PostgreSQL).** השתמשו בו עבור כלי אקוסיסטם-Java: DBeaver, Tableau, Power BI, Metabase, מפעילי JDBC של Airflow. JDBC ברירת מחדל לפרוטוקול השאילתה-הפשוטה, המונע סיבוכי קידוד בינארי. מחרוזת חיבור:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

חלק מכלי ה-BI מבוססי-JDBC שולחים בהתחברות פרץ שאילתות `information_schema` ו-`pg_catalog` כדי לאכלס את דפדפן הסכמה שלהם. אלה כולן נענות על ידי שכבת יירוט הקטלוג — אין תעבורת Trino נוצרת במהלך בדיקת הסכמה. (REQ-532)

**מתי להעדיף אחד על פני השני.** אם הלקוח הוא Python, השתמשו ב-psycopg2 או asyncpg לטיפול טוב יותר בטיפוסים. אם הלקוח הוא כלי BI או כל אפליקציית JVM, השתמשו ב-JDBC. הימנעו מערבוב ציפיות פרוטוקול בינארי וטקסט באותו חיבור אם אתם רואים הפתעות המרת-טיפוסים — התנהגות מצב-הטקסט של JDBC פשוטה יותר להבנה.

---

## אזהרות ואילוצים {: #caveats-and-constraints }

**SQL בלבד.** מאזין ה-pgwire מפענח ומבצע SQL בלבד — מחרוזות GraphQL ו-Cypher אינן מתקבלות. (REQ-614) כתיבת נתונים לטבלאות קיימות מתקבלת לפי הזכויות של התפקיד, הענקות העמודות שלו ומסנן השורות שלו; ראו [כתיבת נתונים](#data-writes-insert-update-delete-merge-copy-from-stdin).

**ללא הגדרות.** `CREATE`, `ALTER`, `DROP` ו-`TRUNCATE` נדחים עם SQLSTATE 0A000 לא משנה מה התפקיד מחזיק, למעט טבלה זמנית של הסשן עצמו; ראו [טבלאות זמניות](#temporary-tables). (REQ-615, REQ-616)

**אין תמיכת עסקאות אמיתית.** BEGIN/COMMIT/ROLLBACK מתקבלים ומתעלמים בשקט. כל משפט פועל באופן עצמאי. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` מחזיר תמיד `False`]

**Timeout לשאילתה.** הצהרה רשאית לקחת את ה-timeout לבקשה של pgwire עצמו, `limits.request_timeouts.pgwire`, או את `limits.request_timeout` המשותף כשהוא אינו מוגדר; שניהם הגדרות של המפעיל. (REQ-590, REQ-1905) [tool-verified: `provisa/core/limits.py`, `server.py`]

**פורמט פלט COPY הוא text או csv.** פורמט COPY בינארי של PG (`FORMAT binary`) אינו ממומש. [inferred: only `text` and `csv` branches exist in `_rows_to_copy_text` / `_rows_to_copy_csv`]

**החלפת פרמטרים היא ליטרלית.** פרמטרים `$1`, `$2`, ... מוחלפים כליטרלי SQL לפני הביצוע, לא נשלחים כפרמטרי bind למנוע ה-upstream. משמעות הדבר שמנוע ה-upstream לעולם לא רואה משפט מוכן (prepared statement). עבור Trino אין לכך השפעה מעשית; עבור מקורות pool-ישיר זה עוקף מטמון prepared-statement. (REQ-581) [tool-verified: `server.py:78-85`]

**DECLARE SCROLL מחזיק את התוצאה כולה בחוצץ.** סמן `SCROLL` צובר כל שורה שהוא מושך מהמנוע ב-heap של Python כדי ש-`FETCH BACKWARD`, `ABSOLUTE`, `FIRST` ו-`LAST` יוכלו להיענות לפי דרישה. מול תוצאה גדולה, פירוש הדבר שהתוצאה כולה חיה בזיכרון כל עוד הסמן פתוח. השתמשו בברירת המחדל `NO SCROLL` להזרמה קדימה בלבד. (REQ-1862) [tool-verified: `server.py:383-389`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** טבלאות אלה קיימות בשכבת הקטלוג אך הן stubs ריקים. כלי ניטור השואלים אותן יקבלו אפס שורות במקום שגיאות. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` מאוכלס — ראו יירוט קטלוג.)
