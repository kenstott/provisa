# Provisa

**חברו את מסדי הנתונים שלכם. שאלו עם GraphQL, gRPC, SQL, או MCP — מעל כל API או פרוטוקול — תוך 5 דקות.**

Provisa משרתת כל משטח API (REST, GraphQL, SQL, gRPC, MCP, ועוד) מעל התוצאה המאוחדת (joined) על פני המקורות שלכם. היא יכולה לעשות זאת כי היא **שכבה סמנטית פעילה**: הגדרה אחת של אחוזת הנתונים (data estate) שלכם — כל דומיין, קשר (relationship), ומדיניות על פני המקורות שלכם, למעט מערכות המקור עצמן — שגם מפעילה את האחוזה וגם ממשלת (governs) אותה. ההגדרה אינה תיעוד שמנוע עשוי להתייעץ בו — היא *עצמה* המנוע. דומיינים וקשרים רשומים הם נתיבי ה-JOIN החוקיים היחידים, ומדיניות גישה מקומפלת (compiled) לתוך כל תוכנית שאילתה (query plan). מודל אחד, שלוש עבודות:

- **הגדרה (Define)** — דומיינים, עמודות, וקשרים מוצהרים פעם אחת. ההצהרה הזו היא הסכמה שכל צרכן רואה, והיא קבוצת נתיבי ה-JOIN היחידה שכל שאילתה רשאית לנקוט.
- **אכיפה (Enforce)** — אבטחה ברמת השורה (row-level security), מיסוך עמודות (column masking), נראות עמודות (column visibility), ואישור שאילתות מיושמים inline על נתיב הביצוע. אף שאילתה לא מגיעה לנתונים בלי לעבור דרכם, כך שהכיסוי מלא מעצם הבנייה ולא מתוך חריצות (diligence).
- **ביקורת (Audit)** — מכיוון שכל בקשה עוברת באותו נתיב ממושל, מי שאל מה, תחת איזה תפקיד (role), ומול איזו מדיניות נרשם באופן אחיד. עקבות מבוזרים (distributed traces), מדדים (metrics), ולוגים עצמם רשומים כטבלאות ניתנות לשאילתה לצד נתוני העסק שלכם.

ליבה ממושלת אחת משרתת כל שפה וכל תעבורה (transport). שאלו עם **GraphQL, Cypher, או SQL**; צרכו מעל **pgwire, Bolt, gRPC, REST, Arrow Flight, או JDBC**. כל שפת שאילתה מורדת (lowers) לייצוג ביניים יחיד (intermediate representation) שבו הממשל מוזרק פעם אחת — כך שמדיניות לא יכולה לסטות (drift) בין שפות — וייצוג הביניים הזה מכוון מחדש (retargets) לניב הילידי (native dialect) של כל מקור ביציאה. הוספת שפה היא חזית חדשה (front-end) על גבי הליבה המשותפת, לא מנוע חדש.

האחוזה היא גם אנליטית וגם טרנזקציונלית. קריאות חוצות-מקור מתפרשות (fan out) דרך שכבת הפדרציה; כתיבות וקריאות של מקור-יחיד מנותבות ישירות לדרייבר של המקור — ממושלות באופן זהה, אך טרנזקציוניות ומתחת ל-100ms. הזרמה טורית (columnar streaming) של Arrow Flight מובנית.

כל המודל בנוי מקומץ פרימיטיבים — דומיינים, קשרים, תפקידים, ומדיניות. אוצר מילים קטן, כך שההגדרה קלה להבנה ופשוטה להערכה ולביקורת: אפשר לקרוא את קבוצת המדיניות ולדעת מה היא עושה. Provisa היא מהדר שאילתות (query compiler) קליל, לא runtime שיושב בנתיב הנתונים. היא ממירה בקשה לשאילתות ילידיות, מנתבת אותן, ויוצאת מהדרך — וזו הסיבה שהאחוזה מבצעת (performs).

התכנון הזה תומך בשתי דרכי שימוש, והן אינן סותרות זו את זו:

- **כפיגום (scaffolding) למודרניזציה** — מדלו את האחוזה שלכם, תנו ל-Provisa לייצר את ה-SQL הילידי לכל מקור, ואז תפסו את ה-SQL הזה ואמצו אותו ישירות במערכת היעד. Provisa היא שכבת מעבר, לא תלות קבועה.
- **כתשתית קבועה האוכפת מדיניות** — השאירו אותה במקום כנתיב הממושל שכל שאילתה נוקטת, כך שהגדרה, אכיפה, וביקורת נשארות מאוחדות כל עוד האחוזה קיימת.

## מודל הפדרציה

כל המודל מתמצה בשני חוזים (contracts) ושתי מדיניות: מקורות מצטמצמים לטבלאות דו-ממדיות מעל מערכת טיפוסים אחת, שאילתות מצטמצמות לייצוג ביניים אחד דמוי-SQL, נגישות (reachability) קובעת מה נשאל בזמן אמת (live) לעומת מה ממומש (materialized), ואסטרטגיית עדכניות (freshness) מנהלת כל עותק ממומש ומערך נתונים נגזר. צורת נתונים בכניסה, צורת שאילתה בכניסה, ממשל בצומת ה-join, שאילתות ילידיות ביציאה. שאר הפרק הזה עובר על כל חלק.

המודל נשען על צמצום אחד: כל מקור מובע כאוסף של טבלאות דו-ממדיות מעל מערכת טיפוסים יחידה ומוכללת. זהו החוזה שמקור חייב לעמוד בו כדי להצטרף לאחוזה, והוא אותו חוזה עבור כולם. חלק מהמקורות כבר מתאימים — טבלת MySQL או PostgreSQL *היא* יחס דו-ממדי מוקלד (typed). חלק מתאימים עם הטלה (projection): תוצאת שאילתת GraphQL, לאחר שטוח (flattened), היא טבלה. חלק זרים לצורה — מאגרי triple של SPARQL, Neo4j — אך נשארים בני-עבודה, כי המשתמש מספק שאילתה שערכת התוצאות שלה טבלאית; השאילתה היא המתאם (adapter). יהיה המקור אשר יהיה, האחוזה רואה שורות, עמודות, וטיפוסים מוכללים, ותו לא. קליטת (onboarding) סוג מקור חדש היא עמידה באותו חוזה יחיד, לעיתים עם שלב של התערבות אנושית, לא כתיבת אינטגרציה מותאמת אישית.

לצמצום הזה יש תאום בצד השאילתה. SQL — על פני כל הניבים והמוזרויות שלו — הוא בעצם השפה לניתוח מעל מערכי נתונים דו-ממדיים, מה שהופך צורה דמוית-SQL ליעד האוניברסלי הטבעי לשאילתות. אז כל בקשה, בכל שפה שהיא מגיעה, מורדת לייצוג הביניים הזה כצעד הראשון שלה ממש. חלקן יורדות בקלות — SQL עצמו, ואפילו GraphQL; חלקן קשות — הסמנטיקה של נתיבים וגרפים ב-Cypher דורשת עבודה של ממש — אך כולן ניתנות לביצוע. ניתוב כל בקשה לייצוג ביניים אחד לפני שדבר אחר קורה הוא מה שמאפשר לממשל להיאכף במקום אחד בדיוק, על צורה אחת, ללא תלות בשפה שממנה היא הגיעה.

מעל שתי הצורות האחידות האלה — מקורות טבלאיים וצורת שאילתה יחידה — פדרציה כאן פירושה גם שאילתה חיה וגם warehousing — אותו טווח שמנוע שאילתה חי כמו Trino מכסה, בתוספת המימוש (materialization) שמנועים כאלה נשענים עליו. המושג שמאחד אותם הוא **נגישות (reachability)**: עבור כל מקור, האם המנוע יכול לשאול אותו במקומו, או שהנתונים שלו חייבים תחילה להיות ממומשים איפשהו שניתן לשאול אותו? נגישות מחלקת את האחוזה למה שנשאל בזמן אמת ומה שמועתק תחילה.

רוב מסדי הנתונים כבר נושאים מושג כלשהו של קישור חי — `ATTACH` של DuckDB, `postgres_fdw` של PostgreSQL, קישורים חיצוניים של Databricks. אז רוב מסדי הנתונים יכולים לפעול כמנוע פדרציה במידה מסוימת. אף אחד אינו מקיף: כל אחד מגיע לקבוצת מקורות מסוימת וממש את השאר, ללא חשבון יחיד של מי הוא מי. המודל סוגר את הפער הזה בכך שהוא הופך את הנגישות למפורשת — קבוצה מוגדרת של שיטות, פר-מקור, שקובעת מה המנוע יכול להגיע אליו בזמן אמת, ומתוך זה, מה חייב להיות ממומש.

מה שנשאר הוא עדכניות: עבור כל מקור בלתי-נגיש, כמה עדכני חייב להיות העותק הממומש שלו? הלכה למעשה זה מצטמצם לקבוצה קטנה של אסטרטגיות — לפי דרישה (on demand), לפי לוח זמנים, לפי אות שינוי (CDC, watermark, snapshot), או מוצמד (pinned). בחירת אחת פר-מקור היא כל מדיניות העדכניות.

מערכי נתונים אנליטיים — טבלאות נגזרות, אגרגציות, תוצרי טרנספורמציה — מתקפלים לאותה צורה. גם הם חייבים להיות מובעים בייצוג הביניים, ומכיוון שכך, שושלת (lineage) אינה מערכת נפרדת לתחזוקה: הנתיב מכל מערכת מקור לתוצר סופי *הוא* ייצוג הביניים שהפיק אותו, קריא מקצה לקצה. בניית מערכים כאלה מעלה את שאלת העדכניות צעד אחד רחוק יותר — האם מערך הנתונים מתרענן (refresh) לפי לוח זמנים, רק כשתנאיו הקודמים מתקיימים, ברציפות כמעט-בזמן-אמת, או כתמונת-מצב היסטורית מוצמדת? הדרכים לבטא איך ומתי לבנות מערך נתונים הן אותה קבוצה קטנה וספירה, כך שמערך נתונים נגזר נושא מדיניות בנייה (build policy) באותו אוצר מילים בדיוק שעותק מקור נושא.

מודלים ממדיים (dimensional models) הם יישום ישיר. טבלאות עובדה (fact) וממד (dimension) של סכמת כוכב (star schema) הן מערכי נתונים אנליטיים ככל אחרים — ממד הוא הטלה מותאמת (conformed) ומנוקה מכפילויות (deduplicated); טבלת עובדה היא join ואגרגציה שהומצתו לגרעין (grain) — כל אחת נושאת מדיניות בנייה ועדכניות משלה. ממדים משתנים לאט (Slowly changing dimensions) אינם דורשים מכניקה מיוחדת: תמונת-מצב מוצמדת היא היסטוריית Type 2, בנייה מחדש מתוזמנת היא Type 1. ומכיוון שהסכמה מוגדרת בייצוג הביניים ולא כבולה פיזית לטבלאות של warehouse אחד, אותן הגדרות עובדה וממד מכוונות מחדש — ממומשות ב-Oracle, ב-Databricks, או נשארות וירטואליות מעל מנוע MPP — בלי לבנות מחדש את המודל. המודל מייצר את סכמת הכוכב; הוא לא נועל אותה למנוע.

Data Vault מתאים באותה דרך, שכבה אחת מוקדם יותר. ה-hubs שלו הם מערכי נתונים של מפתחות עסקיים מנוקים מכפילויות, ה-links שלו הם הקשרים הרשומים ביניהם, וה-satellites שלו הם מערכי נתונים insert-only, מתויגי-זמן — הרשומה ההיסטורית. satellite הוא פשוט מערך נתונים נגזר על אסטרטגיית העדכניות של אות-שינוי: תאריך-טעינה בתוספת hashdiff הוא CDC המיושם על תכונות תיאוריות, והיסטוריה insert-only היא אסטרטגיית תמונת-המצב-המוצמדת. טבלאות point-in-time ו-bridge הן מערכי נתונים נגזרים נוספים שנבנו לביצועי שאילתה. אז raw vault הוא קבוצת מערכי נתונים אנליטיים בייצוג הביניים, וסכמת כוכב היא הטלה ממנו — שניהם מיוצרים, שניהם ניידים על פני מנועים. מה שהמודל לא עושה הוא להחליט את המתודולוגיה: מה הופך ל-hub, הגרעין של satellite, אסטרטגיית הפיצול. אלה נשארות בחירות מידול; ברגע שנעשו, הן חיות כייצוג ביניים ניתן להעברה במקום ETL מולחם ל-warehouse אחד.

שני הדפוסים מוצהרים דרך **שני קיצורי דרך (shortcuts) מדרגה ראשונה** במקום views כתובים ידנית — הפרימיטיבים שמהם כל סכמת כוכב ו-Data Vault בנויים, שמורים כנייטרליים-מתודולוגית:

- **`entity`** — הטלה בעלת מפתח, מנוקה מכפילויות, ואופציונלית מהיסטרת (historized) של מקור. הצהירו מפתח ישות, את התכונות, ומצב היסטוריה; Provisa מורידה אותה ל-view ממומש, וכשהיסטוריה מתבקשת ל-**MV דו-זמני (bitemporal)** (`scd2` → דלתא, `snapshot` → תמונת-מצב). קונסטרוקט אחד משרת **ממד** Kimball (SCD1/SCD2) ו-**hub + satellite** של Data Vault.
- **`fact`** — join למפתחות ישות, מוצמת לגרעין מוצהר, עם מדדים אגרגטיביים. Provisa מורידה אותה ל-MV אגרגטיבי בתוספת קשרים רשומים לישויות. קונסטרוקט אחד משרת **טבלת עובדה** של כוכב ו-**link** של Data Vault (עובדה חסרת-מדדים היא link טהור של קבוצת מפתחות).

מכיוון שההורדה טהורה (pure) — מפרט `entity`/`fact` הופך בדיוק ל-MV, לדו-זמניות, ולהגדרות הקשרים שמדלן היה כותב אחרת בעצמו — ה-warehouse הוא ייצוג ביניים מקצה לקצה ומכוון מחדש על פני מנועים בלי בנייה מחדש. הצהירו warehouse ב-UI הניהולי (טופס **Model** לישויות ועובדות) או מעל ה-API הניהולי (`registerEntity` / `registerFact`); המודל *מייצר* את הכוכב של Kimball או את ה-Data Vault, הוא לא כופה אחד.

### מסע בזמן (Time travel)

מסע בזמן הוא רעיון פשוט — לשמור כל גרסה של שורה במקום לדרוס אותה, כדי שתוכלו לשאול מה הנתונים *היו* בכל רגע בעבר. מה שמשתנה הוא כמה יעיל כל מנוע יכול לעשות זאת, וזו בדיוק הסיבה ש-Provisa הופכת אותו לתכונה של **הגדרת** ה-view הממומש ולא של מנוע האחסון (REQ-1162). הצהירו זאת פעם אחת; זה עובד על כל backend ממש.

הכלל ששומר על ניידות הוא **append-only**: גרסה, ברגע שנכתבה, לעולם לא מתעדכנת או נמחקת. פרישת שורה על ידי כתיבה חוזרת של תאריך "valid-to" — התרגיל הדו-זמני הרגיל — דורשת UPDATE, שמנועים רבים לא יכולים לעשות בזול (או בכלל) מעל חנות מפוזרת (federated), אז Provisa לא עושה זאת. במקום זאת כל רענון **מוסיף (appends)**, ו"איזו גרסה הייתה בתוקף בזמן T" נגזרת בזמן קריאה מהלוג הבלתי ניתן לשינוי. יש בדיוק שתי דרכים להוסיף:

- **Snapshot** — הוספת מערך הנתונים הטרי המלא, מתויג בזמן המערכת של רענון זה. אין diffing; נכון על כל מנוע; האחסון גדל בעותק מלא פר-רענון.
- **Delta** — הוספת רק מה שהשתנה, בתוספת מצבות (tombstones) עבור מפתחות שהוסרו. הדלתא **מחושבת על ידי המנוע** (anti-joins בתוך `INSERT … SELECT`), לעולם לא מקופלת שורה-אחר-שורה ב-Provisa. קטנה יותר, ודורשת מפתח ישות.

זמן המערכת (מתי Provisa רשמה גרסה) מנוהל בדרך הזו; זמן התוקף (מתי עובדה נכונה בעסק) מסופק על ידי ה-SELECT של ה-view עצמו ונשמר. מנועים שמציעים יותר — תמונות-מצב ילידיות של Iceberg, MERGE ששומר פחות שורות — יכולים להיות מכוונים ליעילות מאחורי אותה הצהרה; הנתיב append-only הוא הרצפה שנכונה בכל מקום.

קריאה שקופה. שאילתה רגילה מול MV דו-זמני משחזרת את המצב ה**נוכחי** מלוג ההוספה כברירת מחדל; כדי לנסוע בזמן, שלחו כותרת `X-Provisa-As-Of: <timestamp>` וכל השאילתה נענית כפי שהאחוזה הייתה באותו רגע — סמנטיקה זהה על כל substrate. הפעילו זאת עבור כל view ממומש ב-UI הניהולי (בקרת **Time Travel**: כבוי / snapshot / delta בתוספת מפתח ישות) או מעל ה-API הניהולי.

נגישות בתוספת עדכניות הוא מודל כללי לפדרציית נתונים: הגדרה שאומרת מה חי, מה ממומש, וכמה עדכני נשאר כל עותק — ללא תלות בטווח ההגעה של מנוע יחיד. התוצאה היא חופש מנעילה קניינית (proprietary lock-in). המודל ניתן להעברה; האחוזה אינה שבויה של איזה ספק שקורה להגיע לרוב המקורות היום.

## תכונות (Features)

### ממשקי שאילתה (Query Interfaces)

אלה השפות וה-APIs המובנים שבהם כותבים שאילתות. לכל אחד תחביר וסמנטיקה משלו; ממשל (RLS, מיסוך, נראות עמודות, אכיפת קשרים) מיושם באופן אחיד על פני כולם ללא תלות באיזה פרוטוקול תעבורה מספק אותם.

- **GraphQL** — סכמות פר-תפקיד עם נראות ברמת שדה, סינון, pagination מבוסס-cursor, ושאילתות אגרגטיביות (`count`, `sum`, `avg`, `min`, `max`). מוגבל-סכמה (schema-constrained) לקשרים רשומים — תקף מבנית מעצם הבנייה, הנתיב המהיר ביותר לשאילתה פשוטה ונכונה. Apollo APQ כלול: שאילתות מגובבות (hashed) ורשומות בצד השרת; קריאות עוקבות שולחות רק את ה-hash מעל HTTP GET, מה שהופך תגובות לניתנות ל-caching ב-CDN ללא שינויי לקוח נדרשים. טבלאות בדיקה (lookup) מתחת לסף שורות ניתן להגדרה נחשפות כטיפוסי enum.
- **SQL** — SQL מלא מעל נתונים מפוזרים; לא מוגבל ומבטא יותר מ-GraphQL. כתבו SQL סטנדרטי — כולל תת-שאילתות מתואמות (correlated subqueries) — והוא רץ על פני מקורות ללא שינוי. שאילתות מקור-יחיד עוקפות את שכבת הפדרציה לגמרי (מתחת ל-100ms).
- **Cypher** — שפת שאילתות גרפים מעל אותה סכמה מפוזרת. עברו על קשרים כקשתות גרף; אחדו מקורות; נתיבים באורך משתנה. הממשל מיושם זהה ל-GraphQL ו-SQL.
- **gRPC model API** — `.proto` שנוצר אוטומטית מהסכמה הרשומה; RPCs מוקלדים לשאילתה והכנסה פר-טבלה, תגובות מוזרמות (streamed). מונחה-סכמה באותו מובן כמו GraphQL — מודל הרישום הוא החוזה, protobuf הוא קידוד התעבורה. בניגוד ל-Arrow Flight (שהוא תעבורת הזרמה טורית), זהו ממשק שאילתה מלא פר-טבלה.
- **JSON:API** — API שאילתה מובנה ב-`/data/jsonapi/{table}`, HTTP-בלבד מעצם התכנון. תומך ב-JSON:API 1.1: שדות דלילים (`fields[table]=col1,col2`), ביטויי סינון (`filter[field][op]=value`), מסמכים מורכבים (`include=relation`), ומיון. לא שפת שאילתה כללית — שואל טבלה אחת בכל פעם עם תחביר סינון סטנדרטי במקום מחרוזת שאילתה מאולתרת.
- **Query Language Explorer** — כתבו שאילתת GraphQL וראו תרגומי **Semantic SQL** ו-**Cypher** חיים בפאנלים צדדיים; העתיקו כל אחד או קפצו ישירות לעורך SQL או Graph. זרימת עבודה מעשית היא לשרטט קטעי שאילתה ב-GraphQL, ואז לתפור את ה-SQL המתקבל ל-views או דוחות מורכבים.

ה-Explorer מציג שאילתת GraphQL לצד תרגומי SQL ו-Cypher חיים שלה:

![Query Language Explorer](docs/images/query-explorer.png)

אותה סכמה מפוזרת ניתנת לחקירה כגרף חי — תוויות דומיין וצומת, סוגי קשרים, ומעברים באורך משתנה:

![Graph Visualization](docs/images/graph-view.png)

### כלי הרכבת שאילתות (Query Composition Tools)

הכלים האלה עוזרים לכם לכתוב שאילתות בשפות שלמעלה — הם אינם שפות שאילתה בעצמם.

- **שאילתה בשפה טבעית** — צינור (pipeline) NL→SQL/Cypher/GraphQL מונע על ידי Claude. תארו מה אתם רוצים באנגלית פשוטה; הצינור מפיק שאילתה בשפה שבחרתם עם לולאת אימות אינטראקטיבית לפני הביצוע.

![Natural Language Query](docs/images/natural-language.png)

### פרוטוקולי תעבורה (Wire Protocols)

אלה פרוטוקולי החיבור. SQL, GraphQL, ו-Cypher רוכבים מעליהם — הבחירה בפרוטוקול תעבורה לא משנה את ממשק השאילתה או את התנהגות הממשל.

- **pgwire** — כל לקוח PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) מתחבר בפורט 5439 כאילו היה שרת Postgres. מקבל SQL בלבד. צינור הממשל המלא חל. `pg_catalog` ו-`information_schema` נענים מקטלוג בזיכרון כך שדפדפני סכמה עובדים בלי round-trip פדרציה. TLS אופציונלי.
- **Bolt (Neo4j)** — כל לקוח Neo4j (Neo4j Browser, Bloom, דרייברים רשמיים) מתחבר מעל פרוטוקול Bolt ומריץ Cypher מול הגרף המפוזר. כל תפקיד שהמשתמש מחזיק נחשף כמסד נתונים `provisa_<role>`. אותו ממשל כמו כל תעבורה אחרת. TLS אופציונלי.
- **Arrow Flight** — הזרמה טורית בעלת תפוקה גבוהה מעל gRPC; מקבל GraphQL או SQL כקלט שאילתה. ערכות תוצאה בלתי-מוגבלות, אין מימוש בצד שרת, אין צורך בתשתית נפרדת.
- **JDBC** — אינטגרציית כלי BI (Tableau, Power BI, DBeaver) במצב `approved` או `catalog`.
- **WebSocket / SSE** — מנויים (Subscriptions): אירועי שינוי כמעט-בזמן-אמת; backends: PG native, MongoDB native, CDC, polling. חשוף גם מעל Kafka.

### מקורות נתונים (Data Sources)

- **52 סוגי מקור** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, מאגרי triple של SPARQL, Kafka, Google Sheets, ועוד דרך API יחיד; מקורות גרף ו-RDF הם מדרגה ראשונה, לא מתאמים (adapters)
- **ניתוב חכם** — שאילתות מקור-יחיד עוקפות פדרציה (מתחת ל-100ms); שאילתות רב-מקור מנותבות דרך שכבת הפדרציה — הביאו את הקלאסטר שלכם או השתמשו בעובדים (workers) המוטמעים
- **מקורות API** — רשמו נקודות קצה REST, GraphQL, gRPC, WebSocket, או RSS כטבלאות ניתנות לשאילתה; עוזרי SPARQL כלולים; joins מפוזרים על פני מקורות API ומקורות יחסיים עובדים בשקיפות
- **בדיקת סכמה מרוחקת (Remote schema introspection)** — כוונו לכל נקודת קצה GraphQL, OpenAPI, או gRPC; פעולות מתועדות נחשפות אוטומטית כטבלאות, צמתי גרף, וקשתות ניתנים לשאילתה עם ממשל מלא מיושם מעליהם
- **מקורות קובץ** — קבצי CSV, Parquet, ו-SQLite כטבלאות ניתנות לשאילתה; תומך בנתיבים מקומיים ואחסון אובייקטים מרוחק (`s3://`, `ftp://`, `sftp://`)
- **אינטגרציית Kafka** — נושאים (topics) כטבלאות לקריאה בלבד; תוצאות שאילתה כ-sinks של Kafka
- **טריגרים מתוזמנים** — טריגרים cron ואינטרוול (APScheduler) שמפעילים webhooks, mutations, או פרסומי sink של Kafka
- **רמזי ביצועי פדרציה** — רמזי ניתוב מבוססי-הערת-SQL עוקפים החלטות ניתוב אוטומטיות

![Data Sources](docs/images/data-sources.png)

מקורות, קבצים, ונקודות קצה מרוחקות נרשמים כטבלאות ממושלות מה-UI:

![Table Registration](docs/images/table-registration.png)

### אבטחה וממשל (Security & Governance)

- **אבטחה ברמת שורה** — הזרקת סעיף WHERE פר-טבלה, פר-תפקיד
- **מיסוך עמודות** — מיסוך פר-עמודה (regex, קבוע, קיצוץ) עם עקיפה מבוססת-תפקיד
- **ערכי ברירת מחדל לעמודה (Column presets)** — ערכים סטטיים בצד השרת או משתני-סשן מוזרקים ב-insert/update; לא נחשפים בטיפוסי קלט של mutation
- **הרשאות כתיבה** — בקרת גישה למוטציה פר-עמודה (`writable_by`)
- **תפקידים בירושה** — תפקידים יורשים RLS, נראות, ומיסוך מתפקיד הורה באופן רקורסיבי
- **פונקציות ו-webhooks עקובים** — פונקציות DB ו-webhooks יוצאים חשופים כמוטציות GraphQL עם צורות החזרה מוקלדות
- **hook אישור ABAC** — hook הרשאה טרום-ביצוע; תעבורת webhook, gRPC, או unix_socket; היקף פר-טבלה, פר-מקור, או גלובלי; מדיניות נפילה (fallback) ניתנת להגדרה
- **אימות נתיק (Pluggable auth)** — Firebase, Keycloak, OAuth 2.0, simple (לבדיקות)

![Security Roles](docs/images/security-roles.png)

### אספקה וביצועים (Delivery & Performance)

- **Views ממומשים כטרנספורמציות מתועדות** — MV לוכד את הטרנספורמציה שהפיקה אותו: צורת ה-join או ה-SQL שלו, אותות הקלט פר-מקור (תמונת-מצב Iceberg, watermark של RDB) שממנו נבנה, ובדיקת דטרמיניזם בזמן הרישום. מכיוון שהטרנספורמציה מתועדת, שאילתות (או תת-ביטויים) נכתבות מחדש בשקיפות מעל MV טרי — התאמת דפוס-join מבנית עם תמיכה בהתאמה חלקית, כך ש-MV שמכסה תת-קבוצה של joins עדיין חל, כאשר joins נותרים משתמרים
- **הטמעת טבלאות חמות (Hot table inlining)** — טבלאות בדיקה קטנות ומצורפות-לעיתים-קרובות מוטמעות כ-VALUES CTEs ישירות בתוכנית השאילתה, מבטלות round trips חוצי-מקור עבור נתוני ממד
- **caching שאילתות** — מטמון תוצאות Redis מחולק לפי תפקיד+RLS; מטמון hash של APQ כלול
- **תצפיתיות (Observability) כנתונים** — עקבות מבוזרים, מדדים, ולוגים נאספים דרך OpenTelemetry, מכווצים ל-Iceberg על S3, ונרשמים אוטומטית כטבלאות ניתנות לשאילתה (`traces`, `metrics`, `logs`, `queries`) בסכמה המפוזרת; שאלו אותם עם SQL, GraphQL, או Cypher לצד נתוני העסק שלכם — צרפו טבלת `customers` לטבלת `queries` כדי לראות מי הריץ מה וכמה זמן זה לקח

### ניהול ואינטגרציה (Administration & Integration)

- **Admin API** — GraphQL ב-`/admin/graphql`; העלאה/הורדה של קונפיגורציה, עריכת קשרים, אישור שאילתות
- **מציג דוחות (Reports viewer)** — `/admin/reports` מציג את views ניהול תחום-התפעול המובנים וכל דוח מותאם אישית רשום; דורש את היכולת `observability`
- **תצוגה מקדימה של טבלה** — לכל טבלה רשומה יש מציג נתונים ממושל בעימוד צד-שרת עם מסננים pushed-down, group-by רב-רמתי, וייצוא CSV
- **GraphQL Voyager** — הדמיה אינטראקטיבית של סכמה מוגבלת-תפקיד כתרשים ישות-קשר
- **גילוי קשרים מבוסס-LLM** — הצעות מועמד מפתח זר מונעות Claude
- **לקוח Python** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → טבלאות pyarrow, dialect של SQLAlchemy, תמיכת ADBC
- **קליטת נתונים (Data ingestion)** — נקודות קצה HTTP לדחיפת נתוני אירוע JSON לתוך הפלטפורמה
- **ייבוא Hasura v2 / DDN** — המרת metadata של Hasura v2 או YAML של supergraph של DDN לקונפיגורציית Provisa
- **Apollo Federation** — חשיפת Provisa כ-subgraph של Apollo Federation v2

סכמה מוגבלת-תפקיד מודמת כתרשים ישות-קשר (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

קשרים נרשמים, מאושרים, ונאכפים כנתיבי ה-JOIN החוקיים היחידים:

![Relationships](docs/images/relationships.png)

## מודל האבטחה (Security Model)

כאן זה שבו "בנתיב שכל שאילתה כבר נוקטת" מפסיק להיות סיסמה. Provisa אוכפת מודל אבטחה רב-שכבתי על פני כל שפת שאילתה (GraphQL, SQL, Cypher) וכל תעבורה (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). הממשל מיושם באופן אחיד — אין נתיב שאילתה שעוקף אותו. הכיסוי מלא מעצם הבנייה, לא מתוך חריצות: הוסיפו מקור, עמודה, או קשר וכל שכבה חלה עליו אוטומטית, ללא דבר לזכור לרשום.

השכבות מיושמות בסדר. בקשה חייבת לעבור כל שכבה לפני שהבאה נבדקת.

### שכבה 0 — סינון בדיקת-סכמה (Introspection filtering)

הסכמה והקטלוג המוצגים לתפקיד מכילים רק את הטבלאות ברשימת `domain_access` שלו והעמודות שעוברות כללי `visible_to` פר-עמודה. אובייקטים מחוץ לגישת תפקיד בלתי-נראים בזמן גילוי — הם לא ניתנים לשאילתה, השלמה אוטומטית, או הסקת קיום. זה חל על סכמת GraphQL, קטלוג SQL, ודפדפן הסכמה של עורך השאילתות.

### שכבה 1 — גישה ציבורית

טבלאות בדומיינים ללא הגבלת `domain_access` נראות לכל הזהויות המאומתות ללא קונפיגורציה נוספת. אין חיכוך לנתונים ציבוריים באמת.

### שכבה 2 — גישת דומיין

כל תפקיד נושא רשימת `domain_access` של מזהי דומיין. שאילתה שנוגעת בטבלה מחוץ לדומיינים אלה נדחית לפני ביצוע. זהו גבול הבעלות הגס — תפקיד HR לא יכול להגיע לטבלאות כספים ללא תלות באיך ה-SQL כתוב.

### שכבה 3 — אבטחה ברמת שורה

לאחר שגישת הדומיין מאושרת, פרדיקטים (predicates) `WHERE` פר-טבלה, פר-תפקיד מוזרקים לכל `SELECT` בזמן ביצוע. הפרדיקטים מוערכים מול הנתונים הגולמיים. מנהל אזורי ששואל טבלת הזמנות משותפת רואה רק את שורות האזור שלו אפילו ב-`SELECT *`.

### שכבה 4 — נראות ומיסוך עמודות

עמודות עם רשימת `visible_to` שמחריגה את התפקיד המבקש מוסרות מפלט השאילתה. עמודות עם כלל מיסוך מקבלות את הערכים שלהן מוחלפים — redaction מבוסס-regex, החלפה קבועה, או קיצוץ — לפני שהתוצאות עוזבות את השרת. המיסוך חל בכל שפות השאילתה ופורמטי הפלט.

### שכבה 5 — שומר פרדיקט (Predicate guard)

עמודות ממוסכות נדחות מסעיפי `WHERE` ו-`HAVING`. בלי זה, קורא היה יכול להסיק את הערך הבלתי-ממוסך על ידי חיפוש בינארי בו במסנן למרות שהפלט ממוסך. הדחייה נאכפת בזמן ניתוח (parse) השאילתה, לפני ביצוע.

### ממשל קשרים (Relationship governance)

תנאי JOIN ב-SQL חייבים להתאים לקשר רשום ומאושר בין טבלאות. joins בלתי-מאושרים נדחים. כל קשר נושא סיבה ותיאור קריאים-לאדם — הנחיה גם למשתמשים וגם לסוכנים אוטונומיים לגבי מדוע נתיב מעבר קיים. זו מדיניות ממשל, לא גבול אבטחה קשיח: שכבות 2–5 מחזיקות ללא תלות במבנה ה-join, כך שעקיפה מכוונת לא חושפת נתונים שהתפקיד לא היה יכול להגיע אליהם דרך שתי שאילתות נפרדות. ניסיונות עקיפה נרשמים וניתנים לביקורת.

---

השכבות האלה מתחברות (compose). לתפקיד עם גישת דומיין, RLS, ועמודות ממוסכות יש את כל חמשת האילוצים פעילים בו-זמנית. הוספת מקור נתונים, עמודה, או קשר חדשים לא דורשת עדכון כל כלל — כל שכבה מוגדרת באופן עצמאי וחלה אוטומטית על כל שאילתה שנוגעת באובייקטים ממושלים.

### macOS

1. הורידו את [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (תמיד המהדורה האחרונה)
2. גררו את **Provisa.app** ל-`/Applications` ולחצו פעמיים כדי להפעיל
3. ההפעלה הראשונה משלימה הגדרה חד-פעמית (~2 דקות, אין צורך באינטרנט)
4. פתחו Terminal:

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. הורידו את [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (תמיד המהדורה האחרונה)
2. הפכו אותו לניתן-להרצה והריצו אותו — ההפעלה הראשונה משלימה הגדרה חד-פעמית (אין צורך באינטרנט):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. הורידו את [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (תמיד המהדורה האחרונה)
2. הריצו את המתקין — לא נדרשות הרשאות מנהל
3. פתחו את **Provisa First Launch** מתפריט Start — משלים הגדרה חד-פעמית (~5 דקות, אין צורך באינטרנט)
4. פתחו טרמינל חדש:

```bash
provisa start
```

### שאילתה ראשונה (First Query)

בפיתוח מקומי (`PROVISA_MODE=test`), לא נדרשים אישורים (credentials). בייצור (production), התאמתו עם Bearer token — התפקיד נשלף ממנו אוטומטית.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

הורידו את [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (תמיד המהדורה האחרונה) והוסיפו אותו לנתיב הדרייבר של כלי ה-BI שלכם.

```text
jdbc:provisa://localhost:8815
```

התאמתו עם שם המשתמש והסיסמה שלכם ב-Provisa — השרת מקצה את התפקיד שלכם.

- **מצב `catalog`** — סכמה מלאה נראית; השתמשו עם כלי קטלוג (Collibra, Atlan, DBeaver)

ראו [docs/integrations.md](docs/integrations.md) לשלבי הגדרה של Tableau ו-Power BI.

### פרוטוקול תעבורת PostgreSQL (pgwire)

Provisa דוברת את פרוטוקול התעבורה של PostgreSQL בפורט 5439. כל לקוח שיכול להתחבר ל-Postgres מתחבר ל-Provisa — אין דרייבר, אין מתאם, אין שינויים לכלים קיימים.

**שם המשתמש של PostgreSQL בוחר את התפקיד ב-Provisa.** עם `provider: none` (מצב trust), הסיסמה מתעלמת וכל שם תפקיד מוגדר מתקבל כשם משתמש — התחברו כ-`analyst`, `admin`, או כל תפקיד כדי לראות את התצוגה הממושלת של אותו תפקיד על הנתונים. עם `provider: simple`, הסיסמה מאומתת ב-bcrypt. ספקים אחרים (`firebase`, `keycloak`, `oauth`) אינם נתמכים מעל pgwire.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

כל השאילתות רצות דרך צינור הממשל המלא — גישת דומיין, RLS, מיסוך, ושומר פרדיקט חלים בדיוק כפי שהם חלים עבור GraphQL ו-REST. דפדפני סכמה (DBeaver, DataGrip, pgAdmin) עובדים מהקופסה: שאילתות `pg_catalog` ו-`information_schema` נענות מקטלוג בזיכרון מוגבל לגישת הדומיין של התפקיד, כך שמשתמשים רואים רק את הטבלאות והעמודות שהם מורשים לשאול.

DataGrip מדפדפת בסכמה הממושלת ובתרשים המפתח-הזר שלה מעל pgwire — אין דרייבר, אין מתאם:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS מופעל על ידי הגדרת `PROVISA_PGWIRE_CERT` ו-`PROVISA_PGWIRE_KEY`. הפורט ניתן להגדרה דרך `PROVISA_PGWIRE_PORT` (ברירת מחדל `5439`).

### Bolt (פרוטוקול תעבורת Neo4j)

Provisa דוברת גם את פרוטוקול ה-**Bolt** של Neo4j, כך שכלים ילידי-גרף מתחברים ישירות ומריצים Cypher מול הגרף המפוזר — אין ייצוא, אין מסד נתונים גרף נפרד. כוונו את **Neo4j Browser** או **Bloom** ל-Provisa ועברו על קשרים על פני מקורות עם אותו ממשל (גישת דומיין, RLS, מיסוך) מיושם.

Neo4j Browser מריץ Cypher מול Provisa — תוויות צומת, סוגי קשרים, ומפתחות תכונה מגיעים ישירות מהסכמה הרשומה:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

הפעילו אותו על ידי הגדרת `PROVISA_BOLT_PORT` (ברירת המחדל של Neo4j היא `7687`). TLS מופעל עם `PROVISA_BOLT_CERT` ו-`PROVISA_BOLT_KEY`. כל תפקיד ב-Provisa שהמשתמש המאומת מחזיק נחשף כמסד נתונים נבחר `provisa_<role>` (הבוחר `provisa_admin` למעלה) — בחירת אחד מצמצמת את הסשן לזכויות הדומיין של אותו תפקיד; המשתמש לעולם לא יכול לחרוג מהתפקידים שהוא מחזיק.

### לקוח Python

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ראו [docs/python-client.md](docs/python-client.md) להפניה מלאה.

## תיעוד (Documentation)

| נושא | מסמך |
| --- | --- |
| התחלה מהירה למפתחים (הרצה מהמקור) | [docs/quickstart.md](docs/quickstart.md) |
| הפניית קונפיגורציית YAML מלאה | [docs/configuration.md](docs/configuration.md) |
| הפניית נקודות קצה (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| עיצוב מערכת ומפת רכיבים | [docs/architecture.md](docs/architecture.md) |
| מודל אבטחה (RLS, מיסוך, אימות) | [docs/security.md](docs/security.md) |
| סוגי מקור נתמכים | [docs/sources.md](docs/sources.md) |
| מנויי SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, כלי BI, לקוחות Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| לקוח Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| Admin API | [docs/admin.md](docs/admin.md) |
| פריסה (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| ייבוא Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| זרימת עבודה של שחרור (תגי alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## מידוד (Sizing)

Provisa כוללת מנוע פדרציה מובנה לשאילתות רב-מקור. בהפעלה הראשונה בוחרים תקציב RAM; Provisa גוזרת את מספר עובדי הפדרציה המקומיים אוטומטית.

| RAM מארח | עובדים | עומס עבודה טיפוסי |
| --- | --- | --- |
| < 24 GB | 0 | פיתוח, שאילתות מקור-יחיד, צוותים קטנים |
| 24–47 GB | 1 | צוות קטן, שאילתות חוצות-מקור מתונות |
| 48–95 GB | 2 | פריסה מחלקתית, שימוש מעורב BI + notebook |
| 96 GB+ | 4 | מחלקה גדולה, פדרציה מקבילית כבדה |

מספר העובדים ניתן לשינוי בכל עת על ידי עריכת `~/.provisa/config.yaml` (`federation_workers: N`) והרצת `provisa restart`. הגדירו ל-`0` כדי לרוץ תיאום-בלבד (single-node).

### קנה מידה מעבר לקופסה יחידה

**קנה מידה אופקי (Horizontal scale-out)** — הריצו מספר מופעי Provisa מאחורי מאזן עומסים. כל מופע הוא מערכת פועלת במלואה. כל המופעים חייבים להצביע על אותו DB קונפיגורציה (הגדירו `CONFIG_DB_HOST` בקופסאות משניות) ואופציונלית מופע Redis משותף (`REDIS_URL`) עבור מטמון מאוחד. רוב השאילתות מתפזרות בשקיפות; joins חוצי-מקור גדולים מאוד עשויים לחרוג ממשאבי מופע יחיד ולדרוש קופסה גדולה יותר או קלאסטר פדרציה חיצוני.

**Redis משותף** — הגדירו `REDIS_URL` בכל מופע כדי להצביע על Redis חיצוני. Redis משותף פירושו שרשומות מטמון ממופע אחד זמינות לכולם, משפר שיעורי פגיעה (hit rates) על פני הקלאסטר.

**הביאו קלאסטר פדרציה משלכם** — כוונו את Provisa לקלאסטר פדרציה חיצוני קיים במקום העובדים המוטמעים. מומלץ לפריסות בקנה מידה גדול או ענן; ראו [docs/deployment.md](docs/deployment.md) לקונפיגורציה.

## רישיון (License)

Business Source License 1.1 (בלתי משונה, לפי התחייבויות ה-Licensor של MariaDB). כל
גרסה משוחררת מומרת ל-Change License (GPL v2.0 ומעלה) ביום השנה ה-4
לשחרורה הציבורי; קוד נוכחי ואחרון נשאר תחת BSL.
שימוש בייצור מעל סף ה-Additional Use Grant (פחות מ-100
עובדים/קבלנים ומתחת ל-$1M הכנסה בשנה הקודמת) דורש רישיון
מסחרי. ראו [LICENSE](LICENSE).

ה-Licensor אינו מסכים לשימוש בעבודה זו לאימון AI/ML. ראו
[NOTICE](NOTICE), [ai.txt](ai.txt), ו-[robots.txt](robots.txt). לרישיונות מסחריים
או אימון AI: <kennethstott@gmail.com>
