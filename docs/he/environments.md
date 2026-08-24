# סביבות

סביבה היא עותק בעל שם של המודל הממושל של ארגון. העותק הוא פיזית
סכמת PostgreSQL נפרדת — לא עמודת מבחין, לא קידומת, סכמה אמיתית — כך שכל
שאילתת מאגר קיימת נכונה בתוך סביבה ללא שכתוב כלשהו, ושורות של סביבה
אחת אינן יכולות להגיע לקריאה של אחרת דרך פרדיקט שנשכח (REQ-1487, REQ-1488).
[tool-verified: `environments.py` module docstring; `org_schema()` at environments.py lines 86-96]

כל ארגון מתחיל עם סביבה אחת בשם `prod`. לא ניתן למחוק אותה או לשנות את שמה.
בקשה שאינה נוקבת בשם סביבה מוגשת על ידי `prod`; בקשה הנוקבת בשם סביבה שאינה קיימת
נדחית. [tool-verified: `PROD = "prod"` at environments.py line 44; `select_environment()`
at env_routing.py lines 93-129]

סביבות זמינות לארגונים בתוכנית בתשלום. [inferred: REQ-1507]

## שמות סביבות

שם חייב להתאים ל-`[a-z][a-z0-9_]{1,31}` — שניים עד שלושים ושניים תווים של אותיות קטנות,
ספרות וקווים תחתונים, המתחילים באות. ‏`prod` ושמות המתחילים ב-`pg_` נדחים.
האורך המרבי עבור ארגון מסוים תלוי במזהה של הארגון עצמו: ‏PostgreSQL קוטעת מזהה
מעל 63 בתים בשקט, ושם הסכמה הארוך ביותר שסביבה גוזרת הוא מה שהתקרה מגנה
מפניו. [tool-verified: `ENV_NAME_PATTERN` at environments.py line 59; `validate_env_name()` at
environments.py lines 119-142; `max_env_name_length()` at environments.py lines 108-116]

## מה עותק נושא

כל טבלה בסכמת הארגון נופלת במחלקה אחת בדיוק (REQ-1489). הסיווג הוא
רשימת היתר, לא רשימת החרגה: טבלה שנוספת מאוחר יותר אינה נודדת עד שמישהו נוקב במחלקתה
כאן, כך שמצב הכשל עבור טבלה שנשכחה הוא בדיקה אדומה. [tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| מחלקה | טבלאות | מה קורה בהעתקה |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | מועתקות במלואן |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | שדות זהות וממשל נודדים; ערכי חיבור נשארים מאחור (ראו קישורים) |
| SEEDED_AT_CREATION | roles, user_role_assignments | מועתקות רק כשסביבה נוצרת לראשונה; מיזוגים מאוחרים יותר משאירים אותן לנפשן |
| PARTIAL | org_settings | מועתקות לכל מפתח: הגדרות ממשל נודדות, מפתחות הנוקבים ביעד חיצוני או בזמן ריצה לכל סביבה נשארים מאחור |
| NEVER_SENSITIVE | org_secrets, user_directory | לעולם אינן מועתקות |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log, ואחרות | לעולם אינן מועתקות |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

‏`SEEDED_AT_CREATION` קיים כדי לפתור בעיה ספציפית אחת. סביבה חדשה זקוקה לתפקידים ולהקצאות
אחרת היא נפתחת כשאיש אינו יכול לפעול. אבל מיזוג מאוחר יותר שהיה נושא את שורת ה-`developer`
של `prod` היה דורס את הגרסה המוגבלת שענף מוגבל עשוי להזדקק לה, והיה הופך את נתיב הסקירה
לדרך ההסלמה. לכן תפקידים והקצאות נודדים פעם אחת, ביצירה, ולאחר מכן הם התשובה של כל
סביבה עצמה. [tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## קישורים

קישורים הם העמודות האומרות לאן מקור מצביע בפועל — `host`, `port`, `database`,
‏`username`, וכל היתר. הם לעולם אינם נודדים בשום עותק. סביבה שלא נקשרה
מסומנת `unbound` במקום להישאר ריקה: מארח ריק אינו מארח נעדר, ובונה
החיבור היה קורא אותו כ-`localhost:5432`. [tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

המקורות של סביבה מתפענחים באחת משתי דרכים.

**בסיס** — הסביבה נושאת אישורי גישה משלה. ‏org_admin יוצר בסיס ואז קושר
כל מקור במפורש. [tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**ענף** — הסביבה יורשת את אישורי הגישה של הבסיס בהפניה. שום דבר אינו מועתק.
כששאילתה זקוקה לחיבור, הפענוח מטפס במעלה שרשרת ה-`branched_from` ועוצר
בסביבה הראשונה ששורתה קשורה. סבב אישור גישה בבסיס מתפשט לכל ענף
שלו ללא כל פעולה נדרשת. שלילתו שוללת עבור כולם בבת אחת. שום סוד אינו ממוטריאל
בשום מקום שממנו ענף, ייצוא או מאגר יכלו לשאת אותו הלאה.
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

כדי ליצור ענף, קבעו **Inherit connections** בפאנל הסביבות. ברירת המחדל היא כבוי.
[tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## הטלת ה-git

כל כתיבה למודל מבצעת קומיט של התוצאה לענף ה-git של הסביבה. המאגר הוא
הטלה של המודל, לעולם לא סמכותו: Provisa קוראת וכותבת אל מישור הבקרה;
המאגר הוא הרשומה, לא המקור. פריסת עץ דורשת קריאה מפורשת — pull request
שמוזג במארח ה-git אינו פורס את עצמו (REQ-1524, REQ-1526). [tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

כל ישות מקבלת קובץ אחד. הנתיב הוא ה-URI מ-REQ-1385 עם קיצוץ הסכמה והארגון:
‏`provisa://acme/sales/tables/Order` הופך ל-`sales/tables/Order.yaml`. מקורות נוחתים ב-`sources/`,
פקודות ב-`commands/`, מדדים ב-`metrics/`. שורות בת המדורגות מהורה — עמודות,
קשרים, כללי RLS — נכתבות בתוך קובץ ההורה, לא כקבצים משל עצמן.
[tool-verified: `table_path()` at env_files.py line 109-115; `kind_path()` at env_files.py
lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py line 71; env_files.py module
docstring lines 17-24]

פקודות והקצאות התגיות שלהן שורדות את מסע הלוך ושוב. תגית על פקודה מנותבת לקובץ
של הפקודה עצמה (`commands/<name>.yaml`); תגית שאינה שייכת לשום קובץ נעלמת מן
ההטלה ותימחק בפריסה הבאה של אותו עץ. [tool-verified:
env_project.py lines 346-364; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

שום מפתח חלופי אינו מגיע לקובץ. ‏`registered_tables.id` הוא מספר שלם בקידום אוטומטי — אותו
מודל בשתי סביבות מקבל מספרים שלמים שונים, ולכן dump נאיבי יוצר diff מול עצמו. כל
מפתח חלופי מופל וכל הפניה אליו נכתבת כנתיב היעד.
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

הסריאליזציה דטרמיניסטית. מפתחות נפלטים לפי סדר אלפביתי, אוספי בת ממוינים לפי
כתובתם, וסגנון ה-YAML קבוע. שתי סביבות המחזיקות באותו מודל מייצרות
עצים זהים בית-בבית. [tool-verified: `dump()` at env_files.py lines 131-143]

## מיזוג

מיזוג המודל של סביבה לתוך אחרת מעדכן לפי זהות: כל אובייקט שיש למקור
נוצר או מעודכן ביעד. אובייקטים שכבר אין למקור מוסרים רק כשהקורא מבקש
הסרות במפורש. מיזוג שנכשל באמצע משאיר את היעד כפי שהיה — טרנזקציה
אחת. [tool-verified: `copy_model()` at env_copy.py lines 216-234; REQ-1490 description]

לפני היישום, קראו לנקודת קצה התצוגה המקדימה (`GET /{name}/merge-preview`) או העבירו `dry_run: true`.
התצוגה המקדימה מריצה את אותו נתיב קוד שהמיזוג משתמש בו; זו נקודת קצה `GET` כך שסקריפט CI
שטועה בדגל אינו יכול ליישם בטעות את המיזוג שהתכוון לבחון. [tool-verified:
`preview_merge()` docstring at environments_router.py lines 1086-1095]

מיזוג משאיר את הקישורים, התפקידים והסודות של היעד בדיוק כפי שהיו. סביבת פיתוח
אינה מאבדת את חיבורי מסד הנתונים שלה עצמה כשהיא לוקחת מודל חדש יותר מ-prod. ‏prod אינו רוכש
את ההרשאות של dev. [tool-verified: env_copy.py lines 269-287; REQ-1490 scenario]

### במה הדוח נוקב

דוח המיזוג מונה, לפי נתיב, מה נוסף, מה השתנה, מה הוסר ומה נשאר ללא שינוי. הוא גם
נוקב בשם כל **התנגשות** — אובייקטים ששני הצדדים שינו מאז שחלקו קומיט לאחרונה. התנגשות
מדווחת ולא נפתרת: המקור מנצח, וזו משמעותו של מיזוג לתוך יעד. Provisa
אינה מציעה פתרון התנגשויות, לא סמני מיזוג ולא בחירה לכל אובייקט. ערכה של רשימת
ההתנגשויות הוא האות — שני אנשים ערכו את אותו אובייקט מבלי לדעת (REQ-1555).
[tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165; `detect_conflicts()` called
at env_copy.py lines 261-263; REQ-1555 description]

אובייקט ששני הצדדים שינו לאותו ערך הוא הסכמה, לא התנגשות. כששתי
הסביבות אינן חולקות אב קדמון כלל, הבסיס הוא `None` בדוח ורשימת ההתנגשויות הריקה
פירושה ששום דבר לא הושווה, לא ששום דבר לא התנגש. [tool-verified: `CopyReport.compared`
property at env_copy.py lines 164-166; env_copy.py lines 255-264]

המיזוג נוחת כקומיט מכווץ אחד על ענף היעד. הודעת הקומיט היא חובה
ואסור שתהיה ריקה — היא הדין וחשבון היחיד על טווח העבודה שהכיווץ מייצג.
הקומיטים של המקור נשארים במקומם וניתנים לפריסה לפי SHA גם אחר כך.
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## משיכה

משיכה לוקחת את מה שה-remote מחזיק עבור סביבה והופכת אותו למודל. היא אינה מקדמת
את הענף המקומי ישירות ב-fast-forward; היא מיישמת את העץ שהובא דרך נתיב הפריסה הרגיל,
כך שאותו אימות ואותה ביקורת הממשלים פריסה ידנית ממשלים משיכה.
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

כמו מיזוג, משיכה מדווחת על מה שדרסה — אובייקטים שהעץ הנכנס שינה ושגם הסביבה
המקומית שינתה מאז ששני הקווים חלקו קומיט לאחרונה. שינוי מקומי שלא בוצע בו קומיט
הוא סביבה שסטתה (ראו היסטוריה למטה); משיכה נוקבת בו כשינוי רגיל בדוח.
[tool-verified: REQ-1556 description; `pull_environment()` at environments_router.py
lines 1485-1519]

משיכה נדחית כששני הקווים **התפצלו** — לשניהם יש קומיטים שאין לאחר.
הדחייה נושאת את רשימת האובייקטים ששני הצדדים נגעו בהם, כך שהאדם שעליו להחליט כעת
של מי העבודה שורדת יודע באילו אובייקטים להסתכל. [tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503; `_collisions()` at environments_router.py
lines 1581-1602]

## היסטוריה

כל פריסה מקדמת את הסמן של הסביבה קדימה בקו הקומיטים שלה עצמה. ביטול מדלג אחורה
קומיט אחד; ביצוע חוזר מדלג קדימה שוב לעבר המיקום שהביטול יצא ממנו. אף אחת
מהפעולות אינה מסירה קומיט — דילוג אחורה מוסיף מיקום, הוא אינו משכתב היסטוריה.
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

ענף נזרע בקצה הסביבה שממנה נוצר, ולכן ביטול עוצר בנקודת
הזריעה ההיא ואינו הולך אל הקומיטים של סביבת ההורה. [tool-verified:
`origin_sha` comment at environments_router.py lines 428-448; `_move()` at
environments_router.py lines 907-916]

הדגלים `can_undo` ו-`can_redo` נודדים עם תגובת רשימת הסביבות. שניהם מדווחים `false`
כשההטלה אינה מחזיקה את הקומיט שמישור הבקרה נוקב בשמו — מצב שהתכנון מכיר בו,
הקרוי **סטייה**. צומת שמאגר האחסון שלו מעולם לא קיבל קומיט מסוים עדיין מונה את
סביבותיו; רק תשובות ההיסטוריה משתנות (REQ-1561). [tool-verified: `_with_history()`
at environments_router.py lines 316-344; REQ-1561 description]

## הרשאה

סביבות ממושלות על ידי שתי זכויות. אף אחת מהן אינה של אנליסט כברירת מחדל (REQ-1573).
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| זכות | מי מחזיק בה (זרוע) | במה היא ממשלת |
| --- | --- | --- |
| `environment_management` | org_admin, developer | יצירה ומחיקה של סביבות |
| `environment_switch` | org_admin, developer | להיות מוגש על ידי כל סביבה שאינה prod |

‏`prod` אינו זקוק לזכות — הוא מה שבקשה שאינה נוקבת בדבר מוגשת על ידיו, ודחייתו הייתה
דוחה כל בקשה.

האכיפה היא בנקודת הבחירה, לפני שמגיעים לנתיב כלשהו. חבר שחסרה לו
‏`environment_switch` נדחה עבור כל המשטחים בבת אחת — HTTP, ‏GraphQL, ‏SQL ופרוטוקולי
התעבורה — משום שהסביבה נכבלת ב-middleware, לא במטפלים בודדים.
[tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

אנליסט שאינו נושא זכות סביבה יכול לשאול את `prod` ואינו יכול לראות את מחליף הסביבות.
קבלן שקיבל את תפקיד האנליסט אינו רואה משטח סביבות ואינו יכול ליצור סביבה כלשהי שאינה
ייצור ולא לעבור אליה. [tool-verified: REQ-1573 use_case and scenario]

### סמכות בעלי הסביבה

יצירת סביבה היא הנתיב היחיד שדרכו חבר לקריאה בלבד רוכש זכויות עריכת מודל
(REQ-1528). בתוך הסביבה שיצר, היוצר מחזיק ביכולות של תפקיד ה-`developer` —
פחות זכויות הנתונים (`write`, `full_results`, `usage`). זכויות בניית מודל,
לא זכויות נתונים. [tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77;
`_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py module docstring lines 14-38]

ההענקה נגזרת מ-`environments.created_by` בזמן ההרשאה, ולעולם אינה נכתבת לטבלת
הענקות. מחיקת הסביבה מסירה אותה באותו מעשה.
[tool-verified: env_authority.py module docstring lines 39-42; `environment_owner()` at
env_authority.py lines 84-98]

חברות בדומיין עדיין מגבילה את מה שהבעלים רשאי לשנות. הסתעפות משנה את מה שחבר רשאי לעשות;
היא לעולם אינה משנה לאילו דומיינים הוא רשאי לעשות זאת (REQ-1530).
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## סביבות מוגנות (REQ-1504)

ניתן להגן על סביבה. מיזוג או פריסה לתוך סביבה מוגנת אינם מיושמים
כשהם מתבקשים; הם מוצעים, ומישהו שאינו המבקש חייב לאשר אותם.

‏`prod` מוגן אוטומטית ברגע שלארגון יש יותר מחבר אחד. ארגון בעל חבר יחיד
אינו יכול לקיים את "מישהו שאינו המבקש", ולכן הכלל אינו מיושם שם — הוא היה
הופך את `prod` לבלתי ניתן למיזוג. כל סביבה יכולה להיות מסומנת כמוגנת על ידי org_admin.
[tool-verified: `is_protected()` at env_approvals.py lines 79-96; `protectedHelp2` UI string
in environmentsTab.json line 28]

בקשת מיזוג היא שורה, לא דיאלוג אישור. המאשר הוא בהגדרה אדם שונה
מהמבקש ואינו נוכח ברגע הבקשה; אישור בן-חלוף היה כופה אישור
בתוך המפגש של המבקש, וזהו הסידור היחיד שהדרישה אוסרת. [tool-verified: env_approvals.py module docstring lines 11-17]

שורת הבקשה נושאת את דוח המיזוג לצד הודעת המבקש. התיישנות נגזרת
בזמן הקריאה, לעולם אינה מאוחסנת: תכנון מחדש בזמן הקריאה והשוואה מול הדוח המאוחסן היא
הגרסה היחידה שאינה יכולה להיות שגויה. בקשה מיושנת חייבת להתבקש מחדש. המבקש אינו יכול
לאשר את בקשתו שלו. [tool-verified: `STALE` constant and `effective_state()` at
env_approvals.py lines 53, 215-243; `decide()` lines 265-268]

מצבי מחזור החיים של בקשה: ‏`requested` → `approved`/`rejected` → `applied`. `stale` נגזר.
[tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

אותה דלת מטפלת בפריסות מ-ref של מאגר: הבקשה מקבעת את ה-SHA בזמן ההצעה.
אם ה-ref זז בין ההצעה להחלטה, המאשר קורא את הדוח עבור הקומיט
המקובע, לא החדש. [tool-verified: `request_deploy()` at env_approvals.py lines
150-189; env_approvals.py docstring lines 26-27]

!!! note
    ממשק בקשות המיזוג נמצא תחת הלשונית **Merge requests** בפאנל הסביבות.
    העמודה **Report** מציגה מה היה משתנה לפי ספירה; השורה נפתחת כדי להציג פירוט
    לכל אובייקט. [tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## פקודות ה-CLI ‏`env`

‏`provisa env deploy` שולחת את המודל ב-ref לתוך סביבה. היא יוצאת עם 0 כשהפריסה
יושמה או הייתה הרצה יבשה, ועם 2 כשהסביבה מוגנת והפריסה רק הוצעה
— צינור המתייחס לאישור ממתין כאל פריסה ששוחררה היה טועה, וקוד היציאה
אומר זאת. [tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

‏`provisa env fetch` מביאה את ענפי ה-remote של הארגון אל המאגר המקומי. פריסה יכולה אז
לנקוב ב-`origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

שתי הפקודות מקבלות `--api` (כתובת ה-URL של ה-API של Provisa) ו-`--token` ‏(אסימון bearer). קבעו
`PROVISA_API_URL` ו-`PROVISA_API_TOKEN` בסביבה כדי להימנע מהעברתם בכל קריאה.
[inferred: shared `_api_call()` helper]

צינור ה-CI הטיפוסי עבור זרימת עבודה מגובת-מאגר:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## ראו גם

- [פריסה](deployment.md) — כיצד להקים את מישור הבקרה שאליו סביבות מתחברות
- [פקודות](commands.md) — פונקציות ו-webhooks נמעקבים המופיעים בעץ של כל סביבה
