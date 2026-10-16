# מדריך CLI {: #cli-reference }

הפקודה `provisa` היא נקודת הכניסה היחידה לשכבה המוטבעת (embedded) המותקנת דרך pip (REQ-1128).
היא מפעילה את הריצה, מנהלת רישיונות, מפעילה פרסום מטא-דאטה, פורסת מודלים, ושולטת
בבאנר התחזוקה — ללא Docker, Node, או שירותים חיצוניים כלשהם.

התקן אותה עם התוסף `embedded`, שגם מושך את תוספי DuckDB האופליין
ואת מישור הבקרה המוטבע של PostgreSQL:

```bash
pip install 'provisa[embedded]'
```

**דרישות פלטפורמה.** `provisa run` דורשת Python 3.12 ופלטפורמה עם wheel של pgserver:
linux x86_64, macOS, או Windows x86_64. ל-Linux aarch64 אין wheel של pgserver ואין הפצת מקור,
כך שהשכבה המוטבעת אינה רצה שם. השתמש בשכבת הקונטיינר ב-aarch64.
[tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## אפשרויות משותפות {: #shared-options }

מספר תת-פקודות קוראות ל-API של HTTP של Provisa. הן חולקות שלושה דגלים ושני משתני
סביבה. [tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| דגל | ברירת מחדל | נפילה חוזרת (fallback) למשתנה סביבה |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(ללא)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 עבור `maintenance`) | _(ללא)_ |

`--api` היא כתובת ה-URL הבסיסית של מופע Provisa פעיל. תחת ריבוי-דיירים (multitenancy) שם המארח
נוקב את הארגון — `https://acme.provisa.org` מנתב לדייר של acme. `--token` הוא אסימון Bearer;
כשהוא ריק לא נשלחת כותרת `Authorization`, וזה נכון לפריסות ללא אימות.
[tool-verified: cli.py:314-316, 357-365]

הגדר את שני המשתנים בסביבת ה-CI שלך כדי להימנע מחזרה עליהם בכל קריאה:

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

תת-פקודות המקבלות דגלים אלה: `metadata export`, כל תת-פקודה של `env` (`list`, `show`,
`create`, `delete`, `diff`, `merge`, `deploy`, `revert`, `redo`, `fetch`, `push`, `pull`),
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

הפעל את מערכת Provisa המוטבעת — שרת API ושרת UI static/proxy — בתהליך יחיד.
ללא Docker, ללא Node, ללא שירותים חיצוניים. [tool-verified: cli.py module docstring lines 11-23;
`_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### דגלים {: #flags }

| דגל | ברירת מחדל | הערות |
| --- | --- | --- |
| `--demo` | כבוי | טען את הדמו המצורף — תחומי דוגמה pet-store ו-shelter מעל SQLite מוטבע (REQ-414) |
| `--host` | `127.0.0.1` | כתובת קישור לשני השרתים |
| `--api-port` | `8000` | פורט שרת ה-API |
| `--ui-port` | `3000` | פורט שרת ה-UI static/proxy |
| `--no-browser` | כבוי | דלג על פתיחת דפדפן כאשר ה-UI מוכן; עדיין מדפיס את ה-URL |
| `--reset` | כבוי | מחק ובנה מחדש את חנות מישור הבקרה המוטבעת לפני ההפעלה; השתמש לאחר שדרוג Provisa אם ההפעלה מדווחת על אי-התאמת סכמה |
| `--data-dir` | `~/.provisa/native` | תיקייה המחזיקה את אשכול ה-PostgreSQL המוטבע ואת מטמון תוסף DuckDB |

[tool-verified: run subparser at cli.py:609-634]

### משתני סביבה {: #environment-variables }

`provisa run` קוראת מספר משתנים נוספים לפני שהשרתים של HTTP מתחילים.
הגדר אותם כדי לדרוס את ברירות המחדל ש-`load_profile("native", ...)` היה מחיל אחרת.
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| משתנה | אפקט |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | מחליף את מנוע ה-DuckDB המוטבע ב-Trino coordinator שסופק על ידי הלקוח (REQ-1129) |
| `PROVISA_ENGINE_URL` | דרך חלופית להצביע על מנוע פדרציה חיצוני |
| `PROVISA_CONFIG` | קובץ הגדרות לטעינה; `--demo` קובע אותו להגדרות הדמו המצורפות (REQ-1127) |
| `PROVISA_DEMO` | נקבע ל-`1` על ידי `--demo`; מסמן את הסשן כהרצת דמו |
| `PROVISA_DEMO_DIR` | נתיב לתיקיית נתוני הדוגמה של הדמו; נקבע על ידי `--demo` |
| `PROVISA_DUCKDB_EXT_DIR` | תיקיית תוסף DuckDB מוכנה מראש; נקבעת אוטומטית מהחבילה `provisa-duckdb-ext` אם קיימת; היעדרה משמעו ש-DuckDB מוריד מהרשת בשימוש הראשון |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### רצף ההפעלה {: #startup-sequence }

1. בדיקת פלטפורמה — מפסיקה עם הודעה ברורה על Python לא נתמך או pgserver חסר.
2. `--reset` (אם התבקש) — מוחק את אשכול ה-PostgreSQL המוטבע; הוא נבנה מחדש בשלב הבא.
3. הגדרות דמו (אם `--demo`) — קובעות את `PROVISA_CONFIG` ו-`PROVISA_DEMO_DIR`.
4. סביבה מוטבעת — מפעילה את מישור הבקרה של PostgreSQL, פותרת את כתובת ה-Socket שלו, ומכינה תוספי DuckDB אופליין אם `provisa-duckdb-ext` מותקן.
5. בדיקת סחיפת סכמה (schema drift) — סורקת את מישור הבקרה החי לעמודות חסרות. אם נמצאות כלשהן,
   מדפיסה רמז `--reset` ויוצאת עם קוד 1. ל-V1 אין migrations; עמודה שנוספה בגרסה
   חדשה יותר דורשת reset. [tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. שני השרתים מתחילים במקביל. מכריז המוכנות סוקר `GET /ready` (לא `/health` —
   נקודת הקצה `/ready` מאשרת שהחנות מחוברת ושהמנוע חם) ופותח את הדפדפן
   כשהיא מחזירה 200. [tool-verified: `_announce_ready()` at cli.py:182-224]

### קודי יציאה {: #exit-codes }

| קוד | משמעות |
| --- | --- |
| 0 | כיבוי נקי (Ctrl-C) |
| 1 | שגיאת הפעלה (בדיקת פלטפורמה נכשלה, הגדרות דמו חסרות, זוהתה סחיפת סכמה) |

### דוגמה {: #example }

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

אמת והתקן קובץ רישיון במצב אופליין (REQ-1139). הקובץ הוא ה-`license.json` שהונפק על ידי
provisa.dev. [tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| ארגומנט | הערות |
| --- | --- |
| `file` | נתיב לקובץ הרישיון; הרחבת `~` מוחלת |

קוד יציאה 0 משמעו שהרישיון תקף והותקן. קוד יציאה 1 משמעו שנדחה; הסיבה
מודפסת ל-stderr. [tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

הצג את מזהה המכונה, מצב הניסיון, הימים שחלפו, ותקפות הרישיון (REQ-1139).
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

ללא דגלים. מדפיס ארבע שורות — מזהה מכונה, תאריך ראשון שנצפה, ימים שחלפו, מצב ניסיון —
ומצב רישיון — ויוצא עם 0. [tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

הפעל את פרסום המטא-דאטה על-פי דרישה של השרת הרץ (REQ-1072/REQ-1074). שולח בקשת POST אל
`POST /admin/metadata-export/publish` — אותה נקודת קצה שכפתור **פרסם כעת** בכרטיסיית הניהול
קורא לה, כך ששני הנתיבים שולחים את אותו תמונת מצב (snapshot) מלאה. [tool-verified: `_cmd_metadata_export()`
at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| דגל | ברירת מחדל | הערות |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, ואז `http://127.0.0.1:8000` | תחת ריבוי-דיירים שם המארח נוקב את הארגון |
| `--token` | `$PROVISA_API_TOKEN` | אסימון Bearer לזהות המחזיקה `org_settings`; השמט בפריסות ללא אימות |
| `--timeout` | `300` | שניות לפני שקריאת ה-HTTP ננטשת |

[tool-verified: cli.py:654-669]

| קוד יציאה | משמעות |
| --- | --- |
| 0 | כל נכס פורסם |
| 1 | פרסום חלקי או כשל חיבור; שגיאות לכל נכס מודפסות ל-stderr |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

הפניה המלאה להגדרות — ספקים, אישורים, `reconcile_cron`, ומה מכיל תמונת המצב —
נמצאת ב-[ייצוא מטא-דאטה](metadata-export.md#from-the-command-line).

---

## provisa env

מריץ את מחזור החיים של סביבות מול Provisa פועלת (REQ-1498). כל תת-פקודה היא אותה קריאה
ל-API הניהול שהממשק מבצע; ה-CLI אינו מחליט דבר. הרשימה של מה שעותק נושא, כללי המיזוג
וההסרה, הקישור, ההגנה והזכויות של הקורא שייכים לשרת, ולכן הם חלים
בלי קשר לאופן שבו הופעלה הפעולה. [tool-verified: cli_envs.py:11-28]

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```
### דגלים שכל תת-פקודה מקבלת {: #flags-every-subcommand-takes }

| דגל | חובה | הערות |
| --- | --- | --- |
| `--org` | כן | הארגון שהסביבה שייכת לו |
| `--json` | לא | מדפיס את תגובת ה-API כ-JSON, לסקריפט |
| `--api` | לא | ראו [אפשרויות משותפות](#shared-options) |
| `--token` | לא | טוקן Bearer של זהות המורשית לפעולה |
| `--timeout` | לא | שניות; ברירת מחדל 300 |

תת-פקודות הפועלות על סביבה אחת מקבלות גם `--env ENV` (חובה). `list`, `diff`,
`merge` ו-`fetch` אינן; `diff` ו-`merge` נוקבות בסביבות שלהן עם `--from` ו-`--into`.
[tool-verified: cli_envs.py:349-374, `_command()`]

### קודי יציאה {: #exit-codes_1 }

| קוד | משמעות |
| --- | --- |
| 0 | בוצע, או הרצת ניסיון שלא החילה דבר |
| 1 | נדחה. הקוד של הכלל שדחה מודפס ל-stderr כ-`refused (<code>): <detail>`; עם `--json` הוא `{"refused": {...}}` ב-stdout |
| 2 | הוצע, לא הוחל: סביבת היעד מוגנת והשינוי ממתין לאישור |

קוד היציאה 2 מכוון. צינור שהיה מתייחס לאישור ממתין כאל פריסה שיצאה לדרך
היה טועה. [tool-verified: cli_envs.py:21-28, 40-41, 64-78, 120-135]

### provisa env list

מפרט את הסביבות של הארגון ואת ה-build שבו נמצאת כל אחת. אין דגלים מעבר לדגלים המשותפים.
[tool-verified: cli_envs.py:141-149]

```
provisa env list --org ORG [--json]
```
### provisa env show

מציג סביבה אחת ואת ה-build הנוכחי שלה. יוצא עם 1 ו-`environments.unknown` אם
הסביבה אינה קיימת. [tool-verified: cli_envs.py:163-183]

```
provisa env show --org ORG --env ENV [--json]
```
### provisa env create

יוצר סביבה המחזיקה את המודל של סביבה אחרת, עם הנתונים שנבחרו ביצירה (REQ-1942). ה-API
מחליט אילו בחירות הקורא רשאי לבצע. [tool-verified: cli_envs.py:197-216]

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```
| דגל | חובה | הערות |
| --- | --- | --- |
| `--env` | כן | שם הסביבה החדשה |
| `--from` | לא | הסביבה שממנה מתחילים; ברירת מחדל `prod` |
| `--data-mode` | כן | `inherit`, `unbound`, `test_fake` או `test_synthetic`: חיבורי המקור בהפניה, אף אחד עד שהיא קושרת את שלה, או נתוני בדיקה |
| `--mutation-handling` | לא | `refused`, `reversible` או `direct`; ברירת מחדל `refused` |
| `--expires-at` | לא | רגע ISO-8601 שלאחריו הסביבה מוסרת |

[tool-verified: cli_envs.py:391-416]

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```
### provisa env delete

מוציא סביבה משימוש. הענפים שלה נמחקים רק כשנוקבים בהם. [tool-verified: cli_envs.py:219-232]

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```
| דגל | הערות |
| --- | --- |
| `--delete-branch` | מוחק גם את הענף שלה |
| `--delete-remote-branch` | מוחק גם את הענף שלה במאגר המרוחק |

[tool-verified: cli_envs.py:418-424]

### provisa env diff

מדווח מה מיזוג של סביבה אחת לתוך אחרת היה עושה. זהו ה-GET של `merge-preview`, ולכן
הוא אינו יכול לכתוב; תנו אותו ל-runner שאסור לו לשנות דבר. [tool-verified:
cli_envs.py:186-191]

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```
| דגל | חובה | הערות |
| --- | --- | --- |
| `--from` | כן | הסביבה שממנה ממזגים |
| `--into` | כן | הסביבה שלתוכה ממזגים |
| `--removals` | לא | מסיר גם מהיעד את מה שהמקור כבר אינו מחזיק |
| `--select PATH` | לא | מגביל את המיזוג לאובייקט הזה לפי הנתיב שלו במודל (`metrics/revenue.yaml`); חזרו עליו לאחרים. מה שהוא צריך וחסר ביעד נלקח איתו ומפורט (REQ-1506) |

[tool-verified: cli_envs.py:426-439]

### provisa env merge

ממזג סביבה אחת לתוך אחרת, לפי זהות (REQ-1490). [tool-verified: cli_envs.py:235-250]

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```
| דגל | חובה | הערות |
| --- | --- | --- |
| `--from` | כן | הסביבה שממנה ממזגים |
| `--into` | כן | הסביבה שלתוכה ממזגים |
| `--removals` | לא | מסיר גם מהיעד את מה שהמקור כבר אינו מחזיק |
| `--select PATH` | לא | מגביל את המיזוג לאובייקט הזה לפי הנתיב שלו במודל (`metrics/revenue.yaml`); חזרו עליו לאחרים. מה שהוא צריך וחסר ביעד נלקח איתו ומפורט (REQ-1506) |
| `--dry-run` | לא | מדפיס את דוח המיזוג ואינו מחיל דבר |
| `--message` | לא | מה המיזוג עושה; נדרש כדי להחיל |
| `--retire-source` | לא | מסיר את המקור כשהעבודה שלו נחתה ביעד |
| `--retire-remote` | לא | עם `--retire-source`, מוחק גם את הענף של המקור במאגר המרוחק |

[tool-verified: cli_envs.py:429-451]

### provisa env deploy

פורס את המודל בהפניית git לתוך סביבה, והופך את העץ הזה למודל הנוכחי של הסביבה
(REQ-1496). זו הפקודה שצינור פריסה מריץ; פריסה היא תמיד
קריאה עם זהות מול מישור בקרה בשם. [tool-verified:
cli_envs.py:253-267]

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```
| דגל | חובה | הערות |
| --- | --- | --- |
| `--env` | כן | הסביבה שתחזיק את המודל הנפרס |
| `--ref` | כן | ענף או commit במאגר של הארגון |
| `--dry-run` | לא | מדווח מה הפריסה הייתה עושה; אינו מחיל דבר |
| `--message` | לא | הערה שנישאת לבקשת האישור כשהיעד מוגן |

[tool-verified: cli_envs.py:453-468]

טעינה לתוך סביבה קיימת לעולם אינה מחילה תפקידים. `--seed` כבר אינו קיים: ה-API דוחה seed
עם `environments.seed_only_at_creation`.

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```
### provisa env revert

מחזיר סביבה ל-build קודם (REQ-1543). בלי `--to` הוא נסוג commit אחד
בהיסטוריה של הסביבה עצמה. עם `--to` זו פריסה רגילה של אותו build.
[tool-verified: cli_envs.py:270-284]

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```
| דגל | חובה | הערות |
| --- | --- | --- |
| `--env` | כן | הסביבה להחזרה |
| `--to` | לא | ה-build לפריסה; ברירת מחדל הקודם לנוכחי |
| `--message` | לא | הערה שנישאת לבקשת האישור |

[tool-verified: cli_envs.py:470-480]

### provisa env redo

מתקדם שוב אל ה-build שממנו נסוגה ההחזרה. [tool-verified:
cli_envs.py:287-301]

```
provisa env redo --org ORG --env ENV
```
### provisa env fetch

מושך את הענפים המרוחקים של הארגון לתוך מאגר ה-Provisa שלו (REQ-1541). הריצו אותו לפני פריסה
כשרוצים לנקוב ב-`origin/<branch>`. Provisa לעולם אינה מריצה אותו לפי טיימר. [tool-verified:
cli_envs.py:307-320]

```
provisa env fetch --org ORG
```
מדפיס שורה לכל ענף שנמשך, `origin/<name>  <sha12>`.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```
### provisa env push

שולח את הענף של סביבה למאגר המרוחק של הארגון (REQ-1546). מדפיס `nothing to send` כשאין
מה לשלוח. [tool-verified: cli_envs.py:323-331]

```
provisa env push --org ORG --env ENV
```
### provisa env pull

מחיל את מה שהמאגר המרוחק מחזיק עבור סביבה (REQ-1547). מדפיס
`<env> already holds what the remote does` כשאין מה להחיל. קוד היציאה 2 חל כמו
ב-deploy. [tool-verified: cli_envs.py:334-343]

```
provisa env pull --org ORG --env ENV
```
להסבר המלא של מחלקות הסביבה, כללי ההגנה, דוחות המיזוג ו
מחזור החיים של האישורים, ראו [סביבות](environments.md#the-env-cli-commands).

---

## provisa maintenance on

הרם את באנר התחזוקה המתוזמנת בפריסה (REQ-1466). הרץ פקודה זו לפני עבודה
מתוכננת שמורידה את מישור הנתונים — למשל, לפני החלפת
`var.engine_cluster_mode`, שמחליפה את אשכול המנוע וכל שברד (shard) עליו (REQ-1465).
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| דגל | הערות |
| --- | --- |
| `--message` | דרוס את הניסוח הסטנדרטי של הפריסה; ברירת המחדל היא ההודעה הסטנדרטית של השרת |
| `--ends-at` | רגע ISO-8601 שבו העבודה צפויה להסתיים, למשל `2026-08-14T22:30:00Z`; ברירת המחדל היא ללא הערכה |
| `--api` | ראה [אפשרויות משותפות](#shared-options) |
| `--token` | אסימון Bearer לזהות המחזיקה `platform_settings` |
| `--timeout` | ברירת מחדל 30 שניות |

[tool-verified: cli.py:743-773]

מדפיס את מצב הבאנר המתקבל ויוצא עם 0. מעלה `SystemExit` בכשל HTTP או חיבור.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

נקה את באנר התחזוקה לאחר שהעבודה הושלמה (REQ-1466).
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

מדפיס את מצב הבאנר המתקבל (active: false) ויוצא עם 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

הצג את מצב באנר התחזוקה הנוכחי מבלי לשנות אותו (REQ-1466).
[tool-verified: `_cmd_maintenance_status()` at cli.py:504-507]

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

מדפיס את מצב הבאנר ויוצא עם 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## הפניה מהירה {: #quick-reference }

| פקודה | REQ | מה היא עושה |
| --- | --- | --- |
| `provisa run` | REQ-1128 | הפעל את ה-API + UI המוטבעים |
| `provisa run --demo` | REQ-414 | הפעל עם נתוני דוגמה pet-store / shelter |
| `provisa run --reset` | REQ-1535 | בנה מחדש את מישור הבקרה לפני ההפעלה |
| `provisa license apply <file>` | REQ-1139 | התקן קובץ רישיון במצב אופליין |
| `provisa license status` | REQ-1139 | הצג מזהה מכונה ומצב ניסיון / רישיון |
| `provisa metadata export` | REQ-1072 | פרסם את תמונת המטא-דאטה על-פי דרישה |
| `provisa env list --org ORG` | REQ-1498 | מפרט את הסביבות של הארגון ואת ה-builds שלהן |
| `provisa env show --org ORG --env ENV` | REQ-1498 | מציג סביבה אחת |
| `provisa env create --org ORG --env ENV --data-mode MODE` | REQ-1942 | יוצר סביבה מסביבה אחרת |
| `provisa env delete --org ORG --env ENV` | REQ-1498 | מוציא סביבה משימוש |
| `provisa env diff --org ORG --from A --into B` | REQ-1490 | תצוגה מקדימה של מיזוג (קריאה בלבד) |
| `provisa env merge --org ORG --from A --into B` | REQ-1490 | ממזג סביבה אחת לתוך אחרת |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | פורס הפניה לתוך סביבה |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | REQ-1543 | נסוג commit אחד, או פורס build |
| `provisa env redo --org ORG --env ENV` | REQ-1543 | מתקדם שוב |
| `provisa env fetch --org ORG` | REQ-1541 | מושך ענפים מרוחקים לתוך מאגר ה-Provisa |
| `provisa env push --org ORG --env ENV` | REQ-1546 | שולח את הענף של סביבה למאגר המרוחק |
| `provisa env pull --org ORG --env ENV` | REQ-1547 | מחיל את מה שהמאגר המרוחק מחזיק |
| `provisa maintenance on` | REQ-1466 | הרם את באנר התחזוקה |
| `provisa maintenance off` | REQ-1466 | נקה את באנר התחזוקה |
| `provisa maintenance status` | REQ-1466 | הצג את מצב הבאנר הנוכחי |

## ראה גם {: #see-also }

- [סביבות](environments.md) — מודל הסביבה, סביבות מוגנות, מחזור חיי אישור פריסה
- [ייצוא מטא-דאטה](metadata-export.md) — ספקי קטלוג, הגדרות, ומה מכילה תמונת המצב
- [פריסה](deployment.md) — שכבת קונטיינר ופריסה בענן
