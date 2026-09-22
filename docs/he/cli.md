# מדריך CLI

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

תת-פקודות המקבלות דגלים אלה: `metadata export`, `env deploy`, `env fetch`,
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

### דגלים

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

### משתני סביבה

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
| `PROVISA_CONFIG_REPLACE` | נקבע ל-`true` על ידי `--demo` כדי לאפשר להגדרות הדמו לדרוס הגדרות קיימות |
| `PROVISA_DUCKDB_EXT_DIR` | תיקיית תוסף DuckDB מוכנה מראש; נקבעת אוטומטית מהחבילה `provisa-duckdb-ext` אם קיימת; היעדרה משמעו ש-DuckDB מוריד מהרשת בשימוש הראשון |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### רצף ההפעלה

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

### קודי יציאה

| קוד | משמעות |
| --- | --- |
| 0 | כיבוי נקי (Ctrl-C) |
| 1 | שגיאת הפעלה (בדיקת פלטפורמה נכשלה, הגדרות דמו חסרות, זוהתה סחיפת סכמה) |

### דוגמה

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

## provisa env deploy

פרוס את המודל בהפניית git (ref) לתוך סביבה, והפוך את העץ הזה למודל הנוכחי של הסביבה
(REQ-1496). זו הפקודה שצינור פריסה מריץ; הכלל הוא שפריסה היא
תמיד קריאה הנושאת זהות כנגד מישור בקרה בעל שם. [tool-verified:
`_cmd_env_deploy()` at cli.py:395-417]

```
provisa env deploy --org ORG --env ENV --ref REF
                   [--dry-run] [--seed] [--message MSG]
                   [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| דגל | חובה | הערות |
| --- | --- | --- |
| `--org` | כן | הארגון המחזיק את הסביבה |
| `--env` | כן | הסביבה שתחזיק את המודל הפרוס |
| `--ref` | כן | ענף או SHA של קומיט במאגר הארגון |
| `--dry-run` | לא | דווח מה ישתנה; אל תיישם דבר |
| `--seed` | לא | החל גם מחלקות יצירה-בלבד (תפקידים); נכון רק כאשר פריסה זו יוצרת את הסביבה בפעם הראשונה |
| `--message` | לא | הערה הנישאת על בקשת אישור כאשר סביבת היעד מוגנת |
| `--api` | לא | ראה [אפשרויות משותפות](#shared-options) |
| `--token` | לא | ראה [אפשרויות משותפות](#shared-options) |
| `--timeout` | לא | ברירת מחדל 300 שניות |

[tool-verified: cli.py:677-711]

| קוד יציאה | משמעות |
| --- | --- |
| 0 | הפריסה יושמה, או ש-`--dry-run` הושלם |
| 2 | הסביבה מוגנת; הפריסה רק הוצעה, לא יושמה |

קוד יציאה 2 הוא מכוון. צינור שהיה מתייחס לאישור ממתין כפריסה משוחררת היה
שגוי. [tool-verified: cli.py:416-417]

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

להסבר המלא על מחלקות סביבה, כללי הגנה, דוחות מיזוג, ומחזור חיי
האישור, ראה [סביבות](environments.md#the-env-cli-commands).

---

## provisa env fetch

משוך את הענפים המרוחקים של הארגון לתוך מאגר ה-Provisa שלו (REQ-1541). הרץ פקודה זו לפני
פריסה כשברצונך לנקוב `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at
cli.py:420-432]

```
provisa env fetch --org ORG [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| דגל | חובה | הערות |
| --- | --- | --- |
| `--org` | כן | הארגון שהמרוחק שלו נמשך |
| `--api` | לא | ראה [אפשרויות משותפות](#shared-options) |
| `--token` | לא | אסימון Bearer למנהל ארגון |
| `--timeout` | לא | ברירת מחדל 300 שניות |

[tool-verified: cli.py:716-733]

מדפיס שורה אחת לכל ענף שנמשך — `origin/<name>  <sha12>`. יוצא עם 0 בהצלחה; מעלה
`SystemExit` עם הודעת שגיאה בכשל HTTP או חיבור.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

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

## הפניה מהירה

| פקודה | REQ | מה היא עושה |
| --- | --- | --- |
| `provisa run` | REQ-1128 | הפעל את ה-API + UI המוטבעים |
| `provisa run --demo` | REQ-414 | הפעל עם נתוני דוגמה pet-store / shelter |
| `provisa run --reset` | REQ-1535 | בנה מחדש את מישור הבקרה לפני ההפעלה |
| `provisa license apply <file>` | REQ-1139 | התקן קובץ רישיון במצב אופליין |
| `provisa license status` | REQ-1139 | הצג מזהה מכונה ומצב ניסיון / רישיון |
| `provisa metadata export` | REQ-1072 | פרסם את תמונת המטא-דאטה על-פי דרישה |
| `provisa env fetch --org ORG` | REQ-1541 | משוך ענפים מרוחקים לתוך מאגר ה-Provisa |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | פרוס הפניה לתוך סביבה |
| `provisa maintenance on` | REQ-1466 | הרם את באנר התחזוקה |
| `provisa maintenance off` | REQ-1466 | נקה את באנר התחזוקה |
| `provisa maintenance status` | REQ-1466 | הצג את מצב הבאנר הנוכחי |

## ראה גם

- [סביבות](environments.md) — מודל הסביבה, סביבות מוגנות, מחזור חיי אישור פריסה
- [ייצוא מטא-דאטה](metadata-export.md) — ספקי קטלוג, הגדרות, ומה מכילה תמונת המצב
- [פריסה](deployment.md) — שכבת קונטיינר ופריסה בענן
