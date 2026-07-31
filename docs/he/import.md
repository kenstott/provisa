# ייבוא מ-Hasura

Provisa יכולה להמיר מטא-דאטה קיים של Hasura לתוך `config.yaml` של Provisa, תוך שימור טבלאות מעוקבות (tracked), קשרים, הרשאות, וסכמות מרוחקות.

## Hasura v2

### ייצוא מטא-דאטה

מקונסולת Hasura שלכם או ה-CLI:
```bash
hasura metadata export --output metadata.yaml
```

או השתמשו ב-API של Hasura:
```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### המרה

הממיר של v2 קורא **תיקיית** מטא-דאטה של Hasura (הפריסה המופקת על ידי `hasura metadata export`, או הפריסה השטוחה `tables.yaml` / `actions.yaml`) וכותב תצורת Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

השמיטו את `-o` כדי לכתוב את התצורה ל-stdout.

דגלים:

| דגל | מטרה |
|------|---------|
| `-o`, `--output` | נתיב YAML פלט (ברירת מחדל: stdout) |
| `--source-overrides` | קובץ YAML עם דריסות חיבור לכל מקור (host, port, אישורים) |
| `--domain-map` | מיפויי סכמה-לדומיין כזוגות `SCHEMA=DOMAIN` |
| `--auth-env-file` | קובץ `.env` עם תצורת אימות; ממיר JWT/JWK, סוד admin, ו-claims map |
| `--dry-run` | פענוח ואימות ללא כתיבת פלט |

### מה מומר

| מושג Hasura | שקילות Provisa |
|---------------|-------------------|
| טבלה מעוקבת | `tables[]` עם `publish: true` |
| קשר object | `relationships[]` עם `cardinality: many-to-one` |
| קשר array | `relationships[]` עם `cardinality: one-to-many` |
| הרשאת select | נראות תפקיד + פילטר RLS |
| הרשאת עמודה | `visible_to` / `writable_by` |
| הרשאת insert/update/delete | מוטציית `writable_by` + RLS |
| סכמה מרוחקת | רישום מקור `graphql_remote` |
| שדה מחושב (computed field) | רשומת `functions[]` עם `kind: query` |

### מגבלות

- **Actions** מומרות אוטומטית: actions עם handler‏ HTTP הופכות למוטציות `webhooks[]`; actions עם handler שאינו-HTTP (מסד נתונים) הופכות ל-placeholder של `functions[]` ומנפיקות אזהרה לסקירת ה-handler
- **Event triggers** מומרים לתצורת `event_triggers` לכל-טבלה (פעולות, URL של webhook, מדיניות ניסיון-חוזר) ומנפיקים אזהרה המציינת נאמנות מוגבלת
- **סכמות מרוחקות** מומרות לרשומות מקור `graphql_remote`
- **פונקציות SQL מותאמות אישית** דורשות סקירה — מקרים פשוטים מומרים לרשומות `functions[]`, מורכבים דורשים עבודה ידנית
- **Cron triggers** מומרים לרשומות תצורת `scheduler`, תוך שימור ביטוי ה-cron ודגל ההפעלה

---

## Hasura DDN (v3)

### איתור פרויקט ה-HML

הממיר של DDN קורא ישירות את **תיקיית** פרויקט ה-DDN של קבצי `.hml` — אין צורך בשלב build‏ supergraph. רכיב התיקייה הראשון תחת שורש הפרויקט נלקח כשם ה-subgraph; קבצים תחת `globals/` מוקצים ל-subgraph‏ `globals`.

### המרה

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

השמיטו את `-o` כדי לכתוב את התצורה ל-stdout.

דגלים:

| דגל | מטרה |
|------|---------|
| `-o`, `--output` | נתיב YAML פלט (ברירת מחדל: stdout) |
| `--source-overrides` | קובץ YAML עם דריסות חיבור לכל מקור |
| `--domain-map` | מיפויי subgraph-לדומיין כזוגות `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | נתיב פלט עבור קובץ ה-sidecar של ביטויי-אגרגציה (ברירת מחדל: `<output>-aggregates.yaml`) |
| `--dry-run` | פענוח ואימות ללא כתיבת פלט |

מטא-דאטה מסוג `AggregateExpression` נשמרת בקובץ `*-aggregates.yaml` נפרד (sidecar).

### מה מומר

| מושג DDN | שקילות Provisa |
|------------|-------------------|
| מודל subgraph | `tables[]` תחת מקור |
| קשר | `relationships[]` |
| כלל הרשאה | פילטר RLS |
| Command | מוטציית webhook או תצוגה |
| Connector | רשומת מקור עם פרטי חיבור |

### מגבלות

- **מחברי Lambda** (פונקציות TypeScript/Python) דורשים הגדרת webhook ידנית
- ל-**plugins‏ lifecycle** אין שקילות ישירה
- **מצבי אימות של DDN** ממופים לספקי אימות של Provisa אך נתיבי claim של JWT עשויים לדרוש התאמה

---

## לאחר הייבוא

1. סקרו את `config.yaml` שנוצר — שימו לב ל-`warnings` מהממיר
2. אמתו אישורי חיבור (הממיר משתמש בערכי placeholder)
3. הפעילו את Provisa ואשרו שטבלאות מופיעות ב-Explorer
4. הריצו את שאילתות ה-GraphQL הקיימות שלכם — הסכמה תואמת עבור דפוסים נפוצים
5. הגישו שאילתות לאישור דרך ה-Admin API או ה-UI לפני הפעלת ממשל ייצור
