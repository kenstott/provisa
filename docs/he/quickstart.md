# התחלה מהירה למפתחים

להערכת Provisa בלי בנייה מהמקור, ראו את [Quick Start](index.md) — הורידו את המתקין ל-macOS, Windows או Linux והריצו `provisa start`. (REQ-223, REQ-224, REQ-227)

מדריך זה מיועד להרצת Provisa **מהמאגר (repository)** — פיתוח פעיל, ניפוי שגיאות, או תרומה.

---

## דרישות מוקדמות

- **Docker Desktop** (פועל)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. שכפול (Clone) והגדרה

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` יוצר `.venv/`, מתקין את כל תלויות ה-Python דרך `pip install -e ".[dev]"`, ומגדיר git hooks לתוך `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. הפעלת הכול

```bash
./start-ui.sh
```

כשההפעלה מסתיימת תראו:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**מה זה מפעיל:** [tool-verified: start-ui.sh]

- שירותי הליבה של Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- שכבת הפיתוח של Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- Backend API על פורט 8001 (hot-reload בשינויים ל-`provisa/` ול-`config/`) (REQ-618)
- שרת פיתוח Vite UI על פורט 3000 (HMR)
- מעקב OpenTelemetry ו-Grafana בכתובת `http://localhost:3100`. מחסנית ה-observability היא פרופיל `observability` של docker-compose שנרשמים אליו מרצון (opt-in) (OTel Collector, Prometheus, Tempo, Grafana), לא פעיל כברירת מחדל ברמת הפלטפורמה; `start-ui.sh` מפעיל אותו כנוחות סקריפט-פיתוח אלא אם מעבירים `--no-observability`. (REQ-302, REQ-303, REQ-330)

**Ctrl+C** עוצר את הכול — backend, UI, וכל שירותי Docker — ומשחזר כל תיקוני תצורה (config patches). (REQ-619)

**Ctrl+R** מפעיל מחדש רק את ה-backend (שימושי לאחר שינוי תצורה שה-hot-reload מפספס). (REQ-619)

### אפשרויות

`--no-observability` — משבית מעקב מבוזר (distributed tracing). כברירת מחדל, `start-ui.sh` מוריד את סוכן ה-Java של OpenTelemetry אם עדיין אינו קיים, מתקן את `jvm.config` של Trino כדי לטעון אותו, ומפעיל את ה-OTel collector, Prometheus, Tempo ו-Grafana. העבירו `--no-observability` כדי לדלג על כל זה. תיקון ה-`jvm.config` משוחזר ב-Ctrl+C. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — מזין את Kafka בנתוני הדגמה לאחר ששירותי Docker בריאים. לא רץ כברירת מחדל. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — משאיר את שירותי Docker Compose פועלים לאחר Ctrl+C במקום לקרוא ל-`docker compose down`. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — מוחק את כל volumes של Docker ומפעיל מחדש עם מצב נקי. שימושי להתאוששות מקריסת Docker. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — מפעיל מקורות נתוני הדגמה נוספים (סכמת pet-store של PostgreSQL, מוק OpenAPI petstore, SQLite, ו-GraphQL מרוחק). מזין משתמשי והזמנות petstore אוטומטית. [tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` — מפעיל ספק זהות (identity provider) לאימות. בלי דגל זה, ה-backend רץ ללא ספק אימות וכל הבקשות מטופלות כ-`admin`. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. חיבור מקור נתונים

Provisa קוראת תצורה מתוך `config/`. הוסיפו קובץ מקור — לדוגמה `config/sources/my-db.yaml`:

```yaml
sources:
  - id: my-pg
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    password: ${MY_DB_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id
```

הגדירו את משתנה הסביבה וה-backend יאסוף אותו ברענון הבא:

```bash
export MY_DB_PASSWORD=secret
```

ראו [docs/configuration.md](configuration.md) לרפרנס YAML המלא ולכל סוגי המקורות הנתמכים.

---

## 4. הרצת השאילתה הראשונה שלכם

```bash
# GraphQL
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}' | jq

# SQL — use the /data/sql endpoint
curl -s -X POST http://localhost:8001/data/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

אין צורך באימות כאשר אין סעיף `auth` בקובץ `config/provisa.yaml` (ברירת המחדל בפיתוח). התפקיד ברירת המחדל הוא `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. פתיחת הממשק (UI)

פתחו את `http://localhost:3000` בדפדפן.

לסרגל הניווט ארבעה תפריטים ברמה עליונה: [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — סייר סכמה (`/schema`), עורך GraphQL (`/query`), עורך Cypher (`/graph`), עורך SQL (`/sql`)
- **Model** — Views ו-Commands
- **Security** — מדיניות אבטחה ברמת השורה ומיסוך עמודות (REQ-038, REQ-041)
- **Admin** — סקירה כללית, דומיינים, מטמון, משימות מתוזמנות, בריאות מערכת, observability, משתמשים, ארגונים, תפקידים

ה-API הניהולי של GraphQL נמצא בכתובת `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## פתרון בעיות

**ה-backend לא עולה** — בדקו את `.logs/server.log`. הסיבה הנפוצה ביותר היא משתנה סביבה חסר או התנגשות פורטים בפורט 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**שירותי Docker אינם בריאים** — הריצו `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` כדי לראות איזה שירות תקוע. מנוע הפדרציה לוקח כ-30 שניות בהפעלה ראשונה. (REQ-055)

**התנגשות פורטים על 3000 או 8001** — `start-ui.sh` הורג תהליכים ישנים על פורטים אלה לפני ההפעלה. אם משהו אחר מחזיק בפורט, עצרו אותו ידנית קודם. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**התחלה נקייה** — עצרו את הסקריפט, ואז הריצו `./start-ui.sh --reset-volumes` כדי למחוק את כל ה-volumes ולהפעיל מחדש. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## הצעדים הבאים

| מטרה | מסמך |
| ------ | ----- |
| רפרנס תצורת YAML מלא | [configuration.md](configuration.md) |
| אבטחה ברמת השורה, מיסוך עמודות, אימות | [security.md](security.md) |
| כל סוגי המקורות הנתמכים | [sources.md](sources.md) |
| מנויים בזמן אמת | [subscriptions.md](subscriptions.md) |
| JDBC, כלי BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| לקוח Python | [python-client.md](python-client.md) |
| פריסת ייצור | [deployment.md](deployment.md) |
