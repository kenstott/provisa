# מדריך התחלה מהירה למפתחים {: #developer-quick-start }

כדי להעריך את Provisa בלי לבנות מקוד המקור, ראו את [ההתחלה המהירה](index.md): הורידו את תוכנת ההתקנה ל-macOS, Windows או Linux והריצו `provisa start`.

המדריך הזה מסביר איך להריץ את Provisa **מתוך המאגר (repository)**, כשאתם מפתחים, מדבגים או תורמים קוד.

---

## דרישות מוקדמות {: #prerequisites }

- **Docker Desktop** (פועל)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. שכפול והגדרה {: #1-clone-and-set-up }

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` יוצר את `.venv/`, מתקין את כל תלויות ה-Python דרך `pip install -e ".[dev]"`, ומגדיר git hooks לתוך `.githooks/`.

---

## 2. הפעלת הכול {: #2-start-everything }

```bash
./start-ui.sh
```

עם סיום ההפעלה תראו:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**מה זה מפעיל:**

- שירותי הליבה של Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis
- שכבת הפיתוח של Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry
- Backend API בפורט 8001 (טעינה חוזרת חמה (hot-reload) בעת שינויים ב-`provisa/` וב-`config/`)
- שרת הפיתוח של Vite עבור ה-UI בפורט 3000 (HMR)
- מעקב (tracing) של OpenTelemetry ו-Grafana בכתובת `http://localhost:3100`. מחסנית ה-Observability (OTel Collector, Prometheus, Tempo, Grafana) היא פרופיל `observability` אופציונלי של docker-compose, והיא כבויה כברירת מחדל בפלטפורמה; `start-ui.sh` מפעיל אותה לנוחות בפיתוח, אלא אם תעבירו `--no-observability`.

**Ctrl+C** עוצר הכול (Backend, UI וכל שירותי ה-Docker) ומבטל כל תיקון (patch) בקונפיגורציה.

**Ctrl+R** מפעיל מחדש רק את ה-Backend (שימושי לאחר שינוי קונפיגורציה שה-hot-reload לא תופס).

### אפשרויות {: #options }

`--no-observability` — משבית מעקב מבוזר (distributed tracing). כברירת מחדל, `start-ui.sh` מוריד את סוכן ה-Java של OpenTelemetry אם עדיין אינו קיים, מתקן (patches) את `jvm.config` של Trino כדי לטעון אותו, ומפעיל את OTel collector, Prometheus, Tempo, ו-Grafana. העבירו `--no-observability` כדי לדלג על כל זה. תיקון ה-`jvm.config` מבוטל בעת Ctrl+C.

`--seed-data` — מזריע (seeds) את Kafka בנתוני הדגמה לאחר ששירותי ה-Docker תקינים (healthy). לא רץ כברירת מחדל.

`--keep-docker` — משאיר את שירותי Docker Compose פועלים לאחר Ctrl+C במקום לקרוא ל-`docker compose down`.

`--reset-volumes` — מוחק את כל נפחי (volumes) ה-Docker ומפעיל מחדש עם מצב נקי. שימושי להתאוששות מקריסת Docker.

`--demo` — מפעיל מקורות נתוני הדגמה נוספים (סכמת חנות חיות ב-PostgreSQL, דמה OpenAPI petstore, SQLite, ומרוחק GraphQL). מזריע משתמשי petstore והזמנות באופן אוטומטי.

`--source=<name>` (`start-ui-install.sh` בלבד, ניתן לחזרה) — מקצה (provisions) מקור נתונים אופציונלי לצד `--demo`. כל שם ממופה ל-`demo/sources/<name>/`. ההפעלה קוראת ל-`demo/sources/provision.py up`, אשר מפעיל את `compose.yml` של המקור כפרויקט Docker Compose עצמאי משלו (`provisa-demo-<name>`), ממתין לבדיקת התקינות (health check) שלו, ומריץ את `prime.py` כאשר למקור יש כזה, כדי להזריע נתונים. ההפעלה כותבת לאחר מכן קובץ קונפיגורציה עוטף (wrapper) ב-`${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml` הכולל את הקונפיגורציה הבסיסית בתוספת `fragment.yaml` של כל מקור, ומאתחל ממנו.

אותו `provision.py` הוא זה שסוויטת ה-end-to-end של ה-UI קוראת לו כדי להקים מקורות אלו (תחת קידומת הפרויקט `provisa-e2e-<name>` על הפורטים שלה), כך שנתוני ההדגמה שמוצג בדמו והשורות שהסוויטה בודקת מוגדרים פעם אחת בלבד.

תחת הפעלת Docker (ללא `--demo`/`--native`) המתאם (coordinator) הוא קונטיינר, כך שכל מקור מצטרף לרשת של מחסנית הליבה ונרשם בכתובת `<name>:<container port>`; תחת הפעלה טבעית (native) הוא נרשם בכתובת `localhost:<published port>`. מקור שקובץ `demo/sources/<name>/engine` שלו נוקב מנוע שההפעלה אינה מריצה — נדחה.

מקורות מסופקים (Shipped sources):

| שם | פורט(ים) | הערות |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | שתי טבלאות Cypher (‏`adopter`, `adopter_referral`)‏; הגרף מוזרע על ידי `seed.cypher`; הטבלאות רשומות מתוך ה-fragment |
| `mongodb` | 27117 | המקור רשום; אוסף (collection) `product_reviews` מוזרע על ידי `db/mongo-init.js`; יש לרשום טבלאות ידנית דרך Register Table |
| `redis` | 26379 | המקור רשום; מפות ה-hash‏ `support_agent:*` ו-`agent_status:*` מוזרעות על ידי `prime.py`; כל קידומת נרשמת כטבלה דרך Register Table |
| `cassandra` | 29042 | המקור רשום; `shelter_ops.intake_events` מוזרע על ידי `prime.py` (דורש את התוסף (extra) `cassandra`)‏; ה-keyspace נרשם כסכמה דרך Register Table |
| `sparql` | 23030 | Apache Jena Fuseki; המקור וטבלה אחת מבוססת שאילתה (`volunteer`) רשומים מתוך ה-fragment, הגרף מוזרע על ידי `prime.py`; טבלאות נוספות דרך Register Table (שאילתה + תצוגה מקדימה) |
| `prometheus` | 29090 | המקור רשום; השרת גורף (scrapes) את עצמו, כך ש-`up` והמדדים `prometheus_*` נרשמים כטבלאות דרך Register Table |
| `elasticsearch` | 29200 | המקור ומיפוי האינדקס רשומים; אינדקס `support_tickets` מוזרע על ידי `prime.py`; נקרא דרך HTTP על ידי המנוע הטבעי, ודרך המחבר (connector) ב-Trino |
| `splunk` | mgmt 8089, HEC 8088 | המקור רשום עם אימות באמצעות טוקן (token auth) ו-`disable_ssl_validation` (תעודת הקונטיינר חתומה עצמית); אינדקס, שבעה אירועי התרעה של מקלט (shelter-alert) ומודל הנתונים (Data Model)‏ `shelter_alerts` מוזרעים על ידי `prime.py`, אשר גם מטביע (mints) את טוקן ה-API שה-fragment קורא כ-`PROVISA_DEMO_SPLUNK_TOKEN`. מודלי נתונים נרשמים כטבלאות דרך Register Table: ב-Trino דרך הקטלוג `splunk`, ובכל מנוע אחר דרך נקודת הקצה pgwire של דרייבר Splunk, שהמנוע מצרף |
| `chinook` | 25433 | Postgres המחזיק את קבוצת המשנה של Chinook ב-snake_case שה-metadata לדוגמה של Hasura עוקב אחריה, מוזרע על ידי `prime.py` מתוך `tests/fixtures/hasura_v2_t1_seed.sql`; המקור רשום מתוך ה-fragment, וזהו המקור שעליו נוחת ייבוא Hasura v2 של `tests/fixtures/hasura_v2_t1_metadata.json` |

`--idp=basic|firebase` — מפעיל ספק זהות (identity provider) לאימות. ללא דגל זה, ה-Backend פועל ללא ספק אימות וכל הבקשות מטופלות כ-`admin`.

---

## 3. חיבור מקור נתונים {: #3-connect-a-data-source }

Provisa קורא את הקונפיגורציה שלו מתוך `config/`. הוסיפו קובץ מקור, למשל `config/sources/my-db.yaml`:

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

הגדירו את משתנה הסביבה וה-Backend יאסוף אותו בטעינה החוזרת הבאה:

```bash
export MY_DB_PASSWORD=secret
```

ראו [docs/configuration.md](configuration.md) לעיון המלא ב-YAML ולכל סוגי המקורות הנתמכים.

---

## 4. הרצת השאילתה הראשונה שלכם {: #4-run-your-first-query }

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

לא נדרש אימות כאשר לא קיים סעיף `auth` בקובץ `config/provisa.yaml` (ברירת המחדל בפיתוח). התפקיד (role) ברירת המחדל הוא `admin`.

---

## 5. פתיחת ה-UI {: #5-open-the-ui }

פתחו את `http://localhost:3000` בדפדפן.

בסרגל הניווט ארבעה תפריטים ברמה עליונה:

- **Explore** — סייר סכמות (Schema Explorer) (`/schema`), עורך GraphQL (`/query`), עורך Cypher (`/graph`), עורך SQL (`/sql`)
- **Model** — תצוגות (Views) ופקודות (Commands)
- **Security** — אבטחה ברמת השורה ומדיניות מיסוך עמודות
- **Admin** — סקירה כללית, תחומים, מטמון, משימות מתוזמנות, תקינות מערכת, Observability, משתמשים, ארגונים, תפקידים

ה-API של Admin GraphQL נמצא בכתובת `http://localhost:8001/admin/graphql`.

---

## פתרון בעיות {: #troubleshooting }

**ה-Backend לא מתחיל** — בדקו את `.logs/server.log`. הגורם הנפוץ ביותר הוא משתנה סביבה חסר או התנגשות פורטים בפורט 8001.

**שירותי Docker לא תקינים** — הריצו `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` כדי לראות איזה שירות תקוע. למנוע הפדרציה לוקח כ-30 שניות בהפעלה ראשונה.

**התנגשות פורטים בפורט 3000 או 8001** — `start-ui.sh` הורג תהליכים ישנים על פורטים אלו לפני ההפעלה. אם משהו אחר מחזיק בפורט, עצרו אותו ידנית תחילה.

**התחלה נקייה** — עצרו את הסקריפט, ואז הריצו `./start-ui.sh --reset-volumes` כדי למחוק את כל הנפחים ולהפעיל מחדש.

---

## הצעדים הבאים {: #next-steps }

| מטרה | מסמך |
| ------ | ----- |
| עיון מלא בקונפיגורציית YAML | [configuration.md](configuration.md) |
| אבטחה ברמת השורה, מיסוך עמודות, אימות | [security.md](security.md) |
| כל סוגי המקורות הנתמכים | [sources.md](sources.md) |
| מנויים בזמן אמת (Real-time subscriptions) | [subscriptions.md](subscriptions.md) |
| JDBC, כלי BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| לקוח Python | [python-client.md](python-client.md) |
| פריסה לייצור (Production deployment) | [deployment.md](deployment.md) |
