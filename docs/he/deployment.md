# פריסה (Deployment)

## בחירת נתיב פריסה

Provisa תומכת בשישה נתיבי פריסה. בחרו בהתבסס על הקהל וההקשר התפעולי שלכם:

| נתיב | Artifact / סקריפט | הכי מתאים ל |
| ------ | ------------------- | ---------- |
| **פיתוח (Development)** | `start-ui.sh` | פיתוח מקוד-מקור, הערכה עם נתוני הדגמה מלאים |
| **מתקין macOS** | `Provisa-<version>-macOS.dmg` | תחנות עבודה למפתחים, הערכה |
| **מתקין Windows** | `Provisa-<version>-windows-x64.exe` | תחנות עבודה למפתחים, הערכה |
| **AppImage ל-Linux** | `Provisa.AppImage` | שרתי on-prem, VM-ים בענן, סביבות air-gapped |
| **VM-ים בענן (AWS)** | `terraform/deploy.sh` | פריסה רב-node בענן עם load balancer-ים |
| **Kubernetes** | `helm/provisa/` | צוותים המפעילים כבר K8s |

### VM לעומת Kubernetes

שניהם ברמה ארגונית (enterprise-grade). נתיב ה-VM/AppImage פשוט יותר: אין אשכול לספק, אין מדיניות CNI או RBAC להגדיר, וה-AppImage הוא לגמרי self-contained (REQ-223). הוא משתלב באופן טבעי בכלי ניהול שרתים קיימים (Ansible, Puppet, סוכני Datadog, מעבירי Splunk, וכו').

בחרו Kubernetes רק אם הצוות שלכם כבר מפעיל אשכול K8s ורוצה ש-Provisa ישתתף באותו מודל תפעולי (rolling deploys, HPA, observability מאוחד) (REQ-056). היכולות שוות-ערך — Kubernetes מוסיף overhead תפעולי, לא יכולת.

### רכישת image ו-סריקת אבטחה

כל נתיבי הייצור דורשים השגת ה-artifacts של Provisa לפני שכל פריסה יכולה לרוץ. "Air-gapped" מתייחס למה שקורה בזמן ההתקנה על המכונה היעד — ה-artifacts חייבים להיות מושגים תחילה.

**מתקיני macOS ו-Windows:** הורידו מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases). ארוז לחלוטין (fully bundled); אינטרנט אינו נדרש לאחר ההורדה (REQ-227). מיועד ל-dev/הערכה, לא ייצור — אין ציפייה לשער סריקת image.

**נתיב AppImage:** הורידו מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases) והעבירו למכונה היעד. ה-AppImage אורז את כל image-י הרכיבים כ-tarballs בתוך מערכת קבצים squashfs (REQ-294) — רוב סורקי הרישום אינם יכולים לבדוק אלה במקומם. פנו לצוות החשבון של Provisa לקבלת digests של image-י רכיבים כדי לאמת מול הסורק שלכם באופן עצמאי.

**נתיב Terraform:** ה-AppImage חייב להיות מועלה ל-S3 לפני הרצת `terraform/deploy.sh`. nodes‏ EC2 מורידים אותו בעת האתחול (boot) דרך תפקיד IAM — הם דורשים גישת S3 יוצאת (ישירה או דרך endpoint‏ gateway של VPC). החילו את אותה מדיניות סריקה כמו נתיב ה-AppImage.

**נתיב Helm / Kubernetes:** image-ים בודדים חייבים להידחף לרישום (registry) שהאשכול יכול להגיע אליו. נתיב זה תואם ביותר לסריקה מבוססת-רישום (Prisma Cloud, Aqua, Trivy, AWS Inspector) — image-ים הם אובייקטים ממדרגה-ראשונה שסורקים מבינים באופן ילידי. עבור אשכולות air-gapped, שקפו (mirror) image-ים לרישום פנימי ודרסו הפניות ב-`values.yaml` (REQ-294).

---

## פיתוח (מקוד-מקור)

### מומלץ: `start-ui.sh`

הדרך הקלה ביותר להריץ את Provisa מקוד-מקור. מתחיל את כל התשתית, ה-API של הbackend, ושרת הפיתוח של ה-UI בפקודה אחת (REQ-055). Ctrl+C מכבה הכל בצורה נקייה.

**דרישות מוקדמות:** Docker Desktop, Node.js, virtualenv‏ Python ב-`.venv/`

```bash
./start-ui.sh
```

מה הוא עושה:

- מתחיל את `docker-compose.core.yml` + `docker-compose.dev.yml` (כל שירותי הליבה + ההדגמה) וממתין ל-healthy (REQ-055)
- מזריע (seed) את Kafka עם נתוני הדגמה
- מסנכרן תלויות Python מ-`.venv/`
- מתחיל את ה-API של הbackend על פורט 8001 (לוגים ל-`.logs/server.log`) (REQ-558)
- מתחיל את שרת פיתוח ה-UI של Vite על פורט 3000 (REQ-559)
- מדפיס URL-ים וממתין; Ctrl+C עוצר הכל ומפרק (tears down) את compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**אפשרויות:**

`--reset-volumes` — מריץ `docker compose down -v` לפני ההתחלה, הורס את כל volume-י ה-Docker (נתוני PostgreSQL, אובייקטי MinIO, מצב Redis, וכו') (REQ-170). השתמשו כשאתם רוצים slate נקי לחלוטין — אחרי שינוי סכמה בזמן פיתוח, או כאשר Docker קרס והשאיר volumes פגומים. **כל הנתונים יאבדו.**

`--observability` — מוסיף instrumentation מלא של tracing ומטריקות. מוריד את סוכן ה-Java של OpenTelemetry ומתקן את `jvm.config` של Trino לטעון אותו, מציין (instruments) את ה-backend של Provisa עם ייצוא OTLP, ומתחיל את ה-collector של OTel, Prometheus, Tempo, ו-Grafana (`http://localhost:3100`) (REQ-330). התיקון ל-`jvm.config` מוחזר אוטומטית ב-Ctrl+C.

### שלבים ידניים (backend בלבד, ללא UI)

אם אתם צריכים רק את ה-API:

1. התקינו את [Docker Desktop](https://docs.docker.com/get-docker/)
2. התחילו את שירותי הליבה:

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. התחילו את ה-API:

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. אמתו: `curl http://localhost:8001/health`

### מחסנית מלאה (Provisa בקונטיינר)

כדי להריץ את ה-API כקונטיינר במקום על ה-host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### שירותים

**ליבה (`docker-compose.core.yml`) — נדרש תמיד:**

| שירות | פורט | מטרה |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | מטא-דאטת תצורה + קטלוג Iceberg (REQ-169) |
| PgBouncer | 6432 | pooling חיבורים (REQ-053) |
| מנוע פדרציה | 8080 | פדרציית שאילתות (REQ-028) |
| Redis | 6379 | מטמון תוצאות שאילתה (REQ-371) |
| MinIO | 9000/9001 | אחסון אובייקטים תואם-S3 (REQ-029, REQ-171) |

**הדגמה (`docker-compose.dev.yml`) — אופציונלי, כלול על ידי `start-ui.sh`:**

| שירות | פורט | מטרה |
| --------- | ------ | --------- |
| MongoDB | 27017 | מקור NoSQL להדגמה |
| Kafka | 9092 | מקור streaming להדגמה |
| Schema Registry | 8081 | ניהול סכמת Avro/Protobuf להדגמה |
| Debezium | — | מחבר CDC להדגמה |
| Elasticsearch | 9200 | מקור חיפוש להדגמה |
| Neo4j | 7474/7687 | מקור גרף להדגמה |
| Fuseki | 3030 | triplestore‏ SPARQL להדגמה |
| OpenTelemetry Collector | — | איסוף trace (עם `--observability`) (REQ-302) |
| Prometheus | 9090 | מטריקות (עם `--observability`) (REQ-330) |
| Tempo | — | אחסון trace (עם `--observability`) (REQ-330) |
| Grafana | 3100 | dashboards (עם `--observability`) (REQ-330) |

### Backend טלמטריה (`otlp2sql`)

מחסנית ה-`--observability` שלמעלה (Collector ← Tempo/Prometheus/Grafana) היא נתיב
טלמטריה אחד. השני הוא `otlp2sql` (`provisa.observability.otlp2sql`): מקלט
OTLP/HTTP הכותב traces, מטריקות, ולוגים למסד נתונים SQL
הנבחר על ידי URL של SQLAlchemy, ומחלץ את מאפייני ה-span‏ `provisa.*` בעת הקליטה
כך שאין ריצת job‏ compaction נפרדת. כתיבות נצברות ל-batch
(‏`OTLP2SQL_BATCH_MAX_ROWS`, ברירת מחדל 1000; `OTLP2SQL_BATCH_MAX_SECS`, ברירת מחדל 2 שניות).

לטלמטריה יש מאגר משלה, נפרד ממסד הנתונים של control-plane. בחרו
את ה-backend עם `PROVISA_OPS_DB_URL`:

| `PROVISA_OPS_DB_URL` | Backend | הערות |
| --- | --- | --- |
| *(לא מוגדר)* | DuckDB ייעודי תחת `~/.provisa/telemetry/` | ברירת מחדל; ללא שרת, ללא Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | קליטה בקצב-גבוה עם מיזוגי רקע אוטומטיים |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | נפח בינוני |
| `trino://user@host:8080/otel` | Trino / Iceberg | עובד טכנית, **לא מומלץ** — ראו למטה |

**לגבי `trino://`:** דיאלקט ה-Trino של SQLAlchemy מנפיק DDL תקין של Trino ו-
`INSERT`-ים, כך שהוא ריאלי טכנית כ-backend של `otlp2sql`. הוא אינו
מומלץ עבור דבר מלבד קצבי קליטה נמוכים. כל flush‏ batch הופך ל-
`INSERT` מבוזר של Trino בתוספת snapshot של Iceberg, כך שטלמטריה בקצב-גבוה
מייצרת קבצי ו-snapshots קטנים רבים ועדיין דורשת מעת לעת
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` — ש-`otlp2sql` אינו
מריץ. זה גם מציב את מנוע השאילתה בנתיב הקליטה החם.

עבור טלמטריה בנפח-גבוה ל-Trino/Iceberg, השתמשו במקום זאת ב-`otlp2parquet`: הוא
כותב parquet לאחסון אובייקטים ללא מעבר דרך Trino, ו-compaction מתוזמן של
Trino מגלגל את הקבצים הגולמיים לתוך טבלאות Iceberg החיות. עבור
מנוע יחיד המטפל הן בקליטה בקצב-גבוה והן ב-compaction, העדיפו ClickHouse.

הצביעו את מייצאי ה-OTLP של האפליקציה ו-Trino (‏`OTEL_EXPORTER_OTLP_ENDPOINT`) על
נקודת הקצה של `otlp2sql`, ורשמו את דומיין ה-ops מול אותו
`PROVISA_OPS_DB_URL` כך שהוא קורא את מה שהמקלט כתב.

---

## מתקין macOS

עבור תחנות עבודה למפתחים והערכה. air-gapped לחלוטין — אינטרנט אינו נדרש לאחר ההורדה (REQ-227).

מתקין הבסיס הוא **התקנה ילידית**: מנוע פדרציה DuckDB + control plane‏ SQLite + מטמון בזיכרון (fakeredis), ללא Docker, VM, Trino, Redis, או MinIO (REQ-972, REQ-979). מנוע הפדרציה הוא בחירת-אשף — DuckDB (ילידי, ברירת מחדל), Trino-על-Docker, או מנוע חיצוני (REQ-973). Observability הוא תמיד-פעיל self-telemetry הניתן לצפייה ב-Admin; מחסנית ה-collector/Prometheus/Grafana של Docker היא הדגמה חיצונית אופציונלית, לא מתג הפעלה/כיבוי (REQ-975). חבילת נתוני ההדגמה אופציונלית וכבויה כברירת מחדל (REQ-978). Trino, מחסנית ה-observability של Docker, וההדגמה הם תוספים כבדים הנפתרים local-first (תיקייה סמוכה-למתקין, volumes מותקנים, `~/Downloads`, ואז release ב-GitHub), כך שארגונים יכולים לטעון מראש tarballs עבור התקנות air-gapped (REQ-977).

### שלבים

1. הורידו את `Provisa-<version>-macOS.dmg` מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases)
2. פתחו את ה-DMG וגררו את **Provisa.app** ל-`/Applications`
3. לחצו פעמיים על **Provisa.app** — הגדרת הפעלה-ראשונה רצה פעם אחת; האשף מציע את בחירות המנוע, observability, וההדגמה שלעיל (REQ-1007)
4. פתחו Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### התמדת נתונים (Data persistence)

כל הנתונים מאוחסנים ב-`~/.provisa/` (REQ-224). כדי להסיר הכל: `provisa uninstall`.

---

## מתקין Windows

עבור תחנות עבודה למפתחים והערכה. air-gapped לחלוטין — אינטרנט אינו נדרש לאחר ההורדה (REQ-227).

כמו macOS, מתקין ה-Windows הבסיסי הוא **שכבה ילידית**: runtime‏ Python עצמאי + wheel‏ provisa + DuckDB/pg_duckdb + control plane‏ SQLite, ללא משלוח Docker, VM, או image-י קונטיינר (REQ-979). מנוע הפדרציה (Trino), מחסנית ה-observability, וחבילת נתוני ההדגמה נוספים מאוחר יותר דרך מתקינים שכבתיים נפרדים, לפי סדר: מתקין ה-Container (‏`Provisa-Container-<version>.exe`, המוסיף WSL2 + containerd + Trino), אז מתקין ה-Obs (דורש את שכבת ה-container), ואז מתקין ה-Demo (דורש Core + Obs). הנחיית הפעלה-ראשונה מסבירה כיצד לאתחל את מנוע הפדרציה על ידי הרצת מתקין ה-Container (REQ-1005).

### שלבים

1. הורידו את `Provisa-<version>-windows-x64.exe` מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases)
2. הריצו את המתקין — אין צורך בהרשאות admin; מתקין ל-`%LOCALAPPDATA%\Programs\Provisa\`
3. פתחו את **Provisa First Launch** מתפריט ה-Start — הגדרה ילידית רצה פעם אחת ומדפיסה את הנחיית הצעדים-הבאים עבור התוספים השכבתיים (REQ-1005)
4. פתחו terminal חדש:

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### התמדת נתונים

כל הנתונים מאוחסנים ב-`%USERPROFILE%\.provisa\`.

---

## Linux AppImage — VM יחיד או רב-Node

### מה זה

`Provisa.AppImage` הוא קובץ הרצה יחיד self-contained הארוז (REQ-223, REQ-228):

- דמון Docker חסר-שורש (`dockerd-rootless.sh` + `rootlesskit`) — אין צורך ב-Docker מערכתי או root
- כל tarballs‏ image הקונטיינר (PostgreSQL, PgBouncer, MinIO, Redis, מנוע פדרציה, API של Provisa) (REQ-294)
- ה-wrapper של Provisa CLI וסקריפט הגדרת הפעלה-ראשונה

ה-image של Provisa נבנה מראש בזמן האריזה — קוד מקור Python לעולם אינו נכלל.

### מתי להשתמש

- On-premises bare metal או VM (node יחיד או רב-node)
- VM-ים בענן ללא אשכול K8s
- סביבות Air-gapped (REQ-294)
- כאשר אתם רוצים תפעול פשוט יותר מ-Kubernetes

---

### שלבים — Node יחיד

1. הורידו את `Provisa.AppImage` מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases) והעבירו למכונה היעד
2. הפכו אותו להרצה:

   ```bash
   chmod +x Provisa.AppImage
   ```

3. הריצו הגדרת הפעלה-ראשונה:

   ```bash
   ./Provisa.AppImage
   ```

4. אשף ההגדרה שואל:
   - **תפקיד (Role)** ← בחרו `primary`
   - **תקציב RAM** ← כמות RAM להקצות (0 = כל הזמין); קובע את מספר עובדי Trino
   - **Hostname** ← הכתובת המפורסמת של node זה
   - **פורט API** ← ברירת מחדל `8000` (REQ-560)
5. ההגדרה טוענת את כל image-י הקונטיינר (~2–5 דקות), כותבת תצורה, ומתחילה שירותים
6. אמתו:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### שלבים — רב-Node (Primary)

הריצו שלבים אלה על ה-node ה-primary תחילה. secondaries חייבים להיות מוגדרים לאחר שה-primary רץ.

1. הורידו והעבירו את `Provisa.AppImage` למכונת ה-primary
2. פתחו פורטי firewall נדרשים (secondaries יתחברו inbound על אלה):

   | פורט | שירות |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | קואורדינטור מנוע פדרציה |
   | 8000 | API של Provisa |

3. הפכו להרצה והריצו:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. אשף ההגדרה שואל:
   - **תפקיד** ← בחרו `primary`
   - **תקציב RAM**, **hostname**, **פורט API** ← ענו כמו עבור node יחיד
5. לאחר השלמת ההגדרה, רשמו את ה-**IP הפרטי** של מכונה זו — secondaries צריכים אותו
6. האשף מדפיס בלוק upstream‏ nginx — שמרו אותו עבור תצורת ה-load balancer שלכם
7. אמתו:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### שלבים — רב-Node (כל Secondary)

חזרו על שלבים אלה על כל node נוסף לאחר שה-primary רץ ונגיש.

1. הורידו והעבירו את `Provisa.AppImage` למכונת ה-secondary
2. אשרו שה-secondary יכול להגיע ל-primary:

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. הפכו להרצה והריצו:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. אשף ההגדרה שואל:
   - **תפקיד** ← בחרו `secondary`
   - **IP‏ Primary** ← הזינו את ה-IP של ה-node‏ primary (הקישוריות מאומתת בזמן אמת)
   - **תקציב RAM**, **hostname**, **פורט API** ← ענו כמו לעיל
5. ההגדרה טוענת קבוצת image מצומצמת (ללא PostgreSQL, PgBouncer, MinIO, Redis — אלה רצים רק על primary) (REQ-561), מתחילה את ה-API של Provisa ועובד מנוע פדרציה
6. אמתו:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. הוסיפו node זה ל-upstream‏ ה-load balancer שלכם

---

### טופולוגיית Primary / Secondary

**Node ה-primary** מריץ את כל שירותי ה-singleton:

| שירות | למה singleton |
| --------- | --------------- |
| PostgreSQL | סכמה משותפת, תצורת אפליקציה, מודל סמנטי |
| Redis | מטמון תוצאות שאילתה משותף ומצב מנוי (REQ-371) |
| MinIO | מאגר אובייקטים משותף עבור תוצאות הפניה ו-snapshots של MV (REQ-029) |
| קואורדינטור מנוע פדרציה | כל העובדים (primary + secondaries) נרשמים כאן (REQ-028) |

**Node-ים secondary** מריצים רק:

- API של Provisa — חסר-מצב (stateless); קורא את כל התצורה מ-PostgreSQL על ה-primary בעת ההפעלה (REQ-057, REQ-562)
- עובד מנוע פדרציה — נרשם-עצמית עם הקואורדינטור על ה-primary (REQ-028)

כל מצב האפליקציה זורם דרך ה-PostgreSQL של ה-primary. אין צורך בסנכרון ידני. (REQ-562)

---

### הפעלה-ראשונה לא-אינטראקטיבית (אוטומטית)

עבור Terraform, cloud-init, או Ansible — העבירו דגלים במקום לענות על prompts:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

מצב לא-אינטראקטיבי מתקין יחידת systemd (‏`/etc/systemd/system/provisa.service`) להתחלה-בהפעלה (start-on-boot). (REQ-563)

| דגל | תיאור |
| ------ | ------------- |
| `--non-interactive` | דלג על כל ה-prompts; התקן יחידת systemd |
| `--role primary\|secondary` | תפקיד ה-node |
| `--primary-ip <ip>` | IP‏ node ה-primary (נדרש עבור secondary) |
| `--ram-gb <n>` | RAM להקצות (0 = הכל זמין) |

---

## פריסת VM בענן — Terraform (AWS)

מספק אשכול Provisa רב-node מלא על AWS — VPC, security groups, מופעי EC2, ALB, NLB — בפקודה אינטראקטיבית אחת. (REQ-564)

### קבצים

| קובץ | מטרה |
| ------ | --------- |
| `terraform/deploy.sh` | wrapper אינטראקטיבי — אוסף פרמטרים, מאמת אישורים, כותב `terraform.tfvars`, מריץ apply |
| `terraform/aws/variables.tf` | כל הגדרות המשתנים עם ברירות מחדל |
| `terraform/aws/main.tf` | VPC, subnets, security groups, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL-י נקודת קצה ו-IP-י node |

### שלבים

1. הורידו את `Provisa.AppImage` מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases)

2. העלו אותו ל-bucket‏ S3 בחשבון ה-AWS שלכם:

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. ודאו שאישורי AWS זמינים ב-shell שלכם (כל אחד מאלה):
   - משתני סביבה: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - פרופיל בעל-שם: `export AWS_PROFILE=my-profile`
   - session‏ SSO פעיל: `aws sso login`

4. (אופציונלי) אם אתם רוצים גישת SSH ל-nodes, צרו זוג מפתחות EC2 באזור היעד שלכם ורשמו את שם זוג המפתחות

5. הריצו את ה-wrapper של הפריסה:

   ```bash
   bash terraform/deploy.sh
   ```

6. ענו על שאלות האשף (ראו טבלת רפרנס למטה). הסקריפט מאמת שה-AppImage קיים ב-S3 לפני שממשיך, ומבטל אם לא

7. סקרו את סיכום הפריסה ואשרו

8. Terraform מספק את כל התשתית (~5–10 דקות). לאחר apply, הסקריפט מדפיס:

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (אופציונלי) הצביעו רשומות DNS על שמות ה-DNS של ה-ALB וה-NLB

10. אמתו:

    ```bash
    curl http://<api_endpoint>/health
    ```

### שאלות האשף

| שאלה | ברירת מחדל | הערות |
| ---------- | --------- | ------- |
| ספק ענן | — | AWS בלבד כיום |
| אישורי AWS | — | בודק session פעיל תחילה |
| אזור | `us-east-1` | |
| מספר nodes | `2` | 1 = primary בלבד, ללא LB; 2+ = primary + secondaries + ALB/NLB |
| סוג מופע | `m7i.2xlarge` | ראו מדריך גודל למטה |
| גודל volume ראשי | `100 GB` | לכל node |
| תקציב RAM | `0` (כל ה-RAM) | קובע את מספר עובדי Trino לכל node |
| bucket‏ S3 | — | מאומת בזמן אמת לפני שממשיך |
| מפתח S3 | `releases/Provisa.AppImage` | |
| גישת SSH | לא | דורש שם זוג-מפתחות קיים + CIDR admin |
| CIDR‏ VPC | `10.0.0.0/16` | |

### מדריך גודל מופע

| סוג | vCPU | RAM | עובדי Trino/node | מקרה שימוש |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | פיתוח / סטי נתונים קטנים |
| `m7i.2xlarge` | 8 | 32 GB | 1 | ייצור קטן |
| `m7i.4xlarge` | 16 | 64 GB | 2 | ייצור בינוני |
| `m7i.8xlarge` | 32 | 128 GB | 4 | ייצור גדול |

כל ה-nodes תורמים עובדים לקואורדינטור אחד על ה-primary (REQ-028). אשכול `m7i.4xlarge` בן 3 nodes מניב 6 עובדי Trino בסך הכל.

### מה מסופק

- VPC עם שני subnets ציבוריים על פני שני אזורי זמינות (REQ-564)
- Security groups: קבוצת LB (‏ingress ציבורי על 8000/8815), קבוצת nodes (LB ← nodes, תוך-אשכולי, SSH אופציונלי)
- תפקיד IAM + פרופיל מופע עם S3‏ GetObject על ה-bucket של ה-AppImage
- מופע EC2 ראשי (primary) — מריץ הפעלה-ראשונה במצב `--non-interactive --role primary`
- מופעי EC2 secondary (‏node_count − 1) — מריצים הפעלה-ראשונה במצב `--non-interactive --role secondary --primary-ip <primary private IP>`; תלויים בהשלמת ה-primary תחילה
- ALB על פורט 8000 — API‏ HTTP, בדיקת-בריאות `/health` (REQ-560)
- NLB על פורט 8815 — Arrow Flight / gRPC (REQ-143)
- שני ה-LB-ים מצורפים לכל ה-nodes

### רשימת דרישות מוקדמות

- [ ] הרשאות IAM: EC2 מלא, ELB מלא, VPC מלא, יצירת תפקיד IAM, S3‏ GetObject על bucket ה-AppImage
- [ ] `Provisa.AppImage` הועלה ל-S3
- [ ] ל-nodes‏ EC2 יש גישת S3 יוצאת (אינטרנט ישיר או endpoint‏ gateway של S3‏ VPC)
- [ ] זוג מפתחות EC2 קיים באזור היעד (אם נדרש SSH)
- [ ] Terraform ≥ 1.5 מותקן מקומית
- [ ] רשומות DNS מתוכננות עבור ALB / NLB (אופציונלי אך מומלץ)
- [ ] תעודת ACM מוכנה אם HTTPS נדרש (לא כלולה ב-Terraform הבסיסי)

### סודות

אין סודות משובצים ב-Terraform. ה-AppImage מייצר אישורים בעת הפעלה-ראשונה וכותב אותם ל-`~/.provisa/config.yaml` על כל node (REQ-563). עבור ייצור, שלפו את טוקן ה-admin מה-node ה-primary לאחר הפריסה:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### מתי להשתמש

הצוות שלכם כבר מפעיל אשכול Kubernetes ורוצה ש-Provisa ישתתף באותו מודל תפעולי (REQ-056). אם אתם מעריכים את Provisa או פורסים on-premises ללא אשכול קיים, נתיב ה-AppImage פשוט יותר.

הערה: ה-AppImage של Provisa אינו יכול לרוץ בתוך pod‏ Kubernetes — הוא דורש FUSE ודמון Docker חסר-שורש, שאינם זמינים בפרופילי אבטחת pod סטנדרטיים.

### שלבים

1. אשרו גישת אשכול:

   ```bash
   kubectl cluster-info
   ```

2. משכו ושקפו (mirror) image-ים לרישום הפנימי שלכם (נדרש עבור סביבות air-gapped או נסרקות; דלגו אם מושכים מרישומים ציבוריים ישירות) (REQ-294):

   | Image | נעשה בו שימוש עבור |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API של Provisa |
   | `trinodb/trino:480` | קואורדינטור + עובדי מנוע פדרציה (REQ-169) |
   | `postgres:16` | PostgreSQL בתוך-אשכולי (אם `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer בתוך-אשכולי (אם `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis בתוך-אשכולי (אם `redis.enabled` וללא `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO בתוך-אשכולי (אם `minio.enabled`) (REQ-029) |

   עבור סביבות נסרקות-רישום:
   - דחפו כל image לרישום ה-staging שלכם
   - הריצו את הסורק שלכם (Prisma Cloud, Aqua, Trivy, AWS Inspector) וקבלו אישור
   - קדמו לרישום הפנימי של הייצור שלכם

3. החליטו לפני ההתקנה:
   - **PostgreSQL** — בתוך-אשכולי (‏`postgresql.enabled: true`) או מנוהל חיצוני (`postgresql.host`)? חיצוני מומלץ לייצור
   - **Redis** — בתוך-אשכולי או חיצוני (‏`redis.host`)? שנו את הסיסמה ברירת המחדל (`redis.password`)
   - **MinIO / S3** — MinIO בתוך-אשכולי או S3 ילידי? עבור AWS, השתמשו ב-S3 עם תפקיד IAM
   - **סודות** — העבירו דרך `--set` להערכה; השתמשו ב-External Secrets או Vault Agent לייצור

4. התקינו את ה-chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   אם משתמשים ברישום פנימי, הוסיפו דריסות image:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. אמתו ש-pods רצים:

   ```bash
   kubectl get pods -n provisa
   ```

6. בדקו את ה-API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (אופציונלי) הפעילו ingress עבור גישה חיצונית — הגדירו `ingress.enabled: true` והגדירו את בקר ה-ingress שלכם

### רשימת דרישות מוקדמות

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] מחלקת אחסון התומכת ב-PVC-ים `ReadWriteOnce` (עבור שירותים stateful בתוך-אשכוליים)
- [ ] image-ים זמינים לאשכול (רישום ציבורי או פנימי)
- [ ] נקודת קצה PostgreSQL + אישורים (אם חיצוני)
- [ ] נקודת קצה Redis + אישורים (אם חיצוני)
- [ ] bucket‏ S3 + אישורים או תפקיד IAM
- [ ] טוקן admin נבחר
- [ ] בקר ingress מוגדר (אם נדרשת גישה חיצונית)

### ערכים מרכזיים

| ערך | ברירת מחדל | תיאור |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | replicas של API‏ Provisa (חסר-מצב) (REQ-057) |
| `config.pgHost` | `postgres` | host‏ PostgreSQL |
| `config.pgPassword` | | סיסמת PostgreSQL |
| `config.adminToken` | | טוקן bearer של admin API |
| `redis.enabled` | `true` | פרוס StatefulSet‏ Redis בתוך-אשכולי (REQ-371) |
| `redis.host` | `""` | הגדירו כדי להשתמש ב-Redis חיצוני |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | שנו זאת |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | פרוס מנוע פדרציה (REQ-028) |
| `trino.workers` | `2` | replicas עובד מנוע פדרציה (REQ-056) |
| `postgresql.enabled` | `true` | פרוס PostgreSQL בתוך-אשכולי (REQ-169) |
| `postgresql.host` | `""` | הגדירו כדי להשתמש ב-PostgreSQL חיצוני |
| `minio.enabled` | `true` | פרוס MinIO בתוך-אשכולי (REQ-029) |
| `s3.endpoint` | | URL נקודת קצה תואם-S3 |
| `s3.bucket` | `provisa-results` | bucket להפניית תוצאות גדולות (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | הפעל ingress |

### Scaling

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

עובדי מנוע פדרציה scale-ים באופן עצמאי — יותר עובדים מגדילים תפוקה ויכולת שאילתות במקביל (REQ-056). (REQ-057)

### עדכון תצורה

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## זמינות גבוהה (High Availability) והתאוששות

Provisa מחילה מודל התאוששות דו-שכבתי על פני כל מצבי הפריסה (REQ-703):

- **שכבה 1 — שגיאות חולפות.** פעולות קריאה מנסות שוב למשך עד 30 שניות על שגיאות חולפות באמצעות backoff אקספוננציאלי עם jitter מלא. כוונו את התקציב עם `PROVISA_RETRY_BUDGET_SECS`. פעולות כתיבה לעולם אינן מנוסות שוב פנימית, ושגיאות זיכרון לעולם אינן ניתנות-לניסיון-חוזר.
- **שכבה 2 — כשל רכיב.** צופה מנוע פנימי מזהה ומפעיל מחדש רכיבי תוכנה שנכשלו תוך 2–3 דקות.

כשלים ברמת-מכונה וברמת-אשכול נותרים באחריות המפעיל — ספקו nodes מיותרים (redundant) ו-load balancer (נתיבי Terraform ו-Helm שלמעלה) עבור סובלנות אובדן-node.

## תלויות מנוע פדרציה

מנועי הפדרציה של ה-warehouse דורשים חבילות Python ורכיבים ברמת-מערכת מעבר להתקנת ברירת המחדל של Provisa. כל חבילות ה-Python הרשומות כאן מוצהרות ב-`pyproject.toml` ומותקנות כחלק מ-`pip install provisa` הסטנדרטי או `pip install -e .` [tool-verified: `pyproject.toml` lines 44–52].

חבילות ה-Python מגיעות עם התקנת ברירת המחדל של Provisa — אין צורך ב-extras אופציונליים עבור אף מנוע warehouse. הפריטים ברמת-המערכת (דרייבר ODBC, CLI-י ענן, מפתחות service-account) חייבים להיות מותקנים בנפרד.

### חבילות Python (כבר בתלויות הליבה)

[tool-verified: `pyproject.toml` lines 41–52]

| חבילה | מנוע | מטרה |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | חיבור SQL warehouse; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | חיבור + `fetch_arrow_table` ילידי-Arrow (REQ-988) |
| `google-cloud-bigquery` | BigQuery | ביצוע שאילתה |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API עבור קריאות ילידיות-Arrow |
| `google-cloud-storage` | BigQuery | staging‏ GCS עבור קישורי טבלה חיצונית |
| `pyodbc` | Fabric, Synapse | חיבור ODBC לנקודות קצה T-SQL |
| `azure-identity` | Fabric, Synapse | טוקן Azure AD דרך `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | קריאות עמודתיות HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | נעילת תאימות — `google-cloud-*` ו-OTel חולקים runtime protobuf; `<7` שומר עליהם מיושרים |
| `grpcio-status<1.82` | gRPC | מתיישר עם נעילת `protobuf<7` |

### דרישות ברמת-מערכת

אלה אינן חבילות Python — הן חייבות להיות מותקנות על ה-host או הקונטיינר המריץ את Provisa.

**Microsoft Fabric ו-Azure Synapse (ODBC)**

`pyodbc` מתחבר דרך Microsoft ODBC Driver for SQL Server (`msodbcsql18`). הדרייבר חייב להיות מותקן על ה-host — לא דרך pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

macOS:

```bash
brew install microsoft/mssql-release/msodbcsql18
```

Linux (Ubuntu/Debian):

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

Provisa קולטת את הדרייבר אוטומטית. כדי לדרוס את שם הדרייבר (עבור התקנות לא-סטנדרטיות), הגדירו:

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**אימות Azure AD (Fabric ו-Synapse)**

שני המנועים מאמתים דרך `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` בודק מקורות אישורים לפי סדר: משתני סביבה, workload identity, managed identity, VS Code, `az login`, ואחרים.

עבור פיתוח מקומי, `az login` הוא הנתיב הפשוט ביותר:

```bash
az login
```

עבור ייצור, השתמשו ב-managed identity (על VM-י Azure או AKS) — אין צורך בניהול אישורים. עבור אימות service-principal, הגדירו:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (service account)**

`google-cloud-bigquery` משתמש ב-Application Default Credentials. עבור פיתוח מקומי, הצביעו על קובץ מפתח service-account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

עבור ייצור על GCP (Cloud Run, GKE עם Workload Identity, Compute Engine), הספרייה קולטת את ה-service account המצורף אוטומטית — אין צורך במשתנה סביבה.

ה-service account צריך:

- `roles/bigquery.dataViewer` — קריאת נתונים
- `roles/bigquery.jobUser` — הרצת שאילתות
- `roles/bigquery.dataEditor` — יצירת טבלאות חיצוניות (עבור ATTACH)
- `roles/storage.objectViewer` — קריאת אובייקטי GCS עבור טבלאות חיצוניות

**Databricks (תעודת CA בסביבות proxy לפיתוח)**

אם Provisa רצה מאחורי proxy מיירט-TLS (Charles, mitmproxy, proxy-ים ארגוניים), מחבר ה-SQL של Databricks עשוי לדחות את התעודה של ה-proxy. העבירו חבילת CA מותאמת אישית:

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

מחבר ה-Databricks יורש זאת מ-`requests` — אין צורך במשתנה סביבה ספציפי ל-Databricks.

### רשימת בדיקה לפי מנוע

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` מותקן (ברירת מחדל)
- [ ] URL מנוע עם `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] טוקן גישה אישי או טוקן service principal
- [ ] `REQUESTS_CA_BUNDLE` מוגדר אם מאחורי proxy מיירט-TLS

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` מותקן (ברירת מחדל)
- [ ] URL מנוע: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` ב-`PROVISA_ENGINE_URL` או `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` מותקנים (ברירת מחדל)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` מוגדר (פיתוח) או workload identity מוגדר (ייצור)
- [ ] `GOOGLE_CLOUD_PROJECT` מוגדר אם לא ניתן להסיק את הפרויקט מה-service account
- [ ] ל-service account יש תפקידי BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` + `azure-identity` מותקנים (ברירת מחדל)
- [ ] דרייבר מערכת `msodbcsql18` מותקן
- [ ] `FABRIC_SQL_SERVER` ו-`FABRIC_DATABASE` מוגדרים
- [ ] אימות Azure AD: `az login` (פיתוח) או managed identity / service principal (ייצור)
- [ ] `FABRIC_WORKSPACE_ID` מוגדר אם משתמשים בקישורי אחסון-אובייקטים חיצוניים

**Azure Synapse** (REQ-989)

- [ ] אותן דרישות Python + מערכת כמו Fabric
- [ ] `SYNAPSE_SQL_SERVER` ו-`SYNAPSE_DATABASE` מוגדרים
- [ ] אותה הגדרת אימות Azure AD כמו Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` מותקן (ברירת מחדל)
- [ ] URL מנוע: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` ב-`federation_hints` עבור TLS (פורט 8443)

---

## משתני סביבה

| משתנה | ברירת מחדל | מטרה |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | סיסמת PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | נתיב לקובץ תצורה (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | הפעל הפניית תוצאות גדולות ל-S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | סף ספירת שורות עבור הפניה (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | bucket‏ S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL נקודת קצה תואם-S3 (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL‏ URL חתום מראש (שניות) (REQ-141) |
| `REDIS_HOST` | `localhost` | host‏ Redis |
| `REDIS_PORT` | `6379` | פורט Redis |
| `REDIS_PASSWORD` | | סיסמת Redis |
| `REDIS_TLS` | `false` | הפעל TLS עבור Redis |
| `TRINO_HOST` | `localhost` | host‏ קואורדינטור מנוע פדרציית Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | פורט HTTP‏ קואורדינטור מנוע פדרציית Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | מפתח מנוע הפדרציה הפעיל (REQ-989); דורס תצורה מתמידה |
| `PROVISA_ENGINE_URL` | | URL חיבור עבור מנועים מונעי-URL (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | דריסת URL מאגר מימוש; ברירת מחדל למאגר המנוע עצמו |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | שם דרייבר ODBC עבור Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | נתיב למפתח service-account JSON של GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | מזהה פרויקט GCP (BigQuery; מוסק מ-service account כשלא מוגדר) |
| `FABRIC_SQL_SERVER` | | hostname נקודת קצה אנליטיקת SQL של Microsoft Fabric |
| `FABRIC_DATABASE` | | שם מסד נתונים Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID‏ workspace של Fabric (נדרש עבור קיצורי אחסון-אובייקטים חיצוניים) |
| `SYNAPSE_SQL_SERVER` | | hostname מאגר SQL ייעודי או serverless של Azure Synapse |
| `SYNAPSE_DATABASE` | | שם מסד נתונים Synapse |
| `AZURE_TENANT_ID` | | tenant‏ Azure AD (אימות service-principal עבור Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | מזהה client של אפליקציית Azure AD |
| `AZURE_CLIENT_SECRET` | | סוד client של אפליקציית Azure AD |
| `REQUESTS_CA_BUNDLE` | | נתיב חבילת CA מותאמת אישית (מחבר Databricks, proxy‏ TLS לפיתוח) |

---

## פקודות CLI

```bash
provisa start              # Start all services
provisa stop               # Stop all services
provisa restart            # Restart
provisa status             # Show service health
provisa open               # Open the UI in the browser
provisa logs               # Tail service logs
provisa export             # Print current config as YAML to stdout
provisa export FILE        # Write current config as YAML to FILE
provisa import FILE        # Replace running config with YAML from FILE
```

(REQ-224, REQ-164)

### זרימת קידום תצורה (dev ← test ← prod)

כל ההגדרות הספציפיות-לסביבה (מחרוזות חיבור, סודות, פורטים) שייכות במשתני סביבה או מנהלי סודות — לא בתצורה המיוצאת. ה-YAML המיוצא לוכד את המודל הסמנטי שלכם: מקורות, דומיינים, תפקידים, תצוגות. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```



ראו גם: [סביבות](environments.md) מסביר כיצד לנהל עותקים בעלי שם, מבודדי-סכמה, של המודל הממושל שלכם.
