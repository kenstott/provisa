# פריסה (Deployment)

## בחירת נתיב פריסה {: #choosing-a-deployment-path }

Provisa תומכת בשישה נתיבי פריסה. בחרו בהתבסס על הקהל וההקשר התפעולי שלכם:

| נתיב | Artifact / סקריפט | הכי מתאים ל |
| ------ | ------------------- | ---------- |
| **פיתוח (Development)** | `start-ui.sh` | פיתוח מקוד-מקור, הערכה עם נתוני הדגמה מלאים |
| **מתקין macOS** | `Provisa-<version>-macOS.dmg` | תחנות עבודה למפתחים, הערכה |
| **מתקין Windows** | `Provisa-<version>-windows-x64.exe` | תחנות עבודה למפתחים, הערכה |
| **AppImage ל-Linux** | `Provisa.AppImage` | שרתי on-prem, VM-ים בענן, סביבות air-gapped |
| **VM-ים בענן (AWS)** | `terraform/deploy.sh` | פריסה רב-node בענן עם load balancer-ים |
| **Kubernetes** | `helm/provisa/` | צוותים המפעילים כבר K8s |

### VM לעומת Kubernetes {: #vm-vs-kubernetes }

שניהם ברמה ארגונית (enterprise-grade). נתיב ה-VM/AppImage פשוט יותר: אין אשכול לספק, אין מדיניות CNI או RBAC להגדיר, וה-AppImage הוא לגמרי self-contained. הוא משתלב באופן טבעי בכלי ניהול שרתים קיימים (Ansible, Puppet, סוכני Datadog, מעבירי Splunk, וכו').

בחרו ב-Kubernetes רק אם הצוות שלכם כבר מפעיל אשכול K8s ורוצה ש-Provisa ישתלב בדרך התפעול הזו (פריסות מתגלגלות, HPA, ניטור מאוחד). שתי האפשרויות נותנות את אותן יכולות; Kubernetes מוסיף עומס תפעולי.

### רכישת image ו-סריקת אבטחה {: #image-acquisition-and-security-scanning }

כל נתיב ייצור צריך את תוצרי Provisa לפני שפריסה כלשהי יכולה לרוץ. "Air-gapped" מתאר את מה שקורה בזמן ההתקנה במכונת היעד, ולכן משיגים את התוצרים קודם.

**תוכנות התקנה ל-macOS ול-Windows:** הורידו אותן מ[עמוד השחרורים ב-GitHub](https://github.com/provisa/provisa/releases). הן ארוזות במלואן ולא צריכות אינטרנט אחרי ההורדה. הן מיועדות לפיתוח ולהערכה, ולכן לא מצופה שער של סריקת אימג'ים.

**נתיב AppImage:** הורידו אותו מ[עמוד השחרורים ב-GitHub](https://github.com/provisa/provisa/releases) והעתיקו אותו למכונת היעד. ה-AppImage אורז כל אימג' של רכיב כ-tarball בתוך מערכת קבצים squashfs, שרוב סורקי המאגרים לא יכולים לבדוק במקום. בקשו מצוות החשבון שלכם ב-Provisa את ה-digests של אימג'י הרכיבים כדי לבדוק אותם בעצמכם מול הסורק שלכם.

**נתיב Terraform:** העלו את ה-AppImage ל-S3 לפני שמריצים את `terraform/deploy.sh`. צמתי EC2 מורידים אותו בעת האתחול דרך תפקיד ה-IAM שלהם, ולכן הם צריכים גישה יוצאת ל-S3, ישירות או דרך נקודת קצה של שער VPC. החילו את אותה מדיניות סריקה כמו בנתיב ה-AppImage.

**נתיב Helm / Kubernetes:** דחפו את האימג'ים הנפרדים למאגר שהאשכול מגיע אליו. הנתיב הזה מתאים ביותר לסריקה מבוססת מאגר (Prisma Cloud, Aqua, Trivy, AWS Inspector), כי אימג'ים הם אובייקטים שסורקים כבר מבינים. לאשכול מבודד, שכפלו את האימג'ים למאגר פנימי ודרסו את ההפניות ב-`values.yaml`.

---

## פיתוח (מקוד-מקור) {: #development-from-source }

### מומלץ: `start-ui.sh` {: #recommended-start-uish }

הדרך הקלה ביותר להריץ את Provisa מקוד-מקור. מתחיל את כל התשתית, ה-API של הbackend, ושרת הפיתוח של ה-UI בפקודה אחת. Ctrl+C מכבה הכל בצורה נקייה.

**דרישות מוקדמות:** Docker Desktop, Node.js, virtualenv‏ Python ב-`.venv/`

```bash
./start-ui.sh
```

מה הוא עושה:

- מתחיל את `docker-compose.core.yml` + `docker-compose.dev.yml` (כל שירותי הליבה + ההדגמה) וממתין ל-healthy
- מזריע (seed) את Kafka עם נתוני הדגמה
- מסנכרן תלויות Python מ-`.venv/`
- מתחיל את ה-API של הbackend על פורט 8001 (לוגים ל-`.logs/server.log`)
- מתחיל את שרת פיתוח ה-UI של Vite על פורט 3000
- מדפיס URL-ים וממתין; Ctrl+C עוצר הכל ומפרק (tears down) את compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**אפשרויות:**

`--reset-volumes` — מריץ `docker compose down -v` לפני ההתחלה, הורס את כל volume-י ה-Docker (נתוני PostgreSQL, אובייקטי MinIO, מצב Redis, וכו'). השתמשו כשאתם רוצים slate נקי לחלוטין: אחרי שינוי סכמה בזמן פיתוח, או כאשר Docker קרס והשאיר volumes פגומים. **כל הנתונים יאבדו.**

`--observability` — מוסיף instrumentation מלא של tracing ומטריקות. מוריד את סוכן ה-Java של OpenTelemetry ומתקן את `jvm.config` של Trino לטעון אותו, מציין (instruments) את ה-backend של Provisa עם ייצוא OTLP, ומתחיל את ה-collector של OTel, Prometheus, Tempo, ו-Grafana (`http://localhost:3100`). התיקון ל-`jvm.config` מוחזר אוטומטית ב-Ctrl+C.

### שלבים ידניים (backend בלבד, ללא UI) {: #manual-steps-backend-only-no-ui }

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

### מחסנית מלאה (Provisa בקונטיינר) {: #full-stack-provisa-in-container }

כדי להריץ את ה-API כקונטיינר במקום על ה-host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### שירותים {: #services }

**ליבה (`docker-compose.core.yml`) — נדרש תמיד:**

| שירות | פורט | מטרה |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | מטא-דאטת תצורה + קטלוג Iceberg |
| PgBouncer | 6432 | pooling חיבורים |
| מנוע פדרציה | 8080 | פדרציית שאילתות |
| Redis | 6379 | מטמון תוצאות שאילתה |
| MinIO | 9000/9001 | אחסון אובייקטים תואם-S3 |

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
| OpenTelemetry Collector | — | איסוף trace (עם `--observability`) |
| Prometheus | 9090 | מטריקות (עם `--observability`) |
| Tempo | — | אחסון trace (עם `--observability`) |
| Grafana | 3100 | dashboards (עם `--observability`) |

המאגרים, מנוע הפדרציה ושירותי הטלמטריה האלה מפרסמים על `PROVISA_STORE_BIND_IP` (‏`127.0.0.1` אלא אם נקבע אחרת), עם סיסמאות שנוצרו עבור הפריסה. ראו [הקשחת פריסה](security.md#deployment-hardening).

### Backend טלמטריה (`otlp2sql`) {: #telemetry-backend-otlp2sql }

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

**עם `trino://`:** הניב של Trino ב-SQLAlchemy מפיק DDL ו-`INSERT` תקינים של Trino, כך שאפשר להשתמש בו כ-backend של `otlp2sql`, אבל אנחנו ממליצים עליו רק לקצבי קליטה נמוכים. כל ריקון של אצווה הופך ל-`INSERT` מבוזר של Trino ועוד תמונת מצב של Iceberg, ולכן טלמטריה בקצב גבוה יוצרת הרבה קבצים ותמונות מצב קטנים, ועדיין צריכה `ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` תקופתי, ש-`otlp2sql` לא מריץ. זה גם שם את מנוע השאילתות בנתיב החם של הקליטה.

עבור טלמטריה בנפח-גבוה ל-Trino/Iceberg, השתמשו במקום זאת ב-`otlp2parquet`: הוא
כותב parquet לאחסון אובייקטים ללא מעבר דרך Trino, ו-compaction מתוזמן של
Trino מגלגל את הקבצים הגולמיים לתוך טבלאות Iceberg החיות. עבור
מנוע יחיד המטפל הן בקליטה בקצב-גבוה והן ב-compaction, העדיפו ClickHouse.

הצביעו את מייצאי ה-OTLP של האפליקציה ו-Trino (‏`OTEL_EXPORTER_OTLP_ENDPOINT`) על
נקודת הקצה של `otlp2sql`, ורשמו את דומיין ה-ops מול אותו
`PROVISA_OPS_DB_URL` כך שהוא קורא את מה שהמקלט כתב.

---

## מתקין macOS {: #macos-installer }

לתחנות עבודה של מפתחים ולהערכה. מבודד לגמרי ולא צריך אינטרנט אחרי ההורדה.

תוכנת ההתקנה הבסיסית היא **התקנה מקורית**: מנוע הפדרציה DuckDB, מישור בקרה מוטמע של PostgreSQL (pgserver) ומטמון בזיכרון (fakeredis), בלי Docker, VM, Trino, Redis או MinIO. את מנוע הפדרציה בוחרים באשף: DuckDB (מקורי, ברירת המחדל), Trino על Docker או מנוע חיצוני. הניטור הוא טלמטריה עצמית שתמיד פועלת וגלויה ב-Admin; מחסנית ה-Docker של collector, Prometheus ו-Grafana היא הדגמה אופציונלית נפרדת, והיא לא מדליקה או מכבה אותו. חבילת נתוני ההדגמה אופציונלית וכבויה כברירת מחדל. Trino, מחסנית הניטור ב-Docker וההדגמה הם תוספות כבדות. מחפשים אותם קודם מקומית (התיקייה שליד תוכנת ההתקנה, אמצעי אחסון מחוברים, `~/Downloads`) ורק אחר כך מורידים אותם משחרור ה-GitHub, כך שארגונים יכולים להכין את קובצי ה-tarball מראש להתקנות מבודדות.

### שלבים {: #steps }

1. הורידו את `Provisa-<version>-macOS.dmg` מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases)
2. פתחו את ה-DMG וגררו את **Provisa.app** ל-`/Applications`
3. לחצו פעמיים על **Provisa.app**. הגדרת ההפעלה הראשונה רצה פעם אחת, והאשף מציע את בחירות המנוע, observability, וההדגמה שלעיל
4. פתחו Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

### התמדת נתונים (Data persistence) {: #data-persistence }

כל הנתונים מאוחסנים ב-`~/.provisa/`. כדי להסיר הכל: `provisa uninstall`.

---

## מתקין Windows {: #windows-installer }

לתחנות עבודה של מפתחים ולהערכה. מבודד לגמרי ולא צריך אינטרנט אחרי ההורדה.

כמו macOS, מתקין ה-Windows הבסיסי הוא **שכבה ילידית**: runtime‏ Python עצמאי + wheel‏ provisa + DuckDB/pg_duckdb + control plane‏ PostgreSQL מוטמע (pgserver), ללא משלוח Docker, VM, או image-י קונטיינר. מנוע הפדרציה (Trino), מחסנית ה-observability, וחבילת נתוני ההדגמה נוספים מאוחר יותר דרך מתקינים שכבתיים נפרדים, לפי סדר: מתקין ה-Container (‏`Provisa-Container-<version>.exe`, המוסיף WSL2 + containerd + Trino), אז מתקין ה-Obs (דורש את שכבת ה-container), ואז מתקין ה-Demo (דורש Core + Obs). הנחיית הפעלה-ראשונה מסבירה כיצד לאתחל את מנוע הפדרציה על ידי הרצת מתקין ה-Container.

### שלבים {: #steps_1 }

1. הורידו את `Provisa-<version>-windows-x64.exe` מ[עמוד releases ב-GitHub](https://github.com/provisa/provisa/releases)
2. הריצו את המתקין. הוא לא דורש הרשאות admin ומותקן ב-`%LOCALAPPDATA%\Programs\Provisa\`
3. פתחו את **Provisa First Launch** מתפריט ה-Start. הגדרה ילידית רצה פעם אחת ומדפיסה את הנחיית הצעדים-הבאים עבור התוספים השכבתיים
4. פתחו terminal חדש:

   ```text
   provisa status
   provisa open
   ```

### התמדת נתונים {: #data-persistence_1 }

כל הנתונים מאוחסנים ב-`%USERPROFILE%\.provisa\`.

---

## Linux AppImage — VM יחיד או רב-Node {: #linux-appimage-single-or-multi-node-vm }

### מה זה {: #what-it-is }

`Provisa.AppImage` הוא קובץ הרצה יחיד self-contained הארוז:

- דמון Docker חסר-שורש (`dockerd-rootless.sh` + `rootlesskit`), כך שאין צורך ב-Docker מערכתי או ב-root
- כל tarballs‏ image הקונטיינר (PostgreSQL, PgBouncer, MinIO, Redis, מנוע פדרציה, API של Provisa)
- ה-wrapper של Provisa CLI וסקריפט הגדרת הפעלה-ראשונה

האימג' של Provisa נבנה בזמן האריזה, וקוד מקור של Python לעולם לא כלול בו.

### מתי להשתמש {: #when-to-use }

- On-premises bare metal או VM (node יחיד או רב-node)
- VM-ים בענן ללא אשכול K8s
- סביבות Air-gapped
- כאשר אתם רוצים תפעול פשוט יותר מ-Kubernetes

---

### שלבים — Node יחיד {: #steps-single-node }

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
   - **פורט API** ← ברירת מחדל `8000`
5. ההגדרה טוענת את כל image-י הקונטיינר (~2–5 דקות), כותבת תצורה, ומתחילה שירותים
6. אמתו:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### שלבים — רב-Node (Primary) {: #steps-multi-node-primary }

הריצו שלבים אלה על ה-node ה-primary תחילה. secondaries חייבים להיות מוגדרים לאחר שה-primary רץ.

1. הורידו והעבירו את `Provisa.AppImage` למכונת ה-primary
2. פתחו פורטי firewall נדרשים לשימוש ה-secondaries בלבד (הם מתחברים inbound על אלה):

   | פורט | שירות |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | קואורדינטור מנוע פדרציה |
   | 8000 | API של Provisa |

3. הפכו להרצה והריצו כשה-stores מפורסמים בכתובת הפרטית של ה-node, שם ה-secondaries פונים אליהם. node ללא secondaries מפרסם אותם על `127.0.0.1` בלבד:

   ```bash
   chmod +x Provisa.AppImage
   PROVISA_STORE_BIND_IP=<this node's private IP> ./Provisa.AppImage
   ```

4. אשף ההגדרה שואל:
   - **תפקיד** ← בחרו `primary`
   - **תקציב RAM**, **hostname**, **פורט API** ← ענו כמו עבור node יחיד
5. לאחר השלמת ההגדרה, רשמו את ה-**IP הפרטי** של מכונה זו. ה-secondaries צריכים אותו, ואת אישורי ה-stores שה-primary יצר: `~/.provisa/store-credentials.env` (‏`PROVISA_PG_PASSWORD`, ‏`PROVISA_REDIS_PASSWORD`, ‏`PROVISA_MINIO_ROOT_PASSWORD`, ‏`PROVISA_GRAFANA_ADMIN_PASSWORD`, ‏`PROVISA_TRINO_SHARED_SECRET`). העתיקו קובץ זה לכל secondary בערוץ מאובטח
6. האשף מדפיס בלוק upstream‏ nginx; שמרו אותו עבור תצורת ה-load balancer שלכם
7. אמתו:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### שלבים — רב-Node (כל Secondary) {: #steps-multi-node-each-secondary }

חזרו על שלבים אלה על כל node נוסף לאחר שה-primary רץ ונגיש.

1. הורידו והעבירו את `Provisa.AppImage` למכונת ה-secondary
2. אשרו שה-secondary יכול להגיע ל-primary:

   ```bash
   curl -k https://<primary-ip>:8000/health
   ```

3. הפכו להרצה והריצו כשאישורי ה-stores של ה-primary בסביבה שלו (הקובץ שהועתק מה-primary). secondary שלא קיבל אותם אינו עולה:

   ```bash
   chmod +x Provisa.AppImage
   set -a; . ./store-credentials.env; set +a
   ./Provisa.AppImage
   ```

4. אשף ההגדרה שואל:
   - **תפקיד** ← בחרו `secondary`
   - **IP‏ Primary** ← הזינו את ה-IP של ה-node‏ primary (הקישוריות מאומתת בזמן אמת)
   - **תקציב RAM**, **hostname**, **פורט API** ← ענו כמו לעיל
5. ההגדרה טוענת קבוצת image מצומצמת (ללא PostgreSQL, PgBouncer, MinIO ו-Redis, שרצים רק על ה-primary), מתחילה את ה-API של Provisa ועובד מנוע פדרציה
6. אמתו:

   ```bash
   provisa status
   curl -k https://localhost:8000/health
   ```

7. הוסיפו node זה ל-upstream‏ ה-load balancer שלכם

---

### טופולוגיית Primary / Secondary {: #primary-secondary-topology }

**Node ה-primary** מריץ את כל שירותי ה-singleton:

| שירות | למה singleton |
| --------- | --------------- |
| PostgreSQL | סכמה משותפת, תצורת אפליקציה, מודל סמנטי |
| Redis | מטמון תוצאות שאילתה משותף ומצב מנוי |
| MinIO | מאגר אובייקטים משותף עבור תוצאות הפניה ו-snapshots של MV |
| קואורדינטור מנוע פדרציה | כל העובדים (primary + secondaries) נרשמים כאן |

**Node-ים secondary** מריצים רק:

- API של Provisa — חסר-מצב (stateless); קורא את כל התצורה מ-PostgreSQL על ה-primary בעת ההפעלה
- עובד מנוע פדרציה — נרשם-עצמית עם הקואורדינטור על ה-primary

כל מצב האפליקציה זורם דרך ה-PostgreSQL של ה-primary. אין צורך בסנכרון ידני.

---

### הפעלה-ראשונה לא-אינטראקטיבית (אוטומטית) {: #non-interactive-automated-first-launch }

עבור Terraform, cloud-init, או Ansible, העבירו דגלים במקום לענות על prompts:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

מצב לא-אינטראקטיבי מתקין יחידת systemd (‏`/etc/systemd/system/provisa.service`) להתחלה-בהפעלה (start-on-boot).

| דגל | תיאור |
| ------ | ------------- |
| `--non-interactive` | דלג על כל ה-prompts; התקן יחידת systemd |
| `--role primary\|secondary` | תפקיד ה-node |
| `--primary-ip <ip>` | IP‏ node ה-primary (נדרש עבור secondary) |
| `--ram-gb <n>` | RAM להקצות (0 = הכל זמין) |

---

## פריסת VM בענן — Terraform (AWS) {: #cloud-vm-deployment-terraform-aws }

פקודה אינטראקטיבית אחת מקימה אשכול Provisa מלא מרובה צמתים ב-AWS: VPC, קבוצות אבטחה, מופעי EC2, ALB ו-NLB.

### קבצים {: #files }

| קובץ | מטרה |
| ------ | --------- |
| `terraform/deploy.sh` | wrapper אינטראקטיבי — אוסף פרמטרים, מאמת אישורים, כותב `terraform.tfvars`, מריץ apply |
| `terraform/aws/variables.tf` | כל הגדרות המשתנים עם ברירות מחדל |
| `terraform/aws/main.tf` | VPC, subnets, security groups, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL-י נקודת קצה ו-IP-י node |

### שלבים {: #steps_2 }

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
   api_endpoint      = "https://<alb-dns>:8000"
   protocol_endpoints = { flight = "<alb-dns>:8815", ... }
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

9. (אופציונלי) הצביעו רשומות DNS על שמות ה-DNS של ה-ALB וה-NLB

10. אמתו:

    ```bash
    curl http://<api_endpoint>/health
    ```

### שאלות האשף {: #wizard-questions }

| שאלה | ברירת מחדל | הערות |
| ---------- | --------- | ------- |
| ספק ענן | — | AWS בלבד |
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

### מדריך גודל מופע {: #instance-sizing-guide }

| סוג | vCPU | RAM | עובדי Trino/node | מקרה שימוש |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | פיתוח / סטי נתונים קטנים |
| `m7i.2xlarge` | 8 | 32 GB | 1 | ייצור קטן |
| `m7i.4xlarge` | 16 | 64 GB | 2 | ייצור בינוני |
| `m7i.8xlarge` | 32 | 128 GB | 4 | ייצור גדול |

כל ה-nodes תורמים עובדים לקואורדינטור אחד על ה-primary. אשכול `m7i.4xlarge` בן 3 nodes מניב 6 עובדי Trino בסך הכל.

### מה מסופק {: #what-gets-provisioned }

- VPC עם שני subnets ציבוריים על פני שני אזורי זמינות
- Security groups: קבוצת LB (‏ingress ציבורי על 8000/8815), קבוצת nodes (LB ← nodes, תוך-אשכולי, SSH אופציונלי)
- תפקיד IAM + פרופיל מופע עם S3‏ GetObject על ה-bucket של ה-AppImage
- מופע EC2 ראשי (primary) — מריץ הפעלה-ראשונה במצב `--non-interactive --role primary`
- מופעי EC2 secondary (‏node_count − 1) — מריצים הפעלה-ראשונה במצב `--non-interactive --role secondary --primary-ip <primary private IP>`; תלויים בהשלמת ה-primary תחילה
- ALB על פורט 8000 — API‏ HTTP, בדיקת-בריאות `/health`
- NLB על פורט 8815 — Arrow Flight / gRPC
- שני ה-LB-ים מצורפים לכל ה-nodes

### רשימת דרישות מוקדמות {: #prerequisites-checklist }

- [ ] הרשאות IAM: EC2 מלא, ELB מלא, VPC מלא, יצירת תפקיד IAM, S3‏ GetObject על bucket ה-AppImage
- [ ] `Provisa.AppImage` הועלה ל-S3
- [ ] ל-nodes‏ EC2 יש גישת S3 יוצאת (אינטרנט ישיר או endpoint‏ gateway של S3‏ VPC)
- [ ] זוג מפתחות EC2 קיים באזור היעד (אם נדרש SSH)
- [ ] Terraform ≥ 1.5 מותקן מקומית
- [ ] רשומות DNS מתוכננות עבור ALB / NLB (אופציונלי אך מומלץ)
- [ ] תעודת ACM מוכנה אם HTTPS נדרש (לא כלולה ב-Terraform הבסיסי)

### סודות {: #secrets }

סיסמאות ה-stores המובנים (PostgreSQL, Redis, MinIO, וה-admin של Grafana) והסוד המשותף של אשכול Trino נוצרים על ידי Terraform (`random_password`) ומועברות לכל node, ששומר אותן בקובץ הסביבה של systemd שלו, במצב 600. הן נשמרות ב-state של Terraform, ולכן הגנו על ה-state כפי שהייתם מגינים על הסיסמאות. ללא Terraform, ה-launcher של כל node מייצר אותן ב-`~/.provisa/store-credentials.env`. ה-AppImage מייצר את שאר האישורים בעת הפעלה-ראשונה וכותב אותם ל-`~/.provisa/config.yaml` על כל node. עבור ייצור, שלפו את טוקן ה-admin מה-node ה-primary לאחר הפריסה:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### מתי להשתמש {: #when-to-use_1 }

הצוות שלכם כבר מפעיל אשכול Kubernetes ורוצה ש-Provisa ישתתף באותו מודל תפעולי. אם אתם מעריכים את Provisa או פורסים on-premises ללא אשכול קיים, נתיב ה-AppImage פשוט יותר.

הערה: ה-AppImage של Provisa לא יכול לרוץ בתוך pod של Kubernetes. הוא צריך FUSE ו-daemon של Docker בלי root, ופרופילי האבטחה הרגילים של pods לא מאפשרים אף אחד מהם.

### שלבים {: #steps_3 }

1. אשרו גישת אשכול:

   ```bash
   kubectl cluster-info
   ```

2. משכו ושקפו (mirror) image-ים לרישום הפנימי שלכם (נדרש עבור סביבות air-gapped או נסרקות; דלגו אם מושכים מרישומים ציבוריים ישירות):

   | Image | נעשה בו שימוש עבור |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API של Provisa |
   | `trinodb/trino:480` | קואורדינטור + עובדי מנוע פדרציה |
   | `postgres:16` | PostgreSQL בתוך-אשכולי (אם `postgresql.enabled`) |
   | `edoburu/pgbouncer:latest` | PgBouncer בתוך-אשכולי (אם `pgbouncer.enabled`) |
   | `redis:7.2` | Redis בתוך-אשכולי (אם `redis.enabled` וללא `redis.host`) |
   | `minio/minio:latest` | MinIO בתוך-אשכולי (אם `minio.enabled`) |

   עבור סביבות נסרקות-רישום:
   - דחפו כל image לרישום ה-staging שלכם
   - הריצו את הסורק שלכם (Prisma Cloud, Aqua, Trivy, AWS Inspector) וקבלו אישור
   - קדמו לרישום הפנימי של הייצור שלכם

3. החליטו לפני ההתקנה:
   - **PostgreSQL** — בתוך-אשכולי (‏`postgresql.enabled: true`) או מנוהל חיצוני (`postgresql.host`)? חיצוני מומלץ לייצור
   - **Redis** — בתוך-אשכולי או חיצוני (‏`redis.host`)?
   - **אישורי ה-stores** — צרו את ה-Secret ש-`credentials.existingSecret` מציין, עם המפתחות `pg-password`, ‏`redis-password`, ‏`minio-access-key`, ‏`minio-secret-key`, ‏`grafana-admin-password` ו-`trino-shared-secret`. ה-chart אינו קובע סיסמה משלו ואינו נוצר בלעדיו
   - **TLS** — פעיל כברירת מחדל (`tls.enabled: true`): צרו את ה-Secret מסוג `kubernetes.io/tls` ש-`tls.secretName` מציין, או הגדירו `tls.enabled: false`
   - **MinIO / S3** — MinIO בתוך-אשכולי או S3 ילידי? עבור AWS, השתמשו ב-S3 עם תפקיד IAM
   - **סודות** — העבירו דרך `--set` להערכה; השתמשו ב-External Secrets או Vault Agent לייצור
   - **מטמון ה-coordinator של Trino** — כרך של 1Gi שנדרש כברירת מחדל ומעוגן ב-`/data/trino/cache` (`trino.coordinator.cache.persistence`: `enabled`, `size`, `storageClass`). תוצאות ה-describe של מקור Salesforce נשמרות שם, כדי ש-coordinator שהופעל מחדש לא יבזבז שוב את מכסת ה-API של הארגון על תיאורו. כבו אותו רק כשאף מקור אינו זקוק לו

4. תנו לפריסה את מפתח המאסטר שלה. המפתח מצפין את כספת הסודות; כל pod של ה-API חייב
   להחזיק באותו מפתח, והוא חייב לשרוד כל pod. ה-chart אינו מתרנדר בלעדיו.

   ```bash
   kubectl create namespace provisa
   kubectl create secret generic provisa-master-key --namespace provisa \
     --from-literal=master-key="$(openssl rand -base64 32)"
   ```

   שמרו עותק של המפתח מחוץ ל-cluster. כספת שנכתבה במפתח שאבד לאחר מכן
   אינה ניתנת לפתיחה. החלופה ל-Secret היא כרך נתונים קבוע
   (`encryption.dataVolume.enabled=true`), שבו המוצר שומר את המפתח שהוא מייצר; ראו
   `encryption` ב-`values.yaml`.

5. התקינו את ה-chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set encryption.existingSecret=provisa-master-key \
     --set auth.provider=oidc \
     --set auth.oidc.issuerUrl=https://login.example.com/realms/main \
     --set auth.oidc.clientId=provisa \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   ה-chart אינו מתרנדר עד שנבחר `auth.provider`: `oidc` (מנפיק OpenID Connect
   ארגוני), `saml` (`auth.saml.idpMetadataUrl` ו-`auth.saml.publicUrl`), `ldap` (`auth.ldap.*`),
   `local` (חשבון החירום בלבד, `auth.breakGlass.*`) או `none` (ללא אימות,
   לפיתוח בלבד). כל אישור מגיע מ-Secret של Kubernetes; `ldap`, `saml` ו-`local`
   צריכים גם את `auth.sessionSecret.existingSecret`, המפתח שחותם על סשנים של הדפדפן. ראו `auth`
   ב-`values.yaml`.

   ה-coordinator של Trino מריץ את ההצהרות של כל ארגון בקבוצת המשאבים שלו,
   `global.tenant-<org>`, מתוך קובץ קבוצות המשאבים שה-chart מספק. הצהרות שעוברות דרך
   ה-proxy של Arrow Flight חולקות קבוצה אחת, `global.flight`: ה-proxy פותח חיבור Trino משלו
   ואינו יכול לשאת את הארגון.

   אם משתמשים ברישום פנימי, הוסיפו דריסות image:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

6. אמתו ש-pods רצים:

   ```bash
   kubectl get pods -n provisa
   ```

7. בדקו את ה-API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

8. (אופציונלי) הפעילו ingress עבור גישה חיצונית: הגדירו `ingress.enabled: true` והגדירו את בקר ה-ingress שלכם

### רשימת דרישות מוקדמות {: #prerequisites-checklist_1 }

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] מחלקת אחסון התומכת ב-PVC-ים `ReadWriteOnce` (עבור שירותים stateful בתוך-אשכוליים)
- [ ] image-ים זמינים לאשכול (רישום ציבורי או פנימי)
- [ ] נקודת קצה PostgreSQL + אישורים (אם חיצוני)
- [ ] נקודת קצה Redis + אישורים (אם חיצוני)
- [ ] bucket‏ S3 + אישורים או תפקיד IAM
- [ ] טוקן admin נבחר
- [ ] בקר ingress מוגדר (אם נדרשת גישה חיצונית)

### ערכים מרכזיים {: #key-values }

| ערך | ברירת מחדל | תיאור |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | replicas של API‏ Provisa (חסר-מצב) |
| `config.pgHost` | `postgres` | host‏ PostgreSQL |
| `config.pgPassword` | | סיסמת PostgreSQL |
| `config.adminToken` | | טוקן bearer של admin API |
| `redis.enabled` | `true` | פרוס StatefulSet‏ Redis בתוך-אשכולי |
| `redis.host` | `""` | הגדירו כדי להשתמש ב-Redis חיצוני |
| `redis.port` | `6379` | |
| `credentials.existingSecret` | | Secret המכיל את סיסמאות ה-stores (חובה) |
| `tls.enabled` | `true` | מגיש כל endpoint ב-TLS מתוך `tls.secretName` |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | פרוס מנוע פדרציה |
| `trino.workers` | `2` | replicas עובד מנוע פדרציה |
| `trino.internalTls` | `false` | הצפנת התעבורה בין coordinator ל-workers בתעודה שה-chart מייצר; מאטה שאילתות עתירות shuffle ב-10% ועד למעלה מ-100% |
| `postgresql.enabled` | `true` | פרוס PostgreSQL בתוך-אשכולי |
| `postgresql.host` | `""` | הגדירו כדי להשתמש ב-PostgreSQL חיצוני |
| `minio.enabled` | `true` | פרוס MinIO בתוך-אשכולי |
| `s3.endpoint` | | URL נקודת קצה תואם-S3 |
| `s3.bucket` | `provisa-results` | bucket להפניית תוצאות גדולות |
| `ingress.enabled` | `false` | הפעל ingress |

### Scaling

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

ה-workers של מנוע הפדרציה מתרחבים בנפרד, ויותר workers פירושם יותר תפוקה ויותר שאילתות בו-זמנית.

### עדכון תצורה {: #updating-config }

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## זמינות גבוהה (High Availability) והתאוששות {: #high-availability-recovery }

Provisa מחילה מודל התאוששות דו-שכבתי על פני כל מצבי הפריסה:

- **שכבה 1 — שגיאות חולפות.** פעולות קריאה מנסות שוב למשך עד 30 שניות על שגיאות חולפות באמצעות backoff אקספוננציאלי עם jitter מלא. כוונו את התקציב עם `PROVISA_RETRY_BUDGET_SECS`. פעולות כתיבה לעולם אינן מנוסות שוב פנימית, ושגיאות זיכרון לעולם אינן ניתנות-לניסיון-חוזר.
- **שכבה 2 — כשל רכיב.** צופה מנוע פנימי מזהה ומפעיל מחדש רכיבי תוכנה שנכשלו תוך 2–3 דקות.

כשלים ברמת המכונה והאשכול נשארים באחריות המפעיל. כדי לעמוד באובדן של צומת, הקימו צמתים יתירים ומאזן עומסים (נתיבי Terraform ו-Helm שלמעלה).

### טבלת טלמטריה שהמטא-נתונים שלה חסרים {: #telemetry-table-whose-metadata-is-missing }

במנוע Trino, טבלאות הטלמטריה (`otel.signals.traces`,‏ `metrics`,‏ `logs`) הן טבלאות Iceberg. הרישום של כל אחת הוא שורה במסד הנתונים של מישור הבקרה, והמטא-נתונים והנתונים שלה הם קבצים ב-bucket של הטלמטריה. כשה-bucket מוגש בלי הקבצים האלה בזמן שמסד הנתונים עדיין מחזיק את הרישומים, כל משפט על הטבלה נכשל עם `ICEBERG_MISSING_METADATA`. זה קורה כשהדיסק של ה-bucket עדיין לא היה מעוגן כשאחסון האובייקטים התחיל, או כשה-bucket רוקן או הוחלף.

Provisa אינו משנה דבר במקרה זה. כל הרצת דחיסה רושמת `ERROR` שמציין את הטבלה ואת מיקום המטא-נתונים ש-Trino לא הצליח לקרוא (`otel.signals.traces is registered but its metadata cannot be read …`), מדלגת על האות הזה ושומרת את הקבצים הגולמיים שלו ב-bucket להרצה מאוחרת יותר. שאר האותות נדחסים כרגיל. האתחול משאיר את הטבלה מחוץ לבדיקות הסכימה שלו באותו אופן.

שחזרו אותה ידנית, בסדר הזה:

1. **בררו אם המטא-נתונים עדיין קיימים.** ה-`ERROR` מציין את המיקום. ב-coordinator של Terraform ‏(GCP), ודאו שדיסק אחסון האובייקטים מעוגן (`findmnt /var/lib/provisa-object-store`) ושקונטיינר MinIO התחיל אחרי העיגון. MinIO שהתחיל קודם מגיש את התיקייה הריקה שמתחת לנקודת העיגון; הפעילו אותו מחדש אחרי שהדיסק מעוגן, והטבלה נקראת שוב עם ההיסטוריה שלה שלמה.
2. **אם קובץ המטא-נתונים קיים אבל הרישום מצביע למקום אחר**, רשמו את הטבלה שוב במיקום שלה:

   ```sql
   CALL otel.system.register_table(schema_name => 'signals', table_name => 'traces', table_location => 's3://<bucket>/warehouse/signals/<table directory>')
   ```

   בטלו קודם את השורה המיושנת אם השם תפוס (`CALL otel.system.unregister_table(schema_name => 'signals', table_name => 'traces')`).
3. **אם הקבצים של ה-bucket באמת אבדו**, בטלו את רישום הטבלה. הרצת הדחיסה הבאה יוצרת אותה מחדש, ריקה. לא ניתן לקרוא שוב את האותות שהחזיקה.

אל תריצו `remove_orphan_files` על טבלה שמשוחזרת. הוא מוחק קבצים שהמטא-נתונים הנוכחיים אינם מפנים אליהם, ואלה עשויים להיות ההיסטוריה שאתם משחזרים.

## תלויות מנוע פדרציה {: #federation-engine-dependencies }

מנועי הפדרציה של המחסנים צריכים חבילות Python ורכיבים ברמת המערכת. חבילות ה-Python מוצהרות ב-`pyproject.toml` ומגיעות עם `pip install provisa` או `pip install -e .` הרגילים, כך שאף מנוע מחסן לא צריך תוספת אופציונלית.

את הרכיבים ברמת המערכת (מנהל ה-ODBC, ממשקי ה-CLI של הענן, מפתחות חשבון השירות) צריך להתקין בנפרד.

### חבילות Python (כבר בתלויות הליבה) {: #python-packages-already-in-core-dependencies }

| חבילה | מנוע | מטרה |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | חיבור SQL warehouse; Arrow Cloud Fetch |
| `snowflake-connector-python[pandas]` | Snowflake | חיבור + `fetch_arrow_table` ילידי-Arrow |
| `google-cloud-bigquery` | BigQuery | ביצוע שאילתה |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API עבור קריאות ילידיות-Arrow |
| `google-cloud-storage` | BigQuery | staging‏ GCS עבור קישורי טבלה חיצונית |
| `pyodbc` | Fabric, Synapse | חיבור ODBC לנקודות קצה T-SQL |
| `azure-identity` | Fabric, Synapse | טוקן Azure AD דרך `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | קריאות עמודתיות HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | נעילת תאימות — `google-cloud-*` ו-OTel חולקים runtime protobuf; `<7` שומר עליהם מיושרים |
| `grpcio-status<1.82` | gRPC | מתיישר עם נעילת `protobuf<7` |

### דרישות ברמת-מערכת {: #system-level-requirements }

אלה לא חבילות Python. התקינו אותם על המארח או הקונטיינר שמריץ את Provisa.

**Microsoft Fabric ו-Azure Synapse (ODBC)**

`pyodbc` מתחבר דרך Microsoft ODBC Driver for SQL Server (`msodbcsql18`). התקינו את הדרייבר על המארח עצמו; pip לא יכול להתקין אותו.

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

שני המנועים מאמתים דרך `azure.identity.DefaultAzureCredential`. `DefaultAzureCredential` בודק מקורות אישורים לפי סדר: משתני סביבה, workload identity, managed identity, VS Code, `az login`, ואחרים.

עבור פיתוח מקומי, `az login` הוא הנתיב הפשוט ביותר:

```bash
az login
```

בייצור, השתמשו בזהות מנוהלת (על מכונות וירטואליות של Azure או AKS), שלא דורשת ניהול פרטי התחברות. לאימות באמצעות service principal, הגדירו:

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

בייצור ב-GCP (Cloud Run, GKE עם Workload Identity, Compute Engine), הספרייה מזהה בעצמה את חשבון השירות המצורף, בלי משתנה סביבה.

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

מחבר Databricks יורש זאת מ-`requests`, ולכן הוא לא צריך משתנה סביבה ייעודי ל-Databricks.

### רשימת בדיקה לפי מנוע {: #per-engine-checklist }

**Databricks**

- [ ] `databricks-sql-connector` מותקן (ברירת מחדל)
- [ ] URL מנוע עם `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] טוקן גישה אישי או טוקן service principal
- [ ] `REQUESTS_CA_BUNDLE` מוגדר אם מאחורי proxy מיירט-TLS

**Snowflake**

- [ ] `snowflake-connector-python[pandas]` מותקן (ברירת מחדל)
- [ ] URL מנוע: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` ב-`PROVISA_ENGINE_URL` או `federation_hints`

**BigQuery**

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` מותקנים (ברירת מחדל)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` מוגדר (פיתוח) או workload identity מוגדר (ייצור)
- [ ] `GOOGLE_CLOUD_PROJECT` מוגדר אם לא ניתן להסיק את הפרויקט מה-service account
- [ ] ל-service account יש תפקידי BigQuery Data Viewer + Job User

**Microsoft Fabric**

- [ ] `pyodbc` + `azure-identity` מותקנים (ברירת מחדל)
- [ ] דרייבר מערכת `msodbcsql18` מותקן
- [ ] `FABRIC_SQL_SERVER` ו-`FABRIC_DATABASE` מוגדרים
- [ ] אימות Azure AD: `az login` (פיתוח) או managed identity / service principal (ייצור)
- [ ] `FABRIC_WORKSPACE_ID` מוגדר אם משתמשים בקישורי אחסון-אובייקטים חיצוניים

**Azure Synapse**

- [ ] אותן דרישות Python + מערכת כמו Fabric
- [ ] `SYNAPSE_SQL_SERVER` ו-`SYNAPSE_DATABASE` מוגדרים
- [ ] אותה הגדרת אימות Azure AD כמו Fabric

**ClickHouse**

- [ ] `clickhouse-connect` מותקן (ברירת מחדל)
- [ ] URL מנוע: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` ב-`federation_hints` עבור TLS (פורט 8443)

---

## משתני סביבה {: #environment-variables }

| משתנה | ברירת מחדל | מטרה |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | סיסמת PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | נתיב לקובץ תצורה |
| `PROVISA_REDIRECT_ENABLED` | `false` | הפעל הפניית תוצאות גדולות ל-S3 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | סף ספירת שורות עבור הפניה |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | bucket‏ S3 |
| `PROVISA_REDIRECT_ENDPOINT` | | URL נקודת קצה תואם-S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL‏ URL חתום מראש (שניות) |
| `REDIS_HOST` | `localhost` | host‏ Redis |
| `REDIS_PORT` | `6379` | פורט Redis |
| `REDIS_PASSWORD` | | סיסמת Redis |
| `REDIS_TLS` | `false` | הפעל TLS עבור Redis |
| `TRINO_HOST` | `localhost` | host‏ קואורדינטור מנוע פדרציית Trino |
| `TRINO_PORT` | `8080` | פורט HTTP‏ קואורדינטור מנוע פדרציית Trino |
| `PROVISA_ENGINE` | `duckdb` | מפתח מנוע הפדרציה הפעיל; דורס תצורה מתמידה |
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

## פקודות CLI {: #cli-commands }

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

### זרימת קידום תצורה (dev ← test ← prod) {: #config-promotion-workflow-dev-test-prod }

כל הגדרה שתלויה בסביבה (מחרוזות חיבור, סודות, פורטים) שייכת למשתני סביבה או למנהל סודות, מחוץ לתצורה המיוצאת. ה-YAML המיוצא לוכד את המודל הסמנטי שלכם: מקורות, תחומים, תפקידים ותצוגות.

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
