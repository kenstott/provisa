# קביעת גודל וכוונון {: #sizing-and-tuning }

כלי קביעת הגודל מודד את פריסת Provisa שלכם תחת עומס העבודה שלכם. מהמדידות הוא כותב מדריך לקביעת גודל ולכוונון: כמה ליבות פיזיות ותהליכי worker העומס דורש, ומה כל הגדרה מקנה. כל נתון במדריך מגיע ממדידה, ולכן לאחר שינוי בפריסה או בעומס מריצים את הכלי מחדש ומייצרים את המדריך מחדש.

הכלי נמצא במאגר Provisa תחת `demo/named/perf/bench/`. הריצו כל פקודה להלן מתיקייה זו עם סביבת ה-Python של המאגר (`../../../../.venv/bin/python3`).

## מה הוא מדווח {: #what-it-reports }

ריצת קביעת גודל אחת היא כמה ריצות benchmark מול הפריסה שלכם. כל אחת משנה דבר אחד:

| ריצה | מה משתנה | מה היא נותנת |
|---|---|---|
| קו בסיס | שום דבר: עמודה אחת, שורה אחת, ללא מסנן, ללא צירוף, ללא מטמון | העלות הבסיסית של בקשה |
| ריצות פרמטרים | פרמטר בקשה אחד בכל פעם: שדות, מסננים, שורות, צירופים באותו מקור, צירופים בין מקורות | העלות שכל פרמטר מוסיף ליחידה |
| ריצות צירוף בין מקורות | ערכי הצירוף בין מקורות, פעם עם שני הצדדים נקראים חי, פעם עם שניהם מרפליקות, ופעם עם אחד מכל סוג | העלות של צירוף בין מקורות לפי אופן קריאת צדדיו, ונקודת האיזון בין צדדים חיים לצדדי רפליקה |
| ריצות מטמון | כל בקשה מצטרפת למטמון התגובות, ואז אף אחת | העלות של פגיעת מטמון ושל החטאה |
| ריצות שכפול | מקור הנקרא חי, ואז מרפליקה | הקצב שמעליו רפליקה עולה פחות |
| ריצות workers | תהליכי worker ובקשות בו-זמניות לכל worker | workers לכל ליבה פיזית, בקשות בו-זמניות לכל worker, מה שליבה מספקת |

ריצות פרמטרים, ריצות מטמון וריצות שכפול נערכות לכל ערוץ ולכל מקור, ופעם אחת לכל מנוע federation שתציינו. ריצות workers נערכות לכל ערוץ. [tool-verified: `demo/named/perf/bench/sweeps.py:plan_sweeps,plan_worker_sweep`]

עלות כאן פירושה זמן ה-CPU של Provisa לכל בקשה, במילישניות. ריצות הפרמטרים אינן משתמשות במטמון, ולכן עלות פרמטר לעולם אינה כוללת את השפעת המטמון. [tool-verified: `demo/named/perf/bench/sweeps.py:_baseline` sets `k["cache"] = {"probability": 0.0}`]

מהריצות, `bench_report.py` מתאים מודל עלות לכל מנוע, ערוץ ומקור: עלות בסיס ועוד עלות לכל שדה, מסנן, שורה וצירוף, כשצירוף בין מקורות מתומחר לפי אופן קריאת שני צדדיו. כשהמנוע רץ כשרת נפרד ומודדים את ה-CPU של הקונטיינר שלו, גם למנוע יש מודל עלות. [tool-verified: `demo/named/perf/bench/cost_model.py:fit`] `sizing_guide.py` הופך את הדוח למדריך. בהינתן חוזה המתאר את עומס העבודה שלכם, הוא גם קובע את גודל העומס הזה.

## כתיבת חוזה הגדרה {: #write-a-setup-contract }

חוזה ההגדרה הוא קובץ YAML אחד. הוא מציין את נקודות הקצה של הפריסה שלכם, את הטבלאות שה-benchmark רשאי לקרוא, את צורת הבקשות ואת העומס. ה-benchmark בודק את החוזה מול הפריסה לפני שהוא רץ. הוא דוחה טבלה, עמודה או צירוף שהפריסה אינה חושפת. [tool-verified: `demo/named/perf/bench/setup_contract.py:setup_from_dict`]

טבלאות ועמודות נקראות כפי שהפריסה שלכם רושמת אותן. מה כל ערוץ קורא להן (שמות SQL, שדות GraphQL, תוויות Cypher, נתיבי REST, טיפוסי gRPC) נשלף מהפריסה בתחילת הריצה.

```yaml
version: 1

deployment:
  name: acme-prod
  endpoints:
    http: {base_url: "https://provisa.acme.internal"}
    pgwire: {host: provisa.acme.internal, port: 5439}
    bolt: {host: provisa.acme.internal, port: 7687}
    flight: {host: provisa.acme.internal, port: 8815}
    grpc: {host: provisa.acme.internal, port: 50051}
  # The credential is read from an environment variable; the secret never appears here.
  # kind: password (the variable holds the user's password) or token (a ready bearer token).
  credentials: {mode: env, kind: password, user: bench-user, env: ACME_BENCH_PASSWORD}
  # The roles requests are sent as, with the share of requests each gets.
  roles:
    - {role: analyst, weight: 0.8}
    - {role: regional_manager, weight: 0.2}

sources:
  sales-pg:
    # What this source's reads are measured under: live, or a replica refreshed every ttl_seconds.
    replication: {setting: live, ttl_seconds: 0}
    # Optional: a Docker container whose CPU is sampled as the source's CPU.
    monitor: {container: sales-postgres}
    tables:
      - schema: public
        table: orders
        weight: 1.0              # share of this source's requests that start at this table
        base_column: order_id    # the one column a baseline request selects
        columns:
          # A column with a domain can be filtered on; the filter's value is drawn from it.
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
          - {name: region, domain: {kind: set, values: [NA, EMEA, APAC]}}
          - {name: amount}
      - schema: public
        table: order_lines
        weight: 0.0              # reached only through the join below
        base_column: line_id
        columns:
          - {name: line_id}
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
    # Joins within the source, by registered table and column. The relationship must be registered.
    joins:
      - {left: "orders.order_id", right: "order_lines.order_id"}

# Joins across sources: "source:table.column".
cross_source_joins: []

# The rows a request returns when the rows knob does not apply.
base: {rows: 1}

# Each knob is the probability it applies to a request and the distribution its value is drawn
# from when it does. Distributions: constant {value}, uniform {min, max}, zipf {exponent, max},
# geometric {p, max}.
knobs:
  fields: {probability: 1, distribution: {kind: uniform, min: 1, max: 3}}
  filters: {probability: 0.5, distribution: {kind: constant, value: 1}}
  rows: {probability: 1, distribution: {kind: zipf, exponent: 1.2, max: 100}}
  joins_same_source: {probability: 0.2, distribution: {kind: constant, value: 1}}
  joins_cross_source: {probability: 0, distribution: {kind: constant, value: 0}}
  # How often a request repeats one sent shortly before it: the value is how many requests back.
  cache_repetition: {probability: 0.3, distribution: {kind: uniform, min: 1, max: 50}}
  # Share of requests that opt into the response cache. REST and JSON:API have no opt-in.
  cache: {probability: 0.6}
  # How a request's cross-source joins are chosen by how their two ends are read: both_live,
  # both_replica, or mixed (one of each), as the sources' replication settings above make them.
  # Probability 0 chooses joins whatever their class.
  join_replication: {probability: 0, weights: {both_live: 1.0}}
  # Share of requests whose table is in each source.
  source_weights: {sales-pg: 1.0}
  # auto lets the deployment choose the route; leave every source on auto to size a workload.
  route:
    sales-pg: {mode: auto, federated_probability: 0}

# The transports the benchmark runs, one after another. A transport's entry overrides any of the
# knobs above for that transport only; {} means no override.
transports:
  graphql: {}
  pgwire:
    knobs:
      rows: {probability: 1, distribution: {kind: uniform, min: 100, max: 1000}}

load:
  mode: closed_loop
  steps: [1, 16, 64, 256]   # concurrent connections, one step after another
  window_s: 30              # seconds per step
  processes: half_cores     # load-generator processes: an integer, or half the client's cores
  profile_requests: 0
  idle_window_s: 0          # seconds of no load after the ramp; see the replication runs below

seed: 0

# The workload to size: its request rate and each transport's share of it. The knobs above
# describe its requests. A benchmark run does not read this section.
workload:
  rate_per_s: 1500
  transport_weights: {graphql: 0.7, pgwire: 0.3}
```

פורמט החוזה מוגדר ב-`setup_contract.py` וב-`contract_parse.py`; חוזה לא תקין נדחה בהודעה שמציינת את השדה. [tool-verified: `demo/named/perf/bench/contract_parse.py`, `SetupError`] משקלי ה-`workload` חייבים להסתכם ב-1, וכל ערוץ שהם מציינים חייב להיות ערוץ שהחוזה מריץ. [tool-verified: `demo/named/perf/bench/contract_parse.py:_workload`]

תארו את הבקשות כפי שהן באמת. המדריך קובע גודל בדיוק לבקשות שהחוזה הזה מייצר: קביעת הגודל שואבת אותן מאותו מחולל בעל זרע שריצה משתמשת בו. [tool-verified: `demo/named/perf/bench/sizing.py:_transport` uses `request_mix.RequestGenerator`]

## הרצה {: #run-it }

### 1. פתרון השמות פעם אחת {: #1-resolve-the-names-once }

הריצו את חוזה קו הבסיס פעם אחת. הריצה שואלת את הפריסה מה כל ערוץ קורא לטבלאות שלכם וכותבת את `resolved-names.json` לצד התוצאות שלה. [tool-verified: `demo/named/perf/bench/optimistic_load.py:resolve_names`]

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` מציין את מנוע ה-federation שהפריסה שלכם מריצה (`duckdb`, `pg` או `trino`). `--server-pid` הוא מזהה התהליך של השרת; הריצה קוראת את זמן ה-CPU של התהליך הזה ושל תהליכי הבן שלו (ה-workers). היא קוראת את התהליך ישירות, ולכן עובדת רק כשמחולל העומס רץ על מארח השרת. [tool-verified: `demo/named/perf/bench/optimistic_load.py:_server_cpu_seconds`]

### מדידת ה-CPU של השרת ממארח אחר {: #measuring-the-servers-cpu-from-another-host }

הריצו את מחולל העומס על מכונה נפרדת כשאפשר: כך הוא אינו גוזל ליבות מהשרת. כדי למדוד משם את ה-CPU של השרת, הפעילו את `server_cpu_agent.py` על מארח השרת וספקו לריצה את הכתובת שלו במקום `--server-pid`:

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

הסוכן עונה על נתון אחד, אותו נתון ש-`--server-pid` קורא: `GET /` מחזיר `{"pid": ..., "cpu_seconds": ...}`, שניות ה-CPU המצטברות של תהליך השרת וה-workers שלו. כל נתיב אחר עונה 404, וכל שיטה אחרת נדחית. [tool-verified: `demo/named/perf/bench/server_cpu_agent.py:handler`] ספקו לריצה אחד מבין `--server-pid` ו-`--server-cpu-url`, לעולם לא את שניהם. [tool-verified: `demo/named/perf/bench/optimistic_load.py:server_cpu_source`]

`--listen` נדרש: לסוכן אין כתובת ברירת מחדל. קשרו אותו לכתובת פרטית שרק מחולל העומס יכול להגיע אליה, לעולם לא לממשק ציבורי, ועצרו אותו (Ctrl+C) כשהריצות הסתיימו.

### 2. תכנון הריצות {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`: הליבות הפיזיות של השרת, לא תהליכוני החומרה שלו. ב-Linux, `lscpu` מציג ליבות לכל סוקט וסוקטים; הכפילו אותם. הכלי לעולם אינו מנחש זאת.
- `--shared-host`: `yes` כשהמקורות שלכם או מחולל העומס רצים על מארח השרת. אז הם מתחרים ב-Provisa על הליבות שלו, והמדריך מציין זאת.
- `--workers` ו-`--request-threads`: תהליכי ה-worker והבקשות הבו-זמניות לכל worker שיש לנסות. ספקו את שניהם או אף אחד.
- `--replication` ו-`--ttl`: מוסיפים את ריצות החי והרפליקה לכל מקור, כשהרפליקה מתרעננת כל `--ttl` שניות. גם ריצות הצירוף בין מקורות הקוראות מרפליקות צריכות `--ttl`.
- `--engines`: מנועי ה-federation למדידה, `pg` כברירת מחדל. כל ריצת פרמטר מתוכננת פעם אחת לכל מנוע; המנוע נקבע כשהשרת עולה, ולכן ריצות כל מנוע דורשות הפעלה מחדש של השרת איתו.
- `--engine-container`: `ENGINE=CONTAINER` למנוע שרץ כשרת בקונטיינר משלו, שה-CPU שלו מדווח בכל ריצה. השמיטו מנוע שרץ בתוך Provisa (DuckDB).

התכנון כותב חוזה אחד לכל ריצה וקובץ `manifest.json`. ריצה שהפריסה אינה יכולה לשרת, כמו צירופים מעל gRPC או טבלה שערוץ אינו חושף, מופיעה ב-manifest עם הסיבה. היא לעולם אינה נושרת בשקט. [tool-verified: `demo/named/perf/bench/sweeps.py:_validate`, `write_plan`]

ריצות הרפליקה מודדות את עלות הרענון של הרפליקה בחלון ללא עומס. הגדירו `load.idle_window_s` בחוזה שלכם (למשל `120`) לפני תכנונן. בלעדיו הדוח דוחה את נקודת האיזון עם `no idle.json`. [tool-verified: `demo/named/perf/bench/bench_report.py:_break_even`]

### 3. הרצת כל חוזה מתוכנן {: #3-run-every-planned-contract }

כל רשומה ב-manifest מציינת את קובץ החוזה שלה. הריצו כל אחד אל `results/sizing/results/<run name>`:

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

ממארח אחר, השתמשו ב-`--server-cpu-url` במקום `--server-pid`. ריצת worker מפעילה את השרת מחדש, ולכן מזהה התהליך שלו משתנה: הפעילו מחדש גם את הסוכן עם ה-`--pid` החדש.

הוסיפו `--apply-replication` לכל ריצה שברשומת ה-manifest שלה יש `"apply_replication": true` (ריצות השכפול וריצות הצירוף בין מקורות). הוא קובע את השכפול שחוזה הריצה מצהיר עליו ומשחזר אותו בסיום. [tool-verified: `demo/named/perf/bench/optimistic_load.py:add_arguments`]

כל ריצה נושאת `server_env` ב-manifest: השרת חייב לרוץ איתו. זה של ריצת פרמטר הוא המנוע שלה, `{"PROVISA_ENGINE": "duckdb"}`; הריצו את כל ריצות מנוע אחד, ואז הפעילו מחדש את השרת עם הבא. זה של ריצת worker מוסיף את ה-workers שלה וחסם הבקשות הבו-זמניות, למשל `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`: `PROVISA_WORKERS` הוא מספר תהליכי ה-worker ו-`PROVISA_REQUEST_THREADS` קובע את `concurrency.request_threads`, הבקשות הבו-זמניות שכל worker מריץ בבת אחת. [tool-verified: `provisa/core/settings_catalog.py` `concurrency.request_threads`, `env="PROVISA_REQUEST_THREADS"`] המתינו עד שכל worker משרת (`/health` מדווח `workers.ready` מתוך `workers.expected`) לפני תחילת הריצה.

### 4. בניית הדוח {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (ברירת מחדל `0.05`) קובע עד כמה קרוב לתפוקה הגבוהה ביותר שנמדדה צריכה הגדרת worker להגיע כדי להיחשב מספיקה. [tool-verified: `demo/named/perf/bench/bench_report.py:DEFAULT_TOLERANCE`]

### 5. יצירת המדריך {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

בלי `--setup` למדריך יש רק את סעיפי הכוונון. איתו, המדריך קובע את גודל ה-`workload` של החוזה עם המנוע ש-`--engine` מציין (`pg` כברירת מחדל); מנוע שהריצות לא מדדו נדחה.

## קריאת המדריך {: #read-the-guide }

### קביעת גודל לעומס זה {: #sizing-this-workload }

המדריך מסיים סעיף זה בשורה אחת, למשל: **12 ליבות פיזיות, 24 תהליכי worker, 2 בקשות בו-זמניות לכל worker.** מעליה, טבלה מציגה כל ערוץ ומקור: חלק הבקשות, הממוצע של שדות, מסננים, שורות וצירופים, חלק פגיעות המטמון, וה-CPU שבקשה אחת עולה.

בקשה עולה את מודל העלות בממוצעי הפרמטרים שלה בהחטאה ואת עלות הפגיעה הנמדדת בפגיעה. ה-CPU של Provisa לשנייה של ערוץ, מחולק במה שליבה פיזית אחת מספקת, נותן את הליבות הפיזיות שלו. המדריך מחבר אותן על פני הערוצים ומעגל כלפי מעלה. ה-workers הם הליבות כפול מספר ה-workers לליבה הגדול ביותר שערוץ כלשהו דורש. הבקשות הבו-זמניות לכל worker הן באותו אופן הגדול ביותר שכל אחד מהם דורש. הגדרה אחת משרתת את כולם. [tool-verified: `demo/named/perf/bench/sizing.py:size`]

הכלי אינו קובע גודל למה שלא מדד. חלק מהעומס שאין לו מודל בריצות עוצר את קביעת הגודל בהודעה שמציינת אותו [tool-verified: `demo/named/perf/bench/sizing.py:SizingRefused`]:

- מנוע שהריצות לא מדדו
- ערוץ ללא ריצות workers
- מקור ללא מודל עלות בערוץ זה
- פרמטר שהריצות לא שינו (סרקו אותו, ואז הריצו שוב)
- רמז ניתוב (`direct` או `federated`) על מקור
- הגדרת שכפול או TTL שונים מאלה שנמדדו
- פגיעות מטמון כשלריצות אין נתוני מטמון

### תהליכי worker ובקשות בו-זמניות {: #worker-processes-and-in-flight-requests }

לכל ערוץ, המדריך מציג את מספר ה-workers הקטן ביותר שהתפוקה השיאית שלו בטווח הסובלנות מהטובה ביותר. במספר workers זה הוא בוחר את מספר הבקשות הבו-זמניות הקטן ביותר לכל worker שעושה זאת. הוא מציג הגדרה זו בשתי צורות: workers לכל ליבה פיזית ובקשות בו-זמניות לכל worker. עוד מכל אחד מהם אינו מוסיף תפוקה. פחות משאיר ליבות בטלות. הטבלה נותנת גם את שיא הבקשות לשנייה ואת זמן ההשהיה p99 שלו. כל נקודה שנמדדה מופיעה מתחתיה.

גורם הקיבולת הוא הבקשות לשנייה שליבה פיזית אחת מספקת, כפול ה-CPU שבקשה עולה לבדה, חלקי 1000. מעל 1 הליבה עושה יותר מבקשות של תהליכון אחד, כפי שקורה כששני תהליכוני החומרה שלה משרתים. [tool-verified: `demo/named/perf/bench/bench_report.py:_server`]

### מה בקשה עולה {: #what-a-request-costs }

לכל מנוע, ערוץ ומקור, זה לצד זה: העלות הבסיסית של בקשה ללא מטמון, העלות שכל שדה, מסנן, שורה וצירוף מוסיפים, ומידת ההתאמה של המודל למדידות (R², כש-1 הוא התאמה מושלמת). לצירוף בין מקורות שלוש עלויות, לפי אופן קריאת שני צדדיו: שניהם חי, שניהם מרפליקות, או אחד מכל סוג. מקף פירושו שהריצות לא שינו את הפרמטר. כשהמנוע רץ כשרת נפרד, טבלה שנייה נותנת באותו אופן את ה-CPU של המנוע: בקשה שהמנוע משרת, כמו צירוף בין מקורות, מציגה שם את זמן המנוע שלה. השתמשו בנתונים אלה כדי לראות מה שינוי בבקשות שלכם יעלה לפני שאתם מבצעים אותו: שני שדות נוספים בכל בקשה מוסיפים פעמיים את עלות השדה.

### הצטרפות למטמון התגובות {: #the-response-cache-opt-in }

לכל ערוץ ומקור: ה-CPU של החטאה, של פגיעה, ומה שפגיעה חוסכת. בקשה נענית מהמטמון רק כשהיא מצטרפת וחוזרת על בקשה קודמת, ולכן החיסכון חל על בקשות חוזרות. הצרפו את החוזרות. [tool-verified: `demo/named/perf/bench/bench_report.py:_cache`]

### קריאות חי או מרפליקה, וה-TTL {: #live-or-replica-reads-and-the-ttl }

לכל מנוע, ערוץ ומקור עם ריצות שכפול: עלות קריאה חיה, עלות קריאה מרפליקה, ה-CPU של רענון אחד, ה-TTL ותעריף נקודת האיזון. מעל תעריף נקודת האיזון רפליקה עולה פחות CPU, של Provisa, של המקור ושל המנוע יחד, מקריאה חיה. מתחתיו, קראו חי. טבלה שנייה נותנת את אותו הדבר לצירופים בין מקורות, לפי מספר הצירופים בבקשה: שני הצדדים נקראים חי מול שני הצדדים מרפליקות, כשהרענון סופר כל מקור שהמעבר העביר לרפליקה. [tool-verified: `demo/named/perf/bench/cost_model.py:break_even`]

נקודת האיזון היא ה-CPU של הרענון לשנייה חלקי מה שקריאה אחת חוסכת:

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

הכפלת ה-TTL מחצה את תעריף נקודת האיזון, והנתונים יכולים להיות ישנים עד פי שניים. בחרו את ה-TTL לפי מידת הישנות שהמשתמשים שלכם מקבלים, ואז השוו את קצב הבקשות שלכם במקור זה עם נקודת האיזון ב-TTL זה.

עלות קריאה מרפליקה תלויה במאגר ובאינדקסים שלו, הנבנים זמן קצר אחרי כל טעינה. מדדו אחרי שהאינדקסים במקומם, והריצו שוב אחרי החלפת מאגר. [Replicas](replicas.md) מכסה את בחירת המאגר ואת אסטרטגיות העדכניות.
