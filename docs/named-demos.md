# How to create a named demo

A named demo is a self-contained, reusable demo environment you drop into `demo/named/<name>/`.
Once the directory exists, `start-ui-install.sh --demo <name>` picks it up with no change to
the launcher script and no developer involvement required. This is the mechanism for
client-specific and vertical demo scenarios: a future `--demo banking` or `--demo healthcare`
is just a new directory in this shape.

Named demos are distinct from the `--source=<name>` mechanism. The `--source` flag adds a single
toy source to the standard demo (backed by `demo/sources/<name>/`). A named demo is a
self-contained multi-source package: its own config.yaml (a whole config), its own
docker-compose.yml when it has data stores, and whatever seeding tooling the scenario needs.

## The fixed directory layout

Every named demo lives at `demo/named/<name>/` and contains these files:

```
demo/named/<name>/
  demo.yaml             # required for published demos — bundle metadata and page card
  config.yaml           # required — the demo's whole config, used in place of the standard demo's
  secrets.yaml          # optional — the secrets config.yaml names as ${secret:NAME}, with their values
  docker-compose.yml    # the demo's own data stores at build and authoring time (omit if none)
  docker-compose.run.yml  # the run-time stores, loaded from the bundle's dumps (omit if none)
  setup.yaml            # optional — commands and MV refreshes to run once after the server is up
  tour.yaml             # the demo tour, one step per object it shows
  queries/              # named example queries the tour or Polly can reference
  notebook.ipynb        # optional — a notebook the Notebook tab offers and a notebook step runs
  Dockerfile.seeder     # if the demo self-seeds (recommended)
  seed.py               # entrypoint for the seeder service
  generate_*.py         # one per data store, optional but conventional
  logs/                 # optional — log files the containers write, for the demo's Logs sources
```

`start-ui-install.sh` picks the demo's config generically, from `demo/named/$DEMO_NAME/`
:

```bash
# start-ui-install.sh, after the directory check
_NAMED_WHOLE=false
if [ -n "$DEMO_NAME" ]; then
  _NC="$SCRIPT_DIR/demo/named/$DEMO_NAME/config.yaml"
  _NF="$SCRIPT_DIR/demo/named/$DEMO_NAME/fragment.yaml"
  if [ -f "$_NC" ] && [ -f "$_NF" ]; then
    echo "--demo $DEMO_NAME has both config.yaml and fragment.yaml in demo/named/$DEMO_NAME/. Keep one: ..."; exit 1
  fi
  if [ ! -f "$_NC" ] && [ ! -f "$_NF" ]; then
    echo "--demo $DEMO_NAME has no config.yaml (and no fragment.yaml) in demo/named/$DEMO_NAME/ to register it with."; exit 1
  fi
  [ -f "$_NC" ] && _NAMED_WHOLE=true
fi
```

`config.yaml` is a **whole config**: the launcher sets `PROVISA_CONFIG` to it in place of the
standard demo config (`config/provisa-install.yaml`), so the demo is exactly what the file lists.
A `--source` toy source is still overlaid on top. A demo may instead ship a `fragment.yaml`,
overlaid on the standard config; a demo with neither, or with both, is refused by name. The name
itself is not in any registry or allowlist.

## The one-start-point invariant

`docker compose up -d` against `docker-compose.yml` is the complete setup-and-seed story. No
manual commands. No host-side scripts to run before it works.

This does **not** mean the demo must be a single process or single container. The `perf` demo
runs five containers (four engines plus a seeder). A demo may also reference
external managed resources it does not containerize: a live Databricks or Snowflake source
reached by connection string, registered in `config.yaml` with no corresponding compose
service. Those are implementation details behind the single entry point.

What the invariant prohibits: telling the solutions engineer to run scripts by hand, set up
databases manually, or issue commands in a specific order. One command, fully automated.

## The seeder pattern

Heavy data generation must not re-run on every container restart. The `perf` demo uses a
`seeder` service that writes a marker file on a bind-mounted directory:

```python
# demo/named/perf/seed.py:29,42-43
MARKER = Path("/data-marker/.seeded")

def main() -> int:
    if MARKER.exists():
        print(f"perf demo already seeded ({MARKER.read_text().strip()}); skipping")
        return 0
```

When the seeder container starts, it checks for the marker. If present, it exits immediately. If
absent, it runs all generation scripts in sequence, then writes the marker. Because the marker
lives on the bind-mounted `data/` directory, it survives container restarts but not `data/`
being deleted. A start without `--keep-data` deletes it, so the marker only matters to a demo started
with `--keep-data`.

The seeder service in `docker-compose.yml` gates on every other service's healthcheck
:

```yaml
# demo/named/perf/docker-compose.yml:129-137
depends_on:
  postgresql:
    condition: service_healthy
  mongodb:
    condition: service_healthy
  clickhouse:
    condition: service_healthy
  neo4j:
    condition: service_healthy
```

For lighter demos — a few hundred rows, fast to regenerate — skipping the marker and letting
the seeder re-run every start is fine. The marker pattern is only necessary when generation
takes minutes or writes gigabytes.

## Volumes: named for databases, the demo directory for files

A database's storage is a named volume of the compose project (`weblogs_pg`, `sonnets_pg`,
`sonnets_qdrant`). `docker compose down -v` removes it, so a test that runs the demo's compose
file under its own project starts from an empty database every run and leaves nothing in the
checkout.

What the demo's own tooling reads or writes as files -- the seeder's marker, log files a Logs
source reads -- goes under `${PROVISA_NAMED_DEMO_DIR:-.}`: the demo's directory when
`start-ui-install.sh` runs it, a temporary directory when a test sets the variable
:

```yaml
# demo/named/weblogs/docker-compose.yml (excerpt)
    volumes:
      - weblogs_pg:/var/lib/postgresql/data
      - ${PROVISA_NAMED_DEMO_DIR:-.}/logs/postgres:/var/log/pglog
...
  seeder:
    volumes:
      - ${PROVISA_NAMED_DEMO_DIR:-.}/data:/data-marker
```

`demo/named/*/data/` is ignored by git. The `perf` demo predates this and binds its stores to
`./data/`.

## Container labels

Every service in `docker-compose.yml` carries two labels:

```yaml
# demo/named/perf/docker-compose.yml:36-37, 43-44
x-demo-labels: &demo-labels
  com.provisa.demo: perf   # replace with your demo name
services:
  postgresql:
    labels:
      <<: *demo-labels
      com.provisa.demo.role: rdb   # role: rdb / dw / doc-store / graph / seeder / etc.
```

`com.provisa.demo` groups every container the demo starts so
`docker ps --filter label=com.provisa.demo=<name>` lists exactly those containers without
needing the compose file open. `com.provisa.demo.role` identifies each service's part.

The role value determines what the bundle build does with each store:

- `rdb` — a Postgres service: each of its databases dumped to `dump/<service>/<database>.sql` at
  build, with `dump/load-postgres.sh`, which loads them into the demo's own container.
- `vector` — a Qdrant service: each collection saved as `dump/<service>/<collection>.snapshot` at build.
- `document` — a MongoDB service: every database dumped as one gzipped archive,
  `dump/<service>.archive.gz`, read as the administrator its `MONGO_INITDB_ROOT_USERNAME` and
  `MONGO_INITDB_ROOT_PASSWORD` name.
- `search` — an Elasticsearch service: its indices snapshotted to the repository its `path.repo`
  names, copied out to `dump/<service>/`.
- `graph` — a Neo4j service: stopped, and its `neo4j` database dumped to
  `dump/<service>/neo4j.dump`. It is dumped after every other store.
- `seeder` — run once during the build (and during development starts); never in a bundle.
- `client` — a client of the demo's own that reads the Provisa a start runs on (the provisioning
  demo's `consumer`). The build does not start it: it runs a client a `client:` step of
  `setup.yaml` names against its own setup server instead.

## config.yaml — the demo's whole config

`config.yaml` is a static, checked-in config file in Provisa's standard YAML config format. Every
start rebuilds the control plane from it. The usual way to produce it is to run the standard demo,
build the scenario in the UI, and export the model (Admin → Maintenance → Configuration File →
View / Diff, the **Current** pane, or `GET /admin/config/live`), then save it as `config.yaml`.
The export omits sources added in the UI, so add them to `sources:` by hand. The excerpts below
come from `demo/named/perf/config.yaml`, which is the standard demo config with the perf sections
appended.

### Every source needs explicit `tables:`

Register each source and then list every table it exposes. A bare `sources:` entry never
produces tables in the SQL catalog, for any connector type, because the catalog is built from the
registered tables:

```yaml
# demo/named/perf/config.yaml, the bench-postgresql source
- id: bench-postgresql
  type: postgresql
  host: ${env:PROVISA_BENCH_POSTGRESQL_HOST:-localhost}
  port: ${env:PROVISA_BENCH_POSTGRESQL_PORT:-25632}
  database: provisa_bench
  username: provisa
  password: provisa
```

Use env-var interpolation with a `:-` default for every host and port. The seeder uses the
in-container service name as the host; the host-side generate scripts use `localhost` plus the
published port. The config uses the same env-var names so both work without editing the file
:

```python
HOST = os.environ.get("PROVISA_BENCH_POSTGRESQL_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BENCH_POSTGRESQL_PORT", "25632"))
```

### Sources backed by api_source (each table is a query)

Neo4j is an `api_source`-backed type where each table is one fixed Cypher projection. You
hand-author every table in `config.yaml`:

```yaml
# demo/named/perf/config.yaml (excerpt)
tables:
- source_id: bench-neo4j
  domain_id: perf-bench
  schema: neo4j
  table: bench_order_node
  description: One row per Order node
  query_template: >-
    MATCH (o:Order)
    RETURN o.order_id AS order_id, o.customer_id AS customer_id, ...
    ORDER BY o.order_id
  columns:
  - {name: order_id, data_type: integer, is_primary_key: true, visible_to: [org_admin, analyst]}
  - {name: customer_id, data_type: integer, visible_to: [org_admin, analyst]}
  ...
```

`query_template` is a Cypher query whose result set becomes the table. Write it from the actual
schema your generation scripts produce, not from an assumed schema. Each column entry matches
Provisa's `Column` model shape: `name`, `data_type`, optional `is_primary_key`, and
`visible_to` (list of role names).

### Relationships

Cross-table relationships go in a top-level `relationships:` block:

```yaml
# demo/named/perf/config.yaml (excerpt)
relationships:
- id: bench-placed-to-customer
  source_table_id: bench_placed_edge
  source_column: customer_id
  target_table_id: bench_customer_node
  target_column: customer_id
  cardinality: many-to-one
```

Relationship IDs must be unique within the file. Table IDs referenced here come from the
`table:` field in the `tables:` block above, or from auto-discovered tables for native
connectors (use the actual table name as the ID for those).

### Code sources: the code ships in the demo

A `python` or `node` source runs code the demo ships. Put the package's archive in `code/` beside
`config.yaml` (`code/<name>.tar.gz`, `.tgz` or `.zip`) and name it in the source's mapping by the
content hash of the code it unpacks to:

```yaml
sources:
- id: churn-model
  type: python
  mapping:
    runtime: python
    code: {kind: upload, upload: <content hash>}
```

When the demo's template is built, each archive in `code/` becomes the org's upload under that
hash, and every sandbox of the demo is given it. A Python archive carries its `pyproject.toml` and
`uv.lock` at its root, so its dependencies install from wheels pinned there; nothing is fetched to
find the code itself.

## demo.yaml — bundle metadata and page card

`demo.yaml` is required for any demo that will be published as a bundle. It holds three kinds of
fields:

```yaml
# demo/named/weblogs/demo.yaml
title: Web shop logs
description: >-
  A web shop's nginx access log and its database's statement log, read as tables and joined to
  the shop's own data...
screenshot: card.png
published: true
requires_provisa: 0.1.0a483
```

| Field | Required | Meaning |
|---|---|---|
| `published` | yes | `true` publishes this demo as a bundle; `false` skips it in the workflow |
| `requires_provisa` | yes | The oldest Provisa release the bundle runs on; the build refuses a missing value |
| `title` | for published | The demo's name on the live demos page |
| `description` | for published | The card text on the live demos page |
| `screenshot` | for published | `card.png`, a PNG in the same directory |
| `extras` | optional | Python extras to install before the bundle build (e.g. `[vector]` for sentence-transformers) |
| `order` | optional | A positive integer: demos with one are listed first on the live demos page and by `--list-demos`, lowest first; the rest follow by name |
| `tour` | optional | `core`: the demo is toured by the product's core tour and its Deep Dives, and has no `tour.yaml` of its own |

The `extras` field installs additional Provisa optional dependencies before the demo's setup
runs. The `sonnets` demo uses `[vector]` to install the sentence-transformers package needed to
embed its lines.

## secrets.yaml — the secrets the config names

A demo whose config reads a credential names it `${secret:NAME}` and declares the value in
`secrets.yaml`, beside `config.yaml`:

```yaml
# demo/named/testdata/secrets.yaml
secrets:
  SHOP_DB_PASSWORD: provisa_reader
```

Each org built from the demo gets these secrets in its own vault before its config loads: the
deployment org started on the demo, the demo's template org on the sandbox tier, and every
sandbox opened from that template. A later start writes them again, so a changed value replaces
the stored one. The file travels in the demo's bundle with the rest of its directory. Every
`${secret:NAME}` in a demo's `config.yaml` must be declared here; a unit test checks each demo.

## setup.yaml — run once, on first start and at bundle build

`setup.yaml` names what the demo needs run after the server is up. The launcher runs it on a
demo's first start (a new control plane). The bundle build runs it before dumping the stores, so
the setup's effects are in the dumps from the start.

The file is a list of steps, each one `command`, `refresh`, `profile`, `environment` or `client`
:

```yaml
# demo/named/sonnets/setup.yaml
steps:
- command: embed_lines      # calls the embed_lines command as org_admin
- command: link_echoes      # calls the link_echoes command as org_admin
- refresh: theme_time       # builds the stored rows of the view_theme_time materialized view
- refresh: theme_beauty
- refresh: theme_mortality
- refresh: theme_jealousy
- refresh: theme_verse
```

A `command:` step calls a GraphQL mutation of that name (as `org_admin`, no arguments). A
`refresh:` step calls `refreshMv` on the named view. An `environment:` step names an environment
and its data mode, and creates it in the active org through `POST /admin/orgs/{org}/environments`,
as the Environments page does:

```yaml
# demo/named/testdata/setup.yaml
steps:
- environment: {name: dev, data_mode: test_fake}
- environment: {name: test, data_mode: test_fake}
```

A `client:` step runs a script of the demo's own directory until it exits. It is given the
server's addresses as `PROVISA_URL` and `PROVISA_FLIGHT`, beside the stores' addresses the run
has (the launcher's, or the ports the bundle build ran the stores on). A client that exits non-zero
fails its step. The provisioning demo uses one to record a notice and a pull for each team at its
bundle build, so a hosted sandbox shows them:

```yaml
# demo/named/provisioning/setup.yaml
steps:
- client: setup_pulls.py
```

The launcher passes the server's Flight address (`--flight grpc://localhost:${FLIGHT_PORT:-8815}`),
and the bundle build gives its setup server a Flight port of its own.

The first step that fails stops the run and names the step. Nothing is retried.

The launcher marks a demo set up by adding its name to the operator setting
`demo.setup_completed`. A demo already listed there is not set up again against the same control
plane.

A bundled demo's `command`, `refresh` and `client` steps do not run again where it runs: what they wrote is
already in the bundle's dumps. Its `environment` steps do run there (`--environments-only`),
because an environment is a branch of the model in the control plane, and every start builds the
control plane anew.

## docker-compose.run.yml — the run-time stores

A bundle does not run `docker-compose.yml` (the authoring compose, with the seeder). It runs
`docker-compose.run.yml`, which brings up only what reading the bundle needs: the stores loaded
from the build's dumps.

Every service in `docker-compose.run.yml` carries the standard labels plus the ones the node's
`scripts/demo_stores.py` reads to build `stores.env`:

```yaml
# demo/named/weblogs/docker-compose.run.yml (excerpt)
services:
  postgres:
    labels:
      com.provisa.demo: weblogs
      com.provisa.demo.role: rdb
      com.provisa.demo.host_env: WEBLOGS_PG_HOST    # the env var config.yaml reads for the host
      com.provisa.demo.port_env: WEBLOGS_PG_PORT    # the env var config.yaml reads for the port
      com.provisa.demo.port: "5432"                 # the container port (not the published port)
      com.provisa.demo.ns_env: WEBLOGS_PG_NS        # the suffix of every name of its data
      com.provisa.demo.access: db/reader.sql        # its roles and grants
    ports:
      # On the loopback only. ${VAR-default}: unset → default port; set empty → Docker picks one.
      - "127.0.0.1:${WEBLOGS_PG_PORT-28432}:5432"
    volumes:
      - weblogs_run_pg:/var/lib/postgresql/data
      - ./dump/postgres:/dumps:ro
      - ./dump/load-postgres.sh:/docker-entrypoint-initdb.d/10-load.sh:ro
```

Five things to get right:

- **Ports on the loopback.** Every port binding is `127.0.0.1:<var>:<container port>`. The node
  runs several versions of the same demo at once; each gets its own port, picked by Docker when
  `${VAR}` is set empty.
- **`${VAR-default}` syntax.** The single dash (`-`, not `:-`) means "use the default when unset
  or empty". The node sets the variable empty to let Docker pick a port; the default applies for
  a developer's local start.
- **The three `host_env`/`port_env`/`port` labels.** The node's `scripts/demo_stores_sync.sh`
  reads them to build `stores.env`, the file that tells the tier where each store is. Without
  them, the node cannot connect the running store to the config's env-var references.
- **The `ns_env` label, and the names it suffixes.** Every name `config.yaml` gives a store's data
  ends with the variable its `ns_env` label names, with no default:
  `database: shop${env:WEBLOGS_PG_NS}`. A Mongo table's `schema` (its database), an
  Elasticsearch index or Qdrant collection used as a table (`table: reviews${env:REVIEWS_ES_NS}`,
  with `alias: reviews` keeping the name a query uses), an index or collection inside a
  `query_template`, and a Kafka `topic` all take it. Where the demo runs its own stores
  (`start-ui-install.sh --demo`, the bundle build, a test) each variable is set empty; on the
  sandbox tier's node it holds the version's suffix. A Postgres or MongoDB store also names its
  access script in `access`: its roles and grants, run in each database the node loads.
- **Read-only roles.** Give each store a reader credential and use it in `config.yaml`. The store
  is shared by every sandbox of its version, so Provisa should never write to it. The sonnets
  demo's Qdrant store grants Provisa only its read-only key
.

The reviews demo's run-time compose file loads its three stores the first time each starts
:

- MongoDB restores the archive from an init script (`db/mongo-restore.sh` in
  `/docker-entrypoint-initdb.d/`); Provisa reads as `reviews_reader`, whose role reads every database and writes none.
- Elasticsearch starts through `db/es-restore.sh`, which registers the snapshot repository read
  only at `/snapshots`, restores it, and blocks writes to every index.
- Neo4j starts through `db/neo4j-load.sh`, which loads the dump when the data volume holds no
  database yet; `NEO4J_dbms_databases_default__to__read__only` makes every database read only.

A Qdrant store's run-time compose file recovers its collections from the build's snapshots at
start:

```yaml
# demo/named/sonnets/docker-compose.run.yml (excerpt)
  qdrant:
    image: qdrant/qdrant:v1.19.1
    command: ["./qdrant", "--snapshot", "/snapshots/sonnet_lines.snapshot:sonnet_lines"]
    volumes:
      - sonnets_run_qdrant:/qdrant/storage
      - ./dump/qdrant:/snapshots:ro
```

### On the sandbox tier's node: shared stores, started on demand

The node runs no demo's stores at boot. `scripts/demo_stores_sync.sh` pulls each bundle and marks
it offered (`<bundles>/<name>/pulled`); the tier asks for a demo at its first open
(`<bundles>/<name>/requested`), and only then does the sync bring its stores up and mark the
version ready. At the node's start, `provisa-demo-stores-boot.service` clears every request and
ready mark before docker starts, and no store container restarts with docker.

A demo's Postgres, MongoDB, Elasticsearch and Qdrant data, and its Kafka topics, live in the
node's one instance of each type (`packaging/linux/demo-stores/docker-compose.shared.yml`),
started the first time any demo needs it and left up until the node stops. Each demo version's
data has names of its own, the version's suffix `_<demo>_<8 hex of its version>` appended:

| Store | Loaded as |
|---|---|
| Postgres | each `dump/<service>/<database>.sql` into the database `<database><suffix>`; the `access` script's roles created once, its grants run in each database |
| MongoDB | the archive restored with every database renamed `<database><suffix>`; the `access` script's user created, or granted this version's databases |
| Elasticsearch | the snapshot restored with every index renamed `<index><suffix>`, writes blocked |
| Qdrant | each `dump/<service>/<collection>.snapshot` uploaded as `<collection><suffix>`; the version reads with a token scoped to its collections, read only, written to the variable its `com.provisa.demo.key_env` label names |
| Kafka | no data of the bundle's: its producers write `<topic><suffix>` (their `TOPIC_SUFFIX`), joined to the shared stores' network, where the broker is `kafka` |

A Neo4j store, and a demo's producers and APIs, still run as the version's own compose project.
A version no longer read (`<bundles>/<name>/in-use`) has its data dropped by its suffix and its
own services taken down; another demo's data, or another version's, is untouched.

## Data rule — what the bundle serves live vs. from the dump

A demo reads its **own** sources live wherever it runs: the Postgres database loaded from the
dump, the Qdrant store recovered from the snapshot, the log files shipped in the bundle, the
file sources in the bundle's directory. These are the sources in its `docker-compose.run.yml`,
or files bundled with it.

Only external SaaS-account sources — a Snowflake table, a Databricks dataset, anything the demo
cannot ship in a container — are served from replicas: landed once at bundle build time (when
the stores run and the data is current) and read from those replicas in the bundle.

The build detects which tables need replicas and writes the seed only when there are any. A demo
of only local or file sources has no seed to write and no replica to serve.

## tour.yaml — the demo's guided tour

Every published demo has a `tour.yaml`, except one whose `demo.yaml` says `tour: core` (`starthere`). It describes the demo step by step: what it is, how
its pieces fit, and then the answer it shows. The UI renders it as an interactive tour, with each
step's words in `tour.json` (per locale, in `provisa-ui/src/i18n/`).

### Structure

```yaml
# demo/named/weblogs/tour.yaml (excerpt)
landing:
  route: /sql
  capability: query_development
steps:
- key: weblogsSources
  route: /sources
  capability: source_registration
  source: shop_access
```

`landing` is where "Look around on my own" leaves the visitor. Give it a `route` (a path in the
UI) and the `capability` that gates it. Every published demo must have a `landing`. Provisa
format 2 added this field; a bundle built without it is refused.

Each step has:

| Field | Required | Meaning |
|---|---|---|
| `key` | yes | Unique identifier; the UI finds its text at `tour.steps.<key>` in `tour.json` |
| `route` | if different from previous step | The page route to open (`/sources`, `/sql`, etc.) |
| `capability` | if `route` is set | The UI capability that gates this page |
| step type | yes | One of the types below |

### Step types

| Type | Value | Page |
|---|---|---|
| `source` | source id from `config.yaml` | Sources |
| `table` | `<source id>.<table name>` | Tables |
| `open` (with `table`) | `<section>[.<tab>]`: `identity`, `associations`, `load`, `query`, `quality`, `columns` (tab `naming`, `type`, `access` or `testing`) or `writes` | the table's detail panel, open on that section |
| `relationship` | relationship alias from `config.yaml` | Relationships |
| `example` | example name (without `.graphql`), from `queries/` | New query tab |
| `sql` | a SQL statement | SQL explorer |
| `api` | `GET /data/rest/<domain>/<table>[?<params>]` | REST explorer |
| `jsonapi` | `/data/jsonapi/<domain>/<table>[?<query>]` | JSON:API explorer |
| `grpc` | `<Method>(by=[...], funcs=[...])` | gRPC explorer |
| `cypher` | a Cypher query | Graph explorer |
| `term` | glossary term name | Glossary |
| `role` | role name (or `all`) | SQL explorer's run-as picker |
| `env` | `prod`, or an environment the demo's `setup.yaml` makes | the header's environment switcher; the steps after it read that environment |
| `polly` | a natural-language question | Polly panel |
| `as_of` | an ISO 8601 time | SQL explorer's "As of" picker; the statements after it read bitemporal views as of it |
| `as_recorded` | an ISO 8601 time | SQL explorer's "As recorded at" picker; the statements after it read bitemporal views as Provisa had recorded them then |
| `as_recorded_from` | `{step: <an earlier sql step's key>, row: <0-based>, column: <a column it returns>}` | SQL explorer's "As recorded at" picker, set to that value of the earlier step's answer when the step runs |
| `notebook` | `true` | Notebook tab: the demo's `notebook.ipynb` cells added to the open notebook and run |

A `polly` step is answered once when the demo's bundle is built (Polly asked through OpenRouter on
the sandbox's model, `OPENROUTER_API_KEY` given to the build; the answers are stored beside the
tour as `tour_answers.json`); what the sandbox shows is
that stored answer. Nothing is asked live. The question must be one of the demo's own graded
bake-off questions: `QUESTIONS["<demo>"]` in `tests/integration/polly_bakeoff_questions.py`, each
with the SQL over the demo's tables whose rows are its right answers. A demo with no stored answer
leaves the step out.

A `notebook` step runs the demo's `notebook.ipynb` in the visitor's browser: its code cells are
added to the Notebook tab's open notebook and run in turn, and the step is shown once the last has
answered. The setup status sends the cells as `demo_notebook`, and the Notebook tab also offers
them to a visitor who skipped the tour ("Add the demo's notebook"). The tour test refuses the step
when the demo ships no notebook, when its first code cell does not import `provisa_client` and
call `provisa_client.sign_in()`, and when a statement it reads (`provisa_client.query("...")`)
names anything but the demo's own tables and views.

An `as_recorded_from` step is for a time Provisa stamps, which the tour cannot know when it is
written: a recording time, read from the answer of a `sql` step before it. The tour runner runs
that statement again as it ran (the role and the time travel the steps before it set) and takes
row `row`'s `column`. The `reproduce` demo takes the time its correcting rebuild began from
`"ops"."mv_refresh_lineage"`: after the filed figures were recorded and before the correction was.
Nothing in the tour stamps a recording time.

### Track

The tour follows a settled track: the scenario and the features it shows, then how sources and
tables and relationships fit — each step on its real object — then the solution as predesigned
queries and views, then the `landing` page. The `weblogs` demo ends on `/sql`; the `sonnets`
demo ends on `/graph`, the echo network.

`tests/unit/test_named_demo_tour.py` checks every step: the step type is valid, the named
object exists in `config.yaml`, the route is a real UI route, and the capability is one the UI
gates. Fix tour authoring errors there first.

## Publishing a demo

A demo marked `published: true` in its `demo.yaml` is built into a bundle and published.

The bundle holds: the demo's `config.yaml` and `demo.yaml`, its tour and example queries, what
its setup wrote (available in the stores' dumps), the log files the build's traffic generator
wrote, and — for a demo with replicated external sources — the seed the sandbox tier's template
is built from (`seeded/`). A demo with bitemporal views also carries their history
(`seeded/views.duckdb`) and the records of the setup's refreshes that built it
(`seeded/refresh_lineage.parquet`). The template and each sandbox land those records as their own,
so `ops.mv_refresh_lineage` in a sandbox lists the refreshes its history came from.

The Demo bundles workflow (`.github/workflows/demo-bundles.yml`) builds a demo on a push to
`main` that touches `demo/named/<name>/`, or on a dispatch naming it. It publishes the bundle
to `ghcr.io/kenstott/provisa-demos` — one repository, every demo — under two tags: `<name>-<version>`
and `<name>-latest`. The version is the build's UTC minute and commit, for example
`202610141530-1a2b3c4d5`. A version is never published twice.

### Pulling and running a published demo

```bash
# latest version (default)
./start-ui-install.sh --demo weblogs

# pinned version
./start-ui-install.sh --demo weblogs@202610141530-1a2b3c4d5

# build from the checkout instead of pulling
./start-ui-install.sh --demo weblogs --from-source

# pull programmatically (in a deployment's node)
python -m provisa.core.demo_bundles pull weblogs --into ~/.provisa/demos
```

The launcher pulls the bundle into `$PROVISA_HOME/demos/<name>/<version>` (`~/.provisa` by
default). A version already there is not fetched again. `PROVISA_DEMO_REGISTRY` names another
registry; bundles come from `ghcr.io/kenstott` otherwise.

`--from-source` runs `demo/named/<name>/` through the full build-everything path. Combining
`--from-source` with `@<version>` is refused. Use it while authoring.

### Stopping other demos' stores

When `start-ui-install.sh --demo <name>` starts, it stops every other named demo's stores first
— containers removed, volumes kept — so the ports they hold are free. The current demo's own
stores are not touched. A bundle build's compose projects (`provisa-demo-bundle-*`) are left
alone.

### The demo format version

`DEMO_FORMAT = 2`. Format 2 added the tour's
`landing` field. A bundle built in format 1 (no `landing`) is refused with:

```
demo weblogs <version> is in demo format 1; this Provisa reads format 2:
pull its latest (start-ui-install.sh --demo weblogs)
```

A bundle whose `requires_provisa` is newer than the running Provisa is refused with:

```
demo weblogs <version> needs Provisa <required> or newer; this is Provisa <running>
```

Both refusals happen before the bundle's layer is fetched.

### Updating a published demo

Edit `demo/named/<name>/` and merge to `main`. The workflow publishes a new version. The next
`--demo <name>` pull and the next sandbox on the demos page get it. No release, no redeploy.

## Log files for Logs sources

A demo whose containers write log files can read them into tables with Logs sources.
Give the demo a `logs/` directory, one subdirectory per container, each holding a `.gitignore`
that ignores everything but itself, so the directories are checked in empty. The containers bind
mount their subdirectory and write there; Provisa runs on the host and reads the files where they
land.

For every named demo the launcher exports `PROVISA_NAMED_DEMO_DIR`, the demo's directory. When the
demo has a `logs/` directory, the launcher also adds it to `PROVISA_LOGS_ALLOWED_ROOTS`, the
directories a Logs source may read. A pristine start deletes the files under `logs/` and keeps the
`.gitignore` files; `--keep-data` keeps the logs with the data.

A Logs source's glob is a full path. Name it under the demo's directory with the variable, which
the source keeps as written and resolves each time it reads the files:

```yaml
# demo/named/weblogs/config.yaml (excerpt)
- id: shop_access
  type: logs
  cache_ttl: 30
  mapping:
    glob: ${env:PROVISA_NAMED_DEMO_DIR}/logs/nginx/access.json*
    format: jsonl
```

Give a Logs source a `cache_ttl`: its table is a replica, re-read on that interval. In
`docker-compose.yml`, mount the same directories through the same variable, so a test can point
them elsewhere:

```yaml
# demo/named/weblogs/docker-compose.yml (excerpt)
    volumes:
      - ${PROVISA_NAMED_DEMO_DIR:-.}/logs/nginx:/var/log/shop
```

`demo/named/weblogs/` is the example. nginx in front of a small web shop writes a JSON access log,
and the shop's Postgres logs every statement with its duration as jsonlog. nginx gives each
request an id and passes it to the shop. The shop puts it in a SQLCommenter comment on every
statement, and names each request's database connection by it (`application_name`). The demo's
views join the two logs and the shop's tables: the slowest pages and their SQL, the requests that
failed in the database, the customers served a server error, the products viewed but never
bought, and the slow statements by table. The join between the two logs is on
`application_name`, a column of the database log, because tables are joined only on their columns
along a relationship.

## Current published demos

Three demos are published:

| Name | Title | Data stores |
|---|---|---|
| `weblogs` | Web shop logs | Postgres (shop database), bundled log files (nginx + Postgres statement log) |
| `sonnets` | Shakespeare's sonnets | Postgres (sonnets), Qdrant (line embeddings) |
| `files` | Instant data lake | File sources only (CSV, Parquet, Excel, JSON) — no containers at run time |

## Step-by-step: build a new named demo

This walkthrough assumes a fictional `--demo banking` scenario with two local databases and one
external Snowflake source. The Snowflake source carries no local container.

**1. Create the directory.**

```bash
mkdir -p demo/named/banking/data
```

**2. Write `docker-compose.yml`.**

Include one service per local data store plus a `seeder` service. Add the standard labels.
Keep the database in a named volume and the seeder's marker under `${PROVISA_NAMED_DEMO_DIR:-.}`
(see "Volumes" above). Expose ports on non-conflicting host ports.

```yaml
x-demo-labels: &demo-labels
  com.provisa.demo: banking

services:
  postgresql:
    image: postgres:16
    labels:
      <<: *demo-labels
      com.provisa.demo.role: rdb
    ports:
      - "${PROVISA_BANKING_PG_PORT:-25700}:5432"
    volumes:
      - banking_pg:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U provisa -d banking"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 30s
    environment:
      POSTGRES_USER: provisa
      POSTGRES_PASSWORD: provisa
      POSTGRES_DB: banking

  seeder:
    build:
      context: .
      dockerfile: Dockerfile.seeder
    restart: "no"
    labels:
      <<: *demo-labels
      com.provisa.demo.role: seeder
    depends_on:
      postgresql:
        condition: service_healthy
    environment:
      PROVISA_BANKING_PG_HOST: postgresql
      PROVISA_BANKING_PG_PORT: "5432"
    volumes:
      - ${PROVISA_NAMED_DEMO_DIR:-.}/data:/data-marker

volumes:
  banking_pg:
```

The Snowflake source requires no service entry; it is external. Its credentials belong in
`config.yaml` via env-var interpolation.

**3. Write `Dockerfile.seeder`.**

Install only the drivers your generation scripts need.

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir psycopg2-binary
WORKDIR /app
COPY generate_postgres.py seed.py ./
ENTRYPOINT ["python", "seed.py"]
```

**4. Write `seed.py`.**

Check for a marker file. If present, skip. If absent, run generation scripts in order and
write the marker.

```python
import os, subprocess, sys, time
from pathlib import Path

MARKER = Path("/data-marker/.seeded")

def main():
    if MARKER.exists():
        print("already seeded; skipping")
        return 0
    t0 = time.monotonic()
    subprocess.run([sys.executable, "generate_postgres.py"], check=True)
    MARKER.parent.mkdir(parents=True, exist_ok=True)
    MARKER.write_text(f"seeded in {time.monotonic() - t0:.0f}s\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
```

**5. Write `generate_postgres.py` (and any other generate scripts).**

Read host and port from env vars, with localhost + published port as the defaults. This lets
the script run both from inside the seeder container and directly from the host for debugging.

```python
import os
HOST = os.environ.get("PROVISA_BANKING_PG_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BANKING_PG_PORT", "25700"))
```

**6. Create `config.yaml`.**

Start the data stack and the standard demo, build the scenario in the UI (the two local databases
and the Snowflake source, their tables and relationships), export the model, and save it as
`demo/named/banking/config.yaml`. Add the sources you created in the UI to its `sources:` list by
hand, since the export omits them. Give each connection value as an env-var interpolation, and the
external Snowflake source's credentials too:

```yaml
sources:
- id: banking-pg
  type: postgresql
  host: ${env:PROVISA_BANKING_PG_HOST:-localhost}
  port: ${env:PROVISA_BANKING_PG_PORT:-25700}
  database: banking
  username: provisa
  password: provisa
- id: banking-snowflake
  type: snowflake
  account: ${env:PROVISA_BANKING_SNOWFLAKE_ACCOUNT}
  username: ${env:PROVISA_BANKING_SNOWFLAKE_USER}
  password: ${env:PROVISA_BANKING_SNOWFLAKE_PASSWORD}
  database: ${env:PROVISA_BANKING_SNOWFLAKE_DATABASE}
```

Every source still needs explicit `tables:`, which the export supplies. For a small addition to the
pet store you can ship a `fragment.yaml` of list sections instead of a whole config; see
[Running and building demos on your laptop](sales-engineer-demos.md#pro-tip-a-fragment-instead-of-a-whole-config).

**7. Write `docker-compose.run.yml`.**

For each store the bundle will carry, write a service that loads from the build's dump. Give
every service the three `host_env`/`port_env`/`port` labels and publish on the loopback only.

```yaml
x-demo-labels: &demo-labels
  com.provisa.demo: banking

name: provisa-demo-banking-run

services:
  postgresql:
    image: postgres:16
    restart: unless-stopped
    labels:
      <<: *demo-labels
      com.provisa.demo.role: rdb
      com.provisa.demo.host_env: BANKING_PG_HOST
      com.provisa.demo.port_env: BANKING_PG_PORT
      com.provisa.demo.port: "5432"
    ports:
      - "127.0.0.1:${BANKING_PG_PORT-25700}:5432"
    volumes:
      - banking_run_pg:/var/lib/postgresql/data
      - ./dump/postgresql.sql:/docker-entrypoint-initdb.d/10-banking.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U provisa -d banking"]
      interval: 2s
      timeout: 5s
      retries: 60
    environment:
      POSTGRES_USER: provisa
      POSTGRES_PASSWORD: provisa
      POSTGRES_DB: banking

volumes:
  banking_run_pg:
```

The Snowflake source has no container here either; it is served from a replica the build landed.

**8. Write `demo.yaml`.**

```yaml
title: Banking demo
description: >-
  Customer accounts, transaction history and branch data from two databases and Snowflake,
  joined into one governed catalog.
screenshot: card.png
published: true
requires_provisa: 0.1.0a487
```

Set `requires_provisa` to the current Provisa release, or the oldest one whose Provisa carries
every feature the demo uses.

**9. Write `tour.yaml`.**

```yaml
landing:
  route: /sql
  capability: query_development
steps:
- key: bankingIntro
  route: /sources
  capability: source_registration
  source: banking-pg
- key: bankingTables
  route: /tables
  capability: table_registration
  table: banking-pg.accounts
- key: bankingJoin
  route: /relationships
  capability: create_relationship
  relationship: HOLDS
- key: bankingAnswer
  route: /sql
  capability: query_development
  sql: >-
    SELECT region, COUNT(*) AS accounts, SUM(balance) AS total_balance
    FROM banking.accounts GROUP BY region ORDER BY total_balance DESC
- key: bankingDone
  route: /sources
  capability: source_registration
  source: banking-pg
```

Add a line per key to `provisa-ui/src/i18n/en/tour.json` (and the other locales if you
translate the tour).

**10. Bring the stack up and test.**

```bash
docker compose -f demo/named/banking/docker-compose.yml up -d
```

Watch `docker compose logs seeder` to confirm generation finishes and the marker is written.

**11. Start Provisa with the named demo.**

```bash
./start-ui-install.sh --demo banking
```

The launcher brings the compose stack up and waits for the seeder, uses `config.yaml` in place of the
standard demo config, and prints `Config for named demo 'banking': ...`. The stack in step 10 can be started first for
testing, but unless you pass `--keep-data` the launcher removes it and seeds it again.

**12. Mark it published and submit.**

When the demo is ready, set `published: true` in `demo.yaml` and merge to `main`. The workflow
builds and publishes the bundle automatically.

## Log files for Logs sources

A demo whose containers write log files can read them into tables with Logs sources.
Give the demo a `logs/` directory, one subdirectory per container, each holding a `.gitignore`
that ignores everything but itself, so the directories are checked in empty. The containers bind
mount their subdirectory and write there; Provisa runs on the host and reads the files where they
land.

For every named demo the launcher exports `PROVISA_NAMED_DEMO_DIR`, the demo's directory. When the
demo has a `logs/` directory, the launcher also adds it to `PROVISA_LOGS_ALLOWED_ROOTS`, the
directories a Logs source may read. A pristine start deletes the files under `logs/` and keeps the
`.gitignore` files; `--keep-data` keeps the logs with the data.

A Logs source's glob is a full path. Name it under the demo's directory with the variable, which
the source keeps as written and resolves each time it reads the files:

```yaml
# demo/named/weblogs/config.yaml (excerpt)
- id: shop_access
  type: logs
  cache_ttl: 30
  mapping:
    glob: ${env:PROVISA_NAMED_DEMO_DIR}/logs/nginx/access.json*
    format: jsonl
```

Give a Logs source a `cache_ttl`: its table is a replica, re-read on that interval. In
`docker-compose.yml`, mount the same directories through the same variable, so a test can point
them elsewhere:

```yaml
# demo/named/weblogs/docker-compose.yml (excerpt)
    volumes:
      - ${PROVISA_NAMED_DEMO_DIR:-.}/logs/nginx:/var/log/shop
```

## The `--source` mechanism: what it is not

`--source=<name>` adds a single source from `demo/sources/<name>/` to the standard demo. Each
source in that directory carries its own `fragment.yaml` and a `prime.py` that seeds a small
toy dataset. The launcher starts and stops those containers.

Named demos deliberately do not follow that pattern. Their data is large, meant to persist
across restarts, and they manage their own compose lifecycle. `start-ui-install.sh --demo <name>`
starts the named demo's stack on every launch. By default it first removes the stack's containers and
volumes and deletes its `data/` directory (`docker compose down -v`), so the seeder reseeds from
empty; `--keep-data` skips that and reuses the data from the last seeding. The launcher never stops
the stack: stopping it is yours to do. See [Running and building demos on your
laptop](sales-engineer-demos.md#stop-and-reset).

You can combine both: `./start-ui-install.sh --demo perf --source=cassandra` adds a toy
Cassandra source on top of the perf demo. The two mechanisms are additive.
