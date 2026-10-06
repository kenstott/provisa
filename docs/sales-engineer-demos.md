# Running and building demos on your laptop

This guide is for sales engineers. It shows you how to start a Provisa demo on your own machine, build a new named demo for a customer scenario, and present it. You need a terminal and Docker. You do not need to write application code.

Everything here runs locally from a checkout of the repository.

## Before you start

You need:

- **Docker Desktop**, running. Named demos and `--source` demos start containers.
- **Python 3.12**, to create the repository's virtual environment.
- **Node.js 20 or later**, for the UI. The launcher loads `nvm` if you have it and switches to the version in `.nvmrc`.
- **Git**, to clone the repository.

Set up once:

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` creates `.venv` and installs the dependencies. The launcher calls `.venv/bin/python` and `.venv/bin/uvicorn` directly, so a missing `.venv` stops it. Run `./venv.sh` to rebuild the environment after a pull.

The launcher reads `.env` in the repository root if one exists, and `.env` is not committed. Put anything secret there (see [Demo hygiene](#demo-hygiene)).

## Run a demo with one command

`start-ui-install.sh` takes `--demo` in two forms:

| Command | What starts |
| --- | --- |
| `./start-ui-install.sh --demo` | The standard demo: a pet store with four sources |
| `./start-ui-install.sh --demo <name>` | A named demo from `demo/named/<name>/` |

The word after `--demo` is a name only if it does not start with `--`. So `--demo --source=redis` runs the standard demo, and `--demo perf --source=redis` runs the `perf` demo.

`--demo` always runs natively. The backend is a Python process, the federation engine runs inside it, and the control plane is an embedded PostgreSQL database. The standard demo needs no containers.

### The standard demo

```bash
./start-ui-install.sh --demo
```

The launcher first resets the demo's control-plane database, so every start is pristine. It then starts a demo petstore API, a demo GraphQL server and the backend, and then the UI. When it finishes it prints:

```text
Provisa running (demo mode):
  Backend: http://localhost:8001  (logs: tail -f .../.logs/backend.log)
  UI:      http://localhost:3000
  pgwire:  postgresql://admin:ignored@localhost:5439/provisa  (username = role)
```

It also lists the demo sources:

| Source | Type |
| --- | --- |
| `pet-store-pg` | PostgreSQL, `pet_store` schema |
| `petstore-api` | OpenAPI, `http://localhost:18080/api/v3` |
| `inquiries-sqlite` | SQLite, `demo/files/inquiries.sqlite` |
| `graphql-demo` | GraphQL remote, `http://localhost:4000/graphql` |

Open **http://localhost:3000**. There is no login: the demo's auth provider is `none`, and every request acts as the `org_admin` role. To connect a SQL client over pgwire, use the user name that matches the role you want, for example `admin`; the password is ignored.

Settings you change in the admin do not survive the next start, except organization settings such as the metadata export target and AI keys. The launcher saves those before the reset and restores them after.

### A named demo

```bash
./start-ui-install.sh --demo perf
```

With a name, the launcher does four more things before it starts the backend:

1. Unless you pass `--keep-data`, removes the demo's containers and volumes (`docker compose ... down -v`) and deletes `demo/named/<name>/data`, so the data is seeded from empty. Every named-demo start is pristine by default.
2. Runs `docker compose -f demo/named/<name>/docker-compose.yml up -d --build`.
3. If the compose file defines a service called `seeder`, runs `docker compose ... up seeder` and waits for it.
4. Uses the demo's own `config.yaml`, a whole config, in place of the standard demo config, and prints `Config for named demo '<name>': <path>`. (A demo may instead ship a `fragment.yaml` that the launcher overlays on the standard config; see the [pro tip](#pro-tip-a-fragment-instead-of-a-whole-config).) A `--source` toy source is overlaid on either.

With `--keep-data`, step 1 is skipped. The seeder finds its marker file, prints that the demo is already seeded, and does nothing. Use it for a demo whose data takes too long to regenerate on every start.

`perf` is a benchmark demo, sized for a server, not a laptop. Its compose file gives PostgreSQL 12 GB of shared buffers and Neo4j an 8 GB heap plus a 4 GB page cache, and it loads 20 million orders, which takes a long time to generate. To run it on a laptop you would need to change two things:

- **The row counts.** Set them in the environment before you start:

  ```bash
  PROVISA_BENCH_ORDERS=200000 PROVISA_BENCH_NEO4J_ORDERS=20000 \
    ./start-ui-install.sh --demo perf --keep-data
  ```

- **The memory settings** in `demo/named/perf/docker-compose.yml`: `shared_buffers` and `effective_cache_size` for PostgreSQL, and `NEO4J_server_memory_heap_max__size` and `NEO4J_server_memory_pagecache_size` for Neo4j. Docker Desktop must also be given more memory than the total.

`perf` takes `--keep-data` because 20 million rows should not regenerate on every start. The first start also builds a custom PostgreSQL image, which compiles several extensions. For a customer demo, build a demo of your own, as below, and treat `perf` as a worked example of the layout.

### Add a toy source on top

```bash
./start-ui-install.sh --demo --source=redis
```

See [Add one toy source](#add-one-toy-source-with---source).

### Stop and reset

- **Stop Provisa:** press `Ctrl+C` in the terminal that runs the launcher. It stops the backend, the UI and the demo servers. The embedded PostgreSQL stays up for the next start.
- **Other keys:** `Ctrl+R` restarts the backend, `Ctrl+U` clears the UI cache and restarts the UI, and `Ctrl+E` stops the servers and leaves Docker running.
- **Reset the Provisa side:** just start again. Every `--demo` start drops and rebuilds the demo control plane.
- **Reset a named demo's data:** just start again. A named demo reseeds from empty by default. Pass `--keep-data` to reuse the data from the last seeding instead.
- **Stop a named demo's data stack.** `Ctrl+C` does not stop it. Run:

  ```bash
  docker compose -f demo/named/perf/docker-compose.yml down
  ```

- **Force a reseed while using `--keep-data`.** The seeder skips work when `demo/named/<name>/data/.seeded` exists. Remove the stack's volumes and the data directory, then start:

  ```bash
  docker compose -f demo/named/perf/docker-compose.yml down -v
  rm -rf demo/named/perf/data
  ./start-ui-install.sh --demo perf --keep-data
  ```

  Deleting `data/` alone is not enough for `perf`: MongoDB keeps its data in a named Docker volume, and `down -v` removes it. A start without `--keep-data` does both.

### Troubleshooting

These are the messages the scripts print, and what to do.

| You see | Cause and fix |
| --- | --- |
| `Unknown option: ...` followed by a usage block | A flag the launcher does not know. The valid ones are `--demo [name]`, `--keep-data`, `--source=<name>`, `--native`, `--keep-docker`, `--fast` and `--idp=basic\|firebase`. |
| `Unknown demo name: X. No demo/named/X/ directory.` | The name does not match a directory. Run `ls demo/named`. |
| `--demo X has no config.yaml (and no fragment.yaml) in demo/named/X/ to register it with.` | The demo directory has neither file. Export the model into `config.yaml` (see [Create `config.yaml`](#4-create-configyaml)). |
| `--demo X has both config.yaml and fragment.yaml in demo/named/X/. Keep one: ...` | A demo takes a whole config or a fragment, not both. Delete one. |
| `Embedded control plane requires pgserver (Python <=3.12). Aborting.` | The virtual environment uses a newer Python. Recreate it with `PYTHON=python3.12 ./venv.sh`, and read `.logs/control-plane-pg.log`. |
| `Backend crashed. Last logs:` or `Backend did not become healthy. Last logs:` | Read the 20 lines printed, then `.logs/backend.log`. The launcher waits up to about three minutes for `/health`. |
| `UI dev server crashed — see .../.logs/ui.log` | The UI failed to build. Read `.logs/ui.log`, and check `node --version` against `.nvmrc`. |
| `UI still building after 60s — continuing` | Not an error. Wait, then reload the page. |
| `Warning: no GitHub token ... GovData subscriptions unavailable.` | Harmless. Only the optional government-data subscriptions need that token. |
| `Stopping previous start-ui-install.sh instance` | The launcher runs one copy at a time and replaced an older one. |
| Docker errors during `docker compose up` | The launcher stops at the first failing command. Start Docker Desktop and run the command it printed by hand to see the full error. |
| `perf demo already seeded (...); skipping` | Normal with `--keep-data` after the first start. |
| `rm` reports a permission error while removing `data/` | Files written by a container on Linux can belong to another user. Delete `demo/named/<name>/data` with `sudo`, then start again. |

## Build a new named demo

A named demo is a directory, `demo/named/<name>/`. Create the directory and `./start-ui-install.sh --demo <name>` finds it. You do not edit the launcher, and no name is registered anywhere. The launcher refuses a missing directory, and a demo with neither `config.yaml` nor `fragment.yaml`, or with both.

The worked example is `retail`: a PostgreSQL database with customers and orders, plus a stores list loaded from a CSV file.

### The files

```text
demo/named/retail/
  docker-compose.yml     the one start point: the database and the seeder
  Dockerfile.seeder      the seeder's image
  seed.py                the seeder's entry point
  generate_postgres.py   creates and fills the tables
  stores.csv             a small list of stores
  config.yaml            the demo's whole config: sources, domains, tables, relationships
  .gitignore             one line: data/
```

`perf` has the same shape with four databases instead of one. A demo with no data stores of its own needs only `config.yaml`: no compose file, no seeder.

### 1. Create the directory

```bash
mkdir -p demo/named/retail
cd demo/named/retail
echo "data/" > .gitignore
```

### 2. Write `docker-compose.yml`

```yaml
x-demo-labels: &demo-labels
  com.provisa.demo: retail
services:
  postgresql:
    image: postgres:16
    restart: unless-stopped
    labels:
      <<: *demo-labels
      com.provisa.demo.role: rdb
    environment:
      POSTGRES_USER: provisa
      POSTGRES_PASSWORD: provisa
      POSTGRES_DB: retail
    ports:
      - "${PROVISA_RETAIL_POSTGRESQL_PORT:-25800}:5432"
    volumes:
      - ./data/postgresql:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U provisa -d retail"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 30s
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
      PROVISA_RETAIL_POSTGRESQL_HOST: postgresql
      PROVISA_RETAIL_POSTGRESQL_PORT: "5432"
      PROVISA_RETAIL_ORDERS: "${PROVISA_RETAIL_ORDERS:-5000}"
    volumes:
      - ./data:/data-marker
```

Four things matter here:

- **The service must be named `seeder`.** The launcher checks for that exact name, and runs `docker compose up seeder` to wait for it.
- **Every database service needs a healthcheck.** The seeder waits for `service_healthy`, so a database that is up but not ready never gets loaded halfway.
- **Use bind mounts under `./data/`** for database files. Docker Desktop keeps named volumes inside its own disk image, which fills the system disk. MongoDB is the exception: in `perf` it uses a named volume, because its storage engine needs file locking that bind mounts on Docker Desktop for Mac do not provide.
- **Labels.** `com.provisa.demo` groups every container of the demo, so `docker ps --filter label=com.provisa.demo=retail` lists exactly them.

Pick host ports no other demo uses. `perf` uses 25632, 27317, 28623, 27974, 27987 and 26379. Most toy sources use ports from 21000 to 39999, and Splunk uses 8088 and 8089.

### 3. Write the seeder

`Dockerfile.seeder`:

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir psycopg2-binary
WORKDIR /app
COPY generate_postgres.py seed.py stores.csv ./
ENTRYPOINT ["python", "seed.py"]
```

`seed.py` runs each generator once and records that it did. The marker lives on the `./data` bind mount, so it survives container restarts and disappears when you delete `data/`.

```python
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
MARKER = Path("/data-marker/.seeded")
ORDERS = os.environ.get("PROVISA_RETAIL_ORDERS", "5000")


def main() -> int:
    if MARKER.exists():
        print(f"retail demo already seeded ({MARKER.read_text().strip()}); skipping")
        return 0
    t0 = time.monotonic()
    subprocess.run(
        [sys.executable, str(HERE / "generate_postgres.py"), "--orders", ORDERS], check=True
    )
    MARKER.parent.mkdir(parents=True, exist_ok=True)
    MARKER.write_text(f"seeded in {time.monotonic() - t0:.0f}s, orders={ORDERS}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

`generate_postgres.py` builds the data from nothing. It reads its host and port from the same environment variables the compose file sets, with `localhost` and the published port as defaults, so you can also run it from your laptop while debugging.

```python
import argparse
import csv
import os
import random
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

HOST = os.environ.get("PROVISA_RETAIL_POSTGRESQL_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_RETAIL_POSTGRESQL_PORT", "25800"))
FIRST = ["Ana", "Ben", "Chi", "Dev", "Eli", "Fay", "Gus", "Hana"]
LAST = ["Ames", "Bose", "Cruz", "Diaz", "Egan", "Frey", "Gray", "Holt"]
STATUSES = ["open", "shipped", "delivered", "returned"]


def connect():
    deadline = time.monotonic() + 60
    while True:
        try:
            return psycopg2.connect(
                host=HOST, port=PORT, dbname="retail", user="provisa", password="provisa"
            )
        except psycopg2.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=5000)
    orders = parser.parse_args().orders
    rng = random.Random(7)  # a fixed seed: every seeding gives the same data
    customers = max(orders // 10, 10)
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orders, customers, stores")
        cur.execute("CREATE TABLE stores (store_id integer PRIMARY KEY, city text, region text)")
        cur.execute(
            "CREATE TABLE customers (customer_id integer PRIMARY KEY, name text, email text)"
        )
        cur.execute(
            "CREATE TABLE orders (order_id integer PRIMARY KEY, customer_id integer, "
            "store_id integer, order_date date, amount numeric(10,2), status text)"
        )
        with open(Path(__file__).parent / "stores.csv", newline="") as f:
            stores = [(int(r["store_id"]), r["city"], r["region"]) for r in csv.DictReader(f)]
        execute_values(cur, "INSERT INTO stores VALUES %s", stores)
        execute_values(
            cur,
            "INSERT INTO customers VALUES %s",
            [
                (
                    i,
                    f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                    f"customer{i}@example.com",
                )
                for i in range(1, customers + 1)
            ],
        )
        start = date(2025, 1, 1)
        execute_values(
            cur,
            "INSERT INTO orders VALUES %s",
            [
                (
                    i,
                    rng.randint(1, customers),
                    rng.choice(stores)[0],
                    start + timedelta(days=rng.randint(0, 364)),
                    round(rng.lognormvariate(3.7, 0.8), 2),
                    rng.choice(STATUSES),
                )
                for i in range(1, orders + 1)
            ],
        )


if __name__ == "__main__":
    main()
```

`stores.csv`:

```text
store_id,city,region
1,Austin,South
2,Boston,Northeast
3,Chicago,Midwest
4,Denver,West
```

The names and addresses come from short fixed lists and `@example.com`, so no real person appears anywhere.

### 4. Create `config.yaml`

`config.yaml` is the demo's whole config: every source, domain, table, relationship and role it shows, plus the settings the standard demo carries. The launcher uses it in place of the standard demo config. Every start rebuilds the control plane from it, so the demo is exactly what the file lists.

You do not write it by hand. Build the demo in the running UI and export it.

1. Bring up the data stack yourself, if the demo has one, and start the standard demo (no name):

   ```bash
   docker compose -f demo/named/retail/docker-compose.yml up -d --build
   ./start-ui-install.sh --demo
   ```

2. In the UI, add the database as a source (PostgreSQL, host `localhost`, port `25800`, database `retail`, user `provisa`), register its tables, and add the relationships between them. Add roles or row rules if the story needs them.
3. Export the model. Open **Admin**, then **Maintenance**, then **Configuration File**, and choose **View / Diff**. The right pane, **Current**, is the whole live config. Copy all of it. The same text comes from `curl -s http://localhost:8001/admin/config/live` while the demo runs. In the demo this export is always on.
4. Save it as `demo/named/retail/config.yaml`.
5. Add what the export leaves out. It carries your tables, relationships, roles and domains, but **not the sources you added in the UI**: sources come from the startup file, not from live state. Add each one to the `sources:` list by hand, with its connection values as `${env:...}` references:

   ```yaml
   sources:
   - id: retail-postgresql
     type: postgresql
     host: ${env:PROVISA_RETAIL_POSTGRESQL_HOST:-localhost}
     port: ${env:PROVISA_RETAIL_POSTGRESQL_PORT:-25800}
     database: retail
     username: provisa
     password: ${env:PROVISA_RETAIL_POSTGRESQL_PASSWORD:-provisa}
     description: "Retail demo database (PostgreSQL)"
   ```

6. Decide what else stays. The export starts from the standard demo, so the pet store's sources, tables and relationships are in it. Keep them to show retail beside the pet store, or delete them for a retail-only demo.

Rules that bite:

- **Declare `tables:` for every source.** A bare `sources:` entry never produces tables in the SQL catalog. The export already names each table, its schema, and each column with its type and who may see it.
- **`visible_to` names roles.** The standard config's `analyst` role may only open the pet-store domains. A new domain is visible to `org_admin`, which the demo's requests act as. Add another role under `roles:` with its own `domain_access`, as `perf` does for `org_admin_unguarded`.
- **Connection values are environment variables with defaults:** `${env:NAME:-default}`. The compose file and the generator read the same names.
- **Everything is synthetic.** Describe it that way in the descriptions.

#### Pro tip: a fragment instead of a whole config

For a small addition to the pet store, you can skip the export and ship `demo/named/<name>/fragment.yaml` instead of `config.yaml`. The launcher overlays it on the standard demo config. It holds only list sections, and each is appended to the standard demo's: `domains`, `sources`, `tables`, `relationships` and `roles`. A top-level setting the standard config already sets, with a different value, fails the load with `config include ...: key '...' conflicts with the including file; only list sections merge`.

Give a demo one of the two files. With neither, or with both, the launcher refuses it by name.

The retail demo as a fragment, written by hand:

```yaml
domains:
- id: retail
  description: "Retail demo: orders, customers and stores"
sources:
- id: retail-postgresql
  type: postgresql
  host: ${env:PROVISA_RETAIL_POSTGRESQL_HOST:-localhost}
  port: ${env:PROVISA_RETAIL_POSTGRESQL_PORT:-25800}
  database: retail
  username: provisa
  password: ${env:PROVISA_RETAIL_POSTGRESQL_PASSWORD:-provisa}
  description: "Retail demo database (PostgreSQL)"
tables:
- source_id: retail-postgresql
  domain_id: retail
  schema: public
  table: customers
  description: Customers (synthetic)
  columns:
  - {name: customer_id, data_type: integer, is_primary_key: true, visible_to: [org_admin]}
  - {name: name, data_type: varchar, visible_to: [org_admin]}
  - {name: email, data_type: varchar, visible_to: [org_admin]}
- source_id: retail-postgresql
  domain_id: retail
  schema: public
  table: stores
  description: Stores, loaded from stores.csv
  columns:
  - {name: store_id, data_type: integer, is_primary_key: true, visible_to: [org_admin]}
  - {name: city, data_type: varchar, visible_to: [org_admin]}
  - {name: region, data_type: varchar, visible_to: [org_admin]}
- source_id: retail-postgresql
  domain_id: retail
  schema: public
  table: orders
  description: Orders (synthetic)
  enable_aggregates: true
  enable_group_by: true
  columns:
  - {name: order_id, data_type: integer, is_primary_key: true, visible_to: [org_admin]}
  - {name: customer_id, data_type: integer, visible_to: [org_admin]}
  - {name: store_id, data_type: integer, visible_to: [org_admin]}
  - {name: order_date, data_type: date, visible_to: [org_admin]}
  - {name: amount, data_type: numeric, visible_to: [org_admin]}
  - {name: status, data_type: varchar, visible_to: [org_admin]}
relationships:
- id: retail-orders-customer
  source_table_id: orders
  source_column: customer_id
  target_table_id: customers
  target_column: customer_id
  cardinality: many-to-one
- id: retail-orders-store
  source_table_id: orders
  source_column: store_id
  target_table_id: stores
  target_column: store_id
  cardinality: many-to-one
```

### 5. Test it

Start the data stack by itself first, from the repository root, to check the seeder:

```bash
docker compose -f demo/named/retail/docker-compose.yml up -d --build
docker compose -f demo/named/retail/docker-compose.yml logs -f seeder
```

The seeder exits when it finishes. Confirm the marker and the rows. (The launcher removes this stack on a default start and seeds it again, so this check is only for debugging.)

```bash
ls demo/named/retail/data/.seeded
docker compose -f demo/named/retail/docker-compose.yml exec postgresql \
  psql -U provisa -d retail -c "select count(*) from orders"
```

Then run the whole demo:

```bash
./start-ui-install.sh --demo retail
```

The launcher tears the data stack down and seeds it again, then uses `config.yaml`.

In the UI, open **Tables** and look for the `retail` domain. Open **SQL** and run:

```sql
SELECT s.region, COUNT(*) AS orders, ROUND(SUM(o.amount), 2) AS revenue
FROM retail.orders o JOIN retail.stores s ON s.store_id = o.store_id
GROUP BY s.region
ORDER BY revenue DESC
```

If the table names differ in your install, copy the form shown in the **Tables** list. Then start the demo a second time: without `--keep-data` it reseeds from empty, and with it the seeder prints `retail demo already seeded` and skips.

## Add one toy source with `--source`

`--source=<name>` is a shortcut. It starts a source locally in Docker, fills it with sample data, and registers it as a data source, all in one step. Use it when you want to show a specific connector, say Redis or Elasticsearch, without setting up that system yourself.

You could do the same by hand: stand up the system, load some data, and add it as a source in the UI. `--source` only saves you from standing it up. Each directory under `demo/sources/` holds a `compose.yml`, usually a `prime.py` that loads a few rows, and a `fragment.yaml` that registers the source. The launcher starts the containers under the project name `provisa-demo-<name>`, primes them, and includes the fragment.

```bash
./start-ui-install.sh --demo --source=redis
./start-ui-install.sh --demo --source=redis --source=mongodb
```

List what exists:

```bash
.venv/bin/python demo/sources/provision.py list
```

Only the sources that also have a `fragment.yaml` work with `--source`: `cassandra`, `chinook`, `elasticsearch`, `mongodb`, `neo4j`, `prometheus`, `redis`, `sparql` and `splunk`. For any other directory the containers start and then the launcher stops with `--source=<name> has no .../fragment.yaml to register it with`.

A `--source` fragment registers the source only. Add its tables in the UI with **Register Table**.

An unknown name prints `unknown source 'x'; available: ...`.

To remove a toy source's containers and data:

```bash
.venv/bin/python demo/sources/provision.py down --prefix provisa-demo redis
```

### Which one to use

| You want | Use |
| --- | --- |
| Show one connector, such as Redis or Elasticsearch, next to the pet store | `--source` |
| A scenario for a customer, with your own tables, relationships and data | A named demo |
| Several databases of different kinds joined together, tables pre-registered | A named demo |
| A few rows and no seeding code | `--source` |
| Data that must survive restarts, or large data | A named demo, started with `--keep-data` |

The two combine: `./start-ui-install.sh --demo retail --source=redis`.

## Demo hygiene

- **Use synthetic data only.** Never copy a customer's data into a demo, not even a sample. Generate rows from fixed lists and a seeded random generator, as `generate_postgres.py` does. Use `@example.com` addresses.
- **To make a realistic demo from a customer's shape, use profiles and fakes.** Profile a table, declare fakes on its identifying columns, and generate a synthetic dataset at the volume you need. [Building test environments](test-data.md) walks through it, and [Fake methods](fake-methods.md) lists every fake. That route copies the distributions and relationships without the people.
- **Reset between customer sessions.** Start the launcher again (every `--demo` start rebuilds the control plane), and a named demo reseeds from empty unless you pass `--keep-data`. Do not pass `--keep-data` between customers if a session changed the data.
- **Keep secrets out of files.** Write connection values as `${env:NAME}` in `config.yaml` and put the real values in the repository's `.env`, which git ignores. The launcher loads `.env` before it reads the config. Never commit a password, key or token. The `provisa`/`provisa` password in the examples protects a local container that exists only for the demo.
- **Do not reuse a named demo's ports** for anything else running on your laptop.

## Presenting it

1. **Open on the Sources page.** Show the registered sources and their types. Then show **Tables**: every source becomes the same kind of table.
2. **Take the product tour.** The first time the UI opens, it offers a guided tour. The compass icon in the toolbar starts it again at any time. It registers a source, exposes tables and then queries them in several languages, in about five minutes. Press `Esc` or click outside to leave.
3. **Run one query that crosses tables.** Use the SQL page. The retail query above joins orders to stores and totals revenue by region. To show a join across sources, join a retail table to a pet-store table, or add a toy source with `--source`.
4. **Show the same query in GraphQL.** One model, many query languages.
5. **Show governance.** Open **Security** and show roles and row rules. Connect a SQL client to pgwire (`postgresql://admin:ignored@localhost:5439/provisa`) and show the same data there.
6. **Ask Polly.** Polly is the data assistant, in a panel at the side of the UI. It needs an LLM key first. Put `ANTHROPIC_API_KEY=...` (or `OPENAI_API_KEY=...`) in your own `.env` at the repository root before the session. The launcher loads it. Without a key, Polly shows "Polly isn't set up yet" and a button to the AI Models page (`/admin/ai-models`). For a vendor other than Anthropic, set a model there too. The natural-language page in the tour uses seeded answers and works without one.
7. **End on your customer's question.** Name the question first, then show it answered over their sources, in their domain's words.

Before a session, run the launcher once, click through the tour, and ask Polly one question, so nothing surprises you in front of the customer.

## See also

- [Named demos: developer reference](named-demos.md)
- [Building test environments](test-data.md)
- [Fake methods](fake-methods.md)
