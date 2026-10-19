# Dimensionamiento y ajuste {: #sizing-and-tuning }

La herramienta de dimensionamiento mide su propio despliegue de Provisa bajo su propia carga. A partir de esas mediciones escribe una guía de dimensionamiento y ajuste: cuántos núcleos físicos y procesos worker necesita la carga, y qué aporta cada ajuste. Cada cifra de la guía procede de una medición, así que tras un cambio en el despliegue o en la carga vuelve a ejecutar la herramienta y regenera la guía.

La herramienta está en el repositorio de Provisa, en `demo/named/perf/bench/`. Ejecute cada comando siguiente desde ese directorio con el entorno Python del repositorio (`../../../../.venv/bin/python3`).

## Qué informa {: #what-it-reports }

Una sola ejecución de dimensionamiento son varias ejecuciones de benchmark contra su despliegue. Cada una varía una cosa:

| Ejecución | Qué cambia | Qué da |
|---|---|---|
| Línea base | Nada: una columna, una fila, sin filtro, sin unión, sin caché | El costo base de una solicitud |
| Ejecuciones de perillas | Una perilla de la solicitud cada vez: campos, filtros, filas, uniones dentro del mismo origen, uniones entre orígenes | El costo que cada perilla añade por unidad |
| Ejecuciones de uniones entre orígenes | Los valores de unión entre orígenes, una vez con ambos extremos leídos en vivo, otra con ambos desde réplicas y otra con uno de cada | El costo de una unión entre orígenes según cómo se lean sus extremos, y el punto de equilibrio entre extremos en vivo y en réplica |
| Ejecuciones de caché | Todas las solicitudes con activación de la caché de respuestas, luego ninguna | El costo de un acierto de caché y de un fallo |
| Ejecuciones de replicación | Un origen leído en vivo, luego desde una réplica | La tasa a partir de la cual una réplica cuesta menos |
| Ejecuciones de workers | Procesos worker y solicitudes en curso por worker | Workers por núcleo físico, solicitudes en curso por worker, lo que entrega un núcleo |

Las ejecuciones de perillas, de caché y de replicación se hacen por transporte y por origen, y una vez por cada motor de federación que usted nombre. Las ejecuciones de workers se hacen por transporte.

Aquí, costo es el tiempo de CPU de Provisa por solicitud, en milisegundos. Las ejecuciones de perillas no usan caché, de modo que el costo de una perilla nunca incluye el efecto de la caché.

A partir de las ejecuciones, `bench_report.py` ajusta un modelo de costos por motor, transporte y origen: un costo base más un costo por campo, filtro, fila y unión, y una unión entre orígenes se valora según cómo se lean sus dos extremos. Cuando el motor se ejecuta como servidor propio y se mide la CPU de su contenedor, el motor también recibe un modelo de costos. `sizing_guide.py` convierte el informe en la guía. Dado un contrato que describe su carga, también dimensiona esa carga.

## Escribir un contrato de configuración {: #write-a-setup-contract }

El contrato de configuración es un único archivo YAML. Nombra los endpoints de su despliegue, las tablas que el benchmark puede leer, la forma de las solicitudes y la carga. El benchmark comprueba el contrato contra el despliegue antes de ejecutarse. Rechaza una tabla, columna o unión que el despliegue no expone.

Las tablas y columnas se nombran como su despliegue las registra. Cómo las llama cada transporte (nombres SQL, campos GraphQL, etiquetas Cypher, rutas REST, tipos gRPC) se consulta al despliegue cuando empieza la ejecución.

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

El formato del contrato se define en `setup_contract.py` y `contract_parse.py`; un contrato no válido se rechaza con un mensaje que nombra el campo. Los pesos de `workload` deben sumar 1, y cada transporte que nombran debe ser uno que el contrato ejecute.

Describa las solicitudes tal como son. La guía dimensiona exactamente las solicitudes que genera este contrato: el dimensionamiento las extrae del mismo generador con semilla que usa una ejecución.

## Ejecutarlo {: #run-it }

### 1. Resolver los nombres una vez {: #1-resolve-the-names-once }

Ejecute una vez el contrato de línea base. La ejecución pregunta al despliegue cómo llama cada transporte a sus tablas y escribe `resolved-names.json` junto a sus resultados.

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` nombra el motor de federación que ejecuta su despliegue (`duckdb`, `pg` o `trino`). `--server-pid` es el id de proceso del servidor; la ejecución lee el tiempo de CPU de ese proceso y de sus procesos hijos (los workers). Lee el proceso directamente, así que solo funciona cuando el generador de carga se ejecuta en el host del servidor.

### Medir la CPU del servidor desde otro host {: #measuring-the-servers-cpu-from-another-host }

Ejecute el generador de carga en su propia máquina cuando pueda: así no quita núcleos al servidor. Para medir desde allí la CPU del servidor, inicie `server_cpu_agent.py` en el host del servidor y dé a la ejecución su URL en lugar de `--server-pid`:

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

El agente responde una sola cifra, la misma que lee `--server-pid`: `GET /` devuelve `{"pid": ..., "cpu_seconds": ...}`, los segundos de CPU acumulados del proceso del servidor y de sus workers. Cualquier otra ruta responde 404, y cualquier otro método se rechaza. Dé a una ejecución `--server-pid` o `--server-cpu-url`, nunca ambos.

`--listen` es obligatorio: el agente no tiene dirección por defecto. Enlácelo a una dirección privada a la que solo pueda llegar el generador de carga, nunca a una interfaz pública, y deténgalo (Ctrl+C) cuando terminen las ejecuciones.

### 2. Planificar las ejecuciones {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`: los núcleos físicos del servidor, no sus hilos de hardware. En Linux, `lscpu` muestra núcleos por socket y sockets; multiplíquelos. La herramienta nunca adivina este valor.
- `--shared-host`: `yes` cuando sus orígenes o el generador de carga se ejecutan en el host del servidor. Entonces compiten con Provisa por sus núcleos, y la guía lo indica.
- `--workers` y `--request-threads`: los procesos worker y las solicitudes en curso por worker que se probarán. Indique ambos o ninguno.
- `--replication` y `--ttl`: añaden las ejecuciones en vivo y en réplica para cada origen, con la réplica refrescada cada `--ttl` segundos. Las ejecuciones de uniones entre orígenes que leen desde réplicas también necesitan `--ttl`.
- `--engines`: los motores de federación que se medirán, `pg` por defecto. Cada ejecución de perillas se planifica una vez por motor; el motor se fija cuando arranca el servidor, así que las ejecuciones de cada motor requieren reiniciar el servidor con él.
- `--engine-container`: `ENGINE=CONTAINER` para un motor que se ejecuta como servidor en su propio contenedor, cuya CPU informa entonces cada ejecución. Omita un motor que se ejecuta dentro de Provisa (DuckDB).

El plan escribe un contrato por ejecución y un `manifest.json`. Una ejecución que el despliegue no puede atender, como uniones sobre gRPC o una tabla que un transporte no expone, se lista en el manifiesto con el motivo. Nunca se descarta en silencio.

Las ejecuciones de réplica miden el costo de refresco de la réplica durante una ventana sin carga. Defina `load.idle_window_s` en su contrato (por ejemplo `120`) antes de planificarlas. Sin él, el informe rechaza el punto de equilibrio con `no idle.json`.

### 3. Ejecutar cada contrato planificado {: #3-run-every-planned-contract }

Cada entrada del manifiesto nombra su archivo de contrato. Ejecute cada uno en `results/sizing/results/<run name>`:

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

Desde otro host, use `--server-cpu-url` en lugar de `--server-pid`. Una ejecución de workers reinicia el servidor, así que su id de proceso cambia: reinicie también el agente con el nuevo `--pid`.

Añada `--apply-replication` a cada ejecución cuya entrada del manifiesto tenga `"apply_replication": true` (las ejecuciones de replicación y las de uniones entre orígenes). Establece la replicación que declara el contrato de la ejecución y la restaura después.

Cada ejecución lleva un `server_env` en el manifiesto: el servidor debe estar ejecutándose con él. El de una ejecución de perillas es su motor, `{"PROVISA_ENGINE": "duckdb"}`; ejecute todas las de un motor y después reinicie el servidor con el siguiente. El de una ejecución de workers añade sus workers y el límite de solicitudes en curso, por ejemplo `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`: `PROVISA_WORKERS` es el número de procesos worker y `PROVISA_REQUEST_THREADS` fija `concurrency.request_threads`, las solicitudes en curso que ejecuta cada worker a la vez. Espere a que todos los workers estén atendiendo (`/health` informa `workers.ready` de `workers.expected`) antes de iniciar la ejecución.

### 4. Construir el informe {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (por defecto `0.05`) fija cuán cerca del mejor rendimiento medido debe quedar un ajuste de workers para contar como suficiente.

### 5. Generar la guía {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

Sin `--setup`, la guía solo tiene las secciones de ajuste. Con él, la guía dimensiona la `workload` del contrato con el motor que nombra `--engine` (`pg` por defecto); se rechaza un motor que las ejecuciones no midieron.

## Leer la guía {: #read-the-guide }

### Dimensionar esta carga {: #sizing-this-workload }

La guía termina esta sección con una línea, por ejemplo: **12 physical cores, 24 worker processes, 2 in-flight requests per worker.** Encima, una tabla muestra cada transporte y origen: la proporción de solicitudes, la media de campos, filtros, filas y uniones, la proporción de aciertos de caché y la CPU que cuesta una solicitud.

Una solicitud cuesta, en un fallo, el modelo de costos en las medias de sus perillas y, en un acierto, el costo de acierto medido. La CPU de Provisa por segundo de un transporte, dividida por lo que entrega un núcleo físico, da sus núcleos físicos. La guía los suma entre transportes y redondea hacia arriba. Los workers son los núcleos por el mayor número de workers por núcleo que necesite cualquiera de los transportes. Las solicitudes en curso por worker son igualmente el mayor valor que necesite cualquiera. Un solo ajuste sirve a todos.

La herramienta no dimensiona nada que no haya medido. Una parte de la carga para la que las ejecuciones no tienen modelo detiene el dimensionamiento con un mensaje que la nombra:

- un motor que las ejecuciones no midieron
- un transporte sin ejecuciones de workers
- un origen sin modelo de costos en ese transporte
- una perilla que las ejecuciones no variaron (varíela y vuelva a ejecutar)
- una indicación de ruta (`direct` o `federated`) en un origen
- un ajuste de replicación o un TTL distinto del medido
- aciertos de caché cuando las ejecuciones no tienen cifras de caché

### Procesos worker y solicitudes en curso {: #worker-processes-and-in-flight-requests }

Para cada transporte, la guía muestra el menor número de workers cuyo rendimiento máximo queda dentro de la tolerancia respecto al mejor. Con ese número de workers elige después el menor número de solicitudes en curso por worker que logra lo mismo. Muestra ese ajuste de dos formas: workers por núcleo físico y solicitudes en curso por worker. Más de cualquiera de los dos no aporta rendimiento. Menos deja núcleos ociosos. La tabla da también el máximo de solicitudes por segundo y su latencia p99. Debajo se lista cada punto medido.

El factor de capacidad es las solicitudes por segundo que entrega un núcleo físico, por la CPU que cuesta una solicitud por sí sola, dividido entre 1000. Por encima de 1, el núcleo hace más que las solicitudes de un hilo, como ocurre cuando sirven sus dos hilos de hardware.

### Qué cuesta una solicitud {: #what-a-request-costs }

Para cada motor, transporte y origen, lado a lado: el costo base de una solicitud sin caché, el costo que añade cada campo, filtro, fila y unión, y qué tan bien se ajusta el modelo a las mediciones (R², 1 es un ajuste perfecto). Una unión entre orígenes tiene tres costos, según cómo se lean sus dos extremos: ambos en vivo, ambos desde réplicas, o uno de cada. Un guion significa que las ejecuciones no variaron esa perilla. Cuando el motor se ejecuta como servidor propio, una segunda tabla da la CPU del motor del mismo modo: una solicitud que atiende el motor, como una unión entre orígenes, muestra allí su tiempo de motor. Use estas cifras para ver qué costaría un cambio en sus solicitudes antes de hacerlo: dos campos más en cada solicitud añaden el doble del costo por campo.

### La activación de la caché de respuestas {: #the-response-cache-opt-in }

Para cada transporte y origen: la CPU de un fallo, de un acierto y lo que ahorra un acierto. Una solicitud se responde desde la caché solo cuando se activa y repite una solicitud anterior, así que el ahorro se aplica a las solicitudes repetidas. Active la caché en las repetidas.

### Lecturas en vivo o en réplica, y el TTL {: #live-or-replica-reads-and-the-ttl }

Para cada motor, transporte y origen con ejecuciones de replicación: el costo de una lectura en vivo, de una lectura en réplica, la CPU de un refresco, el TTL y la tasa de punto de equilibrio. Por encima de la tasa de punto de equilibrio, una réplica cuesta menos CPU, la de Provisa, la del origen y la del motor juntas, que leer en vivo. Por debajo, lea en vivo. Una segunda tabla da lo mismo para las uniones entre orígenes, por número de uniones en la solicitud: ambos extremos leídos en vivo frente a ambos desde réplicas, donde el refresco cuenta cada origen que el cambio movió a una réplica.

El punto de equilibrio es la CPU del refresco por segundo dividida entre lo que ahorra una lectura:

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

Duplicar el TTL reduce a la mitad la tasa de punto de equilibrio, y los datos pueden tener hasta el doble de antigüedad. Elija el TTL según cuán desactualizados puedan aceptar los datos sus usuarios y compare después su tasa de solicitudes sobre ese origen con el punto de equilibrio para ese TTL.

El costo de una lectura en réplica depende del almacén y de sus índices, que se construyen poco después de cada aterrizaje. Mida cuando los índices ya existan y vuelva a medir tras un cambio de almacén. [Réplicas](replicas.md) trata la elección del almacén y las estrategias de frescura.
