# Guía rápida para desarrolladores {: #developer-quick-start }

Para evaluar Provisa sin compilar desde el código fuente, consulte el [Inicio rápido](index.md): descargue el instalador para macOS, Windows o Linux y ejecute `provisa start`.

Esta guía explica cómo ejecutar Provisa **desde el repositorio**, cuando usted desarrolla, depura o contribuye.

---

## Requisitos previos {: #prerequisites }

- **Docker Desktop** (en ejecución)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Clonar y configurar {: #1-clone-and-set-up }

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` crea `.venv/`, instala todas las dependencias de Python mediante `pip install -e ".[dev]"`, y configura los git hooks en `.githooks/`.

---

## 2. Iniciar todo {: #2-start-everything }

```bash
./start-ui.sh
```

Cuando termine de iniciar, verá:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Qué inicia:**

- Servicios principales de Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis
- Superposición de desarrollo de Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry
- API de backend en el puerto 8001 (recarga en caliente ante cambios en `provisa/` y `config/`)
- Servidor de desarrollo Vite de la UI en el puerto 3000 (HMR)
- Trazado de OpenTelemetry y Grafana en `http://localhost:3100`. El stack de observabilidad (OTel Collector, Prometheus, Tempo, Grafana) es un perfil `observability` opcional de docker-compose y está desactivado por defecto en la plataforma; `start-ui.sh` lo activa por comodidad en desarrollo salvo que pase `--no-observability`.

**Ctrl+C** lo detiene todo (backend, UI y todos los servicios de Docker) y revierte cualquier parche de configuración.

**Ctrl+R** reinicia solo el backend (útil después de un cambio de configuración que la recarga en caliente no detecta).

### Opciones {: #options }

`--no-observability` — Deshabilita el trazado distribuido. De forma predeterminada, `start-ui.sh` descarga el agente Java de OpenTelemetry si aún no está presente, aplica un parche al `jvm.config` de Trino para cargarlo, e inicia el OTel collector, Prometheus, Tempo y Grafana. Pase `--no-observability` para omitir todo eso. El parche de `jvm.config` se revierte al presionar Ctrl+C.

`--seed-data` — Siembra Kafka con datos de demostración una vez que los servicios de Docker están en buen estado. No se ejecuta de forma predeterminada.

`--keep-docker` — Deja los servicios de Docker Compose en ejecución después de Ctrl+C en lugar de llamar a `docker compose down`.

`--reset-volumes` — Elimina todos los volúmenes de Docker y reinicia con un estado limpio. Útil para la recuperación ante fallos de Docker.

`--demo` — Inicia orígenes de datos de demostración adicionales (esquema PostgreSQL pet-store, mock de OpenAPI petstore, SQLite y un GraphQL remoto). Siembra automáticamente usuarios y pedidos de petstore.

`--source=<name>` (solo `start-ui-install.sh`, repetible) — Aprovisiona un origen de datos opcional junto con `--demo`. Cada nombre corresponde a `demo/sources/<name>/`. El inicio llama a `demo/sources/provision.py up`, que levanta el `compose.yml` del origen como su propio proyecto de Docker Compose (`provisa-demo-<name>`), espera su comprobación de estado (health check) y ejecuta `prime.py` cuando el origen tiene uno para sembrar datos. Luego el inicio escribe una configuración envolvente en `${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml` que incluye la configuración base más el `fragment.yaml` de cada origen, y arranca desde ella.

El mismo `provision.py` es lo que llama la suite de extremo a extremo de la UI para levantar estos orígenes (bajo el prefijo de proyecto `provisa-e2e-<name>` en sus propios puertos), de modo que los datos de siembra que muestra una demo y las filas que verifica la suite se definen una sola vez.

En un inicio con Docker (sin `--demo`/`--native`) el coordinador es un contenedor, por lo que cada origen se une a la red del stack principal y se registra como `<name>:<container port>`; en un inicio nativo se registra como `localhost:<published port>`. Se rechaza un origen cuyo archivo `demo/sources/<name>/engine` nombre un motor que el inicio no ejecuta.

Orígenes incluidos:

| Nombre | Puerto(s) | Notas |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | Dos tablas Cypher (`adopter`, `adopter_referral`); grafo sembrado por `seed.cypher`; tablas registradas desde el fragmento |
| `mongodb` | 27117 | Origen registrado; colección `product_reviews` sembrada por `db/mongo-init.js`; registre las tablas manualmente mediante Register Table |
| `redis` | 26379 | Origen registrado; hashes `support_agent:*` y `agent_status:*` sembrados por `prime.py`; cada prefijo se registra como tabla mediante Register Table |
| `cassandra` | 29042 | Origen registrado; `shelter_ops.intake_events` sembrado por `prime.py` (necesita el extra `cassandra`); el keyspace se registra como esquema mediante Register Table |
| `sparql` | 23030 | Apache Jena Fuseki; origen y una tabla respaldada por consulta (`volunteer`) registrados desde el fragmento, grafo sembrado por `prime.py`; más tablas mediante Register Table (consulta + Preview) |
| `prometheus` | 29090 | Origen registrado; el servidor se autoescanea, por lo que `up` y las métricas `prometheus_*` se registran como tablas mediante Register Table |
| `elasticsearch` | 29200 | Origen y mapeo de índice registrados; índice `support_tickets` sembrado por `prime.py`; leído por HTTP por el motor nativo, a través del conector en Trino |
| `splunk` | mgmt 8089, HEC 8088 | Origen registrado con autenticación por token y `disable_ssl_validation` (el certificado del contenedor es autofirmado); un índice, siete eventos de alerta de refugio y el Data Model `shelter_alerts` sembrados por `prime.py`, que también genera el token de API que el fragmento lee como `PROVISA_DEMO_SPLUNK_TOKEN`. Los Data Models se registran como tablas mediante Register Table: en Trino a través del catálogo `splunk`, y en el resto de los motores a través del endpoint pgwire del driver de Splunk, que el motor adjunta |
| `chinook` | 25433 | Postgres con el subconjunto Chinook en snake_case que rastrean los metadatos de muestra de Hasura, sembrado por `prime.py` desde `tests/fixtures/hasura_v2_t1_seed.sql`; origen registrado desde el fragmento, y el origen sobre el que aterriza una importación de Hasura v2 de `tests/fixtures/hasura_v2_t1_metadata.json` |

`--idp=basic|firebase` — Habilita un proveedor de identidad para la autenticación. Sin este indicador, el backend se ejecuta sin proveedor de autenticación y todas las solicitudes se tratan como `admin`.

---

## 3. Conectar un origen de datos {: #3-connect-a-data-source }

Provisa lee su configuración desde `config/`. Añada un archivo de origen, por ejemplo `config/sources/my-db.yaml`:

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

Defina la variable de entorno y el backend la detectará en la siguiente recarga:

```bash
export MY_DB_PASSWORD=secret
```

Consulte [docs/configuration.md](configuration.md) para la referencia YAML completa y todos los tipos de origen admitidos.

---

## 4. Ejecutar su primera consulta {: #4-run-your-first-query }

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

No se requiere autenticación cuando no hay una sección `auth` presente en `config/provisa.yaml` (el valor predeterminado en desarrollo). El rol predeterminado es `admin`.

---

## 5. Abrir la UI {: #5-open-the-ui }

Abra `http://localhost:3000` en un navegador.

La barra de navegación tiene cuatro menús de nivel superior:

- **Explore** — Explorador de esquemas (`/schema`), editor de GraphQL (`/query`), editor de Cypher (`/graph`), editor de SQL (`/sql`)
- **Model** — Vistas y comandos
- **Security** — Seguridad de nivel de fila y políticas de enmascaramiento de columnas
- **Admin** — Resumen, dominios, caché, tareas programadas, estado del sistema, observabilidad, usuarios, organizaciones, roles

La API GraphQL de administración está en `http://localhost:8001/admin/graphql`.

---

## Solución de problemas {: #troubleshooting }

**El backend no inicia** — revise `.logs/server.log`. La causa más común es una variable de entorno faltante o un conflicto de puertos en el 8001.

**Los servicios de Docker no están en buen estado** — ejecute `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` para ver qué servicio está atascado. El motor de federación tarda ~30 segundos en el primer inicio.

**Conflicto de puertos en el 3000 u 8001** — `start-ui.sh` finaliza los procesos obsoletos en esos puertos antes de iniciar. Si algo más está usando el puerto, deténgalo manualmente primero.

**Inicio limpio** — detenga el script y luego ejecute `./start-ui.sh --reset-volumes` para eliminar todos los volúmenes y reiniciar.

---

## Próximos pasos {: #next-steps }

| Objetivo | Documento |
| ------ | ----- |
| Referencia completa de configuración YAML | [configuration.md](configuration.md) |
| Seguridad de nivel de fila, enmascaramiento de columnas, autenticación | [security.md](security.md) |
| Todos los tipos de origen admitidos | [sources.md](sources.md) |
| Suscripciones en tiempo real | [subscriptions.md](subscriptions.md) |
| JDBC, herramientas de BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Cliente de Python | [python-client.md](python-client.md) |
| Implementación en producción | [deployment.md](deployment.md) |
