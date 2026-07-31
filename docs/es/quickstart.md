# Guía rápida para desarrolladores

Para evaluar Provisa sin compilar desde el código fuente, consulte [Guía rápida](index.md) — descargue el instalador para macOS, Windows o Linux y ejecute `provisa start`. (REQ-223, REQ-224, REQ-227)

Esta guía es para ejecutar Provisa **desde el repositorio** — desarrollo activo, depuración o contribución.

---

## Requisitos previos

- **Docker Desktop** (en ejecución)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Clonar y configurar

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` crea `.venv/`, instala todas las dependencias de Python mediante `pip install -e ".[dev]"`, y configura los git hooks en `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. Iniciar todo

```bash
./start-ui.sh
```

Cuando termine de iniciar, verá:

```
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Qué inicia:** [tool-verified: start-ui.sh]

- Servicios principales de Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- Superposición de desarrollo de Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- API de backend en el puerto 8001 (recarga en caliente ante cambios en `provisa/` y `config/`) (REQ-618)
- Servidor de desarrollo Vite de la UI en el puerto 3000 (HMR)
- Trazado de OpenTelemetry y Grafana en `http://localhost:3100`. El stack de observabilidad es un perfil `observability` opcional de docker-compose (OTel Collector, Prometheus, Tempo, Grafana), no activo de forma predeterminada a nivel de la plataforma; `start-ui.sh` lo habilita como comodidad del script de desarrollo a menos que pase `--no-observability`. (REQ-302, REQ-303, REQ-330)

**Ctrl+C** detiene todo — backend, UI y todos los servicios de Docker — y revierte cualquier parche de configuración. (REQ-619)

**Ctrl+R** reinicia solo el backend (útil después de un cambio de configuración que la recarga en caliente no detecta). (REQ-619)

### Opciones

`--no-observability` — Deshabilita el trazado distribuido. De forma predeterminada, `start-ui.sh` descarga el agente Java de OpenTelemetry si aún no está presente, aplica un parche al `jvm.config` de Trino para cargarlo, e inicia el OTel collector, Prometheus, Tempo y Grafana. Pase `--no-observability` para omitir todo eso. El parche de `jvm.config` se revierte al presionar Ctrl+C. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — Siembra Kafka con datos de demostración una vez que los servicios de Docker están en buen estado. No se ejecuta de forma predeterminada. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — Deja los servicios de Docker Compose en ejecución después de Ctrl+C en lugar de llamar a `docker compose down`. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — Elimina todos los volúmenes de Docker y reinicia con un estado limpio. Útil para la recuperación ante fallos de Docker. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — Inicia orígenes de datos de demostración adicionales (esquema PostgreSQL pet-store, mock de OpenAPI petstore, SQLite y un GraphQL remoto). Siembra automáticamente usuarios y pedidos de petstore. [tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` — Habilita un proveedor de identidad para la autenticación. Sin este indicador, el backend se ejecuta sin proveedor de autenticación y todas las solicitudes se tratan como `admin`. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. Conectar un origen de datos

Provisa lee la configuración desde `config/`. Agregue un archivo de origen — por ejemplo `config/sources/my-db.yaml`:

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

## 4. Ejecutar su primera consulta

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

No se requiere autenticación cuando no hay una sección `auth` presente en `config/provisa.yaml` (el valor predeterminado en desarrollo). El rol predeterminado es `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. Abrir la UI

Abra `http://localhost:3000` en un navegador.

La barra de navegación tiene cuatro menús de nivel superior: [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — Explorador de esquemas (`/schema`), editor de GraphQL (`/query`), editor de Cypher (`/graph`), editor de SQL (`/sql`)
- **Model** — Vistas y comandos
- **Security** — Seguridad de nivel de fila y políticas de enmascaramiento de columnas (REQ-038, REQ-041)
- **Admin** — Resumen, dominios, caché, tareas programadas, estado del sistema, observabilidad, usuarios, organizaciones, roles

La API GraphQL de administración está en `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## Solución de problemas

**El backend no inicia** — revise `.logs/server.log`. La causa más común es una variable de entorno faltante o un conflicto de puertos en el 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**Los servicios de Docker no están en buen estado** — ejecute `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` para ver qué servicio está atascado. El motor de federación tarda ~30 segundos en el primer inicio. (REQ-055)

**Conflicto de puertos en el 3000 u 8001** — `start-ui.sh` finaliza los procesos obsoletos en esos puertos antes de iniciar. Si algo más está usando el puerto, deténgalo manualmente primero. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**Inicio limpio** — detenga el script y luego ejecute `./start-ui.sh --reset-volumes` para eliminar todos los volúmenes y reiniciar. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## Próximos pasos

| Objetivo | Documento |
|------|-----|
| Referencia completa de configuración YAML | [configuration.md](configuration.md) |
| Seguridad de nivel de fila, enmascaramiento de columnas, autenticación | [security.md](security.md) |
| Todos los tipos de origen admitidos | [sources.md](sources.md) |
| Suscripciones en tiempo real | [subscriptions.md](subscriptions.md) |
| JDBC, herramientas de BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Cliente de Python | [python-client.md](python-client.md) |
| Implementación en producción | [deployment.md](deployment.md) |
