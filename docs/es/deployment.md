# Despliegue

## Elegir una ruta de despliegue

Provisa admite seis rutas de despliegue. Elija según su audiencia y su contexto operativo:

| Ruta | Artefacto / Script | Ideal para |
| ------ | ------------------- | ---------- |
| **Desarrollo** | `start-ui.sh` | Desarrollo desde el código fuente, evaluación con datos de demostración completos |
| **Instalador de macOS** | `Provisa-<version>-macOS.dmg` | Estaciones de trabajo de desarrolladores, evaluación |
| **Instalador de Windows** | `Provisa-<version>-windows-x64.exe` | Estaciones de trabajo de desarrolladores, evaluación |
| **AppImage de Linux** | `Provisa.AppImage` | Servidores on-prem, VM en la nube, entornos air-gapped |
| **VM en la nube (AWS)** | `terraform/deploy.sh` | Despliegue multinodo en la nube con balanceadores de carga |
| **Kubernetes** | `helm/provisa/` | Equipos que ya operan K8s |

### VM vs. Kubernetes

Ambas son de nivel empresarial. La ruta VM/AppImage es más simple: no hay que aprovisionar un clúster, no hay que configurar políticas de CNI o RBAC, y el AppImage es totalmente autocontenido (REQ-223). Se integra de forma natural en las herramientas de administración de servidores existentes (Ansible, Puppet, agentes de Datadog, forwarders de Splunk, etc.).

Elija Kubernetes solo si su equipo ya opera un clúster de K8s y quiere que Provisa participe en ese modelo operativo (despliegues rolling, HPA, observabilidad unificada) (REQ-056). Las capacidades son equivalentes: Kubernetes agrega sobrecarga operativa, no capacidad.

### Adquisición de imágenes y análisis de seguridad

Todas las rutas de producción requieren obtener los artefactos de Provisa antes de que pueda ejecutarse cualquier despliegue. "Air-gapped" se refiere a lo que ocurre en el momento de la instalación en la máquina de destino: los artefactos deben adquirirse primero.

**Instaladores de macOS y Windows:** Descargue desde la [página de releases de GitHub](https://github.com/provisa/provisa/releases). Totalmente empaquetado; no se requiere internet después de la descarga (REQ-227). Pensado para desarrollo/evaluación, no para producción; no se espera una puerta de análisis de imágenes.

**Ruta AppImage:** Descargue desde la [página de releases de GitHub](https://github.com/provisa/provisa/releases) y transfiera a la máquina de destino. El AppImage empaqueta todas las imágenes de los componentes como archivos tar dentro de un sistema de archivos squashfs (REQ-294); la mayoría de los escáneres de registros no pueden inspeccionarlas in situ. Contacte a su equipo de cuenta de Provisa para obtener los digests de las imágenes de los componentes y verificarlos de forma independiente contra su escáner.

**Ruta Terraform:** El AppImage debe subirse a S3 antes de ejecutar `terraform/deploy.sh`. Los nodos EC2 lo descargan al arrancar mediante un rol de IAM; requieren acceso saliente a S3 (directo o a través de un endpoint de gateway de VPC). Aplique la misma política de análisis que la ruta AppImage.

**Ruta Helm / Kubernetes:** Las imágenes individuales deben subirse a un registro que el clúster pueda alcanzar. Esta ruta es la más compatible con el análisis basado en registros (Prisma Cloud, Aqua, Trivy, AWS Inspector): las imágenes son objetos de primera clase que los escáneres entienden de forma nativa. Para clústeres air-gapped, replique las imágenes a un registro interno y sobrescriba las referencias en `values.yaml` (REQ-294).

---

## Desarrollo (desde el código fuente)

### Recomendado: `start-ui.sh`

La forma más sencilla de ejecutar Provisa desde el código fuente. Inicia toda la infraestructura, la API del backend y el servidor de desarrollo de la UI en un solo comando (REQ-055). Ctrl+C detiene todo de forma ordenada.

**Requisitos previos:** Docker Desktop, Node.js, entorno virtual de Python en `.venv/`

```bash
./start-ui.sh
```

Qué hace:

- Inicia `docker-compose.core.yml` + `docker-compose.dev.yml` (todos los servicios core + demo) y espera a que estén healthy (REQ-055)
- Carga Kafka con datos de demostración
- Sincroniza las dependencias de Python desde `.venv/`
- Inicia la API del backend en el puerto 8001 (registra en `.logs/server.log`) (REQ-558)
- Inicia el servidor de desarrollo Vite de la UI en el puerto 3000 (REQ-559)
- Imprime las URL y espera; Ctrl+C detiene todo y desmonta compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Opciones:**

`--reset-volumes` — Ejecuta `docker compose down -v` antes de iniciar, destruyendo todos los volúmenes de Docker (datos de PostgreSQL, objetos de MinIO, estado de Redis, etc.) (REQ-170). Úselo cuando quiera partir de cero por completo, después de un cambio de esquema durante el desarrollo, o cuando Docker se haya bloqueado y haya dejado volúmenes corruptos. **Se perderán todos los datos.**

`--observability` — Agrega instrumentación completa de trazas y métricas. Descarga el agente Java de OpenTelemetry y parchea el `jvm.config` de Trino para cargarlo, instrumenta el backend de Provisa con exportación OTLP, e inicia el colector OTel, Prometheus, Tempo y Grafana (`http://localhost:3100`) (REQ-330). El parche de `jvm.config` se revierte automáticamente al presionar Ctrl+C.

### Pasos manuales (solo backend, sin UI)

Si solo necesita la API:

1. Instale [Docker Desktop](https://docs.docker.com/get-docker/)
2. Inicie los servicios core:

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. Inicie la API:

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. Verifique: `curl http://localhost:8001/health`

### Stack completo (Provisa en contenedor)

Para ejecutar la API como contenedor en lugar de en el host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Servicios

**Core (`docker-compose.core.yml`) — siempre requerido:**

| Servicio | Puerto | Propósito |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Metadatos de configuración + catálogo de Iceberg (REQ-169) |
| PgBouncer | 6432 | Agrupación de conexiones (REQ-053) |
| Motor de federación | 8080 | Federación de consultas (REQ-028) |
| Redis | 6379 | Caché de resultados de consultas (REQ-371) |
| MinIO | 9000/9001 | Almacenamiento de objetos compatible con S3 (REQ-029, REQ-171) |

**Demo (`docker-compose.dev.yml`) — opcional, incluido por `start-ui.sh`:**

| Servicio | Puerto | Propósito |
| --------- | ------ | --------- |
| MongoDB | 27017 | Origen NoSQL de demostración |
| Kafka | 9092 | Origen de streaming de demostración |
| Schema Registry | 8081 | Gestión de esquemas Avro/Protobuf de demostración |
| Debezium | — | Conector CDC de demostración |
| Elasticsearch | 9200 | Origen de búsqueda de demostración |
| Neo4j | 7474/7687 | Origen de grafos de demostración |
| Fuseki | 3030 | Triplestore SPARQL de demostración |
| OpenTelemetry Collector | — | Recolección de trazas (con `--observability`) (REQ-302) |
| Prometheus | 9090 | Métricas (con `--observability`) (REQ-330) |
| Tempo | — | Almacenamiento de trazas (con `--observability`) (REQ-330) |
| Grafana | 3100 | Paneles (con `--observability`) (REQ-330) |

### Backend de telemetría (`otlp2sql`)

El stack de `--observability` anterior (Collector → Tempo/Prometheus/Grafana) es una
ruta de telemetría. La otra es `otlp2sql` (`provisa.observability.otlp2sql`): un
receptor OTLP/HTTP que escribe trazas, métricas y registros en una base de datos SQL
elegida mediante una URL de SQLAlchemy, extrayendo los atributos de span `provisa.*` en
la ingesta, de modo que no se ejecuta ningún trabajo de compactación separado. Las escrituras se agrupan en lotes
(`OTLP2SQL_BATCH_MAX_ROWS`, por defecto 1000; `OTLP2SQL_BATCH_MAX_SECS`, por defecto 2s).

La telemetría tiene su propio almacén, separado de la base de datos del plano de control. Seleccione
el backend con `PROVISA_OPS_DB_URL`:

| `PROVISA_OPS_DB_URL` | Backend | Notas |
| --- | --- | --- |
| *(sin definir)* | DuckDB dedicado bajo `~/.provisa/telemetry/` | valor por defecto; sin servidor, sin Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | ingesta de alta tasa con fusiones (merges) automáticas en segundo plano |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | volumen moderado |
| `trino://user@host:8080/otel` | Trino / Iceberg | funciona técnicamente, **no recomendado** — ver más abajo |

**Sobre `trino://`:** el dialecto Trino de SQLAlchemy emite DDL e
`INSERT`s de Trino válidos, por lo que es técnicamente factible como backend de `otlp2sql`. No se
recomienda salvo para tasas de ingesta bajas. Cada vaciado de lote se convierte en un
`INSERT` distribuido de Trino más un snapshot de Iceberg, de modo que la telemetría de alta tasa
produce muchos archivos y snapshots pequeños y sigue requiriendo
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` periódicos, algo que `otlp2sql` no
ejecuta. Además, pone al motor de consultas en la ruta crítica de ingesta.

Para telemetría de alto volumen hacia Trino/Iceberg, use `otlp2parquet` en su lugar: este
escribe parquet en el almacenamiento de objetos sin pasar por Trino, y una compactación
programada de Trino incorpora los archivos crudos en las tablas Iceberg activas. Para un único
motor que maneje tanto la ingesta de alta tasa como la compactación, prefiera ClickHouse.

Apunte los exportadores OTLP de la aplicación y de Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) al
endpoint de `otlp2sql`, y registre el dominio de operaciones contra la misma
`PROVISA_OPS_DB_URL` para que lea lo que escribió el receptor.

---

## Instalador de macOS

Para estaciones de trabajo de desarrolladores y evaluación. Totalmente air-gapped: no se requiere internet después de la descarga (REQ-227).

El instalador base es una **instalación nativa**: motor de federación DuckDB + plano de control SQLite + caché en memoria (fakeredis), sin Docker, VM, Trino, Redis ni MinIO (REQ-972, REQ-979). El motor de federación es una elección del asistente: DuckDB (nativo, por defecto), Trino-en-Docker, o un motor externo (REQ-973). La observabilidad es autotelemetría siempre activa visible en Admin; el stack de Docker de collector/Prometheus/Grafana es una demostración externa opcional, no un interruptor de encendido/apagado (REQ-975). El paquete de datos de demostración es opcional y está desactivado por defecto (REQ-978). Trino, el stack de observabilidad de Docker y la demo son complementos pesados que se resuelven priorizando lo local (directorio adyacente al instalador, volúmenes montados, `~/Downloads`, y luego el release de GitHub), de modo que las empresas puedan preposicionar los archivos tar para instalaciones air-gapped (REQ-977).

### Pasos

1. Descargue `Provisa-<version>-macOS.dmg` desde la [página de releases de GitHub](https://github.com/provisa/provisa/releases)
2. Abra el DMG y arrastre **Provisa.app** a `/Applications`
3. Haga doble clic en **Provisa.app**: la configuración de primer lanzamiento se ejecuta una vez; el asistente ofrece las opciones de motor, observabilidad y demo mencionadas arriba (REQ-1007)
4. Abra Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Persistencia de datos

Todos los datos se almacenan en `~/.provisa/` (REQ-224). Para eliminar todo: `provisa uninstall`.

---

## Instalador de Windows

Para estaciones de trabajo de desarrolladores y evaluación. Totalmente air-gapped: no se requiere internet después de la descarga (REQ-227).

Al igual que en macOS, el instalador base de Windows es un **nivel nativo**: un runtime de Python independiente + wheel de provisa + DuckDB/pg_duckdb + plano de control SQLite, sin incluir Docker, VM ni imágenes de contenedor (REQ-979). El motor de federación (Trino), el stack de observabilidad y el paquete de datos de demostración se agregan después mediante instaladores en capas separados, en orden: el instalador Container (`Provisa-Container-<version>.exe`, que agrega WSL2 + containerd + Trino), luego el instalador Obs (requiere el nivel container), luego el instalador Demo (requiere Core + Obs). La guía de primer lanzamiento explica cómo inicializar el motor de federación ejecutando el instalador Container (REQ-1005).

### Pasos

1. Descargue `Provisa-<version>-windows-x64.exe` desde la [página de releases de GitHub](https://github.com/provisa/provisa/releases)
2. Ejecute el instalador; no se requieren derechos de administrador; instala en `%LOCALAPPDATA%\Programs\Provisa\`
3. Abra **Provisa First Launch** desde el menú Inicio: la configuración nativa se ejecuta una vez e imprime la guía de próximos pasos para los complementos en capas (REQ-1005)
4. Abra una nueva terminal:

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### Persistencia de datos

Todos los datos se almacenan en `%USERPROFILE%\.provisa\`.

---

## AppImage de Linux — VM de nodo único o multinodo

### Qué es

`Provisa.AppImage` es un único ejecutable autocontenido que empaqueta (REQ-223, REQ-228):

- Un daemon de Docker sin privilegios de root (`dockerd-rootless.sh` + `rootlesskit`); no se requiere Docker del sistema ni root
- Todos los archivos tar de las imágenes de contenedor (PostgreSQL, PgBouncer, MinIO, Redis, motor de Federación, API de Provisa) (REQ-294)
- El wrapper de la CLI de Provisa y el script de configuración de primer lanzamiento

La imagen de Provisa se compila previamente en el momento del empaquetado; el código fuente de Python nunca se incluye.

### Cuándo usarlo

- Servidores físicos on-premises o VM (nodo único o multinodo)
- VM en la nube sin un clúster de K8s
- Entornos air-gapped (REQ-294)
- Cuando desee operaciones más simples que Kubernetes

---

### Pasos — Nodo único

1. Descargue `Provisa.AppImage` desde la [página de releases de GitHub](https://github.com/provisa/provisa/releases) y transfiéralo a la máquina de destino
2. Hágalo ejecutable:

   ```bash
   chmod +x Provisa.AppImage
   ```

3. Ejecute la configuración de primer lanzamiento:

   ```bash
   ./Provisa.AppImage
   ```

4. El asistente de configuración pregunta:
   - **Rol** → seleccione `primary`
   - **Presupuesto de RAM** → cantidad de RAM a asignar (0 = toda la disponible); determina el número de workers de Trino
   - **Nombre de host** → la dirección anunciada de este nodo
   - **Puerto de la API** → por defecto `8000` (REQ-560)
5. La configuración carga todas las imágenes de contenedor (~2–5 minutos), escribe la configuración e inicia los servicios
6. Verifique:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Pasos — Multinodo (Primario)

Ejecute estos pasos primero en el nodo primario. Los secundarios deben configurarse después de que el primario esté en ejecución.

1. Descargue y transfiera `Provisa.AppImage` a la máquina primaria
2. Abra los puertos de firewall necesarios (los secundarios se conectarán a estos de forma entrante):

   | Puerto | Servicio |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Coordinador del motor de federación |
   | 8000 | API de Provisa |

3. Hágalo ejecutable y ejecútelo:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. El asistente de configuración pregunta:
   - **Rol** → seleccione `primary`
   - **Presupuesto de RAM**, **nombre de host**, **puerto de la API** → responda como en el nodo único
5. Después de completar la configuración, anote la **IP privada** de esta máquina; los secundarios la necesitan
6. El asistente imprime un bloque upstream de nginx; guárdelo para la configuración de su balanceador de carga
7. Verifique:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Pasos — Multinodo (Cada secundario)

Repita estos pasos en cada nodo adicional después de que el primario esté en ejecución y sea accesible.

1. Descargue y transfiera `Provisa.AppImage` a la máquina secundaria
2. Confirme que el secundario puede alcanzar al primario:

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. Hágalo ejecutable y ejecútelo:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. El asistente de configuración pregunta:
   - **Rol** → seleccione `secondary`
   - **IP del primario** → introduzca la IP del nodo primario (la conectividad se verifica en vivo)
   - **Presupuesto de RAM**, **nombre de host**, **puerto de la API** → responda como arriba
5. La configuración carga un conjunto de imágenes reducido (sin PostgreSQL, PgBouncer, MinIO, Redis; esos se ejecutan solo en el primario) (REQ-561), e inicia la API de Provisa y un worker del motor de federación
6. Verifique:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. Agregue este nodo al upstream de su balanceador de carga

---

### Topología primario/secundario

**El nodo primario** ejecuta todos los servicios singleton:

| Servicio | Por qué es singleton |
| --------- | --------------- |
| PostgreSQL | Esquema compartido, configuración de la aplicación, modelo semántico |
| Redis | Caché de resultados de consultas compartida y estado de suscripción (REQ-371) |
| MinIO | Almacén de objetos compartido para resultados de redirección y snapshots de vistas materializadas (REQ-029) |
| Coordinador del motor de federación | Todos los workers (primario + secundarios) se registran aquí (REQ-028) |

**Los nodos secundarios** ejecutan únicamente:

- API de Provisa: sin estado; lee toda la configuración desde PostgreSQL en el primario al iniciar (REQ-057, REQ-562)
- Worker del motor de federación: se autorregistra con el coordinador en el primario (REQ-028)

Todo el estado de la aplicación fluye a través del PostgreSQL del primario. No se requiere sincronización manual. (REQ-562)

---

### Primer lanzamiento no interactivo (automatizado)

Para Terraform, cloud-init o Ansible: pase flags en lugar de responder a las preguntas:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

El modo no interactivo instala una unidad systemd (`/etc/systemd/system/provisa.service`) para el arranque en el boot. (REQ-563)

| Flag | Descripción |
| ------ | ------------- |
| `--non-interactive` | Omite todas las preguntas; instala la unidad systemd |
| `--role primary\|secondary` | Rol del nodo |
| `--primary-ip <ip>` | IP del nodo primario (obligatorio para el secundario) |
| `--ram-gb <n>` | RAM a asignar (0 = toda la disponible) |

---

## Despliegue en VM en la nube — Terraform (AWS)

Aprovisiona un clúster de Provisa multinodo completo en AWS: VPC, grupos de seguridad, instancias EC2, ALB, NLB, en un único comando interactivo. (REQ-564)

### Archivos

| Archivo | Propósito |
| ------ | --------- |
| `terraform/deploy.sh` | Wrapper interactivo: recopila parámetros, valida credenciales, escribe `terraform.tfvars`, ejecuta apply |
| `terraform/aws/variables.tf` | Todas las definiciones de variables con sus valores por defecto |
| `terraform/aws/main.tf` | VPC, subredes, grupos de seguridad, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL de endpoints e IP de nodos |

### Pasos

1. Descargue `Provisa.AppImage` desde la [página de releases de GitHub](https://github.com/provisa/provisa/releases)

2. Súbalo a un bucket de S3 en su cuenta de AWS:

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Asegúrese de que las credenciales de AWS estén disponibles en su shell (cualquiera de estas):
   - Variables de entorno: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Perfil con nombre: `export AWS_PROFILE=my-profile`
   - Sesión SSO activa: `aws sso login`

4. (Opcional) Si desea acceso SSH a los nodos, cree un par de claves EC2 en su región de destino y anote el nombre del par de claves

5. Ejecute el wrapper de despliegue:

   ```bash
   bash terraform/deploy.sh
   ```

6. Responda las preguntas del asistente (ver la tabla de referencia más abajo). El script verifica que el AppImage exista en S3 antes de continuar y aborta si no es así

7. Revise el resumen del despliegue y confirme

8. Terraform aprovisiona toda la infraestructura (~5–10 minutos). Después de aplicar, el script imprime:

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Opcional) Apunte los registros DNS al ALB y al NLB

10. Verifique:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Preguntas del asistente

| Pregunta | Por defecto | Notas |
| ---------- | --------- | ------- |
| Proveedor de nube | — | Solo AWS por ahora |
| Credenciales de AWS | — | Primero verifica si hay una sesión activa |
| Región | `us-east-1` | |
| Número de nodos | `2` | 1 = solo primario, sin LB; 2+ = primario + secundarios + ALB/NLB |
| Tipo de instancia | `m7i.2xlarge` | Ver la guía de dimensionamiento más abajo |
| Tamaño del volumen raíz | `100 GB` | Por nodo |
| Presupuesto de RAM | `0` (toda la RAM) | Determina el número de workers de Trino por nodo |
| Bucket de S3 | — | Verificado en vivo antes de continuar |
| Clave de S3 | `releases/Provisa.AppImage` | |
| Acceso SSH | No | Requiere un nombre de par de claves existente + CIDR de administración |
| CIDR de la VPC | `10.0.0.0/16` | |

### Guía de dimensionamiento de instancias

| Tipo | vCPU | RAM | Workers de Trino/nodo | Caso de uso |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Desarrollo / conjuntos de datos pequeños |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Producción pequeña |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Producción mediana |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Producción grande |

Todos los nodos aportan workers a un único coordinador en el primario (REQ-028). Un clúster de 3 nodos `m7i.4xlarge` produce 6 workers de Trino en total.

### Qué se aprovisiona

- VPC con dos subredes públicas en dos zonas de disponibilidad (REQ-564)
- Grupos de seguridad: grupo de LB (ingreso público en 8000/8815), grupo de nodos (LB → nodos, intra-clúster, SSH opcional)
- Rol de IAM + perfil de instancia con S3 GetObject en el bucket del AppImage
- Instancia EC2 primaria: ejecuta el primer lanzamiento en modo `--non-interactive --role primary`
- Instancias EC2 secundarias (node_count − 1): ejecutan el primer lanzamiento en modo `--non-interactive --role secondary --primary-ip <primary private IP>`; dependen de que el primario termine primero
- ALB en el puerto 8000: API HTTP, health-check en `/health` (REQ-560)
- NLB en el puerto 8815: Arrow Flight / gRPC (REQ-143)
- Ambos LB se conectan a todos los nodos

### Lista de verificación de requisitos previos

- [ ] Permisos de IAM: EC2 completo, ELB completo, VPC completo, creación de roles de IAM, S3 GetObject en el bucket del AppImage
- [ ] `Provisa.AppImage` subido a S3
- [ ] Los nodos EC2 tienen acceso saliente a S3 (internet directo o endpoint de gateway de VPC de S3)
- [ ] Existe un par de claves EC2 en la región de destino (si se necesita SSH)
- [ ] Terraform ≥ 1.5 instalado localmente
- [ ] Registros DNS planificados para ALB / NLB (opcional pero recomendado)
- [ ] Certificado ACM listo si se requiere HTTPS (no incluido en el Terraform base)

### Secretos

No hay secretos embebidos en Terraform. El AppImage genera credenciales durante el primer lanzamiento y las escribe en `~/.provisa/config.yaml` en cada nodo (REQ-563). Para producción, recupere el token de administrador desde el nodo primario después del despliegue:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Cuándo usarlo

Su equipo ya opera un clúster de Kubernetes y quiere que Provisa participe en ese modelo operativo (REQ-056). Si está evaluando Provisa o desplegando on-premises sin un clúster existente, la ruta AppImage es más simple.

Nota: el AppImage de Provisa no puede ejecutarse dentro de un pod de Kubernetes; requiere FUSE y un daemon de Docker sin privilegios de root, que no están disponibles en los perfiles de seguridad de pod estándar.

### Pasos

1. Confirme el acceso al clúster:

   ```bash
   kubectl cluster-info
   ```

2. Extraiga y replique las imágenes a su registro interno (requerido para entornos air-gapped o analizados; omita si extrae directamente de registros públicos) (REQ-294):

   | Imagen | Se usa para |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API de Provisa |
   | `trinodb/trino:480` | Coordinador y workers del motor de federación (REQ-169) |
   | `postgres:16` | PostgreSQL en el clúster (si `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer en el clúster (si `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis en el clúster (si `redis.enabled` y no hay `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO en el clúster (si `minio.enabled`) (REQ-029) |

   Para entornos con análisis de registro:
   - Suba cada imagen a su registro de staging
   - Ejecute su escáner (Prisma Cloud, Aqua, Trivy, AWS Inspector) y obtenga la aprobación
   - Promueva a su registro interno de producción

3. Decida antes de instalar:
   - **PostgreSQL** — ¿en el clúster (`postgresql.enabled: true`) o gestionado externamente (`postgresql.host`)? Se recomienda externo para producción
   - **Redis** — ¿en el clúster o externo (`redis.host`)? Cambie la contraseña por defecto (`redis.password`)
   - **MinIO / S3** — ¿MinIO en el clúster o S3 nativo? Para AWS, use S3 con un rol de IAM
   - **Secretos** — páselos vía `--set` para evaluación; use External Secrets o Vault Agent para producción

4. Instale el chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Si usa un registro interno, agregue las sobrescrituras de imagen:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Verifique que los pods estén en ejecución:

   ```bash
   kubectl get pods -n provisa
   ```

6. Verifique la API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Opcional) Habilite el ingress para acceso externo: configure `ingress.enabled: true` y ajuste su controlador de ingress

### Lista de verificación de requisitos previos

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Clase de almacenamiento que admita PVC `ReadWriteOnce` (para servicios con estado en el clúster)
- [ ] Imágenes disponibles para el clúster (registro público o interno)
- [ ] Endpoint de PostgreSQL + credenciales (si es externo)
- [ ] Endpoint de Redis + credenciales (si es externo)
- [ ] Bucket de S3 + credenciales o rol de IAM
- [ ] Token de administrador elegido
- [ ] Controlador de ingress configurado (si se necesita acceso externo)

### Valores clave

| Valor | Por defecto | Descripción |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Réplicas de la API de Provisa (sin estado) (REQ-057) |
| `config.pgHost` | `postgres` | Host de PostgreSQL |
| `config.pgPassword` | | Contraseña de PostgreSQL |
| `config.adminToken` | | Token bearer de la API de administrador |
| `redis.enabled` | `true` | Despliega un StatefulSet de Redis en el clúster (REQ-371) |
| `redis.host` | `""` | Configúrelo para usar Redis externo |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | Cambie esto |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Despliega el motor de federación (REQ-028) |
| `trino.workers` | `2` | Réplicas de workers del motor de federación (REQ-056) |
| `postgresql.enabled` | `true` | Despliega PostgreSQL en el clúster (REQ-169) |
| `postgresql.host` | `""` | Configúrelo para usar PostgreSQL externo |
| `minio.enabled` | `true` | Despliega MinIO en el clúster (REQ-029) |
| `s3.endpoint` | | URL del endpoint compatible con S3 |
| `s3.bucket` | `provisa-results` | Bucket para la redirección de resultados grandes (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Habilita el ingress |

### Escalado

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Los workers del motor de federación escalan de forma independiente: más workers aumentan el throughput y la capacidad de consultas concurrentes (REQ-056). (REQ-057)

### Actualización de la configuración

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Alta disponibilidad y recuperación

Provisa aplica un modelo de recuperación de dos niveles en todos los modos de despliegue (REQ-703):

- **Nivel 1 — errores transitorios.** Las operaciones de lectura se reintentan durante hasta 30 segundos ante errores transitorios usando retroceso exponencial (exponential backoff) con jitter completo. Ajuste el presupuesto con `PROVISA_RETRY_BUDGET_SECS`. Las operaciones de escritura nunca se reintentan internamente, y los errores de memoria nunca son reintentables.
- **Nivel 2 — fallo de componente.** Un watcher interno del motor detecta y reinicia los componentes de software fallidos en 2–3 minutos.

Los fallos a nivel de máquina y de clúster siguen siendo responsabilidad del operador: aprovisione nodos redundantes y un balanceador de carga (rutas Terraform y Helm anteriores) para tolerancia a la pérdida de nodos.

## Dependencias del motor de federación

Los motores de federación de almacenes de datos requieren paquetes de Python y componentes a nivel de sistema más allá de la instalación por defecto de Provisa. Todos los paquetes de Python listados aquí están declarados en `pyproject.toml` y se instalan como parte de `pip install provisa` o `pip install -e .` estándar [tool-verified: `pyproject.toml` lines 44–52].

Los paquetes de Python se incluyen en la instalación por defecto de Provisa; no se requieren extras opcionales para ningún motor de almacén de datos. Los elementos a nivel de sistema (driver ODBC, CLI de nube, claves de cuenta de servicio) deben instalarse por separado.

### Paquetes de Python (ya en las dependencias core)

[tool-verified: `pyproject.toml` lines 41–52]

| Paquete | Motor | Propósito |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Conexión al almacén SQL; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Conexión + `fetch_arrow_table` nativo de Arrow (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Ejecución de consultas |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API para lecturas nativas de Arrow |
| `google-cloud-storage` | BigQuery | Staging en GCS para enlaces de tablas externas |
| `pyodbc` | Fabric, Synapse | Conexión ODBC a endpoints T-SQL |
| `azure-identity` | Fabric, Synapse | Token de Azure AD vía `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Lecturas columnares por HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Fijación de compatibilidad: `google-cloud-*` y OTel comparten un runtime de protobuf; `<7` los mantiene alineados |
| `grpcio-status<1.82` | gRPC | Se alinea con la fijación `protobuf<7` |

### Requisitos a nivel de sistema

Estos no son paquetes de Python; deben instalarse en el host o contenedor que ejecuta Provisa.

**Microsoft Fabric y Azure Synapse (ODBC)**

`pyodbc` se conecta a través del driver ODBC de Microsoft para SQL Server (`msodbcsql18`). El driver debe instalarse en el host, no vía pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

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

Provisa detecta el driver automáticamente. Para sobrescribir el nombre del driver (en instalaciones no estándar), configure:

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Autenticación de Azure AD (Fabric y Synapse)**

Ambos motores se autentican vía `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` verifica las fuentes de credenciales en orden: variables de entorno, identidad de carga de trabajo, identidad administrada, VS Code, `az login`, y otras.

Para desarrollo local, `az login` es la ruta más simple:

```bash
az login
```

Para producción, use identidad administrada (en VM de Azure o AKS); no se necesita gestión de credenciales. Para autenticación mediante entidad de servicio, configure:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (cuenta de servicio)**

`google-cloud-bigquery` usa credenciales predeterminadas de la aplicación (Application Default Credentials). Para desarrollo local, apunte a un archivo de clave de cuenta de servicio:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

Para producción en GCP (Cloud Run, GKE con Workload Identity, Compute Engine), la biblioteca detecta automáticamente la cuenta de servicio adjunta; no se necesita variable de entorno.

La cuenta de servicio necesita:

- `roles/bigquery.dataViewer` — leer datos
- `roles/bigquery.jobUser` — ejecutar consultas
- `roles/bigquery.dataEditor` — crear tablas externas (para ATTACH)
- `roles/storage.objectViewer` — leer objetos de GCS para tablas externas

**Databricks (certificado CA en entornos de proxy de desarrollo)**

Si Provisa se ejecuta detrás de un proxy que intercepta TLS (Charles, mitmproxy, proxies corporativos), el conector SQL de Databricks puede rechazar el certificado del proxy. Pase un bundle de CA personalizado:

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

El conector de Databricks hereda esto de `requests`; no se necesita ninguna variable de entorno específica de Databricks.

### Lista de verificación por motor

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` instalado (por defecto)
- [ ] URL de motor con `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Token de acceso personal o token de entidad de servicio
- [ ] `REQUESTS_CA_BUNDLE` configurado si está detrás de un proxy que intercepta TLS

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` instalado (por defecto)
- [ ] URL de motor: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` en `PROVISA_ENGINE_URL` o `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` instalados (por defecto)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` configurado (dev) o identidad de carga de trabajo configurada (prod)
- [ ] `GOOGLE_CLOUD_PROJECT` configurado si el proyecto no puede inferirse de la cuenta de servicio
- [ ] La cuenta de servicio tiene los roles BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` + `azure-identity` instalados (por defecto)
- [ ] Driver de sistema `msodbcsql18` instalado
- [ ] `FABRIC_SQL_SERVER` y `FABRIC_DATABASE` configurados
- [ ] Autenticación de Azure AD: `az login` (dev) o identidad administrada / entidad de servicio (prod)
- [ ] `FABRIC_WORKSPACE_ID` configurado si se usan enlaces externos de almacenamiento de objetos

**Azure Synapse** (REQ-989)

- [ ] Mismos requisitos de Python y sistema que Fabric
- [ ] `SYNAPSE_SQL_SERVER` y `SYNAPSE_DATABASE` configurados
- [ ] Misma configuración de autenticación de Azure AD que Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` instalado (por defecto)
- [ ] URL de motor: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` en `federation_hints` para TLS (puerto 8443)

---

## Variables de entorno

| Variable | Por defecto | Propósito |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Contraseña de PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Ruta al archivo de configuración (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Habilita la redirección de resultados grandes a S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Umbral de número de filas para la redirección (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket de S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL del endpoint compatible con S3 (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de la URL prefirmada (segundos) (REQ-141) |
| `REDIS_HOST` | `localhost` | Host de Redis |
| `REDIS_PORT` | `6379` | Puerto de Redis |
| `REDIS_PASSWORD` | | Contraseña de Redis |
| `REDIS_TLS` | `false` | Habilita TLS para Redis |
| `TRINO_HOST` | `localhost` | Host del coordinador del motor de federación Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | Puerto HTTP del coordinador del motor de federación Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Clave del motor de federación activo (REQ-989); sobrescribe la configuración persistida |
| `PROVISA_ENGINE_URL` | | URL de conexión para motores dirigidos por URL (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Sobrescritura de la URL del almacén de materialización; por defecto usa el almacén propio del motor |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Nombre del driver ODBC para Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Ruta al JSON de clave de cuenta de servicio de GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | ID del proyecto de GCP (BigQuery; se infiere de la cuenta de servicio si no está configurado) |
| `FABRIC_SQL_SERVER` | | Nombre de host del endpoint analítico SQL de Microsoft Fabric |
| `FABRIC_DATABASE` | | Nombre de la base de datos de Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID del workspace de Fabric (obligatorio para los shortcuts externos de almacenamiento de objetos) |
| `SYNAPSE_SQL_SERVER` | | Nombre de host del pool SQL dedicado o serverless de Azure Synapse |
| `SYNAPSE_DATABASE` | | Nombre de la base de datos de Synapse |
| `AZURE_TENANT_ID` | | Tenant de Azure AD (autenticación de entidad de servicio para Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | ID de cliente de la aplicación de Azure AD |
| `AZURE_CLIENT_SECRET` | | Secreto de cliente de la aplicación de Azure AD |
| `REQUESTS_CA_BUNDLE` | | Ruta al bundle de CA personalizado (conector de Databricks, proxy TLS de desarrollo) |

---

## Comandos de la CLI

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

### Flujo de promoción de configuración (dev → test → prod)

Todos los ajustes específicos del entorno (cadenas de conexión, secretos, puertos) deben ir en variables de entorno o gestores de secretos, no en la configuración exportada. El YAML exportado captura su modelo semántico: orígenes, dominios, roles, vistas. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```


Véase también: [Entornos](environments.md) explica cómo gestionar copias con nombre y aisladas por esquema de su modelo gobernado.
