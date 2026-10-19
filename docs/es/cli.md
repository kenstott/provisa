# Referencia de la CLI {: #cli-reference }

El comando `provisa` es el único punto de entrada del nivel embebido instalado con pip. Arranca el runtime, gestiona licencias, lanza la publicación de metadatos, despliega modelos y controla el banner de mantenimiento. No necesita Docker, ni Node, ni servicios externos.

Instálelo con el extra `embedded`, que también trae las extensiones DuckDB sin conexión
y el plano de control PostgreSQL embebido:

```bash
pip install 'provisa[embedded]'
```

**Requisitos de plataforma.** `provisa run` requiere Python 3.12 y una plataforma con un wheel de
pgserver: linux x86_64, macOS, o Windows x86_64. Linux aarch64 no tiene wheel de pgserver ni
distribución de fuentes, así que el nivel embebido no se ejecuta ahí. Use el nivel de contenedor en
aarch64.

---

## Opciones compartidas {: #shared-options }

Varios subcomandos llaman a la API HTTP de Provisa. Comparten tres flags y dos variables de
entorno.

| Flag | Predeterminado | Fallback a variable de entorno |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(ninguno)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 para `maintenance`) | _(ninguno)_ |

`--api` es la URL base de una instancia de Provisa en ejecución. Con multiinquilino, el host nombra la organización, así que `https://acme.provisa.org` lleva al inquilino de acme. `--token` es un token Bearer. Si está vacío, no se envía ninguna cabecera `Authorization`, que es lo que espera un despliegue sin autenticación.

Establezca ambas variables en su entorno de CI para evitar repetirlas en cada llamada:

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Subcomandos que aceptan estos flags: `metadata export`, cada subcomando `env` (`list`, `show`,
`create`, `delete`, `diff`, `merge`, `deploy`, `revert`, `redo`, `fetch`, `push`, `pull`),
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Arranca el sistema Provisa embebido en un solo proceso: el servidor de la API y el servidor de los archivos estáticos y el proxy de la UI. No intervienen Docker, Node ni servicios externos.

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Flags

| Flag | Predeterminado | Notas |
| --- | --- | --- |
| `--demo` | desactivado | Carga la demo incluida — dominios de ejemplo pet-store y shelter sobre SQLite embebido |
| `--host` | `127.0.0.1` | Dirección de enlace para ambos servidores |
| `--api-port` | `8000` | Puerto del servidor de API |
| `--ui-port` | `3000` | Puerto del servidor estático/proxy de la UI |
| `--no-browser` | desactivado | Omite abrir un navegador cuando la UI está lista; igual imprime la URL |
| `--reset` | desactivado | Descarta y reconstruye el almacén del plano de control embebido antes de iniciar; úselo después de una actualización de Provisa si el arranque reporta un desajuste de esquema |
| `--data-dir` | `~/.provisa/native` | Directorio que contiene el clúster PostgreSQL embebido y la caché de extensiones DuckDB |

### Variables de entorno {: #environment-variables }

`provisa run` lee varias variables adicionales antes de que arranquen los servidores HTTP.
Establézcalas para anular los valores predeterminados que aplicaría `load_profile("native", ...)`.

| Variable | Efecto |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Reemplaza el motor DuckDB embebido por un coordinador Trino suministrado por el cliente |
| `PROVISA_ENGINE_URL` | Forma alternativa de apuntar a un motor de federación externo |
| `PROVISA_CONFIG` | Archivo de configuración a cargar; `--demo` lo establece en la configuración de demo incluida |
| `PROVISA_DEMO` | Establecida en `1` por `--demo`; marca la sesión como una ejecución de demo |
| `PROVISA_DEMO_DIR` | Ruta al directorio de datos de muestra de la demo; establecida por `--demo` |
| `PROVISA_DUCKDB_EXT_DIR` | Directorio de extensiones DuckDB preparado de antemano; se establece automáticamente desde el paquete `provisa-duckdb-ext` si está presente; su ausencia significa que DuckDB descarga desde la red en el primer uso |

### Secuencia de arranque {: #startup-sequence }

1. Verificación de plataforma — aborta con un mensaje claro ante un Python no compatible o un pgserver faltante.
2. `--reset` (si se solicita) — descarta el clúster PostgreSQL embebido; se reconstruye en el siguiente paso.
3. Configuración de demo (si `--demo`) — establece `PROVISA_CONFIG` y `PROVISA_DEMO_DIR`.
4. Entorno embebido — inicia el plano de control PostgreSQL, resuelve su URL de socket, y
   prepara las extensiones DuckDB sin conexión si `provisa-duckdb-ext` está instalado.
5. Verificación de desviación de esquema — escanea el plano de control en vivo en busca de columnas
   faltantes. Si encuentra alguna, imprime una sugerencia de `--reset` y sale con código 1. V1 no
   tiene migraciones; una columna agregada en una versión más nueva requiere un reset.
6. Ambos servidores arrancan de forma concurrente. El anunciador de disponibilidad sondea `GET /ready` (no `/health`, porque el endpoint `/ready` confirma que el almacén está conectado y el motor está caliente) y abre el navegador cuando devuelve 200.

### Códigos de salida {: #exit-codes }

| Código | Significado |
| --- | --- |
| 0 | Apagado limpio (Ctrl-C) |
| 1 | Error de arranque (falló la verificación de plataforma, falta la configuración de demo, se detectó desviación de esquema) |

### Ejemplo {: #example }

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

Verifica e instala un archivo de licencia sin conexión. El archivo es el `license.json`
emitido por provisa.dev.

```
provisa license apply <file>
```

| Argumento | Notas |
| --- | --- |
| `file` | Ruta al archivo de licencia; se aplica expansión de `~` |

Código de salida 0 significa que la licencia es válida y está instalada. Código de salida 1
significa que fue rechazada; la razón se imprime a stderr.

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Muestra el ID de máquina, el estado de prueba, los días transcurridos, y la validez de la licencia.

```
provisa license status
```

Sin flags. Imprime cinco líneas (ID de máquina, fecha de primer registro, días transcurridos, estado de la prueba y estado de la licencia) y termina con 0.

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Lanza la publicación de metadatos bajo demanda del servidor en ejecución. El comando envía a `POST /admin/metadata-export/publish`, el endpoint que llama el botón **Publish now** de la pestaña Admin, así que ambas vías envían la misma instantánea completa.

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Predeterminado | Notas |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, luego `http://127.0.0.1:8000` | Bajo multiinquilinato el host nombra la organización |
| `--token` | `$PROVISA_API_TOKEN` | Token Bearer para una identidad que posea `org_settings`; omítalo en despliegues sin autenticación |
| `--timeout` | `300` | Segundos antes de que se abandone la llamada HTTP |

| Código de salida | Significado |
| --- | --- |
| 0 | Todo activo publicado |
| 1 | Publicación parcial o fallo de conexión; los errores por activo se imprimen a stderr |

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

[Metadata Export](metadata-export.md#from-the-command-line) tiene la referencia completa de configuración, con proveedores, credenciales, `reconcile_cron` y el contenido de la instantánea.

---

## provisa env

Ejecuta el ciclo de vida de los entornos contra un Provisa en marcha. Cada subcomando es una llamada a la
API de administración que llama la interfaz; la CLI no decide nada. La lista de lo que lleva una copia, las reglas de
fusión y eliminación, la vinculación, la protección y los derechos de quien llama son del servidor, así que se cumplen
se invoque como se invoque la operación.

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```

### Flags que acepta cada subcomando {: #flags-every-subcommand-takes }

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--org` | sí | Organización que tiene el entorno |
| `--json` | no | Imprime la respuesta de la API como JSON, para un script |
| `--api` | no | Consulte [Opciones compartidas](#shared-options) |
| `--token` | no | Token bearer de una identidad que puede realizar la operación |
| `--timeout` | no | Segundos; por defecto 300 |

Los subcomandos que actúan sobre un entorno también aceptan `--env ENV` (obligatorio). `list`, `diff`,
`merge` y `fetch` no; `diff` y `merge` nombran sus entornos con `--from` y `--into`.

### Códigos de salida {: #exit-codes_1 }

| Código | Significado |
| --- | --- |
| 0 | Hecho, o un ensayo que no aplicó nada |
| 1 | Rechazado. El código de la regla que rechaza se imprime en stderr como `refused (<code>): <detail>`; con `--json` es `{"refused": {...}}` en stdout |
| 2 | Propuesto, no aplicado: el entorno de destino está protegido y el cambio espera una aprobación |

El código de salida 2 es intencionado. Un pipeline que tratara una aprobación pendiente como un despliegue publicado
se equivocaría.

### provisa env list

Lista los entornos de la organización y la build en la que está cada uno. Sin flags aparte de los compartidos.

```
provisa env list --org ORG [--json]
```

### provisa env show

Muestra un entorno y su build actual. Sale con 1 y `environments.unknown` cuando el
entorno no existe.

```
provisa env show --org ORG --env ENV [--json]
```

### provisa env create

Crea un entorno que contiene el modelo de otro, con sus datos elegidos al crearlo. La API
decide qué opciones puede tomar quien llama.

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--env` | sí | Nombre del nuevo entorno |
| `--from` | no | Entorno del que partir; por defecto `prod` |
| `--data-mode` | sí | `inherit`, `unbound`, `test_fake` o `test_synthetic`: las conexiones del origen por referencia, ninguna hasta que vincule las suyas, o datos de prueba |
| `--mutation-handling` | no | `refused`, `reversible` o `direct`; por defecto `refused` |
| `--expires-at` | no | Instante ISO-8601 tras el cual se elimina el entorno |

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```

### provisa env delete

Retira un entorno. Sus ramas solo se van cuando se nombran.

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```

| Flag | Notas |
| --- | --- |
| `--delete-branch` | Elimina también su rama |
| `--delete-remote-branch` | Elimina también su rama en el remoto |

### provisa env diff

Informa de lo que haría fusionar un entorno en otro. Es un GET de `merge-preview`, así que
no puede escribir; déselo a un runner que no deba cambiar nada.

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--from` | sí | Entorno desde el que se fusiona |
| `--into` | sí | Entorno en el que se fusiona |
| `--removals` | no | Quita también del destino lo que el origen ya no tiene |
| `--select PATH` | no | Limita la fusión a este objeto, por su ruta en el modelo (`metrics/revenue.yaml`); repítalo para más. Lo que requiere y le falta al destino se lleva con él y se lista |

### provisa env merge

Fusiona un entorno en otro, por identidad.

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--from` | sí | Entorno desde el que se fusiona |
| `--into` | sí | Entorno en el que se fusiona |
| `--removals` | no | Quita también del destino lo que el origen ya no tiene |
| `--select PATH` | no | Limita la fusión a este objeto, por su ruta en el modelo (`metrics/revenue.yaml`); repítalo para más. Lo que requiere y le falta al destino se lleva con él y se lista |
| `--dry-run` | no | Imprime el informe de fusión y no aplica nada |
| `--message` | no | Qué hace la fusión; obligatorio para aplicar |
| `--retire-source` | no | Elimina el origen cuando su trabajo ha llegado |
| `--retire-remote` | no | Con `--retire-source`, elimina también la rama del origen en el remoto |

### provisa env deploy

Despliega el modelo de una ref de git en un entorno, haciendo de ese árbol el modelo actual del entorno
. Es el comando que ejecuta un pipeline de despliegue; un despliegue es siempre una
invocación con una identidad contra un plano de control nombrado.

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--env` | sí | Entorno que tendrá el modelo desplegado |
| `--ref` | sí | Rama o commit del repositorio de la organización |
| `--dry-run` | no | Informa de lo que haría el despliegue; no aplica nada |
| `--message` | no | Nota que se lleva a una solicitud de aprobación cuando el destino está protegido |

Una carga en un entorno existente nunca aplica roles. No hay `--seed`: la API rechaza una siembra
con `environments.seed_only_at_creation`.

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

### provisa env revert

Devuelve un entorno a una build anterior. Sin `--to` retrocede un commit
en la historia propia del entorno. Con `--to` es el despliegue ordinario de esa build.

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--env` | sí | Entorno que se revierte |
| `--to` | no | Build que se despliega; por defecto la anterior a la build actual |
| `--message` | no | Nota que se lleva a una solicitud de aprobación |

### provisa env redo

Avanza de nuevo hacia la build de la que partió una reversión.

```
provisa env redo --org ORG --env ENV
```

### provisa env fetch

Trae las ramas remotas de la organización a su repositorio de Provisa. Ejecútelo antes de un despliegue
cuando quiera nombrar `origin/<branch>`. Provisa nunca lo ejecuta con un temporizador.

```
provisa env fetch --org ORG
```

Imprime una línea por rama traída, `origin/<name>  <sha12>`.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

### provisa env push

Envía la rama de un entorno al remoto de la organización. Imprime `nothing to send` cuando
no hay nada que enviar.

```
provisa env push --org ORG --env ENV
```

### provisa env pull

Aplica lo que el remoto tiene para un entorno. Imprime
`<env> already holds what the remote does` cuando no hay nada que aplicar. El código de salida 2 se aplica igual
que en deploy.

```
provisa env pull --org ORG --env ENV
```

Para la explicación completa de las clases de entorno, las reglas de protección, los informes de fusión y el
ciclo de vida de aprobación, consulte [Entornos](environments.md#the-env-cli-commands).

---

## provisa maintenance on

Muestra el banner de mantenimiento programado en el despliegue. Ejecútelo antes de un trabajo planificado que detenga el plano de datos. Un ejemplo es cambiar `var.engine_cluster_mode`, porque reemplaza el clúster del motor y cada shard que contiene.

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Notas |
| --- | --- |
| `--message` | Anula el texto estándar del despliegue; el predeterminado es el mensaje estándar del servidor |
| `--ends-at` | Instante ISO-8601 en que se espera que termine el trabajo, p. ej. `2026-08-14T22:30:00Z`; el predeterminado es sin estimación |
| `--api` | Véase [Opciones compartidas](#shared-options) |
| `--token` | Token Bearer para una identidad que posea `platform_settings` |
| `--timeout` | Predeterminado 30 s |

Imprime el estado resultante del banner y sale con 0. Lanza `SystemExit` ante un fallo HTTP o de conexión.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Desactiva el banner de mantenimiento una vez terminado el trabajo.

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Imprime el estado resultante del banner (active: false) y sale con 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

Muestra el estado actual del banner de mantenimiento sin cambiarlo.

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Imprime el estado del banner y sale con 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## Referencia rápida {: #quick-reference }

| Comando | Qué hace |
|---|---|
| `provisa run` | Inicia la API + UI embebidas |
| `provisa run --demo` | Inicia con datos de muestra pet-store / shelter |
| `provisa run --reset` | Reconstruye el plano de control antes de iniciar |
| `provisa license apply <file>` | Instala un archivo de licencia sin conexión |
| `provisa license status` | Muestra el ID de máquina y el estado de prueba/licencia |
| `provisa metadata export` | Publica la instantánea de metadatos a demanda |
| `provisa env list --org ORG` | Lista los entornos de la organización y sus builds |
| `provisa env show --org ORG --env ENV` | Muestra un entorno |
| `provisa env create --org ORG --env ENV --data-mode MODE` | Crea un entorno a partir de otro |
| `provisa env delete --org ORG --env ENV` | Retira un entorno |
| `provisa env diff --org ORG --from A --into B` | Vista previa de una fusión (solo lectura) |
| `provisa env merge --org ORG --from A --into B` | Fusiona un entorno en otro |
| `provisa env deploy --org ORG --env ENV --ref REF` | Despliega una ref en un entorno |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | Retrocede un commit, o despliega una build |
| `provisa env redo --org ORG --env ENV` | Vuelve a avanzar |
| `provisa env fetch --org ORG` | Trae las ramas remotas al repositorio de Provisa |
| `provisa env push --org ORG --env ENV` | Envía la rama de un entorno al remoto |
| `provisa env pull --org ORG --env ENV` | Aplica lo que tiene el remoto |
| `provisa maintenance on` | Activa el banner de mantenimiento |
| `provisa maintenance off` | Desactiva el banner de mantenimiento |
| `provisa maintenance status` | Muestra el estado actual del banner |

## Véase también {: #see-also }

- [Entornos](environments.md) — modelo de entornos, entornos protegidos, ciclo de vida de aprobación de deploy
- [Metadata Export](metadata-export.md) — proveedores de catálogo, configuración, y qué contiene la instantánea
- [Deployment](deployment.md) — nivel de contenedor y despliegue en la nube
