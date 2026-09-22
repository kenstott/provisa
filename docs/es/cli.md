# Referencia de la CLI

El comando `provisa` es el único punto de entrada para el nivel embebido instalado con pip (REQ-1128).
Inicia el runtime, gestiona licencias, dispara la publicación de metadatos, despliega modelos, y
controla el banner de mantenimiento — sin Docker, Node, ni ningún servicio externo.

Instálelo con el extra `embedded`, que también trae las extensiones DuckDB sin conexión
y el plano de control PostgreSQL embebido:

```bash
pip install 'provisa[embedded]'
```

**Requisitos de plataforma.** `provisa run` requiere Python 3.12 y una plataforma con un wheel de
pgserver: linux x86_64, macOS, o Windows x86_64. Linux aarch64 no tiene wheel de pgserver ni
distribución de fuentes, así que el nivel embebido no se ejecuta ahí. Use el nivel de contenedor en
aarch64.
[tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## Opciones compartidas

Varios subcomandos llaman a la API HTTP de Provisa. Comparten tres flags y dos variables de
entorno. [tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| Flag | Predeterminado | Fallback a variable de entorno |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(ninguno)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 para `maintenance`) | _(ninguno)_ |

`--api` es la URL base de una instancia de Provisa en ejecución. Bajo multiinquilinato el host
nombra la organización — `https://acme.provisa.org` enruta al tenant de acme. `--token` es un
token Bearer; cuando está vacío no se envía ningún encabezado `Authorization`, lo cual es correcto
para despliegues sin autenticación. [tool-verified: cli.py:314-316, 357-365]

Establezca ambas variables en su entorno de CI para evitar repetirlas en cada llamada:

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Subcomandos que aceptan estos flags: `metadata export`, `env deploy`, `env fetch`,
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Inicia el sistema Provisa embebido — servidor de API y servidor estático/proxy de la UI — en un
solo proceso. Sin Docker, sin Node, sin servicios externos. [tool-verified: cli.py module docstring lines 11-23;
`_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Flags

| Flag | Predeterminado | Notas |
| --- | --- | --- |
| `--demo` | desactivado | Carga la demo incluida — dominios de ejemplo pet-store y shelter sobre SQLite embebido (REQ-414) |
| `--host` | `127.0.0.1` | Dirección de enlace para ambos servidores |
| `--api-port` | `8000` | Puerto del servidor de API |
| `--ui-port` | `3000` | Puerto del servidor estático/proxy de la UI |
| `--no-browser` | desactivado | Omite abrir un navegador cuando la UI está lista; igual imprime la URL |
| `--reset` | desactivado | Descarta y reconstruye el almacén del plano de control embebido antes de iniciar; úselo después de una actualización de Provisa si el arranque reporta un desajuste de esquema |
| `--data-dir` | `~/.provisa/native` | Directorio que contiene el clúster PostgreSQL embebido y la caché de extensiones DuckDB |

[tool-verified: run subparser at cli.py:609-634]

### Variables de entorno

`provisa run` lee varias variables adicionales antes de que arranquen los servidores HTTP.
Establézcalas para anular los valores predeterminados que aplicaría `load_profile("native", ...)`.
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| Variable | Efecto |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Reemplaza el motor DuckDB embebido por un coordinador Trino suministrado por el cliente (REQ-1129) |
| `PROVISA_ENGINE_URL` | Forma alternativa de apuntar a un motor de federación externo |
| `PROVISA_CONFIG` | Archivo de configuración a cargar; `--demo` lo establece en la configuración de demo incluida (REQ-1127) |
| `PROVISA_DEMO` | Establecida en `1` por `--demo`; marca la sesión como una ejecución de demo |
| `PROVISA_DEMO_DIR` | Ruta al directorio de datos de muestra de la demo; establecida por `--demo` |
| `PROVISA_CONFIG_REPLACE` | Establecida en `true` por `--demo` para permitir que la configuración de demo sobrescriba cualquier configuración existente |
| `PROVISA_DUCKDB_EXT_DIR` | Directorio de extensiones DuckDB preparado de antemano; se establece automáticamente desde el paquete `provisa-duckdb-ext` si está presente; su ausencia significa que DuckDB descarga desde la red en el primer uso |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### Secuencia de arranque

1. Verificación de plataforma — aborta con un mensaje claro ante un Python no compatible o un pgserver faltante.
2. `--reset` (si se solicita) — descarta el clúster PostgreSQL embebido; se reconstruye en el siguiente paso.
3. Configuración de demo (si `--demo`) — establece `PROVISA_CONFIG` y `PROVISA_DEMO_DIR`.
4. Entorno embebido — inicia el plano de control PostgreSQL, resuelve su URL de socket, y
   prepara las extensiones DuckDB sin conexión si `provisa-duckdb-ext` está instalado.
5. Verificación de desviación de esquema — escanea el plano de control en vivo en busca de columnas
   faltantes. Si encuentra alguna, imprime una sugerencia de `--reset` y sale con código 1. V1 no
   tiene migraciones; una columna agregada en una versión más nueva requiere un reset. [tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. Ambos servidores arrancan de forma concurrente. El anunciador de disponibilidad sondea
   `GET /ready` (no `/health` — el endpoint `/ready` confirma que el almacén está conectado y el
   motor está caliente) y abre el navegador cuando devuelve 200. [tool-verified: `_announce_ready()` at cli.py:182-224]

### Códigos de salida

| Código | Significado |
| --- | --- |
| 0 | Apagado limpio (Ctrl-C) |
| 1 | Error de arranque (falló la verificación de plataforma, falta la configuración de demo, se detectó desviación de esquema) |

### Ejemplo

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

Verifica e instala un archivo de licencia sin conexión (REQ-1139). El archivo es el `license.json`
emitido por provisa.dev. [tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| Argumento | Notas |
| --- | --- |
| `file` | Ruta al archivo de licencia; se aplica expansión de `~` |

Código de salida 0 significa que la licencia es válida y está instalada. Código de salida 1
significa que fue rechazada; la razón se imprime a stderr. [tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Muestra el ID de máquina, el estado de prueba, los días transcurridos, y la validez de la licencia (REQ-1139).
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

Sin flags. Imprime cuatro líneas — ID de máquina, fecha de primera vista, días transcurridos, estado
de prueba, y estado de licencia — y sale con 0. [tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Dispara la publicación de metadatos a demanda del servidor en ejecución (REQ-1072/REQ-1074).
Hace POST a `POST /admin/metadata-export/publish` — el mismo endpoint que llama el botón
**Publish now** de la pestaña Admin, así que ambas rutas envían la misma instantánea completa. [tool-verified: `_cmd_metadata_export()`
at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Predeterminado | Notas |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, luego `http://127.0.0.1:8000` | Bajo multiinquilinato el host nombra la organización |
| `--token` | `$PROVISA_API_TOKEN` | Token Bearer para una identidad que posea `org_settings`; omítalo en despliegues sin autenticación |
| `--timeout` | `300` | Segundos antes de que se abandone la llamada HTTP |

[tool-verified: cli.py:654-669]

| Código de salida | Significado |
| --- | --- |
| 0 | Todo activo publicado |
| 1 | Publicación parcial o fallo de conexión; los errores por activo se imprimen a stderr |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

La referencia de configuración completa — proveedores, credenciales, `reconcile_cron`, y qué
contiene la instantánea — está en
[Metadata Export](metadata-export.md#from-the-command-line).

---

## provisa env deploy

Despliega el modelo en una ref de git en un entorno, haciendo de ese árbol el modelo actual del
entorno (REQ-1496). Este es el comando que ejecuta un pipeline de despliegue; la regla es que un
deploy siempre es una invocación que lleva una identidad contra un plano de control nombrado. [tool-verified:
`_cmd_env_deploy()` at cli.py:395-417]

```
provisa env deploy --org ORG --env ENV --ref REF
                   [--dry-run] [--seed] [--message MSG]
                   [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--org` | sí | Organización dueña del entorno |
| `--env` | sí | Entorno que contendrá el modelo desplegado |
| `--ref` | sí | Rama o SHA de commit en el repositorio de la organización |
| `--dry-run` | no | Reporta qué cambiaría; no aplica nada |
| `--seed` | no | También aplica clases de solo-creación (roles); correcto solo cuando este deploy crea el entorno por primera vez |
| `--message` | no | Nota que se lleva a una solicitud de aprobación cuando el entorno destino está protegido |
| `--api` | no | Véase [Opciones compartidas](#opciones-compartidas) |
| `--token` | no | Véase [Opciones compartidas](#opciones-compartidas) |
| `--timeout` | no | Predeterminado 300 s |

[tool-verified: cli.py:677-711]

| Código de salida | Significado |
| --- | --- |
| 0 | Deploy aplicado, o `--dry-run` completado |
| 2 | El entorno está protegido; el deploy solo se propuso, no se aplicó |

El código de salida 2 es intencional. Un pipeline que tratara una aprobación pendiente como un
deploy liberado estaría equivocado. [tool-verified: cli.py:416-417]

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

Para la explicación completa de las clases de entorno, las reglas de protección, los informes de
merge, y el ciclo de vida de aprobación, véase
[Entornos](environments.md#los-comandos-env-de-la-cli).

---

## provisa env fetch

Obtiene las ramas remotas de la organización dentro de su repositorio de Provisa (REQ-1541).
Ejecute esto antes de un deploy cuando quiera nombrar `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at
cli.py:420-432]

```
provisa env fetch --org ORG [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Obligatorio | Notas |
| --- | --- | --- |
| `--org` | sí | Organización cuyo remoto se obtiene |
| `--api` | no | Véase [Opciones compartidas](#opciones-compartidas) |
| `--token` | no | Token Bearer para un administrador de organización |
| `--timeout` | no | Predeterminado 300 s |

[tool-verified: cli.py:716-733]

Imprime una línea por cada rama obtenida — `origin/<name>  <sha12>`. Sale con 0 en éxito; lanza
`SystemExit` con un mensaje de error ante un fallo HTTP o de conexión.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

---

## provisa maintenance on

Activa el banner de mantenimiento programado en el despliegue (REQ-1466). Ejecute esto antes de un
trabajo planificado que desconecte el plano de datos — por ejemplo, antes de cambiar
`var.engine_cluster_mode`, lo cual reemplaza el clúster del motor y cada shard en él (REQ-1465).
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Notas |
| --- | --- |
| `--message` | Anula el texto estándar del despliegue; el predeterminado es el mensaje estándar del servidor |
| `--ends-at` | Instante ISO-8601 en que se espera que termine el trabajo, p. ej. `2026-08-14T22:30:00Z`; el predeterminado es sin estimación |
| `--api` | Véase [Opciones compartidas](#opciones-compartidas) |
| `--token` | Token Bearer para una identidad que posea `platform_settings` |
| `--timeout` | Predeterminado 30 s |

[tool-verified: cli.py:743-773]

Imprime el estado resultante del banner y sale con 0. Lanza `SystemExit` ante un fallo HTTP o de conexión.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Desactiva el banner de mantenimiento una vez terminado el trabajo (REQ-1466).
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

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

Muestra el estado actual del banner de mantenimiento sin cambiarlo (REQ-1466).
[tool-verified: `_cmd_maintenance_status()` at cli.py:504-507]

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

## Referencia rápida

| Comando | REQ | Qué hace |
| --- | --- | --- |
| `provisa run` | REQ-1128 | Inicia la API + UI embebidas |
| `provisa run --demo` | REQ-414 | Inicia con datos de muestra pet-store / shelter |
| `provisa run --reset` | REQ-1535 | Reconstruye el plano de control antes de iniciar |
| `provisa license apply <file>` | REQ-1139 | Instala un archivo de licencia sin conexión |
| `provisa license status` | REQ-1139 | Muestra el ID de máquina y el estado de prueba/licencia |
| `provisa metadata export` | REQ-1072 | Publica la instantánea de metadatos a demanda |
| `provisa env fetch --org ORG` | REQ-1541 | Obtiene ramas remotas dentro del repositorio de Provisa |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | Despliega una ref en un entorno |
| `provisa maintenance on` | REQ-1466 | Activa el banner de mantenimiento |
| `provisa maintenance off` | REQ-1466 | Desactiva el banner de mantenimiento |
| `provisa maintenance status` | REQ-1466 | Muestra el estado actual del banner |

## Véase también

- [Entornos](environments.md) — modelo de entornos, entornos protegidos, ciclo de vida de aprobación de deploy
- [Metadata Export](metadata-export.md) — proveedores de catálogo, configuración, y qué contiene la instantánea
- [Deployment](deployment.md) — nivel de contenedor y despliegue en la nube
