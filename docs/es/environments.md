# Entornos

Un entorno es una copia con nombre del modelo gobernado de una organización. Físicamente, la copia
es un esquema de PostgreSQL aparte —no una columna discriminadora, ni un prefijo, sino un esquema
real—, de modo que toda consulta existente del repositorio es correcta dentro de un entorno sin
reescribir nada, y las filas de un entorno no pueden colarse en la lectura de otro por un predicado
olvidado (REQ-1487, REQ-1488). [tool-verified: `environments.py` module docstring; `org_schema()`
at environments.py lines 86-96]

Toda organización empieza con un entorno llamado `prod`. No se puede eliminar ni renombrar. Una
solicitud que no nombra ningún entorno la atiende `prod`; una solicitud que nombra un entorno
inexistente se rechaza. [tool-verified: `PROD = "prod"` at environments.py line 44;
`select_environment()` at env_routing.py lines 93-129]

Los entornos están disponibles para las organizaciones con un plan de pago. [inferred: REQ-1507]

## Nombres de entorno

Un nombre debe coincidir con `[a-z][a-z0-9_]{1,31}`: de dos a treinta y dos caracteres de letras
minúsculas, dígitos y guiones bajos, empezando por una letra. Se rechazan `prod` y los nombres que
empiezan por `pg_`. La longitud máxima para una organización concreta depende de su propio id:
PostgreSQL trunca en silencio un identificador que supere los 63 bytes, y el nombre de esquema más
largo que deriva un entorno es aquello de lo que protege el límite. [tool-verified:
`ENV_NAME_PATTERN` at environments.py line 59; `validate_env_name()` at environments.py lines
119-142; `max_env_name_length()` at environments.py lines 108-116]

## Qué lleva una copia

Cada tabla del esquema de la organización cae en exactamente una clase (REQ-1489). La clasificación
es una lista de permitidos, no una lista de exclusión: una tabla añadida más adelante no viaja hasta
que alguien nombre aquí su clase, así que el modo de fallo de una tabla olvidada es una prueba en
rojo. [tool-verified: `CLASSIFIED` constant and module docstring, env_classes.py lines 19-22]

| Clase | Tablas | Qué ocurre al copiar |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, términos del glosario, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Se copian enteras |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | Los campos de identidad y gobierno viajan; los valores de conexión se quedan atrás (véase Vinculaciones) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Se copian solo cuando se crea el entorno por primera vez; los merges posteriores no los tocan |
| PARTIAL | org_settings | Se copian clave por clave: los ajustes de gobierno viajan, y las claves que nombran un destino externo o una ejecución propia de cada entorno se quedan atrás |
| NEVER_SENSITIVE | org_secrets, user_directory | Nunca se copian |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log y otras | Nunca se copian |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` existe para resolver un problema concreto. Un entorno nuevo necesita roles y
asignaciones o abre sin que nadie pueda actuar. Pero un merge posterior que llevara la fila
`developer` de `prod` sobrescribiría la versión restringida que podría necesitar una rama
restringida, convirtiendo la ruta de revisión en la vía de escalada. Así que los roles y las
asignaciones viajan una sola vez, al crear el entorno, y después son la respuesta propia de cada
entorno. [tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## Vinculaciones

Las vinculaciones son las columnas que indican adónde apunta realmente un origen: `host`, `port`,
`database`, `username` y las demás. Nunca viajan en ninguna copia. Un entorno que no se ha vinculado
se marca como `unbound` en vez de dejarse en blanco: un host vacío no es un host ausente, y el
constructor de conexiones lo leería como `localhost:5432`. [tool-verified: `BOUND_COLUMN = "bound"`
at env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

Los orígenes de un entorno se resuelven de una de estas dos maneras.

**Base**: el entorno lleva sus propias credenciales. Un org_admin crea una base y después vincula
explícitamente cada origen. [tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**Rama**: el entorno hereda por referencia las credenciales de la base. No se copia nada. Cuando una
consulta necesita una conexión, la resolución sube por la cadena `branched_from` y se detiene en el
primer entorno cuya fila esté vinculada. Rotar una credencial en la base se propaga a todas sus ramas
sin necesidad de hacer nada. Revocarla la revoca para todas a la vez. Ningún secreto se materializa
nunca en un lugar del que una rama, una exportación o un repositorio pudiera llevárselo.
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

Para crear una rama, active **Heredar conexiones** en el panel de Entornos. Por defecto está
desactivado. [tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## La proyección git

Cada escritura en el modelo confirma el resultado en la rama git del entorno. El repositorio es una
proyección del modelo, nunca su autoridad: Provisa lee y escribe el plano de control; el repositorio
es el registro, no el origen. Desplegar un árbol exige una llamada explícita: una pull request
fusionada en el host de git no se despliega sola (REQ-1524, REQ-1526). [tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

Cada entidad obtiene un archivo. La ruta es el URI de REQ-1385 sin el esquema ni la organización:
`provisa://acme/sales/tables/Order` se convierte en `sales/tables/Order.yaml`. Los orígenes
aterrizan en `sources/`, los comandos en `commands/`, las métricas en `metrics/`. Las filas hijas que
se propagan en cascada desde un padre —columnas, relaciones, reglas de RLS— se escriben dentro del
archivo del padre, no como archivos propios. [tool-verified: `table_path()` at env_files.py line
109-115; `kind_path()` at env_files.py lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py
line 71; env_files.py module docstring lines 17-24]

Los comandos y sus asignaciones de etiquetas sobreviven al viaje de ida y vuelta. Una etiqueta puesta
sobre un comando se enruta al archivo propio del comando (`commands/<name>.yaml`); una etiqueta que
no pertenece a ningún archivo desaparece de la proyección y se eliminaría en el siguiente despliegue
de ese árbol. [tool-verified: env_project.py lines 346-364; `owner_command_name` routing in
`_assignments_for()` at env_project.py lines 137-164]

Ninguna clave sustituta llega a un archivo. `registered_tables.id` es un entero autoincremental: el
mismo modelo en dos entornos obtiene enteros distintos, así que un volcado ingenuo difiere consigo
mismo. Toda clave sustituta se descarta y toda referencia a una se escribe como la ruta del destino.
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

La serialización es determinista. Las claves se emiten en orden alfabético, las colecciones hijas se
ordenan por su dirección y el estilo YAML es fijo. Dos entornos que contengan el mismo modelo
producen árboles idénticos byte a byte. [tool-verified: `dump()` at env_files.py lines 131-143]

## Merge

Hacer merge del modelo de un entorno en otro actualiza por identidad: todo objeto que tiene el origen
se crea o se actualiza en el destino. Los objetos que el origen ya no tiene se eliminan solo cuando
quien llama solicita explícitamente las eliminaciones. Un merge que falla a medias deja el destino
como estaba: una sola transacción. [tool-verified: `copy_model()` at env_copy.py lines 216-234;
REQ-1490 description]

Antes de aplicarlo, llame al endpoint de vista previa (`GET /{name}/merge-preview`) o pase
`dry_run: true`. La vista previa recorre la misma ruta de código que usa el merge; es un endpoint
`GET` para que un script de CI que se equivoque con el flag no pueda aplicar por accidente el merge
que pretendía inspeccionar. [tool-verified: `preview_merge()` docstring at environments_router.py
lines 1086-1095]

Un merge deja las vinculaciones, los roles y los secretos del destino exactamente como estaban. Un
entorno de desarrollo no pierde sus propias conexiones a bases de datos por tomar un modelo más
reciente de prod. Prod no adquiere las concesiones de desarrollo. [tool-verified: env_copy.py lines
269-287; REQ-1490 scenario]

### Qué nombra el informe

El informe del merge lista, por ruta, lo que se añadió, cambió, eliminó y quedó sin cambios. También
nombra los **conflictos**: los objetos que ambos lados cambiaron desde el último commit que
compartieron. Un conflicto se informa y no se resuelve: gana el origen, que es lo que significa un
merge hacia un destino. Provisa no ofrece resolución de conflictos, ni marcadores de merge, ni
elección objeto por objeto. El valor de la lista de conflictos es la señal: dos personas estaban
editando el mismo objeto sin saberlo (REQ-1555). [tool-verified: `CopyReport.conflicts` at
env_copy.py lines 151-165; `detect_conflicts()` called at env_copy.py lines 261-263; REQ-1555
description]

Un objeto que ambos lados cambiaron al mismo valor es acuerdo, no conflicto. Cuando los dos entornos
no comparten ningún ancestro, la base es `None` en el informe y la lista vacía de conflictos
significa que no se comparó nada, no que nada chocó. [tool-verified: `CopyReport.compared` property
at env_copy.py lines 164-166; env_copy.py lines 255-264]

El merge aterriza como un único commit aplastado (squash) en la rama del destino. El mensaje del
commit es obligatorio y no puede estar en blanco: es la única constancia del rango de trabajo que
representa el squash. Los commits del origen se quedan donde están y siguen siendo desplegables por
SHA después. [tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## Pull

Hacer pull toma lo que el remoto tiene para un entorno y lo convierte en el modelo. No hace
fast-forward de la rama local directamente; aplica el árbol descargado por la ruta de despliegue
ordinaria, de modo que la misma validación y la misma auditoría que rigen un despliegue manual rigen
un pull. [tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

Igual que un merge, un pull informa de lo que sobrescribió: los objetos que cambió el árbol entrante
y que el entorno local también había cambiado desde el último commit que compartieron ambas líneas.
Un cambio local sin confirmar es un entorno desviado (véase Historial más abajo); un pull lo nombra
como un cambio ordinario en el informe. [tool-verified: REQ-1556 description; `pull_environment()`
at environments_router.py lines 1485-1519]

Un pull se rechaza cuando las dos líneas han **divergido**: cada una tiene commits que la otra no
tiene. El rechazo lleva la lista de objetos que ambos lados tocaron, para que quien deba decidir
ahora qué trabajo sobrevive sepa qué objetos mirar. [tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503; `_collisions()` at environments_router.py lines 1581-1602]

## Historial

Cada despliegue mueve hacia adelante el cursor del entorno en su propia línea de commits. Deshacer
retrocede un commit; rehacer avanza de nuevo hacia la posición de la que partió el deshacer. Ninguna
de las dos operaciones elimina un commit: retroceder añade una posición, no reescribe el historial.
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

Una rama se siembra en la punta del entorno del que se creó, así que un deshacer se detiene en ese
punto de siembra y no camina sobre los commits del entorno padre. [tool-verified: `origin_sha`
comment at environments_router.py lines 428-448; `_move()` at environments_router.py lines 907-916]

Los indicadores `can_undo` y `can_redo` viajan con la respuesta del listado de entornos. Ambos valen
`false` cuando la proyección no contiene el commit que nombra el plano de control, un estado que el
diseño admite y que se llama **desviado**. Un nodo cuyo almacén de repositorios nunca recibió un
commit concreto sigue listando sus entornos; solo cambian las respuestas del historial (REQ-1561).
[tool-verified: `_with_history()` at environments_router.py lines 316-344; REQ-1561 description]

## Autorización

Los entornos se rigen por dos derechos. Ninguno de los dos es de un analista por defecto (REQ-1573).
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| Derecho | Quién lo tiene (sembrado) | Qué rige |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Crear y eliminar entornos |
| `environment_switch` | org_admin, developer | Ser atendido por cualquier entorno distinto de prod |

`prod` no necesita ningún derecho: es lo que atiende a una solicitud que no nombra nada, y
rechazarlo sería rechazar todas las solicitudes.

La aplicación ocurre en el punto de selección, antes de llegar a ninguna ruta. A un miembro que
carece de `environment_switch` se le rechaza en todas las superficies a la vez —HTTP, GraphQL, SQL y
los protocolos de cable— porque el entorno se vincula en el middleware, no en los controladores
individuales. [tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

Un analista que no tiene ningún derecho sobre entornos puede consultar `prod` y no ve el selector de
entornos. A un contratista al que se le concede el rol de analista no le aparece ninguna superficie
de entornos y no puede crear ninguno ni cambiar a ninguno distinto de producción. [tool-verified:
REQ-1573 use_case and scenario]

### Autoridad del propietario del entorno

Crear un entorno es la única vía por la que un miembro de solo lectura adquiere derechos de edición
del modelo (REQ-1528). Dentro del entorno que ha creado, quien lo creó tiene las capacidades del rol
`developer`, menos los derechos sobre los datos (`write`, `full_results`, `usage`). Derechos para
construir el modelo, no derechos sobre los datos. [tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at
env_authority.py lines 75-77; `_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py
module docstring lines 14-38]

La concesión se deriva de `environments.created_by` en el momento de autorizar, nunca se escribe en
una tabla de concesiones. Eliminar el entorno la retira en el mismo acto. [tool-verified:
env_authority.py module docstring lines 39-42; `environment_owner()` at env_authority.py lines 84-98]

La pertenencia a dominios sigue limitando lo que el propietario puede cambiar. Crear una rama cambia
lo que un miembro puede hacer; nunca cambia sobre qué dominios puede hacerlo (REQ-1530).
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## Entornos protegidos (REQ-1504)

Un entorno puede estar protegido. Un merge o un despliegue hacia un entorno protegido no se aplica
cuando se solicita; se propone, y alguien distinto de quien lo solicita debe aprobarlo.

`prod` queda protegido automáticamente en cuanto la organización tiene más de un miembro. Una
organización de un solo miembro no puede satisfacer el "alguien distinto de quien lo solicita", así
que allí la regla no se aplica: dejaría `prod` sin posibilidad de merge. Un org_admin puede marcar
como protegido cualquier entorno. [tool-verified: `is_protected()` at env_approvals.py lines 79-96;
`protectedHelp2` UI string in environmentsTab.json line 28]

Una solicitud de merge es una fila, no un cuadro de diálogo de confirmación. El aprobador es por
definición una persona distinta de quien solicita y no está presente en el momento de la solicitud;
una confirmación efímera obligaría a aprobar dentro de la sesión de quien solicita, que es
justamente la disposición que el requisito prohíbe. [tool-verified: env_approvals.py module
docstring lines 11-17]

La fila de la solicitud lleva el informe del merge junto al mensaje de quien la solicita. La
caducidad se deriva en el momento de la lectura, nunca se almacena: replanificar en el momento de la
lectura y comparar con el informe almacenado es la única versión que no puede equivocarse. Una
solicitud caducada debe volver a solicitarse. Quien solicita no puede aprobar su propia solicitud.
[tool-verified: `STALE` constant and `effective_state()` at env_approvals.py lines 53, 215-243;
`decide()` lines 265-268]

Estados del ciclo de vida de una solicitud: `requested` → `approved`/`rejected` → `applied`. `stale`
es derivado. [tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at
env_approvals.py lines 47-53]

La misma puerta atiende los despliegues desde una referencia del repositorio: la solicitud fija el
SHA en el momento de proponerla. Si la referencia se mueve entre la propuesta y la decisión, el
aprobador lee el informe del commit fijado, no el del nuevo. [tool-verified: `request_deploy()` at
env_approvals.py lines 150-189; env_approvals.py docstring lines 26-27]

!!! note
    La interfaz de solicitudes de merge está en la pestaña **Solicitudes de merge** del panel de
    Entornos. La columna **Informe** muestra por recuento lo que cambiaría; la fila se expande para
    mostrar el detalle objeto por objeto. [tool-verified: `environmentsTab.json` keys
    `requestsTitle`, `colReport`, `approve`, `reject`]

## Los comandos `env` de la CLI

`provisa env deploy` envía a un entorno el modelo que hay en una referencia. Sale con 0 cuando el
despliegue se aplicó o fue una ejecución de prueba, y con 2 cuando el entorno está protegido y el
despliegue solo se propuso: un pipeline que tratara una aprobación pendiente como un despliegue
publicado se equivocaría, y el código de salida lo dice. [tool-verified: `_cmd_env_deploy()` at
cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` trae al repositorio local las ramas remotas de la organización. Un despliegue
puede entonces nombrar `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

Ambos comandos aceptan `--api` (la URL de la API de Provisa) y `--token` (un token de portador).
Defina `PROVISA_API_URL` y `PROVISA_API_TOKEN` en el entorno para no tener que pasarlos en cada
llamada. [inferred: shared `_api_call()` helper]

El pipeline de CI típico para un flujo respaldado por repositorio:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Véase también

- [Despliegue](deployment.md) — cómo levantar el plano de control con el que se conectan los entornos
- [Comandos](commands.md) — funciones y webhooks rastreados que aparecen en el árbol de cada entorno
