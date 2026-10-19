# Entornos {: #environments }

Un entorno es una copia con nombre del modelo gobernado de una organización. Físicamente, la copia es un esquema de PostgreSQL aparte: un esquema real, sin columna discriminadora ni prefijo. Por eso toda consulta de repositorio existente es correcta dentro de un entorno sin reescribirla, y un predicado olvidado no puede hacer que las filas de un entorno se lean desde otro.

Toda organización empieza con un entorno llamado `prod`. No se puede eliminar ni renombrar. Una
solicitud que no nombra ningún entorno la atiende `prod`; una solicitud que nombra un entorno
inexistente se rechaza.

Los entornos están disponibles para las organizaciones con un plan de pago.

Para llenar un entorno que no es de producción con datos seguros de usar, consulte [Crear entornos de prueba](test-data.md).

## Nombres de entorno {: #environment-names }

Un nombre debe coincidir con `[a-z][a-z0-9_]{1,31}`: de dos a treinta y dos letras minúsculas, dígitos y guiones bajos, empezando por una letra. Se rechazan `prod` y los nombres que empiezan por `pg_`. La longitud máxima para una organización depende de su propio id. PostgreSQL trunca sin avisar un identificador de más de 63 bytes, y el límite protege frente al nombre de esquema más largo que deriva un entorno.

## Qué lleva una copia {: #what-a-copy-carries }

Cada tabla del esquema de la organización pertenece exactamente a una clase. La clasificación es una lista de permitidos: una tabla añadida después no viaja hasta que alguien nombra aquí su clase, así que olvidar una tabla se nota como un test en rojo.

| Clase | Tablas | Qué ocurre al copiar |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, términos del glosario, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Se copian enteras |
| IDENTITY_ONLY | sources, api_sources, kafka_sources | Los campos de identidad y gobierno viajan; los valores de conexión se quedan atrás (véase Vinculaciones) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Se copian solo cuando se crea el entorno por primera vez; los merges posteriores no los tocan |
| PARTIAL | org_settings | Se copian clave por clave: los ajustes de gobierno viajan, y las claves que nombran un destino externo o una ejecución propia de cada entorno se quedan atrás |
| NEVER_SENSITIVE | org_secrets, user_directory | Nunca se copian |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log y otras | Nunca se copian |

`SEEDED_AT_CREATION` existe para resolver un problema concreto. Un entorno nuevo necesita roles y
asignaciones o abre sin que nadie pueda actuar. Pero un merge posterior que llevara la fila
`developer` de `prod` sobrescribiría la versión restringida que podría necesitar una rama
restringida, convirtiendo la ruta de revisión en la vía de escalada. Así que los roles y las
asignaciones viajan una sola vez, al crear el entorno, y después son la respuesta propia de cada
entorno.

## Vinculaciones {: #bindings }

Los enlaces son las columnas que dicen adónde apunta de verdad un origen: `host`, `port`, `database`, `username` y las demás. Nunca viajan en una copia. Un entorno que aún no se ha enlazado se marca como `unbound` en lugar de dejarse en blanco, porque un host vacío no es lo mismo que ningún host, y el constructor de conexiones lo leería como `localhost:5432`.

Un entorno creado de cualquier forma salvo Unbound copia la conexión de cada origen desde su padre,
exactamente como el padre la escribió: una referencia `${secret:...}` o `${env:...}` se copia como esa
referencia, nunca resuelta. Tras la creación, la copia es la fila propia del entorno, editable en la
página Orígenes, y nada se resuelve a través del padre cuando se construye el runtime del entorno.
El campo `binding` de cada origen dice de dónde vino su conexión: `copied`, `own` (dada en este
entorno) o `unbound`. Editar una conexión copiada la convierte en `own`. **Volver a copiar del padre**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) copia de nuevo las conexiones del padre.
Una fusión o un despliegue no llevan conexiones, así que un origen que introducen llega `unbound`.

## La proyección git {: #the-git-projection }

Cada escritura en el modelo confirma el resultado en la rama git del entorno. Desplegar un árbol requiere una llamada explícita: una pull request fusionada en el host de git no se despliega sola.

Cada entidad recibe un archivo. La ruta es la dirección `provisa://` de la entidad sin el esquema ni la organización: `provisa://acme/sales/tables/Order` pasa a `sales/tables/Order.yaml`. Los orígenes van a `sources/`, los commands a `commands/` y las métricas a `metrics/`. Las filas hijas que dependen de un padre (columnas, relaciones, reglas RLS) se escriben dentro del archivo del padre, sin archivos propios.

Los comandos y sus asignaciones de etiquetas sobreviven al viaje de ida y vuelta. Una etiqueta puesta
sobre un comando se enruta al archivo propio del comando (`commands/<name>.yaml`); una etiqueta que
no pertenece a ningún archivo desaparece de la proyección y se eliminaría en el siguiente despliegue
de ese árbol.

Ninguna clave sustituta llega a un archivo. `registered_tables.id` es un entero autoincremental, así que el mismo modelo recibe enteros distintos en dos entornos, y un volcado ingenuo daría diferencias consigo mismo. Cada clave sustituta se omite, y cada referencia a una se escribe como la ruta del destino.

La serialización es determinista. Las claves se emiten en orden alfabético, las colecciones hijas se
ordenan por su dirección y el estilo YAML es fijo. Dos entornos que contengan el mismo modelo
producen árboles idénticos byte a byte.

## Merge

Fusionar el modelo de un entorno en otro actualiza por identidad: cada objeto que tiene el origen se crea o actualiza en el destino. Los objetos que el origen ya no tiene solo se eliminan cuando quien llama pide eliminaciones de forma explícita. Una fusión es una sola transacción, así que si falla a medias el destino queda como estaba.

Antes de aplicarlo, llame al endpoint de vista previa (`GET /{name}/merge-preview`) o pase
`dry_run: true`. La vista previa recorre la misma ruta de código que usa el merge; es un endpoint
`GET` para que un script de CI que se equivoque con el flag no pueda aplicar por accidente el merge
que pretendía inspeccionar.

Un merge deja las vinculaciones, los roles y los secretos del destino exactamente como estaban. Un
entorno de desarrollo no pierde sus propias conexiones a bases de datos por tomar un modelo más
reciente de prod. Prod no adquiere las concesiones de desarrollo.

### Qué nombra el informe {: #what-the-report-names }

El informe de fusión enumera, por ruta, lo que se añadió, cambió, eliminó y quedó igual. También nombra los **conflictos**: objetos que ambos lados cambiaron desde que compartieron un commit por última vez. Un conflicto se informa, y gana el origen, que es lo que significa fusionar en un destino. Provisa no ofrece resolución de conflictos, ni marcas de fusión, ni elección por objeto. La lista de conflictos vale como señal de que dos personas editaban el mismo objeto sin saberlo.

Cuando ambos lados cambiaron un objeto al mismo valor, están de acuerdo y no hay conflicto. Cuando los dos entornos no comparten ningún antepasado, la base es `None` en el informe, y una lista de conflictos vacía significa que no se comparó nada, no que nada chocara.

La fusión llega como un único commit aplastado en la rama del destino. El mensaje del commit es obligatorio y no puede estar vacío, porque es la única descripción del trabajo que representa ese commit. Los commits del origen se quedan donde están y después se pueden seguir desplegando por SHA.

## Pull

Hacer pull toma lo que el remoto tiene para un entorno y lo convierte en el modelo. No hace
fast-forward de la rama local directamente; aplica el árbol descargado por la ruta de despliegue
ordinaria, de modo que la misma validación y la misma auditoría que rigen un despliegue manual rigen
un pull.

Igual que una fusión, un pull informa de lo que sobrescribió: objetos que el árbol entrante cambió y que el entorno local también había cambiado desde que las dos líneas compartieron un commit por última vez. Un cambio local que nunca se confirmó deja el entorno desviado (vea Historial más abajo), y un pull lo nombra como un cambio normal en el informe.

Un pull se rechaza cuando las dos líneas han **divergido**, es decir, cada una tiene commits que la otra no tiene. El rechazo enumera los objetos que tocaron ambos lados, para que quien tenga que decidir qué trabajo sobrevive sepa qué objetos mirar.

## Historial {: #history }

Cada despliegue avanza el cursor del entorno por su propia línea de commits. Deshacer retrocede un commit, y rehacer vuelve a avanzar hacia donde empezó el deshacer. Ninguno elimina un commit: retroceder añade una posición y deja el historial como está.

Una rama se siembra en la punta del entorno del que se creó, así que un deshacer se detiene en ese
punto de siembra y no camina sobre los commits del entorno padre.

Los indicadores `can_undo` y `can_redo` llegan con la respuesta de la lista de entornos. Ambos valen `false` cuando la proyección no tiene el commit que nombra el plano de control, un estado que el diseño admite y llama **desviado**. Un nodo cuyo almacén de repositorio nunca recibió un commit concreto sigue listando sus entornos; solo cambian las respuestas del historial.

## Autorización {: #authorization }

Los entornos se rigen por dos derechos. Ninguno de los dos es de un analista por defecto.

| Derecho | Quién lo tiene (sembrado) | Qué rige |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Crear y eliminar entornos |
| `environment_switch` | org_admin, developer | Ser atendido por cualquier entorno distinto de prod |

`prod` no necesita ningún derecho. Es el que atiende una petición que no nombra entorno, y negarlo sería negar todas las peticiones.

La comprobación ocurre donde se elige el entorno, antes de llegar a ninguna ruta. A un miembro sin `environment_switch` se le rechaza en todas las superficies a la vez (HTTP, GraphQL, SQL y los protocolos de red), porque el entorno se fija en el middleware y no en cada handler.

Un analista que no tiene ningún derecho sobre entornos puede consultar `prod` y no ve el selector de
entornos. A un contratista al que se le concede el rol de analista no le aparece ninguna superficie
de entornos y no puede crear ninguno ni cambiar a ninguno distinto de producción.

### Autoridad del propietario del entorno {: #environment-owner-authority }

Crear un entorno es la única forma en que un miembro de solo lectura obtiene derechos para editar el modelo. Dentro de un entorno que creó, el creador tiene las capacidades del rol `developer`, menos los derechos sobre los datos (`write`, `full_results`, `usage`): derechos para construir el modelo, sin derechos sobre los datos.

La concesión se deriva de `environments.created_by` en el momento de autorizar, nunca se escribe en
una tabla de concesiones. Eliminar el entorno la retira en el mismo acto.

La pertenencia a dominios sigue limitando lo que el propietario puede cambiar. Crear una rama cambia
lo que un miembro puede hacer; nunca cambia sobre qué dominios puede hacerlo.

## Entornos protegidos {: #protected-environments }

Un entorno puede estar protegido. Un merge o un despliegue hacia un entorno protegido no se aplica
cuando se solicita; se propone, y alguien distinto de quien lo solicita debe aprobarlo.

`prod` pasa a estar protegido automáticamente en cuanto la organización tiene más de un miembro. Una organización de un solo miembro no puede cumplir "alguien distinto del solicitante", así que ahí la regla no se aplica; haría imposible fusionar en `prod`. Un org_admin puede marcar cualquier entorno como protegido.

Una solicitud de fusión es una fila guardada, y ningún diálogo de confirmación la sustituye. Quien aprueba es por definición alguien distinto del solicitante y no está presente en el momento de la solicitud. Una confirmación efímera obligaría a aprobar dentro de la sesión del solicitante, que es justo lo que la regla prohíbe.

La fila de la solicitud lleva el informe del merge junto al mensaje de quien la solicita. La
caducidad se deriva en el momento de la lectura, nunca se almacena: replanificar en el momento de la
lectura y comparar con el informe almacenado es la única versión que no puede equivocarse. Una
solicitud caducada debe volver a solicitarse. Quien solicita no puede aprobar su propia solicitud.

Estados del ciclo de vida de una solicitud: `requested` → `approved`/`rejected` → `applied`. `stale`
es derivado.

Los despliegues desde una referencia del repositorio pasan por la misma puerta, y la solicitud fija el SHA al proponerse. Si la referencia se mueve entre la propuesta y la decisión, quien aprueba lee el informe del commit fijado, no el del nuevo.

!!! note
    La interfaz de solicitudes de merge está en la pestaña **Solicitudes de merge** del panel de
    Entornos. La columna **Informe** muestra por recuento lo que cambiaría; la fila se expande para
    mostrar el detalle objeto por objeto.

## Los comandos `env` de la CLI {: #the-env-cli-commands }

`provisa env deploy` envía el modelo de una referencia a un entorno. Termina con 0 cuando el despliegue se aplicó o fue una prueba en seco, y con 2 cuando el entorno está protegido y el despliegue solo se propuso. Un pipeline que tratara una aprobación pendiente como un despliegue publicado se equivocaría, y el código de salida distingue ambos casos.

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` trae al repositorio local las ramas remotas de la organización. Un despliegue
puede entonces nombrar `origin/<branch>`.

```
provisa env fetch --org acme --api <url> --token <token>
```

Ambos comandos aceptan `--api` (la URL de la API de Provisa) y `--token` (un token de portador).
Defina `PROVISA_API_URL` y `PROVISA_API_TOKEN` en el entorno para no tener que pasarlos en cada
llamada.

El pipeline de CI típico para un flujo respaldado por repositorio:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Véase también {: #see-also }

- [Despliegue](deployment.md) — cómo levantar el plano de control con el que se conectan los entornos
- [Comandos](commands.md) — funciones y webhooks rastreados que aparecen en el árbol de cada entorno
- [Crear entornos de prueba](test-data.md) — perfilar producción, declarar datos falsos y generar datos sintéticos para un entorno que no es de producción
- [Métodos de datos falsos](fake-methods.md) — cada tipo de dato falso y cada método que una columna puede declarar
