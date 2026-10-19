# Modelo de seguridad {: #security-model }

Provisa aplica un modelo de seguridad de varias capas en todo lenguaje de consulta (GraphQL, SQL, Cypher) y todo transporte (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) El gobierno se aplica de forma uniforme: no hay ninguna vía de consulta que lo evite. (REQ-002, REQ-266)

Las capas se aplican en orden. Una solicitud debe superar cada capa antes de que se evalúe la siguiente.

## Modelo por capas {: #layered-model }

### Capa 0 — Filtrado de introspección {: #layer-0-introspection-filtering }

El esquema y el catálogo que se presentan a un rol contienen solo las tablas de su lista `domain_access` y las columnas que superan las reglas `visible_to` de cada columna. (REQ-039) Los objetos fuera del acceso de un rol son invisibles en el momento del descubrimiento: no se pueden consultar, autocompletar ni deducir que existen. (REQ-039) Esto se aplica al esquema GraphQL, al catálogo SQL y al explorador de esquemas del editor de consultas. (REQ-039, REQ-363)

Consulte [Visibilidad del esquema](#schema-visibility).

### Capa 1 — Acceso público {: #layer-1-public-access }

Una columna publicada con el alcance `public` se sirve a todos los roles de la organización, incluidos los roles cuyo `domain_access` no alcanza el dominio de la tabla; un rol así ve la tabla solo con sus columnas públicas. Nada es público salvo que quien registra una tabla lo diga. (REQ-1959) Consulte [Columnas públicas](#public-columns).

### Capa 2 — Acceso por dominio {: #layer-2-domain-access }

Cada rol lleva una lista `domain_access` de ids de dominio. Una consulta que toca una tabla fuera de esos dominios se rechaza antes de ejecutarse. (REQ-038, REQ-039) Este es el límite grueso de propiedad: un rol de RR. HH. no llega a las tablas de finanzas, se escriba como se escriba el SQL. (REQ-002)

Consulte [Modelo de derechos](#rights-model).

### Capa 3 — Seguridad de nivel de fila {: #layer-3-row-level-security }

Una vez confirmado el acceso por dominio, se inyectan predicados `WHERE` por tabla y por rol en cada `SELECT` en tiempo de ejecución. (REQ-041, REQ-263) Los predicados se evalúan sobre los datos sin procesar. Un gerente regional que consulta una tabla de pedidos compartida solo ve las filas de su región, incluso con `SELECT *`. (REQ-264)

Consulte [Seguridad de nivel de fila (RLS)](#row-level-security-rls).

### Capa 4 — Visibilidad y enmascaramiento de columnas {: #layer-4-column-visibility-and-masking }

Las columnas con una lista `visible_to` que excluye al rol solicitante se quitan de la salida de la consulta. (REQ-040, REQ-263) Las columnas con una regla de enmascaramiento ven sus valores sustituidos (redacción por regex, sustitución por constante o truncamiento) antes de que los resultados salgan del servidor. (REQ-263) El enmascaramiento se aplica en todos los lenguajes de consulta y formatos de salida. (REQ-263)

Consulte [Modelo de permisos de columna](#column-permission-model) y [Enmascaramiento de columnas](#column-level-masking).

### Capa 5 — Protección de predicados {: #layer-5-predicate-guard }

Las columnas enmascaradas se rechazan en las cláusulas `WHERE` y `HAVING`. (REQ-263) Sin esto, quien llama podría deducir el valor sin enmascarar buscándolo por bisección en un filtro aunque la salida esté enmascarada. El rechazo se aplica al analizar la consulta, antes de ejecutarla. (REQ-531)

### Gobierno de relaciones (V002) {: #relationship-governance-v002 }

Las condiciones JOIN en SQL deben coincidir con una relación registrada y aprobada entre tablas. (REQ-001) Los joins no aprobados se rechazan. Cada relación lleva un motivo y una descripción legibles: una guía para usuarios y agentes autónomos sobre por qué existe una ruta de recorrido. Es una política de gobierno, no un límite de seguridad estricto: las capas 2–5 se cumplen sea cual sea la estructura del join, así que una evasión deliberada no expone datos que el rol no pudiera alcanzar con dos consultas separadas. Los intentos de evasión se registran y se pueden auditar.

**Mecanismos de omisión** — V002 se puede omitir de dos formas. La primera es una capacidad: un rol que tiene `ignore_relationships` une relaciones que el catálogo no cubre. Entre los roles de sistema predefinidos solo `modeler` la tiene: el rol de descubrimiento, cuyo trabajo es determinar el modelo, no aplicarlo. (REQ-1297) `analyst` no la tiene.

La segunda es una exclusión con dos condiciones, que deben cumplirse ambas:

1. **Indicador del rol** — `relationship_guard: false` en la definición del rol (predeterminado: `true`).
2. **Exclusión por consulta** — el SQL contiene el comentario `--relationship-guard=false`.

El indicador del rol por sí solo no omite V002; el comentario por sí solo no omite V002.

**El modo de alta seguridad fija la protección.** Con `security.mode: high` no se aplica ninguna omisión: `ignore_relationships` se ignora, `relationship_guard: false` se ignora, y cada join debe existir en el catálogo de relaciones aprobadas. (REQ-693) Es una redundancia deliberada: un rol de producción al que se concedió la capacidad por error sigue sin poder salirse del modelo.

**Vía GraphQL** — V002 se omite siempre en las consultas GraphQL. Las relaciones definidas en el SDL están preaprobadas por diseño; la comprobación es redundante y no se aplica.

**Vías SQL y Cypher** — V002 está activo por defecto. Tanto `endpoint_dev.py` como `cypher_router.py` aplican la comprobación de dos condiciones antes de llamar a `validate_sql`.

**Vía pgwire** — la misma comprobación de dos condiciones que en SQL. El comentario `--relationship-guard=false` se quita de la consulta antes de ejecutarla; no llega a la base de datos.

---

Estas capas se combinan. Un rol con acceso por dominio, RLS y columnas enmascaradas tiene las cinco restricciones activas a la vez. Añadir un origen de datos, una columna o una relación no exige actualizar cada regla: cada capa se configura de forma independiente y se aplica automáticamente a cualquier consulta que toque objetos gobernados.

---

## Modelo de derechos {: #rights-model }

Capacidades asignadas de forma independiente con una jerarquía de roles opcional mediante `parent_role_id`. `admin` lo concede todo. (REQ-042)

| Capacidad | Descripción |
| ----------- | ------------- |
| `source_registration` | Registrar orígenes de datos |
| `table_registration` | Registrar tablas y columnas |
| `create_relationship` | Definir relaciones FK |
| `access_config` | Configurar RLS y enmascaramiento |
| `query_development` | Ejecutar consultas |
| `write` | Invocar mutaciones registradas (puerta gruesa; consulte Autorización de mutaciones) |
| `full_results` | Omitir los límites de muestreo |
| `ignore_relationships` | Omitir el gobierno de relaciones (V002). Entre los roles de sistema solo la tiene `modeler`, y se ignora por completo en el modo de alta seguridad |
| `admin` | Superusuario: lo concede todo |

### Herencia de roles {: #role-inheritance }

Los roles pueden heredar de un rol padre mediante `parent_role_id`, definido en la configuración o en la página Seguridad como "Hereda de". (REQ-215) La cadena se pliega al arrancar: un hijo tiene la unión de las capacidades y el acceso por dominio de sus antepasados, cada concesión de columna, métrica, función y webhook que nombra a un antepasado, y las reglas RLS de los antepasados por tabla, con prioridad para el hijo: el rol más cercano de la cadena con una regla para una tabla aporta el predicado de esa tabla, y una regla guardada para el hijo sustituye a la del padre para esa tabla. (REQ-1677)

```yaml
roles:
  - id: basic_user
    description: Queries the public domain.
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    description: Analyses sales; reads full results.
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```
## Roles: concedidos, derivados y meta {: #roles-granted-derived-and-meta }

Tres tipos de rol funcionan juntos en el modelo de acceso de Provisa. La página Roles lista los dos primeros juntos; un meta-rol nunca se lista.

### Roles concedidos {: #granted-roles }

Un rol concedido se asigna directamente a un usuario. Tiene derechos: capacidades, acceso por dominio, entradas de enmascaramiento, filtros de filas. La herencia (`parent_role_id`) pliega al arrancar los derechos de un antepasado en el hijo. Los derechos solo suman; no hay resta. Para restringir a un lector, conceda a un compuesto que nombre tanto el rol como una condición, o ponga una entrada de máscara antes de la permisiva. (REQ-215, REQ-1677)

### Asignaciones que caducan y sin uso {: #expiring-and-unused-grants }

Una asignación que Provisa mantiene puede terminar. Una asignación concedida con caducidad deja de tenerse desde ese instante, en cada solicitud. Un rol puede indicar `inactivity_days`: una asignación de ese rol de la que ninguna solicitud ha dependido durante más tiempo, contado desde el más reciente entre su último uso y su creación, se elimina. Los roles que vienen de las reclamaciones o asignaciones de roles de un proveedor de identidad los limita en el tiempo el proveedor; fije allí su caducidad. Los roles derivados y los meta-roles nunca caducan. (REQ-2077)

Una solicitud dependió de un rol cuando ese rol la atendió por sí solo o, cuando la solicitud actúa como varios roles, cuando al rol se le concede una tabla que la sentencia leyó. Actuar como «Todos» no cuenta como usado cada rol que se tiene. El último uso de una asignación se registra como mucho una vez al día.

Cada 15 minutos, el barrido de cada organización elimina las asignaciones caducadas y sin uso y registra cada una en el registro administrativo como `role.expired` o `role.removed_inactive`. Donde el correo saliente está configurado, también en una implantación de una sola organización, el titular y los administradores de la organización reciben un correo `auth.role_grant_notice_days` días (7 por defecto) antes de que termine una asignación, y los administradores reciben otro cuando se elimina; donde no hay correo configurado, la entrada del registro es la constancia. Estos avisos se envían sea cual sea la preferencia de correo del usuario.

### Roles derivados {: #derived-roles }

Un rol derivado procede de los atributos de un usuario. La organización declara qué atributos se convierten en roles (`derived_roles`), cada uno con una descripción. Para cada atributo declarado `a` cuyo valor `v` aportan los atributos del usuario, el usuario tiene el rol padre `a` y su rol de valor `a__v`: `region = eu` da `region` y `region__eu`. Sin valor, no tiene ninguno de los dos roles.

La resolución de atributos importa. El registro propio de la organización sobre un miembro prevalece sobre lo que aporta el proveedor de identidad: si ambos nombran el mismo atributo, se usa el valor de la organización y el del proveedor se marca como sustituido.

Los roles derivados no llevan derechos y nunca se asignan a mano. Una lista de concesión, una máscara o una regla de filas los nombra exactamente como nombra un rol concedido; una solicitud los nombra en `X-Provisa-Role` y actúa como ellos. (REQ-2057)

En la página Roles, cada atributo declarado es un rol padre marcado como *derivado*, con sus roles de valor listados debajo a medida que se ven sus valores. **+ Rol → Derivado de un atributo** declara uno. Su panel muestra el atributo, su descripción, los valores vistos hasta ahora y cada máscara, regla de filas y concesión que lo nombra a él o a uno de sus roles de valor. No tiene controles de concesión. La descripción de un rol de valor se genera: "Se tiene cuando el region del usuario es eu". (REQ-2078, REQ-2079)

Declárelos en la configuración como una lista:

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

Piense en un JWT: sus claims son hechos sobre la persona. Los roles derivados convierten hechos elegidos en roles, de modo que una concesión puede dirigirse a ellos sin crear un rol por combinación.

### Meta-roles

Cuando una solicitud nombra más de un rol, Provisa construye un meta-rol efímero identificado como `meta:<ids sorted, joined by +>`. Es un único hijo de cada rol del conjunto nombrado. Todo el pipeline se ejecuta entonces con él como un rol único normal.

Los derechos son la unión de los de cada miembro. Los filtros de filas son el OR del filtro resuelto de cada miembro; si algún miembro que lee una tabla no tiene filtro, el meta-rol tampoco: sin filtro significa todas las filas. Un meta-rol nunca se guarda, se lista ni se exporta. Se reconstruye a partir de sus miembros cuando cambia el modelo. (REQ-1620)

Las columnas de cada fila se enmascaran y muestran según los roles cuyo filtro de filas admitió esa fila: de esos roles se aplica la lectura menos restrictiva de la columna, y un rol que no admitió la fila no aporta nada a cómo se lee. Las máscaras se aplican donde se lee la tabla, antes de cualquier join o agregación, de modo que una agregación lee los valores enmascarados. Las escrituras se comprueban del mismo modo: una columna se escribe en una fila solo donde un rol que concede esa columna admite la fila. (REQ-1620)

El registro de auditoría nombra cada rol que actúa, no el id del meta-rol. Como un meta-rol nunca se guarda, no tiene descripción propia: dondequiera que se muestre una, su descripción es la de sus miembros, cada id con la suya. (REQ-2080)

### Descripciones {: #descriptions }

Cada rol y cada regla de filas dice para qué sirve. Una revisión de acceso lee quién puede ver qué y por qué. Un rol o una regla que nadie sabe explicar es uno que nadie puede revisar, así que la descripción es obligatoria: un rol, un rol padre derivado o una regla de filas con la descripción vacía es rechazado por la carga de configuración, la API de administración y la UI, con el motivo. Las listas Roles y Reglas de filas muestran cada descripción, y sus paneles de detalle la muestran y la editan. (REQ-2080, REQ-2081)

Un rol tiene una descripción, no una por dominio. Si un rol general no describe sus responsabilidades dentro de un dominio concreto, ese dominio necesita un rol propio, normalmente un hijo del rol general (`parent_role_id`), que hereda sus derechos y describe lo que hace allí.

### Cómo nombra roles una solicitud {: #how-a-request-names-roles }

Un cliente envía un rol, o un conjunto separado por comas, en `X-Provisa-Role`. Cada rol nombrado debe tenerse. Un rol que no está asignado a la identidad se rechaza por su nombre.

Los derechos y los datos siguen solo a los roles con los que actúa la solicitud: tener un rol no es actuar como él. Un rol es ese rol; dos o más actúan como su meta-rol. Un rol que actuara trayendo consigo los derechos de cada rol que se tiene no significaría nada, así que una solicitud obtiene el mínimo privilegio y el registro de auditoría nombra exactamente lo que actuó.

Los roles derivados nombrados entran como calificadores. Un calificador no tiene derechos, acceso por dominio, máscaras ni filtros de filas propios, así que no es miembro de los derechos del meta-rol: como miembro sin máscaras desenmascararía todas las columnas, y como uno que no lee ninguna tabla acabaría con cualquier OR de filtros de filas. Entra solo en las concesiones: una concesión o entrada de máscara que nombra `region__eu` alcanza a una solicitud que nombra ese calificador junto con un rol concedido. (REQ-2057)

Un agente MCP nunca actúa más allá de los roles que se le dieron. Un agente remoto solo nombra roles que posee el principal de su token. Un agente local por stdio solo nombra roles dentro del conjunto fijado en `mcp.role` (`PROVISA_MCP_ROLE`), y actúa como ese conjunto cuando no nombra ninguno; cualquier otro rol se rechaza por su nombre. (REQ-2075)

Un agente pregunta con qué rol actúa mediante la herramienta MCP `describe_role`. La respuesta es ese rol y para qué sirve (un meta-rol con cada uno de sus miembros), y los roles a los que el agente puede cambiar, cada uno con su descripción. Nunca incluye una regla de filas, máscara, concesión ni condición. Provisa nunca le dice a un agente por qué se redujo un resultado, y un resultado no lleva ninguna marca de "filtrado": los datos de un rol son toda su vista, y un motivo de lo que falta invitaría a sondearlo. (REQ-2082)

### Qué significa `+` {: #what-means }

`+` aparece en dos lugares, con significados distintos en cada uno.

**En una entrada de concesión.** `analyst+region__eu` en una lista `visible_to` o `to` significa que el lector los tiene todos: `analyst` Y `region__eu` a la vez. Una entrada que no es un compuesto es un rol único. Entre las entradas de una lista la regla es cualquiera: un lector tiene la concesión si tiene cualquier entrada. Y dentro, O entre ellas.

**En un id de meta-rol.** `meta:analyst+engineer` es el identificador del servidor para el meta-rol del conjunto {analyst, engineer}. Sus derechos son la unión de ambos. Un cliente nunca nombra directamente el id del meta-rol; nombra los roles que tiene y el servidor construye el meta-rol.

### Reglas de columna ABAC como RBAC simple {: #abac-column-rules-as-plain-rbac }

Una política basada en atributos pregunta: "¿puede este usuario, con estos atributos, ver esta columna?" RBAC pregunta: "¿qué roles pueden ver esta columna?" Las concesiones compuestas y los roles derivados las convierten en la misma pregunta.

"Desenmascarar este correo solo para un analista en la UE" es una entrada de máscara `{to: [analyst+region__eu], mask: null}`. Cualquier lector que tenga tanto el rol `analyst` como el rol derivado `region__eu` lee la columna sin enmascarar. Sin lenguaje de políticas. Sin un rol por combinación.

La contrapartida es cálculo en lugar de enumeración. Un catálogo de roles pequeño mantiene bajo el mantenimiento, pero "¿quién puede ver este correo de la UE?" pasa a ser una respuesta calculada: las entradas compuestas se evalúan contra las asignaciones y los claims de cada usuario. Provisa puede responder en ambas direcciones porque el modelo es una sola definición. La comprobación de acceso del editor de columnas (`POST /admin/masks/check`) ejecuta esa evaluación para un conjunto de roles elegido.

### Ventajas {: #benefits }

- Una regla de columna ABAC se convierte en una entrada de concesión RBAC.
- Un catálogo de roles pequeño en lugar de un rol por combinación.
- Una definición que coordina las reglas de cada región.
- Mínimo privilegio y un registro de auditoría exacto por solicitud.

### Buenas prácticas {: #best-practices }

- Mantenga el catálogo de roles tan pequeño como lo permita la organización.
- Herede dentro de un eje (antigüedad, nivel de habilitación). Combine entre ejes independientes (rol + región, rol + departamento).
- Ponga primero las entradas de máscara más específicas: una entrada anterior gana aunque el lector tenga una posterior.
- Compruebe el acceso efectivo con `POST /admin/masks/check` antes de desplegar una combinación en producción.

## Modelo de permisos de columna {: #column-permission-model }

Cada columna tiene un modelo de permisos que controla el acceso de lectura, escritura y enmascaramiento por rol. (REQ-042, REQ-249)

### Visibilidad en tres niveles {: #three-tier-visibility }

| Nivel | Condición | Resultado |
| ------ | ----------- | -------- |
| **Oculta** | Rol no en `visible_to` | Columna ausente del esquema y del catálogo |
| **Enmascarada** | Rol en `visible_to`; la primera entrada de máscara que tiene lleva una `mask` | Columna visible, valor sustituido en la salida |
| **Sin enmascarar** | Rol en `visible_to`; la primera entrada de máscara que tiene no lleva `mask`, o no tiene ninguna | Acceso completo de lectura |

### Columnas públicas {: #public-columns }

Cada columna tiene un `scope`, definido en el archivo del modelo, la API de administración o la página Tablas. Los tres alcances se ordenan según a quién sirven: `restricted`, luego `domain`, luego `public`. Quien edita una tabla (`table_registration` en el dominio de la tabla) puede estrechar el alcance de una columna. Ampliarlo (`restricted` a `domain` o `public`, `domain` a `public`) sirve la columna a más roles y requiere el derecho que requiere concederla: `column_grant` o `access_config` en el dominio de la tabla. Una columna que se está registrando se compara con `domain`, así que registrarla como `public` es una ampliación. En una columna con una etiqueta sensible, cualquier cambio de alcance requiere `sensitive_data`. Un cambio rechazado nombra la columna y el derecho. (REQ-1959, REQ-1943, REQ-1944)

| Alcance | Se sirve a |
| ------- | ----------- |
| `domain` (predeterminado) | Los roles que alcanzan el dominio de la tabla, según diga `visible_to`. Un `visible_to` vacío es cada uno de esos roles. |
| `public` | Los mismos roles, y también los que no alcanzan el dominio de la tabla. |
| `restricted` | Solo los roles que nombra `visible_to`. Un `visible_to` vacío es nadie. |

Publicar una columna quita la condición de dominio para esa columna y nada más:

- A un rol fuera del dominio se le sirve la tabla solo con sus columnas públicas. Las demás columnas de la tabla, y las tablas que no publican nada, quedan fuera de su esquema, su catálogo y su SQL.
- El propio `visible_to` de la columna sigue decidiendo quién: una columna pública concedida a un rol es pública solo para ese rol.
- Las máscaras, los filtros de filas, los límites de filas y la protección de relaciones se aplican como dentro del dominio. Sin filtro de filas para el rol, las columnas publicadas se leen sin filtrar; un filtro de filas sobre la tabla o su dominio sigue aplicándose.
- Una relación se sirve cuando se sirven las columnas de sus dos extremos.
- Una columna de parámetro de una tabla de API (un argumento de ruta o de consulta) no son datos y no publica nada.
- El dominio `ops` necesita una concesión explícita para cada columna (REQ-1133); nada en él se sirve entre dominios, sea cual sea su alcance.

La regla es la misma en toda superficie, porque cada una lee el esquema compilado del rol: GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher y Bolt, gRPC, REST y JSON:API, los catálogos de Flight y MCP y los listados de administración. Un alcance que no es uno de los tres valores se rechaza al guardar una columna.

### Permisos de escritura {: #write-grants }

`visible_to` controla la lectura de una columna (vacío significa que todos los roles pueden leer). La escritura se controla por tabla con `write_grants`, no por columna. Una tabla no tiene ningún permiso por defecto, así que ningún rol puede escribir en ella. (REQ-2074)

Un permiso de escritura indica qué puede escribir un rol en una tabla mediante una operación (`insert`, `update` o `delete`):

| Campo | Propósito |
| ------- | --------- |
| `columns` | Las columnas que un `INSERT` puede listar o un `UPDATE` puede establecer |
| `rows` | Las filas que un `UPDATE` o `DELETE` puede tocar (un predicado SQL) |
| `check` | Lo que debe cumplir cada fila que escribe un `INSERT`, o que deja un `UPDATE` (un predicado SQL) |
| `presets` | Columnas que el permiso establece por sí mismo, desde una variable de sesión, un literal o el momento de la escritura. La sentencia no puede suministrarlas. |

Si omite `rows` o `check`, el permiso toma el filtro de filas de lectura del rol sobre la tabla, de modo que un rol escribe las filas que lee. `"TRUE"` significa todas las filas.

Una sola admisión decide cada escritura en cada superficie, en este orden: el rol tiene el derecho `write`; tiene un permiso para la operación sobre la tabla; cada columna que escribe la sentencia está en el permiso; se añaden los presets; `rows` se combina con AND en el `WHERE`; y `check` se evalúa sobre las filas nuevas antes de enviar nada al origen. Un rechazo nombra la tabla, el rol y la operación. (REQ-2074)

Las sentencias más amplias necesitan permisos más amplios. `TRUNCATE` necesita un permiso `delete` sobre todas las filas. `MERGE` necesita un permiso `insert` y uno `update` sobre todas las filas. `INSERT ... ON CONFLICT DO UPDATE` necesita un permiso `update` sobre todas las filas sin presets. `COPY` necesita un permiso `insert` sin presets.

GraphQL ofrece a cada rol un campo de mutación solo para las operaciones en las que tiene permisos, con las columnas del permiso. Un rol hijo hereda los permisos de su padre, los propios primero. Un metarrol mantiene el pliegue de los permisos de sus miembros: la unión de las columnas, el OR de `rows` y `check`, y solo los presets que comparten todos los miembros. En la interfaz de administración, la sección **Escrituras** de una tabla lista y edita sus permisos.

### Ejemplo {: #example }

```yaml
tables:
  - source_id: hr-pg
    domain_id: people
    schema: public
    table: employees
    columns:
      - name: id
        visible_to: []                # all can read
      - name: email
        visible_to: [admin, analyst, viewer]
        masks:
          - to: [admin]               # admin sees raw value (no mask)
          - to: ["*"]                 # everyone else sees it masked
            mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}
      - name: salary
        visible_to: [admin, hr]
        masks:
          - to: [admin, hr]           # admin and hr see real value
          - to: ["*"]                 # all other visible roles see 0
            mask: {type: constant, value: 0}
      - name: created_at
        visible_to: []                # all can read
    write_grants:
      - role: admin
        operation: update
        columns: [email]
        rows: "TRUE"
      - role: hr
        operation: update
        columns: [salary]
      - role: hr
        operation: insert
        columns: [email, salary]
        presets:
          - {column: created_at, source: now}
```

En este ejemplo:

- `email`: admin ve `alice@example.com` y puede actualizarlo en cualquier fila; analyst y viewer ven `a***@example.com` y no pueden escribir
- `salary`: admin y hr ven el valor real; hr puede actualizarlo en las filas que hr puede leer, porque el permiso no indica `rows`; cualquier otro rol visible ve `0`
- `created_at`: todos pueden leerlo, ningún rol lo suministra, y un insert de hr lo establece al momento de la escritura

## Autorización de mutaciones {: #mutation-authorization }

Las mutaciones registradas (GraphQL remoto, OpenAPI, gRPC, Hasura) están protegidas por dos comprobaciones independientes. (REQ-868) Un rol solo puede invocar una mutación si tiene la capacidad global `write` Y el comando le está asignado mediante `visible_to` (una lista vacía lo asigna a todos los roles). (REQ-868) Un comando no tiene lista `writable_by`; la configuración rechaza una por su nombre. [tool-verified: `provisa/security/mutation_authz.py` `command_assigned`, `admit_command`; `provisa/core/models.py` `_one_role_list`]

Las mutaciones se clasifican como escrituras por contrato, no por lo que declare quien llama. (REQ-869) Un `SELECT` que hace referencia a una función de tipo mutación se promueve a escritura y queda sujeto a la misma comprobación de dos puertas, así que quien llama no puede invocar una mutación disfrazándola de lectura. (REQ-869) Reclasificar una mutación como segura para lectura requiere la capacidad `access_config` y se registra como una decisión de gobierno; no hay exclusión por solicitud. (REQ-870)

## Visibilidad del esquema {: #schema-visibility }

Los esquemas GraphQL por rol ocultan el contenido no autorizado: (REQ-039)

- **Acceso por dominio**: un rol ve tablas solo en sus dominios `domain_access` (`"*"` = todos) (REQ-039)
- **Visibilidad de columnas**: las columnas que no están en `visible_to` para un rol se omiten del SDL (REQ-039)
- Las tablas/columnas no autorizadas no aparecen en el esquema (REQ-039)

## Seguridad de nivel de fila (RLS) {: #row-level-security-rls }

Inyección de cláusulas SQL WHERE por tabla y por rol. Se aplica después de compilar, antes de ejecutar. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```
El filtro se añade con AND a la cláusula WHERE de la consulta. Funciona tanto en consultas como en mutaciones (UPDATE/DELETE). (REQ-035, REQ-041)

Un término `current_setting('provisa.<name>')` se resuelve contra las variables de sesión que enlaza la solicitud: `user_id` a partir de la identidad autenticada, `role` a partir del rol que actúa, y cada claim escalar del token bajo su nombre en minúsculas, con guiones como guiones bajos. Una variable de sesión de Hasura conserva su propio nombre: `X-Hasura-User-Id` es `hasura_user_id`, leída del claim `x-hasura-user-id` del token (en el nivel superior o dentro del objeto de claims de Hasura, como `https://hasura.io/jwt/claims`) o registrada para el miembro como el atributo `hasura_user_id`. Por eso un filtro de Hasura importado sobre `X-Hasura-User-Id` lee `provisa.hasura_user_id`. `provisa.user_id` es siempre la identidad con sesión iniciada; ningún claim la reemplaza. Las `session_vars` configuradas de un rol son las constantes de base; los enlaces de la solicitud las superponen. Un rol que hereda de otro toma las constantes de sus ancestros bajo las suyas, con el valor del rol más cercano para cada nombre. `user_id`, `role`, `region` y `org` los enlaza la solicitud o el nodo y no se pueden configurar como constantes. En modo no protegido, una cabecera `x-provisa-session-<name>` enlaza `<name>`. Una variable no enlazada en ningún sitio se resuelve a NULL y no coincide con ninguna fila. (REQ-1682)

Una constante de rol es la inversa de un rol derivado. Un rol derivado convierte un valor que tiene un miembro en un rol; una constante de rol da a cada solicitud que actúa como el rol un valor, que una regla de fila lee como lee un claim. Una constante de rol puede ser una referencia `${env:NAME}` o `${secret:NAME}`, resuelta mediante el mismo resolutor que cualquier otra referencia cuando una consulta la usa; el modelo conserva la referencia, nunca su valor resuelto, y una referencia que no se resuelve en nada hace fallar la consulta nombrando la constante. Una constante resuelta forma parte de la consulta. Los registros propios de Provisa sobre la consulta están cubiertos por la redacción central; los registros propios de una fuente federada quedan fuera del alcance de Provisa. (REQ-2086)

## Enmascaramiento de columnas {: #column-level-masking }

El enmascaramiento se define como una lista ordenada en cada columna. La primera entrada que tiene el lector decide lo que ve. (REQ-2059, REQ-263)

| Tipo de máscara | Tipos de columna admitidos | Expresión SQL |
| ----------- | ----------------------- | --------------- |
| `regex` | Cadena (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Cualquiera | Valor literal (`NULL`, `0`, una cadena propia) |
| `truncate` | Fecha/Marca de tiempo | `DATE_TRUNC(precision, col)` |

Cada entrada tiene una lista `to` que nombra a los lectores a los que apunta, y una `mask` opcional. Una entrada sin `mask` significa sin enmascarar para esos lectores. `*` en `to` coincide con cualquier lector.

La expresión de máscara se lleva a la proyección SQL SELECT. La base de datos devuelve el valor enmascarado; los datos sin enmascarar nunca cruzan la red para los roles enmascarados. (REQ-263) Las columnas enmascaradas también se bloquean en las cláusulas `WHERE` y `HAVING` (protección de predicados de la capa 5) para impedir deducir valores mediante filtros. (REQ-263, REQ-531)

**Una máscara cubre todos los nombres de la columna, no una columna relacionada.** Una columna se enmascara igual cuando se lee por su alias o por su nombre físico. Una máscara no oculta una clave que una relación aprobada une a una columna que el rol puede ver: el join compara los valores reales, así que la columna relacionada muestra lo que contiene la clave enmascarada. Enmascare también la columna relacionada, o no apruebe la relación para ese rol. (REQ-263, REQ-019)

**Sin entrada, sin máscara.** Un lector que no tiene ninguna entrada lee la columna sin enmascarar. Termine una lista con una entrada `*` cuando a cada lector se le deba aplicar alguna máscara. El editor de columnas avisa cuando una lista no tiene entrada `*` (`unlisted_unmasked`).

**Denegar por orden.** Una entrada anterior gana aunque el lector tenga también una posterior más permisiva. Para que los contratistas vean siempre un salario enmascarado, tengan los demás roles que tengan, ponga la entrada de contratistas antes de la entrada sin máscara. Un rol concedido nunca pierde lo que su padre concede por herencia, pero una entrada de máscara situada antes en la lista se aplica primero: así funciona la restricción aquí.

**Meta-roles.** La cadena de un meta-rol incluye los roles de cada miembro, sus antepasados y los roles derivados nombrados. Una entrada concedida a cualquier miembro alcanza al meta-rol. Una entrada anterior, que tiene otro miembro, se sigue aplicando primero.

**Comprobar el acceso.** `POST /admin/masks/check` acepta una lista de máscaras y un conjunto opcional de roles, y devuelve qué entrada se aplica para esos roles más las entradas que ningún lector puede alcanzar. Protegido por `masking_config`.

```yaml
# email column: analyst+region__eu sees raw; everyone else sees masked
masks:
  - to: [analyst+region__eu]          # composite: analyst AND in EU region
  - to: ["*"]                         # all others
    mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}

# hire_date column: hr sees raw; all other visible roles see year only
masks:
  - to: [hr]
  - to: ["*"]
    mask: {type: truncate, precision: year}
```
## Gobierno de las respuestas de acciones {: #governing-action-responses }

La respuesta de una función rastreada o de un webhook se gobierna como las filas de una tabla. (REQ-1679) Las filas que devuelve una acción se enlazan como una relación cuyas columnas son el contrato de salida declarado de la acción (`output_columns` para una función, `inline_return_type` para un webhook, o las propiedades de objeto del `return_schema` de tipo array de una función), y esa relación pasa por la misma etapa de gobierno que toda lectura de tabla, sobre una CTE VALUES con las filas. (REQ-1679) Nada de RLS, enmascaramiento ni visibilidad se reimplementa para las acciones.

Se aplican tres cosas, en el mismo orden que para una tabla. (REQ-1679)

- Un filtro de filas: la regla RLS del rol guardada contra la acción por su nombre (`upsertRlsRule` con `actionName`), o, cuando la acción no tiene una propia, la regla de dominio del dominio de la acción. El predicado se valida contra el contrato al guardarlo, igual que una regla de tabla. (REQ-1676)
- Visibilidad de columnas: una columna del contrato que declara `visible_to` se quita para un rol que la lista no nombra; una columna que no declara ninguno forma parte de la forma pública de la acción y se queda.
- Máscaras: una columna del contrato lleva `masks`, la misma lista ordenada de máscaras que una columna de tabla. (REQ-2059)

La herencia de roles resuelve la cadena para las tres. (REQ-1677) Una respuesta que devuelve una columna fuera del contrato declarado se rechaza con un 502 en lugar de pasar sin gobierno. Una acción que no declara contrato de columnas devuelve un escalar y no tiene nada que enlazar; se devuelve tal cual. La invocación de prueba de administración informa de los filtros, exclusiones y máscaras aplicados, y la sentencia gobernada se escribe en el registro de auditoría de consultas.

## Muestreo {: #sampling }

Todos los roles ven resultados muestreados (predeterminado: 100 filas) salvo que tengan la capacidad `full_results`. (REQ-554) Se controla con la variable de entorno `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Registro de auditoría {: #audit-logging }

Cada consulta que toca un activo de dominio se registra en `query_audit_log`. (REQ-596, REQ-613) El registro de auditoría pertenece a la organización, no a la plataforma: el propio registro de la plataforma de organizaciones, usuarios e invitaciones no contiene registros de auditoría. (REQ-1922) El `query_audit_log` de cada organización es una tabla en el esquema propio de esa organización, en el almacén que la organización nombra para su registro en cada región que selecciona. Una organización que no declara regiones mantiene ese esquema en la base de datos del plano de control del despliegue, junto a su modelo.

Cada fila guarda `tenant_id` (la organización), `user_id`, `role_id`, un hash SHA-256 del texto de la consulta, el propio texto de la consulta cifrado (REQ-689), `table_ids` y `table_names`, `source`, `status_code`, `duration_ms`, la ruta y el recuento de filas, lo que se aplicó a la sentencia y `logged_at`. El texto de la consulta se cifra con la clave de la organización cuando esta ha fijado una; si no, con la clave del despliegue (REQ-1574). Un despliegue que no configura ningún proveedor de cifrado lo guarda sin cifrar. El texto nunca se expone en las tablas de operaciones consultables y solo se descifra en lecturas de administración autorizadas.

El registro es de solo anexado a nivel de base de datos: las reglas de PostgreSQL convierten `DELETE` y `UPDATE` sobre `query_audit_log` en operaciones nulas. (REQ-596, REQ-613) Dos índices, `(tenant_id, logged_at)` y `(user_id, logged_at)`, admiten consultas de cumplimiento por organización y por usuario sobre intervalos de tiempo. (REQ-596, REQ-613)

Retención: las filas se conservan indefinidamente. Provisa no tiene ningún ajuste de retención y nunca purga el registro de auditoría; como `DELETE` es una operación nula, eliminar filas antiguas implica que un operador elimine o archive los datos de la tabla fuera de Provisa.

## Redacción de datos sensibles {: #redaction }

Lo que Provisa registra sobre su propio trabajo pasa por una lista de reglas de redacción antes de escribirse en ningún sitio. El texto que coincide con una regla se sustituye por `[REDACTED:<rule>]`, que nombra la regla que coincidió. (REQ-2087)

La lista tiene cuatro partes:

- **Patrones integrados.** Números de tarjeta que superan la comprobación de Luhn, números de la Seguridad Social de EE. UU., tokens bearer, tokens de acceso de Provisa, claves de AWS, GCP, Azure, GitHub y Slack, y claves privadas. Son un conjunto inicial, no una garantía.
- **Las reglas del despliegue**, que se aplican en todas las organizaciones. Cambiarlas requiere el derecho `platform_settings`.
- **Las reglas propias de la organización.** Cambiarlas requiere el derecho `org_settings`.
- **La regla automática** sobre los secretos de la organización: todo valor que el almacén propio de Provisa guarda para la organización, todo `${secret:NAME}` que nombran el modelo y la configuración de la organización (en cualquier backend de secretos), y cada valor `${secret:…}` o `${user:…}` desde que se resuelve por primera vez. Ninguna regla tiene que nombrarlos.

Una regla es una expresión regular con nombre o una referencia `${secret:NAME}` / `${env:NAME}` con nombre, que coincide con el valor al que se resuelve la referencia. Una referencia que no se resuelve se registra en `ERROR` por su nombre y se muestra como no resuelta; las demás reglas siguen aplicándose. Los valores de menos de cuatro caracteres no se detectan.

Provisa no es responsable de conocer todos los formatos sensibles. Los patrones integrados cubren formas comunes; una organización añade patrones para sus propios formatos, como números de empleado, identificadores de cuenta o nombres de host internos. La redacción se hace en la medida de lo posible: un valor escrito en una forma que ninguna regla describe pasa sin cambios.

Las reglas están en la página **Secretos de la organización**, en **Reglas de redacción**. La página muestra los patrones integrados, las reglas del despliegue (editables con `platform_settings`), las reglas de la organización, los secretos de la regla automática solo por su nombre y toda referencia que no se resuelve. Las reglas se cargan al arrancar el servidor y al construirse el entorno de ejecución de una organización, y se recargan cuando cambian el modelo o la configuración y cuando se guarda o elimina un secreto de la organización.

La lista se aplica en cada salida que escribe Provisa:

- cada span, como una copia redactada que se entrega a cada exportador, y la vista de spans recientes;
- cada registro de log en la canalización de logs de OpenTelemetry de Provisa, y cada línea que escribe un manejador de logging, incluidas la salida estándar y los logs de los contenedores;
- el texto de consulta del registro de auditoría, antes de calcular su hash y cifrarlo, de modo que ni el texto ni su hash revelan un valor detectado; y el detalle del registro administrativo;
- los mensajes de error que recibe un cliente por HTTP, pgwire, gRPC, Arrow Flight y MCP.

La telemetría y los errores de cliente no están ligados a una sola organización cuando se escriben, así que se redactan con las reglas de todas las organizaciones juntas. Dos errores pasan sin redactar: un cuerpo de error HTTP de más de 1 MiB y el error de un flujo de Arrow Flight que falla después de empezar a enviar.

Una fuente federada mantiene sus propios logs, que Provisa no escribe ni puede alcanzar. Los registros que una fuente hace de las sentencias que Provisa le envía le corresponde protegerlos a esa fuente.

## Limitación de tasa {: #rate-limiting }

Los límites de tasa por rol se configuran en `provisa.yaml`: máximo de solicitudes por segundo, máximo de suscripciones SSE simultáneas y máximo de flujos Arrow Flight simultáneos. (REQ-369) Los límites se aplican en la capa de API antes de compilar o ejecutar; las solicitudes que superan el límite se rechazan con HTTP 429 y una cabecera `Retry-After`. (REQ-369)

El servicio de consultas NL (`POST /query/nl`) tiene un límite propio mediante `nl.rate_limit` (solicitudes por minuto y rol). Las solicitudes que superan el límite se rechazan antes de llamar a ningún LLM. (REQ-370)

El estado de los límites de tasa vive en Redis (`cache.redis_url`) como un contador de ventana deslizante, sin estado por instancia, así que los límites se cumplen en todas las instancias horizontales de Provisa. (REQ-371)

## Límite de complejidad de consulta {: #query-complexity-limit }

Un límite de tasa acota cuántas solicitudes envía un rol. El límite de complejidad acota lo que puede pedir una sentencia. (REQ-1174)

Cada sentencia tiene una puntuación de complejidad: las relaciones que lee, sus joins, las columnas que selecciona (un `*` cuenta como las columnas que representa) y las consultas anidadas en ella. Una relación leída de un origen de API remoto (OpenAPI, GraphQL remoto, gRPC remoto) cuenta 10, porque cada lectura gasta un presupuesto que el sistema remoto fija para todos los que usan ese origen.

El límite es `max_query_complexity` en el `rate_limit` de un rol. El ajuste de toda la organización `limits.max_query_complexity` es un techo para cada rol: un rol puede fijar un límite menor, y uno mayor no tiene efecto. Ninguno está definido por defecto.

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```
La puntuación se mide después de analizar una solicitud y antes de gobernarla o ejecutarla, así que un mismo límite se cumple en cada interfaz por la que puede llegar una sentencia: GraphQL, SQL sobre HTTP, pgwire, Arrow Flight, gRPC, Cypher, JSON:API y MCP. Una sentencia por encima del límite se rechaza con su puntuación, el límite y lo que pidió. Las interfaces HTTP responden 413. El rechazo se registra como una denegación de política.

## Autenticación {: #authentication }

Proveedores de autenticación conectables: (REQ-120)

| Proveedor | Credencial | Caso de uso |
| ---------- | ----------- | ---------- |
| `none` | Cabecera X-Provisa-Role | Solo desarrollo |
| `basic` | Cuentas locales (bcrypt) + JWT de sesión | Despliegues autónomos |
| `firebase` | Token de ID de Firebase | Producción |
| `keycloak` | JWT de Keycloak | Empresa |
| `oauth` / `oidc` | JWT de OIDC | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Nombre de usuario y contraseña del directorio (LDAP o Active Directory) | Directorios empresariales (REQ-1265) |
| `saml` | Aserción firmada SAML 2.0 y luego un JWT de sesión | Inicio de sesión único empresarial (REQ-1265) |
| `simple` | bcrypt + JWT | Pruebas |

`ldap` inicia la sesión de un usuario mediante búsqueda y enlace: Provisa se enlaza como una cuenta de servicio, encuentra la única entrada que nombra el nombre de usuario y luego se enlaza como esa entrada con la contraseña presentada, de modo que el directorio la comprueba. Los grupos del usuario pasan a ser sus roles. La cuenta y sus grupos se vuelven a leer cada vez que regresa un token de sesión, de modo que un usuario eliminado del directorio, o de un grupo, pierde ese acceso antes de que el token caduque. Se admiten tanto `ldaps://` como StartTLS.

`saml` es Web Browser SSO iniciado por el SP. La firma XML de la respuesta se verifica con los metadatos del proveedor de identidad, y los claims se leen únicamente del elemento firmado. Se comprueban Issuer, Audience, la URL ACS, la caducidad e `InResponseTo`. Se rechazan las respuestas no solicitadas (iniciadas por el IdP), las aserciones cifradas, los documentos con DOCTYPE, las aserciones múltiples y las firmas SHA-1. Una sesión SAML no se vuelve a comprobar con el proveedor de identidad: un usuario eliminado allí conserva el acceso hasta que caduca el token de sesión.

Asignación de roles: claims de identidad → rol de Provisa mediante reglas configurables. (REQ-120) El campo `assignments_source` controla de dónde vienen las asignaciones de roles: `claims` las lee de los claims del token JWT (predeterminado), `provisa` las lee del almacén interno de asignaciones de Provisa. (REQ-551) Un despliegue multiinquilino (`multitenancy: true`) concede los roles él mismo, por organización, así que debe usar `assignments_source: provisa`: con `claims` no arranca, y un cambio de ajustes que produjera esa combinación se rechaza. Un despliegue de un solo inquilino puede usar cualquiera.

Un superusuario configurado en `provisa.yaml` (nombre de usuario más una contraseña de un secreto de entorno) recibe siempre el rol admin y todas las capacidades, sea cual sea el proveedor configurado: una vía de arranque para la configuración inicial. (REQ-125)

### Superficies y credenciales {: #surfaces-and-credentials }

Cada superficie se autentica mediante el mismo contrato de proveedor, así que una credencial que funciona en una funciona en todas allí donde el protocolo pueda llevarla. (REQ-124, REQ-1263) Esta tabla es la única referencia; la documentación de cada superficie no la repite.

| Superficie | Contraseña | Token del proveedor | Token de acceso personal | Certificado de cliente (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | mediante un proxy de terminación |
| pgwire | campo de contraseña (texto plano o SCRAM) | campo de contraseña, despliegues OIDC | campo de contraseña | sí |
| Bolt | esquema `basic` | esquema `bearer` | esquema `bearer` | sí |
| Arrow Flight | — | `token` en el handshake o en el payload del ticket; cabecera de llamada `authorization: Bearer` en `list_flights`, `get_flight_info` y `get_schema` | igual | sí |
| gRPC | — | metadatos `authorization` | metadatos `authorization` | sí |
| Airport (extensión `airport` de DuckDB, opcional) | — | `authorization: Bearer` (el `auth_token` del secreto) | igual | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | mediante un proxy de terminación |

La única opción de autenticación de la extensión `airport` de DuckDB es el `auth_token` bearer de su secreto, así que un cliente DuckDB presenta ahí un token del proveedor o un token de acceso personal y actúa como el rol al que se resuelve su identidad. Un cliente que puede fijar cabeceras de llamada puede pedir un rol que tiene, o un conjunto de ellos separado por comas, en `x-provisa-role`. (REQ-1120, REQ-1263)

Un rechazo dice de qué tipo es en cada una de estas superficies. Una credencial ausente, rechazada o inutilizable es un rechazo de autenticación: HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight y Airport `Unauthenticated`. Una credencial válida que pide actuar como un rol que su identidad no tiene es un rechazo de autorización, con el rol nombrado: HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight y Airport `Unauthorized`. (REQ-273, REQ-1263) Hay una excepción en la biblioteca cliente de Arrow, no en el servidor: un rechazo de la llamada `get_schema` de Flight llega a un cliente Python como un error genérico de Arrow con el motivo del servidor, sin su tipo; `list_flights`, `get_flight_info` y `do_get` entregan el tipo.

Donde una celda muestra `—`, el protocolo no lleva un campo de nombre de usuario con el que emparejar una contraseña; las formas de token lo cubren. pgwire es el caso inverso: el paquete de inicio tiene un campo secreto y ningún esquema, así que lo que *es* el secreto elige el método: un PAT se reconoce por su prefijo, el secreto se lee como token bearer cuando el proveedor configurado es un proveedor de tokens, y cualquier otra cosa es una contraseña. La elección se hace una vez: una credencial que rechaza el validador elegido no se reintenta con otro.

La matriz la impone `tests/unit/test_auth_surface_conformance.py`, que ejecuta el punto de entrada de validación real de cada superficie y falla cuando se añade una superficie sin fila.

### Tokens de acceso personal {: #personal-access-tokens }

Un PAT es un secreto bearer de larga duración que un usuario genera para un cliente que no puede completar un inicio de sesión interactivo: un script, una herramienta de BI, un controlador. (REQ-1263) Lleva su propia organización y rol, y cada superficie lo resuelve mediante el mismo validador, así que ninguna superficie necesita saber qué es un PAT.

La forma en el cable es `provisa_pat_` seguido de 43 caracteres base64 seguros para URL. El prefijo es lo que dirige un secreto presentado al almacén de tokens en lugar de al proveedor de identidad, y hace que un token filtrado se pueda buscar en registros y repositorios.

- **Almacenamiento** — solo se guarda el SHA-256 del secreto. El secreto se muestra exactamente una vez, al crearlo, y no se puede recuperar. El listado lleva el prefijo de visualización y las marcas de tiempo del ciclo de vida, nunca una credencial que funcione.
- **Emisión y revocación** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, más la sección de autoservicio en el propio perfil del usuario en la interfaz de administración. Generar y revocar una credencial es un acto de quien tiene el token.
- **Atribución** — un PAT validado se resuelve a la cuenta de su propietario: id de usuario, correo y nombre visible. Por eso una fila de auditoría o un informe de uso escritos con un PAT nombran a la persona, no a la credencial. Cuál de los tokens de esa persona actuó se lleva aparte, en `raw_claims["token_name"]`.
- **Caducidad** — un token puede llevar una caducidad; un token caducado se rechaza en la validación. Eliminar la pertenencia de un usuario revoca sus tokens con ella.

### SCRAM-SHA-256 en pgwire {: #scram-sha-256-on-pgwire }

Con el proveedor `basic`, fijar `auth.scram: true` hace que pgwire anuncie SASL (código de autenticación 10) con el mecanismo `SCRAM-SHA-256`, de modo que una contraseña se demuestra en lugar de enviarse. (REQ-1394) No se ofrece channel binding (`SCRAM-SHA-256-PLUS`).

SCRAM necesita un verificador RFC 5802, que no se puede derivar de un hash bcrypt. Se escribe un verificador cada vez que una contraseña pasa en texto plano (registro, inicio de sesión, cambio de contraseña, restablecimiento por un administrador), así que un despliegue que activa SCRAM reúne verificadores a medida que sus usuarios se autentican, y la primera conexión SCRAM de cada usuario sigue a su próxima introducción de contraseña. A un usuario sin verificador todavía se le responde con un intercambio simulado indistinguible de uno real, para que la red no revele quién ha migrado.

### TLS mutuo {: #mutual-tls }

La verificación del certificado de cliente traslada la primera comprobación al handshake TLS: quien llama sin un certificado firmado por la CA del despliegue nunca llega a la capa de credenciales. (REQ-1228) Está disponible en pgwire, Bolt, gRPC y Arrow Flight, los cuatro transportes que terminan su propio TLS.

| Variable | Significado |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Paquete PEM de las CA autorizadas a firmar certificados de cliente |
| `PROVISA_MTLS_MODE` | `required` (el predeterminado una vez definida una CA) u `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Si es true, el nombre común del certificado debe coincidir con el nombre de usuario con el que luego se autentica la conexión |

Las sustituciones por protocolo siguen la misma nomenclatura que los ajustes TLS. No se deduce nada: un modo definido sin CA se niega a arrancar, y un modo no reconocido se niega a arrancar en lugar de leerse como el vecino más seguro: un despliegue que cree exigir certificados de cliente y no lo hace está peor que uno que no arranca.

### Versiones de TLS {: #tls-versions }

Cada listener que Provisa termina (la API HTTP y la UI, pgwire, Bolt, MCP, gRPC y Arrow Flight) acepta TLS 1.2 y TLS 1.3, y rechaza TLS 1.0 y 1.1. Provisa no fija por sí misma ninguna versión de protocolo ni lista de cifrados. Los listeners HTTP, pgwire y Bolt usan los valores predeterminados de servidor de `ssl` de Python (mínimo TLS 1.2). gRPC y Arrow Flight usan los valores predeterminados de gRPC (mínimo TLS 1.2). Las versiones no son configurables.

### Limitación de inicios de sesión {: #login-throttling }

Adivinar contraseñas no depende del protocolo: la misma cuenta puede atacarse por HTTP, pgwire y Bolt. Por eso el contador vive en la capa de validación de credenciales, no en una superficie concreta, de modo que un bloqueo ganado en cualquier sitio se aplica en todas partes. (REQ-1393)

Está activada por defecto (cinco fallos en cinco minutos bloquean al sujeto durante quince minutos) y se ajusta en `auth.login_throttle`. Un sujeto bloqueado se rechaza antes de examinar siquiera la credencial, y una autenticación correcta borra el historial de ese sujeto.

La clave es el principal que lleva el protocolo. Una superficie solo bearer no lleva principal, así que la clave es un resumen de la propia credencial; lo que eso impide es que un token incorrecto se reproduzca sin límite. El almacén es por proceso, así que un despliegue con varios workers de API permite hasta `max_attempts` por worker: la limitación frena las adivinanzas, no es una cuota distribuida.

### Sondeo de roles {: #role-probing }

Nombrar un rol que el autor de la llamada no tiene se trata como un evento de seguridad detectado de gravedad alta, en todas las superficies: la API HTTP, MCP, pgwire, Bolt, Arrow Flight, el servicio Flight de airport y gRPC. Un cliente legítimo nombra los roles que se le dieron; uno que nombra otro está roto o está sondeando. (REQ-2076)

La solicitud se rechaza. Al autor de la llamada solo se le dice «not permitted», nada sobre si el rol existe, y la conexión o sesión termina donde la hay: la conexión pgwire, la sesión Bolt, la llamada gRPC, la sesión MCP. La excepción es la API HTTP con cualquier credencial que no sea un token de acceso personal: allí el navegador presenta su sesión o el token de su proveedor de identidad, por lo que el rechazo sigue nombrando el rol.

Lo demás depende de la credencial y del transporte:

| Credencial | Transporte | Se suspende |
|---|---|---|
| El token de un visitante de un sandbox de demo | la API HTTP | nada |
| Token de acceso personal | cualquiera | el token |
| Token firmado por un proveedor de identidad externo | MCP, pgwire, Bolt, Flight, gRPC | el acceso por token del principal a la organización |
| Token firmado por un proveedor de identidad externo | la API HTTP | nada |
| Una sesión o contraseña que este despliegue comprueba | cualquiera | nada |
| Una sesión MCP local (sin bearer) | MCP | nada; el evento va al registro del operador |

La API HTTP es la excepción porque el navegador presenta el mismo token bearer que un cliente de API, de modo que el servidor no puede distinguirlos, y una pestaña que aún nombra un rol que el usuario ha perdido (después de que un administrador cambie sus roles, o tras cambiar de organización) es algo habitual. MCP y los demás transportes nunca son una sesión de navegador. Allí el evento se rechaza, se registra y se notifica como cualquier otro; solo se omite la suspensión. El token de un visitante de un sandbox de demo es un token de acceso personal, pero es la única credencial de la pestaña del navegador del visitante, así que en la API HTTP se trata como una sesión: se rechaza y se registra, nunca se suspende.

Un token suspendido se rechaza como uno revocado. Un principal cuyo acceso por token está suspendido se rechaza en MCP, pgwire, Bolt, Flight y gRPC en esa organización, y aún puede iniciar sesión en la interfaz. Solo un administrador levanta una suspensión; nada la levanta por tiempo. Equipo → Acceso suspendido enumera cada suspensión con el evento que la causó y la levanta (`GET /admin/orgs/{org}/suspensions`, `POST /admin/orgs/{org}/suspensions/lift`).

Cada evento se escribe en el registro de administración de la organización (acción `security.role_probe`) con el principal, el identificador de la credencial, la dirección del cliente, el transporte, el endpoint o la herramienta, el rol nombrado, los roles que tiene, la hora y los identificadores de las sentencias auditadas más recientes del principal. Su texto permanece en el registro de auditoría de consultas y se lee con el derecho `view_governance`.

Los administradores de la organización reciben un correo donde el despliegue envía correo. El evento también se envía como JSON al webhook de seguridad del operador cuando `security.webhook_url` está definido. Lo que sale de la organización nombra el principal, el identificador de la credencial, el transporte, el endpoint, el rol nombrado, la hora y la gravedad, con un enlace a la vista de administración; no incluye ninguna sentencia ni ningún rol que se tenga. Un aviso que no se puede entregar se registra y nunca deshace el rechazo ni la suspensión.

### Nombrar la organización {: #naming-the-org }

En modo multiinquilino cada solicitud nombra la organización para la que es. Pertenecer exactamente a una organización no la nombra: una solicitud que no nombra ninguna se rechaza, y el rechazo dice qué enviar. (REQ-1235) Cada superficie nombra una organización así:

| Superficie | Cómo nombra un cliente la organización |
| ------- | -------------------------- |
| HTTP (interfaz, REST, GraphQL) | El nombre de host propio de la organización, `acme.provisa.dev`, o la cabecera `X-Org-Provisa` en el host del plano de control |
| pgwire | El nombre de la base de datos (`psql -d acme`, el campo de base de datos de una herramienta de BI), o el nombre de host propio de la organización marcado por TLS |
| Bolt | El nombre de la base de datos, `<org>.provisa_<role>` (o `<org>.provisa_ops_<role>`), o el nombre de host propio de la organización marcado por TLS |
| Arrow Flight | `"org"` en el ticket |
| gRPC | La cabecera de metadatos `x-provisa-org` |
| MCP (transporte HTTP) | El nombre de host propio de la organización, o la cabecera `X-Org-Provisa` |
| Cliente Python | `org=` en `ProvisaClient`, `connect()` y `adbc_connect()`; `?org=` en una URL de SQLAlchemy |

Un cliente pgwire que marca un nombre de host por TLS lo envía en el ClientHello de TLS, y Provisa lee de ahí la organización (REQ-1234). Cuando el nombre de host y el nombre de la base de datos nombran ambos una organización y difieren, la conexión se rechaza; no se prefiere ninguno. El nombre de base de datos `provisa`, el predeterminado, no nombra ninguna organización, y una herramienta de BI conectada a una organización muestra
esa organización como su base de datos. En Bolt, `SHOW DATABASES` lista un `<org>.provisa_<role>` por cada organización a la que
pertenece el usuario; un `provisa_<role>` sin más se rechaza en modo multiinquilino, y una sesión se queda en la
organización que nombró primero.

Un nombre es una petición, no una concesión. Llega en todas partes al mismo resolutor, que rechaza una organización de la que el principal autenticado no es miembro ni tiene el derecho entre organizaciones.

Un token de acceso personal se emite para una organización y la nombra él mismo. Solo abre esa organización: una solicitud que nombra otra se rechaza, aunque el propietario del token pertenezca a ambas.

Un despliegue de un solo inquilino no tiene organización que nombrar. Ninguna superficie lee ahí un nombre de host, un nombre de base de datos ni una cabecera para ello.

## Secretos entre organizaciones {: #secrets-across-organizations }

Los secretos de una organización solo los lee esa organización. En el almacén propio de Provisa cada secreto se guarda bajo su organización. En un servicio central (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) de un despliegue de muchas organizaciones, cada lectura se hace dentro del prefijo de la organización que lee, y una referencia que lo abandonaría se rechaza. (REQ-1557) Los prefijos y los pasos para mover los secretos existentes bajo ellos están en [Secretos](secrets.md#one-organizations-secrets-in-a-central-service).

## Telemetría entre organizaciones {: #telemetry-across-organizations }

En un despliegue que comparten muchas organizaciones, las señales operativas (`"ops"."traces"` y, en el motor Trino, las métricas y los registros que lo acompañan) son un único almacén al que llega el trabajo de todas las organizaciones. Cada span registra la organización para la que se inició, guardada como `tenant_id`, y cada lectura de las señales responde solo las filas de la organización que lee, sea cual sea el rol. Un span iniciado fuera del trabajo de cualquier organización (el arranque del servidor, por ejemplo) no pertenece a ninguna y ninguna lo lee. El administrador de la plataforma no tiene acceso a datos en ninguna organización y no lee ninguna a través de Provisa. (REQ-1349, REQ-1327)

Las señales operativas se conservan indefinidamente en ambos motores, como el registro de auditoría: la compactación reescribe los archivos en que se guardan y no elimina ninguna de sus filas. En Trino son las tablas Iceberg del catálogo `otel`. En un motor nativo (DuckDB) el servidor deposita sus propios registros de traza como archivos Parquet en su carpeta de telemetría y el motor los lee con el mismo nombre, `"ops"."traces"`, con la vista de linaje de refrescos `"ops"."mv_refresh_lineage"` sobre ellos; ambos se leen con la misma regla por organización. (REQ-862)

Las señales se guardan en claro en su almacén. Una carga de trabajo que necesite más que un límite por organización dentro de un despliegue se ejecuta en un despliegue dedicado; una muy sensible o regulada se autoaloja.

## Trabajo en segundo plano entre organizaciones {: #background-work-across-organizations }

Un proceso que sirve a muchas organizaciones ejecuta el trabajo en segundo plano de cada una como esa organización. El procesamiento de cambios de cada organización (el bucle de eventos que deposita los cambios de las fuentes y refresca las vistas sobre ellos), sus construcciones de réplicas y los disparadores de tabla que activan pertenecen a su propio runtime: un cambio lo procesa el trabajo de su propia organización, lee su fuente con la dirección y la credencial de esa organización y escribe en el almacén de esa organización. Cada entorno de una organización tiene su propio slot de replicación lógica de PostgreSQL y su publicación, nombrados por la organización, el entorno y la fuente, de modo que dos organizaciones que nombran igual una fuente en una misma base de datos leen cada una sus propios cambios.

## Residencia de datos {: #data-residency }

En un despliegue con regiones, una tabla puede nombrar una región como su hogar, y sus datos personales se almacenan y procesan allí. (REQ-1921, REQ-1922)

- Solo la región hogar construye y refresca la réplica de la tabla. Cualquier otra región lee esa réplica en su sitio, con las reglas de fila y columna del lector aplicadas en la región hogar, de modo que solo sale de ella lo que el lector puede ver.
- Una región hogar cuya réplica no está construida, o cuyos almacenes no se pueden alcanzar, rechaza la lectura (`query.home_region_unavailable`). La fuente nunca se lee en vivo desde otra región en su lugar.
- Un borrador solo se previsualiza donde vive. Una tabla en borrador que nombra una región se lee para la vista previa de su responsable solo en los nodos de esa región y se rechaza en otros sitios; la copia que deposita la vista previa se conserva allí para su publicación. Una copia que cualquier región tenga de una tabla, en borrador o publicada, cuyo hogar es ahora otra región se retira y después se elimina.
- Establecer, cambiar o quitar la región de una tabla requiere el derecho `data_residency` para las regiones implicadas, además de ser dueño de la tabla a través de su dominio. Un traslado puede pasar por borrador, de modo que la tabla no se sirve en ningún sitio entretanto.

Las reglas de fila y columna escritas sobre la región del nodo que responde (`current_setting('provisa.region')`) y sobre el atributo `region` de los miembros deciden qué cruza entre regiones. [Regiones](regions.md) describe por completo las regiones, la asignación de hogar y estas reglas.

## Refuerzo del despliegue {: #deployment-hardening }

### Credenciales de los almacenes y dónde escuchan {: #store-credentials-and-where-the-stores-listen }

Los almacenes incluidos no tienen credenciales predeterminadas. El plano de control PostgreSQL, Redis, MinIO, la cuenta de administración de Grafana y el secreto compartido del clúster Trino reciben cada uno un valor generado para el despliegue:

- Los lanzadores (`provisa start`, los instaladores de escritorio y de servidor) los generan una vez y los guardan en `~/.provisa/store-credentials.env` (modo 600). Un valor que ya esté en el entorno se usa tal cual, y un nodo secundario, que usa los almacenes del primario, se niega a iniciar sin los del primario.
- Los módulos de Terraform los generan con `random_password` y entregan los mismos valores a cada nodo. Se conservan en el estado de Terraform, así que proteja el estado como protegería las contraseñas.
- El chart de Helm no fija ninguno. Exige un Secret que usted crea, nombrado por `credentials.existingSecret`, con `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` y `trino-shared-secret`.

Redis exige su contraseña, y la contraseña del rol de PostgreSQL se vuelve a fijar en cada inicio para que un volumen creado antes siga la contraseña actual del despliegue. Los almacenes, el motor de federación y los servicios de telemetría publican sus puertos en una sola dirección, `PROVISA_STORE_BIND_IP`: `127.0.0.1` en un nodo independiente, o la dirección privada del primario cuando los secundarios se conectan a él. La cuenta de administración de Grafana recibe la contraseña generada; la pila de compose deja que visitantes anónimos vean los paneles, y el chart de Helm no admite ningún visitante anónimo.

### Cifrado en tránsito {: #transport-encryption }

Cada listener que Provisa termina sirve TLS (véase [Versiones de TLS](#tls-versions)). Un nodo servidor usa el certificado de `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY`, o genera uno autofirmado. El chart de Helm sirve TLS de forma predeterminada (`tls.enabled: true`) desde el Secret `kubernetes.io/tls` que nombra `tls.secretName`.

### El PostgreSQL embebido {: #the-embedded-postgresql }

Los niveles de escritorio y nativo ejecutan PostgreSQL embebido. En macOS y Linux escucha en un socket unix dentro de su directorio de datos (modo 700), al que solo llega la cuenta propietaria del directorio. Por TCP (Windows, donde escucha en 127.0.0.1), cada conexión debe dar una contraseña generada para el directorio de datos y guardada en él (`provisa.password`, modo 600). Nunca se inicia como root.

### El motor de federación (Trino) {: #the-federation-engine-trino }

- **Entre nodos.** Un coordinador y sus workers se autentican entre sí con el secreto compartido generado del despliegue (`internal-communication.shared-secret`). Cifrar su tráfico es una opción del operador, desactivada de forma predeterminada: desactivada, el tráfico queda dentro de la red privada o el clúster del despliegue. Actívela donde un requisito de cumplimiento o de confianza cero pida cifrado dentro del despliegue. Trino documenta que ralentiza las consultas con mucho shuffle (joins distribuidos, agregaciones, funciones de ventana) entre un 10 % y más de un 100 %. En un despliegue con compose es el ajuste `engine.trino_internal_tls` (Ajustes del despliegue → Motor), con la red del motor y la dirección fija del coordinador a su lado, y se aplica en el siguiente inicio. Con el chart de Helm es el valor `trino.internalTls`; el chart genera la CA y el certificado que usan los nodos, los conserva entre actualizaciones y direcciona cada nodo por el nombre DNS de su pod. Los motores de la plataforma alojada son coordinadores de un solo nodo, sin tráfico entre nodos, y lo rechazan. (REQ-2072)
- **Datos volcados.** La ejecución tolerante a fallos cifra los datos de intercambio que vuelca con una clave generada para cada consulta y descartada cuando esta termina (`fault-tolerant-execution.exchange-encryption-enabled=true`, fijado explícitamente). No se usa el volcado a disco heredado.
- **Organizaciones en un motor.** Los catálogos de cada organización son suyos, con el nombre de la organización, y las sentencias de cada organización se ejecutan en su propio grupo de recursos.
- **Los catálogos, y las credenciales que contienen, viven solo en la memoria del motor.** Provisa crea cada catálogo de Trino en tiempo de ejecución, y cada uno guarda las propiedades de conexión de su fuente, contraseñas incluidas. Todo motor que ejecuta Provisa los mantiene en memoria (`catalog.store=memory`): ningún catálogo se escribe en el disco del motor y ninguno se monta desde un archivo. Un coordinador que se reinicia vuelve sin catálogos; cada proceso de Provisa lo nota en su siguiente consulta, y el vigilante del motor en menos de un minuto, y vuelve a crear los catálogos que sirve. Una fuente Kafka con credenciales SASL necesita un archivo: el conector Kafka de Trino lee su configuración de cliente solo de un archivo, así que Provisa la escribe, contraseña incluida, en `kafka-client/<catalog>.properties`. El archivo solo lo lee el usuario de Trino (modo 0600 en un directorio 0700). En la pila de contenedores el directorio es un volumen en memoria que comparten la API y el motor. En los motores de la plataforma alojada es el propio Secret de Kubernetes del motor, guardado en la memoria del nodo y legible solo por el grupo de Trino (modo 0440). En ambos casos el archivo nunca llega a un disco. En los motores alojados una fuente Kafka debe nombrar un registro de esquemas: una cuyos tópicos tienen columnas que solo declara el modelo se rechaza por nombre al guardarla o aplicarla, ya que sus descripciones de tabla necesitarían un directorio que no se puede dar al motor en ejecución. Al arrancar la pila se eliminan los archivos de catálogo y de cliente Kafka que versiones anteriores dejaron en disco.

### Orígenes en un despliegue compartido {: #sources-on-a-shared-deployment }

En un despliegue que aloja muchas organizaciones, los orígenes de una organización solo pueden alcanzar direcciones públicas: nunca la red privada de la plataforma, donde están su plano de control, su caché y su almacén de objetos. La comprobación se aplica cuando se guarda un origen, desde la interfaz, la API de administración o una aplicación de configuración, y de nuevo en cada conexión y cada redirección HTTP. Los orígenes de un sandbox de demostración quedan además atados a la dirección en la que se comprobaron. La organización propia del despliegue, cuyos orígenes configura el operador, no está sujeta a esto.

## Cifrado en reposo y las claves propias de la organización {: #encryption-at-rest-and-the-organizations-own-keys }

El cifrado con una clave propia de la organización está disponible y permanece desactivado hasta que la organización lo activa. Nada se cifra con una clave de la organización a menos que un administrador de la organización cree una.

**La clave de cifrado de la organización** (REQ-1574) envuelve los secretos de la organización, sus columnas cifradas y el texto de consulta de su auditoría. Un administrador de la organización la crea en **Admin → Org Secrets → Org Key**, o con `PUT /admin/org-encryption`: sin cuerpo, Provisa genera la clave, que nunca se muestra, y un cuerpo `key_b64` aporta 32 bytes propios. `GET /admin/org-encryption` informa solo de su huella. Sin clave de organización, los datos de la organización se envuelven con la clave del despliegue (`encryption.provider` en `provisa.yaml`). Un despliegue que no configura ningún proveedor de cifrado guarda sin cifrar las columnas cifradas y el texto de consulta de la auditoría; los secretos nunca se guardan en claro (véase [Secrets](secrets.md#provisas-built-in-store)).

**La carpeta de la organización** (REQ-1992) contiene sus archivos subidos y aterrizados, los archivos de volcado de su motor y sus archivos temporales. Un administrador de la organización activa su cifrado en **Admin → Org Secrets → File folder encryption**, o con `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` responde una sola vez con una clave nueva de 256 bits, que debe guardarse en la bóveda. La clave permanece en la bóveda propia de la organización (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault o HashiCorp Vault): Provisa guarda solo la referencia, y gocryptfs mantiene cifrados en disco los archivos de la carpeta con esa clave. Esto requiere FUSE, es decir, un host o contenedor Linux con `/dev/fuse`; en macOS el ajuste se rechaza.

En un despliegue autoalojado, que normalmente ejecuta una organización en sus propios motores, activar ambos cifra con sus claves los secretos, archivos, archivos de volcado y archivos temporales de esa organización:

- El motor DuckDB que ejecuta una organización vuelca a la carpeta de la organización.
- Las filas que obtiene un filtro de filas y el código subido se preparan en la carpeta de la organización.
- La ejecución tolerante a fallos de Trino cifra los datos de intercambio volcados con una clave generada para cada consulta y descartada cuando esta termina (`fault-tolerant-execution.exchange-encryption-enabled=true`). Provisa no activa el volcado a disco heredado de Trino.
- Trino mantiene los catálogos de la organización, con sus credenciales, en memoria y no escribe ninguno en disco (`catalog.store=memory`); la configuración de cliente SASL de una fuente Kafka está en un archivo que solo lee el usuario de Trino, en memoria en la pila de contenedores (consulte [El motor de federación](#the-federation-engine-trino)).
- Un motor de federación PostgreSQL escribe sus archivos temporales donde ese servidor PostgreSQL los guarda, bajo el cifrado de almacenamiento propio del servidor, no en la carpeta de la organización.

### Plataformas alojadas compartidas y dedicadas {: #shared-and-dedicated-hosted-platforms }

La plataforma alojada compartida aísla las organizaciones de forma lógica. Un solo despliegue las atiende a todas, y los datos de cada organización siguen siendo suyos mediante la gobernanza, sus propios esquemas, catálogos y grupos de recursos en el motor, sus propias claves, claves por consulta para los datos volcados del motor y el rechazo de sus orígenes en las direcciones privadas de la plataforma. Se recomienda para experimentación y pequeñas empresas. Una plataforma alojada dedicada está separada físicamente: sus propias máquinas, red y almacenes, al servicio de un solo cliente. Para cargas de trabajo muy sensibles o reguladas, autoaloje Provisa dentro de su propio perímetro, donde sus propios operadores tienen cada clave y cada host. Las plataformas alojadas no cuentan con un informe SOC 2. Si necesita SOC 2, recomendamos el autoalojamiento.

### Python en el navegador {: #python-in-the-browser }

Las pestañas Python y Notebook de Explore ejecutan Python en el navegador del visitante. Cada lectura que hace va al servidor como el usuario con sesión iniciada, con su rol activo, su organización y su entorno, y se gobierna exactamente igual que la misma sentencia desde cualquier otro cliente.

## Modo de alta seguridad {: #high-security-mode }

`security.mode: high` en `provisa.yaml` afirma una garantía: el backend de Provisa nunca maneja datos en texto plano. (REQ-693) Cada columna que importa está cifrada en el origen, y solo un cliente con la clave de descifrado puede leerla. Esa garantía tiene consecuencias que un despliegue debe planificar.

**Qué hace el modo:**

- **Los endpoints de datos exigen prueba de descifrado en el cliente.** Todo bajo `/data/` devuelve 403 salvo que quien llama presente la cabecera `X-Provisa-KMS-Key`, la marca de un cliente JDBC o Python configurado para descifrar localmente. Un navegador o un consumidor REST en texto plano no lleva esa clave y se rechaza. La puerta deniega por defecto todo el árbol: una ruta añadida mañana queda protegida el día que se publica, y una excepción hay que argumentarla.
- **Los endpoints de metadatos de esquema siguen abiertos.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` y `/data/compile` no devuelven datos de filas, y un cliente tiene que leer el esquema (incluidos los campos `@encrypted`) antes de poder conectarse.
- **gRPC y Arrow Flight siguen sirviendo, con la misma prueba.** Son los transportes que usan de verdad los clientes que cifran; cerrarlos dejaría un despliegue de alta seguridad sin protocolo de cable. Una llamada de datos en cualquiera de los dos debe llevar la misma clave KMS como metadatos de llamada.
- **pgwire, Bolt y MCP no arrancan.** Ninguno de los tres tiene un handshake por conexión que pueda llevar un contexto de descifrado: un conjunto de filas de pgwire y un resultado de Cypher van en texto plano por la red, y una llamada a una herramienta MCP entrega sus resultados a un modelo como texto. Un puerto configurado para cualquiera de ellos se rechaza al arrancar en lugar de servirse.
- **La protección de relaciones no se puede omitir.** `ignore_relationships` y `relationship_guard: false` se ignoran ambos; consulte [Gobierno de relaciones](#relationship-governance-v002).

**Comprobar que un despliegue está en el modo:** el registro de arranque lo nombra, una solicitud `/data/sql` sin clave KMS responde 403 con un mensaje que nombra REQ-693, y los puertos de pgwire, Bolt y MCP no escuchan.

## Hook de aprobación ABAC {: #abac-approval-hook }

Un hook de política externo opcional que se dispara antes de ejecutar la consulta. (REQ-203) Cuando está configurado, Provisa llama a su motor de políticas con la identidad del usuario, los roles, las tablas, las columnas y la operación. La respuesta determina si la consulta continúa. (REQ-203) Un comando registrado con **Requiere aprobación** se somete al mismo hook antes de cada llamada; consulte [Llamadas a comandos](#command-calls). (REQ-1924)

### Ámbito {: #scoping }

El hook solo se dispara cuando la consulta toca una tabla o un origen incluidos: sobrecarga nula para todo lo demás. (REQ-204)

| Configuración | Efecto |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Cada consulta dispara el hook |
| `sources[].approval_hook: true` | Todas las tablas de ese origen disparan el hook |
| `tables[].approval_hook: true` | Esa tabla dispara el hook |

### Protocolos {: #protocols }

Se admiten tres transportes: (REQ-246)

| Tipo | Caso de uso | Campo de configuración |
| ------ | ---------- | ------------- |
| `webhook` | Cualquier servicio de políticas con HTTP (OPA, propio) | `url` |
| `unix_socket` | OPA o sidecar de políticas en la misma máquina | `socket_path` + `url` |
| `grpc` | Servicio de políticas de alto rendimiento en el mismo lugar | `url` (host:port) |

El transporte gRPC usa el contrato `provisa.auth.ApprovalService` definido en `provisa/auth/approval.proto`. Implemente este servicio en su motor de políticas: (REQ-246)

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
  map<string, string> session_vars = 6;
  string command = 7;         // a command call's name; empty for a query
  string arguments_json = 8;  // a command call's arguments as a JSON object
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```
El canal gRPC es persistente: un canal por instancia de Provisa, reutilizado en todas las llamadas a ese endpoint del hook. (REQ-555)

### Solicitud / respuesta {: #request-response }

Los tres transportes llevan el mismo payload: (REQ-246)

| Campo | Tipo | Descripción |
| ------- | ------ | ------------- |
| `user` | string | Identidad del usuario autenticado |
| `roles` | string[] | Roles de Provisa del usuario |
| `tables` | string[] | Ids de tabla a los que hace referencia la consulta |
| `columns` | string[] | Columnas seleccionadas en la consulta |
| `operation` | string | `"query"` o `"mutation"`; `"command"` para una llamada a un comando |
| `command` | string | El nombre del comando en una llamada a un comando; vacío en una consulta |
| `arguments` | object | Los argumentos con los que se llama a un comando; vacío en una consulta |

Los transportes webhook y socket Unix intercambian JSON, con `command` y `arguments` como claves. En gRPC los argumentos viajan como el texto JSON `arguments_json`. La respuesta debe incluir `approved` (bool) y, opcionalmente, `reason` (string). (REQ-246)

### Llamadas a comandos {: #command-calls }

Un comando con `requires_approval` activado (el interruptor **Requiere aprobación** del formulario del comando) pasa por el hook antes de cada llamada, en todas las superficies. Solo se ejecuta si el hook lo aprueba. La solicitud lleva `operation: "command"`, el nombre del comando y sus argumentos; `tables` y `columns` están vacíos. (REQ-1924) [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

```json
{
  "user": "analyst",
  "roles": ["analyst"],
  "tables": [],
  "columns": [],
  "operation": "command",
  "command": "create_issue",
  "arguments": {"input": {"title": "Crash on save"}}
}
```
[inferred: the payload also carries `session_vars`; values shown are placeholders]

Dos rechazos, ambos 403:

- `functions.approval_unavailable`: no hay hook configurado. A diferencia de una consulta, un comando que necesita aprobación nunca pasa sin uno.
- `functions.approval_denied`: el hook respondió `approved: false`. El motivo está en el mensaje.

### Tiempo límite y alternativa {: #timeout-and-fallback }

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```
Ante un tiempo límite o un error de transporte se aplica la política `fallback`. (REQ-247) Un disyuntor (predeterminado: se abre tras 5 fallos consecutivos, semiabierto tras 30 s) evita fallos en cascada por un endpoint de hook lento. (REQ-556)

### Ejemplo de configuración {: #configuration-example }

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```
## Secretos {: #secrets }

Las credenciales usan la sintaxis `${env:VAR_NAME}`, resuelta en tiempo de ejecución. (REQ-557) Las contraseñas nunca se guardan en la base de datos de configuración. (REQ-557)

Para el servicio completo de secretos (bóvedas, sintaxis de referencias y proveedores), consulte [Secretos](secrets.md).
