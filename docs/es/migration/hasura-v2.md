# Migración de Hasura v2 a Provisa {: #migrating-from-hasura-v2-to-provisa }

## Requisitos previos {: #prerequisites }

1. Una instancia de Hasura v2 (v2.x) en ejecución con los metadatos exportados.
2. Exporte los metadatos en cualquiera de los dos formatos:

   **El directorio de metadatos de la CLI:**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Esto crea un directorio `metadata/`. `databases/databases.yaml` lista cada origen con su tipo
   y su conexión y apunta a sus tablas y funciones mediante índices `!include`;
   `actions.yaml`, `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` y los
   demás archivos están a su lado. El formato plano más antiguo (`tables.yaml` en la raíz)
   también se convierte.

   **El documento único de la API de metadatos:**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   Funcionan tanto el objeto simple como el sobre `{resource_version, metadata}`. La importación
   de administración (Admin, Import Hasura Config) admite esta forma; el conversor de línea de
   comandos que sigue lee directorios.
3. Python 3.11+ con el paquete `provisa` instalado.

## Uso de la CLI {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argumentos {: #arguments }

| Argumento | Obligatorio | Descripción |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Sí | Ruta al directorio de metadatos exportado de Hasura v2 |

### Opciones {: #options }

| Opción | Predeterminado | Descripción |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Ruta del archivo YAML de salida |
| `--source-overrides FILE` | Ninguno | Archivo YAML con anulaciones de conexión por origen |
| `--domain-map KEY=VAL ...` | Ninguno | Asignaciones de esquema a dominio (por ejemplo, `public=core hr=people`) |
| `--auth-env-file FILE` | Ninguno | Ruta a un archivo `.env` con la configuración de autenticación JWT/admin-secret |
| `--dry-run` | desactivado | Analiza y valida sin escribir la salida |

### Archivo de anulaciones de origen {: #source-overrides-file }

Un archivo YAML indexado por nombre de origen con propiedades de conexión a anular:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Archivo de entorno de autenticación {: #auth-env-file }

Un archivo con estilo `.env` que contiene la configuración de autenticación de Hasura
a convertir. El convertidor asigna:

- JWT con `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Autenticación por webhook -> se emite una advertencia (sin equivalente en Provisa).

## El informe {: #the-report }

La conversión termina con un informe en tres partes:

1. **Trasladado tal cual.** El objeto tiene una contrapartida en Provisa y se convierte sin cambiar de significado.
2. **Asignado a una función mejor de Provisa.** Cada elemento dice por qué la función de Provisa hace mejor el trabajo.
3. **Rehacer a mano.** Cada elemento dice qué hacer. Estas son las advertencias.

El conversor de línea de comandos imprime el informe en stderr como `Carried as`, `Mapped to` y `Redo by hand`, cada una con un recuento, cuando queda algo por rehacer. La importación de administración devuelve las tres listas con la vista previa.

## Matriz de paridad de funciones {: #feature-parity-matrix }

| Función de Hasura v2 | Parte | Equivalente en Provisa | Notas |
| --- | --- | --- | --- |
| **Orígenes** (postgres, mysql, mssql, bigquery, citus) | trasladado | `sources[]` | Tipo asignado: pg/postgres -> postgresql, mssql -> sqlserver. Una URL de conexión escrita se analiza en host/puerto/base de datos/usuario/contraseña. Se conserva el tamaño del pool. Una URL leída de una variable de entorno no se adivina (véase abajo). |
| **Tablas** (tablas rastreadas) | trasladado | `tables[]` | Se conservan el esquema y el nombre de tabla. `source_id` enlaza con el origen. |
| **Nombres de tabla personalizados** (`custom_name`, `custom_root_fields.select`) | trasladado | `tables[].alias` | El primer valor no nulo de `select`, `select_by_pk`, `custom_name`. |
| **Nombres de columna personalizados** (`column_config`, antiguo `custom_column_names`) | trasladado | `columns[].alias` | El nombre personalizado de cada columna pasa a ser su alias. |
| **Otros nombres de campo raíz** (`select_by_pk`, `select_aggregate`, `insert`, ...) | asignado | El campo único de la tabla | Se informa por nombre. Una lectura por clave es un filtro sobre la tabla, un agregado su campo `_aggregate`, una escritura su mutación. |
| **Permisos de select** (columnas, filtro) | trasladado | `columns[].visible_to[]`, `rls_rules[]` | Las listas de columnas pasan a ser `visible_to`; se admite `*`. Los filtros pasan a SQL; las variables de sesión pasan a `current_setting('provisa.<name>')`. |
| **Filtros a través de relationships** | trasladado | Subconsultas `EXISTS` en `rls_rules[]` | La regla sigue la relationship hasta su tabla. |
| **Permisos de insert, update y delete** | trasladado | `tables[].write_grants[]` | Un permiso por rol y operación: columnas de `columns`, `rows` de `filter`, `check` de `check`, presets de `set`. Los roles obtienen el derecho `write`. |
| **Rol admin** | trasladado | `org_admin` | Mantiene todos los permisos sobre todas las columnas y filas, y se nombra en cada columna importada. |
| **Relationships de objeto** | trasladado | `relationships[]` con `cardinality: many-to-one` | Aprobadas. El campo GraphQL es el nombre de la relationship de Hasura. |
| **Relationships de array** | trasladado | `relationships[]` con `cardinality: one-to-many` | Aprobadas. El campo GraphQL es el nombre de la relationship de Hasura. |
| **Roles heredados** (un rol) | trasladado | `roles[].parent_role_id` | El rol pasa a ser el padre. |
| **Roles heredados** (varios roles) | asignado | Un rol que mantiene la unión de sus permisos | Los roles de Provisa tienen un padre, así que la unión se escribe explícitamente en el rol. |
| **Computed fields** | rehacer | Una columna de vista | La exportación nombra la función, no su cuerpo. El informe da el `SELECT` que añadir. |
| **Funciones rastreadas** | trasladado, si no rehacer | Comandos `functions[]` | La vista previa lee la firma de la base de datos. Sin ella, añada la función en la página Commands. |
| **Actions** | asignado | Comandos que llaman al handler | `{{VAR}}` en la URL del handler se escribe `${env:VAR}` y se resuelve cuando se llama al comando. |
| **Cron triggers** | trasladado | `scheduled_triggers[]` | Expresión cron, URL, payload e indicador enabled. Los reintentos no se trasladan (se advierte). |
| **Event triggers** | asignado | `scheduled_triggers[]` sobre el cambio de la tabla | Se dispara por lote de cambios con cualquier cambio. Los reintentos no se trasladan (se advierte). |
| **Remote schemas** | asignado | `sources[]` (`graphql_remote`) incorporado como tablas | Se conservan nombre, URL, cabeceras y autenticación; los campos raíz pasan a ser tablas gobernadas. |
| **Tablas enum** | trasladado | Tabla creada | El indicador `is_enum` no se traslada (sin equivalente en Provisa). |
| **Allow lists, query collections, endpoints REST** | rehacer | Ninguno | Se informan cuando están presentes en una exportación consolidada; no se convierten. |

### Relationships {: #relationships }

- El campo GraphQL de cada relationship es el nombre de Hasura. Cuando la tabla ya tiene un campo con ese nombre, la relationship toma el nombre que le da Provisa y el informe advierte.
- Dos relationships que declaran el mismo join se fusionan en una. El informe indica cuál se fusionó con cuál.
- Una relationship que une por varios pares de columnas se advierte y no se crea, ya que una relación de Provisa une por un par. Las reglas de filas a través de ella se convierten igualmente.
- Una relationship de objeto declarada solo por columna de clave foránea se resuelve mediante la relationship de array inversa; sin una, se advierte.

### Permisos de escritura {: #write-grants }

Un permiso indica qué puede escribir un rol en una tabla mediante una operación. Un `filter` o `check` vacío es todas las filas (`TRUE`). Un valor de `set` `X-Hasura-User-Id` pasa a ser un preset `session` de `hasura_user_id`; cualquier otro valor pasa a ser un preset `literal`. Una columna con preset no está también en las columnas del permiso.

### Actions, funciones rastreadas y triggers {: #actions-tracked-functions-and-triggers }

- Una **action** se convierte en un comando que llama a su handler, visible para los roles que nombran sus permisos (solo para `org_admin` cuando no nombran ninguno). Una action asíncrona se advierte: el comando espera la respuesta del handler.
- Una **función rastreada** se convierte en un comando cuando la vista previa puede leer sus argumentos y su tabla de retorno de un origen PostgreSQL. Sin conexión, y para cualquier función que no exista, esté sobrecargada o devuelva una tabla que la importación no registra, pasa a rehacer.
- Un **event trigger** se dispara con cada cambio de su tabla, una vez por lote, y envía la tabla y las filas cambiadas en lugar de un evento por fila con valores antiguos y nuevos. Un trigger que escuchaba menos de las tres operaciones se advierte.

## Pasos posteriores a la conversión {: #post-conversion-steps }

1. **Revise el informe.** Empiece por `Redo by hand`.
2. **Configure las conexiones de origen.** Una conexión que la exportación escribe se convierte tal como está. Una leída de una variable de entorno llega vacía. Use `--source-overrides` o edite la salida directamente.
3. **Verifique las asignaciones de dominio.** Sin `--domain-map`, todas las tablas van a `default`.
   Asigne esquemas a dominios con `--domain-map public=core analytics=reporting`.
4. **Revise las reglas de filas.** Los filtros se convierten a SQL. Revise las expresiones booleanas
   complejas (`_and`/`_or`/`_exists` anidados) y los filtros a través de relationships.
5. **Rehaga lo que se lista.** Computed fields, funciones rastreadas que la vista previa no pudo leer,
   reintentos en triggers y autenticación por webhook no tienen conversión automática.
6. **Configure la autenticación.** Si su instancia de Hasura usa autenticación JWT/webhook, cree un archivo
   de entorno de autenticación y vuelva a ejecutar con `--auth-env-file`.
7. **Envíe las variables de sesión.** Configure el proveedor de identidad para que el token lleve cada variable de sesión que el informe lista en «trasladado a» (por ejemplo `x-hasura-user-id`, en el nivel superior o dentro del objeto de claims de Hasura), o regístrela por miembro como atributo.
8. **Pruebe.** Inicie el servidor de Provisa y verifique las consultas contra sus orígenes de datos.

## Problemas comunes y solución de problemas {: #common-issues-and-troubleshooting }

### La conexión viene de una variable de entorno {: #connection-comes-from-an-environment-variable }

Si la `database_url` del origen es una referencia a una variable de entorno (`{"from_env": "PG_URL"}`),
su valor no está en la exportación. El conversor no adivina ninguno: el origen tiene vacíos
host, base de datos, usuario y contraseña, y el informe lo lista en `Redo by hand`.
Introduzca la conexión en la configuración de orígenes de la importación (o con `--source-overrides`) y convierta
de nuevo.

### Columnas comodín {: #wildcard-columns }

Cuando un permiso otorga `columns: "*"`, el convertidor crea una única entrada de columna
comodín. Después de la conversión, reemplácela con una lista de columnas explícita
leída del esquema de la base de datos.

### Fidelidad de los event triggers {: #event-trigger-fidelity }

Un event trigger se convierte en un trigger sobre los cambios de su tabla, que llama al mismo webhook. Se dispara
por lote de cambios con cualquier cambio, tanto si el cambio llegó por Provisa como directamente a la base de datos, y envía la tabla y las
filas cambiadas. Los valores antiguos y nuevos por fila, el filtrado por operación y los reintentos de Hasura no se trasladan;
el informe advierte de cada uno que corresponda.
Haga que el handler actúe sobre las filas a las que está destinado y reintente dentro de él si importa una llamada perdida.

### Roles ausentes {: #missing-roles }

Los roles se recogen de los permisos de tabla, los permisos de action y los roles heredados. Si un rol existe en Hasura
pero no aparece en ninguno de ellos, no aparecerá en la salida.

### Campos raíz personalizados {: #custom-root-fields }

Los nombres de campo raíz `select` y `select_by_pk` dan el alias de la tabla. Los demás campos raíz personalizados
(`select_aggregate`, `insert`, `update`, `delete`) se informan por nombre: Provisa sirve
una tabla como un solo campo, con los agregados y las escrituras como su campo `_aggregate` y sus mutaciones.

## Ejemplo {: #example }

Convertir un proyecto típico de Hasura v2 con dos esquemas asignados a dominios:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Estructura de la salida (abreviada):

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.hasura_user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.hasura_user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
