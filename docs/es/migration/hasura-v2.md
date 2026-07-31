# Migración de Hasura v2 a Provisa

## Requisitos previos

1. Una instancia de Hasura v2 en ejecución (v2.x) con los metadatos exportados.
2. Exportar los metadatos con la CLI de Hasura:

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Esto crea un directorio `metadata/` que contiene `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml`, etc.
3. Python 3.11+ con el paquete `provisa` instalado.

## Uso de la CLI

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argumentos

| Argumento | Obligatorio | Descripción |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Sí | Ruta al directorio de metadatos exportado de Hasura v2 |

### Opciones

| Opción | Predeterminado | Descripción |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Ruta del archivo YAML de salida |
| `--source-overrides FILE` | Ninguno | Archivo YAML con anulaciones de conexión por origen |
| `--domain-map KEY=VAL ...` | Ninguno | Asignaciones de esquema a dominio (por ejemplo, `public=core hr=people`) |
| `--auth-env-file FILE` | Ninguno | Ruta a un archivo `.env` con la configuración de autenticación JWT/admin-secret |
| `--dry-run` | desactivado | Analiza y valida sin escribir la salida |

### Archivo de anulaciones de origen

Un archivo YAML indexado por nombre de origen con propiedades de conexión a anular:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Archivo de entorno de autenticación

Un archivo con estilo `.env` que contiene la configuración de autenticación de Hasura
a convertir. El convertidor asigna:

- JWT con `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Autenticación por webhook -> se emite una advertencia (sin equivalente en Provisa).

## Matriz de paridad de funciones

| Función de Hasura v2 | Equivalente en Provisa | Notas |
| --- | --- | --- |
| **Orígenes** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | Tipo asignado: pg/postgres -> postgresql, mssql -> sqlserver. La URL de conexión se analiza en host/port/database/username/password. Se conserva la configuración del pool. |
| **Tablas** (tablas rastreadas) | `tables[]` | Se conservan el esquema y el nombre de la tabla. `source_id` enlaza con el origen. |
| **Nombres de tabla personalizados** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | Primer valor no nulo entre `select`, `select_by_pk`, `custom_name`. |
| **Nombres de columna personalizados** | `columns[].alias` | Asigna el diccionario `custom_column_names` a alias de columna. |
| **Permisos de selección** (columnas, filtro) | `columns[].visible_to[]`, `rls_rules[]` | Las listas de columnas se convierten en `visible_to`. Se admiten columnas comodín (`*`). Los filtros se convierten a SQL mediante `bool_expr_to_sql`. |
| **Permisos de inserción/actualización** (columnas) | `columns[].writable_by[]` | Las listas de columnas se convierten en `writable_by`. Los roles se actualizan con la capacidad `write`. |
| **Permisos de eliminación** | Actualización de capacidad de rol | El rol obtiene la capacidad `write`. No hay asignación de eliminación por tabla. |
| **Relaciones de objeto** | `relationships[]` con `cardinality: many-to-one` | Se conserva la asignación de columnas. |
| **Relaciones de arreglo** | `relationships[]` con `cardinality: one-to-many` | Se conserva la asignación de columnas. |
| **Campos calculados** | `functions[]` | Se asignan a Function con `returns` apuntando al ID de la tabla principal. |
| **Funciones rastreadas** | `functions[]` | `exposed_as` toma mutation de forma predeterminada. Se conserva el esquema. |
| **Actions** (controlador de procedimiento almacenado) | `functions[]` | Se convierte en una configuración de Function cuando está respaldada por un procedimiento almacenado. |
| **Actions** (controlador de webhook) | No se convierte | Se emite una advertencia, incluida la URL del controlador. |
| **Disparadores cron** | No se convierten | Se emite una advertencia. (Existen disparadores programados en tiempo de ejecución, pero el convertidor no los asigna). |
| **Disparadores de eventos** | No se convierten | Se emite una advertencia. (Existen disparadores de eventos en tiempo de ejecución, pero el convertidor no los asigna). |
| **Roles heredados** | `roles[].parent_role_id` | El primer rol en `role_set` se convierte en el rol principal. Se crean todos los roles secundarios. |
| **Esquemas remotos** | `sources[]` (`graphql_remote`) | Se registra como un origen `graphql_remote`. Se conservan el nombre, la URL, los encabezados y la configuración de autenticación. |
| **Tablas enum** | Se crea la tabla | El indicador `is_enum` no se traslada (sin equivalente en Provisa). |
| **Listas de permitidos** | Se omiten | No están presentes en el modelo de metadatos. |

## Pasos posteriores a la conversión

1. **Revisar el YAML de salida.** Verificar que los orígenes, las tablas y los roles se vean correctos.
2. **Configurar las conexiones de origen.** El convertidor analiza las URL de conexión, pero
   recurre a `localhost` si el análisis falla. Use `--source-overrides` o edite la salida directamente.
3. **Verificar las asignaciones de dominio.** Sin `--domain-map`, todas las tablas quedan en `default`.
   Asigne esquemas a dominios con `--domain-map public=core analytics=reporting`.
4. **Revisar las reglas de RLS.** Los filtros se convierten en aproximaciones SQL. Las expresiones
   booleanas complejas (`_and`/`_or`/`_exists` anidados) deben revisarse manualmente.
5. **Revisar las advertencias.** El convertidor imprime un resumen de advertencias en stderr para las
   funciones que no puede asignar (disparadores de eventos, disparadores cron, actions respaldadas por webhook).
6. **Configurar la autenticación.** Si su instancia de Hasura usa autenticación JWT/webhook, cree un
   archivo de entorno de autenticación y vuelva a ejecutar con `--auth-env-file`.
7. **Probar.** Inicie el servidor de Provisa y verifique las consultas contra sus orígenes de datos.

## Problemas comunes y solución de problemas

### La URL de conexión no se analiza

Si `database_url` del origen es una referencia a una variable de entorno (`{"from_env": "PG_URL"}`),
el convertidor no puede resolverla en el momento de la conversión. El origen tendrá valores
de marcador de posición (`host: localhost`, `database: default`). Corríjalo con `--source-overrides`.

### Columnas comodín

Cuando un permiso otorga `columns: "*"`, el convertidor crea una única entrada de columna
comodín. Después de la conversión, quizás quiera reemplazarla con listas de columnas explícitas
inspeccionando el esquema real de la base de datos.

### Fidelidad de los disparadores de eventos

Los disparadores de eventos se convierten con `operations` y `webhook_url`, pero las garantías
de entrega específicas de Hasura (exactamente una vez, reentrega) no tienen equivalentes directos
en Provisa. Revise la sección `event_triggers` y configure su infraestructura de webhooks en consecuencia.

### Roles faltantes

Los roles se recopilan únicamente a partir de las entradas de permisos. Si un rol existe en
Hasura pero no tiene permisos en ninguna tabla ni action, no aparecerá en la salida.

### Campos raíz personalizados

Solo los campos raíz `select` y `select_by_pk` se usan para el alias de la tabla. Otros
campos raíz personalizados (`select_aggregate`, `insert`, `update`, `delete`) no se asignan.

## Ejemplo

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

Estructura de salida:

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
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
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
