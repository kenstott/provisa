# Migración de Hasura DDN (v3) a Provisa

## Prerrequisitos

1. Un proyecto de Hasura DDN con archivos HML (extensión `.hml`).
   Los proyectos DDN normalmente tienen una estructura de directorios como:
   ```
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```
2. Python 3.11+ con el paquete `provisa` instalado.

## Uso de la CLI

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### Argumentos

| Argumento | Requerido | Descripción |
|----------|----------|-------------|
| `hml_dir` | Sí | Ruta al directorio del proyecto DDN HML (se explora recursivamente en busca de archivos `.hml`) |

### Opciones

| Opción | Predeterminado | Descripción |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | Ruta del archivo YAML de salida |
| `--source-overrides FILE` | Ninguno | Archivo YAML con anulaciones de conexión por origen |
| `--domain-map KEY=VAL ...` | Ninguno | Mapeos de subgraph a dominio (por ejemplo, `app=core analytics=reporting`) |
| `--dry-run` | desactivado | Analiza y valida sin escribir la salida |

### Archivo de anulaciones de origen

Un archivo YAML indexado por nombre de conector (tras la sanitización del ID: espacios, puntos y
barras se convierten en guiones bajos) con propiedades de conexión:

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## Matriz de paridad de funciones

| Tipo DDN | Equivalente en Provisa | Notas |
|---|---|---|
| **DataConnectorLink** | `sources[]` | El tipo de origen se infiere de la URL del conector (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). Los detalles de conexión usan marcadores de posición por defecto; use `--source-overrides` para establecer los valores reales. |
| **ObjectType** | Definiciones de columnas en `tables[]` | Los campos se convierten en columnas. `dataConnectorTypeMapping.fieldMapping` resuelve los nombres de campo de GraphQL a nombres de columna físicos. |
| **Model** | `tables[]` | Cada Model produce una tabla. `source_id` proviene del conector, `table_name` de la colección. `graphql_type_name` se convierte en `alias`. El subgraph (y por tanto `domain_id`) se deriva del directorio del archivo: el primer componente de directorio bajo la raíz del proyecto. |
| **Relationship** | `relationships[]` | Tipo Object -> `many-to-one`, tipo Array -> `one-to-many`. El mapeo de campos se resuelve mediante la búsqueda de la columna física. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` determina qué roles pueden ver cada columna. |
| **ModelPermissions** | `rls_rules[]` | Los predicados de filtro se convierten en cláusulas SQL WHERE. Admite `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. Las referencias a variables de sesión se conservan como `${x-hasura-...}`. |
| **Command** | `functions[]` | Se mapean tanto funciones como procedimientos. Se conservan los argumentos, el tipo de retorno y el nombre del campo raíz de GraphQL. `domain_id` se establece a partir del subgraph. |
| **AggregateExpression** | Archivo adjunto `provisa-aggregates.yaml` | Count, count_distinct y las funciones de agregación por campo se conservan en un archivo adjunto y se convierten a la configuración de agregados de Provisa. |
| **BooleanExpressionType** | Omitido (en silencio) | Usado internamente por DDN para el filtrado; no se necesita un equivalente directo en Provisa. |
| **AuthConfig** | Omitido (en silencio) | La configuración de autenticación de DDN no se mapea; configure la autenticación de Provisa por separado. |
| **ScalarType** | Omitido | Se emite una advertencia con el recuento. |
| **GraphqlConfig** | Omitido | Se emite una advertencia con el recuento. |
| **CompatibilityConfig** | Omitido | Se emite una advertencia con el recuento. |
| **Otros tipos no reconocidos** | Omitido | Se emite una advertencia con el recuento por tipo. |

## Concepto clave: resolución de campo GraphQL a columna física

DDN separa el esquema GraphQL (nombres de campo) del esquema físico de la base de datos
(nombres de columna) mediante `dataConnectorTypeMapping` en los ObjectTypes. El conversor:

1. Lee las entradas de `fieldMapping` de los mapeos de tipo de cada ObjectType.
2. Construye una tabla de búsqueda: `{graphql_field_name -> physical_column_name}`.
3. Para los campos sin un mapeo explícito, asume que el nombre del campo es igual al de la columna.
4. Usa esta tabla de búsqueda al construir columnas, relaciones y expresiones de filtro RLS.

Esto significa que el `provisa.yaml` de salida usa **nombres de columna físicos** para `columns[].name`
y establece `columns[].alias` con el nombre del campo GraphQL cuando difieren.

## Pasos posteriores a la conversión

1. **Revise el YAML de salida.** Verifique los orígenes, las tablas y los mapeos de columnas.
2. **Configure las conexiones de origen.** Los conectores solo proporcionan una pista de URL para la
   detección de tipo. El host, puerto, base de datos y credenciales reales deben suministrarse mediante
   `--source-overrides` o editando la salida.
3. **Verifique las asignaciones de dominio.** Los nombres de subgraph se derivan de la estructura de
   directorios (el primer componente de directorio bajo la raíz del proyecto). Sin `--domain-map`, cada
   nombre de subgraph se convierte directamente en un ID de dominio. Use `--domain-map` para renombrarlos.
4. **Compruebe las reglas RLS.** Los predicados de filtro de DDN se convierten en aproximaciones SQL.
   La lógica booleana anidada (`_and`/`_or`/`_not`) es compatible, pero los filtros complejos que
   recorren relaciones pueden requerir revisión manual.
5. **Revise la configuración de agregados.** Las expresiones de agregación se escriben en un archivo
   adjunto `provisa-aggregates.yaml` y se convierten a la configuración de agregados de Provisa.
6. **Revise las advertencias.** El conversor imprime un resumen en stderr con los tipos DDN omitidos
   y cualquier modelo que haga referencia a ObjectTypes desconocidos.
7. **Pruebe.** Inicie el servidor Provisa y verifique las consultas contra sus orígenes de datos.

## Problemas comunes y solución de problemas

### Falla la detección del tipo de origen

La URL del conector se usa de forma heurística (buscando palabras clave como "postgres",
"mysql", "mongo"). Si la URL no contiene una palabra clave reconocible, el origen usa
`postgresql` por defecto. Anule este comportamiento con `--source-overrides`.

### Falta el ObjectType de un Model

Si un Model hace referencia a un nombre de ObjectType que no se encontró en ningún archivo `.hml`,
la tabla se omite y se emite una advertencia. Asegúrese de que todos los archivos HML estén incluidos
en el directorio explorado.

### Descubrimiento de subgraphs

Los subgraphs se derivan de la estructura de directorios: el primer componente de directorio bajo la
raíz del proyecto se toma como el nombre del subgraph. El campo `subgraph` dentro de los documentos
HML no se utiliza. Los archivos bajo un directorio `globals/` se asignan al subgraph `globals` y se
excluyen del descubrimiento de dominios.

### Resolución del origen de la relación

Las relaciones hacen referencia a un `source_type` (nombre de ObjectType) y a un `target_model` (nombre
de Model). Si ningún Model usa el ObjectType indicado, la relación se omite en silencio.

### Alias de columna por todas partes

Si su proyecto DDN usa `fieldMapping` de forma extensiva, espere que la mayoría de las columnas tengan
un `alias` en la salida. Este es el comportamiento correcto: `name` es la columna física,
`alias` es el nombre GraphQL que usaba su aplicación.

### Expresiones de agregación

Las expresiones de agregación se conservan en un archivo adjunto `provisa-aggregates.yaml` escrito
junto a la salida y se convierten a la configuración de agregados de Provisa. No se almacenan en
la `description` de la tabla.

## Ejemplo: conversión de un proyecto DDN de Chinook

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

Estructura de salida:

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
