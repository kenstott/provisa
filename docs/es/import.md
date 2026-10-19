# Importando desde Hasura {: #importing-from-hasura }

Provisa puede convertir metadatos existentes de Hasura en un `config.yaml` de Provisa, preservando las tablas rastreadas, relaciones, permisos y esquemas remotos.

## Importación interactiva (Admin → Import Hasura Config) {: #interactive-import-admin-import-hasura-config }

La superficie de administración ejecuta los mismos convertidores, por lo que una importación no requiere acceso a shell ni un ciclo de ida y vuelta de archivo de configuración. Requiere la capacidad `org_settings`; la importación se aplica en la organización en la que la sesión está actuando.

1. **Cargar.** Elija un directorio de metadatos de Hasura v2 comprimido en zip, un proyecto DDN comprimido en zip, una exportación de metadatos consolidada (`.yaml`/`.json`, incluido el envoltorio `{resource_version, metadata}` que devuelve la API de metadatos), o un único `.hml`. Deje el formato en *Detect automatically* a menos que la carga sea ambigua.
2. **Mapear dominios** (opcional). Cada par mapea un esquema v2 o un subgraph de DDN a un dominio de Provisa; lo que no se mapee conserva su nombre original.
3. **Convertir y previsualizar.** El servidor convierte y devuelve los recuentos, el informe de la conversión (véase [El informe](#the-report)) y la configuración generada. En este paso no se escribe nada.
4. **Revisar y editar.** La configuración es editable in situ: detalles de conexión, nombres de dominio, nombres de rol. Lo que aplique es lo que se muestra.
5. **Aplicar.** *Aplicar a esta organización* carga la configuración y reconstruye los esquemas de la organización. Una importación siempre se fusiona con lo que ya tiene la organización.

Endpoints: `POST /admin/import/hasura/preview` y `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Exportar metadatos {: #export-metadata }

Se convierten ambos formatos de exportación.

**El directorio de metadatos de la CLI.** `hasura metadata export` escribe un directorio. `databases/databases.yaml` lista cada origen con su tipo y su conexión, y apunta a sus tablas y funciones mediante índices `!include`:

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

El formato plano (`tables.yaml` y `actions.yaml` en la raíz) también se convierte.

**El documento único de la API de metadatos.** La API de metadatos devuelve toda la exportación como un solo objeto, con la `configuration` de cada origen que el formato de directorio no tiene dónde poner:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

Se aceptan tanto el objeto simple como el sobre `{resource_version, metadata}`. La importación de administración admite esta forma (véase [Importación interactiva](#interactive-import-admin-import-hasura-config)); el conversor de línea de comandos solo lee directorios.

### Convertir {: #convert }

El conversor de línea de comandos lee un **directorio** de metadatos de Hasura (el formato que produce `hasura metadata export`, o el formato plano `tables.yaml` / `actions.yaml`) y escribe una configuración de Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omita `-o` para escribir la configuración en stdout. Cuando queda algo por rehacer a mano, el informe se imprime en stderr.

Opciones:

| Opción | Propósito |
| ------ | --------- |
| `-o`, `--output` | Ruta del YAML de salida (por defecto: stdout) |
| `--source-overrides` | Archivo YAML con sobrescrituras de conexión por origen (host, puerto, credenciales), indexado por nombre de origen |
| `--domain-map` | Asignaciones de esquema a dominio como pares `SCHEMA=DOMAIN` |
| `--auth-env-file` | Archivo `.env` con la configuración de autenticación; convierte JWT/JWK, el secreto de administrador y el mapa de claims |
| `--dry-run` | Analizar y validar sin escribir la salida |

### El informe {: #the-report }

Cada conversión clasifica lo que encuentra en tres partes. Léalas en este orden.

1. **Trasladado tal cual.** El objeto tiene una contrapartida directa en Provisa y se convierte sin cambiar de significado: orígenes, tablas, relaciones aprobadas, permisos de select, cron triggers.
2. **Asignado a una función mejor de Provisa.** Provisa tiene una función que hace mejor el trabajo que la de Hasura, así que el objeto se convierte en esa función. Cada elemento dice por qué. Aquí van los event triggers, las actions, los remote schemas, los nombres de campo raíz adicionales y los roles heredados de varios roles.
3. **Rehacer a mano.** El objeto no tiene equivalente en Provisa, o a la exportación le falta lo que necesita la conversión. Cada elemento dice qué hacer. Estas son las advertencias.

La vista previa devuelve las tres partes como listas. El conversor de línea de comandos las imprime en stderr como `Carried as`, `Mapped to` y `Redo by hand`, cada una con un recuento. Una conversión sin nada en la tercera parte está terminada.

### Qué se convierte {: #what-gets-converted }

| Concepto de Hasura | Parte | Resultado en Provisa |
| --------------- | ---- | -------------- |
| Origen (`kind`, `connection_info`) | trasladado | `sources[]`. Una conexión escrita en la exportación se convierte en host, puerto, base de datos y credenciales. Una leída de una variable de entorno no se adivina (véase [Conexiones y dominios](#connections-and-domains-on-the-import-tab)). |
| Tabla rastreada | trasladado | Una tabla registrada. Su nombre personalizado (o su campo raíz `select`) pasa a ser el alias de la tabla. |
| Nombres de columna personalizados (`column_config`, o el antiguo `custom_column_names`) | trasladado | `alias` de la columna. |
| Nombres de campo raíz más allá de `select` (`select_by_pk`, `select_aggregate`, `insert`, ...) | asignado | Se informa por nombre. Provisa sirve una tabla como un solo campo: una lectura por clave es un filtro sobre él, un agregado su campo `_aggregate`, una escritura su mutación. Compruebe que las consultas de la app tengan esa forma. |
| Relationship de objeto y de array | trasladado | Una relación aprobada (`many-to-one` para objeto, `one-to-many` para array) cuyo campo GraphQL es el nombre de la relationship de Hasura. Los casos que difieren se detallan abajo. |
| Permiso de select | trasladado | Permisos de columna (`visible_to`) y una regla de filas. Un término con variable de sesión (`X-Hasura-User-Id`) pasa a ser `current_setting('provisa.hasura_user_id')`, que la solicitud enlaza en el momento de la consulta con los claims de la identidad o los atributos del miembro. Una variable de sesión de Hasura conserva su propio nombre; `provisa.user_id` sigue siendo la identidad con sesión iniciada. |
| Regla de filas a través de una relationship | trasladado | Una subconsulta `EXISTS` sobre la tabla relacionada. |
| Permiso de insert, update y delete | trasladado | Permisos de escritura sobre la tabla: uno por rol y operación. `columns` pasan a ser las columnas del permiso, `filter` su `rows`, `check` su `check` y `set` sus presets. |
| Rol heredado de un rol | trasladado | Ese rol como `parent_role_id`. |
| Rol heredado de varios roles | asignado | Un rol que mantiene la unión de sus permisos, escrita explícitamente: un rol de Provisa tiene un padre. |
| Computed field | rehacer | Una columna de una vista sobre la tabla. |
| Función rastreada | trasladado | Un comando; véase abajo. Si no, rehacer. |
| Action | asignado | Un comando que llama al handler de la action. |
| Cron trigger | trasladado | Un trigger programado. |
| Event trigger | asignado | Un trigger sobre los cambios de la tabla. |
| Remote schema | asignado | Un origen `graphql_remote` cuyos campos raíz se incorporan como tablas gobernadas. |

Casos de relationships:

- **Colisión.** Cuando la tabla ya tiene un campo con el nombre de la relationship, la relationship toma el nombre que le da Provisa, y el informe advierte. Renómbrela en la página Relationships si la app necesita otro nombre.
- **El mismo join dos veces.** Una segunda relationship que declara un join ya declarado se fusiona con la primera. El informe lo indica entre lo trasladado.
- **Varios pares de columnas.** Una relación de Provisa une por un par de columnas. Una relationship que une por varios se advierte y no se crea, pero las reglas de filas a través de ella se trasladan.
- **Solo columna de clave foránea.** Una relationship de objeto declarada con `foreign_key_constraint_on: artist_id` no nombra destino en la exportación. El conversor la resuelve mediante la relationship de array inversa y advierte cuando no hay ninguna.

Detalles de los permisos de escritura:

- Un `filter` o `check` vacío es todas las filas, escrito `TRUE`.
- Un valor de `set` que es una variable de sesión (`X-Hasura-User-Id`) pasa a ser un preset `session` de la variable de sesión correspondiente (`X-Hasura-User-Id` pasa a ser `hasura_user_id`). Cualquier otro valor pasa a ser un preset `literal`. Una columna con preset no está también en las columnas del permiso.
- El admin implícito de Hasura tiene todos los permisos. Pasa a ser el rol `org_admin`, que mantiene un permiso de insert, update y delete sobre todas las columnas y todas las filas, y se nombra en cada columna importada.
- Cada rol con un permiso de insert, update o delete recibe además el derecho `write`.

Detalles de los comandos:

- **Función rastreada.** La exportación nombra la función y los roles que la llaman, no sus argumentos ni lo que devuelve. La vista previa lee ambos del catálogo de la base de datos y registra un comando con la firma encontrada. Cuando no se alcanza el origen, no es PostgreSQL, o la función no existe, está sobrecargada o devuelve filas de una tabla que la importación no registra, la función pasa a rehacer. El conversor de línea de comandos nunca alcanza una base de datos, así que toda función rastreada que encuentra pasa a rehacer.
- **Action.** El comando llama al handler de la action con un `POST`. Un `{{VAR}}` en la URL del handler se escribe `${env:VAR}` y se resuelve cuando se llama al comando. El comando es visible para los roles que nombran los permisos de la action, o solo para `org_admin` cuando no nombran ninguno. Una action asíncrona se advierte: su comando espera la respuesta del handler. Un tipo de retorno cuyos campos no trae la exportación se advierte.
- **Event trigger.** El trigger se dispara con cada cambio de la tabla, una vez por lote de cambios, y envía la tabla y las filas cambiadas. No se dispara por fila con valores antiguos y nuevos. Un trigger de Hasura sobre menos de las tres operaciones se advierte, ya que el trigger de Provisa se dispara con todas. Los reintentos no se trasladan: un trigger de Hasura con `num_retries` se advierte, porque un trigger de Provisa llama una vez. Reintente en el handler si importa una llamada perdida.
- **Cron trigger.** La expresión cron, la URL, el payload y el indicador enabled se trasladan. Los reintentos no se trasladan y se advierten igual.

Un documento consolidado también contiene secciones para las que el conversor no tiene destino: `query_collections`, `allowlist`, `rest_endpoints`, `network` y `opentelemetry`. Cuando hay una presente, el informe la lista entre lo que hay que rehacer. El lector de directorios nunca abre sus archivos.

### Conexiones y dominios en la pestaña de importación {: #connections-and-domains-on-the-import-tab }

Una conexión que la exportación escribe se convierte tal como está. Cuando la exportación lee una URL de base de datos de una variable de entorno (`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`), el valor de la variable no está en la exportación, así que la conversión no adivina ninguno. El origen se registra con los campos de conexión vacíos, y el informe indica rehacerlo: introduzca el host, puerto, base de datos, usuario y contraseña del origen en la configuración de orígenes de la importación y convierta de nuevo. Solo viajan los campos que cambie, como sobrescrituras de origen. Las filas de dominio cubren cada esquema, subgrafo y remote schema que trae la carga; cada una es un selector sobre los dominios existentes de la organización que también acepta un nombre escrito, marcado como «nuevo dominio» cuando no coincide con ninguno. Aplicar se fusiona con lo que la organización ya tiene.

### Los tipos provienen del origen en la vista previa {: #types-come-from-the-source-at-preview }

Una exportación de Hasura nombra columnas sin tipos, y una tabla rastreada sin permisos no nombra columnas. La vista previa se ejecuta con las conexiones de origen que usted suministra, así que lee `information_schema.columns` de cada origen SQL alcanzable: cada columna sin tipo obtiene el tipo del origen mapeado al vocabulario de la IR, y una tabla sin columnas toma cada columna que tiene el origen, visible solo para `org_admin`, ya que Hasura no la expuso a ningún otro rol. Un origen que la vista previa no puede alcanzar se reporta como una advertencia `[sources]` y sus columnas quedan sin tipo para que usted las complete antes de aplicar.

### Limitaciones {: #limitations }

- **Computed fields** se rehacen a mano: la exportación nombra la función de la base de datos pero no la define. Añádala como columna de una vista sobre la tabla, o calcúlela en la app.
- **Remote schemas** se incorporan como tablas a partir de los SDL de permisos de rol. Un remote schema sin permisos no incorpora nada, ya que la exportación no trae otra declaración de su forma.
- **Las funciones rastreadas** necesitan la base de datos en la vista previa para leer su firma; sin conexión, se rehacen en la página Commands.
- **Los reintentos de event y cron triggers** no se trasladan.

---

## Hasura DDN (v3)

### Ubicar el proyecto HML {: #locate-the-hml-project }

El convertidor DDN lee directamente el **directorio** del proyecto DDN con archivos `.hml`, sin ningún paso de build del supergraph. El primer componente de directorio bajo la raíz del proyecto se toma como el nombre del subgraph; los archivos bajo `globals/` se asignan al subgraph `globals`.

### Convertir {: #convert_1 }

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Omita `-o` para escribir el config en stdout.

Flags:

| Flag | Propósito |
| ------ | --------- |
| `-o`, `--output` | Ruta del YAML de salida (por defecto: stdout) |
| `--source-overrides` | Archivo YAML con overrides de conexión por origen |
| `--domain-map` | Mapeos de subgraph a dominio como pares `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Ruta de salida para el archivo complementario de expresiones agregadas (por defecto: `<output>-aggregates.yaml`) |
| `--dry-run` | Analiza y valida sin escribir la salida |

Los metadatos de `AggregateExpression` se preservan en un archivo complementario `*-aggregates.yaml`.

### Qué se convierte {: #what-gets-converted_1 }

| Concepto de DDN | Equivalente en Provisa |
| ------------ | ------------------- |
| Modelo de subgraph | `tables[]` bajo un origen |
| Relación | `relationships[]` |
| Regla de permiso | Filtro RLS |
| Command | Mutación webhook o vista |
| Connector | Entrada de origen con detalles de conexión |

### Limitaciones {: #limitations_1 }

- **Lambda connectors** (funciones TypeScript/Python) requieren configuración manual de webhook
- **Lifecycle plugins** no tienen equivalente directo
- **Modos de autenticación de DDN** se mapean a proveedores de autenticación de Provisa, pero las rutas de claims JWT pueden requerir ajustes

---

## Después de la importación {: #after-import }

1. Revise el `config.yaml` generado y el informe. Empiece por los elementos de `Redo by hand`
2. Verifique las credenciales de conexión. Una conexión que la exportación leyó de una variable de entorno llega vacía; el informe la nombra
3. Inicie Provisa y confirme que las tablas aparecen en el Explorer
4. Ejecute sus consultas GraphQL existentes; el esquema es compatible con patrones comunes
5. Envíe las consultas para aprobación mediante la Admin API o la UI antes de habilitar el gobierno de producción
