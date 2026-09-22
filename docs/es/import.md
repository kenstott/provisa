# Importando desde Hasura

Provisa puede convertir metadatos existentes de Hasura en un `config.yaml` de Provisa, preservando las tablas rastreadas, relaciones, permisos y esquemas remotos.

## Importación interactiva (Admin → Import Hasura Config)

La superficie de administración ejecuta los mismos convertidores, por lo que una importación no requiere acceso a shell ni un ciclo de ida y vuelta de archivo de configuración. Requiere la capacidad `org_settings`; la importación se aplica en la organización en la que la sesión está actuando.

1. **Cargar.** Elija un directorio de metadatos de Hasura v2 comprimido en zip, un proyecto DDN comprimido en zip, una exportación de metadatos consolidada (`.yaml`/`.json`, incluido el envoltorio `{resource_version, metadata}` que devuelve la API de metadatos), o un único `.hml`. Deje el formato en *Detect automatically* a menos que la carga sea ambigua.
2. **Mapear dominios** (opcional). Cada par mapea un esquema v2 o un subgraph de DDN a un dominio de Provisa; lo que no se mapee conserva su nombre original.
3. **Convertir y previsualizar.** El servidor convierte y devuelve los recuentos, las advertencias del convertidor y la configuración generada. En este paso no se escribe nada.
4. **Revisar y editar.** La configuración es editable in situ — detalles de conexión, nombres de dominio, nombres de rol. Lo que aplique es lo que se muestra.
5. **Aplicar.** *Replace the existing semantic layer* elimina todo origen, tabla, rol y regla ausente de la configuración; si se deja desactivado, la importación se fusiona con lo que ya tiene la organización. Aplicar carga la configuración y reconstruye los esquemas de la organización.

Endpoints: `POST /admin/import/hasura/preview` y `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Exportar metadatos

Desde su consola o CLI de Hasura:

```bash
hasura metadata export --output metadata.yaml
```

O use la API de Hasura:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Convertir

El convertidor v2 lee un **directorio** de metadatos de Hasura (el diseño producido por `hasura metadata export`, o el diseño plano `tables.yaml` / `actions.yaml`) y escribe un config de Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omita `-o` para escribir el config en stdout.

Flags:

| Flag | Propósito |
| ------ | --------- |
| `-o`, `--output` | Ruta del YAML de salida (por defecto: stdout) |
| `--source-overrides` | Archivo YAML con overrides de conexión por origen (host, puerto, credenciales) |
| `--domain-map` | Mapeos de esquema a dominio como pares `SCHEMA=DOMAIN` |
| `--auth-env-file` | Archivo `.env` con configuración de autenticación; convierte JWT/JWK, secreto de administrador y mapa de claims |
| `--dry-run` | Analiza y valida sin escribir la salida |

### Qué se convierte

| Concepto de Hasura | Equivalente en Provisa |
| --------------- | ------------------- |
| Tabla rastreada | `tables[]` con `publish: true` |
| Relación de objeto | `relationships[]` con `cardinality: many-to-one`. Una declarada solo por columna FK (`foreign_key_constraint_on: artist_id`) no nombra un destino en la exportación; el conversor la resuelve a través de la relación de array inversa, y la descarta con una advertencia `[relationships]` cuando no hay ninguna. (REQ-1680) |
| Relación de array | `relationships[]` con `cardinality: one-to-many` |
| Permiso de select | Visibilidad de rol + filtro RLS. Un término de variable de sesión (`X-Hasura-User-Id`) se convierte en `current_setting('provisa.user_id')`, que la solicitud vincula desde el id de usuario y los claims de la identidad en el momento de la consulta. (REQ-1682) |
| Permiso de columna | `visible_to` / `writable_by` |
| Permiso de insert/update/delete | Mutación `writable_by` + RLS |
| Esquema remoto | Registro de origen `graphql_remote` más una tabla aterrizada por cada campo raíz de Query que los SDL de rol exponen; una columna es visible para cada rol cuyo SDL la expone, un argumento raíz no nulo se convierte en una columna de filtro nativo `_nf_`, los campos anidados se nombran en una advertencia. (REQ-1681) |
| Campo calculado | Entrada de `functions[]` con `kind: query` |

### Conexiones y dominios en la pestaña de importación

La exportación nombra sus bases de datos por variable de entorno, así que después de la primera conversión la pestaña lista cada origen SQL con la conexión que la conversión adivinó. Complete el host, puerto, base de datos, usuario y contraseña, y convierta de nuevo; solo los campos que cambió viajan, como anulaciones de origen. Las filas de dominio cubren cada esquema, subgrafo y esquema remoto que trae la carga; cada una es un selector sobre los dominios existentes de la organización que también acepta un nombre escrito, marcado como "new domain" cuando no coincide con ninguno. Aplicar hace merge con lo que la organización ya tiene, a menos que la casilla de reemplazo esté activada. (REQ-1687)

### Los tipos provienen del origen en la vista previa

Una exportación de Hasura nombra columnas sin tipos, y una tabla rastreada sin permisos no nombra columnas. La vista previa se ejecuta con las conexiones de origen que usted suministra, así que lee `information_schema.columns` de cada origen SQL alcanzable: cada columna sin tipo obtiene el tipo del origen mapeado al vocabulario de la IR, y una tabla sin columnas toma cada columna que tiene el origen, visible solo para `org_admin`, ya que Hasura no la expuso a ningún otro rol. Un origen que la vista previa no puede alcanzar se reporta como una advertencia `[sources]` y sus columnas quedan sin tipo para que usted las complete antes de aplicar. (REQ-1691, REQ-1684)

### Limitaciones

- **Actions**: se convierten automáticamente: las actions con handler HTTP se convierten en mutaciones `webhooks[]`; las actions con handler no HTTP (base de datos) se convierten en un placeholder de `functions[]` y emiten una advertencia para revisar el handler
- **Event triggers**: se convierten en configuración `event_triggers` por tabla (operaciones, URL de webhook, política de reintentos) y emiten una advertencia señalando fidelidad limitada
- **Esquemas remotos**: se convierten en entradas de origen `graphql_remote` y se aterrizan como tablas a partir de los SDL de permisos de rol; un esquema remoto sin permisos no aterriza nada, ya que la exportación no lleva ninguna otra declaración de su forma (REQ-1681)
- **Funciones SQL personalizadas**: requieren revisión — los casos simples se convierten en entradas de `functions[]`, los complejos requieren trabajo manual
- **Cron triggers**: se convierten en entradas de configuración de `scheduler`, preservando la expresión cron y el flag de habilitado

---

## Hasura DDN (v3)

### Ubicar el proyecto HML

El convertidor DDN lee directamente el **directorio** del proyecto DDN con archivos `.hml` — no se requiere un paso de build del supergraph. El primer componente de directorio bajo la raíz del proyecto se toma como el nombre del subgraph; los archivos bajo `globals/` se asignan al subgraph `globals`.

### Convertir

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

### Qué se convierte

| Concepto de DDN | Equivalente en Provisa |
| ------------ | ------------------- |
| Modelo de subgraph | `tables[]` bajo un origen |
| Relación | `relationships[]` |
| Regla de permiso | Filtro RLS |
| Command | Mutación webhook o vista |
| Connector | Entrada de origen con detalles de conexión |

### Limitaciones

- **Lambda connectors** (funciones TypeScript/Python) requieren configuración manual de webhook
- **Lifecycle plugins** no tienen equivalente directo
- **Modos de autenticación de DDN** se mapean a proveedores de autenticación de Provisa, pero las rutas de claims JWT pueden requerir ajustes

---

## Después de la importación

1. Revise el `config.yaml` generado — preste atención a las `warnings` del convertidor
2. Verifique las credenciales de conexión (el convertidor usa valores de marcador de posición)
3. Inicie Provisa y confirme que las tablas aparecen en el Explorer
4. Ejecute sus consultas GraphQL existentes — el esquema es compatible con patrones comunes
5. Envíe las consultas para aprobación mediante la Admin API o la UI antes de habilitar el gobierno de producción
