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
| Relación de objeto | `relationships[]` con `cardinality: many-to-one` |
| Relación de array | `relationships[]` con `cardinality: one-to-many` |
| Permiso de select | Visibilidad de rol + filtro RLS |
| Permiso de columna | `visible_to` / `writable_by` |
| Permiso de insert/update/delete | Mutación `writable_by` + RLS |
| Esquema remoto | Registro de origen `graphql_remote` |
| Campo calculado | Entrada de `functions[]` con `kind: query` |

### Limitaciones

- **Actions**: se convierten automáticamente: las actions con handler HTTP se convierten en mutaciones `webhooks[]`; las actions con handler no HTTP (base de datos) se convierten en un placeholder de `functions[]` y emiten una advertencia para revisar el handler
- **Event triggers**: se convierten en configuración `event_triggers` por tabla (operaciones, URL de webhook, política de reintentos) y emiten una advertencia señalando fidelidad limitada
- **Esquemas remotos**: se convierten en entradas de origen `graphql_remote`
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
