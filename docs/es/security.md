# Modelo de seguridad

Provisa aplica un modelo de seguridad de múltiples capas en todos los lenguajes de consulta (GraphQL, SQL, Cypher) y todos los transportes (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) El gobierno se aplica de manera uniforme — no existe ninguna ruta de consulta que lo evite. (REQ-002, REQ-266)

Las capas se aplican en orden. Una solicitud debe superar cada capa antes de que se evalúe la siguiente.

## Modelo por capas

### Capa 0 — Filtrado de introspección

El esquema y el catálogo presentados a un rol contienen únicamente las tablas de su lista `domain_access` y las columnas que superan las reglas `visible_to` por columna. (REQ-039) Los objetos fuera del acceso de un rol son invisibles en el momento del descubrimiento — no se pueden consultar, autocompletar ni inferir que existen. (REQ-039) Esto aplica al esquema de GraphQL, al catálogo SQL y al navegador de esquemas del editor de consultas. (REQ-039, REQ-363)

Consulte [Visibilidad de esquemas](#visibilidad-de-esquemas).

### Capa 1 — Acceso público

Las tablas de dominios sin restricción `domain_access` son visibles para todas las identidades autenticadas sin configuración adicional. Fricción cero para datos genuinamente públicos.

### Capa 2 — Acceso por dominio

Cada rol tiene una lista `domain_access` de IDs de dominio. Una consulta que toca una tabla fuera de esos dominios se rechaza antes de la ejecución. (REQ-038, REQ-039) Este es el límite de propiedad de grano grueso — un rol de RR. HH. no puede acceder a tablas de finanzas sin importar cómo esté escrito el SQL. (REQ-002)

Consulte [Modelo de derechos](#modelo-de-derechos).

### Capa 3 — Seguridad de nivel de fila

Después de confirmar el acceso al dominio, se inyectan predicados `WHERE` por tabla y por rol en cada `SELECT` en el momento de la ejecución. (REQ-041, REQ-263) Los predicados se evalúan contra los datos sin procesar. Un gerente regional que consulta una tabla de pedidos compartida solo ve las filas de su región, incluso en un `SELECT *`. (REQ-264)

Consulte [Seguridad de nivel de fila (RLS)](#seguridad-de-nivel-de-fila-rls).

### Capa 4 — Visibilidad y enmascaramiento de columnas

Las columnas con una lista `visible_to` que excluye al rol solicitante se eliminan de la salida de la consulta. (REQ-040, REQ-263) Las columnas con una regla de enmascaramiento tienen sus valores reemplazados — mediante redacción por regex, reemplazo por constante o truncamiento — antes de que los resultados salgan del servidor. (REQ-263) El enmascaramiento se aplica en todos los lenguajes de consulta y formatos de salida. (REQ-263)

Consulte [Modelo de permisos de columna](#modelo-de-permisos-de-columna) y [Enmascaramiento a nivel de columna](#enmascaramiento-a-nivel-de-columna).

### Capa 5 — Protección de predicados

Las columnas enmascaradas se rechazan en las cláusulas `WHERE` y `HAVING`. (REQ-263) Sin esto, quien realiza la llamada podría inferir el valor sin enmascarar mediante búsqueda binaria en un filtro, aunque la salida esté enmascarada. El rechazo se aplica en el momento del análisis de la consulta, antes de la ejecución. (REQ-531)

### Gobierno de relaciones (V002)

Las condiciones JOIN en SQL deben coincidir con una relación registrada y aprobada entre tablas. (REQ-001) Los joins no aprobados se rechazan. Cada relación lleva un motivo y una descripción legibles por humanos — orientación tanto para usuarios como para agentes autónomos sobre por qué existe una ruta de recorrido. Esto es política de gobierno, no un límite de seguridad estricto: las capas 2–5 se mantienen sin importar la estructura del join, de modo que una elusión deliberada no expone datos que el rol no pudiera alcanzar mediante dos consultas separadas. Los intentos de elusión se registran y son auditables.

**Mecanismos de omisión** — V002 solo se puede omitir cuando se cumplen dos condiciones independientes simultáneamente:

1. **Indicador de rol** — `relationship_guard: false` en la definición del rol (valor predeterminado: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Exclusión por consulta** — el SQL contiene el comentario `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

Ambas deben estar presentes. El indicador de rol por sí solo no omite V002; el comentario por sí solo no omite V002.

**Ruta GraphQL** — V002 se omite incondicionalmente en las consultas GraphQL. Las relaciones definidas en SDL están preaprobadas por diseño; la verificación es redundante y no se aplica. [tool-verified: `provisa/api/data/endpoint.py:468`]

**Rutas SQL y Cypher** — V002 está activo de forma predeterminada. Tanto `endpoint_dev.py` como `cypher_router.py` aplican la verificación de dos condiciones antes de llamar a `validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**Ruta pgwire** — misma verificación de dos condiciones que en SQL. El comentario `--relationship-guard=false` se elimina de la consulta antes de la ejecución; no llega a la base de datos. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

Estas capas se componen entre sí. Un rol con acceso por dominio, RLS y columnas enmascaradas tiene las cinco restricciones activas simultáneamente. Agregar un nuevo origen de datos, columna o relación no requiere actualizar cada regla — cada capa se configura de manera independiente y se aplica automáticamente a cualquier consulta que toque objetos gobernados.

---

## Modelo de derechos

Capacidades asignadas de forma independiente, con jerarquía de roles opcional mediante `parent_role_id`. `admin` otorga todas. (REQ-042)

| Capacidad | Descripción |
|-----------|-------------|
| `source_registration` | Registrar orígenes de datos |
| `table_registration` | Registrar tablas, columnas |
| `create_relationship` | Definir relaciones de clave foránea |
| `access_config` | Configurar RLS, enmascaramiento |
| `query_development` | Ejecutar consultas |
| `write` | Invocar mutaciones registradas (control de grano grueso; consulte Autorización de mutaciones) |
| `full_results` | Omitir los límites de muestreo |
| `ignore_relationships` | Omitir el gobierno de relaciones (V002) |
| `admin` | Superusuario — otorga todas |

### Herencia de roles

Los roles pueden heredar capacidades y acceso por dominio de un rol padre mediante `parent_role_id`. (REQ-215) La jerarquía se aplana al iniciar — los roles hijos combinan las capacidades y el acceso por dominio del padre con los propios. (REQ-215)

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## Modelo de permisos de columna

Cada columna tiene un modelo de permisos de cuatro campos que controla el acceso de lectura, escritura y enmascaramiento por rol. (REQ-042, REQ-249)

### Visibilidad de tres niveles

| Nivel | Condición | Resultado |
|------|-----------|--------|
| **Oculta** | El rol no está en `visible_to` | Columna ausente del SDL de GraphQL |
| **Enmascarada** | El rol está en `visible_to`, tiene regla de enmascaramiento, el rol no está en `unmasked_to` | Columna visible pero datos enmascarados en SQL |
| **Sin enmascarar** | El rol está en `visible_to` Y el rol está en `unmasked_to` (o no hay regla de enmascaramiento) | Acceso de lectura completo |

### Permisos de escritura

| Campo | Vacío significa | Propósito |
|-------|------------|---------|
| `visible_to` | Todos los roles pueden leer | Controla quién ve la columna (enmascarada o sin enmascarar) |
| `unmasked_to` | Ningún rol la ve sin enmascarar | Controla quién omite el enmascaramiento |
| `writable_by` | Ningún rol puede escribir | Controla quién puede mutar (INSERT/UPDATE) |

El permiso de escritura se aplica en el pipeline de mutaciones. Un rol que no está en `writable_by` recibe un error 403 al intentar escribir en una columna restringida. (REQ-033, REQ-034)

### Ejemplo

```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

En este ejemplo:
- `email`: admin ve `alice@example.com` y puede editar; analyst/viewer ven `a***@example.com`
- `salary`: admin y hr ven el valor real; hr puede editar; el resto de los roles no ven la columna en absoluto
- `created_at`: todos pueden leer, nadie puede escribir

## Autorización de mutaciones

Las mutaciones registradas (GraphQL remoto, OpenAPI, gRPC, Hasura) están controladas por dos verificaciones independientes. (REQ-867, REQ-868) Un rol solo puede invocar una mutación si posee la capacidad global `write` Y aparece en la lista `writable_by` de esa mutación. (REQ-868) Un `writable_by` vacío es denegación predeterminada — ningún rol puede invocarla. (REQ-867)

Las mutaciones se clasifican como escrituras por contrato, no por declaración de quien realiza la llamada. (REQ-869) Un `SELECT` que hace referencia a una función de tipo mutación se promueve a escritura y queda sujeto a la misma verificación de dos controles, de modo que quien realiza la llamada no puede invocar una mutación disfrazándola de lectura. (REQ-869) Reclasificar una mutación como segura para lectura requiere la capacidad `access_config` y se registra como una decisión de gobierno; no existe una exclusión por solicitud. (REQ-870)

## Visibilidad de esquemas

Los esquemas de GraphQL por rol ocultan el contenido no autorizado: (REQ-039)

- **Acceso por dominio**: el rol ve tablas solo en sus dominios `domain_access` (`"*"` = todos) (REQ-039)
- **Visibilidad de columnas**: las columnas que no están en `visible_to` para un rol se omiten del SDL (REQ-039)
- Las tablas/columnas no autorizadas no aparecen en el esquema (REQ-039)

## Seguridad de nivel de fila (RLS)

Inyección de cláusulas SQL WHERE por tabla y por rol. Se aplica después de la compilación, antes de la ejecución. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

El filtro se combina con AND en la cláusula WHERE de la consulta. Funciona tanto para consultas como para mutaciones (UPDATE/DELETE). (REQ-035, REQ-041)

## Enmascaramiento a nivel de columna

El enmascaramiento se define una vez por columna — es una propiedad de la columna, no del rol. El campo `unmasked_to` controla qué roles lo omiten. (REQ-249)

| Tipo de enmascaramiento | Tipos admitidos | Expresión SQL |
|-----------|----------------|----------------|
| `regex` | Cadena (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Cualquiera | Valor literal (NULL, 0, personalizado) |
| `truncate` | Fecha/Timestamp | `DATE_TRUNC(precision, col)` |

El enmascaramiento se traslada a la proyección SQL SELECT — la base de datos devuelve los datos enmascarados. (REQ-263) Los datos sin enmascarar nunca atraviesan la red para los roles enmascarados. (REQ-263) Las columnas enmascaradas también se bloquean en las cláusulas `WHERE` y `HAVING` (protección de predicados de la capa 5) para evitar la inferencia del valor sin enmascarar mediante filtrado. (REQ-263, REQ-531)

## Muestreo

Todos los roles ven resultados muestreados (predeterminado: 100 filas) a menos que tengan la capacidad `full_results`. (REQ-554) Se controla mediante la variable de entorno `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Registro de auditoría

Cada consulta que toca un activo de dominio se registra en el `query_audit_log`, de solo adición. (REQ-596, REQ-613) Cada fila captura `tenant_id`, `user_id`, `role_id`, un hash SHA-256 del texto de la consulta, `table_ids`, `source`, `status_code`, `duration_ms` y `logged_at`. (REQ-596) El texto de la consulta nunca se almacena tal cual — solo su hash. (REQ-596)

El registro es de solo adición a nivel de base de datos: las reglas de PostgreSQL bloquean `DELETE` y `UPDATE`. (REQ-596, REQ-613) Dos índices — `(tenant_id, logged_at)` y `(user_id, logged_at)` — respaldan las consultas de cumplimiento con alcance por inquilino y por rango de tiempo por usuario. (REQ-596, REQ-613)

Cuando el cifrado está habilitado, la columna del hash del texto de la consulta se almacena cifrada y solo se descifra en lecturas de administrador autorizadas. (REQ-689)

## Limitación de tasa

Los límites de tasa por rol se configuran en `provisa.yaml`: número máximo de solicitudes por segundo, número máximo de suscripciones SSE concurrentes y número máximo de flujos Arrow Flight concurrentes. (REQ-369) Los límites se aplican en la capa de API antes de la compilación o la ejecución; las solicitudes que superan el límite se rechazan con HTTP 429 y un encabezado `Retry-After`. (REQ-369)

El servicio de consulta en lenguaje natural (`POST /query/nl`) tiene un límite independiente mediante `nl.rate_limit` (solicitudes por minuto por rol). Las solicitudes que superan el límite se rechazan antes de realizar cualquier llamada al LLM. (REQ-370)

El estado del límite de tasa reside en Redis (`cache.redis_url`) como un contador de ventana deslizante — sin estado por instancia — de modo que los límites se mantienen en todas las instancias horizontales de Provisa. (REQ-371)

## Autenticación

Proveedores de autenticación conectables: (REQ-120)

| Proveedor | Tipo de token | Caso de uso |
|----------|-----------|----------|
| `none` | Encabezado X-Provisa-Role | Desarrollo |
| `firebase` | Token de ID de Firebase | Producción |
| `keycloak` | JWT de Keycloak | Empresarial |
| `oauth` | JWT de OIDC | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Pruebas |

Asignación de roles: reclamaciones de identidad → rol de Provisa mediante reglas configurables. (REQ-120) El campo `assignments_source` controla de dónde provienen las asignaciones de roles: `claims` las lee de las reclamaciones (claims) del token JWT (predeterminado), `provisa` las lee del almacén interno de asignaciones de Provisa. (REQ-551)

Un superusuario configurado en `provisa.yaml` (nombre de usuario más una contraseña proveniente de un secreto de entorno) siempre recibe el rol admin y todas las capacidades, independientemente del proveedor configurado — una ruta de arranque para la configuración inicial. (REQ-125)

## Hook de aprobación ABAC

Un hook de política externo opcional que se activa antes de la ejecución de la consulta. (REQ-203) Cuando está configurado, Provisa realiza una llamada a su motor de políticas con la identidad del usuario, los roles, las tablas, las columnas y la operación. La respuesta determina si la consulta continúa. (REQ-203)

### Alcance

El hook solo se activa cuando la consulta toca una tabla u origen con alcance definido — sobrecarga cero para todo lo demás. (REQ-204)

| Configuración | Efecto |
|--------|--------|
| `auth.approval_hook.scope: all` | Cada consulta activa el hook |
| `sources[].approval_hook: true` | Todas las tablas de ese origen activan el hook |
| `tables[].approval_hook: true` | Esa tabla activa el hook |

### Protocolos

Se admiten tres transportes: (REQ-246)

| Tipo | Caso de uso | Campo de configuración |
|------|----------|-------------|
| `webhook` | Cualquier servicio de políticas compatible con HTTP (OPA, personalizado) | `url` |
| `unix_socket` | OPA o sidecar de políticas en la misma máquina | `socket_path` + `url` |
| `grpc` | Servicio de políticas colocalizado de alto rendimiento | `url` (host:puerto) |

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
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

El canal gRPC es persistente — un canal por instancia de Provisa, reutilizado en todas las llamadas a ese endpoint de hook. (REQ-555)

### Solicitud / Respuesta

Los tres transportes llevan la misma carga útil: (REQ-246)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `user` | string | Identidad del usuario autenticado |
| `roles` | string[] | Roles de Provisa del usuario |
| `tables` | string[] | IDs de tabla referenciados en la consulta |
| `columns` | string[] | Columnas seleccionadas en la consulta |
| `operation` | string | `"query"` o `"mutation"` |

Los transportes webhook y Unix socket intercambian JSON. La respuesta debe incluir `approved` (bool) y, opcionalmente, `reason` (string). (REQ-246)

### Tiempo de espera y comportamiento por defecto

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

Ante un tiempo de espera agotado o un error de transporte, se aplica la política `fallback`. (REQ-247) Un disyuntor (circuit breaker) (predeterminado: se abre después de 5 fallos consecutivos, semiabierto después de 30s) evita fallos en cascada provocados por un endpoint de hook lento. (REQ-556)

### Ejemplo de configuración

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

## Secretos

Las credenciales usan la sintaxis `${env:VAR_NAME}`, resuelta en tiempo de ejecución. (REQ-557) Las contraseñas nunca se almacenan en la base de datos de configuración. (REQ-557)
