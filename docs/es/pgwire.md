# Servidor pgwire de Provisa

Provisa expone un endpoint del protocolo de cable de PostgreSQL (pgwire). Cualquier herramienta que hable el protocolo de cliente de PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — puede conectarse y consultar datos de Provisa a través del mismo pipeline de gobierno que gobierna la API HTTP. (REQ-266)

Las consultas pasan por el stack de gobierno completo: aplicación de RLS, reglas de enmascaramiento, guardas de relación, verificaciones de acceso a dominio. (REQ-001, REQ-002, REQ-263) La interfaz pgwire no es un mecanismo de evasión. (REQ-002, REQ-266)

---

## Detalles de conexión

El servidor se inicia cuando `PROVISA_PGWIRE_PORT` está configurado con un entero distinto de cero. Está deshabilitado de forma predeterminada. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Configure `PROVISA_PGWIRE_CERT` y `PROVISA_PGWIRE_KEY` con las rutas de un certificado y una clave PEM. Cuando ambos están presentes, el servidor envuelve las conexiones entrantes en TLS. Cuando están ausentes, TLS está desactivado y el servidor responde `N` a las solicitudes de negociación SSL. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Versión de servidor reportada.** Los clientes ven `14.0.provisa`. Las herramientas que activan funciones según el número de versión pueden comportarse como si estuvieran conectadas a PostgreSQL 14. (REQ-579) [tool-verified: `server.py:208`]

---

## Autenticación

Dos modos, controlados por la clave `provider` en `auth_config`:

| Modo | Valor de `provider` | Comportamiento |
| ------ | ----------------- | ----------- |
| Trust | `none` (o middleware de autenticación inactivo) | El nombre de usuario enviado por el cliente se usa directamente como `role_id`. La contraseña se ignora. |
| Simple | `simple` | La contraseña se verifica contra el proveedor de autenticación `simple` (bcrypt). El nombre de usuario se convierte en `role_id` si tiene éxito. (REQ-124) |

Cualquier otro valor de `provider` devuelve un error FATAL al iniciar sesión. (REQ-529) El protocolo siempre usa el tipo de autenticación 3 de PG (contraseña en texto plano). (REQ-529) No use el modo trust sobre una conexión sin cifrar. [tool-verified: `server.py:282-311`]

---

## Qué funciona

### SELECT

Todas las sentencias SELECT pasan por el pipeline de gobierno (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) El pipeline:

1. Reescribe SQL semántico a SQL físico (`rewrite_semantic_to_physical`)
2. Aplica el gobierno (RLS, enmascaramiento, acceso a dominio) (REQ-263)
3. Valida contra el esquema registrado (REQ-011)
4. Enruta a Trino o al pool directo del origen (REQ-027, REQ-028)

Se admiten consultas simples de múltiples sentencias. Las sentencias separadas por punto y coma se dividen y se ejecutan en orden. (REQ-580) [tool-verified: `server.py:318-381`]

Las consultas parametrizadas (`$1`, `$2`, ...) se admiten tanto en el modo de consulta simple como en el modo de consulta extendida (Bind/Execute). Los parámetros se sustituyen como literales antes de la ejecución. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` y `SELECT fn(args)` — donde `fn` nombra una función registrada y rastreada — se interceptan antes del pipeline de gobierno y se enrutan a través del único ejecutor gobernado (`invoke_tracked_function`). El resultado es un conjunto de filas tipado idéntico al que devuelve cualquier otra superficie para ese comando. `writable_by` y las reglas de gobierno se aplican dentro del ejecutor. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

Las sentencias DDL se detectan mediante la expresión regular en `server.py` y se despachan a `DdlHandler`. El rol debe tener la capacidad `"ddl"`. (REQ-042) Sin ella, la sentencia se rechaza con SQLSTATE 42501. [tool-verified: `ddl_handler.py:82-83`]

Las formas de DDL reconocidas son:

```sql
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

Existen dos rutas de ejecución según `ddl_catalog`: (REQ-582)

**Ruta Trino** — se usa cuando `ddl_catalog` es un catálogo Trino de Iceberg, Hive u otro no registrado (por ejemplo, `iceberg`, `hive`, `otel`, `results`). En esta ruta solo se admiten `CREATE TABLE` y `CREATE VIEW`. Intentar `ALTER`, `DROP` o `CREATE INDEX` genera un error. El nombre de la tabla se califica completamente como `catalog.schema.table`. [tool-verified: `ddl_handler.py:92-100`]

**Ruta directa** — se usa cuando `ddl_catalog` coincide con un ID de origen registrado. Se admite DDL completo: CREATE, ALTER, DROP, índices, secuencias. `CREATE TABLE` y `CREATE VIEW` se califican por esquema como `schema.table`. El resto del DDL (ALTER, DROP, CREATE INDEX) se transmite tal cual después de establecer el contexto de esquema. Para orígenes PostgreSQL y SQLite, el contexto se establece con `SET search_path TO schema`. Para MySQL y MariaDB, el contexto se establece con `USE schema`. [tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

Después del DDL en cualquiera de las dos rutas, la nueva tabla se registra en el contexto de compilación del rol para que quede consultable de inmediato. (REQ-583) [tool-verified: `ddl_handler.py:216-250`]

**Resolución del destino de escritura.** El catálogo y el esquema de DDL provienen de los campos `ddl_catalog` y `ddl_schema` del dominio. Si `ddl_catalog` no está configurado, el sistema usa por defecto el catálogo Iceberg. Si `ddl_schema` no está configurado, usa por defecto el ID del dominio. El dominio se resuelve a través de la lista `domain_access` del rol. (REQ-584) [tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

Se admiten tanto `COPY ... TO STDOUT` como `COPY ... FROM STDIN`. (REQ-585) [tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT** — exporta los resultados de la consulta en el formato de cable COPY de PG. Funcionan dos formas:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Formatos admitidos: `text` (delimitado por tabulaciones, predeterminado) y `csv`. El formato binario no se admite en la salida de COPY. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** — inserta filas en una tabla de destino. Restringido a orígenes de tipo `postgresql`, `mysql`, `sqlite` o `mariadb`. (REQ-586) Intentar COPY FROM contra un origen exclusivo de Trino (por ejemplo, Iceberg) genera un error de permisos. [tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

Si no se proporciona una lista de columnas, estas se infieren a partir del esquema registrado. [tool-verified: `copy_handler.py:357`]

### Transacciones y comandos de sesión

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET y DEALLOCATE se interceptan y devuelven una respuesta de éxito vacía. (REQ-587) El servidor no mantiene estado respecto a las transacciones — no hay aislamiento de transacciones ni soporte de reversión. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Intercepción de catálogo

Las consultas contra `information_schema` y `pg_catalog` se responden localmente sin un viaje de ida y vuelta a Trino. (REQ-532) La capa de intercepción construye una base de datos DuckDB en memoria por solicitud, poblada a partir del contexto de compilación del rol. (REQ-532) [tool-verified: `catalog.py:210-213`]

Tablas interceptadas:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` se puebla con datos reales de PK y FK derivados de los campos `pk_columns` y `joins` del modelo de dominio. (REQ-392, REQ-399) Las herramientas de BI que inspeccionan relaciones de clave foránea (Tableau, DBeaver, etc.) verán el grafo de joins que Provisa conoce. [tool-verified: `catalog.py:551-632`] Los joins de una sola columna entre el mismo par origen/destino cuyas columnas de destino forman en conjunto la clave primaria compuesta del destino se colapsan en una sola fila de FK con arreglos `conkey`/`confkey` de varios elementos. (REQ-1094) [tool-verified: `catalog_constraints.py`]

`pg_index` se puebla con una fila por cada restricción de clave primaria y UNIQUE (`indrelid` = oid de la tabla, `indkey` = attnums de clave ordenados, `indisprimary`/`indisunique` establecidos). Los clientes que resuelven columnas clave mediante `pg_index.indkey` en lugar de `pg_constraint` — DataGrip, por ejemplo — descubren las columnas correctas a través del join estándar `pg_index` → `pg_attribute`. (REQ-1095) [tool-verified: `catalog_constraints.py:340-384`]

También se interceptan las siguientes expresiones escalares: (REQ-588)

- `current_user`, `session_user` → el `role_id` autenticado
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → devuelve el valor de una tabla de configuración fija
- `SHOW <setting>` → devuelve el valor de la misma tabla de configuración

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## Codificación binaria de parámetros

El protocolo de consulta extendida (Bind/Execute) admite parámetros codificados en binario. (REQ-589) Los siguientes OID de tipo se decodifican desde binario: [tool-verified: `postgres.py:69-97`]

| OID | Tipo PG | Tipo Python |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

Cualquier OID que no esté en esta tabla genera `"Unsupported binary parameter type: <oid>"`. (REQ-589) [tool-verified: `postgres.py:579`]

Las columnas de resultado también se envían en binario cuando el cliente lo solicita, para el mismo conjunto de tipos más ARRAY, JSON, INTERVAL y BIGINT. (REQ-589) [tool-verified: `postgres.py:191-244`]

---

## Recomendaciones de driver

**Drivers nativos de Python (psycopg2, asyncpg).** Estos negocian el protocolo de consulta extendida de forma predeterminada y usan codificación binaria para la mayoría de los tipos. La fidelidad de tipos es máxima aquí — las columnas `NUMERIC` llegan como `Decimal`, `TIMESTAMP` como `datetime`, y así sucesivamente. Úselos para ETL basado en Python, scripts o integración directa.

**JDBC (driver JDBC de PostgreSQL).** Úselo para herramientas del ecosistema Java: DBeaver, Tableau, Power BI, Metabase, operadores JDBC de Airflow. JDBC usa por defecto el protocolo de consulta simple, lo que evita complicaciones de codificación binaria. Cadena de conexión:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Algunas herramientas de BI basadas en JDBC envían una ráfaga de consultas a `information_schema` y `pg_catalog` al conectarse para poblar su explorador de esquemas. Todas se responden mediante la capa de intercepción de catálogo — no se genera tráfico a Trino durante la inspección de esquemas. (REQ-532)

**Cuándo preferir uno sobre otro.** Si el cliente es Python, use psycopg2 o asyncpg para un mejor manejo de tipos. Si el cliente es una herramienta de BI o cualquier aplicación JVM, use JDBC. Evite mezclar expectativas de protocolo binario y de texto en la misma conexión si observa sorpresas de conversión de tipos — el comportamiento en modo texto de JDBC es más simple de razonar.

---

## Advertencias y restricciones

**Solo SQL; sin mutaciones DML.** El listener pgwire analiza y ejecuta únicamente SQL — no se aceptan cadenas de GraphQL ni de Cypher. (REQ-614) `INSERT`, `UPDATE` y `DELETE` planos no se enrutan a una ruta de escritura. (REQ-615) Escriba datos mediante `COPY FROM STDIN` (orígenes con capacidad de escritura) o `CREATE TABLE AS`; las mutaciones a nivel de fila deben pasar en cambio por las rutas de escritura de GraphQL, Cypher o Trino.

**COPY y DDL requieren la capacidad `ddl`.** Tanto `COPY` (en cualquier dirección) como DDL están controlados por la capacidad `ddl` del rol; los roles que no la tienen reciben SQLSTATE 42501. (REQ-616)

**Sin soporte real de transacciones.** BEGIN/COMMIT/ROLLBACK se aceptan y se ignoran silenciosamente. Cada sentencia se ejecuta de forma independiente. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` siempre devuelve `False`]

**Tiempo de espera de 60 segundos para DDL, 120 segundos para consultas.** Estos valores están codificados de forma fija en los hilos del manejador. (REQ-590) El DDL de larga duración contra orígenes remotos (cambios de esquema en tablas grandes) puede agotar el tiempo de espera. [tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM solo funciona con orígenes con capacidad de escritura.** Iceberg, Hive, orígenes exclusivos de Trino y tipos de origen de solo lectura no aceptan COPY FROM. El error es SQLSTATE 42501. (REQ-586) [tool-verified: `copy_handler.py:65`]

**El formato de salida de COPY es text o csv.** El formato binario COPY de PG (`FORMAT binary`) no está implementado. [inferred: solo existen las ramas `text` y `csv` en `_rows_to_copy_text` / `_rows_to_copy_csv`]

**El DDL en la ruta Trino es solo CREATE.** ALTER, DROP y CREATE INDEX contra catálogos Iceberg o Hive no se admiten. Use un origen SQL registrado como `ddl_catalog` si necesita DDL completo. (REQ-582) [tool-verified: `ddl_handler.py:92-100`]

**La sustitución de parámetros es literal.** Los parámetros `$1`, `$2`, ... se sustituyen como literales SQL antes de la ejecución, no se envían como parámetros de enlace al motor subyacente. Esto significa que el motor subyacente nunca ve una sentencia preparada. Para Trino esto no tiene impacto práctico; para orígenes de pool directo, evita el almacenamiento en caché de sentencias preparadas. (REQ-581) [tool-verified: `server.py:78-85`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Estas tablas existen en la capa de catálogo pero son stubs vacíos. Las herramientas de monitoreo que las consultan recibirán cero filas en lugar de errores. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` sí está poblada — vea Intercepción de catálogo.)
