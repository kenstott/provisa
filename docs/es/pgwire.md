# Servidor pgwire de Provisa {: #provisa-pgwire-server }

Provisa expone un endpoint del protocolo de red de PostgreSQL (pgwire). Cualquier herramienta que hable el protocolo de cliente de PostgreSQL, como psycopg2, asyncpg, DBeaver, Tableau o JDBC, puede conectarse y consultar datos de Provisa a través del mismo pipeline de gobernanza que rige la API HTTP.

Las consultas pasan por el stack de gobierno completo: aplicación de RLS, reglas de enmascaramiento, guardas de relación, verificaciones de acceso a dominio. La interfaz pgwire no es un mecanismo de evasión.

---

## Detalles de conexión {: #connection-details }

El servidor se inicia cuando `PROVISA_PGWIRE_PORT` está configurado con un entero distinto de cero. Está deshabilitado de forma predeterminada.

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Configure `PROVISA_PGWIRE_CERT` y `PROVISA_PGWIRE_KEY` con las rutas de un certificado y una clave PEM. Cuando ambos están presentes, el servidor envuelve las conexiones entrantes en TLS. Cuando están ausentes, TLS está desactivado y el servidor responde `N` a las solicitudes de negociación SSL.

**Versión de servidor reportada.** Los clientes ven `14.0.provisa`. Las herramientas que activan funciones según el número de versión pueden comportarse como si estuvieran conectadas a PostgreSQL 14.

---

## Autenticación {: #authentication }

El paquete de inicio lleva un nombre de usuario y un único campo secreto, y ningún esquema que indique qué es ese secreto. Provisa lo decide a partir del propio secreto, de modo que un cliente no necesita más configuración que `user` y `password`:

| El secreto es | Se reconoce por | Se resuelve a |
| --------------- | --------------- | ------------- |
| Un token de acceso personal | su prefijo `provisa_pat_` | el propietario y el rol del token |
| Un token bearer de OIDC / del proveedor | que el proveedor configurado sea un proveedor de tokens | la identidad que afirma el token |
| Una contraseña | cualquier otra cosa | la cuenta en el proveedor configurado (`basic` o `simple`) |

La decisión se toma una sola vez. Una credencial que el validador elegido rechaza no se reintenta contra otro, así que un rechazo no se convierte en un segundo intento.

El modo trust (`provider: none`, o middleware de autenticación inactivo) es la excepción: el nombre de usuario se usa directamente como `role_id` y el secreto se ignora. No lo use sobre una conexión sin cifrar.

**SCRAM-SHA-256.** Con `provider: basic` y `auth.scram: true`, el servidor anuncia SASL (código de autenticación 10) con `SCRAM-SHA-256` y la contraseña se demuestra en lugar de enviarse. `SCRAM-SHA-256-PLUS` no se ofrece. A un usuario cuyo verificador aún no se ha escrito — los verificadores no pueden derivarse de hashes bcrypt — se le responde con un intercambio simulado, de modo que el cable no revela quién ha migrado; ese usuario se autentica con contraseña en texto plano sobre TLS hasta que su siguiente introducción de contraseña escriba uno. Con `auth.scram` desactivado, el servidor usa el tipo de autenticación 3 de PG (contraseña en texto plano). MD5 no se admite en ninguno de los dos casos.

**Certificados de cliente.** Defina `PROVISA_MTLS_CLIENT_CA` y el servidor verificará un certificado de cliente durante el handshake, antes de examinar cualquier credencial. Con `PROVISA_MTLS_BIND_PRINCIPAL`, el common name del certificado debe coincidir con el `user` con el que la conexión se autentica después. Consulte [Configuración](configuration.md#mutual-tls).

**Los intentos fallidos se cuentan.** Cinco fallos en cinco minutos bloquean la cuenta durante quince minutos. El contador se comparte con HTTP y Bolt, así que un bloqueo ganado en cualquier superficie vale en todas.

**Elegir una organización.** En un despliegue multiorganización, conéctese a `<org>.<your-domain>` y pgwire leerá la organización del nombre de host en el ClientHello de TLS, igual que HTTP la lee de la cabecera `Host`. El nombre de host solicita una organización; no la concede, y un principal sin pertenencia a ella queda rechazado. Conectarse por dirección IP no solicita ninguna organización.

---

## Qué funciona {: #what-works }

### SELECT

Todas las sentencias SELECT pasan por el pipeline de gobierno (`_pipeline.py`). El pipeline:

1. Reescribe SQL semántico a SQL físico (`rewrite_semantic_to_physical`)
2. Aplica el gobierno (RLS, enmascaramiento, acceso a dominio)
3. Valida contra el esquema registrado
4. Enruta a Trino o al pool directo del origen

Se admiten consultas simples de múltiples sentencias. Las sentencias separadas por punto y coma se dividen y se ejecutan en orden.

Las consultas parametrizadas (`$1`, `$2`, ...) se admiten tanto en el modo de consulta simple como en el modo de consulta extendida (Bind/Execute). Los valores siguen vinculados hasta el motor o la fuente (vea [Advertencias y restricciones](#caveats-and-constraints)).

`SELECT * FROM fn(args)` y `SELECT fn(args)` — donde `fn` nombra una función registrada y rastreada — se interceptan antes del pipeline de gobierno y se enrutan a través del único ejecutor gobernado (`invoke_tracked_function`). El resultado es un conjunto de filas tipado idéntico al que devuelve cualquier otra superficie para ese comando. La asignación del comando (`visible_to`), el derecho `write` para una mutación y las reglas de gobierno se aplican dentro del ejecutor.

### Cursores (DECLARE / FETCH / MOVE / CLOSE) {: #cursors-declare-fetch-move-close }

Provisa admite la sintaxis textual de cursores SQL sobre el protocolo de consulta simple. Los clientes que envían `DECLARE`/`FETCH`/`MOVE`/`CLOSE` como instrucciones SQL normales obtienen streaming real del lado del servidor, sin error de protocolo y sin cargar el resultado entero en la memoria del cliente.

**Esto es distinto del streaming basado en portales.** El protocolo de consulta extendida — Bind/Execute con un campo de límite de filas — es un mecanismo distinto del protocolo de cable. asyncpg y la mayoría de los controladores modernos de PostgreSQL lo usan automáticamente sin SQL de cursores. Si su controlador ya usa portales, no necesita esta sintaxis. `DECLARE CURSOR` es para clientes que envían SQL de cursores literalmente: la función de cursores con nombre/del lado del servidor de psycopg2 (`conn.cursor(name="...")`), ciertas herramientas de BI y algunos controladores JDBC antiguos.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```

El SELECT interior pasa por el pipeline gobernado normal de Provisa, con la misma aplicación de RLS, el mismo enmascaramiento y las mismas comprobaciones de acceso por dominio que cualquier SELECT independiente. Declarar un cursor no esquiva la gobernanza.

La palabra clave `SCROLL` controla tanto la navegación permitida como la memoria que usa el cursor:

| Declaración | Navegación hacia atrás | Coste de memoria |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (predeterminado) | No permitida | Limitado a un lote de FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | No permitida | Limitado a un lote de FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | Permitida | Resultado completo en búfer en la memoria de Python |

Cuando no se escribe ni `SCROLL` ni `NO SCROLL`, el valor predeterminado es `NO SCROLL`, igual que el predeterminado de PostgreSQL. Un cursor `NO SCROLL` recorta las filas consumidas de su búfer interno tras cada paso hacia delante, así que un recorrido `FETCH FORWARD` por un resultado grande nunca mantiene más de aproximadamente un lote en la memoria de Python a la vez. Un cursor `SCROLL` conserva cada fila que ha obtenido: es el coste de admitir `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` y `LAST`. Antes de declarar un cursor `SCROLL` sobre un resultado grande, presupueste el tamaño completo del resultado como heap de Python.

Volver a declarar un nombre de cursor que ya está abierto cierra primero el cursor anterior. `WITH HOLD` y `WITHOUT HOLD` se aceptan en la sintaxis; consulte la duración del cursor más abajo.

**FETCH y MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```

`FETCH` devuelve filas y envía la etiqueta de comando `FETCH <n>`, donde `n` es el número real devuelto, que puede ser menor que el pedido cuando el cursor llega al final. `MOVE` avanza la posición sin devolver filas y envía `MOVE <n>`.

`FROM` e `IN` son intercambiables como introducción del nombre del cursor. Los nombres de cursor sin comillas se pasan a minúsculas; los nombres entre comillas (`"CaseSensitive"`) conservan las mayúsculas.

La navegación hacia atrás (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, `RELATIVE` negativo) sobre un cursor `NO SCROLL` devuelve SQLSTATE 55000 (`object_not_in_prerequisite_state`), igual que el rechazo del propio PostgreSQL. Un FETCH o MOVE que nombra un cursor nunca declarado devuelve SQLSTATE 34000 (`invalid_cursor_name`).

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```

`CLOSE` libera el flujo de resultados subyacente y quita el cursor de la sesión. Desconectarse sin emitir `CLOSE` es seguro: `ProvisaSession.close()` libera automáticamente todos los cursores aún abiertos al desconectar.

**Vida de un cursor.** El servidor pgwire de Provisa no tiene máquina de estados de transacción (`ProvisaSession.in_transaction()` devuelve siempre `False`). Por eso todo cursor se comporta como si se hubiera declarado `WITH HOLD`: vive lo que dura la conexión, y `COMMIT` no lo termina. Las palabras clave `WITH HOLD` y `WITHOUT HOLD` se aceptan y se ignoran, ya que no hay ciclo de vida de cursor por transacción que aplicar.

### Definiciones (CREATE, ALTER, DROP) {: #definitions-create-alter-drop }

Nada se define a través de pgwire. Una sentencia que crea, altera o elimina un objeto (`CREATE TABLE`, `CREATE TABLE … AS SELECT`, `CREATE VIEW`, `ALTER`, `DROP`, un índice, una secuencia o un esquema) se rechaza con SQLSTATE 0A000, tenga el rol lo que tenga; la excepción es la tabla temporal propia de una sesión (consulte [Tablas temporales](#temporary-tables)). Una tabla o vista es un objeto del modelo: créela en el modelo (páginas de administración, API de administración o configuración), o créela en el origen de datos y admítala en el modelo. `TRUNCATE` no define nada: es una escritura de datos (consulte [Escrituras de datos](#data-writes-insert-update-delete-merge-copy-from-stdin)).

El rechazo se hace una vez, en el pipeline por el que pasa toda superficie SQL, así que SQL sobre HTTP, Flight y MCP rechazan las mismas sentencias con el mismo mensaje.

### Tablas temporales {: #temporary-tables }

Una sesión puede crear, escribir, leer y eliminar tablas temporales propias:

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```

- **Crear** con `CREATE TEMP TABLE name (column type, ...)` o `CREATE TEMP TABLE name AS SELECT ...`. El nombre no lleva esquema. Un `SELECT` a partir del que se crea es una lectura como cualquier otra: las reglas de filas, las máscaras, los datos falsos y el límite de filas del rol se aplican a las filas que toma.
- **Tipos de columna**: `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean`, `date`, `timestamp`, `text`, `varchar` y `DECIMAL(precision, scale)`. Un decimal conserva su precisión y escala. Cualquier otro tipo se rechaza, nombrando la columna y el tipo: un valor nunca se guarda como un tipo distinto del suyo. Una columna creada a partir de un `SELECT` toma el tipo de sus valores; una sin valor con el que deducir un tipo se rechaza, así que aplique `CAST` en el `SELECT`.
- **Límite de filas.** Las filas que toma un `CREATE ... AS SELECT` o un `INSERT ... SELECT` se leen como el rol lee cualquier cosa, así que se aplica el límite de filas del rol. Una tabla temporal llenada desde una lectura que el límite recortó lleva la misma advertencia de sentencia que cualquier lectura recortada (`statement.rows_cut`), con el límite.
- **Escribir** con `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` y `DELETE`, sin `RETURNING` y sin `FROM`. Una escritura en una tabla temporal no es una mutación: no llega a ningún origen ni a ningún dato del entorno, así que funciona sea cual sea el manejo de mutaciones del entorno y no necesita derecho de escritura.
- **Leer** por su nombre, sola o unida a tablas del modelo. El motor calcula la lectura.
- **Duración.** Una tabla temporal no es un objeto del modelo, ninguna otra sesión la ve y termina con la sesión. En pgwire una sesión es la conexión. En una superficie sin conexión propia (SQL sobre HTTP, Flight, MCP) una sesión es una solicitud, así que una tabla temporal vive para las sentencias enviadas juntas en esa solicitud.
- **Almacenamiento.** Las filas se guardan en un esquema propio de la sesión en el almacén del motor y se eliminan cuando termina la sesión. Un `INSERT`, `UPDATE` o `DELETE` reescribe toda la tabla a través del motor, así que una tabla temporal sirve para datos de trabajo, no para tablas grandes escritas fila a fila. Un proceso que se detiene sin terminar sus sesiones deja sus esquemas en el almacén; se eliminan cuando se retira el entorno.

Las mismas sentencias funcionan en toda superficie SQL: se manejan en el pipeline que todas comparten.

### Escrituras de datos (INSERT, UPDATE, DELETE, MERGE, COPY FROM STDIN) {: #data-writes-insert-update-delete-merge-copy-from-stdin }

Las filas de una tabla registrada existente se escriben a través de pgwire como en cualquier otra superficie SQL, mediante la única admisión de escrituras (`provisa/compiler/write_admission.py`):

1. **El derecho.** El rol tiene `write`.
2. **El permiso.** El rol tiene un permiso de escritura sobre la tabla para la operación: un `INSERT` necesita un permiso `insert`, un `UPDATE` uno `update`, un `DELETE` uno `delete`. Un `MERGE` necesita un permiso `insert` y uno `update` sobre todas las filas, y un `INSERT ... ON CONFLICT DO UPDATE` uno `update` sobre todas las filas.
3. **Las columnas.** Cada columna que escribe la sentencia es una que el permiso nombra. Una columna que el permiso predefine es del permiso: la sentencia no puede suministrarla, y se añade el valor del permiso (una variable de sesión, un literal o el momento de la escritura).
4. **Las filas que puede tocar.** El predicado `rows` del permiso se añade al `WHERE` de un `UPDATE` o `DELETE`. Un permiso que no indica ninguno toma el filtro de filas del rol, el mismo predicado que llevan sus lecturas.
5. **Las filas que deja.** Las filas de un `INSERT`, y los nuevos valores de un `UPDATE` que establece una columna que lee el `check` del permiso, se comprueban contra el `check` antes de enviar nada al origen. Un permiso que no indica `check` toma el filtro de filas del rol.

`TRUNCATE` también es una escritura de datos, se ejecuta como tal y nunca se reescribe como `DELETE`. Necesita el derecho `write`, un permiso `delete` sobre todas las filas de la tabla y un origen que admita borrados. Vacía una tabla cada vez, y se rechaza a un rol cuyo permiso limita las filas (un predicado `rows`, o el filtro de filas del rol cuando el permiso no indica ninguno), ya que un `TRUNCATE` no puede aplicar el predicado: use `DELETE`, que sí lo aplica.

Lo que no se puede decidir se rechaza por su nombre en lugar de admitirse con dudas (un `INSERT … SELECT`, un valor que no es ni un literal ni un parámetro enlazado, un filtro que lee una columna que la sentencia no proporciona).

### COPY

Se admiten tanto `COPY ... TO STDOUT` como `COPY ... FROM STDIN`.

**COPY TO STDOUT** — exporta resultados de consulta en el formato de cable COPY de PG. Funcionan dos formas:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Formatos admitidos: `text` (delimitado por tabulaciones, predeterminado), `csv` y `binary`, el formato COPY binario de PostgreSQL, con el que lee la extensión postgres de DuckDB.

**COPY FROM STDIN** es una escritura de datos en una tabla registrada existente: la admite la misma admisión de escritura que un `INSERT` de las mismas filas, con la condición añadida de que el permiso `insert` no tenga presets (véase [Escrituras de datos](#data-writes-insert-update-delete-merge-copy-from-stdin)). Lee `text` (por defecto) y `csv`. Cualquier otro formato se rechaza con SQLSTATE 0A000, nombrándolo, antes de enviar ningún dato.

### Transacciones y comandos de sesión {: #transactions-and-session-commands }

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET y DEALLOCATE se interceptan y se responden con un éxito vacío. El servidor no guarda estado de transacción, así que no hay aislamiento de transacciones ni rollback.

---

## Intercepción de catálogo {: #catalog-intercept }

Las consultas contra `information_schema` y `pg_catalog` se responden localmente sin un viaje de ida y vuelta a Trino. La capa de intercepción construye una base de datos DuckDB en memoria por solicitud, poblada a partir del contexto de compilación del rol.

Tablas interceptadas:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

`pg_constraint` se puebla con datos reales de PK y FK derivados de los campos `pk_columns` y `joins` del modelo de dominio. Las herramientas de BI que inspeccionan relaciones de clave foránea (Tableau, DBeaver, etc.) verán el grafo de joins que Provisa conoce. Los joins de una sola columna entre el mismo par origen/destino cuyas columnas de destino forman en conjunto la clave primaria compuesta del destino se colapsan en una sola fila de FK con arreglos `conkey`/`confkey` de varios elementos.

Una relación respaldada por tabla de unión no produce fila FK. Es una arista a través de una tabla asociativa, y `pg_constraint` no tiene forma para dos saltos, así que el modelo de dominio la deja fuera de `joins` y la tabla de unión aparece como una tabla normal con sus propias claves foráneas a cada extremo. Los clientes SQL llegan a ella uniendo esa tabla; los clientes Cypher la recorren como una sola relación.

`pg_index` se puebla con una fila por cada restricción de clave primaria y UNIQUE (`indrelid` = oid de la tabla, `indkey` = attnums de clave ordenados, `indisprimary`/`indisunique` establecidos). Los clientes que resuelven columnas clave mediante `pg_index.indkey` en lugar de `pg_constraint` — DataGrip, por ejemplo — descubren las columnas correctas a través del join estándar `pg_index` → `pg_attribute`.

También se interceptan las siguientes expresiones escalares:

- `current_user`, `session_user` → el `role_id` autenticado
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → devuelve el valor de una tabla de configuración fija
- `SHOW <setting>` → devuelve el valor de la misma tabla de configuración

---

## Codificación binaria de parámetros {: #binary-parameter-encoding }

El protocolo de consulta extendida (Bind/Execute) admite parámetros codificados en binario. Los siguientes OID de tipo se decodifican desde binario:

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

Cualquier OID que no esté en esta tabla genera `"Unsupported binary parameter type: <oid>"`.

Las columnas de resultado también se envían en binario cuando el cliente lo solicita, para el mismo conjunto de tipos más ARRAY, JSON, INTERVAL y BIGINT.

---

## Recomendaciones de driver {: #driver-recommendations }

**Drivers nativos de Python (psycopg2, asyncpg).** Estos negocian el protocolo de consulta extendida de forma predeterminada y usan codificación binaria para la mayoría de los tipos. La fidelidad de tipos es máxima aquí: las columnas `NUMERIC` llegan como `Decimal`, `TIMESTAMP` como `datetime`, y así sucesivamente. Úselos para ETL basado en Python, scripts o integración directa.

**JDBC (driver JDBC de PostgreSQL).** Úselo para herramientas Java: DBeaver, Tableau, Power BI, Metabase, operadores JDBC de Airflow. JDBC usa por defecto el protocolo de consulta simple, lo que evita complicaciones de codificación binaria. Cadena de conexión:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Algunas herramientas de BI basadas en JDBC envían una ráfaga de consultas a `information_schema` y `pg_catalog` al conectarse para poblar su explorador de esquemas. Todas se responden mediante la capa de intercepción de catálogo, así que no se genera tráfico a Trino durante la inspección de esquemas.

**Cuándo preferir uno sobre otro.** Si el cliente es Python, use psycopg2 o asyncpg para un mejor manejo de tipos. Si el cliente es una herramienta de BI o cualquier aplicación JVM, use JDBC. Evite mezclar expectativas de protocolo binario y de texto en la misma conexión si observa sorpresas de conversión de tipos. El comportamiento en modo texto de JDBC es más simple de razonar.

---

## Advertencias y restricciones {: #caveats-and-constraints }

**Solo SQL.** El listener de pgwire analiza y ejecuta SQL; no acepta cadenas GraphQL ni Cypher. Las escrituras de datos en tablas existentes las admiten los derechos del rol, sus concesiones de columnas y su filtro de filas; consulte [Escrituras de datos](#data-writes-insert-update-delete-merge-copy-from-stdin).

**Sin definiciones.** `CREATE`, `ALTER` y `DROP` se rechazan con SQLSTATE 0A000 tenga el rol lo que tenga, salvo la tabla temporal propia de una sesión; consulte [Tablas temporales](#temporary-tables). `TRUNCATE` es una escritura de datos.

**Sin soporte real de transacciones.** BEGIN/COMMIT/ROLLBACK se aceptan y se ignoran silenciosamente. Cada sentencia se ejecuta de forma independiente.

**Tiempo límite de consulta.** Una sentencia puede usar el tiempo límite de solicitud propio de pgwire, `limits.request_timeouts.pgwire`, o el compartido `limits.request_timeout` cuando aquel no está definido; ambos son ajustes del operador.

**La entrada de COPY es text o csv.** `COPY ... TO STDOUT` escribe `text`, `csv` y `binary`; `COPY ... FROM STDIN` lee `text` y `csv`.

**Los parámetros siguen vinculados.** Los valores de `$1`, `$2`, ... nunca se insertan en el texto de la instrucción: viajan vinculados hasta el motor o el origen, así que todos los valores comparten un mismo texto SQL (los prepares del servidor y las cachés basadas en el texto SQL siguen funcionando) y un valor nunca puede cambiar la forma gobernada. Solo la intercepción del catálogo, que responde ella misma a las consultas de `information_schema` y `pg_catalog`, lee los valores en línea.

**DECLARE SCROLL guarda en búfer el resultado completo.** Un cursor `SCROLL` acumula en el heap de Python cada fila que obtiene del motor para poder servir `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` y `LAST` cuando se piden. Con un resultado grande, todo el resultado vive en memoria mientras el cursor esté abierto. Use el predeterminado `NO SCROLL` para streaming solo hacia delante.

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Estas tablas existen en la capa de catálogo pero son stubs vacíos. Las herramientas de monitoreo que las consultan recibirán cero filas en lugar de errores. (`pg_index` sí está poblada; vea Intercepción de catálogo.)
