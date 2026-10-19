# Réplicas

Una réplica es una copia de una tabla de origen que Provisa mantiene en el almacén de materialización del motor y lee en lugar del origen. Si una tabla se replica o no lo fija [`replicate`](configuration.md#the-replicate-setting) y lo que el motor puede alcanzar; consulte [Qué cambia un motor](engines.md#what-an-engine-changes).

## Las réplicas están indexadas {: #replicas-are-indexed }

Provisa indexa una réplica según cómo el modelo usa su tabla.

- **La clave primaria declarada**, aplicada como clave.
- **Las columnas sobre las que se unen las relaciones declaradas**:
  - la clave foránea de cada relación cuyo origen es esta tabla;
  - la columna de destino de cada relación cuyo destino es esta tabla, salvo que esa columna ya sea la clave;
  - ambas claves foráneas de una tabla de unión que recorre una relación.

Tome esta relación:

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

La réplica de `orders` obtiene su clave y un índice en `customer_id`. La réplica de `customers` obtiene su clave y, cuando `id` es esa clave, no necesita nada más. Una unión de las dos lee entonces un índice en ambos lados, en un almacén que usa índices para las uniones (consulte [Qué construye cada almacén](#what-each-store-builds)). Una relación sobre una columna calculada que la réplica no contiene no obtiene índice.

Declare la clave con `is_primary_key: true` en la columna. Una réplica sin clave declarada obtiene solo sus índices de unión.

### Legible primero, indexada después {: #readable-first-indexed-after }

Un aterrizaje nunca espera a los índices. La réplica se lee desde el momento en que se intercambia; el runner de réplicas construye después sus índices uno a uno, en una pasada aparte, bajo un bloqueo de índices por réplica. Cuente con que las primeras consultas tras un aterrizaje se ejecuten sin ellos.

Cada aterrizaje registra los índices que solicitó, los construidos y los que fallaron, por nombre, en el registro de la réplica en el plano de control (`replica_state.index_state`).

- **Un índice fallido nunca hace fallar el aterrizaje.** Se registra con su causa y el siguiente aterrizaje vuelve a solicitarlo. La línea del registro dice `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`.
- **Las filas que repiten una clave declarada son un fallo aparte**, con el código `replica.index_duplicate_key`, que nombra las columnas de la clave. El origen contiene duplicados de un conjunto de columnas que el modelo declara único. Un reintento no lo arregla: corrija los datos en el origen o corrija la clave declarada.
- **Un aterrizaje que reemplaza la tabla durante una construcción** deja atrás las construcciones anteriores; se detienen y no registran nada, y la nueva tabla obtiene las suyas. En un almacén PostgreSQL, el intercambio cancela una construcción que aún se ejecuta sobre la tabla que reemplaza.
- **El plan sigue al modelo en cada aterrizaje.** Una relación añadida más tarde obtiene su índice en el siguiente aterrizaje de la réplica.

La interfaz de administración no muestra el registro de índices. Lea el registro del servidor o la columna `index_state` de la fila de la réplica.

### Qué construye cada almacén {: #what-each-store-builds }

| Almacén | Qué se construye | Cómo |
| --- | --- | --- |
| PostgreSQL: la base de datos propia del motor `pg`, el almacén PostgreSQL de Trino o DuckDB, el motor `sqlalchemy` sobre PostgreSQL | Índices de clave y de unión | `CREATE INDEX CONCURRENTLY` en su propia conexión, de modo que no se bloquea a lectores ni escritores. La clave se construye como índice único y luego se convierte en clave primaria con `ADD CONSTRAINT ... PRIMARY KEY USING INDEX`. Un índice no válido que dejó una construcción cancelada se elimina y se construye de nuevo. |
| Otras bases de datos tras un motor relacional (`mysql`, `oracle`, `mssql` y el resto) | Índices de clave y de unión | El DDL propio del dialecto. Una base de datos que rechaza el DDL registra el índice como fallido. |
| Archivo DuckDB integrado, MotherDuck | Índices de clave y de unión | A través del broker del almacén: `ALTER TABLE ... ADD PRIMARY KEY`, `CREATE INDEX IF NOT EXISTS`. |
| ClickHouse | Solo la clave de ordenación | Una réplica de ClickHouse se ordena por su clave declarada (`ORDER BY`), fijada al aterrizar. ClickHouse no tiene un índice B-tree secundario que construir. |
| Snowflake, BigQuery, Databricks, Fabric, Synapse | Nada | El almacén de datos organiza la tabla por sí mismo. |

Un índice solo ayuda a una unión que se ejecuta donde está el índice. Dos motores leen su almacén sin usar sus índices para las uniones:

- **DuckDB con su archivo integrado.** El motor nunca adjunta el archivo. Cada instrucción lee su propia copia local de cada tabla del almacén que nombra, rehecha cada vez que el almacén ha cambiado. Las uniones se ejecutan como uniones hash sobre esa copia, así que los índices de unión no las aceleran; los índices del archivo aplican la clave y sirven a todo lo que lee el archivo in situ.
- **DuckDB con un almacén PostgreSQL.** DuckDB lee el almacén a través de su extensión `postgres` con el pushdown de filtros desactivado (error upstream duckdb/duckdb-postgres#564), así que escanea la réplica y filtra y une en DuckDB. Los índices del almacén se construyen, pero estas lecturas no los usan.

Un almacén MotherDuck se adjunta y se lee in situ.

## Use una base de datos relacional como almacén de réplicas donde el motor ejecuta SQL en ella {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

Cuando el motor ejecuta sus uniones dentro de una base de datos relacional, conserve allí las réplicas. Las uniones leen entonces los índices anteriores. Tres configuraciones lo hacen:

- **El motor `pg`.** Las réplicas aterrizan en la propia base de datos PostgreSQL del motor y cada instrucción se ejecuta allí, a través de sus índices.
- **Un motor relacional** (`mysql`, `oracle`, `mssql`, el comodín `sqlalchemy` y el resto). Cada origen aterriza en esa base de datos y se consulta allí.
- **Trino con su almacén PostgreSQL.** PostgreSQL es el único almacén que admite Trino; Trino lee las réplicas a través de su conector PostgreSQL. Un filtro o una unión que Trino delega en PostgreSQL usa los índices; el trabajo que hace Trino por sí mismo, no.

Apunte un motor a un almacén con `PROVISA_MATERIALIZE_URL` o `materialize_store_url`; el almacén registrado por la propia organización tiene prioridad sobre ambos.

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

Esta es una recomendación general, no una regla. Que compense depende del motor, los orígenes, el almacén y el tamaño de las tablas. Sopese lo siguiente:

- **Alcance por consulta.** Un almacén al que el motor llega por la red añade un viaje de ida y vuelta y una transferencia a cada lectura. El archivo DuckDB integrado es local y columnar, y escanea rápido.
- **Costo del aterrizaje.** Escribir una réplica en una base de datos relacional y luego construir sus índices cuesta más que escribir un archivo local.
- **La carga propia de la base de datos.** Las lecturas de réplicas, los aterrizajes y las construcciones de índices se ejecutan todos en esa base de datos. Dimensiónela para ellos.
- **Escaneos grandes.** Un índice no sirve de nada para una consulta que lee la mayor parte de una tabla grande. Un almacén columnar la escanea más rápido que uno por filas.

## Elegir dónde viven las réplicas {: #choosing-where-replicas-live }

El almacén es una palanca entre varias, y se solapan. Resuelva primero las demás; el almacén decide solo lo que ellas dejan.

| Palanca | Qué hace | Dónde ayuda |
| --- | --- | --- |
| Tablas hot | Una tabla de consulta pequeña se mantiene en Redis y se incluye en cada consulta que la lee como un CTE (expresión de tabla común) `VALUES` en línea. | Uniones con tablas de consulta pequeñas, en cualquier almacén: las filas viajan dentro de la consulta. El destino de una relación muchos-a-uno, o una tabla con `hot_tables.auto_threshold` filas o menos (1000 por defecto), se hace hot en su primera lectura. `hot: true` obliga a incluir una tabla y `hot: false` la mantiene fuera. |
| Vistas materializadas que usted define | Una vista con `materialize: true` se almacena y se refresca. Una consulta que lee la vista se responde desde las filas almacenadas cuando está fresca y el lector la ve completa: ninguna entrada lleva una regla de filas más estrecha que todo. | Agregados sobre tablas grandes. Usted decide cuáles existen; no se crea ninguna por usted. Consulte [Vistas](configuration.md#views-governed-computed-datasets). |
| Relación con `materialize: true` | Una relación entre orígenes obtiene una vista padre-hijo almacenada y preunida. Una solicitud cuya unión coincide con el patrón, total o parcialmente, se reescribe para leerla, tanto si llega como SQL como si es una consulta que construye el compilador. | Uniones en un almacén que no construye índices de unión. Consulte [Automaterialización](configuration.md#auto-materialization). |
| Ubicación | Los orígenes que el motor puede leer en vivo se leen en vivo; el resto se replica. Lo que acaba en el almacén se deduce del motor que elija. | Elegir el motor decide la mayor parte de lo que debe contener el almacén. |

La materialización de relaciones y las uniones indexadas responden a la misma necesidad de dos maneras. En un almacén columnar o de data warehouse (el archivo DuckDB, ClickHouse, Snowflake, BigQuery, Databricks, Fabric, Synapse) no se construye ningún índice de unión, así que una vista preunida es la forma de abaratar una unión frecuente. En un almacén relacional las columnas de unión ya están indexadas, y las mismas uniones pueden ejecutarse rápido sin materializarlas.

Lo que aún depende del almacén: los escaneos y uniones ad hoc sobre tablas grandes que ninguna vista materializada cubre y ninguna tabla hot atiende. Estos se ejecutan sobre los índices del almacén en una base de datos relacional, o sobre su velocidad de escaneo en una columnar.

Los resultados varían según la carga. Mida antes y después de un cambio con la [herramienta de dimensionamiento](sizing.md); sus [ejecuciones en vivo o en réplica](sizing.md#live-or-replica-reads-and-the-ttl) dan el costo de una lectura en réplica frente al de una lectura en vivo en su despliegue.

## Frescura {: #freshness }

Haga que la frescura forme parte de la decisión, junto a la velocidad. Cuán antigua puede ser una respuesta, y qué le cuesta al origen mantenerla al día, deciden si una tabla se lee en vivo o desde una réplica y, por tanto, lo que debe contener el almacén.

Los orígenes empresariales rara vez admiten tiempo real. Los datos llegan por cargas por lotes o ETL nocturno, los flujos de cambios se retrasan y muchos sistemas no soportan lecturas en vivo al volumen de consultas. Parta de la frescura que necesita cada tabla y elija después la estrategia más barata que la cumpla. Las reglas del lado del operador están en [Frescura y carga: quién decide](configuration.md#freshness-and-load-who-decides).

| Estrategia | Se fija con | Qué garantiza | Qué le cuesta al origen |
| --- | --- | --- | --- |
| Lectura en vivo | `replicate: -1` | Actual al momento de la consulta | Cada consulta lee el origen |
| TTL | `change_signal: ttl`, `cache_ttl` | Una réplica no más antigua que `cache_ttl` | Una reconstrucción por `cache_ttl`; con una `watermark_column`, solo las filas posteriores a la marca de agua |
| Sonda de cambios | `change_signal: probe` o `ttl_probe`, `cache_ttl` | Se reconstruye solo cuando la sonda informa un cambio; la sonda se ejecuta cada `cache_ttl` | Una sonda por intervalo, y una reconstrucción solo si hay cambio |
| Flujo de cambios push | Un origen Kafka o WebSocket; `change_signal: debezium` en un origen con bloque `cdc`; `change_signal: native` en PostgreSQL, MongoDB o Cosmos DB | Actual dentro del retraso del flujo y del debounce | Kafka, WebSocket y Debezium: ninguno tras la primera construcción, el flujo lleva las filas. PostgreSQL, MongoDB y Cosmos DB: una reconstrucción por ráfaga de cambios |
| Señal externa | `change_signal: signal` | Se reconstruye cada vez que `POST /data/sources/{source_id}/signal` indica que el origen cambió; nunca por reloj | Una reconstrucción por señal |
| TTL por rol | `role_ttl` en una tabla | Un rol listado acepta una réplica de hasta su propio TTL; el `cache_ttl` de la tabla sigue siendo el piso | Menos reconstrucciones: la lectura de un rol tolerante nunca pide una |
| Protección de carga | `load_protected: true`, con `cache_ttl`, una sonda o una `off_peak_window` | Las consultas nunca leen el origen; solo el runner refresca la réplica, y con una ventana fuera de horas punta solo mientras está abierta | Solo los refrescos programados |
| Compuerta de frescura | `freshness_gate: true` en un origen | Cada lectura se comprueba primero contra el `change_signal` y el `cache_ttl` del origen; un veredicto de obsoleto refresca antes de la lectura | Un refresco en la ruta de lectura cuando está obsoleta |
| Nunca caduca | `cache_ttl: -1` en un origen o una tabla | Se carga una vez; cada lectura se responde desde la réplica, y la lectura de una tabla aún no cargada se rechaza indicando su nombre | Solo esa carga: un origen cuyas tablas se han cargado todas no se vuelve a adjuntar, sondear ni leer, y no necesita credenciales |

Una sonda que no tiene nada que comparar (sin marca de agua, centinela ni consulta de sonda) recurre al TTL.

### Casi tiempo real desde un flujo de cambios {: #near-real-time-from-a-change-feed }

Cuando un origen no puede soportar carga de consultas en vivo pero ya publica sus cambios, un flujo de cambios da datos casi en tiempo real sin tocar el origen en el momento de la consulta. Las consultas leen la réplica local, que es rápida y no pone carga en el origen, mientras el bucle de eventos aplica los cambios a medida que llegan. Los datos solo se retrasan lo que se retrasa el flujo, normalmente segundos.

Que el flujo exista no basta. Provisa lo sigue solo para una tabla que esté conectada a él:

- **Un origen Kafka.** La tabla nombra su topic y cada cambio se aplica a la réplica por clave primaria, incluidas las eliminaciones.

  ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **Un origen WebSocket.** Su flujo se aplica del mismo modo.
- **Una tabla con `change_signal: debezium`** (en la tabla o en su origen) sobre un origen cuyo bloque `cdc` indica el transporte Kafka y el prefijo de topic del conector. La primera lectura construye la réplica; después, cada evento de cambio del topic `{topic_prefix}.{schema}.{table}` se aplica por clave primaria, incluidas las eliminaciones, y el offset se confirma solo cuando el cambio ya se ha cargado.
- **Una tabla de un origen así con `change_signal: kafka`**. Sus cambios se leen del topic de deltas que indica su `live.kafka.topic`, por el mismo transporte, y se aplican del mismo modo; una tabla con esta señal que no indica topic se rechaza por nombre al guardar la tabla o su origen y al cargar la configuración.
- **Una tabla de MongoDB o Cosmos DB con `change_signal: native`** (en la tabla o en su origen). Cada ráfaga de cambios en el flujo de cambios solicita una reconstrucción de la réplica.
- **Una tabla PostgreSQL con `change_signal: native`** (en la tabla o en su origen). Provisa lee la replicación lógica de la base de datos desde un slot propio, `provisa_<source id>`, sobre una publicación del mismo nombre que enumera las tablas nativas del origen. No interviene Kafka. Cada ráfaga de cambios en una tabla solicita una reconstrucción de su réplica. La base de datos necesita lo mismo que Debezium: `wal_level` en `logical`, un rol con el permiso de replicación que pueda crear la publicación, y un primario en lugar de una réplica de lectura; un origen al que le falte algo se rechaza por nombre al guardarlo y al cargar la configuración. El slot y la publicación se crean cuando la primera tabla los sigue y se eliminan cuando la última deja de hacerlo o se elimina el origen. Al arrancar, un servidor revisa cada origen PostgreSQL registrado y elimina un slot de Provisa que no siga ninguna tabla nativa, de modo que un slot dejado por un cambio de configuración hecho sin ningún servidor en marcha no siga reteniendo el WAL del origen.

Una tabla alimentada por push necesita una clave primaria declarada; la que no la tiene se omite y se registra.

Una tabla con una señal de cambio push cuenta como fresca en la ruta de lectura, porque su flujo es su comprobación de frescura. Por eso se rechaza una señal push que nada recibiría: `change_signal: native` en cualquier tipo que no sea Kafka, WebSocket, ingest, PostgreSQL, MongoDB o Cosmos DB se rechaza al guardar el origen o la tabla y al cargar la configuración, y también `debezium` en un origen sin bloque `cdc`.
