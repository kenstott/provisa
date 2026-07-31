# Provisa

**Conecte sus bases de datos. Consulte con GraphQL, gRPC, SQL o MCP — sobre cualquier API o protocolo — en 5 minutos.**

Provisa expone cada superficie de API (REST, GraphQL, SQL, gRPC, MCP y más) sobre el resultado combinado de sus orígenes. Puede hacerlo porque es una **capa semántica activa**: una definición única de su estado de datos — cada dominio, relación y política a través de sus orígenes, excluyendo únicamente los sistemas de origen mismos — que a la vez opera el estado de datos y lo gobierna. La definición no es documentación que un motor pueda consultar; *es* el motor. Los dominios y relaciones registrados son las únicas rutas de unión legales, y las políticas de acceso se compilan en cada plan de consulta. Un modelo, tres funciones:

- **Definir** — Los dominios, columnas y relaciones se declaran una sola vez. Esa declaración es el esquema que ve cada consumidor y el único conjunto de rutas de unión que puede tomar cualquier consulta.
- **Aplicar** — La seguridad de nivel de fila, el enmascaramiento de columnas, la visibilidad de columnas y la aprobación de consultas se aplican en línea en la ruta de ejecución. Ninguna consulta llega a los datos sin pasar por ellas, de modo que la cobertura es total por construcción, no por diligencia.
- **Auditar** — Debido a que cada solicitud recorre la misma ruta gobernada, se registra de manera uniforme quién consultó qué, bajo qué rol y contra qué política. Las trazas distribuidas, métricas y registros se registran ellos mismos como tablas consultables junto a sus datos de negocio.

Un núcleo gobernado único sirve a cada lenguaje y transporte. Consulte con **GraphQL, Cypher o SQL**; consuma a través de **pgwire, Bolt, gRPC, REST, Arrow Flight o JDBC**. Cada lenguaje de consulta se reduce a una única representación intermedia donde el gobierno se inyecta una sola vez — de modo que una política no puede divergir entre lenguajes — y esa IR se redirige al dialecto nativo de cada origen a la salida. Agregar un lenguaje es un nuevo front-end sobre el núcleo compartido, no un nuevo motor.

El estado de datos es a la vez analítico y transaccional. Las lecturas entre orígenes se distribuyen a través de la capa de federación; las escrituras y lecturas de un solo origen se enrutan directamente al controlador del origen — gobernadas de manera idéntica, pero transaccionales y con menos de 100 ms. La transmisión columnar de Arrow Flight está integrada.

Todo el modelo se construye a partir de un puñado de primitivas — dominios, relaciones, roles y políticas. Vocabulario reducido, de modo que la definición es fácil de comprender y simple de evaluar y auditar: puede leer el conjunto de políticas y saber qué hace. Provisa es un compilador de consultas ligero, no un runtime que se sitúa en la ruta de datos. Convierte una solicitud en consultas nativas, las enruta y se aparta del camino — por eso el estado de datos rinde.

Ese diseño admite dos formas de usarlo, y no son excluyentes:

- **Como andamiaje para modernización** — Modele su estado de datos, deje que Provisa genere el SQL nativo para cada origen, y luego capture ese SQL y adóptelo directamente en el sistema de destino. Provisa es la capa de transición, no una dependencia permanente.
- **Como infraestructura permanente de aplicación de políticas** — Manténgalo en su lugar como la ruta gobernada que toma cada consulta, de modo que la definición, la aplicación y la auditoría permanezcan unificadas mientras exista el estado de datos.

## El modelo de federación

Todo el modelo se reduce a dos contratos y dos políticas: los orígenes se reducen a tablas 2-D sobre un único sistema de tipos, las consultas se reducen a una IR similar a SQL, la alcanzabilidad decide qué se consulta en vivo frente a qué se materializa, y una estrategia de frescura gobierna cada copia materializada y conjunto de datos derivado. Forma de datos de entrada, forma de consulta de entrada, gobierno en la unión, consultas nativas de salida. El resto de esta sección recorre cada pieza.

El modelo descansa en una reducción: cada origen se expresa como una colección de tablas bidimensionales sobre un único sistema de tipos generalizado. Ese es el contrato que un origen debe cumplir para unirse al estado de datos, y es el mismo contrato para todos ellos. Algunos orígenes ya encajan — una tabla de MySQL o PostgreSQL *es* una relación 2-D tipada. Algunos encajan con una proyección: el resultado de una consulta GraphQL, una vez aplanado, es una tabla. Algunos son ajenos a la forma — los triplestores SPARQL, Neo4j — pero siguen siendo manejables, porque el usuario proporciona una consulta cuyo conjunto de resultados es tabular; la consulta es el adaptador. Sea cual sea el origen, el estado de datos ve filas, columnas y tipos generalizados, y nada más. Incorporar un nuevo tipo de origen consiste en cumplir ese único contrato, a veces con un paso de intervención humana, no en escribir una integración a medida.

Esa reducción tiene una gemela en el lado de las consultas. SQL — a través de todos sus dialectos y particularidades — es esencialmente el lenguaje para el análisis sobre conjuntos de datos 2-D, lo que hace de una forma similar a SQL el objetivo universal natural para las consultas. Así que cada solicitud, en el lenguaje en que llegue, se reduce a esa representación intermedia como su primer paso. Algunas se reducen limpiamente — el propio SQL, e incluso GraphQL; algunas son difíciles — la semántica de rutas y grafos de Cypher requiere trabajo real — pero todas son factibles. Canalizar cada solicitud hacia una única IR antes de que ocurra cualquier otra cosa es lo que permite que el gobierno se aplique en exactamente un lugar, sobre una forma, sin importar el lenguaje en que haya llegado.

Sobre esas dos formas uniformes — orígenes tabulares y una única forma de consulta — la federación aquí significa tanto consulta en vivo como almacenamiento en warehouse — el mismo alcance que cubre un motor de consulta en vivo como Trino, más la materialización en la que se apoyan esos motores. El concepto que los unifica es la **alcanzabilidad**: para cualquier origen, ¿puede el motor consultarlo en su lugar, o sus datos deben materializarse primero en algún lugar consultable? La alcanzabilidad divide el estado de datos entre lo que se consulta en vivo y lo que se copia primero.

La mayoría de las bases de datos ya llevan alguna noción de enlace en vivo — `ATTACH` de DuckDB, `postgres_fdw` de PostgreSQL, enlaces externos de Databricks. Así que la mayoría de las bases de datos pueden actuar como motor de federación hasta cierto punto. Ninguna es exhaustiva: cada una alcanza un conjunto particular de orígenes y materializa el resto, sin un registro único de cuál es cuál. El modelo cierra esa brecha haciendo explícita la alcanzabilidad — un conjunto definido de métodos, por origen, que indican qué puede alcanzar el motor en vivo y, por eliminación, qué debe materializarse.

Lo que queda es la frescura: para cada origen no alcanzable, ¿qué tan actual debe estar su copia materializada? En la práctica esto se reduce a un pequeño conjunto de estrategias — bajo demanda, en un horario programado, ante una señal de cambio (CDC, marca de agua, snapshot), o fijada. Elegir una por origen es toda la política de frescura.

Los conjuntos de datos analíticos — tablas derivadas, agregados, las salidas de una transformación — encajan en la misma forma. También deben expresarse en la IR, y por serlo, el linaje no es un sistema aparte que mantener: la ruta desde cada sistema de origen hasta una salida final *es* la IR que la produjo, legible de principio a fin. Construirlos plantea la cuestión de la frescura un paso más allá — ¿el conjunto de datos se actualiza según un horario, solo una vez que se cumplen sus condiciones previas, de forma continua como casi en tiempo real, o como un snapshot histórico fijado? Las formas de expresar cómo y cuándo construir un conjunto de datos son el mismo pequeño vocabulario enumerable, de modo que un conjunto de datos derivado lleva una política de construcción en exactamente el mismo vocabulario que una copia de origen.

Los modelos dimensionales son una aplicación directa. Las tablas de hechos y dimensiones de un esquema en estrella son conjuntos de datos analíticos como cualquier otro — una dimensión es una proyección conformada y deduplicada; una tabla de hechos es una unión y un agregado reducidos a un grano — cada una con su propia política de construcción y frescura. Las dimensiones de cambio lento no necesitan maquinaria especial: un snapshot fijado es historial de Tipo 2, una reconstrucción programada es Tipo 1. Y debido a que el esquema se define en la IR en lugar de estar físicamente vinculado a las tablas de un solo warehouse, las mismas definiciones de hechos y dimensiones se redirigen — materializadas en Oracle, en Databricks, o dejadas virtuales sobre un motor MPP — sin necesidad de remodelar. El modelo genera el esquema en estrella; no lo ata a un motor.

Data Vault encaja de la misma manera, una capa antes. Sus hubs son conjuntos de datos de clave de negocio deduplicados, sus links son las relaciones registradas entre ellos, y sus satellites son conjuntos de datos de atributos de solo inserción, con marca de tiempo — el registro histórico. Un satellite es simplemente un conjunto de datos derivado bajo la estrategia de frescura por señal de cambio: fecha de carga más hashdiff es CDC aplicado a atributos descriptivos, e historial de solo inserción es la estrategia de snapshot fijado. Las tablas point-in-time y bridge son conjuntos de datos derivados adicionales construidos para el rendimiento de consulta. Así, un raw vault es un conjunto de conjuntos de datos analíticos en la IR, y un esquema en estrella es una proyección a partir de él — ambos generados, ambos portables entre motores. Lo que el modelo no hace es decidir la metodología: qué se convierte en hub, el grano de un satellite, la estrategia de división. Esas siguen siendo decisiones de modelado; una vez tomadas, viven como IR portable en lugar de ETL soldado a un solo warehouse.

Ambos patrones se declaran mediante **dos atajos de primera clase** en lugar de vistas escritas a mano — las primitivas a partir de las cuales se construyen todo esquema en estrella y todo Data Vault, mantenidas neutrales respecto a la metodología:

- **`entity`** — una proyección con clave, deduplicada y opcionalmente historizada de un origen. Declare una clave de entidad, los atributos y un modo de historial; Provisa la reduce a una vista materializada, y cuando se solicita historial, a una **MV bitemporal** (`scd2` → delta, `snapshot` → snapshot). Un solo constructo sirve tanto a una **dimensión** de Kimball (SCD1/SCD2) como a un **hub + satellite** de Data Vault.
- **`fact`** — una unión a claves de entidad, reducida a un grano declarado, con medidas agregadas. Provisa la reduce a una MV agregada más relaciones registradas hacia las entidades. Un solo constructo sirve tanto a una **tabla de hechos** en estrella como a un **link** de Data Vault (un hecho sin medidas es un link puro de conjunto de claves).

Debido a que la reducción es pura — una especificación de `entity`/`fact` se convierte exactamente en las definiciones de MV, bitemporal y relación que un modelador escribiría de otro modo a mano — el warehouse es IR de principio a fin y se redirige entre motores sin necesidad de remodelar. Declare un warehouse en la UI de administración (un formulario **Model** para entidades y hechos) o mediante la API de administración (`registerEntity` / `registerFact`); el modelo *genera* la estrella de Kimball o el Data Vault, no impone uno.

### Viaje en el tiempo

El viaje en el tiempo es una idea simple — conservar cada versión de una fila en lugar de sobrescribirla, de modo que pueda preguntar cómo *eran* los datos en cualquier momento pasado. Lo que difiere es la eficiencia con la que cada motor puede hacerlo, que es exactamente la razón por la que Provisa lo convierte en una propiedad de la **definición** de la vista materializada en lugar de del motor de almacenamiento (REQ-1162). Declárelo una sola vez; funciona en cualquier backend que materialice.

La regla que lo mantiene portable es **solo-anexar** (append-only): una versión, una vez escrita, nunca se actualiza ni se elimina. Retirar una fila escribiendo una fecha "válida hasta" — el truco bitemporal habitual — necesita un UPDATE, que muchos motores no pueden hacer de forma económica (o en absoluto) sobre un almacén federado, así que Provisa no lo hace. En cambio, cada actualización **anexa**, y "qué versión estaba vigente en el momento T" se deriva en el momento de la lectura a partir del registro inmutable. Hay exactamente dos formas de anexar:

- **Snapshot** — anexa todo el conjunto de datos actualizado, marcado con la hora de sistema de esta actualización. Sin comparación de diferencias; correcto en cualquier motor; el almacenamiento crece en una copia completa por actualización.
- **Delta** — anexa solo lo que cambió, más tombstones para las claves eliminadas. El delta lo **calcula el motor** (anti-joins dentro de un `INSERT … SELECT`), nunca se pliega fila por fila en Provisa. Más pequeño, y necesita una clave de entidad.

El tiempo de sistema (cuándo Provisa registró una versión) se gestiona de esta manera; el tiempo de validez (cuándo un hecho es verdadero en el negocio) lo proporciona el propio SELECT de la vista y se preserva. Los motores que ofrecen más — snapshots nativos de Iceberg, un MERGE que mantiene menos filas — pueden usarse como objetivo por eficiencia detrás de la misma declaración; la ruta solo-anexar es el mínimo garantizado que es correcto en todas partes.

La lectura es transparente. Una consulta simple contra una MV bitemporal reconstruye el estado **actual** a partir del registro de anexado por defecto; para viajar en el tiempo, envíe un encabezado `X-Provisa-As-Of: <timestamp>` y toda la consulta se responde como estaba el estado de datos en ese momento — semántica idéntica en cualquier sustrato. Actívelo para cualquier vista materializada en la UI de administración (un control **Time Travel**: off / snapshot / delta más una clave de entidad) o mediante la API de administración.

Alcanzabilidad más frescura es un modelo general para la federación de datos: una definición que indica qué está en vivo, qué está materializado y cuán fresca se mantiene cada copia — independiente del alcance de un motor concreto. El resultado es libertad frente al bloqueo con proveedores propietarios. El modelo es portable; el estado de datos no está cautivo del proveedor que en un momento dado alcance la mayor cantidad de orígenes.

## Funcionalidades

### Interfaces de consulta

Estos son los lenguajes y las API estructuradas en las que se escriben las consultas. Cada uno tiene su propia sintaxis y semántica; el gobierno (RLS, enmascaramiento, visibilidad de columnas, aplicación de relaciones) se aplica de manera uniforme en todos ellos sin importar qué protocolo de transporte los entregue.

- **GraphQL** — Esquemas por rol con visibilidad a nivel de campo, filtrado, paginación basada en cursor y consultas de agregación (`count`, `sum`, `avg`, `min`, `max`). Restringido por esquema a las relaciones registradas — estructuralmente válido por construcción, la ruta más rápida hacia una consulta simple correcta. Incluye Apollo APQ: las consultas se hashean y se registran en el servidor; las llamadas posteriores envían solo el hash mediante HTTP GET, lo que hace que las respuestas sean cacheables por CDN sin necesidad de cambios en el cliente. Las tablas de referencia por debajo de un umbral de filas configurable se exponen como tipos enum.
- **SQL** — SQL completo sobre datos federados; no restringido y más expresivo que GraphQL. Escriba SQL estándar — subconsultas correlacionadas incluidas — y se ejecuta a través de los orígenes sin cambios. Las consultas de un solo origen omiten por completo la capa de federación (menos de 100 ms).
- **Cypher** — Lenguaje de consulta de grafos sobre el mismo esquema federado. Recorra relaciones como aristas de grafo; combine orígenes mediante unión; rutas de longitud variable. El gobierno se aplica de forma idéntica a GraphQL y SQL.
- **API de modelo gRPC** — `.proto` autogenerado a partir del esquema registrado; RPC de consulta e inserción tipadas por tabla, respuestas en streaming. Impulsado por esquema en el mismo sentido que GraphQL — el modelo de registro es el contrato, protobuf es la codificación de transporte. A diferencia de Arrow Flight (que es un transporte de streaming columnar), esta es una interfaz de consulta completa por tabla.
- **JSON:API** — API de consulta estructurada en `/data/jsonapi/{table}`, diseñada exclusivamente para HTTP. Admite JSON:API 1.1: conjuntos de campos dispersos (`fields[table]=col1,col2`), expresiones de filtro (`filter[field][op]=value`), documentos compuestos (`include=relation`) y ordenamiento. No es un lenguaje de consulta de propósito general — consulta una tabla a la vez con sintaxis de filtro estandarizada en lugar de una cadena de consulta ad hoc.
- **Explorador de lenguajes de consulta** — Escriba una consulta GraphQL y vea traducciones en vivo a **SQL semántico** y **Cypher** en paneles laterales; copie cualquiera de ellas o salte directamente al editor de SQL o de Grafo. Un flujo de trabajo práctico es esbozar fragmentos de consulta en GraphQL y luego ensamblar el SQL resultante en vistas o informes complejos.

El Explorador muestra una consulta GraphQL junto con sus traducciones en vivo a SQL y Cypher:

![Explorador de lenguajes de consulta](docs/images/query-explorer.png)

El mismo esquema federado se puede explorar como un grafo en vivo — etiquetas de dominio y nodo, tipos de relación y recorridos de longitud variable:

![Visualización de grafo](docs/images/graph-view.png)

### Herramientas de composición de consultas

Estas herramientas le ayudan a escribir consultas en los lenguajes anteriores — no son lenguajes de consulta en sí mismas.

- **Consulta en lenguaje natural** — Pipeline de NL→SQL/Cypher/GraphQL impulsado por Claude. Describa lo que desea en lenguaje sencillo; el pipeline produce una consulta en el lenguaje elegido con un ciclo de validación interactivo antes de la ejecución.

![Consulta en lenguaje natural](docs/images/natural-language.png)

### Protocolos de transporte

Estos son los protocolos de conexión. SQL, GraphQL y Cypher viajan sobre ellos — la elección del protocolo de transporte no cambia la interfaz de consulta ni el comportamiento de gobierno.

- **pgwire** — Cualquier cliente de PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` de pandas) se conecta en el puerto 5439 como si fuera un servidor Postgres. Acepta solo SQL. Se aplica el pipeline de gobierno completo. `pg_catalog` e `information_schema` se responden desde un catálogo en memoria, de modo que los navegadores de esquemas funcionan sin un round-trip de federación. TLS opcional.
- **Bolt (Neo4j)** — Cualquier cliente de Neo4j (Neo4j Browser, Bloom, controladores oficiales) se conecta mediante el protocolo Bolt y ejecuta Cypher contra el grafo federado. Cada rol que posee el usuario se expone como una base de datos `provisa_<role>`. Mismo gobierno que cualquier otro transporte. TLS opcional.
- **Arrow Flight** — Streaming columnar de alto rendimiento sobre gRPC; acepta GraphQL o SQL como entrada de consulta. Conjuntos de resultados no acotados, sin materialización en el servidor, sin infraestructura separada requerida.
- **JDBC** — Integración con herramientas de BI (Tableau, Power BI, DBeaver) en modo `approved` o `catalog`.
- **WebSocket / SSE** — Suscripciones: eventos de cambio casi en tiempo real; backends: nativo de PG, nativo de MongoDB, CDC, sondeo. También expuesto mediante Kafka.

### Orígenes de datos

- **46 tipos de origen** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestores SPARQL, Kafka, Google Sheets y más a través de una única API; los orígenes de grafo y RDF son de primera clase, no adaptadores
- **Enrutamiento inteligente** — Las consultas de un solo origen omiten la federación (menos de 100 ms); las consultas multi-origen se enrutan a través de la capa de federación — traiga su propio clúster o use los workers integrados
- **Orígenes de API** — Registre endpoints REST, GraphQL, gRPC, WebSocket o RSS como tablas consultables; se incluyen ayudantes de SPARQL; las uniones federadas entre orígenes de API y orígenes relacionales funcionan de forma transparente
- **Introspección de esquema remoto** — Apunte a cualquier endpoint de GraphQL, OpenAPI o gRPC; las operaciones documentadas se exponen automáticamente como tablas consultables, nodos de grafo y aristas, con el gobierno completo aplicado encima
- **Orígenes de archivos** — Archivos CSV, Parquet y SQLite como tablas consultables; admite rutas locales y almacenamiento de objetos remoto (`s3://`, `ftp://`, `sftp://`)
- **Integración con Kafka** — Los topics como tablas de solo lectura; los resultados de consulta como sinks de Kafka
- **Disparadores programados** — Disparadores cron e de intervalo (APScheduler) que activan webhooks, mutaciones o publicaciones a sinks de Kafka
- **Sugerencias de rendimiento de federación** — Sugerencias de enrutamiento mediante comentarios SQL que anulan las decisiones de enrutamiento automático

![Orígenes de datos](docs/images/data-sources.png)

Los orígenes, archivos y endpoints remotos se registran como tablas gobernadas desde la UI:

![Registro de tablas](docs/images/table-registration.png)

### Seguridad y gobierno

- **Seguridad de nivel de fila** — Inyección de cláusula WHERE por tabla y por rol
- **Enmascaramiento de columnas** — Enmascaramiento por columna (regex, constante, truncado) con exención basada en el rol
- **Preajustes de columna** — Valores estáticos del lado del servidor o de variable de sesión inyectados en insert/update; no expuestos en los tipos de entrada de mutación
- **Permisos de escritura** — Control de acceso de mutación por columna (`writable_by`)
- **Roles heredados** — Los roles heredan RLS, visibilidad y enmascaramiento de un rol padre de forma recursiva
- **Funciones y webhooks rastreados** — Funciones de base de datos y webhooks salientes expuestos como mutaciones GraphQL con formas de retorno tipadas
- **Hook de aprobación ABAC** — Hook de autorización previo a la ejecución; transporte webhook, gRPC o unix_socket; alcance por tabla, por origen o global; política de fallback configurable
- **Autenticación conectable** — Firebase, Keycloak, OAuth 2.0, simple (para pruebas)

![Roles de seguridad](docs/images/security-roles.png)

### Entrega y rendimiento

- **Vistas materializadas como transformaciones registradas** — Una MV captura la transformación que la produjo: su forma de unión o SQL, las señales de entrada por origen (snapshot de Iceberg, marca de agua de RDB) con las que se construyó, y una verificación de determinismo en el registro. Debido a que la transformación queda registrada, las consultas (o subexpresiones) se reescriben de forma transparente sobre una MV actualizada — coincidencia estructural de patrones de unión con soporte de coincidencia parcial, de modo que una MV que cubre un subconjunto de uniones aun así se aplica, preservando las uniones restantes
- **Inclusión de tablas activas** — Las tablas de referencia pequeñas y frecuentemente unidas se incluyen como CTE de tipo VALUES directamente en el plan de consulta, eliminando los round-trips entre orígenes para datos de dimensión
- **Caché de consultas** — Caché de resultados en Redis particionada por rol+RLS; incluye caché de hash de APQ
- **Observabilidad como datos** — Las trazas distribuidas, métricas y registros se recopilan mediante OpenTelemetry, se compactan en Iceberg sobre S3, y se registran automáticamente como tablas consultables (`traces`, `metrics`, `logs`, `queries`) en el esquema federado; consúltelas con SQL, GraphQL o Cypher junto a sus datos de negocio — combine mediante join la tabla `customers` con la tabla `queries` para ver quién ejecutó qué y cuánto tardó

### Administración e integración

- **API de administración** — GraphQL en `/admin/graphql`; carga/descarga de configuración, edición de relaciones, aprobación de consultas
- **GraphQL Voyager** — Visualización interactiva del esquema por rol como diagrama entidad-relación
- **Descubrimiento de relaciones con LLM** — Sugerencias de candidatos a clave foránea impulsadas por Claude
- **Cliente Python** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → Tables de pyarrow, dialecto de SQLAlchemy, soporte de ADBC
- **Ingesta de datos** — Endpoints HTTP para enviar datos de eventos JSON a la plataforma
- **Importación de Hasura v2 / DDN** — Convierte metadatos de Hasura v2 o YAML de supergrafo DDN a configuración de Provisa
- **Apollo Federation** — Expone Provisa como un subgrafo de Apollo Federation v2

Esquema con alcance por rol visualizado como diagrama entidad-relación (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Las relaciones se registran, aprueban y aplican como las únicas rutas de JOIN legales:

![Relaciones](docs/images/relationships.png)

## Modelo de seguridad

Aquí es donde "en la ruta que ya toma cada consulta" deja de ser un eslogan. Provisa aplica un modelo de seguridad multicapa a través de cada lenguaje de consulta (GraphQL, SQL, Cypher) y cada transporte (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). El gobierno se aplica de manera uniforme — no existe una ruta de consulta que lo eluda. La cobertura es total por construcción, no por diligencia: agregue un origen, columna o relación, y cada capa se le aplica automáticamente, sin nada que recordar registrar.

Las capas se aplican en orden. Una solicitud debe superar cada capa antes de que se evalúe la siguiente.

### Capa 0 — Filtrado de introspección

El esquema y el catálogo presentados a un rol contienen solo las tablas de su lista `domain_access` y las columnas que superan las reglas `visible_to` por columna. Los objetos fuera del acceso de un rol son invisibles en el momento del descubrimiento — no pueden consultarse, autocompletarse ni inferirse como existentes. Esto se aplica al esquema GraphQL, al catálogo SQL y al navegador de esquemas del editor de consultas.

### Capa 1 — Acceso público

Las tablas en dominios sin restricción de `domain_access` son visibles para todas las identidades autenticadas sin configuración adicional. Fricción cero para datos genuinamente públicos.

### Capa 2 — Acceso por dominio

Cada rol lleva una lista `domain_access` de IDs de dominio. Una consulta que toca una tabla fuera de esos dominios se rechaza antes de la ejecución. Este es el límite de propiedad grueso — un rol de RR. HH. no puede alcanzar tablas de finanzas sin importar cómo esté escrito el SQL.

### Capa 3 — Seguridad de nivel de fila

Una vez confirmado el acceso por dominio, se inyectan predicados `WHERE` por tabla y por rol en cada `SELECT` en el momento de la ejecución. Los predicados se evalúan contra los datos crudos. Un gerente regional que consulta una tabla de pedidos compartida ve solo las filas de su región, incluso en un `SELECT *`.

### Capa 4 — Visibilidad y enmascaramiento de columnas

Las columnas con una lista `visible_to` que excluye al rol solicitante se eliminan de la salida de la consulta. Las columnas con una regla de enmascaramiento tienen sus valores reemplazados — redacción por regex, reemplazo por constante o truncado — antes de que los resultados salgan del servidor. El enmascaramiento se aplica en todos los lenguajes de consulta y formatos de salida.

### Capa 5 — Guardia de predicados

Las columnas enmascaradas se rechazan en las cláusulas `WHERE` y `HAVING`. Sin esto, quien realiza la llamada podría inferir el valor sin enmascarar mediante búsqueda binaria en un filtro, aunque la salida esté enmascarada. El rechazo se aplica en el momento del análisis de la consulta, antes de la ejecución.

### Gobierno de relaciones

Las condiciones JOIN en SQL deben coincidir con una relación registrada y aprobada entre tablas. Las uniones no aprobadas se rechazan. Cada relación lleva una razón y descripción legibles por humanos — orientación tanto para usuarios como para agentes autónomos sobre por qué existe una ruta de recorrido. Esto es política de gobierno, no un límite de seguridad estricto: las capas 2 a 5 se mantienen sin importar la estructura de la unión, de modo que una elusión deliberada no expone datos a los que el rol no pudiera acceder mediante dos consultas separadas. Los intentos de elusión se registran y son auditables.

---

Estas capas se componen. Un rol con acceso por dominio, RLS y columnas enmascaradas tiene las cinco restricciones activas simultáneamente. Agregar un nuevo origen de datos, columna o relación no requiere actualizar cada regla — cada capa se configura de forma independiente y se aplica automáticamente a cualquier consulta que toque objetos gobernados.

### macOS

1. Descargue [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (siempre la última versión)
2. Arrastre **Provisa.app** a `/Applications` y haga doble clic para iniciarlo
3. El primer inicio completa una configuración única (~2 min, sin necesidad de internet)
4. Abra Terminal:

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Descargue [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (siempre la última versión)
2. Hágalo ejecutable y ejecútelo — el primer inicio completa una configuración única (sin necesidad de internet):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Descargue [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (siempre la última versión)
2. Ejecute el instalador — no requiere permisos de administrador
3. Abra **Provisa First Launch** desde el menú Inicio — completa una configuración única (~5 min, sin necesidad de internet)
4. Abra una nueva terminal:

```bash
provisa start
```

### Primera consulta

En desarrollo local (`PROVISA_MODE=test`), no se requieren credenciales. En producción, autentíquese con un token Bearer — el rol se extrae de él automáticamente.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Descargue [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (siempre la última versión) y agréguelo a la ruta de controladores de su herramienta de BI.

```text
jdbc:provisa://localhost:8815
```

Autentíquese con su nombre de usuario y contraseña de Provisa — el servidor asigna su rol.

- **modo `catalog`** — esquema completo visible; úselo con herramientas de catálogo (Collibra, Atlan, DBeaver)

Consulte [docs/integrations.md](docs/integrations.md) para los pasos de configuración de Tableau y Power BI.

### Protocolo de transporte de PostgreSQL (pgwire)

Provisa habla el protocolo de transporte de PostgreSQL en el puerto 5439. Cualquier cliente que pueda conectarse a Postgres se conecta a Provisa — sin controlador, sin adaptador, sin cambios en las herramientas existentes.

**El nombre de usuario de PostgreSQL selecciona el rol de Provisa.** Con `provider: none` (modo de confianza), la contraseña se ignora y se acepta como nombre de usuario cualquier nombre de rol configurado — conéctese como `analyst`, `admin`, o cualquier rol para ver la vista gobernada de los datos correspondiente a ese rol. Con `provider: simple`, la contraseña se valida con bcrypt. Otros proveedores (`firebase`, `keycloak`, `oauth`) no son compatibles con pgwire.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

Todas las consultas se ejecutan a través del pipeline de gobierno completo — el acceso por dominio, RLS, el enmascaramiento y la guardia de predicados se aplican exactamente igual que en GraphQL y REST. Los navegadores de esquema (DBeaver, DataGrip, pgAdmin) funcionan de inmediato: las consultas a `pg_catalog` e `information_schema` se responden desde un catálogo en memoria acotado al acceso por dominio del rol, de modo que los usuarios ven solo las tablas y columnas que tienen permiso de consultar.

DataGrip explorando el esquema gobernado y su diagrama de claves foráneas sobre pgwire — sin controlador, sin adaptador:

![Provisa en DataGrip sobre pgwire](docs/images/pgwire-datagrip.png)

TLS se habilita configurando `PROVISA_PGWIRE_CERT` y `PROVISA_PGWIRE_KEY`. El puerto es configurable mediante `PROVISA_PGWIRE_PORT` (por defecto `5439`).

### Bolt (protocolo de transporte de Neo4j)

Provisa también habla el protocolo **Bolt** de Neo4j, de modo que las herramientas nativas de grafo se conectan directamente y ejecutan Cypher contra el grafo federado — sin exportación, sin una base de datos de grafo separada. Apunte **Neo4j Browser** o **Bloom** hacia Provisa y recorra relaciones a través de orígenes con el mismo gobierno (acceso por dominio, RLS, enmascaramiento) aplicado.

Neo4j Browser ejecutando Cypher contra Provisa — las etiquetas de nodo, tipos de relación y claves de propiedad provienen directamente del esquema registrado:

![Provisa en Neo4j Browser sobre Bolt](docs/images/bolt-neo4j-browser.png)

Habilítelo configurando `PROVISA_BOLT_PORT` (el valor por defecto de Neo4j es `7687`). TLS se habilita con `PROVISA_BOLT_CERT` y `PROVISA_BOLT_KEY`. Cada rol de Provisa que posee el usuario autenticado se expone como una base de datos seleccionable `provisa_<role>` (el selector `provisa_admin` mostrado arriba) — elegir una acota la sesión a los derechos de dominio de ese rol; el usuario nunca puede exceder los roles que posee.

### Cliente Python

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Consulte [docs/python-client.md](docs/python-client.md) para la referencia completa.

## Documentación

| Tema | Documento |
| --- | --- |
| Inicio rápido para desarrolladores (ejecución desde el código fuente) | [docs/quickstart.md](docs/quickstart.md) |
| Referencia completa de configuración YAML | [docs/configuration.md](docs/configuration.md) |
| Referencia de endpoints (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Diseño del sistema y mapa de componentes | [docs/architecture.md](docs/architecture.md) |
| Modelo de seguridad (RLS, enmascaramiento, autenticación) | [docs/security.md](docs/security.md) |
| Tipos de origen admitidos | [docs/sources.md](docs/sources.md) |
| Suscripciones SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, herramientas de BI, clientes de Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Cliente Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API de administración | [docs/admin.md](docs/admin.md) |
| Despliegue (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Importación de Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Flujo de trabajo de lanzamiento (etiquetas alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionamiento

Provisa incluye un motor de federación integrado para consultas multi-origen. En el primer inicio elige un presupuesto de RAM; Provisa deriva automáticamente el número de workers de federación locales.

| RAM del host | Workers | Carga de trabajo típica |
| --- | --- | --- |
| < 24 GB | 0 | Desarrollo, consultas de un solo origen, equipos pequeños |
| 24–47 GB | 1 | Equipo pequeño, consultas moderadas entre orígenes |
| 48–95 GB | 2 | Despliegue departamental, uso mixto de BI + notebook |
| 96 GB+ | 4 | Departamento grande, federación concurrente intensa |

El número de workers puede cambiarse en cualquier momento editando `~/.provisa/config.yaml` (`federation_workers: N`) y ejecutando `provisa restart`. Establézcalo en `0` para ejecutar solo en modo de coordinación (nodo único).

### Escalamiento más allá de una sola máquina

**Escalamiento horizontal** — Ejecute múltiples instancias de Provisa detrás de un balanceador de carga. Cada instancia es un sistema completamente funcional. Todas las instancias deben apuntar a la misma base de datos de configuración (establezca `CONFIG_DB_HOST` en las máquinas secundarias) y, opcionalmente, a una instancia de Redis compartida (`REDIS_URL`) para una caché unificada. La mayoría de las consultas se distribuyen de forma transparente; las uniones entre orígenes muy grandes pueden exceder los recursos de una sola instancia y requerir una máquina más grande o un clúster de federación externo.

**Redis compartido** — Establezca `REDIS_URL` en cada instancia para apuntar a un Redis externo. Un Redis compartido significa que las entradas de caché de una instancia están disponibles para todas, mejorando las tasas de acierto en todo el clúster.

**Traiga su propio clúster de federación** — Apunte Provisa a un clúster de federación externo existente en lugar de los workers integrados. Recomendado para despliegues a gran escala o en la nube; consulte [docs/deployment.md](docs/deployment.md) para la configuración.

## Licencia

Business Source License 1.1 (sin modificar, según los convenios de Licenciante de MariaDB). Cada
versión publicada se convierte en la Change License (GPL v2.0 o posterior) al cumplirse el 4.º
aniversario de su publicación pública; el código actual y reciente permanece bajo BSL.
El uso en producción por encima de los umbrales de la Additional Use Grant (menos de 100
empleados/contratistas y menos de 1 millón de USD de ingresos del año anterior) requiere una licencia
comercial. Consulte [LICENSE](LICENSE).

El Licenciante no consiente el uso de esta obra para entrenamiento de IA/ML. Consulte
[NOTICE](NOTICE), [ai.txt](ai.txt) y [robots.txt](robots.txt). Para licencias comerciales
o de entrenamiento de IA: <kennethstott@gmail.com>
