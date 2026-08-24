# Provisa

**Conecte sus bases de datos. Consulte con GraphQL, gRPC, SQL o MCP — sobre cualquier API o protocolo — en 5 minutos.**

Provisa sirve todas las superficies de API (REST, GraphQL, SQL, gRPC, MCP y más) sobre el resultado unido de todos sus orígenes. Puede hacerlo porque es una **capa semántica activa**: una única definición de su patrimonio de datos — cada dominio, relación y política a través de sus orígenes, excluyendo únicamente los propios sistemas de origen — que a la vez opera el patrimonio y lo gobierna. La definición no es documentación que un motor pueda consultar; *es* el motor. Los dominios y relaciones registrados son las únicas rutas de unión legales, y las políticas de acceso se compilan en cada plan de consulta. Un modelo, tres funciones:

- **Definir** — Los dominios, columnas y relaciones se declaran una sola vez. Esa declaración es el esquema que ve cada consumidor y el único conjunto de rutas de unión que puede tomar cualquier consulta.
- **Aplicar** — La seguridad de nivel de fila, el enmascaramiento de columnas, la visibilidad de columnas y la aprobación de consultas se aplican en línea en la ruta de ejecución. Ninguna consulta llega a los datos sin pasar por ellas, de modo que la cobertura es total por construcción y no por diligencia.
- **Auditar** — Como cada solicitud recorre la misma ruta gobernada, quién consultó qué, bajo qué rol y contra qué política se registra de manera uniforme. Los trazos distribuidos, las métricas y los registros se registran ellos mismos como tablas consultables junto a sus datos de negocio.

Un único núcleo gobernado sirve a todo lenguaje y transporte. Consulte con **GraphQL, Cypher o SQL**; consuma sobre **pgwire, Bolt, gRPC, REST, Arrow Flight o JDBC**. Cada lenguaje de consulta se reduce a una única representación intermedia donde el gobierno se inyecta una sola vez — de modo que una política no puede divergir entre lenguajes — y esa IR se retraduce al dialecto nativo de cada origen a la salida. Añadir un lenguaje es un nuevo front-end sobre el núcleo compartido, no un motor nuevo.

El patrimonio es a la vez analítico y transaccional. Las lecturas entre orígenes se distribuyen a través de la capa de federación; las escrituras y las lecturas de un solo origen se enrutan directamente al controlador del origen — gobernadas de forma idéntica, pero transaccionales y en menos de 100 ms. La transmisión columnar de Arrow Flight está integrada.

Todo el modelo se construye a partir de un puñado de primitivas — dominios, relaciones, roles y políticas. Vocabulario reducido, de modo que la definición es fácil de comprender y sencilla de evaluar y auditar: puede leer el conjunto de políticas y saber qué hace. Provisa es un compilador de consultas ligero, no un runtime que se sitúa en la ruta de los datos. Convierte una solicitud en consultas nativas, las enruta y se hace a un lado — por eso el patrimonio rinde.

Ese diseño admite dos formas de usarlo, y no son excluyentes:

- **Como andamiaje para la modernización** — Modele su patrimonio, deje que Provisa genere el SQL nativo de cada origen y luego capture ese SQL y adóptelo directamente en el sistema destino. Provisa es la capa de transición, no una dependencia permanente.
- **Como infraestructura permanente de aplicación de políticas** — Manténgalo en su lugar como la ruta gobernada que toma cada consulta, de modo que la definición, la aplicación y la auditoría permanezcan unificadas mientras exista el patrimonio.

## El modelo de federación

Todo el modelo se reduce a dos contratos y dos políticas: los orígenes se reducen a tablas 2-D sobre un único sistema de tipos, las consultas se reducen a una única IR tipo SQL, la alcanzabilidad decide qué se consulta en vivo frente a qué se materializa, y una estrategia de frescura gobierna cada copia materializada y cada conjunto de datos derivado. Forma de datos de entrada, forma de consulta de entrada, gobierno en la unión, consultas nativas de salida. El resto de esta sección recorre cada pieza.

El modelo se apoya en una reducción: cada origen se expresa como una colección de tablas bidimensionales sobre un único sistema de tipos generalizado. Ese es el contrato que un origen debe cumplir para unirse al patrimonio, y es el mismo contrato para todos ellos. Algunos orígenes ya encajan — una tabla de MySQL o PostgreSQL *es* una relación 2-D tipada. Algunos encajan con una proyección: el resultado de una consulta GraphQL, una vez aplanado, es una tabla. Algunos son ajenos a la forma — almacenes de triples SPARQL, Neo4j — pero siguen siendo utilizables, porque el usuario suministra una consulta cuyo conjunto de resultados es tabular; la consulta es el adaptador. Sea cual sea el origen, el patrimonio ve filas, columnas y tipos generalizados, y nada más. Incorporar un nuevo tipo de origen es cumplir ese único contrato, a veces con un paso de intervención humana, no escribir una integración a medida.

Esa reducción tiene su gemela en el lado de la consulta. SQL — en todos sus dialectos y peculiaridades — es esencialmente el lenguaje para el análisis sobre conjuntos de datos 2-D, lo que hace de una forma tipo SQL el objetivo universal natural para las consultas. Así que cada solicitud, en cualquier lenguaje en que llegue, se reduce a esa representación intermedia como su primer paso. Algunas se reducen con limpieza — el propio SQL, e incluso GraphQL; algunas son difíciles — la semántica de rutas y grafos de Cypher exige trabajo real — pero todas son factibles. Canalizar cada solicitud hacia una única IR antes de que ocurra cualquier otra cosa es lo que permite que el gobierno se aplique en exactamente un lugar, sobre una sola forma, sin importar el lenguaje en que llegó.

Sobre esas dos formas uniformes — orígenes tabulares y una única forma de consulta —, la federación aquí significa tanto la consulta en vivo como el almacenamiento: el mismo alcance que cubre un motor de consulta en vivo como Trino, más la materialización en la que se apoyan esos motores. El concepto que los unifica es la **alcanzabilidad**: para cualquier origen, ¿puede el motor consultarlo en su lugar, o sus datos deben materializarse primero en algún sitio consultable? La alcanzabilidad divide el patrimonio entre lo que se consulta en vivo y lo que se copia primero.

La mayoría de las bases de datos ya llevan alguna noción de enlace en vivo — `ATTACH` de DuckDB, `postgres_fdw` de PostgreSQL, enlaces externos de Databricks. Así que la mayoría de las bases de datos pueden actuar como motor de federación en cierta medida. Ninguna es integral: cada una alcanza un conjunto particular de orígenes y materializa el resto, sin un relato único de cuál es cuál. El modelo cierra esa brecha haciendo explícita la alcanzabilidad — un conjunto definido de métodos, por origen, que indica qué puede alcanzar el motor en vivo y, por eliminación, qué debe materializarse.

Lo que queda es la frescura: para cada origen no alcanzable, ¿qué tan actual debe estar su copia materializada? En la práctica esto se reduce a un pequeño conjunto de estrategias — bajo demanda, en un horario, ante una señal de cambio (CDC, marca de agua, instantánea) o fijada. Elegir una por origen es toda la política de frescura.

Los conjuntos de datos analíticos — tablas derivadas, agregados, las salidas de una transformación — se pliegan en la misma forma. Ellos también deben expresarse en la IR, y precisamente por eso el linaje no es un sistema aparte que mantener: la ruta desde cada sistema de origen hasta una salida final *es* la IR que la produjo, legible de principio a fin. Construirlos plantea la cuestión de la frescura un paso más allá — ¿el conjunto de datos se actualiza en un horario, solo una vez cumplidas sus condiciones previas, de forma continua casi en tiempo real, o como una instantánea histórica fijada? Las formas de expresar cómo y cuándo construir un conjunto de datos son el mismo pequeño conjunto enumerable, así que un conjunto de datos derivado lleva una política de construcción en exactamente el mismo vocabulario que una copia de origen.

Los modelos dimensionales son una aplicación directa. Las tablas de hechos y dimensiones de un esquema en estrella son conjuntos de datos analíticos como cualquier otro — una dimensión es una proyección conformada y deduplicada; una tabla de hechos es una unión y agregación reducida a un grano — cada una con su propia política de construcción y frescura. Las dimensiones de cambio lento no necesitan maquinaria especial: una instantánea fijada es historia Tipo 2, una reconstrucción programada es Tipo 1. Y como el esquema se define en la IR en lugar de estar físicamente ligado a las tablas de un solo almacén, las mismas definiciones de hechos y dimensiones se retraducen — materializadas en Oracle, en Databricks, o dejadas virtuales sobre un motor MPP — sin remodelar. El modelo genera el esquema en estrella; no lo ata a un motor.

Data Vault encaja de la misma manera, una capa antes. Sus hubs son conjuntos de datos deduplicados por clave de negocio, sus links son las relaciones registradas entre ellos, y sus satellites son conjuntos de datos de atributos con marca de tiempo y solo de inserción — el registro histórico. Un satellite es solo un conjunto de datos derivado bajo la estrategia de frescura de señal de cambio: fecha de carga más hashdiff es CDC aplicado a atributos descriptivos, y el historial de solo inserción es la estrategia de instantánea fijada. Las tablas point-in-time y bridge son conjuntos de datos derivados adicionales construidos para el rendimiento de consulta. Así que un raw vault es un conjunto de conjuntos de datos analíticos en la IR, y un esquema en estrella es una proyección sobre él — ambos generados, ambos portables entre motores. Lo que el modelo no hace es decidir la metodología: qué se convierte en hub, el grano de un satellite, la estrategia de división. Esas siguen siendo decisiones de modelado; una vez tomadas, viven como IR portable en lugar de ETL soldado a un solo almacén.

Ambos patrones se declaran mediante **dos atajos de primera clase** en lugar de vistas escritas a mano — las primitivas a partir de las cuales se construyen todo esquema en estrella y todo Data Vault, mantenidas neutrales respecto a la metodología:

- **`entity`** — una proyección con clave, deduplicada y opcionalmente historizada de un origen. Declare una clave de entidad, los atributos y un modo de historial; Provisa la reduce a una vista materializada, y cuando se solicita historial, a una **MV bitemporal** (`scd2` → delta, `snapshot` → instantánea). Una sola construcción sirve tanto a una **dimensión** de Kimball (SCD1/SCD2) como a un **hub + satellite** de Data Vault.
- **`fact`** — una unión a claves de entidad, reducida a un grano declarado, con medidas agregadas. Provisa la reduce a una MV agregada más relaciones registradas con las entidades. Una sola construcción sirve tanto a una **tabla de hechos** en estrella como a un **link** de Data Vault (un hecho sin medidas es un link puro de conjunto de claves).

Como la reducción es pura — una especificación `entity`/`fact` se convierte exactamente en las definiciones de MV, bitemporal y relación que un modelador escribiría a mano de otro modo —, el almacén es IR de principio a fin y se retraduce entre motores sin remodelar. Declare un almacén en la interfaz de administración (un formulario **Model** para entidades y hechos) o mediante la API de administración (`registerEntity` / `registerFact`); el modelo *genera* la estrella de Kimball o el Data Vault, no impone uno.

### Viaje en el tiempo

El viaje en el tiempo es una idea simple — conservar cada versión de una fila en lugar de sobrescribirla, de modo que pueda preguntar cómo *era* el dato en cualquier momento pasado. Lo que difiere es cuán eficientemente puede hacerlo cada motor, y es exactamente por eso que Provisa lo convierte en una propiedad de la **definición** de la vista materializada en lugar de del motor de almacenamiento (REQ-1162). Declárelo una vez; funciona sobre cualquier backend que materialice.

La regla que lo mantiene portable es **solo-anexar**: una versión, una vez escrita, nunca se actualiza ni se elimina. Retirar una fila escribiendo una fecha "válido-hasta" — el truco bitemporal habitual — necesita un UPDATE, que muchos motores no pueden hacer de forma económica (o en absoluto) sobre un almacén federado, así que Provisa no lo hace. En su lugar, cada actualización **anexa**, y "qué versión estaba vigente en el momento T" se deriva en tiempo de lectura a partir del registro inmutable. Hay exactamente dos formas de anexar:

- **Instantánea (Snapshot)** — anexa el conjunto de datos fresco completo, con la marca del tiempo de sistema de esta actualización. Sin comparación de diferencias; correcto en todo motor; el almacenamiento crece una copia completa por actualización.
- **Delta** — anexa solo lo que cambió, más las lápidas para las claves eliminadas. El delta es **calculado por el motor** (anti-uniones dentro de un `INSERT … SELECT`), nunca plegado fila por fila en Provisa. Más pequeño, y necesita una clave de entidad.

El tiempo de sistema (cuándo Provisa registró una versión) se gestiona de esta manera; el tiempo válido (cuándo un hecho es verdadero en el negocio) lo suministra el propio SELECT de la vista y se conserva. Los motores que ofrecen más — instantáneas nativas de Iceberg, un MERGE que mantiene menos filas — pueden orientarse hacia la eficiencia detrás de la misma declaración; la ruta de solo-anexar es el piso que es correcto en todas partes.

La lectura es transparente. Una consulta simple contra una MV bitemporal reconstruye el estado **actual** a partir del registro de anexado por defecto; para viajar en el tiempo, envíe un encabezado `X-Provisa-As-Of: <timestamp>` y toda la consulta se responde como estaba el patrimonio en ese momento — semántica idéntica en todo sustrato. Actívelo para cualquier vista materializada en la interfaz de administración (un control **Time Travel**: off / snapshot / delta más una clave de entidad) o mediante la API de administración.

Alcanzabilidad más frescura es un modelo general para la federación de datos: una definición que indica qué está en vivo, qué está materializado y qué tan fresca se mantiene cada copia — independiente del alcance de cualquier motor concreto. El resultado es libertad frente al bloqueo propietario. El modelo es portable; el patrimonio no queda cautivo de cualquiera que sea el vendedor de federación que hoy alcance más orígenes.

## Funciones

### Interfaces de consulta

Estos son los lenguajes y las API estructuradas en los que escribe consultas. Cada uno tiene su propia sintaxis y semántica; el gobierno (RLS, enmascaramiento, visibilidad de columnas, aplicación de relaciones) se aplica de manera uniforme en todos ellos, sin importar qué protocolo de transporte los entregue.

- **GraphQL** — Esquemas por rol con visibilidad a nivel de campo, filtrado, paginación basada en cursor y consultas agregadas (`count`, `sum`, `avg`, `min`, `max`). Restringido por esquema a relaciones registradas — estructuralmente válido por construcción, la ruta más rápida hacia una consulta simple correcta. Apollo APQ incluido: las consultas se hashean y se registran del lado del servidor; las llamadas posteriores envían solo el hash sobre HTTP GET, haciendo las respuestas cacheables por CDN sin cambios requeridos en el cliente. Las tablas de búsqueda por debajo de un umbral de filas configurable se exponen como tipos enum.
- **SQL** — SQL completo sobre datos federados; sin restricciones y más expresivo que GraphQL. Escriba SQL estándar — subconsultas correlacionadas incluidas — y se ejecuta entre orígenes sin cambios. Las consultas de un solo origen omiten la capa de federación por completo (menos de 100 ms).
- **Cypher** — Lenguaje de consulta de grafos sobre el mismo esquema federado. Recorra relaciones como aristas de grafo; una orígenes; rutas de longitud variable. El gobierno se aplica de forma idéntica a GraphQL y SQL.
- **API de modelo gRPC** — `.proto` autogenerado a partir del esquema registrado; RPC tipados de consulta e inserción por tabla, respuestas en streaming. Impulsado por esquema en el mismo sentido que GraphQL — el modelo de registro es el contrato, protobuf es la codificación de transporte. A diferencia de Arrow Flight (que es un transporte de streaming columnar), esta es una interfaz de consulta completa por tabla.
- **JSON:API** — API de consulta estructurada en `/data/jsonapi/{table}`, exclusiva de HTTP por diseño. Soporta JSON:API 1.1: conjuntos de campos dispersos (`fields[table]=col1,col2`), expresiones de filtro (`filter[field][op]=value`), documentos compuestos (`include=relation`) y ordenamiento. No es un lenguaje de consulta de propósito general — consulta una tabla a la vez con sintaxis de filtro estandarizada en lugar de una cadena de consulta ad hoc.
- **Explorador de lenguaje de consulta** — Escriba una consulta GraphQL y vea traducciones en vivo de **SQL semántico** y **Cypher** en paneles laterales; copie cualquiera de las dos o salte directamente al editor de SQL o de Grafo. Un flujo de trabajo práctico es esbozar fragmentos de consulta en GraphQL y luego integrar el SQL resultante en vistas o informes complejos.

El Explorador muestra una consulta GraphQL junto a sus traducciones en vivo a SQL y Cypher:

![Explorador de lenguaje de consulta](docs/images/query-explorer.png)

El mismo esquema federado es explorable como un grafo en vivo — etiquetas de dominio y nodo, tipos de relación y recorridos de longitud variable:

![Visualización de grafo](docs/images/graph-view.png)

### Herramientas de composición de consultas

Estas herramientas le ayudan a escribir consultas en los lenguajes anteriores — no son lenguajes de consulta en sí mismas.

- **Consulta en lenguaje natural** — Pipeline NL→SQL/Cypher/GraphQL impulsado por Claude. Describa lo que desea en inglés sencillo; el pipeline produce una consulta en el lenguaje elegido con un bucle de validación interactivo antes de la ejecución.

![Consulta en lenguaje natural](docs/images/natural-language.png)

### Protocolos de transporte

Estos son los protocolos de conexión. SQL, GraphQL y Cypher viajan sobre ellos — la elección de protocolo de transporte no cambia la interfaz de consulta ni el comportamiento de gobierno.

- **pgwire** — Cualquier cliente de PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` de pandas) se conecta en el puerto 5439 como si fuera un servidor Postgres. Acepta solo SQL. Se aplica el pipeline de gobierno completo. `pg_catalog` e `information_schema` se responden desde un catálogo en memoria para que los navegadores de esquema funcionen sin un viaje de ida y vuelta de federación. TLS opcional.
- **Bolt (Neo4j)** — Cualquier cliente de Neo4j (Neo4j Browser, Bloom, drivers oficiales) se conecta mediante el protocolo Bolt y ejecuta Cypher contra el grafo federado. Cada rol que posee el usuario se muestra como una base de datos `provisa_<role>`. Mismo gobierno que cualquier otro transporte. TLS opcional.
- **Arrow Flight** — Transmisión columnar de alto rendimiento sobre gRPC; acepta GraphQL o SQL como entrada de consulta. Conjuntos de resultados no acotados, sin materialización del lado del servidor, sin infraestructura separada requerida.
- **JDBC** — Integración con herramientas de BI (Tableau, Power BI, DBeaver) en modo `approved` o `catalog`.
- **WebSocket / SSE** — Suscripciones: eventos de cambio casi en tiempo real; backends: PG nativo, MongoDB nativo, CDC, sondeo. También expuesto sobre Kafka.

### Orígenes de datos

- **53 tipos de origen** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, almacenes de triples SPARQL, Kafka, Google Sheets y más a través de una única API; los orígenes de grafo y RDF son de primera clase, no adaptadores
- **Enrutamiento inteligente** — Las consultas de un solo origen omiten la federación (menos de 100 ms); las consultas multiorigen se enrutan a través de la capa de federación — traiga su propio clúster o use los workers integrados
- **Orígenes de API** — Registre endpoints REST, GraphQL, gRPC, WebSocket o RSS como tablas consultables; ayudantes de SPARQL incluidos; las uniones federadas entre orígenes de API y orígenes relacionales funcionan de manera transparente
- **Introspección de esquemas remotos** — Apunte a cualquier endpoint GraphQL, OpenAPI o gRPC; las operaciones documentadas se exponen automáticamente como tablas consultables, nodos de grafo y aristas con el gobierno completo aplicado encima
- **Orígenes de archivo** — Archivos CSV, Parquet y SQLite como tablas consultables; admite rutas locales y almacenamiento de objetos remoto (`s3://`, `ftp://`, `sftp://`)
- **Integración con Kafka** — Los tópicos como tablas de solo lectura; los resultados de consulta como destinos (sinks) de Kafka
- **Disparadores programados** — Disparadores por cron e intervalo (APScheduler) que activan webhooks, mutaciones o publicaciones a destinos de Kafka
- **Sugerencias de rendimiento de federación** — Los comentarios SQL de enrutamiento anulan las decisiones de enrutamiento automáticas

![Orígenes de datos](docs/images/data-sources.png)

Los orígenes, archivos y endpoints remotos se registran como tablas gobernadas desde la interfaz:

![Registro de tablas](docs/images/table-registration.png)

### Seguridad y gobierno

- **Seguridad de nivel de fila** — Inyección de cláusula WHERE por tabla y por rol
- **Enmascaramiento de columnas** — Enmascaramiento por columna (regex, constante, truncado) con exclusión basada en rol
- **Preajustes de columna** — Valores estáticos o de variable de sesión inyectados del lado del servidor al insertar/actualizar; no expuestos en los tipos de entrada de mutación
- **Permisos de escritura** — Control de acceso de mutación por columna (`writable_by`)
- **Roles heredados** — Los roles heredan RLS, visibilidad y enmascaramiento de un rol padre de forma recursiva
- **Funciones y webhooks rastreados** — Funciones de base de datos y webhooks salientes expuestos como mutaciones GraphQL con formas de retorno tipadas
- **Hook de aprobación ABAC** — Hook de autorización previo a la ejecución; transporte webhook, gRPC o unix_socket; alcance por tabla, por origen o global; política de reserva configurable
- **Autenticación conectable** — Firebase, Keycloak, OAuth 2.0, simple (pruebas)

![Roles de seguridad](docs/images/security-roles.png)

### Entrega y rendimiento

- **Vistas materializadas como transformaciones registradas** — Una MV captura la transformación que la produjo: su forma de unión o SQL, las señales de entrada por origen (instantánea de Iceberg, marca de agua de RDB) a partir de las cuales se construyó, y una comprobación de determinismo en el registro. Como la transformación queda registrada, las consultas (o subexpresiones) se reescriben de forma transparente sobre una MV fresca — comparación estructural de patrones de unión con soporte de coincidencia parcial, de modo que una MV que cubre un subconjunto de uniones aún se aplica, preservando las uniones restantes
- **Inlining de tablas activas** — Las tablas de búsqueda pequeñas y frecuentemente unidas se insertan en línea como CTE VALUES directamente en el plan de consulta, eliminando los viajes de ida y vuelta entre orígenes para datos de dimensión
- **Caché de consultas** — Caché de resultados Redis particionada por rol+RLS; caché de hash APQ incluida
- **Observabilidad como datos** — Los trazos distribuidos, las métricas y los registros se recolectan mediante OpenTelemetry, se compactan en Iceberg sobre S3 y se registran automáticamente como tablas consultables (`traces`, `metrics`, `logs`, `queries`) en el esquema federado; consúltelos con SQL, GraphQL o Cypher junto a sus datos de negocio — una tabla `customers` con la tabla `queries` para ver quién ejecutó qué y cuánto tardó

### Administración e integración

- **API de administración** — GraphQL en `/admin/graphql`; carga/descarga de configuración, edición de relaciones, aprobación de consultas
- **Visor de informes** — `/admin/reports` lista las vistas de gestión del dominio de operaciones integradas y cualquier informe personalizado registrado; requiere la capacidad `observability`
- **Vista previa de tablas** — cada tabla registrada tiene un visor de datos gobernado paginado del lado del servidor con filtros empujados hacia abajo, agrupación multinivel y exportación a CSV
- **GraphQL Voyager** — Visualización interactiva del esquema, alcanzada por rol, como diagrama entidad-relación
- **Descubrimiento de relaciones por LLM** — Sugerencias de claves foráneas candidatas impulsadas por Claude
- **Cliente Python** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → Tables de pyarrow, dialecto SQLAlchemy, soporte ADBC
- **Ingesta de datos** — Endpoints HTTP para insertar datos de eventos JSON en la plataforma
- **Importación de Hasura v2 / DDN** — Convierte metadatos de Hasura v2 o YAML de supergrafo DDN a configuración de Provisa
- **Federación Apollo** — Expone Provisa como un subgrafo de Apollo Federation v2

Esquema alcanzado por rol visualizado como diagrama entidad-relación (GraphQL Voyager):

![Esquema Voyager](docs/images/schema-voyager.png)

Las relaciones se registran, aprueban y aplican como las únicas rutas JOIN legales:

![Relaciones](docs/images/relationships.png)

## Modelo de seguridad

Aquí es donde "en la ruta que ya toma cada consulta" deja de ser un eslogan. Provisa aplica un modelo de seguridad multicapa en cada lenguaje de consulta (GraphQL, SQL, Cypher) y cada transporte (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). El gobierno se aplica de manera uniforme — no existe ninguna ruta de consulta que lo eluda. La cobertura es total por construcción, no por diligencia: añada un origen, una columna o una relación y cada capa se le aplica automáticamente, sin nada que recordar registrar.

Las capas se aplican en orden. Una solicitud debe superar cada capa antes de que se evalúe la siguiente.

### Capa 0 — Filtrado de introspección

El esquema y el catálogo presentados a un rol contienen solo las tablas de su lista `domain_access` y las columnas que superan las reglas `visible_to` por columna. Los objetos fuera del acceso de un rol son invisibles en el momento del descubrimiento — no pueden consultarse, autocompletarse ni inferirse como existentes. Esto se aplica al esquema GraphQL, al catálogo SQL y al navegador de esquema del editor de consultas.

### Capa 1 — Acceso público

Las tablas en dominios sin restricción `domain_access` son visibles para todas las identidades autenticadas sin configuración adicional. Fricción cero para datos genuinamente públicos.

### Capa 2 — Acceso por dominio

Cada rol lleva una lista `domain_access` de IDs de dominio. Una consulta que toca una tabla fuera de esos dominios se rechaza antes de la ejecución. Este es el límite de propiedad grueso — un rol de RR. HH. no puede alcanzar tablas de finanzas sin importar cómo esté escrito el SQL.

### Capa 3 — Seguridad de nivel de fila

Una vez confirmado el acceso al dominio, los predicados `WHERE` por tabla y por rol se inyectan en cada `SELECT` en tiempo de ejecución. Los predicados se evalúan contra los datos crudos. Un gerente regional que consulta una tabla de pedidos compartida ve solo las filas de su región incluso en un `SELECT *`.

### Capa 4 — Visibilidad y enmascaramiento de columnas

Las columnas con una lista `visible_to` que excluye al rol solicitante se eliminan de la salida de la consulta. Las columnas con una regla de enmascaramiento tienen sus valores reemplazados — redacción por regex, reemplazo por constante o truncado — antes de que los resultados salgan del servidor. El enmascaramiento se aplica en todos los lenguajes de consulta y formatos de salida.

### Capa 5 — Guardia de predicados

Las columnas enmascaradas se rechazan en las cláusulas `WHERE` y `HAVING`. Sin esto, quien llama podría inferir el valor sin enmascarar mediante búsqueda binaria en un filtro aunque la salida esté enmascarada. El rechazo se aplica en el momento del análisis de la consulta, antes de la ejecución.

### Gobierno de relaciones

Las condiciones JOIN en SQL deben coincidir con una relación registrada y aprobada entre tablas. Las uniones no aprobadas se rechazan. Cada relación lleva una razón y una descripción legibles por humanos — orientación tanto para usuarios como para agentes autónomos sobre por qué existe una ruta de recorrido. Esto es política de gobierno, no un límite de seguridad estricto: las Capas 2–5 se mantienen sin importar la estructura de unión, de modo que una elusión deliberada no expone datos a los que el rol no pudiera llegar mediante dos consultas separadas. Los intentos de elusión se registran y son auditables.

---

Estas capas se componen. Un rol con acceso a dominio, RLS y columnas enmascaradas tiene las cinco restricciones activas simultáneamente. Añadir un nuevo origen de datos, columna o relación no requiere actualizar cada regla — cada capa se configura de forma independiente y se aplica automáticamente a cualquier consulta que toque objetos gobernados.

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
2. Ejecute el instalador — no requiere derechos de administrador
3. Abra **Provisa First Launch** desde el Menú Inicio — completa una configuración única (~5 min, sin necesidad de internet)
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

Descargue [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (siempre la última versión) y añádalo a la ruta de drivers de su herramienta de BI.

```text
jdbc:provisa://localhost:8815
```

Autentíquese con su nombre de usuario y contraseña de Provisa — el servidor asigna su rol.

- **Modo `catalog`** — esquema completo visible; para usar con herramientas de catálogo (Collibra, Atlan, DBeaver)

Consulte [docs/integrations.md](docs/integrations.md) para los pasos de configuración de Tableau y Power BI.

### Protocolo de cable de PostgreSQL (pgwire)

Provisa habla el protocolo de cable de PostgreSQL en el puerto 5439. Cualquier cliente que pueda conectarse a Postgres se conecta a Provisa — sin driver, sin adaptador, sin cambios en las herramientas existentes.

**El nombre de usuario de PostgreSQL selecciona el rol de Provisa.** Con `provider: none` (modo de confianza), la contraseña se ignora y cualquier nombre de rol configurado se acepta como nombre de usuario — conéctese como `analyst`, `admin` o cualquier rol para ver la vista gobernada de ese rol sobre los datos. Con `provider: simple`, la contraseña se valida con bcrypt. Otros proveedores (`firebase`, `keycloak`, `oauth`) no son compatibles sobre pgwire.

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

Todas las consultas se ejecutan a través del pipeline de gobierno completo — el acceso por dominio, RLS, el enmascaramiento y la guardia de predicados se aplican exactamente igual que para GraphQL y REST. Los navegadores de esquema (DBeaver, DataGrip, pgAdmin) funcionan de inmediato: las consultas a `pg_catalog` e `information_schema` se responden desde un catálogo en memoria delimitado al acceso por dominio del rol, de modo que los usuarios ven solo las tablas y columnas que tienen permitido consultar.

DataGrip navegando el esquema gobernado y su diagrama de claves foráneas sobre pgwire — sin driver, sin adaptador:

![Provisa en DataGrip sobre pgwire](docs/images/pgwire-datagrip.png)

TLS se habilita configurando `PROVISA_PGWIRE_CERT` y `PROVISA_PGWIRE_KEY`. El puerto es configurable mediante `PROVISA_PGWIRE_PORT` (por defecto `5439`).

### Bolt (protocolo de cable de Neo4j)

Provisa también habla el protocolo **Bolt** de Neo4j, de modo que las herramientas nativas de grafo se conectan directamente y ejecutan Cypher contra el grafo federado — sin exportación, sin base de datos de grafo separada. Apunte **Neo4j Browser** o **Bloom** a Provisa y recorra relaciones entre orígenes con el mismo gobierno (acceso por dominio, RLS, enmascaramiento) aplicado.

Neo4j Browser ejecutando Cypher contra Provisa — las etiquetas de nodo, los tipos de relación y las claves de propiedad provienen directamente del esquema registrado:

![Provisa en Neo4j Browser sobre Bolt](docs/images/bolt-neo4j-browser.png)

Actívelo configurando `PROVISA_BOLT_PORT` (el valor por defecto de Neo4j es `7687`). TLS se habilita con `PROVISA_BOLT_CERT` y `PROVISA_BOLT_KEY`. Cada rol de Provisa que posee el usuario autenticado se muestra como una base de datos `provisa_<role>` seleccionable (el selector `provisa_admin` arriba) — elegir una limita la sesión a los derechos de dominio de ese rol; el usuario nunca puede exceder los roles que posee.

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
| Guía de inicio rápido para desarrolladores (ejecución desde el código fuente) | [docs/quickstart.md](docs/quickstart.md) |
| Referencia completa de configuración YAML | [docs/configuration.md](docs/configuration.md) |
| Referencia de endpoints (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Diseño del sistema y mapa de componentes | [docs/architecture.md](docs/architecture.md) |
| Modelo de seguridad (RLS, enmascaramiento, autenticación) | [docs/security.md](docs/security.md) |
| Almacenamiento de secretos y referencias `${secret:NAME}` | [docs/secrets.md](docs/secrets.md) |
| Glosario de negocio y curación de términos | [docs/glossary.md](docs/glossary.md) |
| Entornos (dev / staging / prod) | [docs/environments.md](docs/environments.md) |
| Tipos de origen soportados | [docs/sources.md](docs/sources.md) |
| Suscripciones SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, herramientas de BI, clientes Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Cliente Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API de administración | [docs/admin.md](docs/admin.md) |
| Despliegue (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Importación de Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Flujo de trabajo de publicación (etiquetas alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionamiento

Provisa incluye un motor de federación integrado para consultas multiorigen. En el primer inicio elige un presupuesto de RAM; Provisa deriva automáticamente el número de workers de federación locales.

| RAM del host | Workers | Carga de trabajo típica |
| --- | --- | --- |
| < 24 GB | 0 | Desarrollo, consultas de un solo origen, equipos pequeños |
| 24–47 GB | 1 | Equipo pequeño, consultas moderadas entre orígenes |
| 48–95 GB | 2 | Despliegue departamental, uso mixto de BI + notebook |
| 96 GB+ | 4 | Departamento grande, federación concurrente intensiva |

El número de workers puede cambiarse en cualquier momento editando `~/.provisa/config.yaml` (`federation_workers: N`) y ejecutando `provisa restart`. Establézcalo en `0` para ejecutar solo la coordinación (nodo único).

### Escalado más allá de una sola máquina

**Escalado horizontal** — Ejecute múltiples instancias de Provisa detrás de un balanceador de carga. Cada instancia es un sistema completamente funcional. Todas las instancias deben apuntar a la misma BD de configuración (configure `CONFIG_DB_HOST` en las máquinas secundarias) y opcionalmente a una instancia de Redis compartida (`REDIS_URL`) para una caché unificada. La mayoría de las consultas se distribuyen de forma transparente; las uniones entre orígenes muy grandes pueden exceder los recursos de una sola instancia y requerir una máquina más grande o un clúster de federación externo.

**Redis compartido** — Configure `REDIS_URL` en cada instancia para apuntar a un Redis externo. Redis compartido significa que las entradas de caché de una instancia están disponibles para todas, mejorando las tasas de acierto en todo el clúster.

**Traiga su propio clúster de federación** — Apunte Provisa a un clúster de federación externo existente en lugar de a los workers integrados. Recomendado para despliegues a gran escala o en la nube; consulte [docs/deployment.md](docs/deployment.md) para la configuración.

## Licencia

Business Source License 1.1 (sin modificar, según los pactos de Licenciante de MariaDB). Cada
versión publicada se convierte a la Licencia de Cambio (GPL v2.0 o posterior) en el 4.º
aniversario de su publicación pública; el código actual y reciente permanece bajo BSL.
El uso en producción por encima de los umbrales de la Concesión de Uso Adicional (menos de 100
empleados/contratistas y menos de $1M en ingresos del año anterior) requiere una licencia
comercial. Consulte [LICENSE](LICENSE).

El Licenciante no consiente el uso de esta obra para entrenamiento de IA/ML. Consulte
[NOTICE](NOTICE), [ai.txt](ai.txt) y [robots.txt](robots.txt). Para licencias comerciales
o de entrenamiento de IA: <kennethstott@gmail.com>
