# Provisa

**Conecte sus bases de datos. Consulte con GraphQL, gRPC, SQL o MCP — sobre cualquier API o protocolo — en 5 minutos.**

Provisa pone una sola API sobre todos sus datos. Apúntelo a sus bases de datos y aplicaciones de negocio, describa cómo encajan y consulte el resultado unido por REST, GraphQL, SQL, gRPC, MCP y más.

Funciona porque todo sale de un único modelo: sus dominios, las relaciones entre sus tablas y las políticas sobre ellas, en todas las fuentes. Provisa se ejecuta a partir de ese modelo y gobierna a partir de él, así que no hay nada que mantener sincronizado. Las relaciones registradas son los únicos joins que puede hacer una consulta, y las políticas de acceso se compilan en cada plan de consulta. Del único modelo se derivan cuatro cosas:

- **Federación.** 73 conectores llegan a más de 85 tipos de fuente, entre ellos Salesforce, ServiceNow y Stripe. Los sistemas SaaS con los que funciona su negocio se leen como tablas junto a sus bases de datos, en su sitio, sin una canalización previa hacia un almacén. Consúltelos en SQL, GraphQL o Cypher, por nueve protocolos.
- **Métricas.** Una métrica es un agregado con nombre y gobernado. Se define una vez, y cada herramienta y lenguaje que la pide recibe la misma definición.
- **Ontología.** El glosario de negocio es un grafo de conceptos, sus definiciones y cómo se relacionan. Provisa lo asigna a las tablas y columnas que contienen los valores, y eso le permite responder preguntas sobre sus datos. Un concepto sigue siendo un borrador hasta que llega a los datos, directamente o a través de otros conceptos. El glosario dice qué es un concepto; una métrica dice cómo se calcula un número.
- **Gobernanza.** Seis capas de comprobaciones se compilan en cada plan de consulta, en cada lenguaje y por cada protocolo, y cada petición se registra igual: quién consultó qué, con qué rol, bajo qué política. Las trazas, métricas y registros están registrados como tablas consultables junto a sus datos de negocio.

Puede preguntar en **GraphQL, Cypher o SQL** y conectarse por **pgwire, Bolt, gRPC, REST, Arrow Flight o JDBC**. Cada lenguaje de consulta se reduce a una única representación intermedia, y la gobernanza se aplica ahí, una sola vez, para que una política no pueda divergir entre lenguajes. A la salida, esa forma se traduce al dialecto propio de cada fuente. Un lenguaje de consulta nuevo es una interfaz nueva sobre el mismo núcleo.

Lecturas y escrituras pasan por él. Una consulta entre fuentes se reparte por la capa de federación. Las escrituras, y las lecturas de una sola fuente, van directas al controlador de esa fuente, gobernadas igual y normalmente de vuelta en menos de 100 ms. El streaming columnar de Arrow Flight viene incluido.

El modelo tiene un vocabulario pequeño: dominios, relaciones, roles y políticas. Eso lo hace fácil de leer y de auditar. Puede leer el conjunto de políticas y saber qué hace. Provisa es un compilador de consultas ligero, no un entorno de ejecución que se interpone en su camino de datos. Convierte una petición en consultas nativas, las enruta y se aparta, y por eso el conjunto de datos sigue siendo rápido.

Puede usarlo de dos maneras, y se combinan:

- **Para modernizar.** Modele su conjunto de datos, deje que Provisa genere el SQL nativo de cada fuente y luego tome ese SQL y adóptelo en el sistema de destino. Provisa es el puente para la transición, y puede prescindir de él después.
- **Para gobernar de forma permanente.** Manténgalo como el camino que sigue cada consulta, para que definición, aplicación y auditoría sigan juntas mientras exista el conjunto de datos.

## Probar una demo {: #try-a-demo }

Lo más rápido es una demo en vivo en el navegador, sin instalar nada: [provisa.dev/demos](https://provisa.dev/demos). Cada una abre un sandbox privado con una visita guiada.

Desde una copia de este repositorio puede ejecutar cualquier demo en su propia máquina:

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

Una demo publicada se descarga como un paquete listo; las demás se ejecutan desde `demo/named/<name>/`. Consulte [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) para ver todas las opciones.

## El modelo de federación {: #the-federation-model }

Cuatro ideas sostienen todo el diseño. Cada fuente se reduce a tablas. Cada consulta se reduce a una forma similar a SQL. La alcanzabilidad decide qué se consulta en vivo y qué se copia primero. Y una estrategia de frescura mantiene cada copia, y cada conjunto de datos derivado, tan actual como haga falta.

Empecemos por las fuentes. Cada una se expresa como un conjunto de tablas bidimensionales sobre un único sistema de tipos compartido, y ese es el único contrato que una fuente debe cumplir para entrar en el conjunto. Una tabla de MySQL o PostgreSQL ya lo es. El resultado de una consulta GraphQL lo es en cuanto se aplana. Los almacenes de tripletas SPARQL y Neo4j no se parecen en nada a una tabla, pero usted aporta una consulta con resultado tabular y la consulta se convierte en el adaptador. Sea cual sea la fuente, el conjunto ve filas, columnas y tipos. Incorporar un tipo nuevo de fuente es cumplir ese único contrato, a veces con algo de ayuda humana, en lugar de escribir una integración a medida.

Las consultas reciben el mismo trato. SQL, con todos sus dialectos, es el lenguaje del análisis sobre tablas, y eso hace de una forma similar a SQL el destino común natural. Así que cada petición, llegue en el lenguaje que llegue, se reduce primero a esa forma. SQL y GraphQL se reducen con facilidad. Los caminos y la semántica de grafos de Cypher cuestan trabajo de verdad, pero llegan. Como todo pasa por una forma antes de que ocurra nada más, la gobernanza se aplica en un solo sitio, sea cual sea el lenguaje de la petición.

Con fuentes tabulares y una sola forma de consulta, la federación puede cubrir tanto las consultas en vivo como el almacenamiento: lo que hace un motor de consultas en vivo como Trino, más la materialización en la que se apoyan esos motores. La **alcanzabilidad** une ambas cosas. ¿Puede el motor consultar una fuente donde está, o hay que copiar antes sus datos a un sitio consultable? La respuesta divide el conjunto en lo que se lee en vivo y lo que se copia.

La mayoría de las bases de datos ya tienen algún tipo de enlace en vivo: `ATTACH` de DuckDB, `postgres_fdw` de PostgreSQL, los enlaces externos de Databricks. Así que la mayoría puede actuar como motor de federación hasta cierto punto. Ninguna lo cubre todo. Cada una llega a un conjunto concreto de fuentes y copia el resto, y ninguna dice con claridad qué es cada cosa. El modelo hace explícita la alcanzabilidad, como un conjunto definido de métodos por fuente que dicen qué alcanza el motor en vivo y, por descarte, qué hay que copiar.

Queda la frescura. Para cada fuente que el motor no alcanza, ¿qué tan actual debe ser su copia? En la práctica hay pocas respuestas: a demanda, según una programación, ante una señal de cambio (CDC, una marca de agua, una instantánea) o fijada. Elegir una por fuente es toda la política de frescura.

Los conjuntos de datos derivados encajan en la misma forma: tablas derivadas, agregados, la salida de una transformación. Se escriben en la misma forma, así que el linaje viene de serie. El camino desde cada sistema de origen hasta una salida final es la definición que la produjo, legible de principio a fin. Construirlos plantea la pregunta de frescura un paso más allá: ¿se actualiza el conjunto según una programación, solo cuando se cumplen sus condiciones previas, de forma continua o como una instantánea histórica fijada? Las opciones son el mismo pequeño conjunto que para la copia de una fuente, así que la política de construcción de un conjunto derivado usa el mismo vocabulario.

Los modelos dimensionales se derivan directamente. Los hechos y dimensiones de un esquema en estrella son conjuntos derivados como cualquier otro. Una dimensión es una proyección conformada y deduplicada, y una tabla de hechos es un join con agregado reducido a un grano, cada una con su propia política de construcción y de frescura. Las dimensiones de cambio lento no necesitan nada especial: una instantánea fijada es historia de Tipo 2 y una reconstrucción programada es de Tipo 1. Como el esquema se define en el modelo en lugar de atarse a las tablas de un almacén, los mismos hechos y dimensiones pueden construirse en Oracle, en Databricks o quedarse virtuales sobre un motor MPP, sin remodelar.

Data Vault encaja igual, una capa antes. Los hubs son conjuntos deduplicados de claves de negocio, los links son las relaciones registradas entre ellos y los satélites son conjuntos de atributos de solo inserción, con marca de tiempo, que guardan la historia. Un satélite es sencillamente un conjunto derivado con la estrategia de señal de cambio: fecha de carga más hashdiff es captura de cambios aplicada a atributos descriptivos, y la historia de solo inserción es la estrategia de instantánea fijada. Las tablas point-in-time y bridge son más conjuntos derivados, construidos para la velocidad de consulta. Así, una raw vault es un conjunto de datos derivados en el modelo y un esquema en estrella es una proyección de ella, ambos generados y ambos portables entre motores. La metodología la sigue decidiendo usted: qué se convierte en hub, el grano de un satélite, cómo dividir. Una vez decidido, vive en el modelo y no en un ETL soldado a un almacén.

Ambos patrones se declaran con **dos atajos** en lugar de vistas escritas a mano. Son las piezas con las que se construyen todos los esquemas en estrella y todas las Data Vault, y no le atan a ninguno de los dos métodos:

- **`entity`** es una proyección de una fuente con clave y deduplicada que puede guardar historia. Declare la clave, los atributos y un modo de historia, y Provisa construye una vista materializada. Si pide historia, construye en su lugar una **MV bitemporal** (`scd2` → delta, `snapshot` → snapshot). Una sola construcción le da una **dimensión** de Kimball (SCD1 o SCD2) o un **hub con satélite** de Data Vault.
- **`fact`** se une a claves de entidad, reduce a un grano declarado y agrega medidas. Provisa construye una MV de agregado más las relaciones registradas con las entidades. Una sola construcción le da una **tabla de hechos** en estrella o un **link** de Data Vault; un hecho sin medidas es un link de conjunto de claves puro.

Cada `entity` o `fact` se convierte exactamente en las vistas materializadas, los ajustes bitemporales y las relaciones que un modelador escribiría a mano, de modo que el almacén se queda en el modelo y pasa de un motor a otro sin remodelar. Declare uno en la interfaz de administración (el formulario **Model**) o por la API de administración (`registerEntity` / `registerFact`). El modelo le genera una estrella de Kimball o una Data Vault; no impone ninguna.

### Viaje en el tiempo {: #time-travel }

Viajar en el tiempo significa guardar cada versión de una fila en lugar de sobrescribirla, para poder preguntar cómo eran los datos en cualquier momento pasado. Los motores difieren mucho en la eficiencia con que pueden hacerlo, así que Provisa lo convierte en parte de la **definición** de la vista materializada y no del motor de almacenamiento. Se declara una vez y funciona en cualquier almacén que pueda guardar una vista materializada.

Lo que lo mantiene portable es que es **de solo anexado**. Una versión, una vez escrita, nunca se actualiza ni se borra. El truco bitemporal habitual de cerrar una fila escribiendo una fecha «válido hasta» necesita un UPDATE, que muchos motores no pueden hacer de forma barata, o en absoluto, sobre un almacén federado. Por eso cada actualización **anexa**, y qué versión estaba vigente en un momento dado se calcula al leer, a partir del registro. Hay dos formas de anexar:

- **Snapshot** anexa todo el conjunto recién obtenido, marcado con la hora de sistema de la actualización. No hay comparación de diferencias y es correcto en todos los motores, pero el almacenamiento crece una copia completa por actualización.
- **Delta** anexa solo lo que cambió, más lápidas para las claves eliminadas. El propio motor calcula el delta, con anti-joins dentro de un `INSERT … SELECT`, en lugar de que Provisa lo combine fila a fila. Es más pequeño y necesita una clave de entidad.

Provisa gestiona así el tiempo de sistema, cuándo registró una versión. El tiempo de validez, cuándo un hecho es cierto en el negocio, viene del propio SELECT de la vista y se conserva. Los motores que pueden hacerlo mejor, como las instantáneas nativas de Iceberg o un MERGE que toca menos filas, pueden usarse tras la misma declaración; el camino de solo anexado es el mínimo que es correcto en todas partes.

La lectura es transparente. Una consulta simple sobre una MV bitemporal devuelve el estado **actual**, reconstruido a partir del registro. Para volver atrás en el tiempo, envíe una cabecera `X-Provisa-As-Of: <timestamp>` y toda la consulta se responde tal como estaba el conjunto en ese momento, igual en todos los almacenes. Añada `X-Provisa-As-Recorded` para leer los datos tal como Provisa los tenía registrados en un momento, de modo que una corrección registrada después no cambie la reproducción del pasado. Actívelo para cualquier vista materializada en la interfaz de administración (el control **Time Travel**: desactivado, snapshot o delta, más una clave de entidad) o por la API de administración.

La alcanzabilidad y la frescura juntas forman un modelo general de federación: una definición de qué es en vivo, qué se copia y qué tan actual se mantiene cada copia, independiente de lo que alcance cualquier motor. El modelo es portable, así que su conjunto de datos no queda atado al proveedor cuya federación alcance hoy más fuentes.

## Funciones {: #features }

### Interfaces de consulta {: #query-interfaces }

Estos son los lenguajes y las API estructuradas en los que escribe consultas. Cada uno tiene su propia sintaxis y semántica; el gobierno (RLS, enmascaramiento, visibilidad de columnas, aplicación de relaciones) se aplica de manera uniforme en todos ellos, sin importar qué protocolo de transporte los entregue.

- **GraphQL** — Esquemas por rol con visibilidad a nivel de campo, filtrado, paginación basada en cursor y consultas agregadas (`count`, `sum`, `avg`, `min`, `max`). Restringido por esquema a relaciones registradas — estructuralmente válido por construcción, la ruta más rápida hacia una consulta simple correcta. Apollo APQ incluido: las consultas se hashean y se registran del lado del servidor; las llamadas posteriores envían solo el hash sobre HTTP GET, haciendo las respuestas cacheables por CDN sin cambios requeridos en el cliente. Las tablas de búsqueda por debajo de un umbral de filas configurable se exponen como tipos enum.
- **SQL** — SQL completo sobre datos federados; sin restricciones y más expresivo que GraphQL. Escriba SQL estándar — subconsultas correlacionadas incluidas — y se ejecuta entre orígenes sin cambios. Las consultas de un solo origen omiten la capa de federación por completo (menos de 100 ms).
- **Cypher** — Lenguaje de consulta de grafos sobre el mismo esquema federado. Recorra relaciones como aristas de grafo; una orígenes; rutas de longitud variable. El gobierno se aplica de forma idéntica a GraphQL y SQL.
- **API de modelo gRPC** — `.proto` autogenerado a partir del esquema registrado; RPC tipados de consulta e inserción para cada tabla en la raíz del esquema de un rol, respuestas en streaming. Impulsado por esquema en el mismo sentido que GraphQL — el modelo de registro es el contrato, protobuf es la codificación de transporte. A diferencia de Arrow Flight (que es un transporte de streaming columnar), esta es una interfaz de consulta completa por tabla.
- **JSON:API** — API de consulta estructurada en `/data/jsonapi/{table}`, exclusiva de HTTP por diseño. Soporta JSON:API 1.1: conjuntos de campos dispersos (`fields[table]=col1,col2`), expresiones de filtro (`filter[field][op]=value`), documentos compuestos (`include=relation`) y ordenamiento. No es un lenguaje de consulta de propósito general — consulta una tabla a la vez con sintaxis de filtro estandarizada en lugar de una cadena de consulta ad hoc.
- **Explorador de lenguaje de consulta** — Escriba una consulta GraphQL y vea traducciones en vivo de **SQL semántico** y **Cypher** en paneles laterales; copie cualquiera de las dos o salte directamente al editor de SQL o de Grafo. Un flujo de trabajo práctico es esbozar fragmentos de consulta en GraphQL y luego integrar el SQL resultante en vistas o informes complejos.

El Explorador muestra una consulta GraphQL junto a sus traducciones en vivo a SQL y Cypher:

![Explorador de lenguaje de consulta](docs/images/query-explorer.png)

El mismo esquema federado es explorable como un grafo en vivo — etiquetas de dominio y nodo, tipos de relación y recorridos de longitud variable:

![Visualización de grafo](docs/images/graph-view.png)

### Herramientas de composición de consultas {: #query-composition-tools }

Estas herramientas le ayudan a escribir consultas en los lenguajes anteriores — no son lenguajes de consulta en sí mismas.

- **Consulta en lenguaje natural** — Pipeline NL→SQL/Cypher/GraphQL impulsado por Claude. Describa lo que desea en inglés sencillo; el pipeline produce una consulta en el lenguaje elegido con un bucle de validación interactivo antes de la ejecución.

![Consulta en lenguaje natural](docs/images/natural-language.png)

### Protocolos de transporte {: #wire-protocols }

Estos son los protocolos de conexión. SQL, GraphQL y Cypher viajan sobre ellos — la elección de protocolo de transporte no cambia la interfaz de consulta ni el comportamiento de gobierno.

- **pgwire** — Cualquier cliente de PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` de pandas) se conecta en el puerto 5439 como si fuera un servidor Postgres. Acepta solo SQL. Se aplica el pipeline de gobierno completo. `pg_catalog` e `information_schema` se responden desde un catálogo en memoria para que los navegadores de esquema funcionen sin un viaje de ida y vuelta de federación. TLS opcional.
- **Bolt (Neo4j)** — Cualquier cliente de Neo4j (Neo4j Browser, Bloom, drivers oficiales) se conecta mediante el protocolo Bolt y ejecuta Cypher contra el grafo federado. Cada rol que posee el usuario se muestra como una base de datos `provisa_<role>`. Mismo gobierno que cualquier otro transporte. TLS opcional.
- **Arrow Flight** — Transmisión columnar de alto rendimiento sobre gRPC; acepta GraphQL o SQL como entrada de consulta. Conjuntos de resultados no acotados, sin materialización del lado del servidor, sin infraestructura separada requerida.
- **JDBC** — Integración con herramientas de BI (Tableau, Power BI, DBeaver).
- **WebSocket / SSE** — Suscripciones: eventos de cambio casi en tiempo real; backends: PG nativo, MongoDB nativo, CDC, sondeo. También expuesto sobre Kafka.

### Orígenes de datos {: #data-sources }

- **73 conectores, más de 85 tipos de fuente** — La cifra mayor cuenta por separado los servicios de PostgreSQL alojados (RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon) y los formatos de archivo del File Crawler. PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, almacenes de triples SPARQL, Kafka, Google Sheets, Kaggle y más a través de una única API; los orígenes de grafo y RDF son de primera clase, no adaptadores
- **Enrutamiento inteligente** — Las consultas de un solo origen omiten la federación (menos de 100 ms); las consultas multiorigen se enrutan a través de la capa de federación — traiga su propio clúster o use los workers integrados
- **Orígenes de API** — Registre endpoints REST, GraphQL, gRPC, WebSocket o RSS como tablas consultables; ayudantes de SPARQL incluidos; las uniones federadas entre orígenes de API y orígenes relacionales funcionan de manera transparente
- **Introspección de esquemas remotos** — Apunte a cualquier endpoint GraphQL, OpenAPI o gRPC; las operaciones documentadas se exponen automáticamente como tablas consultables, nodos de grafo y aristas con el gobierno completo aplicado encima
- **Orígenes de archivo** — Archivos CSV, Parquet y SQLite como tablas consultables; admite rutas locales y almacenamiento de objetos remoto (`s3://`, `ftp://`, `sftp://`)
- **Conjuntos de datos de Kaggle** — busca el catálogo público de Kaggle y registra un conjunto de datos mediante búsqueda en vivo autenticada por token, sin descarga manual; se registra como un único origen de archivo cuyas tablas se descubren automáticamente, solo paquetes CSV/Parquet
- **Integración con Kafka** — Los tópicos como tablas de solo lectura; los resultados de consulta como destinos (sinks) de Kafka
- **Disparadores programados** — Disparadores por cron e intervalo (APScheduler) que activan webhooks, mutaciones o publicaciones a destinos de Kafka
- **Sugerencias de rendimiento de federación** — Los comentarios SQL de enrutamiento anulan las decisiones de enrutamiento automáticas

![Orígenes de datos](docs/images/data-sources.png)

Los orígenes, archivos y endpoints remotos se registran como tablas gobernadas desde la interfaz:

![Registro de tablas](docs/images/table-registration.png)

### Seguridad y gobierno {: #security-governance }

- **Seguridad de nivel de fila** — Inyección de cláusula WHERE por tabla y por rol
- **Enmascaramiento de columnas** — Enmascaramiento por columna (regex, constante, truncado) con exclusión basada en rol
- **Preajustes de columna** — Valores estáticos o de variable de sesión inyectados del lado del servidor al insertar/actualizar; no expuestos en los tipos de entrada de mutación
- **Permisos de escritura** — Control de acceso de mutación por columna (`writable_by`)
- **Roles heredados** — Los roles heredan RLS, visibilidad y enmascaramiento de un rol padre de forma recursiva
- **Funciones y webhooks rastreados** — Funciones de base de datos y webhooks salientes expuestos como mutaciones GraphQL con formas de retorno tipadas
- **Hook de aprobación ABAC** — Hook de autorización previo a la ejecución; transporte webhook, gRPC o unix_socket; alcance por tabla, por origen o global; política de reserva configurable
- **Autenticación conectable** — Firebase, Keycloak, OAuth 2.0, simple (pruebas)

![Roles de seguridad](docs/images/security-roles.png)

### Entrega y rendimiento {: #delivery-performance }

- **Vistas materializadas como transformaciones registradas** — Una MV captura la transformación que la produjo: su forma de unión o SQL, las señales de entrada por origen (instantánea de Iceberg, marca de agua de RDB) a partir de las cuales se construyó, y una comprobación de determinismo en el registro. Como la transformación queda registrada, las consultas (o subexpresiones) se reescriben de forma transparente sobre una MV fresca — comparación estructural de patrones de unión con soporte de coincidencia parcial, de modo que una MV que cubre un subconjunto de uniones aún se aplica, preservando las uniones restantes
- **Inlining de tablas activas** — Las tablas de búsqueda pequeñas y frecuentemente unidas se insertan en línea como CTE VALUES directamente en el plan de consulta, eliminando los viajes de ida y vuelta entre orígenes para datos de dimensión
- **Caché de consultas** — Caché de resultados Redis particionada por rol+RLS; caché de hash APQ incluida
- **Observabilidad como datos** — Los trazos distribuidos, las métricas y los registros se recolectan mediante OpenTelemetry, se compactan en Iceberg sobre S3 y se registran automáticamente como tablas consultables (`traces`, `metrics`, `logs`, `queries`) en el esquema federado; consúltelos con SQL, GraphQL o Cypher junto a sus datos de negocio — una tabla `customers` con la tabla `queries` para ver quién ejecutó qué y cuánto tardó

### Administración e integración {: #administration-integration }

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

## Modelo de seguridad {: #security-model }

Aquí es donde "en el camino que ya recorre toda consulta" tiene que sostenerse. Provisa aplica un único modelo de seguridad por capas a todo lenguaje de consulta (GraphQL, SQL, Cypher) y a todo transporte (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket), y ningún camino de consulta lo esquiva. La cobertura sale del diseño, así que nadie tiene que acordarse de nada: añada un origen, una columna o una relación, y cada capa se le aplica de inmediato, sin nada más que registrar.

Las capas se aplican en orden, y una petición tiene que superar cada una antes de que se compruebe la siguiente.

### Capa 0 — Filtrado de introspección {: #layer-0-introspection-filtering }

El esquema y el catálogo que ve un rol contienen solo las tablas de su lista `domain_access` y las columnas que superan sus reglas `visible_to`. Todo lo que queda fuera del acceso del rol es invisible desde el principio: no se puede consultar, ni autocompletar, ni siquiera deducir que existe. Esto cubre el esquema GraphQL, el catálogo SQL y el explorador de esquema del editor de consultas.

### Capa 1 — Acceso público {: #layer-1-public-access }

Las tablas de dominios sin restricción `domain_access` son visibles para toda identidad autenticada, sin nada más que configurar. Los datos que de verdad son públicos no cuestan esfuerzo.

### Capa 2 — Acceso por dominio {: #layer-2-domain-access }

Cada rol lleva una lista `domain_access` de ID de dominio, y una consulta que toca una tabla fuera de esos dominios se rechaza antes de ejecutarse. Es la frontera amplia de propiedad: un rol de RR. HH. no llega a las tablas de finanzas, se escriba como se escriba el SQL.

### Capa 3 — Seguridad de nivel de fila {: #layer-3-row-level-security }

Una vez confirmado el acceso al dominio, se añaden predicados `WHERE` por tabla y por rol a cada `SELECT` en el momento de la ejecución, y se evalúan sobre los datos sin procesar. Una gerente regional que consulta una tabla de pedidos compartida solo ve las filas de su región, incluso con `SELECT *`.

### Capa 4 — Visibilidad y enmascaramiento de columnas {: #layer-4-column-visibility-and-masking }

Las columnas cuya lista `visible_to` no incluye al rol que pregunta se quitan de la salida. Las columnas con una regla de enmascaramiento ven sus valores sustituidos por redacción con regex, una constante o un truncado antes de que los resultados salgan del servidor. El enmascaramiento se aplica en todo lenguaje de consulta y todo formato de salida.

### Capa 5 — Guardia de predicados {: #layer-5-predicate-guard }

Las columnas enmascaradas se rechazan en las cláusulas `WHERE` y `HAVING`. Si no, quien consulta podría averiguar el valor real buscándolo por bisección en un filtro, aunque la salida esté enmascarada. El rechazo ocurre al analizar la consulta, antes de que se ejecute nada.

### Gobierno de relaciones {: #relationship-governance }

En SQL, una condición JOIN tiene que coincidir con una relación registrada y aprobada entre las tablas, y un join no aprobado se rechaza. Cada relación lleva un motivo y una descripción legibles, que explican a personas y a agentes autónomos por qué existe ese camino. Es una política de gobernanza y no una frontera de seguridad estricta. Las capas 2 a 5 se mantienen sea cual sea la forma del join, así que un rodeo deliberado no expone nada que el rol no pudiera alcanzar con dos consultas separadas, y cada intento queda registrado para auditoría.

Ningún join inválido se ejecuta nunca sobre sus datos. Los roles están gobernados por defecto, y salir de ello significa conceder a propósito el derecho `ignore_relationships` a un rol de confianza. El token OAuth de un agente corresponde a un rol, así que un agente recibe las mismas capas que una persona. Los catálogos de datos con una capa de contexto le dicen a un agente lo que debería hacer; Provisa impone lo que puede hacer.

---

Estas capas se combinan. Un rol con acceso a dominios, RLS y columnas enmascaradas tiene todas las capas activas a la vez. Añadir un origen, una columna o una relación no obliga a actualizar cada regla, porque cada capa se configura por separado y se aplica sola a toda consulta que toca objetos gobernados.

## Inicio rápido {: #quick-start }

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

### Primera consulta {: #first-query }

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

Consulte [docs/integrations.md](docs/integrations.md) para los pasos de configuración de Tableau y Power BI.

### Protocolo de cable de PostgreSQL (pgwire) {: #postgresql-wire-protocol-pgwire }

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

### Bolt (protocolo de cable de Neo4j) {: #bolt-neo4j-wire-protocol }

Provisa también habla el protocolo **Bolt** de Neo4j, de modo que las herramientas nativas de grafo se conectan directamente y ejecutan Cypher contra el grafo federado — sin exportación, sin base de datos de grafo separada. Apunte **Neo4j Browser** o **Bloom** a Provisa y recorra relaciones entre orígenes con el mismo gobierno (acceso por dominio, RLS, enmascaramiento) aplicado.

Neo4j Browser ejecutando Cypher contra Provisa — las etiquetas de nodo, los tipos de relación y las claves de propiedad provienen directamente del esquema registrado:

![Provisa en Neo4j Browser sobre Bolt](docs/images/bolt-neo4j-browser.png)

Actívelo configurando `PROVISA_BOLT_PORT` (el valor por defecto de Neo4j es `7687`). TLS se habilita con `PROVISA_BOLT_CERT` y `PROVISA_BOLT_KEY`. Cada rol de Provisa que posee el usuario autenticado se muestra como una base de datos `provisa_<role>` seleccionable (el selector `provisa_admin` arriba) — elegir una limita la sesión a los derechos de dominio de ese rol; el usuario nunca puede exceder los roles que posee.

### Cliente Python {: #python-client }

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

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Consulte [docs/python-client.md](docs/python-client.md) para la referencia completa.

## Documentación {: #documentation }

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
| Crear entornos de prueba (perfilado, datos falsos, datos sintéticos) | [docs/test-data.md](docs/test-data.md) |
| Referencia de métodos de datos falsos | [docs/fake-methods.md](docs/fake-methods.md) |
| Ejecutar y crear demos en un portátil (ingenieros de ventas) | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| Tipos de origen soportados | [docs/sources.md](docs/sources.md) |
| Suscripciones SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, herramientas de BI, clientes Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Cliente Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API de administración | [docs/admin.md](docs/admin.md) |
| Despliegue (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Importación de Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Flujo de trabajo de publicación (etiquetas alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionamiento {: #sizing }

Provisa incluye un motor de federación integrado para consultas multiorigen. En el primer inicio elige un presupuesto de RAM; Provisa deriva automáticamente el número de workers de federación locales.

| RAM del host | Workers | Carga de trabajo típica |
| --- | --- | --- |
| < 24 GB | 0 | Desarrollo, consultas de un solo origen, equipos pequeños |
| 24–47 GB | 1 | Equipo pequeño, consultas moderadas entre orígenes |
| 48–95 GB | 2 | Despliegue departamental, uso mixto de BI + notebook |
| 96 GB+ | 4 | Departamento grande, federación concurrente intensiva |

El número de workers puede cambiarse en cualquier momento editando `~/.provisa/config.yaml` (`federation_workers: N`) y ejecutando `provisa restart`. Establézcalo en `0` para ejecutar solo la coordinación (nodo único).

### Escalado más allá de una sola máquina {: #scaling-beyond-a-single-box }

**Escalado horizontal** — Ejecute múltiples instancias de Provisa detrás de un balanceador de carga. Cada instancia es un sistema completamente funcional. Todas las instancias deben apuntar a la misma BD de configuración (configure `CONFIG_DB_HOST` en las máquinas secundarias) y opcionalmente a una instancia de Redis compartida (`REDIS_URL`) para una caché unificada. La mayoría de las consultas se distribuyen de forma transparente; las uniones entre orígenes muy grandes pueden exceder los recursos de una sola instancia y requerir una máquina más grande o un clúster de federación externo.

**Redis compartido** — Configure `REDIS_URL` en cada instancia para apuntar a un Redis externo. Redis compartido significa que las entradas de caché de una instancia están disponibles para todas, mejorando las tasas de acierto en todo el clúster.

**Traiga su propio clúster de federación** — Apunte Provisa a un clúster de federación externo existente en lugar de a los workers integrados. Recomendado para despliegues a gran escala o en la nube; consulte [docs/deployment.md](docs/deployment.md) para la configuración.

## Licencia {: #license }

Business Source License 1.1 (sin modificar, según los pactos de Licenciante de MariaDB). Cada
versión publicada se convierte a la Licencia de Cambio (GPL v2.0 o posterior) en el 4.º
aniversario de su publicación pública; el código actual y reciente permanece bajo BSL.
El uso en producción por encima de los umbrales de la Concesión de Uso Adicional (menos de 100
empleados/contratistas y menos de $1M en ingresos del año anterior) requiere una licencia
comercial. Consulte [LICENSE](LICENSE).

El Licenciante no consiente el uso de esta obra para entrenamiento de IA/ML. Consulte
[NOTICE](NOTICE), [ai.txt](ai.txt) y [robots.txt](robots.txt). Para licencias comerciales
o de entrenamiento de IA: <kennethstott@gmail.com>
