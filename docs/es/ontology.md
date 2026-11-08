# Ontología {: #ontology }

La ontología es un vocabulario vivo sobre su modelo de datos. Cada columna física se resuelve en un término, y las columnas que tienen el mismo significado comparten un término, por distinto que lo escriban. Cada término puede tener una definición, relaciones tipadas con otros términos, una lista de expertos en la materia y etiquetas como `pii` que lleva cada columna con ese significado.

Es un grafo de conceptos, sus definiciones y las relaciones entre ellos. Provisa lo asigna a las tablas y columnas que contienen los valores, y eso le permite responder preguntas sobre sus datos. Un término es un borrador hasta que un steward lo aprueba, y si alcanza datos se deduce del grafo. La ontología está separada de las métricas: un término dice qué es un concepto, una métrica dice cómo se calcula un número.

Ese vocabulario compartido conecta el lenguaje del negocio con los datos físicos. Un agente de IA que sabe que «customer» nombra cada columna con un identificador de cliente no tiene que adivinar cuál de `cust_id`, `customerId` y `CUSTOMER_KEY` es la correcta. Todas se resuelven en el mismo término, y el término lleva la definición.

## Clases, propiedades y conceptos {: #classes-properties-and-concepts }

Cada término tiene exactamente un rol, derivado de lo que ancla. Usted nunca lo establece.

| Rol | Anclado en | Ejemplo |
| --- | --- | --- |
| **Clase** | Una o más tablas o vistas. Sus instancias son sus filas. | `customer`, que ancla `customers` y `crm_contacts` |
| **Propiedad** | Columnas que no son claves. | `customer email`, que contiene `customers.email` |
| **Concepto** | Nada directamente. Alcanza los datos a través de sus relaciones. | `revenue`, con aristas `PART_OF` desde `order amount` y `refund amount` |

Un término es una cosa u otra. Provisa se niega a anclar una tabla en un término que contiene columnas que no son claves, y se niega a mover una columna que no es clave a un término anclado en tablas. Cada rechazo nombra el término y lo que ya contiene.

### Cada tabla con clave propone una clase {: #every-keyed-table-proposes-a-class }

Cuando se guarda una tabla con clave primaria, Provisa la ancla en un término de clase en borrador con el nombre del concepto de la tabla: `orders` propone `order`, `order_lines` propone `order line`. Las tablas del mismo concepto comparten una clase. Una tabla sin clave primaria no puede ser una clase, y su anclaje se rechaza nombrando la tabla.

Un anclaje que hace usted mismo, en la UI, en la configuración o mediante Polly, es curado. La sincronización nunca lo mueve, y anclar una tabla en un término curado reemplaza su propuesta en borrador. Cuando un concepto ya es una propiedad con columnas que no son claves, sus tablas no reciben propuesta; la ontología las lista en cambio en **Por revisar**.

### Las claves pertenecen a la clase {: #keys-belong-to-the-class }

Una clave primaria no tiene término propio. Cada una de sus columnas, incluidas las partes de una clave compuesta, pone su referencia en la clase de su tabla.

Una clave foránea funciona igual. Cada columna del lado múltiple de una relación declarada pone su referencia en la clase de la tabla de destino, se llame como se llame la columna. `manager_id` en `employees`, que apunta de vuelta a `employees`, va a parar a `employee`; el rol «manager» vive en el nombre de la relación. Una columna sin relación declarada es una propiedad, aunque su nombre coincida con una clase.

Provisa coloca las referencias de clave solo por derivación, cada vez que una tabla se sincroniza, cambia una relación o cambia la clase de una tabla. No puede mover una columna de clave a mano, y el rechazo nombra la columna, su tabla y su clase. Ancle una tabla en otro término y sus claves la siguen de inmediato.

### La misma entidad en varias tablas {: #the-same-entity-in-several-tables }

Las instancias de tablas distintas son distintas hasta que usted diga lo contrario. Cuando `customers` y `crm_contacts` describen ambas a un mismo cliente, marque la relación que las une como **misma entidad**. Debe unir dos tablas que ancla la misma clase, uno a uno, sobre una clave de cada una: la clave primaria o una restricción de unicidad declarada.

Mientras exista una relación de misma entidad sobre una tabla, su clase no puede retirarse ni reemplazarse. Desmarque primero la relación.

Una clase anclada en varias tablas que las relaciones de misma entidad no unen todas se señala en **Por revisar** como *tablas no vinculadas*.

## Cómo se derivan los términos {: #how-terms-are-derived }

Provisa crea automáticamente un término en borrador a partir de cada nombre de columna, con una regla de normalización determinista: unificación de mayúsculas y minúsculas, separación por separadores y camelCase, expansión de abreviaturas y eliminación de tokens sustitutos finales.

**La expansión de abreviaturas** asigna las abreviaturas empresariales comunes a su forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, etcétera. Tanto `id` como `key` se expanden a `identifier`. La tabla es fija y prudente. Las abreviaturas ambiguas como `st`, `min` y `no` se quedan como están, para no adivinar nada mal.

**La eliminación de tokens sustitutos** quita un token final `identifier`, `code`, `index` o `reference`. Una columna llamada `cust_id` no nombra el identificador en sí; nombra a un cliente mediante un valor sustituto. Al quitar el sustituto, `cust_id` y `customerId` van a parar al término `customer`. Solo se quitan los tokens finales, y nunca el último que queda, de modo que una columna `id` sola se expande a `identifier` y ahí se queda.

**La deduplicación** es el objetivo. La regla es determinista, así que `cust_id`, `customerId` y `CUSTOMER_KEY` producen todos `customer`. Cada columna recibe una referencia en ese único término, y la curación tiene un solo lugar donde añadir la definición en vez de tres.

Las columnas de clave se saltan esta regla. Sus referencias van a una clase, como se describe arriba.

### Frases genéricas {: #generic-phrases }

Algunas frases normalizadas son demasiado genéricas para ser un concepto por sí mismas. Una columna `name`, `date` o `identifier` sola nombra un atributo del concepto de su tabla, que no existe fuera de esa tabla. Los empleados tienen nombres y los productos tienen nombres, pero no son lo mismo.

Cuando una frase está en el conjunto genérico y hay un contexto de tabla disponible, el término se califica como `<table concept> <phrase>`: `employees.first_name` se normaliza a `employee first name`.

El conjunto genérico abarca sustantivos de atributo (`name`, `date`, `status`, `type`, `amount`, `quantity`), frases de pista de auditoría (`created_at`, `modified_by`, `submitted_timestamp`) y unas cuantas más que aparecen en casi todas las tablas.

### Los términos siguen el nombre de negocio {: #terms-follow-the-business-name }

Un término derivado sigue el **nombre de negocio** de la columna: su alias cuando el modelador definió uno, y su nombre físico en caso contrario. Cuando `usr_nm` tiene el alias `user name`, el término derivado es `user name`, donde expandir `usr_nm` podría haber dado `user number`.

Poner un alias a una columna es la corrección más fuerte. Un alias llega a cada superficie que lee la columna (SQL, GraphQL, agentes de IA, el catálogo), así que el modelo se describe correctamente en todas partes. Renombrar el término corrige una entrada del catálogo y deja la columna como `usr_nm` para el siguiente lector. El banner Borrador de la UI lo dice: ponga primero un alias a la columna, y renombre el término solo cuando el nombre de la columna sea correcto y el vocabulario no.

Cambiar el alias de una columna vuelve a derivar su término en borrador, así que la ontología sigue al modelo y nunca pide dos veces la misma corrección. Una vez que un curador ha añadido a un término una definición, una relación, un experto o una etiqueta, una edición del alias deja la columna donde está, porque ese trabajo es del curador y se queda.

### Nombres de tabla de ruta de acceso {: #access-path-table-names }

Algunos nombres de tabla describen una ruta de acceso en lugar de un concepto: `user_by_name` es un usuario al que se llega por una búsqueda por nombre, no un tipo de entidad aparte. Cuando Provisa deriva el concepto de una tabla, corta el nombre en el conector, así que `user_by_name` se convierte en `user` y `orders_by_customer` en `order`.

Sin el corte, `user_by_name` propondría una clase `user name` que chocaría con el atributo real `users.name`. El corte se aplica solo a los conceptos de tabla. En un nombre de columna, `by` forma parte del sustantivo compuesto, así que `pet_by_name` y `pet_name` se normalizan en el mismo término, `pet name`.

## Por qué importan la aprobación y el anclaje {: #why-approval-and-grounding-matter }

Los agentes y otra automatización encuentran los datos con los que trabajan a través de la ontología, y la citan. Un agente parte de un término que una persona aprobó. Sigue una ruta hasta datos cuya fiabilidad puede ver, anclados o todavía pendientes de una aprobación. Llega a tablas y columnas que lee bajo los permisos, las reglas de fila y las máscaras de su rol. Cada paso de esa ruta tiene un nombre que un agente puede poner en una cita.

Así que dos hechos sobre un término deciden qué puede hacer un agente con él. Las personas fijan su **estado**. Provisa calcula su **anclaje**. La página los muestra como dos chips separados, y usted filtra por cada uno.

## Estado {: #status }

Una persona fija el estado de un término. Tiene uno a la vez.

| Estado | Significado | Cómo llega ahí |
| --- | --- | --- |
| **Borrador** | Aún no aprobado. | Todo término derivado empieza aquí, y también todo término que usted crea. |
| **Aprobado** | La decisión de un steward: este es vocabulario con el que trabajar. | Un steward lo aprueba. La aprobación requiere una definición. |
| **Obsoleto** | De salida. | Un curador marca obsoleto un término aprobado, o un término curado pierde su última columna. |
| **Retirado** | Fuera de servicio, conservado como registro. | Un curador lo retira y nombra un sucesor. |

Una definición por sí sola no aprueba nada. Apruebe un término cuando su definición diga lo que el negocio entiende por él.

## Anclaje {: #grounding }

El anclaje dice si un término alcanza datos. Nadie lo fija: Provisa lo calcula a partir de las relaciones del término cada vez que cambia el grafo.

El anclaje de un término mira hacia abajo por sus propias rutas hacia los datos. Una ruta va desde el término, por sus relaciones, hasta el primer término que contiene una columna o una tabla: el término del extremo de datos, que tiene la vinculación. Solo cuentan los términos estrictamente entre los dos extremos. El término mismo nunca cuenta contra su propio anclaje, ni tampoco nada por encima de él.

| Anclaje | Marca | Significado |
| --- | --- | --- |
| **Anclado** | icono ⏚ verde | Alguna ruta hacia abajo no tiene ningún término en borrador entre el término y el extremo de datos. |
| **Anclado · ruta pendiente de aprobación** | icono ⏚ atenuado | Cada ruta hacia abajo tiene un término en borrador en medio. Pase el cursor sobre el icono: dice «Alcanza datos, pero solo a través de términos aún no aprobados». |
| **No anclado** | ninguna | Ninguna ruta alcanza datos. |

El icono es el símbolo eléctrico de tierra. Aparece en la lista de términos junto al número de columnas, en la página del término, antes del nombre del término en los grafos y en la leyenda del grafo. El texto emergente de un término anclado dice hasta dónde llega, por ejemplo «Anclado: alcanza datos en 3 columnas»; un lector de pantalla lee las mismas palabras.

Algunos casos se siguen directamente de la regla:

- Un concepto en borrador que usted redacta sobre una cadena aprobada está anclado. Su chip de estado sigue diciendo Borrador, y los términos de debajo no cambian.
- Un término derivado de una columna es un borrador en el extremo de datos. Nunca deja pendiente la cadena que tiene encima.
- Un término en borrador en medio de la única ruta deja ⏚ atenuado a cada término por encima. Aprobar ese único borrador los ancla.
- Basta una segunda ruta limpia. Un borrador al lado no cambia nada.
- Los términos obsoletos y retirados rompen la cadena. Un término cuya única ruta pasaba por uno deja de estar anclado, y sus chips y filtros lo dicen.

## Lo que ve un agente {: #what-an-agent-sees }

La búsqueda de términos de MCP, el vocabulario de SPARQL y cualquier otra superficie para agentes muestran a un agente la misma porción de la ontología:

- un término aprobado en una ruta hacia los datos;
- un término en borrador definido en una ruta hacia los datos que contiene al menos un término aprobado, marcado como borrador;
- un término sin definir en una ruta así solo como paso anónimo, sin nombre ni definición. Cada paso sigue siendo distinto, así que la forma de la ruta no cambia.

Nada más llega a un agente. Una cadena sin ningún término aprobado queda oculta, igual que los términos que no alcanzan datos y los términos obsoletos o retirados. Las personas ven todos los términos en la página Ontología, con su estado y su anclaje.

Cada resultado de búsqueda lleva el estado y el anclaje del término, sus columnas y tablas, sus expertos y sus rutas hacia los datos. Cada paso de una ruta es un término con nombre o un paso anónimo, y el extremo de datos lista aquello a lo que está vinculado. Esa ruta es la que un agente sigue y cita.

### Marcar un término como obsoleto {: #deprecating-a-term }

Marque como obsoleto un término aprobado cuando esté de salida. Usted da un motivo, que es obligatorio, y opcionalmente una fecha de retiro prevista, hoy o más adelante. En esa fecha el planificador lo retira. Un término obsoleto rompe la cadena que pasa por él desde el momento en que lo marca, así que conviene leer antes la vista previa del impacto.

Un término cuyo retiro ya toca y que aún contiene columnas o tablas no puede retirarse por fecha, porque solo un sucesor puede quedárselas. Espera, y **Por revisar** lo lista como *retiro bloqueado* hasta que usted lo retire con un sucesor.

Un término curado que pierde su última columna se marca automáticamente como obsoleto y se conserva. Sale de servicio, conserva el contenido que aportaron sus editores y vuelve si reaparece la misma columna. Un término no curado sin columnas se elimina.

### Retirar un término {: #retiring-a-term }

Retirar un término mueve todo lo que tiene asociado a un **sucesor** y luego lo pone fuera de servicio.

- Las columnas de una propiedad pasan al sucesor. Debe ser una propiedad o un concepto.
- Las tablas de una clase pasan al sucesor, y sus claves las siguen. Debe ser una clase o un concepto.
- Un concepto no contiene nada, así que su sucesor es opcional.

El retiro necesita un motivo, y escribe una arista `REPLACES` del sucesor al término retirado. El término retirado se queda como registro inerte: su definición, etiquetas, relaciones, expertos y el motivo. No conduce nada. El sucesor conserva su propio estado; un retiro nunca lo aprueba.

Como sus columnas ahora pertenecen al sucesor, el panel de un término retirado muestra a qué **se aplicó por última vez**: sus columnas y tablas tal como el modelo las tenía en el commit anterior al retiro. Su panel también enlaza con el término que lo reemplaza.

### Restaurar un término {: #restoring-a-term }

**Restaurar** deshace lo que afirmó un curador. Un término obsoleto vuelve a estar aprobado. Un término retirado vuelve como concepto vacío y en borrador, ya que sus columnas pasaron a su sucesor, y su arista `REPLACES` se elimina.

### Qué hace que un término sea curado {: #what-makes-a-term-curated }

Un término nacido de la normalización de columnas empieza en blanco. Pasa a ser curado cuando tiene cualquiera de estos:

- una definición guardada
- una arista de relación
- un experto en la materia asignado
- una etiqueta, como `pii`
- un retiro

Los términos curados se conservan cuando sus columnas se van. La sincronización nunca vuelve a derivar en otro lugar las columnas de un término curado.

## Volver a sincronizar desde las tablas {: #re-syncing-from-tables }

Cada vez que una tabla se guarda o se recarga, Provisa concilia las columnas de esa tabla con la ontología. Las columnas nuevas crean o vinculan términos, las columnas que desaparecen sueltan sus referencias, y todo término que se queda sin referencias se elimina o se marca obsoleto según la regla anterior. Las columnas de clave van a su clase, y una tabla con clave sin clase propone una.

Solo se vuelven a derivar los términos no curados. Si pone un alias a una columna y su término en borrador cambia, la columna pasa al término nuevo. Si el término es curado, la columna se queda, porque una edición del alias no anula la decisión de un curador.

Cuando eliminar un término dejaría un concepto sin ruta hacia los datos, Provisa marca ese término como obsoleto en su lugar, de modo que la estructura que usted construyó sobrevive hasta que la vuelva a conectar.

## Relaciones {: #relationships }

Los términos se relacionan con otros términos mediante aristas tipadas.

| Tipo | Significado |
| --- | --- |
| `KIND_OF` | El término de origen es un tipo del término de destino. |
| `PART_OF` | El término de origen es un componente del término de destino. |
| `SYNONYM_OF` | Los dos términos son intercambiables en este dominio. |
| `RELATED_TO` | Dos temas distintos que van juntos, cuando ningún otro tipo encaja. Guarda un lugar y no ancla. |
| `VALID_VALUE_OF` | El origen es un valor permitido de la enumeración o del dominio de destino. |
| `DERIVED_FROM` | El origen se calcula u obtiene a partir del destino. |
| `REPLACES` | El origen sustituye al destino. Retirar un término escribe una desde su sucesor. |
| `PREFERRED_TERM_FOR` | El origen es el término preferido frente al destino desaconsejado. |
| `TRANSLATION_OF` | El origen es una traducción del destino a un idioma o configuración regional. |
| `ANTONYM_OF` | El origen es el opuesto semántico del destino. |

Las relaciones tienen dirección. La UI muestra tanto las aristas salientes (este término → otro) como las entrantes (otro término → este término), y etiqueta cada dirección con su propia frase en lenguaje llano.

Las aristas viven en una tabla asociativa declarada como relación de unión. Su columna de tipo de relación es el discriminador, así que cada tipo de arriba es un tipo de relación Cypher distinto entre dos nodos de término. Nada de esto es propio de la ontología: se declara como declararía una unión sobre sus propias tablas, y el mismo código la lee.

## Construir un glosario que ancle {: #building-a-glossary-that-grounds }

Un término solo ayuda a un agente cuando está anclado, es decir, cuando alguna ruta de relaciones lleva de él a una columna o una tabla. Los agentes solo ven los términos de esas rutas, como describe [Lo que ve un agente](#what-an-agent-sees). Un término anclado le dice dónde viven sus datos. Uno no anclado nombra una idea y le deja a usted encontrar los datos.

Los tipos de relación que elige deciden si esa ruta existe. El anclaje va de un término más amplio hacia uno más estrecho. `revenue` alcanza datos porque `order amount` es parte de él y `order amount` contiene una columna. Nunca va en sentido contrario, así que `order amount` no gana nada por ser parte de `revenue`.

### Pruebe cada tipo antes de elegirlo {: #test-each-type-before-you-pick-it }

Lea una arista con X como el término del que parte e Y como el término al que apunta. Después aplique las pruebas en este orden y quédese con la primera que se cumpla.

| Tipo | Prueba | Ejemplo | Qué ancla |
| --- | --- | --- | --- |
| **Tipo de** | Todo X es un Y. | Todo cliente corporativo es un cliente. | Y alcanza los datos de X. |
| **Parte de** | Un Y tiene un X. | Un pedido tiene un importe del pedido. | Y alcanza los datos de X. |
| **Valor válido de** | X es uno de los valores permitidos de Y. | Platino es un nivel de cliente. | Y alcanza los datos de X. |
| **Sinónimo de** | X e Y significan lo mismo. | Cliente y comprador. | Cada uno alcanza los datos del otro. |
| **Relacionado con** | X e Y son temas distintos que van juntos, y ninguno es un tipo, una parte o un valor del otro. | La tarta y las velas. | Nada. |

Derivado de, Sustituye a, Término preferido para, Traducción de y Antónimo de registran cada uno un hecho sobre dos términos. Ninguno ancla.

Una propiedad que describe un concepto es parte de él. El inicio de turno describe un turno, así que es **Parte de** la cobertura de turno. Nunca está simplemente relacionado con ella.

Cuando añade una relación o cambia su tipo, el selector de tipos lista primero Tipo de, Parte de y Valor válido de, cada uno con su prueba debajo. Relacionado con va al final, con su prueba y el recordatorio de que no ancla. El enlace **Elegir una relación que ancle** bajo el selector abre esta sección.

### «Relacionado con» guarda un lugar {: #related-to-holds-a-place }

Relacionado con es el último recurso. Como no ancla, un término cuyos únicos vínculos son Relacionado con no alcanza datos, tenga los que tenga. Trate cada uno como un marcador de posición para un par que aún no ha resuelto.

Para trabajar los marcadores de posición, marque **Relacionado con** encima de la lista de términos, junto a **Obsoleto** y **Por revisar**. La lista se reduce a los términos con al menos un vínculo Relacionado con, en cualquiera de los dos extremos. El icono del libro junto a la casilla abre esta sección.

Un término que no alcanza datos y solo tiene vínculos Relacionado con muestra una nota en su página. La nota dice que al par puede faltarle un término intermedio y nombra las formas que lo resolverían. Es solo una orientación. Nada del término queda bloqueado.

### Resuelva un par con un término nuevo {: #resolve-a-pair-with-one-new-term }

A la mayoría de los pares Relacionado con les falta un término intermedio. Un término nuevo convierte el marcador de posición en dos aristas que anclan, y cinco formas lo consiguen. Estas son para la tarta y las velas, de la más ajustada a la más amplia:

| Forma | Término nuevo | Aristas |
| --- | --- | --- |
| **Tipo de componente** | decoración de tarta | velas Tipo de decoración de tarta; decoración de tarta Parte de tarta |
| **Especialización** | tarta de cumpleaños | velas Parte de tarta de cumpleaños; tarta de cumpleaños Tipo de tarta |
| **Todo común** | fiesta | tarta Parte de fiesta; velas Parte de fiesta |
| **Género común** | artículos de fiesta | tarta Tipo de artículos de fiesta; velas Tipo de artículos de fiesta |
| **Enumeración común** | tipo de artículo de fiesta | tarta Valor válido de tipo de artículo de fiesta; velas Valor válido de tipo de artículo de fiesta |

Siga el anclaje hacia abajo en cada fila. En las dos primeras formas, tarta alcanza los datos de velas a través del término nuevo. En las tres últimas, el término nuevo es el extremo más amplio de ambas aristas, así que alcanza los datos de las dos. Ni tarta ni velas alcanza los de la otra.

A veces el término intermedio ya existe y solo hace falta cambiar el tipo de los vínculos. La demo del refugio que se incluye tiene un concepto, `shift coverage`, definido como la franja y el lugar en que un miembro del personal tiene turno asignado. Sus partes `shift start`, `shift end` y `schedule location` contienen una columna cada una. Vinculado a ellas con Relacionado con, shift coverage no alcanzaba datos. Con cada una de las tres vinculada como Parte de shift coverage, queda anclado a través de las tres columnas. Es un todo común, y el todo estuvo ahí desde el principio.

### Prefiera la forma más ajustada {: #prefer-the-tightest-shape }

Cuanto más amplio es el término nuevo, menos le dice su anclaje a quien lo lee. Fiesta conecta la tarta y las velas con la misma seguridad que decoración de tarta. Pero un nodo tan amplio también está por encima de la lista de invitados, el local y la música, así que su anclaje ya no dice qué datos responden a una pregunta sobre las velas. Decoración de tarta le da a un agente una ruta corta hacia las columnas correctas. Fiesta le da una ruta larga hacia muchas.

Empiece por el tipo de componente y baje por la lista solo cuando no encaje una forma más ajustada. Use un género común o una enumeración común cuando el par no comparta nada más estrecho.

### Cuando elige «Relacionado con» {: #when-you-choose-related-to }

Elegir **Relacionado con** para un vínculo que está añadiendo abre un cuadro de diálogo, por ejemplo «¿Vincular velas y tarta con «Relacionado con»?». Cambiar a Relacionado con el tipo de un vínculo existente también lo abre. La interrupción es deliberada, porque es el momento en que corregir el vínculo cuesta menos.

El cuadro de diálogo explica primero por qué Relacionado con es la arista menos útil. No ancla, así que el término sigue sin poder alcanzar datos a través de ella. Debajo dibuja las cinco formas para sus dos términos, de la más ajustada a la más amplia. Cada forma muestra un nombre para el término nuevo, sugerido por el modelo de IA de su organización, por ejemplo velas → decoración de tarta → tarta. Si el modelo no sugiere nombres, el cuadro de diálogo dice por qué, y usted escribe el nombre.

Luego elige una de tres acciones.

- **Empezar** junto a una forma abre el formulario de término nuevo. El nombre sugerido aparece rellenado, y **Relaciones que se añaden con él** lista las dos aristas. Cambie el nombre del término, añada una definición y sus dominios, y haga clic en **Crear**. Crear añade el término. La arista que toca el término que está editando se incorpora a su formulario y se guarda con él. La otra arista se escribe de inmediato.
- **Añadir «Relacionado con» de todos modos** añade el vínculo con un clic. Se suma a los marcadores de posición que lista el filtro **Relacionado con**.
- **Cancelar** no cambia nada.

### Cómo elige el asistente de relaciones {: #how-the-relationship-assistant-chooses }

**Relaciones** en el menú ✨ pide al modelo de IA de su organización relaciones entre todos los términos de sus dominios. Las instrucciones del modelo llevan las mismas pruebas. Prefiere Tipo de, Parte de y Valor válido de siempre que uno encaje, y vincula una propiedad con el concepto que describe mediante Parte de. Propone Relacionado con solo cuando dos términos van claramente juntos y ningún otro tipo encaja.

El asistente vincula términos que ya existen, y nunca añade un término. Para cada Relacionado con que añade, nombra términos candidatos para cada forma que encaje. Cuando termina, **Vínculos «Relacionado con» por revisar** lista cada uno de esos pares con sus formas, de la más ajustada a la más amplia, y los candidatos de cada una. Haga clic en cualquiera de los dos términos para abrirlo. Si un par necesita un término nuevo, y cómo llamarlo, lo decide usted.

### Mida el glosario {: #measure-the-glossary }

El informe **Ontology quality** mide el glosario a partir del modelo, por dominio y por steward. Ábralo en el [visor de informes](admin.md#reports-viewer). Informa de:

- la proporción de términos en servicio que están anclados, contando aparte los totalmente anclados y los pendientes de aprobación;
- la proporción de términos aprobados y anclados, que son los que un agente puede usar;
- los marcadores de posición sin resolver, es decir, los vínculos Relacionado con y los términos cuyos únicos vínculos son Relacionado con;
- la precisión del anclaje, como los pasos de cada término a los datos y el número de rutas que pasan por un término nodo amplio;
- los términos anclados solo a través de borradores, cada uno con el término cuya aprobación lo anclaría del todo;
- los términos sin definir en rutas hacia los datos;
- la deriva, los términos que perdieron o ganaron anclaje desde el periodo anterior.

Cada fila enlaza con su término, y cada fila de marcador de posición con los vínculos Relacionado con de ese término para revisarlos. Como todo informe, se lee a través del pipeline gobernado bajo su rol, y puede exportarlo.

## Conceptos {: #concepts }

Un concepto no tiene columnas ni tablas propias. Use uno para una idea de negocio que abarca varios términos concretos, y conéctelo después con los términos que sí contienen datos. `revenue`, por ejemplo, podría ser un concepto, con aristas `PART_OF` desde `order amount`, `adjustment amount` y `refund amount`.

Un concepto que no puede alcanzar ninguna columna ni tabla a través del grafo de relaciones no nombra datos. No aparece en la búsqueda de términos de los agentes ni en la exportación de metadatos, y **Por revisar** lo lista como *sin datos*. Los dominios se aplican solo a un concepto. Cualquier otro término toma sus dominios de las tablas que ancla o de las columnas que contiene.

## PII según el significado {: #pii-on-meanings }

La PII es una propiedad de lo que significa una columna, así que Provisa le permite marcarla una vez, en el término, y cubre cada columna con ese significado, incluidas las que se registren más tarde.

La etiqueta `pii` va en una columna o en un término de propiedad, nunca en una fuente, una tabla, una clase o un concepto. Un término debe estar definido y no retirado para recibirla. Marcar un término en borrador se rechaza con «Defina *término* para marcarlo», y no puede borrar la definición de un término etiquetado hasta que lo desmarque.

Cada columna de un término etiquetado lleva `pii`, y `sensitive` con ella, allí donde se leen etiquetas: gobernanza, enmascaramiento, sensibilidad, exportación y las vistas de etiquetas. No se copia nada a las columnas. Una columna movida al término lleva la etiqueta de inmediato, y una movida fuera de él deja de llevarla. Cada columna dice de dónde viene su etiqueta: puesta en la columna, con su motivo, o a través de un término, que nombra.

Cuando marca una columna cuyo término es una propiedad, la UI pregunta si marcar las otras columnas que significan lo mismo.

- **Sí** pone la etiqueta en el término.
- **Solo esta** marca solo la columna, y pregunta por qué.

Una columna que es PII a través de su término no puede desmarcarse por sí sola. Desmarque el término, o mueva la columna al término que corresponde a lo que contiene.

### Movimientos que pierden etiquetas {: #moves-that-lose-tags }

Mover una columna a otro término puede quitarle una etiqueta: mover `customers.email` de `customer email` (etiquetado `pii`) a un término sin etiqueta dejaría sin protección la columna de correo electrónico. Cada movimiento que hace una persona se comprueba antes. Eso abarca reclamar o mover una columna, retirar un término, y guardar o eliminar una relación que reubica columnas de clave.

Cuando una columna dejaría de llevar `pii` por completo, el movimiento se rechaza hasta que usted decida. La UI muestra un resumen de lo que cada columna movida pierde y gana, primero las pérdidas.

- **Mover** sigue adelante, con cada etiqueta perdida confirmada.
- **Mover y marcar** lleva las etiquetas perdidas al término al que van las columnas, en el mismo cambio.
- **Cancelar** no mueve nada.

Una columna que conserva la etiqueta por su propia marca no necesita confirmación. Los movimientos que hace el propio modelo, como volver a derivar las columnas de un borrador, nunca cambian una etiqueta heredada.

## Por revisar {: #to-review }

La ontología informa de lo que necesita su atención como hallazgos. Un término con algún hallazgo está **Por revisar**, y la lista de términos filtra por ello. Un simple borrador no es un hallazgo; los borradores son el estado ordinario de la ontología. Un término que alcanza datos solo a través de borradores sí lo es, porque una sola aprobación lo pondría delante de los agentes.

| Hallazgo | Qué significa | La solución |
| --- | --- | --- |
| Tablas no vinculadas | Una clase ancla varias tablas que las relaciones de misma entidad no unen todas. | Marque relaciones de misma entidad entre ellas. |
| Clase y atributo | Una clase contiene columnas que no son sus claves, o una tabla con clave no tiene clase porque su concepto es una propiedad. | Mueva las columnas a una propiedad, o vuelva a anclar la tabla. |
| Etiqueta en el tipo equivocado | Un término que no es una propiedad lleva una etiqueta que solo una propiedad transmite a las columnas. | Quite la etiqueta. |
| Retiro bloqueado | Un término obsoleto ha pasado su fecha de retiro prevista pero aún contiene columnas o tablas. | Retírelo con un sucesor. |
| Sin datos | Un concepto en borrador no alcanza datos. | Relaciónelo con un término que contenga datos. |
| Aprobado, no anclado | Un término aprobado no alcanza datos, así que ningún agente lo ve. | Relaciónelo con un término que contenga datos. |
| Anclado · ruta pendiente de aprobación | Cada ruta hacia los datos pasa por un borrador. El hallazgo nombra el borrador cuya aprobación ancla el término. | Abra ese término y apruébelo. |

Los hallazgos informan; no cambian nada hasta que usted actúa.

## Exportación de metadatos {: #metadata-export }

La ontología se publica en catálogos de datos externos como parte de la exportación de metadatos. Se publican los términos aprobados, cada uno con su anclaje: `grounded`, `grounded_pending` o `not_grounded`. Un borrador nunca sale de Provisa. El anclaje para la exportación se juzga sobre las columnas y tablas que realmente se publican. Un término cuyas columnas se retienen todas de la exportación, porque sus tablas no son productos de datos o porque los filtros técnicos las excluyen, se retiene con ellas.

Un término aprobado que un curador ha marcado después como obsoleto se publica con su estado, su motivo y su fecha de retiro prevista, para que cada catálogo pueda asignarlo a su propio marcador de obsolescencia.

Las aristas de relación se publican solo cuando se publican sus dos términos extremos. Los activos de columna se exportan de forma independiente, así que excluir un término no oculta los datos subyacentes.

### Excluir un término de la exportación {: #excluding-a-term-from-export }

Algunas columnas llevan fontanería, como identificadores de lote ETL, versiones de fila y marcas de tiempo de ingesta. Un término derivado de una columna así puede tener una definición exacta y aun así no ser vocabulario de negocio. El control **Excluir de la exportación de metadatos** retiene el término, y cualquier arista de relación que termine en él, de los catálogos en los que publica Provisa. Las columnas siguen exportándose como activos.

Lo que cuenta es si el negocio usa la palabra. Un identificador de lote ETL tiene un significado claro que pertenece a la ontología para los ingenieros, pero no a un catálogo de negocio junto a `customer` y `revenue`.

## Trabajar con la ontología {: #working-with-the-ontology }

Abra **Ontología** en la barra de navegación. El panel izquierdo lista cada término con su rol, su estado salvo que esté aprobado, y su chip de anclaje. Los filtros **Estado** y **Anclaje** reducen la lista por cada uno, la casilla **Obsoleto** muestra u oculta los términos obsoletos, **Por revisar** la reduce a los términos con hallazgos, y **Relacionado con** la reduce a los términos con un vínculo Relacionado con. Haga clic en un término para abrir su panel.

El panel muestra el rol, el estado y el anclaje del término uno al lado del otro, y para una propiedad su marca de PII con el número de columnas que cubre. Un término ⏚ atenuado nombra los borradores cuya aprobación lo anclaría. Un término obsoleto o retirado muestra quién cambió su estado y por qué. Un término con hallazgos se abre con una sección **Por revisar**: haga clic en un hallazgo para ver qué se encontró y la acción que lo corrige.

### Resumen de stewardship {: #stewardship-summary }

Sin ningún término abierto, el panel muestra el resumen de stewardship. Lista los términos que los agentes pueden ver, los términos ⏚ atenuados que esperan cada uno una aprobación, con el término que hay que aprobar, y los términos aprobados que no alcanzan datos. **Aprobar borradores definidos y anclados** lista cada borrador que tiene definición y está anclado; desmarque aquellos cuya definición no sea correcta, y apruebe el resto a la vez. El botón del portapapeles de la cabecera hace volver el resumen.

### Ver primero el impacto {: #seeing-the-impact-first }

Aprobar, retirar y vincular un término cambian cada uno lo que alcanza datos. Antes de cualquiera de ellos, la página muestra lo que cambiaría la acción: cuántos términos pasan a estar anclados, ⏚ atenuados o no anclados, y a cuántas tablas ganan o pierden ruta los agentes. Por ejemplo, «Aprobar region ancla 3 términos y da a los agentes una ruta a 2 tablas más». **Aprobar** abre la vista previa con su botón de confirmación, **Retirar** la muestra bajo el sucesor que elija, y anclar una tabla en el formulario de edición la muestra bajo la tabla antes de guardar.

**Editar** abre el término entero en un solo formulario. Guardar aplica todos los cambios juntos.

- **Cambie el nombre** del término para cambiar su redacción sin mover sus columnas.
- **Añada una definición** escribiéndola, o haga clic en el botón de borrador de IA para generar un punto de partida a partir del nombre del término, sus columnas y sus relaciones. El borrador no se guarda hasta que guarde el formulario.
- **Reclame columnas** para mover columnas de otros términos a este, o elija **Mover a término** junto a una de sus propias columnas. Cada columna pertenece a un término. Las columnas de clave siguen a su clase y no pueden moverse.
- **Ancle tablas** en una clase, o quítelas.
- **Marque PII** en una propiedad.
- **Añada una relación**, eligiendo su tipo. Cambie el tipo de una arista existente en el sitio en lugar de eliminarla y volver a añadirla.
- **Asigne expertos** por ID de usuario, como `expert` o `author`.

Junto a Editar, el panel ofrece las acciones de estado que permite el término: **Aprobar** un borrador definido, **Marcar obsoleto** un término aprobado, **Retirar** cualquier término en servicio, y **Restaurar** uno obsoleto o retirado.

Desde el menú ✨, **Definiciones** rellena cada definición vacía en una pasada y nunca sobrescribe el texto de una persona. **Relaciones** añade aristas tipadas en toda la lista de términos; se descartan las propuestas que nombran términos desconocidos, el mismo término dos veces o un tipo no reconocido. Cada Relacionado con que añade se lista para revisión, como describe [Cómo elige el asistente de relaciones](#how-the-relationship-assistant-chooses).

El banner **Borrador** de un término le dice si aún necesita una definición (ponga un alias a la columna o añada una) o solo su aprobación. Un banner aparte dice cuándo un término no alcanza datos: relaciónelo con un término que tenga datos.

## Consultar la ontología {: #querying-the-ontology }

La ontología es un esquema que puede consultar, llamado `ontology`, junto al esquema `meta` que describe la estructura del modelo. Sus tablas empiezan con un guion bajo, así que la tabla propia de una clase nunca comparte nombre con una de ellas.

| Tabla | Una fila por |
|---|---|
| `_terms` | Término, con su estado, su anclaje, su definición y si es una clase, una propiedad o ambas |
| `_term_refs` | Vinculación de una propiedad a una columna, con la tabla y el dominio de la columna |
| `_term_edges` | Relación entre dos términos |
| `_domains` | Dominio al que pertenece un término |
| `_experts` | Experto o autor asignado a un término |
| `_axioms` | Enunciado de la ontología en RDFS, OWL y SKOS |
| `_closure` | Término emparejado con cada término del que es un tipo, a cualquier profundidad, y consigo mismo |

Una vinculación enlaza un término con una columna del esquema `meta`, así que puede unir `ontology._term_refs` con `meta.table_columns` y `meta.registered_tables`. Las tablas de la ontología son de solo lectura. Cambian a medida que usted cura términos.

Solo lee un término cuando alcanza uno de sus dominios. Un término sin dominio es visible para todos, igual que un término de empresa (`*`). Una relación o un axioma es visible cuando lo son sus dos términos.

### La ontología en SPARQL {: #the-ontology-in-sparql }

SPARQL devuelve la ontología en los vocabularios estándar, así que una herramienta RDF la lee como lee cualquier ontología OWL:

- una clase es una `owl:Class`, y una relación de tipo entre clases es `rdfs:subClassOf`;
- una propiedad es una `owl:DatatypeProperty`, o una `owl:ObjectProperty` cuando cada columna que vincula es una clave hacia una tabla con clase;
- el `rdfs:domain` de una propiedad es la clase de cada tabla de la que vincula una columna, y su `rdfs:range` es la clase a la que llevan sus claves o el tipo XML Schema de sus columnas;
- una relación de tipo entre propiedades es `rdfs:subPropertyOf`;
- cada término tiene su nombre como `skos:prefLabel` y su definición como `skos:definition`.

Un término se nombra por su IRI de vocabulario, `<your Provisa address>/vocab/term/<name>`, la IRI que tipa las filas de las tablas que ancla. Un término retirado no enuncia nada.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?class ?parent ?label WHERE {
  ?class a owl:Class ; rdfs:subClassOf ?parent .
  ?parent skos:prefLabel ?label .
}
```

Una consulta puede unir la ontología con las filas que usted lee. Las instancias de una clase incluyen las de cada clase que es un tipo de ella, a cualquier profundidad:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX term: <https://your-provisa/vocab/term/>

SELECT ?party ?class WHERE {
  ?party a ?class .
  ?class rdfs:subClassOf* term:Party .
}
```

Una propiedad funciona igual: `?x ?p ?v . ?p rdfs:subPropertyOf* term:amount` lee los valores de cada propiedad que es un tipo de `amount`. Obtiene las instancias de las tablas que lee, así que una clase cuyas tablas no puede leer no añade ninguna.

Los mismos enunciados son filas de `ontology._axioms` en SQL, y los pares de tipo, cerrados a cualquier profundidad, son filas de `ontology._closure`. GraphQL, Cypher y los demás lenguajes de consulta no ofrecen estas dos tablas.

## Véase también {: #see-also }

- [Exportación de metadatos](metadata-export.md) explica cómo se publican los términos y las relaciones en catálogos de datos externos.
- [Linaje a nivel de columna](lineage.md) explica el explorador de linaje y cómo una columna física informa de los términos vinculados a ella.

## El esquema de la ontología {: #the-ontology-schema }

El esquema de sistema `ontology` permite consultar el vocabulario desde SQL, GraphQL, Cypher, REST y MCP, las mismas superficies que usa para sus propios datos. Está junto a `meta`, que contiene la estructura del modelo; `ontology` contiene su significado.

El esquema tiene dos partes: vistas de definición y tablas de clase.

### Vistas de definición {: #definition-views }

Cinco vistas exponen el contenido del glosario:

| Vista | Contenido |
| --- | --- |
| `_terms` | Cada término: su nombre, rol (clase, propiedad, concepto), definición, estado y anclaje. |
| `_term_refs` | Cada vinculación de término a columna: el nombre del término, la fuente, la tabla y la columna. |
| `_term_edges` | Cada arista de relación: término de origen, tipo de arista y término de destino. |
| `_domains` | Pertenencia a dominios: el nombre del término y cada dominio al que pertenece. |
| `_experts` | Asignaciones de expertos: el nombre del término, el ID de usuario y el rol (`expert` o `author`). |

Una sexta vista, `_axioms`, contiene cada tripleta RDFS/OWL/SKOS que el esquema puede expresar, una fila por tripleta, con las columnas `subject`, `predicate`, `object`, `object_iri` y `object_literal`. `object` nombra el objeto cuando es un término; `object_iri` lo contiene cuando es una IRI, y `object_literal` cuando es un literal. Una de las tres contiene el valor en cada fila. Las herramientas que prefieren tripletas a filas tabulares leen de aquí.

Una **vinculación de término**, es decir, un término vinculado a una columna y registrado en `_term_refs`, es distinta de una vinculación de comando. Una vinculación de término dice qué significa una columna; una vinculación de comando conecta una función al pipeline de consultas.

### Tablas de clase {: #class-tables }

Cada término de clase que no está retirado se convierte en una vista del esquema `ontology` con el nombre de ese término, con los espacios sustituidos por guiones bajos. Una clase anclada en una tabla produce una vista; una clase anclada en varias tablas produce la unión de esas tablas.

En una vista de unión aparecen las filas de cada tabla de origen. La columna `_source_table` identifica de qué tabla viene cada fila: contiene el nombre calificado de la tabla, `domain.table`. Una columna de propiedad que una tabla de origen no vincula es `NULL` para sus filas. Una propiedad vinculada a varias columnas de una misma tabla se convierte en una columna de matriz.

Cada columna de una vista de clase corresponde a una propiedad vinculada. La vista se gobierna mediante el mismo pipeline que cualquier otra lectura.

**Ejemplo.** Una clase `customer` anclada en `sales.customers` y `crm.contacts`, con una propiedad `name` vinculada a `customers.name` y `contacts.full_name`, y una propiedad `segment` vinculada solo a `customers.segment`:

```sql
-- rows from both tables; segment is NULL for crm.contacts rows
SELECT _source_table, name, segment
FROM ontology.customer
WHERE segment = 'enterprise';
```

### Derechos de lectura {: #read-rights }

Aquí se aplican las mismas reglas de gobernanza que a sus propias tablas.

Un término aparece en las vistas de definición solo si usted alcanza al menos uno de sus dominios. Un término sin dominio es visible para todos. Una fila de vinculación aparece solo cuando su columna se le sirve. Una arista aparece solo cuando sus dos términos le son visibles. Una columna de propiedad en una vista de clase aparece solo cuando se sirven sus columnas subyacentes.

No hace falta ningún acceso especial. Si puede consultar una tabla, puede consultar los términos de clase anclados en ella.

### SPARQL {: #sparql }

El punto de acceso SPARQL sirve el esquema `ontology` como una ontología RDFS/OWL/SKOS estándar. Cada término de clase se convierte en una `owl:Class`, cada arista de tipo en `rdfs:subClassOf`, cada término de propiedad en `owl:DatatypeProperty` u `owl:ObjectProperty` con `rdfs:domain` y `rdfs:range`, las aristas de subpropiedad en `rdfs:subPropertyOf`, los nombres en `skos:prefLabel` y las definiciones en `skos:definition`. Se aplican los mismos derechos de lectura.

La IRI base de todos los términos se deriva del ajuste `server.public_url`, bajo la ruta `/vocab/term/`. Un término de clase llamado `customer` tiene la IRI `{public_url}/vocab/term/customer`.

Tres consultas que escribiría un ontólogo:

**Encontrar todos los subtipos de Party, a cualquier profundidad:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?c WHERE {
  ?c rdfs:subClassOf* :Party .
}
```

**Encontrar clientes con un valor de segmento, usando la IRI del término:**

```sparql
PREFIX : <{public_url}/vocab/term/>

SELECT ?x ?s WHERE {
  ?x a :Customer ;
     :segment ?s .
}
```

**Listar todas las propiedades declaradas en Customer con sus rangos:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?p ?range WHERE {
  ?p rdfs:domain :Customer ;
     rdfs:range ?range .
}
```

Sustituya `{public_url}` por la dirección pública de su despliegue, tal como esté configurada en `server.public_url`.
