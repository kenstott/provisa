# Ontología {: #ontology }

La ontología es un vocabulario vivo sobre su modelo de datos. Cada columna física se resuelve en un término, y las columnas con el mismo significado comparten un término, por distinto que lo escriban. Cada término puede tener una definición, relaciones tipadas con otros términos, una lista de expertos en la materia y etiquetas como `pii` que lleva cada columna con ese significado.

Es un grafo de conceptos, sus definiciones y las relaciones entre ellos. Provisa lo asigna a las tablas y columnas que guardan los valores, y eso es lo que le permite responder preguntas sobre sus datos. Un término es un borrador hasta que tiene una definición y llega a los datos. La ontología está separada de las métricas: un término dice qué es un concepto, una métrica dice cómo se calcula un número.

Ese vocabulario compartido conecta el lenguaje del negocio con los datos físicos. Un agente de IA que sabe que «customer» nombra cada columna con un identificador de cliente no tiene que adivinar cuál de `cust_id`, `customerId` y `CUSTOMER_KEY` es la correcta. Todas se resuelven en el mismo término, y el término lleva la definición.

## Clases, propiedades y conceptos {: #classes-properties-and-concepts }

Cada término tiene exactamente un rol, derivado de aquello en lo que está anclado. Usted nunca lo fija.

| Rol | Anclado en | Ejemplo |
| --- | --- | --- |
| **Clase** | Una o más tablas o vistas. Sus instancias son sus filas. | `customer`, anclado en `customers` y `crm_contacts` |
| **Propiedad** | Columnas que no son claves. | `customer email`, con `customers.email` |
| **Concepto** | Nada directamente. Llega a los datos a través de sus relaciones. | `revenue`, con aristas `PART_OF` desde `order amount` y `refund amount` |

Un término es una cosa u otra. Provisa rechaza anclar una tabla en un término que tiene columnas que no son claves, y rechaza mover una columna que no es clave a un término anclado en tablas. Cada rechazo nombra el término y lo que ya tiene.

### Cada tabla con clave propone una Clase {: #every-keyed-table-proposes-a-class }

Cuando se guarda una tabla con clave primaria, Provisa la ancla en un término Clase en borrador, nombrado por el concepto de la tabla: `orders` propone `order`, `order_lines` propone `order line`. Las tablas del mismo concepto comparten una Clase. Una tabla sin clave primaria no puede ser una Clase, y su anclaje se rechaza nombrando la tabla.

Un anclaje que hace usted mismo, en la interfaz, en la configuración o con Polly, está curado. La sincronización nunca lo mueve, y anclar una tabla en un término curado sustituye su propuesta en borrador. Cuando un concepto ya es una Propiedad con columnas que no son claves, sus tablas no reciben propuesta; la ontología las muestra en **Por revisar**.

### Las claves pertenecen a la Clase {: #keys-belong-to-the-class }

Una clave primaria no tiene término propio. Cada una de sus columnas, incluidas las partes de una clave compuesta, pone su referencia en la Clase de su tabla.

Una clave foránea funciona igual. Cada columna del lado «muchos» de una relación declarada pone su referencia en la Clase de la tabla de destino, se llame como se llame. `manager_id` en `employees`, que apunta de vuelta a `employees`, termina en `employee`; el papel «manager» vive en el nombre de la relación. Una columna sin relación declarada es una Propiedad, aunque su nombre coincida con una Clase.

Provisa coloca las referencias de clave solo por derivación, cada vez que una tabla se sincroniza, cambia una relación o cambia la Clase de una tabla. No puede mover a mano una columna clave; el rechazo nombra la columna, su tabla y su Clase. Ancle una tabla en otro término y sus claves la siguen al instante.

### La misma entidad en varias tablas {: #the-same-entity-in-several-tables }

Las instancias de tablas distintas son distintas hasta que usted diga lo contrario. Cuando `customers` y `crm_contacts` describen al mismo cliente, marque como **misma entidad** la relación que las une. Debe unir dos tablas que ancla la misma Clase, uno a uno, sobre una clave de cada lado: la clave primaria o una restricción única declarada.

Mientras una relación de misma entidad exista sobre una tabla, su Clase no se puede retirar ni sustituir. Desmarque primero la relación.

Una Clase anclada en varias tablas que las relaciones de misma entidad no unen todas aparece en **Por revisar** como *tablas sin vincular*.

## Cómo se derivan los términos {: #how-terms-are-derived }

Provisa crea automáticamente un término en borrador a partir de cada nombre de columna, con una regla de normalización determinista: unificación de mayúsculas y minúsculas, tokenización por separadores y camelCase, expansión de abreviaturas y eliminación de tokens proxy finales.

La **expansión de abreviaturas** convierte las abreviaturas empresariales comunes en su forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, etc. Tanto `id` como `key` se expanden a `identifier`. La tabla es fija y prudente. Las abreviaturas ambiguas como `st`, `min` y `no` se quedan como están, para no adivinar nada mal.

La **eliminación de tokens proxy** quita un token final `identifier`, `code`, `index` o `reference`. Una columna llamada `cust_id` no nombra el identificador en sí; nombra a un cliente mediante un valor sustituto. Quitar el proxy hace que `cust_id` y `customerId` terminen en el término `customer`. Solo se quitan tokens finales, y nunca el último que queda, así que una columna `id` a secas se expande a `identifier` y se queda ahí.

La **deduplicación** es la razón de todo esto. La regla es determinista, así que `cust_id`, `customerId` y `CUSTOMER_KEY` producen todos `customer`. Cada columna recibe una referencia a ese único término, y la curación tiene un solo lugar para añadir la definición en vez de tres.

Las columnas clave se saltan esta regla. Sus referencias van a una Clase, como se describe arriba.

### Frases genéricas {: #generic-phrases }

Algunas frases normalizadas son demasiado genéricas para ser un concepto por sí mismas. Una columna `name`, `date` o `identifier` a secas nombra un atributo del concepto de su tabla, que no existe aparte de esa tabla. Los empleados tienen nombre y los productos tienen nombre, pero no son lo mismo.

Cuando una frase está en el conjunto genérico y hay un contexto de tabla, el término se califica como `<concepto de la tabla> <frase>`: `employees.first_name` se normaliza a `employee first name`.

El conjunto genérico abarca sustantivos de atributo (`name`, `date`, `status`, `type`, `amount`, `quantity`), frases de auditoría (`created_at`, `modified_by`, `submitted_timestamp`) y unas cuantas más que aparecen en casi todas las tablas.

### Los términos siguen el nombre de negocio {: #terms-follow-the-business-name }

Un término derivado sigue el **nombre de negocio** de la columna: su alias cuando el modelador definió uno, y su nombre físico en caso contrario. Cuando `usr_nm` tiene el alias `user name`, el término derivado es `user name`, donde expandir `usr_nm` podría haber dado `user number`.

Poner un alias a una columna es la corrección más fuerte. Un alias viaja a todas las superficies que leen la columna (SQL, GraphQL, agentes de IA, el catálogo), así que el modelo se describe correctamente en todas partes. Renombrar el término corrige una entrada del catálogo y deja la columna llamándose `usr_nm` para el siguiente lector. El aviso de borrador de la interfaz lo dice: ponga primero un alias a la columna, y renombre el término solo cuando el nombre de la columna sea correcto y el vocabulario no.

Cambiar el alias de una columna vuelve a derivar su término en borrador, así que la ontología sigue al modelo y nunca pide dos veces la misma corrección. Una vez que un curador ha añadido a un término una definición, una relación, un experto o una etiqueta, un cambio de alias deja la columna donde está, porque ese trabajo es del curador y se mantiene.

### Nombres de tabla de ruta de acceso {: #access-path-table-names }

Algunos nombres de tabla describen una ruta de acceso en lugar de un concepto: `user_by_name` es un usuario al que se llega mediante una búsqueda por nombre, y no un tipo de entidad aparte. Cuando Provisa deriva el concepto de una tabla, corta el nombre en el conector, así que `user_by_name` pasa a `user` y `orders_by_customer` pasa a `order`.

Sin el corte, `user_by_name` propondría una Clase `user name` que chocaría con el atributo real `users.name`. El corte se aplica solo a los conceptos de tabla. En un nombre de columna, `by` forma parte del sustantivo compuesto, así que `pet_by_name` y `pet_name` se normalizan al mismo término, `pet name`.

## Estado {: #status }

Un término tiene un estado a la vez.

| Estado | Significado | Cómo llega ahí |
| --- | --- | --- |
| **Borrador** | No disponible para agentes, búsqueda ni catálogos hasta que tenga una definición y llegue a los datos. | Todo término derivado empieza aquí. |
| **Activo** | Admitido: se ofrece a agentes, búsqueda y catálogos. | Definido, anclado y en servicio. |
| **Obsoleto** | De salida. | Un curador marca obsoleto un término activo, o un término curado pierde su última columna. |
| **Retirado** | Fuera de servicio, conservado como registro. | Un curador lo retira y nombra un sucesor. |

### La regla de admisión {: #the-admission-rule }

Un término está activo cuando cumple tres condiciones.

1. **En servicio.** No está retirado ni quedó obsoleto por perder su última columna. Un término que un curador marcó como obsoleto sigue en servicio hasta su retirada prevista.
2. **Definido.** Tiene una definición. Un término derivado de un nombre de columna es solo un token. Sin definición es un borrador a la espera de un curador, nunca vocabulario en el que un agente pueda basar una pregunta.
3. **Anclado.** Está conectado, a través de términos en servicio, con un término que tiene una columna o una tabla. La ontología es una puerta de entrada a los datos, así que toda cadena debe terminar en los datos.

La conectividad se propaga por el grafo. Un Concepto llega a los datos a través de cualquier vecino en servicio que lo haga. Los términos fuera de servicio no conducen, así que un término retirado no mantiene vivos a sus dependientes.

### Marcar un término obsoleto {: #deprecating-a-term }

Marque obsoleto un término activo cuando esté de salida pero aún se use. Da un motivo, que es obligatorio, y opcionalmente una fecha de retirada prevista, hoy o más adelante. El término sigue en servicio, y sigue contando para la regla de admisión, hasta esa fecha. Entonces el planificador lo retira.

Un término que debe retirarse pero aún tiene columnas o tablas no puede retirarse por fecha, porque solo un sucesor puede recibirlas. Espera, y **Por revisar** lo muestra como *retirada bloqueada* hasta que usted lo retire con un sucesor.

Un término curado que pierde su última columna se marca obsoleto automáticamente y se conserva. Sale de servicio, conserva el contenido que aportaron sus editores y vuelve si la misma columna reaparece. Un término sin curar que se queda sin columnas se elimina.

### Retirar un término {: #retiring-a-term }

Retirar un término mueve todo lo que tiene a un **sucesor** y luego lo saca de servicio.

- Las columnas de una Propiedad pasan al sucesor. Debe ser una Propiedad o un Concepto.
- Las tablas de una Clase pasan al sucesor, y sus claves las siguen. Debe ser una Clase o un Concepto.
- Un Concepto no tiene nada, así que su sucesor es opcional.

La retirada necesita un motivo y escribe una arista `REPLACES` del sucesor al término retirado. El término retirado queda como registro inerte: su definición, etiquetas, relaciones, expertos y el motivo. No conduce nada.

Como sus columnas pertenecen ahora al sucesor, el panel de un término retirado muestra a qué se **aplicó por última vez**: sus columnas y tablas tal como las tenía el modelo en el commit anterior a la retirada. El panel también enlaza al término que lo sustituye.

### Restaurar un término {: #restoring-a-term }

**Restaurar** deshace lo que un curador afirmó. Un término obsoleto deja de estarlo. Un término retirado vuelve como un Concepto vacío y en borrador, ya que sus columnas pasaron al sucesor, y se elimina su arista `REPLACES`.

### Qué hace que un término esté curado {: #what-makes-a-term-curated }

Un término nacido de la normalización de columnas empieza vacío. Pasa a estar curado cuando tiene cualquiera de estas cosas:

- una definición guardada
- una arista de relación
- un experto en la materia asignado
- una etiqueta, como `pii`
- una retirada

Los términos curados se conservan cuando sus columnas se van. La sincronización nunca vuelve a derivar las columnas de un término curado hacia otro.

## Resincronizar desde las tablas {: #re-syncing-from-tables }

Cada vez que se guarda o recarga una tabla, Provisa concilia sus columnas con la ontología. Las columnas nuevas crean o enlazan términos, las que desaparecen pierden sus referencias, y todo término que se queda sin referencias se elimina o se marca obsoleto según la regla de arriba. Las columnas clave van a su Clase, y una tabla con clave sin Clase propone una.

Solo se vuelven a derivar los términos sin curar. Si pone un alias a una columna y su término en borrador cambia, la columna pasa al nuevo término. Si el término está curado, la columna se queda, porque un cambio de alias no anula la elección de un curador.

Cuando eliminar un término dejaría un Concepto sin camino a los datos, Provisa marca obsoleto ese término en su lugar, para que la estructura que construyó se mantenga hasta que la vuelva a cablear.

## Relaciones {: #relationships }

Los términos se relacionan con otros términos mediante aristas tipadas.

| Tipo | Significado |
| --- | --- |
| `KIND_OF` | El término de origen es un tipo del término de destino. |
| `PART_OF` | El término de origen es un componente del término de destino. |
| `SYNONYM_OF` | Los dos términos son intercambiables en este dominio. |
| `RELATED_TO` | Una asociación laxa, cuando no encaja una afirmación más fuerte. |
| `VALID_VALUE_OF` | El origen es un valor permitido de la enumeración o dominio de destino. |
| `DERIVED_FROM` | El origen se calcula u obtiene a partir del destino. |
| `REPLACES` | El origen sustituye al destino. Retirar un término escribe una desde su sucesor. |
| `PREFERRED_TERM_FOR` | El origen es el término preferido frente al destino desaconsejado. |
| `TRANSLATION_OF` | El origen es una traducción del destino a otra configuración regional o idioma. |
| `ANTONYM_OF` | El origen es el opuesto semántico del destino. |

Las relaciones tienen dirección. La interfaz muestra las aristas salientes (este término → otro) y las entrantes (otro término → este término), y etiqueta cada dirección con su propia frase en lenguaje claro.

Las aristas viven en una tabla asociativa declarada como relación de unión. Su columna de tipo de relación es el discriminador, así que cada tipo de arriba es un tipo de relación de Cypher distinto entre dos nodos de término. Nada en ella es específico de la ontología: se declara igual que declararía una unión sobre sus propias tablas, y la lee el mismo código.

## Conceptos {: #concepts }

Un Concepto no tiene columnas ni tablas propias. Úselo para una idea de negocio que abarca varios términos concretos, y luego conéctelo con los términos que sí tienen datos. `revenue`, por ejemplo, podría ser un Concepto, con aristas `PART_OF` desde `order amount`, `adjustment amount` y `refund amount`.

Un Concepto que no llega a ninguna columna ni tabla a través del grafo de relaciones no nombra datos. No aparece en la búsqueda de términos de los agentes ni en la exportación de metadatos, y **Por revisar** lo muestra como *sin datos*. Los dominios se aplican solo a un Concepto. Cualquier otro término toma sus dominios de las tablas en las que está anclado o de las columnas que tiene.

## PII según el significado {: #pii-on-meanings }

La PII es una propiedad de lo que significa una columna, así que Provisa le permite marcarla una vez, en el término, y cubre cada columna con ese significado, incluidas las que se registren después.

La etiqueta `pii` va en una columna o en un término Propiedad, nunca en un origen, una tabla, una Clase o un Concepto. Un término debe estar definido y en servicio (activo u obsoleto) para llevarla. Marcar un término en borrador se rechaza con «Defina *término* para marcarlo», y no puede borrar la definición de un término marcado hasta que lo desmarque.

Cada columna de un término marcado lleva `pii`, y con ella `sensitive`, dondequiera que se lean las etiquetas: gobierno, enmascaramiento, sensibilidad, exportación y las vistas de etiquetas. No se copia nada en las columnas. Una columna movida al término lleva la etiqueta al instante, y una columna movida fuera ya no la lleva. Cada columna dice de dónde viene su etiqueta: puesta en la columna, con su motivo, o a través de un término, nombrándolo.

Cuando marca una columna cuyo término es una Propiedad, la interfaz pregunta si marcar también las demás columnas que significan lo mismo.

- **Sí** pone la etiqueta en el término.
- **Solo esta** marca solo la columna y pregunta por qué.

Una columna que es PII a través de su término no se puede desmarcar por sí sola. Desmarque el término, o mueva la columna al término que corresponda a lo que contiene.

### Movimientos que pierden etiquetas {: #moves-that-lose-tags }

Mover una columna a otro término puede quitarle una etiqueta: mover `customers.email` de `customer email` (marcado con `pii`) a un término sin marcar dejaría desprotegida la columna de correo. Cada movimiento que hace una persona se comprueba antes. Eso incluye reclamar o mover una columna, retirar un término y guardar o eliminar una relación que reubica columnas clave.

Cuando una columna dejaría de llevar `pii` por completo, el movimiento se rechaza hasta que usted decida. La interfaz muestra un resumen de lo que cada columna movida pierde y gana, primero las pérdidas.

- **Mover** sigue adelante, con cada etiqueta perdida confirmada.
- **Mover y marcar** lleva las etiquetas perdidas al término al que se mueven las columnas, en el mismo cambio.
- **Cancelar** no mueve nada.

Una columna que conserva la etiqueta mediante su propia marca no necesita confirmación. Los movimientos que hace el propio modelo, como volver a derivar las columnas de un borrador, nunca cambian una etiqueta heredada.

## Por revisar {: #to-review }

La ontología informa como hallazgos de lo que necesita su atención. Un término con algún hallazgo está **Por revisar**, y la lista de términos se puede filtrar por ello. Un borrador sin más no es un hallazgo; los borradores son el estado normal de la ontología.

| Hallazgo | Qué significa | La solución |
| --- | --- | --- |
| Tablas sin vincular | Una Clase ancla varias tablas que las relaciones de misma entidad no unen todas. | Marque relaciones de misma entidad entre ellas. |
| Clase y atributo | Una Clase tiene columnas que no son sus claves, o una tabla con clave no tiene Clase porque su concepto es una Propiedad. | Mueva las columnas a una Propiedad, o vuelva a anclar la tabla. |
| Etiqueta de tipo incorrecto | Un término que no es una Propiedad lleva una etiqueta que solo una Propiedad transmite a las columnas. | Quite la etiqueta. |
| Retirada bloqueada | Un término obsoleto ha pasado su retirada prevista pero aún tiene columnas o tablas. | Retírelo con un sucesor. |
| Sin datos | Un Concepto en servicio no llega a ningún dato. | Relaciónelo con un término que tenga datos. |

Los hallazgos informan; no cambian nada hasta que usted actúa.

## Exportación de metadatos {: #metadata-export }

La ontología se publica en catálogos de datos externos como parte de la exportación de metadatos. Solo se publican los términos activos, juzgados sobre las columnas y tablas que realmente se publican. Un término cuyas columnas se retienen todas de la exportación, porque sus tablas no son productos de datos o porque los filtros técnicos las excluyen, no se considera anclado para la exportación, aunque tenga columnas en el plano de control.

Un término obsoleto se publica con su estado, su motivo y su fecha de retirada prevista, para que cada catálogo lo asigne a su propia marca de obsolescencia.

Las aristas de relación se publican solo cuando se publican los dos términos de los extremos. Los activos de columna se exportan por separado, así que excluir un término no oculta los datos subyacentes.

### Excluir un término de la exportación {: #excluding-a-term-from-export }

Algunas columnas llevan fontanería, como identificadores de lote de ETL, versiones de fila y marcas de tiempo de ingesta. Un término derivado de una columna así puede tener una definición precisa y aun así no ser vocabulario de negocio. El control **Excluir de la exportación de metadatos** retiene el término, y cualquier arista de relación que termine en él, de los catálogos en los que publica Provisa. Las columnas siguen exportándose como activos.

Lo que cuenta es si el negocio usa la palabra. Un identificador de lote de ETL tiene un significado claro que pertenece a la ontología para los ingenieros, pero no a un catálogo de negocio junto a `customer` y `revenue`.

## Trabajar con la ontología {: #working-with-the-ontology }

Abra **Ontología** en la barra de navegación. El panel izquierdo lista cada término con su rol y, salvo que esté activo, su estado. Las casillas **Obsoleto** y **Borrador** muestran u ocultan esos términos, y **Por revisar** reduce la lista a los términos con hallazgos. Haga clic en un término para abrir su panel.

El panel muestra el rol y el estado del término y, para una Propiedad, su marca PII con el número de columnas que cubre. Un término obsoleto o retirado muestra quién cambió su estado y por qué. Un término con hallazgos se abre con una sección **Por revisar**: haga clic en un hallazgo para ver qué se encontró y la acción que lo resuelve.

**Editar** abre el término completo como un formulario. Guardar aplica todos los cambios juntos.

- **Renombre** el término para cambiar su redacción sin mover sus columnas.
- **Añada una definición** escribiéndola, o haga clic en el botón de borrador de IA para generar un punto de partida a partir del nombre del término, sus columnas y sus relaciones. El borrador no se guarda hasta que guarda el formulario.
- **Reclame columnas** de otros términos para este, o elija **Mover a término** junto a una de sus columnas. Cada columna pertenece a un término. Las columnas clave siguen a su Clase y no se pueden mover.
- **Ancle tablas** en una Clase, o quítelas.
- **Marque PII** en una Propiedad.
- **Añada una relación**, eligiendo su tipo. Cambie el tipo de una arista existente en su sitio en lugar de eliminarla y volver a crearla.
- **Asigne expertos** por ID de usuario, como `expert` o `author`.

Junto a Editar, el panel ofrece las acciones de estado que permite el término: **Marcar obsoleto** un término activo, **Retirar** cualquier término en servicio y **Restaurar** uno obsoleto o retirado.

En el menú ✨, **Definiciones** rellena cada definición vacía en una pasada y nunca sobrescribe el texto de una persona. **Relaciones** propone aristas tipadas para toda la lista de términos; las propuestas con términos desconocidos, el mismo término dos veces o un tipo no reconocido se descartan.

El aviso **Borrador** de un término le indica si está sin definir (ponga un alias a la columna o añada una definición) o sin anclar (relaciónelo con un término que tenga datos). Mientras lo vea, los agentes y los catálogos no pueden llegar al término.

## Véase también {: #see-also }

- [Exportación de metadatos](metadata-export.md) explica cómo se publican los términos y las relaciones en catálogos de datos externos.
- [Linaje a nivel de columna](lineage.md) explica el explorador de linaje y cómo una columna física informa de los términos vinculados a ella.
