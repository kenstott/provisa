# Glosario de negocio {: #business-glossary }

El glosario de negocio es un vocabulario vivo sobre su modelo de datos. Cada columna física del modelo se resuelve en un término, y las columnas que llevan el mismo concepto comparten un término, por distinto que lo escriban. Cada término puede tener una definición, un conjunto de relaciones tipadas con otros términos y una lista de expertos en la materia responsables de su significado.

El glosario es una ontología: un grafo de conceptos, sus definiciones y las relaciones entre ellos. Provisa lo asigna a las estructuras de base de datos que contienen los valores, lo que le permite responder preguntas sobre sus datos. Un concepto es un borrador hasta que llega a los datos, directamente o a través de otros conceptos. El glosario es independiente de las métricas: un término dice qué es un concepto, una métrica dice cómo se calcula una cifra.

Ese vocabulario compartido une el lenguaje de negocio con los datos físicos. Un agente de IA que sabe que "customer" nombra cada columna que lleva un identificador de cliente no tiene que adivinar cuál de `cust_id`, `customerId` y `CUSTOMER_KEY` es la buena. Todas se resuelven en el mismo término, y el término lleva la definición.

## Cómo se derivan los términos {: #how-terms-are-derived }

Provisa deriva automáticamente un término de cada nombre de columna mediante una regla de
normalización determinista: plegado de mayúsculas y minúsculas, tokenización por
separadores y camelCase, expansión de abreviaturas y eliminación de los tokens proxy finales.

La **expansión de abreviaturas** asigna a las abreviaturas empresariales habituales su forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, etc. Tanto `id` como `key` se expanden a `identifier`. La tabla es fija y prudente: las abreviaturas ambiguas como `st`, `min` y `no` se quedan como están, para no adivinar mal.

**La eliminación de tokens proxy** quita un token final `identifier`, `code`, `index` o
`reference`. Una columna llamada `cust_id` no nombra al identificador en sí; nombra a un cliente a
través de un valor sustituto. Eliminar el proxy hace que `cust_id` y `customerId` aterricen ambos en
el término `customer`. Solo se eliminan los tokens finales, y nunca el último token que queda: una
columna `id` desnuda se expande a `identifier` y ahí se queda.

La **deduplicación** es el objetivo. La regla de normalización es determinista, así que `cust_id`, `customerId` y `CUSTOMER_KEY` producen todos `customer`. Cada columna recibe una referencia a ese único término, y la curación tiene un solo lugar donde añadir la definición en vez de tres.

### Frases genéricas {: #generic-phrases }

Algunas frases normalizadas son demasiado genéricas para ser un concepto por sí solas. Una columna `name`, `date` o `identifier` a secas nombra un atributo del concepto de su tabla, que no existe aparte de esa tabla. Los empleados tienen nombre y los productos tienen nombre, pero no son lo mismo.

Cuando una frase cae en el conjunto genérico y hay un contexto de tabla disponible, el término se
califica a `<table concept> <phrase>`: `employees.first_name` se normaliza a `employee first name`,
y `orders.id` se normaliza a `order`, porque la eliminación del proxy colapsa entonces la
frase calificada sobre el concepto que identifica. Ese último caso es importante: la clave primaria
de `orders` y todas las claves foráneas `order_id` de otras tablas aterrizan en `order`, sin
necesidad de curación adicional.

El conjunto genérico cubre sustantivos de atributo (`name`, `date`, `status`, `type`, `amount`,
`quantity`), frases de rastro de auditoría (`created_at`, `modified_by`, `submitted_timestamp`) y
unas cuantas más que aparecen en casi todas las tablas.

### El nombre de negocio, no el nombre físico {: #the-business-name-not-the-physical-name }

Un término derivado sigue el **nombre de negocio** de la columna: su alias si el modelador puso uno y, si no, su nombre físico. Cuando `usr_nm` tiene el alias `user name`, el término derivado es `user name`, cuando expandir `usr_nm` podría haber dado `user number`.

Poner un alias a la columna es la corrección más fuerte. Un alias llega a todas las superficies que leen la columna (SQL, GraphQL, agentes de IA, el catálogo), así que el modelo se describe bien en todas partes. Renombrar el término arregla una entrada del catálogo y deja la columna como `usr_nm` para el siguiente lector. El aviso de término propuesto en la UI lo dice así: primero ponga un alias a la columna, y renombre el término solo cuando el nombre de la columna sea correcto y el vocabulario no.

Cambiar el alias de una columna vuelve a derivar su término propuesto, así que el glosario sigue al modelo y nunca pide dos veces la misma corrección. Cuando un curador ya ha añadido a un término una definición, una relación o un experto, una edición del alias deja la referencia donde está, porque ese trabajo es del curador y se mantiene.

### Nombres de tabla que describen una ruta de acceso {: #access-path-table-names }

Algunos nombres de tabla describen una vía de acceso y no un concepto: `user_by_name` es un usuario al que se llega buscando por nombre, y no un tipo de entidad aparte. Cuando Provisa deriva el concepto de la tabla para calificar frases genéricas, corta el nombre en el conector, así que `user_by_name` pasa a `user` y `orders_by_customer` pasa a `order`.

Sin el corte, la clave sustituta de `user_by_name` se normalizaría a `user name` y chocaría con el atributo real `users.name`, dando un término que contiene a la vez una cosa y uno de sus propios campos. El corte solo se aplica a conceptos de tabla. En un nombre de columna, `by` forma parte del sustantivo compuesto, así que `pet_by_name` y `pet_name` se normalizan al mismo término, `pet name`.

## Qué hace que un término esté curado {: #what-makes-a-term-curated }

Un término que nace de normalizar columnas empieza en blanco, como una propuesta que aún no es vocabulario. Pasa a estar curado cuando se cumple cualquiera de estas condiciones:

- Se ha guardado una definición.
- Se ha añadido una arista de relación.
- Se ha asignado un experto en la materia.
- Un curador lo ha retirado manualmente.

La curación importa para el ciclo de vida del término. Cuando la última columna física de un término curado sale del modelo, el término se deprecia y se conserva: deja de estar en servicio, guarda el contenido que aportaron sus editores y vuelve automáticamente si reaparece la misma columna. Un término sin curar que se queda sin columnas simplemente se elimina.

## Resincronización desde las tablas {: #re-syncing-from-tables }

Cada vez que se guarda o se recarga una tabla, `sync_table_refs` reconcilia las columnas de esa tabla
con las referencias existentes. Las columnas nuevas crean o enlazan términos; las columnas que
desaparecen sueltan sus referencias; y la regla de eliminar-o-marcar-obsoleto resuelve cualquier
término que pierda su última referencia.

Solo se vuelven a derivar los términos sin curar. Si puso un alias a una columna y el término propuesto ahora es otro, la referencia pasa al término nuevo. Si el término está curado, el vínculo se mantiene, porque una edición del alias no anula la elección de término del curador.

Un término abstracto cuya única ruta a los datos físicos pasaba por un término que desaparece se
marca como obsoleto en vez de eliminarse, preservando la estructura conceptual hasta que se vuelva a
conectar.

## Relaciones {: #relationships }

Los términos se relacionan con otros términos mediante aristas tipadas. Los tipos de relación
admitidos son:

| Tipo | Significado |
| --- | --- |
| `KIND_OF` | El término de origen es una clase del término de destino. |
| `PART_OF` | El término de origen es un componente del término de destino. |
| `SYNONYM_OF` | Los dos términos son intercambiables en este dominio. |
| `RELATED_TO` | Una asociación laxa: no encaja ninguna afirmación más fuerte. |
| `VALID_VALUE_OF` | El origen es un valor permitido de la enumeración o el dominio de destino. |
| `DERIVED_FROM` | El origen se calcula u obtiene a partir del destino. |
| `REPLACES` | El origen sustituye al destino obsoleto. |
| `PREFERRED_TERM_FOR` | El origen es el término preferido frente al destino desaconsejado. |
| `TRANSLATION_OF` | El origen es una traducción a otra configuración regional o idioma del destino. |
| `ANTONYM_OF` | El origen es el opuesto semántico del destino. |

Las relaciones son direccionales. La interfaz muestra tanto las aristas salientes (este término →
otro) como las entrantes (otro término → este término), y etiqueta cada dirección con su propia
frase en lenguaje llano.

Las aristas viven en `glossary_term_edges`, una tabla asociativa declarada como relación de unión
(junction): su columna `rel_type` es el discriminador, de modo que cada uno de los tipos
anteriores es un tipo de relación Cypher distinto entre dos nodos `GlossaryTerm`, y no una propiedad
de un nodo reificado. La tabla se aprovisiona con el resto del esquema de metadatos y no se muestra como
nodo en los clientes de grafo — es la arista. Nada en ella es específico del glosario: se declara igual que
declararías una unión sobre tus propias tablas, y la lee el mismo código.

## Términos abstractos {: #abstract-terms }

Un término abstracto no tiene referencias propias a columnas físicas. Use uno para un concepto de
negocio que abarque varios términos concretos: un paraguas que después conecta con los términos
específicos que sí tienen columnas. `revenue`, por ejemplo, podría ser abstracto, con aristas
`PART_OF` que apunten hacia él desde `order amount`, `adjustment amount` y `refund amount`.

Un término abstracto que no puede llegar a ninguna columna física a través del grafo de relaciones es una propuesta colgante. No aparece en la búsqueda de términos de los agentes ni en la exportación de metadatos, porque un término que no nombra datos no puede responder nada.

## La regla de admisión para las superficies de consumo {: #the-admission-rule-for-consuming-surfaces }

Un término que una superficie de consumo puede ofrecer debe cumplir tres condiciones:

1. **En servicio**: ni retirado (un curador lo sacó de servicio) ni obsoleto (perdió su última
   columna y se conservó solo porque eliminarlo dejaría algo colgando).
2. **Definido**: lleva una definición. Un término derivado de un nombre de columna es un token, no un
   significado. Sin definición es una propuesta a la espera de un curador, nunca vocabulario sobre el
   que un agente pueda fundamentar una pregunta.
3. **Anclado**: conectado, a través de términos en servicio, con al menos un término que tenga una
   referencia a una columna física. El glosario es un punto de entrada a los datos, así que toda
   cadena debe terminar en una columna.

La conectividad se propaga por el grafo: un término abstracto llega a los datos a través de cualquier vecino en servicio que llegue. Los términos fuera de servicio no transmiten, así que un término retirado no mantiene vivos a sus dependientes.

## Exportación de metadatos {: #metadata-export }

El glosario se publica en catálogos de datos externos como parte de la exportación de metadatos. Se aplica la misma regla de admisión, con un matiz más estricto: si un término está enraizado se juzga solo con las columnas que de verdad se publican. Un término cuyas columnas se retienen todas de la exportación, porque sus tablas no están marcadas como productos de datos o porque los filtros técnicos las excluyen, no está enraizado a efectos de exportación, aunque tenga referencias en el plano de control.

Las aristas de relación se publican solo cuando se publican los términos de ambos extremos.

Los activos de columna se exportan de forma independiente. Que un término quede excluido no oculta
los datos subyacentes.

### Excluir un término de la exportación {: #excluding-a-term-from-export }

Algunas columnas llevan fontanería en vez de datos de negocio: identificadores de lote de ETL,
versiones de fila, marcas de tiempo de ingesta. Un término derivado de una columna así puede tener
una definición perfectamente exacta que sencillamente no es vocabulario de negocio. El
control **Excluir de la exportación de metadatos** retiene el término, y cualquier arista de
relación que termine en él, frente a los catálogos en los que publica Provisa, mientras que las
columnas en sí siguen exportándose como activos.

Lo que cuenta es si el negocio usa la palabra; lo buena que sea la definición no lo decide. Un identificador de lote de ETL tiene un significado claro que pertenece al glosario para los ingenieros, pero no a un catálogo de negocio junto a `customer` y `revenue`.

## Trabajar con el glosario {: #working-with-the-glossary }

Abra **Administración → Glosario** en la interfaz. El panel izquierdo lista todos los términos; haga
clic en uno para abrir su vista de detalle. Desde ahí:

- **Renombre** el término para cambiar su redacción sin mover sus columnas.
- **Añada una definición** escribiéndola o haciendo clic en el botón de borrador con IA para generar
  un punto de partida a partir del nombre del término, sus columnas físicas y sus relaciones. El
  borrador no se guarda hasta que lo confirme.
- **Mueva una referencia** para consolidar dos términos: elija el término de destino en la lista
  desplegable que hay junto a cualquier referencia física. Si el término de origen pierde su última
  referencia, se resuelve automáticamente bajo la regla de eliminar-o-marcar-obsoleto.
- **Añada una relación** entre este término y otro, eligiendo el tipo del conjunto cerrado. Vuelva a
  tipificar una arista existente en su sitio en vez de eliminarla y volver a añadirla.
- **Asigne expertos** por ID de usuario, con la clase `expert` o `author`.
- **Retire** un término para sacarlo de servicio. Conserva sus columnas y sigue siendo editable aquí,
  pero tanto la búsqueda de términos de los agentes como la exportación de metadatos lo omiten.
  Restáurelo más adelante si el concepto vuelve.
- **Genere definiciones de forma masiva** para rellenar todas las definiciones en blanco en una sola
  pasada. Solo se escriben las definiciones vacías; el texto humano nunca se sobrescribe.
- **Genere relaciones de forma masiva** para proponer aristas tipadas en toda la lista de términos.
  Las propuestas mal formadas —nombres de término desconocidos, auto-aristas, tipos no
  reconocidos— se descartan automáticamente.

El banner **Propuesto** de un término sin definición le indica si el término está indefinido (ponga
un alias a la columna o añada una definición) o sin anclar (relaciónelo con un término que tenga
columnas). Cuando lo vea, el término todavía no es alcanzable por los agentes ni por los catálogos.

## Véase también {: #see-also }

- [Exportación de metadatos](metadata-export.md) — cómo se publican los términos y las relaciones en
  catálogos de datos externos, incluido qué términos admite la regla de admisión de la exportación.
- [Linaje a nivel de columna](lineage.md) — el explorador de linaje y cómo `columnDependents`
  informa de los vínculos del glosario como dependientes de una columna física.
