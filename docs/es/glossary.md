# Glosario de negocio

El glosario de negocio es un vocabulario vivo sobre su modelo de datos. Cada columna física de la
capa semántica se resuelve a un término: un único término compartido siempre que varias columnas
lleven el mismo concepto, por distinta que sea su escritura. Cada término puede contener una
definición, un conjunto de relaciones tipadas con otros términos y una lista de expertos en la
materia que son dueños del significado.

Ese vocabulario compartido es el puente entre el lenguaje de negocio y los datos físicos. Un agente
de IA que sabe que "customer" nombra todas las columnas que llevan un identificador de cliente no
tiene que adivinar cuál de `cust_id`, `customerId` y `CUSTOMER_KEY` es la correcta: todas se
resuelven al mismo término, y el término lleva la definición.

## Cómo se derivan los términos

Provisa deriva automáticamente un término de cada nombre de columna mediante una regla de
normalización determinista (REQ-1387): plegado de mayúsculas y minúsculas, tokenización por
separadores y camelCase, expansión de abreviaturas y eliminación de los tokens proxy finales.

**La expansión de abreviaturas** asigna las abreviaturas empresariales habituales a su forma
completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity`, etcétera. Tanto `id` como
`key` se expanden a `identifier`. La tabla es fija y conservadora: las abreviaturas ambiguas como
`st`, `min` y `no` se dejan tal cual en vez de adivinar mal.

**La eliminación de tokens proxy** quita un token final `identifier`, `code`, `index` o
`reference`. Una columna llamada `cust_id` no nombra al identificador en sí; nombra a un cliente a
través de un valor sustituto. Eliminar el proxy hace que `cust_id` y `customerId` aterricen ambos en
el término `customer`. Solo se eliminan los tokens finales, y nunca el último token que queda: una
columna `id` desnuda se expande a `identifier` y ahí se queda.

**La deduplicación** es el objetivo. La regla de normalización es determinista, así que `cust_id`,
`customerId` y `CUSTOMER_KEY` producen todos `customer`. Cada columna obtiene una referencia al
único término resultante en vez de tres términos separados. La curación tiene entonces un solo
lugar donde añadir la definición, no tres.

### Frases genéricas

Algunas frases normalizadas son demasiado genéricas para ser un concepto por sí solas. Una columna
`name`, `date` o `identifier` desnuda nombra un atributo del concepto de su tabla, no un concepto
independiente de esa tabla. Los empleados tienen nombres; los productos tienen nombres; no son lo
mismo.

Cuando una frase cae en el conjunto genérico y hay un contexto de tabla disponible, el término se
califica a `<concepto de tabla> <frase>`: `employees.first_name` se normaliza a `employee first
name`, y `orders.id` se normaliza a `order`, porque la eliminación del proxy colapsa entonces la
frase calificada sobre el concepto que identifica. Ese último caso es importante: la clave primaria
de `orders` y todas las claves foráneas `order_id` de otras tablas aterrizan en `order`, sin
necesidad de curación adicional.

El conjunto genérico cubre sustantivos de atributo (`name`, `date`, `status`, `type`, `amount`,
`quantity`), frases de rastro de auditoría (`created_at`, `modified_by`, `submitted_timestamp`) y
unas cuantas más que aparecen en casi todas las tablas.

### El nombre de negocio, no el nombre físico

Un término derivado sigue el **nombre de negocio** de la columna: su alias cuando el modelador
estableció uno, y su nombre físico cuando no lo hizo (REQ-1581). Cuando `usr_nm` lleva el alias
`user name`, el término derivado es `user name`, no `user number` ni alguna expansión de `usr_nm`.

Poner un alias a una columna es la corrección más fuerte. Un alias viaja a todas las superficies que
leen la columna —SQL, GraphQL, agentes de IA, el catálogo—, de modo que el modelo se describe
correctamente en todas partes. Renombrar un término arregla una entrada del catálogo y deja la
columna leyéndose como `usr_nm` para el siguiente lector. El banner de término propuesto de la
interfaz lo dice directamente: ponga primero el alias a la columna; renombre el término solo cuando
el nombre de la columna sea correcto y el vocabulario no lo sea.

Volver a poner un alias a una columna vuelve a derivar su término propuesto, de modo que el glosario
sigue al modelo en vez de pedir dos veces la misma corrección. Una vez que un curador ha añadido una
definición, una relación o un experto a un término, una edición del alias no mueve la referencia:
ese trabajo es del curador, y se queda.

### Nombres de tabla que describen una ruta de acceso

Algunos nombres de tabla describen una ruta de acceso en vez de un concepto: `user_by_name` es un
usuario alcanzado mediante una búsqueda por nombre, no una clase distinta de entidad. Cuando Provisa
deriva el concepto de tabla para calificar una frase genérica, corta el nombre en el conector
(REQ-1582). `user_by_name` se convierte en `user`; `orders_by_customer` se convierte en `order`.

Sin ese corte, la clave sustituta de `user_by_name` se normalizaría a `user name` y chocaría con el
atributo genuino `users.name`: un solo término conteniendo una cosa y uno de sus propios campos. El
corte se aplica solo a los conceptos de tabla. En un nombre de columna, `by` forma parte del
sustantivo compuesto: `pet_by_name` y `pet_name` se normalizan al mismo término, `pet name`.

## Qué hace que un término esté curado

Un término nacido de la normalización de una columna empieza en blanco: es una propuesta, todavía no
es vocabulario. Pasa a estar curado cuando se cumple cualquiera de estas condiciones:

- Se ha guardado una definición.
- Se ha añadido una arista de relación.
- Se ha asignado un experto en la materia.
- Un curador lo ha retirado manualmente.

La curación importa para el ciclo de vida del término. Cuando se elimina del modelo la última
columna física de un término curado, el término se marca como obsoleto en vez de eliminarse: queda
fuera de servicio, conserva el contenido aportado por sus editores y se revive automáticamente si la
misma columna reaparece. Un término no curado que se queda sin columnas simplemente se elimina.

## Resincronización desde las tablas

Cada vez que se guarda o se recarga una tabla, `sync_table_refs` reconcilia las columnas de esa tabla
con las referencias existentes. Las columnas nuevas crean o enlazan términos; las columnas que
desaparecen sueltan sus referencias; y la regla de eliminar-o-marcar-obsoleto resuelve cualquier
término que pierda su última referencia.

La rederivación ocurre únicamente para los términos no curados. Si puso un alias a una columna y el
término propuesto ahora es distinto, la referencia se mueve al término nuevo. Si el término está
curado, el enlace se mantiene: la edición del alias no anuló la elección de término del curador.

Un término abstracto cuya única ruta a los datos físicos pasaba por un término que desaparece se
marca como obsoleto en vez de eliminarse, preservando la estructura conceptual hasta que se vuelva a
conectar.

## Relaciones

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
(junction) (REQ-1586): su columna `rel_type` es el discriminador, de modo que cada uno de los tipos
anteriores es un tipo de relación Cypher distinto entre dos nodos `GlossaryTerm`, y no una propiedad
de un nodo reificado. La tabla se aprovisiona con el resto del esquema de metadatos y no se muestra como
nodo en los clientes de grafo — es la arista. Nada en ella es específico del glosario: se declara igual que
declararías una unión sobre tus propias tablas, y la lee el mismo código.
[tool-verified: `provisa/cypher/label_map.py:378-397`, `provisa/api/startup_seed.py:508-550`]

## Términos abstractos

Un término abstracto no tiene referencias propias a columnas físicas. Use uno para un concepto de
negocio que abarque varios términos concretos: un paraguas que después conecta con los términos
específicos que sí tienen columnas. `revenue`, por ejemplo, podría ser abstracto, con aristas
`PART_OF` que apunten hacia él desde `order amount`, `adjustment amount` y `refund amount`.

Un término abstracto que no puede alcanzar ninguna columna física a través del grafo de relaciones es
una propuesta colgante. No aparece en la búsqueda de términos de los agentes ni en la exportación de
metadatos: un término que no nombra datos no puede responder nada.

## La regla de admisión para las superficies de consumo

Un término que una superficie de consumo puede ofrecer debe cumplir tres condiciones (REQ-1387):

1. **En servicio**: ni retirado (un curador lo sacó de servicio) ni obsoleto (perdió su última
   columna y se conservó solo porque eliminarlo dejaría algo colgando).
2. **Definido**: lleva una definición. Un término derivado de un nombre de columna es un token, no un
   significado. Sin definición es una propuesta a la espera de un curador, nunca vocabulario sobre el
   que un agente pueda fundamentar una pregunta.
3. **Anclado**: conectado, a través de términos en servicio, con al menos un término que tenga una
   referencia a una columna física. El glosario es un punto de entrada a los datos, así que toda
   cadena debe terminar en una columna.

La conectividad se propaga por el grafo: un término abstracto alcanza los datos a través de
cualquier vecino en servicio que lo haga. Los términos fuera de servicio no conducen: un término
retirado no mantiene vivos a sus dependientes.

## Exportación de metadatos

El glosario se publica en catálogos de datos externos como parte de la exportación de metadatos. Se
aplica la misma regla de admisión, con un estrechamiento: el enraizamiento de un término se juzga
solo frente a las columnas que se publican realmente. Un término cuyas columnas quedan todas fuera
de la exportación —porque sus tablas no están marcadas como productos de datos, o porque los filtros
técnicos las excluyen— no está enraizado a efectos de la exportación, aunque tenga referencias en el
plano de control.

Las aristas de relación se publican solo cuando se publican los términos de ambos extremos.

Los activos de columna se exportan de forma independiente. Que un término quede excluido no oculta
los datos subyacentes.

### Excluir un término de la exportación

Algunas columnas llevan fontanería en vez de datos de negocio: identificadores de lote de ETL,
versiones de fila, marcas de tiempo de ingesta. Un término derivado de una columna así puede tener
una definición perfectamente exacta que sencillamente no es vocabulario de negocio (REQ-1583). El
control **Excluir de la exportación de metadatos** retiene el término, y cualquier arista de
relación que termine en él, frente a los catálogos en los que publica Provisa, mientras que las
columnas en sí siguen exportándose como activos.

La prueba es si el negocio usa esta palabra, no si la definición es buena. Un identificador de lote
de ETL tiene un significado claro que corresponde al glosario para el equipo de ingeniería; no
corresponde a un catálogo de negocio junto a `customer` y `revenue`.

## Trabajar con el glosario

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

## Véase también

- [Exportación de metadatos](metadata-export.md) — cómo se publican los términos y las relaciones en
  catálogos de datos externos, incluido qué términos admite la regla de admisión de la exportación.
- [Linaje a nivel de columna](lineage.md) — el explorador de linaje y cómo `columnDependents`
  informa de los vínculos del glosario como dependientes de una columna física.
