# Regiones {: #regions }

Provisa le da dos primitivas para ubicar datos: una **región** y la **asignación de región**, es decir, crear una región y asignarle una tabla. Ninguna es por sí sola una política. Para qué las use se reparte en tres grupos:

- **Rendimiento.** La latencia hacia quienes leen, lo reciente que es su copia, y la adecuación del motor: dónde está su centro de datos, qué tipo de almacenamiento lee, cuánta CPU y memoria tiene.
- **Coste.** Dónde se ejecuta el trabajo y sobre qué: hardware propio o nube, un motor grande o uno pequeño, y lo que cuesta mover datos entre lugares (egress).
- **Soberanía.** Quién controla los datos y adónde pueden ir: las reglas de residencia de la ley (dónde pueden procesarse y dónde pueden almacenarse los datos personales) y las líneas de control propias de una organización, como una unidad de negocio o una filial que mantiene sus datos e infraestructura bajo su propio control.

La mecánica es la misma para los tres. Solo cambian las reglas que usted escribe encima.

## Una región es un motor con sus almacenes en un lugar {: #a-region-is-an-engine-and-its-stores-in-a-place }

Una región es un lugar donde Provisa trabaja y guarda lo que el trabajo deja. Cada región tiene su propio motor y sus propios almacenes. (REQ-1922) [tool-verified: `provisa/core/regions.py` `OrgRegion`, `STORE_ROLES`]

| Almacén | Qué contiene |
| --- | --- |
| `engine` | El motor de federación que ejecuta sentencias en esta región |
| `replicas` | Réplicas de las tablas asignadas aquí y de las tablas sin región que esta región copia |
| `views` | Las copias de vistas materializadas de esta región |
| `cache` | Respuestas en caché |
| `state` | Estado de construcción y actualización |
| `record` | El registro de cada solicitud atendida aquí |

Un solo modelo gobierna todas las regiones. Orígenes, tablas, relaciones, roles, reglas de filas y columnas, vistas y ajustes de frescura se declaran una vez, en un almacén de modelo, y todas las regiones sirven ese mismo modelo, de modo que las definiciones no pueden divergir entre regiones. Un cambio llega a cada región con la misma marca, y cada región converge después en él. (REQ-1914, REQ-1922)

La plataforma declara las regiones físicas en las que se ejecutan sus nodos, con la dirección en la que responde cada región. El modelo de cada organización elige cuáles de esas regiones usa y nombra su propio almacén para cada rol en cada región. Un nodo arranca en una región con `provisa run --region <id>` y sirve esa región para cada organización que la elige. Con regiones declaradas, un nodo arrancado sin `--region` se niega a arrancar y enumera las regiones que podría servir. (REQ-1922) [tool-verified: `provisa/cli.py` `--region`; `provisa/core/regions.py` `validate_regions`]

```yaml
platform:
  regions:
    - { id: eu, address: "https://eu.provisa.example" }
    - { id: us, address: "https://us.provisa.example" }

stores:
  - { id: eu_engine, url: "${secret:eu-trino-url}", kind: trino }
  - { id: us_engine, url: "${secret:us-trino-url}", kind: trino }
  - { id: eu_data,   url: "${secret:eu-pg-url}" }
  - { id: us_data,   url: "${secret:us-pg-url}" }
  - { id: eu_cache,  url: "${secret:eu-redis-url}" }
  - { id: us_cache,  url: "${secret:us-redis-url}" }

regions:
  - { id: eu, engine: eu_engine, replicas: eu_data, views: eu_data, cache: eu_cache, state: eu_data, record: eu_data }
  - { id: us, engine: us_engine, replicas: us_data, views: us_data, cache: us_cache, state: us_data, record: us_data }
```
[inferred: the URLs and secret names are placeholders; the shape follows `PlatformRegion`, `StoreConfig` and `OrgRegion` in `provisa/core/regions.py`]

Al cargarse, la configuración se rechaza, nombrando el motivo, si las regiones no encajan. Una región debe nombrar un almacén declarado para cada rol. Sus `replicas` y `views` deben ser el mismo almacén, y su almacén de motor debe nombrar un `kind` de motor. Con más de una región de plataforma, se rechaza un archivo de motor embebido, porque las demás regiones no pueden alcanzarlo. [tool-verified: `require_one_materialize_store`, `require_engine_kind`, `require_reachable_engine`]

Varias regiones pueden compartir una instancia de base de datos, un Redis o un bucket de almacenamiento de objetos. Todo lo que guarda una región lleva la región en su nombre, así que sus almacenes no chocan. (REQ-1922)

Un despliegue que no declara regiones tiene una región implícita y no muestra la región en ningún sitio. No hay campo de región ni opción `--region`, y una configuración que nombra una región se rechaza. (REQ-1922)

## Asignación de región: una tabla nombra su región de origen {: #region-homing-a-table-names-its-home }

Una tabla puede nombrar una de las regiones de su organización como su región de origen. Esa región es la única que construye y actualiza su réplica, y la única que puede mantenerla caliente. Todas las demás regiones leen la tabla de la réplica de la región de origen, in situ, en la dirección que da el modelo. Las reglas de filas y columnas de quien lee entran en esa sentencia, de modo que el filtrado ocurre en la región de origen y solo cruza lo que quien lee puede ver. (REQ-1921) [tool-verified: `provisa/core/regions.py` module docstring]

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```
Algunas consecuencias:

- Ninguna otra región lee el origen de la tabla; solo leen la réplica de la región de origen. Un origen que usted replica para protegerlo de la carga sigue protegido.
- Si la réplica no se ha construido o la región de origen no está accesible, la solicitud se rechaza nombrando la región de origen. El origen no se lee en vivo en su lugar.
- Una tabla asignada que no se replica, con `replicate` Never o con un ajuste que aún no ha pedido una copia, la lee en vivo desde su origen cada región, como cualquier tabla no replicada.
- El recuento caliente de una tabla es solo el tráfico de su región de origen. (REQ-239)
- Una vista materializada también puede nombrar una región. Entonces se construye solo allí, y todas las demás regiones leen esa copia, igual que con una tabla asignada.

Una región que lee la réplica de otra necesita un motor que pueda leerla: `duckdb`, `motherduck`, `pg`, `trino` o `trino-byo`. El almacén `replicas` de la región de origen debe ser un servidor PostgreSQL. La configuración nombra ambos cuando no encajan. [tool-verified: `require_readable_elsewhere`; `REGION_READERS` in `provisa/core/engine_kinds.py`]

Un origen también puede nombrar una región, pero esa región solo rellena el formulario cuando usted registra una tabla nueva del origen. No decide nada sobre dónde viven los datos, y cambiarla después no mueve ninguna tabla. (REQ-1921)

## Las tablas sin región se copian donde se leen {: #unhomed-tables-are-copied-where-they-are-read }

Una tabla que no nombra región puede copiarse en todas las regiones. Cuando su ajuste `replicate` pide una copia, la región a la que se conectó quien lee construye una en su propio almacén. Una vista materializada que no nombra región se construye en todas las regiones, cada copia en el almacenamiento de vistas de esa región. (REQ-1921)

Añadir una región significa, por tanto, más copias y más trabajo de actualización. Nunca significa más gobierno. Un modelo gobierna todas las copias, y en cada región se aplican las mismas reglas de filas y columnas. La construcción de una vista en una región se gobierna como el administrador de la organización actuando en esa región, de modo que la copia de cada región contiene lo que puede guardarse allí. (REQ-1921, REQ-1922)

## Tres motivos para usar regiones {: #three-reasons-to-use-regions }

### Rendimiento {: #performance }

Las regiones usadas solo por rendimiento no declaran reglas entre regiones. Las tablas, los roles y las reglas son los que escribiría para una sola región, y la asignación de región decide dónde ocurre el trabajo:

- **Latencia.** Quienes leen se conectan a la región cercana, y su motor, su caché y sus copias son locales para ellos.
- **Frescura.** Una tabla asignada donde cambia se actualiza allí, cerca de su origen.
- **Adecuación del motor.** Cada región ejecuta su propio motor, así que puede ajustar un motor a un lugar: el centro de datos en el que está, el almacenamiento que lee y la CPU y memoria que tiene.

**Ejemplo: dos regiones idénticas.** Declare `east` y `west` en dos centros de datos, cada una con el mismo tipo de motor y los mismos tipos de almacén, y no asigne ninguna tabla. Cada región atiende a quienes leen más cerca de ella: construye sus propias copias de lo que leen y guarda en caché sus respuestas. Las dos pueden estar detrás de un balanceador de carga y repartirse la carga. Hay un único calendario de actualización, el del modelo, y ambas regiones empiezan a actualizar sus copias según él, pero puede que sus motores no terminen en el mismo tiempo. Así que en un momento dado no está garantizado que las dos estén en el mismo estado, y quien lee y pasa de una región a la otra puede ver datos de distinta frescura. Provisa no mueve a un cliente a la otra región cuando una se detiene; adónde va un cliente lo decide el balanceador de carga. [inferred: no failover or cross-region redirect exists in `provisa/`; clients connect to a region by its address, and each region builds and refreshes only its own copies, on the model's one schedule (REQ-1922)]

### Coste {: #cost }

Las regiones de coste tampoco declaran reglas entre regiones. Llevan el trabajo adonde es más barato ejecutarlo:

- **Hardware propio y nube.** Mantenga una región en sus instalaciones, en hardware que ya posee, para las tablas y los lectores que están allí, y ejecute otra región en la nube para el resto.
- **Tamaño del motor.** Ejecute un motor grande en una región para trabajo pesado ocasional y uno pequeño en otra para las lecturas de cada día. Quienes leen eligen la región por su dirección.
- **Egress.** Quienes leen en otras regiones reciben solo las filas filtradas de la réplica de una tabla asignada, y una tabla grande asignada a una región no se copia en todas las regiones.

### Soberanía {: #sovereignty }

Las regiones usadas por soberanía sí declaran reglas entre regiones, sea el motivo la ley o la organización: una filial que asigna sus tablas a su propia región las hace procesar y almacenar en su propia infraestructura, y decide mediante estas reglas qué lee el resto del grupo. Son reglas de filas y columnas corrientes, escritas sobre atributos que dicen dónde está quien lee, o el nodo que hace el trabajo:

- `current_setting('provisa.region')` es la región del nodo que responde a la solicitud, cuando la plataforma declara regiones. Un filtro de filas escrito sobre él mantiene las filas de una región en esa región. (REQ-1922) [tool-verified: `provisa/core/request_context.py` `session_vars_for`]
- El atributo `region` de un miembro le hace tener los roles derivados `region` y `region__<value>`, de modo que una entrada de máscara o de concesión puede nombrar `analyst+region__eu`. El trabajo en segundo plano propio de un nodo lleva su región del mismo modo. (REQ-2057) Consulte [Roles: concedidos, derivados y meta](security.md#roles-granted-derived-and-meta).

Junto con la asignación, estas reglas deciden qué cruza una frontera. Los datos personales de una tabla asignada se almacenan y procesan en su región de origen, y quien lee en otro sitio recibe solo lo que sus reglas permiten, filtrado en origen. Una vista sin región construida en cada región contiene lo que un administrador de esa región puede ver, así que la copia de una región puede incluir filas que la copia de otra región omite. (REQ-1921)

## Quién puede asignar una tabla {: #who-may-home-a-table }

Fijar, cambiar o quitar la región de una tabla, un origen o una vista materializada mueve datos a través de una frontera. Requiere el derecho `data_residency` además de ser propietario del objeto a través de su dominio. La concesión nombra los valores que cubre: las regiones de la organización y "sin región". Mover un objeto entre dos regiones requiere una concesión que cubra ambas. Un guardado fuera de la concesión se rechaza, nombrando el valor que no cubre. (REQ-1921)

El derecho solo existe cuando la plataforma declara regiones. La propiedad del dominio sigue decidiendo quién puede editar todo lo demás del objeto. Una región no confiere ningún derecho propio; una organización que quiere que las tablas de una región las gestione la gente de esa región les da dominios propios. (REQ-1921)

Una tabla o vista también puede pasar a **borrador**: fuera de servicio y no ofrecida a ningún lector. Una tabla registrada desde la administración empieza como borrador. Su responsable la previsualiza solo en los nodos de la región que nombra (una que no nombra ninguna, donde está el responsable), y la copia que deposita esa vista previa se conserva en esa región para la publicación de la tabla. Una copia que otra región tenga de ella, de antes de que se estableciera o cambiara su región, se retira y se elimina como en una tabla publicada. Un traslado entre regiones puede pasar por borrador, de modo que la tabla no se sirve en ningún sitio entretanto: la parte de la primera región la pone en borrador y el titular de la segunda región reclama el objeto estableciendo su región. (REQ-1921)

Consulte también la [demo de dos regiones](regions-demo.md).
