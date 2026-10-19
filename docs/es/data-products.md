# Productos de datos {: #data-products }

Un producto de datos es un conjunto con nombre y propietario de tablas publicadas juntas para su consumo. Es la unidad que el catálogo expone a los consumidores: no tablas individuales, sino una superficie curada que un dominio declara explícitamente lista. Los campos siguen el vocabulario de ODPS (Open Data Product Standard) donde Provisa ya tiene la fuente de verdad.

## Regla de propiedad de dominio {: #domain-ownership-rule }

Cada producto de datos pertenece exactamente a un dominio (`domain_id` es un campo obligatorio). Una tabla puede unirse a un producto de datos solo cuando ambos comparten el mismo `domain_id`. La interfaz limita el selector de tablas al dominio del producto; el backend rechaza una asignación de `product_id` cuyo dominio no coincida con el del producto en el momento de guardar.

Un producto que necesita datos de otro dominio debe traer esos datos como una vista de dominio primero y luego incluir la vista como miembro.

## Puertos de salida {: #output-ports }

Las tablas y comandos asignados a un producto de datos son sus **puertos de salida**: la superficie consultable que ven los consumidores. Asignar una tabla establece `Table.product_id`; eliminarla quita la pertenencia. Una tabla pertenece a lo sumo a un producto. Los comandos del mismo dominio también pueden asignarse como miembros.

## Secciones del panel de detalle {: #detail-panel-sections }

Al abrir un producto de datos en la interfaz de administración se muestran estos paneles:

| Panel | Qué muestra |
| --- | --- |
| Output Ports | Tablas miembro y sus columnas; comandos miembro; consultas de ejemplo (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Related Terms | Términos del glosario vinculados a las tablas miembro del producto |
| Related Tables | Tablas accesibles desde las tablas miembro a través de relaciones aprobadas pero que aún no forman parte del producto |
| Relationships | Relaciones aprobadas entre las tablas miembro de este producto |
| Lineage | Grafo de linaje de columnas que muestra las tablas miembro como punto final publicado más cada tabla ascendente. Requiere la capacidad `view_governance` |
| Input Ports | Entradas a un salto → transformación → salidas derivadas del linaje. Requiere `view_governance` |
| Data Quality | Tablas de comprobación cuyos contratos analizan los puertos de salida de este producto; una fila por comprobación por ejecución. Incluye un modal de reglas y la visualización de etiquetas PII |

## Exportación de metadatos {: #metadata-export }

Solo las tablas asignadas a un producto se publican en catálogos externos de forma predeterminada. `build_snapshot` aplica un filtro `data_products_only`: las tablas no asignadas se retienen, junto con sus aristas de relación, aristas de linaje y etiquetas de gobierno. Los orígenes y dominios siempre se publican independientemente.

Un producto sin miembros exportados no se publica: una ficha vacía afirmaría que existe un producto sin nada detrás.

Solo los catálogos con un concepto nativo de producto de datos lo publican como entidad de primer orden; el resto publica las tablas miembro (ya filtradas) sin agrupación de producto:

| Catálogo | Publicado como |
| --- | --- |
| Snowflake Horizon | SHARE + ficha de organización (Data Product nativo); `publish=false` lo mantiene como DRAFT, `publish=true` lo pone en activo. El diccionario de datos de la ficha cubre cada miembro y columna; los primeros cinco miembros se destacan y las columnas enmascaradas se ocultan en la vista previa |
| BigQuery Analytics Hub | Ficha de Analytics Hub (nativo) |
| OpenMetadata | Entidad `DataProduct` (nativa) |
| DataHub | Entidad URN `dataProduct` nativa con sus propios aspectos de propiedades/propiedad |
| Collibra | Activo de tipo de comunidad `Data Product`, relacionado con las tablas miembro |
| Apache Atlas | typedef personalizado `provisa_data_product` con mejor esfuerzo — Atlas no tiene tipo nativo de producto de datos |
| Atlan | Suposición de typedef personalizado `DataProduct` con mejor esfuerzo — Atlan no tiene un tipo estable documentado para esto |
| OpenLineage | No es una ficha — las tablas miembro llevan una faceta personalizada `provisa_data_product` que nombra el producto |

## Campos {: #fields }

| Campo | Obligatorio | Notas |
| --- | --- | --- |
| `id` | Sí | Identificador estable legible por máquina, p. ej. `customer_360` |
| `domain_id` | Sí | Dominio propietario; la regla de pertenencia se aplica sobre este campo |
| `name` | Sí | Nombre para mostrar |
| `owner_role` | No | Rol responsable de este producto; distinto del steward del dominio |
| `team_role` | No | Rol cuyos miembros forman el equipo de trabajo cotidiano; se resuelve en individuos |
| `purpose` | No | Qué publica este producto y por qué |
| `limitations` | No | Restricciones, advertencias o exclusiones conocidas |
| `usage` | No | Cómo consumir este producto |
| `version` | No | p. ej. `1.2.0` |
| `status` | No | p. ej. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | No | Compromisos de nivel de servicio; solo texto — un producto abarca varias tablas miembro y un SLA estructurado no puede nombrar sin ambigüedad qué miembro describe |
| `support` | No | Orientación de soporte en texto libre |
| `support_contact` | No | Correo electrónico o URL; requerido por los manifiestos de ficha de organización del Catálogo Snowflake Horizon |
| `publish` | No | `true` para publicar fichas del Catálogo Horizon de inmediato; las nuevas fichas son DRAFT por defecto |
| `custom_properties` | No | Metadatos clave-valor arbitrarios no cubiertos por los campos estándar |
