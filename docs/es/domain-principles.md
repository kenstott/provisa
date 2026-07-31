# Principios del Modelo de Dominio

---

## 1. Gobierno

### Principios fundamentales

1. **Todo recurso debe pertenecer a un dominio.** Las tablas, vistas y relaciones son todos activos de dominio. No existen recursos flotantes sin gobernar. El dominio es la unidad de responsabilidad.
2. **Todo dominio debe tener un steward.** Un dominio puede existir en estado pendiente hasta que se asigne un steward, pero no puede servir datos gobernados sin uno.
3. **El administrador es propietario de los orígenes.** Los orígenes son infraestructura, no recursos de dominio. El administrador registra y gestiona las conexiones a los sistemas de datos externos.
4. **Los stewards pueden reclamar tablas para un dominio.** Reclamar es exclusivo: una tabla pertenece exactamente a un dominio. Este es el acto gobernado que conecta la infraestructura con la capa semántica.
5. **Los stewards pueden crear vistas intradominio a partir de activos de dominio.** Las vistas expresan lógica de negocio (uniones, agregaciones, métricas derivadas) sobre activos que el steward posee dentro del mismo dominio. Las vistas crean nuevo significado semántico y requieren aprobación del steward.
6. **Los analistas pueden crear consultas entre dominios a partir de relaciones aprobadas.** Las consultas son vistas interdominio expresadas en cualquier lenguaje de consulta admitido. No crean nueva semántica: recorren rutas de relación aprobadas. No se requiere aprobación adicional: el gobierno se gestiona previamente en las capas de Relación y visibilidad de columnas. El catálogo es el mecanismo de aplicación: el compilador rechaza los recorridos que no están en el catálogo de relaciones aprobadas.
7. **Cualquiera puede solicitar acceso a un recurso de dominio.** El acceso se otorga a nivel de recurso, no a nivel de consulta. Si tiene acceso a un recurso, puede consultarlo. El gobierno se aplica en tiempo de ejecución mediante el pipeline.

### Recursos: tablas y vistas como pares

La distinción entre una tabla y una vista es solo de origen: una tabla se reclama de un origen, una vista la define un steward. Una vez que cualquiera de las dos existe como activo de dominio, el modelo de gobierno las trata de forma idéntica:

- Ambas son activos de dominio de primera clase, visibles en el catálogo
- Ambas pueden ser el destino de una relación
- Ambas pueden concederse bajo el Principio 6
- Ambas están sujetas al mismo pipeline de gobierno

Un steward puede reclamar tablas de forma privada y exponer únicamente vistas curadas como productos de datos de cara al público.

### Composición de vistas

Una vista siempre pertenece a un único dominio; solo existe un tipo de vista, siempre intradominio. Una vista existe para uno de dos propósitos:

- **Importación entre dominios**: el origen está fuera del dominio. Los datos entre dominios solo pueden ingresar a un dominio mediante una vista, que actúa como un adaptador de solo lectura que nombra los datos externos como un concepto de negocio del dominio.
- **Derivación local**: el origen es del mismo dominio. La vista deriva datos nuevos o calculados a partir de activos de dominio existentes. Los datos nuevos o derivados solo pueden existir como una vista.

Una vista puede hacer referencia a:
- Tablas reclamadas dentro del mismo dominio
- Campos importados de otro dominio bajo una concesión de acceso a campos
- Otra vista dentro del mismo dominio, cuando la variación tiene un propósito: restricción de campos, agregación o enriquecimiento mediante una unión adicional

La profundidad de composición no se aplica técnicamente; el criterio del steward durante la revisión HITL es el mecanismo de control de calidad.

Toda vista lleva un propósito de negocio declarado, indicado en el momento de la creación:
- Forma parte del artefacto gobernado: los stewards aprueban sabiendo para qué sirve la vista
- Se hace referencia a él en las solicitudes de acceso bajo el Principio 7, para que el steward pueda evaluar la idoneidad
- Se transmite desde la creación de la vista a lo largo de todo el flujo de trabajo de gobierno

### Consultas

Una consulta (Query) recorre rutas de relación aprobadas sobre activos de dominio. A diferencia de las vistas, las consultas no crean nuevo significado semántico: recorren la estructura aprobada del modelo. Las consultas pueden expresarse en cualquier lenguaje de consulta admitido (SQL, GraphQL, Cypher).

**Aplicación estructural:** el catálogo de relaciones es el mecanismo de aplicación. El compilador valida cada recorrido contra las entradas aprobadas del catálogo y rechaza las consultas que hacen referencia a rutas no aprobadas. El gobierno es estructural, no una verificación en tiempo de ejecución.

**No se requiere aprobación:** el gobierno ocurre previamente, en las capas de Relación y visibilidad de columnas. Si un usuario tiene acceso a las columnas y la ruta de recorrido está aprobada, la consulta es un uso válido. No hay control adicional.

**Diferencia con las vistas:**
- Vistas: intradominio, introducen nuevo significado semántico, curadas por el steward
- Consultas: recorren relaciones aprobadas, sin nueva semántica, sin control de aprobación

**Expresión del dominio según el lenguaje de consulta:**

Cada lenguaje admitido expresa el dominio como un espacio de nombres estructural nativo de ese lenguaje:

| Lenguaje | Expresión del dominio | Ejemplo |
|---|---|---|
| GraphQL | Prefijo del nombre de tipo y campo | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Nombre de esquema | `SELECT * FROM sales.orders` |
| Cypher | Etiqueta de nodo adicional (el dominio solo es necesario cuando el nombre de tipo es ambiguo) | `MATCH (o:Sales:Order)` |

El compilador resuelve la pertenencia al dominio a partir de estas posiciones estructurales; no se requiere ninguna anotación ni indicación.

### Relaciones

Una relación es una ruta de recorrido aprobada entre dos activos. Los límites de dominio son irrelevantes para lo que es una relación; solo determinan quién la aprueba.

**Aprobación:**
- Se requiere la aprobación de cada steward distinto que posea un activo involucrado en la relación
- Si un steward posee ambos activos, se requiere una aprobación. Si están involucrados dos stewards, se requieren dos aprobaciones
- No existe una clasificación intradominio/entre dominios: la propiedad determina de forma natural la carga de aprobación
- Aprobar una relación construye el grafo de dependencias de cada steward, lo que permite notificaciones proactivas de evolución del esquema

Las relaciones se crean por demanda, no de forma especulativa. El primer equipo con la necesidad de negocio realiza el trabajo; los equipos posteriores heredan la infraestructura.

**Consecuencia de optimización:** una declaración de relación no es solo un artefacto de gobierno, también es una descripción estructural de la forma de una unión (join). Las dos tablas, las dos columnas y el tipo de unión que definen una relación son exactamente lo que necesita el optimizador de consultas para pre-materializar esa unión. Las relaciones entre orígenes generan automáticamente tablas de unión pre-materializadas; las relaciones del mismo origen pueden optar por ello mediante `materialize: true`. Los stewards que analizan y aprueban relaciones válidas obtienen aceleración de consultas como subproducto directo: el trabajo de gobierno y el de optimización son el mismo acto.

### Concesiones de acceso a campos

Una concesión de acceso a campos es un permiso de dominio a dominio: el Dominio A puede usar campos específicos del Dominio B en sus vistas.

**Ciclo de vida de la concesión:**
- Se origina cuando la creación de una vista identifica campos externos necesarios
- Se aprueba una vez, por el steward del dominio destino
- Pertenece al dominio solicitante, no a la vista que la originó
- Cualquier vista posterior del dominio solicitante puede usar los campos concedidos sin más intervención entre dominios
- Los campos adicionales no concedidos requieren una nueva solicitud

**Notificación posterior al uso:** cuando se crea una vista usando campos concedidos, se notifica al steward de origen, sin pedirle aprobación. La notificación incluye el nombre de la vista, el propósito de negocio declarado, los campos específicos usados y qué steward la aprobó. Esto le da al steward de origen:
- **Visibilidad**: conocimiento de cómo se están usando sus datos
- **Supervisión**: base para plantear una inquietud si el uso parece inapropiado
- **Recurso**: capacidad de revocar la concesión, invalidando las vistas dependientes

La contrapartida: el dominio de origen aprueba el acceso a campos sin conocer cada uso futuro. La aprobación por vista es correcta en teoría e inviable en la práctica.

### Flujo de trabajo de creación de consultas

Tres etapas, en orden.

**Etapa 1 — Modelado exploratorio (descubrimiento SQL, desde la página de Relaciones):**
- El analista abre la herramienta de modelado exploratorio desde la página de Relaciones para explorar posibles rutas de unión en SQL sin procesar
- El SQL se ejecuta contra los datos accesibles, sujeto a la RLS y al enmascaramiento de columnas existentes
- Las cláusulas JOIN del SQL se analizan y se presentan como propuestas de Relación candidatas
- Los candidatos sugeridos por máquina (inferencia de claves foráneas, inferencia semántica) se muestran junto a la exploración SQL del analista en la misma vista
- El analista selecciona candidatos para promoverlos a una solicitud formal de Relación

**Etapa 2 — Aprobación de la relación** (consecuente: estructural y permanente):
- Se plantea a cada steward distinto que posea un activo involucrado en la relación
- ¿Es esta una ruta de recorrido legítima? ¿Es la unión semánticamente válida?
- Todos los stewards implicados deben aprobar; la relación se convierte en una entrada permanente del catálogo

**Etapa 3 — Creación de la consulta:**
- El analista construye la consulta (Query) en cualquier lenguaje admitido (SQL, GraphQL, Cypher), recorriendo rutas de relación aprobadas
- Solo son recorribles las relaciones aprobadas del catálogo; el compilador lo aplica de forma estructural
- No se requiere aprobación: la visibilidad de columnas y la aprobación de relaciones son los únicos controles

### HITL como control principal

Las reglas técnicas gestionan lo que es objetivo: el seguimiento de procedencia de campos, la aplicación de límites de dominio, la validación del compilador. El criterio contextual queda en manos del steward. Restricciones como la profundidad de composición de vistas, los requisitos de propósito por consulta y las decisiones de aprobación de relaciones son asuntos de HITL, no reglas aplicadas por el compilador.

**Neutralidad del dominio de origen:** el steward del dominio de origen aprueba la relación una vez y la concesión de campos una vez. Después de eso, los dominios descendentes operan dentro de esos límites concedidos:
- **Alta consideración** en la decisión de cruce de límites
- **Conocimiento ligero** a partir de entonces, mediante notificaciones e historial de consultas

---

## 2. Descubribilidad

### Niveles de descubrimiento

El descubrimiento está estructurado en cinco niveles de gobierno creciente. Cada nivel es un prerrequisito para el siguiente.

| Nivel | Descripción | Estado de gobierno |
|---|---|---|
| 1 — Esquema de origen registrado | Toda tabla, columna y tipo de un origen registrado. Visibilidad a nivel de administrador. | Ninguno: inventario sin procesar |
| 2 — Tablas no reclamadas | Tablas introspeccionadas de orígenes registrados sin propietario de dominio. Visibles para stewards con acceso al origen. | Disponible pero sin gobernar |
| 3 — Activos de dominio | Tablas reclamadas y vistas definidas por el steward. Totalmente gobernadas, con propietario, visibles en el catálogo. | Totalmente gobernado |
| 4 — Relaciones | Rutas de recorrido aprobadas entre activos de Nivel 3. Prerrequisito para la creación de vistas entre dominios. | Aprobado por ambos stewards |
| 5 — Concesiones de campos | Permisos de acceso a campos de dominio a dominio. El acceso gobernado más específico y deliberado. | Aprobado por el steward de origen |

Una tabla no reclamada es una señal de vacío: si los datos necesarios existen solo en el Nivel 2, un steward debe reclamarla antes de que el gobierno pueda proceder. La ausencia de cualquier candidato en todos los niveles requiere escalamiento al administrador.

### Restricciones de clave foránea

Las restricciones de clave foránea son una construcción a nivel de origen: no pueden abarcar múltiples orígenes de datos. Las rutas de unión entre orígenes se derivan enteramente de relaciones de catálogo aprobadas (Nivel 4), que son más sólidas, al haber sido validadas por ambos stewards.

Dentro de un origen:
- Las restricciones de clave foránea se presentan automáticamente como relaciones candidatas al registrar el origen
- Representan una intención de modelado explícita, no aplicada en la mayoría de los sistemas SQL analíticos, pero declarada deliberadamente
- Aun así, se requiere validación del steward antes de que un candidato se convierta en una relación aprobada

### Jerarquía de confianza de relaciones

| Evidencia | Confianza |
|---|---|
| Relación de catálogo aprobada: entre orígenes, validada por ambos stewards | Máxima |
| Restricción de clave foránea intra-origen: intención de modelado explícita, no aplicada pero deliberada | Alta |
| Inferencia semántica intra-origen: similitud de nombre/tipo de columna dentro de un esquema consistente | Media |
| Inferencia semántica entre orígenes: las convenciones de nomenclatura divergen entre sistemas; alto riesgo de falsos positivos | Baja |

Las sugerencias corroboradas por múltiples tipos de evidencia acumulan confianza.

### Sondeo y correlación de datos

Para los candidatos inferidos semánticamente, el sondeo de datos ofrece un paso de validación:
- **Superposición de valores**: proporción de valores de la columna de origen que aparecen en la columna de destino
- **Cardinalidad**: si la distribución coincide con el tipo de relación esperado
- **Tasa de nulos**: proporción de la columna de origen que es nula, lo que indica opcionalidad

Una correlación alta aumenta la confianza; una correlación baja suprime o degrada el candidato. El sondeo es evidencia corroborante, no prueba: los rangos de enteros pueden superponerse por coincidencia y la integridad referencial parcial es común en los sistemas analíticos. Queda un margen de error considerable. El criterio semántico del steward es la única verificación final confiable.

### Descubrimiento asistido por LLM

El LLM opera en los cinco niveles simultáneamente, sugiriendo relaciones, reclamos candidatos y rutas de recorrido clasificadas por confianza.

**Lo que presenta el LLM:**
- Relaciones candidatas clasificadas por confianza
- Tablas no reclamadas que podrían satisfacer una necesidad de datos, con una indicación para iniciar el reclamo
- Ausencia de cualquier candidato: señal para escalar al administrador

**Diseño de vistas a partir de una descripción de negocio:**

El analista proporciona una descripción en lenguaje natural y restricciones opcionales. El LLM produce una estructura de vista sugerida.

*Entrada:*
- Descripción de negocio: entidades, métricas, relaciones, intención
- Restricciones opcionales: filtros, ventanas de tiempo, agregaciones, campos excluidos, restricciones de sensibilidad

*Ejemplo:*
> "Volúmenes de operaciones diarios por contraparte de los últimos 30 días, solo contrapartes activas, mostrando la razón social de la contraparte y la calificación crediticia. Sin PII."

*Proceso del LLM:*
1. Analizar: identificar entidades, métricas, dimensiones, filtros, exclusiones
2. Buscar: en todos los niveles del catálogo, activos coincidentes
3. Sugerir: activos de dominio, relaciones, campos, estructura de agregación
4. Puntuar: confianza por componente según la evidencia de nivel
5. Prerrequisitos: lista ordenada de reclamos, relaciones y concesiones de campos requeridos
6. Vacíos: entidades o campos sin candidato en ningún nivel, señalados para escalamiento al administrador

*Salida:*
- Borrador de consulta para revisión y ajuste del analista
- Puntuaciones de confianza por componente
- Lista ordenada de prerrequisitos
- Lista de vacíos

La descripción de negocio se convierte en el propósito de negocio declarado de la vista una vez que esta se crea formalmente.

**Descubrimiento de relaciones basado en SQL (herramienta de Modelado):**

Se accede como un modal desde la página de Relaciones. La intención es construir el modelo semántico: identificar rutas de unión estructurales antes de formalizarlas como relaciones gobernadas.

1. El analista escribe SQL libre contra las tablas accesibles (con RLS y enmascaramiento aún aplicados)
2. Se analiza el AST del SQL: cada condición JOIN se convierte en una propuesta de Relación candidata
3. La lista de candidatos se muestra junto con los candidatos sugeridos por máquina (inferencia de claves foráneas, inferencia semántica) para una revisión unificada
4. El analista promueve los candidatos seleccionados a solicitudes formales de Relación
5. Las relaciones aprobadas se agregan al catálogo y se vuelven recorribles en las consultas

La herramienta de Modelado puede mostrar todas las tablas registradas para exploración estructural, incluso cuando el analista no puede ver los datos subyacentes: la aprobación del steward gobierna el acceso real a los datos, no la visibilidad del esquema.

---

## 3. Uso

### Registro de auditoría de consultas

Toda consulta que toca un activo de dominio se registra en un `query_audit_log` de solo adición. Cada entrada captura:

- `tenant_id`, `user_id`, `role_id`: el contexto de identidad
- Un hash SHA-256 de la consulta: el texto textual de la consulta nunca se almacena
- `table_ids`: los activos de dominio que la consulta tocó
- `source`, `status_code`, `duration_ms`
- `logged_at`: la marca de tiempo

El registro es de solo adición (DELETE y UPDATE están bloqueados a nivel de base de datos) e indexado por `(tenant_id, logged_at)` y `(user_id, logged_at)`.

El informe de historial de consultas del steward es una vista agregada sobre este registro, filtrable por activo, rol y ventana de tiempo. El catálogo es un instrumento de gobierno en vivo: los stewards mantienen conocimiento de cómo se usan sus activos en el momento en que ocurre, no después.

**Dos mecanismos de visibilidad:**
- **Push**: notificaciones posteriores al uso para actos estructurales (se creó una nueva vista usando sus campos)
- **Pull**: historial de consultas para patrones de uso en tiempo de ejecución
