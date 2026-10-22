# La lógica de negocio en Provisa

## El problema {: #the-problem }

Toda organización tiene estándares para la lógica de negocio, y la mayoría de los equipos los siguen. Pero con los años, a medida que se construyen y reemplazan sistemas y cambian los equipos, la lógica se reparte entre trabajos ETL, paneles, APIs y procedimientos almacenados. Cada pieza tiene sentido por sí sola. A menudo nadie planificó el conjunto, y más a menudo nadie planificó el conjunto a lo largo del tiempo. El resultado es conocido: los «ingresos netos» se calculan de tres maneras en tres lugares, y nadie puede decir cuál es la correcta.

Para partes de esto existen respuestas de la industria. Una capa de métricas, a menudo vendida como capa semántica, define los «ingresos netos» una vez para que todas las herramientas los lean. El [Open Data Product Standard](https://github.com/bitol-io/open-data-product-standard) (ODPS) describe productos de datos y el [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard) (ODCS) describe contratos de datos. Provisa [exporta ambos](metadata-export.md#data-products-as-odps-and-odcs). Todas son buenas ideas y todas ganan adopción, pero cada una cubre una parte. La pregunta mayor es cómo encajan en datos controlados y gobernados como un modelo único, coherente y componible. La lógica que deriva o cambia datos necesita la misma gobernanza que los propios datos: quién puede usarla, qué lee y escribe, y adónde van sus resultados.

Los canales de entrega lo empeoran. La lógica se construye en cada uno: un campo calculado en la herramienta de BI, una regla en el manejador de la API, un filtro en el resolvedor de la aplicación. Cada canal acaba con su propia versión de la verdad, así que la misma pregunta hecha por dos de ellos rara vez obtiene la misma respuesta.

## Qué es la lógica de negocio {: #definition }

La lógica de negocio son las reglas que crean, actualizan, eliminan o transforman datos.

La terminología de negocio es otra cosa. Los nombres y descripciones de dominios, tablas y columnas, y los términos del [glosario](glossary.md), dicen qué significan los datos. La lógica de negocio dice qué se hace con ellos.

## Tres roles {: #roles }

Cada regla cumple uno de tres roles, y cada rol puede escribirse de tres maneras:

| Rol | SQL | Llamada externa | Código |
|---|---|---|---|
| **Conjunto de datos** | vista, comando de sentencia | API o procedimiento que devuelve filas | resolvedor de código `@table`, `@stream`; comando `python` o `script` |
| **Valor** | expresión de vista, métrica | — | comando `python` o `script` |
| **Cambio** | `INSERT`, `UPDATE`, `DELETE`, comando de sentencia | escritura en API, mutación, procedimiento | `@command`; comando `python` o `script` |

- **Llamada externa.** La API GraphQL, REST o gRPC de otro sistema, o el procedimiento almacenado de una base de datos, registrados como comando.
- **Tablas como argumentos.** Un comando puede recibir tablas como argumentos, así que un conjunto de datos puede construirse a partir de otros.
- **Métrica.** Un agregado con nombre, definido una vez, como `net_revenue` como `SUM(orders.amount) - SUM(orders.refunds)`. Cada consulta lo desglosa por las dimensiones que pide, así que el almacén, el panel y la API leen el mismo número.

## Efectos secundarios ocultos {: #side-effects }

Un efecto secundario oculto es uno que no se ve en la definición de la regla.

Cada regla tiene nombre y tipo, y está registrada, auditada y protegida. Un comando también puede exigir aprobación antes de ejecutarse.

| | Posibles efectos secundarios | Limitados por |
|---|---|---|
| **SQL** | Ninguno | — |
| **Llamada externa** | Los propios flujos de trabajo, notificaciones y cascadas del otro sistema | Lista de salida permitida (HTTP, gRPC); credenciales guardadas como secretos (procedimientos, operaciones de origen) |
| **Resolvedor de código** | Llamadas a servicios y redes externos; resultados que varían; coste | Código fijado, huella del host fijada, sandbox, medición |
| **Código en el host** | Lee todo lo que puede leer el servidor (memoria, credenciales, archivos, red); puede dejar al servidor sin recursos o tumbarlo | `commands.allow_host_code`, desactivado por defecto |

Una llamada externa está tipada por el esquema propio del sistema, o por los argumentos y columnas de retorno que declara al registrar el comando.

El código de un resolvedor de código está fijado por versión de paquete, commit de Git o hash de archivo, así que se promueve con el modelo.

El código en el host es un comando `python` o `script`. Se ejecuta dentro del servidor con todo su acceso, así que un código malicioso, con errores o voraz puede leer todo lo que el servidor puede, dejarlo sin recursos o tumbarlo. Es cómodo, y seguro cuando confía en el código. El modelo solo guarda una referencia (`module:attr` o una línea de comandos), así que el código no viaja con la promoción. Solo se ejecuta donde `commands.allow_host_code` está activado.

Estos límites contienen los efectos secundarios, pero no pueden eliminarlos. Lo que haga un sistema externo o su propio código depende en último término de él.

## Lo que le da el modelo {: #what-you-get }

- La lógica vive en el modelo, no en el canal. Cada canal (SQL, GraphQL, REST, Arrow Flight, MCP y el resto) lee las mismas reglas, así que la misma pregunta obtiene la misma respuesta en todas partes.
- Cada regla está registrada en un dominio, con permisos.
- Cada regla se gobierna igual, se escriba como se escriba.
- Cada regla es visible en el [linaje](lineage.md).

## Siga leyendo {: #read-next }

- [Vistas](configuration.md#views-governed-computed-datasets)
- [Métricas](modeling.md#metrics)
- [Comandos](commands.md)
- [Resolvers de código](code-sources.md)
- [Comprobaciones de calidad de datos](sources.md#data-quality-checkers)
