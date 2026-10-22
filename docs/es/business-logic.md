# La lógica de negocio en Provisa

## El problema {: #the-problem }

La lógica de negocio acaba en todas partes: en trabajos ETL, paneles y APIs, en manos de equipos distintos y catalogada por nadie. Los «ingresos netos» se calculan de una forma en el trabajo del almacén, de otra en el panel de finanzas y de una tercera en la API de facturación, y nadie sabe decir cuál es la correcta.

No es culpa de nadie. No ha existido un modelo común y sencillo donde poner las reglas.

## Qué es la lógica de negocio {: #definition }

La lógica de negocio son las reglas que crean, actualizan, eliminan o transforman datos.

La terminología de negocio es otra cosa. Los nombres y descripciones de dominios, tablas y columnas, y los términos del [glosario](glossary.md), dicen qué significan los datos. La lógica de negocio dice qué se hace con ellos.

## Tres roles {: #roles }

Cada regla cumple uno de tres roles, y cada rol puede escribirse de tres maneras:

| Rol | SQL | Llamada externa | Resolver de código |
|---|---|---|---|
| **Conjunto de datos** | vista, comando de sentencia | API o procedimiento que devuelve filas | `@table`, `@stream` |
| **Valor** | expresión de vista, métrica | — | — |
| **Cambio** | `INSERT`, `UPDATE`, `DELETE`, comando de sentencia | escritura en API, mutación, procedimiento | `@command` |

- **Llamada externa.** La API GraphQL, REST o gRPC de otro sistema, o el procedimiento almacenado de una base de datos, registrados como comando.
- **Métrica.** Un agregado con nombre, definido una vez, como `net_revenue` como `SUM(orders.amount) - SUM(orders.refunds)`. Cada consulta lo desglosa por las dimensiones que pide, así que el almacén, el panel y la API leen el mismo número.

## Efectos secundarios ocultos {: #side-effects }

Un efecto secundario oculto es uno que no se ve en la definición de la regla.

Cada regla tiene nombre y tipo, y está registrada, auditada y protegida.

| | Posibles efectos secundarios | Limitados por |
|---|---|---|
| **SQL** | Ninguno | — |
| **Llamada externa** | Los propios flujos de trabajo, notificaciones y cascadas del otro sistema | Aprobación |
| **Resolver de código** | Llamadas a servicios y redes externos; resultados que varían; coste | Sandbox, medición |

Una llamada externa está tipada por el esquema propio del sistema, o por los argumentos y columnas de retorno que declara al registrar el comando.

Estos límites contienen los efectos secundarios, pero no pueden eliminarlos. Lo que haga un sistema externo o su propio código depende en último término de él.

## Lo que le da el modelo {: #what-you-get }

- Cada regla está registrada en un dominio, con permisos.
- Cada regla se gobierna igual, se escriba como se escriba.
- Cada regla es visible en el [linaje](lineage.md).

## Siga leyendo {: #read-next }

- [Vistas](configuration.md#views-governed-computed-datasets)
- [Métricas](modeling.md#metrics)
- [Comandos](commands.md)
- [Resolvers de código](code-sources.md)
- [Comprobaciones de calidad de datos](sources.md#data-quality-checkers)
