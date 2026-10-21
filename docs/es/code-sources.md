# Fuentes Node y Python

A veces los datos que necesita están detrás de código que su equipo ya escribió: un modelo de puntuación, un cliente para un servicio interno, un script que ordena una exportación. Una fuente Node o Python le permite servir ese código como tablas. Usted etiqueta las funciones que quiere compartir, indica a Provisa dónde está el código, y cada función etiquetada se convierte en una tabla que cada rol lee con las mismas reglas que cualquier otra tabla.

## En qué se convierte una función {: #what-a-function-becomes }

Provisa lee las propias declaraciones de su código: etiquetas JSDoc y tipos de TypeScript en Node, docstrings y anotaciones de tipo en Python. Cuentan tres etiquetas.

| Etiqueta | Se convierte en |
|---|---|
| `@table` | Una tabla. La función devuelve una lista o un iterable, y cada elemento es una fila |
| `@command` | Un command, que se ejecuta a petición |
| `@stream` | Una tabla en vivo. La función es un generador, y cada fila se envía en cuanto se produce |

Una función sin etiqueta se ignora. Una etiqueta que Provisa no conoce se rechaza, igual que un tipo que no puede mapear. El rechazo nombra la función y enumera todos los problemas a la vez, para que los corrija en una sola pasada.

El tipo de fila da las columnas. Texto, números, booleanos, fechas y marcas de tiempo se mapean a sus tipos de columna, las listas siguen siendo listas y los objetos anidados se convierten en tipos con nombre. Cuando el tipo de una fila es laxo (`any`, `unknown` o un `Record` en TypeScript, `Any` o un `dict` en Python), Provisa llama a la función una vez, sin argumentos, y sugiere columnas a partir de las filas que devuelve. Si la función tiene un parámetro obligatorio no se puede llamar así, y usted tipa las columnas al registrar la tabla.

Los parámetros de la función se convierten en los parámetros de la tabla. Un parámetro sin valor por defecto es obligatorio, así que una lectura que lo omite se rechaza antes de que su código se ejecute. Un parámetro con valor por defecto es opcional; si lo omite, se aplica su valor por defecto. Un steward puede igualmente hacer obligatorio uno opcional.

## De dónde viene el código {: #where-the-code-comes-from }

El código está fijado, de modo que una fuente siempre ejecuta exactamente lo que se registró:

- **Un paquete**: un paquete npm para Node o un paquete PyPI para Python, en una versión exacta.
- **Un repositorio Git**: un commit completo de 40 caracteres, con un subdirectorio si el código no está en la raíz.
- **Una subida**: un `.zip`, `.tgz` o `.tar.gz` del código.

El código Node se ejecuta en Node.js, Bun o Deno, el que usted elija. El código Python se ejecuta en el Python que incluye Provisa. Cuando el código tiene más de un módulo Python de nivel superior, indique en **Módulo de entrada** por cuál se entra.

## Instalar dependencias {: #installing-dependencies }

El código con dependencias necesita un lockfile, y cada instalación lo sigue al pie de la letra. Sin él, un paquete que declara dependencias se rechaza, y el rechazo le indica que añada uno.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` o `npm-shrinkwrap.json` |
| Bun | `bun.lock` o `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (con su `pyproject.toml`), `pylock.toml` o un `requirements.txt` con `--hash` en cada línea |

Un lockfile es opcional, pero sin él dos instalaciones del mismo código pueden diferir, así que una prueba que hoy pasa puede fallar mañana por una nueva versión de una dependencia. Para una instalación idéntica cada vez, el código debe traer un lockfile, o usted crea uno y lo indica aquí. Una imagen construida ya está fijada. Una vez que construye la imagen de la fuente ([Construir imagen](#running-on-a-remote-host)), sus dependencias quedan dentro, y si esa imagen es la que pasa por sus pruebas y su CI/CD, la imagen que probó es la que se ejecuta. Un lockfile importa para el modo dev y para cada nueva construcción de la imagen. El código que trae su propio lockfile se instala a partir de él: un repositorio de Git o una subida que lo incluya, o un paquete de npm cuyo autor publicó `npm-shrinkwrap.json` (npm nunca publica `package-lock.json`). El código sin lockfile se instala con sus dependencias sin fijar: un repositorio de Git o una subida sin lockfile, la mayoría de los paquetes de npm y los paquetes de Python de PyPI, cuyos wheels no tienen lugar para uno. Indique un lockfile junto a ese código para que cada instalación sea idéntica. Hay código que no necesita ninguno, porque su forma de distribución ya fija cada dependencia.

Ningún paquete puede ejecutar sus propios scripts de instalación: npm instala con `--ignore-scripts`, y Python instala solo wheels, sin compilar nunca desde una distribución de código fuente.

Los paquetes vienen de los registros propios de su organización cuando los tiene. Un administrador introduce una vez sus registros npm, índices PyPI y hosts Git en **Admin → Registros de paquetes**, y cada fuente Node y Python de la organización instala a través de ellos. El registro npm público y el PyPI público no necesitan entrada. Consulte [Registros de paquetes](package-registries.md).

Una instalación se guarda en caché por el hash de contenido del código, así que el mismo código se instala una sola vez.

## El sandbox {: #the-sandbox }

Su código se ejecuta en su propio proceso worker, dentro de un sandbox. Puede leer su propio código instalado y nada más en disco. Solo alcanza los hosts de la lista de salida permitida del despliegue, a través de un proxy que ejecuta Provisa, y nunca los puertos propios de Provisa. Un host que no puede ofrecer el sandbox se niega a iniciar el worker y nombra lo que falta, en lugar de ejecutar el código sin restricciones. En Windows, las fuentes de código se rechazan.

## Leer una tabla {: #reading-a-table }

La tabla de una fuente de código se aterriza, nunca se lee en su sitio. Provisa llama a la función con los argumentos de la lectura, comprueba cada fila contra las columnas y conserva las filas bajo esos argumentos durante el TTL de caché de la tabla. La visibilidad, las máscaras y las reglas de fila se aplican luego a las filas aterrizadas, como en cualquier tabla.

Los argumentos se pasan como cualquier parámetro de tabla: como condiciones `_nf_` en SQL y como argumentos de campo en GraphQL.

## Claves por lotes: el parámetro `loader` {: #batched-keys }

Un join puede llegar a una tabla de código: las filas de otra tabla aportan sus parámetros, como los clientes unidos a su puntuación de abandono. Por defecto, Provisa llama a la función una vez por cada conjunto distinto de argumentos que aportan las filas que se unen, y envía todas esas llamadas al worker en una sola solicitud.

Una función que recibe un parámetro llamado `loader` se llama en cambio una vez por lote, el patrón que popularizó DataLoader de GraphQL. `loader` es una lista de objetos JSON, una clave cada uno, con los valores de las filas que se unen para los demás parámetros de la función. Provisa elimina las claves duplicadas, de modo que cada combinación distinta de valores aparece una vez. La lista es dispersa: un campo sin valor se omite de su clave en lugar de enviarse como null, así que se aplica el valor por defecto de la propia función, como en una llamada individual.

La función devuelve filas normales que llevan sus columnas clave, y Provisa las une de vuelta a las filas que se unen por esas columnas, nunca por posición. Las filas pueden volver en cualquier orden, y una clave puede dar ninguna fila o muchas.

Use `loader` cuando una llamada sobre muchas claves funciona mejor que muchas llamadas sobre una:

- Puntuar un modelo sobre un lote entero en una sola pasada vectorizada.
- Agrupar solicitudes a una API o a un modelo de lenguaje que acepta muchas entradas a la vez.
- Preparar una vez por lote, como cargar un modelo o abrir una conexión, en lugar de una vez por clave.

Esta función de Python puntúa a cada cliente de un lote en una sola pasada:

```python
from dataclasses import dataclass
from typing import TypedDict


class CustomerKey(TypedDict, total=False):
    customer_id: str
    plan: str
    region: str


@dataclass
class Score:
    customer_id: str
    plan: str
    region: str
    score: float


def churn_score(
    customer_id: str, plan: str = "basic", region: str = "US", loader: list[CustomerKey] | None = None
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or [{"customer_id": customer_id, "plan": plan, "region": region}]
    rows = [{"plan": "basic", "region": "US", **key} for key in keys]
    scores = MODEL.predict(features(rows))  # one vectorized call for the whole batch
    return [Score(r["customer_id"], r["plan"], r["region"], s) for r, s in zip(rows, scores)]
```

Unida a una tabla de clientes, se llama con `loader=[{"customer_id": "637", "plan": "pro", "region": "EU"}, {"customer_id": "638"}, ...]`. La segunda clave solo lleva `customer_id`, así que su plan y su región toman los valores por defecto. Cada fila devuelta lleva `customer_id`, `plan` y `region`, y Provisa la empareja con su cliente por ellas.

La misma función en TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  plan: string;
  region: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id: string,
  plan = "basic",
  region = "US",
  loader?: CustomerKey[],
): Promise<Score[]> {
  const keys = loader ?? [{ customer_id, plan, region }];
  const rows = keys.map((key) => ({ plan: "basic", region: "US", ...key }));
  const scores = await model.predict(rows); // one call for the whole batch
  return rows.map((row, i) => ({ ...row, score: scores[i] }) as Score);
}
```

## Un ejemplo {: #an-example }

Este módulo Python puntúa la probabilidad de que una cuenta se vaya:

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RiskRow:
    account_id: str
    score: float
    computed_at: datetime
    reasons: list[str]


def churn_risk(account_id: str, as_of: datetime | None = None) -> list[RiskRow]:
    """Churn risk of one account.

    @table
    """
    ...
```

Regístrelo como fuente `python` desde su paquete PyPI, su commit Git o una subida. El descubrimiento ofrece entonces `churn_risk` como tabla con cuatro columnas (`account_id`, `score`, `computed_at`, `reasons`), un parámetro obligatorio `account_id` y uno opcional, `as_of`. Cuando un steward registra la tabla, léala en SQL:

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

La misma función en TypeScript se ve así:

```typescript
export interface RiskRow {
  account_id: string;
  score: number;
  computed_at: Date;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(accountId: string, asOf?: Date): Promise<RiskRow[]> {
  // ...
}
```
