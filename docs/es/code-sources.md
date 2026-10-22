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

El tipo de fila da las columnas. Texto, números, booleanos, fechas y marcas de tiempo se mapean a sus tipos de columna, las listas siguen siendo listas y los objetos anidados se convierten en tipos con nombre. En TypeScript, un tipo de fila es una interfaz, un alias de tipo o una clase. En Python es una dataclass, un `TypedDict`, un `NamedTuple`, un modelo de Pydantic o cualquier clase con campos anotados. Las filas de un modelo de Pydantic se envían tal como las da `model_dump(by_alias=True)`, así que un campo con alias se convierte en una columna con el nombre de su alias. La documentación propia de un campo se convierte en la descripción de su columna al registrar la tabla: un comentario de documentación en una propiedad de TypeScript o un `Field(description=...)` de Pydantic. Cuando el tipo de una fila es laxo (`any`, `unknown` o un `Record` en TypeScript, `Any` o un `dict` en Python), Provisa llama a la función una vez, sin argumentos, y sugiere columnas a partir de las filas que devuelve. Si la función tiene un parámetro obligatorio no se puede llamar así, y usted tipa las columnas al registrar la tabla.

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

## Secretos {: #secrets }

El código que llama a una API de pago necesita su clave, y la clave pertenece al vault de su organización, no al código. En **Secrets**, en el formulario de la fuente, elija los secretos de la organización que el código puede leer. En un archivo de configuración, enumérelos en el mapping de la fuente:

```yaml
mapping:
  secrets: [openrouter]
```

Un nombre por sí solo es el secreto de la organización con ese nombre. Para leer en su lugar una variable de entorno del despliegue, indique la referencia: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`. La lista es una referencia al vault, nunca una copia de un valor, así que una configuración que nombra un secreto que nadie ha guardado todavía se carga igualmente.

Su código recibe una variable de entorno, `PROVISA_SERVICES`, y lee un secreto de `$PROVISA_SERVICES/secrets/<name>` cuando lo necesita. En Python:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

En TypeScript:

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

La solicitud pasa por el mismo proxy de salida que el resto del tráfico de su código, y el proxy la responde él mismo. Nada más de Provisa es accesible por esa vía. La dirección lleva un token que solo tiene el worker de esta fuente; solo lee los nombres que la fuente enumera, y deja de funcionar cuando el worker se detiene. Cada lectura resuelve el secreto en el vault en ese momento, así que un secreto rotado surte efecto en la siguiente lectura.

Un nombre que la fuente no enumera se rechaza, y también un secreto que no se puede resolver. El rechazo nombra el secreto, nunca su valor. Cada lectura queda registrada en el registro de auditoría de la organización con la fuente, el nombre y la hora, y ningún valor se escribe jamás en un log. El host de la API sigue teniendo que estar en la lista de salida permitida.

## Llamar a un modelo {: #calling-a-model }

Su código puede llamar a un modelo de lenguaje a través de Provisa en lugar de guardar una clave de proveedor. `$PROVISA_SERVICES/llm/v1` es un endpoint compatible con OpenAI, así que cualquier SDK de OpenAI funciona si apunta su URL base allí. La dirección es la credencial, así que la clave que pide el SDK puede ser cualquier texto. En Python:

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

En TypeScript:

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

Nombre un modelo por su rol, como `provisa:<role>`: obtiene el modelo que su organización asigna a ese rol en **Admin → AI Models**, y sigue esa elección cuando un administrador la cambia. También puede nombrar un modelo directamente cuando está asignado a un rol. Para embeddings, `$PROVISA_SERVICES/llm/v1/embeddings` acepta cualquiera de los modelos vectoriales de su organización. Cualquier otro modelo se rechaza por su nombre. La llamada sale por los propios proveedores, endpoints y claves de su organización, y lo que envía pasa antes por las reglas de redacción de Provisa. No se ofrece streaming: envíe `stream: false`, que es el valor por defecto.

Cada llamada queda registrada para la fuente, con el modelo, sus tokens y su coste según la entrada del modelo en **Polly model prices**, y se escribe en el registro de auditoría de la organización. Para limitar lo que gasta una fuente, fije **LLM monthly cap** en su formulario, en USD. En un archivo de configuración va en el mapping:

```yaml
mapping:
  llm: {cap_usd: 20}
```

Una fuente con límite solo puede llamar a modelos que tengan precio allí. Cuando su gasto del mes (UTC) alcanza el límite, sus llamadas se rechazan hasta que cambia el mes. Las llamadas ya en curso cuando se alcanza el límite terminan igualmente, así que el mes puede cerrar ligeramente por encima del límite.

## Los servicios de Provisa {: #provisas-services }

Su código puede importar un módulo que Provisa proporciona siempre, allí donde se ejecute: `provisa_services` en Python y `@provisa/services` en Node.js, Bun y Deno. No hay que instalar nada, y no está en ningún registro de paquetes. Fuera de Provisa el módulo no existe, así que las pruebas de sus funciones lo simulan.

```python
import provisa_services as ps

region = ps.settings.get("region")
rates = ps.cache.get("rates") or fetch_rates()
ps.cache.set("rates", rates, ttl=3600)
```

```typescript
import { settings, cache } from "@provisa/services";

const region = await settings.get("region");
const rates = (await cache.get("rates")) ?? (await fetchRates());
await cache.set("rates", rates, 3600);
```

| Llamada | Qué hace |
|---|---|
| `secrets.get(name)` | Uno de los secretos declarados de la fuente, leído ahora del vault. Consulte [Secretos](#secrets). |
| `settings.get(name, default)` | Uno de los ajustes de la fuente, de **Settings** en su formulario, que un steward puede cambiar sin cambiar el código. Si falta y no hay valor por defecto, lanza un error. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | La caché propia de la fuente, compartida por todos sus workers. Un valor es cualquier cosa que JSON pueda contener, hasta 1 MB, guardada `ttl` segundos o hasta que se sustituya. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | La colección vectorial propia de la fuente, para sus propios datos de referencia. Un elemento tiene un `id` y un `text` o un `vector`; un texto se incrusta con uno de los modelos vectoriales de su organización. No puede buscar en sus tablas gobernadas: sus datos llegan al código solo como parámetros. |
| `http.get(url)`, `http.post(url, ...)` | Una petición que Provisa hace por el código, a través del proxy de salida y las autoridades de certificación del despliegue. El host debe estar en la lista de salida permitida, y cada petición queda registrada en el registro de auditoría. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` en JavaScript) | El servicio LLM de Provisa. Consulte [Llamar a un modelo](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Una línea de log y un punto de métrica llamado `code_source.<name>`, vinculados a la solicitud que atiende su código. |

Una llamada que el servicio rechaza lanza un `ServiceError` que nombra el servicio y explica el motivo.

Los servicios responden al código que se ejecuta en el servidor de Provisa. En un host remoto todavía no son accesibles, y cada llamada lanza un error.

Cada llamada a su código se traza como parte de la solicitud que la provocó, con la función, su tipo, cuántos argumentos recibió (nunca sus valores), las filas y los lotes que devolvió, su duración y la CPU del worker. Cada llamada a un servicio se traza igual. Con el detalle de traza normal, estos datos están en el span de la solicitud; con el detalle de depuración, cada llamada es un span propio. Si su código usa OpenTelemetry, sus spans se unen a la misma traza.

## Ejecutar en un host remoto {: #running-on-a-remote-host }

Una fuente se ejecuta por defecto en el servidor de Provisa; es el modo dev. Cuando su código necesita una máquina que el servidor no es, como un equipo con GPU o un host dentro de otra red, construya su imagen y ejecútela allí. La fuente pasa entonces al modo built, y sus tablas, permisos y lectores siguen igual.

**Construir la imagen.** Guarde primero la fuente en modo dev, para que su código se lea y se fije. En **Remote host**, en su formulario, elija la arquitectura (Linux x86-64 o ARM64) y adónde va la imagen: a uno de los registros de contenedores de su organización, dados de alta en **Admin → Container registries**, o a una descarga. Provisa monta la imagen él mismo. Contiene el código con su hash fijado, sus dependencias instaladas para esa plataforma a partir del lockfile del código a través de sus registros de paquetes, el runtime fijado y el programa del host remoto, sobre una imagen base mínima. Nada de la imagen se ejecuta mientras se construye, y no interviene ningún daemon de Docker. La etiqueta de la imagen lleva el hash del código y la versión de Provisa, y sus labels nombran el runtime, el hash del código y las exportaciones.

**Ejecutarla.** El formulario muestra un servicio de compose para la imagen. Una imagen descargada se carga primero con `docker load -i <file>`. El contenedor escucha en el puerto 7443 por dentro y guarda su identidad en el volumen. El puerto del host lo elige con `PROVISA_HOST_PORT`; sin él, el servicio no arranca:

```yaml
services:
  provisa-code-rates:
    image: ghcr.io/example/provisa-code-rates:3f9a1c2e7b40-0.1.0
    restart: unless-stopped
    ports:
      - "${PROVISA_HOST_PORT:?set PROVISA_HOST_PORT to a free port on this host}:7443"
    volumes:
      - provisa-code-rates-state:/var/lib/provisa
volumes:
  provisa-code-rates-state: {}
```

En su primer arranque, el contenedor escribe en su log una línea que empieza por `pairing code:`.

**Conectarla.** De vuelta en el formulario, indique el host en el que Provisa alcanza el contenedor, el puerto que publicó y el código de emparejamiento. En una red de Docker compartida o en Kubernetes, indique el nombre del contenedor o del Service y 7443, sin publicar nada. Provisa se empareja con el contenedor mediante TLS mutuo. El código se usa una vez, y a partir de ahí cada lado acepta solo al otro. Provisa comprueba entonces que el contenedor ejecuta el código de la fuente y ofrece sus exportaciones, y pasa la fuente al modo built. Cada proceso de Provisa mantiene su propia conexión con el host y vuelve a conectarse si el host deja de responder. Mientras el host no responde, las lecturas de las tablas de la fuente fallan y nombran el host.

**Si el código cambia.** Un contenedor que vuelve con otro código se rechaza, con el hash al que está fijada la fuente y el que informa el host, hasta que un steward revisa el código y lo vuelve a fijar desde el formulario. Para cambiar el propio código, vuelva a poner la fuente en dev, guarde el código nuevo y construya de nuevo.

En el contenedor, su política de red es el límite de salida del código; la lista de salida permitida del servidor se aplica al modo dev.

## Leer una tabla {: #reading-a-table }

La tabla de una fuente de código se aterriza, nunca se lee en su sitio. Provisa llama a la función con los argumentos de la lectura, comprueba cada fila contra las columnas y conserva las filas bajo esos argumentos durante el TTL de caché de la tabla. La visibilidad, las máscaras y las reglas de fila se aplican luego a las filas aterrizadas, como en cualquier tabla.

Los argumentos se pasan como cualquier parámetro de tabla: como condiciones `_nf_` en SQL y como argumentos de campo en GraphQL.

## Claves por lotes: el parámetro `loader` {: #batched-keys }

Un join puede llegar a una tabla de código: las filas de otra tabla aportan sus parámetros, como los clientes unidos a su puntuación de abandono. Por defecto, Provisa llama a la función una vez por cada conjunto distinto de argumentos que aportan las filas que se unen, y envía todas esas llamadas al worker en una sola solicitud.

Una función que recibe un parámetro llamado `loader` se llama en cambio una vez por lote, el patrón que popularizó DataLoader de GraphQL. `loader` es un hash: un dict (un objeto en TypeScript) que va de un hash estable de cada clave a la clave misma, un objeto JSON con los valores de las filas que se unen para los demás parámetros de la función. Provisa elimina las claves duplicadas, de modo que cada combinación distinta de valores aparece una vez. Una clave es dispersa: un campo sin valor se omite en lugar de enviarse como null, así que se aplica el valor por defecto de la propia función, como en una llamada individual.

La función devuelve la misma forma: un hash que va de cada clave recibida a las filas de esa clave, filas normales de la tabla. Provisa entrega las filas de cada clave a todas las filas que se unen y compartían esa clave, y una clave puede corresponder a ninguna fila o a muchas. Emparejar por el hash, y no por los valores de las filas, mantiene emparejada una clave dispersa: la función completa sus campos ausentes con valores por defecto, así que los valores que ve difieren de los de la fila que se une. Provisa calcula el hash; la función solo devuelve las claves que recibió. La función sigue declarando solo su tipo de fila, una lista de filas, y las filas de cada clave son de ese tipo. Sus parámetros junto a `loader` necesitan valores por defecto, porque una llamada con loader solo pasa `loader`.

Una función puede limitar el tamaño de su lote, como hace `maxBatchSize` de DataLoader, declarando `loader N` en cualquiera de tres sitios: una línea `@loader 100` junto a `@table` en su docstring o JSDoc, un comentario `/* @loader 100 */` justo antes del parámetro `loader` en TypeScript, o una cadena `Annotated` en el parámetro en Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa divide entonces las claves en lotes de ese tamaño, llama a la función una vez por lote y combina las respuestas por hash. Sin tamaño, un solo lote contiene todas las claves. El descubrimiento rechaza por nombre un tamaño en una función sin parámetro `loader`, un tamaño que no es un entero positivo y dos tamaños en una misma función.

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
    score: float


def churn_score(
    customer_id: str | None = None,
    plan: str = "basic",
    region: str = "US",
    loader: dict[str, CustomerKey] | None = None,
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or {"": {"customer_id": customer_id, "plan": plan, "region": region}}
    rows = {h: {"plan": "basic", "region": "US", **key} for h, key in keys.items()}
    scores = MODEL.predict(features(list(rows.values())))  # one vectorized call for the batch
    out = {h: [Score(r["customer_id"], s)] for (h, r), s in zip(rows.items(), scores)}
    return out if loader else out[""]  # a loader call returns the hash of key to rows
```

Unida a una tabla de clientes, se llama con un `loader` como `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. La segunda clave solo lleva `customer_id`, así que su plan y su región toman los valores por defecto. La función devuelve `{"9f2c...": [...], "41ab...": [...], ...}`, y Provisa entrega cada lista de filas a cada fila de cliente que compartía esa clave.

La misma función en TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id?: string,
  plan = "basic",
  region = "US",
  loader?: Record<string, CustomerKey>,
): Promise<Score[]> {
  const keys = loader ?? { "": { customer_id, plan, region } };
  const rows = Object.entries(keys).map(([h, key]) => [h, { plan: "basic", region: "US", ...key }] as const);
  const scores = await model.predict(rows.map(([, row]) => row)); // one call for the whole batch
  const out = Object.fromEntries(
    rows.map(([h, row], i) => [h, [{ customer_id: row.customer_id, score: scores[i] }]]),
  );
  return loader ? out : out[""]; // a loader call returns the hash of key to rows
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
