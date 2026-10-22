# Resolvers de código

Lógica de negocio en Python o Node.

Si ha escrito un servidor GraphQL, conoce este patrón. Un resolver de código es una función que responde por un tipo de objeto: en GraphQL, una función marcada con `@table` *es* el resolver del tipo de su tabla. Provisa la llama con los argumentos de la consulta y gobierna lo que devuelve.

Lo que cambia respecto a un resolver escrito a mano:

- **Gobernado.** La visibilidad, las máscaras y las reglas de fila se aplican a las filas que devuelve su función, como a cualquier tabla.
- **Por lotes.** Una función que recibe un parámetro `loader` obtiene todas las claves de un join en una sola llamada, el patrón DataLoader.
- **Delegado.** Un filtro sobre una columna llega a su función como parámetro cuando usted lo pide.
- **Servido en todas partes.** La misma función responde a GraphQL, SQL (sobre HTTP, pgwire y Arrow Flight), Cypher (sobre HTTP y Bolt), gRPC, REST, JSON:API y MCP.

Los resolvers de código son una de las tres formas de escribir lógica de negocio en Provisa. Vea [La lógica de negocio en Provisa](business-logic.md).

## Un primer resolver {: #an-example }

```python
from dataclasses import dataclass


@dataclass
class Risk:
    score: float
    reasons: list[str]


def churn_risk(account_id: str) -> list[Risk]:
    """Churn risk of one account.

    @table
    """
    return [Risk(score=0.82, reasons=["no logins in 30 days"])]
```

Para servirlo:

1. Añada un origen `python` y apúntelo al código: un paquete de PyPI, un commit de Git, una subida o una carpeta (vea [De dónde viene el código](#where-the-code-comes-from)).
2. Guárdelo. Provisa instala el código, lo lee y ofrece `churn_risk` como tabla con las columnas `score` y `reasons` y un parámetro obligatorio, `account_id`.
3. Un steward registra la tabla en un dominio, aquí `accounts`, y la concede como cualquier otra.

Luego léala. En GraphQL el parámetro es un argumento de campo:

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

En SQL es una condición `_nf_`:

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

Node funciona igual, con etiquetas JSDoc y tipos de TypeScript: vea [Node](#node).

## Construir {: #build }

### En qué se convierte una función {: #what-a-function-becomes }

Provisa lee las propias declaraciones de su código: docstrings y anotaciones de tipo en Python, etiquetas JSDoc y tipos de TypeScript en Node.

| Etiqueta | Se convierte en |
|---|---|
| `@table` | Una tabla. La función devuelve una lista o un iterable, y cada elemento es una fila. |
| `@command` | Un comando, ejecutado a petición. |
| `@stream` | Una tabla en vivo. La función es un generador, y cada fila se envía a medida que se produce. |

- Una función sin etiqueta se ignora.
- Una etiqueta que Provisa no conoce se rechaza, igual que un tipo que no puede mapear. El rechazo enumera todos los problemas a la vez, para que los corrija de una pasada.

El tipo de fila da las columnas:

- Texto, números, booleanos, fechas y marcas de tiempo se mapean a sus tipos de columna. Las listas siguen siendo listas, y los objetos anidados se convierten en tipos con nombre.
- En Python un tipo de fila es una dataclass, un `TypedDict`, un `NamedTuple`, un modelo de Pydantic o cualquier clase con campos anotados. Las filas de un modelo de Pydantic se envían tal como las da `model_dump(by_alias=True)`, así que un campo con alias se convierte en una columna con el nombre del alias.
- En TypeScript un tipo de fila es una interfaz, un alias de tipo o una clase.
- La documentación propia de un campo se convierte en la descripción de su columna cuando registra la tabla: un `Field(description=...)` de Pydantic, o un comentario de documentación en una propiedad de TypeScript.
- Un tipo de fila laxo (`Any` o un `dict` en Python, `any`, `unknown` o un `Record` en TypeScript) no tiene columnas que leer. Provisa llama a la función una vez sin argumentos y sugiere columnas a partir de las filas que devuelve. Si la función tiene un parámetro obligatorio no puede llamarse así, y usted mismo indica los tipos de las columnas al registrar la tabla.

Los parámetros de la función se convierten en los parámetros de la tabla:

- Un parámetro sin valor por defecto es obligatorio. Una lectura que lo omite se rechaza antes de que se ejecute su código.
- Un parámetro con valor por defecto es opcional. Omítalo y se aplica su valor por defecto.
- Un steward puede hacer obligatorio uno opcional.

### Delegación de filtros {: #pushdown }

Un filtro sobre una de las columnas devueltas puede llegar a su función como argumento, de modo que obtenga solo las filas pedidas. Declare qué parámetro lo recibe de una de tres formas:

- **Por nombre.** Un parámetro con el nombre exacto de una columna devuelta recibe una igualdad sobre esa columna. Uno llamado `<op>_<column>` recibe ese operador, como `gte_score` o `in_region`.
- **Por etiqueta.** Una línea `@<op>_<column> <parameter>` (o `@<column> <parameter>`) en el docstring o el JSDoc, o un `/* @<op>_<column> */` en línea antes de un parámetro de TypeScript.
- **Por `Annotated`.** En Python, la cadena `"<op>_<column>"` (o `"<column>"`) en la anotación de tipo del parámetro.

Los operadores:

| `op` | Recibe |
|---|---|
| `ne`, `gt`, `gte`, `lt`, `lte` | Un valor del tipo de la columna |
| `in` | Una lista del tipo de la columna |
| `like` | Texto, sobre una columna de texto |

- Un rango sobre una columna rellena sus dos parámetros: `score BETWEEN 0.5 AND 0.9` da `gte_score` y `lte_score`.
- Los nombres de columna se comparan exactamente como los declaró en el tipo de fila. Un nombre que solo coincide cuando Provisa renombra la columna para un lector no es una coincidencia.
- Una declaración que los tipos de su función no pueden aceptar se rechaza.
- El filtro se sigue aplicando a las filas que devuelve su función, así que una función que ignora o relaja un valor que recibe no puede devolver filas incorrectas.

### Claves por lotes: el parámetro `loader` {: #batched-keys }

Un join puede alcanzar una tabla de código, con las filas de otra tabla aportando sus parámetros: por ejemplo, clientes unidos a su puntuación de abandono. Por defecto Provisa llama a la función una vez por cada conjunto distinto de argumentos que dan las filas del join, y envía todas esas llamadas al worker en una sola petición.

Una función que recibe un parámetro llamado `loader` se llama en cambio una vez por lote, el patrón que popularizó el DataLoader de GraphQL.

- **Lo que recibe.** `loader` es un hash: un dict (un objeto en TypeScript) de un hash estable de cada clave a la clave misma. Una clave es un objeto JSON con los valores de las filas del join para los demás parámetros de la función.
- **Sin duplicados.** Cada combinación distinta de valores aparece una vez.
- **Dispersa.** Un campo sin valor se omite de la clave, así se aplica el valor por defecto de la propia función, como en una llamada individual.
- **Lo que devuelve.** Un hash de cada clave recibida a las filas de esa clave: filas normales de la tabla, ninguna o muchas. Provisa entrega las filas de cada clave a cada fila del join que compartía esa clave.
- **Asociado por hash.** La función rellena con valores por defecto los campos que faltan en una clave dispersa, así que los valores que ve difieren de los de la fila del join. Provisa calcula el hash, y la función solo devuelve las claves que recibió.
- **Tipado como siempre.** La función sigue declarando solo su tipo de fila, una lista de filas.
- **Hacen falta valores por defecto.** Sus parámetros junto a `loader` necesitan valores por defecto, porque una llamada con loader solo pasa `loader`.
- **Tamaño de lote.** Limítelo, como `maxBatchSize` de DataLoader, con `loader N` en uno de tres sitios: una línea `@loader 100` junto a `@table`, un comentario `/* @loader 100 */` antes del parámetro `loader` en TypeScript, o una cadena `Annotated` en Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa divide entonces las claves en lotes de ese tamaño y combina las respuestas por hash. Sin tamaño, un lote contiene todas las claves.
- **Rechazado.** Un tamaño en una función sin parámetro `loader`, un tamaño que no es un entero positivo y dos tamaños en una función.

Use `loader` cuando una llamada sobre muchas claves rinde más que muchas llamadas sobre una:

- Puntuar un modelo sobre un lote entero en una pasada vectorizada.
- Agrupar peticiones a una API o a un modelo de lenguaje que acepta muchas entradas a la vez.
- Preparar algo una vez por lote, como cargar un modelo o abrir una conexión.

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

Unida a una tabla de clientes, se llama con un `loader` como `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. La segunda clave solo lleva `customer_id`, así que su plan y su región toman los valores por defecto. La función devuelve `{"9f2c...": [...], "41ab...": [...], ...}`.

### De dónde viene el código {: #where-the-code-comes-from }

- **Un paquete.** Un paquete npm para Node o un paquete de PyPI para Python, en una versión exacta.
- **Un repositorio Git.** Un commit completo de 40 caracteres, con un subdirectorio si el código no está en la raíz.
- **Una subida.** Un `.zip`, `.tgz` o `.tar.gz` del código.
- **Una carpeta de su equipo.** **Elegir carpeta** comprime la carpeta en su navegador, dejando fuera `node_modules`, `.venv`, `__pycache__` y `.git`, y la sube como archivo del origen. En un navegador que puede conservar la carpeta (Chrome, Edge), **Guardar** la comprime y la envía de nuevo; otro navegador le pide la carpeta otra vez.
- **Un directorio en el servidor.** En una instalación de una sola organización, un directorio bajo los que el operador permite. Se lee donde está, así que un cambio lo lee la siguiente consulta. Un servidor de muchas organizaciones lo rechaza.

Todos los orígenes salvo el directorio del servidor quedan fijados, así que el origen ejecuta exactamente lo que se guardó.

- **Módulo de entrada.** Cuando el código Python tiene más de un módulo de nivel superior, nombre el de entrada.
- **Abrir en VS Code.** Un directorio del servidor se abre en su VS Code instalado; un origen Git se abre en su commit en vscode.dev. Un paquete o una subida no tiene ese enlace.

### Instalar dependencias {: #installing-dependencies }

El código con dependencias necesita un lockfile, y cada instalación lo sigue exactamente. Sin uno, un paquete que declara dependencias se rechaza, y el rechazo le pide que añada uno.

| Entorno | Lockfile |
|---|---|
| Node.js | `package-lock.json` o `npm-shrinkwrap.json` |
| Bun | `bun.lock` o `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (con su `pyproject.toml`), `pylock.toml`, o un `requirements.txt` con `--hash` en cada línea |

- **Incluido o aportado.** El código que trae su propio lockfile instala desde él: un repositorio Git, una subida o una carpeta con uno, o un paquete npm cuyo autor publicó `npm-shrinkwrap.json` (npm nunca publica `package-lock.json`). La mayoría de paquetes npm, y los paquetes de Python de PyPI, no traen ninguno, así que aporte uno junto a ellos.
- **Por qué importa.** Sin uno, dos instalaciones del mismo código pueden diferir, y una prueba que pasa hoy puede fallar mañana por una nueva versión de una dependencia.
- **Una imagen construida ya es fija.** En cuanto [construye la imagen del origen](#running-on-a-remote-host), sus dependencias están dentro. Un lockfile importa para el modo dev y para cada nueva construcción.
- **Sin scripts de instalación.** npm instala con `--ignore-scripts`, y Python solo instala wheels, nunca construye desde una distribución de código fuente.
- **Sus registros.** Los paquetes vienen de los propios registros npm, índices de PyPI y hosts Git de su organización, introducidos una vez en **Admin → Package registries**. El registro público de npm y PyPI no necesitan entrada. Vea [Package Registries](package-registries.md).
- **En caché.** Una instalación se guarda en caché por el hash del contenido del código, así que el mismo código se instala una vez.

### Node {: #node }

- El código Node se ejecuta en Node.js, Bun o Deno, el que elija.
- Las etiquetas van en JSDoc, y los tipos vienen de TypeScript.

El primer resolver en TypeScript:

```typescript
export interface Risk {
  score: number;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(account_id: string): Promise<Risk[]> {
  return [{ score: 0.82, reasons: ["no logins in 30 days"] }];
}
```

El `churnScore` por lotes en TypeScript:

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

## Despliegue {: #deployment }

### Modo dev y workers {: #workers }

Un origen se ejecuta por defecto en el servidor de Provisa; este es el modo dev.

- Su código se ejecuta en un proceso worker propio, uno por origen y versión de su código en cada proceso de Provisa, iniciado en su primera llamada.
- Un worker que falla hace fallar las llamadas en curso, nombrando el origen y la función. La siguiente llamada inicia uno nuevo.
- Una llamada se cancela cuando vence el plazo de su petición: se le indica al worker que la detenga, y la petición en espera se libera de inmediato.

### Leer una tabla {: #reading-a-table }

Cómo llega una lectura a su función depende de qué más lee:

- **Una lectura de una sola tabla de código** va directa a su función. No se almacena nada. Provisa la llama con los argumentos de la lectura y sus filtros delegados, y aplica el resto de la sentencia a las filas que devuelve.
- **Una lectura que une otras tablas** es del motor de federación. La tabla de un origen en modo dev se almacena para ella: las filas se guardan bajo esos argumentos durante el TTL de caché de la tabla.
- **La tabla de un origen construido** en ese join se lee en vivo de su host remoto, que el motor adjunta como catálogo Postgres. Se almacena en su lugar cuando su origen está replicado o con suelo, cuando la tabla se conserva para siempre, cuando declara un TTL de almacenamiento mayor que 0, o en un motor que no puede adjuntar un host.

En cualquier caso, la visibilidad, las máscaras y las reglas de fila se aplican a las filas, como a cualquier tabla. Pasa los argumentos como cualquier parámetro de tabla: condiciones `_nf_` en SQL, argumentos de campo en GraphQL.

### Ejecutar en un host remoto {: #running-on-a-remote-host }

Cuando su código necesita una máquina que no es el servidor, como un equipo con GPU o un host dentro de otra red, construya su imagen y ejecútela allí. El origen pasa al modo construido, y sus tablas, permisos y lectores siguen igual.

**Construir la imagen.**

- Guarde primero el origen en modo dev, para que su código se lea y se fije.
- En **Remote host** de su formulario, elija la arquitectura (Linux x86-64 o ARM64) y a dónde va la imagen: uno de los registros de contenedores de su organización (**Admin → Container registries**), o una descarga.
- Provisa ensambla la imagen por sí mismo, sobre una base mínima, sin demonio de Docker: el código en su hash fijado, sus dependencias instaladas para esa plataforma desde su lockfile a través de sus registros de paquetes, el entorno fijado y el programa de host remoto. Nada de la imagen se ejecuta mientras se construye.
- La etiqueta de la imagen lleva el hash del código y la versión de Provisa, y sus labels nombran el entorno, el hash del código y las exportaciones.

**Ejecutarla.** El formulario muestra un servicio compose para la imagen. Cargue primero una imagen descargada con `docker load -i <file>`. El contenedor escucha en 7443 y guarda su identidad en su volumen. Usted elige el puerto del host con `PROVISA_HOST_PORT`; el servicio no arranca sin él.

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

En su primer arranque el contenedor imprime en su log una línea que empieza por `pairing code:`.

**Conectarla.**

- En el formulario, introduzca el host en el que Provisa alcanza el contenedor, el puerto que publicó y el código de emparejamiento. En una red Docker compartida o en Kubernetes, introduzca el nombre del contenedor o del Service y 7443, sin publicar nada.
- Provisa se empareja con el contenedor mediante TLS mutuo. El código se usa una vez; desde entonces cada lado solo acepta al otro.
- Provisa comprueba que el contenedor ejecuta el código del origen y ofrece sus exportaciones, y pasa el origen al modo construido.
- Cada proceso de Provisa mantiene su propia conexión con el host y vuelve a marcar si el host deja de responder. Mientras no responde, las lecturas de las tablas del origen fallan y nombran el host.

**Si el código cambia.**

- Un contenedor que vuelve ejecutando otro código se rechaza hasta que un steward revisa el código y lo vuelve a fijar desde el formulario.
- Para cambiar el código en sí, vuelva el origen al modo dev, guarde el código nuevo y construya de nuevo.

### Los servicios de Provisa {: #provisas-services }

Su código puede importar un módulo que Provisa siempre ofrece en el servidor: `provisa_services` en Python, `@provisa/services` en Node.js, Bun y Deno. No hay que instalar nada, y no está en ningún registro de paquetes. Fuera de Provisa no existe, así que sus pruebas lo simulan.

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
| `secrets.get(name)` | Uno de los secretos declarados del origen, leído ahora de la bóveda. Vea [Secretos](#secrets). |
| `settings.get(name, default)`, `settings.all()` | Los ajustes del origen, de **Settings** en su formulario, que un steward puede cambiar sin cambiar el código. Uno que falta sin valor por defecto lanza un error. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | La caché propia del origen, compartida por todos sus workers. Un valor es cualquier cosa que JSON pueda contener, hasta 1 MB, guardada `ttl` segundos o hasta que se sustituya. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | La colección de vectores propia del origen, para sus propios datos de referencia. Un elemento tiene un `id` y un `text` o un `vector`; un texto lo incrusta uno de los modelos vectoriales de su organización. No puede buscar en sus tablas gobernadas, cuyos datos solo llegan al código como parámetros. |
| `http.get(url)`, `http.post(url, ...)` | Una petición que Provisa hace por el código, a través del proxy de salida y las autoridades de certificación del despliegue. El host debe estar en la lista de salida permitida, y cada petición queda registrada en la pista de auditoría. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` en JavaScript) | El servicio LLM de Provisa. Vea [Llamar a un modelo](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Una línea de log, y un punto de métrica llamado `code_source.<name>`, adjuntos a la petición que su código atiende. |

- Una llamada que el servicio rechaza lanza un `ServiceError` que nombra el servicio y dice por qué.
- Los servicios responden al código que se ejecuta en el servidor de Provisa. En un host remoto toda llamada lanza un error.

#### Secretos {: #secrets }

Una clave de una API de pago pertenece a la bóveda de su organización, no al código.

- En **Secrets** del formulario del origen, elija los secretos de la organización que el código puede leer. En un archivo de configuración, enumérelos en el mapping del origen:

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- Un nombre solo es el secreto de la organización con ese nombre. Para leer en su lugar una variable de entorno del despliegue, dé la referencia: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`.
- La lista hace referencia a la bóveda y nunca copia un valor, así que una configuración que nombra un secreto que nadie ha guardado aún sigue cargando.

Su código lee un secreto con `secrets.get(name)`, o directamente de `$PROVISA_SERVICES/secrets/<name>`:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` es la única variable de entorno que recibe su código. La petición pasa por el mismo proxy de salida que el resto de su tráfico, y el proxy la responde él mismo; nada más de Provisa es alcanzable así.
- La dirección lleva un token que solo tiene el worker de este origen. Solo lee los nombres que el origen enumera, y deja de funcionar cuando el worker se detiene.
- Cada lectura resuelve el secreto en la bóveda en ese momento, así que un secreto rotado surte efecto en la siguiente lectura.
- Un nombre que el origen no enumera se rechaza, igual que un secreto que no puede resolverse. El rechazo nombra el secreto, nunca su valor.
- Cada lectura queda registrada en la pista de auditoría de la organización con el origen, el nombre y la hora. Ningún valor se escribe nunca en un log.
- El host de la API sigue teniendo que estar en la lista de salida permitida.

#### Llamar a un modelo {: #calling-a-model }

Su código puede llamar a un modelo de lenguaje a través de Provisa en lugar de tener una clave de proveedor. `$PROVISA_SERVICES/llm/v1` es un endpoint compatible con OpenAI, así que cualquier SDK de OpenAI funciona con su URL base apuntando ahí. La dirección es la credencial, así que la clave que pide el SDK puede ser cualquier texto.

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

- **Por rol.** Nombre un modelo como `provisa:<role>` y obtendrá el modelo que su organización asigna a ese rol en **Admin → AI Models**; sigue esa elección cuando un administrador la cambia. También puede nombrar un modelo directamente cuando se le asigna un rol.
- **Embeddings.** `$PROVISA_SERVICES/llm/v1/embeddings` acepta cualquiera de los modelos vectoriales de su organización.
- Cualquier otro modelo se rechaza.
- La llamada sale por los propios proveedores, endpoints y claves de su organización, y lo que envía pasa antes por las reglas de redacción de Provisa.
- No se ofrece streaming. Envíe `stream: false`, que es el valor por defecto.

## Salvaguardas {: #safeguards }

### El sandbox {: #the-sandbox }

- Su código se ejecuta en su propio proceso worker, en un sandbox. Puede leer su propio código instalado y nada más del disco.
- Solo alcanza los hosts de la lista de salida permitida del despliegue, a través de un proxy que ejecuta Provisa, y nunca los puertos propios de Provisa.
- Un host que no puede ofrecer el sandbox no inicia el worker y nombra lo que falta; el código nunca se ejecuta sin confinar. En Windows, los orígenes de código se rechazan.
- En un host remoto, la política de red del contenedor es el límite de salida del código. La lista de salida permitida del servidor se aplica al modo dev.

### Rechazos {: #refusals }

Cada uno de estos casos se rechaza, antes de que se ejecute su código cuando es posible:

- Una etiqueta desconocida o un tipo que Provisa no puede mapear, todos los problemas de una función a la vez.
- Una lectura que omite un parámetro obligatorio.
- Una declaración de delegación o de `loader` que los tipos de la función no pueden aceptar.
- Un paquete con dependencias y sin lockfile.
- Un directorio del servidor fuera de los permitidos, o en un servidor de muchas organizaciones.
- Un host remoto que ejecuta otro código distinto del que el origen tiene fijado.
- Un secreto que el origen no enumera, un modelo no permitido, una llamada de servicio que el servicio no responde.

### Medición y límites {: #metering }

- Cada llamada a un modelo queda registrada para el origen, con el modelo, sus tokens y su coste según la entrada del modelo en **Polly model prices**, y se escribe en la pista de auditoría de la organización.
- Para limitar lo que gasta un origen, establezca **LLM monthly cap** en su formulario, en USD. En un archivo de configuración va en el mapping:

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- Un origen con límite solo puede llamar a modelos que tengan un precio ahí.
- Cuando su gasto del mes (UTC) alcanza el límite, sus llamadas se rechazan hasta que cambia el mes. Las llamadas ya en curso al alcanzarse el límite terminan igualmente, así que el mes puede acabar ligeramente por encima del límite.

### Gobierno sobre los resultados {: #governance }

- Las filas de su función se comprueban contra las columnas de la tabla.
- La visibilidad, las máscaras y las reglas de fila se aplican después para cada lector, como a cualquier tabla.
- Su código ve los datos de una tabla gobernada solo como los parámetros que le da una lectura.

### Trazas {: #tracing }

- Cada llamada a su código se traza como parte de la petición que la desencadenó: la función, su tipo, cuántos argumentos recibió (nunca sus valores), las filas y lotes que devolvió, su duración y la CPU del worker.
- Cada llamada de servicio se traza igual.
- Con el detalle de traza normal estos datos están en el span de la petición; con el detalle de depuración cada llamada es un span propio.
- Si su código usa OpenTelemetry, sus spans se unen a la misma traza.
