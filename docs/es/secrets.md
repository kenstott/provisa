# Secretos

**Los nombres entran. Los valores nunca vuelven a salir.**

Ningún endpoint de la API devuelve el valor de un secreto almacenado. Ninguna pantalla ofrece un botón de "mostrar". Quien haya perdido un valor lo reemplaza: es la misma llamada que lo creó, a través del mismo formulario. No se trata de una decisión de política: la ruta de lectura sencillamente no existe en el código. (REQ-1558)

---

## Sintaxis de referencia

Hay tres formas de referencia válidas allí donde Provisa resuelve credenciales:

| Forma | Se resuelve desde | Quién puede usarla |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | El entorno del proceso del servidor | Solo la configuración del despliegue |
| `${secret:NAME}` | La bóveda de la organización, compartida por todos sus miembros | Cualquier campo que acepte una referencia a credencial |
| `${user:NAME}` | La bóveda personal de quien actúa | Cualquier campo que acepte una referencia a credencial |

La resolución es de cierre seguro en todo momento. Un nombre de proveedor desconocido, un nombre sin definir y un backend inalcanzable generan un error. Una referencia que no se pudo resolver nunca se sustituye en silencio por una cadena vacía. (REQ-1557) [tool-verified: `provisa/core/secrets.py:92-117`]

### Formato del nombre

Los nombres de secreto deben coincidir con `[A-Za-z_][A-Za-z0-9_]*`: letras, dígitos y guiones bajos, empezando por una letra o un guion bajo. La restricción es práctica: `${secret:NAME}` lo analiza la gramática de referencias, que lee hasta la `}` de cierre. Un nombre que contenga una llave, un espacio o dos puntos produciría una referencia que se analiza como otra cosa. [tool-verified: `provisa/core/secrets_store.py:61`]

---

## Dos bóvedas, un servicio

Cada organización tiene dos bóvedas. Ambas viven dentro del mismo servicio de secretos. (REQ-1560)

**Bóveda de la organización** — La credencial que un administrador de organización guarda aquí es compartida. Todos los miembros que referencien `${secret:DATABASE_TOKEN}` obtienen el mismo valor. Es para las credenciales que posee la *organización*: una contraseña de base de datos compartida, la clave de una cuenta de servicio, un token de despliegue. La bóveda de la organización exige la capacidad `org_settings` para leer o escribir.

**Bóveda personal** — Una credencial guardada aquí pertenece exactamente a una persona. Cuando dos personas tienen cada una un `GIT_TOKEN`, `${user:GIT_TOKEN}` se resuelve al de quien esté actuando. El mismo texto de referencia entrega a cada persona su propia credencial. Quien no haya guardado nada recibe un error, no el valor de otra persona. Ninguna capacidad restringe la bóveda personal: tener la credencial propia no es un privilegio que conceda un administrador. Y no existe sintaxis de solicitud para nombrar la bóveda de otra persona. [tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

El alcance forma parte de la referencia, no es un permiso que la envuelva. `${secret:NAME}` y `${user:NAME}` nunca responden el uno por el otro.

---

## Elegir un servicio de secretos

**Administración → Seguridad → Servicio de secretos.** El panel es visible para quien tenga la capacidad `platform_settings`. Se listan todos los backends que conoce la compilación, esté o no instalado el SDK. Una fila atenuada indica qué paquete de Python falta: el panel lo nombra en vez de ocultar la opción por completo.

Se incluyen cinco backends:

| Clave | Etiqueta | Requiere |
| ----- | ------- | ------- |
| `provisa` | Provisa (integrado, cifrado) | Nada; es el valor por defecto |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secretos) | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

La selección es de cierre seguro: un backend desconocido o no disponible genera un error en el arranque en lugar de recurrir en silencio a otro. (REQ-1557)

### La credencial del propio backend

La credencial de conexión de un backend central es configuración de proceso. Procede únicamente de `${env:...}`, nunca de `${secret:...}`. Un servicio de secretos cuya propia credencial viva dentro de sí mismo no se puede abrir, así que la cadena de confianza termina en el entorno del host por diseño. El registro lo impone: todo valor de configuración de una especificación de backend se resuelve con `providers=("env",)` antes de construir el backend. [tool-verified: `provisa/core/secrets_registry.py:128-141`]

Ejemplo: configuración de Vault en `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Servicio central frente al integrado

Cuando hay un servicio central configurado, Provisa lee de él pero no escribe en él. La creación y la eliminación de entradas son competencia del servicio central: esas operaciones corresponden a sus propias herramientas. La página de Secretos así lo indica y no ofrece un botón de creación. (REQ-1557)

Cuando el backend integrado `provisa` está activo, la página de Secretos permite escritura completa: crear, reemplazar y eliminar desde la interfaz o mediante la API.

---

## El almacén integrado de Provisa

Es el valor por defecto cuando no hay un servicio central configurado. Cada fila de `secrets_store` contiene un blob de sobre cifrado: la columna `value` es binaria, no texto, y la clave de descifrado vive en el entorno del proceso, no en la base de datos. Una copia del plano de control sin la clave maestra del despliegue contiene texto cifrado y nada más. (REQ-1558)

El cifrado nunca es opcional. Cuando no hay configurada una clave de cifrado para todo el proceso, el almacén recurre a un llavero local. Si el host no tiene un llavero donde guardar una clave, el almacén se niega a escribir en lugar de guardar el valor en claro. [tool-verified: `provisa/core/secrets_store.py:130-159`]

**Forma del almacenamiento** [tool-verified: `provisa/core/schema_admin.py:493-505`]:

| Columna | Tipo | Propósito |
| -------- | ------ | --------- |
| `org_id` | Text | La organización propietaria de este secreto |
| `owner_id` | Text | `"*"` para la bóveda de la organización; id de usuario para la bóveda personal |
| `name` | Text | El nombre de referencia |
| `value` | LargeBinary | Blob de sobre cifrado |
| `description` | Text | Para qué sirve el secreto; nunca se deriva del valor |
| `updated_by` | Text | Quién lo estableció por última vez |

La columna `value` no se selecciona en ninguna consulta de listado. [tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## Endpoints de la API

Todas las rutas están bajo `/admin/orgs/{org_id}`. La bóveda de la organización exige `org_settings` en esa organización. La bóveda personal no exige ninguna capacidad: el propietario se lee de la identidad autenticada; no hay ningún parámetro de solicitud para nombrar la bóveda de otra persona.

| Método | Ruta | Qué hace |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Lista los nombres y las referencias de la bóveda de la organización |
| `PUT` | `/secrets/{name}` | Crea o reemplaza un secreto de la organización |
| `DELETE` | `/secrets/{name}` | Elimina un secreto de la organización |
| `GET` | `/my-secrets` | Lista los nombres y las referencias personales de quien llama |
| `PUT` | `/my-secrets/{name}` | Crea o reemplaza uno de los secretos de quien llama |
| `DELETE` | `/my-secrets/{name}` | Elimina uno de los secretos de quien llama |

Cada respuesta devuelve metadatos —nombre, descripción, `updated_at`, `updated_by` y la cadena `reference` que se pega—, pero nunca el valor. El cuerpo del `PUT` lleva `value` (obligatorio) y `description` (opcional). Un reemplazo es la misma llamada que una creación: el nombre es la identidad, no un ID aparte.

Toda escritura queda registrada en el registro de auditoría. La entrada del registro nombra al actor y al secreto. El valor no se registra, ni siquiera su longitud. [tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## Dónde se resuelve `${secret:NAME}`

La resolución ocurre dentro de una operación ligada a un contexto, no en el momento de importar ni en el arranque. El almacén lee y descifra los secretos de la organización una vez al comienzo de esa operación y mantiene el mapa en un `ContextVar` mientras dura. Fuera de una operación ligada, `${secret:NAME}` genera un error. (REQ-1557) [tool-verified: `provisa/core/secrets_store.py:269-290`]

Hay dos puntos de llamada que establecen ese vínculo:

**Operaciones remotas de Git.** Cuando la URL del remoto del repositorio de una organización contiene una referencia `${secret:...}` o `${user:...}` —por ejemplo, un token de push incrustado en la URL—, el router de entornos vincula tanto la bóveda de la organización como la bóveda personal de quien actúa alrededor de la llamada a git. La forma `${user:GIT_TOKEN}` significa que un commit queda registrado bajo la credencial de quien lo envió, no bajo una cuenta de servicio compartida. [tool-verified: `provisa/api/admin/environments_router.py:1263`]

**Lecturas de la clave de API del proveedor de IA.** Cuando Provisa lee la clave del proveedor de LLM de una organización y esa clave está guardada como una referencia `${secret:NAME}`, `bound_to_request_org` establece la bóveda de la organización para esa solicitud. La referencia se resuelve a la salida; el texto de la referencia en sí nunca se envía al proveedor. (REQ-1580) [tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## Claves de proveedor de IA de la organización como referencias a secretos

La clave del proveedor de IA de una organización (Anthropic, OpenAI y otros) puede guardarse como una referencia `${secret:NAME}` en lugar de como una clave literal. (REQ-1580)

Guarde primero la clave en la bóveda de la organización:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Después, configure la IA de la organización para que la referencie:

```
vendor key field → ${secret:OPENAI_KEY}
```

La referencia se guarda cifrada en `org_secrets`. En el momento de la consulta, Provisa resuelve `${secret:OPENAI_KEY}` contra la bóveda de la organización y entrega la clave literal al SDK del proveedor. Rotar la entrada de la bóveda surte efecto de inmediato: no hay que cambiar nada en la configuración de la organización. [tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## Acceso del administrador de la plataforma

Un administrador de la plataforma que opera el plano de control no puede leer los valores de los secretos de ninguna organización. La comprobación de `org_settings` rechaza explícitamente `cross_org` y la excepción de plataforma: administrar el ciclo de vida de una organización no es leer las credenciales que esa organización guarda. El servidor lo impone con independencia de la interfaz. (REQ-1361) [tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## Véase también

- [Modelo de seguridad](security.md) — control de acceso por capas, autenticación y registro de auditoría
- [Referencia de configuración](configuration.md) — sintaxis `${env:VAR}` para credenciales a nivel de proceso
