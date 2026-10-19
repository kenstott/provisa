# Secretos {: #secrets }

**Los nombres entran. Los valores nunca vuelven a salir.**

Ningún endpoint de la API devuelve el valor de un secreto guardado, y ninguna UI ofrece un botón "mostrar". Quien ha perdido un valor lo reemplaza, con la misma llamada y el mismo formulario con que se creó. No es una decisión de política: la vía de lectura simplemente no existe en el código.

---

## Sintaxis de referencia {: #reference-syntax }

Hay tres formas de referencia válidas allí donde Provisa resuelve credenciales:

| Forma | Se resuelve desde | Quién puede usarla |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | El entorno del proceso del servidor | Solo la configuración del despliegue |
| `${secret:NAME}` | La bóveda de la organización, compartida por todos sus miembros | Cualquier campo que acepte una referencia a credencial |
| `${user:NAME}` | La bóveda personal de quien actúa | Cualquier campo que acepte una referencia a credencial |

En un despliegue multiinquilino, guarde las credenciales de la plataforma en un servicio de secretos y no en variables de entorno. Un administrador de inquilino puede escribir `${env:...}` en un campo de credencial propio, así que cualquier valor del entorno del proceso está al alcance de todos los inquilinos. Deje en el entorno solo la credencial propia del backend de secretos.

La resolución es de cierre seguro en todo momento. Un nombre de proveedor desconocido, un nombre sin definir y un backend inalcanzable generan un error. Una referencia que no se pudo resolver nunca se sustituye en silencio por una cadena vacía.

### Formato del nombre {: #name-format }

Los nombres de secretos deben coincidir con `[A-Za-z_][A-Za-z0-9_]*`: letras, dígitos y guiones bajos, empezando por una letra o un guion bajo. El motivo es práctico. `${secret:NAME}` lo analiza la gramática de referencias, que lee hasta la `}` de cierre, así que un nombre con una llave, un espacio o dos puntos daría una referencia que se analiza como otra cosa.

---

## Dos bóvedas, un servicio {: #two-vaults-one-service }

Cada organización tiene dos bóvedas. Ambas viven dentro del mismo servicio de secretos.

**Bóveda de la organización.** La credencial que un administrador de organización guarda aquí es compartida, así que todos los miembros que referencien `${secret:DATABASE_TOKEN}` obtienen el mismo valor. Úsela para las credenciales que posee la *organización*, como una contraseña de base de datos compartida, la clave de una cuenta de servicio o un token de despliegue. Leer o escribir en la bóveda de la organización exige la capacidad `org_settings`.

**Bóveda personal.** Una credencial guardada aquí pertenece exactamente a una persona. Cuando dos personas tienen cada una un `GIT_TOKEN`, `${user:GIT_TOKEN}` se resuelve en el de quien esté actuando, así que el mismo texto de referencia da a cada persona su propia credencial. Quien no ha guardado nada recibe un error y nunca el valor de otro. Ninguna capacidad protege la bóveda personal, porque tener tu propia credencial no es un privilegio que conceda un administrador, y ninguna sintaxis de petición nombra la bóveda de otra persona.

El ámbito forma parte de la propia referencia, y ningún permiso la envuelve. `${secret:NAME}` y `${user:NAME}` nunca responden el uno por el otro.

---

## Elegir un servicio de secretos {: #choosing-a-secrets-service }

**Administración → Seguridad → Servicio de secretos.** Cualquiera con la capacidad `platform_settings` ve el panel. Enumera todos los backends que conoce la versión, tenga o no instalado su SDK, y una fila en gris nombra el paquete de Python que falta en lugar de ocultar la opción.

Se incluyen cinco backends:

| Clave | Etiqueta | Requiere |
| ----- | ------- | ------- |
| `provisa` | Provisa (integrado, cifrado) | Nada; es el valor por defecto |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secretos) | `azure-keyvault-secrets` |

La selección falla de forma segura: un backend desconocido o no disponible detiene el arranque con un error, y Provisa nunca recurre a otro sin avisar.

### La credencial del propio backend {: #the-backends-own-credential }

La credencial de conexión de un backend central es configuración del proceso y sale solo de `${env:...}`. `${secret:...}` no puede aportarla, porque un servicio de secretos cuya propia credencial vive dentro de sí mismo nunca podría abrirse. La cadena de confianza termina en el entorno del host por diseño. Provisa resuelve cada valor de configuración de un backend desde el entorno antes de construirlo.

Por ejemplo, una configuración de Vault en `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Servicio central frente al integrado {: #central-service-vs-built-in }

Con un servicio central configurado, Provisa lee de él y nunca escribe en él. Crear y borrar entradas corresponde a las herramientas propias del servicio, así que la página de secretos lo indica y no ofrece botón de crear.

Con el backend integrado `provisa` activo, la página de secretos admite escritura completa. Usted crea, reemplaza y borra secretos desde la UI o mediante la API.

---

### Los secretos de una organización en un servicio central {: #one-organizations-secrets-in-a-central-service }

Un despliegue que aloja muchas organizaciones y lee un servicio central lo lee, para cada organización, dentro del prefijo propio de esa organización. Una referencia sigue siendo el nombre corto (`${secret:db#password}`); el prefijo se añade al leerla.

| Servicio | Dónde se guarda el secreto `db` de la organización `acme` |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db` (`o`, la longitud del id de la organización, el id con `_` escrito como `-`, y luego el nombre) |

Una referencia se rechaza, por su nombre, cuando:

- su nombre saldría del prefijo: empieza por `/`, contiene un segmento `..` o una barra invertida, o contiene `/` en GCP o Azure, cuyos nombres son planos;
- su nombre es demasiado largo para el servicio una vez prefijado (GCP 255 caracteres, Azure 127); esto se rechaza al guardar la referencia, indicando el límite;
- ninguna organización está vinculada a la operación que la lee.

Un despliegue de una sola organización lee la referencia exactamente como está escrita, sin prefijo: el servicio al que está conectado es el de esa organización y no hay que mover nada.

**Pasar al prefijo.** En un despliegue de muchas organizaciones, cada secreto al que hace referencia una organización tiene que estar bajo el prefijo de esa organización. Copie cada uno allí con las herramientas propias del servicio y luego acorte la referencia al nombre dentro del prefijo (`${secret:teams/data/db}`, guardado en `orgs/acme/db`, pasa a ser `${secret:db}`). Una referencia cuyo secreto no se ha movido falla como no encontrada; no se lee desde donde estaba.

## El almacén integrado de Provisa {: #provisas-built-in-store }

Es el valor por defecto cuando no hay un servicio central configurado. Cada fila de `secrets_store` contiene un blob de sobre cifrado. La columna `value` es binaria, y la clave de descifrado vive en el entorno del proceso, fuera de la base de datos. Una copia del plano de control sin la clave maestra del despliegue contiene texto cifrado y nada más.

El cifrado nunca es opcional. Si no hay una clave de cifrado configurada para todo el proceso, el almacén usa un llavero local. Si el host no tiene un llavero que guarde una clave, el almacén se niega a escribir, y el valor nunca se guarda en claro.

**Forma del almacenamiento**:

| Columna | Tipo | Propósito |
| -------- | ------ | --------- |
| `org_id` | Text | La organización propietaria de este secreto |
| `owner_id` | Text | `"*"` para la bóveda de la organización; id de usuario para la bóveda personal |
| `name` | Text | El nombre de referencia |
| `value` | LargeBinary | Blob de sobre cifrado |
| `description` | Text | Para qué sirve el secreto; nunca se deriva del valor |
| `updated_by` | Text | Quién lo estableció por última vez |

Una organización que ha creado su propia clave de cifrado tiene sus secretos envueltos con esa clave; de lo contrario, se envuelven con la del despliegue. Véase [Cifrado en reposo](security.md#encryption-at-rest-and-the-organizations-own-keys).

La columna `value` no se selecciona en ninguna consulta de listado.

---

## Endpoints de la API {: #api-endpoints }

Todas las rutas están bajo `/admin/orgs/{org_id}`. La bóveda de la organización exige `org_settings` en esa organización. La bóveda personal no exige ninguna capacidad, porque el propietario se lee de la identidad autenticada, y ningún parámetro de petición nombra la bóveda de otra persona.

| Método | Ruta | Qué hace |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Lista los nombres y las referencias de la bóveda de la organización |
| `PUT` | `/secrets/{name}` | Crea o reemplaza un secreto de la organización |
| `DELETE` | `/secrets/{name}` | Elimina un secreto de la organización |
| `GET` | `/my-secrets` | Lista los nombres y las referencias personales de quien llama |
| `PUT` | `/my-secrets/{name}` | Crea o reemplaza uno de los secretos de quien llama |
| `DELETE` | `/my-secrets/{name}` | Elimina uno de los secretos de quien llama |

Cada respuesta devuelve metadatos y nunca el valor: el nombre, la descripción, `updated_at`, `updated_by` y la cadena `reference` para pegar. El cuerpo del `PUT` lleva `value` (obligatorio) y `description` (opcional). Reemplazar es la misma llamada que crear, porque el nombre es la identidad y no hay un ID aparte.

Cada escritura queda registrada en el registro de auditoría. La entrada nombra a quien actuó y el secreto. No se registran ni el valor ni su longitud.

---

## Dónde se resuelve `${secret:NAME}` {: #where-secretname-resolves }

La resolución ocurre dentro de una operación vinculada. Nada se resuelve al importar ni al arrancar. El almacén lee y descifra los secretos de la organización una vez, al empezar la operación, y guarda el mapa mientras dura. Fuera de una operación vinculada, `${secret:NAME}` lanza un error.

Hay dos puntos de llamada que establecen ese vínculo:

**Operaciones remotas de Git.** La URL remota del repositorio de una organización puede contener una referencia `${secret:...}` o `${user:...}`, como un token de push incrustado en la URL. El router de entornos vincula entonces la bóveda de la organización y la bóveda personal del usuario que actúa alrededor de la llamada a git. Con la forma `${user:GIT_TOKEN}`, un commit queda bajo la credencial de quien hizo el push. No interviene ninguna cuenta de servicio compartida.

**Lectura de claves de API de proveedores de IA.** Cuando Provisa lee la clave del proveedor de LLM de una organización y esa clave está guardada como referencia `${secret:NAME}`, vincula la bóveda de la organización para esa petición. La referencia se resuelve a la salida, y el texto de la referencia nunca llega al proveedor.

---

## Claves de proveedor de IA de la organización como referencias a secretos {: #org-ai-vendor-keys-as-secret-references }

La clave del proveedor de IA de una organización (Anthropic, OpenAI y otros) puede guardarse como una referencia `${secret:NAME}` en lugar de como una clave literal.

Guarde primero la clave en la bóveda de la organización:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Después, configure la IA de la organización para que la referencie:

```
vendor key field → ${secret:OPENAI_KEY}
```

La referencia se guarda cifrada en `org_secrets`. En el momento de la consulta, Provisa resuelve `${secret:OPENAI_KEY}` contra la bóveda de la organización y entrega la clave literal al SDK del proveedor. Rotar la entrada de la bóveda surte efecto de inmediato, sin cambiar la configuración de la organización.

---

## Las credenciales propias de los almacenes del despliegue {: #the-deployments-own-store-credentials }

Las contraseñas de los almacenes que ejecuta Provisa (PostgreSQL, Redis, MinIO, la cuenta de administración de Grafana y el secreto compartido del clúster Trino) no son secretos de ninguna de las dos bóvedas. Se generan por despliegue y las conserva el propio despliegue: en `~/.provisa/store-credentials.env` (modo 600), en el estado de Terraform o en el Secret de Kubernetes que nombra `credentials.existingSecret` del chart de Helm. Véase [Refuerzo del despliegue](security.md#deployment-hardening).

---

## Acceso del administrador de la plataforma {: #platform-admin-access }

Un administrador de plataforma que opera el plano de control no puede leer los valores secretos de ninguna organización. La guarda `org_settings` rechaza de plano `cross_org` y el bypass de plataforma, porque administrar el ciclo de vida de una organización no da lectura de las credenciales que guarda. El servidor lo impone por sí mismo, muestre lo que muestre la UI.

---

## Véase también {: #see-also }

- [Modelo de seguridad](security.md) — control de acceso por capas, autenticación y registro de auditoría
- [Referencia de configuración](configuration.md) — sintaxis `${env:VAR}` para credenciales a nivel de proceso
