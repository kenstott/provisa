# Registros de paquetes

Las fuentes Node y Python instalan su propio código y sus dependencias. **Admin → Registros de paquetes** indica de dónde instalan: los registros npm, los índices PyPI y los hosts Git de la organización. Un administrador de la organización los introduce una vez, y todas las instalaciones Node y Python de la organización los usan. El registro npm público y el PyPI público no necesitan entrada. (REQ-1984, REQ-1993)

Cada campo contiene lo que se escribió: un valor, o una referencia como `${secret:npm_token}` o `${env:NPM_TOKEN}`, que se resuelve en el propio almacén de la organización cada vez que se ejecuta una instalación. Una referencia que no se resuelve en nada detiene la instalación y nombra el registro y el campo. Provisa nunca escribe en el almacén ni inventa el nombre de un secreto. Consulte [Secretos](secrets.md).

---

## Registros npm {: #npm-registries }

| Campo | Contiene |
|---|---|
| URL del registro | La dirección del registro, como `https://artifactory.example.com/artifactory/api/npm/npm/` |
| Ámbito | Un ámbito como `@acme`, cuyos paquetes vienen de este registro. Vacío si no hay ninguno |
| Token o usuario:contraseña | Un token de acceso, o `user:password` |
| Espejo de cada instalación | Todo paquete sin ámbito propio se instala a través de este registro. Uno por organización |
| Autoridad de certificación (PEM) | Consulte [Una autoridad de certificación propia](#a-private-certificate-authority) |

Un token se escribe en el `.npmrc` de la instalación como `_authToken` y se envía como token bearer. `user:password`, como un usuario de Artifactory y su clave de API, se escribe como `_auth` (base64) y se envía como Basic. Cualquiera de los dos va solo al host del propio registro. (REQ-2088)

## Índices PyPI {: #pypi-indexes }

| Campo | Contiene |
|---|---|
| URL del índice | La dirección simple-index del índice, como `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| Token o usuario:contraseña | Un token de acceso, enviado como el usuario `__token__`, o `user:password` |
| Índice predeterminado de cada instalación | Todo paquete Python se instala desde este índice en lugar del PyPI público. Uno por organización |
| Autoridad de certificación (PEM) | Consulte [Una autoridad de certificación propia](#a-private-certificate-authority) |

Los índices y sus credenciales llegan a uv en el entorno propio de la instalación, nunca en un archivo.

## Hosts Git {: #git-hosts }

Una entrada de host Git cubre los repositorios de los que se obtienen el código de una fuente o sus dependencias Git.

| Campo | Contiene |
|---|---|
| Host | El host, con su puerto y un prefijo de ruta cuando el servidor los tiene: `github.com`, `gitlab.example.com`, `bitbucket.example.com:7990/bitbucket` |
| Token o usuario:token | Un token, o `user:token` |
| Autoridad de certificación (PEM) | Consulte [Una autoridad de certificación propia](#a-private-certificate-authority) |

Una URL de repositorio se asocia con la entrada cuyo host, puerto y prefijo de ruta la inician. Si coinciden varias, gana la más larga: una entrada para `bitbucket.example.com:7990/bitbucket/scm/data` cubre ese proyecto y una para `bitbucket.example.com:7990/bitbucket` cubre el resto. (REQ-2088)

Un token solo se envía con el usuario `x-access-token`. Otros proveedores esperan un usuario, así que escriba `user:token`:

| Proveedor | Usuario |
|---|---|
| GitHub | `x-access-token`, el predeterminado: escriba solo el token |
| GitLab | Cualquier nombre de usuario no vacío |
| Bitbucket Cloud | `x-token-auth` para un token de acceso de repositorio o de workspace; su nombre de usuario o correo electrónico para un token de API |
| Bitbucket Data Center | Su nombre de usuario; `x-token-auth` para un token de proyecto o de repositorio |

## Una autoridad de certificación propia {: #a-private-certificate-authority }

Un registro, índice o servidor Git detrás de su propia autoridad de certificación, como un proxy de Artifactory dentro de su red, necesita que se confíe en esa autoridad. Pegue su certificado PEM en el campo **Autoridad de certificación (PEM)** de la entrada, o una referencia a un secreto que lo contenga. Deje el campo vacío para un servidor cuyo certificado sea de confianza pública. Un valor sin certificado PEM se rechaza al guardar la entrada, y una referencia que no se resuelve en ninguno se rechaza al ejecutar una instalación. (REQ-2088)

Se confía en la autoridad junto a las de confianza pública, nunca en su lugar:

- Las propias descargas de Provisa a través de la entrada (los metadatos y el tarball de un paquete, un wheel, un repositorio Git) verifican el servidor contra ella.
- Durante una instalación, uv la lee mediante `SSL_CERT_FILE`, npm y Bun mediante `cafile` en el `.npmrc` propio de la instalación, Deno mediante `DENO_CERT` y Git mediante `GIT_SSL_CAINFO`.

Esos ajustes nombran un único archivo con las autoridades de confianza pública y las de cada entrada. El archivo se escribe en el directorio temporal de la instalación y se elimina con él, de modo que nada fuera de la instalación lo lee.
