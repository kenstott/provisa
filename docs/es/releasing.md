# Publicación de versiones {: #releasing }

Las versiones se activan al enviar (push) una etiqueta de git. El nombre de la etiqueta determina el canal.

## Convenciones de etiquetas {: #tag-conventions }

| Formato de etiqueta | Canal | Tipo de GitHub Release |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Crear una versión {: #creating-a-release }

```bash
# Alpha
git tag v1.2.3-alpha.1 && git push origin v1.2.3-alpha.1

# Beta
git tag v1.2.3-beta.1 && git push origin v1.2.3-beta.1

# Release candidate
git tag v1.2.3-rc.1 && git push origin v1.2.3-rc.1

# Stable
git tag v1.2.3 && git push origin v1.2.3
```

El flujo de trabajo de CI (`build-dmg.yml`, llamado "Build Provisa Packages") se activa con cualquier etiqueta `v*` y ejecuta estos jobs, la mayoría en paralelo:

1. **Resolve release metadata** — detecta el canal a partir del sufijo de la etiqueta, deriva la versión PEP 440 y los nombres de los assets
2. **Download / package Trino plugins** — descarga los conectores Calcite Trino y empaqueta un tarball
3. **Pull core / obs / demo Docker images** — guarda los tarballs de las imágenes de servicio (arm64, más amd64 core para el nivel de contenedor de Windows)
4. **Build macOS Core / Obs / Demo DMGs** — se ejecuta en `macos-14` (Apple Silicon), sin conexión (airgapped)
5. **Build Linux AppImage** — core, sin conexión (airgapped)
6. **Build Windows Core installer** — nativo, Python embebido, sin Docker
7. **Build Windows Container-tier installer** — WSL2 + Trino, obtiene las imágenes bajo demanda (sin VirtualBox/OVA)
8. **Build JDBC driver** — JAR shaded de Maven
9. **Build and test Python client**, y luego **Publish to PyPI**
10. **Publish GitHub Release** — sube todos los assets, activa la marca de pre-release para alpha/beta/rc

## Recursos de la versión {: #release-assets }

Cada versión publica los siguientes assets, todos adjuntos al GitHub Release (el wheel también se publica en PyPI):

| Asset | Plataforma / Uso |
| ------- | ---------------- |
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, sin conexión) |
| `Provisa-Runtime-<tag>-macOS.dmg` | Runtime nativo de Python para macOS (montar junto con Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | Extensión de observabilidad para macOS |
| `Provisa-Demo-<tag>-macOS.dmg` | Extensión de demostración para macOS (requiere Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Core para Linux x86_64 (sin conexión) |
| `Provisa-<tag>-windows-x64.exe` | Instalador nativo para Windows x64 (Python embebido, sin Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Actualización al nivel de contenedor para Windows x64 (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | Driver JDBC — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Cliente de Python (también en PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Tarballs de imágenes de Core Services (arm64, sin conexión) |
| `provisa-core-images-amd64-<tag>.zip` | Imágenes de Core Services (amd64, nivel de contenedor de Windows / sin conexión) |
| `provisa-obs-images-<tag>.tar.gz` | Imágenes del Observability Stack (opcional) |
| `provisa-demo-images-<tag>.tar.gz` | Imágenes del Demo Data Pack (opcional) |
| `provisa-trino-plugins-<tag>.tar.gz` | Conectores del Coordination Engine (SharePoint, Splunk, File) |

La versión del cliente de Python se convierte automáticamente al formato PEP 440:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## Configuración de publicación en PyPI (una sola vez) {: #pypi-publishing-setup-one-time }

Los jobs `publish-pypi` (`provisa-client`) y `publish-provisa-pypi` (`provisa`) de `.github/workflows/build-dmg.yml` publican con la publicación de confianza de PyPI: el token OIDC de GitHub del job (`id-token: write`) lo autentica, y no se lee ningún token de API. Registre este repositorio y `build-dmg.yml` como publicador de confianza de cada proyecto en pypi.org (**Your projects → Manage → Publishing**). Las publicaciones se ejecutan entonces automáticamente en cada etiqueta.

## Firma y cadena de suministro {: #signing-and-supply-chain }

Lo que lleva cada artefacto de release, tal como lo construyen los workflows de release:

| Artefacto | Firma |
| -------- | --------- |
| `provisa` y `provisa-client` en PyPI | Atestación PEP 740 firmada con Sigstore por archivo (`attestations: true`), verificable en pypi.org |
| `provisa-pg-ext`, `provisa-duckdb-ext` en PyPI | Atestación PEP 740 firmada con Sigstore por archivo |
| `.jar` del controlador JDBC | Atestación de procedencia de compilación de Sigstore (`actions/attest-build-provenance`) |
| Instaladores `.dmg` de macOS | Firma de código con Developer ID y notarización de Apple |
| Instaladores `.exe` de Windows | Sin firmar |
| `.AppImage` de Linux | Sin firmar |

La instalación firmada en cualquier SO es `pip install provisa`: el paquete de PyPI con atestación de Sigstore. Un revisor que necesite un artefacto firmado en Linux o Windows instala con pip. La compilación de Windows firma con Authenticode cuando hay un certificado de firma de código configurado (`WINDOWS_CERT_PFX_BASE64`, `WINDOWS_CERT_PFX_PASSWORD`, `WINDOWS_CERT_TIMESTAMP_URL`, leídos por `packaging/windows/build-sfx.ps1`); no hay ninguno configurado, por lo que se distribuye sin firmar.

Dependencias y puertas de CVE:

- Las dependencias de Python están bloqueadas con hashes en `uv.lock`; CI las instala con `uv sync --frozen`.
- El workflow `CVE Audit` (`.github/workflows/cve-audit.yml`) ejecuta `pip-audit` sobre el conjunto bloqueado en cada push a `main`, en cada pull request y semanalmente.
- En el release, las compilaciones de `provisa` y `provisa-client` producen cada una un SBOM CycloneDX y ejecutan `pip-audit` sobre lo que se distribuye. El controlador JDBC se analiza con `grype --fail-on medium`. Un hallazgo hace fallar el job y el release no publica nada: las subidas a PyPI y el release de GitHub esperan ambos a estos jobs. Los SBOM y los informes de CVE se adjuntan a cada release.
- Los instaladores de escritorio compilan el wheel de `provisa` desde el código fuente en el runner del release y resuelven sus dependencias con `pip` en tiempo de compilación, no desde `uv.lock`; la puerta de CVE del release analiza el conjunto bloqueado, no las dependencias incluidas en los instaladores.
- Se ignora una excepción documentada: PYSEC-2026-4114 / CVE-2026-49265 (PKCE de oauthlib del lado del servidor; Provisa usa oauthlib solo del lado del cliente).
- `packaging/pypi/requirements-embedded.lock` es una resolución de `provisa[embedded]` fijada por hash para réplicas air-gapped (véase `packaging/pypi/MIRROR-CHECKLIST.md`). Se regenera a mano, no en cada release.

## Secretos requeridos del repositorio {: #required-repository-secrets }

Configure los siguientes en **Settings → Secrets → Actions**:

| Secreto | Requerido para | Descripción |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | Builds firmados | Archivo de certificado `.p12` codificado en Base64 (ver más abajo) |
| `APPLE_CERT_P12_PASSWORD` | Builds firmados | Contraseña definida al exportar el `.p12` desde Keychain Access |
| `APPLE_DEVELOPER_ID` | Builds firmados | Nombre completo del certificado: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Builds notarizados | Correo electrónico del Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | Builds notarizados | Contraseña específica de la app desde appleid.apple.com (no su contraseña de inicio de sesión) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Builds notarizados | ID de equipo de Apple de 10 caracteres |

Los builds sin estos secretos se completan correctamente pero producen un DMG sin firmar/sin notarizar (los usuarios verán una advertencia de Gatekeeper).

## Exportar el certificado .p12 {: #exporting-the-p12-certificate }

1. Abra **Keychain Access** → llavero de inicio de sesión → **My Certificates**
2. Busque **Developer ID Application: Your Name (TEAMID)** y expándalo para confirmar que la clave privada está anidada debajo
3. Seleccione tanto el certificado como su clave privada → clic derecho → **Export 2 Items** → guarde como `.p12` → defina una contraseña segura
4. Codifique en Base64 y copie al portapapeles:

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. Péguelo como el valor de `APPLE_CERT_P12_BASE64`; defina `APPLE_CERT_P12_PASSWORD` con la contraseña del paso 3

## Encontrar el nombre de su certificado {: #finding-your-certificate-name }

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Copie la cadena completa entre comillas. Esa cadena es el valor de `APPLE_DEVELOPER_ID`.

## Eliminar una etiqueta incorrecta {: #deleting-a-bad-tag }

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Luego elimine el GitHub Release correspondiente en la interfaz antes de volver a etiquetar.
