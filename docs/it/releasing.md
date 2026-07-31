# Rilascio delle versioni

I rilasci vengono attivati eseguendo il push di un tag git. Il nome del tag determina il canale.

## Convenzioni sui tag

| Formato tag | Canale | Tipo di GitHub Release |
|-----------|---------|-------------------|
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Creare un rilascio

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

Il workflow di CI (`build-dmg.yml`, denominato "Build Provisa Packages") si attiva su qualsiasi tag `v*` ed esegue i seguenti job, la maggior parte in parallelo:

1. **Resolve release metadata** — rileva il canale dal suffisso del tag, deriva la versione PEP 440 e i nomi degli asset
2. **Download / package Trino plugins** — scarica i connettori Calcite Trino e crea un pacchetto tarball
3. **Pull core / obs / demo Docker images** — salva i tarball delle immagini dei servizi (arm64, più amd64 core per il livello container Windows)
4. **Build macOS Core / Obs / Demo DMGs** — eseguito su `macos-14` (Apple Silicon), airgapped
5. **Build Linux AppImage** — core, airgapped
6. **Build Windows Core installer** — nativo, Python incorporato, senza Docker
7. **Build Windows Container-tier installer** — WSL2 + Trino, recupera le immagini on demand (senza VirtualBox/OVA)
8. **Build JDBC driver** — JAR shaded Maven
9. **Build and test Python client**, poi **Publish to PyPI**
10. **Publish GitHub Release** — carica tutti gli asset, imposta il flag pre-release per alpha/beta/rc

## Asset del rilascio

Ogni rilascio pubblica i seguenti asset, tutti allegati al GitHub Release (il wheel viene pubblicato anche su PyPI):

| Asset | Piattaforma / Uso |
|-------|----------------|
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, airgapped) |
| `Provisa-Runtime-<tag>-macOS.dmg` | Runtime Python nativo macOS (da montare insieme a Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | Estensione Observability per macOS |
| `Provisa-Demo-<tag>-macOS.dmg` | Estensione Demo per macOS (richiede Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Core Linux x86_64 (airgapped) |
| `Provisa-<tag>-windows-x64.exe` | Installer nativo Windows x64 (Python incorporato, senza Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Upgrade Windows x64 al livello container (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | Driver JDBC — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Client Python (anche su PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Tarball delle immagini Core Services (arm64, airgapped) |
| `provisa-core-images-amd64-<tag>.zip` | Immagini Core Services (amd64, livello container Windows / airgap) |
| `provisa-obs-images-<tag>.tar.gz` | Immagini dell'Observability Stack (opzionale) |
| `provisa-demo-images-<tag>.tar.gz` | Immagini del Demo Data Pack (opzionale) |
| `provisa-trino-plugins-<tag>.tar.gz` | Connettori del Coordination Engine (SharePoint, Splunk, File) |

La versione del client Python viene convertita automaticamente nel formato PEP 440:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## Configurazione della pubblicazione su PyPI (una tantum)

1. Copiare il proprio token API da `~/.pypirc` (il valore `pypi-...` per `pypi.org`)
2. Aggiungerlo come secret del repository denominato `PYPI_API_TOKEN` in **Settings → Secrets → Actions**

Il job `publish-pypi` pubblicherà quindi automaticamente a ogni tag.

## Secret del repository richiesti

Configurare quanto segue in **Settings → Secrets → Actions**:

| Secret | Richiesto per | Descrizione |
|--------|-------------|-------------|
| `PYPI_API_TOKEN` | Pubblicazione su PyPI | Token API da `~/.pypirc` (inizia con `pypi-`) |
| `APPLE_CERT_P12_BASE64` | Build firmate | File certificato `.p12` codificato in Base64 (vedi sotto) |
| `APPLE_CERT_P12_PASSWORD` | Build firmate | Password impostata durante l'esportazione del `.p12` da Keychain Access |
| `APPLE_DEVELOPER_ID` | Build firmate | Nome completo del certificato: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Build notarizzate | Email dell'Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | Build notarizzate | Password specifica dell'app da appleid.apple.com (non la password di accesso) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Build notarizzate | ID team Apple di 10 caratteri |

Le build senza questi secret vengono completate correttamente ma producono un DMG non firmato/non notarizzato (gli utenti vedranno un avviso di Gatekeeper).

## Esportazione del certificato .p12

1. Aprire **Keychain Access** → portachiavi di accesso → **My Certificates**
2. Cercare **Developer ID Application: Your Name (TEAMID)** — espandere per confermare che la chiave privata sia annidata sotto di esso
3. Selezionare sia il certificato che la relativa chiave privata → clic destro → **Export 2 Items** → salvare come `.p12` → impostare una password sicura
4. Codificare in Base64 e copiare negli appunti:
   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```
5. Incollarlo come valore di `APPLE_CERT_P12_BASE64`; impostare `APPLE_CERT_P12_PASSWORD` con la password del passaggio 3

## Trovare il nome del proprio certificato

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Copiare la stringa completa tra virgolette — quello è il valore per `APPLE_DEVELOPER_ID`.

## Eliminazione di un tag errato

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Eliminare quindi il GitHub Release corrispondente nell'interfaccia prima di ricreare il tag.
