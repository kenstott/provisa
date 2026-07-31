# Publication des versions

Les publications sont déclenchées par l'envoi (push) d'une étiquette git. Le nom de l'étiquette détermine le canal.

## Conventions d'étiquetage

| Format d'étiquette | Canal | Type de GitHub Release |
|-----------|---------|-------------------|
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Créer une version

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

Le workflow de CI (`build-dmg.yml`, nommé « Build Provisa Packages ») se déclenche sur toute étiquette `v*` et exécute les jobs suivants, la plupart en parallèle :

1. **Resolve release metadata** — détecte le canal à partir du suffixe de l'étiquette, dérive la version PEP 440 et les noms des assets
2. **Download / package Trino plugins** — récupère les connecteurs Calcite Trino et empaquette une archive tarball
3. **Pull core / obs / demo Docker images** — enregistre les archives tarball des images de service (arm64, plus amd64 core pour le palier conteneur Windows)
4. **Build macOS Core / Obs / Demo DMGs** — exécuté sur `macos-14` (Apple Silicon), sans connexion (airgapped)
5. **Build Linux AppImage** — core, sans connexion (airgapped)
6. **Build Windows Core installer** — natif, Python embarqué, sans Docker
7. **Build Windows Container-tier installer** — WSL2 + Trino, récupère les images à la demande (sans VirtualBox/OVA)
8. **Build JDBC driver** — JAR shaded Maven
9. **Build and test Python client**, puis **Publish to PyPI**
10. **Publish GitHub Release** — téléverse tous les assets, définit l'indicateur pre-release pour alpha/beta/rc

## Assets de la version

Chaque version publie les assets suivants, tous attachés au GitHub Release (le wheel est également publié sur PyPI) :

| Asset | Plateforme / Usage |
|-------|----------------|
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, sans connexion) |
| `Provisa-Runtime-<tag>-macOS.dmg` | Runtime Python natif macOS (à monter aux côtés de Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | Extension d'observabilité macOS |
| `Provisa-Demo-<tag>-macOS.dmg` | Extension de démonstration macOS (nécessite Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Core Linux x86_64 (sans connexion) |
| `Provisa-<tag>-windows-x64.exe` | Installateur natif Windows x64 (Python embarqué, sans Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Mise à niveau vers le palier conteneur Windows x64 (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | Pilote JDBC — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Client Python (également sur PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Archives tarball des images Core Services (arm64, sans connexion) |
| `provisa-core-images-amd64-<tag>.zip` | Images Core Services (amd64, palier conteneur Windows / sans connexion) |
| `provisa-obs-images-<tag>.tar.gz` | Images de l'Observability Stack (optionnel) |
| `provisa-demo-images-<tag>.tar.gz` | Images du Demo Data Pack (optionnel) |
| `provisa-trino-plugins-<tag>.tar.gz` | Connecteurs du Coordination Engine (SharePoint, Splunk, File) |

La version du client Python est automatiquement convertie au format PEP 440 :
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## Configuration de la publication PyPI (à faire une seule fois)

1. Copiez votre jeton d'API depuis `~/.pypirc` (la valeur `pypi-...` pour `pypi.org`)
2. Ajoutez-le comme secret de dépôt nommé `PYPI_API_TOKEN` sous **Settings → Secrets → Actions**

Le job `publish-pypi` publiera alors automatiquement à chaque étiquette.

## Secrets de dépôt requis

Configurez les éléments suivants sous **Settings → Secrets → Actions** :

| Secret | Requis pour | Description |
|--------|-------------|-------------|
| `PYPI_API_TOKEN` | Publication PyPI | Jeton d'API depuis `~/.pypirc` (commence par `pypi-`) |
| `APPLE_CERT_P12_BASE64` | Builds signés | Fichier de certificat `.p12` encodé en Base64 (voir ci-dessous) |
| `APPLE_CERT_P12_PASSWORD` | Builds signés | Mot de passe défini lors de l'exportation du `.p12` depuis Keychain Access |
| `APPLE_DEVELOPER_ID` | Builds signés | Nom complet du certificat : `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Builds notarisés | Adresse e-mail de l'Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | Builds notarisés | Mot de passe spécifique à l'application depuis appleid.apple.com (pas votre mot de passe de connexion) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Builds notarisés | ID d'équipe Apple à 10 caractères |

Les builds sans ces secrets réussissent mais produisent un DMG non signé/non notarisé (les utilisateurs verront un avertissement Gatekeeper).

## Exporter le certificat .p12

1. Ouvrez **Keychain Access** → trousseau de connexion → **My Certificates**
2. Recherchez **Developer ID Application: Your Name (TEAMID)** — développez-le pour confirmer que la clé privée est imbriquée en dessous
3. Sélectionnez à la fois le certificat et sa clé privée → clic droit → **Export 2 Items** → enregistrez sous `.p12` → définissez un mot de passe fort
4. Encodez en Base64 et copiez dans le presse-papiers :
   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```
5. Collez-le comme valeur de `APPLE_CERT_P12_BASE64` ; définissez `APPLE_CERT_P12_PASSWORD` avec le mot de passe de l'étape 3

## Trouver le nom de votre certificat

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Copiez la chaîne complète entre guillemets — c'est la valeur pour `APPLE_DEVELOPER_ID`.

## Supprimer une étiquette incorrecte

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Supprimez ensuite le GitHub Release correspondant dans l'interface avant de réétiqueter.
