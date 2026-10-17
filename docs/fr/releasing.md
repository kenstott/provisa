# Publication des versions {: #releasing }

Les publications sont déclenchées par l'envoi (push) d'une étiquette git. Le nom de l'étiquette détermine le canal.

## Conventions d'étiquetage {: #tag-conventions }

| Format d'étiquette | Canal | Type de GitHub Release |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Créer une version {: #creating-a-release }

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

## Assets de la version {: #release-assets }

Chaque version publie les assets suivants, tous attachés au GitHub Release (le wheel est également publié sur PyPI) :

| Asset | Plateforme / Usage |
| ------- | ---------------- |
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

## Configuration de la publication PyPI (à faire une seule fois) {: #pypi-publishing-setup-one-time }

Les jobs `publish-pypi` (`provisa-client`) et `publish-provisa-pypi` (`provisa`) de `.github/workflows/build-dmg.yml` publient avec la publication de confiance PyPI : le jeton OIDC GitHub du job (`id-token: write`) l'authentifie, et aucun jeton d'API n'est lu. Enregistrez ce dépôt et `build-dmg.yml` comme éditeur de confiance de chaque projet sur pypi.org (**Your projects → Manage → Publishing**). Les publications s'exécutent alors automatiquement à chaque étiquette.

## Signature et chaîne d'approvisionnement {: #signing-and-supply-chain }

Ce que porte chaque artefact de release, tel que les workflows de release le construisent :

| Artefact | Signature |
| -------- | --------- |
| `provisa` et `provisa-client` sur PyPI | Attestation PEP 740 signée par Sigstore pour chaque fichier (`attestations: true`), vérifiable sur pypi.org |
| `provisa-pg-ext`, `provisa-duckdb-ext` sur PyPI | Attestation PEP 740 signée par Sigstore pour chaque fichier |
| `.jar` du pilote JDBC | Attestation de provenance de build Sigstore (`actions/attest-build-provenance`) |
| Installeurs `.dmg` macOS | Signature de code Developer ID et notarisation Apple |
| Installeurs `.exe` Windows | Non signés |
| `.AppImage` Linux | Non signé |

L'installation signée sur n'importe quel OS est `pip install provisa` : le paquet PyPI attesté par Sigstore. Un relecteur qui a besoin d'un artefact signé sous Linux ou Windows installe avec pip. Le build Windows signe avec Authenticode lorsqu'un certificat de signature de code est configuré (`WINDOWS_CERT_PFX_BASE64`, `WINDOWS_CERT_PFX_PASSWORD`, `WINDOWS_CERT_TIMESTAMP_URL`, lus par `packaging/windows/build-sfx.ps1`) ; aucun n'est configuré, il est donc livré non signé.

Dépendances et barrières CVE :

- Les dépendances Python sont verrouillées avec des hachages dans `uv.lock` ; la CI les installe avec `uv sync --frozen`.
- Le workflow `CVE Audit` (`.github/workflows/cve-audit.yml`) exécute `pip-audit` sur l'ensemble verrouillé à chaque push sur `main`, à chaque pull request et chaque semaine.
- À la release, les builds de `provisa` et de `provisa-client` produisent chacun un SBOM CycloneDX et exécutent `pip-audit` sur ce qui est livré. Le pilote JDBC est analysé avec `grype --fail-on medium`. Un résultat fait échouer le job, et la release ne publie rien : les envois vers PyPI et la release GitHub attendent tous deux ces jobs. Les SBOM et les rapports CVE sont joints à chaque release.
- Les installeurs de bureau construisent le wheel `provisa` depuis les sources sur le runner de release et résolvent ses dépendances avec `pip` au moment du build, et non depuis `uv.lock` ; la barrière CVE de la release analyse l'ensemble verrouillé, pas les dépendances embarquées dans les installeurs.
- Une exception documentée est ignorée : PYSEC-2026-4114 / CVE-2026-49265 (PKCE oauthlib côté serveur ; Provisa n'utilise oauthlib que côté client).
- `packaging/pypi/requirements-embedded.lock` est une résolution de `provisa[embedded]` épinglée par hachage pour les miroirs en réseau isolé (voir `packaging/pypi/MIRROR-CHECKLIST.md`). Elle est régénérée à la main, pas à chaque release.

## Secrets de dépôt requis {: #required-repository-secrets }

Configurez les éléments suivants sous **Settings → Secrets → Actions** :

| Secret | Requis pour | Description |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | Builds signés | Fichier de certificat `.p12` encodé en Base64 (voir ci-dessous) |
| `APPLE_CERT_P12_PASSWORD` | Builds signés | Mot de passe défini lors de l'exportation du `.p12` depuis Keychain Access |
| `APPLE_DEVELOPER_ID` | Builds signés | Nom complet du certificat : `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Builds notarisés | Adresse e-mail de l'Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | Builds notarisés | Mot de passe spécifique à l'application depuis appleid.apple.com (pas votre mot de passe de connexion) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Builds notarisés | ID d'équipe Apple à 10 caractères |

Les builds sans ces secrets réussissent mais produisent un DMG non signé/non notarisé (les utilisateurs verront un avertissement Gatekeeper).

## Exporter le certificat .p12 {: #exporting-the-p12-certificate }

1. Ouvrez **Keychain Access** → trousseau de connexion → **My Certificates**
2. Recherchez **Developer ID Application: Your Name (TEAMID)** — développez-le pour confirmer que la clé privée est imbriquée en dessous
3. Sélectionnez à la fois le certificat et sa clé privée → clic droit → **Export 2 Items** → enregistrez sous `.p12` → définissez un mot de passe fort
4. Encodez en Base64 et copiez dans le presse-papiers :

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. Collez-le comme valeur de `APPLE_CERT_P12_BASE64` ; définissez `APPLE_CERT_P12_PASSWORD` avec le mot de passe de l'étape 3

## Trouver le nom de votre certificat {: #finding-your-certificate-name }

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Copiez la chaîne complète entre guillemets — c'est la valeur pour `APPLE_DEVELOPER_ID`.

## Supprimer une étiquette incorrecte {: #deleting-a-bad-tag }

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Supprimez ensuite le GitHub Release correspondant dans l'interface avant de réétiqueter.
