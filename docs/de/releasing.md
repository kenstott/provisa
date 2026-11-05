# Releases veröffentlichen {: #releasing }

Releases werden durch das Pushen eines Git-Tags ausgelöst. Der Tag-Name bestimmt den Kanal.

## Tag-Konventionen {: #tag-conventions }

| Tag-Format | Kanal | GitHub-Release-Typ |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Ein Release erstellen {: #creating-a-release }

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

Der CI-Workflow (`build-dmg.yml`, benannt „Build Provisa Packages") wird bei jedem `v*`-Tag ausgelöst und führt folgende Jobs aus, die meisten davon parallel:

1. **Resolve release metadata** — erkennt den Kanal anhand des Tag-Suffixes, leitet die PEP-440-Version und die Asset-Namen ab
2. **Download / package Trino plugins** — lädt die Calcite-Trino-Connectors herunter und packt ein Tarball
3. **Pull core / obs / demo Docker images** — speichert die Tarballs der Service-Images (arm64, plus amd64 core für die Windows-Container-Stufe)
4. **Build macOS Core / Obs / Demo DMGs** — läuft auf `macos-14` (Apple Silicon), airgapped
5. **Build Linux AppImage** — core, airgapped
6. **Build Windows Core installer** — nativ, eingebettetes Python, ohne Docker
7. **Build Windows Container-tier installer** — WSL2 + Trino, ruft Images bei Bedarf ab (ohne VirtualBox/OVA)
8. **Build JDBC driver** — Maven Shaded JAR
9. **Build and test Python client**, dann **Publish to PyPI**
10. **Publish GitHub Release** — lädt alle Assets hoch, setzt das Pre-Release-Flag für alpha/beta/rc

## Release-Assets

Jedes Release veröffentlicht die folgenden Assets, alle am GitHub Release angehängt (das Wheel wird zusätzlich auf PyPI veröffentlicht):

| Asset | Plattform / Verwendung |
| ------- | ---------------- |
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, airgapped) |
| `Provisa-Runtime-<tag>-macOS.dmg` | Natives macOS-Python-Runtime (neben Core zu mounten) |
| `Provisa-Obs-<tag>-macOS.dmg` | macOS-Observability-Erweiterung |
| `Provisa-Demo-<tag>-macOS.dmg` | macOS-Demo-Erweiterung (erfordert Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Linux-x86_64-Core (airgapped) |
| `Provisa-<tag>-windows-x64.exe` | Nativer Windows-x64-Installer (eingebettetes Python, ohne Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Windows-x64-Upgrade auf die Container-Stufe (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | JDBC-Treiber — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Python-Client (auch auf PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Tarballs der Core-Services-Images (arm64, airgapped) |
| `provisa-core-images-amd64-<tag>.zip` | Core-Services-Images (amd64, Windows-Container-Stufe / airgapped) |
| `provisa-obs-images-<tag>.tar.gz` | Images des Observability Stack (optional) |
| `provisa-demo-images-<tag>.tar.gz` | Images des Demo Data Pack (optional) |
| `provisa-trino-plugins-<tag>.tar.gz` | Connectors der Coordination Engine (SharePoint, Splunk, File) |

Die Version des Python-Clients wird automatisch in das PEP-440-Format umgewandelt:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## PyPI-Veröffentlichung einrichten (einmalig) {: #pypi-publishing-setup-one-time }

Die Jobs `publish-pypi` (`provisa-client`) und `publish-provisa-pypi` (`provisa`) in `.github/workflows/build-dmg.yml` veröffentlichen mit PyPI Trusted Publishing: Das GitHub-OIDC-Token des Jobs (`id-token: write`) authentifiziert ihn, und es wird kein API-Token gelesen. Registrieren Sie dieses Repository und `build-dmg.yml` für jedes Projekt auf pypi.org als Trusted Publisher (**Your projects → Manage → Publishing**). Die Veröffentlichungen laufen dann bei jedem Tag automatisch.

## Signierung und Lieferkette {: #signing-and-supply-chain }

Was jedes Release-Artefakt trägt, so wie die Release-Workflows es bauen:

| Artefakt | Signatur |
| -------- | --------- |
| `provisa` und `provisa-client` auf PyPI | Sigstore-signierte PEP-740-Attestation pro Datei (`attestations: true`), auf pypi.org prüfbar |
| `provisa-pg-ext`, `provisa-duckdb-ext` auf PyPI | Sigstore-signierte PEP-740-Attestation pro Datei |
| JDBC-Treiber-`.jar` | Sigstore-Build-Provenance-Attestation (`actions/attest-build-provenance`) |
| macOS-`.dmg`-Installer | Developer-ID-Codesignatur und Apple-Notarisierung |
| Windows-`.exe`-Installer | Nicht signiert |
| Linux-`.AppImage` | Nicht signiert |

Die signierte Installation auf jedem Betriebssystem ist `pip install provisa`: das Sigstore-attestierte PyPI-Paket. Ein Prüfer, der ein signiertes Artefakt unter Linux oder Windows braucht, installiert mit pip. Der Windows-Build signiert mit Authenticode, wenn ein Code-Signing-Zertifikat konfiguriert ist (`WINDOWS_CERT_PFX_BASE64`, `WINDOWS_CERT_PFX_PASSWORD`, `WINDOWS_CERT_TIMESTAMP_URL`, gelesen von `packaging/windows/build-sfx.ps1`); keines ist konfiguriert, daher wird er unsigniert ausgeliefert.

Abhängigkeiten und CVE-Gates:

- Python-Abhängigkeiten sind mit Hashes in `uv.lock` gesperrt; CI installiert sie mit `uv sync --frozen`.
- Der Workflow `CVE Audit` (`.github/workflows/cve-audit.yml`) führt `pip-audit` über die gesperrte Menge bei jedem Push auf `main`, bei jedem Pull Request und wöchentlich aus.
- Beim Release erzeugen die Builds von `provisa` und `provisa-client` jeweils ein CycloneDX-SBOM und führen `pip-audit` über das Ausgelieferte aus. Der JDBC-Treiber wird mit `grype --fail-on medium` gescannt. Ein Fund lässt den Job fehlschlagen, und das Release veröffentlicht nichts: Die PyPI-Uploads und das GitHub-Release warten beide auf diese Jobs. Die SBOMs und CVE-Berichte werden an jedes Release angehängt.
- Die Desktop-Installer bauen das `provisa`-Wheel auf dem Release-Runner aus dem Quellcode und lösen seine Abhängigkeiten zur Build-Zeit mit `pip` auf, nicht aus `uv.lock`; das CVE-Gate des Releases scannt die gesperrte Menge, nicht die gebündelten Abhängigkeiten der Installer.
- `packaging/pypi/requirements-embedded.lock` ist eine Hash-gepinnte Auflösung von `provisa[embedded]` für Airgap-Mirrors (siehe `packaging/pypi/MIRROR-CHECKLIST.md`). Sie wird von Hand neu erzeugt, nicht pro Release.

## Erforderliche Repository-Secrets {: #required-repository-secrets }

Konfigurieren Sie Folgendes unter **Settings → Secrets → Actions**:

| Secret | Erforderlich für | Beschreibung |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | Signierte Builds | Base64-codierte `.p12`-Zertifikatsdatei (siehe unten) |
| `APPLE_CERT_P12_PASSWORD` | Signierte Builds | Beim Export des `.p12` aus Keychain Access festgelegtes Passwort |
| `APPLE_DEVELOPER_ID` | Signierte Builds | Vollständiger Zertifikatsname: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Notarisierte Builds | E-Mail-Adresse der Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | Notarisierte Builds | App-spezifisches Passwort von appleid.apple.com (nicht Ihr Anmeldepasswort) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Notarisierte Builds | 10-stellige Apple-Team-ID |

Builds ohne diese Secrets werden erfolgreich abgeschlossen, erzeugen jedoch ein unsigniertes/nicht notarisiertes DMG (Benutzer sehen eine Gatekeeper-Warnung).

## Das .p12-Zertifikat exportieren {: #exporting-the-p12-certificate }

1. Öffnen Sie **Keychain Access** → Anmeldeschlüsselbund → **My Certificates**
2. Suchen Sie **Developer ID Application: Your Name (TEAMID)** und erweitern Sie den Eintrag, um zu bestätigen, dass der private Schlüssel darunter eingebettet ist
3. Wählen Sie sowohl das Zertifikat als auch den zugehörigen privaten Schlüssel aus → Rechtsklick → **Export 2 Items** → als `.p12` speichern → ein sicheres Passwort festlegen
4. Base64-codieren und in die Zwischenablage kopieren:

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. Fügen Sie den Wert als `APPLE_CERT_P12_BASE64` ein; setzen Sie `APPLE_CERT_P12_PASSWORD` auf das Passwort aus Schritt 3

## Den Namen Ihres Zertifikats finden {: #finding-your-certificate-name }

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Kopieren Sie die vollständige Zeichenfolge in Anführungszeichen. Diese Zeichenfolge ist der Wert für `APPLE_DEVELOPER_ID`.

## Ein fehlerhaftes Tag löschen {: #deleting-a-bad-tag }

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Löschen Sie anschließend das entsprechende GitHub Release in der Oberfläche, bevor Sie erneut taggen.
